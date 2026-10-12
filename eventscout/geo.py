"""Location filter, matching the place names listed in channels.yaml and, for
an event that carries coordinates, the distance to its search area."""
from __future__ import annotations

import math
import re
from collections.abc import Sequence

from .models import Event, coordinates_or_none

_EARTH_RADIUS_MI = 3958.8


def _miles(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle distance between two (latitude, longitude) points."""
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * _EARTH_RADIUS_MI * math.asin(math.sqrt(h))

# Marks a location naming no fixed venue at all, which accept_virtual decides.
# Only "remote only" also stops a named Bay Area city from counting, because it
# says the event cannot be attended in person. "Anywhere" and "worldwide" sit
# beside a city in hybrid listings ("In person in San Jose or join from
# anywhere"), and such an event is attendable whatever accept_virtual says.
# Broadening to bare "remote" was rejected without data: Bay Area hybrid events
# routinely say "in-person or remote", and excluding those would lose real
# events to catch a phrasing not yet observed in any verified feed.
_REMOTE_ONLY = "remote only"
_NEGATIVE = (_REMOTE_ONLY, "anywhere", "worldwide")

# A location that names no place because the event has none. Measured on
# studentlife.bayarea.northeastern.edu, which sets eventAttendanceMode to
# "offline" while writing "Virtual" as the location on the same event, so the
# declared mode alone misses them. Where the two disagree the text wins, since
# a human wrote it for the attendee and the mode field is boilerplate.
_VIRTUAL_HINTS = ("virtual", "online", "zoom", "teams meeting", "webinar")


_US_STATES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar", "california": "ca",
    "colorado": "co", "connecticut": "ct", "delaware": "de", "district of columbia": "dc",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id", "illinois": "il",
    "indiana": "in", "iowa": "ia", "kansas": "ks", "kentucky": "ky", "louisiana": "la",
    "maine": "me", "maryland": "md", "massachusetts": "ma", "michigan": "mi",
    "minnesota": "mn", "mississippi": "ms", "missouri": "mo", "montana": "mt",
    "nebraska": "ne", "nevada": "nv", "new hampshire": "nh", "new jersey": "nj",
    "new mexico": "nm", "new york": "ny", "north carolina": "nc", "north dakota": "nd",
    "ohio": "oh", "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa",
    "rhode island": "ri", "south carolina": "sc", "south dakota": "sd", "tennessee": "tn",
    "texas": "tx", "utah": "ut", "vermont": "vt", "virginia": "va", "washington": "wa",
    "west virginia": "wv", "wisconsin": "wi", "wyoming": "wy",
}
_STATE_CODES = {code: name for name, code in _US_STATES.items()}


def _normalise(location: str) -> str:
    """Lower-cased, with whitespace and the space after each comma made uniform.

    The location text is never rewritten beyond that, because folding a state's
    name to its code turned "Klaus Building, Georgia Tech" into ", ga tech" and
    cost the entry "georgia tech" its own campus, and folding only where the
    name closed its comma segment missed "Dublin, California - Library". Both
    spellings are matched from the entry's side instead (see _state_variants).
    """
    text = re.sub(r"\s+", " ", (location or "").lower())
    text = re.sub(r"\s*,\s*", ", ", text)
    # Stripped so a quoted entry such as "san jose " still compiles to a
    # pattern that can match.
    return text.strip()


def _state_variants(entry: str) -> tuple[str, ...]:
    """A normalised place entry, plus the same entry with its trailing US state
    written the other way, so "dublin, ca" also matches "Dublin, California"."""
    head, sep, tail = entry.rpartition(", ")
    if not sep:
        return (entry,)
    other = _STATE_CODES.get(tail) or _US_STATES.get(tail)
    return (entry, f"{head}, {other}") if other else (entry,)


class GeoFilter:
    """Keeps attendable events, satisfying EventFilter with a single argument.

    `regional_sources` comes from channels.yaml's per-source `in_region` flag
    and is resolved once at construction rather than passed per call, so
    `keep` can stay single-arg and match the `EventFilter` protocol without
    callers having to isinstance-check for a wider signature.

    `places` are matched as whole words, so "dublin, ca" does not match
    "Dublin, Canada" and "pittsburg" does not match "Pittsburgh".

    `search_area` is (latitude, longitude, radius in miles) per centre, from
    channels.yaml geo.search_area. An event with coordinates within any one
    radius is in region. Empty leaves the name rules as the only test.
    """

    def __init__(
        self,
        places: Sequence[str],
        accept_virtual: bool = True,
        regional_sources: frozenset[str] = frozenset(),
        search_area: Sequence[tuple[float, float, float]] = (),
    ):
        # Validated here rather than in build_geo, because a blank entry
        # compiles to a pattern that matches every location and a bare string
        # iterates into one-letter patterns. Neither raises on its own.
        if isinstance(places, str):
            raise TypeError("GeoFilter places must be a sequence of names, not one string")
        names = [_normalise(p) if isinstance(p, str) else "" for p in places]
        if not names or not all(n.strip(" ,") for n in names):
            raise ValueError("GeoFilter needs at least one place name and no blank or "
                             "non-string entry (channels.yaml geo.places)")
        self._place_patterns = tuple(
            re.compile(rf"(?<![a-z]){re.escape(v)}(?![a-z])")
            for n in names for v in _state_variants(n))
        self._accept_virtual = accept_virtual
        self._regional_sources = regional_sources
        # Neither raises on its own. A zero or negative radius admits nothing,
        # and a centre with latitude and longitude swapped puts every Bay Area
        # venue thousands of miles out.
        centres = []
        for c in search_area:
            point = coordinates_or_none(c[0], c[1]) if len(c) == 3 else None
            if point is None or not c[2] > 0:
                raise ValueError("GeoFilter search_area needs (latitude, longitude, radius > 0) "
                                 f"per centre (channels.yaml geo.search_area), got {c!r}")
            centres.append((point, float(c[2])))
        self._search_area = tuple(centres)

    def keep(self, event: Event) -> bool:
        # A named Bay Area city wins over everything, INCLUDING virtual, unless
        # the text says remote only. A hybrid listed as "Online, San Francisco,
        # CA" is attendable in person, so accept_virtual must not get to decide
        # it. accept_virtual only adds events back.
        if self._matches_name(event.location):
            return True
        # Everything below here did not match by name, because no Bay Area city
        # is named or because the text says remote only. An event whose location
        # text says it is online, or names nowhere on earth ("Remote only",
        # "Worldwide"), is accept_virtual's decision and nothing else's. Falling through
        # instead reached the two fallbacks, which answer different questions:
        # page_unreadable means nothing could be READ, and the in-region
        # fallback means no city was NAMED, while these locations named
        # something that simply is not a place. That is how a remote-only event
        # from an in_region source was kept as if it were local, and why setting
        # accept_virtual false changed nothing for it. An earlier review
        # proposed rejecting these outright instead, which would drop the online
        # events this flag exists to keep.
        if self._text_says_virtual(event.location) or self._states_nowhere(event.location):
            return self._accept_virtual
        # Coordinates settle what the name rules below cannot, a venue-only
        # location or a town the list leaves out. On one fetch the name rules
        # dropped 37 events from four sources that carried coordinates, and all
        # 37 were within 50 miles. After the name match so that fast path is
        # unchanged, and ahead of the online-mode test, as a named city is, so
        # a position decides whatever accept_virtual says.
        if event.coordinates is not None and self._search_area:
            return any(_miles(event.coordinates, centre) <= radius
                       for centre, radius in self._search_area)
        # A named non-Bay-Area city beats an online attendance MODE, as
        # _VIRTUAL_HINTS gives text precedence over the mode. cerebralvalley.ai
        # marks World AI Week online (VirtualLocation) while its own page names
        # Amsterdam, which the extractor wrote into the location. Testing the
        # mode first mailed it as a normal pick. Cost, an online listing located
        # at its organizer's non-Bay-Area city is now dropped too. A 415-row
        # ledger held only two online rows naming any place, Amsterdam and
        # Dubai, both from cerebralvalley.ai.
        #
        # Both remaining fallbacks also apply only when the location names
        # nowhere at all, so an event that says somewhere else is out
        # regardless of either.
        if self._names_a_place(event.location):
            return False
        if event.is_virtual:
            return self._accept_virtual
        # An unread page states no location because nothing could be read, not
        # because the event is elsewhere; dropping it here is the silent miss
        # this project exists to prevent, so it goes on to be scored instead.
        #
        # A source in the region naming no city is the other case: without it
        # venue-only locations sink a whole feed, since the Bay Area campus
        # student-life calendar writes "Welcome Center" and "Room 1045" and all
        # 15 of its events were dropped as out of region.
        return event.page_unreadable or event.source in self._regional_sources

    @staticmethod
    def _text_says_virtual(location: str) -> bool:
        text = (location or "").lower()
        return any(hint in text for hint in _VIRTUAL_HINTS)

    @staticmethod
    def _states_nowhere(location: str) -> bool:
        """Does this location name something that is not a place?

        Reads all of _NEGATIVE, while _matches_name reads only _REMOTE_ONLY,
        because "anywhere" beside a named city still describes an event that
        can be attended in person. Every term _matches_name refuses on must
        also be in _NEGATIVE, or a refused location falls through to the
        fallbacks and an in_region source keeps it as local.
        """
        text = _normalise(location)
        return any(n in text for n in _NEGATIVE)

    def _matches_name(self, location: str) -> bool:
        text = _normalise(location)
        if not text or _REMOTE_ONLY in text:
            return False
        return any(p.search(text) for p in self._place_patterns)

    @staticmethod
    def _names_a_place(location: str) -> bool:
        """True when the string looks like a settlement rather than a venue.

        "San Jose, CA" and "Boston, MA" qualify; "Room 1045" and "Welcome Center"
        do not. Used by `keep` to stop either of its fallbacks from overriding
        a location that actively says the event is somewhere else.
        """
        text = re.sub(r"\s+", " ", (location or "").strip())
        return bool(re.search(r",\s*[A-Z]{2}\b", text) or re.search(r",\s*[A-Z][a-z]+$", text))
