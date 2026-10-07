"""Bay Area location filter, matching the place names listed in channels.yaml."""
from __future__ import annotations

import re
from collections.abc import Sequence

from .models import Event

# Rejects a location naming no fixed venue at all, e.g. "Remote only"; a city
# listed alongside others still passes, e.g. "San Francisco, New York, or
# remote". Broadening to bare "remote" was rejected without data: Bay Area
# hybrid events routinely say "in-person or remote", and excluding those would
# lose real events to catch a phrasing not yet observed in any verified feed.
_NEGATIVE = ("remote only", "anywhere", "worldwide")

# A location that names no place because the event has none. Measured on
# studentlife.bayarea.northeastern.edu, which sets eventAttendanceMode to
# "offline" while writing "Virtual" as the location on the same event, so the
# declared mode alone misses them. Where the two disagree the text wins, since
# a human wrote it for the attendee and the mode field is boilerplate.
_VIRTUAL_HINTS = ("virtual", "online", "zoom", "teams meeting", "webinar")


def _normalise(location: str) -> str:
    """Lower-cased, with ", California" folded to ", ca" so one entry such as
    "dublin, ca" matches both spellings Luma emits within one feed ("San
    Francisco, CA" and "San Francisco, California").
    """
    text = re.sub(r"\s+", " ", (location or "").lower())
    text = re.sub(r"\s*,\s*", ", ", text)
    # Stripped so a quoted entry such as "san jose " still compiles to a
    # pattern that can match.
    return re.sub(r", california\b", ", ca", text).strip()


class GeoFilter:
    """Keeps attendable events, satisfying EventFilter with a single argument.

    `regional_sources` comes from channels.yaml's per-source `in_region` flag
    and is resolved once at construction rather than passed per call, so
    `keep` can stay single-arg and match the `EventFilter` protocol without
    callers having to isinstance-check for a wider signature.

    `places` are matched as whole words, so "dublin, ca" does not match
    "Dublin, Canada" and "pittsburg" does not match "Pittsburgh".
    """

    def __init__(
        self,
        places: Sequence[str],
        accept_virtual: bool = True,
        regional_sources: frozenset[str] = frozenset(),
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
            re.compile(rf"(?<![a-z]){re.escape(n)}(?![a-z])") for n in names)
        self._accept_virtual = accept_virtual
        self._regional_sources = regional_sources

    def keep(self, event: Event) -> bool:
        # A named Bay Area city wins over everything, INCLUDING virtual. A
        # hybrid listed as "Online, San Francisco, CA" is attendable in person,
        # so accept_virtual must not get to decide it.
        if self._matches_name(event.location):
            return True
        # Everything below here named no Bay Area city. An event whose location
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

        Beside _matches_name because the two read the same _NEGATIVE list for
        different questions. This one asks "is this a non-place", that one asks
        the weaker "does this prove Bay Area presence", so a term added for
        either cannot be missed by the other.
        """
        text = _normalise(location)
        return any(n in text for n in _NEGATIVE)

    def _matches_name(self, location: str) -> bool:
        text = _normalise(location)
        if not text or any(n in text for n in _NEGATIVE):
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
