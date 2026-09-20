"""Geographic scoping for the South Bay anchors in channels.yaml."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass

from .models import Event

# Substring match on free-text location: schema.org gives a display string,
# not coordinates, so the radius test below has nothing to work with without a
# geocoder. Spellings vary within one feed, e.g. Luma emits both "San
# Francisco, CA" and "San Francisco, California".
_BAY_CITIES = (
    "san francisco", "san jose", "mountain view", "santa clara", "palo alto",
    "sunnyvale", "oakland", "berkeley", "menlo park", "cupertino", "fremont",
    "redwood city", "san mateo", "milpitas", "campbell", "los altos",
    "foster city", "burlingame", "emeryville", "alameda", "hayward",
    "bay area", "silicon valley",
)

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


@dataclass(frozen=True)
class Anchor:
    label: str
    lat: float
    lon: float
    radius_mi: float


def _haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class GeoFilter:
    """Keeps attendable events, satisfying EventFilter with a single argument.

    `regional_sources` comes from channels.yaml's per-source `in_region` flag
    and is resolved once at construction rather than passed per call, so
    `keep` can stay single-arg and match the `EventFilter` protocol without
    callers having to isinstance-check for a wider signature.
    """

    def __init__(
        self,
        anchors: list[Anchor],
        accept_virtual: bool = True,
        regional_sources: frozenset[str] = frozenset(),
    ):
        if not anchors:
            raise ValueError("GeoFilter needs at least one anchor")
        self._anchors = anchors
        self._accept_virtual = accept_virtual
        self._regional_sources = regional_sources

    def keep(self, event: Event) -> bool:
        # A named Bay Area city wins over everything, INCLUDING virtual. A
        # hybrid listed as "Online, San Francisco, CA" is attendable in person,
        # so accept_virtual must not get to decide it.
        if self._matches_name(event.location):
            return True
        # Everything below here named no Bay Area city. An event that is online,
        # or whose location names nowhere on earth ("Remote only", "Worldwide"),
        # is accept_virtual's decision and nothing else's. Falling through
        # instead reached the two fallbacks, which answer different questions:
        # page_unreadable means nothing could be READ, and the in-region
        # fallback means no city was NAMED, while these locations named
        # something that simply is not a place. That is how a remote-only event
        # from an in_region source was kept as if it were local, and why setting
        # accept_virtual false changed nothing for it. An earlier review
        # proposed rejecting these outright instead, which would drop the online
        # events this flag exists to keep.
        if self._is_virtual(event) or self._states_nowhere(event.location):
            return self._accept_virtual
        # Below here the location matched no Bay Area name. Both remaining
        # fallbacks apply only when it names nowhere at all, so an event that
        # explicitly says somewhere else is out regardless of either.
        if self._names_a_place(event.location):
            return False
        # An unread page states no location because nothing could be read, not
        # because the event is elsewhere; dropping it here is the silent miss
        # this project exists to prevent, so it goes on to be scored instead.
        #
        # A source in the region naming no city is the other case: without it
        # venue-only locations sink a whole feed, since the Bay Area campus
        # student-life calendar writes "Welcome Center" and "Room 1045" and all
        # 15 of its events were dropped as out of region.
        return event.page_unreadable or event.source in self._regional_sources

    def distance_mi(self, lat: float, lon: float) -> float:
        """Miles to the nearest anchor; not used by `keep` yet.

        Every source reports a city string and never a coordinate, so there is
        nothing to measure against and `keep` matches on names instead. Retained
        so the configured radius has somewhere to plug in: wire a geocoder in as
        a constructor dependency, and call it only for events that arrive
        without a coordinate.
        """
        return min(_haversine_mi(lat, lon, a.lat, a.lon) for a in self._anchors)

    def _is_virtual(self, event: Event) -> bool:
        if event.is_virtual:
            return True
        text = (event.location or "").lower()
        return any(hint in text for hint in _VIRTUAL_HINTS)

    @staticmethod
    def _states_nowhere(location: str) -> bool:
        """Does this location name something that is not a place?

        Beside _matches_name because the two read the same _NEGATIVE list for
        different questions. This one asks "is this a non-place", that one asks
        the weaker "does this prove Bay Area presence", so a term added for
        either cannot be missed by the other.
        """
        text = re.sub(r"\s+", " ", (location or "").lower())
        return any(n in text for n in _NEGATIVE)

    @staticmethod
    def _matches_name(location: str) -> bool:
        text = re.sub(r"\s+", " ", (location or "").lower())
        if not text or any(n in text for n in _NEGATIVE):
            return False
        return any(city in text for city in _BAY_CITIES)

    @staticmethod
    def _names_a_place(location: str) -> bool:
        """True when the string looks like a settlement rather than a venue.

        "San Jose, CA" and "Boston, MA" qualify; "Room 1045" and "Welcome Center"
        do not. Used by `keep` to stop either of its fallbacks from overriding
        a location that actively says the event is somewhere else.
        """
        text = re.sub(r"\s+", " ", (location or "").strip())
        return bool(re.search(r",\s*[A-Z]{2}\b", text) or re.search(r",\s*[A-Z][a-z]+$", text))
