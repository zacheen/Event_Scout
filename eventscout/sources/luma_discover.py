"""Luma discover listing source, read through the API the page itself pages with.

luma.com/discover/<place>/<category> server-renders only its first 25 events as
JSON-LD and loads the rest as the reader scrolls. During SF Tech Week those 25
covered about a day, and JsonLdSource first saw 131 of the listing's 142 dated
events less than two days before they started. The scroll calls
api.luma.com/discover/get-paginated-events, which answers without a login and
continues from a cursor encoding the last entry's start and id. Verified
2026-10-09: at most 50 entries a page, typed start and end instants, and an
address that names the city. The JSON-LD named only a venue or a street for 15
of the same 25 events, such as "Mox" or "977 Howard St", and the geo filter
dropped all 15 as out of region. Neither the JSON-LD nor the API carries a
description.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, ClassVar
from urllib.parse import urlencode

from ..http import HttpClient
from ..models import AttendanceMode, Coverage, Event, coordinates_or_none, iso_or_empty, parse_iso
from ..urls import canon_url

log = logging.getLogger(__name__)

_ENDPOINT = "https://api.luma.com/discover/get-paginated-events"
# The endpoint's own ceiling. Asking for 100 returns 50.
_PAGE_SIZE = 50
_MODE_BY_TYPE = {"offline": AttendanceMode.OFFLINE, "online": AttendanceMode.ONLINE}
# Shown as the calendar of an event run from a personal account. It names
# nobody, so it is not an organizer.
_PLACEHOLDER_CALENDAR = "Personal"


class LumaDiscoverSource:
    kind: ClassVar[str] = "luma_discover"

    def __init__(self, name: str, category: str, latitude: float, longitude: float,
                 http: HttpClient, days_ahead: int = 21, max_pages: int = 10,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        if not category:
            raise ValueError(f"{name}: a Luma discover source needs its category slug")
        self.name = name
        self._params = {"latitude": str(latitude), "longitude": str(longitude),
                        "pagination_limit": str(_PAGE_SIZE), "slug": category}
        self._http = http
        self._days_ahead = days_ahead
        self._max_pages = max_pages
        self._now = now
        self._coverage: Coverage | None = None
        self._fetched = False

    def fetch(self) -> list[Event]:
        horizon = self._now() + timedelta(days=self._days_ahead)
        entries: list[dict] = []
        cursor = None
        self._coverage = None
        for page in range(1, self._max_pages + 1):
            try:
                data = self._page(cursor)
            except Exception as exc:
                # The first page is the whole source, so its failure raises. A
                # later one keeps what was read and reports the gap through
                # coverage(), as WordPressSource does, since a 429 on page 4
                # would otherwise discard pages 1 to 3.
                if page == 1:
                    raise
                log.warning("%s: page %d failed, keeping %d entries: %s",
                            self.name, page, len(entries), exc)
                self._coverage = Coverage(len(entries), "events", None)
                break
            batch = data["entries"]
            entries.extend(batch)
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
            # has_more with an empty page or no cursor to continue from is a
            # gap, not the end.
            if not batch or not cursor:
                self._coverage = Coverage(len(entries), "events", None)
                break
            if self._reaches(batch, horizon):
                break
        else:
            self._coverage = Coverage(len(entries), "events", None)
        events = [self._to_event(entry) for entry in entries]
        self._fetched = True
        return [e for e in events if e]

    def coverage(self) -> Coverage | None:
        if not self._fetched:
            # See BoundedSource.coverage() for why this must raise rather
            # than return None.
            raise RuntimeError(f"{self.name}: coverage() called before fetch()")
        return self._coverage

    def _page(self, cursor: str | None) -> dict:
        params = dict(self._params)
        if cursor:
            params["pagination_cursor"] = cursor
        data = json.loads(self._http.get_text(f"{_ENDPOINT}?{urlencode(params)}"))
        # A missing list means the response shape changed. Reading it as empty
        # would be the silent zero fetch() must never return.
        if not isinstance(data, dict) or not isinstance(data.get("entries"), list):
            raise ValueError(f"{self.name}: response carries no entries list")
        return data

    @staticmethod
    def _reaches(batch: list[dict], horizon: datetime) -> bool:
        """Whether this non-empty page ends at or past `horizon`. Pages run in
        start order, because the cursor is the last entry's start and id, so
        the last entry decides."""
        last = parse_iso(str((batch[-1].get("event") or {}).get("start_at") or ""))
        return last is not None and last >= horizon

    def _to_event(self, entry: dict) -> Event | None:
        item = entry.get("event") or {}
        slug = str(item.get("url") or "").strip().strip("/")
        title = str(item.get("name") or "").strip()
        if not slug or not title:
            return None
        url = canon_url(f"https://luma.com/{slug}")
        mode = _MODE_BY_TYPE.get(str(item.get("location_type") or ""), AttendanceMode.UNKNOWN)
        # A blank location sends the event to PageFactExtractor, which would
        # fetch the event page to learn what location_type already says.
        location = self._location(item) or ("Online" if mode is AttendanceMode.ONLINE else "")
        # A guests-only event's coordinate is moved to a point inside its city
        # rather than withheld, as probed on 2026-10-09, close enough for a
        # distance in miles.
        point = item.get("coordinate")
        coordinates = (coordinates_or_none(point.get("latitude"), point.get("longitude"))
                       if isinstance(point, dict) else None)
        return Event(
            event_uid=f"{self.kind}:{url}",
            title=title,
            url=url,
            source=self.name,
            source_kind=self.kind,
            start=self._instant(item, "start_at"),
            end=self._instant(item, "end_at"),
            organizer=self._organizer(entry),
            location=location,
            coordinates=coordinates,
            attendance_mode=mode,
            description="",
            published_at="",
        )

    def _instant(self, item: dict, key: str) -> str:
        """item[key] as ISO 8601, or "" when it is absent.

        A value that is present but unreadable raises, naming the field, as
        EightfoldSource._iso does. Read as absent it would leave every event
        undated, which sends the whole listing to the extractor at one page
        fetch each, and an undated event passes the date window.
        """
        value = item.get(key)
        if value in (None, ""):
            return ""
        text = iso_or_empty(value) if isinstance(value, str) else ""
        if not text:
            raise ValueError(f"{self.name}: {key} {value!r} is not an ISO instant")
        return text

    @staticmethod
    def _location(item: dict) -> str:
        """full_address when the host made it public, else the city.

        A guests-only address arrives obfuscated to city_state, as in 21 of 50
        entries probed, and the city alone is what the geo filter needs.
        full_address already names the city, so nothing is appended to it.
        """
        geo = item.get("geo_address_info") or {}
        full = str(geo.get("full_address") or "").strip()
        return full or str(geo.get("city_state") or "").strip()

    @staticmethod
    def _organizer(entry: dict) -> str:
        """The calendar, then each host, each named once.

        Matches the organizer the discover page's JSON-LD gave, so the scorer
        sees the same names.
        """
        calendar = str((entry.get("calendar") or {}).get("name") or "").strip()
        names = [calendar] if calendar != _PLACEHOLDER_CALENDAR else []
        names += [str(h.get("name") or "").strip()
                  for h in entry.get("hosts") or [] if isinstance(h, dict)]
        return ", ".join(dict.fromkeys(n for n in names if n))
