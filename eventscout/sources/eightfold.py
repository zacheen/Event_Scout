"""Eightfold employer event source.

Many employers run their recruiting events on Eightfold, each at
<tenant>.eightfold.ai/events/open?domain=<employer domain>. That page renders
in script, and its only JSON-LD is a page-level placeholder with no date and no
URL, so JsonLdSource finds nothing there. This reads the JSON list the page
itself calls instead, which carries epoch start and end timestamps, the venue
and the attendance mode as typed fields, so no model is needed. Verified
2026-10-09 against AstraZeneca, Qualcomm and Lam Research.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import ClassVar
from urllib.parse import parse_qs, urlencode, urlsplit

from ..http import HttpClient
from ..models import AttendanceMode, Coverage, Event, iso_or_empty
from ..urls import canon_url
from ._text import strip_html

_MODE_BY_TYPE = {
    "in_person": AttendanceMode.OFFLINE,
    "virtual_event": AttendanceMode.ONLINE,
    "virtual_and_in_person": AttendanceMode.MIXED,
}

_DESCRIPTION_MAX = 2000

# 2100-01-01 UTC. A millisecond epoch is a thousand times the seconds for the
# same instant, so a unit change lands above this and raises naming the field,
# rather than reaching fromtimestamp's unnamed OSError or ValueError.
_EPOCH_CEILING = 4_102_444_800


class EightfoldSource:
    kind: ClassVar[str] = "eightfold"

    def __init__(self, name: str, url: str, http: HttpClient, max_pages: int = 20):
        parts = urlsplit(url)
        domain = (parse_qs(parts.query).get("domain") or [""])[0].strip()
        # Refused here, not at fetch: the list endpoint answers HTTP 401
        # without `domain`, which reads as the tenant revoking access rather
        # than a URL copied without its query string. A URL without a scheme
        # has no host and would fail at fetch with an unrelated urllib error.
        if not domain or not parts.netloc:
            raise ValueError(f"{name}: Eightfold events URL needs https://<host> and "
                             f"the ?domain= parameter the list endpoint requires: {url!r}")
        self.name = name
        self._domain = domain
        self._endpoint = f"{parts.scheme or 'https'}://{parts.netloc}/api/events/open/list"
        self._http = http
        self._max_pages = max_pages
        self._coverage: Coverage | None = None
        self._fetched = False

    def fetch(self) -> list[Event]:
        items: list[dict] = []
        total = None
        self._coverage = None
        for page in range(1, self._max_pages + 1):
            # Unlike WordPressSource, no page is guarded. This endpoint answers
            # past the end with an empty list, not an error, so a failure on
            # page 2 is a real outage, and skipping it silently drops events.
            data = self._page(page)
            batch = data["plannedEventList"]
            total = data.get("totalEventsCount")
            items.extend(batch)
            # The server, not this client, sets the page size (10 on every
            # tenant probed), so a short page is no end signal.
            if not batch or (isinstance(total, int) and len(items) >= total):
                break
        else:
            self._coverage = Coverage(len(items), "events",
                                      total if isinstance(total, int) else None)
        events = [self._to_event(item) for item in items]
        self._fetched = True
        return [e for e in events if e]

    def coverage(self) -> Coverage | None:
        if not self._fetched:
            # See BoundedSource.coverage() for why this must raise rather
            # than return None.
            raise RuntimeError(f"{self.name}: coverage() called before fetch()")
        return self._coverage

    def _page(self, page: int) -> dict:
        raw = self._http.get_text(
            f"{self._endpoint}?{urlencode({'domain': self._domain, 'page': page})}")
        data = json.loads(raw)
        # A missing list means the response shape changed, since a tenant with
        # nothing listed still sends the key. Reading it as empty would be the
        # silent zero fetch() must never return.
        if not isinstance(data, dict) or not isinstance(data.get("plannedEventList"), list):
            raise ValueError(f"{self.name}: page {page} carries no plannedEventList")
        return data

    def _to_event(self, item: dict) -> Event | None:
        if item.get("isCancelled"):
            return None
        # The landing page addresses the event by its ?plannedEventId= query,
        # which canon_url keeps, so events of one tenant stay distinct.
        url = canon_url(str(item.get("eventLandingPage") or ""))
        title = strip_html(str(item.get("name") or ""))
        if not url or not title:
            return None
        mode = _MODE_BY_TYPE.get(str(item.get("eventLocationType") or ""),
                                 AttendanceMode.UNKNOWN)
        # A virtual event names no venue, and a blank location sends the event
        # to PageFactExtractor, which would fetch the script-rendered page to
        # learn what eventLocationType already says.
        location = self._location(item) or ("Online" if mode is AttendanceMode.ONLINE else "")
        return Event(
            event_uid=f"{self.kind}:{url}",
            title=title,
            url=url,
            source=self.name,
            source_kind=self.kind,
            start=self._iso(item, "startTimestamp"),
            end=self._iso(item, "endTimestamp"),
            # No employer name field exists, and the scorer's access_value asks
            # which employer the event leads to. Many titles never say, such as
            # Lam Research's "2026 Stanford University Career Fair Fall (US)",
            # so the domain from the configured URL stands in for it.
            organizer=self._domain,
            location=location,
            attendance_mode=mode,
            description=strip_html(str(item.get("description") or ""))[:_DESCRIPTION_MAX],
            # publishJson carries publish timestamps, all null on every event
            # probed, so there is nothing to report.
            published_at="",
        )

    def _iso(self, item: dict, key: str) -> str:
        """item[key], in epoch seconds, as ISO 8601 in UTC, or "" when absent.

        A value that is present but is not epoch seconds raises, naming the
        field. Reading it as absent would leave the event undated, and an
        undated event passes the date window (Known_concern #12).
        """
        value = item.get(key)
        if value in (None, "", 0):
            return ""
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not 0 < value < _EPOCH_CEILING):
            raise ValueError(f"{self.name}: {key} {value!r} is not epoch seconds")
        return iso_or_empty(datetime.fromtimestamp(value, timezone.utc).isoformat())

    @staticmethod
    def _location(item: dict) -> str:
        """venue, completeVenue and institution joined, each named once.

        completeVenue usually repeats venue at its head ("Canfield Court" then
        "Canfield Court, Stanford Campus"). institution is often the only field
        naming a campus, for example Georgia Tech beside a venue of "TBD".
        It counts as already named only as a whole word, since "MIT" is a
        substring of "Smith Auditorium". `address` is left out, because a
        virtual event puts its meeting link there.
        """
        venue = strip_html(str(item.get("venue") or ""))
        full = strip_html(str(item.get("completeVenue") or ""))
        institution = strip_html(str(item.get("institution") or ""))
        parts = [full] if venue and full.lower().startswith(venue.lower()) else [venue, full]
        text = ", ".join(p for p in parts if p)
        if institution and not re.search(rf"(?<!\w){re.escape(institution)}(?!\w)",
                                         text, re.IGNORECASE):
            text = ", ".join(p for p in (institution, text) if p)
        return text
