"""schema.org JSON-LD event source, the highest-leverage adapter in the
project: a page's <script type="application/ld+json"> Event node gives
startDate, endDate, location, organizer and eventAttendanceMode as typed
fields, so this path needs no LLM and cannot hallucinate a date. Verified
2026-09-07 against four sites (two Luma calendars, one university
student-life site, one AI events site) exposing 70 events through one parser.

The parsing itself lives in _jsonld.py, because the extractor reads the same
markup on an individual event's page.
"""
from __future__ import annotations

from typing import ClassVar

from ..http import HttpClient
from ..models import AttendanceMode, Event, iso_or_empty
from ..urls import canon_url
from ._jsonld import event_nodes, flatten_text



class JsonLdSource:
    kind: ClassVar[str] = "jsonld"

    def __init__(self, name: str, url: str, http: HttpClient):
        self.name = name
        self._url = url
        self._http = http

    def fetch(self) -> list[Event]:
        # Unguarded on purpose: a 403 or timeout must propagate. Returning []
        # would be recorded as "no new events", which is exactly how a broken
        # scraper stays invisible for weeks.
        html = self._http.get_text(self._url)
        events, seen = [], set()
        for node in event_nodes(html):
            event = self._to_event(node)
            # One page can list the same event in both an ItemList and a
            # standalone block; key on the canonical URL so the duplicate is
            # dropped here rather than reaching the cross-source dedupe.
            if event and event.event_uid not in seen:
                seen.add(event.event_uid)
                events.append(event)
        return events

    def _to_event(self, node: dict) -> Event | None:
        url = canon_url(str(node.get("url") or node.get("@id") or ""))
        title = flatten_text(node.get("name"))
        if not url or not title:
            return None
        return Event(
            event_uid=f"{self.kind}:{url}",
            title=title,
            url=url,
            source=self.name,
            source_kind=self.kind,
            # Third-party markup, so normalised rather than trusted: a CMS
            # template left holding a placeholder would otherwise be stored as
            # a start no SQL comparison can read (see models.iso_or_empty).
            start=iso_or_empty(node.get("startDate")),
            end=iso_or_empty(node.get("endDate")),
            organizer=flatten_text(node.get("organizer")),
            location=flatten_text(node.get("location")),
            attendance_mode=self._attendance_mode(str(node.get("eventAttendanceMode") or "")),
            description=str(node.get("description") or "").strip(),
            # None of the four verified sites emit schema.org's datePublished
            # (see Event.published_at for the consequence of leaving it empty).
            published_at="",
        )

    @staticmethod
    def _attendance_mode(raw: str) -> AttendanceMode:
        """Map a schema.org eventAttendanceMode URI onto the shared enum.

        Kept out of AttendanceMode per its no-normalisation rule; matching the
        URI tail is safe here since it's a controlled vocabulary, not free text.
        """
        tail = (raw or "").rsplit("/", 1)[-1].lower()
        if tail.startswith("online"):
            return AttendanceMode.ONLINE
        if tail.startswith("mixed"):
            return AttendanceMode.MIXED
        if tail.startswith("offline"):
            return AttendanceMode.OFFLINE
        return AttendanceMode.UNKNOWN

