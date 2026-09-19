"""WordPress REST event source.

Many university and organisation sites run WordPress and expose /wp-json. Probe
/wp-json/wp/v2/types FIRST to learn whether a site has a real event post type or
only blog posts, because that decides whether the LLM extractor is needed at all.
alumni.northeastern.edu has a typed `event`; neurai.sites.northeastern.edu
registers `calendar_event` but returns zero items, so its events hide in posts.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import ClassVar

from ..http import HttpClient
from ..models import AttendanceMode, Coverage, Event, iso_or_empty
from ..urls import canon_url
from ._text import strip_html

# Candidate keys for the real event start, in priority order; `date` is LAST
# on purpose because WordPress sets it to the post's PUBLISH time, not the
# event's start, so a months-early announcement would otherwise sort as if
# happening now. Measured on alumni.northeastern.edu: date=2026-09-03 but the
# event itself ran 2026-10-02, a month later.
# `date_gmt` before `date` because WordPress states `date` in the SITE's own
# zone with no offset, e.g. "2026-09-03T10:00:00", and every consumer then reads
# it as UTC. alumni.northeastern.edu is Eastern, so such a start would land in
# the digest and the .ics four hours early, and across a midnight boundary on
# the wrong day. `date_gmt` carries the same instant in UTC. Measured:
# all 219 items of that endpoint use start_date_time, so this
# fallback fires on nothing there and the fix is pre-emptive.
_START_KEYS = ("start_date_time", "start_date", "event_start", "_event_start",
               "date_gmt", "date")
_END_KEYS = ("end_date_time", "end_date", "event_end", "_event_end")
# `location` is a plain string like "OAKLAND, CA". `event-location` is
# deliberately ABSENT: it's a TAXONOMY TERM ID ARRAY (e.g. [20, 62, 372]), and
# including it once put "[20, 62, 372, 358, 37]" straight into this field.
# Measured over 100 alumni events: only one has an empty `location`, and its
# `event-location` was empty too, so this fallback would rescue nothing while
# risking digit soup in the digest.
_LOCATION_KEYS = ("location", "venue", "event_location")
# event_types is an inlined taxonomy, e.g. [{"id":332,"name":"In Person"}].
_MODE_BY_NAME = {
    "in person": AttendanceMode.OFFLINE,
    "in-person": AttendanceMode.OFFLINE,
    "virtual": AttendanceMode.ONLINE,
    "online": AttendanceMode.ONLINE,
    "hybrid": AttendanceMode.MIXED,
}

_DESCRIPTION_MAX = 2000


log = logging.getLogger("eventscout.wordpress")


class WordPressSource:
    kind: ClassVar[str] = "wordpress_rest"

    def __init__(self, name: str, endpoint: str, http: HttpClient,
                 per_page: int = 100, max_pages: int = 20):
        self.name = name
        self._endpoint = endpoint
        self._http = http
        self._per_page = per_page
        # Measured: neu-alumni-events reports X-WP-Total 219 against
        # a per_page of 100, so the single-request version read 100 and dropped
        # 119 while the funnel showed a healthy-looking "100". Ordering is post
        # date descending, confirmed by request, so the dropped rows were the
        # OLDEST POSTED rather than the furthest out, and post date is when
        # someone typed the listing (see channels.yaml's TRAP note).
        self._max_pages = max_pages
        self._coverage: Coverage | None = None
        self._fetched = False

    def fetch(self) -> list[Event]:
        items: list[dict] = []
        self._coverage = None
        for page in range(1, self._max_pages + 1):
            try:
                batch = self._page(page)
            except Exception as exc:
                # Page 1 is UNGUARDED, like every other source: a broken
                # endpoint must raise rather than report an empty week. A later
                # page is different, because page 1 already proved the channel
                # works. WordPress answers HTTP 400 rest_post_invalid_page_number
                # past the end, which happens whenever the total is an exact
                # multiple of per_page, so raising there would turn a complete
                # read into a dead source. Coverage records it either way, so
                # the tolerance cannot hide a real outage.
                if page == 1:
                    raise
                # Logged because the except is far broader than the case the
                # comment justifies: a timeout, a 5xx and a JSON shape error all
                # land here too, and Coverage alone renders them identically to
                # the harmless off-by-one 400. Same idiom as the tolerated
                # per-item failures in gmail_label.py and newsletter.py.
                log.warning("%s: page %d treated as end of data (%s: %s)",
                            self.name, page, type(exc).__name__, exc)
                self._coverage = Coverage(len(items), "events")
                break
            items.extend(batch)
            # A short page is the only end-of-data signal available here.
            # X-WP-Total states it outright, but HttpClient.get_text returns a
            # body without headers, and widening that interface for one source
            # costs more than one extra request.
            if len(batch) < self._per_page:
                break
        else:
            # Every page came back full, so the remainder is unknown rather than
            # merely uncounted, which is what Coverage's available=None means.
            self._coverage = Coverage(len(items), "events")
        events = [self._to_event(item) for item in items]
        self._fetched = True
        return [e for e in events if e]

    def coverage(self) -> Coverage | None:
        if not self._fetched:
            # See BoundedSource.coverage() for why this must raise rather
            # than return None.
            raise RuntimeError(f"{self.name}: coverage() called before fetch()")
        return self._coverage

    def _page(self, page: int) -> list[dict]:
        sep = "&" if "?" in self._endpoint else "?"
        raw = self._http.get_text(
            f"{self._endpoint}{sep}per_page={self._per_page}&page={page}")
        items = json.loads(raw)
        if not isinstance(items, list):
            raise ValueError(f"{self.name}: expected a JSON array, got {type(items).__name__}")
        return items

    def _to_event(self, item: dict) -> Event | None:
        url = canon_url(str(item.get("link") or ""))
        title = strip_html(self._rendered(item.get("title")))
        if not url or not title:
            return None
        return Event(
            event_uid=f"{self.kind}:{url}",
            title=title,
            url=url,
            source=self.name,
            source_kind=self.kind,
            start=self._iso(self._first(item, _START_KEYS)),
            end=self._iso(self._first(item, _END_KEYS)),
            location=self._first(item, _LOCATION_KEYS),
            description=strip_html(self._rendered(item.get("excerpt") or item.get("content")))[:_DESCRIPTION_MAX],
            attendance_mode=self._mode(item),
            # Unlike schema.org, WordPress always reports when the listing was
            # posted, so these DO qualify under first_run.require_known_publish_time.
            published_at=iso_or_empty(item.get("date_gmt") or item.get("date")),
        )

    @staticmethod
    def _first(item: dict, keys: tuple[str, ...]) -> str:
        for key in keys:
            value = item.get(key)
            if isinstance(value, dict):
                value = WordPressSource._rendered(value)
            if isinstance(value, list):
                # An all-numeric list is a taxonomy TERM ID array, never display
                # text; skip it here so ANY key with this shape is caught, not
                # just the known `event-location` case.
                if all(str(v).strip().isdigit() for v in value if v not in ("", None)):
                    continue
                value = ", ".join(str(v) for v in value if v)
            if value:
                return strip_html(str(value))
        return ""

    @staticmethod
    def _iso(value: str) -> str:
        """Normalise a WordPress date to ISO 8601.

        Event plugins store the start as a Unix epoch WRAPPED IN A STRING, e.g.
        "1790967600", which looks like an opaque id and sorts lexically rather
        than chronologically. Everything else goes through the shared gate, so
        a field this plugin family invents cannot reach the ledger as a start
        no SQL comparison can read.
        """
        text = (value or "").strip()
        if text.isdigit() and len(text) >= 9:
            return datetime.fromtimestamp(int(text), timezone.utc).isoformat()
        return iso_or_empty(text)

    @staticmethod
    def _mode(item: dict) -> AttendanceMode:
        for term in item.get("event_types") or []:
            if isinstance(term, dict):
                mode = _MODE_BY_NAME.get(str(term.get("name") or "").strip().lower())
                if mode:
                    return mode
        return AttendanceMode.UNKNOWN

    @staticmethod
    def _rendered(value) -> str:
        """WordPress wraps user text as {"rendered": "<p>...</p>"}."""
        if isinstance(value, dict):
            return str(value.get("rendered") or "")
        return str(value or "")

