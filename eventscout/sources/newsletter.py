"""Newsletter archive source.

Reads a beehiiv publication's PUBLIC archive, which needs no subscription, and
harvests outbound event links from each issue. It reaches a class of event no
other tier can, namely an unlisted campaign landing page distributed only by
email and invisible to any crawl of the employer's own site.

Two things are deliberate.

Issue slugs and dates are recovered from metadata beehiiv embeds in the
archive page, but not as parsed JSON: slug and date sit in separate,
non-adjacent fields, so they are matched by pattern and paired by proximity.
That makes this the most fragile source here, so _slug_dates raises when a
large page yields nothing rather than reporting an empty publication.

Emitted events have no start date. A newsletter states one in prose, so these
are candidates for the LLM extractor, unlike the typed JSON-LD sources.
"""
from __future__ import annotations


import logging
import re
from typing import ClassVar, Iterable

from ..http import HttpClient
from ..models import Coverage, Event, iso_or_empty
from ..urls import canon_url
from ._links import harvest

log = logging.getLogger("eventscout.newsletter")

# Post metadata embedded in the archive page. The date arrives under any of
# four names and on either side of the slug: beehiiv emits
# "override_scheduled_at" BEFORE the slug but "scheduled_at" AFTER it.
# Matching only one side yielded "" for every issue, which would have tripped
# first_run.require_known_publish_time to drop the entire tier.
_SLUG_PATTERN = re.compile(r'"slug":"([a-z0-9-]{4,})"')
_DATE_FIELD = re.compile(
    r'"(?:scheduled_at|override_scheduled_at|publish_date|displayed_date)":"(?P<iso>\d{4}-\d\d-\d\dT[^"]*)"'
)
# How far either side of a slug to look for that issue's own date. Wide enough
# to clear the intervening fields, tight enough not to borrow the next post's.
_DATE_WINDOW = 600
# An archive page this large that yields no slug means the pattern stopped
# matching, not that the publication went quiet. Guarding on it keeps a beehiiv
# redesign from degrading into a permanently silent source.
# Characters, not bytes: HttpClient.get_text already decoded the response.
_MIN_ARCHIVE_CHARS = 20_000


class NewsletterArchiveSource:
    kind: ClassVar[str] = "newsletter"

    def __init__(self, name: str, host: str, http: HttpClient, archive_path: str = "/archive",
                 max_issues: int = 10, only_slugs: Iterable[str] | None = None):
        self.name = name
        self._host = host.rstrip("/")
        self._http = http
        self._archive_path = archive_path
        self._max_issues = max_issues
        # `is None`, not falsy: an explicitly empty list means "no issues this
        # run", distinct from None's "use the newest instead". Pinning specific
        # slugs this way is what makes the archive backtest reproducible.
        self._only_slugs = None if only_slugs is None else list(only_slugs)
        self._coverage: Coverage | None = None
        self._fetched = False

    def fetch(self) -> list[Event]:
        # The publish date lives in the ARCHIVE INDEX, not the issue page, so it
        # has to be carried down here rather than re-derived per issue (same
        # empty-date failure mode the constants above describe).
        dates = self._slug_dates()
        issues = self._only_slugs if self._only_slugs is not None else self._order(dates)
        events: dict[str, Event] = {}
        picked = issues[: self._max_issues]
        self._coverage = (None if len(picked) == len(issues) else
                          Coverage(len(picked), "issues", len(issues)))
        failed = 0
        for slug in picked:
            try:
                body = self._http.get_text(self.issue_url(slug))
            except Exception as exc:
                # Same split as GmailLabelSource: one bad issue is logged and
                # skipped, but every issue failing signals a broken channel
                # (raised below), not a quiet week, since the archive index
                # already parsed and only the per-issue page could have moved.
                # Only the FETCH is guarded; a bug while building the Event
                # must fail loud, not get counted as network noise.
                failed += 1
                log.warning("%s: could not read issue %s (%s: %s)",
                            self.name, slug, type(exc).__name__, exc)
                continue
            for event in self._events_from_body(body, self.issue_url(slug),
                                                dates.get(slug, "")):
                # One issue can link the same event twice, e.g. a banner image
                # and a text call to action.
                events.setdefault(event.event_uid, event)
        if picked and failed == len(picked):
            raise RuntimeError(
                f"{self.name}: every issue failed to load ({failed}/{len(picked)})")
        self._fetched = True
        return list(events.values())

    def coverage(self) -> Coverage | None:
        if not self._fetched:
            # See BoundedSource.coverage() for why this must raise rather
            # than return None.
            raise RuntimeError(f"{self.name}: coverage() called before fetch()")
        return self._coverage

    def issue_url(self, slug: str) -> str:
        return f"https://{self._host}/p/{slug}"

    def _slug_dates(self) -> dict[str, str]:
        """Map issue slug -> ISO publish date, read from the archive index."""
        html_text = self._http.get_text(f"https://{self._host}{self._archive_path}")
        dates: dict[str, str] = {}
        for match in _SLUG_PATTERN.finditer(html_text):
            slug = match.group(1)
            if slug in dates:
                continue
            window = html_text[max(0, match.start() - _DATE_WINDOW): match.end() + _DATE_WINDOW]
            found = _DATE_FIELD.findall(window)
            # Latest wins: an issue rescheduled after drafting carries both the
            # original and the override, and the later value is when it shipped.
            dates[slug] = max(found) if found else ""
        if not dates and len(html_text) >= _MIN_ARCHIVE_CHARS:
            raise ValueError(
                f"{self.name}: archive returned {len(html_text)} characters but no slugs; "
                "the beehiiv markup likely changed"
            )
        return dates

    @staticmethod
    def _order(dates: dict[str, str]) -> list[str]:
        """Issue slugs, newest first. Undated issues sort last, not first."""
        return [slug for _, slug in sorted(((v, k) for k, v in dates.items()), reverse=True)]

    def _events_from_body(self, body: str, issue: str,
                          published: str = "") -> list[Event]:
        return [Event(
            event_uid=f"{self.kind}:{canon_url(href)}",
            title=title,
            url=canon_url(href),
            source=self.name,
            source_kind=self.kind,
            # Deliberately NOT the issue text, unlike GmailLabelSource: that
            # source's body feeds the keyword gate, but newsletters are exempt.
            # This field is read only by the scorer and by
            # PageFactExtractor._page_text as a fallback when an event's own
            # page fails to fetch. A newsletter's opening paragraphs describe
            # its LEAD story, not the linked event, so on ANY fetch failure the
            # extractor would stamp the lead story's date/location onto the
            # wrong event, corrupting the ledger, date window, urgency tier and
            # .ics all at once.
            description=f"Linked from {issue}",
            # The issue's own publish date. Unlike the JSON-LD sources this is
            # real, so newsletter items DO qualify on the first run.
            # beehiiv writes these with a Z suffix, which Event rejects as
            # unnormalised; five rows in a real ledger arrived that way.
            published_at=iso_or_empty(published),
        ) for href, title in harvest(body)]
