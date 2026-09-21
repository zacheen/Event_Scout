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

A start date is read from the prose beside each link when it is stated there,
and left empty otherwise. This is the only route to a date for an event whose
own page the scraper cannot open: measured 2026-09-19, every tesla.com/event
page still answers 403, and the four of five that have since closed no longer
print their date even to a real browser, while the issue still carries all five.
"""
from __future__ import annotations


import logging
import re
from datetime import datetime, timedelta, timezone
from typing import ClassVar, Iterable

from ..http import HttpClient
from ..models import Coverage, Event, iso_or_empty, parse_iso
from ..urls import canon_url
from ._links import harvest, tails

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
# A date stated beside a link, as "(Monday, August 31 at 5:30pm EST)". The
# weekday is REQUIRED and is what makes the missing year safe to infer, since a
# given month and day falls on the stated weekday in only one of the candidate
# years. Without it this would be guessing, and PageFactExtractor._page_text
# records what guessing a date from thin input produced last time.
_PROSE_DATE = re.compile(
    r"(?P<weekday>Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*\s*,\s*"
    r"(?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+"
    r"(?P<day>\d{1,2})"
    r"(?:\s*(?:at|@)?\s*(?P<hour>\d{1,2})(?::(?P<minute>\d\d))?\s*(?P<meridiem>am|pm))?"
    r"(?:\s*(?P<zone>E[SD]T|C[SD]T|M[SD]T|P[SD]T|UTC|GMT))?",
    re.I)
_MONTHS = {m: i for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"), start=1)}
# Standard offsets only. A name like "EST" is used loosely in this prose for
# whatever New York is observing that week, so the DST variant is accepted and
# mapped to the same city, and the error is then at most one hour rather than
# the five that dropping the zone entirely would cost.
_ZONES = {"est": -5, "edt": -4, "cst": -6, "cdt": -5,
          "mst": -7, "mdt": -6, "pst": -8, "pdt": -7, "utc": 0, "gmt": 0}
# How far ahead of its issue an event may be dated.
#
# MUST stay below 365, and that is the whole reason the year inference is safe
# rather than merely usually right. The two candidate years put the same month
# and day about 365 days apart, so a window shorter than that admits exactly one
# of them and the weekday check never has to break a tie. At 400 it did: prose
# reading "Tuesday, August 31" beside an issue published Monday 2026-08-31 was
# resolved to 2027-08-31, which really is a Tuesday and sat 365 days out, so a
# mistyped weekday became a confident date a year late instead of nothing.
#
# 120 clears the longest real lead by a wide margin. Measured on issue 004,
# published 2026-08-31, the furthest event it announced was 53 days out.
_MAX_LEAD_DAYS = 120


def start_from_prose(text: str, published: str) -> str:
    """An ISO start read from `text`, or "" when it does not state one.

    `published` supplies the year, which this prose never prints. The stated
    weekday then has to agree with the resulting date or "" is returned, so an
    issue whose year is unknown, or prose that names an impossible date such as
    "Monday, February 30", yields nothing rather than a plausible wrong answer.

    A time with no zone is emitted WITHOUT an offset. Not because naive is
    right, but because this is the wrong layer to fix it: the pipeline's
    _stamp_naive attaches the reader's offset once, after every adapter and the
    extractor have run, so a source stating "no zone" says exactly that and one
    place decides what it means.

    An earlier version of this note claimed the UTC reading was only an
    hours-scale error and so no row changed the day it expired on. That is
    false for any stamp before 07:00 local: "2026-10-08T00:00:00" read as UTC
    is 2026-10-07 17:00 Pacific, and a measured San Francisco job fair expired
    the evening before it happened.
    """
    issue = parse_iso(published)
    match = _PROSE_DATE.search(text or "")
    if not (issue and match):
        return ""
    month = _MONTHS[match.group("month")[:3].lower()]
    day = int(match.group("day"))
    hour, minute = 0, 0
    if match.group("hour"):
        hour = int(match.group("hour")) % 12
        minute = int(match.group("minute") or 0)
        if match.group("meridiem").lower() == "pm":
            hour += 12
    for year in (issue.year, issue.year + 1):
        try:
            when = datetime(year, month, day, hour, minute)
        except ValueError:
            continue    # "February 30", in either candidate year
        if when.strftime("%a").lower() != match.group("weekday")[:3].lower():
            continue
        if not 0 <= (when.date() - issue.date()).days <= _MAX_LEAD_DAYS:
            continue
        zone = (match.group("zone") or "").lower()
        if zone:
            when = when.replace(tzinfo=timezone(timedelta(hours=_ZONES[zone])))
        return iso_or_empty(when.isoformat())
    return ""


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
        # Keyed by RAW href, as `tails` documents, so look up before canon_url.
        after = tails(body)
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
            # From the prose beside THIS link, never the issue at large (see the
            # description comment above). `tails`' short window plus the weekday
            # check means a slice that ran into the next item fails closed
            # instead of dating this event from its neighbour.
            start=start_from_prose(after.get(href, ""), published),
        ) for href, title in harvest(body)]
