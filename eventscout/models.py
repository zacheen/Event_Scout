"""Immutable value objects passed between pipeline stages.

Side-effect free. Any normalisation that could fail belongs in the adapter that
builds the object, not here, so that a malformed source cannot silently produce a
half-valid Event that later stages treat as trustworthy.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, fields, replace
from datetime import datetime, timezone
from enum import StrEnum
from zoneinfo import ZoneInfo

from .urls import canon_url

_DATE_ONLY = re.compile(r"\d{4}-\d{2}-\d{2}")


def is_date_only(value: str) -> bool:
    """Did the source give a DAY rather than a moment?

    "2026-10-08" yes, "2026-10-08T00:00:00" no: a source that spelled out
    midnight meant midnight, and only the bare form says the time is unknown.
    Shared rather than re-tested at each caller, because iso_or_empty decides
    what survives normalisation and the pipeline decides what it turns into,
    and a second copy of the pattern is how those two would drift.

    Shape AND validity, because neither test alone is enough. The pattern is
    what separates a date from a spelled-out midnight, which fromisoformat
    accepts equally; fromisoformat is what rejects "2026-13-45", which the
    pattern matches and SQLite then reads as NULL, dropping the row out of
    every lifecycle query at once (see iso_or_empty).
    """
    text = (value or "").strip()
    if not _DATE_ONLY.fullmatch(text):
        return False
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


class AttendanceMode(StrEnum):
    """How an event is attended, reduced to the three cases we act on.

    Holds no source-vocabulary knowledge: parsing schema.org URIs, WordPress
    taxonomies, or free text into these members is each adapter's job (this
    module's no-normalisation rule), so adding a source never means growing
    this enum a `from_<source>` classmethod.
    """

    OFFLINE = "offline"
    ONLINE = "online"
    MIXED = "mixed"
    UNKNOWN = ""


class Urgency(StrEnum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"


class State(StrEnum):
    """Lifecycle of one event in the ledger. `saved`, `registered`, `dismissed`
    and `expired` are terminal for alerting purposes; the sweeper only
    re-alerts the others."""

    NEW = "new"
    SEEN = "seen"
    SAVED = "saved"
    REGISTERED = "registered"
    DISMISSED = "dismissed"
    EXPIRED = "expired"
    # Recorded INSTEAD of a mail record, so the ledger distinguishes "full, so
    # deliberately not sent" from both "sent" and "not sent yet". Kept out of
    # alerted_at, which stays a timestamp: three queries test `alerted_at != ''`
    # to mean "already told", and a marker there would make a full event count
    # as reported and would also be copied into cleared_floor_at.
    SOLD_OUT = "sold_out"

    @property
    def choosable(self) -> bool:
        """States the READER can put an event into.

        silences_sweeper minus EXPIRED, which only time can cause. Named here
        so the CLI's allow-list and its rejection test cannot drift into
        disagreeing about the same rule.
        """
        return self.silences_sweeper and self not in (State.EXPIRED, State.SOLD_OUT)

    @property
    def silences_sweeper(self) -> bool:
        return self in (State.SAVED, State.REGISTERED, State.DISMISSED,
                        State.EXPIRED, State.SOLD_OUT)


# Fields with no sensible empty value. Everything else may legitimately be blank,
# e.g. an event with no stated end time.
_IDENTITY_FIELDS = frozenset({"event_uid", "title", "url", "source", "source_kind"})


@dataclass(frozen=True)
class Event:
    # "{source}:{canonical-url}" — the dedupe key. Built by the adapter so a
    # source with stable native ids can use them instead of the URL.
    event_uid: str
    title: str
    url: str
    source: str
    # Adapter family, e.g. "jsonld". Drives config.source_precedence when the
    # same event arrives twice, so it must match the names used there.
    source_kind: str
    # ISO 8601. Empty means the source did not state one, which is NOT the same
    # as "starts now" — every consumer must branch on the empty case rather than
    # parsing it, or an undated event sorts to the epoch and leads the digest.
    start: str = ""
    end: str = ""
    organizer: str = ""
    location: str = ""
    attendance_mode: AttendanceMode = AttendanceMode.UNKNOWN
    description: str = ""
    rsvp_deadline: str = ""
    # When the SOURCE published this listing, not when the event happens. Empty
    # for every schema.org source, which carries startDate but no datePublished;
    # first_run.require_known_publish_time turns that emptiness into exclusion.
    published_at: str = ""
    # Set by the extractor when the event's own page could not be read, so the
    # geo filter can tell "states no location" apart from "is elsewhere". Not
    # persisted: a later run re-fetches and decides again.
    page_unreadable: bool = False
    # The source gave a date and no time, so start holds local midnight and
    # the event is not over until the day is.
    #
    # PERSISTED, unlike page_unreadable, and the asymmetry is expire_past
    # rather than importance. That runs as one SQL scan over EVERY stored row,
    # not just the ones this run re-fetched, so a row whose source dropped it
    # this cycle still has to answer "was this all day" from what the ledger
    # holds. page_unreadable has no cross-run batch reader, so re-deciding it
    # from scratch each run is free and storing it would only let a stale
    # answer outlive the attempt that produced it.
    #
    # Set by the pipeline's _stamp_naive, the one place that sees the bare
    # date before it becomes an instant.
    all_day: bool = False

    def __post_init__(self):
        # JSON nulls (e.g. "location": null) survive .get(key, "") since the key
        # exists; coerce None here so no downstream .lower()/regex ever sees it.
        #
        # Reads f.default, never f.type: this module's PEP 563 (`from
        # __future__ import annotations`) makes f.type the *string*
        # "AttendanceMode", so `f.type is AttendanceMode` is always False. An
        # earlier version compared against f.type and silently coerced None
        # to "" instead of the enum, breaking isinstance/.name downstream.
        for f in fields(self):
            if getattr(self, f.name) is None:
                if f.name in _IDENTITY_FIELDS:
                    # Coercing these to "" would be worse than crashing. Two
                    # adapters that both forget to build a uid would produce
                    # event_uid="" and then dedupe INTO EACH OTHER, silently
                    # collapsing unrelated events into one digest entry.
                    raise ValueError(f"Event.{f.name} must not be None")
                fallback = (f.default
                            if isinstance(f.default, (AttendanceMode, bool))
                            else "")
                object.__setattr__(self, f.name, fallback)
        # A source that forgets iso_or_empty stores a start SQL cannot compare,
        # silently dropping the row out of both expire_past and due_for_resweep
        # (see iso_or_empty). Enforced here because there is no other place
        # every adapter passes through; rsvp_deadline is included because
        # DeadlineUrgency reads it in preference to start, so an unparseable
        # one decides the urgency tier. published_at is here for consistency
        # rather than a known failure: _seed_eligible already treats an
        # unreadable one as absent, but leaving one field out of a contract the
        # other three keep is how the next reader learns the wrong rule.
        for field in ("start", "end", "rsvp_deadline", "published_at"):
            stamp = getattr(self, field)
            if stamp and stamp != iso_or_empty(stamp):
                raise ValueError(
                    f"Event.{field} must be normalised by iso_or_empty: {stamp!r}")
        # Same reasoning, for url: cross-source dedupe and the "already
        # reported" test both compare it against canonical URLs from the
        # ledger, so an adapter that forgets canon_url quietly reports the same
        # event twice instead of failing outright. canon_url is idempotent, so
        # a correctly built event never trips this.
        if self.url != canon_url(self.url):
            raise ValueError(
                f"Event.url must be canonical: {self.url!r} should be "
                f"{canon_url(self.url)!r}")

    @property
    def is_virtual(self) -> bool:
        return self.attendance_mode in (AttendanceMode.ONLINE, AttendanceMode.MIXED)

    @property
    def has_known_start(self) -> bool:
        return bool(self.start)


    def merged_with(self, other: "Event") -> "Event":
        """Fill this event's empty fields from `other`, keeping identity fields.

        The CALLER decides which side wins by call order (per
        config.source_precedence); this method never compares sources. For 3+
        sources, fold left in descending precedence,
        `best.merged_with(second).merged_with(third)`: each step only fills
        gaps, so a higher-precedence value is never overwritten by a lower one.

        ONE exception, and it is deliberate. start, end and all_day describe a
        single occurrence, so taking a start means taking the end and the flag
        that came with it, EVEN IF a higher-precedence source already supplied
        an end. Reproduced over a three-source fold: a middle source knowing
        only an end ("the event finishes at 3pm", no date) loses that end to
        the lowest-precedence source's complete occurrence. Kept that way
        because an end belonging to a start we did NOT take is not a better
        fact about this event, it is a fact about a different one, and pairing
        it with the start we did take is how an all-day event on the 8th ends
        up claiming to run until the 30th.
        """
        # page_unreadable is excluded because it is not a fact ABOUT the event,
        # it is the result of one attempt to read one URL, and "False" means
        # "not found unreadable yet" rather than "known readable". Today merge
        # runs before extraction so every value is False anyway; excluding it
        # makes that a structural guarantee instead of an ordering accident.
        #
        # Deliberately one field SHORTER than _IDENTITY_FIELDS: `title` must not
        # be None but may still be filled in from a lower-precedence source when
        # blank. Do not DRY these two lists together; they answer different
        # questions, and merging them would freeze titles against enrichment.
        patch = {
            f.name: getattr(other, f.name)
            for f in fields(self)
            if f.name not in ("event_uid", "url", "source", "source_kind",
                                    "page_unreadable", "all_day")
            and not getattr(self, f.name)
            and getattr(other, f.name)
        }
        # start, end and all_day are one occurrence, not three facts, so they
        # move together or not at all. Merged independently each does its own
        # damage: a winner with a real time takes a loser's all_day and claims
        # to run all day, a winner filling an empty start from an all-day loser
        # drops the flag and reads as midnight, and a timed winner with no end
        # inherits an all-day loser's 23:59:59, which _stamp_naive SYNTHESISED
        # for a day it no longer describes.
        if "start" in patch:
            patch["all_day"], patch["end"] = other.all_day, other.end
        elif other.all_day:
            patch.pop("end", None)
        return replace(self, **patch) if patch else self


def display_zone(name: str) -> ZoneInfo:
    """The project's one local zone, resolved once and eagerly.

    Two callers with opposite jobs. The notifier RENDERS absolute times in it,
    because the raw string was being sliced for display, which is only right
    when the source happens to store the reader's own offset. Measured on the
    live ledger: Luma and Gmail events carry -07:00 and printed correctly,
    while every neu-alumni-events row carries +00:00 and printed 7 hours late,
    one of them on the wrong DAY (2026-09-24T02:00:00+00:00 shown as
    "2026-09-24 02:00" for an event that starts 2026-09-23 19:00 Pacific).
    The pipeline's _stamp_naive READS a source's unzoned time as it, which is
    the same zone for the opposite reason, an input assumption rather than an
    output format.

    Here rather than in notifier.py, which is where it started: pipeline.py
    depends only on protocols, config and this module, so reaching into the
    email adapter for a pure ZoneInfo lookup would have been the one import
    binding the core pass to one output channel.

    An unknown name raises here rather than at send time, since the alternative
    is a digest full of times in a zone nobody chose.
    """
    return ZoneInfo(name)


def parse_iso(value: str) -> datetime | None:
    """An Event timestamp as an instant, or None when it is not one.

    Assumes UTC when the value states no offset. Shared because five call
    sites had grown their own copy of the same three lines, and the lesson of
    the bug this file's iso_or_empty exists for is that two parsers of one
    format drift.
    """
    try:
        when = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return when if when.tzinfo else when.replace(tzinfo=timezone.utc)


def iso_or_empty(value: object) -> str:
    """`value` as a canonical ISO 8601 timestamp, or "" when it is not one.

    NORMALISES rather than merely validating, because two parsers have to agree
    about every stored start. Python accepts forms SQLite does not, "2026-W37-3"
    and "20260909T180000" among them, and the ledger compares instants in SQL:
    a value SQLite cannot read makes datetime(start) NULL, and NULL fails every
    comparison, so the row drops out of BOTH expire_past and due_for_resweep at
    once. It never expires and never gets a last call, with no error anywhere.
    Re-emitting what Python parsed removes that whole class of disagreement.
    """
    text = str(value or "").strip()
    if not text:
        return ""
    # A pure calendar date stays one. Expanding "2026-10-08" to midnight is
    # what threw away the only signal that an event is ALL DAY: measured, all
    # 18 nodes on the eventbrite feed state startDate and endDate as bare
    # dates, so the whole tier arrived claiming to start at 00:00. SQLite reads
    # a bare date natively, so this costs nothing the docstring above promises,
    # and the pipeline's _stamp_naive is what turns it into an instant.
    if is_date_only(text):
        return text
    # Re-emitted WITHOUT forcing a timezone: a source that stated none is not
    # improved by pretending it meant UTC here. parse_iso is what fills one in
    # for callers needing an instant, and it assumes UTC. An earlier version of
    # this comment claimed the geo and urgency layers read a naive stamp as
    # LOCAL, which was never true: geo.py parses no dates at all, and every
    # urgency path goes through parse_iso. The consequence to remember is that a
    # naive value and a parse_iso instant agree only when the source really did
    # mean UTC, which is why the pipeline's _stamp_naive attaches an offset
    # before anything is stored: SQLite makes the same UTC assumption inside
    # expire_past, and no Python-side fallback can reach that comparison.
    try:
        return datetime.fromisoformat(
            text.replace("Z", "+00:00")).isoformat(timespec="seconds")
    except ValueError:
        return ""


@dataclass(frozen=True)
class Coverage:
    """What a source's OWN cap stopped it from reading.

    EventSource.fetch forbids returning a silently truncated list, and a count
    on its own cannot show that it happened. A WordPress endpoint answering
    exactly per_page reads as 100 events rather than as 100 of 219, and a
    mailbox capped at its newest 40 reads as a healthy 40.

    `available` is None when the source cannot know the total. A paged endpoint
    only learns it ran out by receiving a short page, so a cap that fires leaves
    the remainder genuinely unknown rather than merely uncounted.
    """

    fetched: int
    unit: str
    available: int | None = None

    def __str__(self) -> str:
        if self.available is None:
            return f"CAPPED at {self.fetched} {self.unit}, more may exist"
        return (f"CAPPED, read {self.fetched} of {self.available} {self.unit}, "
                f"{self.available - self.fetched} never seen")


@dataclass(frozen=True)
class Score:
    """Three axes, deliberately not collapsed into one number.

    `fit` alone under-ranks a resume drop, which reads like an unrelated
    workshop by topic alone yet is a direct pipeline to an employer;
    `access_value` exists to capture that.
    """

    fit: int
    access_value: int
    cost: int
    reason: str = ""
    method: str = ""

    @property
    def rank(self) -> int:
        """Sort key. `cost` is deliberately excluded, not forgotten.

        Multiplying cost in would systematically demote San Francisco events for
        a South Bay commute, and SF is where the density is. Cost survives on the
        Score object for UrgencyEngine and the digest to show, so the reader
        weighs the travel themselves rather than having it silently pre-applied.
        """
        return self.fit * self.access_value
