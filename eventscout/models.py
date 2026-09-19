"""Immutable value objects passed between pipeline stages.

Side-effect free. Any normalisation that could fail belongs in the adapter that
builds the object, not here, so that a malformed source cannot silently produce a
half-valid Event that later stages treat as trustworthy.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, replace
from datetime import datetime, timezone
from enum import StrEnum

from .urls import canon_url


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
                                    "page_unreadable")
            and not getattr(self, f.name)
            and getattr(other, f.name)
        }
        return replace(self, **patch) if patch else self


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
    # Re-emitted WITHOUT forcing a timezone: a source that stated none is not
    # improved by pretending it meant UTC here. parse_iso is what fills one in
    # for callers needing an instant, and it assumes UTC. An earlier version of
    # this comment claimed the geo and urgency layers read a naive stamp as
    # LOCAL, which was never true: geo.py parses no dates at all, and every
    # urgency path goes through parse_iso. The consequence to remember is that a
    # naive value and a parse_iso instant agree only when the source really did
    # mean UTC.
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
