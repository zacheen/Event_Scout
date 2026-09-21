"""Structural interfaces (DIP). Implementations satisfy these by shape, no inheritance.

These are only worth having if the implementations and the callers both refer to
them. An earlier version of this file drifted out of sync with store.py and
notifier.py precisely because nothing imported it: `Notifier.send_digest` had no
implementation at all, and `EventStore.upsert` was declared to return None while
the pipeline branched on its bool. Both would have failed silently the first time
a second implementation appeared. Keep the annotations in the pipeline pointing
here so that cannot happen again.
"""
from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Protocol, runtime_checkable

from .models import Coverage, Event, Score, State, Urgency

# One digest section: a heading plus its ranked events. Urgency travels with each
# item because the renderer tags every line with it, so it cannot be recovered
# from the section heading alone.
Section = tuple[str, list[tuple[Event, Score, Urgency]]]


class EventSource(Protocol):
    name: str
    # Must match a config.source_precedence entry, or a duplicate arriving from
    # two adapters cannot be resolved and the later one silently wins.
    kind: ClassVar[str]

    def fetch(self) -> list[Event]:
        """Return every currently listed event. Dedupe is the pipeline's job.

        Must never let a broken channel look like a quiet week, because an
        empty list would let the pipeline record "nothing new" for a dead
        source. A source that makes ONE call raises when it fails. A source
        that reads many items (a mailbox, an archive) may log and skip an
        individual unreadable item, but must raise once every item failed.
        Returning a silently truncated list is what this forbids.
        """
        ...


@runtime_checkable
class BoundedSource(EventSource, Protocol):
    """A source whose own cap can cut a fetch short.

    Separate from EventSource rather than folded into it, because JsonLdSource
    reads one page and can never truncate. Giving it a coverage() to implement
    would be a method with nothing to report, so the pipeline asks only the
    sources that can actually answer.
    """

    def coverage(self) -> Coverage | None:
        """What the LAST fetch could not reach, or None when it read everything.

        Read after fetch, never before: every implementation raises rather
        than answering None on an early call, since None would be
        indistinguishable from "read everything" instead of "not yet
        determined". Reporting this is not optional under EventSource.fetch's
        contract, which forbids a silently truncated list.
        """
        ...


class EventFilter(Protocol):
    def keep(self, event: Event) -> bool: ...


class Extractor(Protocol):
    """Fills fields a source could not supply, e.g. a start date buried in prose.

    Only reached for sources without typed fields. Must fail open, returning the
    event unchanged on error, and must return the SAME object when it filled
    nothing so the pipeline can skip the redundant re-filter on identity.
    """

    def needs_extraction(self, event: Event) -> bool:
        """True when a source gave too little for the filters to judge the event.

        The pipeline asks BEFORE spending a model call, so the predicate lives
        with the extractor that defines it. Keeping a second copy in the
        pipeline meant the two could disagree about who gets enriched with
        nothing to catch it.
        """
        ...

    def extract(self, event: Event) -> Event: ...


class JsonInvoker(Protocol):
    """Ask a model for JSON matching `schema`. Transport only.

    Separate from EventScorer because "call a model" and "judge an event" are
    different jobs that happened to share a class. The extractor needs only
    this, and taking the whole scorer forced a throwaway instance to be built
    just to reach one bound method.

    The schema is a PARAMETER. Baking it into an implementation is what made
    the extractor ask for dates and silently receive scores.
    """

    def json_call(self, system_prompt: str, user_prompt: str,
                  schema: dict, schema_name: str) -> str: ...


class EventScorer(Protocol):
    method_label: ClassVar[str]
    # Whether wrapping this scorer in a cache pays for itself. False for a
    # cheap deterministic tier; see KeywordScorer.cacheable for why.
    cacheable: ClassVar[bool]

    def score(self, event: Event) -> Score:
        """Return a real judgement, or raise -- never a placeholder Score.

        CachingScorer caches whatever comes back forever, keyed on the uid;
        see _parse_score's docstring for the incident that makes raising
        here a MUST, not a suggestion.
        """
        ...


class ScoreCache(Protocol):
    """Deliberately separate from EventStore (ISP).

    CachingScorer needs only this slice. Requiring a whole EventStore would make
    an in-memory test double impossible to supply without also implementing the
    ledger, and the dependency would be invisible to a type checker.
    """

    def get_cached_score(self, event_uid: str, model_tag: str) -> Score | None: ...

    def put_cached_score(self, event_uid: str, model_tag: str, score: Score) -> None: ...


class UrgencyEngine(Protocol):
    def classify(self, event: Event, score: Score,
                 now: datetime | None = None) -> Urgency:
        """`now` is injected rather than read from the clock so a run can be
        replayed at a fixed time. It was missing here while both the
        implementation and the caller passed it, which is the same drift that
        this file's header warns about."""
        ...


class EventStore(ScoreCache, Protocol):
    """Includes ScoreCache so a store can be passed where either is expected.

    CachingScorer still depends on the narrow ScoreCache alone, which is the
    point of the split; this inheritance only makes the containment explicit to
    a type checker instead of relying on the concrete class happening to have
    both sets of methods.
    """

    def reported_urls(self) -> set[str]:
        """Canonical URLs (urls.canon_url) already reported to the user.

        Keyed on the URL, not the event_uid, because the uid embeds source_kind
        and the same event from a second source would otherwise read as new.
        Must come from a value that is only ever SET: a stored score moves when
        the event is re-scored, and a ledger that forgets an event was reported
        sends it again.
        """
        ...

    def reported_identities(self) -> list[tuple[str, str]]:
        """(title, start) for every reported row that has a start.

        The companion to reported_urls, for the same question asked the other
        way. A URL answers "this exact listing"; two sources publish one event
        under two URLs, and the second one is not news. Returns raw values so
        the caller applies its own identity rule to both sides.
        """
        ...

    def mark_cleared_floor(self, event_uids: list[str]) -> None:
        """Record that these events were judged worth sending. Idempotent.

        Separate from mark_alerted, which the seeding filter can hold an event
        back from even on a real send. Both are skipped on a dry run, because a
        preview has not put anything in front of anyone.
        """
        ...

    def upsert(self, event: Event, score: Score | None = None,
               urgency: Urgency | None = None, keyword_hits: str = "") -> None:
        """Insert or refresh one event.

        Deliberately returns nothing. It used to hand back "was this uid new",
        which reads like the answer to "should I report this" and was twice
        used as one: a below-floor sighting inserts too, so True reported an
        event never worth sending, and False silently dropped the run where it
        finally cleared the floor. reported_urls answers that question.
        """
        ...

    def set_state(self, event_uid: str, state: State) -> None:
        """Move an event into SAVED, REGISTERED or DISMISSED.

        Nothing in the pipeline calls this yet, so those three states are
        currently unreachable outside check.py. Kept because expire_past and
        due_for_resweep already treat them as first-class terminal states and
        are tested on them; what is missing is a way for the reader to say so,
        not the ledger's ability to record it.
        """
        ...

    def mark_by_url(self, url: str, state: State) -> list[tuple[str, str]]:
        """Set `state` on every listing of one event, returning what changed.

        Keyed on the URL rather than a uid because the reader has a link, not
        an id, and one event can sit in the ledger once per source.
        """
        ...

    def mark_sold_out(self, event_uids: list[str]) -> int: ...

    def mark_alerted(self, event_uids: list[str]) -> None: ...

    def expire_past(self, now_iso: str, grace_hours: int) -> int:
        """Mark already-started events expired and return how many rows changed.

        `grace_hours` comes from the pipeline's own start grace, so an
        implementation must not decide for itself when an event is over; the
        digest and the ledger disagreeing about that is a defect, not a policy
        an implementation gets to set.

        The pipeline calls this on the store it was handed, so leaving it out of
        the protocol meant a second implementation could satisfy every declared
        member and still crash at run().
        """
        ...

    def counts_by_state(self) -> dict[str, int]:
        """Ledger population per state, for the end-of-run summary line."""
        ...

    def due_for_resweep(self, within_hours: int, now_iso: str) -> list[Event]:
        """Events CLOSING inside the window that still need a nudge.

        The window is measured against `rsvp_deadline or start`, the same
        expression DeadlineUrgency._hours_until uses, because the two have to
        agree on what closing soon means. They did not: urgency read the
        deadline while this query read the start alone, so an event ten days
        out whose registration closes in twelve hours classified P0 and was
        never swept. The deadline is the door this reminder exists to catch
        before it shuts.

        Three further conditions, each answering a different question, and dropping any
        one of them was measured putting the wrong events in front of the
        reader:

        state not in State.silences_sweeper -- they already decided. Only
            set_state reaches those states, so this condition does nothing
            until the reader has a way to say so; local_run.py --mark is it.
        swept_at empty -- they have not had this nudge yet. Without it an
            hourly schedule sends one reminder per hour of the window.
        alerted_at set -- they were TOLD in the first place. state 'new' does
            not mean that: a below-floor sighting is recorded for audit and
            never mailed, and a listing absorbed by a merge is recorded under
            its own uid while only the winner is mailed. Both would arrive
            under a heading that says the reader had not acted, and the second
            one puts the same real event in the mail twice.

        Note this deliberately does NOT ask how long ago the alert was: being
        told about an event three weeks ago is exactly the case this catches.
        """
        ...

    def mark_swept(self, event_uids: list[str]) -> None:
        """Record that a closing-soon reminder went out. Idempotent."""
        ...

    def save(self) -> None: ...


class Notifier(Protocol):
    def send(self, sections: list[Section], subject: str, footer: str = "") -> int:
        """Send one digest and return how many events it carried.

        Zero sections means zero events, and that must send nothing at all
        rather than an empty message.
        """
        ...
