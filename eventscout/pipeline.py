"""The run itself, with a stage-by-stage funnel so a zero is always explained.

Every stage reports what it dropped. That is the point: "no matching events"
is a legitimate outcome, but it is indistinguishable from a broken source unless
the run says WHICH stage the events disappeared at.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Callable, Hashable, NamedTuple
from zoneinfo import ZoneInfo

from .config import Settings
from .models import (display_zone, is_date_only, iso_or_empty, parse_iso,
                     Event, Score, Urgency)
from .protocols import (BoundedSource, EventFilter, EventScorer, EventSource, EventStore,
                        Extractor, Notifier, Section, UrgencyEngine)
from .scoring import DeadlineUrgency, build_scorer


@dataclass(frozen=True)
class Digest:
    """Everything _deliver needs to know about one run, before anything is sent.

    Groups the values _deliver needs about THIS run, so its signature stays
    about the collaborators it talks to. `cleared` is separate from
    `to_send` because the seeding filter withholds events that were still
    judged worth reporting, and the ledger has to record that judgement.
    """

    to_send: list[tuple[Event, Score, Urgency]]
    reminders: list[tuple[Event, Score, Urgency]]
    cleared: list[str]
    seeding: bool
    # What the rank floor rejected, carried only on a report_everything first
    # run so the reader can see what the floor costs and make that cut himself.
    # Deliberately absent from `cleared` and from mark_alerted: showing an event
    # once in a dump of everything is not the same as judging it worth sending,
    # and marking it would both falsify that judgement and hand these to the
    # sweep, which gates on alerted_at and never looks at rank -- so every one
    # of them, at any rank, would come back as a last call.
    below_floor: list[tuple[Event, Score, Urgency]]
    # The run's clock, which mark_alerted stamps with so that due_for_resweep
    # compares alerted_at against the same clock on the next run.
    now: datetime


class AllSourcesFailedError(RuntimeError):
    """Every configured source raised during this run's fetch.

    Its own type rather than a bare RuntimeError, because run() also reaches
    four unrelated RuntimeErrors: EmailNotifier.send's credential check and the
    three sources' coverage()-before-fetch invariant. A caller catching the
    base class would label any of those "no usable source" and send whoever is
    on call looking for a network outage instead of a config or ordering bug.
    """


class AllScoringFailedError(RuntimeError):
    """Every event that reached the scorer raised, so the run judged nothing.

    Its own type, and NOT a subclass of AllSourcesFailedError, because the two
    name opposite causes: that one is "we learned nothing", this one is "we
    learned plenty and could not judge any of it". A single handler for both
    would point a reader at the network when the model tier is what is down.

    Raised AFTER delivery rather than in place of it. The ledger's closing-soon
    reminders are read from rows scored on earlier runs and owe nothing to this
    run's scorer, and an outage on the day an RSVP closes is exactly when
    withholding them costs the most.
    """


class ScoringOutcome(NamedTuple):
    attempted: int
    failed: int


class Funnel:
    """Records how many items survived each stage.

    Recording only. Rendering lives in `render_funnel`, so changing the report
    format and adding a stage stay independent reasons to edit, and first_zero
    (the diagnostic core) is testable without capturing stdout.
    """

    def __init__(self) -> None:
        self.stages: list[tuple[str, int, str]] = []
        self.per_source: list[tuple[str, int, str]] = []
        # Kept alongside per_source rather than recovered by matching "ERROR"
        # back out of the note. A rendered string is a display concern and
        # sniffing it later is how a wording change silently disarms a check.
        self.source_errors: list[tuple[str, str]] = []
        self.additions: list[tuple[str, int, str]] = []
        # Scored/failed as a pair of numbers rather than recovered by
        # subtracting two stage counts, for the same reason source_errors is
        # kept beside per_source: a caller that re-derives a health fact from
        # the report is a caller a re-worded stage label can silently disarm.
        #
        # Named rather than a bare tuple, because scored() takes SUCCEEDED and
        # stores FAILED. Both are ints, so positional access at the read site
        # would let anyone re-ordering the pair turn "3 failed" into "3 scored"
        # with nothing to catch it.
        self.scoring: ScoringOutcome | None = None

    def source(self, name: str, count: int, note: str = "") -> None:
        self.per_source.append((name, count, note))

    def source_failed(self, name: str, exc: Exception) -> None:
        """One source that raised. Reported and skipped, never fatal by itself.

        `all_sources_failed` is the different judgement. See EventSource.fetch,
        and the same split inside GmailLabelSource.fetch and
        NewsletterArchiveSource.fetch one level down.
        """
        detail = f"{type(exc).__name__}: {str(exc)[:44]}"
        self.source_errors.append((name, detail))
        self.per_source.append((name, 0, f"ERROR {detail}"))

    def all_sources_failed(self) -> bool:
        """Did EVERY source raise? False when there were none to try.

        A run configured with no sources is a configuration question, not an
        outage, and answering True would make an empty channels.yaml look like
        a dead network.
        """
        return bool(self.per_source) and len(self.source_errors) == len(self.per_source)

    def scored(self, attempted: int, succeeded: int) -> None:
        self.scoring = ScoringOutcome(attempted, attempted - succeeded)

    def all_scoring_failed(self) -> bool:
        """Did EVERY event that reached the scorer raise?

        False when none reached it, mirroring all_sources_failed: a run whose
        filters legitimately emptied before scoring is a quiet week, and
        answering True would make one look like a model outage.
        """
        return (self.scoring is not None and self.scoring.attempted > 0
                and self.scoring.failed == self.scoring.attempted)

    def stage(self, name: str, count: int, note: str = "") -> None:
        """One narrowing of the SAME set. Only for counts the line above it
        can shrink into; see `added` for anything drawn from elsewhere."""
        self.stages.append((name, count, note))

    def added(self, name: str, count: int, note: str = "") -> None:
        """A count that JOINS the digest instead of surviving the chain.

        Kept out of `stages` because render_funnel reads that list as one
        shrinking set and prints every line's delta against the line above.
        A count drawn from the ledger rather than from this run's fetch shares
        no population with the line above it, so a delta there is arithmetic
        over two unrelated sets. Measured on 2026-09-12, 2 reminders sitting
        under 30 new events rendered as "(-28)", reading as a late collapse on
        the last line of the report, where the digest had in fact grown to 32.
        """
        self.additions.append((name, count, note))

    def first_zero(self) -> str | None:
        """Which filter emptied the run, or None.

        `stages` only. A zero in `additions` means nothing was due for a
        reminder and a zero in `per_source` already renders its own flag, so
        neither answers this question and folding them in would misattribute
        a quiet run to a filter that dropped nothing.
        """
        for name, count, _ in self.stages:
            if count == 0:
                return name
        return None


def render_funnel(funnel: "Funnel") -> str:
    lines = ["", "FETCH"]
    for name, count, note in funnel.per_source:
        flag = "  <-- returned nothing" if count == 0 and not note else ""
        lines.append(f"  {name:<32} {count:>5}  {note}{flag}")
    lines.append(f"  {'TOTAL':<32} {sum(c for _, c, _ in funnel.per_source):>5}")
    lines += ["", "FUNNEL"]
    previous = None
    for name, count, note in funnel.stages:
        delta = "" if previous is None else f"({count - previous:+d})"
        lines.append(f"  {name:<32} {count:>5}  {delta:<8}{note}")
        previous = count
    # Deltas are deliberately absent here. These counts join the digest from
    # the ledger, so there is no previous line for them to have shrunk from.
    if funnel.additions:
        lines += ["", "ALSO IN THE DIGEST, not filtered from the chain above"]
        for name, count, note in funnel.additions:
            lines.append(f"  {name:<32} {count:>5}  {note}")
    return "\n".join(lines)


def run(sources: list[EventSource], store: EventStore, geo: EventFilter,
        settings: Settings, notifier: Notifier, dry_run: bool = False,
        now: datetime | None = None, report_all: bool = False,
        extractor: Extractor | None = None) -> int:
    """One pass. `report_all` reports every in-scope event, not just unseen
    ones (see local_run.py's --all for why). `extractor` is optional because
    the keyword-only tier has no model to call."""
    now = now or datetime.now(timezone.utc)
    funnel = Funnel()
    # Read before this run marks anything, so its own events still count as new.
    reported_urls = store.reported_urls()
    # Companion to reported_urls (see EventStore.reported_identities for the
    # "same question asked the other way" rationale). _collapse only compares
    # listings within one batch, so without this a second source publishing
    # the same event a day later reads as news.
    reported_events = {key for title, start in store.reported_identities()
                       if (key := _identity(title, start)) is not None}
    # "Has the user ever actually been told anything", NOT "does the ledger have
    # rows". upsert records what a run saw even on a dry run, so a ledger-rows
    # test flipped to False after any preview and let the very next real run
    # skip _seed_eligible and mail the whole backlog. Measured: one event
    # published 30 days ago, seed_recent_days=3, sent 0 on a fresh live run and
    # 1 when a dry run came first.
    seeding = not reported_urls

    # 1. fetch. A source that raises is reported and skipped, never fatal to
    #    the run (see EventSource.fetch's fail-closed contract).
    raw: list[Event] = []
    for source in sources:
        try:
            events = source.fetch()
            raw.extend(events)
            # Beside the count, because the count alone looks healthy (see
            # Coverage's docstring for the WordPress example).
            capped = source.coverage() if isinstance(source, BoundedSource) else None
            funnel.source(source.name, len(events), str(capped) if capped else "")
        except Exception as exc:
            funnel.source_failed(source.name, exc)
    # Raised, not reported. One source failing is noise a digest can survive,
    # but EVERY source failing means the run learned nothing, and returning
    # normally there is what let a cloud run exit 0 with no digest and a green
    # tick. Same split one level down, see source_failed's docstring above.
    #
    # The funnel is printed FIRST so the aligned table and the coverage notes
    # survive. The message alone still names every source and its error, so this
    # is a legibility choice rather than the only record.
    if funnel.all_sources_failed():
        print(render_funnel(funnel))
        names = ", ".join(f"{name} ({detail})" for name, detail in
                          funnel.source_errors)
        raise AllSourcesFailedError(
            f"every source failed ({len(funnel.source_errors)}/"
            f"{len(funnel.per_source)}): {names}")
    funnel.stage("fetched", len(raw))

    # 2. cross-source dedupe on the canonical URL, keeping the most structured
    #    record when the same event arrives twice (config.source_precedence).
    rank = settings.precedence_rank
    # The absorbed listings are dropped here, unlike the second pass: two
    # adapters colliding on this key hold the SAME url by definition, so
    # recording them would add ledger rows that answer nothing new.
    deduped, _ = _collapse(raw, rank, lambda e: e.url)
    funnel.stage("after url dedupe", len(deduped))

    # Extraction runs BEFORE the geo filter, not after. Skipping out-of-scope
    # events would be cheaper, but the events that need extraction are exactly
    # the ones the geo filter CANNOT judge: the newsletter and mailbox tiers
    # emit a URL and a title with no location at all, so every one of them was
    # being dropped as out of region and never reached the extractor. Measured
    # with the wrong order: gmail_label 0 events, newsletter 0 events. Those two
    # tiers are the only ones that reach mail-only employer events, so the wrong
    # order silently costs the whole category.
    if extractor is not None:
        thin = [e for e in deduped if extractor.needs_extraction(e)]
        if thin:
            filled = _extract_all(extractor, thin, settings.score_workers)
            by_uid = {e.event_uid: e for e in filled}
            deduped = [by_uid.get(e.event_uid, e) for e in deduped]
            got_date = sum(1 for e in deduped if e.start and e.event_uid in by_uid)
            got_place = sum(1 for e in deduped if e.location and e.event_uid in by_uid)
            funnel.stage("after extraction", len(deduped),
                         f"of {len(thin)} thin items: {got_date} gained a date, "
                         f"{got_place} a location")

    # The one point every adapter AND the extractor have already run, which is
    # why the offset is attached here rather than in each source. A stamp with
    # no offset is read as UTC by parse_iso and, decisively, by SQLite inside
    # expire_past and due_for_resweep, and no Python-side fallback can reach
    # those two. Measured on the live ledger: a San Francisco job fair stored as
    # "2026-10-08T00:00:00" landed at 2026-10-07 17:00 Pacific, so the row
    # expired the evening BEFORE the event and could never earn a last call.
    #
    # Reported on stdout rather than as a funnel stage, because a stage means a
    # delta between two of the same set and this one drops nothing.
    zone = display_zone(settings.display_timezone)
    stamped = [_stamp_naive(e, zone) for e in deduped]
    unzoned = sum(1 for was, now_ in zip(deduped, stamped) if was is not now_)
    if unzoned:
        print(f"{unzoned} event(s) stated no UTC offset; read as "
              f"{settings.display_timezone}")
    deduped = stamped

    # Same event, different URL. Two sources can each publish their own link to
    # one real event (a Luma page and the host's own page), and the URL pass
    # above cannot see that. Runs AFTER extraction on purpose: the tiers most
    # likely to carry a duplicate arrive with no start at all, and this key
    # needs one. Runs after the stamp too, since _same_event_key compares
    # instants and a naive copy of a zoned listing would miss its own twin.
    before = len(deduped)
    deduped, merged_away = _collapse(deduped, rank, _same_event_key)
    if before != len(deduped):
        funnel.stage("after title+time dedupe", len(deduped),
                     f"{before - len(deduped)} collapsed onto another listing")

    in_region = [e for e in deduped if geo.keep(e)]
    funnel.stage("after geo filter", len(in_region))

    horizon = now + timedelta(days=settings.lookahead_days)
    # Name the bound that cut them, because the two mean opposite things: below
    # the floor is an event you have already missed, above the horizon is one
    # announced unusually early. Measured on one full live run, at
    # lookahead_days 120: of 150 in-region events 49 had already started and 0
    # sat beyond the horizon -- so a stage that looks over-filtered is nearly
    # always showing the day's own finished events, not a lookahead set too
    # short. Counts in_region, i.e. AFTER extraction has filled thin items in
    # (73 gained a date that run), so it cannot be reproduced from a raw fetch.
    #
    # Derived from _bound_reason rather than re-tested here, so the funnel's
    # explanation cannot drift from the filter (see its docstring for why).
    reasons = [_bound_reason(e, now, horizon) for e in in_region]
    dated = [e for e, why in zip(in_region, reasons) if why is None]
    funnel.stage(f"starts within {settings.lookahead_days}d", len(dated),
                 f"{reasons.count(_STARTED)} already started, "
                 f"{reasons.count(_TOO_FAR)} beyond the horizon")

    gated = [e for e in dated if _passes_gate(e, settings)]
    funnel.stage(f"keyword gate (min_hits={settings.min_hits})", len(gated))

    scorer = build_scorer(settings, store)
    urgency_engine: UrgencyEngine = DeadlineUrgency(
        urgent_hours=settings.urgent_hours,
        p0_min_rank=settings.p0_min_rank,
        p1_min_rank=settings.p1_min_rank,
        unknown_time_max=Urgency(settings.unknown_time_max_urgency))
    scored = _score_all(scorer, gated, settings.score_workers)
    store.save()   # persist cached scores even if a later stage raises
    funnel.scored(len(gated), len(scored))
    # Read back rather than subtracted a second time, so the note and the
    # health fact cannot disagree about how many failed.
    funnel.stage(f"scored [{scorer.method_label}]", len(scored),
                 f"{funnel.scoring.failed} failed" if funnel.scoring.failed else "")
    # A failed event is NOT written to the ledger, so the below-floor audit
    # trail cannot answer for it. That is deliberate: upsert needs a Score, and
    # inventing one is precisely the middling-score fallback _parse_score was
    # changed to raise on. The funnel fact above is the record instead.

    # Everything scored is recorded, but only what clears the floor is reported.
    # Scoring without a threshold is what put a poker tournament and a student
    # health-plan orientation into the digest: the model judged them correctly
    # and nothing acted on the judgement.
    above_floor = [(e, sc) for e, sc in scored if sc.rank >= settings.digest_min_rank]
    funnel.stage(f"rank >= {settings.digest_min_rank}", len(above_floor),
                 f"{len(scored) - len(above_floor)} scored but below the floor")

    # Withheld AFTER scoring, not gated out before it, so the ledger still holds
    # a judgement and the reason survives. Dropping a full event earlier would
    # answer "why was I never told about X" with silence, which is the failure
    # the below-floor audit trail above exists to avoid.
    #
    # Rank cannot carry this on its own. The keyword tier takes the MAXIMUM
    # access weight and reads no capacity marker at all, so on that tier a full
    # event scores exactly as an open one, and that tier is what a cloud run
    # without an API key uses for every event.
    full: list[tuple[Event, Score]] = []
    worth_sending: list[tuple[Event, Score]] = []
    for event, score in above_floor:
        (full if _at_capacity(event, settings) else worth_sending).append((event, score))

    # 3. persist BEFORE deciding what to send, so a crash in the notifier cannot
    #    lose the fact that these events were seen.
    #
    #    Below-floor events are stored too, for AUDIT. When you later ask "why
    #    was I never told about X", the ledger answers "rank 4, below the floor"
    #    rather than leaving you unable to distinguish that from a broken source
    #    -- the same reason the funnel reports every stage. It does NOT save a
    #    model call: score_cache is written during scoring, independently of this
    #    table, so dropping these rows would cost nothing in LLM spend.
    below_floor: list[tuple[Event, Score, Urgency]] = []
    for event, score in scored:
        if score.rank < settings.digest_min_rank:
            store.upsert(event, score, Urgency.P2,
                         keyword_hits=",".join(_hits(event, settings)))
            below_floor.append((event, score, Urgency.P2))
    # Recorded for the same audit reason, then marked so the ledger says WHY no
    # mail went out. upsert runs FIRST because it inserts a new row at state
    # 'new' and never rewrites state on a repeat sighting, so marking before it
    # would either find no row or be immediately meaningless.
    if full:
        for event, score in full:
            store.upsert(event, score, Urgency.P2,
                         keyword_hits=",".join(_hits(event, settings)))
        # Reported from what the ledger actually changed, not from len(full).
        # mark_sold_out leaves a row the reader already saved or dismissed
        # alone, so the two differ exactly when a decision is being preserved,
        # and a stage that quoted the wrong one would claim a withholding the
        # ledger does not show.
        marked = store.mark_sold_out([e.event_uid for e, _ in full])
        funnel.stage("not full", len(worth_sending),
                     f"{len(full)} withheld, the listing says sold out"
                     + (f", {marked} newly marked" if marked != len(full) else ""))
    fresh: list[tuple[Event, Score, Urgency]] = []
    for event, score in worth_sending:
        urgency = urgency_engine.classify(event, score, now)
        store.upsert(event, score, urgency,
                     keyword_hits=",".join(_hits(event, settings)))
        # Recording each alias this event absorbed (see _collapse) is what
        # stops a second mail once the winning source drops the event and
        # only the alias is left.
        for alias in merged_away.get(event.event_uid, ()):
            store.upsert(alias, score, urgency,
                         keyword_hits=",".join(_hits(alias, settings)))
        # Deliberately NOT upsert's insert/update flag. That flag is per uid and
        # the uid embeds source_kind, so the same event from a second source read
        # as new (measured: one URL via newsletter then jsonld, new both times);
        # and an event already in the ledger from a below-floor sighting read as
        # old forever, so the run where it finally cleared the floor was dropped
        # (also measured). The canonical URL is the identity that matters.
        is_new = (event.url not in reported_urls
                  and _same_event_key(event) not in reported_events)
        if is_new or report_all:
            fresh.append((event, score, urgency))
    # Computed after the loop so this run's own events still counted as new
    # above, but NOT written yet. This is the record that makes an event stop
    # being new, so writing it before the mail goes out turns one failed SMTP
    # call into permanent silence: next run they read as already reported,
    # while alerted_at stays empty so the sweep skips them too. _deliver writes
    # it once a send has actually happened, and skips it on a dry run for the
    # same reason mark_alerted does (see EventStore.mark_cleared_floor).
    #
    # Covers worth_sending rather than to_send because the seeding filter
    # deliberately withholds old-but-qualifying events, and they must not
    # arrive as a backlog on the second run.
    cleared = ([e.event_uid for e, _ in worth_sending]
               + [a.event_uid for e, _ in worth_sending
                  for a in merged_away.get(e.event_uid, ())])
    # The same grace _bound_reason applies, passed rather than re-stated, so
    # the ledger cannot call an event over while the digest still offers it.
    store.expire_past(now.isoformat(),
                      int(_START_GRACE.total_seconds() // 3600))
    store.save()
    funnel.stage("all in scope" if report_all else "new since last run", len(fresh))

    to_send = fresh
    # Carried only by the run that has nothing to compare against, and never
    # marked afterwards (see Digest.below_floor).
    show_below_floor = False
    if seeding and not report_all:
        # report_everything deliberately runs no filter at all, so it needs its
        # own funnel line: a stage that silently does nothing reads as a stage
        # that was skipped, and the reader cannot tell a mode from a bug.
        if settings.first_run_mode == "report_everything":
            show_below_floor = True
            funnel.stage("first run: report_everything", len(to_send),
                         f"whole backlog, plus {len(below_floor)} below the floor")
        elif settings.first_run_mode == "seed_plus_recent":
            to_send = [t for t in fresh if _seed_eligible(t[0], now, settings)]
            funnel.stage(f"first run: published within {settings.seed_recent_days}d",
                         len(to_send),
                         "unknown publish time excluded" if settings.require_known_publish_time else "")
        else:
            # config._check_policies rejects an unknown mode, but Settings is a
            # plain dataclass anyone can build directly, and check.py does
            # exactly that. Without this, a typo would silently take the other
            # branch, which is the failure _check_policies exists to prevent.
            raise ValueError(f"unhandled first_run_mode {settings.first_run_mode!r}")

    # 5. the anti-miss sweep. Being told about an event three weeks ago is not
    #    the same as being reminded the week it happens, and the ledger is the
    #    only thing that remembers an event the sources have long since stopped
    #    listing. Skipped while seeding, for the same reason the digest is: the
    #    first run must not empty the backlog into the reader's inbox.
    reminders: list[tuple[Event, Score, Urgency]] = []
    if not seeding:
        # Two exclusions, because a resweep candidate and a fresh listing can be
        # the same real event under different uids: the ledger identifies by
        # URL, and _collapse only ever compared listings WITHIN one batch, so a
        # source that newly starts carrying an event another source already
        # mailed produces one of each. Measured, and the pair arrives in one
        # email with the older copy headed "you have not acted on it".
        already = {e.event_uid for e, _, _ in to_send}
        already_same = {key for e, _, _ in to_send
                        if (key := _same_event_key(e)) is not None}
        for event in store.due_for_resweep(
                settings.urgent_hours, now.isoformat(),
                min_gap_hours=settings.resweep_min_gap_hours):
            key = _same_event_key(event)
            if event.event_uid in already or (key is not None and key in already_same):
                continue
            # A cache hit in every normal case, since these were scored when
            # first seen and the cache is keyed on the same uid. Guarded anyway:
            # a miss reaches the model, and letting one optional reminder abort
            # the run would lose the whole digest that is already assembled.
            score = _safe_score(scorer, event)
            if score is None:
                continue
            reminders.append((event, score, urgency_engine.classify(event, score, now)))
            # Accumulated, not fixed up front: two aliases of one event can both
            # be due, and a set built only from to_send cannot separate them
            # when to_send is empty, which is exactly the quiet run where a last
            # call goes out.
            already.add(event.event_uid)
            if key is not None:
                already_same.add(key)
        if reminders:
            funnel.added("closing soon, never acted on", len(reminders),
                         "already reported once, starting soon, never acted on")

    print(render_funnel(funnel))
    sent = _deliver(Digest(to_send, reminders, cleared, seeding,
                           below_floor if show_below_floor else [], now),
                    funnel, store, notifier, settings, dry_run)
    # Last, so the reminders above have already been mailed and the funnel has
    # already been printed. Zero sent and exit 0 is the outcome a total scoring
    # outage used to produce, and it is indistinguishable from a quiet week,
    # which is the same confusion AllSourcesFailedError was added to end one
    # stage earlier.
    if funnel.all_scoring_failed():
        raise AllScoringFailedError(
            f"every scored event failed ({funnel.scoring.failed}/{funnel.scoring.attempted}) "
            f"on the {scorer.method_label} tier; {sent} still sent from the ledger")
    return sent


def _start_order(entry: tuple[Event, Score, Urgency]) -> datetime:
    """Sort key for a digest line, as an INSTANT rather than the raw string.

    Sources state different offsets, so a lexical sort disagrees with the clock.
    Measured on the live ledger 2026-09-18: "2026-09-08T09:00:00-07:00" sorts
    before "2026-09-08T12:30:00+00:00" on text while happening three and a half
    hours LATER, and six dated rows changed position once the comparison used
    instants. Undated events sort last, where the previous "9999" sentinel put
    them.
    """
    when = parse_iso(entry[0].start) if entry[0].start else None
    return when or datetime.max.replace(tzinfo=timezone.utc)


def _deliver(digest: Digest, funnel: Funnel, store: EventStore,
             notifier: Notifier, settings: Settings, dry_run: bool) -> int:
    to_send, reminders = digest.to_send, digest.reminders
    below_floor = digest.below_floor
    if not to_send and not reminders and not below_floor:
        print("\nRESULT")
        stage = funnel.first_zero()
        print("  No matching events to report.")
        if stage:
            print(f"  Everything was dropped at: {stage}")
            print("  If that stage is 'fetched', the sources are the problem, not the filters.")
        else:
            print("  Sources returned events, but none were both new and in scope.")
            print("  That is a normal quiet run. Re-check when the funnel above shows")
            print("  a stage dropping to zero that previously did not.")
        if digest.seeding:
            print("  This was a SEEDING run: existing events were recorded, not mailed.")
        print("  NO EMAIL SENT (nothing to send)." if dry_run
              else "  NO EMAIL SENT (nothing cleared the threshold).")
        # Nothing was mailed and nothing could fail, so the judgement stands:
        # these were weighed and found worth telling the reader about, even
        # where the seeding filter held them back this once.
        if not dry_run:
            store.mark_cleared_floor(digest.cleared)
            store.save()
        return 0

    # urgent and rest are new lists, so sorting them in place is local. The
    # rebind below is NOT cosmetic: `reminders` and `below_floor` are the very
    # objects held by the frozen Digest, and frozen only blocks rebinding the
    # field, not mutating what it points at -- .sort() here would reorder the
    # caller's list.
    urgent = [t for t in to_send if t[2] is Urgency.P0]
    rest = [t for t in to_send if t[2] is not Urgency.P0]
    # urgent needs the sort as much as rest does. Left in fetch order, a
    # digest has printed its P0 ranks as 48, 48, 42, 48.
    urgent.sort(key=lambda t: (-t[1].rank, _start_order(t)))
    rest.sort(key=lambda t: (-t[1].rank, _start_order(t)))
    reminders = sorted(reminders, key=_start_order)
    sections: list[Section] = [
        # Named for the rank cut, not for the clock. These two split on
        # Urgency.P0, which needs a rank of p0_min_rank as well as a start
        # inside urgent_hours, so a heading promising only "within 72h" was
        # read as a time partition it does not perform. Measured on one run:
        # the 72h heading was empty while 10 of the 11 events under OTHER
        # PICKS started inside 72 hours, the nearest in 13.
        (f"TOP PICKS - closing within {settings.urgent_hours}h", urgent),
        ("OTHER PICKS", rest)]
    if below_floor:
        below_floor = sorted(below_floor,
                             key=lambda t: (-t[1].rank, _start_order(t)))
        sections.append(
            (f"BELOW THE USUAL FLOOR - rank under {settings.digest_min_rank}, "
             "shown once on the first run so the cut is yours to make",
             below_floor))
    # Its own section, not folded into TOP PICKS: these were sent once
    # already, so the reader needs to see why the same event is back. Last in
    # the mail for the same reason, since what the reader has never seen
    # belongs above what they have.
    #
    # The heading says what the ledger actually knows. It cannot say "you have
    # not acted on it": the only states that would prove otherwise are set by
    # EventStore.set_state, which nothing but local_run.py --mark reaches, so
    # for a reader who has never run that the claim would always be true by
    # construction and therefore meaningless.
    sections.append(
        (f"LAST CALL - starts within {settings.urgent_hours}h, "
         "sent once before, and this is the only reminder", reminders))
    total = len(to_send) + len(reminders) + len(below_floor)
    subject = (f"[Event Scout] {total} events"
               + (f" ({len(urgent) + len(reminders)} closing soon)"
                  if urgent or reminders else ""))
    method = (to_send or reminders or below_floor)[0][1].method
    footer = (f"Scored by the {method} tier. fit x access_value drives "
              "the ordering; cost is shown, not applied.")

    sent = notifier.send(sections, subject, footer)
    print("\nRESULT")
    print(f"  {sent} events  |  P0 {len(urgent)}  other {len(rest)}"
          + (f"  last call {len(reminders)}" if reminders else "")
          + (f"  below floor {len(below_floor)}" if below_floor else ""))
    # Restated here because the header has scrolled far off screen by now, and
    # "did that just email me?" is the one question the output must never leave
    # ambiguous.
    print("  NO EMAIL SENT (dry run)." if dry_run
          else "  EMAIL SENT.")
    if not dry_run:
        # After the send, never before: every line here claims the reader has
        # seen these, and notifier.send raises when SMTP refuses.
        store.mark_cleared_floor(digest.cleared)
        store.mark_alerted([e.event_uid for e, _, _ in to_send],
                           at=digest.now.isoformat())
        # Marked separately and permanently: mark_alerted would leave these
        # eligible again on the next hourly run, which is 72 reminders for one
        # event over a 72h window.
        store.mark_swept([e.event_uid for e, _, _ in reminders])
        store.save()
    print(f"  ledger states: {store.counts_by_state()}")
    return sent


def _score_all(scorer: EventScorer, events: list[Event],
               workers: int) -> list[tuple[Event, Score]]:
    """Score concurrently; one failure drops that event, never the whole run.

    The CLI tier spawns a heavy subprocess per worker, so this is the difference
    between a run taking a minute and taking an hour.
    """
    if workers <= 1 or len(events) <= 1:
        pairs = [(e, _safe_score(scorer, e)) for e in events]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            pairs = list(zip(events, pool.map(lambda e: _safe_score(scorer, e), events)))
    return [(e, s) for e, s in pairs if s is not None]


def _extract_all(extractor: Extractor, events: list[Event],
                 workers: int) -> list[Event]:
    """Fill dates concurrently. Each call fetches a page and asks a model, so
    this is as I/O-bound as scoring and benefits from the same pool."""
    if workers <= 1 or len(events) <= 1:
        return [_safe_extract(extractor, e) for e in events]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(lambda e: _safe_extract(extractor, e), events))


def _safe_extract(extractor: Extractor, event: Event) -> Event:
    try:
        return extractor.extract(event)
    except Exception as exc:
        # Fail open: an undated event is still worth reporting.
        print(f"  extraction failed for {event.title[:40]!r}: {type(exc).__name__}")
        return event


def _safe_score(scorer: EventScorer, event: Event) -> Score | None:
    try:
        return scorer.score(event)
    except Exception as exc:
        print(f"  scoring failed for {event.title[:40]!r}: {type(exc).__name__}: {str(exc)[:90]}")
        return None


# How long after it starts an event is still worth reporting. You can still
# walk into this morning's session; yesterday's is gone.
_START_GRACE = timedelta(hours=12)
_STARTED = "started"
_TOO_FAR = "far"


def _stamp_naive(event: Event, zone: ZoneInfo) -> Event:
    """`event` with every unzoned lifecycle timestamp read as `zone`, not UTC.

    The wall time is kept and an offset is attached, so a source writing
    "18:00" is taken to mean 18:00 where the reader is. ZoneInfo derives the
    offset from the date, so a January event gets standard time rather than the
    summer one a fixed offset would freeze in.

    start, end and rsvp_deadline only. published_at is left naive because no
    lifecycle query reads it and _seed_eligible compares it in whole days,
    where a seven-hour shift cannot reach the boundary that setting turns on.

    A bare date is the other case, and it is not the same as midnight. The
    source said WHICH DAY and nothing more, so a bare start becomes local
    midnight with all_day set, and a bare end becomes the last second of its
    own local day. Midnight alone would be read as a midnight start, and the
    row would be retired at midday by expire_past even with the start grace.

    An all-day event then ends at the last second of the later day, the one
    the source stated or the start's own. That keeps a multi-day listing alive
    for its whole range and still gives a single-day one an end it can be
    drawn with, and it holds the end to day granularity so the all_day flag
    stays true of the whole row.

    Whatever the shape, an end that precedes the start is dropped. A source
    can state one directly, so this is not a repair of anything above it.

    Returns the SAME object when nothing changed, which is what lets the caller
    count how often this fired without re-testing every field.
    """
    patch = {}
    for field in ("start", "end", "rsvp_deadline"):
        text = getattr(event, field)
        if not text:
            continue
        # Cannot raise: Event.__post_init__ has already rejected any value
        # iso_or_empty could not re-emit, and iso_or_empty resolves "Z" itself,
        # so nothing reaching here carries a suffix fromisoformat would refuse.
        when = datetime.fromisoformat(text)
        if when.tzinfo is None:
            patch[field] = iso_or_empty(when.replace(tzinfo=zone).isoformat())
    # A bare date in the END means "through that day", and that holds whatever
    # the start looks like, so it is settled BEFORE the all-day branch rather
    # than inside it. The loop above has just stamped it to local MIDNIGHT,
    # which beside an 09:00 start lands nine hours EARLIER and makes build_ics
    # emit a DTEND before its DTSTART, which RFC 5545 forbids and a calendar
    # client either rejects or silently rewrites. Reachable, because the URL
    # collapse runs before this function and can pair one source's timed start
    # with another's bare-date end.
    if is_date_only(event.end):
        patch["end"] = _end_of_day(event.end, zone)
    if is_date_only(event.start):
        patch["all_day"] = True
        patch["start"] = iso_or_empty(
            datetime.fromisoformat(event.start).replace(tzinfo=zone).isoformat())
        # The last second of the LATER day, the one the source stated or the
        # start's own -- day only, never a stated clock time, since all_day
        # means the source knew a day and not a time. An end that merely
        # repeats the start is pushed out to end-of-day rather than left at
        # the same instant, which is what stops build_ics emitting a
        # zero-length VEVENT, and a day genuinely further out is kept in full:
        # expire_past and _bound_reason both read this column for an all-day
        # row on the strength that _stamp_naive always synthesises it, so
        # flattening a five-day conference onto its opening day, or letting a
        # stated clock time through unaltered, would each just as surely
        # reopen the case those two say they are safe from.
        #
        # Reads `patch.get("end") or event.end`, never the patch alone. An end
        # that already carried an offset was never stamped, so it is absent
        # from the patch, and comparing against the patch only would treat the
        # same event differently for having stated its offset.
        #
        # Which local day, converted, never sliced off the front of the
        # string. A stated end carries whatever offset its source used, and 22
        # rows in the live ledger end in "+00:00", so the date in the text is
        # not the date the reader is on: "2026-10-06T05:00:00+00:00" is still
        # the 5th in Pacific, and slicing it would hold the row a day too
        # long. Comparing instants and then reading a date off the text is the
        # same text-versus-instant split expire_past's docstring measures.
        own_day = _end_of_day(event.start, zone)
        stated = patch.get("end") or event.end
        stated_when = parse_iso(stated) if stated else None
        patch["end"] = (
            _end_of_day(stated_when.astimezone(zone).date().isoformat(), zone)
            if stated_when and stated_when > parse_iso(own_day)
            else own_day)
    # An end before the start is not a short event, it is an unusable value,
    # so it is dropped rather than carried. build_ics then falls back to the
    # hour-after-the-start it already uses for a missing end, instead of
    # emitting a DTEND earlier than its DTSTART, which RFC 5545 forbids.
    #
    # Last, and on the merged values rather than the raw ones, because every
    # branch above can produce this and no single one of them owns it. Two
    # fully zoned timestamps can arrive inverted from a source without this
    # function touching either.
    final_start = patch.get("start") or event.start
    final_end = patch.get("end") or event.end
    if final_start and final_end and parse_iso(final_end) < parse_iso(final_start):
        patch["end"] = ""
    return replace(event, **patch) if patch else event


def _end_of_day(date_text: str, zone: ZoneInfo) -> str:
    """The last second of `date_text` as a local instant.

    Takes a bare date, since that is the only input whose whole day is being
    claimed. Callers ordering this against anything else compare instants
    through parse_iso rather than the strings, because only one side is
    guaranteed to be this function's own fixed-width output.
    """
    return iso_or_empty(datetime.fromisoformat(date_text)
                        .replace(tzinfo=zone, hour=23, minute=59, second=59)
                        .isoformat())


def _bound_reason(event: Event, now: datetime, horizon: datetime) -> str | None:
    """Which date bound excludes this event, or None to keep it.

    The single owner of that decision. run() reports its counts by asking here
    rather than by repeating the comparison, so the funnel's explanation cannot
    drift from the filter it is explaining.

    An event with no usable start is KEPT, not guessed at. The extractor fills
    these in later, and excluding them here would silently discard the whole
    newsletter tier, whose events state their date in prose rather than a field.
    """
    when = parse_iso(event.start) if event.start else None
    if when is None:
        return None
    # An all-day event is over when its LAST day is, not when its midnight
    # start was. Reads the end _stamp_naive synthesised, the same column
    # expire_past reads, so the filter and the ledger cannot disagree about
    # when something is finished and a multi-day listing survives its opening
    # day in both. A stated end is not consulted for anything else; see
    # expire_past for the measurement that rules it out.
    over = parse_iso(event.end) if event.all_day and event.end else None
    over = over or when
    if over < now - _START_GRACE:
        return _STARTED
    return _TOO_FAR if when > horizon else None



def _seed_eligible(event: Event, now: datetime, settings: Settings) -> bool:
    if not event.published_at:
        return not settings.require_known_publish_time
    when = parse_iso(event.published_at)
    return when is not None and (now - when).days <= settings.seed_recent_days


def _at_capacity(event: Event, settings: Settings) -> bool:
    """Does the listing SAY it is full.

    Sits beside _hits and reads the same two fields, because the two answer
    opposite halves of one question and a marker list that drifted apart from
    the keyword list would be the same scan with different rules.

    UNLIKE _hits this reads the WHOLE description rather than a bounded prefix.
    The bound there stops a newsletter issue matching every interest term it
    happens to mention; here the risk runs the other way, since a capacity line
    sits at the end of a listing at least as often as the start, and a marker
    missed means a full event is mailed as if it were open.
    """
    text = f"{event.title} {event.description}".lower()
    return any(marker in text for marker in settings.capacity_markers)


def _hits(event: Event, settings: Settings) -> list[str]:
    """Terms admitting this event, matched on the title plus a bounded prefix.

    The bound is the point. A newsletter issue runs to hundreds of thousands of
    characters and mentions nearly every candidate term somewhere, so matching
    the whole body with min_hits 1 admits every issue and filters nothing.
    """
    body = event.description[: settings.keyword_match_prefix_chars]
    text = f"{event.title} {body}".lower()
    return [k for k in settings.keywords if k in text]


def _passes_gate(event: Event, settings: Settings) -> bool:
    # The gate applies only to the sources named in config, because a curated
    # calendar is already on-topic and gating it drops real events on naming
    # technicalities (measured: 1 of 4 in-scope Cerebral Valley events).
    if event.source_kind not in settings.keyword_gate_applies_to:
        return True
    return len(_hits(event, settings)) >= settings.min_hits


def _collapse(events: list[Event], rank: dict[str, int],
              key: Callable[[Event], Hashable | None],
              ) -> tuple[list[Event], dict[str, list[Event]]]:
    """Merge events sharing a key, the highest-precedence source winning.

    Returns the survivors and, per surviving event_uid, the listings it
    absorbed. The caller needs the second half because those listings have
    their own URLs and the ledger answers "already reported" by URL.

    `key` returning None means the event cannot be identified this way; those
    pass through untouched rather than piling into one bucket together.
    """
    best: dict[object, Event] = {}
    winner: dict[object, str] = {}
    absorbed: dict[str, list[Event]] = {}
    loose: list[Event] = []
    for event in sorted(events, key=lambda e: rank.get(e.source_kind, 99)):
        marker = key(event)
        if marker is None:
            loose.append(event)
            continue
        held = best.get(marker)
        if held is None:
            best[marker] = event
            winner[marker] = event.event_uid
        else:
            best[marker] = held.merged_with(event)
            if event.event_uid != winner[marker]:
                absorbed.setdefault(winner[marker], []).append(event)
    return list(best.values()) + loose, absorbed


def _identity(title: str, start: str) -> tuple[str, float] | None:
    """A name and the instant it starts, or None if either is missing.

    Compared as an instant rather than as text, because the same moment arrives
    written differently per source: schema.org markup carries milliseconds
    ("...T18:00:00.000-07:00") where a scraped page does not.

    A start is REQUIRED. Two undated listings sharing a title are far more
    likely a recurring series, one meetup name across many nights, than one
    event seen twice, and collapsing those would silently drop real dates.

    Takes the two fields rather than an Event so the ledger's stored rows go
    through the identical rule; a second copy of it for the persisted side is
    how the batch and the ledger would end up disagreeing.

    Two unrelated events collide here only by sharing a name after casefolding
    AND starting in the same second, but the price differs by caller: _collapse
    merges them into one entry the reader still sees, while the reported check
    drops the second one entirely. That second outcome is why this key demands
    an exact instant rather than a same-day match.
    """
    when = parse_iso(start)
    name = " ".join(title.split()).casefold()
    if not name or when is None:
        return None
    return name, when.timestamp()


def _same_event_key(event: Event) -> tuple[str, float] | None:
    return _identity(event.title, event.start)

