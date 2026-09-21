"""SQLite ledger and state machine.

Chosen over CSV shards because the questions here are
queries, not appends: what is upcoming, whose deadline closes next, what state
is each item in.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from .models import (AttendanceMode, Event, Score, State, Urgency,
                     iso_or_empty)
from .urls import canon_url

# Columns export_jsonl holds back. Removing them does NOT make the mirror
# publishable, since titles, URLs and delivery state still describe one
# reader; the mirror belongs in private storage regardless. These three are
# withheld on top of that because they are other people's writing rather than
# facts about an event. `description` is where GmailLabelSource parks whole
# message bodies, `organizer` is the sender's own From header, and `reason` is
# the model's prose about a specific reader.
#
# Withholding round-trips cleanly: import_jsonl builds each INSERT from the
# keys the line actually carries, so an absent column takes the schema default.
# No digest section renders any of the three either.
#
# Redaction happens HERE and not at upsert, so a live run still scores against
# full text and only the published copy is thinned. The cost is paid only on a
# reload: the score cache still HITS on its key, but a row restored from the
# mirror carries an empty reason, and a genuine cache miss then re-scores on
# title and venue alone.
_UNPUBLISHED_COLUMNS: dict[str, frozenset[str]] = {
    "events": frozenset({"description", "organizer", "reason"}),
    "score_cache": frozenset({"reason"}),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_uid       TEXT PRIMARY KEY,
    canonical_url   TEXT NOT NULL,
    title           TEXT NOT NULL,
    organizer       TEXT DEFAULT '',
    start           TEXT DEFAULT '',
    end_at          TEXT DEFAULT '',
    location        TEXT DEFAULT '',
    attendance_mode TEXT DEFAULT '',
    description     TEXT DEFAULT '',
    rsvp_deadline   TEXT DEFAULT '',
    published_at    TEXT DEFAULT '',
    source          TEXT NOT NULL,
    source_kind     TEXT NOT NULL,
    fit             INTEGER,
    access_value    INTEGER,
    cost            INTEGER,
    reason          TEXT DEFAULT '',
    score_method    TEXT DEFAULT '',
    urgency         TEXT DEFAULT '',
    state           TEXT NOT NULL DEFAULT 'new',
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    alerted_at      TEXT DEFAULT '',
    -- Sticky, and deliberately not derivable from the columns beside it.
    -- fit/access_value are overwritten on every sighting, so they cannot say
    -- whether a URL was ever worth telling the user about; alerted_at is
    -- written only outside dry runs and only for events that survived the
    -- seeding filter, so it cannot either.
    cleared_floor_at TEXT DEFAULT '',
    -- Sticky too. The sweeper reminds ONCE per event; the cloud runs
    -- hourly, so without this a 72h window would mail 72 reminders.
    swept_at TEXT DEFAULT '',
    keyword_hits    TEXT DEFAULT ''
);
-- Cross-source dedupe reads this constantly; the PK alone does not serve it
-- because two adapters build different uids for the same canonical URL.
CREATE INDEX IF NOT EXISTS idx_events_url ON events(canonical_url);
CREATE INDEX IF NOT EXISTS idx_events_state_start ON events(state, start);


-- Scores keyed by (event, model tier); see CachingScorer for why caching pays
-- off. Keyed on the tier tag too, so switching model invalidates rather than
-- reuses a stale answer.
CREATE TABLE IF NOT EXISTS score_cache (
    event_uid TEXT NOT NULL,
    model_tag TEXT NOT NULL,
    fit INTEGER, access_value INTEGER, cost INTEGER,
    reason TEXT DEFAULT '', method TEXT DEFAULT '',
    scored_at TEXT NOT NULL,
    PRIMARY KEY (event_uid, model_tag)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _row_to_event(row: sqlite3.Row) -> Event:
    """Rehydrate an Event so SQL column names never escape this module.

    Timestamps go back through iso_or_empty: rows written before that gate
    existed carry the source's own spelling, and Event now rejects anything
    unnormalised. Measured on the real ledger, 70 of 93 dated rows are still
    in the old form, so reading without this would raise on most of them.
    """
    return Event(
        event_uid=row["event_uid"], title=row["title"], url=canon_url(row["canonical_url"]),
        source=row["source"], source_kind=row["source_kind"],
        start=iso_or_empty(row["start"] or ""),
        end=iso_or_empty(row["end_at"] or ""),
        organizer=row["organizer"] or "", location=row["location"] or "",
        attendance_mode=AttendanceMode(row["attendance_mode"] or ""),
        description=row["description"] or "",
        rsvp_deadline=iso_or_empty(row["rsvp_deadline"] or ""),
        published_at=iso_or_empty(row["published_at"] or ""))


def _merge_timestamp(column: str, pick: str) -> str:
    """SQL keeping whichever side actually has a timestamp, `pick` if both.

    The values are fixed-format UTC ISO 8601, so MIN/MAX on the text is MIN/MAX
    in time. The empty string means "never happened" and must lose to any real
    value, which a bare MIN would get backwards since '' sorts first.
    """
    return (f"{column} = CASE"
            f" WHEN events.{column} = '' THEN excluded.{column}"
            f" WHEN excluded.{column} = '' THEN events.{column}"
            f" ELSE {pick}(events.{column}, excluded.{column}) END")


# When the door shuts on an event, as SQL. The same expression as
# DeadlineUrgency._hours_until's `event.rsvp_deadline or event.start`, and it
# has to stay that way: the urgency tier and the sweep both answer "is this
# closing soon" and disagreeing put a P0 outside every reminder window.
_CLOSES_AT = "COALESCE(NULLIF(rsvp_deadline, ''), NULLIF(start, ''))"


# Only these four columns merge; everything else keeps whole-row precedence,
# because a later sighting's title or score is not more correct than an earlier
# one. The picks are not uniform on purpose: mark_cleared_floor and mark_swept
# each keep the first timestamp they ever wrote, first_seen is set once at
# INSERT, while mark_alerted overwrites with the latest. Merging them all the
# same way would make one of the four disagree with what a single store would
# have recorded.
_STICKY_MERGE = ("ON CONFLICT(event_uid) DO UPDATE SET "
                 + ", ".join((_merge_timestamp("cleared_floor_at", "MIN"),
                              _merge_timestamp("first_seen", "MIN"),
                              _merge_timestamp("swept_at", "MIN"),
                              _merge_timestamp("alerted_at", "MAX"))))


class SqliteEventStore:
    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False because the score cache is read and written
        # from ThreadPoolExecutor workers during concurrent scoring. Safe only
        # because EVERY method below serialises on _lock. An earlier version
        # locked just the two cache methods and claimed the class was
        # thread-safe; that held purely because of the order run() happened to
        # call things, not because the class enforced anything.
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._conn.executescript(_SCHEMA)
        # CREATE TABLE IF NOT EXISTS leaves an existing table alone, so a ledger
        # written before cleared_floor_at existed needs the column added here.
        # The backfill from alerted_at keeps already-emailed events suppressed;
        # without it the first run after upgrading would re-send every one.
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(events)")}
        if "swept_at" not in columns:
            # No backfill: nothing was ever swept before this column existed, so
            # an empty value is the truth rather than a gap to paper over.
            self._conn.execute("ALTER TABLE events ADD COLUMN swept_at TEXT DEFAULT ''")
        if "cleared_floor_at" not in columns:
            self._conn.execute(
                "ALTER TABLE events ADD COLUMN cleared_floor_at TEXT DEFAULT ''")
            self._conn.execute(
                "UPDATE events SET cleared_floor_at = alerted_at WHERE alerted_at != ''")
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def reported_urls(self) -> set[str]:
        """Canonical URLs that have ever cleared the digest floor."""
        with self._lock:
            return {canon_url(r[0]) for r in self._conn.execute(
                "SELECT canonical_url FROM events WHERE cleared_floor_at != ''")}

    def reported_identities(self) -> list[tuple[str, str]]:
        """(title, start) for every row already reported, dated ones only.

        Raw values, not keys: the caller turns them into identities with the
        same rule it applies to fresh events, so the two cannot drift into
        disagreeing about what counts as the same event.
        """
        with self._lock:
            return [(r[0], r[1]) for r in self._conn.execute(
                "SELECT title, start FROM events "
                "WHERE cleared_floor_at != '' AND start != ''")]

    def mark_cleared_floor(self, event_uids: list[str]) -> None:
        """Record, once and for good, that these events were worth sending.

        The WHERE clause keeps the FIRST timestamp: this answers "have I already
        put this in front of you", so it must never move or reset.
        """
        now = _now()
        with self._lock:
            self._conn.executemany(
                "UPDATE events SET cleared_floor_at=? "
                "WHERE event_uid=? AND cleared_floor_at=''",
                [(now, uid) for uid in event_uids])

    def upsert(self, event: Event, score: Score | None = None,
               urgency: Urgency | None = None, keyword_hits: str = "") -> None:
        """Insert or refresh one event.

        A repeat sighting must not reset `state` or `first_seen`: doing so would
        resurrect an event the user already dismissed, and the sweeper would then
        alert on it again every run.
        """
        now = _now()
        values = (
            event.url, event.title, event.organizer, event.start, event.end,
            event.location, str(event.attendance_mode), event.description,
            event.rsvp_deadline, event.published_at, event.source, event.source_kind,
            score.fit if score else None,
            score.access_value if score else None,
            score.cost if score else None,
            score.reason if score else "",
            score.method if score else "",
            str(urgency) if urgency else "",
            keyword_hits, now,
        )
        # ONE lock around read, decide and write. Splitting them, as an earlier
        # version did, leaves a check-then-act window: two threads can both see
        # "not present" and both take the INSERT branch, and the second one dies
        # on UNIQUE constraint failed. It never fired because upsert is called
        # sequentially today, but the class docstring promises thread safety and
        # this method was the one place not keeping that promise.
        with self._lock:
            existing = self._conn.execute(
                "SELECT event_uid FROM events WHERE event_uid = ?",
                (event.event_uid,)).fetchone()
            if existing:
                self._conn.execute(
                    """UPDATE events SET canonical_url=?, title=?, organizer=?, start=?,
                   end_at=?, location=?, attendance_mode=?, description=?,
                   rsvp_deadline=?, published_at=?, source=?, source_kind=?,
                   fit=?, access_value=?, cost=?, reason=?, score_method=?,
                       urgency=?, keyword_hits=?, last_seen=?
                       WHERE event_uid=?""", values + (event.event_uid,))
                return
            self._conn.execute(
                """INSERT INTO events (canonical_url, title, organizer, start, end_at,
               location, attendance_mode, description, rsvp_deadline, published_at,
               source, source_kind, fit, access_value, cost, reason, score_method,
               urgency, keyword_hits, last_seen, event_uid, state, first_seen)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'new',?)""",
                values + (event.event_uid, now))

    def set_state(self, event_uid: str, state: State) -> None:
        # No production caller yet; see EventStore.set_state.
        with self._lock:
            self._conn.execute("UPDATE events SET state=? WHERE event_uid=?",
                               (str(state), event_uid))


    def mark_by_url(self, url: str, state: State) -> list[tuple[str, str]]:
        """Set `state` on every listing sharing this canonical URL.

        Returns (event_uid, title) per row changed, so a caller can report what
        happened without asking twice.

        One URL means one event because every adapter builds its uid as
        "{kind}:{url}", so listings only multiply across SOURCES -- the same
        assumption the pipeline's first dedupe pass already makes. An adapter
        that used native ids instead (see Event.event_uid) would break it.
        """
        with self._lock:
            # Two passes, because canon_url has gained rules (recipient params,
            # a lowercase fix) since the oldest rows were written, so a row can
            # be stored under a spelling the caller's clean URL no longer
            # matches. The indexed lookup handles every row written since, and
            # the scan runs ONLY when that finds nothing. A full normalising
            # migration is not done instead because event_uid embeds the URL
            # and score_cache keys on event_uid, so rewriting one column would
            # have to cascade through the primary key.
            wanted = canon_url(url)
            rows = self._conn.execute(
                "SELECT event_uid, title FROM events WHERE canonical_url=?",
                (wanted,)).fetchall()
            if not rows:
                rows = [row for row in self._conn.execute(
                    "SELECT event_uid, title, canonical_url FROM events")
                    if canon_url(row["canonical_url"]) == wanted]
            self._conn.executemany(
                "UPDATE events SET state=? WHERE event_uid=?",
                [(str(state), r["event_uid"]) for r in rows])
        return [(r["event_uid"], r["title"]) for r in rows]

    def mark_sold_out(self, event_uids: list[str]) -> int:
        """Record that these were withheld because the listing says they are full.

        Writes `state` and NEVER `alerted_at`, so the mail record keeps meaning
        exactly one thing and the three `alerted_at != ''` queries are untouched.

        Only `new` and `seen` are overwritten, for the same reason upsert leaves
        state alone on a repeat sighting: a reader who saved or dismissed an
        event has made a decision, and a later listing edit must not undo it.
        `expired` is also left, because having started is the more final fact.
        """
        with self._lock:
            cursor = self._conn.executemany(
                "UPDATE events SET state=? WHERE event_uid=? "
                "AND state IN ('new', 'seen')",
                [(str(State.SOLD_OUT), uid) for uid in event_uids])
            return cursor.rowcount

    def mark_alerted(self, event_uids: list[str]) -> None:
        now = _now()
        with self._lock:
            self._conn.executemany(
                "UPDATE events SET alerted_at=?, state=CASE WHEN state='new' THEN 'seen' "
                "ELSE state END WHERE event_uid=?",
                [(now, uid) for uid in event_uids])

    def expire_past(self, now_iso: str, grace_hours: int) -> int:
        """Mark started events expired so the sweeper stops considering them.

        `grace_hours` is how long after its start an event is still live, and
        it is REQUIRED rather than defaulted to nothing. The pipeline's date
        window already keeps a started event for a while, and a default here
        would let this method and that window answer "has it started" twelve
        hours apart, which is exactly the split that put an event in the digest
        and marked it expired in the same run.

        The exclusion list is State.silences_sweeper spelled out in SQL. It has
        to stay in step: an event the user dismissed and then let start would
        otherwise be rewritten as merely expired, turning a decision they made
        into a timeout, and counts_by_state would report it as one.

        Compares INSTANTS. Sources write local offsets -- 80 of the 93 dated
        rows in one real ledger carry "-07:00" -- while now_iso arrives as UTC,
        and text order puts "2026-09-09T17:30:00-07:00" before
        "2026-09-09T18:00:00+00:00" though it starts six hours later. Measured
        on that ledger at 11:00 PDT: 32 rows expired by text, 22 by instant,
        and the ten it invented were all that afternoon's Bay Area events. They
        are also exactly the ones the last-call sweep exists for, since it
        skips anything already expired.
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE events SET state='expired' WHERE start != '' "
                "AND datetime(start) < datetime(?, ?) "
                "AND state NOT IN ('expired','registered','saved','dismissed',"
                "'sold_out')",
                (now_iso, f"-{grace_hours} hours"))
        return cur.rowcount

    def get_cached_score(self, event_uid: str, model_tag: str) -> Score | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT fit, access_value, cost, reason, method FROM score_cache "
                "WHERE event_uid=? AND model_tag=?", (event_uid, model_tag)).fetchone()
        # Named, like _row_to_event: with positional indices, reordering the
        # SELECT would silently feed cost into access_value, and every axis is
        # an int so nothing would notice while rank quietly changed.
        return Score(fit=row["fit"], access_value=row["access_value"],
                     cost=row["cost"], reason=row["reason"],
                     method=row["method"]) if row else None

    def put_cached_score(self, event_uid: str, model_tag: str, score: Score) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO score_cache(event_uid, model_tag, fit, access_value, cost,"
                " reason, method, scored_at) VALUES(?,?,?,?,?,?,?,?)"
                " ON CONFLICT(event_uid, model_tag) DO UPDATE SET fit=excluded.fit,"
                " access_value=excluded.access_value, cost=excluded.cost,"
                " reason=excluded.reason, method=excluded.method,"
                " scored_at=excluded.scored_at",
                (event_uid, model_tag, score.fit, score.access_value, score.cost,
                 score.reason, score.method, _now()))

    # -- text mirror, for the cloud ledger ---------------------------------
    # SQLite is a binary blob: committing the .db itself to a data branch stores
    # a whole new copy every run and can never be text-merged, so two runs
    # racing produce an unresolvable conflict. A sorted plain-text file avoids
    # that: rows are written one JSON object per line, sorted by key, so a run
    # that changes three events produces a three-line diff and a concurrent
    # push can be merged by git.
    def export_jsonl(self, events_path: Path, cache_path: Path) -> tuple[int, int]:
        counts = []
        for path, table, key in ((events_path, "events", "event_uid"),
                                 (cache_path, "score_cache", "event_uid, model_tag")):
            held = _UNPUBLISHED_COLUMNS.get(table, frozenset())
            with self._lock:
                rows = self._conn.execute(
                    f"SELECT * FROM {table} ORDER BY {key}").fetchall()
            path.parent.mkdir(parents=True, exist_ok=True)
            # newline="\n" explicitly: the default on Windows writes CRLF, and a
            # ledger committed from here would then show every line as changed
            # when a Linux runner rewrites it.
            with path.open("w", encoding="utf-8", newline="\n") as handle:
                for row in rows:
                    record = {k: v for k, v in dict(row).items() if k not in held}
                    if table == "events":
                        record["canonical_url"] = canon_url(record["canonical_url"])
                    if table == "events" and (
                            record.get("source_kind") == "gmail_label"
                            or str(record.get("source", "")).startswith("gmail:")):
                        record["source"] = "gmail_label"
                    handle.write(json.dumps(record, ensure_ascii=False,
                                            sort_keys=True) + "\n")
            counts.append(len(rows))
        return counts[0], counts[1]

    def import_jsonl(self, events_path: Path, cache_path: Path) -> tuple[int, int]:
        """Load a text mirror. Union semantics, never a replace.

        An existing row wins over the file, so importing can only ADD, with one
        exception: the four sticky timestamps merge column-wise via
        _STICKY_MERGE (three keep the earlier non-empty value, one the later).
        Whole-row precedence alone would let a side that had not yet reported
        an event overwrite the side that had, and the merge would then hand
        back a URL as unreported and mail it twice.

        Rows are matched by event_uid only, never by the pipeline's notion of
        the same real event: two branches that each discover one event through
        a different source inside the same race window will each have mailed
        it, and both rows survive the merge as separately reported. Accepted,
        because that needs genuinely concurrent runs and its cost is a
        duplicate send rather than the silent miss the rest of this ledger
        exists to prevent.
        """
        counts = []
        for path, table in ((events_path, "events"), (cache_path, "score_cache")):
            loaded = 0
            # `with self._conn` is the only self-managed transaction in this
            # class; every other write defers to an external save(). Deliberate:
            # a half-applied import is worse than none, so one malformed line
            # rolls the whole file back rather than leaving a partial ledger.
            if path.exists():
                with self._lock, self._conn:
                    for line in path.read_text(encoding="utf-8").splitlines():
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        cols = ",".join(row)
                        marks = ",".join("?" * len(row))
                        self._conn.execute(
                            f"INSERT INTO {table}({cols}) VALUES({marks}) "
                            + (_STICKY_MERGE if table == "events"
                               else "ON CONFLICT DO NOTHING"),
                            tuple(row.values()))
                        loaded += 1
            counts.append(loaded)
        # The same backfill __init__ does for a local ledger, because the cloud
        # entry point never takes that path: it imports a JSONL mirror into a
        # FRESH database, which already has the column, so rows written by an
        # older version arrive with cleared_floor_at empty. Without this, the
        # first cloud run after this column landed would re-send every event it
        # had ever alerted on.
        with self._lock, self._conn:
            self._conn.execute("UPDATE events SET cleared_floor_at = alerted_at "
                               "WHERE alerted_at != '' AND cleared_floor_at = ''")
        return counts[0], counts[1]

    def save(self) -> None:
        with self._lock:
            self._conn.commit()

    # -- reads -------------------------------------------------------------
    def due_for_resweep(self, within_hours: int, now_iso: str) -> list[Event]:
        """Events closing inside the window that were mailed and left alone.

        See EventStore.due_for_resweep for the closing timestamp and for why
        all three of the other conditions are needed.
        """
        with self._lock:
            rows = self._conn.execute(
                # NULLIF, because these columns hold '' for absent rather than
                # NULL (see Event.__post_init__), and COALESCE alone would
                # take an empty deadline as a present value and shadow start.
                # Referring to the result alias from WHERE is a SQLite
                # extension, not standard SQL; it is used here to keep the
                # expression written once, and would need expanding inline on
                # any other engine.
                f"SELECT *, {_CLOSES_AT} AS closes_at FROM events "
                "WHERE state IN ('new','seen') AND closes_at IS NOT NULL "
                "AND swept_at = '' AND alerted_at != '' "
                # datetime() on BOTH bounds. The upper one always had it; the
                # lower one compared text and so read a same-day afternoon
                # event written in -07:00 as already past, which is precisely
                # the event this query exists to catch.
                "AND datetime(closes_at) > datetime(?) "
                "AND datetime(closes_at) <= datetime(?, ?) "
                "ORDER BY datetime(closes_at) ASC",
                (now_iso, now_iso, f"+{within_hours} hours")).fetchall()
        return [_row_to_event(r) for r in rows]

    def mark_swept(self, event_uids: list[str]) -> None:
        """Record that a closing-soon reminder went out. Idempotent.

        Separate from mark_alerted: that one is about the digest a reader has
        already seen, this one about the single nudge they get before the door
        closes.
        """
        now = _now()
        with self._lock:
            self._conn.executemany(
                "UPDATE events SET swept_at=? WHERE event_uid=? AND swept_at=''",
                [(now, uid) for uid in event_uids])

    def counts_by_state(self) -> dict[str, int]:
        with self._lock:
            return {r[0]: r[1] for r in self._conn.execute(
                "SELECT state, COUNT(*) FROM events GROUP BY state")}
