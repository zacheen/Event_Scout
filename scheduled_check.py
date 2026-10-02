"""Fixed entry point for the scheduled daily run, so it never improvises a command.

    conda run --no-capture-output -n ML python scheduled_check.py snapshot
        read-only. Prints the clock, the ledger counts and the config floor.
    conda run --no-capture-output -n ML python scheduled_check.py run
        snapshot, then `local_run.py --show-digest` (SENDS MAIL), then the
        snapshot again and the rows this run wrote, each LAST CALL checked
        against the reminder rules.

The scheduled task runs under an allowlist that matches command strings
verbatim, and a prompt nobody answers stalls the run. Anything the check needs
from the ledger is therefore printed here rather than queried ad hoc, and the
child's output goes to a fixed log path rather than a per-session scratchpad,
which no allowlist rule could name.
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from eventscout.config import load_settings

DB = ROOT / "local_events.db"
LOG = ROOT / ".private" / "scheduled_run.log"
STICKY = ("alerted_at", "swept_at", "cleared_floor_at")


def _connect() -> sqlite3.Connection:
    # Read-only, so a snapshot can never create or migrate the ledger the way
    # SqliteEventStore's constructor does.
    conn = sqlite3.connect(DB.as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _snapshot(label: str) -> dict[str, int]:
    settings = load_settings()
    with _connect() as conn:
        columns = [r["name"] for r in conn.execute("PRAGMA table_info(events)")]
        # NULLIF because '' is how these columns spell absent, and bare
        # count(col) would count it.
        row = conn.execute(
            "SELECT count(*) AS total, count(NULLIF(alerted_at, '')) AS alerted_at, "
            "count(NULLIF(swept_at, '')) AS swept_at, "
            "count(NULLIF(cleared_floor_at, '')) AS cleared_floor_at, "
            "max(alerted_at) AS max_alerted, max(swept_at) AS max_swept FROM events"
        ).fetchone()
    print(f"=== SNAPSHOT {label} ===")
    print(f"utc_now: {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    print(f"events columns: {', '.join(columns)}")
    print(f"rows:{row['total']}")
    for col in STICKY:
        print(f"non-empty {col}: {row[col]}")
    print(f"max alerted_at: {row['max_alerted'] or '(none)'}")
    print(f"max swept_at: {row['max_swept'] or '(none)'}")
    print(f"config digest_min_rank: {settings.digest_min_rank}")
    print(f"config urgent_section_within_hours: {settings.urgent_hours}")
    print(f"config resweep_min_gap_hours: {settings.resweep_min_gap_hours}")
    return {k: row[k] for k in ("total", *STICKY)}


def _written_since(start: datetime) -> None:
    settings = load_settings()
    since = start.isoformat(timespec="seconds")
    with _connect() as conn:
        # datetime() on both sides, because these timestamps keep whatever
        # offset they were written with, and text comparison across offsets
        # is wrong.
        alerted = conn.execute(
            "SELECT title, fit, access_value, alerted_at FROM events "
            "WHERE alerted_at != '' AND datetime(alerted_at) >= datetime(?) "
            "ORDER BY title", (since,)).fetchall()
        cleared = conn.execute(
            "SELECT count(*) FROM events WHERE cleared_floor_at != '' "
            "AND datetime(cleared_floor_at) >= datetime(?)", (since,)).fetchone()[0]
        swept = conn.execute(
            "SELECT title, fit, access_value, alerted_at, swept_at, "
            "COALESCE(NULLIF(rsvp_deadline, ''), NULLIF(start, '')) AS closes_at "
            "FROM events WHERE swept_at != '' AND datetime(swept_at) >= datetime(?) "
            "ORDER BY datetime(closes_at)", (since,)).fetchall()

    print(f"=== WRITTEN THIS RUN (since {since}) ===")
    print(f"alerted_at written: {len(alerted)}")
    for r in alerted:
        print(f"  rank {_rank(r)}  {r['title']}")
    print(f"cleared_floor_at written: {cleared}")
    print(f"swept_at written: {len(swept)}")
    window = timedelta(hours=settings.urgent_hours)
    gap = timedelta(hours=settings.resweep_min_gap_hours)
    for r in swept:
        first_sent = datetime.fromisoformat(r["alerted_at"])
        closes = datetime.fromisoformat(r["closes_at"])
        rank = _rank(r)
        checks = {
            "first send before window": first_sent < closes - window,
            f"first send >= {settings.resweep_min_gap_hours}h ago":
                first_sent <= start - gap,
            f"rank >= {settings.digest_min_rank}":
                rank is not None and rank >= settings.digest_min_rank,
        }
        verdict = "OK" if all(checks.values()) else "VIOLATION"
        print(f"  [{verdict}] {r['title']}")
        print(f"    closes_at {r['closes_at']}  window opens "
              f"{(closes - window).isoformat()}  first sent {r['alerted_at']}  rank {rank}")
        for name, passed in checks.items():
            print(f"    {'pass' if passed else 'FAIL'}: {name}")


def _rank(row: sqlite3.Row) -> int | None:
    # Mirrors Score.rank, fit x access with cost left out on purpose.
    if row["fit"] is None or row["access_value"] is None:
        return None
    return row["fit"] * row["access_value"]


def _run() -> int:
    start = datetime.now(timezone.utc).replace(microsecond=0)
    print(f"run start utc: {start.isoformat()}")
    before = _snapshot("BEFORE")
    LOG.parent.mkdir(exist_ok=True)
    # UTF-8 for the child too, since a cp950 console encoding raises on the
    # Chinese in score reasons after the ledger has already moved.
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
    with LOG.open("wb") as log:
        code = subprocess.call(
            [sys.executable, str(ROOT / "local_run.py"), "--show-digest"],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    print(f"=== local_run.py --show-digest exit code: {code} ===")
    print(f"full output (read it whole with the Read tool): {LOG}")
    after = _snapshot("AFTER")
    print("=== DELTA (after - before) ===")
    for key in before:
        print(f"{key}: {after[key] - before[key]:+d}")
    _written_since(start)
    return code


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    if mode == "snapshot":
        _snapshot("NOW")
        return 0
    if mode == "run":
        return _run()
    print("usage: scheduled_check.py snapshot | run", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
