"""Run the real pipeline locally and judge it from the result.

    conda run -n ML python local_run.py            # fetch, score, EMAIL
    conda run -n ML python local_run.py --dry-run  # same, printed instead of sent
    conda run -n ML python local_run.py --show-digest  # EMAIL, and also print
                                                  # the body exactly as sent
    conda run -n ML python local_run.py --reset    # forget the ledger, first run
                                                  # again. What that mails is
                                                  # config.yaml first_run.mode;
                                                  # report_everything sends the
                                                  # whole backlog in one digest
    conda run -n ML python local_run.py --mark registered https://luma.com/x
                                                  # and: saved, dismissed. Stops
                                                  # the last-call reminder for
                                                  # that event, and nothing else
    conda run -n ML python local_run.py --all      # report EVERY in-scope event,
                                                  # ignoring "already seen"
    conda run -n ML python local_run.py --no-extract   # skip page extraction
                                                  # entirely: the free JSON-LD
                                                  # path as well as any model call

`--all` exists because judging output quality is impossible once a run has
actually reported something. Everything it reported is then marked, so later
runs correctly report zero new ones and there is nothing left to look at. Use
--all to inspect, not on a schedule.

A --dry-run is NOT read-only, which the ledger file size makes obvious and
this paragraph exists so nobody has to discover it that way. It upserts every
event it saw, lets expire_past flip started ones to `expired`, and keeps the
scores it already paid the model for. What it withholds are the three records
that mean "you were told", namely cleared_floor_at, alerted_at and swept_at.
Only the first of those decides whether an event still counts as new, so a
preview costs you no coverage even though the file grows. That split is not a
convenience; writing "reported" before the send would turn one refused SMTP
call into permanent silence, since the event would then read as reported while
alerted_at stayed empty and the last-call sweep skipped it too.

There is no git plumbing here: the ledger is a
single SQLite file (local_events.db, gitignored), so a local run never touches
shared state and can be reset freely.

Secrets come from .env (GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO). Without
them the run still works, it just falls back to printing the digest.
"""
from __future__ import annotations

import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # optional; plain environment variables still work
    load_dotenv = None

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from eventscout.config import build_runtime, load_channels, load_settings
from eventscout.notifier import ConsoleNotifier, EmailNotifier
from eventscout.pipeline import AllScoringFailedError, AllSourcesFailedError, run
from eventscout.models import State
from eventscout.store import SqliteEventStore
from eventscout.urls import canon_url

DB = ROOT / "local_events.db"


def _mark(argv: list[str]) -> int:
    """Record what you did about one event, by URL.

    The ledger has carried SAVED / REGISTERED / DISMISSED since the beginning
    and nothing could ever write them, so "have you dealt with this" had no
    answer and the last-call reminder had to assume no. This is the smallest
    thing that makes the question answerable; a reply-to-the-digest flow would
    be friendlier and is a separate piece of work.
    """
    try:
        state = State(argv[argv.index("--mark") + 1])
        url = canon_url(argv[argv.index("--mark") + 2])
    except (IndexError, ValueError):
        allowed = ", ".join(s.value for s in State if s.choosable)
        print(f"usage: local_run.py --mark <{allowed}> <event url>")
        return 2
    if not state.choosable:
        print(f"--mark takes a state you can choose; {state.value} is not one")
        return 2
    store = SqliteEventStore(DB)
    try:
        marked = store.mark_by_url(url, state)
        if not marked:
            print(f"nothing in the ledger for {url}")
            return 1
        store.save()
        print(f"marked {len(marked)} listing(s) of {marked[0][1][:60]!r} as {state.value}")
        print("it will not come back as a last call")
    finally:
        store.close()
    return 0


def main() -> int:
    args = set(sys.argv[1:])
    dry_run = "--dry-run" in args
    report_all = "--all" in args
    show_digest = "--show-digest" in args
    if load_dotenv:
        load_dotenv(ROOT / ".env")

    if "--mark" in args:
        return _mark(sys.argv[1:])

    if "--reset" in args and DB.exists():
        DB.unlink()
        print(f"removed {DB.name}; this run will seed from scratch")

    settings = load_settings()
    # Same assembly as run.py, so config.yaml's request_timeout and user_agent
    # cannot be honoured in one entry point and ignored in the other.
    sources, geo, extractor = build_runtime(settings, load_channels())
    if "--no-extract" in args:
        extractor = None
    store = SqliteEventStore(DB)

    have_creds = all((settings.gmail_user, settings.gmail_password, settings.mail_to))
    if dry_run or not have_creds:
        notifier = ConsoleNotifier(settings.display_timezone)
        if not have_creds and not dry_run:
            print("GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO not all set in .env -"
                  " printing the digest instead of sending it.")
        dry_run = True
    else:
        notifier = EmailNotifier(settings.gmail_user, settings.gmail_password,
                                 settings.mail_to,
                                 settings.display_timezone,
                                 echo=show_digest)

    print("=" * 68)
    if dry_run:
        print("DRY RUN - NO EMAIL WILL BE SENT. The digest is printed below.")
        if not have_creds:
            print("Reason: GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO are not all")
            print("set in .env, so there is nothing to send with.")
        else:
            print("Reason: --dry-run was passed.")
    else:
        print(f"LIVE RUN - an email WILL be sent to {settings.mail_to}")
    # Three states, not two. --no-extract drops the extractor entirely, while
    # a run with no model tier keeps it and loses only the prose half, and a
    # single truthiness test reported the first as the second.
    if extractor is None:
        extraction = "off (--no-extract)"
    else:
        extraction = "schema.org + prose" if extractor.prose_enabled else "schema.org only"
    print(f"{len(sources)} sources | ledger {DB.name} | extraction {extraction}")
    print("=" * 68)
    failed = False
    try:
        run(sources, store, geo, settings, notifier, dry_run=dry_run,
            report_all=report_all, extractor=extractor)
    except AllSourcesFailedError as exc:
        # Symmetrical with run.py on purpose. run() only grew this contract
        # for the cloud entry point's exit code, but leaving the local one
        # unhandled meant a total outage ended in a traceback rather than the
        # explained zero this project's funnel exists to give.
        print(f"ERROR: {exc}")
        failed = True
    except AllScoringFailedError as exc:
        print(f"ERROR: {exc}")
        failed = True
    finally:
        store.close()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
