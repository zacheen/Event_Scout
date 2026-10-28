"""Run the real pipeline locally and judge it from the result.

Each line below runs as
`conda run --no-capture-output -n ML python local_run.py ...`. Without that
flag conda re-prints the output through the console codepage, and the Chinese
in the score reasons raises UnicodeEncodeError. The run itself has completed
by then, so it looks like a crash after the ledger has already moved.

    local_run.py                        # fetch, score, record as told and
                                        # open the results page. NO email
    local_run.py --email --no-html      # EMAIL instead, the scheduled run
    local_run.py --email                # EMAIL and open the page
    local_run.py --dry-run              # print the digest, record nothing,
                                        # open no page
    local_run.py --email --show-digest  # EMAIL, and also print the body
                                        # exactly as sent
    local_run.py --reset                # forget the ledger, first run again.
                                        # What that mails is config.yaml
                                        # first_run.mode; report_everything
                                        # sends the whole backlog in one digest
    local_run.py --mark registered https://luma.com/x
                                        # and: saved, dismissed. Stops the
                                        # last-call reminder for that event,
                                        # and nothing else
    local_run.py --all                  # report EVERY in-scope event,
                                        # ignoring "already seen"
    local_run.py --no-extract           # skip page extraction entirely: the
                                        # free JSON-LD path as well as any
                                        # model call

`--all` exists because judging output quality is impossible once a run has
actually reported something. Everything it reported is then marked, so later
runs correctly report zero new ones and there is nothing left to look at. Use
--all to inspect, not on a schedule.

The results page counts as delivery. A run without --email still records its
events as told, because the page lists exactly the rows carrying that record,
and a page-only run that recorded nothing would show nothing and offer the
same events again next run. --no-email --no-html delivers nowhere, so it runs
as a dry run.

A --dry-run is NOT read-only. It upserts every event it saw, lets expire_past
flip started ones to `expired`, and keeps the scores it already paid the model
for. What it withholds are the three records that mean "you were told", namely
cleared_floor_at, alerted_at and swept_at. Only the first of those decides
whether an event still counts as new, so a preview costs you no coverage even
though the file grows. That split is not a convenience; writing "reported"
before the send would turn one refused SMTP call into permanent silence, since
the event would then read as reported while alerted_at stayed empty and the
last-call sweep skipped it too.

The ledger is a single SQLite file (local_events.db, gitignored), so a local
run never touches shared state. --reset deletes it, and with it the record of
what was already mailed, even when --dry-run is passed too.

Secrets come from .env (GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO). Without
them --email is dropped with a warning and the run carries on as if it had
been --no-email.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Literal

try:
    from dotenv import load_dotenv
except ImportError:  # optional; plain environment variables still work
    load_dotenv = None

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from eventscout.config import build_runtime, load_channels, load_settings
from eventscout.notifier import ConsoleNotifier, EmailNotifier, PageOnlyNotifier
from eventscout.pipeline import AllScoringFailedError, AllSourcesFailedError, run
from eventscout.models import State
from eventscout.store import SqliteEventStore
from eventscout.urls import canon_url
import results_page

DB = ROOT / "local_events.db"
RESULTS_PAGE = ROOT / "results_page.py"
# WMI rather than Popen, so the page outlives this process when a scheduled
# task's job object ends. ShowWindow=0 keeps Windows Terminal from opening a
# console whose closing would kill the page. The command line arrives through
# the environment to avoid a second quoting layer.
_WMI_LAUNCH = (
    "$si = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly "
    "-Property @{ ShowWindow = [uint16]0 }; "
    "$r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments "
    "@{ CommandLine = $env:EVENTSCOUT_PAGE_CMD; CurrentDirectory = $env:EVENTSCOUT_PAGE_CWD; "
    "ProcessStartupInformation = $si }; "
    "exit $r.ReturnValue"
)


def _parse(argv: list[str]) -> argparse.Namespace:
    # parse_args, not a set of strings: an unknown flag must stop the run,
    # because a typo such as --dryrun used to fall through to a live run that
    # mails the digest and sets the three sticky ledger columns for good.
    # allow_abbrev=False because argparse otherwise reads --re as --reset,
    # which deletes the ledger.
    parser = argparse.ArgumentParser(
        prog="local_run.py", description="Run the real pipeline locally.",
        allow_abbrev=False)
    parser.add_argument("--dry-run", action="store_true")
    # Off by default, so a run by hand never mails. The scheduled run passes it.
    parser.add_argument("--email", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--html", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--all", action="store_true", dest="report_all")
    parser.add_argument("--show-digest", action="store_true")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--no-extract", action="store_true")
    parser.add_argument("--mark", nargs=2, metavar=("STATE", "URL"))
    return parser.parse_args(argv)


def _mark(state_name: str, raw_url: str) -> int:
    """Record what you did about one event, by URL.

    The ledger has carried SAVED / REGISTERED / DISMISSED since the beginning
    and nothing could ever write them, so "have you dealt with this" had no
    answer and the last-call reminder had to assume no. This is the smallest
    thing that makes the question answerable; a reply-to-the-digest flow would
    be friendlier and is a separate piece of work.
    """
    try:
        state = State(state_name)
        # Inside the try: a malformed URL such as http://[bad raises ValueError.
        url = canon_url(raw_url)
    except ValueError:
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


def _delivery(dry_run: bool, email: bool, html: bool,
              have_creds: bool) -> Literal["email", "page", "dry"]:
    """The one channel this run delivers through.

    Missing credentials drop --email rather than the run, so a run that asked
    for both still produces its page.
    """
    if dry_run:
        return "dry"
    if email and have_creds:
        return "email"
    return "page" if html else "dry"


def _open_results_page() -> None:
    """Never raises: the ledger is already saved, so a page that fails to open
    must not turn the run into a failure."""
    pythonw = Path(sys.executable).with_name("pythonw.exe")
    if not pythonw.exists():
        print(f"results page not opened, {pythonw} not found")
        return
    env = {**os.environ, "EVENTSCOUT_PAGE_CMD": f'"{pythonw}" "{RESULTS_PAGE}"',
           "EVENTSCOUT_PAGE_CWD": str(ROOT)}
    try:
        proc = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive",
                               "-Command", _WMI_LAUNCH],
                              env=env, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"results page not opened: {exc}")
        return
    if proc.returncode:
        print(f"results page not opened, Win32_Process.Create returned "
              f"{proc.returncode} {proc.stderr.strip()}")
    else:
        print(f"results page launched at {results_page.URL}")


def main() -> int:
    args = _parse(sys.argv[1:])
    dry_run = args.dry_run
    report_all = args.report_all
    show_digest = args.show_digest
    if load_dotenv:
        load_dotenv(ROOT / ".env")

    if args.mark:
        return _mark(*args.mark)

    if args.reset and DB.exists():
        DB.unlink()
        print(f"removed {DB.name}; this run will seed from scratch")

    settings = load_settings()
    # Same assembly as run.py, so config.yaml's request_timeout and user_agent
    # cannot be honoured in one entry point and ignored in the other.
    sources, geo, extractor = build_runtime(settings, load_channels())
    if args.no_extract:
        extractor = None
    store = SqliteEventStore(DB)

    have_creds = all((settings.gmail_user, settings.gmail_password, settings.mail_to))
    if args.email and not have_creds and not dry_run:
        print("GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO not all set in .env -"
              " not emailing, as if --no-email had been passed.")
    delivery = _delivery(dry_run, args.email, args.html, have_creds)
    dry_run = delivery == "dry"
    if dry_run:
        notifier = ConsoleNotifier(settings.display_timezone,
                                   long_running_days=settings.long_running_days)
    elif delivery == "email":
        notifier = EmailNotifier(settings.gmail_user, settings.gmail_password,
                                 settings.mail_to,
                                 settings.display_timezone,
                                 echo=show_digest,
                                 long_running_days=settings.long_running_days)
    else:
        notifier = PageOnlyNotifier(settings.display_timezone,
                                    long_running_days=settings.long_running_days)

    print("=" * 68)
    if dry_run:
        print("DRY RUN - NO EMAIL WILL BE SENT. The digest is printed below.")
        if args.dry_run:
            print("Reason: --dry-run was passed.")
        else:
            print("Reason: no email and --no-html deliver nowhere, so nothing is")
            print("recorded as told.")
    elif delivery == "email":
        print(f"LIVE RUN - an email WILL be sent to {settings.mail_to}")
    else:
        print("LIVE RUN - NO EMAIL. Events are recorded as told and shown on the")
        print("results page. Pass --email to mail them.")
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
    # Opened even when this run found nothing, since the page also holds earlier
    # todos and saved events. Never after a dry run, which recorded nothing it
    # shows. A failed run still opens it: AllScoringFailedError is raised after
    # the last calls were delivered, and those are only on the page.
    if args.html and not dry_run:
        _open_results_page()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
