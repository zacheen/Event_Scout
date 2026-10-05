"""Cloud entry point. One pass, always live, no interactive flags.

`local_run.py` is the counterpart for this machine. The split exists because
the two environments differ in ways a flag cannot paper over.

                     local_run.py                run.py (cloud)
  ledger             local_events.db, kept       imported from a separate private
                     across runs on this disk    repository, exported back after
  scorer             codex CLI, no API key       OpenAI API; codex does not exist
                     needed                      on a GitHub runner
  extraction         schema.org, then a model    schema.org only, unless
                     for the rest                OPENAI_API_KEY is set
  email              opt-in via --dry-run        always sends
  reporting          --all shows everything      new events only

The ledger crosses runs as JSONL, not as the .db file. SQLite is a binary blob:
committing it stores a fresh copy every run and two runs racing produce a
conflict git cannot resolve. The text mirror diffs by line and merges.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from eventscout.config import build_runtime, load_channels, load_settings
from eventscout.notifier import EmailNotifier
from eventscout.pipeline import AllScoringFailedError, AllSourcesFailedError, run
from eventscout.store import SqliteEventStore

# Private data-repository checkout in CI; never publish this directory.
LEDGER_DIR = Path(os.getenv("LEDGER_DIR", ROOT / "cloud_data"))
EVENTS_JSONL = LEDGER_DIR / "events.jsonl"
CACHE_JSONL = LEDGER_DIR / "score_cache.jsonl"
# Rebuilt from the JSONL every run, so it is scratch and never committed.
DB = ROOT / "cloud_events.db"


def main() -> int:
    settings = load_settings()
    missing = [name for name, value in
               (("GMAIL_USER", settings.gmail_user),
                ("GMAIL_APP_PASSWORD", settings.gmail_password),
                ("MAIL_TO", settings.mail_to)) if not value]
    if missing:
        # Fail loudly rather than degrading to a print nobody reads. A cloud run
        # that silently skips the email is a green tick and no digest, which is
        # the worst possible combination.
        print(f"ERROR: missing secret(s): {', '.join(missing)}")
        return 1
    if not settings.openai_api_key:
        print("NOTICE: OPENAI_API_KEY is not set. The codex CLI does not exist on "
              "a runner, so scoring falls back to keywords and only the schema.org "
              "half of date extraction runs. Expect weaker ranking than a local run.")

    if DB.exists():
        DB.unlink()   # always rebuild from the committed text mirror
    store = SqliteEventStore(DB)
    seen, cached = store.import_jsonl(EVENTS_JSONL, CACHE_JSONL)
    print(f"ledger imported: {seen} events, {cached} cached scores "
          f"from {LEDGER_DIR}")

    sources, geo, extractor = build_runtime(settings, load_channels())
    notifier = EmailNotifier(settings.gmail_user, settings.gmail_password,
                             settings.mail_to,
                             settings.display_timezone)

    failed = False
    try:
        run(sources, store, geo, settings, notifier, dry_run=False,
            extractor=extractor)
    except AllSourcesFailedError as exc:
        # The workflow's only health signal is this exit status, and its alert
        # job keys on `if: failure()`, so a total outage has to be non-zero or
        # it reads as a quiet week. Caught, rather than left to propagate, so
        # the failure is a GitHub annotation the run summary shows instead of
        # a traceback buried in the job log (the finally below runs either way).
        print(f"::error::Event Scout found no usable source. {exc}")
        failed = True
    except AllScoringFailedError as exc:
        # Separate handler, not folded into AllSourcesFailedError's tuple —
        # see AllScoringFailedError's docstring for why conflating the two
        # would misdirect the reader.
        print(f"::error::Event Scout scored nothing. {exc}")
        failed = True
    finally:
        # Export even on failure: events already scored must not be re-scored,
        # and anything already emailed must not be emailed again next run.
        events, scores = store.export_jsonl(EVENTS_JSONL, CACHE_JSONL)
        store.close()
        print(f"ledger exported: {events} events, {scores} cached scores")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
