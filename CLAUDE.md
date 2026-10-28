# Event Scout project instructions

`README.md` explains what the project does. This file covers working on it.

## Running it

- When asked to run local run, run `python local_run.py` with no flags and do not ask first. That run mails nothing, prints the digest, opens the results page and permanently sets `alerted_at`, `cleared_floor_at` and `swept_at`, because the page counts as delivery and lists exactly the rows carrying those columns. A dry run leaves every event it found undelivered, so choosing one by default holds back events the reader asked for.
- Add `--email` only when asked to mail. `--email --show-digest` also prints the mail body after the send, the same text that went out, so the run can be checked from its own output. The scheduled run uses `--email --no-html --show-digest` through `scheduled_check.py`, since it starts at 2 a.m. with nobody at the screen.
- Use `--dry-run` only when explicitly asked for it, or to inspect the pipeline after a code change. It sends no mail, opens no page and writes none of the sticky ledger columns, so repeating it costs nothing.
- A dry run still grows the ledger, because `upsert` records whatever the run saw. A
  rising row count is normal. The invariant worth checking is that those three columns
  did not move.
- `--reset` deletes `local_events.db`, destroying the record of what was already
  emailed. `--dry-run` does NOT prevent the delete. Ask before running it.
- Redirect the output to a file and read it whole. Piping through `tail` has produced
  wrong conclusions here more than once, because the funnel sits at the top.
- Run it as `conda run --no-capture-output -n ML python ...`. Without that flag conda
  buffers the child's output and re-prints it through the console codepage, which on a
  cp950 machine raises `UnicodeEncodeError` on the Chinese in every score reason. The
  run itself completes, so the failure looks like a crash while the ledger has already
  moved. Same flag for `check.py`.
- Luma rate-limits by how many requests arrive over a few minutes, not by the gap between them. A 429 blocks the whole luma.com host, calendar pages included, and carries no `Retry-After`. Measured 2026-10-09, so re-measure before leaning on the numbers.
  - About 103 requests inside 6 minutes passed. About 163 inside 2.2 minutes earned `HTTP Error 429`, which lifted 4 to 8 minutes later.
  - One full fetch makes about 54 luma.com requests. Three full fetches run back to back, 162 requests over 16 minutes, all passed.
  - Runs that overlap are what reach it, such as a manual run or `check.py` started beside the scheduled one. `HostRateLimiter` caps each host at 60 requests per 6 minutes, but only inside one process. A 429 then surfaces as `check.py` failures that are not regressions.

## Verifying a change

- `python check.py` is the whole suite, and `--offline` skips the part that hits live
  sources.
- A new test is not trusted until a mutation proves it can fail. Break the line the test
  covers, confirm the expected assertion fails, then restore.
- When mutation testing, read the exit code as well as the `[FAIL]` count. A mutation
  that makes `check.py` raise reports zero `[FAIL]` lines while still being caught, so
  counting only failures makes a caught mutation look like a surviving one.

## Comments

The long comments here are load-bearing. Most carry a measured number that is the reason
a line exists, so check what a comment is holding up before shortening it.
`_row_to_event` piping every timestamp back through `iso_or_empty` is the example. It
reads as redundant, and its comment is what stops someone deleting it and breaking every
read of a pre-gate row.
