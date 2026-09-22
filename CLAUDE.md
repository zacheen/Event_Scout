# Event Scout project instructions

`README.md` explains what the project does. This file covers working on it.

## Running it

- `python local_run.py --dry-run` is the default. It sends no mail and writes none of
  the sticky ledger columns, so repeating it costs nothing.
- A run with NO flags sends email and permanently sets `alerted_at`, `cleared_floor_at`
  and `swept_at`. Ask before starting one.
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
- Luma rate-limits. Three full fetches within a few minutes earns `HTTP Error 429` on
  the four luma sources, which surfaces as `check.py` failures that are not regressions.

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
