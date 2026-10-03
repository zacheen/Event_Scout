# Scheduled run rules

This file is the only source of rules for the scheduled task `event-scout-daily-run`, whose SKILL.md only tells the model to read it. The report language is set by the paragraph appended to the end of SKILL.md, and this file does not set one. How the stub is deployed is in README.md.

Run the Event Scout local run once in D:\dont_move\git_save\Event_Scout, which really sends mail, then check the output for logic violations or new issues.

## Rule sources

1. The repo's CLAUDE.md describes running and testing Event Scout by hand. Run none of the commands it names, such as `check.py` or a bare `local_run.py`, because the closed list below is the only set allowed, and `scheduled_check.py run` already applies the flags CLAUDE.md asks for and sends the output to a fixed file. An `HTTP Error 429` from the Luma sources is rate limiting, not a regression.
2. Before running, read Pending_fix.md and Known_concern.md at the repo root in full. Both are registers of known defects, kept to suppress repeat reports. Never report anything already registered unless its measured facts changed (fixed, worse, or failing a different way), and in that case name the entry number and what changed.

## Allowed commands (closed list)

Only the three commands below may run during a scheduled run. Commands 1 and 2 must not change by a single character, and in command 3 only the text inside the `-Message` quotes may change. None of them may gain `&&`, `;`, a pipe or a redirect, or be split or rewritten into another form. Any command outside the list stops at a permission prompt, nobody is there to approve it during a scheduled run, and the whole run hangs. To look at any file, use the Read tool and never assemble a command to read it. Everything about the ledger or the config is in the output of command 1, so do not write SQL or `python -c` yourself.

1. `conda run --no-capture-output -n ML python "D:\dont_move\git_save\Event_Scout\scheduled_check.py" run`
2. `conda run --no-capture-output -n ML python "D:\dont_move\git_save\Event_Scout\scheduled_check.py" snapshot`
3. `powershell.exe -NoProfile -File "D:\dont_move\git_save\Daily_Task\shared\notify.ps1" -Message "Event Scout 今日執行發現問題 / 請看排程任務報告" -Title "Event Scout"`

## Running

- Run command 1 with the Bash tool and `run_in_background` set to true. It takes about 11 minutes, which is longer than the Bash tool's 10-minute foreground limit. Do not poll or sleep while it runs, since any command off the closed list stops at a permission prompt. Wait for the completion notification, then read the output file it names with the Read tool. In order, the command records the start time in UTC, prints the ledger snapshot before the run, runs `local_run.py --show-digest` (which really sends mail), prints the snapshot after the run and the differences, lists the rows this run wrote to `alerted_at` and `swept_at`, and prints how many `cleared_floor_at` values it wrote.
- The full output of `local_run.py` is in `D:\dont_move\git_save\Event_Scout\.private\scheduled_run.log`. Read all of it with the Read tool, not just the end.
- Command 2 is read-only and prints only the current ledger counts and config thresholds. Use it only when you need to look again.

## What to check

- Whether command 1 prints `local_run.py --show-digest exit code: 0`, and whether the log contains `EMAIL SENT`.
- Whether every FUNNEL stage adds up, and whether the `N first seen this run` after `never emailed before` is plausible.
- The printed digest is everything after `--- SENT ---`. Check each of the following.
  - The section order is TOP PICKS, OTHER PICKS, with LAST CALL at the end.
  - TOP PICKS and OTHER PICKS are sorted by fit×access from high to low, with ties broken by start time. LAST CALL is sorted by start time.
  - The "N closing soon" in the subject equals the number of TOP PICKS plus LAST CALL.
  - A timed event must not appear once it has started, whether or not it lists an end time. Any line whose when is marked `(already started)` and is not an `(all day)` event is a problem. An all-day event may appear as long as its day, or its last day for a multi-day event, has not ended.
  - Do not pass an event clearly outside the Bay Area as a normal result. If it is the kind Known_concern.md #4 describes, where the source itself has the wrong city, follow the register rules and report it as a new sighting for that entry, with the date, source and URL.
- Check the ledger against the `WRITTEN THIS RUN` block in the output of command 1. `alerted_at written` must equal the number of new events, and `swept_at written` must equal the number of LAST CALL events. Each LAST CALL event lists three checks below it, that its first send came before its 72-hour window opened, that the first send was at least 24 hours ago, and that its fit×access is no lower than `config digest_min_rank`. Any check that reads `FAIL`, which also marks the line `[VIOLATION]`, is a problem. If the FUNNEL "closing soon" line says "N more now below the floor", N events got no reminder because their score fell below the floor. That is normal behaviour, not a problem.

## Forbidden

- Do not change any code, config or register, and do not stage or commit.
- Do not write findings into Pending_fix.md or Known_concern.md yourself. List them in the report and let the user decide.

## Report

The first sentence of the report is the conclusion, meaning how many new events and how many LAST CALL events were sent and whether anything is wrong. List each newly found problem as a numbered item with its evidence, its cost and where you recommend it go, which is fix now, Pending_fix.md, Known_concern.md, or not yet worth registering.

## Notification

Only when the run fails or finds a new problem, run command 3 **with the Bash tool** to send a toast. Copy it whole and only replace the text inside the `-Message` quotes with one short line saying what the problem is. The details go in the report.
