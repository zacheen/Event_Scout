# Event Scout

Finds career-relevant events in the San Francisco Bay Area, ranks them against a profile, and mails a digest when there is something worth reporting.

Career fairs, employer info sessions, hackathons and campus recruiting events are announced across calendars, newsletters and mailing lists that share no format and no schedule. They also expire. An event you hear about the week after it ran is not late, it is gone. Event Scout reads those channels on a timer so that none of them has to be checked by hand.

## How it works

One pass runs the following stages, and every stage that filters reports how many items it dropped.

| Stage | What it does |
| --- | --- |
| Fetch | Each configured source returns whatever it currently lists |
| URL dedupe | Canonical URLs collapse, with tracking parameters stripped |
| Extraction | Items missing a date or location get one page fetch. Schema.org markup on the page is read first, and an LLM reads the page only when that markup yields no usable event and a model tier is available. Nothing is dropped here |
| Event dedupe | Same title plus the same start instant collapses across sources |
| Geo filter | A location that names a Bay Area city or region passes, and one that names a place elsewhere is dropped. Virtual events pass when `accept_virtual` is on |
| Horizon | Events already started, or further out than `lookahead_days`, are dropped |
| Keyword gate | Mailbox items must contain one term from `config.yaml` |
| Scoring | Each survivor gets a rank, and everything below the floor is withheld |
| Sold out | Events whose listing says they are full are withheld |
| Already reported | Events an earlier run already marked as reported are withheld, matched by URL or by title plus start |
| First run | While no event has ever been marked as reported, `first_run.mode` in `config.yaml` decides whether the whole backlog is mailed or only recently published items |
| Digest | One mail with up to four sections. Top picks closing soon, other picks, a below-the-floor section shown once on the first run, and last-call reminders for events mailed before |

A run with nothing new and no last-call reminder due sends no mail at all. That makes silence a signal, which is the whole reason the counts above matter. A source that quietly returns nothing looks exactly like a quiet week, so a source raises rather than returning an empty list when its fetch fails, and a source that can truncate its own read has to report what it could not reach. Two gaps remain. A page that loads but carries no events still returns an empty list, which is why `check.py` asserts lower bounds on counts. A Gmail entry with no credentials is skipped with a printed line rather than raised. Source failures fail the run as a whole only when every source fails.

## Sources

Four adapters cover every channel, so adding a site is usually a config entry rather than code.

- `jsonld` reads schema.org `Event` markup. Dates arrive as typed fields, which removes the largest correctness risk in the project, namely a model guessing a date or a timezone.
- `wordpress_rest` reads a site's `/wp-json` event post type.
- `newsletter` reads a beehiiv publication's public archive and harvests the event links out of each issue.
- `gmail_label` reads one Gmail label over IMAP with an app password. It opens All Mail read-only and searches it for the label, issues no command that changes the mailbox, and fetches bodies with `BODY.PEEK`, so nothing is marked as read. A Gmail rule decides what lands in the label, which means a new newsletter needs no code change at all.

`channels.yaml` holds the inventory. Entries are started only when their `status` is exactly `verified`, so a source that was probed but held back costs nothing.

## Scoring

Two axes, multiplied, giving `rank = fit * access_value` on a 1 to 100 scale.

`fit` is topical relevance to the profile. `access_value` asks whether attending creates a real pathway to a named employer. The second axis is what separates a generic resume workshop from the same workshop where one company reviews resumes for direct consideration.

Three tiers run in precedence order. The OpenAI API is used when a key is present, otherwise a local `codex` CLI, otherwise a deterministic keyword tier. Only the keyword tier is always available, so a missing model never stops a run. A model that is present but failing is not replaced by the keyword tier. The events it fails to score are dropped for that run, and when every event fails the run exits with an error.

Rank decides three things. Whether the event reaches the digest at all, whether it is marked P0, P1 or P2, and whether it earns a last-call reminder before its date.

## Running it

Python 3.13, and three dependencies.

```bash
pip install -r requirements.txt
cp .env.example .env
```

Fill in `.env`. Point `PROFILE_PATH` at a resume or profile file, ideally one kept outside this directory, since a file that never enters the repo cannot be committed from it. Then preview without sending anything.

```bash
python local_run.py --dry-run
```

A run with no flags sends mail. Other flags are documented in `local_run.py`, the useful ones being `--all` to re-report everything in scope when judging output quality, and `--reset` to forget the ledger.

Note that `--dry-run` is not read-only. It records every event it saw, but withholds the three timestamps that mean "you were told". Writing those before the mail is sent would turn one refused SMTP call into permanent silence.

Nothing is required to get a first result. Without credentials the digest prints to the console instead of being mailed, and says so in its first lines.

## Scheduled daily run

A Claude Code scheduled task, `event-scout-daily-run`, does the local run every day at 3 PM and checks its output. Its rules are in `SCHEDULED_RUN.md`, which the run reads afresh every time, so an edit there takes effect on the next run with no further step.

The app reads the task's prompt from `~/.claude/scheduled-tasks/event-scout-daily-run/SKILL.md`, which lies outside this repo and has no history. That prompt is only a stub pointing at `SCHEDULED_RUN.md`, and its source is `Scheduled_Tasks/event-scout-daily-run.md`. After editing the source, copy it into place with the deploy tool from the Daily_Task repo, which also appends the paragraph that sets the reply language. Run it from this repo's root, with Daily_Task checked out beside this repo.

```bash
conda run --no-capture-output -n ML python ../Daily_Task/shared/deploy_skills.py .
```

It prints `deployed` or `up to date`, and adding `--check` compares without writing. Never edit the deployed SKILL.md directly. The tool notices such an edit, prints the diff and refuses to overwrite it until it is rerun with `--replace-edited event-scout-daily-run`, which is also how the stub goes back after the task is recreated in the app.

## Configuration

| File | Holds |
| --- | --- |
| `channels.yaml` | The source inventory and the geography |
| `config.yaml` | Keywords, score thresholds, horizon, delivery |
| `.env` | Every secret, and which mailbox and labels to read |

Credentials and mailbox labels belong in `.env`, which is ignored by Git. Mailbox sources are named after their label and keep the sender's From header as the event's organizer, which scoring reads. Both stay in the local ledger and are stripped only when the cloud ledger is exported. The local database, profile and run logs remain private files. Share the tracked source files, never a ZIP of the entire working directory.

LLM scoring sends the configured profile and event text to the selected model provider, including mailbox text when those events are scored. A CLI invocation does not make inference local. Use the keyword tier if that transfer is unwanted.

`channels.yaml` is tuned for one reader, a Bay Area based student in US tech.

- The geo filter keeps an event whose location names one of the places under `geo.places`, which lists the cities within 50 miles of San Jose, Mountain View and Santa Clara, plus a few campuses. It matches names rather than distance, because no source exposes coordinates.
- `geo.accept_virtual` only adds events back. With it on, an online event that names no Bay Area city passes, and so does any event whose location says remote only. An event naming a Bay Area city without saying remote only is kept either way.
- A name that another state or country also uses is listed with its state, such as `dublin, ca`, so Dublin, Ireland is dropped.
- `geo.search_area` records the same 50 mile area for a future source whose query takes a centre and a distance. No verified source does, so nothing reads it yet.
- Retuning the area means editing `geo.places`, and retuning the sources means editing the rest of `channels.yaml`. Neither needs a code change.

## Cloud

`.github/workflows/scan.yml` runs the same pipeline hourly on GitHub Actions. `run.py` is the cloud entry point and `local_run.py` is its counterpart for a workstation. The local ledger is SQLite. Cloud state is JSONL on the `data` branch of a separate **private** GitHub repository; it is never pushed to this code repo.

Before enabling cloud runs, create that private repository and configure Actions secrets `LEDGER_REPOSITORY` (`owner/repository`) and `LEDGER_TOKEN` (a token with Contents read/write access to that repository). The existing mail credentials and `RESUME_TEXT` are also required; set `RESUME_TEXT` to the approved profile version. The workflow verifies repository visibility before fetching and again before saving. Missing configuration, a public destination or a changed remote fails closed. The code repository token only needs read access.

Exports omit descriptions and organizer headers from the events table and scoring reasons from both tables, and replace mailbox source labels with `gmail_label`. Remaining titles, URLs and attendance state are still private. Detailed scan output is withheld from Actions logs and is not uploaded as an artifact. Existing recipient query parameters must also be removed from legacy data before it is shared.

## Daily routine

On the author's workstation Event Scout is the first half of a routine run by Claude Code scheduled tasks. The second half lives in [Daily_Task](https://github.com/zacheen/Daily_Task).

1. A scheduled agent runs the Event Scout code, which searches every verified source, scores each career event for relevance and mails the result, so the reader can decide which events to attend.
2. The same agent then checks that the run itself went correctly. It reads the funnel counts, the digest that went out and the ledger columns this run wrote, and compares them against the rules in `SCHEDULED_RUN.md`.
3. Another scheduled agent, the important-mail check in [`Email_Check`](https://github.com/zacheen/Daily_Task/tree/main/Email_Check), reads the inbox over IMAP through a Gmail MCP server the author wrote and picks out mail that asks the reader to act, such as an event they registered for, an employer's own recruiting session, a registration form or a bill to pay.
4. It checks the reader's calendar feeds for events already on the schedule, using [`calendar_check.py`](https://github.com/zacheen/Daily_Task/blob/main/Email_Check/calendar_check.py).
5. It hands its decisions to a Python state machine, [`statemachine.py`](https://github.com/zacheen/Daily_Task/blob/main/Email_Check/statemachine.py), which updates the to-do list.

## Tests

```bash
python check.py
```

`check.py` asserts design invariants rather than only exercising functions. Its live section deliberately asserts lower bounds on counts, because a scraper whose pattern stopped matching returns an empty list, and an empty list is indistinguishable from a genuinely quiet week unless something asserts that zero is wrong. That failure mode is the whole reason the file exists.

```bash
python check.py --offline   # skip anything that touches the network
```

Privacy regression checks use only synthetic data and mocked remote responses.

```bash
python -m unittest test_privacy
```
