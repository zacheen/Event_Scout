"""Load config.yaml / channels.yaml and build the configured sources."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .geo import GeoFilter
from .protocols import EventSource
from .http import HttpClient, UrllibHttpClient
from .sources.gmail_label import GmailLabelSource
from .sources.jsonld import JsonLdSource
from .sources.newsletter import NewsletterArchiveSource
from .sources.wordpress import WordPressSource

ROOT = Path(__file__).resolve().parent.parent


# What the first run does with a backlog nothing has ever reported.
#
#   seed_plus_recent   record everything, mail only what _seed_eligible calls
#                      recent, so a new ledger does not land as one huge digest.
#   report_everything  mail the whole in-scope backlog once and let the reader
#                      pick. require_known_publish_time and recent_days then do
#                      NOTHING, since _seed_eligible is the only thing that
#                      reads either -- that filter alone was observed withholding
#                      all 70 JSON-LD events, which have no datePublished.
FIRST_RUN_MODES = frozenset({"seed_plus_recent", "report_everything"})


@dataclass
class Settings:
    keywords: list[str] = field(default_factory=list)
    capacity_markers: list[str] = field(default_factory=list)
    min_hits: int = 1
    keyword_gate_applies_to: list[str] = field(default_factory=list)
    keyword_match_prefix_chars: int = 2000
    lookahead_days: int = 30
    urgent_hours: int = 72
    resweep_min_gap_hours: int = 24
    digest_min_rank: int = 10
    p0_min_rank: int = 40
    p1_min_rank: int = 9
    unknown_time_max_urgency: str = "P1"
    seed_recent_days: int = 3
    require_known_publish_time: bool = True
    first_run_mode: str = "seed_plus_recent"
    display_timezone: str = "America/Los_Angeles"
    source_precedence: list[str] = field(default_factory=list)
    gmail_user: str = ""
    gmail_password: str = ""
    mail_to: str = ""
    # The mailbox the gmail_label source READS, distinct from gmail_user above,
    # which is the account the digest is SENT FROM. Its labels are here rather
    # than in channels.yaml because a label name describes a private mailbox
    # and that file is published.
    gmail_read_user: str = ""
    gmail_read_password: str = ""
    gmail_read_labels: list[str] = field(default_factory=list)
    # LLM tiering: API beats CLI beats keyword.
    openai_api_key: str = ""
    model: str = "gpt-5.6"
    reasoning_effort: str = "medium"
    gpt_cli: str = "codex"
    gpt_cli_args: list[str] = field(default_factory=list)
    max_description_chars: int = 4000
    score_workers: int = 5
    request_timeout: int = 20
    user_agent: str = "event-scout/1.0"
    profile_text: str = ""

    @property
    def precedence_rank(self) -> dict[str, int]:
        return {kind: i for i, kind in enumerate(self.source_precedence)}


def load_settings(config_path: Path | None = None) -> Settings:
    raw = yaml.safe_load((config_path or ROOT / "config.yaml").read_text(encoding="utf-8"))
    first = raw.get("first_run") or {}
    _check_policies(raw, first)
    return Settings(
        keywords=[str(k).lower() for k in raw.get("keywords") or []],
        # Lowercased on load like keywords, so the comparison site does not
        # have to remember and a marker typed in capitals still matches.
        capacity_markers=[str(k).lower() for k in raw.get("capacity_markers") or []],
        min_hits=int(raw.get("min_hits", 1)),
        keyword_gate_applies_to=list(raw.get("keyword_gate_applies_to") or []),
        keyword_match_prefix_chars=int(raw.get("keyword_match_prefix_chars", 2000)),
        lookahead_days=int(raw.get("lookahead_days", 30)),
        urgent_hours=int(raw.get("urgent_section_within_hours", 72)),
        resweep_min_gap_hours=int(raw.get("resweep_min_gap_hours", 24)),
        digest_min_rank=int(raw.get("digest_min_rank", 10)),
        p0_min_rank=int(raw.get("p0_min_rank", 40)),
        p1_min_rank=int(raw.get("p1_min_rank", 9)),
        unknown_time_max_urgency=str(raw.get("unknown_time_max_urgency", "P1")),
        first_run_mode=str(first.get("mode", "seed_plus_recent")),
        seed_recent_days=int(first.get("recent_days", 3)),
        require_known_publish_time=bool(first.get("require_known_publish_time", True)),
        display_timezone=str(raw.get("display_timezone", "America/Los_Angeles")),
        source_precedence=list(raw.get("source_precedence") or []),
        # Secrets come from the environment only, never from a committed file.
        gmail_user=os.getenv("GMAIL_USER", ""),
        gmail_password=os.getenv("GMAIL_APP_PASSWORD", ""),
        mail_to=os.getenv("MAIL_TO", ""),
        gmail_read_user=os.getenv("GMAIL_READ_USER", ""),
        gmail_read_password=os.getenv("GMAIL_READ_APP_PASSWORD", ""),
        gmail_read_labels=_parse_labels(os.getenv("GMAIL_READ_LABELS", "")),
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        model=str(raw.get("model", "gpt-5.6")),
        reasoning_effort=str(raw.get("reasoning_effort", "") or ""),
        # GPT_CLI may be a full path with spaces, so it overrides the bare name.
        gpt_cli=os.getenv("GPT_CLI") or str(raw.get("gpt_cli", "codex")),
        gpt_cli_args=[str(a) for a in raw.get("gpt_cli_args") or []],
        max_description_chars=int(raw.get("max_description_chars", 4000)),
        score_workers=max(1, int(raw.get("score_workers", 5))),
        request_timeout=int(raw.get("request_timeout", 20)),
        user_agent=str(raw.get("user_agent", "event-scout/1.0")),
        profile_text=_profile_text(),
    )


def _check_policies(raw: dict, first: dict) -> None:
    """Reject a policy key set to something no code implements.

    These two DECLARE a rule rather than tune one, so neither is stored on
    Settings; there would be nothing to read it. Both used to be ignored
    outright, which let config.yaml describe a keyword scope nothing applied.
    The whole-body alternative is not offered because it is the configuration
    that failed: a single newsletter issue measured 564,318 characters after
    HTML stripping and contained nearly every candidate term, so matching all
    of it with min_hits 1 admits everything.

    check.py finds config keys by looking for `.get` on the literal names `raw`
    and `first`, here and in load_settings; renaming either parameter makes that
    check report keys as unread.
    """
    policies = (
        ("keyword_match_scope", raw.get("keyword_match_scope", "subject+prefix"),
         "subject+prefix",
         "_hits matches the title plus keyword_match_prefix_chars of the body"),
    )
    for key, value, implemented, what in policies:
        if str(value) != implemented:
            raise ValueError(f"config.yaml {key}={value!r} is not implemented; {what}")
    mode = str(first.get("mode", "seed_plus_recent"))
    if mode not in FIRST_RUN_MODES:
        raise ValueError(f"config.yaml first_run.mode={mode!r} is not implemented; "
                         f"choose one of {', '.join(sorted(FIRST_RUN_MODES))}")


def _profile_text() -> str:
    """Profile that drives the `fit` axis.

    Without it every fit collapses to a neutral 5 and ranking is decided by
    access_value alone, which is the same degeneracy access_value itself had
    before it was fixed. Resolution order, most explicit first:

      PROFILE_TEXT / RESUME_TEXT   inline, for a cloud run with no file
      PROFILE_PATH                 any readable file, relative paths resolving
                                   against the project root. Pointing OUTSIDE
                                   the project is the intended use, because a
                                   resume that never enters this directory
                                   cannot be committed from it by accident
      ./profile.txt, ./resume.txt  gitignored local files

    A set but unreadable PROFILE_PATH raises rather than falling through to the
    defaults. Falling through would degrade every fit score to a constant 5 and
    report nothing, which is the failure this project exists to prevent.
    """
    inline = os.getenv("PROFILE_TEXT") or os.getenv("RESUME_TEXT")
    if inline:
        return inline
    candidates = []
    if configured := os.getenv("PROFILE_PATH"):
        path = Path(configured)
        path = path if path.is_absolute() else ROOT / path
        if not path.is_file():
            raise ValueError(f"PROFILE_PATH does not name a readable file: {path}")
        candidates.append(path)
    candidates += [ROOT / "profile.txt", ROOT / "resume.txt"]
    for path in candidates:
        if path.exists():
            return path.read_text(encoding="utf-8", errors="replace")
    return ""


def load_channels(path: Path | None = None) -> dict:
    return yaml.safe_load((path or ROOT / "channels.yaml").read_text(encoding="utf-8")) or {}


def _parse_labels(raw: str) -> list[str]:
    """Split GMAIL_READ_LABELS into label names. Pure, so it takes the string.

    A comma cannot appear IN a label name. Gmail permits one, so such a label
    splits here into two that match nothing rather than failing, and a label
    matching nothing looks exactly like a quiet week. .env.example says so.

    Blanks are dropped, making a trailing comma harmless. Each name is stripped
    because the delimiter invites a space after it, while a space INSIDE a name
    is kept: Gmail counts that one as part of the name.
    """
    return [label.strip() for label in raw.split(",") if label.strip()]


def _build_gmail_sources(entry: dict, user: str, password: str,
                         labels: list[str], *, body_chars: int,
                         skip_from: str) -> list[EventSource]:
    """One mailbox source per label, or none with a reason on stdout.

    Split out of build_sources so that function stays four parallel blocks of
    one test and one append. Skipping is right here, but silently skipping is
    not: an unset credential otherwise looks like a quiet mailbox. The two
    causes are named apart because the fix differs, one being a missing secret
    and the other a mailbox configured to read nothing.
    """
    if not (user and password):
        print(f"  [skip] {entry['name']} - GMAIL_READ_USER/_APP_PASSWORD not set")
        return []
    if not labels:
        print(f"  [skip] {entry['name']} - GMAIL_READ_LABELS is empty")
        return []
    # Named after the label, because this name is what the funnel and every
    # [skip] line print, and an anonymous "mailbox-2" cannot tell you WHICH
    # label went quiet. The name is redacted where it would be published, at
    # export (see store.export_jsonl) and by keeping the cloud run log out of
    # the Actions log, rather than by throwing the label away everywhere.
    return [GmailLabelSource(
        f"gmail:{label}", user, password, label,
        newer_than_days=int(entry.get("newer_than_days", 60)),
        body_chars=body_chars, skip_from=skip_from) for label in labels]


def build_sources(channels: dict, http: HttpClient,
                  body_chars: int = 4000,
                  skip_from: str = "",
                  gmail_read_user: str = "", gmail_read_password: str = "",
                  gmail_read_labels: list[str] | None = None) -> list[EventSource]:
    """Instantiate every ACTIVE source. Entries whose status is held or deferred
    are skipped here rather than filtered later, so a held source costs nothing.

    The status test is exact for that reason. Three of these four blocks used to
    accept any status STARTING WITH "verified", which would have quietly enabled
    a verified_but_held or verified_pattern entry the moment one was filed under
    a block build_sources reads.

    Nothing here reads the environment. Every value that comes from it arrives
    as an argument, so load_settings stays the single place secrets enter and
    Settings stays a complete picture of what the run was given. The mailbox
    arguments defaulting to empty means an omitted one prints a [skip] rather
    than connecting to whatever the ambient environment happens to hold.

    `skip_from` is the address the notifier sends the digest from, handed to
    every mailbox source so none of them reads it back. Defaults to empty, which
    turns the check OFF, so build_runtime passes settings.gmail_user.
    """
    sources = []
    for entry in channels.get("jsonld") or []:
        if entry.get("status") == "verified" and entry.get("url"):
            sources.append(JsonLdSource(entry["name"], entry["url"], http))
    for entry in channels.get("wordpress") or []:
        if entry.get("status") == "verified" and entry.get("url"):
            sources.append(WordPressSource(entry["name"], entry["url"], http))
    for entry in channels.get("gmail") or []:
        if entry.get("status") == "verified":
            sources += _build_gmail_sources(
                entry, gmail_read_user, gmail_read_password,
                list(gmail_read_labels or []),
                body_chars=body_chars, skip_from=skip_from)
    for entry in channels.get("newsletters") or []:
        if entry.get("status") == "verified" and entry.get("host"):
            sources.append(NewsletterArchiveSource(
                entry["name"], entry["host"], http,
                archive_path=entry.get("archive_path", "/archive")))
    return sources


def build_runtime(settings: Settings, channels: dict):
    """Everything both entry points assemble identically.

    Exists because they did NOT: local_run.py hard-coded timeout=30 and no user
    agent, so config.yaml's request_timeout and user_agent were silently ignored
    on this machine while the cloud honoured them. Deliberate local/cloud
    differences (ledger location, notifier, dry_run, report_all) stay with the
    entry points; anything else living in two places will drift again.
    """
    from .extract import PageFactExtractor
    from .scoring import build_invoker

    http = UrllibHttpClient(timeout=settings.request_timeout,
                            user_agent=settings.user_agent,
                            min_delay=0.2, max_delay=0.8)
    invoker = build_invoker(settings)
    # The client itself is not returned: neither entry point uses it, and
    # keeping it would be an extension point with no consumer, which is the
    # same speculative generality rejected for the _links constants.
    return (build_sources(channels, http, settings.max_description_chars,
                          skip_from=settings.gmail_user,
                          gmail_read_user=settings.gmail_read_user,
                          gmail_read_password=settings.gmail_read_password,
                          gmail_read_labels=settings.gmail_read_labels),
            build_geo(channels),
            PageFactExtractor(invoker, http))


def build_geo(channels: dict) -> GeoFilter:
    geo = channels.get("geo") or {}
    # Passed through uncoerced, so GeoFilter still sees a bare string or a None
    # entry as YAML gave it and can refuse it (see its constructor).
    places = geo.get("places") or ()
    regional = {e["name"] for block in ("jsonld", "wordpress")
                for e in channels.get(block) or [] if e.get("in_region")}
    return GeoFilter(places, bool(geo.get("accept_virtual", True)), frozenset(regional))
