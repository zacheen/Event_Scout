"""Event Scout self-check. Run it directly, read the terminal.

    conda run -n ML python check.py            # everything
    conda run -n ML python check.py --offline  # skip anything that hits the network
    conda run -n ML python check.py --live     # only the network checks

Three outcomes, and the distinction matters.

  FAIL  behaviour contradicts a documented design rule. Fix before shipping.
  WARN  legal but suspicious. Usually a source that still answers but is
        drifting, which is how a scraper dies quietly.
  PASS  the rule held.

The live section deliberately asserts LOWER BOUNDS on counts rather than just
"no exception raised". A scraper whose pattern stopped matching returns an empty
list, and an empty list is indistinguishable from a genuinely quiet week unless
something asserts that zero is wrong. That failure mode is the whole reason this
file exists.
"""
from __future__ import annotations

import ast
import contextlib
import inspect
import logging
import io
import re
import sqlite3
import subprocess
import json
import sys
import tempfile
import traceback

import yaml
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from eventscout.config import (_parse_labels, build_geo, build_runtime, build_sources,
                               load_channels,
                               load_settings)
from eventscout.geo import Anchor, GeoFilter
from eventscout.http import (HttpClient, StubHttpClient, UrllibHttpClient,
                             _decode_body)
from eventscout.notifier import (ConsoleNotifier, EmailNotifier, _lead_time,
                                 build_ics, format_digest)
from eventscout import notifier as notifier_module, pipeline, protocols
from eventscout.scoring import (CachingScorer, CliScorer, DeadlineUrgency,
                                _parse_score,
                                KeywordScorer, OpenAiScorer)
from eventscout.pipeline import (AllScoringFailedError, AllSourcesFailedError,
                                 Digest, Funnel,
                                 _at_capacity, _bound_reason, _collapse, _deliver,
                                 _START_GRACE, _stamp_naive, _start_order,
                                 _passes_gate, _same_event_key, render_funnel,
                                 run)
from eventscout.store import SqliteEventStore
from eventscout.models import (AttendanceMode, Coverage, Event, Score, State,
                               Urgency,
                               display_zone, is_date_only, iso_or_empty,
                               parse_iso)
from eventscout.extract import PageFactExtractor
from eventscout.sources._links import is_event_link, tails
from eventscout.sources.gmail_label import GmailLabelSource
from eventscout.sources._text import strip_html
from eventscout.sources.jsonld import JsonLdSource
from eventscout.sources.newsletter import NewsletterArchiveSource, start_from_prose
from eventscout.sources.wordpress import WordPressSource
from eventscout.urls import canon_url

FAILS: list[str] = []
WARNS: list[str] = []


def check(rule: str, ok: bool, detail: str = "") -> bool:
    """Report one design rule. `rule` names what SHOULD be true."""
    if ok:
        print(f"  [PASS] {rule}")
    else:
        print(f"  [FAIL] {rule}")
        if detail:
            print(f"         {detail}")
        FAILS.append(rule)
    return ok


def warn(rule: str, ok: bool, detail: str = "") -> None:
    if ok:
        print(f"  [PASS] {rule}")
    else:
        print(f"  [WARN] {rule}")
        if detail:
            print(f"         {detail}")
        WARNS.append(rule)


def section(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


def _params(func) -> list[tuple]:
    """Comparable shape of a signature: name, kind, and whether it has a default.

    Names alone would miss a parameter changing from required to optional, which
    silently widens what an implementation accepts relative to its Protocol.
    """
    return [(n, p.kind, p.default is inspect.Parameter.empty)
            for n, p in inspect.signature(func).parameters.items() if n != "self"]


def _conformance(proto, impl) -> tuple[list[str], list[str]]:
    """Return (missing members, methods whose parameter names drifted).

    `impl` may be a class or an instance. Pass an INSTANCE whenever the Protocol
    declares a plain (non-ClassVar) attribute: EventSource.name is set in
    __init__, so hasattr on the class is False even though every source is
    correct. Checking the class alone reported all three sources as broken.

    Two more blind spots this closes: a bare class-level annotation such as
    `cacheable: ClassVar[bool]` never appears in dir(proto), because Python
    records an unassigned annotation only in __annotations__, so a scorer that
    forgot `cacheable` would otherwise pass; and hasattr alone cannot see a
    changed parameter list, which is exactly how UrgencyEngine.classify
    drifted, with the Protocol declaring two parameters while every caller and
    the only implementation passed three.
    """
    methods = [n for n in dir(proto)
               if not n.startswith("_") and callable(getattr(proto, n, None))]
    data = [n for n in getattr(proto, "__annotations__", {}) if not n.startswith("_")]
    missing = [n for n in methods + data if not hasattr(impl, n)]
    drifted = []
    for name in methods:
        if not hasattr(impl, name):
            continue
        try:
            if _params(getattr(proto, name)) != _params(getattr(impl, name)):
                drifted.append(name)
        except (TypeError, ValueError):
            continue
    return missing, drifted


def _dotted(node: ast.expr) -> str:
    """"self._store" for a chain of plain attribute access, "" for anything else."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted(node.value)
        return f"{parent}.{node.attr}" if parent else ""
    return ""


def _config_keys_read(path: Path) -> set[str]:
    """Keys config.py actually looks up, read syntactically like _attrs_used.

    Restricted to the two names that actually hold config.yaml, `raw` and
    `first`, which config.py's docstrings name as load-bearing for this
    reason. Renaming either there makes this report keys as unread, which
    is the safe direction to fail but still needs the tuple below updated.
    Scanning every `.get` in the file would pass silently the moment a
    channels.yaml key shared a name with a config.yaml one: the unrelated
    lookup would satisfy the assertion for a key nothing reads any more.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {n.args[0].value for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "get" and n.args
            and isinstance(n.args[0], ast.Constant)
            and isinstance(n.args[0].value, str)
            and _dotted(n.func.value) in ("raw", "first")}


def _members(proto) -> set[str]:
    """Public members of a Protocol, methods AND bare annotations.

    dir() alone misses an annotation-only member such as `cacheable:
    ClassVar[bool]`, which is why _conformance unions __annotations__ too. Kept
    as one helper because computing the two sets differently is how a member
    silently stops being checked.
    """
    return ({n for n in dir(proto) if not n.startswith("_")}
            | set(getattr(proto, "__annotations__", {})))


def _attrs_used(path: Path, var: str) -> set[str]:
    """Public attributes read off `var` anywhere in a module.

    Deliberately syntactic. It answers "what does this module actually touch"
    without importing or running it, so a Protocol that omits a member its
    caller uses fails here rather than at run time. `var` may be dotted
    ("self._invoker"), which is how a collaborator injected into a CLASS is
    reached; without that the scan would only ever see free functions.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {n.attr for n in ast.walk(tree)
            if isinstance(n, ast.Attribute) and not n.attr.startswith("_")
            and _dotted(n.value) == var}


def _event(**kw) -> Event:
    base = dict(event_uid="u", title="t", url="https://x/1", source="s", source_kind="jsonld")
    return Event(**{**base, **kw})


# --------------------------------------------------------------------------
# OFFLINE
# --------------------------------------------------------------------------
def offline() -> None:
    section("Funnel - a delta only means something between two of the same set")
    # The bug these lock down was found by eye, not by a test: one run's
    # reminders count was recorded with stage(), so render_funnel subtracted it
    # from the line above and printed "(-28)" under 30 new events. It reads as a
    # late collapse on the LAST line of the report, where the digest had in fact
    # grown to 32. Asserted on the rendered text, because the misleading part was
    # never the stored count.
    _f = Funnel()
    _f.stage("fetched", 30)
    _f.added("closing soon, never acted on", 2, "from the ledger")
    _rendered = render_funnel(_f)
    check("the reminders line carries no delta", "(-28)" not in _rendered
          and re.search(r"never acted on\s+2\s+\(", _rendered) is None,
          _rendered)
    check("and it is rendered outside the chain, not as another stage",
          "ALSO IN THE DIGEST" in _rendered, _rendered)
    _f.stage("after geo filter", 25)
    check("a real narrowing still shows its delta",
          "(-5)" in render_funnel(_f), render_funnel(_f))
    # first_zero answers "which filter emptied the run". A zero in additions
    # means nothing was due for a reminder, and a zero in per_source is already
    # flagged separately, so neither belongs in that answer.
    _z = Funnel()
    _z.source("dead-source", 0)
    _z.stage("fetched", 5)
    _z.stage("after geo filter", 0)
    _z.added("closing soon, never acted on", 0)
    check("first_zero names the stage that emptied the run, ignoring the rest",
          _z.first_zero() == "after geo filter", f"got {_z.first_zero()!r}")

    section("protocols - implementations must not drift from the interface")
    # This section exists because they DID drift exactly like this once (see
    # protocols.py's module docstring for the Notifier/EventStore story).
    # EventSource entries below pass instances, not classes: `name` is set in
    # __init__ and invisible on the class (see _conformance's docstring).
    _stub = StubHttpClient({})

    class _stub_invoker:   # satisfies JsonInvoker without touching a model
        @staticmethod
        def json_call(system, user, schema, name):
            return "{}"

    conformance = [
        (protocols.EventStore, SqliteEventStore, "SqliteEventStore"),
        (protocols.ScoreCache, SqliteEventStore, "SqliteEventStore (cache slice)"),
        (protocols.Notifier, EmailNotifier, "EmailNotifier"),
        (protocols.Notifier, ConsoleNotifier, "ConsoleNotifier"),
        (protocols.EventScorer, KeywordScorer, "KeywordScorer"),
        (protocols.EventScorer, OpenAiScorer, "OpenAiScorer"),
        (protocols.EventScorer, CliScorer, "CliScorer"),
        (protocols.EventScorer, CachingScorer, "CachingScorer"),
        (HttpClient, UrllibHttpClient, "UrllibHttpClient"),
        # The stub is what every offline source test runs through, so a drift
        # here would silently invalidate those tests rather than fail loudly.
        (HttpClient, StubHttpClient({}), "StubHttpClient"),
        (protocols.EventSource, JsonLdSource("n", "http://x/", _stub), "JsonLdSource"),
        (protocols.EventSource,
         NewsletterArchiveSource("n", "x.beehiiv.com", _stub), "NewsletterArchiveSource"),
        (protocols.BoundedSource,
         NewsletterArchiveSource("n", "x.beehiiv.com", _stub),
         "NewsletterArchiveSource (coverage)"),
        (protocols.EventSource, WordPressSource("n", "http://x/", _stub), "WordPressSource"),
        # Listed twice on purpose, exactly as SqliteEventStore is above: without
        # the second row _conformance never compares coverage()'s signature, so
        # a required argument added to one implementation and not the Protocol
        # would only be caught by the name-only scan, which cannot see params.
        (protocols.BoundedSource, WordPressSource("n", "http://x/", _stub),
         "WordPressSource (coverage)"),
        (protocols.EventSource,
         GmailLabelSource("n", "u", "p", "label"), "GmailLabelSource"),
        (protocols.BoundedSource,
         GmailLabelSource("n", "u", "p", "label"), "GmailLabelSource (coverage)"),
        # Extractor had NO implementation until this round, so the safety net
        # had never covered it once.
        (protocols.Extractor, PageFactExtractor(_stub_invoker, _stub), "PageFactExtractor"),
        (protocols.JsonInvoker, CliScorer(["x"], ""), "CliScorer as JsonInvoker"),
        (protocols.JsonInvoker, OpenAiScorer("k", "m", ""), "OpenAiScorer as JsonInvoker"),
        (protocols.EventFilter, GeoFilter, "GeoFilter"),
        (protocols.UrgencyEngine, DeadlineUrgency, "DeadlineUrgency"),
    ]
    for proto, impl, label in conformance:
        missing, drifted = _conformance(proto, impl)
        check(f"{label} implements every {proto.__name__} member",
              not missing, f"missing: {missing}")
        check(f"{label} matches every {proto.__name__} signature",
              not drifted, f"parameter drift: {drifted}")
    _pkg = Path(__file__).resolve().parent / "eventscout"
    _pipeline_src = (_pkg / "pipeline.py").read_text(encoding="utf-8")
    check("EventStore.upsert hands back no verdict to misuse",
          protocols.EventStore.upsert.__annotations__.get("return") == "None")
    # The digest must not be re-derived from that bool (see EventStore.upsert
    # for the two measured defects this caused). Checked by name, not text, so
    # a comment mentioning the old variable does not fail this; the annotation
    # check above is the real guard, since upsert returns None and cannot be
    # silently half-worked around.
    _pipeline_names = {n.id for n in ast.walk(ast.parse(_pipeline_src))
                       if isinstance(n, ast.Name)}
    check("the digest decision is not taken from upsert's return value",
          "unseen_uid" not in _pipeline_names and "reported_urls" in _pipeline_names)

    # The loop above only proves an implementation satisfies its Protocol. It is
    # blind to the OTHER direction, the pipeline calling something the Protocol
    # never declared, which is how store.expire_past and store.counts_by_state
    # stayed invisible here (see EventStore.expire_past for the consequence).
    # Every place a Protocol-typed collaborator is reached, not just the
    # pipeline: CachingScorer and PageFactExtractor hold theirs as attributes, so
    # scanning free functions alone left two injection points unguarded.
    for module, var, proto in (
            ("pipeline.py", "store", protocols.EventStore),
            ("pipeline.py", "notifier", protocols.Notifier),
            ("pipeline.py", "geo", protocols.EventFilter),
            ("pipeline.py", "extractor", protocols.Extractor),
            # A TUPLE because run() narrows this one. coverage() is declared on
            # BoundedSource, not EventSource, so that JsonLdSource is not handed a
            # method it can never answer. The guard assertion below covers what
            # this union cannot see, namely that the narrowing is really there.
            ("pipeline.py", "source",
             (protocols.EventSource, protocols.BoundedSource)),
            ("pipeline.py", "scorer", protocols.EventScorer),
            ("pipeline.py", "urgency_engine", protocols.UrgencyEngine),
            ("scoring.py", "self._store", protocols.ScoreCache),
            ("scoring.py", "self._inner", protocols.EventScorer),
            ("extract.py", "self._invoker", protocols.JsonInvoker),
            ("extract.py", "self._http", HttpClient),
            ("sources/jsonld.py", "self._http", HttpClient),
            ("sources/wordpress.py", "self._http", HttpClient),
            ("sources/newsletter.py", "self._http", HttpClient)):
        protos = proto if isinstance(proto, tuple) else (proto,)
        declared = set()
        for one in protos:
            declared |= _members(one)
        label = " or ".join(one.__name__ for one in protos)
        used = _attrs_used(_pkg / module, var)
        # Without this, renaming the pipeline's parameter would make the scan
        # find nothing and the check below pass vacuously, which is the same
        # "empty looks clean" failure the live section exists to prevent.
        check(f"the scan still sees {module}'s {var} call sites", bool(used))
        undeclared = sorted(used - declared)
        check(f"{module} calls no {label} member the protocol omits",
              not undeclared, f"undeclared: {', '.join(undeclared)}")
        # The union above would also pass if the pipeline called a narrow-only
        # member with no narrowing at all, which is the one thing it must not do.
        # _attrs_used reads attribute names and cannot see an isinstance, so the
        # narrowing is asserted directly against the source text.
        for one in protos[1:]:
            narrow = _members(one) - _members(protos[0])
            reached = sorted(narrow & used)
            if not reached:
                continue
            text = (_pkg / module).read_text(encoding="utf-8")
            check(f"{module} narrows {var} before calling {one.__name__}",
                  f"isinstance({var}, {one.__name__})" in text,
                  f"calls {reached} with no isinstance guard")
            # The check above is a whole-file substring, so a SECOND unguarded
            # call site would stay green on the strength of the first one's
            # isinstance. Pinning the count forces a human back here instead,
            # which is cheaper than proving dominance from the AST.
            for member in reached:
                check(f"{module} still has exactly one {var}.{member} call site",
                      text.count(f"{var}.{member}") == 1,
                      "a second call site needs its own guard, and the check "
                      "above cannot tell them apart")

    # Same drift, other direction: a setting nobody reads. config.yaml carried
    # six of them, one declaring a keyword-scope safeguard that was never wired,
    # so the file documented protection the pipeline did not apply.
    _cfg = yaml.safe_load((_pkg.parent / "config.yaml").read_text(encoding="utf-8"))
    declared_keys = ({k for k in _cfg if k != "first_run"}
                     | set(_cfg.get("first_run") or {}))
    unread = sorted(declared_keys - _config_keys_read(_pkg / "config.py"))
    check("every config.yaml key is read by config.py", not unread,
          f"declared but never read: {', '.join(unread)}")

    # Two of those keys declare a policy rather than tune one, so being read is
    # not enough: they have to be REJECTED when set to something unimplemented.
    # A validator that never fires would leave them exactly as misleading as
    # when nothing read them at all.
    _raw_cfg = (_pkg.parent / "config.yaml").read_text(encoding="utf-8")
    policy_edits = [
        ("keyword_match_scope", 'keyword_match_scope: "subject+prefix"',
         'keyword_match_scope: "full_body"'),
        ("first_run.mode", 'mode: "report_everything"', 'mode: "seed_only"'),
    ]
    with tempfile.TemporaryDirectory() as tmp_cfg:
        path = Path(tmp_cfg) / "c.yaml"
        for key, before, after in policy_edits:
            check(f"config.yaml still declares {key} as implemented",
                  before in _raw_cfg, f"missing: {before}")
            path.write_text(_raw_cfg.replace(before, after, 1), encoding="utf-8")
            try:
                load_settings(path)
                rejected = False
            except ValueError:
                rejected = True
            check(f"an unimplemented {key} is rejected, not ignored", rejected,
                  "it loaded, so the key documents a rule nothing enforces")
        path.write_text(_raw_cfg, encoding="utf-8")
        try:
            load_settings(path)
            check("the real config.yaml still loads", True)
        except Exception as exc:
            check("the real config.yaml still loads", False,
                  f"{type(exc).__name__}: {exc}")

    section("urls.canon_url - dedupe must survive campaign tracking")
    tesla_raw = ("https://www.tesla.com/event/northeastern-resume"
                 "?utm_campaign=direct-consideration-004&utm_medium=referral"
                 "&utm_source=directconsideration.beehiiv.com")
    got = canon_url(tesla_raw)
    check("utm_* stripped so one event shared by 3 channels is one row",
          got == "https://tesla.com/event/northeastern-resume", f"got {got}")
    check("lu.ma folds onto luma.com (the host 301s, both forms circulate)",
          canon_url("https://lu.ma/NUSiliconValleyCampus") == "https://luma.com/NUSiliconValleyCampus")
    check("a non-tracking query is KEPT (it can identify the event itself)",
          canon_url("https://luma.com/x?locale=zh-TW").endswith("?locale=zh-TW"))

    section("models.Event - fail loud, never half-valid")
    for field in ("event_uid", "title", "url", "source", "source_kind"):
        try:
            _event(**{field: None})
            check(f"{field}=None raises rather than becoming ''", False,
                  "two adapters both omitting it would produce '' and dedupe into each other")
        except ValueError:
            check(f"{field}=None raises rather than becoming ''", True)
    e = _event(location=None, end=None, attendance_mode=None)
    check("optional None coerces to ''", e.location == "" and e.end == "")
    check("attendance_mode stays an AttendanceMode, not a bare str "
          "(PEP 563 makes f.type a string, so the fallback reads f.default)",
          isinstance(e.attendance_mode, AttendanceMode) and e.attendance_mode.name == "UNKNOWN",
          f"got {type(e.attendance_mode).__name__}")

    # The None coercion reads the FIELD DEFAULT, so every new non-string field
    # has to be covered by that isinstance check or it silently becomes "".
    # attendance_mode was the first to be caught by this; page_unreadable is a
    # bool and would have been the second.
    check("a None bool field falls back to its default, not to a string",
          _event(page_unreadable=None).page_unreadable is False,
          f"got {_event(page_unreadable=None).page_unreadable!r}")

    section("pipeline - one event under two URLs is one event")
    # See pipeline._collapse's "Same event, different URL" comment and
    # _identity's docstring for why a start (not just a title) is required.
    _rank = {"jsonld": 0, "newsletter": 2}

    def _ev(uid, url, title, start, kind="newsletter"):
        return _event(event_uid=uid, url=url, title=title, start=start,
                      source=kind, source_kind=kind)

    PACIFIC = "2026-09-20T18:00:00-07:00"
    dedupe_cases = [
        ("the same name and instant collapse, even written differently",
         [_ev("a", "https://luma.com/a", "AI Hiring Mixer", PACIFIC, "jsonld"),
          _ev("b", "https://host.com/b", "ai hiring  mixer",
              # Written with milliseconds by schema.org markup; the gate at the
              # source boundary is what makes the two spellings one instant.
              iso_or_empty("2026-09-20T18:00:00.000-07:00"))], 1),
        ("the same instant expressed in another zone still collapses",
         [_ev("a", "https://luma.com/a", "AI Hiring Mixer", PACIFIC, "jsonld"),
          _ev("b", "https://host.com/b", "AI Hiring Mixer",
              "2026-09-21T01:00:00+00:00")], 1),
        ("one name on two nights is a series, not a duplicate",
         [_ev("a", "https://luma.com/a", "Weekly AI Meetup", PACIFIC, "jsonld"),
          _ev("b", "https://host.com/b", "Weekly AI Meetup",
              "2026-09-27T18:00:00-07:00")], 2),
        ("two undated listings are never collapsed on the name alone",
         [_ev("a", "https://luma.com/a", "Weekly AI Meetup", "", "jsonld"),
          _ev("b", "https://host.com/b", "Weekly AI Meetup", "")], 2),
        ("two events sharing a start time keep their own names",
         [_ev("a", "https://luma.com/a", "AI Hiring Mixer", PACIFIC, "jsonld"),
          _ev("b", "https://host.com/b", "Robotics Demo Night", PACIFIC)], 2),
    ]
    for why, events, want in dedupe_cases:
        kept, _absorbed = _collapse(events, _rank, _same_event_key)
        check(why, len(kept) == want, f"kept {len(kept)}, expected {want}")

    # merged_with folds left over 3+ sources; _collapse has to put every loser
    # under the same winner or the ledger records only one of them as reported.
    three = [_ev("a", "https://luma.com/a", "AI Hiring Mixer", PACIFIC, "jsonld"),
             _ev("b", "https://host.com/b", "AI Hiring Mixer", PACIFIC),
             _ev("c", "https://third.com/c", "ai hiring mixer",
                 "2026-09-21T01:00:00+00:00")]
    kept3, absorbed3 = _collapse(three, _rank, _same_event_key)
    check("three listings of one event collapse to one",
          len(kept3) == 1, f"kept {len(kept3)}")
    check("and both losers are filed under the single winner",
          sorted(a.event_uid for a in absorbed3.get("a", ())) == ["b", "c"],
          f"{ {k: sorted(a.event_uid for a in v) for k, v in absorbed3.items()} }")

    survivors, absorbed = _collapse(dedupe_cases[0][1], _rank, _same_event_key)
    check("the higher-precedence source wins the merge",
          survivors[0].source_kind == "jsonld", survivors[0].source_kind)
    check("and the listing it absorbed is handed back, because its URL still "
          "has to count as reported",
          [a.url for a in absorbed.get("a", ())] == ["https://host.com/b"],
          f"{ {k: [a.url for a in v] for k, v in absorbed.items()} }")

    section("pipeline - a merged-away listing must not mail again later")
    # The one piece of this that only run() can prove: the absorbed listing has
    # its own URL, and "already reported" is a URL question. The run where the
    # winning source stops listing the event has to stay quiet. Uses the real
    # run() on the keyword tier, so it costs no model call, and a fixed `now`
    # so seeding and the date window do not drift with the calendar.
    _NOW = datetime(2026, 9, 20, tzinfo=timezone.utc)
    _WHEN = "2026-09-25T18:00:00-07:00"
    # "software engineer" carries the FIT weight on purpose. Before the keyword
    # tier gained a left word boundary, this fixture cleared digest_min_rank
    # only because the bare term "ai" matched inside "fair", which is the exact
    # collision that boundary was added to remove. Measured: without
    # a real topical term the fixture scored fit 1, access 5, rank 5, so every
    # pipeline test below it silently stopped exercising a reportable event.
    _RICH = ("Career fair and resume review with recruiters hiring software "
             "engineers for internships and full-time roles")

    def _listing(kind, url):
        return _event(event_uid=f"{kind}:{url}", url=url, source=kind,
                      source_kind=kind, title=_RICH, description=_RICH,
                      start=_WHEN, location="Palo Alto, CA",
                      published_at="2026-09-19T00:00:00")

    class _OneListing:
        def __init__(self, kind, url):
            self.kind = self.name = kind
            self._url = url

        def fetch(self):
            return [_listing(self.kind, self._url)]

    class _Mailbox:
        def __init__(self):
            self.mailed = []

        def send(self, sections, subject, footer=""):
            # Returns THIS digest's count, not the running total: Notifier.send
            # promises "how many events it carried", and these fakes are reused
            # across rounds.
            urls = [e.url for _, items in sections for e, _, _ in items]
            self.mailed += urls
            return len(urls)

    # No API key and no CLI name resolves to the keyword tier (see
    # scoring._configured_tier), which is deterministic and free.
    _offline = replace(load_settings(), openai_api_key="", gpt_cli="")
    _luma, _host = "https://luma.com/dup", "https://host.example/dup"
    with tempfile.TemporaryDirectory() as tmp_run:
        db = SqliteEventStore(Path(tmp_run) / "run.db")
        box = _Mailbox()
        try:
            # run() prints its own funnel; this file's output is the report.
            with contextlib.redirect_stdout(io.StringIO()):
                run([_OneListing("jsonld", _luma), _OneListing("newsletter", _host)],
                    db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_NOW)
            check("both listings of one event mail once, not twice",
                  box.mailed == [_luma], f"mailed {box.mailed}")
            with contextlib.redirect_stdout(io.StringIO()):
                run([_OneListing("newsletter", _host)], db,
                    build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_NOW)
            check("the absorbed listing alone mails nothing",
                  box.mailed == [_luma], f"mailed {box.mailed}")
        finally:
            db.close()

    section("pipeline - first_run.mode decides what the backlog does")
    # Why report_everything is the shipped default lives on config.yaml's
    # first_run key. Both modes are exercised here because config.yaml can pick
    # either, and a mode nothing tests is a mode that quietly stops working.
    _OLD_PUB = "2026-08-10T00:00:00"   # 41 days before _NOW, far past recent_days

    class _Backlog:
        kind = name = "jsonld"

        def fetch(self):
            return [_listing("jsonld", "https://luma.com/backlog")]

    def _first_run(mode, published_at):
        settings = replace(_offline, first_run_mode=mode)
        with tempfile.TemporaryDirectory() as tmp_first:
            db = SqliteEventStore(Path(tmp_first) / "first.db")
            box = _Mailbox()
            try:
                class _Src(_Backlog):
                    def fetch(self):
                        return [replace(_listing("jsonld", "https://luma.com/backlog"),
                                        published_at=published_at)]
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    run([_Src()], db, build_geo(load_channels()), settings, box,
                        dry_run=False, now=_NOW)
                return box.mailed, out.getvalue()
            finally:
                db.close()

    mailed_seed, funnel_seed = _first_run("seed_plus_recent", _OLD_PUB)
    check("seed_plus_recent withholds a backlog item on the first run",
          mailed_seed == [], f"mailed {mailed_seed}")
    mailed_all, funnel_all = _first_run("report_everything", _OLD_PUB)
    check("report_everything mails that same item instead",
          mailed_all == ["https://luma.com/backlog"], f"mailed {mailed_all}")
    check("and says so in the funnel, so an unfiltered stage cannot read as a "
          "skipped one",
          "first run: report_everything" in funnel_all,
          "no line for the mode that ran")
    check("while seed_plus_recent still names its own cut",
          "first run: published within" in funnel_seed, "no line for the filter")
    # Guards the elif: were report_everything to fall through to _seed_eligible,
    # a RECENT item would still mail and the pair above would look identical.
    mailed_recent, _ = _first_run("seed_plus_recent", "2026-09-19T00:00:00")
    check("seed_plus_recent is not simply mailing nothing",
          mailed_recent == ["https://luma.com/backlog"], f"mailed {mailed_recent}")

    section("http._decode_body - a compressed body is not text")
    # Reproduces the gotowebinar.com regression; see _decode_body's docstring
    # for the measured numbers and why silently guessing the encoding is unsafe.
    import gzip as _gzip

    page = "<html><body>Career fair on 2026-10-01</body></html>"
    packed = _gzip.compress(page.encode("utf-8"))
    check("a gzip body is inflated when Content-Encoding says so",
          _decode_body(packed, "gzip") == page)
    check("header case and whitespace do not decide it",
          _decode_body(packed, " GZIP ") == page)
    check("an identity body is passed through",
          _decode_body(page.encode("utf-8"), "identity") == page)
    check("so is a body with no Content-Encoding at all",
          _decode_body(page.encode("utf-8"), "") == page)
    blind = packed.decode("utf-8", "replace")
    check("reading a gzip body as identity really does yield NUL-laden garbage, "
          "which is why guessing is not an option",
          "\x00" in blind and page not in blind, f"{blind[:40]!r}")
    unknown = False
    try:
        _decode_body(packed, "br")
    except ValueError:
        unknown = True
    check("an encoding we cannot inflate raises instead of returning garbage",
          unknown, "it returned something, so a source would report zero events")

    # _decode_body being right is not enough; get_text has to actually hand it
    # the header. urlopen is stubbed rather than called, so this stays offline.
    import urllib.request as _urlreq

    class _FakeResponse:
        def __init__(self, body, headers):
            self._body, self.headers = body, headers

        def read(self):
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    _real_urlopen = _urlreq.urlopen
    try:
        _urlreq.urlopen = lambda req, timeout=None: _FakeResponse(
            packed, {"Content-Encoding": "gzip"})
        wired = UrllibHttpClient(timeout=1).get_text("https://example.test/x")
    finally:
        _urlreq.urlopen = _real_urlopen
    check("get_text reads Content-Encoding off the response, not just the body",
          wired == page, f"{wired[:40]!r}")

    section("pipeline - the first run shows what the rank floor cut")
    # "Print everything and let me choose" only holds if the floor is shown
    # too (see Digest.below_floor for why these are shown but never marked).
    class _FloorPair:
        kind = name = "jsonld"

        def __init__(self, published_at):
            self._pub = published_at

        def fetch(self):
            good = replace(_listing("jsonld", "https://luma.com/good"),
                           published_at=self._pub)
            # rank 1 on the keyword tier, against the floor of 10.
            poor = replace(_listing("jsonld", "https://luma.com/poor"),
                           title="Board game night",
                           description="Weekly board game night, bring snacks",
                           published_at=self._pub)
            return [good, poor]

    class _Sections:
        """Keeps the section HEADINGS, which _Mailbox discards."""

        def __init__(self):
            self.sections = []

        def send(self, sections, subject, footer=""):
            self.sections = [(head, [e.url for e, _, _ in items])
                             for head, items in sections]
            return sum(len(items) for _, items in sections)

    def _first_with(mode, published_at):
        with tempfile.TemporaryDirectory() as tmp_floor:
            db = SqliteEventStore(Path(tmp_floor) / "floor.db")
            box = _Sections()
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    run([_FloorPair(published_at)], db, build_geo(load_channels()),
                        replace(_offline, first_run_mode=mode), box,
                        dry_run=False, now=_NOW)
                marked = {r[0] for r in db._conn.execute(
                    "SELECT canonical_url FROM events WHERE alerted_at != ''")}
                return box.sections, marked
            finally:
                db.close()

    secs, marked = _first_with("report_everything", _OLD_PUB)
    below = [urls for head, urls in secs if head.startswith("BELOW THE USUAL FLOOR")]
    check("report_everything gives the below-floor events a section of their own",
          below == [["https://luma.com/poor"]], f"sections {secs}")
    check("and the event that cleared the floor stays in the ranked part",
          any("https://luma.com/good" in urls for head, urls in secs
              if not head.startswith("BELOW")), f"sections {secs}")
    check("a below-floor event is shown but never marked, so the sweep can "
          "never chase it",
          marked == {"https://luma.com/good"}, f"alerted {marked}")

    # A recent publish date, so seed_plus_recent actually mails something and
    # the absent section proves a choice rather than an empty run.
    secs_seed, _ = _first_with("seed_plus_recent", "2026-09-19T00:00:00")
    check("seed_plus_recent mails the qualifying event",
          any("https://luma.com/good" in urls for _, urls in secs_seed),
          f"sections {secs_seed}")
    check("but carries no below-floor section",
          not any(h.startswith("BELOW") for h, _ in secs_seed),
          f"sections {secs_seed}")

    raised = False
    try:
        _first_with("seed_only", _OLD_PUB)
    except ValueError:
        raised = True
    check("an unhandled first_run_mode raises rather than quietly seeding, "
          "since Settings can be built without load_settings",
          raised, "it fell through to the other branch")

    section("store.mark_sold_out - a decision the reader made is not overwritten")
    # The rule that keeps this safe is the state filter, and the end-to-end test
    # below cannot see it: there every row is fresh, so every row is writable.
    with tempfile.TemporaryDirectory() as tmp_mso:
        db = SqliteEventStore(Path(tmp_mso) / "mso.db")
        try:
            keep = {}
            for state in (State.NEW, State.SEEN, State.SAVED,
                          State.REGISTERED, State.DISMISSED, State.EXPIRED):
                uid = f"jsonld:https://luma.com/{state}"
                db.upsert(_event(event_uid=uid, url=f"https://luma.com/{state}"),
                          Score(fit=5, access_value=5, cost=1, reason="", method="m"))
                if state is not State.NEW:
                    db.set_state(uid, state)
                keep[uid] = state
            changed = db.mark_sold_out(list(keep))
            after = dict(db._conn.execute("SELECT event_uid, state FROM events"))
        finally:
            db.close()
    check("only the two undecided states are rewritten", changed == 2, f"rowcount {changed}")
    for uid, was in keep.items():
        want = "sold_out" if was in (State.NEW, State.SEEN) else str(was)
        check(f"{was} -> {want}", after[uid] == want, f"got {after[uid]}")
    # The returned count is what the funnel quotes, so it has to be the rows
    # actually changed rather than the rows offered.
    check("the count reports rows changed, not rows offered",
          changed != len(keep), f"{changed} of {len(keep)} offered")

    section("pipeline - a full event is recorded but never mailed")
    # TODO 1 end to end. The unit tests above cover _at_capacity, but the
    # wiring is what decides whether a full event reaches the digest, and it is
    # one list comprehension away from every one of them.
    class _FullPair:
        kind = name = "jsonld"

        def fetch(self):
            open_one = replace(_listing("jsonld", "https://luma.com/open"),
                               published_at=_OLD_PUB)
            # Same listing, same score, and the ONLY difference is the marker.
            # Anything that passes here for another reason would pass for both.
            full = replace(_listing("jsonld", "https://luma.com/full"),
                           title=_listing("jsonld", "https://luma.com/full").title
                                 + " (SOLD OUT)",
                           published_at=_OLD_PUB)
            return [open_one, full]

    with tempfile.TemporaryDirectory() as tmp_full:
        db = SqliteEventStore(Path(tmp_full) / "full.db")
        box = _Sections()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run([_FullPair()], db, build_geo(load_channels()),
                    replace(_offline, first_run_mode="report_everything"),
                    box, dry_run=False, now=_NOW)
            mailed = {u for _, urls in box.sections for u in urls}
            rows = dict(db._conn.execute(
                "SELECT canonical_url, state FROM events"))
            alerted = {r[0] for r in db._conn.execute(
                "SELECT canonical_url FROM events WHERE alerted_at != ''")}
        finally:
            db.close()
    check("the full event is not mailed, in any section including below-floor",
          "https://luma.com/full" not in mailed, f"mailed {mailed}")
    check("the open twin still is, so the withholding is the marker and "
          "not the fixture", "https://luma.com/open" in mailed, f"mailed {mailed}")
    # The point of the whole item. The ledger has to say WHY, because silence
    # reads the same as a broken source.
    check("the full event is still recorded, so the reason survives",
          "https://luma.com/full" in rows, f"rows {rows}")
    check("and its state says sold_out rather than sent or not sent",
          rows.get("https://luma.com/full") == "sold_out", f"rows {rows}")
    check("the mail record itself stays empty, since no mail was sent",
          "https://luma.com/full" not in alerted, f"alerted {alerted}")

    section("newsletter - the prose date reaches the Event")
    # start_from_prose is unit tested above; this covers the one line that
    # attaches its answer to the event, which no unit test can reach.
    _nl = NewsletterArchiveSource("dc", "h.beehiiv.com", StubHttpClient({}))
    _issue_body = (
        '<a href="https://luma.com/dated">Sign up for the workshop</a> '
        '(Monday, August 31 at 5:30pm EST) '
        '<a href="https://luma.com/undated">Another workshop</a> (sign up soon)')
    _made = {e.url: e.start for e in _nl._events_from_body(
        _issue_body, "https://h.beehiiv.com/p/i", published="2026-08-31T16:23:13Z")}
    check("an event whose prose states a date carries it",
          _made.get("https://luma.com/dated") == "2026-08-31T17:30:00-05:00",
          str(_made))
    check("and one whose prose does not is left undated rather than guessed",
          _made.get("https://luma.com/undated") == "", str(_made))

    section("pipeline - _deliver leaves the caller's lists alone")
    # Guards _deliver's sorted()-not-.sort() choice (see its comment): sorting
    # reminders/below_floor in place would silently reorder the caller's lists.
    def _entry(uid, rank, start):
        return (_event(event_uid=f"jsonld:{uid}", url=f"https://luma.com/{uid}",
                       source="jsonld", source_kind="jsonld", title=_RICH,
                       description=_RICH, start=start, location="Palo Alto, CA"),
                Score(fit=rank, access_value=1, cost=2, reason="r", method="Keyword"),
                Urgency.P2)

    with tempfile.TemporaryDirectory() as tmp_frozen:
        db = SqliteEventStore(Path(tmp_frozen) / "frozen.db")
        try:
            # Both lists start in the order _deliver's sort would NOT produce.
            reminders_in = [_entry("late", 1, "2026-09-30T18:00:00-07:00"),
                            _entry("early", 1, "2026-09-21T18:00:00-07:00")]
            below_in = [_entry("low", 1, _WHEN), _entry("high", 9, _WHEN)]
            digest = Digest([], reminders_in, [], False, below_in,
                            datetime(2026, 9, 1, tzinfo=timezone.utc))
            with contextlib.redirect_stdout(io.StringIO()):
                _deliver(digest, Funnel(), db, _Mailbox(), _offline, dry_run=True)
            check("_deliver does not reorder the reminders it was handed",
                  [e.url for e, _, _ in digest.reminders]
                  == ["https://luma.com/late", "https://luma.com/early"],
                  f"{[e.url for e, _, _ in digest.reminders]}")
            check("nor the below-floor list",
                  [e.url for e, _, _ in digest.below_floor]
                  == ["https://luma.com/low", "https://luma.com/high"],
                  f"{[e.url for e, _, _ in digest.below_floor]}")
        finally:
            db.close()

    section("pipeline - the date window says which bound cut what")
    # Same two bounds as run()'s "Name the bound" comment; this proves the
    # funnel reports each count separately rather than one combined total.
    class _Both:
        kind = name = "jsonld"

        def fetch(self):
            def one(uid, start):
                return replace(_listing("jsonld", f"https://luma.com/{uid}"),
                               event_uid=f"jsonld:{uid}", start=start)
            return [one("past", "2026-09-01T18:00:00-07:00"),
                    one("far", "2027-06-01T18:00:00-07:00"),
                    one("ok", _WHEN)]

    with tempfile.TemporaryDirectory() as tmp_win:
        db = SqliteEventStore(Path(tmp_win) / "win.db")
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                run([_Both()], db, build_geo(load_channels()), _offline,
                    _Mailbox(), dry_run=False, now=_NOW)
            line = next((ln for ln in out.getvalue().splitlines()
                         if "starts within" in ln), "")
            check("the funnel counts the already-started and the too-far apart",
                  "1 already started, 1 beyond the horizon" in line, f"line: {line!r}")
        finally:
            db.close()

    section("pipeline._stamp_naive - a stamp with no offset is not UTC")
    # The measured row this exists for: eventbrite-sf-tech-career published
    # "San Francisco Job Fair October 8, 2026" as "2026-10-08T00:00:00". Read
    # as UTC that is 2026-10-07 17:00 Pacific, so expire_past retired it the
    # evening BEFORE it happened and due_for_resweep, which takes only 'new'
    # and 'seen', could never offer the last call.
    _pac = display_zone("America/Los_Angeles")
    _naive = _event(start="2026-10-08T00:00:00", end="2026-10-08T17:00:00",
                    rsvp_deadline="2026-10-07T12:00:00",
                    published_at="2026-09-01T00:00:00")
    _fixed = _stamp_naive(_naive, _pac)
    check("the summer offset is attached and the wall time is kept",
          _fixed.start == "2026-10-08T00:00:00-07:00", f"got {_fixed.start!r}")
    check("end and rsvp_deadline are stamped too, since both decide a deadline",
          (_fixed.end, _fixed.rsvp_deadline) == ("2026-10-08T17:00:00-07:00",
                                                 "2026-10-07T12:00:00-07:00"),
          f"got {_fixed.end!r} and {_fixed.rsvp_deadline!r}")
    # Left alone on purpose: _seed_eligible compares it in whole days, so a
    # seven-hour shift cannot reach the boundary it turns on, and stamping it
    # would claim a precision the publish time never had.
    check("published_at is left naive",
          _fixed.published_at == "2026-09-01T00:00:00",
          f"got {_fixed.published_at!r}")
    check("and the day no longer moves when it is rendered",
          parse_iso(_fixed.start).astimezone(_pac).strftime("%Y-%m-%d")
          == "2026-10-08",
          "the job fair still shows the day before")
    # ZoneInfo reads the offset off the DATE. A fixed -07:00 would put a
    # January event an hour out, which is the bug display_zone already fixed on
    # the rendering side.
    _winter = _stamp_naive(_event(start="2026-01-15T09:00:00"), _pac)
    check("a winter date gets standard time, not a frozen summer offset",
          _winter.start == "2026-01-15T09:00:00-08:00", f"got {_winter.start!r}")
    # Identity, not equality: run() counts how often this fired by asking
    # whether the object came back unchanged.
    _zoned = _event(start=_WHEN)
    check("an event that already states an offset is returned unchanged",
          _stamp_naive(_zoned, _pac) is _zoned, "it was rebuilt anyway")
    check("and an undated event is too",
          _stamp_naive(_event(), _pac) is not None
          and _stamp_naive(_event(), _pac).start == "")

    class _NaiveSource:
        kind = name = "jsonld"

        def fetch(self):
            return [replace(_listing("jsonld", "https://luma.com/fair"),
                            event_uid="jsonld:fair",
                            start="2026-10-08T00:00:00")]

    with tempfile.TemporaryDirectory() as tmp_tz:
        db = SqliteEventStore(Path(tmp_tz) / "tz.db")
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run([_NaiveSource()], db, build_geo(load_channels()), _offline,
                    _Mailbox(), dry_run=False, now=_NOW)
            stored = db._conn.execute(
                "SELECT start FROM events WHERE event_uid='jsonld:fair'"
            ).fetchone()[0]
            check("what reaches the LEDGER carries the offset, not the run only",
                  stored == "2026-10-08T00:00:00-07:00", f"stored {stored!r}")
            # The whole point. SQLite compares this string itself inside
            # expire_past, so a Python-side fix would never have reached here.
            #
            # 20:00 Pacific on the 7th sits BETWEEN the two readings on
            # purpose: the naive string read as UTC started three hours ago,
            # while the stamped one is still eleven hours away. A `now` outside
            # that gap passes either way, which an earlier draft of this line
            # did until a mutation showed it surviving.
            db.expire_past("2026-10-08T03:00:00+00:00", 0)
            state = db._conn.execute(
                "SELECT state FROM events WHERE event_uid='jsonld:fair'"
            ).fetchone()[0]
            check("and SQL no longer expires it the evening before",
                  state != "expired", f"state={state}")
        finally:
            db.close()

    section("all-day - a source that gave a DAY did not give a midnight")
    # The grace both this section and the next one measure against, read from
    # the pipeline rather than restated, for the same reason expire_past takes
    # it as an argument.
    _grace = int(_START_GRACE.total_seconds() // 3600)
    # Measured on the live eventbrite feed: all 18 nodes state startDate and
    # endDate as bare dates, "2026-10-08" for "San Francisco Job Fair October
    # 8, 2026". Expanded to midnight, that row printed a 00:00 start, and the
    # start grace then retired it at midday on the day of the fair.
    check("iso_or_empty keeps a bare date bare, since midnight is not a day",
          iso_or_empty("2026-10-08") == "2026-10-08",
          f"got {iso_or_empty('2026-10-08')!r}")
    check("and a spelled-out midnight stays a moment",
          not is_date_only("2026-10-08T00:00:00"), "it was read as a day")
    check("an impossible date is still rejected, shape alone is not validity",
          iso_or_empty("2026-13-45") == "", f"got {iso_or_empty('2026-13-45')!r}")

    _allday = _stamp_naive(_event(start="2026-10-08", end="2026-10-08"), _pac)
    check("a bare date becomes local midnight and is flagged all day",
          (_allday.start, _allday.all_day) == ("2026-10-08T00:00:00-07:00", True),
          f"got {_allday.start!r}, all_day={_allday.all_day}")
    # Without this build_ics emits DTSTART == DTEND, because the source states
    # the same bare date on both sides and both stamp to the same instant.
    check("and the end becomes the last second of that local day",
          _allday.end == "2026-10-08T23:59:59-07:00", f"got {_allday.end!r}")
    check("a timed event is not flagged",
          _stamp_naive(_event(start="2026-10-08T09:00:00"), _pac).all_day is False)
    # A multi-day conference is the case one feed's start==end does not cover.
    # Taking that for a rule would cut it down to its opening day.
    _multi = _stamp_naive(_event(start="2026-10-08", end="2026-10-10"), _pac)
    check("a multi-day all-day event keeps its LAST day, not its first",
          (_multi.start, _multi.end) == ("2026-10-08T00:00:00-07:00",
                                         "2026-10-10T23:59:59-07:00"),
          f"got {_multi.start!r} -> {_multi.end!r}")
    check("and an end that precedes the start does not shorten it",
          _stamp_naive(_event(start="2026-10-08", end="2026-10-01"), _pac).end
          == "2026-10-08T23:59:59-07:00", "the end went backwards")
    # Every shape an end can arrive in beside a bare-date start; see
    # _stamp_naive for why "whichever is later" is the rule. Each row below is
    # a case where guessing the rule from the others would get it wrong.
    for _end, _want, _why in [
        ("2026-10-12",            "2026-10-12T23:59:59-07:00", "a later bare date"),
        ("2026-10-12T15:00:00",   "2026-10-12T23:59:59-07:00", "a later naive time"),
        ("2026-10-12T15:00:00-07:00", "2026-10-12T23:59:59-07:00",
         "a later time that already stated its offset"),
        ("2026-10-08T15:00:00",   "2026-10-08T23:59:59-07:00", "a time on the start's own day"),
        ("2026-10-07T15:00:00",   "2026-10-08T23:59:59-07:00", "a time before the start"),
        ("",                      "2026-10-08T23:59:59-07:00", "no end at all"),
    ]:
        _got = _stamp_naive(_event(start="2026-10-08", end=_end), _pac).end
        check(f"an all-day start with {_why} ends at {_want[:10]}",
              _got == _want, f"got {_got!r}, wanted {_want!r}")
    # The local day is the one the instant lands on, not the one the string
    # opens with; see _stamp_naive for why slicing the text would be wrong in
    # both directions, and for the "+00:00" count that makes this a real case.
    for _end, _want_day, _why in [
        ("2026-10-06T05:00:00+00:00", "2026-10-05", "a UTC end that is still the 5th here"),
        ("2026-10-05T23:30:00-11:00", "2026-10-06", "an end further west that is already the 6th"),
    ]:
        _got = _stamp_naive(_event(start="2026-10-04", end=_end), _pac).end
        check(f"{_why} ends on {_want_day}",
              _got.startswith(_want_day), f"got {_got!r}")

    # The flag has to be true of the whole row; see _stamp_naive for why
    # expire_past and _bound_reason depend on that.
    check("and every all-day end is a synthesised last second, never a stated time",
          all(_stamp_naive(_event(start="2026-10-08", end=e), _pac).end
              .endswith("23:59:59-07:00")
              for e in ("", "2026-10-12", "2026-10-12T15:00:00",
                        "2026-10-12T15:00:00-07:00", "2026-10-08T15:00:00")),
          "one of them carried a clock time through")

    # An end before the start is dropped whatever shape it arrived in,
    # including two fully zoned timestamps a source inverted itself, not only
    # a bare-date one; build_ics then falls back to its hour-after-the-start
    # default.
    for _s, _e, _why in [
        ("2026-10-08T09:00:00", "2026-10-05", "a bare date before the start"),
        ("2026-10-08T09:00:00-07:00", "2026-10-05", "the same with a stated offset"),
        ("2026-10-08T09:00:00-07:00", "2026-10-07T15:00:00-07:00",
         "two zoned times the source itself inverted"),
    ]:
        _x = _stamp_naive(_event(start=_s, end=_e), _pac)
        _ics = build_ics([_x])
        check(f"{_why} is dropped rather than inverted",
              _x.end == "" and "DTEND:20261008T170000Z" in _ics,
              f"end={_x.end!r}, {[l for l in _ics.splitlines() if l[:2] == 'DT']}")
    # A bare date in the END beside a TIMED start; see _stamp_naive for why
    # this has to be settled before the all-day branch.
    _mixed = _stamp_naive(_event(start="2026-10-08T09:00:00",
                                 end="2026-10-08"), _pac)
    check("a bare-date end beside a timed start runs to the end of that day",
          _mixed.end == "2026-10-08T23:59:59-07:00", f"got {_mixed.end!r}")
    check("so the end is never before the start",
          parse_iso(_mixed.end) > parse_iso(_mixed.start),
          f"{_mixed.start!r} -> {_mixed.end!r}")
    check("and it is not mistaken for an all-day event",
          _mixed.all_day is False, "a timed start was flagged all day")
    _mixed_ics = build_ics([_mixed])
    check("the calendar entry it produces is not inverted",
          "DTSTART:20261008T160000Z" in _mixed_ics
          and "DTEND:20261009T065959Z" in _mixed_ics, _mixed_ics)
    # Reachable, not merely constructible: the URL collapse runs BEFORE
    # _stamp_naive, so nothing is flagged all_day yet and merged_with's guard
    # against a synthesised end cannot fire. One source's timed start and
    # another's bare-date end therefore meet in one event.
    _w = _event(event_uid="jsonld:https://x/9", url="https://x/9",
                source="jsonld", source_kind="jsonld",
                start="2026-10-08T09:00:00-07:00")
    _l = _event(event_uid="wordpress:https://x/9", url="https://x/9",
                source="wordpress", source_kind="wordpress", end="2026-10-08")
    _pair_merged, _ = _collapse([_w, _l], {"jsonld": 0, "wordpress": 1},
                                lambda e: e.url)
    _joined = _stamp_naive(_pair_merged[0], _pac)
    check("a timed start and a bare-date end meeting at the URL collapse agree",
          parse_iso(_joined.end) > parse_iso(_joined.start),
          f"{_joined.start!r} -> {_joined.end!r}")

    _shown_allday = format_digest(
        [("S", [(_allday, Score(fit=5, access_value=5, cost=2), Urgency.P2)])],
        _pac, now=datetime(2026, 9, 1, tzinfo=timezone.utc))
    check("the digest prints the day and refuses to invent a clock time",
          "2026-10-08 (all day)" in _shown_allday and "00:00" not in _shown_allday,
          f"line: {[l for l in _shown_allday.splitlines() if 'when:' in l]}")
    # RFC 5545 3.6.1: a DATE value, and DTEND is exclusive, so a one-day event
    # ends on the following date. Emitting the stored instants would put a
    # 07:00-to-07:00 timed block in the calendar, which is the same false
    # precision the digest line above stopped printing.
    _ics = build_ics([_allday])
    check("the calendar says all day too, not a 07:00 block in UTC",
          "DTSTART;VALUE=DATE:20261008" in _ics
          and "DTEND;VALUE=DATE:20261009" in _ics, _ics)
    check("and a multi-day one ends the day after its last",
          "DTEND;VALUE=DATE:20261011" in build_ics([_multi]), build_ics([_multi]))
    check("while a timed event still gets an instant, not a date",
          "DTSTART:20261008T160000Z" in build_ics(
              [_stamp_naive(_event(start="2026-10-08T09:00:00"), _pac)]),
          build_ics([_stamp_naive(_event(start="2026-10-08T09:00:00"), _pac)]))

    # all_day is a property OF start, so merged_with must move the two
    # together or a winner with a real time inherits the flag.
    _timed = _event(event_uid="t", url="https://x/t", start="2026-10-08T09:00:00-07:00")
    check("merging does not paste all_day onto an event that states a time",
          _timed.merged_with(_allday).all_day is False, "it claimed to run all day")
    # The end is synthesised for a day this winner does not describe, so it
    # must not fill the winner's gap either. Only start moving takes the whole
    # occurrence across.
    check("nor the 23:59:59 end that was synthesised alongside it",
          _timed.merged_with(_allday).end == "",
          f"got {_timed.merged_with(_allday).end!r}")
    _timed_end = _event(event_uid="te", url="https://x/te",
                        start="2026-10-08T09:00:00-07:00")
    check("but a real end from a timed source still fills a gap",
          _timed.merged_with(replace(_timed_end,
                                     end="2026-10-08T11:00:00-07:00")).end
          == "2026-10-08T11:00:00-07:00", "gap filling was broken for everyone")
    _blank = _event(event_uid="b", url="https://x/b")
    check("but an empty start takes the flag along with the date it fills",
          (_blank.merged_with(_allday).start, _blank.merged_with(_allday).all_day)
          == ("2026-10-08T00:00:00-07:00", True),
          f"got {_blank.merged_with(_allday).start!r}, "
          f"all_day={_blank.merged_with(_allday).all_day}")
    # An end with no start is the only shape where taking other's start does
    # NOT already take its end through gap filling, so it is the case that
    # proves the occurrence moves as a unit rather than field by field.
    _orphan_end = _event(event_uid="o", url="https://x/o",
                         end="2026-11-30T23:00:00-08:00")
    check("and an orphan end is replaced by the occurrence it is given",
          _orphan_end.merged_with(_allday).end == "2026-10-08T23:59:59-07:00",
          f"kept {_orphan_end.merged_with(_allday).end!r}, a month after the start")
    # The documented exception to merged_with's precedence contract, pinned
    # over the three-source fold where it actually shows. The MIDDLE source
    # knows only an end; the lowest-precedence source supplies the whole
    # occurrence and takes that end with it. Keeping the middle one would pair
    # an end in November with a start on 8 October.
    # The general form of the same rule, which the two cases above only imply:
    # taking a start replaces the end even when the incoming occurrence states
    # none, because an end kept from a start we did not take would describe
    # something else. Blank is the honest answer, not the old value.
    _endless = _event(event_uid="ne", url="https://x/ne",
                      start="2026-10-08T09:00:00-07:00")
    check("taking a start with no end leaves no end, rather than the orphan's",
          _orphan_end.merged_with(_endless).end == "",
          f"kept {_orphan_end.merged_with(_endless).end!r}")

    _blank_top = _event(event_uid="p0", url="https://x/p0")
    _folded = _blank_top.merged_with(_orphan_end).merged_with(_allday)
    check("a fold lets a complete occurrence replace a higher-ranked orphan end",
          (_folded.start, _folded.end, _folded.all_day)
          == ("2026-10-08T00:00:00-07:00", "2026-10-08T23:59:59-07:00", True),
          f"got {_folded.start!r} -> {_folded.end!r}, all_day={_folded.all_day}")

    with tempfile.TemporaryDirectory() as tmp_ad:
        db = SqliteEventStore(Path(tmp_ad) / "allday.db")
        try:
            db.upsert(_allday)
            check("the column persists",
                  bool(db._conn.execute(
                      "SELECT all_day FROM events").fetchone()[0]),
                  "upsert did not write it")
            # Through due_for_resweep, the only caller of _row_to_event, so
            # the READ path is covered too: writing the column and dropping it
            # on the way back out looks identical from the table.
            db.mark_alerted([_allday.event_uid], at="2026-09-01T00:00:00+00:00")
            _due = db.due_for_resweep(48, "2026-10-07T00:00:00+00:00", min_gap_hours=24)
            check("and rehydrating an Event brings the flag back with it",
                  len(_due) == 1 and _due[0].all_day is True,
                  f"got {[(e.event_uid, e.all_day) for e in _due]}")
            # 16:00 Pacific on the day of the fair, chosen so the two answers
            # differ. The start is 07:00 UTC and the grace puts the cutoff at
            # 11:00 UTC, so an unshifted row is already past it while the
            # shifted one is not. A `now` of 19:00 UTC, the obvious "midday",
            # lands the cutoff exactly ON the start and passes either way,
            # which is what a mutation caught.
            db.expire_past("2026-10-08T23:00:00+00:00", _grace)
            state = db._conn.execute("SELECT state FROM events").fetchone()[0]
            check("and SQL keeps it alive at midday on the day itself",
                  state != "expired", f"state={state}")
            db.expire_past("2026-10-09T20:00:00+00:00", _grace)
            state = db._conn.execute("SELECT state FROM events").fetchone()[0]
            check("while the day after, past the grace, it is over",
                  state == "expired", f"state={state}")
        finally:
            db.close()

    # Through the LIFECYCLE, not just the stamp. _stamp_naive and build_ics
    # both understood a date range while expire_past and _bound_reason only
    # knew the start, so a three-day conference was retired on its second
    # morning. Reading the synthesised end in both is what closes that.
    with tempfile.TemporaryDirectory() as tmp_md:
        db = SqliteEventStore(Path(tmp_md) / "multi.db")
        try:
            db.upsert(_multi)
            def _md_state(now):
                db.expire_past(now, _grace)
                return db._conn.execute("SELECT state FROM events").fetchone()[0]
            check("a three-day event is still live on its second day",
                  _md_state("2026-10-09T23:00:00+00:00") != "expired",
                  "it was retired on day two")
            check("and on its third",
                  _md_state("2026-10-10T23:00:00+00:00") != "expired",
                  "it was retired on the closing day")
            check("but not two days after the last one",
                  _md_state("2026-10-12T23:00:00+00:00") == "expired",
                  "it outlived its own range")
        finally:
            db.close()
    _day2 = datetime(2026, 10, 9, 23, tzinfo=timezone.utc)
    check("_bound_reason keeps a multi-day event on its second day too",
          _bound_reason(_multi, _day2, _day2 + timedelta(days=120)) is None,
          "the date window cut it a day in")
    # Same shift, same instant, same reason, now on the filter side, so the
    # filter and the ledger cannot disagree.
    _pm = datetime(2026, 10, 8, 23, tzinfo=timezone.utc)
    check("_bound_reason keeps an all-day event through its own day",
          _bound_reason(_allday, _pm, _pm + timedelta(days=120)) is None,
          "the date window cut it while the doors were open")
    check("and a timed event that started 16h ago is still cut",
          _bound_reason(_event(start="2026-10-08T07:00:00+00:00"), _pm,
                        _pm + timedelta(days=120)) is not None,
          "the grace was widened for everything, not just all-day events")
    # A timed event is cut the moment it starts, with or without a stated end,
    # since one digest offered an event six hours after it began.
    _started_1h = datetime(2026, 10, 8, 8, tzinfo=timezone.utc)
    _hz = _started_1h + timedelta(days=120)
    check("a timed event that started an hour ago is cut",
          _bound_reason(_event(start="2026-10-08T07:00:00+00:00"),
                        _started_1h, _hz) == pipeline._STARTED,
          "it was kept though it had already begun")
    check("and so is one whose stated end is still ahead",
          _bound_reason(_event(start="2026-10-08T07:00:00+00:00",
                               end="2026-10-08T10:00:00+00:00"),
                        _started_1h, _hz) == pipeline._STARTED,
          "a stated end kept a started event in the digest")
    check("while one not yet started is kept",
          _bound_reason(_event(start="2026-10-08T09:00:00+00:00"),
                        _started_1h, _hz) is None,
          "an upcoming event was cut")

    section("store.expire_past - the ledger and the digest agree on 'started'")
    # _bound_reason keeps a started event for _START_GRACE and expire_past used
    # to keep it for nothing, so a run could still offer an event in the digest
    # and mark the same row expired. One constant now answers for both.
    with tempfile.TemporaryDirectory() as tmp_gr:
        db = SqliteEventStore(Path(tmp_gr) / "grace.db")
        try:
            db.upsert(_event(event_uid="fresh", url="https://x/fresh",
                             start="2026-09-20T06:00:00+00:00"))
            db.upsert(_event(event_uid="stale", url="https://x/stale",
                             start="2026-09-19T20:00:00+00:00"))
            # A literal 12, not _grace. This half proves the store honours
            # whatever grace it is handed, which a grace of 0 cannot show.
            db.expire_past("2026-09-20T12:00:00+00:00", 12)
            def _state(uid):
                return db._conn.execute(
                    "SELECT state FROM events WHERE event_uid=?",
                    (uid,)).fetchone()[0]
            check("the store keeps an event 6h into a 12h grace",
                  _state("fresh") != "expired", f"state={_state('fresh')}")
            check("and retires one 16h in",
                  _state("stale") == "expired", f"state={_state('stale')}")
        finally:
            db.close()
    check("and the grace the pipeline hands over is none at all",
          _grace == 0, f"_START_GRACE is {_START_GRACE}")

    # Through run(), because the store honouring a grace it is handed proves
    # nothing about the pipeline handing over the right one. The measured shape
    # was an event still sent in the digest while the same run's expire_past
    # retired it. With no grace the two meet at the start itself, so one event
    # sits just before now and one just after.
    class _AroundNow:
        kind = name = "jsonld"

        def fetch(self):
            return [replace(_listing("jsonld", "https://luma.com/started"),
                            event_uid="jsonld:started",
                            start="2026-09-19T23:00:00+00:00",
                            end="2026-09-20T03:00:00+00:00"),
                    replace(_listing("jsonld", "https://luma.com/upcoming"),
                            event_uid="jsonld:upcoming",
                            start="2026-09-20T01:00:00+00:00")]

    with tempfile.TemporaryDirectory() as tmp_run:
        db = SqliteEventStore(Path(tmp_run) / "started.db")
        box = _Mailbox()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run([_AroundNow()], db, build_geo(load_channels()), _offline,
                    box, dry_run=False, now=_NOW)
            row = db._conn.execute(
                "SELECT state FROM events WHERE event_uid='jsonld:upcoming'"
            ).fetchone()
            check("an event an hour away is mailed and not expired",
                  "https://luma.com/upcoming" in box.mailed
                  and row is not None and row[0] != "expired",
                  f"mailed={box.mailed}, state={row and row[0]}")
            check("while one that began an hour ago is not mailed",
                  "https://luma.com/started" not in box.mailed,
                  f"mailed={box.mailed}")
        finally:
            db.close()

    section("pipeline._deliver - a heading names the cut it actually makes")
    # Measured on one run: the heading promised "within 72h" and was empty,
    # while 10 of the 11 events below it started inside 72 hours, the nearest
    # in 13. The split is Urgency.P0, which needs p0_min_rank as well as the
    # clock, so the heading was describing a partition it does not perform.
    with tempfile.TemporaryDirectory() as tmp_head:
        db = SqliteEventStore(Path(tmp_head) / "head.db")
        box = _Sections()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                _deliver(Digest([(_event(start=_WHEN),
                                  Score(fit=5, access_value=5, cost=2),
                                  Urgency.P2)], [], [], False, [],
                                 datetime(2026, 9, 1, tzinfo=timezone.utc)),
                         Funnel(), db, box, _offline, dry_run=True)
            heads = [head for head, _ in box.sections]
            check("a P1 or P2 event is not filed under a bare time heading",
                  not any(h.startswith("CLOSING SOON") for h in heads),
                  f"headings {heads}")
            check("the sections say they are picks, not a clock",
                  heads == ["TOP PICKS - closing within 72h", "OTHER PICKS",
                            "LAST CALL - starts within 72h, sent once before, "
                            "and this is the only reminder"],
                  f"headings {heads}")
        finally:
            db.close()

    section("pipeline._deliver - TOP PICKS is ordered like OTHER PICKS")
    # A digest has printed TOP PICKS ranks as 48, 48, 42, 48, because only
    # OTHER PICKS was sorted while TOP PICKS kept fetch order.
    with tempfile.TemporaryDirectory() as tmp_top:
        db = SqliteEventStore(Path(tmp_top) / "top.db")
        box = _Sections()
        _late = "2026-09-24T09:00:00-07:00"
        _soon = "2026-09-23T09:00:00-07:00"
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                _deliver(Digest([
                    (_event(event_uid="x:a", url="https://luma.com/a", start=_soon),
                     Score(fit=7, access_value=6, cost=2), Urgency.P0),
                    (_event(event_uid="x:b", url="https://luma.com/b", start=_late),
                     Score(fit=8, access_value=6, cost=2), Urgency.P0),
                    (_event(event_uid="x:c", url="https://luma.com/c", start=_soon),
                     Score(fit=8, access_value=6, cost=2), Urgency.P0)],
                    [], [], False, [], datetime(2026, 9, 1, tzinfo=timezone.utc)),
                    Funnel(), db, box, _offline, dry_run=True)
            urls = next(u for head, u in box.sections if head.startswith("TOP PICKS"))
            check("_deliver orders TOP PICKS by rank, then by the clock",
                  urls == ["https://luma.com/c", "https://luma.com/b",
                           "https://luma.com/a"],
                  f"order {urls}")
        finally:
            db.close()

    section("pipeline - the last-call sweep, once and only once")
    # See run()'s step-5 comment for why the sweep exists. Run 2 below fetches
    # nothing at all -- only the ledger still remembers the event.
    _T0 = datetime(2026, 9, 20, tzinfo=timezone.utc)
    _SOON = "2026-09-30T18:00:00-07:00"

    def _later(hours):
        return _T0 + timedelta(hours=hours)

    class _Once:
        kind = name = "jsonld"

        def fetch(self):
            return [_event(event_uid="jsonld:https://luma.com/soon",
                           url="https://luma.com/soon", source=self.kind,
                           source_kind=self.kind, title=_RICH, description=_RICH,
                           start=_SOON, location="Palo Alto, CA",
                           published_at="2026-09-19T00:00:00")]

    with tempfile.TemporaryDirectory() as tmp_sweep:
        db = SqliteEventStore(Path(tmp_sweep) / "sweep.db")
        box = _Mailbox()
        try:
            def _run(sources, when):
                with contextlib.redirect_stdout(io.StringIO()):
                    run(sources, db, build_geo(load_channels()), _offline, box,
                        dry_run=False, now=when)

            _run([_Once()], _T0)
            first = len(box.mailed)
            check("the event is reported when first seen", first == 1,
                  f"mailed {box.mailed}")
            # Ten days on, an hour before it starts, and the source has dropped
            # it. Only the ledger still knows.
            _run([], _later(239))
            check("it comes back as a last call once the start is close",
                  len(box.mailed) == first + 1, f"mailed {box.mailed}")
            _run([], _later(240))
            check("and never a second time, or an hourly schedule would send "
                  "one reminder per hour of the window",
                  len(box.mailed) == first + 1, f"mailed {box.mailed}")
        finally:
            db.close()

    # Through run(), because the store query only sees alerted_at, and the
    # pipeline is what stamps it. Stamped with the wall clock instead of the
    # run's own `now`, this passes or fails depending on the day check.py runs.
    class _Late:
        kind = name = "jsonld"

        def fetch(self):
            return [_event(event_uid="jsonld:https://luma.com/late",
                           url="https://luma.com/late", source=self.kind,
                           source_kind=self.kind, title=_RICH, description=_RICH,
                           start="2026-09-30T16:00:00-07:00",
                           location="Palo Alto, CA",
                           published_at="2026-09-28T00:00:00")]

    with tempfile.TemporaryDirectory() as tmp_late:
        db = SqliteEventStore(Path(tmp_late) / "late.db")
        box = _Mailbox()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run([_Once()], db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_T0)
                # 63h before its start, so the first mail is already inside
                # the 72h window. A different start from _Once's, or the
                # title+time key would treat the two as one event.
                run([_Late()], db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_later(200))
                run([], db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_later(239))
            check("an event first mailed inside the window is mailed once, "
                  "never again as a last call",
                  box.mailed.count("https://luma.com/late") == 1,
                  f"mailed {box.mailed}")
            check("while the event first mailed ten days out still gets one",
                  box.mailed.count("https://luma.com/soon") == 2,
                  f"mailed {box.mailed}")
        finally:
            db.close()

    section("pipeline - the funnel names never-emailed, not new")
    # Dry runs mail nothing, so the same event stays in the count run after run.
    # The note is what separates it from an event this run saw for the first time.
    with tempfile.TemporaryDirectory() as tmp_label:
        db = SqliteEventStore(Path(tmp_label) / "label.db")
        try:
            outs = []
            for _ in range(2):
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    run([_Once()], db, build_geo(load_channels()), _offline,
                        _Mailbox(), dry_run=True, now=_T0)
                outs.append(next((line for line in buf.getvalue().splitlines()
                                  if "never emailed before" in line), ""))
            # "never emailed before  <count>  <delta>  <n> first seen this run"
            check("the first dry run counts the event as first seen",
                  outs[0].split()[3:4] == ["1"] and "1 first seen this run" in outs[0],
                  f"line {outs[0]!r}")
            check("the second still counts it as never emailed, but not first seen",
                  outs[1].split()[3:4] == ["1"] and "0 first seen this run" in outs[1],
                  f"line {outs[1]!r}")
        finally:
            db.close()

    section("notifier - --show-digest prints exactly the body that was sent")
    # The printout is only evidence of what was mailed if it is the same string,
    # not a second rendering of the same sections.
    class _FakeSmtp:
        sent = []

        def __init__(self, host, port):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def starttls(self, context=None):
            pass

        def login(self, user, password):
            pass

        def send_message(self, message):
            _FakeSmtp.sent.append(message)

    real_smtp = notifier_module.smtplib.SMTP
    notifier_module.smtplib.SMTP = _FakeSmtp
    try:
        _sections = [("OTHER PICKS", [(_event(title="Echoed", start=_SOON),
                                        Score(fit=5, access_value=5, cost=2),
                                        Urgency.P1)])]
        for echo in (True, False):
            _FakeSmtp.sent.clear()
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                EmailNotifier("u@example.test", "pw", "t@example.test",
                              "America/Los_Angeles", echo=echo).send(
                    _sections, "[Event Scout] 1 events", "footer text")
            mailed = _FakeSmtp.sent[0].get_body(("plain",)).get_content().strip()
            if echo:
                check("the printed body is the mailed body, footer included",
                      mailed in buf.getvalue() and "footer text" in mailed,
                      f"printed {buf.getvalue()[:200]!r}")
            else:
                check("and without the flag a send prints nothing",
                      buf.getvalue() == "", f"printed {buf.getvalue()[:200]!r}")
    finally:
        notifier_module.smtplib.SMTP = real_smtp
    # Read from the source, like the main() checks below, because running
    # local_run.main would fetch every live source and send real mail.
    _lr_main = next(n for n in ast.walk(ast.parse(
        (Path(__file__).parent / "local_run.py").read_text(encoding="utf-8")))
        if isinstance(n, ast.FunctionDef) and n.name == "main")
    _flag_read = any(isinstance(n, ast.Compare) and isinstance(n.left, ast.Constant)
                     and n.left.value == "--show-digest" for n in ast.walk(_lr_main))
    _echo_kw = [kw.value for n in ast.walk(_lr_main)
                if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "EmailNotifier"
                for kw in n.keywords if kw.arg == "echo"]
    check("local_run reads --show-digest and hands it to EmailNotifier as echo",
          _flag_read and len(_echo_kw) == 1 and isinstance(_echo_kw[0], ast.Name)
          and _echo_kw[0].id == "show_digest",
          f"flag read {_flag_read}, echo args {[ast.dump(v) for v in _echo_kw]}")

    # "You have not acted on it" vs state 'new' is EventStore.due_for_resweep's
    # third condition -- both the below-floor and the absorbed-alias paths to
    # a false 'new' were measured here, and the second puts one real event in
    # the mail twice.
    _DULL = "Friday poker night, cards and drinks with friends"
    # Pinned to seed_plus_recent, not left on whatever config.yaml says. Under
    # report_everything a first run deliberately DOES mail the dull event, in
    # its own below-floor section, so on the shipped config this block would be
    # asserting the opposite of the intended behaviour. What it is really about
    # is due_for_resweep, and that has to hold under either mode.
    _SEEDING = replace(_offline, first_run_mode="seed_plus_recent")

    def _listing_at(kind, url, title, body, start):
        return _event(event_uid=f"{kind}:{url}", url=url, source=kind,
                      source_kind=kind, title=title, description=body,
                      start=start, location="Palo Alto, CA",
                      published_at="2026-09-19T00:00:00")

    class _Mixed:
        def __init__(self, kind, items):
            self.kind = self.name = kind
            self._items = items

        def fetch(self):
            return [_listing_at(self.kind, *item) for item in self._items]

    with tempfile.TemporaryDirectory() as tmp_mix:
        db = SqliteEventStore(Path(tmp_mix) / "mix.db")
        box = _Mailbox()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run([_Mixed("jsonld", [("https://luma.com/win", _RICH, _RICH, _SOON),
                                       ("https://luma.com/poker", _DULL, _DULL, _SOON)]),
                     _Mixed("newsletter", [("https://host.example/alias", _RICH,
                                            _RICH, _SOON)])],
                    db, build_geo(load_channels()), _SEEDING, box,
                    dry_run=False, now=_T0)
            first = list(box.mailed)
            check("the merge mails the winner only, and the dull event not at all",
                  first == ["https://luma.com/win"], f"mailed {first}")
            with contextlib.redirect_stdout(io.StringIO()):
                run([], db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_later(239))
            last_call = box.mailed[len(first):]
            check("the last call holds the mailed event and nothing else",
                  last_call == ["https://luma.com/win"],
                  f"last call {last_call}")
        finally:
            db.close()

    # The third way one real event can reach one email twice: a DIFFERENT
    # source newly starts carrying it while the old row is due a last call (see
    # run()'s "Two exclusions" comment for why the ledger can't spot this alone).
    with tempfile.TemporaryDirectory() as tmp_cross:
        db = SqliteEventStore(Path(tmp_cross) / "cross.db")
        box = _Mailbox()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run([_Mixed("jsonld", [("https://luma.com/both", _RICH, _RICH, _SOON)])],
                    db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_T0)
            first = list(box.mailed)
            # The original source drops it; a newsletter picks it up under a new URL.
            with contextlib.redirect_stdout(io.StringIO()):
                run([_Mixed("newsletter", [("https://host.example/both-late",
                                            _RICH, _RICH, _SOON)])],
                    db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_later(239))
            second = box.mailed[len(first):]
            check("a new listing of an event already due a last call sends one "
                  "entry, not two", len(second) == 1, f"mailed {second}")
        finally:
            db.close()

    # cleared_floor_at is what makes an event stop being new (see run()'s
    # "Computed after the loop" comment for why writing it early would turn a
    # refused SMTP call into permanent silence).
    class _Refused:
        def send(self, sections, subject, footer=""):
            raise RuntimeError("SMTP said no")

    with tempfile.TemporaryDirectory() as tmp_fail:
        db = SqliteEventStore(Path(tmp_fail) / "fail.db")
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                try:
                    run([_Mixed("jsonld", [("https://luma.com/smtp", _RICH,
                                            _RICH, _SOON)])],
                        db, build_geo(load_channels()), _offline, _Refused(),
                        dry_run=False, now=_T0)
                    raised = False
                except RuntimeError:
                    raised = True
            check("a refused send is not swallowed", raised)
            check("and nothing is recorded as reported", not db.reported_urls(),
                  f"{sorted(db.reported_urls())}")
            box = _Mailbox()
            with contextlib.redirect_stdout(io.StringIO()):
                run([_Mixed("jsonld", [("https://luma.com/smtp", _RICH,
                                        _RICH, _SOON)])],
                    db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_T0)
            check("so the next run still sends it",
                  box.mailed == ["https://luma.com/smtp"], f"mailed {box.mailed}")
        finally:
            db.close()

    # Nothing forces the two listings to arrive close together. When the alias
    # turns up far outside the last-call window, due_for_resweep never sees the
    # old row, so the only thing that can recognise it is the ledger's own
    # record of what has been reported -- by NAME and start, not by URL.
    with tempfile.TemporaryDirectory() as tmp_far:
        db = SqliteEventStore(Path(tmp_far) / "far.db")
        box = _Mailbox()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run([_Mixed("jsonld", [("https://luma.com/far", _RICH, _RICH, _SOON)])],
                    db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_T0)
            first = list(box.mailed)
            with contextlib.redirect_stdout(io.StringIO()):
                run([_Mixed("newsletter", [("https://host.example/far-alias",
                                            _RICH, _RICH, _SOON)])],
                    db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_later(24))
            check("an alias arriving days early is not news either",
                  box.mailed == first, f"mailed {box.mailed[len(first):]}")
        finally:
            db.close()

    # Two aliases can both end up reported when the second was mailed with no
    # start yet. Seeded directly -- reaching this through run() takes four
    # rounds just to set up (see run()'s "Accumulated, not fixed up front"
    # comment for why the sweep needs this).
    with tempfile.TemporaryDirectory() as tmp_pair:
        db = SqliteEventStore(Path(tmp_pair) / "pair.db")
        box = _Mailbox()
        try:
            for kind, url in (("jsonld", "https://luma.com/twin"),
                              ("newsletter", "https://host.example/twin")):
                uid = f"{kind}:{url}"
                db.upsert(_listing_at(kind, url, _RICH, _RICH, _SOON),
                          Score(9, 9, 1), Urgency.P2)
                db.mark_alerted([uid], at=_T0.isoformat())
                db.mark_cleared_floor([uid])
            db.save()
            with contextlib.redirect_stdout(io.StringIO()):
                run([], db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_later(239))
            check("two reported aliases produce one last call, not two",
                  len(box.mailed) == 1, f"mailed {box.mailed}")
        finally:
            db.close()

    # A reader who already dealt with it must not be nudged (State.silences_sweeper).
    with tempfile.TemporaryDirectory() as tmp_done:
        db = SqliteEventStore(Path(tmp_done) / "done.db")
        box = _Mailbox()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                run([_Once()], db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_T0)
            db.set_state("jsonld:https://luma.com/soon", State.REGISTERED)
            db.save()
            before = len(box.mailed)
            with contextlib.redirect_stdout(io.StringIO()):
                run([], db, build_geo(load_channels()), _offline, box,
                    dry_run=False, now=_later(239))
            check("an event the reader already registered for is not swept",
                  len(box.mailed) == before, f"mailed {box.mailed}")
        finally:
            db.close()

    section("models.iso_or_empty - two parsers have to agree on every start")
    # See iso_or_empty's docstring for the mechanism. These are the concrete
    # forms that divide the two parsers in practice.
    iso_cases = [
        ("an offset timestamp", "2026-09-09T17:30:00-07:00", True),
        ("milliseconds from schema.org markup", "2026-09-11T09:00:00.000-07:00", True),
        ("a Z suffix", "2026-09-11T09:00:00Z", True),
        ("an ISO week date Python takes and SQLite does not", "2026-W37-3", True),
        ("a compact form Python takes and SQLite does not", "20260909T180000", True),
        ("prose", "next Thursday", False),
        ("an impossible date", "2026-13-45", False),
        ("nothing at all", "", False),
    ]
    probe = sqlite3.connect(":memory:")
    try:
        for why, raw, want_kept in iso_cases:
            out = iso_or_empty(raw)
            check(f"iso_or_empty keeps {why}" if want_kept
                  else f"iso_or_empty rejects {why}",
                  bool(out) is want_kept, f"{raw!r} -> {out!r}")
            if out:
                readable = probe.execute("SELECT datetime(?)", (out,)).fetchone()[0]
                check(f"and SQLite can compare it: {why}", readable is not None,
                      f"datetime({out!r}) is NULL")
    finally:
        probe.close()

    # The adapters that read third-party fields must go through it, or the row
    # reaches the ledger with a start no comparison can read.
    bad_markup = StubHttpClient({"http://t/": '''<script type="application/ld+json">
    {"@type":"Event","name":"Placeholder","url":"https://t/e",
     "startDate":"YYYY-MM-DDTHH:MM:SS"}</script>'''})
    parsed = JsonLdSource("stub", "http://t/", bad_markup).fetch()
    check("a placeholder startDate is dropped, not stored",
          bool(parsed) and not parsed[0].start,
          f"stored {parsed[0].start!r}" if parsed else "no event parsed")

    # url has had this construction check since an adapter forgot canon_url;
    # start/end/rsvp_deadline/published_at need the same guard now that
    # iso_or_empty exists (see Event.__post_init__; published_at's reason
    # there differs from the other three's).
    for field, bad in (("start", "2026-09-20T18:00:00.000-07:00"),
                       ("end", "20260920T180000"),
                       ("rsvp_deadline", "next Friday"),
                       # The real shape: five ledger rows arrived from beehiiv
                       # with a Z suffix before the source gate existed.
                       ("published_at", "2026-08-31T16:23:13Z")):
        try:
            _event(**{field: bad})
            raised = False
        except ValueError:
            raised = True
        check(f"an unnormalised {field} cannot be constructed", raised,
              f"{bad!r} was accepted")
    check("a normalised one still can",
          _event(start="2026-09-20T18:00:00-07:00").start
          == "2026-09-20T18:00:00-07:00")

    # Same crash scoring.py's _hours_until comment fixes: parse_iso reports
    # failure rather than raising, and a stale try/except let None fall
    # through to when.tzinfo. Exercised on a stand-in because Event itself
    # now refuses to hold a stamp this shape.
    class _Undated:
        rsvp_deadline = "not-a-date"
        start = ""

    try:
        hours = DeadlineUrgency(72, 40, 9, "P1")._hours_until(
            _Undated(), datetime(2026, 9, 20, tzinfo=timezone.utc))
        crashed = False
    except Exception as exc:
        hours, crashed = None, f"{type(exc).__name__}: {exc}"
    check("an unreadable deadline is reported, not raised",
          crashed is False and hours is None, str(crashed))

    section("notifier - the digest has to say how soon, not just when")
    # Every _lead_time branch, rounding included (see its docstring for why).
    lead_cases = [
        ("tonight", "2026-09-09T17:30:00-07:00", "in 8h"),
        ("tomorrow evening", "2026-09-10T18:00:00-07:00", "in 33h"),
        ("beyond two days, in whole days", "2026-09-17T10:00:00-07:00", "in 8 days"),
        ("minutes away", "2026-09-09T16:30:00+00:00", "in under an hour"),
        ("already begun", "2026-09-09T10:00:00+00:00", "already started"),
        ("no start at all", "", ""),
    ]
    _at = datetime(2026, 9, 9, 16, tzinfo=timezone.utc)
    for why, start, want in lead_cases:
        check(f"lead time for an event {why}", _lead_time(start, _at) == want,
              f"got {_lead_time(start, _at)!r}, expected {want!r}")

    section("pipeline - every source failing is not a quiet week")
    # Same judgement as run()'s raise; see its comment for why.
    class _Dead:
        kind = "jsonld"

        def __init__(self, name):
            self.name = name

        def fetch(self):
            raise RuntimeError(f"{self.name} is unreachable")

    class _Alive:
        kind = "jsonld"
        name = "alive"

        def fetch(self):
            return [_listing("jsonld", "https://luma.com/alive")]

    def _run_with(srcs):
        with tempfile.TemporaryDirectory() as tmp_dead:
            db = SqliteEventStore(Path(tmp_dead) / "dead.db")
            out = io.StringIO()
            try:
                with contextlib.redirect_stdout(out):
                    run(srcs, db, build_geo(load_channels()), _offline,
                        _Mailbox(), dry_run=True, now=_NOW)
                return None, out.getvalue()
            except RuntimeError as exc:
                return exc, out.getvalue()
            finally:
                db.close()

    exc, printed = _run_with([_Dead("a"), _Dead("b")])
    check("every source failing raises", exc is not None, "it returned normally")
    check("and the message names each one, so the log is not the only record",
          exc is not None and "a (RuntimeError" in str(exc)
          and "b (RuntimeError" in str(exc), f"message was {exc}")
    check("the funnel is printed BEFORE the raise, since the per-source errors "
          "are the whole diagnosis",
          "ERROR RuntimeError" in printed, "the diagnosis was discarded")

    exc, _ = _run_with([_Dead("a"), _Alive()])
    check("one source failing beside a working one does NOT raise",
          exc is None, f"raised {exc}")

    exc, _ = _run_with([])
    check("no sources configured at all does not raise either, because that is "
          "a configuration question rather than an outage",
          exc is None, f"raised {exc}")

    # BOTH entry points, because run() grew this contract for one of them and
    # the other was left to traceback. Widening a shared function's exceptions
    # is a change every caller has to answer, so the check covers every caller
    # rather than the one that prompted it. Read syntactically, since neither
    # main() runs without secrets, a network and a ledger.
    _root = Path(__file__).resolve().parent
    for entry in ("run.py", "local_run.py"):
        src_text = (_root / entry).read_text(encoding="utf-8")
        main_fn = next(n for n in ast.walk(ast.parse(src_text))
                       if isinstance(n, ast.FunctionDef) and n.name == "main")
        handlers = [h for n in ast.walk(main_fn) if isinstance(n, ast.Try)
                    for h in n.handlers]
        for name in ("AllSourcesFailedError", "AllScoringFailedError"):
            check(f"{entry}'s main catches {name}",
                  any(isinstance(h.type, ast.Name)
                      and h.type.id == name for h in handlers),
                  "nothing catches it, so a total outage ends in a traceback "
                  "rather than an explained zero")
        # The LAST return, not "any return that is 1". run.py's main already
        # returns 1 for missing secrets, so a check for any return of 1 kept
        # passing even after the final conditional return was mutated to
        # `return 0` -- a gap only mutation testing caught, since the final
        # return is the one a completed run actually reaches.
        returns = sorted((n for n in ast.walk(main_fn)
                          if isinstance(n, ast.Return)), key=lambda n: n.lineno)
        check(f"{entry}'s main has a FINAL return that is conditional, so a "
              f"total outage cannot exit 0",
              returns and not isinstance(returns[-1].value, ast.Constant),
              f"it returns the constant "
              f"{getattr(returns[-1].value, 'value', None)!r}, so the handler "
              f"above it changes nothing")
    # Only the cloud entry point needs the annotation. A local run is read by a
    # person watching the terminal, where a plain line is what is wanted.
    check("run.py emits a GitHub annotation, not just a plain print",
          "::error::" in (_root / "run.py").read_text(encoding="utf-8"),
          "the message would land in a job log that needs admin rights to read")
    check("the exception is its own type, so an unrelated RuntimeError cannot "
          "be labelled a source outage",
          issubclass(AllSourcesFailedError, RuntimeError)
          and AllSourcesFailedError is not RuntimeError,
          "a bare RuntimeError would catch the notifier's credential check and "
          "the coverage-before-fetch invariant too")

    section("A total SCORING outage is not a quiet week either")
    # Same shape as the outage above, one stage later: _score_all used to
    # drop every failure as None and leave zero sent behind a green exit.
    # See AllScoringFailedError's docstring for why it needs its own type.
    check("AllScoringFailedError is NOT a subclass of AllSourcesFailedError, "
          "because the two name opposite causes",
          issubclass(AllScoringFailedError, RuntimeError)
          and not issubclass(AllScoringFailedError, AllSourcesFailedError),
          "one handler for both would point the reader at the network when "
          "the model tier is what is down")

    _f = Funnel()
    check("a funnel that never reached the scorer does not claim an outage",
          not _f.all_scoring_failed(), "an empty run would read as a model outage")
    _f.scored(0, 0)
    check("nor does one where the filters legitimately emptied first",
          not _f.all_scoring_failed(), "a quiet week would read as an outage")
    _f = Funnel()
    _f.scored(4, 1)
    check("three failures out of four is not a total outage",
          not _f.all_scoring_failed(), "a partial failure would exit non-zero")
    _f = Funnel()
    _f.scored(4, 0)
    check("four out of four is", _f.all_scoring_failed(), "nothing would notice")

    class _DeadScorer:
        cacheable = False
        method_label = "dead"

        def score(self, event):
            raise RuntimeError("model down")

    def _run_scoring_dead(srcs):
        """run() with every score() raising, and the real build_scorer replaced.

        Patched on the pipeline module rather than passed in, because run()
        chooses its own tier from settings and there is no seam for one here.
        A seam added purely for this test would be an extension point with no
        production caller, which this codebase has rejected before.
        """
        with tempfile.TemporaryDirectory() as tmp_ds:
            db = SqliteEventStore(Path(tmp_ds) / "ds.db")
            original = pipeline.build_scorer
            pipeline.build_scorer = lambda settings, store=None: _DeadScorer()
            out = io.StringIO()
            try:
                with contextlib.redirect_stdout(out):
                    run(srcs, db, build_geo(load_channels()), _offline,
                        _Mailbox(), dry_run=True, now=_NOW)
                return None, out.getvalue()
            except RuntimeError as exc:
                return exc, out.getvalue()
            finally:
                pipeline.build_scorer = original
                db.close()

    exc, printed = _run_scoring_dead([_OneListing("jsonld", "https://luma.com/ds")])
    check("every score failing raises rather than returning a green zero",
          isinstance(exc, AllScoringFailedError), f"it returned {exc!r}")
    check("and the message carries the counts and the tier, so the reader "
          "knows which half of the pipeline is down",
          exc is not None and "1/1" in str(exc) and "dead" in str(exc),
          f"message was {exc}")
    # Same "raised after delivery" reasoning as AllScoringFailedError's
    # docstring; this proves _deliver actually ran first.
    check("the funnel and the RESULT block are printed before the raise",
          "RESULT" in printed, "the diagnosis was discarded")

    exc, _ = _run_scoring_dead([])
    check("a run with nothing to score does not raise",
          exc is None, f"raised {exc}")

    # The other half of a non-zero exit. run.py's finally exports the ledger on
    # the way out so scored events are not re-scored, but a workflow step
    # defaults to `if: success()`, so the commit that PUBLISHES that export was
    # being skipped on exactly the runs the export exists for. Asserted against
    # the real workflow, because none of it has ever executed (item 7).
    _wf = yaml.safe_load(
        (_root / ".github" / "workflows" / "scan.yml").read_text(encoding="utf-8"))
    _steps = _wf["jobs"]["scan"]["steps"]
    _scan = next(st for st in _steps if st.get("name") == "Run scan")
    _commit = next(st for st in _steps
                   if st.get("name") == "Save private ledger")
    check("the scan step carries an id the commit step can key on",
          _scan.get("id") == "scan", f"id is {_scan.get('id')!r}")
    check("the ledger commit still runs when the scan FAILED, or a non-zero "
          "exit silently discards the export run.py made on purpose",
          "cancelled()" in str(_commit.get("if"))
          and "steps.scan" in str(_commit.get("if")),
          f"its condition is {_commit.get('if')!r}")
    check("and it is not a bare always(), which would fire when the scan never "
          "ran and .ledger holds nothing",
          str(_commit.get("if")).strip() != "${{ always() }}",
          "a checkout failure would then try to commit an empty directory")

    section("pipeline._start_order - the digest is ordered by the clock")
    # Same instants and the six-row consequence as _start_order's docstring.
    _earlier = "2026-09-08T12:30:00+00:00"   # 12:30 UTC
    _later = "2026-09-08T09:00:00-07:00"     # 16:00 UTC
    check("the text order really is the wrong way round, so this is not a "
          "test of nothing",
          _later < _earlier, "the lexical comparison no longer inverts these")
    _pair = [(_event(event_uid="x:l", url="https://luma.com/l", start=_later),
              Score(fit=5, access_value=5, cost=2), Urgency.P2),
             (_event(event_uid="x:e", url="https://luma.com/e", start=_earlier),
              Score(fit=5, access_value=5, cost=2), Urgency.P2)]
    check("_start_order puts the earlier INSTANT first",
          sorted(_pair, key=_start_order)[0][0].start == _earlier,
          "it kept the text order")
    _undated = (_event(event_uid="x:u", url="https://luma.com/u", start=""),
                Score(fit=5, access_value=5, cost=2), Urgency.P2)
    check("and an undated event sorts last, where the old sentinel put it",
          sorted(_pair + [_undated], key=_start_order)[-1][0].start == "",
          "an undated event jumped the queue")

    # Through _deliver, because the sort the reader actually sees is the one in
    # there and a helper can be right while its call site still uses the string.
    with tempfile.TemporaryDirectory() as tmp_sort:
        db = SqliteEventStore(Path(tmp_sort) / "sort.db")
        box = _Sections()
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                _deliver(Digest(list(_pair), [], [], False, [], datetime(2026, 9, 1, tzinfo=timezone.utc)),
                         Funnel(), db,
                         box, _offline, dry_run=True)
            urls = next(u for head, u in box.sections if head == "OTHER PICKS")
            check("_deliver orders OTHER PICKS by the clock too",
                  urls == ["https://luma.com/e", "https://luma.com/l"],
                  f"order {urls}")
        finally:
            db.close()

    section("notifier - an absolute time is converted, never sliced")
    # Same measurement as display_zone's docstring; the second
    # case below is that exact WordPress row, landing on the wrong DAY.
    _la = display_zone("America/Los_Angeles")

    def _shown(start):
        ev = _event(title="T", start=start, location="San Francisco, CA")
        body = format_digest([("S", [(ev, Score(fit=5, access_value=5, cost=2),
                                      Urgency.P2)])], _la,
                             now=datetime(2026, 9, 1, tzinfo=timezone.utc))
        line = next(l for l in body.splitlines() if l.strip().startswith("when:"))
        return line.split("when:")[1].split("|")[0].split("(")[0].strip()

    stamp_cases = [
        ("a Luma-shaped offset is unchanged, since it already is Pacific",
         "2026-09-25T18:00:00-07:00", "2026-09-25 18:00 PDT"),
        ("the measured WordPress row lands on the right DAY, not 02:00 the next",
         "2026-09-24T02:00:00+00:00", "2026-09-23 19:00 PDT"),
        ("a UTC stamp in winter uses the standard-time offset, not a fixed one",
         "2026-01-15T02:00:00+00:00", "2026-01-14 18:00 PST"),
        # Still UTC here, and deliberately so. The notifier is a LAST resort
        # for a naive value: _stamp_naive gave every event an offset before the
        # ledger saw it, so anything still naive by now is a row stored before
        # that existed, where guessing the reader's zone would rewrite history.
        ("a naive stamp is still read as UTC, the assumption _lead_time makes",
         "2026-09-24T02:00:00", "2026-09-23 19:00 PDT"),
        ("no start at all still says so", "", "date unknown"),
    ]
    for why, start, want in stamp_cases:
        got = _shown(start)
        check(why, got == want, f"printed {got!r}, expected {want!r}")

    # Fail at construction, not at send. A digest already costs a fetch and a
    # scoring pass by the time a notifier is used.
    bad = False
    try:
        ConsoleNotifier("Mars/Olympus_Mons")
    except Exception:
        bad = True
    check("an unknown display_timezone raises when the notifier is built",
          bad, "it was accepted, so the digest would state times in a zone "
          "nobody chose")
    check("and the configured name resolves",
          display_zone(load_settings().display_timezone) is not None)

    section("scoring - a bad response must not be cached as a real judgement")
    # Same incident as _parse_score's docstring (a fallback score cached
    # forever). Worst for exactly the resume drops this project exists to
    # catch, whose access_value belongs at the top of the scale, not the floor.
    parse_cases = [
        ("a valid object", '{"fit":8,"access_value":9,"cost":2,"reason":"ok"}', True),
        ("an object wrapped in prose", 'Sure! {"fit":8,"access_value":9,"cost":2}', True),
        ("a fenced reply with no object", "\nGreat event!\n", False),
        ("an empty response", "", False),
        # Readable but not numeric; see _parse_score's docstring ("high").
        ("a non-numeric axis", '{"fit":"high","access_value":9,"cost":2}', False),
        ("a missing axis", '{"fit":8,"access_value":9}', False),
        ("a null axis", '{"fit":8,"access_value":null,"cost":2}', False),
        ("numbers written as strings", '{"fit":"8","access_value":"9","cost":"2"}', True),
        # Both pass a naive numeric check (float()/json.loads accept them);
        # see _as_number's docstring for why finiteness needs its own gate.
        ("a NaN axis", '{"fit": NaN, "access_value": 9, "cost": 2}', False),
        ("an Infinity axis", '{"fit": Infinity, "access_value": 9, "cost": 2}', False),
    ]
    for why, raw, parses in parse_cases:
        try:
            _parse_score(raw, "CLI")
            got = True
        except ValueError:
            got = False
        check(f"_parse_score on {why}", got is parses,
              "returned a score" if got else "raised")

    class _NoCache:
        def __init__(self):
            self.written = []

        def get_cached_score(self, uid, tag):
            return None

        def put_cached_score(self, uid, tag, score):
            self.written.append(score)

    class _Unparseable:
        method_label = "CLI"
        cacheable = True

        def score(self, event):
            return _parse_score("no json here", self.method_label)

    _cache = _NoCache()
    try:
        CachingScorer(_Unparseable(), _cache, "cli:p3").score(_event())
    except ValueError:
        pass
    check("nothing is written to the score cache when parsing failed",
          not _cache.written, f"cached {_cache.written}")

    # Exercises get_cached_score's named-column read; see its comment in
    # store.py for why a positional index here would be unsafe.
    with tempfile.TemporaryDirectory() as tmp_cache:
        db = SqliteEventStore(Path(tmp_cache) / "c.db")
        try:
            db.put_cached_score("u", "cli:p3",
                                Score(fit=3, access_value=7, cost=9,
                                      reason="r", method="CLI"))
            back = db.get_cached_score("u", "cli:p3")
            check("a cached score round-trips onto the right axes",
                  (back.fit, back.access_value, back.cost) == (3, 7, 9),
                  f"got {(back.fit, back.access_value, back.cost)}")
        finally:
            db.close()

    section("local_run --mark - the verb the ledger always had a noun for")
    # SAVED / REGISTERED / DISMISSED silence the sweeper, but nothing could
    # write them, so "have you dealt with this" had no answer and the last call
    # had to assume no. The heading used to claim otherwise.
    import local_run

    with tempfile.TemporaryDirectory() as tmp_mark:
        real_db, local_run.DB = local_run.DB, Path(tmp_mark) / "m.db"
        db = SqliteEventStore(local_run.DB)
        try:
            url = "https://luma.com/marked"
            for kind in ("jsonld", "newsletter"):
                db.upsert(_event(event_uid=f"{kind}:{url}", url=url,
                                 source_kind=kind, start=_SOON))
                db.mark_alerted([f"{kind}:{url}"], at="2026-09-01T00:00:00+00:00")
            db.save()
            # 44h before _SOON, so the row is inside the 72h window and only
            # the state set by --mark can keep it out of the sweep.
            _in_window = "2026-09-29T05:00:00+00:00"
            check("before marking, the event is due for a last call",
                  len(db.due_for_resweep(72, _in_window, min_gap_hours=24)) == 2,
                  "the check below would pass whatever --mark does")
            db.close()

            def mark(*argv):
                # _mark explains itself on stdout; this file's output is the report.
                with contextlib.redirect_stdout(io.StringIO()):
                    return local_run._mark(["--mark", *argv])

            check("an unknown state is refused", mark("attended", url) == 2)
            check("a state the reader cannot choose is refused",
                  mark("expired", url) == 2)
            check("a url the ledger never saw is reported, not silently accepted",
                  mark("saved", "https://luma.com/never") == 1)
            check("marking succeeds", mark("registered", url) == 0)

            db = SqliteEventStore(local_run.DB)
            states = {r[0] for r in db._conn.execute(
                "SELECT state FROM events WHERE canonical_url=?", (url,))}
            check("every listing of that event is marked, not just one",
                  states == {"registered"}, f"states {states}")
            check("and it stops being due for a last call",
                  not db.due_for_resweep(72, _in_window, min_gap_hours=24),
                  "the sweep still wants it")
        finally:
            db.close()
            local_run.DB = real_db

    section("newsletter.start_from_prose - TODO 2, the only route to a Tesla date")
    # Every tesla.com/event page answers 403 to this scraper, re-measured
    # 2026-09-19, and the four of five that have since closed no longer print
    # their date even to a real browser. The issue still carries all five, so
    # this parser is the only thing standing between those rows and no date.
    _ISSUE = "2026-08-31T16:23:13Z"       # the real publish date of issue 004
    _real = [
        ("Monday, August 31 at 5:30pm EST", "2026-08-31T17:30:00-05:00"),
        ("Tuesday, September 8 at 4:30pm", "2026-09-08T16:30:00"),
        ("Wednesday, September 16 at 5:00pm EST", "2026-09-16T17:00:00-05:00"),
        ("Monday, September 21 at 5:30pm EST", "2026-09-21T17:30:00-05:00"),
        ("Friday, October 23 at 6:00pm", "2026-10-23T18:00:00"),
    ]
    for prose, want in _real:
        got = start_from_prose(f"({prose})", _ISSUE)
        check(f"reads {prose!r}", got == want, f"got {got!r}, wanted {want!r}")
    # A time with no stated zone stays offset-free rather than being assigned
    # one HERE. The pipeline's _stamp_naive attaches the reader's offset later,
    # after every adapter and the extractor, so one layer decides what "no
    # zone" means. The note this replaced claimed the UTC reading was never
    # wrong by a day, which "2026-10-08T00:00:00" disproves.
    check("no stated zone means no invented offset",
          start_from_prose("(Tuesday, September 8 at 4:30pm)", _ISSUE).endswith("16:30:00"),
          start_from_prose("(Tuesday, September 8 at 4:30pm)", _ISSUE))
    # The weekday is the whole reason inferring the missing year is safe. Each
    # case below would otherwise produce a confident wrong date.
    for why, prose in [
            ("a weekday that does not match the date",
             "(Tuesday, August 31 at 5:30pm EST)"),
            ("a date that does not exist", "(Monday, February 30 at 5:00pm)"),
            ("no weekday at all", "(August 31 at 5:30pm EST)"),
            ("prose with no date in it", "(sign up now)")]:
        check(f"returns nothing for {why}",
              start_from_prose(prose, _ISSUE) == "",
              f"got {start_from_prose(prose, _ISSUE)!r}")
    check("an issue with no publish date yields nothing",
          start_from_prose("(Monday, August 31 at 5:30pm EST)", "") == "", "")
    # Year rollover. A December issue naming a January date means next year, and
    # the weekday is what decides it rather than a rule about month numbers.
    check("a date after New Year takes the following year",
          start_from_prose("(Friday, January 8 at 6:00pm)",
                           "2026-12-20T00:00:00+00:00").startswith("2027-01-08"),
          start_from_prose("(Friday, January 8 at 6:00pm)", "2026-12-20T00:00:00+00:00"))
    # _MAX_LEAD_DAYS below 365 is what makes the year unambiguous, so the two
    # candidates can never both be in range. Asserted through the behaviour
    # rather than the constant, since the constant alone proves nothing.
    check("a date further ahead than a newsletter ever announces is refused",
          start_from_prose("(Saturday, August 31 at 5:30pm)",
                           "2024-01-01T00:00:00+00:00") == "", "")
    check("the same prose one year on does not sneak in as next year",
          start_from_prose("(Tuesday, August 31 at 5:30pm EST)", _ISSUE) == "",
          "2027-08-31 is a Tuesday, and accepting it would date the event a "
          "year late off a single mistyped weekday")

    section("_links.tails - the text a date is read out of")
    _body = ('<a href="https://luma.com/one">First</a> (Monday, August 31 at 5:30pm) '
             '<a href="https://luma.com/two">Second</a> (Tuesday, September 8 at 4pm)')
    _t = tails(_body)
    check("each link gets the text that follows IT, not the body",
          "August 31" in _t["https://luma.com/one"]
          and "September 8" in _t["https://luma.com/two"],
          str(_t))
    check("a link's tail does not reach back to the previous item",
          "August 31" not in _t["https://luma.com/two"], _t["https://luma.com/two"])
    check("a bare url has no markup to bound its tail, so it gets none",
          tails("see https://luma.com/three for details") == {},
          str(tails("see https://luma.com/three for details")))

    section("pipeline._at_capacity - TODO 1, a full event is not worth a slot")
    # Against the REAL config, not a fixture list. The markers are data now, so
    # a test carrying its own copy would pass while config.yaml said otherwise.
    _live = load_settings()
    check("config.yaml actually defines capacity markers",
          len(_live.capacity_markers) > 0,
          "an empty list silently disables the whole withholding path")
    for why, kw in [
            ("the measured title marker",
             dict(title="K-AI Tech Week - Silicon Valley (SOLD OUT)")),
            ("a marker in the description counts too",
             dict(description="Doors at 6. This event is full.")),
            ("hyphenated", dict(title="Mixer [SOLD-OUT]")),
            ("case is ignored", dict(title="Mixer (Sold Out)")),
            ("waitlist only", dict(description="Waitlist only from here."))]:
        check(f"at_capacity sees {why}", _at_capacity(_event(**kw), _live), str(kw))
    # The marker set has to stay narrow, which is the whole risk of this method.
    # Marketing copy is not a statement that THIS event is full, and a passed
    # deadline is the date window's business.
    for why, kw in [
            ("marketing urgency is not a capacity fact",
             dict(description="Seats sell out fast, book early")),
            ("a sold-out OTHER event mentioned in prose",
             dict(description="Unlike last year we did not sell out")),
            ("a plain open listing", dict(title="Career Fair", description="All welcome"))]:
        check(f"at_capacity ignores {why}", not _at_capacity(_event(**kw), _live), str(kw))

    # SOLD_OUT silences the sweeper but is NOT something the reader can choose,
    # because it is a fact read off the listing rather than a decision.
    check("sold_out silences the sweeper", State.SOLD_OUT.silences_sweeper, "")
    check("sold_out is not reader-choosable", not State.SOLD_OUT.choosable, "")

    section("models.Event.merged_with - caller decides precedence")
    high = _event(event_uid="a", title="Real", url="https://x/a", start="", location="SF")
    low = _event(event_uid="b", title="Other", url="https://x/b", start="2026-10-01T10:00:00", location="NY")
    merged = high.merged_with(low)
    check("gaps fill from the lower-precedence side",
          merged.start == "2026-10-01T10:00:00")
    check("non-empty higher-precedence values are never overwritten", merged.location == "SF")
    check("identity fields never move", merged.url == "https://x/a" and merged.event_uid == "a")

    # page_unreadable is excluded from merge for the reason in Event.merged_with
    # (a read attempt, not a fact about the event); this checks the practical
    # consequence, that one side's failure cannot follow the other event.
    unread = _event(page_unreadable=True, location="San Jose, CA")
    check("merged_with does not carry a page-read failure across",
          not _event().merged_with(unread).page_unreadable)
    check("merged_with still fills the ordinary fields it is for",
          _event().merged_with(unread).location == "San Jose, CA")

    section("models.Score - three axes, two of them rank")
    check("rank excludes cost, so a South Bay commute cannot demote all of SF",
          Score(fit=5, access_value=4, cost=99).rank == 20)
    check("State.silences_sweeper covers all four terminal states",
          all(State(s).silences_sweeper for s in ("saved", "registered", "dismissed", "expired"))
          and not State.NEW.silences_sweeper)

    section("config.build_sources - a held source must not be startable")
    # Regression test for the exact-match status check (see build_sources).
    adversarial = {
        "jsonld": [{"name": "held", "url": "https://x/h", "status": "verified_but_held"},
                   {"name": "pattern", "url": "https://x/p", "status": "verified_pattern"},
                   {"name": "live", "url": "https://x/r", "status": "verified"}],
        "wordpress": [{"name": "wp-held", "url": "https://x/w", "status": "verified_but_held"}],
        "newsletters": [{"name": "nl-held", "host": "x.beehiiv.com", "status": "verified_but_held"}],
    }
    started = sorted(s.name for s in build_sources(adversarial, StubHttpClient({})))
    check("only an exactly-verified entry is started", started == ["live"],
          f"started {started}")

    # The inverse, which an earlier review asked for and which was never
    # added. That fix only corrected the one entry then claiming coverage,
    # leaving nothing to stop the next one. build_sources reads four blocks of
    # channels.yaml and cannot see the rest, while `verified` is exact-matched
    # everywhere else in that file to mean "started", so the word inside an
    # unread block claims coverage the pipeline does not have. The blocks are
    # derived rather than listed, because a hardcoded list would go stale in
    # precisely the direction that makes this pass vacuously.
    read_blocks = {n.args[0].value for n in ast.walk(
        ast.parse((_pkg / "config.py").read_text(encoding="utf-8")))
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
        and n.func.attr == "get" and n.args
        and isinstance(n.args[0], ast.Constant)
        and isinstance(n.args[0].value, str)
        and _dotted(n.func.value) == "channels"}
    check("the scan still sees which channels.yaml blocks build_sources reads",
          {"jsonld", "wordpress", "gmail", "newsletters"} <= read_blocks,
          f"found {sorted(read_blocks)}")
    for block, entries in load_channels().items():
        if block in read_blocks or not isinstance(entries, list):
            continue
        claiming = [e.get("name") for e in entries
                    if isinstance(e, dict) and e.get("status") == "verified"]
        check(f"nothing in the unread block {block!r} claims plain 'verified'",
              not claiming,
              f"{claiming} reads as live coverage to anyone auditing "
              f"channels.yaml, but build_sources never opens {block!r}")

    # TODO item 4's fix is two call sites away from the code that enforces it,
    # and mutating either one leaves every behavioural test in the mailbox
    # section green: they construct GmailLabelSource directly and so never go
    # through config at all. Checked structurally for that reason, over the same
    # parsed config.py the block above uses. Behaviour is not asserted here
    # because doing so needs GMAIL_READ_* in the environment, and this file
    # deliberately never writes there -- the live-source section at the end
    # reads the real credentials.
    _cfg = ast.parse((_pkg / "config.py").read_text(encoding="utf-8"))
    def _callees_passing(kwarg: str) -> set[str]:
        return {node.func.id for node in ast.walk(_cfg)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and any(kw.arg == kwarg for kw in node.keywords)}

    _passes_skip_from = _callees_passing("skip_from")
    for callee, why in [
            ("GmailLabelSource",
             "build_sources would build a mailbox source that reads our own "
             "digest back, which is TODO item 4"),
            ("build_sources",
             "build_runtime would leave skip_from at its empty default, so the "
             "check would be off for every real run while staying on in tests")]:
        check(f"config.py still passes skip_from to {callee}",
              callee in _passes_skip_from, why)

    # Same hazard, one seam further along. build_sources defaults all three to
    # empty so that a test omitting them cannot reach a real account, which
    # means build_runtime forgetting one is SILENT: the mailbox prints a [skip]
    # and the run reads no mail while every other source still reports.
    for kwarg in ("gmail_read_user", "gmail_read_password", "gmail_read_labels"):
        check(f"build_runtime still passes {kwarg} to build_sources",
              "build_sources" in _callees_passing(kwarg),
              "omitted, the mailbox tier goes dark behind a [skip] line that "
              "reads exactly like an unconfigured mailbox")

    section("config - the mailbox is passed in, not read from channels.yaml")
    # channels.yaml is published, so a label name must not be expressible there.
    check("channels.yaml declares no label of its own",
          not any(e.get("sources") or e.get("label")
                  for e in load_channels().get("gmail") or []),
          "a label name in a published file describes a private mailbox")

    for raw, want, why in [
            ("one", ["one"], "the single-label case"),
            ("a,b", ["a", "b"], "a plain comma list"),
            ("a, b/c ", ["a", "b/c"], "surrounding space is trimmed, '/' is not a delimiter"),
            ("has space", ["has space"], "a space INSIDE a name belongs to the name"),
            ("a,,b,", ["a", "b"], "blanks dropped, so a trailing comma is harmless"),
            ("  ", [], "whitespace only is empty, not a label named ' '"),
            ("", [], "unset is empty")]:
        check(f"GMAIL_READ_LABELS {raw!r} parses as {want} - {why}",
              _parse_labels(raw) == want, f"got {_parse_labels(raw)}")

    # Credentials are fake and never connect: build_sources only CONSTRUCTS
    # GmailLabelSource, and every IMAP call lives in fetch(), which nothing here
    # invokes. Passed as arguments rather than patched into os.environ, which is
    # the point of the signature -- the live-source run at the end reads the
    # real environment, and a test that wrote there could reach a real account.
    _gmail_entry = {"gmail": [{"name": "mail-labels", "status": "verified"}]}
    for user, password, labels, want_names, why in [
            ("", "", ["a"], [], "no credentials, whatever the labels say"),
            ("u@x", "", ["a"], [], "half a credential pair is not a credential"),
            ("u@x", "pw", [], [], "credentials alone; the labels say what to read"),
            ("u@x", "pw", ["a", "b"], ["gmail:a", "gmail:b"],
             "one source per label, named after it")]:
        with contextlib.redirect_stdout(io.StringIO()) as _out:
            built = build_sources(_gmail_entry, StubHttpClient({}),
                                  gmail_read_user=user, gmail_read_password=password,
                                  gmail_read_labels=labels)
        check(f"build_sources({user!r}, {labels}) yields {want_names} - {why}",
              [s.name for s in built] == want_names,
              f"built {[s.name for s in built]}")
        # Building nothing in silence is the failure this project exists to
        # prevent, so every empty case must still say why it read no mail.
        check(f"building no mailbox source is reported, not silent ({why})",
              bool(want_names) or "[skip]" in _out.getvalue(),
              f"said {_out.getvalue()!r}")

    # The environment reaches build_sources only through Settings, so a mailbox
    # variable that load_settings forgets is invisible however build_runtime is
    # called. Asserted over the source because a live run is what would notice.
    _cfg_src = (_pkg / "config.py").read_text(encoding="utf-8")
    for _var in ("GMAIL_READ_USER", "GMAIL_READ_APP_PASSWORD", "GMAIL_READ_LABELS"):
        check(f"{_var} is read exactly once, in load_settings",
              _cfg_src.count(f'"{_var}"') == 1,
              "a second reader would bypass Settings and diverge from it")
    check("build_sources reaches for no environment variable of its own",
          "os.getenv" not in _cfg_src[_cfg_src.index("def build_sources"):],
          "secrets enter in load_settings; build_sources takes them as arguments")

    section("store.due_for_resweep - the anti-miss query, on real rows")
    # The query behind pipeline's last-call sweep. Exercised directly here as
    # well as through run(), because the three conditions it ANDs together are
    # each easy to drop and the symptom of dropping one is a wrong email rather
    # than a crash.
    with tempfile.TemporaryDirectory() as tmp:
        db = SqliteEventStore(Path(tmp) / "t.db")
        # try/finally, because Windows refuses to delete an open sqlite file: a
        # failing check would otherwise be replaced by a PermissionError raised
        # during cleanup, hiding the real result.
        try:
            now = "2026-09-08T00:00:00+00:00"
            rows = [
                ("in-window", "2026-09-09T10:00:00+00:00", State.NEW, True),
                ("seen-counts", "2026-09-09T11:00:00+00:00", State.SEEN, True),
                ("beyond-window", "2026-09-30T10:00:00+00:00", State.NEW, False),
                ("already-started", "2026-09-07T10:00:00+00:00", State.NEW, False),
                ("registered", "2026-09-09T12:00:00+00:00", State.REGISTERED, False),
                ("dismissed", "2026-09-09T13:00:00+00:00", State.DISMISSED, False),
                ("undated", "", State.NEW, False),
            ]
            for uid, start, state, _ in rows:
                db.upsert(_event(event_uid=uid, url=f"https://x/{uid}", start=start))
                # Alerted, or the sweep would correctly ignore them: a row the
                # reader was never told about is not one they failed to act on.
                db.mark_alerted([uid], at="2026-09-01T00:00:00+00:00")
                db.set_state(uid, state)
            got = {e.event_uid for e in db.due_for_resweep(72, now, min_gap_hours=24)}
            want = {uid for uid, _, _, keep in rows if keep}
            check("a 72h window returns exactly the unactioned, still-future events",
                  got == want, f"got {sorted(got)}, want {sorted(want)}")
            # The SQL hardcodes state IN ('new','seen'); State.silences_sweeper says
            # the same thing in Python and nothing links the two.
            sweepable = {s.value for s in State if not s.silences_sweeper}
            check("the query's state list still agrees with State.silences_sweeper",
                  sweepable == {"new", "seen"}, f"State now says {sorted(sweepable)}")
            # Concrete instance of the string-vs-instant bug expire_past's
            # docstring measures (32 by text vs 22 by instant on the real
            # ledger).
            noon_utc = "2026-09-09T18:00:00+00:00"
            evening = iso_or_empty("2026-09-09T17:30:00.000-07:00")
            db.upsert(_event(event_uid="tz", url="https://x/tz", start=evening))
            db.mark_alerted(["tz"], at="2026-09-01T00:00:00+00:00")
            db.expire_past(noon_utc, 0)
            still = db._conn.execute(
                "SELECT state FROM events WHERE event_uid='tz'").fetchone()[0]
            check("an event six hours away is not expired by string order",
                  still != "expired", f"state={still}")
            check("and it is what the last call is looking for",
                  "tz" in {e.event_uid for e in db.due_for_resweep(24, noon_utc, min_gap_hours=24)},
                  "the sweep skipped a same-day event written in -07:00")

            # Same disagreement due_for_resweep's docstring describes (P0
            # classified but outside the window), reproduced as a concrete row.
            db.upsert(_event(event_uid="rsvp", url="https://x/rsvp",
                             start="2026-09-18T10:00:00+00:00",
                             rsvp_deadline="2026-09-08T12:00:00+00:00"))
            db.mark_alerted(["rsvp"], at="2026-09-01T00:00:00+00:00")
            check("an event ten days out whose RSVP closes in 12h IS due",
                  "rsvp" in {e.event_uid for e in db.due_for_resweep(72, now, min_gap_hours=24)},
                  "the sweep read the start and missed the door")
            check("the same row is P0, which is the agreement being asserted",
                  DeadlineUrgency(72, 40, 9).classify(
                      _event(start="2026-09-18T10:00:00+00:00",
                             rsvp_deadline="2026-09-08T12:00:00+00:00"),
                      Score(fit=10, access_value=10, cost=1, reason="", method="k"),
                      datetime.fromisoformat(now)) is Urgency.P0,
                  "the urgency tier no longer reads the deadline, so this "
                  "check has stopped testing the disagreement it was written for")
            # The other direction: a deadline that has passed must not keep an
            # event sweepable on its start alone. COALESCE picks the deadline,
            # so this row falls out of the window rather than back onto start.
            db.upsert(_event(event_uid="rsvp-past", url="https://x/rsvp-past",
                             start="2026-09-09T10:00:00+00:00",
                             rsvp_deadline="2026-09-01T12:00:00+00:00"))
            db.mark_alerted(["rsvp-past"], at="2026-09-01T00:00:00+00:00")
            check("a closed RSVP is not swept on its start instead",
                  "rsvp-past" not in {e.event_uid for e in db.due_for_resweep(72, now, min_gap_hours=24)},
                  "the deadline was ignored once it stopped being convenient")

            # Simulates a row written before the iso_or_empty gate existed, by
            # inserting the source's raw spelling directly via SQL.
            # _row_to_event's docstring has the measured count of how many of
            # the real ledger's dated rows still look like this.
            db._conn.execute(
                "INSERT INTO events(event_uid, canonical_url, title, source, "
                "source_kind, start, state, first_seen, last_seen, alerted_at) "
                "VALUES('legacy','https://x/legacy','Legacy','s','jsonld',"
                "'2026-09-09T17:30:00.000-07:00','seen','x','x',"
                "'2026-09-01T00:00:00+00:00')")
            got = {e.event_uid for e in db.due_for_resweep(24, noon_utc, min_gap_hours=24)}
            check("a row stored before the gate existed still loads",
                  "legacy" in got, "reading the ledger raised or skipped it")

            # One live run sent 3 of its 7 last calls to events first mailed
            # the day before, all already inside the window when first mailed.
            # Each case isolates one condition: the other one passes.
            db.upsert(_event(event_uid="first-inside", url="https://x/first-inside",
                             start="2026-09-09T10:00:00+00:00"))
            db.mark_alerted(["first-inside"], at="2026-09-07T00:00:00+00:00")
            check("an event first mailed inside the window never gets a last call",
                  "first-inside" not in {e.event_uid for e in
                                         db.due_for_resweep(72, now, min_gap_hours=24)},
                  "its first mail was already the closing-soon notice")
            db.upsert(_event(event_uid="just-told", url="https://x/just-told",
                             start="2026-09-09T10:00:00+00:00"))
            db.mark_alerted(["just-told"], at="2026-09-06T00:00:00+00:00")
            check("nor does one mailed just before the window, inside the gap",
                  "just-told" not in {e.event_uid for e in db.due_for_resweep(
                      72, "2026-09-06T12:00:00+00:00", min_gap_hours=24)},
                  "a last call went out 12h after the first mail")
            check("but the same row is due once the gap has passed",
                  "just-told" in {e.event_uid for e in db.due_for_resweep(
                      72, "2026-09-07T12:00:00+00:00", min_gap_hours=24)},
                  "the gap withheld a reminder it should only delay")

            # expire_past spells the same grouping out in SQL. Checked by
            # behaviour rather than by reading the string, because the failure
            # that matters is a dismissed event coming back as merely expired.
            past = "2026-09-01T10:00:00+00:00"
            for state in State:
                uid = f"terminal-{state.value}"
                db.upsert(_event(event_uid=uid, url=f"https://x/{uid}", start=past))
                db.set_state(uid, state)
            db.expire_past(now, 0)
            survived = {s.value for s in State
                        if db._conn.execute(
                            "SELECT state FROM events WHERE event_uid=?",
                            (f"terminal-{s.value}",)).fetchone()[0] == s.value}
            check("expire_past rewrites exactly the states silences_sweeper does not",
                  survived == {s.value for s in State if s.silences_sweeper},
                  f"kept {sorted(survived)}, terminal states are "
                  f"{sorted(s.value for s in State if s.silences_sweeper)}")
            # Why "reported" keys on the URL, not event_uid: see
            # EventStore.reported_urls. These two cases are why the store applies
            # the digest floor itself: a below-floor row was recorded for audit
            # and never mailed, so counting it as reported would silently drop
            # the event once a richer source lifts the same URL over the floor.
            # Measured on a newsletter bare link, then the same URL from a
            # JSON-LD feed.
            url = "https://tesla.com/event/northeastern-resume"
            db.upsert(_event(event_uid=f"newsletter:{url}", url=url,
                             source_kind="newsletter"), Score(2, 2, 3))
            reported = db.reported_urls()
            check("a below-floor audit row does NOT count as already reported",
                  url not in reported, f"reported holds {sorted(reported)}")
            # Same risk pipeline.py's `seeding` comment measures (a dry run's
            # upsert must not look "reported"). Predates mark_cleared_floor.
            db.upsert(_event(event_uid="newsletter:https://luma.com/above",
                             url="https://luma.com/above"), Score(9, 9, 3))
            check("recording an above-floor event is not the same as reporting it",
                  "https://luma.com/above" not in db.reported_urls())
            db.upsert(_event(event_uid=f"jsonld:{url}", url=url, source_kind="jsonld"),
                      Score(8, 7, 3))
            db.mark_cleared_floor([f"jsonld:{url}"])
            reported = db.reported_urls()
            check("once one source clears the floor, the URL counts as reported",
                  url in reported, f"reported holds {sorted(reported)}")
            # The whole point of a sticky column. A later sighting that scores
            # BELOW the floor must not erase the fact that it was already
            # reported, or the next source to list the same URL sends it again.
            db.upsert(_event(event_uid=f"jsonld:{url}", url=url,
                             source_kind="jsonld"), Score(1, 1, 3))
            check("a later low score does not erase that the URL was reported",
                  url in db.reported_urls())
            db.mark_cleared_floor([f"jsonld:{url}"])
            first = {r[0] for r in db._conn.execute(
                "SELECT cleared_floor_at FROM events WHERE event_uid=?",
                (f"jsonld:{url}",))}
            check("marking twice keeps the FIRST timestamp", len(first) == 1)

        finally:
            db.close()

        # The text mirror is committed to a branch of a public repo, so this is
        # a privacy boundary rather than a formatting preference. Asserted on
        # the FILE, not on _UNPUBLISHED_COLUMNS, so that rewriting export_jsonl
        # to build its JSON some other way cannot pass by agreeing with itself.
        redact_e, redact_c = Path(tmp) / "r-e.jsonl", Path(tmp) / "r-c.jsonl"
        db4 = SqliteEventStore(Path(tmp) / "redact.db")
        try:
            private = "Dear Candidate, per our call I am forwarding the offer"
            db4.upsert(_event(event_uid="gmail_label:https://luma.com/r",
                              url="https://luma.com/r", description=private),
                       Score(8, 7, 3, reason="r"))
            db4.save()
            db4.export_jsonl(redact_e, redact_c)
        finally:
            db4.close()
        mirror = redact_e.read_text(encoding="utf-8")
        row = json.loads(mirror.strip())
        check("the published mirror carries no message bodies",
              "description" not in row and private not in mirror,
              f"exported keys: {sorted(row)}")
        check("redaction drops that column ONLY, so the ledger still works",
              {"event_uid", "canonical_url", "cleared_floor_at", "alerted_at",
               "swept_at", "state", "start"} <= set(row),
              f"exported keys: {sorted(row)}")
        db5 = SqliteEventStore(Path(tmp) / "redact-back.db")
        try:
            db5.import_jsonl(redact_e, redact_c)
            check("a redacted mirror still imports, taking the schema default",
                  db5._conn.execute(
                      "SELECT description FROM events").fetchone()[0] == "",
                  "import of a mirror missing a column must not fail or corrupt")
        finally:
            db5.close()

        # Exercises the cloud backfill path (see SqliteEventStore.import_jsonl):
        # simulated by deleting the key, exactly what an older export looks like.
        events_file, cache_file = Path(tmp) / "e.jsonl", Path(tmp) / "c.jsonl"
        db2 = SqliteEventStore(Path(tmp) / "src.db")
        try:
            old_url = "https://luma.com/already-mailed"
            db2.upsert(_event(event_uid=f"newsletter:{old_url}", url=old_url),
                       Score(8, 7, 3))
            db2.mark_alerted([f"newsletter:{old_url}"])
            db2.save()
            db2.export_jsonl(events_file, cache_file)
        finally:
            db2.close()
        events_file.write_text(
            "\n".join(json.dumps({k: v for k, v in json.loads(line).items()
                                  if k != "cleared_floor_at"})
                      for line in events_file.read_text(encoding="utf-8").splitlines()
                      if line.strip()) + "\n", encoding="utf-8")
        db3 = SqliteEventStore(Path(tmp) / "cloud.db")
        try:
            db3.import_jsonl(events_file, cache_file)
            check("an already-alerted row imported from an older ledger stays "
                  "suppressed", old_url in db3.reported_urls(),
                  "the first cloud run after this column landed would re-send it")
        finally:
            db3.close()

        # Same risk as SqliteEventStore.import_jsonl's docstring describes;
        # asserted both load orders here since neither should lose the flag.
        def _mirror(name, mark):
            store = SqliteEventStore(Path(tmp) / f"{name}.db")
            try:
                store.upsert(_event(event_uid="newsletter:https://luma.com/m",
                                    url="https://luma.com/m"), Score(8, 7, 3))
                if mark:
                    store.mark_cleared_floor(["newsletter:https://luma.com/m"])
                store.save()
                paths = (Path(tmp) / f"{name}-e.jsonl", Path(tmp) / f"{name}-c.jsonl")
                store.export_jsonl(*paths)
            finally:
                store.close()
            return paths

        reported, unreported = _mirror("reported", True), _mirror("unreported", False)
        for order, sides in (("reported side last", (unreported, reported)),
                             ("reported side first", (reported, unreported))):
            merged = SqliteEventStore(Path(tmp) / f"merge-{order.replace(' ', '-')}.db")
            try:
                for side in sides:
                    merged.import_jsonl(*side)
                check(f"a union merge keeps the reported flag, {order}",
                      "https://luma.com/m" in merged.reported_urls(),
                      "the merge handed back a reported URL as unreported")
            finally:
                merged.close()

        # Both sides populated is the branch the two cases above skip. The picks
        # differ per column and are asserted separately because merging them the
        # same way is the easy mistake.
        uid, mixed = "newsletter:https://luma.com/m", Path(tmp) / "mixed.db"
        seed = SqliteEventStore(mixed)
        try:
            seed.upsert(_event(event_uid=uid, url="https://luma.com/m",
                               title="first title"), Score(8, 7, 3))
            seed.mark_cleared_floor([uid])
            seed.mark_alerted([uid])
            seed.mark_swept([uid])
            seed.save()
            early = seed.export_jsonl(Path(tmp) / "x-e.jsonl", Path(tmp) / "x-c.jsonl")
        finally:
            seed.close()
        del early
        late_e, late_c = Path(tmp) / "y-e.jsonl", Path(tmp) / "y-c.jsonl"
        row = json.loads((Path(tmp) / "x-e.jsonl").read_text(encoding="utf-8").strip())
        row["cleared_floor_at"] = "2099-01-01T00:00:00+00:00"
        row["alerted_at"] = "2099-01-01T00:00:00+00:00"
        row["first_seen"] = "2099-01-01T00:00:00+00:00"
        row["swept_at"] = "2099-01-01T00:00:00+00:00"
        row["title"] = "later title"
        late_e.write_text(json.dumps(row) + "\n", encoding="utf-8")
        late_c.write_text("", encoding="utf-8")
        both = SqliteEventStore(Path(tmp) / "both.db")
        try:
            both.import_jsonl(Path(tmp) / "x-e.jsonl", Path(tmp) / "x-c.jsonl")
            both.import_jsonl(late_e, late_c)
            got = both._conn.execute(
                "SELECT cleared_floor_at, alerted_at, first_seen, swept_at, title "
                "FROM events WHERE event_uid=?", (uid,)).fetchone()
            check("cleared_floor_at keeps the EARLIER of two real timestamps",
                  not got["cleared_floor_at"].startswith("2099"), got["cleared_floor_at"])
            check("alerted_at keeps the LATER, matching mark_alerted's overwrite",
                  got["alerted_at"].startswith("2099"), got["alerted_at"])
            check("swept_at keeps the EARLIER, a reminder is sent once",
                  not (got["swept_at"] or "").startswith("2099"), got["swept_at"])
            check("first_seen keeps the EARLIER, it is set once at insert",
                  not got["first_seen"].startswith("2099"), got["first_seen"])
            check("a merge moves nothing but the four merged columns",
                  got["title"] == "first title", got["title"])
        finally:
            both.close()

    section("scoring._bounded - a left boundary, and only a left one")
    # See _bounded's docstring for the measured rationale (a trailing-space
    # hack, since a two-sided pattern was tried and rejected).
    _kw = KeywordScorer()

    def _rank_of(text):
        return _kw.score(_event(title="Board game night", description=text,
                                location="San Francisco, CA")).rank

    _base = _rank_of("cards and snacks")
    for why, text in [
            ("'resume' no longer matches 'presume'", "we presume you attend"),
            ("'ml' no longer matches 'html'", "see the html tutorial")]:
        check(why, _rank_of(text) == _base,
              f"rank moved {_base} -> {_rank_of(text)}")
    for why, text in [
            ("'recruit' still reaches 'recruiting'", "a recruiting event"),
            ("'hack' still reaches 'hackathon'", "an AI hackathon"),
            ("'hack' still reaches 'hackday'", "a hackday at the office"),
            ("'intern' still reaches 'internship'", "an internship program"),
            ("'career' still reaches 'Career Symposium'", "the Career Symposium")]:
        check(why, _rank_of(text) > _base,
              f"prefix matching broke, rank stayed at {_base}")
    # Accepted survivors, asserted so that removing them later is a deliberate
    # act rather than an accident. See _bounded's docstring for why no boundary
    # can separate two continuations of one prefix.
    for why, text in [
            ("'intern' still collides with 'internal', by design",
             "an internal process"),
            ("'hack' still collides with 'hackneyed', by design",
             "a hackneyed idea")]:
        check(why, _rank_of(text) > _base, "the collision is gone, so the "
              "docstring listing it as accepted is now wrong")

    section("sources.wordpress._START_KEYS - the last resort needs an offset")
    # See _START_KEYS's own comment for the offset bug this ordering avoids
    # and the 219-item measurement showing it is dormant today.
    from eventscout.sources import wordpress as _wp_mod

    _keys = list(_wp_mod._START_KEYS)
    check("date_gmt is consulted before the naive date",
          _keys.index("date_gmt") < _keys.index("date"),
          f"order is {_keys}")
    _wp_item = {"link": "https://wp.test/e", "title": {"rendered": "Career fair"},
                "date": "2026-09-03T10:00:00",
                "date_gmt": "2026-09-03T14:00:00"}
    _wp_src = WordPressSource("wp", "http://wp/", StubHttpClient(
        {"http://wp/?per_page=2&page=1": json.dumps([_wp_item])}), per_page=2)
    _got = _wp_src.fetch()[0]
    check("so an item with only those two keys takes the UTC one",
          _got.start.startswith("2026-09-03T14:00:00"), f"start {_got.start!r}")

    section("geo.GeoFilter - single-arg, satisfies EventFilter")
    geo = GeoFilter([Anchor("San Jose", 37.336157, -121.890608, 50)],
                    accept_virtual=True,
                    regional_sources=frozenset({"campus-feed"}))
    cases = [
        ("venue-only name on a regional feed is kept",
         _event(source="campus-feed", location="Welcome Center",
                attendance_mode=AttendanceMode.OFFLINE), True),
        ("an explicit other city is dropped even on a regional feed",
         _event(source="campus-feed", location="Boston, MA",
                attendance_mode=AttendanceMode.OFFLINE), False),
        ("venue-only name on a non-regional feed is dropped",
         _event(source="other", location="Welcome Center",
                attendance_mode=AttendanceMode.OFFLINE), False),
        ("location text 'Virtual' wins over a contradicting offline mode",
         _event(source="other", location="Virtual",
                attendance_mode=AttendanceMode.OFFLINE), True),
        ("a Bay Area city is kept regardless of feed",
         _event(source="other", location="San Francisco, CA",
                attendance_mode=AttendanceMode.OFFLINE), True),
    ]
    for rule, ev, want in cases:
        check(rule, geo.keep(ev) is want)

    # Recreates the case described in geo.py keep()'s comment.
    # Both accept_virtual values are exercised, since the bug was that
    # neither one mattered.
    nowhere = [("Remote Only", "in-region"), ("Anywhere", "in-region"),
               ("Worldwide", "in-region"), ("Online Event", "in-region")]
    for accept in (True, False):
        gf = GeoFilter([Anchor("San Jose", 37.336157, -121.890608, 50)],
                       accept_virtual=accept,
                       regional_sources=frozenset({"in-region"}))
        for loc, src in nowhere:
            got = gf.keep(_event(source=src, location=loc,
                                 attendance_mode=AttendanceMode.OFFLINE))
            check(f"{loc!r} from an in-region feed follows accept_virtual "
                  f"{accept}", got is accept, f"kept={got}")
        # The precedence that makes the above safe: see keep()'s first
        # comment for why a named city must win over the flag.
        check(f"a named Bay Area city still wins at accept_virtual {accept}",
              gf.keep(_event(source="other", location="Online, San Francisco, CA",
                             attendance_mode=AttendanceMode.ONLINE)),
              "a hybrid in SF was dropped")
        check(f"a venue-only in-region name is untouched at accept_virtual "
              f"{accept}",
              gf.keep(_event(source="in-region", location="Welcome Center",
                             attendance_mode=AttendanceMode.OFFLINE)),
              "the fallback this filter exists for stopped working")

    # distance_mi has no caller yet (see GeoFilter.distance_mi); tested anyway
    # so it isn't the only unverified arithmetic in the project, and so
    # channels.yaml's radius is a number something has actually agreed with.
    # Expected values are straight-line distances known independently of this
    # implementation, not whatever it happened to return.
    distances = [
        ("the anchor itself is zero miles away", 37.336157, -121.890608, 0, 0),
        ("San Francisco sits inside the 50mi radius", 37.7749, -122.4194, 30, 35),
        ("Oakland sits inside it too", 37.8044, -122.2712, 28, 33),
        ("Sacramento falls outside", 38.5816, -121.4944, 85, 92),
        ("New York is not a rounding error away", 40.7128, -74.0060, 2500, 2600),
    ]
    # Built from channels.yaml rather than a hand-copied fixture: the radius
    # only means anything against the geography actually deployed, and a copy
    # would keep passing after someone moved the anchors. Reads a local file,
    # so it stays inside the offline section.
    bay = build_geo(load_channels())
    for why, lat, lon, low, high in distances:
        miles = bay.distance_mi(lat, lon)
        check(why, low <= miles <= high, f"got {miles:.1f}mi, expected {low}-{high}")

    section("extract - typed markup beats asking a model to read prose")
    # See PageFactExtractor's class docstring for the measured split behind
    # this ordering: most events never need the model at all.
    def _ld(url, start="2026-09-20T18:00:00-07:00", name="Real Name",
            location="San Jose, CA"):
        """A page carrying Event markup AND prose, so both paths are reachable."""
        node = {"@type": "Event", "url": url, "name": name,
                "location": {"@type": "Place", "name": location}}
        if start:
            node["startDate"] = start
        return ('<html><script type="application/ld+json">'
                + json.dumps(node)
                + "</script><body><p>Join us on September 20 at 6pm.</p>"
                + "</body></html>")

    class _Page:
        def __init__(self, html):
            self.html = html

        def get_text(self, url):
            return self.html

    class _Refuser:
        called = False

        def json_call(self, system, user, schema, name):
            _Refuser.called = True
            return "{}"

    def _extract(html, **kw):
        _Refuser.called = False
        event = _event(event_uid="newsletter:https://luma.com/x",
                       url="https://luma.com/x", source_kind="newsletter",
                       description="Linked from https://d.beehiiv.com/p/x", **kw)
        return PageFactExtractor(_Refuser(), _Page(html)).extract(event)

    typed = _extract(_ld("https://luma.com/x"))
    check("an exact startDate is read from the page, not guessed",
          typed.start == "2026-09-20T18:00:00-07:00", f"got {typed.start!r}")
    check("the model is never called when the markup answers",
          not _Refuser.called)
    check("the typed location comes with it", typed.location == "San Jose, CA",
          f"got {typed.location!r}")
    check("a prose-tier title is replaced by the event's real name",
          typed.title == "Real Name", f"got {typed.title!r}")

    # The keyword-only run, which on a GitHub runner is every run made without
    # OPENAI_API_KEY, since the codex CLI does not exist there. build_runtime
    # used to return NO extractor in that case, disabling this free path along
    # with the model it never calls.
    def _extract_modelless(html, **kw):
        event = _event(event_uid="newsletter:https://luma.com/x",
                       url="https://luma.com/x", source_kind="newsletter",
                       description="Linked from https://d.beehiiv.com/p/x", **kw)
        return PageFactExtractor(None, _Page(html)).extract(event)

    modelless = _extract_modelless(_ld("https://luma.com/x"))
    check("with no model at all, the typed path still fills the date",
          modelless.start == "2026-09-20T18:00:00-07:00", f"got {modelless.start!r}")
    check("and the location with it", modelless.location == "San Jose, CA",
          f"got {modelless.location!r}")
    # The prose half must be SKIPPED, not attempted and swallowed. extract's
    # json_call sits inside `except Exception`, so a None invoker reaching it
    # raises AttributeError and returns the same undated event either way: the
    # outcome is identical and only the log tells the two apart. Asserted on
    # the log for that reason, and because "extraction failed" is a lie about
    # a run that has no extractor to fail with. Mutation testing is what
    # exposed this: deleting the guard survived an outcome-only check.
    _extract_log = logging.getLogger("eventscout.extract")
    _records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            _records.append(record)

    _handler = _Capture()
    _extract_log.addHandler(_handler)
    try:
        prose_only = _extract_modelless(
            "<html><body><p>Join us on September 20 at 6pm.</p></body></html>")
    finally:
        _extract_log.removeHandler(_handler)
    check("but the prose path is skipped, not attempted, when there is no model",
          not prose_only.start and not prose_only.page_unreadable,
          f"got start={prose_only.start!r}")
    check("and nothing is logged as a failed extraction, because none was tried",
          not any("extraction failed" in r.getMessage() for r in _records),
          f"logged {[r.getMessage()[:60] for r in _records]}")
    check("PageFactExtractor says which of its two paths is live",
          PageFactExtractor(None, _Page("")).prose_enabled is False
          and PageFactExtractor(_Refuser(), _Page("")).prose_enabled is True,
          "the entry points print 'extraction off' for a run that still extracts")
    # The assembly, not just the class. Returning None here is what disabled
    # the whole extractor, so the check has to reach build_runtime itself.
    _, _, _modelless_runtime = build_runtime(_offline, load_channels())
    check("build_runtime hands back an extractor even with no model configured",
          _modelless_runtime is not None and not _modelless_runtime.prose_enabled,
          "the free schema.org path is disabled on every keyword-only run")

    # Regression for _facts_from_jsonld's "prefer the url match" rule (see its
    # docstring): the first node found is not necessarily this event's own.
    neighbour = _extract(_ld("https://luma.com/somebody-else",
                             start="2099-01-01T00:00:00-07:00"))
    check("a node describing a different url is not used",
          neighbour.start != "2099-01-01T00:00:00-07:00",
          f"took the neighbour's date: {neighbour.start!r}")
    check("and the model is asked instead", _Refuser.called)

    # Markup with no startDate is not worth preferring over the prose path.
    dateless = _extract(_ld("https://luma.com/x", start=""))
    check("markup without a startDate falls through to the model",
          _Refuser.called and not dateless.start, f"got {dateless.start!r}")

    # Markup isn't trustworthy either (see _facts_from_jsonld): it still has
    # to clear the same _valid_iso gate as the model's answer.
    absurd = _extract(_ld("https://luma.com/x", start="2099-01-01T00:00:00-07:00"))
    check("a startDate outside the plausible window is refused even when typed",
          not absurd.start, f"accepted {absurd.start!r}")
    malformed = _extract(_ld("https://luma.com/x", start="next Tuesday"))
    check("a startDate that is not a timestamp is refused",
          not malformed.start, f"accepted {malformed.start!r}")

    # A node that names no url could be this event or the neighbour beside it.
    def _two_unnamed():
        one = ('{"@type": "Event", "name": "A", '
               '"startDate": "2026-09-20T18:00:00-07:00"}')
        two = ('{"@type": "Event", "name": "B", '
               '"startDate": "2026-10-05T18:00:00-07:00"}')
        return ('<html><script type="application/ld+json">[' + one + "," + two
                + "]</script><body><p>Join us on September 20 at 6pm.</p>"
                + "</body></html>")

    ambiguous = _extract(_two_unnamed())
    check("two nodes naming no url are ambiguous, so neither is used",
          ambiguous.start != "2026-09-20T18:00:00-07:00"
          and ambiguous.start != "2026-10-05T18:00:00-07:00",
          f"picked one anyway: {ambiguous.start!r}")

    def _one_unnamed():
        node = ('{"@type": "Event", "name": "Only", '
                '"startDate": "2026-09-20T18:00:00-07:00"}')
        return ('<html><script type="application/ld+json">' + node
                + "</script><body><p>prose</p></body></html>")

    lone = _extract(_one_unnamed())
    check("a lone node naming no url is the page's own event",
          lone.start == "2026-09-20T18:00:00-07:00", f"got {lone.start!r}")

    # Regression fixture for _facts_from_jsonld's identity-before-date
    # ordering: this event's own (dateless) node beside an unrelated (dated)
    # one, so a wrong pick would look like a confident extraction, not a crash.
    def _mine_dateless_neighbour_dated():
        mine = ('{"@type": "Event", "url": "https://luma.com/x", '
                '"name": "The Real Event"}')
        other = ('{"@type": "Event", "name": "Unrelated Neighbour", '
                 '"startDate": "2026-11-01T18:00:00-07:00", '
                 '"location": {"@type": "Place", "name": "Nowhere, XX"}}')
        return ('<html><script type="application/ld+json">[' + mine + "," + other
                + "]</script><body><p>Join us on September 20 at 6pm.</p>"
                + "</body></html>")

    borrowed = _extract(_mine_dateless_neighbour_dated())
    check("a dateless node for THIS event does not hand the page to a neighbour",
          borrowed.start != "2026-11-01T18:00:00-07:00"
          and borrowed.location != "Nowhere, XX",
          f"borrowed start={borrowed.start!r} location={borrowed.location!r}")
    check("it falls through to the model instead", _Refuser.called)

    # Self-references are often relative, which compares unequal until resolved.
    relative = _extract(_ld("/x"))
    check("a node referring to itself by a relative url still matches",
          relative.start == "2026-09-20T18:00:00-07:00", f"got {relative.start!r}")

    section("extract - an unreadable page must yield nothing, not a guess")
    # Same fabrication _page_text's docstring measures (tesla.com 403 -> a
    # confident but jittering start time), affecting all five tesla.com
    # /event/*-resume links; unguarded, it would reach the ledger, the date
    # window, the urgency tier and the .ics alike.
    class _Dead:
        def get_text(self, url):
            raise RuntimeError("HTTP Error 403: Forbidden")

    class _LoudInvoker:
        called = False

        def json_call(self, system, user, schema, name):
            _LoudInvoker.called = True
            return '{"title": "invented", "start": "2026-08-31T17:30:00-05:00", '
            '"end": "", "location": "Virtual", "rsvp_deadline": "", "organizer": "Tesla"}'

    thin = _event(event_uid="newsletter:https://tesla.com/event/x",
                  url="https://tesla.com/event/x", source_kind="newsletter",
                  description="Linked from https://d.beehiiv.com/p/x")
    out = PageFactExtractor(_LoudInvoker(), _Dead()).extract(thin)
    check("an unreadable page never reaches the model", not _LoudInvoker.called,
          "the model was asked to extract from the placeholder description")
    check("nothing is invented for it", not out.start and not out.location,
          f"start={out.start!r} location={out.location!r}")
    check("the failure is recorded, not swallowed", out.page_unreadable)

    # Second guard, for the case where a page IS readable but states no date.
    class _DatelessPage:
        def get_text(self, url):
            return "<html><body><p>Tesla resume workshop. Sign up now.</p></body></html>"

    class _DateInventor:
        def json_call(self, system, user, schema, name):
            return ('{"title": "", "start": "2026-08-31T17:30:00-05:00", "end": "", '
                    '"location": "Virtual", "rsvp_deadline": "", "organizer": "Tesla"}')

    dated = PageFactExtractor(_DateInventor(), _DatelessPage()).extract(thin)
    check("a timestamp is refused when the page states no date at all",
          not dated.start, f"accepted start={dated.start!r}")
    check("non-date facts from a readable page are still kept",
          dated.organizer == "Tesla", f"organizer={dated.organizer!r}")

    check("an event whose page could not be read is kept, not dropped as "
          "out of region", geo.keep(_event(url="https://tesla.com/event/x",
                                           source="direct-consideration",
                                           page_unreadable=True)),
          "dropping it here is the silent miss the project exists to prevent")
    check("page_unreadable does not override a location that names elsewhere",
          not geo.keep(_event(url="https://tesla.com/event/y",
                              source="direct-consideration", location="Boston, MA",
                              page_unreadable=True)))

    # Same false positives _DATE_EVIDENCE's comment measures ("Q4 2026",
    # "Builders Cup 2026" read as bare years); treating them as evidence
    # would let an invented timestamp through the guard meant to stop that.
    evidence = [
        ("a spelled-out date", "September 20, doors at 6pm.", True),
        ("a numeric date with no year", "Join us on 9/20 for the workshop.", True),
        ("an ISO timestamp", "Starts 2026-09-20T18:00.", True),
        ("a bare year in an event name", "Builders Cup 2026, a hackathon.", False),
        ("a bare year in a sentence", "What AI search means for Q4 2026.", False),
        ("a price that looks like a year", "Tickets are $2026 per seat.", False),
        ("no date at all", "Tesla resume workshop. Sign up now.", False),
        # A bare month alternation matched all four of these on real pages.
        ("may the modal verb", "You may bring a guest.", False),
        ("march the verb", "March through the exhibits at your own pace.", False),
        ("an id that looks like a date", "Order id 00-8 confirmed.", False),
        ("an abbreviated month with a day", "Sept. 20 at the campus centre", True),
        ("a day before its month", "20 September, doors at 6pm.", True),
    ]
    for why, text, want in evidence:
        check(f"date evidence: {why}",
              PageFactExtractor._states_a_date(text) is want, repr(text))

    section("sources._text.strip_html - one implementation, both scrapers")
    check("HTML entities are decoded (23 of 100 alumni titles carry one)",
          strip_html("Welcome to Your City 2026 &#8211; <b>SF</b>") == "Welcome to Your City 2026 – SF")

    section("sources._links - only real event links become candidates")
    # Every case here is one that actually went wrong on live data.
    link_cases = [
        ("https://www.tesla.com/event/northeastern-resume", True,
         "an employer campaign landing page"),
        ("https://luma.com/genai-sf", True, "a Luma calendar"),
        ("https://app.joinhandshake.com/events/12345", True, "a Handshake event"),
        ("https://handshake-production-cdn.joinhandshake.com/static_assets/"
         "email_assets/student_footer_v3_icons/x-lavender.png", False,
         "an email footer icon: five of these were harvested as events and sent "
         "to the extractor, which tried to UTF-8 decode a PNG"),
        ("https://media.beehiiv.com/img/foo.png", False, "a newsletter image"),
        ("https://email.g.joinhandshake.com/c/eJx8k81yFAKE", False,
         "a click tracker: per-send URL, so dedupe can never collapse it"),
        ("https://directconsideration.beehiiv.com/unsubscribe/abc", False,
         "an unsubscribe footer"),
    ]
    for url, want, why in link_cases:
        check(f"{'accepts' if want else 'rejects'} {why}",
              is_event_link(url) is want, url[:88])

    section("sources.jsonld - parses offline through StubHttpClient")
    stub = StubHttpClient({"http://t/": '''<script type="application/ld+json">
    {"@type":"ItemList","itemListElement":[{"@type":"ListItem","item":{
      "@type":["Event","Hackathon"],"name":"Stub Career Fair",
      "url":"https://t/e?utm_source=x","startDate":"2026-10-01T10:00:00-07:00",
      "location":{"name":"San Jose, CA"},
      "eventAttendanceMode":"https://schema.org/OfflineEventAttendanceMode"}}]}
    </script>'''})
    parsed = JsonLdSource("stub", "http://t/", stub).fetch()
    check("one Event parsed out of a nested ItemList", len(parsed) == 1, f"got {len(parsed)}")
    if parsed:
        ev = parsed[0]
        check("startDate kept verbatim, never re-guessed", ev.start == "2026-10-01T10:00:00-07:00")
        check("tracking stripped from the event url", ev.url == "https://t/e")
        check("schema.org URI mapped to AttendanceMode.OFFLINE",
              ev.attendance_mode is AttendanceMode.OFFLINE)

    section("sources.gmail_label - a dead mailbox must not look like a quiet one")
    # imap.uid can fail two ways (see GmailLabelSource._events_in_message's
    # comment); both are exercised here because a test for only one is what
    # let the other slip through review.
    import imaplib as _imaplib

    _MSG = (b"Subject: T\r\nDate: Mon, 8 Sep 2026 10:00:00 -0700\r\n"
            b'Content-Type: text/html\r\n\r\n<a href="https://luma.com/a">E</a>')

    class _FakeImap:
        def __init__(self, mode):
            self.mode, self.n, self.literal = mode, 0, None

        def login(self, *a): return ("OK", [b""])
        def list(self): return ("OK", [b'(\\All) "/" "[Gmail]/All Mail"'])
        def select(self, box, readonly=False):
            return (("NO" if self.mode == "select_fails" else "OK"), [b"3"])

        def uid(self, cmd, *a):
            if cmd == "SEARCH":
                if self.mode == "search_rejected":
                    return ("BAD", [b""])
                return ("OK", [b"" if self.mode == "empty_label" else b"1 2 3"])
            self.n += 1
            if self.mode == "fetch_all_abort":
                raise _imaplib.IMAP4.abort("socket error")
            if self.mode == "fetch_one_abort" and self.n == 2:
                raise _imaplib.IMAP4.abort("socket error")
            if self.mode == "fetch_all_no":
                return ("NO", [b""])
            return ("OK", [(b"1 (BODY[] {0}", _MSG)])

        def close(self): pass
        def logout(self): pass

    _real = _imaplib.IMAP4_SSL
    gmail_cases = [
        ("empty_label", "kept", "a label with no mail returns empty, not an error"),
        ("search_rejected", "raised", "a rejected SEARCH raises, never an empty list"),
        ("select_fails", "raised", "a failed SELECT raises before SEARCH can lie"),
        ("fetch_all_no", "raised", "every FETCH refused raises"),
        ("fetch_all_abort", "raised", "every FETCH raising IMAP4.abort still raises"),
        ("fetch_one_abort", "kept", "ONE FETCH raising is tolerated, the rest continue"),
    ]
    try:
        for mode, want, why in gmail_cases:
            _imaplib.IMAP4_SSL = lambda *a, _m=mode, **k: _FakeImap(_m)
            try:
                GmailLabelSource("t", "u", "p", "L").fetch()
                got = "kept"
            except RuntimeError:
                got = "raised"
            check(why, got == want, f"expected {want}, got {got}")
    finally:
        _imaplib.IMAP4_SSL = _real

    # Regression for the gap _summary's docstring describes (measured before
    # body_chars existed): a generic subject alone starved the keyword gate.
    #
    # TWO bounds apply in series and they are not the same number: the source
    # stores max_description_chars of body (what the scorer will read), and
    # _hits then looks at only keyword_match_prefix_chars of that. The cases
    # below pin each one separately, because a term landing between them looks
    # like a pass for the wrong reason.
    _settings = load_settings()
    _kw = next((k for k in _settings.keywords if " " not in k), "recruiter")

    def _mail_with_term_at(chars: int) -> bytes:
        """A message whose only keyword sits roughly `chars` bytes into the body."""
        filler = b"<p>filler</p>" * max(0, chars // 7)
        return (b"Subject: New events this week"
                b"\r\nDate: Mon, 8 Sep 2026 10:00:00 -0700\r\n"
                b"Content-Type: text/html\r\n\r\n<html><body>"
                b'<a href="https://luma.com/handshake-event">Register</a>'
                + filler + b"<p>" + _kw.encode() + b"</p></body></html>")

    def _gated(message: bytes, body_chars: int) -> bool:
        class _Rich(_FakeImap):
            def uid(self, cmd, *a):
                if cmd == "SEARCH":
                    return ("OK", [b"1"])
                return ("OK", [(b"1 (BODY[] {0}", message)])

        _imaplib.IMAP4_SSL = lambda *a, **k: _Rich("ok")
        event = GmailLabelSource("t", "u", "p", "L", body_chars=body_chars).fetch()[0]
        return _passes_gate(event, _settings)

    # Derived, not a magic number: `far` only tests the prefix bound while it
    # lands beyond it, so it has to move when the setting does.
    # Every fixture above carries ONE link, so nothing pinned what happens when
    # a message carries several: organizer and the body summary are per MESSAGE
    # and were hoisted out of the harvest loop, while the title is per link.
    _two = (b"Subject: Two events\r\nFrom: Handshake <no-reply@joinhandshake.com>\r\n"
            b"Date: Mon, 8 Sep 2026 10:00:00 -0700\r\n"
            b"Content-Type: text/html\r\n\r\n<html><body><p>career fair week</p>"
            b'<a href="https://luma.com/first">Resume Review</a>'
            b'<a href="https://luma.com/second">Career Fair</a></body></html>')

    class _TwoLinkImap(_FakeImap):
        def uid(self, cmd, *a):
            if cmd == "SEARCH":
                return ("OK", [b"1"])
            return ("OK", [(b"1 (BODY[] {0}", _two)])

    _imaplib.IMAP4_SSL = lambda *a, **k: _TwoLinkImap("ok")
    try:
        pair = sorted(GmailLabelSource("t", "u", "p", "L").fetch(), key=lambda e: e.url)
        check("one message yields one Event per link", len(pair) == 2, f"got {len(pair)}")
        if len(pair) == 2:
            one, two = pair
            check("both links share the message's organizer and summary",
                  one.organizer == two.organizer and one.description == two.description,
                  f"{one.organizer!r} vs {two.organizer!r}")
            check("each link keeps its own title", one.title != two.title,
                  f"both titled {one.title!r}")
            check("the summary carries the body, not just the subject",
                  "career fair week" in one.description.lower(), one.description[:80])
    finally:
        _imaplib.IMAP4_SSL = _real

    # TODO item 4. The digest this project mails is delivered to an address it
    # also reads, so without skip_from every link it printed comes back as an
    # Event whose description is the whole digest.
    _own = (b"Subject: [Event Scout] 53 events (6 closing soon)\r\n"
            b"From: Event Scout <digest@example.com>\r\n"
            b"Date: Mon, 8 Sep 2026 10:00:00 -0700\r\n"
            b"Content-Type: text/html\r\n\r\n<html><body><p>fit=9 access=8</p>"
            b'<a href="https://luma.com/first">Antler SF After Dark</a>'
            b'<a href="https://luma.com/second">Career Fair</a></body></html>')

    class _OwnMailImap(_FakeImap):
        def uid(self, cmd, *a):
            if cmd == "SEARCH":
                return ("OK", [b"1"])
            return ("OK", [(b"1 (BODY[] {0}", _own)])

    def _own_fetch(skip_from):
        """Events, or the RuntimeError text, never a raise.

        Returning the error instead of propagating it lets the [] vs None
        assertion below report a [FAIL]: a skip that wrongly returned None
        would trip fetch's every-FETCH-failed guard and abort check.py,
        which counts as caught only if the mutation is judged by exit code
        rather than the [FAIL] tally.
        """
        try:
            return GmailLabelSource("t", "u", "p", "L", skip_from=skip_from).fetch()
        except RuntimeError as exc:
            return f"raised {exc}"

    _imaplib.IMAP4_SSL = lambda *a, **k: _OwnMailImap("ok")
    try:
        check("without skip_from the digest is harvested link by link",
              _own_fetch("") == _own_fetch("") and len(_own_fetch("")) == 2,
              "the fixture stopped producing two links, so the cases below "
              "would pass for the wrong reason")
        # Case matters: the address is compared lowercased, and a From header
        # is free to capitalise it. An empty list is also asserted against a
        # raise, because _events_in_message returns [] and not None for a skip
        # so that a label of nothing but our own mail contributes no events
        # rather than tripping the every-FETCH-failed guard.
        for why, configured in [
                ("skip_from drops mail sent from that address", "digest@example.com"),
                ("the address match is case-insensitive", "Digest@Example.COM")]:
            got = _own_fetch(configured)
            check(why, got == [], f"got {got if isinstance(got, str) else len(got)}")
        # The guard keys on the sender, not on the subject: a run's own counts
        # build that subject, so it changes without anyone editing it.
        other = _own_fetch("someone-else@example.com")
        check("an unrelated skip_from leaves the message alone",
              isinstance(other, list) and len(other) == 2, f"got {other}")
    finally:
        _imaplib.IMAP4_SSL = _real

    near = _mail_with_term_at(600)
    far = _mail_with_term_at(_settings.keyword_match_prefix_chars + 1000)
    try:
        gate_cases = [
            ("a keyword in the body admits a mail with a generic subject",
             near, _settings.max_description_chars, True),
            ("the subject alone would have dropped it", near, 0, False),
            ("body_chars bounds what the source hands on",
             near, 100, False),
            ("keyword_match_prefix_chars bounds what the gate reads, and it is "
             "the stricter of the two", far, _settings.max_description_chars, False),
        ]
        for why, message, chars, want in gate_cases:
            check(why, _gated(message, chars) is want,
                  f"body_chars={chars}, expected gate {want}")
    finally:
        _imaplib.IMAP4_SSL = _real

    # Guards the import-order hazard gmail_label.py's own comment names,
    # which would silently take all three configured mailbox sources dark
    # at once (they share this one module-level import).
    _probe = subprocess.run(
        [sys.executable, "-c",
         "import sys; sys.path.insert(0, %r)\n"
         "import eventscout.sources.gmail_label\n"
         "print('email.utils' in sys.modules)" % str(Path(__file__).resolve().parent)],
        capture_output=True, text=True)
    check("gmail_label imports email.utils rather than inheriting it",
          _probe.stdout.strip() == "True", _probe.stdout.strip() or _probe.stderr[-120:])

    section("sources.newsletter - a broken pattern must not look like a quiet week")
    big_but_empty = StubHttpClient({"https://fake.beehiiv.com/archive": "<html>" + "x" * 30000 + "</html>"})
    try:
        NewsletterArchiveSource("dc", "fake.beehiiv.com", big_but_empty).fetch()
        check("a large archive yielding zero slugs raises", False,
              "it returned empty, which the pipeline would record as 'no new events'")
    except ValueError as exc:
        check("a large archive yielding zero slugs raises", True)
        print(f"         {exc}")
    tiny = StubHttpClient({"https://new.beehiiv.com/archive": "<html>tiny</html>"})
    try:
        NewsletterArchiveSource("dc", "new.beehiiv.com", tiny).fetch()
        check("a genuinely tiny archive does NOT raise (new publication)", True)
    except ValueError as exc:
        check("a genuinely tiny archive does NOT raise (new publication)", False, str(exc))

    # Same real-ledger shape as the published_at case in models.Event's
    # construction check above; this side checks the SOURCE normalises it,
    # not just that Event would reject a raw value.
    zulu = StubHttpClient({
        "https://z.beehiiv.com/archive":
            '{"slug":"issue-z"}{"scheduled_at":"2026-08-31T16:23:13Z"}',
        "https://z.beehiiv.com/p/issue-z": '<a href="https://luma.com/z">E</a>'})
    dated = NewsletterArchiveSource("dc", "z.beehiiv.com", zulu).fetch()
    check("a Z-suffixed publish date is normalised at the source",
          bool(dated) and dated[0].published_at == "2026-08-31T16:23:13+00:00",
          f"got {dated[0].published_at!r}" if dated else "no event harvested")

    # Same rule as GmailLabelSource, checked the same way: one unreachable issue
    # is tolerated, every issue unreachable is a broken channel. StubHttpClient
    # raises KeyError for a page it was not given, standing in for the network
    # failure.
    _INDEX = '{"slug":"issue-one"}{"slug":"issue-two"}'
    _ISSUE = '<a href="https://luma.com/x">E</a>'
    partial = StubHttpClient({"https://h.beehiiv.com/archive": _INDEX,
                              "https://h.beehiiv.com/p/issue-one": _ISSUE})
    try:
        got = NewsletterArchiveSource("dc", "h.beehiiv.com", partial).fetch()
        check("one unreachable issue is tolerated, the readable one survives",
              len(got) == 1, f"got {len(got)} events")
    except Exception as exc:
        check("one unreachable issue is tolerated, the readable one survives",
              False, f"{type(exc).__name__}: {exc}")
    all_gone = StubHttpClient({"https://h.beehiiv.com/archive": _INDEX})
    try:
        NewsletterArchiveSource("dc", "h.beehiiv.com", all_gone).fetch()
        check("every issue unreachable raises, never an empty list", False,
              "it returned empty, which the pipeline would record as 'no new events'")
    except RuntimeError:
        check("every issue unreachable raises, never an empty list", True)

    # The guard covers the fetch ONLY (see NewsletterArchiveSource.fetch); a
    # builder bug would otherwise surface as the generic "every issue failed
    # to load" instead of its own message, so the test asserts on the message.
    import eventscout.sources.newsletter as _nl

    def _exploding_harvest(_body):
        raise RuntimeError("builder bug")

    reachable = StubHttpClient({"https://h.beehiiv.com/archive": _INDEX,
                                "https://h.beehiiv.com/p/issue-one": _ISSUE,
                                "https://h.beehiiv.com/p/issue-two": _ISSUE})
    _real_harvest = _nl.harvest
    _nl.harvest = _exploding_harvest
    try:
        NewsletterArchiveSource("dc", "h.beehiiv.com", reachable).fetch()
        check("a bug in the Event builder is not absorbed as a read failure",
              False, "it was swallowed and counted as an unreadable issue")
    except RuntimeError as exc:
        check("a bug in the Event builder is not absorbed as a read failure",
              "builder bug" in str(exc), f"raised the wrong error: {exc}")
    finally:
        _nl.harvest = _real_harvest

    section("sources - a cap that fires must say so")
    # Reproduces the two real regressions from Coverage's docstring
    # (WordPress) and GmailLabelSource's cap comment (the mailbox).
    def _wp_page(tag, size):
        return json.dumps([{"link": f"https://wp.test/e{tag}{i}",
                            "title": {"rendered": f"Event {tag}{i}"}}
                           for i in range(size)])

    wp = WordPressSource("wp", "http://wp/", StubHttpClient({
        "http://wp/?per_page=2&page=1": _wp_page(1, 2),
        "http://wp/?per_page=2&page=2": _wp_page(2, 1)}), per_page=2)
    got = wp.fetch()
    check("paging stops on a short page, and a complete read reports nothing",
          len(got) == 3 and wp.coverage() is None,
          f"{len(got)} events, coverage {wp.coverage()}")

    wp = WordPressSource("wp", "http://wp/", StubHttpClient({
        "http://wp/?per_page=2&page=1": _wp_page(1, 2),
        "http://wp/?per_page=2&page=2": _wp_page(2, 2)}), per_page=2, max_pages=2)
    got = wp.fetch()
    cov = wp.coverage()
    check("exhausting max_pages reports a cap whose remainder is UNKNOWN, not "
          "merely uncounted",
          len(got) == 4 and cov is not None and cov.available is None,
          f"{len(got)} events, coverage {cov}")
    check("and its wording says so",
          str(cov) == "CAPPED at 4 events, more may exist", str(cov))

    raised = False
    try:
        WordPressSource("wp", "http://wp/", StubHttpClient({}), per_page=2).fetch()
    except Exception:
        raised = True
    check("a failure on PAGE 1 still raises, so a dead endpoint cannot pass for "
          "a quiet week", raised, "it swallowed the failure")

    # Page 2 missing is the WordPress 400 case (see fetch()'s except
    # comment): page 1 already proved the channel works, so this is
    # recorded, not raised.
    wp = WordPressSource("wp", "http://wp/", StubHttpClient(
        {"http://wp/?per_page=2&page=1": _wp_page(1, 2)}), per_page=2)
    got = wp.fetch()
    check("a failure on a LATER page is recorded instead, since page 1 proved "
          "the channel works",
          len(got) == 2 and wp.coverage() is not None,
          f"{len(got)} events, coverage {wp.coverage()}")

    nl_pages = {"https://h.beehiiv.com/archive": "<html>no slugs</html>"}
    for slug in ("one", "two"):
        nl_pages["https://h.beehiiv.com/p/" + slug] = (
            '<a href="https://luma.com/' + slug + '">Career fair</a>')
    nl = NewsletterArchiveSource("nl", "h.beehiiv.com", StubHttpClient(nl_pages),
                                 max_issues=2, only_slugs=["one", "two", "three"])
    nl.fetch()
    check("the newsletter issue cap is reported with both numbers",
          str(nl.coverage()) == "CAPPED, read 2 of 3 issues, 1 never seen",
          str(nl.coverage()))

    _real_ssl = _imaplib.IMAP4_SSL
    try:
        _imaplib.IMAP4_SSL = lambda *a, **k: _FakeImap("ok")
        gm = GmailLabelSource("t", "u", "p", "L", max_messages=2)
        gm.fetch()
        check("the mailbox cap is reported with both numbers",
              str(gm.coverage()) == "CAPPED, read 2 of 3 messages, 1 never seen",
              str(gm.coverage()))
        gm = GmailLabelSource("t", "u", "p", "L", max_messages=99)
        gm.fetch()
        check("a mailbox under its cap reports nothing",
              gm.coverage() is None, f"coverage {gm.coverage()}")
    finally:
        _imaplib.IMAP4_SSL = _real_ssl

    raised = False
    try:
        WordPressSource("wp", "http://wp/", StubHttpClient({})).coverage()
    except RuntimeError:
        raised = True
    check("coverage() before fetch() raises, because None would be "
          "indistinguishable from a complete read",
          raised, "it answered None, which reads as full coverage")

    class _Capped:
        kind = name = "jsonld"

        def fetch(self):
            return [_listing("jsonld", "https://luma.com/capped")]

        def coverage(self):
            return Coverage(1, "events", 9)

    with tempfile.TemporaryDirectory() as tmp_cov:
        db = SqliteEventStore(Path(tmp_cov) / "cov.db")
        try:
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                run([_Capped()], db, build_geo(load_channels()), _offline,
                    _Mailbox(), dry_run=True, now=_NOW)
            line = next((l for l in out.getvalue().splitlines()
                         if l.strip().startswith("jsonld")), "")
            check("the funnel prints the cap beside the count, because the count "
                  "on its own looks healthy",
                  "CAPPED, read 1 of 9 events" in line, f"line {line!r}")
        finally:
            db.close()


# --------------------------------------------------------------------------
# LIVE
# --------------------------------------------------------------------------
# (name, url, minimum event count, expected to keep at least one after geo).
# The floor is what catches a silently broken parser; keep it well under the
# observed count so normal week-to-week variation does not cry wolf.
JSONLD_SOURCES = [
    ("neu-silicon-valley-luma", "https://luma.com/NUSiliconValleyCampus", 5, True),
    ("luma-genai-sf", "https://luma.com/genai-sf", 5, True),
    ("neu-bayarea-student-life", "https://studentlife.bayarea.northeastern.edu/events/", 5, True),
    ("cerebral-valley-events", "https://cerebralvalley.ai/events", 5, True),
    ("luma-sf", "https://luma.com/sf", 5, True),
    ("luma-discover-sf-ai", "https://luma.com/discover/sf/ai", 5, True),
    ("luma-ai-sf", "https://luma.com/ai-sf", 5, True),
    # Eventbrite is judged on category, not volume: typically only one or two of
    # its listings survive the geo filter, but they are career fairs, which no
    # other source surfaces. So the floor is low and no in-scope hit is required.
    ("eventbrite-sf-tech-career",
     "https://www.eventbrite.com/d/ca--san-francisco/tech-career-fair/", 3, False),
]
TESLA = "https://tesla.com/event/northeastern-resume"


def live() -> None:
    http = UrllibHttpClient(timeout=30)
    # The deployed filter, not a copy of it: a live source's yield is only
    # meaningful against the geography the real runs use.
    geo = build_geo(load_channels())

    section("BACKTEST - a mail-only employer event, recovered from the archive")
    try:
        pinned = NewsletterArchiveSource("direct-consideration", "directconsideration.beehiiv.com",
                                         http, only_slugs=["direct-consideration-004"])
        found = pinned.fetch()
        hit = [e for e in found if e.url == TESLA]
        check("direct-consideration-004 yields tesla.com/event/northeastern-resume",
              bool(hit), f"harvested {[e.url for e in found]}")
        if hit:
            check("the harvested item carries the issue's publish date, so "
                  "first_run.require_known_publish_time will not drop it",
                  bool(hit[0].published_at), "published_at is empty")
            warn("its start is empty, as expected for prose (the LLM extractor fills it)",
                 not hit[0].start, f"unexpectedly got start={hit[0].start!r}")
    except Exception:
        check("backtest ran", False, traceback.format_exc(limit=2))

    section("newsletter archive - can it still discover issues?")
    try:
        live_src = NewsletterArchiveSource("direct-consideration",
                                           "directconsideration.beehiiv.com", http)
        dates = live_src._slug_dates()
        check("archive index yields issues", len(dates) >= 3, f"only {len(dates)}")
        undated = [s for s, d in dates.items() if not d]
        warn("every discovered issue has a publish date",
             not undated, f"undated: {undated}")
        newest = max(dates.values()) if dates else ""
        print(f"         newest issue: {newest}  ({len(dates)} total)")
    except Exception:
        check("archive index readable", False, traceback.format_exc(limit=2))

    section("JSON-LD sources - silent zero is the failure to catch")
    total = kept = 0
    for name, url, floor, in_region in JSONLD_SOURCES:
        try:
            events = JsonLdSource(name, url, http).fetch()
            scoped = [e for e in events if geo.keep(e)]
            total += len(events)
            kept += len(scoped)
            check(f"{name} returns at least {floor} events",
                  len(events) >= floor, f"got {len(events)} - pattern may have broken")
            if in_region:
                warn(f"{name} keeps at least one event after the geo filter",
                     bool(scoped), f"{len(events)} fetched, 0 in scope")
            undated = [e for e in events if not e.has_known_start]
            warn(f"{name} events all carry a start date", not undated,
                 f"{len(undated)} without one")
            print(f"         {len(events)} fetched -> {len(scoped)} in scope")
        except Exception:
            check(f"{name} fetched", False, traceback.format_exc(limit=2))
    print(f"\n  TOTAL {total} fetched -> {kept} in scope")

    section("WordPress source - the post-date trap and field hygiene")
    try:
        wp = WordPressSource("neu-alumni-events",
                             "https://alumni.northeastern.edu/wp-json/wp/v2/event",
                             http, per_page=100)
        events = wp.fetch()
        check("alumni endpoint returns at least 20 events", len(events) >= 20, f"got {len(events)}")
        iso = [e for e in events if e.start and not e.start[:4].isdigit()]
        check("start is ISO, not the raw epoch string WordPress stores", not iso,
              f"{len(iso)} unconverted, e.g. {iso[0].start if iso else ''}")
        drifted = [e for e in events if e.start and e.published_at
                   and e.start[:10] != e.published_at[:10]]
        check("start differs from the post date on at least some events, proving "
              "the publish-time field is not being read as the event time",
              bool(drifted), "every start equals its publish date, which is the trap")
        digits = [e for e in events if e.location
                  and e.location.replace(",", "").replace(" ", "").isdigit()]
        check("no taxonomy term IDs leaked into location", not digits,
              f"e.g. {digits[0].location if digits else ''}")
        ents = [e for e in events if "&#" in e.title or "&amp;" in e.title]
        check("titles are HTML-unescaped", not ents,
              f"e.g. {ents[0].title if ents else ''}")
        print(f"         {len(events)} events, {len(drifted)} with start != publish date")
    except Exception:
        check("alumni endpoint fetched", False, traceback.format_exc(limit=2))


def main() -> int:
    args = set(sys.argv[1:])
    run_offline = "--live" not in args
    run_live = "--offline" not in args
    parts = [name for name, on in (("design invariants", run_offline),
                                   ("live sources", run_live)) if on]
    print(f"Event Scout self-check  |  running: {' + '.join(parts)}")
    total = len(parts)
    if run_offline:
        label = f"PART 1 of {total}" if total > 1 else "ONLY SECTION"
        print("\n" + "=" * 74)
        print(f"{label}  -  DESIGN INVARIANTS   (no network, fully deterministic)")
        print("=" * 74)
        offline()
    if run_live:
        label = f"PART {total} of {total}" if total > 1 else "ONLY SECTION"
        print("\n" + "=" * 74)
        print(f"{label}  -  LIVE SOURCES   (real network; a FAIL here usually means a site changed)")
        print("=" * 74)
        live()

    print("\n" + "=" * 74)
    if FAILS:
        print(f"FAILED {len(FAILS)} design rule(s):")
        for rule in FAILS:
            print(f"  - {rule}")
    if WARNS:
        print(f"\n{len(WARNS)} warning(s):")
        for rule in WARNS:
            print(f"  - {rule}")
    if not FAILS and not WARNS:
        print(f"All checks passed  ({' + '.join(parts)}).")
    elif not FAILS:
        print("\nNo design rules broken.")
    # Conditional on run_live because the flat version claimed "never touches
    # the network" on every run, contradicting the LIVE SOURCES header printed
    # a few lines earlier. That made a Luma HTTP 429 read as a regression.
    print("\nScope: sources, parsing, filters and protocol conformance. The")
    print("pipeline runs end to end once, on the keyword tier against a throwaway")
    print("ledger, so no model is called and no mail leaves. For a real run, use")
    print("local_run.py --dry-run")
    if run_live:
        print("\nThe live-sources part DID use the network. A FAIL there is usually a")
        print("site change or a rate limit, not a regression, so re-run before")
        print("believing it. --offline skips that part entirely.")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
