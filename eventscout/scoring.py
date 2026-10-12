"""Scoring and urgency.

Three axes, never collapsed before the digest sees them.

  fit           topical relevance to the profile
  access_value  how much attending this KIND of event is worth to the reader
  cost          travel and time

Only fit and access_value rank; see Score.rank for why cost is excluded rather
than forgotten. The two axes are deliberately independent because they point in
opposite directions often enough to matter. An employer resume-review event is
low fit, because it reads as a generic resume workshop, and maximum access
value for a reader who wants hiring channels, because that employer reads the
resume for direct consideration. Rank on fit alone and exactly the events worth
attending are the ones discarded.

What each axis rewards is config.yaml's to say (see preferences.py). This
module holds only the rules that hold for any reader.

Tiers, in precedence order: OpenAI API, else each `llm_clis` entry installed on
this machine in config order, else the deterministic keyword tier. Only the
keyword tier is guaranteed available.
"""
from __future__ import annotations

import hashlib
import logging
import math
import re
import shutil
import threading
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import ClassVar

from ._llm_json import lenient_json
from .models import parse_iso, Event, Score, Urgency
from .preferences import ACCESS_SCALE, ScoringPreferences
from .protocols import EventScorer, JsonInvoker, ScoreCache

log = logging.getLogger("eventscout.scoring")

# Part of the score cache key. The system prompt is fingerprinted into the key
# on its own (see ScoringPrompt), so bump this only for what the fingerprint
# cannot see, which is _blob, _SCHEMA, the CLI's closing instruction and
# _parse_score. A stale tag keeps serving answers to the old question.
# Switching `reason` to Chinese once changed nothing on screen, because every
# event was a cache hit on a tag that only named the model.
PROMPT_VERSION = "5"

# Holds for every reader, so it is not in config.yaml. Measured on five
# tesla.com/event/*-resume links, one per university, which scored 80-90 alike
# until this clause existed, so four of the five would have been mailed as if
# the reader could attend them.
_ELIGIBILITY_CAP = (
    "Eligibility caps all of the above. An event restricted to another school's or "
    "another program's members is a 1 no matter how well it matches the levels "
    "above, because this person cannot attend it.")


@dataclass(frozen=True)
class ScoringPrompt:
    """The system prompt every LLM tier sends, the same for every event.

    `fingerprint` goes into the score cache key, so any change to a preference,
    the profile or this wording retires the scores judged against the old text.
    """

    system: str

    @classmethod
    def build(cls, preferences: ScoringPreferences, profile_text: str) -> "ScoringPrompt":
        levels = "\n".join(f"{n}  {preferences.access_levels[n]}"
                           for n in sorted(preferences.access_levels, reverse=True))
        travel = preferences.travel
        regions = "".join(f", {n} {travel.describe[n]}" for n in sorted(travel.describe))
        return cls(
            "You rate events for one person. Return three integers 1-10.\n\n"
            "fit: topical relevance to their background and interests, as described "
            "in the profile below.\n\n"
            "access_value: how much attending this kind of event is worth to this "
            "person, judged ONLY by the levels below, scaled to 1-10 by doubling. "
            "This is INDEPENDENT of topic. Do not let fit leak into access_value.\n"
            f"{levels}\n\n{_ELIGIBILITY_CAP}\n\n"
            f"cost: burden of attending. {travel.virtual} virtual{regions}, "
            f"{travel.default} anywhere else, higher if it spans multiple days.\n\n"
            f"reason: ONE short sentence in {preferences.reason_language} naming the "
            "deciding factor. That field only; the other three stay numbers. Keep "
            "company and product names in their original spelling rather than "
            "transliterating them.\n\n"
            f"The person:\n{profile_text or '(no profile supplied)'}")

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.system.encode("utf-8")).hexdigest()[:16]


def _clamp(value, low: int = 1, high: int = 10) -> int:
    # Stays lenient (defaults to `low`, NaN included): also called by
    # KeywordScorer on values it computed itself, where that default is
    # correct. Model output gets the strict check first; see _as_number.
    try:
        return max(low, min(high, int(round(float(value)))))
    except (TypeError, ValueError):
        return low


# ---------------------------------------------------------------------------
# Keyword tier
# ---------------------------------------------------------------------------
_REASON_MIN_LEVEL = 4


def _bounded(terms) -> list[tuple[str, "re.Pattern", int]]:
    """(term, left-boundary pattern, weight) for each weighted term.

    A LEFT boundary ONLY. The right side stays open because prefix matching here
    is deliberate and measured. Exact-phrase matching once left 65 of 70 events
    at the access floor, and `recruit` has to keep reaching "recruiting",
    `hack` "hackathon" and "hackday", `intern` "internship". A two-sided
    `(?<![a-z0-9])kw s?(?![a-z0-9])` was tried and rejected here, because it
    broke all four of those. So only the LEFT-side collisions are fixed:
    `resume` no longer matches "presume", `hack` no longer matches "shack", and
    `ml` no longer matches inside "html".

    Three RIGHT-side collisions are accepted, because no boundary can separate
    two continuations of one prefix. `intern` still reaches "internal",
    `hack` "hackneyed", and `career` "careers page", which is arguably not a
    collision at all since a careers page does signal an employer. Removing the
    first two would mean dropping the bare terms, and that changes what gets
    mailed, so it is tuning rather than a defect fix.
    """
    return [(t.strip(), re.compile(rf"(?<![a-z0-9]){re.escape(t.strip())}"), w)
            for t, w in terms.items() if t.strip()]


class KeywordScorer:
    """Deterministic tier. No API key, no network, always available.

    Cannot honour the eligibility cap. Substring matching cannot tell
    "restricted to Georgia Tech students" from an open event, nor know which
    school the reader attends, so a restricted resume drop still scores
    access_value 10 here where the LLM tiers give it 1. Measured on five
    tesla.com/event/*-resume links. Accepted only because this tier runs when
    neither LLM tier is reachable at all; the digest footer names the tier, so
    a reader can see which judgement they are getting.
    """

    method_label: ClassVar[str] = "Keyword"
    # Caching a pure function that reads a dict costs a SQLite round trip to
    # save nothing, and would serve stale scores after a weight change.
    cacheable: ClassVar[bool] = False

    def __init__(self, preferences: ScoringPreferences):
        self._access = _bounded(preferences.access_keywords)
        self._fit = _bounded(preferences.fit_terms)
        self._penalty = _bounded(preferences.fit_penalty)
        self._travel = preferences.travel

    def score(self, event: Event) -> Score:
        text = f"{event.title} {event.description} {event.organizer}".lower()
        fit = sum(w for _, pat, w in self._fit if pat.search(text))
        fit += sum(w for _, pat, w in self._penalty if pat.search(text))
        # MAX, not sum: one unambiguous signal ("resume") should not be diluted
        # by the absence of others, and stacking weak terms should not fake a 5.
        level = max((w for _, pat, w in self._access if pat.search(text)), default=1)
        hits = sorted({t for t, pat, w in self._access
                       if w >= _REASON_MIN_LEVEL and pat.search(text)})
        # Doubled like the LLM tiers' access_value. Unscaled, this tier's rank
        # topped out at 50 against their 100, so p0_min_rank 40 was out of
        # reach for all but its highest fits.
        return Score(fit=_clamp(fit), access_value=_clamp(level * ACCESS_SCALE),
                     cost=self._cost(event),
                     reason=(f"keywords: {', '.join(hits[:3])}" if hits
                             else "keywords: topic only"),
                     method=self.method_label)

    def _cost(self, event: Event) -> int:
        if event.is_virtual:
            return self._travel.virtual
        here = (event.location or "").lower()
        return min((lvl for place, lvl in self._travel.places.items() if place in here),
                   default=self._travel.default)


# ---------------------------------------------------------------------------
# LLM tiers
# ---------------------------------------------------------------------------
_SCHEMA = {
    "type": "object",
    "properties": {
        "fit": {"type": "integer"},
        "access_value": {"type": "integer"},
        "cost": {"type": "integer"},
        "reason": {"type": "string"},
    },
    "required": ["fit", "access_value", "cost", "reason"],
    "additionalProperties": False,
}


def _as_number(value: object) -> float | None:
    """`value` as a FINITE number, or None.

    Separate from _clamp because that one is also used by KeywordScorer on
    values it computed itself, where defaulting is correct and raising is not.

    Finiteness is checked because float() does not: json.loads accepts the bare
    NaN and Infinity tokens, and float("nan") succeeds. A NaN axis would pass
    this gate, then fail inside _clamp's own int(round(...)), be swallowed by
    _clamp's except back to a real-looking 1, and be cached forever as a
    judgement -- the exact failure this gate exists to stop. Infinity instead
    raises OverflowError, which _clamp does not catch, so it surfaces as an
    unrecognisable error rather than "unusable response".
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_score(raw: str, method: str) -> Score:
    """Pull the score object out of a model response.

    An unparseable response RAISES rather than returning a middling score. The
    old fallback of fit=5, access_value=1 is rank 5, below the digest floor,
    and CachingScorer stored it like any real judgement -- keyed on the uid
    with no expiry, so one markdown-fenced reply buried that event until the
    prompt version changed for an unrelated reason. It never cleared the floor,
    so it never got cleared_floor_at either, and the sweep skips anything that
    was never reported. Raising instead lets _safe_score drop the event for
    this run only; the next run scores it again from nothing cached.
    """
    data = lenient_json(raw)
    # Every axis has to be a number. _clamp turns anything else into 1, which
    # is a real-looking score CachingScorer then keeps forever: "fit": "high",
    # a missing cost, or a null access_value all used to be stored as a
    # judgement. Same damage as an unreadable reply, narrower trigger, so the
    # same answer.
    if any(_as_number(data.get(axis)) is None
           for axis in ("fit", "access_value", "cost")):
        raise ValueError(f"unusable {method} response: {(raw or '').strip()[:120]!r}")
    return Score(fit=_clamp(data["fit"]), access_value=_clamp(data["access_value"]),
                 cost=_clamp(data["cost"]),
                 reason=str(data.get("reason", ""))[:200], method=method)


class _LlmEventScorer(ABC):
    """Template Method: shared prompt and parsing; subclasses only do the call."""

    # Round trip is the per-run cost driver here; see CachingScorer for why
    # that makes caching worthwhile.
    cacheable: ClassVar[bool] = True

    def __init__(self, prompt: ScoringPrompt, max_description_chars: int = 4000):
        self._prompt = prompt
        self._max_chars = max_description_chars

    def score(self, event: Event) -> Score:
        return _parse_score(
            self.json_call(self._prompt.system, self._blob(event), _SCHEMA, "event_scores"),
            self.method_label)

    def _blob(self, event: Event) -> str:
        # The url is included because it is sometimes the ONLY thing that
        # separates two events: tesla.com/event/northeastern-resume and
        # .../purdue-resume arrive with identical unknown fields when neither
        # page can be read, and only one of them is worth this reader's time.
        return (f"url: {event.url}\n"
                f"title: {event.title}\n"
                f"organizer: {event.organizer or 'unknown'}\n"
                f"when: {event.start or 'unstated'}\n"
                f"where: {event.location or 'unstated'}"
                f"{' (virtual)' if event.is_virtual else ''}\n"
                f"source: {event.source}\n"
                f"description: {event.description[:self._max_chars] or '(none)'}")

    @property
    @abstractmethod
    def method_label(self) -> str: ...

    @property
    @abstractmethod
    def cache_scope(self) -> str:
        """What this tier's answers depend on besides the prompt, as the head
        of the score cache tag. Anything that changes the model belongs here."""
        ...

    @abstractmethod
    def json_call(self, system_prompt: str, user_prompt: str,
                  schema: dict, schema_name: str) -> str:
        """Ask the model for JSON matching `schema`. Transport only; see
        JsonInvoker for why the schema must stay a parameter, not baked in.
        """
        ...


class OpenAiScorer(_LlmEventScorer):
    method_label = "API"

    def __init__(self, api_key: str, model: str, prompt: ScoringPrompt,
                 max_description_chars: int = 4000, reasoning_effort: str = "",
                 max_retries: int = 3):
        super().__init__(prompt, max_description_chars)
        self._api_key = api_key
        self._model = model
        self._reasoning_effort = reasoning_effort
        self._max_retries = max_retries
        self._client = None
        self._client_lock = threading.Lock()

    @property
    def cache_scope(self) -> str:
        return f"api:{self._model}:{self._reasoning_effort or 'default'}"

    def _client_instance(self):
        """The client, built once. Lazy so importing this module never needs
        the openai package, locked because _score_all calls score() from a
        ThreadPoolExecutor and two workers can otherwise both find None."""
        with self._client_lock:
            if self._client is None:
                from openai import OpenAI
                self._client = OpenAI(api_key=self._api_key)
            return self._client

    def json_call(self, system_prompt: str, user_prompt: str,
                  schema: dict, schema_name: str) -> str:
        request = {
            "model": self._model,
            "messages": [{"role": "system", "content": system_prompt},
                         {"role": "user", "content": user_prompt}],
            "response_format": {"type": "json_schema", "json_schema": {
                "name": schema_name, "strict": True, "schema": schema}},
        }
        # Only reasoning models accept this; sending it to a standard model errors.
        if self._reasoning_effort:
            request["reasoning_effort"] = self._reasoning_effort
        last: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                resp = self._client_instance().chat.completions.create(**request)
                return resp.choices[0].message.content
            except Exception as exc:
                last = exc
                log.warning("OpenAI scoring attempt %d failed: %s", attempt + 1, exc)
                if attempt + 1 < self._max_retries:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"OpenAI scoring failed after {self._max_retries} attempts") from last


class CliScorer(_LlmEventScorer):
    """Drives a local agent CLI for users without an API key. Best effort."""

    def __init__(self, command: list[str], prompt: ScoringPrompt,
                 max_description_chars: int = 4000, *, name: str, timeout: int = 180):
        super().__init__(prompt, max_description_chars)
        self._command = command
        self._name = name
        self._timeout = timeout

    @property
    def method_label(self) -> str:
        """Which llm_clis entry won is a property of the machine, so without the
        name a fall-through to the second entry would be invisible in the
        digest footer and the ledger."""
        return f"CLI ({self._name})"

    @property
    def cache_scope(self) -> str:
        # The model is pinned inside the args, so they are hashed in. argv[0]
        # is left out because it is this machine's install path, which changes
        # nothing about the answer.
        args = hashlib.sha256("\0".join(self._command[1:]).encode("utf-8")).hexdigest()[:12]
        return f"cli:{self._name}-{args}"

    def json_call(self, system_prompt: str, user_prompt: str,
                  schema: dict, schema_name: str) -> str:
        # No structured-output mode here, so the shape is restated in prose. It
        # is DERIVED from the schema argument rather than written out: hardcoding
        # the score keys made this tier answer a different question from the API
        # tier, and silently returned scores to the date extractor.
        keys = ", ".join(f'"{k}"' for k in schema.get("properties", {}))
        prompt = (f"{system_prompt}\n\n{user_prompt}\n\n"
                  f"Return ONLY one JSON object with exactly these keys: {keys}. "
                  "No other text, no markdown fence.")
        result = subprocess.run(
            [*self._command, prompt],
            capture_output=True, text=True, timeout=self._timeout,
            # codex reads stdin regardless of the argv prompt; DEVNULL sends EOF
            # at once. Without it an inherited open-but-empty stdin (debugger,
            # task runner) blocks forever.
            stdin=subprocess.DEVNULL,
            # The CLI emits UTF-8. Without this, text=True decodes via the OS
            # locale (cp950 on zh-TW Windows) and the reader thread dies on
            # bytes like 0xe2.
            encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            # codex prefixes stderr with a ~200-char startup banner, so the head
            # never contains the actual error.
            raise RuntimeError(f"{' '.join(self._command)} exited "
                               f"{result.returncode}: {result.stderr[-300:]}")
        return result.stdout


class CachingScorer:
    """Wraps any scorer with a persistent cache keyed on event_uid.

    An LLM call per event per run is the dominant cost, and an event's text does
    not change between runs, so re-scoring the same 70 events hourly would be
    pure waste. Cache misses fall through to the wrapped scorer.
    """

    cacheable: ClassVar[bool] = False   # already cached; never wrap twice

    def __init__(self, inner: EventScorer, store: ScoreCache, model_tag: str):
        self._inner = inner
        self._store = store
        self._tag = model_tag

    @property
    def method_label(self) -> str:
        return self._inner.method_label

    def score(self, event: Event) -> Score:
        cached = self._store.get_cached_score(event.event_uid, self._tag)
        if cached:
            return cached
        score = self._inner.score(event)
        self._store.put_cached_score(event.event_uid, self._tag, score)
        return score


def _require_preferences(settings) -> ScoringPreferences:
    if settings.preferences is None:
        raise ValueError("Settings.preferences is unset. Build Settings with "
                         "load_settings, or pass config.load_preferences() by hand")
    return settings.preferences


def _configured_tier(settings, prompt: ScoringPrompt) -> "_LlmEventScorer | None":
    """The one place that decides which model tier is available.

    Both build_scorer and build_invoker read it. They each used to carry their
    own copy of this if/elif, so a change to the fallback rules could leave the
    scorer and the extractor silently talking to different tiers -- the same
    two-copies-drift-apart failure this codebase has already hit twice.
    """
    if settings.openai_api_key:
        return OpenAiScorer(settings.openai_api_key, settings.model, prompt,
                            settings.max_description_chars, settings.reasoning_effort)
    for tool in settings.llm_clis:
        # which() both tests for the install and returns the full path used as
        # argv[0]. CreateProcess appends only ".exe" to a bare name, so a CLI
        # stored under any other extension is launchable solely by that path.
        executable = shutil.which(tool.cmd)
        if not executable:
            log.info("scoring CLI %r not installed; trying the next llm_clis entry", tool.cmd)
            continue
        return CliScorer([executable, *tool.args], prompt,
                         settings.max_description_chars, name=tool.name)
    return None


def build_invoker(settings) -> JsonInvoker | None:
    """The configured tier as a JsonInvoker, or None for keyword-only.

    Returns the object, not a bound method, so the dependency is a declared
    Protocol rather than an untyped callable.
    """
    # Never sent. The extractor passes its own prompts to json_call, and this
    # one exists only because the tier constructors take one.
    prompt = ScoringPrompt.build(_require_preferences(settings), settings.profile_text)
    return _configured_tier(settings, prompt)


def build_scorer(settings, store: ScoreCache | None = None) -> EventScorer:
    """Pick the best available tier, in the precedence this module documents.

    Whether to wrap in a cache is read from the tier's own `cacheable` flag
    rather than decided by an early return here, so adding a fifth tier does not
    mean re-deriving that policy from the shape of this if/elif chain.
    """
    preferences = _require_preferences(settings)
    prompt = ScoringPrompt.build(preferences, settings.profile_text)
    llm = _configured_tier(settings, prompt)
    tier: EventScorer
    if llm is None:
        log.info("scorer: keyword-only fallback (no API key or scoring CLI found)")
        tier, tag = KeywordScorer(preferences), ""
    else:
        log.info("scorer: %s tier", llm.method_label)
        # The prompt fingerprint covers the profile, so the tag needs no profile term.
        tier, tag = llm, f"{llm.cache_scope}:p{PROMPT_VERSION}:prompt-{prompt.fingerprint}"
    return CachingScorer(tier, store, tag) if store is not None and tier.cacheable else tier


# ---------------------------------------------------------------------------
# Urgency
# ---------------------------------------------------------------------------
class DeadlineUrgency:
    """Maps a scored event onto a delivery channel.

    P0 is deliberately narrow: it is the only tier meant to interrupt, so
    widening it spends the system's credibility.
    """

    def __init__(self, urgent_hours: int = 72, p0_min_rank: int = 40,
                 p1_min_rank: int = 9, unknown_time_max: Urgency = Urgency.P1):
        self._urgent_hours = urgent_hours
        self._p0_min_rank = p0_min_rank
        self._p1_min_rank = p1_min_rank
        # Ceiling for an event whose start could not be determined. Escalating
        # unknowns is the right instinct, but every extraction failure then
        # becomes an interruption, so they are capped rather than promoted.
        self._unknown_time_max = unknown_time_max

    def classify(self, event: Event, score: Score, now: datetime | None = None) -> Urgency:
        now = now or datetime.now(timezone.utc)
        hours = self._hours_until(event, now)
        # An unknown time still counts as closing, so it is never buried in P2;
        # it is only barred from P0 by the cap.
        closing = hours is None or hours <= self._urgent_hours
        if closing and score.rank >= self._p0_min_rank:
            return Urgency.P0 if hours is not None else self._unknown_time_max
        if score.rank >= self._p1_min_rank:
            return Urgency.P1
        return Urgency.P2

    @staticmethod
    def _hours_until(event: Event, now: datetime) -> float | None:
        stamp = event.rsvp_deadline or event.start
        # parse_iso REPORTS failure, it does not raise, and it always fills in a
        # timezone. Keeping the old try/except here left the None case falling
        # through to when.tzinfo, and classify() is called without a guard from
        # both the scoring loop and the resweep, so that AttributeError took the
        # whole run down.
        when = parse_iso(stamp) if stamp else None
        # Once under way, an event with no deadline closes at its end. Measured
        # to its start, a listing kept in scope for an end a month or more away
        # would read as closing now, and a rank of 40 would put it in P0.
        # due_for_resweep still uses the start (Known_concern #3).
        if when is not None and not event.rsvp_deadline and when < now and event.end:
            when = parse_iso(event.end) or when
        return None if when is None else (when - now).total_seconds() / 3600
