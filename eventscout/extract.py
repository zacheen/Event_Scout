"""LLM extractor for events whose date lives in prose rather than a field.

In practice only the newsletter and mailbox tiers need this, but the gate is a
MISSING FIELD, not a source kind: JSON-LD and WordPress carry typed values, so
they simply never qualify, and running a model over them would replace a fact
with a guess.

Date extraction is the single largest correctness risk in the pipeline: an event
read as next Thursday when it is this Thursday is a miss dressed as a hit. Three
rules follow from that.

  A refusal is preferred to a guess. The prompt says to return an empty string
  when the page does not state a date, and an empty result leaves the event
  undated rather than inventing a plausible one.
  Every value is re-parsed here before it is accepted. A model that returns
  "2026-13-45" or prose must not reach the ledger.
  Failure returns the SAME object, per the Extractor identity contract
  (protocols.py).
"""
from __future__ import annotations

import logging
import re
from dataclasses import replace
from datetime import datetime, timezone
from urllib.parse import urljoin

from .http import HttpClient
from .protocols import JsonInvoker
from .models import Event, iso_or_empty
from ._llm_json import lenient_json
from .sources._text import strip_html
from .sources._jsonld import event_nodes, flatten_text
from .urls import canon_url

log = logging.getLogger("eventscout.extract")

_SYSTEM = (
    "Extract event facts from the page text. Return ONLY a JSON object:\n"
    '{"title": "<the event\'s own name, or empty>", '
    '"start": "<ISO 8601 with UTC offset, or empty>", '
    '"end": "<ISO 8601 with UTC offset, or empty>", '
    '"location": "<city and state, or Virtual, or empty>", '
    '"rsvp_deadline": "<ISO 8601 with UTC offset, or empty>", '
    '"organizer": "<hosting company or school, or empty>"}\n\n'
    "Rules. Return an empty string for anything the page does not state; never "
    "infer or estimate a date. If a time is given without a zone, assume "
    "America/Los_Angeles and write the offset explicitly. Today is "
)
_MAX_CHARS = 6000
# What a start time could have been READ FROM. A month name has to sit next to
# a day number, and a numeric date has to use a real month and day: measured on
# the 81 pages behind one run's thin events, a bare month alternation matched
# "may" the modal verb on five of them and "00-8" out of an id on another, and
# a bare year matched "Q4 2026" in a market-trends sentence and "Builders Cup
# 2026" in an event name. None of those is a date.
_MONTHS = (r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?"
           r"|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?"
           r"|nov(?:ember)?|dec(?:ember)?")
_DATE_EVIDENCE = re.compile(
    rf"\b(?:{_MONTHS})\.?\s+\d{{1,2}}(?:st|nd|rd|th)?\b"
    rf"|\b\d{{1,2}}(?:st|nd|rd|th)?\s+(?:{_MONTHS})\b"
    r"|\b\d{4}-\d{2}-\d{2}\b"
    r"|\b(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\d|3[01])(?:[/-]\d{2,4})?(?![\d/-])",
    re.I)

_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "start": {"type": "string"}, "end": {"type": "string"},
        "location": {"type": "string"}, "rsvp_deadline": {"type": "string"},
        "organizer": {"type": "string"},
    },
    "required": ["title", "start", "end", "location", "rsvp_deadline", "organizer"],
    "additionalProperties": False,
}


# Tiers whose titles come from link text rather than from the event itself, so
# the extractor may replace them. A newsletter issue linking 20 events yields 20
# items sharing one subject line, which names none of them: that defeats the
# keyword gate and makes the digest unreadable.
_PROSE_KINDS = frozenset({"newsletter", "gmail_label"})


class PageFactExtractor:
    """Fills start/end/location for undated events by reading their own page.

    Two paths, in this order. The page's schema.org Event markup is exact and
    free, and on one measured run answered 61 of 85 events outright; only the
    remainder reach the model, which reads prose and can be wrong. Named for
    the job rather than the tool because most of the work never calls a model.

    Takes a JsonInvoker, so it reuses whichever tier the run configured instead
    of owning a second model client, and the dependency is a declared Protocol
    rather than a bare callable.

    The invoker is OPTIONAL, and None is the keyword-only run rather than a
    broken one. build_runtime used to return no extractor at all in that case,
    which disabled the free schema.org path along with the model it does not
    use -- on a GitHub runner, where the codex CLI does not exist, that is every
    run made without OPENAI_API_KEY. With None the typed path still runs and
    only the prose path is skipped.
    """

    def __init__(self, invoker: JsonInvoker | None, http: HttpClient):
        self._invoker = invoker
        self._http = http

    @property
    def prose_enabled(self) -> bool:
        """Whether the model half runs. The typed half always does.

        Exists so an entry point can say which of the two paths is live. The
        line it feeds used to read "extraction off" for the whole extractor,
        which is now wrong in the only case it ever printed.
        """
        return self._invoker is not None

    @staticmethod
    def needs_extraction(event: Event) -> bool:
        """Either gap disqualifies the event from being judged.

        No start and the date window cannot place it; no location and the geo
        filter drops it outright.
        """
        return not event.start or not event.location

    def extract(self, event: Event) -> Event:
        if not self.needs_extraction(event):
            return event
        html = self._page_html(event)
        if html is None:
            # Say so rather than returning the event untouched. Unread is not
            # the same as "has no location", and the geo filter drops the
            # second: measured, all five tesla.com/event/*-resume links answer
            # 403 and were dropped there before this flag existed.
            return replace(event, page_unreadable=True)
        # Typed markup first: asking a model to re-derive a startDate the page
        # already states exactly only risks inventing one (see the class
        # docstring for the measured split).
        typed = self._facts_from_jsonld(html, event)
        if typed:
            # Overwrites rather than only filling gaps, deliberately: a
            # WordPress event's start is the POST date when the body states no
            # other, so the event's own page correcting it is the point. This
            # is safe even though a matched node and a lone unnamed one carry
            # different evidence strength, because _facts_from_jsonld's own
            # matching rule already filters out ambiguous candidates before
            # either reaches here.
            return replace(event, **typed)
        # Everything above this line is free and needs no model. Returning the
        # event here rather than earlier is what keeps page_unreadable and the
        # typed path working on a keyword-only run.
        if self._invoker is None:
            return event
        text = self._page_text(html)
        if not text:
            return event
        today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
        try:
            raw = self._invoker.json_call(
                _SYSTEM + today,
                f"url: {event.url}\ntitle: {event.title}\n\n{text}",
                _EXTRACT_SCHEMA, "event_facts")
        except Exception as exc:
            # Fail open. One unreadable page must not abort a run, and an
            # undated event is still worth reporting.
            log.warning("extraction failed for %s: %s", event.url, exc)
            return event
        patch = self._parse(raw, text)
        title = self._prose_title(patch.pop("title", ""), event.source_kind)
        if title:
            patch["title"] = title
        # Identity, not truthiness: the pipeline skips re-filtering when an
        # extractor returns the object it was given.
        return replace(event, **patch) if patch else event

    def _page_html(self, event: Event) -> str | None:
        """The event's own page, or None when the site would not give it to us.

        None rather than "": the caller has to tell "we were refused" apart
        from "the page said nothing", because only the first one means we know
        nothing about the event rather than knowing it states nothing.
        """
        try:
            return self._http.get_text(event.url)
        except Exception as exc:
            log.warning("no page for %s, skipping extraction: %s", event.url, exc)
            return None

    @staticmethod
    def _facts_from_jsonld(html: str, event: Event) -> dict:
        """Typed fields from the page's own Event markup, or {} if it has none.

        Picking the node is the whole problem. A Luma event page also embeds the
        host's OTHER events, so the first node found is not necessarily this
        one: prefer a node whose url resolves to this event's, and fall back to
        an unnamed node ONLY when the page has exactly one. Two unnamed nodes
        give no way to tell which is this event, and guessing there would stamp
        a neighbour's date onto it. Identity is decided before the date (see
        the loop below for why that ordering matters).

        The date goes through the same _valid_iso gate as the model's answer.
        Markup is typed, not trustworthy: it comes from whatever page a
        newsletter linked to, and a CMS template left with its example values
        is exactly as wrong as a hallucination.
        """
        matched, unnamed = [], []
        for candidate in event_nodes(html):
            raw = str(candidate.get("url") or candidate.get("@id") or "").strip()
            if not raw:
                unnamed.append(candidate)
            elif canon_url(urljoin(event.url, raw)) == event.url:
                matched.append(candidate)
        # Identity first, THEN the date. Filtering on the date up front made a
        # node that is this event but states no usable one disappear entirely,
        # and a single unrelated node then became "the only candidate" and had
        # its start, location and organizer stamped on. A page that names this
        # event with no usable date must fall through to the prose path, not
        # borrow a neighbour's.
        node, start = None, ""
        for candidate in matched or (unnamed if len(unnamed) == 1 else []):
            start = PageFactExtractor._valid_iso(str(candidate.get("startDate") or ""))
            if start:
                node = candidate
                break
        if node is None:
            return {}
        facts = {"start": start}
        end = PageFactExtractor._valid_iso(str(node.get("endDate") or ""))
        if end:
            facts["end"] = end
        for field in ("location", "organizer"):
            value = flatten_text(node.get(field))
            if value:
                facts[field] = value[:200]
        title = PageFactExtractor._prose_title(node.get("name"), event.source_kind)
        if title:
            facts["title"] = title
        return facts

    @staticmethod
    def _prose_title(raw: object, source_kind: str) -> str:
        """The page's own name, but only where the harvested title never named
        the event to begin with. Keyed on source_kind rather than on matching
        known subject lines: a per-publication string test rots the moment a new
        newsletter is added."""
        title = str(raw or "").strip()
        return title[:200] if title and source_kind in _PROSE_KINDS else ""

    @staticmethod
    def _page_text(html: str) -> str:
        """Readable prose from a page already fetched.

        Only reached when the page carries no usable Event markup. Falling back
        to event.description here instead was measured producing invented
        facts: tesla.com answers 403, the newsletter tier's description is the
        placeholder "Linked from <issue url>", and the model, given that plus
        the event URL, returned a confident start time whose UTC offset moved
        between -04:00 and -05:00 across three identical runs.
        """
        body = re.sub(r"(?is)<(script|style|nav|footer).*?</\1>", " ", html)
        return strip_html(body)[:_MAX_CHARS]

    @staticmethod
    def _states_a_date(text: str) -> bool:
        """Whether `text` contains anything a date could have been read FROM.

        Deliberately coarse. It is not checking that the model quoted the page
        correctly, only that the page said something date-shaped at all: a page
        naming no year and no month cannot support any timestamp, so one coming
        back from the model was invented rather than read.
        """
        return bool(_DATE_EVIDENCE.search(text))

    def _parse(self, raw: str, source_text: str) -> dict:
        data = lenient_json(raw)
        patch = {}
        if "title" in data:
            patch["title"] = str(data.get("title") or "").strip()
        dated = self._states_a_date(source_text)
        for field in ("start", "end", "rsvp_deadline"):
            stamp = self._valid_iso(str(data.get(field) or ""))
            if stamp and not dated:
                log.warning("dropping %s=%s: the page states no date to read it from",
                            field, stamp)
                continue
            if stamp:
                patch[field] = stamp
        for field in ("location", "organizer"):
            value = str(data.get(field) or "").strip()
            if value:
                patch[field] = value[:200]
        return patch

    @staticmethod
    def _valid_iso(value: str) -> str:
        """Accept a timestamp only if it round-trips through fromisoformat.

        The model is asked for ISO 8601, but asking is not enforcing: a plain
        string check would let "next Thursday" or "2026-13-45" into the ledger,
        where it would sort as garbage and could route an event to the wrong
        urgency tier.
        """
        text = value.strip()
        stamp = iso_or_empty(text)
        if not stamp:
            if text:
                log.info("discarded unparseable extracted timestamp: %r", text[:40])
            return ""
        # Anything more than a decade out is a hallucinated century or a typo,
        # not a real event listing. This bound is the extractor's alone; the
        # shared gate only decides whether the value is a timestamp at all.
        when = datetime.fromisoformat(stamp)
        years = abs((when.replace(tzinfo=when.tzinfo or timezone.utc)
                     - datetime.now(timezone.utc)).days) / 365
        if years > 10:
            log.info("discarded implausible extracted timestamp: %r", text[:40])
            return ""
        return stamp
