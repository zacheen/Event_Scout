"""Reading JSON back out of a model reply.

Extracted rather than duplicated, for the reason _configured_tier's docstring
already records twice: two copies of one rule drift, and the next edit to this
regex would only ever reach one of them.
"""
from __future__ import annotations

import json
import re


def lenient_json(raw: str) -> dict:
    """The first JSON object in a reply, or {} when there is none.

    Strict parsing first, then a brace scan, because only the API tier can be
    held to a schema; the CLI tier answers in whatever shape it feels like and
    routinely wraps the object in a fence or a sentence.

    Returns {} rather than raising: what an empty result MEANS differs by
    caller, and each one already says so. The scorer treats it as a failure to
    judge, the extractor as a page that stated nothing.
    """
    text = (raw or "").strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.S)
        try:
            data = json.loads(match.group(0)) if match else {}
        except json.JSONDecodeError:
            data = {}
    return data if isinstance(data, dict) else {}
