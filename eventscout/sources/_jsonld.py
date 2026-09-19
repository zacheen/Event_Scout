"""schema.org JSON-LD parsing, shared by the source and the extractor.

Extracted rather than duplicated for the same reason as _text.strip_html
and _links.harvest: the extractor reads this markup on an INDIVIDUAL
event's page, and a second parser for one format is how the two drift.
"""
from __future__ import annotations

import json
import re
from typing import Iterator

_LD_BLOCK = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I
)
# schema.org subtypes we treat as events. `@type` is often a list, e.g.
# ["Event", "Hackathon"] on Cerebral Valley, so membership is tested per element.
_EVENT_TYPES = frozenset({
    "Event", "Hackathon", "BusinessEvent", "EducationEvent", "SocialEvent",
    "NetworkingEvent", "CourseInstance", "ExhibitionEvent", "Festival",
})
def event_nodes(html: str) -> Iterator[dict]:
    """Yield every Event-typed dict in a page's JSON-LD, at any depth.

    Sites nest differently: Luma and Cerebral Valley wrap events in an ItemList
    of ListItem/item, while others emit @graph or a bare array. A recursive walk
    covers all of them without a per-site branch.
    """
    def walk(node):
        if isinstance(node, dict):
            raw = node.get("@type")
            types = raw if isinstance(raw, list) else [raw]
            if any(str(t) in _EVENT_TYPES for t in types):
                yield node
            for value in node.values():
                yield from walk(value)
        elif isinstance(node, list):
            for value in node:
                yield from walk(value)

    for block in _LD_BLOCK.findall(html):
        try:
            yield from walk(json.loads(block.strip()))
        except (json.JSONDecodeError, ValueError):
            # One malformed block must not discard the valid ones alongside it;
            # sites routinely ship a broken WebSite node next to good data.
            continue


def flatten_text(value) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return flatten_text(value.get("name") or value.get("address") or "")
    if isinstance(value, list):
        return ", ".join(p for p in (flatten_text(v) for v in value) if p)
    return ""

