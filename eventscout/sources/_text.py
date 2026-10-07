"""Text helpers shared by the scraping sources.

Extracted after two implementations silently diverged, one calling
html.unescape and the other not, so Event.title arrived raw from one source
and clean from the other. 23 of 100 alumni titles carry an entity (e.g.
"Welcome to Your City 2026 &#8211; San Francisco"), so this is routine input,
not an edge case.

JsonLdSource deliberately skips this: running schema.org field values through
an HTML stripper would corrupt them. It still decodes entities, in
_jsonld.flatten_text, because some sites leave them in the markup.
"""
from __future__ import annotations

import html
import re

_TAG = re.compile(r"<[^>]+>")


def strip_html(value: str) -> str:
    """Flatten a scraped HTML fragment into clean single-line display text."""
    return re.sub(r"\s+", " ", html.unescape(_TAG.sub(" ", value))).strip()
