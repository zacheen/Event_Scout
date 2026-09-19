"""URL canonicalisation for cross-source dedupe."""
from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Campaign and click-tracking params. A measured campaign link carried three of
# these at once, so the same event shared by newsletter, email and calendar
# produces three distinct raw URLs. Without stripping them, dedupe fails on
# every item that arrives through more than one channel.
_TRACKING_PREFIXES = ("utm_", "pk_", "mtm_", "hsa_", "vero_")
_TRACKING_EXACT = frozenset({
    "fbclid", "gclid", "gbraid", "wbraid", "msclkid", "twclid", "igshid",
    "mc_cid", "mc_eid", "ref", "referrer", "source", "_hsenc", "_hsmi",
    "yclid", "dclid", "si", "trk", "trkcampaign", "mkt_tok",
})
_RECIPIENT_PARAMS = {"luma.com": frozenset({"tk", "pk"}),
                     "zoom.us": frozenset({"user_id"})}


def _is_tracking(key: str, host: str) -> bool:
    lowered = key.lower()
    return (lowered in _TRACKING_EXACT or lowered.startswith(_TRACKING_PREFIXES)
            or any((host == domain or host.endswith("." + domain)) and lowered in keys
                   for domain, keys in _RECIPIENT_PARAMS.items()))


def canon_url(url: str) -> str:
    """Canonical form used as the dedupe key.

    Lowercases scheme and host, drops the fragment, removes tracking params, and
    sorts what remains so param order cannot split one event into two. Keeps a
    non-tracking query intact, because some event pages address the event itself
    that way (e.g. ?event=123) and stripping it would merge distinct events.
    """
    if not url:
        return ""
    parts = urlsplit(url.strip())
    host = parts.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    # lu.ma 301s to luma.com; folding it here keeps a bookmarked short link and
    # a redirected one from being recorded as two different events.
    if host == "lu.ma":
        host = "luma.com"
    kept = sorted((k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
                  if not _is_tracking(k, host))
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower() or "https", host, path, urlencode(kept), ""))
