"""Event-link harvesting, shared by the newsletter and mailbox sources.

Both answer the same question: given a page or message full of links, which ones
point at an event rather than at an article, an image or an unsubscribe footer.
Extracted rather than duplicated after `_text.strip_html` showed how quickly two
copies of the same rule drift apart.
"""
from __future__ import annotations

import re
from urllib.parse import urlsplit

from ._text import strip_html

_ANCHOR = re.compile(
    r'<a\b[^>]*href="(?P<href>https?://[^"]+)"[^>]*>(?P<text>.*?)</a>', re.S | re.I)
# Bare URLs, for the plain-text half of a multipart email where there is no <a>.
_BARE_URL = re.compile(r'(?<![">])\bhttps?://[^\s<>()\[\]"\']{6,}')

# Click-tracker redirects, dropped rather than resolved. The tail is per-send,
# so canon_url cannot collapse two mails advertising one event, and the
# destination is unknowable without following the redirect. In a labelled
# sample, every link was one of these, so nothing is
# lost: the same events arrive by a direct URL elsewhere. A source found ONLY
# behind a tracker would need a HEAD request here.
_TRACKER_HOSTS = (
    "email.g.joinhandshake.com/", "email.notifications.joinhandshake.com/",
    "click.", "/c/eJ", "links.", "email.mg.", "sendgrid.net/", "mandrillapp.com/",
    "e.customeriomail.com/", "tracking.", "url" + "defense.",
)

# Every list here is private: nothing outside this module imports one, and
# exposing them would be an extension point with no consumer. What one reader
# adds goes in channels.yaml's event_link_hints instead (see parse_link_hints).
# Hosts whose links are plumbing, never the event being advertised.
_IGNORED_HOSTS = (
    "beehiiv.com", "unsub.beehiiv.com", "media.beehiiv.com", "twitter.com",
    "x.com", "linkedin.com", "facebook.com", "instagram.com", "youtube.com",
    "apple.com/apple-news", "mailto", "unsubscribe", "list-manage.com",
    "googleusercontent.com", "/privacy", "/terms",
)
# Path shapes that mark a link as a registration page rather than an article,
# on platforms any reader meets. Kept narrow on purpose: one newsletter issue
# carries dozens of links, and a loose pattern turns a single issue into
# dozens of junk candidates.
_EVENT_HINTS = (
    "/event/", "/events/", "luma.com/", "lu.ma/", "eventbrite.", "meetup.com/",
    "/register", "/rsvp", "hopin.", "airmeet.", "/webinar", "splashthat.com",
)

# Static assets. An email footer's icon set matched an event hint purely because
# the host was right, so five Handshake logo PNGs were harvested as events and
# sent to the extractor, which then tried to UTF-8 decode a PNG. Checked against
# the path, not the Content-Type, because rejecting these must not cost a fetch.
_ASSET_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
                   ".css", ".js", ".pdf", ".zip", ".mp4", ".woff", ".woff2")
_ASSET_PATHS = ("/static_assets/", "/assets/", "/images/", "/img/", "/icons/",
                "/logos/", "/cdn-cgi/")

# Luma pages that are app, account or meeting plumbing, not an event. The bare
# "luma.com/" hint above accepts every one of them, and one full fetch sent
# about 20 such links from Luma mail to the extractor. Opened anonymously on
# 2026-10-09, /android lands on Google Play and /join/g-... on a Microsoft Teams
# launcher. /e/ticket/ is deliberately absent, because a ticket link redirects
# to its public event page.
_LUMA_HOSTS = frozenset({"luma.com", "www.luma.com", "lu.ma"})
# Matched as the whole first path segment, so a slug that merely starts with
# one, such as luma.com/iosdevs, is still an event.
_LUMA_PLUMBING_SEGMENTS = frozenset({"android", "ios", "settings", "discover", "join"})
_LUMA_PLUMBING_PREFIXES = ("/event/manage/",)

_TITLE_MAX = 200


def _is_luma_plumbing(lowered: str) -> bool:
    parts = urlsplit(lowered)
    if parts.hostname not in _LUMA_HOSTS:
        return False
    first = parts.path.strip("/").split("/", 1)[0]
    # An empty first segment is Luma's home page, which mail has delivered as
    # an event link.
    return (not first or first in _LUMA_PLUMBING_SEGMENTS
            or parts.path.startswith(_LUMA_PLUMBING_PREFIXES))


def parse_link_hints(hints, owner: str) -> tuple[str, ...]:
    """One source's extra event hints, stripped and lowered, () for None.

    Called by each harvesting source's constructor, as EightfoldSource checks
    its own phrases, so a source built by hand gets the same refusals as one
    built from channels.yaml's event_link_hints. A bare string iterates into
    one-letter hints and a blank one is a substring of every URL, so either
    would turn every link in every issue into a candidate event. Neither
    raises on its own.
    """
    if hints is None:
        return ()
    if isinstance(hints, str):
        raise TypeError(f"{owner}: link_hints must be a list of URL fragments, not one string")
    if not all(isinstance(h, str) and h.strip() for h in hints):
        raise ValueError(f"{owner}: link_hints holds a blank or non-string entry")
    return tuple(h.strip().lower() for h in hints)


def is_event_link(href: str, hints: tuple[str, ...] = ()) -> bool:
    lowered = href.lower().split("?", 1)[0]
    if lowered.endswith(_ASSET_SUFFIXES) or any(p in lowered for p in _ASSET_PATHS):
        return False
    if any(t in lowered for t in _TRACKER_HOSTS):
        return False
    if any(host in lowered for host in _IGNORED_HOSTS):
        return False
    if _is_luma_plumbing(lowered):
        return False
    return any(hint in lowered for hint in _EVENT_HINTS + hints)


def tails(body: str, chars: int = 140, hints: tuple[str, ...] = ()) -> dict[str, str]:
    """Stripped text FOLLOWING each event link, by url.

    Anchors only. A bare URL in a plain-text part has no markup to end it, so
    where its text stops is a guess, and guessing is what this returns "" for.

    Keyed by raw href to match `harvest` before canonicalisation, so a caller
    pairing the two must canonicalise both sides or neither.

    First occurrence wins, matching `harvest`'s setdefault. A newsletter that
    links one event twice states the date beside the first mention and uses a
    bare "sign up" for the second.
    """
    out: dict[str, str] = {}
    for match in _ANCHOR.finditer(body):
        href = match.group("href")
        if is_event_link(href, hints) and href not in out:
            out[href] = " ".join(strip_html(body[match.end():match.end() + chars]).split())
    return out


def harvest(body: str, fallback_title: str = "",
            hints: tuple[str, ...] = ()) -> list[tuple[str, str]]:
    """Return (url, title) for every event-looking link in an HTML or text body.

    Anchors first, then bare URLs for the plain-text alternative part. Titles
    fall back through anchor text, then the caller's subject line, then the last
    path segment, because an image or button link carries no readable text and
    the extractor still needs something a human can recognise.
    """
    found: dict[str, str] = {}
    for match in _ANCHOR.finditer(body):
        href = match.group("href")
        if is_event_link(href, hints):
            found.setdefault(href, strip_html(match.group("text")))
    for match in _BARE_URL.finditer(body):
        href = match.group(0).rstrip(".,);")
        if is_event_link(href, hints):
            found.setdefault(href, "")
    out = []
    multi = len(found) > 1
    for href, title in found.items():
        if len(title) < 4:
            # With more than one link in the body the subject is the SAME for
            # all of them, so it identifies nothing; the URL slug at least
            # differs per event. With a single link the subject is the better
            # description of the two.
            slug = href.rstrip("/").rsplit("/", 1)[-1].replace("-", " ")
            title = (slug or fallback_title) if multi else (fallback_title or slug)
        out.append((href, title.strip()[:_TITLE_MAX] or href))
    return out
