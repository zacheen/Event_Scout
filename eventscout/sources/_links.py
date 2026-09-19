"""Event-link harvesting, shared by the newsletter and mailbox sources.

Both answer the same question: given a page or message full of links, which ones
point at an event rather than at an article, an image or an unsubscribe footer.
Extracted rather than duplicated after `_text.strip_html` showed how quickly two
copies of the same rule drift apart.
"""
from __future__ import annotations

import re

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
# exposing them would be an extension point with no consumer. Making these
# configurable later means moving the DATA to channels.yaml, which does not
# depend on the Python names being public.
# Hosts whose links are plumbing, never the event being advertised.
_IGNORED_HOSTS = (
    "beehiiv.com", "unsub.beehiiv.com", "media.beehiiv.com", "twitter.com",
    "x.com", "linkedin.com", "facebook.com", "instagram.com", "youtube.com",
    "apple.com/apple-news", "mailto", "unsubscribe", "list-manage.com",
    "googleusercontent.com", "/privacy", "/terms",
)
# Path shapes that mark a link as a registration page rather than an article.
# Kept narrow on purpose: one newsletter issue carries dozens of links, and a
# loose pattern turns a single issue into dozens of junk candidates.
_EVENT_HINTS = (
    "/event/", "/events/", "luma.com/", "lu.ma/", "eventbrite.", "meetup.com/",
    "/register", "/rsvp", "hopin.", "airmeet.", "/webinar", "splashthat.com",
    "/careers/events",
    # Handshake's own event paths only. A bare "joinhandshake.com/" also matched
    # its CDN, which is how footer icons became candidate events.
    "joinhandshake.com/events", "joinhandshake.com/career_fairs",
)

# Static assets. An email footer's icon set matched _EVENT_HINTS purely because
# the host was right, so five Handshake logo PNGs were harvested as events and
# sent to the extractor, which then tried to UTF-8 decode a PNG. Checked against
# the path, not the Content-Type, because rejecting these must not cost a fetch.
_ASSET_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
                   ".css", ".js", ".pdf", ".zip", ".mp4", ".woff", ".woff2")
_ASSET_PATHS = ("/static_assets/", "/assets/", "/images/", "/img/", "/icons/",
                "/logos/", "/cdn-cgi/")

_TITLE_MAX = 200


def is_event_link(href: str) -> bool:
    lowered = href.lower().split("?", 1)[0]
    if lowered.endswith(_ASSET_SUFFIXES) or any(p in lowered for p in _ASSET_PATHS):
        return False
    if any(t in lowered for t in _TRACKER_HOSTS):
        return False
    if any(host in lowered for host in _IGNORED_HOSTS):
        return False
    return any(hint in lowered for hint in _EVENT_HINTS)


def harvest(body: str, fallback_title: str = "") -> list[tuple[str, str]]:
    """Return (url, title) for every event-looking link in an HTML or text body.

    Anchors first, then bare URLs for the plain-text alternative part. Titles
    fall back through anchor text, then the caller's subject line, then the last
    path segment, because an image or button link carries no readable text and
    the extractor still needs something a human can recognise.
    """
    found: dict[str, str] = {}
    for match in _ANCHOR.finditer(body):
        href = match.group("href")
        if is_event_link(href):
            found.setdefault(href, strip_html(match.group("text")))
    for match in _BARE_URL.finditer(body):
        href = match.group(0).rstrip(".,);")
        if is_event_link(href):
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
