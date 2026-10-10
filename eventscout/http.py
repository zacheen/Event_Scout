"""Shared HTTP transport, injected into every source.

Centralised because per-adapter urlopen calls
drift apart on timeout, user agent and politeness, and make a source
impossible to unit test without real network access or a monkeypatched global.
"""
from __future__ import annotations

import gzip
import logging
import random
import threading
import time
import urllib.request
from collections import deque
from typing import Callable, Protocol
from urllib.parse import urlsplit

log = logging.getLogger(__name__)

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)


def _decode_body(raw: bytes, encoding: str) -> str:
    """Decode a response body, honouring Content-Encoding.

    Servers compress whether or not we ask. get_text sends no Accept-Encoding,
    yet gotowebinar.com answers `Content-Encoding: gzip` regardless (measured
    2026-09-10: 604 bytes of gzip that decoded to 570 characters of mojibake
    carrying 9 NUL bytes, versus 1404 characters of real HTML once inflated).
    Decoding those bytes as text does not raise; it yields garbage that parses
    to zero events, which is the failure mode HttpClient.get_text exists to
    prevent. A NUL also cannot survive an argv, so it crashed the CLI scorer's
    subprocess with "embedded null character" rather than scoring the page.

    An encoding we cannot inflate RAISES, because guessing is what produced the
    silence above; the source then reports an error the funnel can show.
    Brotli is not handled because no decoder ships with this environment.
    """
    name = (encoding or "identity").strip().lower()
    if name == "gzip":
        raw = gzip.decompress(raw)
    elif name != "identity":
        raise ValueError(f"cannot decode Content-Encoding {name!r}")
    return raw.decode("utf-8", "replace")


class HttpClient(Protocol):
    def get_text(self, url: str) -> str:
        """Fetch and decode a page.

        MUST raise on failure rather than returning "". A source that swallows
        an error reports zero events, which the pipeline cannot distinguish from
        a genuinely quiet week, so a broken adapter would stay silent for weeks.
        """
        ...


class HostRateLimiter:
    """At most `max_requests` per host inside any `window_seconds`, shared by
    every thread holding the client.

    A count over a sliding window rather than a gap between requests, because
    that is what Luma enforces. Measured against luma.com on 2026-10-09: 80
    requests in 6 s from a quiet start passed, about 103 inside 6 minutes
    passed, and about 163 inside 2.2 minutes earned HTTP 429 for the whole
    host, calendar pages included, lifting 4 to 8 minutes later with no
    Retry-After. A per-request sleep long enough to hold that line would add
    minutes to every run, while a count costs nothing until the cap is reached.
    It only sees this process, so two runs overlapping still add up.
    """

    def __init__(self, max_requests: int, window_seconds: float,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        # Zero would never grant a slot and then index an empty window.
        if max_requests < 1 or window_seconds <= 0:
            raise ValueError(f"need max_requests >= 1 and window_seconds > 0, "
                             f"got {max_requests} and {window_seconds}")
        self._max = max_requests
        self._window = window_seconds
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._sent: dict[str, deque[float]] = {}

    def acquire(self, host: str) -> float:
        """Block until `host` has a free slot, take it, and return the seconds waited."""
        waited = 0.0
        while True:
            with self._lock:
                now = self._clock()
                sent = self._sent.setdefault(host, deque())
                while sent and now - sent[0] >= self._window:
                    sent.popleft()
                if len(sent) < self._max:
                    sent.append(now)
                    return waited
                wait = self._window - (now - sent[0])
            # Outside the lock, so one host's wait does not hold up the others.
            # Logged before waiting, because a silent pause of minutes reads
            # as a hung run.
            log.warning("waiting %.0fs for a request slot on %s", wait, host)
            self._sleep(wait)
            waited += wait


class UrllibHttpClient:
    def __init__(
        self,
        timeout: int = 30,
        user_agent: str = _DEFAULT_UA,
        min_delay: float = 0.0,
        max_delay: float = 0.0,
        limiter: HostRateLimiter | None = None,
    ):
        self._timeout = timeout
        self._ua = user_agent
        self._min_delay = min_delay
        self._max_delay = max(min_delay, max_delay)
        self._limiter = limiter

    def get_text(self, url: str) -> str:
        self._sleep()
        if self._limiter is not None:
            self._limiter.acquire(urlsplit(url).hostname or "")
        request = urllib.request.Request(
            url, headers={"User-Agent": self._ua, "Accept-Language": "en-US,en;q=0.9"}
        )
        with urllib.request.urlopen(request, timeout=self._timeout) as response:
            return _decode_body(
                response.read(), response.headers.get("Content-Encoding", ""))

    def _sleep(self) -> None:
        # Randomised, not fixed: a constant interval between requests is itself a
        # bot signature. Zero delay by default so tests stay fast.
        if self._max_delay > 0:
            time.sleep(random.uniform(self._min_delay, self._max_delay))


class StubHttpClient:
    """Serves canned pages by URL, so source parsing is testable offline."""

    def __init__(self, pages: dict[str, str]):
        self._pages = pages

    def get_text(self, url: str) -> str:
        if url not in self._pages:
            raise KeyError(f"StubHttpClient has no page for {url!r}")
        return self._pages[url]
