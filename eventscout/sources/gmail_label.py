"""Gmail label source, over IMAP.

The tier that covers everything with no public feed: school listservs, Handshake
notifications, employer talent-network mail, and any newsletter whose archive is
private. A Gmail rule decides what lands in the label, so adding a sender needs
no code change here.

Access is an app password over IMAP, NOT OAuth. An OAuth app left in "Testing"
publishing status issues refresh tokens that expire after seven days, and moving
to production needs a verified domain this project does not have. App passwords
do not expire. The tradeoff is that an app password is not scope-limited the way
gmail.readonly was, so read-only is a property of THIS code, not something Google
enforces: only SEARCH and FETCH are issued, the mailbox is selected read-only,
and bodies are fetched with BODY.PEEK so nothing is marked as read.
"""
from __future__ import annotations

import email
# Used below as email.utils.parsedate_to_datetime. `import email` alone
# does NOT provide it; it resolves today only because email.message
# pulls it in as a side effect, and the AttributeError from a changed
# import order would be swallowed by the per-message except and read as
# every FETCH failing.
import email.utils
import imaplib
import logging
import re
from contextlib import contextmanager
from email.header import decode_header, make_header
from typing import ClassVar

from ..models import Coverage, Event, iso_or_empty
from ..urls import canon_url
from ._links import harvest
from ._text import strip_html

log = logging.getLogger("eventscout.gmail")

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993


def _decode_header(value: str) -> str:
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def _sender(message) -> str:
    """The bare address out of From, lowercased, or "" if there is none.

    parseaddr rather than a substring test on the whole header, so a display
    name that happens to contain the address cannot decide the match.
    """
    return email.utils.parseaddr(message.get("From", ""))[1].strip().lower()


class GmailLabelSource:
    kind: ClassVar[str] = "gmail_label"

    def __init__(self, name: str, user: str, app_password: str, label: str,
                 newer_than_days: int = 60, max_messages: int = 500,
                 body_chars: int = 4000, skip_from: str = ""):
        self.name = name
        self._user = user
        # Google prints app passwords in four groups of four; the spaces are
        # cosmetic and IMAP rejects them.
        self._password = (app_password or "").replace(" ", "")
        self._label = label
        self._newer_than = newer_than_days
        # Newest-first cap, and 500 rather than the 40 it started at. Measured
        # against a live label holding 222 messages inside its 60-day window, a
        # cap of 40 dropped 182 and the 40 it kept were all from one day.
        #
        # That is the trap worth remembering: a count cap always beats a time
        # bound, so newer_than_days silently stops deciding anything once the
        # cap bites. This value is a runaway guard rather than the operating
        # bound, which is why coverage() reports whenever it fires. At 350ms
        # per IMAP FETCH, 500 messages is about three minutes, and that is the
        # real reason it is not simply unbounded.
        self._max_messages = max_messages
        self._coverage: Coverage | None = None
        self._fetched = False
        # How much message text each Event carries. Comes from
        # config.max_description_chars so it cannot drift from the amount the
        # scorer will actually read.
        self._body_chars = body_chars
        # Sender to refuse: in practice this project's own digest address.
        # MAIL_TO is one of the accounts read here, so every digest lands back
        # in a watched label, and `harvest` turns each of its ~50 links into an
        # Event carrying the whole digest as description. Measured:
        # 80 of 220 ledger rows had been overwritten that way (merged_with only
        # fills EMPTY fields, so the row still reported its original source).
        #
        # Compared by sender, not subject: the digest subject is built from a
        # run's own counts and changes on its own, while the address matches
        # the setting the notifier sends with.
        self._skip_from = (skip_from or "").strip().lower()
        self._skipped_own = 0

    def fetch(self) -> list[Event]:
        if not (self._user and self._password):
            raise RuntimeError(f"{self.name}: mailbox credentials are not set")
        events: dict[str, Event] = {}
        self._skipped_own = 0
        with self._mailbox() as imap:
            matched = self._search(imap)
            uids = matched[: self._max_messages]
            self._coverage = (None if len(uids) == len(matched) else
                              Coverage(len(uids), "messages", len(matched)))
            failed = 0
            for uid in uids:
                found = self._events_in_message(imap, uid)
                if found is None:
                    failed += 1
                    continue
                for event in found:
                    events.setdefault(event.event_uid, event)
            # One unreadable message is tolerated and logged; EVERY message
            # unreadable is a broken connection, a revoked permission or a
            # server fault, and returning [] for that is exactly the "empty
            # result indistinguishable from a quiet week" that EventSource.fetch
            # forbids. Tolerating the single case and raising on the total one
            # are different judgements, not different degrees of the same one.
            if uids and failed == len(uids):
                raise RuntimeError(
                    f"{self.name}: every FETCH failed ({failed}/{len(uids)} uids)")
            # WARNING, not INFO: nothing in this project configures logging, so
            # the root logger has no handler and logging.lastResort filters
            # INFO out entirely. That would make the skip silent, and a label
            # that is mostly our own mail must not quietly read as a thin week.
            if self._skipped_own:
                log.warning("%s: skipped %d message(s) from this project's own digest",
                            self.name, self._skipped_own)
        self._fetched = True
        return list(events.values())

    def coverage(self) -> Coverage | None:
        if not self._fetched:
            # See BoundedSource.coverage() for why this must raise rather
            # than return None.
            raise RuntimeError(f"{self.name}: coverage() called before fetch()")
        return self._coverage

    # -- IMAP plumbing -----------------------------------------------------
    @contextmanager
    def _mailbox(self):
        imap = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        try:
            imap.login(self._user, self._password)
            typ, data = imap.select(self._all_mail(imap), readonly=True)
            if typ != "OK":
                # A failed SELECT leaves the connection with no mailbox, and the
                # SEARCH that follows would answer "no messages" rather than
                # erroring. That is the exact shape of failure this project
                # exists to prevent.
                raise RuntimeError(
                    f"{self.name}: could not select the mailbox ({typ}): {data}")
            yield imap
        finally:
            for close in (imap.close, imap.logout):
                try:
                    close()
                except Exception:
                    pass

    @staticmethod
    def _all_mail(imap: imaplib.IMAP4_SSL) -> str:
        """Locate All Mail by its RFC 6154 \\All attribute.

        The display name follows the ACCOUNT's interface language, not the
        language of the mail in it, and IMAP sends folder names as modified
        UTF-7. A zh-TW account therefore reports "[Gmail]/&UWiQ6JD1TvY-", so
        matching the literal "[Gmail]/All Mail" fails on any non-English
        account. It fails badly: SELECT misses, and the SEARCH after it then
        answers "no messages" rather than erroring.
        """
        typ, lines = imap.list()
        if typ == "OK":
            for raw in lines:
                line = raw.decode("utf-8", "replace")
                if r"\All" in line:
                    match = re.search(r'"([^"]+)"\s*$', line)
                    if match:
                        return f'"{match.group(1)}"'
        return '"[Gmail]/All Mail"'

    def _search(self, imap: imaplib.IMAP4_SSL) -> list[bytes]:
        """Newest-first UIDs for the label.

        The query is sent as a UTF-8 literal, never interpolated into a quoted
        string: a quoted string cannot carry an unescaped double quote (Gmail's
        own phrase syntax needs one) nor any non-ASCII (this account has Chinese
        label names), and both used to fail with `BAD Could not parse command`
        rather than returning nothing.

        `in:anywhere` matters because a plain search skips Spam, and forwarded
        mail fails SPF often enough to land there.

        Bounded by newer_than rather than by a stored watermark. At this volume
        (tens of messages) a cursor would add a persistence failure mode for no
        gain, and a watermark advanced before its batch is committed loses that
        batch permanently.
        """
        query = f"label:{self._label} in:anywhere newer_than:{self._newer_than}d"
        # imaplib appends the {n} literal marker after the final arg, so
        # CHARSET UTF-8 has to precede X-GM-RAW.
        imap.literal = query.encode("utf-8")
        typ, res = imap.uid("SEARCH", "CHARSET", "UTF-8", "X-GM-RAW")
        if typ != "OK":
            # A rejected SEARCH and a label with nothing in it are different
            # facts and must not share a return value. Collapsing them is
            # exactly what EventSource.fetch's fail-closed contract forbids,
            # and it is how a dead source passes for a quiet week.
            raise RuntimeError(f"{self.name}: IMAP SEARCH failed ({typ}): {res}")
        if not res or not res[0]:
            return []   # genuinely empty label
        return res[0].split()[::-1]

    def _events_in_message(self, imap: imaplib.IMAP4_SSL,
                           uid: bytes) -> list[Event] | None:
        """Events linked from one message, or None when the FETCH itself failed.

        None and [] are deliberately different: [] means the message was read,
        None means it could not be read at all, and the caller counts the second
        to decide whether the whole label is broken. [] covers two cases that
        differ for a debugger, no event link in the message and a skip_from
        match, and a skipped message usually has plenty of links.
        """
        try:
            typ, data = imap.uid("FETCH", uid, "(BODY.PEEK[])")
            if typ != "OK" or not data or not isinstance(data[0], tuple):
                log.warning("%s: FETCH failed for uid %s (%s)", self.name, uid, typ)
                return None
            message = email.message_from_bytes(data[0][1])
            if self._skip_from and _sender(message) == self._skip_from:
                self._skipped_own += 1
                # [] and NOT None: the message was read fine, but None is this
                # method's "could not read it" signal, counted toward the
                # every-FETCH-failed guard. A label of nothing but our own
                # digests would then raise and take a working source down,
                # instead of correctly contributing no events.
                return []
            subject = _decode_header(message.get("Subject", "")).strip()
            sent = email.utils.parsedate_to_datetime(message.get("Date", ""))                 if message.get("Date") else None
            body = self._body(message)
        except Exception as exc:
            # imap.uid does not only fail through `typ`. A socket error during
            # send() surfaces as IMAP4.abort and a malformed tagged response as
            # IMAP4.error (checked against CPython 3.13's imaplib). Uncaught,
            # either would escape the caller's loop and abandon every remaining
            # uid, so one transient blip would turn "39 of 40 read" into "this
            # label produced nothing" -- the opposite of the tolerance the
            # caller's comment promises.
            log.warning("%s: could not read uid %s (%s: %s)",
                        self.name, uid, type(exc).__name__, exc)
            return None
        # Hoisted: both are the same for every link in this message, and
        # _summary strips the entire body each time it runs.
        # The From header, which both scoring tiers read. Kept in the live run
        # and withheld at export instead (see store._UNPUBLISHED_COLUMNS),
        # because it names a third party and a sender is not always the
        # organizer, so it is input to a score rather than a fact to publish.
        organizer = _decode_header(message.get("From", ""))
        description = self._summary(subject, body)
        out = []
        for href, title in harvest(body, fallback_title=subject):
            url = canon_url(href)
            out.append(Event(
                event_uid=f"{self.kind}:{url}",
                title=title or subject or url,
                url=url,
                source=self.name,
                source_kind=self.kind,
                organizer=organizer,
                description=description,
                # The message date. Real, unlike the JSON-LD sources, so mailbox
                # items qualify under first_run.require_known_publish_time.
                published_at=iso_or_empty(sent.isoformat()) if sent else "",
            ))
        return out

    def _summary(self, subject: str, body: str) -> str:
        """Subject plus a bounded slice of the message text.

        The body is included because the keyword gate and the scorer both read
        `description`, and the subject alone is often signal-free: a Handshake
        digest titled "New events this week" says nothing, while the line naming
        a resume review sits in the body. Bounded because every link in one
        message gets its own Event carrying this same text.
        """
        text = " ".join(strip_html(body).split())[: self._body_chars]
        header = f"From email: {subject}"
        return f"{header}\n\n{text}" if text else header

    @staticmethod
    def _body(message) -> str:
        """Concatenate every text part.

        Both halves of a multipart alternative are read, not just the preferred
        one: marketing mail routinely puts the call-to-action link in the HTML
        part and a bare URL in the plain-text part, and either may be the only
        place a given event appears.
        """
        parts = []
        for part in message.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if part.get_content_type() not in ("text/plain", "text/html"):
                continue
            try:
                payload = part.get_payload(decode=True)
            except Exception:
                continue
            if payload:
                parts.append(payload.decode(part.get_content_charset() or "utf-8",
                                            errors="replace"))
        return "\n".join(parts)
