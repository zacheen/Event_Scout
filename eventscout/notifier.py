"""Digest email with .ics attachments.

One message per run, split into a closing-soon section and a general section.
Chosen over separate per-urgency emails because there is no push channel, and
three messages a day about one event reads as spam rather than urgency.
"""
from __future__ import annotations

import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

from zoneinfo import ZoneInfo

from .models import parse_iso, Event
from .protocols import Section


def display_zone(name: str) -> ZoneInfo:
    """The zone absolute times are RENDERED in. Resolved once, at construction.

    Needed because the raw string was being sliced for display, which is only
    right when the source happens to store the reader's own offset. Measured
    2026-09-18 on the live ledger: Luma and Gmail events carry -07:00 and
    printed correctly, while every neu-alumni-events row carries +00:00 and
    printed 7 hours late, one of them on the wrong DAY
    (2026-09-24T02:00:00+00:00 shown as "2026-09-24 02:00" for an event that
    starts 2026-09-23 19:00 Pacific).

    An unknown name raises here rather than at send time, since the alternative
    is a digest full of times in a zone nobody chose.
    """
    return ZoneInfo(name)


def _ics_stamp(value: str) -> str:
    when = parse_iso(value)
    return when.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ") if when else ""


def _escape(text: str) -> str:
    # RFC 5545 3.3.11: these four must be escaped or a calendar client silently
    # truncates the field at the offending character.
    return (text.replace("\\", "\\\\").replace(";", r"\;")
                .replace(",", r"\,").replace("\n", r"\n"))


def build_ics(events: list[Event]) -> str:
    """One VCALENDAR holding every event that has a start time."""
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//event-scout//EN",
             "CALSCALE:GREGORIAN", "METHOD:PUBLISH"]
    for event in events:
        start = _ics_stamp(event.start)
        if not start:
            continue
        # An hour after the start, for an event that states no end. Reaching
        # this line means _ics_stamp already parsed event.start successfully,
        # so parse_iso cannot return None here.
        end = _ics_stamp(event.end) or _ics_stamp(
            (parse_iso(event.start) + timedelta(hours=1)).isoformat())
        lines += [
            "BEGIN:VEVENT",
            f"UID:{event.event_uid}",
            f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
            f"DTSTART:{start}",
            f"DTEND:{end}",
            f"SUMMARY:{_escape(event.title)}",
            f"LOCATION:{_escape(event.location)}",
            f"DESCRIPTION:{_escape(event.url)}",
            f"URL:{event.url}",
            "END:VEVENT",
        ]
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


def _lead_time(start: str, now: datetime) -> str:
    """How long from now, or "" when the event states no start.

    The date alone does not say whether something is tonight or next week, and
    a last call for an event starting in four hours needs a different decision
    from one starting in three days. Rounded down deliberately: "in 5h" for
    something 5h50m away is the cautious reading.
    """
    when = parse_iso(start)
    if when is None:
        return ""
    hours = (when - now).total_seconds() / 3600
    if hours < 0:
        return "already started"
    if hours < 1:
        return "in under an hour"
    if hours < 48:
        return f"in {int(hours)}h"
    return f"in {int(hours // 24)} days"


def format_digest(sections: list[Section], zone: ZoneInfo,
                  now: datetime | None = None) -> str:
    """Plain-text rendering shared by every notifier.

    A free function rather than a method: ConsoleNotifier previously reached
    into EmailNotifier._body, so the two classes were coupled through a name
    whose leading underscore promised no such thing.

    `zone` is required rather than defaulted, since the bug this guards
    against was exactly a time rendered in a zone nobody chose (see
    display_zone).
    """
    now = now or datetime.now(timezone.utc)
    blocks: list[str] = []
    for name, items in sections:
        if not items:
            continue
        blocks.append(f"##### {name} ({len(items)}) #####\n")
        for event, score, urgency in items:
            lead = _lead_time(event.start, now)
            # Converted, not sliced. The stored offset is whatever the
            # SOURCE used, so slicing prints Pacific for Luma and UTC for
            # WordPress. parse_iso fills in UTC for a naive stamp, which is the
            # same assumption _lead_time beside it already makes, so the two
            # cannot disagree about the same event.
            start = parse_iso(event.start) if event.start else None
            # %Z included so the line says which zone it is in. Mail cannot
            # convert anything for a reader who is travelling, and the .ics
            # attachment beside it carries UTC for the calendar to convert, so
            # an unlabelled wall clock was the one ambiguity left here.
            when = (start.astimezone(zone).strftime("%Y-%m-%d %H:%M %Z")
                    if start else "date unknown")
            if lead:
                when = f"{when} ({lead})"
            where = event.location or ("online" if event.is_virtual else "location unknown")
            blocks.append(
                f"[{urgency}] {event.title}\n"
                f"    when: {when} | where: {where}\n"
                f"    fit={score.fit} access={score.access_value} cost={score.cost}"
                f" | {score.reason}\n"
                f"    source: {event.source}\n"
                f"    {event.url}\n")
    return "\n".join(blocks)


class EmailNotifier:
    def __init__(self, user: str, app_password: str, mail_to: str,
                 timezone_name: str, host: str = "smtp.gmail.com",
                 port: int = 587):
        self._user = user
        self._app_password = app_password
        self._mail_to = mail_to
        # Resolved now so a bad name fails before a run does any work, rather
        # than after the fetch and the scoring have already been paid for.
        self._zone = display_zone(timezone_name)
        self._host = host
        self._port = port

    def send(self, sections: list[Section], subject: str, footer: str = "") -> int:
        """Send one digest. Returns the number of events sent; 0 sends nothing."""
        total = sum(len(items) for _, items in sections)
        if total == 0:
            return 0
        if not (self._user and self._app_password and self._mail_to):
            raise RuntimeError("GMAIL_USER / GMAIL_APP_PASSWORD / MAIL_TO not all set")

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self._user
        message["To"] = self._mail_to
        body = format_digest(sections, self._zone)
        if footer:
            body += f"\n\n---\n{footer}\n"
        message.set_content(body)

        events = [e for _, items in sections for e, _, _ in items]
        ics = build_ics(events)
        if ics.count("BEGIN:VEVENT"):
            message.add_attachment(ics.encode("utf-8"), maintype="text",
                                   subtype="calendar", filename="event-scout.ics")
        with smtplib.SMTP(self._host, self._port) as server:
            server.starttls(context=ssl.create_default_context())
            server.login(self._user, self._app_password)
            server.send_message(message)
        return total

class ConsoleNotifier:
    """Dry-run stand-in. Prints the digest instead of sending it.

    Exists so a local run can be inspected without SMTP credentials and without
    putting mail in the inbox while thresholds are still being tuned.
    """

    def __init__(self, timezone_name: str):
        self._zone = display_zone(timezone_name)

    def send(self, sections: list[Section], subject: str, footer: str = "") -> int:
        total = sum(len(items) for _, items in sections)
        print(f"\n--- DRY RUN, no mail sent ---\nSubject: {subject}\n")
        print(format_digest(sections, self._zone) or "(no events)")
        if footer:
            print(f"---\n{footer}")
        return total
