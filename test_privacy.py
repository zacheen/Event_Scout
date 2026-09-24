"""Offline privacy regression tests using synthetic data only."""
from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

from eventscout import config, private_ledger, scoring
from eventscout.models import Event, Score, State
from eventscout.sources.gmail_label import GmailLabelSource
from eventscout.store import SqliteEventStore
from eventscout.urls import canon_url

ROOT = Path(__file__).resolve().parent


class PrivacyTests(unittest.TestCase):
    def test_recipient_parameters_removed_without_losing_event_identity(self):
        raw = ("https://luma.com/event?event=42&locale=en"
               "&tk=invite&pk=recipient&mkt_tok=tracking&utm_source=mail&trkCampaign=x")
        self.assertEqual(canon_url(raw),
                         "https://luma.com/event?event=42&locale=en")
        self.assertEqual(canon_url("https://us06web.zoom.us/event?type=webinar&user_id=person"),
                         "https://us06web.zoom.us/event?type=webinar")
        self.assertEqual(canon_url("https://events.example.test/event?pk=42&tk=event"),
                         "https://events.example.test/event?pk=42&tk=event")

    def test_legacy_ledger_urls_preserve_delivery_and_registration_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            events, cache = Path(tmp) / "events.jsonl", Path(tmp) / "cache.jsonl"
            legacy_url = "https://luma.com/example?tk=recipient"
            uid = "gmail_label:" + legacy_url
            events.write_text(json.dumps({"event_uid": uid, "canonical_url": legacy_url,
                "title": "Career fair", "source": "gmail_label", "source_kind": "gmail_label",
                "start": "2026-10-02T10:00:00+00:00", "first_seen": "2026-09-20T10:00:00+00:00",
                "last_seen": "2026-09-20T10:00:00+00:00", "alerted_at": "2026-09-20T10:00:00+00:00",
                "cleared_floor_at": "2026-10-01T10:00:00+00:00"}))
            store = SqliteEventStore(":memory:")
            try:
                store.import_jsonl(events, cache)
                self.assertIn("https://luma.com/example", store.reported_urls())
                reminders = store.due_for_resweep(72, "2026-10-01T12:00:00+00:00", min_gap_hours=24)
                self.assertEqual([event.url for event in reminders], ["https://luma.com/example"])
                self.assertEqual(reminders[0].event_uid, uid)
                self.assertEqual(store.mark_by_url("https://luma.com/example", State.REGISTERED),
                                 [(uid, "Career fair")])
                self.assertEqual(store.due_for_resweep(72, "2026-10-01T12:00:00+00:00", min_gap_hours=24), [])
                store.export_jsonl(events, cache)
                self.assertEqual(json.loads(events.read_text())["canonical_url"],
                                 "https://luma.com/example")
            finally:
                store.close()

    def test_private_mirror_omits_sender_text_labels_and_both_reason_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = SqliteEventStore(":memory:")
            restored = SqliteEventStore(":memory:")
            try:
                event = Event(event_uid="gmail_label:https://events.example.test/event",
                              url="https://events.example.test/event", title="Career fair",
                              source="gmail:private-label", source_kind="gmail_label",
                              organizer="Private Sender <sender@example.test>",
                              description="Private mail body")
                score = Score(8, 7, 3, reason="Private reason")
                store.upsert(event, score)
                store.put_cached_score(event.event_uid, "model", score)
                store.mark_alerted([event.event_uid])
                store.mark_cleared_floor([event.event_uid])
                store.set_state(event.event_uid, State.REGISTERED)
                events, cache = Path(tmp) / "events.jsonl", Path(tmp) / "cache.jsonl"
                store.export_jsonl(events, cache)
                contents = events.read_text() + cache.read_text()
                for marker in ("Private Sender", "sender@example.test", "private-label",
                               "Private mail body", "Private reason"):
                    self.assertNotIn(marker, contents)
                row = json.loads(events.read_text())
                self.assertEqual(row["source"], "gmail_label")
                self.assertEqual(row["state"], "registered")
                self.assertTrue(row["alerted_at"])
                self.assertTrue(row["cleared_floor_at"])
                restored.import_jsonl(events, cache)
                self.assertIn(event.url, restored.reported_urls())
                self.assertEqual(restored.get_cached_score(event.event_uid, "model").reason, "")
            finally:
                store.close()
                restored.close()

    def test_sender_header_is_scored_locally_and_never_exported(self):
        """The From header reaches scoring but not the mirror.

        Blanking it at ingestion would also have taken it away from the two
        scoring tiers that read Event.organizer, so the boundary is export.
        """
        message = (b"From: Private Sender <sender@example.test>\r\n"
                   b"Subject: Career Fair\r\nContent-Type: text/html\r\n\r\n"
                   b'<a href="https://luma.com/example">Career Fair</a>')
        imap = Mock()
        imap.uid.return_value = ("OK", [(b"1", message)])
        events = GmailLabelSource("mailbox", "user", "password", "private-label"
                                  )._events_in_message(imap, b"1")
        self.assertEqual(len(events), 1)
        self.assertIn("sender@example.test", events[0].organizer)

        store = SqliteEventStore(":memory:")
        try:
            store.upsert(events[0], Score(5, 5, 5, reason="r"))
            with tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "events.jsonl"
                store.export_jsonl(out, Path(tmp) / "cache.jsonl")
                body = out.read_text(encoding="utf-8")
            self.assertNotIn("sender@example.test", body)
            self.assertNotIn("organizer", body)
        finally:
            store.close()

    def test_profile_may_live_outside_the_project_but_must_exist(self):
        """A resume OUTSIDE the project is the supported case, not an attack.

        Keeping it out of this directory is what stops it being committed from
        here, so refusing an external path would force a copy IN and make the
        exposure worse. A path naming nothing still raises, because falling
        through to an empty profile flattens every fit score to a constant and
        the run would report that as normal.
        """
        with tempfile.TemporaryDirectory() as tmp:
            sandbox = Path(tmp) / "project"
            sandbox.mkdir()
            outside = Path(tmp) / "outside.txt"
            outside.write_text("external profile")
            (sandbox / "approved.txt").write_text("approved profile")
            with patch.object(config, "ROOT", sandbox), patch.dict(os.environ, {
                    "PROFILE_TEXT": "", "RESUME_TEXT": "", "PROFILE_PATH": "approved.txt"}):
                self.assertEqual(config._profile_text(), "approved profile")
                os.environ["PROFILE_PATH"] = str(outside)
                self.assertEqual(config._profile_text(), "external profile")
                os.environ["PROFILE_PATH"] = "missing.txt"
                with self.assertRaisesRegex(ValueError, "readable file"):
                    config._profile_text()

    def test_new_profile_does_not_reuse_old_profile_scores(self):
        inner = Mock(cacheable=True, method_label="API")
        inner.score.side_effect = [Score(2, 3, 4), Score(8, 9, 4)]
        store = SqliteEventStore(":memory:")
        event = Event(event_uid="test", title="Career fair", url="https://example.test/event",
                      source="calendar", source_kind="jsonld")
        try:
            with patch.object(scoring, "_configured_tier", return_value=inner):
                first = scoring.build_scorer(config.Settings(profile_text="first profile"), store)
                second = scoring.build_scorer(config.Settings(profile_text="approved profile"), store)
                self.assertEqual(first.score(event).fit, 2)
                self.assertEqual(first.score(event).fit, 2)
                self.assertEqual(second.score(event).fit, 8)
                self.assertEqual(inner.score.call_count, 2)
        finally:
            store.close()

    def test_private_repository_required_before_any_git_operation(self):
        for metadata in ({"private": False, "full_name": "example/private-data"},
                         {"full_name": "example/private-data"},
                         {"private": True, "full_name": "example/wrong-repo"}):
            response = io.BytesIO(json.dumps(metadata).encode())
            output = io.StringIO()
            with patch.dict(os.environ, {"LEDGER_REPOSITORY": "example/private-data",
                                         "LEDGER_TOKEN": "synthetic-token"}), \
                    patch.object(private_ledger, "urlopen", return_value=response), \
                    patch.object(private_ledger, "_git") as git, \
                    contextlib.redirect_stdout(output):
                self.assertEqual(private_ledger.main(["prepare"]), 1)
                git.assert_not_called()
            self.assertNotIn("private-data", output.getvalue())
            self.assertNotIn("synthetic-token", output.getvalue())

    def test_verified_private_repository_is_accepted(self):
        response = io.BytesIO(b'{"private":true,"full_name":"example/private-data"}')
        with patch.object(private_ledger, "urlopen", return_value=response), \
                patch.dict(os.environ, {"GITHUB_REPOSITORY": "example/public-code"}):
            self.assertEqual(private_ledger.validate_repository("example/private-data", "token"),
                             "https://github.com/example/private-data.git")

    def test_private_repository_errors_never_echo_response_or_credentials(self):
        output = io.StringIO()
        with patch.object(private_ledger, "validate_repository",
                          side_effect=RuntimeError("PRIVATE_SENTINEL")), \
                contextlib.redirect_stdout(output):
            self.assertEqual(private_ledger.main(["save"]), 1)
        self.assertNotIn("PRIVATE_SENTINEL", output.getvalue())

    def test_changed_remote_cannot_receive_ledger(self):
        result = subprocess.CompletedProcess([], 0, stdout="https://github.com/example/public.git\n")
        with patch.object(private_ledger, "_git", return_value=result) as git:
            with self.assertRaisesRegex(ValueError, "differs"):
                private_ledger.save("https://github.com/example/private-data.git", "token")
            self.assertEqual(git.call_count, 1)

    def test_workflow_withholds_scan_output_and_uses_only_private_ledger_helper(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/scan.yml").read_text())
        steps = workflow["jobs"]["scan"]["steps"]
        commands = [step.get("run", "") for step in steps]
        self.assertIn("python -m eventscout.private_ledger prepare", commands)
        self.assertIn("python -m eventscout.private_ledger save", commands)
        scan = next(step for step in steps if step.get("id") == "scan")
        self.assertIn("> .private/scan.log 2>&1", scan["run"])
        self.assertFalse(any("git push" in command for command in commands))
        self.assertEqual(workflow["permissions"]["contents"], "read")

    def test_private_ledger_roundtrip_and_rejected_push_preserve_both_writers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            remote = root / "remote.git"
            checkout = root / "checkout"
            subprocess.run(["git", "init", "--bare", str(remote)], check=True,
                           capture_output=True)

            def write_event(directory, key):
                store = SqliteEventStore(":memory:")
                files = [directory / name for name in private_ledger.FILES]
                try:
                    store.import_jsonl(*files)
                    event = Event(event_uid=key, title="Career fair",
                                  url=f"https://example.test/{key}",
                                  source="calendar", source_kind="jsonld")
                    store.upsert(event, Score(8, 7, 2))
                    store.mark_alerted([key])
                    store.mark_cleared_floor([key])
                    store.export_jsonl(*files)
                finally:
                    store.close()

            with patch.object(private_ledger, "ROOT", root), \
                    patch.object(private_ledger, "LEDGER", checkout):
                private_ledger.prepare(str(remote), "test-token")
                write_event(checkout, "first")
                private_ledger.save(str(remote), "test-token")
                rival = root / "rival"
                private_ledger._git(["clone", "--branch", "data", str(remote), str(rival)],
                                    "test-token")
                with patch.object(private_ledger, "LEDGER", rival):
                    write_event(rival, "rival")
                    private_ledger.save(str(remote), "test-token")
                write_event(checkout, "ours")
                private_ledger.save(str(remote), "test-token")
                result = subprocess.run(
                    ["git", "--git-dir", str(remote), "show", "data:cloud_data/events.jsonl"],
                    check=True, capture_output=True, text=True)
                rows = [json.loads(line) for line in result.stdout.splitlines()]
                self.assertEqual({row["event_uid"] for row in rows}, {"first", "rival", "ours"})
                self.assertTrue(all(row["alerted_at"] and row["cleared_floor_at"] for row in rows))


if __name__ == "__main__":
    unittest.main()
