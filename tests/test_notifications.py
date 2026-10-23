import asyncio
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agent_bridge.broker import Broker
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.notifications import deliver
from agent_bridge.store import Store
from agent_bridge.telegram_guard import delivery_status, register_child


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))
        self.profile = Profile("test", "codex", "fixed")
        self.config = Config({"test": self.profile}, codex_async_python="configured-test-helper")
        self.store.parent_control("parent", "parent_register", 0, "register", "native-parent", self.temp.name)
        self.sid = self.store.open(self.profile, self.temp.name, "read-only", "open", "parent")["session_id"]
        self.store.worker_ready(self.sid, "g", 1, "start", "native-child")
        self.task = self.store.dispatch(self.sid, "test", "dispatch")["task_id"]
        self.store.claim(self.config)
        self.attempt = self.store.begin_attempt(self.sid, "g")["active_attempt"]
        self.store.finish(self.sid, "g", self.task, self.attempt, "completed", {"answer": "done"})
        self.calls = []

    async def sender(self, config, parent, message, event_seq, receipt=None):
        self.calls.append({"event": event_seq, "receipt": receipt})
        return {"receipt": "submission", "ready": bool(receipt)}

    async def test_paused_and_closed_parent_never_woken(self):
        self.store.parent_control("parent", "parent_pause", 0, "pause")
        await deliver(self.store, self.config, self.sender)
        self.store.parent_control("parent", "parent_close", 1, "close")
        await deliver(self.store, self.config, self.sender)
        self.assertEqual([], self.calls)
        self.assertEqual("pending", self.store.get_parent("parent")["deliveries"][0]["status"])

    async def test_received_and_processed_are_separate_and_no_duplicate_send(self):
        await deliver(self.store, self.config, self.sender)
        await deliver(self.store, self.config, self.sender)
        await deliver(self.store, self.config, self.sender)
        self.assertEqual([None, "submission"], [c["receipt"] for c in self.calls])
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual("accepted", item["status"])
        self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=item["event_seq"])
        self.assertEqual("processed", self.store.get_parent("parent")["deliveries"][0]["status"])

    async def test_lost_queue_reply_is_not_retried(self):
        async def uncertain(*args):
            self.calls.append("sent")
            raise ConnectionError("ack lost")

        await deliver(self.store, self.config, uncertain)
        await deliver(self.store, self.config, uncertain)
        self.assertEqual(["sent"], self.calls)
        self.assertEqual("uncertain", self.store.get_parent("parent")["deliveries"][0]["status"])

    async def test_helper_spawn_failure_remains_pending_and_can_be_retried(self):
        self.config.codex_async_python = str(self.store.state / "missing-python")
        await deliver(self.store, self.config)
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual("pending", item["status"])
        self.assertIn("did not start", item["error"])
        await deliver(self.store, self.config, self.sender)
        self.assertEqual(1, len(self.calls))
        self.assertEqual("wake_pending", self.store.get_parent("parent")["deliveries"][0]["status"])

    async def test_parent_pause_during_slow_send_skips_remaining_notifications(self):
        (self.store.state / "config.json").write_text(json.dumps({
            "profiles": {"test": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
            "telegram_database": None, "codex_async_python": "fixture",
        }))
        broker = Broker(self.store.state)
        for index in range(2):
            tid = self.store.dispatch(self.sid, "task", str(index))["task_id"]
            aid = self.store.claim(self.config)["attempt_id"]
            self.store.begin_attempt(self.sid, "g")
            self.store.finish(self.sid, "g", tid, aid, "completed", {"answer": "done"})
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def sender(*args):
            calls.append(args[3])
            entered.set()
            await release.wait()
            return {"ready": True, "receipt": "test"}

        async def slow_deliver(store, config):
            await deliver(store, config, sender)

        with patch("agent_bridge.notifications.deliver", slow_deliver):
            running = asyncio.create_task(broker.notifications())
            try:
                await asyncio.wait_for(entered.wait(), 1)
                await asyncio.wait_for(broker.call("agent_control", {"session_id": "parent", "action": "parent_pause", "expected_revision": 0, "request_id": "pause"}), .5)
                self.assertEqual("paused", self.store.get_parent("parent")["desired"])
            finally:
                broker.stopping.set()
                release.set()
                await asyncio.wait_for(running, 1)
        self.assertEqual(1, len(calls))
        self.assertEqual(["accepted", "pending", "pending"], [d["status"] for d in self.store.get_parent("parent")["deliveries"]])

    async def test_acknowledgement_during_send_is_not_overwritten(self):
        await deliver(self.store, self.config, self.sender)

        async def acknowledging_sender(config, parent, message, seq, receipt=None):
            self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=seq)
            return {"ready": True, "receipt": receipt}

        await deliver(self.store, self.config, acknowledging_sender)
        self.assertEqual("processed", self.store.get_parent("parent")["deliveries"][0]["status"])

    async def check_early_ack(self, fail_reply):
        async def sender(config, parent, message, seq, receipt=None):
            self.calls.append(seq)
            ack = self.store.parent_control("parent", "parent_ack", 0, "early-ack", event_seq=seq)
            self.assertEqual(ack, self.store.parent_control("parent", "parent_ack", 0, "early-ack", event_seq=seq))
            if fail_reply:
                raise ConnectionError("reply lost after parent processed event")
            return {"ready": True, "receipt": "received"}

        await deliver(self.store, self.config, sender)
        await deliver(self.store, self.config, sender)
        self.assertEqual(1, len(self.calls))
        self.assertEqual("processed", self.store.get_parent("parent")["deliveries"][0]["status"])

    async def test_parent_can_ack_initial_send_before_receipt_returns(self):
        await self.check_early_ack(False)

    async def test_parent_ack_survives_lost_initial_send_reply(self):
        await self.check_early_ack(True)

    async def test_uncertain_delivery_can_be_acknowledged_without_resending(self):
        sender = AsyncMock(side_effect=ConnectionError("queue reply lost"))
        await deliver(self.store, self.config, sender)
        delivery = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual("uncertain", delivery["status"])
        self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=delivery["event_seq"])
        await deliver(self.store, self.config, sender)
        self.assertEqual(1, sender.await_count)
        self.assertEqual("processed", self.store.get_parent("parent")["deliveries"][0]["status"])

    def test_ack_still_requires_sending_and_the_correct_parent(self):
        seq = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        with self.assertRaisesRegex(BridgeError, "sent to this parent"):
            self.store.parent_control("parent", "parent_ack", 0, "pending-ack", event_seq=seq)
        self.store.parent_control("other", "parent_register", 0, "register-other", "native-other", self.temp.name)
        with self.store.connection(write=True) as db:
            db.execute("UPDATE deliveries SET status='uncertain' WHERE event_seq=?", (seq,))
        with self.assertRaisesRegex(BridgeError, "sent to this parent"):
            self.store.parent_control("other", "parent_ack", 0, "wrong-parent", event_seq=seq)

    async def check_delivery_fairness(self, mode):
        for index in range(9):
            tid = self.store.dispatch(self.sid, "task", "many-" + str(index))["task_id"]
            aid = self.store.claim(self.config)["attempt_id"]
            self.store.begin_attempt(self.sid, "g")
            self.store.finish(self.sid, "g", tid, aid, "completed", {"answer": "done"})
        if mode == "wake":
            with self.store.connection(write=True) as db:
                db.execute("UPDATE deliveries SET status='wake_pending',receipt='already-queued'")
        self.store.parent_control("other", "parent_register", 0, "register-other", "native-other", self.temp.name)
        sid = self.store.open(self.profile, self.temp.name, "read-only", "open-other", "other")["session_id"]
        self.store.worker_ready(sid, "g", 2, "start", "native-other-child")
        tid = self.store.dispatch(sid, "task", "other-task")["task_id"]
        aid = self.store.claim(self.config)["attempt_id"]
        self.store.begin_attempt(sid, "g")
        self.store.finish(sid, "g", tid, aid, "completed", {"answer": "done"})

        async def sender(config, parent, message, seq, receipt=None):
            self.calls.append(parent["id"])
            return {"deferred": "busy"} if parent["id"] == "parent" else {"ready": True, "receipt": "sent"}

        with patch("agent_bridge.notifications.delivery_status", side_effect=lambda db, native: "paused" if mode == "telegram" and native == "native-parent" else "open"):
            await deliver(self.store, self.config, sender)
            self.assertNotIn("other", self.calls)
            # A new Store uses the persisted ordering, as after a broker restart.
            await deliver(Store(self.store.state), self.config, sender)
        self.assertEqual(1, self.calls.count("other"))
        self.assertEqual("accepted", self.store.get_parent("other")["deliveries"][0]["status"])

    async def test_busy_parent_does_not_starve_other_parents(self):
        await self.check_delivery_fairness("pending")

    async def test_wake_pending_does_not_starve_other_parents(self):
        await self.check_delivery_fairness("wake")

    async def test_telegram_paused_parent_does_not_starve_other_parents(self):
        await self.check_delivery_fairness("telegram")

    async def test_notification_action_uses_the_event_status_even_after_retry(self):
        statuses = {}
        for status in ("recovery_needed", "failed", "interrupted", "cancelled"):
            tid = self.store.dispatch(self.sid, "task", "status-" + status)["task_id"]
            aid = self.store.claim(self.config)["attempt_id"]
            self.store.begin_attempt(self.sid, "g")
            self.store.finish(self.sid, "g", tid, aid, status, {"answer": status})
            seq = self.store.events(task_id=tid)[-1]["seq"]
            statuses[seq] = status
            if status != "recovery_needed":
                self.store.retry(tid, self.sid, "task", "retry-" + status)
                self.store.cancel(tid, "cancel-retry-" + status)
        messages = {}

        async def sender(config, parent, message, seq, receipt=None):
            messages[seq] = message
            return {"ready": True, "receipt": "sent"}

        await deliver(self.store, self.config, sender)
        for seq, status in statuses.items():
            self.assertNotIn("agent_review", messages[seq])
            self.assertIn("resolve" if status == "recovery_needed" else "retry", messages[seq])
        completed = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        self.assertIn("agent_review", messages[completed])

    async def test_startup_failure_notification_requests_inspection_and_resume(self):
        tid = self.store.dispatch(self.sid, "queued task", "queued")["task_id"]
        self.store.worker_stopped(self.sid, "g", "CLI unavailable")
        seq = self.store.events(task_id=tid)[-1]["seq"]
        messages = {}

        async def sender(config, parent, message, event_seq, receipt=None):
            messages[event_seq] = message
            return {"ready": True, "receipt": "sent"}

        await deliver(self.store, self.config, sender)
        self.assertIn("agent_inspect", messages[seq])
        self.assertIn("resume", messages[seq])
        self.assertNotIn("agent_review", messages[seq])
        self.assertNotIn("resolve", messages[seq])

    def test_delivery_order_migration_preserves_pending_records(self):
        before = self.store.get_parent("parent")["deliveries"]
        with self.store.connection(write=True) as db:
            db.execute("ALTER TABLE deliveries DROP COLUMN last_attempt")
        upgraded = Store(self.store.state)
        self.assertEqual(before, upgraded.get_parent("parent")["deliveries"])

    async def test_telegram_pause_continue_and_deletion_are_checked_read_only(self):
        path = Path(self.temp.name) / "telegram.sqlite"
        with closing(sqlite3.connect(path)) as db, db:
            db.executescript(
                "CREATE TABLE topics(thread_id TEXT,topic_id INTEGER); CREATE TABLE removed_topics(thread_id TEXT,topic_id INTEGER); CREATE TABLE inbox(thread_id TEXT,topic_id INTEGER,status TEXT);"
            )
            db.execute("INSERT INTO topics VALUES('native-parent',10)")
            db.execute("INSERT INTO inbox VALUES('native-parent',10,'stop_pending')")
        self.config.telegram_database = str(path)
        self.assertEqual("paused", delivery_status(path, "native-parent"))
        await deliver(self.store, self.config, self.sender)
        self.assertEqual([], self.calls)
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("UPDATE inbox SET status='cancelled'")
        self.assertEqual("open", delivery_status(path, "native-parent"))
        await deliver(self.store, self.config, self.sender)
        self.assertEqual(1, len(self.calls))
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("INSERT INTO removed_topics VALUES('native-parent',10)")
        await deliver(self.store, self.config, self.sender)
        self.assertEqual(1, len(self.calls))
        self.assertEqual("closed", self.store.get_parent("parent")["desired"])

    def test_unreadable_telegram_state_does_not_enable_delivery(self):
        self.assertEqual("unavailable", delivery_status(Path(self.temp.name) / "missing.sqlite", "native"))

    def test_child_registration_requires_filter_before_model_execution(self):
        path = Path(self.temp.name) / "bot.sqlite"
        with closing(sqlite3.connect(path)) as db, db:
            db.executescript(
                "CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE external_children(thread_id TEXT PRIMARY KEY,owner TEXT);"
            )
        with self.assertRaisesRegex(RuntimeError, "support external child filtering"):
            register_child(path, "codex", "native")
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("INSERT INTO meta VALUES('external_child_filter','1')")
        register_child(path, "codex", "native")
        register_child(path, "claude", "claude-native")
        register_child(path, "codex", "native")
        with closing(sqlite3.connect(path)) as db, db:
            self.assertEqual(
                {"native", "claude:claude-native"},
                {r[0] for r in db.execute("SELECT thread_id FROM external_children")},
            )
