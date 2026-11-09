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
from agent_bridge.codex_parent import advance
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.notifications import cleanup_processed, deliver
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
        return {"receipt": "submission", "ready": bool(receipt),
                "native_status": "received" if receipt else "enqueued",
                "native_turn_id": "turn-1" if receipt else None}

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

    async def test_ready_without_native_user_message_never_means_received(self):
        sender = AsyncMock(return_value={"receipt": "submission", "ready": True, "native_status": "enqueued"})
        await deliver(self.store, self.config, sender)
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual(("wake_pending", "enqueued", None),
                         (item["status"], item["native_status"], item["native_turn_id"]))
        with self.store.connection() as db:
            self.assertEqual("enqueued", db.execute("SELECT status FROM native_send_queue").fetchone()[0])

    async def test_ack_before_queue_reply_retains_receipt_and_retracts_once(self):
        async def sender(config, parent, message, seq, receipt=None):
            self.store.parent_control("parent", "parent_ack", 0, "early-ack", event_seq=seq)
            return {"receipt": "late-receipt", "ready": False, "native_status": "enqueued"}

        await deliver(self.store, self.config, sender)
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual(("processed", "late-receipt", "enqueued", "pending"),
                         (item["status"], item["receipt"], item["native_status"], item["queue_cleanup"]))
        # ACK proves that the event was handled, not that the queued prompt was read.
        with self.store.connection() as db:
            self.assertEqual("enqueued", db.execute("SELECT status FROM native_send_queue").fetchone()[0])
        retractor = AsyncMock(return_value={"receipt": "late-receipt", "queue_cleanup": "removed", "native_status": "removed"})
        self.store.parent_control("parent", "parent_pause", 1, "pause-after-ack")
        await cleanup_processed(self.store, self.config, retractor)
        await cleanup_processed(self.store, self.config, retractor)
        retractor.assert_awaited_once()
        self.assertEqual((item["event_seq"], "late-receipt"), retractor.call_args.args[2:])
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual(("processed", "removed", "removed", None),
                         (item["status"], item["native_status"], item["queue_cleanup"], item["native_turn_id"]))
        with self.store.connection() as db:
            self.assertEqual("cancelled", db.execute("SELECT status FROM native_send_queue").fetchone()[0])

    async def test_ack_cleanup_through_native_helper_leaves_other_queue_entries_alone(self):
        queue = [{"id": "unrelated", "clientUserMessageId": "other-tool:message"}]
        calls = []

        class Client:
            def enqueue(self, thread_id, message, client_id):
                calls.append(("enqueue", thread_id))
                queue.append({"id": "own-receipt", "clientUserMessageId": client_id})
                return "own-receipt"

            def request(self, method, params):
                calls.append((method, params))
                if method == "thread/queue/list":
                    return {"data": list(queue)}
                if method == "thread/queue/delete":
                    self_id = params["queuedSubmissionId"]
                    self_entry = next(item for item in queue if item["id"] == self_id)
                    queue.remove(self_entry)
                    return {"deleted": True}
                raise AssertionError("Cleanup must not resume, interrupt, or inspect a different session")

        client = Client()

        async def sender(config, parent, message, seq, receipt=None):
            return advance(client, {"thread_id": parent["native_id"], "message": message,
                                    "event_id": str(seq), "receipt": receipt}, RuntimeError)

        async def cleaner(config, parent, seq, receipt=None, *, check_only=False):
            return advance(client, {"thread_id": parent["native_id"], "event_id": str(seq),
                                    "receipt": receipt, "retract": True, "cleanup_check_only": check_only}, RuntimeError)

        await deliver(self.store, self.config, sender, cleaner)
        seq = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=seq)
        self.store.parent_control("parent", "parent_pause", 1, "pause-after-ack")
        await deliver(self.store, self.config, sender, cleaner)
        await deliver(self.store, self.config, sender, cleaner)
        self.assertEqual([{"id": "unrelated", "clientUserMessageId": "other-tool:message"}], queue)
        self.assertEqual(1, sum(method == "enqueue" for method, _ in calls))
        self.assertEqual([(method, params) for method, params in calls if method.endswith("/delete")], [
            ("thread/queue/delete", {"threadId": "native-parent", "queuedSubmissionId": "own-receipt"}),
        ])

    async def test_cleanup_failure_keeps_ack_and_does_not_resend(self):
        await deliver(self.store, self.config, self.sender)
        seq = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=seq)
        retractor = AsyncMock(side_effect=ConnectionError("delete reply lost"))
        await deliver(self.store, self.config, self.sender, retractor)
        await deliver(self.store, self.config, self.sender, retractor)
        self.assertEqual(1, retractor.await_count)
        self.assertEqual(1, len(self.calls))
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual(("processed", "uncertain", "enqueued"),
                         (item["status"], item["queue_cleanup"], item["native_status"]))
        self.assertIn("delete reply lost", item["error"])

    async def test_late_send_reply_cannot_undo_completed_cleanup(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def sender(config, parent, message, seq, receipt=None):
            entered.set()
            await release.wait()
            return {"receipt": "late-receipt", "ready": False, "native_status": "enqueued"}

        running = asyncio.create_task(deliver(self.store, self.config, sender))
        await entered.wait()
        seq = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=seq)
        retractor = AsyncMock(return_value={"receipt": "late-receipt", "queue_cleanup": "removed", "native_status": "removed"})
        try:
            await cleanup_processed(self.store, self.config, retractor)
        finally:
            release.set()
            await running
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual(("processed", "removed", "removed", "late-receipt"),
                         (item["status"], item["queue_cleanup"], item["native_status"], item["receipt"]))
        with self.store.connection() as db:
            self.assertEqual("cancelled", db.execute("SELECT status FROM native_send_queue").fetchone()[0])

    async def test_old_cleanup_reply_cannot_undo_newer_terminal_result(self):
        await deliver(self.store, self.config, self.sender)
        seq = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=seq)
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow_cleaner(*args, **kwargs):
            entered.set()
            await release.wait()
            return {"receipt": "submission", "queue_cleanup": "uncertain", "native_status": "unconfirmed", "error": "old reply"}

        with patch("agent_bridge.notifications.time.time", return_value=100):
            running = asyncio.create_task(cleanup_processed(self.store, self.config, slow_cleaner))
            await entered.wait()
        latest = AsyncMock(return_value={"receipt": "submission", "queue_cleanup": "already_started",
                                        "native_status": "received", "native_turn_id": "actual-turn"})
        try:
            with patch("agent_bridge.notifications.time.time", return_value=131):
                await cleanup_processed(self.store, self.config, latest)
        finally:
            release.set()
            await running
        self.assertTrue(latest.call_args.kwargs["check_only"])
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual(("already_started", "received", None), (item["queue_cleanup"], item["native_status"], item["error"]))
        with self.store.connection() as db:
            self.assertEqual("received", db.execute("SELECT status FROM native_send_queue").fetchone()[0])

    async def test_uncertain_cleanup_stays_read_only_even_if_setup_fails(self):
        await deliver(self.store, self.config, self.sender)
        seq = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=seq)
        with self.store.connection(write=True) as db:
            db.execute("UPDATE deliveries SET queue_cleanup='uncertain'")
        retractor = AsyncMock(return_value={"deferred": "client unavailable", "queue_cleanup": "pending"})
        await cleanup_processed(self.store, self.config, retractor)
        self.assertTrue(retractor.call_args.kwargs["check_only"])
        self.assertEqual("uncertain", self.store.get_parent("parent")["deliveries"][0]["queue_cleanup"])

    async def test_cleanup_can_confirm_started_but_never_fabricates_receipt(self):
        await deliver(self.store, self.config, self.sender)
        seq = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        self.store.parent_control("parent", "parent_ack", 0, "ack", event_seq=seq)
        retractor = AsyncMock(return_value={"receipt": "submission", "queue_cleanup": "already_started", "native_status": "received"})
        await cleanup_processed(self.store, self.config, retractor)
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual(("uncertain", "enqueued"), (item["queue_cleanup"], item["native_status"]))
        with self.store.connection(write=True) as db:
            db.execute("UPDATE deliveries SET cleanup_last_attempt=0")
        retractor.return_value["native_turn_id"] = "actual-turn"
        await cleanup_processed(self.store, self.config, retractor)
        item = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual(("already_started", "received", "actual-turn"),
                         (item["queue_cleanup"], item["native_status"], item["native_turn_id"]))

    async def test_legacy_accepted_is_rechecked_without_resubmitting(self):
        await deliver(self.store, self.config, self.sender)
        with self.store.connection(write=True) as db:
            db.execute("UPDATE deliveries SET status='accepted',native_status='unknown'")
        sender = AsyncMock(return_value={"receipt": "submission", "ready": False, "native_status": "enqueued"})
        await deliver(self.store, self.config, sender)
        self.assertEqual("submission", sender.call_args.args[4])
        self.assertEqual("wake_pending", self.store.get_parent("parent")["deliveries"][0]["status"])

    def test_upgrade_does_not_treat_legacy_accepted_or_ack_as_native_receipt(self):
        with self.store.connection(write=True) as db:
            db.execute("UPDATE deliveries SET status='processed',receipt='legacy'")
            for column in ("native_status", "native_turn_id", "queue_cleanup", "cleanup_last_attempt"):
                db.execute("ALTER TABLE deliveries DROP COLUMN " + column)
        item = Store(self.store.state).get_parent("parent")["deliveries"][0]
        self.assertEqual(("processed", "unknown", "pending", "legacy"),
                         (item["status"], item["native_status"], item["queue_cleanup"], item["receipt"]))

    def test_upgrade_schedules_ack_written_by_old_broker_after_columns_exist(self):
        with self.store.connection(write=True) as db:
            db.execute("UPDATE deliveries SET status='processed',receipt='legacy',queue_cleanup=NULL")
        upgraded = Store(self.store.state)
        item = upgraded.get_parent("parent")["deliveries"][0]
        self.assertEqual(("processed", "unknown", "pending", "legacy"),
                         (item["status"], item["native_status"], item["queue_cleanup"], item["receipt"]))
        with upgraded.connection(write=True) as db:
            db.execute("UPDATE deliveries SET queue_cleanup='uncertain'")
        self.assertEqual("uncertain", Store(self.store.state).get_parent("parent")["deliveries"][0]["queue_cleanup"])

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
            return {"ready": True, "receipt": "test", "native_status": "received", "native_turn_id": "turn-1"}

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
            return {"ready": True, "receipt": receipt, "native_status": "received", "native_turn_id": "turn-1"}

        await deliver(self.store, self.config, acknowledging_sender)
        self.assertEqual("processed", self.store.get_parent("parent")["deliveries"][0]["status"])

    async def check_early_ack(self, fail_reply):
        async def sender(config, parent, message, seq, receipt=None):
            self.calls.append(seq)
            ack = self.store.parent_control("parent", "parent_ack", 0, "early-ack", event_seq=seq)
            self.assertEqual(ack, self.store.parent_control("parent", "parent_ack", 0, "early-ack", event_seq=seq))
            if fail_reply:
                raise ConnectionError("reply lost after parent processed event")
            return {"ready": True, "receipt": "received", "native_status": "received", "native_turn_id": "turn-1"}

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
            return {"deferred": "busy"} if parent["id"] == "parent" else {"ready": True, "receipt": "sent", "native_status": "received", "native_turn_id": "turn-1"}

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
            return {"ready": True, "receipt": "sent", "native_status": "received", "native_turn_id": "turn-1"}

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
            return {"ready": True, "receipt": "sent", "native_status": "received", "native_turn_id": "turn-1"}

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
