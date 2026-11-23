import json
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agent_bridge import groups
from agent_bridge.broker import Broker
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.native_broadcast import deliver as deliver_broadcasts
from agent_bridge.notifications import deliver
from agent_bridge.store import Store


class NativeSendQueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.target = ("codex", str(uuid.uuid4()))
        self.other = ("antigravity", str(uuid.uuid4()))

    def enqueue(self, target=None, delivery_id="delivery", source="local_broadcast", message="message", status="pending"):
        target = target or self.target
        return self.store.enqueue_native_send(*target, source, delivery_id, message, status)

    def item(self, item_id):
        return self.store.get_native_send_queue_item(item_id)

    def test_local_sequence_allocation_is_monotonic_per_target(self):
        self.assertEqual(1, self.store.allocate_native_local_seq(*self.target))
        self.assertEqual(2, self.store.allocate_native_local_seq(*self.target))
        self.assertEqual(1, self.store.allocate_native_local_seq(*self.other))
        self.assertEqual(3, self.store.allocate_native_local_seq(*self.target))

        reopened = Store(self.root)
        self.assertEqual(4, reopened.allocate_native_local_seq(*self.target))
        seqs = [row["local_seq"] for row in reopened.claim_eligible_native_sends(10)]
        self.assertEqual([], seqs)

    def test_double_enqueue_is_idempotent_on_source_and_delivery(self):
        first = self.enqueue(delivery_id="delivery-1", message="first")
        second = self.enqueue(delivery_id="delivery-1", message="changed after retry")
        self.assertEqual(first, second)
        self.assertEqual(1, first["local_seq"])
        self.assertEqual("pending", first["status"])
        with self.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM native_send_queue").fetchone()[0])

        # The same delivery_id in another source namespace is a separate ordered write.
        other_source = self.enqueue(delivery_id="delivery-1", source="peer_deliver")
        self.assertNotEqual(first["id"], other_source["id"])
        self.assertEqual(2, other_source["local_seq"])

    def test_sending_and_uncertain_heads_block_later_sequences_for_same_target(self):
        items = [self.enqueue(delivery_id=f"delivery-{index}") for index in range(3)]

        claimed = self.store.claim_eligible_native_sends(10)
        self.assertEqual([items[0]["id"]], [row["id"] for row in claimed])
        sending = self.item(items[0]["id"])
        self.assertEqual(("sending", 1), (sending["status"], sending["revision"]))
        self.assertGreater(sending["last_attempt"], 0)
        self.assertEqual([], self.store.claim_eligible_native_sends(10))

        self.store.update_native_send(items[0]["id"], status="received", expected_status="sending")
        claimed = self.store.claim_eligible_native_sends(10)
        self.assertEqual([items[1]["id"]], [row["id"] for row in claimed])
        self.store.update_native_send(items[1]["id"], status="uncertain", expected_status="sending")
        self.assertEqual([], self.store.claim_eligible_native_sends(10))

        self.store.update_native_send(items[1]["id"], status="received", expected_status="uncertain")
        claimed = self.store.claim_eligible_native_sends(10)
        self.assertEqual([items[2]["id"]], [row["id"] for row in claimed])

    def test_terminal_or_progressed_items_do_not_block_later_sequences(self):
        first = self.enqueue(delivery_id="first")
        second = self.enqueue(delivery_id="second")
        self.store.update_native_send(first["id"], status="cancelled", expected_status="pending")
        self.assertEqual([second["id"]], [row["id"] for row in self.store.claim_eligible_native_sends(10)])

        self.assertEqual([], self.store.claim_eligible_native_sends(0))
        with self.assertRaises(BridgeError):
            self.store.claim_eligible_native_sends(-1)
        with self.assertRaises(BridgeError):
            self.store.claim_eligible_native_sends(True)

    def test_background_retry_wait_preserves_the_head_and_survives_reopening_the_db(self):
        head = self.enqueue(delivery_id="deferred")
        later = self.enqueue(delivery_id="behind-deferred")
        with patch("agent_bridge.store.time.time", return_value=1_000):
            self.assertEqual(head["id"], self.store.claim_eligible_native_sends(retry_interval=30)[0]["id"])
        self.store.update_native_send(head["id"], status="pending", error="busy", expected_status="sending")
        other = self.enqueue(self.other, delivery_id="other-target")
        reopened = Store(self.root)
        with patch("agent_bridge.store.time.time", return_value=1_029):
            self.assertEqual([other["id"]], [row["id"] for row in reopened.claim_eligible_native_sends(retry_interval=30)])
        self.assertEqual("pending", self.item(later["id"])["status"])
        with patch("agent_bridge.store.time.time", return_value=1_030):
            self.assertEqual([head["id"]], [row["id"] for row in reopened.claim_eligible_native_sends(retry_interval=30)])

    def test_empty_queue_claim_is_read_only_during_a_concurrent_writer(self):
        connection = self.store.connection
        statements = []
        with connection(timeout=0) as reader:
            reader.set_trace_callback(statements.append)
            with connection(write=True):
                with patch.object(self.store, "connection", side_effect=lambda: connection(reuse=reader)):
                    self.assertEqual([], self.store.claim_eligible_native_sends(retry_interval=30))
        self.assertFalse(any(sql.startswith("BEGIN IMMEDIATE") for sql in statements))

    def test_unrelated_target_is_not_blocked_by_another_targets_uncertain_head(self):
        blocked_head = self.enqueue(delivery_id="a-1")
        blocked_next = self.enqueue(delivery_id="a-2")
        free_head = self.enqueue(self.other, delivery_id="b-1")
        free_next = self.enqueue(self.other, delivery_id="b-2")

        claimed = self.store.claim_eligible_native_sends(10)
        self.assertEqual({blocked_head["id"], free_head["id"]}, {row["id"] for row in claimed})
        self.store.update_native_send(blocked_head["id"], status="uncertain", expected_status="sending")
        self.store.update_native_send(free_head["id"], status="received", expected_status="sending")

        claimed = self.store.claim_eligible_native_sends(10)
        self.assertEqual([free_next["id"]], [row["id"] for row in claimed])
        self.assertEqual("pending", self.item(blocked_next["id"])["status"])

    def test_fair_dispatch_claims_one_item_per_target_per_round(self):
        targets = [("codex", str(uuid.uuid4())) for _ in range(3)]
        for target in targets:
            for index in range(3):
                self.enqueue(target, delivery_id=f"{target[1]}-{index}")

        claimed_items = []
        while True:
            claimed = self.store.claim_eligible_native_sends(2)
            if not claimed:
                break
            self.assertLessEqual(len(claimed), 2)
            # Fairness: each round never claims two items for the same physical target.
            self.assertEqual(len(claimed), len({(row["runtime"], row["native_id"]) for row in claimed}))
            for row in claimed:
                claimed_items.append(row)
                self.store.update_native_send(row["id"], status="received", expected_status="sending")

        self.assertEqual(9, len(claimed_items))
        for target in targets:
            seqs = [row["local_seq"] for row in claimed_items if row["native_id"] == target[1]]
            self.assertEqual([1, 2, 3], seqs)

    def test_update_synchronizes_the_legacy_broadcast_delivery(self):
        member = {"runtime": self.target[0], "native_id": self.target[1], "cwd": str(self.root), "label": ""}
        groups.control_group(self.store, "group", "create", 0, "create", native_members=[member])
        recipient = groups.broadcast(self.store, "group", "hello", 0, "broadcast")["recipients"][0]

        queued = self.store.get_native_send_by_delivery("local_broadcast", recipient["delivery_id"])
        self.assertIsNotNone(queued)
        self.assertEqual((queued["id"], 1, "pending"), (recipient["delivery_id"], queued["local_seq"], queued["status"]))

        updated = self.store.update_native_send(
            queued["id"], status="enqueued", receipt="native-receipt",
            processing_status="running", expected_status="pending",
        )
        self.assertEqual(("enqueued", "native-receipt"), (updated["status"], updated["receipt"]))
        with self.store.connection() as db:
            legacy = dict(db.execute(
                "SELECT * FROM native_broadcast_deliveries WHERE id=?", (recipient["delivery_id"],)
            ).fetchone())
        self.assertEqual(("enqueued", "native-receipt", "running"), (
            legacy["status"], legacy["receipt"], legacy["processing_status"],
        ))

        # A stale caller cannot overwrite the newer state.
        stale = self.store.update_native_send(queued["id"], status="failed", expected_status="pending")
        self.assertEqual("enqueued", stale["status"])

    def test_migration_imports_legacy_rows_once_in_per_target_insertion_order(self):
        member = {"runtime": self.target[0], "native_id": self.target[1], "cwd": str(self.root), "label": ""}
        groups.control_group(self.store, "group", "create", 0, "create", native_members=[member])
        now = time.time()
        with self.store.connection(write=True) as db:
            rows = [
                ("legacy-a1", self.target, "pending", None, None),
                ("legacy-b1", self.other, "received", "receipt-b", "7"),
                ("legacy-a2", self.target, "sending", None, None),
                ("legacy-a3", self.target, "enqueued", "receipt-a", None),
            ]
            for index, (delivery_id, target, status, receipt, turn_id) in enumerate(rows):
                db.execute(
                    "INSERT INTO native_broadcast_deliveries("
                    "id,broadcast_id,group_id,runtime,native_id,cwd,message,status,receipt,native_turn_id,"
                    "processing_status,created,last_attempt) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (delivery_id, f"broadcast-{index}", "group", target[0], target[1], str(self.root), "hello",
                     status, receipt, turn_id, "running" if turn_id else None, now + index, 0),
                )

        upgraded = Store(self.root)
        first_target = [
            row for row in (upgraded.get_native_send_queue_item(delivery_id) for delivery_id in
                            ("legacy-a1", "legacy-a2", "legacy-a3"))
        ]
        self.assertEqual([1, 2, 3], [row["local_seq"] for row in first_target])
        self.assertEqual(["pending", "uncertain", "enqueued"], [row["status"] for row in first_target])
        other_target = upgraded.get_native_send_queue_item("legacy-b1")
        self.assertEqual((1, "received"), (other_target["local_seq"], other_target["status"]))
        self.assertEqual(
            "native_broadcast",
            upgraded.get_native_send_by_delivery("native_broadcast", "legacy-a1")["delivery_source"],
        )
        upgraded.update_native_send(first_target[1]["id"], status="received", expected_status="uncertain")
        with upgraded.connection() as db:
            self.assertEqual(
                "received",
                db.execute("SELECT status FROM native_broadcast_deliveries WHERE id='legacy-a2'").fetchone()[0],
            )
        self.assertEqual(4, upgraded.allocate_native_local_seq(*self.target))
        self.assertEqual(2, upgraded.allocate_native_local_seq(*self.other))

        # Reopening must not duplicate rows or move the counters backwards.
        again = Store(self.root)
        with again.connection() as db:
            self.assertEqual(4, db.execute("SELECT COUNT(*) FROM native_send_queue").fetchone()[0])
            self.assertEqual(
                5, db.execute(
                    "SELECT next_seq FROM native_target_counters WHERE runtime=? AND native_id=?", self.target
                ).fetchone()[0],
            )

    def test_broker_restart_resets_inflight_sending_and_running_to_uncertain(self):
        (self.root / "config.json").write_text(json.dumps({"profiles": {}, "telegram_database": None}))
        sending = self.enqueue(delivery_id="sending")
        running = self.enqueue(self.other, delivery_id="running")
        claimed = self.store.claim_eligible_native_sends(10)
        self.assertEqual({sending["id"], running["id"]}, {row["id"] for row in claimed})
        self.store.update_native_send(
            running["id"], status="received", receipt="receipt", native_turn_id="7",
            processing_status="running", expected_status="sending",
        )

        Broker(self.root)

        sending = self.item(sending["id"])
        self.assertEqual("uncertain", sending["status"])
        self.assertIn("restarted", sending["error"])
        running = self.item(running["id"])
        self.assertEqual("received", running["status"])
        self.assertEqual("uncertain", running["processing_status"])
        self.assertIn("execution outcome is unknown", running["error"])

        # An uncertain head must keep blocking later writes after the restart.
        self.enqueue(delivery_id="later")
        self.assertEqual([], self.store.claim_eligible_native_sends(10))


class NativeWakePlatformTests(unittest.IsolatedAsyncioTestCase):
    async def test_macos_automatic_wake_fails_before_starting_a_helper(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            queued = store.enqueue_native_send(
                "antigravity", "native-session", "local_broadcast", "macos-native-delivery", "fixture",
            )
            with (
                patch("agent_bridge.agy_presence.sys.platform", "darwin"),
                patch("agent_bridge.native_broadcast.asyncio.create_subprocess_exec", new_callable=AsyncMock) as spawn,
            ):
                await deliver_broadcasts(store, Config({}))
                result = store.get_native_send_queue_item(queued["id"])
                await deliver_broadcasts(store, Config({}))
                self.assertEqual(result, store.get_native_send_queue_item(queued["id"]))
        spawn.assert_not_awaited()
        self.assertEqual("failed", result["status"])
        self.assertIn("unsupported on macOS", result["error"])
        self.assertIsNone(result["receipt"])


class NotificationOrderingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.profile = Profile("test", "codex", "fixed")
        self.config = Config({"test": self.profile}, codex_async_python="configured-test-helper")
        self.store.parent_control("parent", "parent_register", 0, "register", "native-parent", str(self.root))
        self.sid = self.store.open(self.profile, str(self.root), "read-only", "open", "parent")["session_id"]
        self.store.worker_ready(self.sid, "g", 1, "start", "native-child")
        task_id = self.store.dispatch(self.sid, "test", "dispatch")["task_id"]
        self.store.claim(self.config)
        attempt = self.store.begin_attempt(self.sid, "g")["active_attempt"]
        self.store.finish(self.sid, "g", task_id, attempt, "completed", {"answer": "done"})

    async def test_parent_notification_waits_behind_pending_native_head(self):
        blocker = self.store.enqueue_native_send(
            "codex", "native-parent", "local_broadcast", "blocker", "blocker",
        )
        claimed = self.store.claim_eligible_native_sends(10, delivery_sources=("local_broadcast",))
        self.assertEqual([blocker["id"]], [row["id"] for row in claimed])
        self.store.update_native_send(blocker["id"], status="uncertain", expected_status="sending")

        calls = []

        async def sender(config, parent, message, event_seq, receipt=None):
            calls.append(event_seq)
            return {"receipt": "submission", "ready": True, "native_status": "received", "native_turn_id": "turn-1"}

        await deliver(self.store, self.config, sender)
        delivery = self.store.get_parent("parent")["deliveries"][0]
        self.assertEqual([], calls)
        self.assertEqual("pending", delivery["status"])
        queued = self.store.get_native_send_by_delivery("parent_notification", str(delivery["event_seq"]))
        self.assertEqual("pending", queued["status"])

        self.store.update_native_send(blocker["id"], status="received", expected_status="uncertain")
        await deliver(self.store, self.config, sender)
        self.assertEqual([delivery["event_seq"]], calls)
        self.assertEqual("accepted", self.store.get_parent("parent")["deliveries"][0]["status"])
        queued = self.store.get_native_send_by_delivery("parent_notification", str(delivery["event_seq"]))
        self.assertEqual("received", queued["status"])


if __name__ == "__main__":
    unittest.main()
