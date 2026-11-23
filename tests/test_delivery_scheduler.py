import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_bridge import groups
from agent_bridge.broker import Broker, DeliveryScheduler
from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import PeerErrorCode
from agent_bridge.store import Store

REMOTE_BRIDGE = {
    "host_id": "gpu-a", "node_id": "node-a", "state_generation": "gen-a",
    "target_kind": "bridge", "session_id": "remote-a",
}
REMOTE_BRIDGE_B = {**REMOTE_BRIDGE, "session_id": "remote-b"}
REMOTE_NATIVE = {
    "host_id": "gpu-a", "node_id": "node-a", "state_generation": "gen-a",
    "target_kind": "native", "runtime": "codex", "native_id": "native-a",
}


class FakePeerClient:
    def __init__(self):
        self.deliveries = []
        self.deliver_calls = []
        self.lookups = []
        self.lookup_calls = []

    async def deliver(self, params):
        self.deliver_calls.append(params)
        outcome = self.deliveries.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def lookup_delivery(self, params):
        self.lookup_calls.append(params)
        if self.lookups:
            outcome = self.lookups.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        delivered = next(row for row in self.deliver_calls if row["delivery_id"] == params["delivery_id"])
        return {"found": True, "delivery": {
            **delivered["operation"], "delivery_id": delivered["delivery_id"],
            "target_seq": delivered["target_seq"], "target": delivered["target"],
            "send_status": "pending", "processing_status": None, "receipt": None,
        }}


class FakePeerFactory:
    def __init__(self, client):
        self._client = client

    def client(self, host_id):
        return self._client


class DeliverySchedulerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = Store(self.state)
        self.client = FakePeerClient()
        self.scheduler = DeliveryScheduler(self.store, FakePeerFactory(self.client))

    def create(self, members, name="team"):
        return groups.control_group(self.store, name, "create", 0, "create-" + name, remote_members=members)

    def broadcast(self, message, request_id, name="team", revision=0):
        return groups.broadcast(self.store, name, message, revision, request_id)

    def outbox(self, delivery_id):
        with self.store.connection() as db:
            return dict(db.execute(
                "SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)
            ).fetchone())

    def stream(self, stream_id):
        with self.store.connection() as db:
            return dict(db.execute(
                "SELECT * FROM target_streams WHERE stream_id=?", (stream_id,)
            ).fetchone())

    def recipient(self, broadcast_id):
        with self.store.connection() as db:
            return dict(db.execute(
                "SELECT * FROM broadcast_recipients WHERE broadcast_id=?", (broadcast_id,)
            ).fetchone())

    def native_lookup(self, sent, send_status="received", **changes):
        outbox = self.outbox(sent["recipients"][0]["delivery_id"])
        return {"found": True, "delivery": {
            **{key: outbox[key] for key in ("delivery_id", "node_id", "state_generation", "operation_id", "target_seq")},
            "target": {"kind": "native", "id": outbox["target_id"], "runtime": outbox["runtime"]},
            "send_status": send_status, "processing_status": "running", "receipt": "native-receipt",
            "native_turn_id": "native-turn", **changes,
        }}

    async def test_bridge_ack_releases_the_stream_and_updates_the_recipient(self):
        self.create([REMOTE_BRIDGE])
        sent = self.broadcast("hello", "send")
        self.client.deliveries.append({"status": "accepted_remote", "task_id": "task-1"})

        [result] = await self.scheduler.step()

        self.assertEqual("released", result["status"])
        self.assertEqual(2, result["revision"])
        row = self.outbox(result["delivery_id"])
        self.assertEqual("released", row["status"])
        self.assertEqual(1, self.stream(row["stream_id"])["released_seq"])
        self.assertEqual("released", self.recipient(sent["broadcast_id"])["status"])
        params = self.client.deliver_calls[0]
        self.assertEqual("message", params["seq_kind"])
        self.assertEqual("hello", params["message"])
        self.assertEqual(1, params["target_seq"])
        self.assertEqual("ordered", params["ordering_mode"])
        self.assertEqual("bridge", params["target"]["kind"])
        self.assertEqual("remote-a", params["target"]["id"])
        self.assertEqual("gen-a", params["operation"]["state_generation"])

    async def test_later_sequences_wait_for_the_released_head(self):
        self.create([REMOTE_BRIDGE])
        first = self.broadcast("one", "send-1")
        second = self.broadcast("two", "send-2")
        self.client.deliveries.extend([
            {"status": "accepted_remote"}, {"status": "accepted_remote"},
        ])

        await self.scheduler.step()
        self.assertEqual(1, len(self.client.deliver_calls))
        self.assertEqual("pending", self.outbox(second["recipients"][0]["delivery_id"])["status"])
        self.assertEqual("released", self.outbox(first["recipients"][0]["delivery_id"])["status"])

        await self.scheduler.step()
        self.assertEqual(2, len(self.client.deliver_calls))
        self.assertEqual(2, self.client.deliver_calls[1]["target_seq"])
        self.assertEqual("released", self.outbox(second["recipients"][0]["delivery_id"])["status"])

    async def test_native_ack_does_not_release_the_network_stream(self):
        self.create([REMOTE_NATIVE])
        first = self.broadcast("one", "send-1")
        second = self.broadcast("two", "send-2")
        self.client.deliveries.extend([
            {"status": "accepted_remote", "send_status": "pending"},
            {"status": "accepted_remote", "send_status": "pending"},
        ])

        [result] = await self.scheduler.step()
        self.assertEqual("accepted_remote", result["status"])
        self.assertEqual("pending", result["send_status"])
        self.assertEqual("accepted_remote", self.recipient(first["broadcast_id"])["status"])
        self.assertEqual(0, self.stream(result["stream_id"])["released_seq"])
        self.assertEqual([], await self.scheduler.step())
        self.assertEqual(1, len(self.client.deliver_calls))
        self.assertEqual("pending", self.outbox(second["recipients"][0]["delivery_id"])["status"])

    async def test_unchanged_pending_lookup_preserves_the_acceptance_receipt_without_writing(self):
        self.create([REMOTE_NATIVE])
        self.broadcast("one", "send-1")
        receipt = {"accepted_at": 12.5}
        self.client.deliveries.append({"status": "accepted_remote", "send_status": "pending", "receipt": receipt})
        [accepted] = await self.scheduler.step()

        with patch.object(self.scheduler, "_finish", wraps=self.scheduler._finish) as finish:
            self.assertEqual([], await self.scheduler.step())
            self.assertEqual([], await self.scheduler.step())
        finish.assert_not_called()
        stored = self.outbox(accepted["delivery_id"])
        self.assertEqual(receipt, json.loads(stored["receipt"]))
        self.assertEqual(accepted["revision"], stored["revision"])
        self.assertEqual(2, len(self.client.lookup_calls))

    async def test_automatic_deferred_receipts_back_off_to_a_cap_and_never_resubmit(self):
        self.create([REMOTE_NATIVE])
        first = self.broadcast("one", "send-1")
        second = self.broadcast("two", "send-2")
        self.client.deliveries.extend([{"status": "accepted_remote", "send_status": "pending"}] * 2)
        first_id = first["recipients"][0]["delivery_id"]
        second_id = second["recipients"][0]["delivery_id"]

        now = 0.0
        with patch("agent_bridge.broker.monotonic", side_effect=lambda: now):
            await self.scheduler.step(automatic=True)
            due_times = (0.5, 1.5, 3.5, 7.5, 15.5, 31.5, 61.5, 91.5)
            for index, due in enumerate(due_times):
                if index:
                    now = due - 0.25
                    self.assertEqual([], await self.scheduler.step(automatic=True))
                    self.assertEqual(index, len(self.client.lookup_calls))
                self.client.lookups.append(self.native_lookup(
                    first, "sending" if index % 2 else "pending",
                    processing_status=None, receipt=None, native_turn_id=None,
                ))
                now = due
                await self.scheduler.step(automatic=True)
                self.assertEqual(index + 1, len(self.client.lookup_calls))
                self.assertEqual("pending", self.outbox(second_id)["status"])
                self.assertEqual(1, len(self.client.deliver_calls))
            self.client.lookups.append(self.native_lookup(first))
            now += 30
            [released] = await self.scheduler.step(automatic=True)
            self.assertEqual("released", released["status"])
            self.assertNotIn(first_id, self.scheduler._receipt_polls)
            [next_delivery] = await self.scheduler.step(automatic=True)
            self.assertEqual(second_id, next_delivery["delivery_id"])
        self.assertEqual([first_id, second_id], [call["delivery_id"] for call in self.client.deliver_calls])

    async def test_automatic_read_errors_back_off_but_step_and_pump_bypass_the_delay(self):
        self.create([REMOTE_NATIVE])
        first = self.broadcast("one", "send-1")
        self.client.deliveries.append({"status": "accepted_remote", "send_status": "pending"})
        enqueued_lookup = self.native_lookup(first, "enqueued", native_turn_id=None)
        self.client.lookups.extend([
            ConnectionError("first read failed"), ConnectionError("different read error"),
            enqueued_lookup, enqueued_lookup, self.native_lookup(first),
        ])
        now = 0.0
        with patch("agent_bridge.broker.monotonic", side_effect=lambda: now):
            await self.scheduler.step(automatic=True)
            now = 0.5
            await self.scheduler.step(automatic=True)
            now = 1.5
            await self.scheduler.step(automatic=True)
            now = 2.0
            self.assertEqual([], await self.scheduler.step(automatic=True))
            self.assertEqual(2, len(self.client.lookup_calls))
            [enqueued] = await self.scheduler.step()
            self.assertEqual("enqueued", enqueued["send_status"])
            # A changed local revision bypasses the prior automatic deadline too.
            self.assertEqual([], await self.scheduler.step(automatic=True))
            self.assertEqual(4, len(self.client.lookup_calls))
            [released] = await self.scheduler.pump()
            self.assertEqual("released", released["status"])
        self.assertEqual(5, len(self.client.lookup_calls))
        self.assertEqual(1, len(self.client.deliver_calls))

    async def test_automatic_enqueued_receipts_are_checked_quickly_after_new_sends(self):
        self.create([REMOTE_NATIVE])
        self.create([REMOTE_BRIDGE], name="other")
        first = self.broadcast("one", "send-1")
        self.client.deliveries.extend([
            {"status": "accepted_remote", "send_status": "pending"}, {"status": "accepted_remote"},
        ])
        enqueued = self.native_lookup(first, "enqueued", native_turn_id=None)
        self.client.lookups.extend([enqueued] * 5)
        now = 0.0
        with patch("agent_bridge.broker.monotonic", side_effect=lambda: now):
            await self.scheduler.step(automatic=True)
            now = 0.5
            await self.scheduler.step(automatic=True)
            self.broadcast("other stream", "different", name="other")
            now = 1.0
            lookup = self.client.lookup_delivery
            heads = self.scheduler.heads

            def native_first(*args, **kwargs):
                return sorted(heads(*args, **kwargs), key=lambda head: head["target_kind"] != "native")

            async def lookup_after_send(params):
                self.assertEqual(2, len(self.client.deliver_calls))
                return await lookup(params)

            with (
                patch.object(self.scheduler, "heads", native_first),
                patch.object(self.client, "lookup_delivery", lookup_after_send),
            ):
                await self.scheduler.step(automatic=True)
            self.assertEqual(2, len(self.client.lookup_calls))
            for index, due in enumerate((2.0, 4.0, 6.0), start=2):
                now = due - 0.25
                self.assertEqual([], await self.scheduler.step(automatic=True))
                self.assertEqual(index, len(self.client.lookup_calls))
                now = due
                self.assertEqual([], await self.scheduler.step(automatic=True))
                self.assertEqual(index + 1, len(self.client.lookup_calls))

    async def test_native_receipt_releases_once_and_processing_failure_does_not_block(self):
        self.create([REMOTE_NATIVE])
        first = self.broadcast("one", "send-1")
        second = self.broadcast("two", "send-2")
        self.client.deliveries.extend([
            {"status": "accepted_remote", "send_status": "pending"},
            {"status": "accepted_remote", "send_status": "pending"},
        ])
        await self.scheduler.step()
        self.client.lookups.append(self.native_lookup(first, processing_status="failed"))
        [released] = await self.scheduler.step()
        self.assertEqual("released", released["status"])
        self.assertEqual("received", released["send_status"])
        self.assertEqual("failed", released["processing_status"])
        self.assertEqual(1, self.stream(released["stream_id"])["released_seq"])
        self.assertEqual("released", self.recipient(first["broadcast_id"])["status"])
        await self.scheduler.step()
        await self.scheduler.step()
        self.assertEqual(2, len(self.client.deliver_calls))
        self.assertEqual("accepted_remote", self.outbox(second["recipients"][0]["delivery_id"])["status"])
        self.assertEqual([first["recipients"][0]["delivery_id"], second["recipients"][0]["delivery_id"]],
                         [call["delivery_id"] for call in self.client.deliver_calls])

    async def test_native_enqueued_is_not_receipt_and_pump_stops(self):
        self.create([REMOTE_NATIVE])
        first = self.broadcast("one", "send-1")
        self.broadcast("two", "send-2")
        self.client.deliveries.append({"status": "accepted_remote", "send_status": "pending"})
        await self.scheduler.step()
        self.client.lookups.append(self.native_lookup(first, "enqueued", native_turn_id=None))
        rows = await asyncio.wait_for(self.scheduler.pump(), 2)
        self.assertEqual("accepted_remote", rows[0]["status"])
        self.assertEqual("enqueued", rows[0]["send_status"])
        self.assertEqual(0, self.stream(rows[0]["stream_id"])["released_seq"])
        self.assertEqual(1, len(self.client.deliver_calls))

    async def test_native_lookup_disconnect_can_recover_without_resubmitting(self):
        self.create([REMOTE_NATIVE])
        first = self.broadcast("one", "send-1")
        self.client.deliveries.append({"status": "accepted_remote", "send_status": "pending"})
        await self.scheduler.step()
        self.client.lookups.extend([ConnectionError("lost reply"), self.native_lookup(first)])
        [waiting] = await self.scheduler.step()
        self.assertEqual("accepted_remote", waiting["status"])
        self.assertIn("lost reply", waiting["error"])
        [released] = await self.scheduler.step()
        self.assertEqual("released", released["status"])
        self.assertEqual(1, len(self.client.deliver_calls))

    async def test_native_unknown_failed_and_invalid_receipts_block_without_resending(self):
        cases = [
            ("absent", {"found": False}),
            ("no delivery", {"found": True}),
            ("uncertain", {"send_status": "uncertain"}),
            ("failed", {"send_status": "failed", "error": "native preflight failed"}),
            ("no receipt", {"receipt": None}),
            ("no turn", {"native_turn_id": None}),
            ("wrong delivery", {"delivery_id": "other"}),
            ("wrong node", {"node_id": "other"}),
            ("wrong generation", {"state_generation": "other"}),
            ("wrong operation", {"operation_id": "other"}),
            ("wrong sequence", {"target_seq": 2}),
            ("boolean sequence", {"target_seq": True}),
            ("wrong target", {"target": {"kind": "native", "runtime": "codex", "id": "other"}}),
        ]
        self.create([REMOTE_NATIVE])
        for index, (label, changes) in enumerate(cases):
            with self.subTest(label=label):
                # Separate streams allow each invalid fact to be tested without bypassing an uncertain head.
                native = {**REMOTE_NATIVE, "native_id": "native-" + str(index)}
                group_id = "invalid-" + str(index)
                self.create([native], group_id)
                first = self.broadcast("one", "send-1-" + str(index), group_id)
                second = self.broadcast("two", "send-2-" + str(index), group_id)
                self.client.deliveries.append({"status": "accepted_remote", "send_status": "pending"})
                await self.scheduler.step()
                lookup = changes if "found" in changes else self.native_lookup(first, **changes)
                self.client.lookups.append(lookup)
                [uncertain] = await self.scheduler.step()
                self.assertEqual("uncertain", uncertain["status"])
                self.assertEqual(0, self.stream(uncertain["stream_id"])["released_seq"])
                self.assertEqual("pending", self.outbox(second["recipients"][0]["delivery_id"])["status"])
                self.assertEqual([], await self.scheduler.step())
        self.assertEqual(len(cases), len(self.client.deliver_calls))

    async def test_native_lookup_cannot_overwrite_a_concurrent_resolution(self):
        self.create([REMOTE_NATIVE])
        first = self.broadcast("one", "send-1")
        self.client.deliveries.append({"status": "accepted_remote", "send_status": "pending"})
        await self.scheduler.step()
        delivery_id = first["recipients"][0]["delivery_id"]
        async def resolve_during_lookup(params):
            with self.store.connection(write=True) as db:
                db.execute("UPDATE peer_outbox SET status='uncertain',revision=revision+1 WHERE delivery_id=?", (delivery_id,))
            return self.native_lookup(first)
        self.client.lookup_delivery = resolve_during_lookup
        await self.scheduler.step()
        self.assertEqual("uncertain", self.outbox(delivery_id)["status"])
        self.assertEqual(0, self.stream(self.outbox(delivery_id)["stream_id"])["released_seq"])
        self.assertEqual(1, len(self.client.deliver_calls))

    async def test_offline_and_uncertain_heads_block_without_skipping(self):
        self.create([REMOTE_BRIDGE])
        first = self.broadcast("one", "send-1")
        self.broadcast("two", "send-2")
        self.client.deliveries.append(BridgeError(PeerErrorCode.HOST_OFFLINE, "ssh down"))

        [result] = await self.scheduler.step()
        self.assertEqual("waiting_for_host", result["status"])
        self.assertEqual([], await self.scheduler.step())
        self.assertEqual(1, len(self.client.deliver_calls))

        # A new pending sequence cannot overtake the waiting head.
        self.assertEqual("waiting_for_host", self.outbox(first["recipients"][0]["delivery_id"])["status"])
        self.assertEqual("pending", self.outbox(self.broadcast("three", "send-3")["recipients"][0]["delivery_id"])["status"])

    async def test_connection_loss_marks_uncertain_and_never_retries_automatically(self):
        self.create([REMOTE_BRIDGE])
        sent = self.broadcast("one", "send-1")
        self.client.deliveries.append(ConnectionError("connection reset"))

        [result] = await self.scheduler.step()
        self.assertEqual("uncertain", result["status"])
        self.assertEqual(1, len(self.client.deliver_calls))
        self.assertEqual([], await self.scheduler.step())
        self.assertEqual(1, len(self.client.deliver_calls))
        self.assertEqual("uncertain", self.recipient(sent["broadcast_id"])["status"])

    async def test_pending_message_becomes_a_tombstone_without_its_body(self):
        self.create([REMOTE_BRIDGE])
        sent = self.broadcast("secret message", "send")
        delivery_id = sent["recipients"][0]["delivery_id"]
        original_operation = self.outbox(delivery_id)["operation_id"]

        pending = self.scheduler.cancel(delivery_id)
        self.assertEqual("tombstone_pending", pending["status"])
        self.assertEqual("tombstone", pending["seq_kind"])
        # The tombstone is a distinct operation so it cannot be mistaken for the original message write.
        self.assertNotEqual(original_operation, self.outbox(delivery_id)["operation_id"])
        self.assertEqual("tombstone_pending", self.recipient(sent["broadcast_id"])["status"])

        self.client.deliveries.append({"status": "cancelled"})
        [result] = await self.scheduler.step()
        self.assertEqual("cancelled", result["status"])
        params = self.client.deliver_calls[0]
        self.assertEqual("tombstone", params["seq_kind"])
        self.assertNotIn("message", params)
        self.assertEqual(1, params["target_seq"])
        self.assertEqual(1, self.stream(result["stream_id"])["released_seq"])
        self.assertEqual("cancelled", self.recipient(sent["broadcast_id"])["status"])

    async def test_cancel_refuses_an_in_flight_or_uncertain_head(self):
        self.create([REMOTE_BRIDGE])
        sent = self.broadcast("one", "send")
        delivery_id = sent["recipients"][0]["delivery_id"]
        self.client.deliveries.append(ConnectionError("lost"))
        await self.scheduler.step()

        with self.assertRaisesRegex(BridgeError, "prove its outcome"):
            self.scheduler.cancel(delivery_id)

    async def test_different_targets_progress_independently(self):
        self.create([REMOTE_BRIDGE, REMOTE_BRIDGE_B])
        self.broadcast("fanout", "send")
        self.client.deliveries.extend([
            {"status": "accepted_remote"}, {"status": "accepted_remote"},
        ])

        results = await self.scheduler.step()

        self.assertEqual(2, len(results))
        self.assertEqual(2, len(self.client.deliver_calls))
        # Sequences are per target stream, so each target starts its own stream at 1.
        self.assertEqual({1}, {row["target_seq"] for row in results})
        self.assertEqual(2, len({row["stream_id"] for row in results}))
        self.assertTrue(all(row["status"] == "released" for row in results))

    async def test_pump_drains_sequential_bridge_messages(self):
        self.create([REMOTE_BRIDGE])
        self.broadcast("one", "send-1")
        self.broadcast("two", "send-2")
        self.client.deliveries.extend([
            {"status": "accepted_remote"}, {"status": "accepted_remote"},
        ])

        results = await self.scheduler.pump()

        self.assertEqual([1, 2], [row["target_seq"] for row in results])
        self.assertEqual(2, len(self.client.deliver_calls))


class BrokerStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_broker_restart_turns_in_flight_peer_writes_uncertain(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state = Path(temp.name)
        (state / "config.json").write_text(json.dumps({
            "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
            "worker_launcher": "detached", "telegram_database": None,
        }))
        store = Store(state)
        groups.control_group(
            store, "team", "create", 0, "create",
            remote_members=[REMOTE_BRIDGE],
        )
        sent = groups.broadcast(store, "team", "hello", 0, "send")
        delivery_id = sent["recipients"][0]["delivery_id"]
        with store.connection(write=True) as db:
            db.execute(
                "UPDATE peer_outbox SET status='sending',last_attempt=1 WHERE delivery_id=?",
                (delivery_id,),
            )

        broker = Broker(state)

        with broker.store.connection() as db:
            row = db.execute(
                "SELECT status,error FROM peer_outbox WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
        self.assertEqual("uncertain", row["status"])
        self.assertIn("restarted", row["error"])
        # An uncertain head is not retried by a scheduler pass.
        scheduler = DeliveryScheduler(broker.store, broker.peer_factory)
        self.assertEqual([], await asyncio.wait_for(scheduler.step(), 2))


class BrokerPublicCancelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.state = Path(temp.name)
        (self.state / "config.json").write_text(json.dumps({
            "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
            "worker_launcher": "detached", "telegram_database": None,
        }))
        self.client = FakePeerClient()
        self.broker = Broker(self.state, peer_factory=FakePeerFactory(self.client))

    def create_and_send(self, name="team", request_id="send"):
        groups.control_group(
            self.broker.store, name, "create", 0, "create-" + name,
            remote_members=[REMOTE_BRIDGE],
        )
        sent = groups.broadcast(self.broker.store, name, "queued message", 0, request_id)
        return sent["recipients"][0]["delivery_id"]

    def outbox(self, delivery_id):
        with self.broker.store.connection() as db:
            return dict(db.execute(
                "SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)
            ).fetchone())

    async def test_public_cancel_tombstones_a_pending_group_delivery_and_replays(self):
        delivery_id = self.create_and_send()
        original_operation = self.outbox(delivery_id)["operation_id"]

        first = await self.broker.api_call(
            "agent_cancel", {"delivery_id": delivery_id, "request_id": "cancel"}
        )
        self.assertEqual("tombstone_pending", first["result"]["status"])
        self.assertEqual(delivery_id, first["result"]["delivery_id"])
        self.assertEqual(1, first["result"]["tombstone_seq"])
        self.assertNotEqual(original_operation, self.outbox(delivery_id)["operation_id"])
        self.assertEqual([], self.client.deliver_calls)

        replay = await self.broker.api_call(
            "agent_cancel", {"delivery_id": delivery_id, "request_id": "cancel"}
        )
        self.assertEqual(first, replay)
        self.assertEqual("tombstone_pending", self.outbox(delivery_id)["status"])
        self.assertEqual([], self.client.deliver_calls)

        self.client.deliveries.append({"status": "cancelled"})
        [result] = await self.broker.delivery_scheduler.step()
        self.assertEqual("cancelled", result["status"])
        [params] = self.client.deliver_calls
        self.assertEqual(delivery_id, params["delivery_id"])
        self.assertEqual("tombstone", params["seq_kind"])
        self.assertNotIn("message", params)
        self.assertEqual("cancelled", self.outbox(delivery_id)["status"])

    async def test_public_cancel_keeps_a_pending_delivery_local_when_host_id_is_supplied(self):
        delivery_id = self.create_and_send("team-host", "send-host")
        response = await self.broker.api_call(
            "agent_cancel",
            {"delivery_id": delivery_id, "host_id": "gpu-a", "request_id": "cancel-host"},
        )
        self.assertEqual("tombstone_pending", response["result"]["status"])
        self.assertEqual("tombstone_pending", self.outbox(delivery_id)["status"])
        self.assertEqual([], self.client.deliver_calls)

    async def test_public_cancel_refuses_sending_and_uncertain_local_deliveries(self):
        for status in ("sending", "uncertain"):
            with self.subTest(status=status):
                delivery_id = self.create_and_send(
                    "team-" + status, "send-" + status,
                )
                with self.broker.store.connection(write=True) as db:
                    db.execute(
                        "UPDATE peer_outbox SET status=?,revision=revision+1 WHERE delivery_id=?",
                        (status, delivery_id),
                    )
                response = await self.broker.api_call(
                    "agent_cancel",
                    {"delivery_id": delivery_id, "request_id": "cancel-" + status},
                )
                self.assertEqual("delivery_blocked", response["error"]["code"])
                self.assertEqual(status, self.outbox(delivery_id)["status"])
                self.assertEqual([], self.client.deliver_calls)
                replay = await self.broker.api_call(
                    "agent_cancel",
                    {"delivery_id": delivery_id, "request_id": "cancel-" + status},
                )
                self.assertEqual(response, replay)


if __name__ == "__main__":
    unittest.main()
