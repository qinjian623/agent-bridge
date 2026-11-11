import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agent_bridge import groups, native_broadcast
from agent_bridge.broker import DeliveryScheduler
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import MAX_FRAME_BYTES, DeliverySource, PeerErrorCode
from agent_bridge.peer_transport import DirectRelayServer
from agent_bridge.store import Store


class ServiceClient:
    def __init__(self, service):
        self.service = service
        self.deliver_calls = []

    async def call(self, method, params):
        response = await self.service.handle({"v": 1, "id": "test", "method": method, "params": params})
        if not response["ok"]:
            raise BridgeError(response["error"]["code"], response["error"]["message"])
        return response["result"]

    async def deliver(self, params):
        self.deliver_calls.append(params)
        return await self.call("peer.deliver", params)

    async def lookup_delivery(self, params):
        return await self.call("peer.lookup_delivery", params)

    def client(self, host_id):
        return self


class PeerReviewFixTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = Store(self.root / "remote")
        self.identity = FileNodeIdentityStore(self.store.state)
        self.node = self.identity.load_or_create()
        self.config = Config({"worker": Profile("worker", "codex", "test", executable=sys.executable)})
        self.service = DirectPeerService(self.identity, host_id="remote", store=self.store, config=self.config)
        self.client = ServiceClient(self.service)

    async def wire_page(self, after=0, limit=100):
        source = io.BytesIO((json.dumps({
            "v": 1, "id": "unicode-请求", "method": "peer.pull_requests", "params": {"after": after, "limit": limit},
        }) + "\n").encode())
        sink = io.BytesIO()
        await DirectRelayServer(self.service).serve(source, sink)
        wire = sink.getvalue()
        self.assertLessEqual(len(wire), MAX_FRAME_BYTES)
        return json.loads(wire)

    async def test_byte_pagination_preserves_all_requests_and_can_replay_the_page(self):
        for index in range(6):
            self.store.add_outbox(f"op-{index}", "agent_broadcast", {"message": "汉字\n" * 40000})
        first = await self.wire_page()
        self.assertTrue(first["ok"], first)
        self.assertTrue(first["result"]["has_more"])
        replay = await self.wire_page()
        self.assertEqual(first["result"], replay["result"])
        items, after = [], 0
        while True:
            page = (await self.wire_page(after))["result"]
            items.extend(page["items"])
            if not page["has_more"]:
                break
            self.assertGreater(page["cursor"], after)
            after = page["cursor"]
        self.assertEqual([f"op-{index}" for index in range(6)], [item["operation_id"] for item in items])

    async def test_single_oversized_request_is_rejected_before_enqueue(self):
        with self.assertRaises(BridgeError) as caught:
            self.store.add_outbox("oversized", "agent_broadcast", {"message": "\x00" * 200000})
        self.assertEqual(PeerErrorCode.FRAME_TOO_LARGE, caught.exception.code)
        self.assertIsNone(self.store.get_node_outbox("oversized"))

    async def test_historical_oversized_request_is_identified_without_acknowledgement(self):
        self.store.add_outbox("historical", "agent_broadcast", {"message": "small"})
        with self.store.connection(write=True) as db:
            db.execute("UPDATE node_outbox SET payload=?", (json.dumps({"message": "a" * MAX_FRAME_BYTES}),))
        response = await self.wire_page()
        self.assertFalse(response["ok"])
        self.assertEqual(PeerErrorCode.FRAME_TOO_LARGE, response["error"]["code"])
        self.assertIn("historical", response["error"]["message"])
        self.assertIsNone(self.store.get_node_outbox("historical")["acked_at"])

    async def test_remote_agy_read_only_is_rejected_without_creating_work(self):
        self.config.profiles["agy"] = Profile("agy", "antigravity", "gemini-test-medium", provider="google-antigravity")
        response = await self.service.handle({"v": 1, "id": "bad-access", "method": "peer.deliver", "params": {
            "operation": {"node_id": self.node.node_id, "state_generation": self.node.state_generation, "operation_id": "agy"},
            "delivery_id": "bad-agy", "target": {"kind": "bridge", "id": ""}, "target_seq": 1,
            "message": "do work", "open": {"profile": "agy", "cwd": str(self.root), "access": "read-only"},
        }})
        self.assertFalse(response["ok"])
        self.assertEqual("unsupported_access", response["error"]["code"])
        with self.store.connection() as db:
            for table in ("sessions", "tasks", "peer_operations", "peer_inbox"):
                self.assertEqual(0, db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def native_group(self, runtime="codex"):
        coordinator = Store(self.root / "coordinator")
        groups.control_group(coordinator, "team", "create", 0, "create", remote_members=[{
            "host_id": "remote", "node_id": self.node.node_id, "state_generation": self.node.state_generation,
            "target_kind": "native", "runtime": runtime, "native_id": "original-native", "cwd": str(self.root),
        }])
        first = groups.broadcast(coordinator, "team", "first", 0, "first")
        second = groups.broadcast(coordinator, "team", "second", 0, "second")
        return coordinator, DeliveryScheduler(coordinator, self.client), first, second

    async def test_real_peer_receipt_releases_the_next_native_message_once(self):
        coordinator, scheduler, first, second = self.native_group()
        await scheduler.step()
        delivery_id = first["recipients"][0]["delivery_id"]
        sender = AsyncMock(return_value={"status": "enqueued", "receipt": "queue-receipt"})
        await native_broadcast.deliver(self.store, self.config, sender=sender)
        self.assertEqual(str(self.root), sender.call_args.args[1]["cwd"])
        self.assertEqual("team", sender.call_args.args[1]["group_id"])
        inspector = AsyncMock(side_effect=[
            TimeoutError("read disconnected"),
            {"status": "enqueued", "receipt": "queue-receipt"},
            {"status": "received", "receipt": "queue-receipt", "turn_id": "matching-turn"},
        ])
        with patch.object(native_broadcast, "send", inspector):
            await scheduler.step()
            await scheduler.step()
            self.assertEqual(1, len(self.client.deliver_calls))
            [released] = await scheduler.step()
        self.assertEqual("released", released["status"])
        self.assertEqual("received", released["send_status"])
        queued = self.store.get_native_send_by_delivery(DeliverySource.PEER_DELIVER, delivery_id)
        self.assertEqual("matching-turn", queued["native_turn_id"])
        sender.assert_awaited_once()
        self.assertTrue(all(call.kwargs["inspect"] for call in inspector.call_args_list))
        await scheduler.step()
        self.assertEqual(2, len(self.client.deliver_calls))
        with coordinator.connection() as db:
            next_status = db.execute("SELECT status FROM peer_outbox WHERE delivery_id=?", (second["recipients"][0]["delivery_id"],)).fetchone()[0]
        self.assertEqual("accepted_remote", next_status)

    async def test_missing_native_receipt_stays_uncertain_without_resending(self):
        coordinator, scheduler, first, _ = self.native_group()
        await scheduler.step()
        await native_broadcast.deliver(self.store, self.config, sender=AsyncMock(return_value={"status": "enqueued", "receipt": "queue"}))
        with patch.object(native_broadcast, "send", AsyncMock(return_value={"status": "unconfirmed", "error": "not found in history"})):
            [unknown] = await scheduler.step()
        self.assertEqual("uncertain", unknown["status"])
        self.assertEqual([], await scheduler.step())
        self.assertEqual(1, len(self.client.deliver_calls))
        queue = self.store.get_native_send_by_delivery(DeliverySource.PEER_DELIVER, first["recipients"][0]["delivery_id"])
        self.assertEqual("uncertain", queue["status"])
        with coordinator.connection() as db:
            self.assertEqual(0, db.execute("SELECT released_seq FROM target_streams").fetchone()[0])

    async def test_missing_queue_record_is_unknown_instead_of_stale_pending(self):
        _, scheduler, _, _ = self.native_group()
        await scheduler.step()
        with self.store.connection(write=True) as db:
            db.execute("DELETE FROM native_send_queue")
        [unknown] = await scheduler.step()
        self.assertEqual("uncertain", unknown["status"])
        self.assertIn("missing", unknown["error"])
        self.assertEqual([], await scheduler.step())
        self.assertEqual(1, len(self.client.deliver_calls))

    async def test_peer_agy_retains_original_directory_and_receipt(self):
        _, scheduler, first, _ = self.native_group("antigravity")
        await scheduler.step()
        async def wake(store, config, row, *, queue_id):
            self.assertEqual(str(self.root), row["cwd"])
            self.assertEqual("team", row["group_id"])
            return {"status": "received", "receipt": row["id"], "turn_id": "7", "processing_status": "failed"}
        with patch.object(native_broadcast, "send_agy", wake):
            await native_broadcast.deliver(self.store, self.config)
        lookup = await self.client.lookup_delivery({"delivery_id": first["recipients"][0]["delivery_id"]})
        self.assertEqual("received", lookup["delivery"]["send_status"])
        self.assertEqual("failed", lookup["delivery"]["processing_status"])
        [released] = await scheduler.step()
        self.assertEqual("released", released["status"])


if __name__ == "__main__":
    unittest.main()
