import json
import sys
import tempfile
import unittest
from pathlib import Path

from agent_bridge import groups, peer_api
from agent_bridge.broker import Broker
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import PeerErrorCode
from agent_bridge.store import Store

REMOTE = {
    "host_id": "gpu-a", "node_id": "node-a", "state_generation": "gen-a",
    "target_kind": "bridge", "session_id": "remote-a",
}
REMOTE_B = {
    "host_id": "gpu-a", "node_id": "node-a", "state_generation": "gen-a",
    "target_kind": "bridge", "session_id": "remote-b",
}
RECEIVED = {
    "delivery_id": "delivery",
    "status": "received",
    "delivery_revision": 1,
    "receipt": {"received_at": 1.0, "resolution": "received"},
}
CANCELLED = {
    "delivery_id": "delivery",
    "status": "cancelled",
    "delivery_revision": 1,
    "receipt": {"cancelled_at": 1.0, "resolution": "not_received"},
}


class FakeResolutionClient:
    def __init__(self):
        self.calls = []
        self.results = []

    async def resolve_delivery(self, params):
        self.calls.append(params)
        outcome = self.results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeResolutionFactory:
    def __init__(self, client):
        self._client = client

    def client(self, host_id):
        return self._client


class ResolutionCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = Store(self.state)
        self.client = FakeResolutionClient()
        self.factory = FakeResolutionFactory(self.client)
        groups.control_group(
            self.store, "team", "create", 0, "create", remote_members=[REMOTE],
        )
        sent = groups.broadcast(self.store, "team", "hello", 0, "send")
        self.broadcast_id = sent["broadcast_id"]
        self.delivery_id = sent["recipients"][0]["delivery_id"]
        self.stream_id = sent["recipients"][0]["stream_id"]
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE peer_outbox SET status='uncertain',revision=1,error='connection reset' "
                "WHERE delivery_id=?",
                (self.delivery_id,),
            )
            db.execute(
                "UPDATE broadcast_recipients SET status='uncertain' WHERE delivery_id=?",
                (self.delivery_id,),
            )

    def args(self, **changes):
        values = {
            "delivery_id": self.delivery_id,
            "resolution": "received",
            "evidence": "remote journal shows the turn started",
            "expected_revision": 0,
            "expected_delivery_revision": 1,
            "expected_remote_revision": 0,
            "request_id": "resolve-1",
        }
        values.update(changes)
        return values

    def resolve(self, **changes):
        return peer_api.resolve_delivery(self.store, self.factory, self.args(**changes))

    def outbox(self, delivery_id=None):
        with self.store.connection() as db:
            return dict(db.execute(
                "SELECT * FROM peer_outbox WHERE delivery_id=?",
                (delivery_id or self.delivery_id,),
            ).fetchone())

    def recipient(self):
        with self.store.connection() as db:
            return dict(db.execute(
                "SELECT * FROM broadcast_recipients WHERE delivery_id=?", (self.delivery_id,)
            ).fetchone())

    def resolutions(self):
        with self.store.connection() as db:
            return [dict(row) for row in db.execute("SELECT * FROM delivery_resolutions")]

    def stream(self):
        with self.store.connection() as db:
            return dict(db.execute(
                "SELECT * FROM target_streams WHERE stream_id=?", (self.stream_id,)
            ).fetchone())

    async def test_received_releases_the_uncertain_head_and_the_next_sequence(self):
        groups.broadcast(self.store, "team", "next", 0, "send-next")
        self.client.results.append(RECEIVED)

        result = await self.resolve()

        self.assertEqual("released", result["status"])
        self.assertEqual(self.delivery_id, result["delivery_id"])
        self.assertEqual(3, result["revision"])
        self.assertEqual(1, result["remote_revision"])
        self.assertEqual(RECEIVED["receipt"], result["receipt"])
        self.assertNotIn("blocked_by", result)

        outbox = self.outbox()
        self.assertEqual("released", outbox["status"])
        self.assertEqual(3, outbox["revision"])
        self.assertIsNone(outbox["error"])
        self.assertEqual(1, self.stream()["released_seq"])
        self.assertEqual("released", self.recipient()["status"])

        [resolution] = self.resolutions()
        self.assertEqual("completed", resolution["status"])
        self.assertEqual("received", resolution["resolution"])
        # The intent keeps the administrator-supplied remote revision; the response carries the outcome revision.
        self.assertEqual(0, resolution["remote_revision"])
        self.assertEqual(1, json.loads(resolution["remote_result"])["delivery_revision"])

        params = self.client.calls[0]
        self.assertEqual(self.delivery_id, params["delivery_id"])
        self.assertEqual("received", params["resolution"])
        self.assertEqual("remote journal shows the turn started", params["evidence"])
        self.assertEqual(0, params["expected_delivery_revision"])
        self.assertEqual("node-a", params["operation"]["node_id"])
        self.assertEqual("gen-a", params["operation"]["state_generation"])
        self.assertTrue(params["operation"]["operation_id"])

    async def test_not_received_cancels_the_head_and_advances_released_seq(self):
        self.client.results.append(CANCELLED)

        result = await self.resolve(resolution="not_received")

        self.assertEqual("cancelled", result["status"])
        self.assertEqual("cancelled", self.outbox()["status"])
        self.assertEqual(1, self.stream()["released_seq"])
        self.assertEqual("cancelled", self.recipient()["status"])

    async def test_replaying_the_same_request_id_returns_the_stored_outcome(self):
        self.client.results.append(RECEIVED)
        first = await self.resolve()

        second = await self.resolve()

        self.assertEqual(first, second)
        self.assertEqual(1, len(self.client.calls))
        self.assertEqual(1, len(self.resolutions()))

    async def test_a_request_id_cannot_describe_two_different_decisions(self):
        self.client.results.append(RECEIVED)
        await self.resolve()

        with self.assertRaisesRegex(BridgeError, "different resolution decision"):
            await self.resolve(evidence="a different story")

        self.assertEqual(1, len(self.client.calls))

    async def test_offline_peer_leaves_the_head_resolving_until_a_retry_succeeds(self):
        self.client.results.append(BridgeError(PeerErrorCode.HOST_OFFLINE, "ssh is down"))

        pending = await self.resolve()

        self.assertEqual("resolving", pending["status"])
        self.assertEqual("resolving", pending["blocked_by"])
        self.assertEqual(2, pending["revision"])
        self.assertEqual("resolving", self.outbox()["status"])
        [resolution] = self.resolutions()
        self.assertEqual("resolving", resolution["status"])
        # Later sequences remain blocked while the resolution is unresolved.
        groups.broadcast(self.store, "team", "next", 0, "send-next")
        with self.store.connection() as db:
            pending_row = db.execute(
                "SELECT status FROM peer_outbox WHERE status != 'resolving' AND status != 'released'"
            ).fetchone()
        self.assertEqual("pending", pending_row["status"])

        self.client.results.append(RECEIVED)
        completed = await self.resolve()

        self.assertEqual("released", completed["status"])
        self.assertEqual(1, len(self.resolutions()))
        self.assertEqual(2, len(self.client.calls))

    async def test_a_timeout_is_treated_like_an_unreachable_host(self):
        self.client.results.append(TimeoutError("no answer"))

        result = await self.resolve()

        self.assertEqual("resolving", result["status"])
        self.assertEqual("resolving", self.outbox()["status"])

    async def test_contradictory_remote_fact_restores_uncertain_and_fails_the_decision(self):
        self.client.results.append({**CANCELLED, "status": "cancelled"})

        with self.assertRaisesRegex(BridgeError, "contradictory delivery fact"):
            await self.resolve()

        outbox = self.outbox()
        self.assertEqual("uncertain", outbox["status"])
        self.assertEqual(3, outbox["revision"])
        [resolution] = self.resolutions()
        self.assertEqual("failed", resolution["status"])
        self.assertIn("cancelled", resolution["remote_result"])

    async def test_only_an_active_group_with_the_same_revision_and_member_can_resolve(self):
        with self.assertRaisesRegex(BridgeError, "Group revision changed"):
            await self.resolve(expected_revision=1)
        self.assertEqual("uncertain", self.outbox()["status"])
        self.assertEqual([], self.client.calls)

        with self.assertRaisesRegex(BridgeError, "Delivery revision changed"):
            await self.resolve(expected_delivery_revision=0)
        self.assertEqual([], self.client.calls)

        groups.control_group(
            self.store, "team", "remove", 0, "remove", remote_members=[REMOTE],
        )
        groups.control_group(
            self.store, "team", "add", 1, "add", remote_members=[REMOTE_B],
        )
        with self.assertRaisesRegex(BridgeError, "identity changed"):
            await self.resolve(expected_revision=2)
        self.assertEqual([], self.client.calls)

    async def test_deleted_group_and_non_group_delivery_are_rejected(self):
        groups.control_group(
            self.store, "doomed", "create", 0, "create-doomed", remote_members=[REMOTE],
        )
        doomed = groups.broadcast(self.store, "doomed", "hello", 0, "send-doomed")
        doomed_id = doomed["recipients"][0]["delivery_id"]
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE peer_outbox SET status='uncertain',revision=1 WHERE delivery_id=?",
                (doomed_id,),
            )
        with self.assertRaises(BridgeError) as caught:
            groups.control_group(self.store, "doomed", "delete", 0, "delete-doomed")
        self.assertEqual(PeerErrorCode.GROUP_PRUNE_BLOCKED, caught.exception.code)
        # Deletion is impossible until the uncertain delivery settles; apply the soft-delete CAS
        # directly to verify that delivery resolution still rejects an already-deleted group.
        self.store.prune_group_cas("doomed", 0)

        with self.assertRaisesRegex(BridgeError, "group was deleted"):
            await peer_api.resolve_delivery(
                self.store, self.factory,
                self.args(delivery_id=doomed_id, request_id="resolve-doomed"),
            )

        with self.store.connection(write=True) as db:
            db.execute("UPDATE peer_outbox SET group_id=NULL WHERE delivery_id=?", (self.delivery_id,))
        with self.assertRaisesRegex(BridgeError, "group broadcast deliveries"):
            await self.resolve()

    async def test_a_non_head_sequence_cannot_be_resolved(self):
        groups.broadcast(self.store, "team", "first", 0, "send-first")
        second = groups.broadcast(self.store, "team", "second", 0, "send-second")
        second_id = second["recipients"][0]["delivery_id"]
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE peer_outbox SET status='uncertain',revision=1 WHERE delivery_id=?",
                (second_id,),
            )

        with self.assertRaisesRegex(BridgeError, "blocking head"):
            await peer_api.resolve_delivery(
                self.store, self.factory,
                self.args(delivery_id=second_id, request_id="resolve-second"),
            )
        self.assertEqual([], self.client.calls)

    async def test_administrator_evidence_and_identity_are_required(self):
        with self.assertRaisesRegex(BridgeError, "administrator"):
            await peer_api.resolve_delivery(self.store, self.factory, self.args(), admin=False)
        with self.assertRaisesRegex(BridgeError, "administrator"):
            await peer_api.resolve_delivery(self.store, self.factory, self.args(), is_admin=False)
        with self.assertRaisesRegex(BridgeError, "evidence must not be empty"):
            await self.resolve(evidence="  ")
        with self.assertRaisesRegex(BridgeError, "16 KiB"):
            await self.resolve(evidence="x" * (16 * 1024 + 1))
        with self.assertRaisesRegex(BridgeError, "request_id"):
            await self.resolve(request_id="")
        with self.assertRaisesRegex(BridgeError, "resolution must be"):
            await self.resolve(resolution="maybe")
        with self.assertRaisesRegex(BridgeError, "non-empty administrator identity"):
            await peer_api.resolve_delivery(self.store, self.factory, self.args(), actor=" ")
        self.assertEqual([], self.client.calls)

    async def test_unknown_delivery_is_not_found(self):
        with self.assertRaisesRegex(BridgeError, "Unknown delivery"):
            await peer_api.resolve_delivery(
                self.store, self.factory, self.args(delivery_id="missing", request_id="missing"),
            )


class BrokerResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_broker_dispatch_releases_after_remote_receipt(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state = Path(temp.name)
        (state / "config.json").write_text(json.dumps({
            "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
            "worker_launcher": "detached", "telegram_database": None,
        }))
        broker = Broker(state)
        groups.control_group(
            broker.store, "team", "create", 0, "create", remote_members=[REMOTE],
        )
        sent = groups.broadcast(broker.store, "team", "hello", 0, "send")
        delivery_id = sent["recipients"][0]["delivery_id"]
        with broker.store.connection(write=True) as db:
            db.execute(
                "UPDATE peer_outbox SET status='uncertain',revision=1 WHERE delivery_id=?",
                (delivery_id,),
            )
        client = FakeResolutionClient()
        client.results.append(RECEIVED)
        broker.peer_factory = FakeResolutionFactory(client)

        result = await broker.call("agent_delivery_resolve", {
            "delivery_id": delivery_id,
            "resolution": "received",
            "evidence": "checked the execution node",
            "expected_revision": 0,
            "expected_delivery_revision": 1,
            "expected_remote_revision": 0,
            "request_id": "resolve",
        })

        self.assertEqual("released", result["status"])
        with broker.store.connection() as db:
            self.assertEqual(
                "released",
                db.execute("SELECT status FROM peer_outbox WHERE delivery_id=?", (delivery_id,)).fetchone()[0],
            )


class RemoteHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = Store(self.state)
        self.identity = FileNodeIdentityStore(self.state)
        self.profile = Profile("worker", "codex", "worker-model")
        self.service = DirectPeerService(
            self.identity, host_id="gpu-a", store=self.store, config=Config({"worker": self.profile}),
        )

    def operation(self, operation_id):
        identity = self.identity.load_or_create()
        return {
            "node_id": identity.node_id,
            "state_generation": identity.state_generation,
            "operation_id": operation_id,
        }

    def deliver_params(self, **changes):
        params = {
            "operation": self.operation("deliver-1"),
            "delivery_id": "delivery-1",
            "group_id": "group-1",
            "broadcast_id": "broadcast-1",
            "target": {"kind": "bridge", "id": ""},
            "target_seq": 1,
            "seq_kind": "message",
            "message": "do the work",
            "expected_group_revision": 0,
            "ordering_mode": "ordered",
            "stream_id": "stream-1",
            "authority_ref": {"host_id": "coordinator", "node_id": "node-c", "state_generation": "gen-c"},
            "open": {"profile": "worker", "cwd": str(self.state), "access": "read-only"},
        }
        params.update(changes)
        return params

    def resolve_params(self, **changes):
        params = {
            "operation": self.operation("resolve-1"),
            "delivery_id": "delivery-1",
            "resolution": "received",
            "evidence": "coordinator journal",
            "expected_delivery_revision": 0,
        }
        params.update(changes)
        return params

    async def peer(self, method, params):
        return await self.service.handle({"v": 1, "id": "test", "method": method, "params": params})

    def assert_error(self, response, code):
        self.assertFalse(response["ok"], response)
        self.assertEqual(str(code), response["error"]["code"])

    def row(self, table, **where):
        clause = " AND ".join(f"{key}=?" for key in where)
        with self.store.connection() as db:
            return dict(db.execute(f"SELECT * FROM {table} WHERE {clause}", tuple(where.values())).fetchone())

    async def test_received_updates_the_inbox_revision_and_commit_log(self):
        delivered = await self.peer("peer.deliver", self.deliver_params())
        self.assertTrue(delivered["ok"], delivered)
        response = await self.peer("peer.resolve_delivery", self.resolve_params())

        self.assertTrue(response["ok"], response)
        result = response["result"]
        self.assertEqual("received", result["status"])
        self.assertEqual(1, result["delivery_revision"])
        self.assertEqual("received", json.loads(result["receipt"])["resolution"])
        inbox = self.row("peer_inbox", delivery_id="delivery-1")
        self.assertEqual("received", inbox["status"])
        self.assertEqual(1, inbox["revision"])
        with self.store.connection() as db:
            commits = db.execute(
                "SELECT * FROM peer_commit_log WHERE method='peer.resolve_delivery'"
            ).fetchall()
        self.assertEqual(1, len(commits))
        self.assertGreaterEqual(self.identity.load_or_create().max_confirmed_seq, 1)

    async def test_idempotent_replay_does_not_advance_the_inbox_twice(self):
        await self.peer("peer.deliver", self.deliver_params())
        first = await self.peer("peer.resolve_delivery", self.resolve_params())
        second = await self.peer("peer.resolve_delivery", self.resolve_params())

        self.assertTrue(second["ok"], second)
        self.assertEqual(first["result"], second["result"])
        self.assertEqual(1, self.row("peer_inbox", delivery_id="delivery-1")["revision"])

    async def test_not_received_cancels_a_pending_native_queue_row(self):
        params = self.deliver_params(
            operation=self.operation("deliver-native"),
            delivery_id="native-delivery",
            target={"kind": "native", "id": "native-1", "runtime": "codex"},
            target_seq=0,
            ordering_mode="legacy",
            stream_id=None,
            authority_ref=None,
            open=None,
        )
        delivered = await self.peer("peer.deliver", params)
        self.assertTrue(delivered["ok"], delivered)
        self.assertEqual("pending", delivered["result"]["send_status"])

        response = await self.peer("peer.resolve_delivery", self.resolve_params(
            operation=self.operation("resolve-native"),
            delivery_id="native-delivery",
            resolution="not_received",
        ))

        self.assertTrue(response["ok"], response)
        self.assertEqual("cancelled", response["result"]["status"])
        inbox = self.row("peer_inbox", delivery_id="native-delivery")
        self.assertEqual("cancelled", inbox["status"])
        self.assertEqual(1, inbox["revision"])
        queue = self.row("native_send_queue", delivery_id="native-delivery")
        self.assertEqual("cancelled", queue["status"])
        self.assertEqual("not_received", json.loads(queue["receipt"])["resolution"])

    async def test_received_marks_a_pending_native_queue_row_received(self):
        params = self.deliver_params(
            operation=self.operation("deliver-native"),
            delivery_id="native-delivery",
            target={"kind": "native", "id": "native-1", "runtime": "codex"},
            target_seq=0,
            ordering_mode="legacy",
            stream_id=None,
            authority_ref=None,
            open=None,
        )
        await self.peer("peer.deliver", params)

        response = await self.peer("peer.resolve_delivery", self.resolve_params(
            operation=self.operation("resolve-native"),
            delivery_id="native-delivery",
        ))

        self.assertTrue(response["ok"], response)
        self.assertEqual("received", response["result"]["status"])
        self.assertEqual("received", self.row("native_send_queue", delivery_id="native-delivery")["status"])

    async def test_revision_mismatch_and_missing_inbox(self):
        await self.peer("peer.deliver", self.deliver_params())

        conflict = await self.peer("peer.resolve_delivery", self.resolve_params(expected_delivery_revision=1))
        self.assert_error(conflict, PeerErrorCode.REVISION_CONFLICT)

        missing = await self.peer("peer.resolve_delivery", self.resolve_params(
            delivery_id="missing", operation=self.operation("resolve-missing"),
        ))
        self.assert_error(missing, PeerErrorCode.OPERATION_NOT_FOUND)

        absent = await self.peer("peer.resolve_delivery", {
            "operation": self.operation("resolve-absent"),
            "delivery_id": "absent",
            "resolution": "not_received",
            "evidence": "never arrived",
            "expected_delivery_revision": 0,
        })
        self.assertTrue(absent["ok"], absent)
        self.assertEqual("cancelled", absent["result"]["status"])
        self.assertEqual(0, absent["result"]["delivery_revision"])

    async def test_bridge_delivery_with_a_queued_task_cannot_be_cancelled(self):
        await self.peer("peer.deliver", self.deliver_params())

        response = await self.peer("peer.resolve_delivery", self.resolve_params(resolution="not_received"))

        self.assert_error(response, PeerErrorCode.OPERATION_CONFLICT)
        self.assertEqual("received", self.row("peer_inbox", delivery_id="delivery-1")["status"])

    async def test_ordered_streams_reject_sequence_gaps(self):
        params = self.deliver_params(
            operation=self.operation("deliver-gap"),
            delivery_id="gap",
            target_seq=2,
        )
        gap = await self.peer("peer.deliver", params)
        self.assert_error(gap, PeerErrorCode.SEQUENCE_GAP)

        first = await self.peer("peer.deliver", self.deliver_params(
            operation=self.operation("deliver-one"), delivery_id="one", target_seq=1,
        ))
        second = await self.peer("peer.deliver", self.deliver_params(
            operation=self.operation("deliver-two"), delivery_id="two", target_seq=2,
        ))
        self.assertTrue(first["ok"], first)
        self.assertTrue(second["ok"], second)
        self.assertEqual(2, self.row("incoming_streams", stream_id="stream-1")["admitted_seq"])

    async def test_resolve_requires_evidence_and_a_valid_decision(self):
        await self.peer("peer.deliver", self.deliver_params())

        blank = await self.peer("peer.resolve_delivery", self.resolve_params(evidence=" "))
        self.assert_error(blank, PeerErrorCode.INVALID_PARAMS)

        huge = await self.peer("peer.resolve_delivery", self.resolve_params(evidence="x" * (16 * 1024 + 1)))
        self.assert_error(huge, PeerErrorCode.INVALID_PARAMS)

        invalid = await self.peer("peer.resolve_delivery", self.resolve_params(resolution="maybe"))
        self.assert_error(invalid, PeerErrorCode.INVALID_PARAMS)


if __name__ == "__main__":
    unittest.main()
