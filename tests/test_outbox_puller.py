import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

from agent_bridge import groups
from agent_bridge.broker import Broker, OutboxPuller
from agent_bridge.errors import BridgeError
from agent_bridge.hosts import HostConfig
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import PeerErrorCode
from agent_bridge.store import Store, encode

HOST = "gpu-a"
NODE = "node-a"
GENERATION = "gen-a"
SENDER = "remote-session"


class FakeHostRegistry:
    def __init__(self, configs):
        self.configs = {config.host_id: config for config in configs}

    def get(self, host_id):
        return self.configs[host_id]

    def list(self):
        return list(self.configs.values())


class FakePeerClient:
    def __init__(self, node, responses=()):
        self.node = node
        self.responses = list(responses)
        self.pull_params = []
        self.acks = []
        self.hello_calls = 0

    async def hello(self, params):
        self.hello_calls += 1
        return {"node": self.node}

    async def pull_requests(self, params):
        self.pull_params.append(dict(params))
        if not self.responses:
            return {"items": [], "cursor": params["after"], "has_more": False, "scan_upper_seq": params["after"]}
        response = self.responses.pop(0)
        return response(params) if callable(response) else response

    async def ack_request(self, params):
        self.acks.append(dict(params))
        return {"operation_id": params["operation_id"], "status": "acked"}


class FakePeerFactory:
    def __init__(self, clients):
        self.clients = clients

    def client(self, host_id):
        return self.clients[host_id]


def host_config(host_id=HOST, node_id=NODE, generation=GENERATION):
    return HostConfig(
        host_id=host_id,
        ssh_target=host_id + ".internal",
        remote_executable="/usr/local/bin/agent-bridge",
        pinned=None,
    )


def remote_member(host_id=HOST, node_id=NODE, generation=GENERATION, target=SENDER):
    return {
        "host_id": host_id,
        "node_id": node_id,
        "state_generation": generation,
        "target_kind": "bridge",
        "session_id": target,
        "label": "remote",
    }


def outbox_item(operation_id, seq, *, payload=None, method="agent_broadcast", sender=SENDER, fingerprint=None,
                group_id="team", request_id=None):
    payload = payload or {
        "op": "broadcast", "group_id": group_id, "message": "hello", "revision": 0, "reservation": 0,
    }
    sender_ref = {
        "host_id": HOST, "node_id": NODE, "state_generation": GENERATION, "local_id": sender,
    }
    authority = {"host_id": "local", "node_id": "coordinator-node", "state_generation": "coordinator-gen"}
    return {
        "operation_id": operation_id,
        "method": method,
        "payload": payload,
        "created": 1.0,
        "seq": seq,
        "sender_ref": sender_ref,
        "authority_ref": authority,
        "authorization_revision": 0,
        "fingerprint": fingerprint or hashlib.sha256(encode(payload).encode()).hexdigest(),
        "group_id": group_id,
        "request_id": request_id or operation_id,
    }


class PullerTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.store = Store(self.root)
        self.member = remote_member()
        self.group = groups.control_group(
            self.store, "team", "create", 0, "create", remote_members=[self.member],
        )

    async def run_puller(self, responses):
        node = {"node_id": NODE, "state_generation": GENERATION}
        client = FakePeerClient(node, responses)
        factory = FakePeerFactory({HOST: client})
        puller = OutboxPuller(
            self.store, factory, FakeHostRegistry([host_config()]),
        )
        processed = await puller.run_once(HOST)
        return puller, client, processed

    def add_item(self, item):
        return self.store.add_outbox(
            item["operation_id"],
            item["method"],
            item["payload"],
            item["group_id"],
            request_id=item["request_id"],
            sender_ref=item["sender_ref"],
            authority_ref=item["authority_ref"],
            authorization_revision=item["authorization_revision"],
            fingerprint=item["fingerprint"],
        )


class OutboxPullerTests(PullerTestCase):
    async def test_older_relay_frame_errors_reduce_only_the_read_page(self):
        client = FakePeerClient({"node_id": NODE, "state_generation": GENERATION})
        async def pull(params):
            client.pull_params.append(params)
            if params["limit"] > 3:
                raise BridgeError(PeerErrorCode.FRAME_TOO_LARGE, "old relay frame limit")
            return {"items": [], "cursor": params["after"], "has_more": False, "scan_upper_seq": 0}
        client.pull_requests = pull
        puller = OutboxPuller(self.store, FakePeerFactory({HOST: client}), FakeHostRegistry([host_config()]))
        self.assertEqual(0, await puller.run_once(HOST))
        self.assertEqual([100, 50, 25, 12, 6, 3], [call["limit"] for call in client.pull_params])
        self.assertEqual({0}, {call["after"] for call in client.pull_params})
        self.assertEqual([], client.acks)

    async def test_oversized_single_request_stops_reducing_and_preserves_cursor(self):
        client = FakePeerClient({"node_id": NODE, "state_generation": GENERATION})
        async def pull(params):
            client.pull_params.append(params)
            raise BridgeError(PeerErrorCode.FRAME_TOO_LARGE, "one oversized request")
        client.pull_requests = pull
        puller = OutboxPuller(self.store, FakePeerFactory({HOST: client}), FakeHostRegistry([host_config()]))
        puller._temp_after[HOST] = 7
        with self.assertRaises(BridgeError):
            await puller.run_once(HOST)
        self.assertEqual([100, 50, 25, 12, 6, 3, 1], [call["limit"] for call in client.pull_params])
        self.assertEqual(7, puller._temp_after[HOST])
        self.assertEqual([], client.acks)

    async def test_pull_verifies_membership_and_broadcasts_atomically(self):
        item = outbox_item("op-1", 1)
        self.add_item(item)
        item["authority_ref"] = groups.resolve_authority_ref(self.store)

        puller, client, processed = await self.run_puller(
            [{"items": [item], "cursor": 1, "has_more": False, "scan_upper_seq": 1}]
        )

        self.assertEqual(1, processed)
        self.assertEqual([{"after": 0, "limit": 100}], client.pull_params)
        with self.store.connection() as db:
            broadcast = dict(db.execute("SELECT * FROM group_broadcasts").fetchone())
            self.assertEqual("team", broadcast["group_id"])
            self.assertEqual("hello", broadcast["message"])
            outbox_count = db.execute("SELECT COUNT(*) FROM peer_outbox").fetchone()[0]
        self.assertEqual(1, outbox_count)
        receipt = self.store.get_pulled_request_receipt(HOST, NODE, GENERATION, "op-1")
        self.assertEqual("accepted", receipt["outcome"])
        self.assertEqual(broadcast["id"], receipt["result"]["broadcast_id"])
        self.assertIsNotNone(receipt["acked_at"])
        self.assertEqual("accepted", client.acks[0]["outcome"])
        self.assertEqual(receipt["result"], client.acks[0]["result"])
        self.assertEqual(NODE, client.acks[0]["operation"]["node_id"])
        self.assertEqual(0, puller._temp_after[HOST])
        with self.store.connection() as db:
            row = db.execute("SELECT status,outcome,result FROM node_outbox WHERE operation_id='op-1'").fetchone()
        # This fake transport does not implement the execution-node pull handler, so the row stays pending;
        # the real peer.pull_requests handler marks it pulled before returning the page.
        self.assertEqual("pending", row["status"])

    async def test_replayed_request_reuses_receipt_and_does_not_broadcast_twice(self):
        item = outbox_item("op-1", 1)
        self.add_item(item)
        item["authority_ref"] = groups.resolve_authority_ref(self.store)
        response = {"items": [item], "cursor": 1, "has_more": False, "scan_upper_seq": 1}

        await self.run_puller([response])
        # Simulate an ACK whose reply was lost: the receipt exists but acked_at is not set.
        _, client, processed = await self.run_puller([response])

        self.assertEqual(1, processed)
        self.assertEqual(1, len(client.acks))
        self.assertEqual("accepted", client.acks[0]["outcome"])
        with self.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM group_broadcasts").fetchone()[0])

    async def test_rejected_requests_are_receipted_and_acknowledged(self):
        cases = [
            ("wrong revision", {"revision": 99}, self.member, "stale_revision"),
            ("not a member", {}, remote_member(target="someone-else"), "not_a_member"),
            ("unsupported", {}, self.member, "unsupported_method"),
        ]
        for index, (name, payload, member, code) in enumerate(cases):
            with self.subTest(name=name):
                temp = tempfile.TemporaryDirectory()
                self.addCleanup(temp.cleanup)
                store = Store(Path(temp.name))
                groups.control_group(store, "team", "create", 0, "create", remote_members=[member])
                item = outbox_item(f"op-{index}", 1, payload={**outbox_item(f"op-{index}", 1)["payload"], **payload})
                if name == "unsupported":
                    item["method"] = "agent_group_inbox"
                item["authority_ref"] = groups.resolve_authority_ref(store)
                node = {"node_id": NODE, "state_generation": GENERATION}
                client = FakePeerClient(node, [{"items": [item], "cursor": 1, "scan_upper_seq": 1}])
                puller = OutboxPuller(store, FakePeerFactory({HOST: client}), FakeHostRegistry([host_config()]))
                await puller.run_once(HOST)
                receipt = store.get_pulled_request_receipt(HOST, NODE, GENERATION, f"op-{index}")
                self.assertEqual("rejected", receipt["outcome"])
                self.assertEqual(code, receipt["result"]["error"]["code"])
                self.assertEqual("rejected", client.acks[0]["outcome"])
                with store.connection() as db:
                    self.assertEqual(0, db.execute("SELECT COUNT(*) FROM group_broadcasts").fetchone()[0])

    async def test_deleted_group_is_rejected(self):
        deleted = groups.control_group(self.store, "team", "delete", 0, "delete")
        item = outbox_item("op-deleted", 1, payload={
            "op": "broadcast", "group_id": "team", "message": "hello",
            "revision": deleted["revision"], "reservation": 0,
        })
        self.add_item(item)
        item["authority_ref"] = groups.resolve_authority_ref(self.store)
        _, client, _ = await self.run_puller(
            [{"items": [item], "cursor": 1, "scan_upper_seq": 1}]
        )
        receipt = self.store.get_pulled_request_receipt(HOST, NODE, GENERATION, "op-deleted")
        self.assertEqual("rejected", receipt["outcome"])
        self.assertEqual("group_deleted", receipt["result"]["error"]["code"])
        self.assertEqual("rejected", client.acks[0]["outcome"])

    async def test_temporary_cursor_scans_to_scan_upper_seq_then_restarts_at_zero(self):
        first = outbox_item("op-1", 1, method="agent_group_inbox")
        second = outbox_item("op-2", 3, method="agent_group_inbox")
        self.add_item(first)
        self.add_item(second)
        self.add_item(outbox_item("op-3", 4, method="agent_group_inbox"))
        responses = [
            {"items": [first], "cursor": 1, "has_more": True, "scan_upper_seq": 4},
            {"items": [second], "cursor": 3, "has_more": True, "scan_upper_seq": 4},
            {"items": [], "cursor": 4, "has_more": False, "scan_upper_seq": 4},
        ]
        puller, client, _ = await self.run_puller(responses)
        await puller.run_once(HOST)
        await puller.run_once(HOST)

        self.assertEqual(
            [{"after": 0, "limit": 100}, {"after": 1, "limit": 100}, {"after": 3, "limit": 100}],
            client.pull_params,
        )
        self.assertEqual(0, puller._temp_after[HOST])
        self.assertNotIn(HOST, puller._scan_upper)

    async def test_pull_without_scan_upper_advances_and_resets_on_empty_page(self):
        item = outbox_item("op-1", 2, method="agent_group_inbox")
        self.add_item(item)
        puller, client, _ = await self.run_puller(
            [
                {"items": [item], "cursor": 2, "has_more": True},
                {"items": [], "cursor": 2, "has_more": False},
            ]
        )
        await puller.run_once(HOST)
        self.assertEqual(
            [{"after": 0, "limit": 100}, {"after": 2, "limit": 100}],
            client.pull_params,
        )
        self.assertEqual(0, puller._temp_after[HOST])


class PeerOutboxHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.store = Store(self.root)
        self.identity = FileNodeIdentityStore(self.root)
        self.service = DirectPeerService(self.identity, host_id=HOST, store=self.store)

    async def peer(self, method, params):
        return await self.service.handle({"v": 1, "id": "t", "method": method, "params": params})

    def operation(self, operation_id="op-1"):
        identity = self.identity.load_or_create()
        return {
            "node_id": identity.node_id,
            "state_generation": identity.state_generation,
            "operation_id": operation_id,
        }

    async def test_pull_and_ack_are_idempotent_and_conflicts_are_detected(self):
        self.store.add_outbox("op-1", "agent_broadcast", {"message": "hello"})
        pulled = await self.peer("peer.pull_requests", {"after": 0, "limit": 10})
        self.assertTrue(pulled["ok"], pulled)
        self.assertEqual(1, len(pulled["result"]["items"]))
        self.assertEqual(1, pulled["result"]["scan_upper_seq"])
        self.assertEqual("pulled", self.store.get_node_outbox("op-1")["status"])

        params = {"operation_id": "op-1", "outcome": "accepted", "result": {"broadcast_id": "b"}}
        acked = await self.peer("peer.ack_request", params)
        self.assertTrue(acked["ok"], acked)
        self.assertEqual("acked", acked["result"]["status"])
        repeated = await self.peer("peer.ack_request", params)
        self.assertTrue(repeated["ok"], repeated)
        conflicting = await self.peer(
            "peer.ack_request", {**params, "result": {"broadcast_id": "other"}},
        )
        self.assertFalse(conflicting["ok"])
        self.assertEqual(PeerErrorCode.OPERATION_CONFLICT, conflicting["error"]["code"])

    async def test_ack_requires_node_identity_when_operation_is_supplied(self):
        self.store.add_outbox("op-1", "agent_broadcast", {})
        response = await self.peer(
            "peer.ack_request",
            {
                "operation": {"node_id": "other", "state_generation": "gen", "operation_id": "op-1"},
                "operation_id": "op-1",
                "outcome": "accepted",
                "result": {},
            },
        )
        self.assertFalse(response["ok"])
        self.assertEqual(PeerErrorCode.IDENTITY_MISMATCH, response["error"]["code"])

    async def test_pull_rejects_invalid_limits(self):
        for params in ({"after": -1}, {"limit": 0}, {"after": True}):
            response = await self.peer("peer.pull_requests", params)
            self.assertFalse(response["ok"], params)
            self.assertEqual(PeerErrorCode.INVALID_PARAMS, response["error"]["code"])


class MemberBroadcastTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.state = Path(temp.name)
        (self.state / "config.json").write_text(json.dumps({
            "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
            "worker_launcher": "detached",
            "telegram_database": None,
        }))
        self.broker = Broker(self.state)
        identity = FileNodeIdentityStore(self.state).node_ref(HOST)
        sender_ref = {"host_id": HOST, "node_id": identity.node_id,
                      "state_generation": identity.state_generation, "local_id": SENDER}
        authority = FileNodeIdentityStore(self.state).node_ref("local").as_dict()
        with self.broker.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO member_bindings(id,token_digest,sender_ref,authority_ref,group_id,allowed_methods,"
                "authorization_revision,pid,process_start,expires_at,created) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "binding-1", "digest", encode(sender_ref), encode(authority), "team",
                    json.dumps(["agent_broadcast"]), 0, 1, "start", 4102444800.0, 1.0,
                ),
            )

    async def test_member_broadcast_writes_node_outbox_and_operation_reports_ack(self):
        result = await self.broker.call(
            "agent_broadcast",
            {
                "binding_id": "binding-1",
                "group_id": "team",
                "message": "hello",
                "expected_revision": 0,
                "request_id": "request-1",
            },
        )
        self.assertEqual("pending", result["status"])
        operation_id = result["operation_id"]
        row = self.broker.store.get_node_outbox(operation_id)
        self.assertEqual("agent_broadcast", row["method"])
        self.assertEqual("pending", row["status"])
        self.assertEqual("binding-1", row["binding_id"])
        self.assertEqual("team", row["group_id"])
        self.assertEqual("hello", row["payload"]["message"])

        pending = await self.broker.call("agent_operation", {"operation_id": operation_id, "wait": 0})
        self.assertEqual("pending", pending["status"])
        self.assertEqual("binding-1", row["binding_id"])

        self.broker.store.ack_outbox(operation_id, {"broadcast_id": "b-1"}, "accepted")
        acked = await self.broker.call("agent_operation", {"operation_id": operation_id})
        self.assertEqual("acked", acked["status"])
        self.assertEqual("accepted", acked["outcome"])
        self.assertEqual({"broadcast_id": "b-1"}, acked["result"])

        with self.broker.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM node_outbox").fetchone()[0])


class OutboxAckReplayBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_ack_request_replay_through_transport_is_idempotent(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        store = Store(Path(temp.name))
        identity = FileNodeIdentityStore(Path(temp.name))
        store.add_outbox("op-1", "agent_broadcast", {"x": 1})
        service = DirectPeerService(identity, host_id=HOST, store=store)
        params = {
            "operation_id": "op-1",
            "outcome": "accepted",
            "result": {"broadcast_id": "b"},
        }
        first = await service.handle({"v": 1, "id": "1", "method": "peer.ack_request", "params": params})
        second = await service.handle({"v": 1, "id": "2", "method": "peer.ack_request", "params": params})
        self.assertTrue(first["ok"], first)
        self.assertTrue(second["ok"], second)
        self.assertEqual(first["result"], second["result"])


if __name__ == "__main__":
    unittest.main()
