import json
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

from agent_bridge import groups
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import compute_stream_id
from agent_bridge.store import Store

REMOTE_BRIDGE = {
    "host_id": "gpu-a", "node_id": "node-a", "state_generation": "gen-a",
    "target_kind": "bridge", "session_id": "remote-session-a", "label": "remote",
}
REMOTE_NATIVE = {
    "host_id": "gpu-b", "node_id": "node-b", "state_generation": "gen-b",
    "kind": "native", "runtime": "codex", "native_id": "native-a", "cwd": "/srv/project", "label": "native",
}


class RemoteGroupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.profile = Profile("fixture", "codex", "fixed", executable=sys.executable)
        self.config = Config({"fixture": self.profile})

    def local_session(self, name):
        sid = self.store.open(self.profile, str(self.root), "read-only", "open-" + name)["session_id"]
        self.store.worker_ready(sid, "g", 123, "fixture", "native-" + sid)
        return sid

    def test_remote_members_are_registered_listed_and_counted(self):
        group = groups.control_group(
            self.store, "team", "create", 0, "create", remote_members=[REMOTE_BRIDGE],
        )

        self.assertEqual(0, group["revision"])
        [member] = group["remote_members"]
        self.assertEqual("gpu-a", member["host_id"])
        self.assertEqual("node-a", member["node_id"])
        self.assertEqual("gen-a", member["state_generation"])
        self.assertEqual("bridge", member["target_kind"])
        self.assertEqual("remote-session-a", member["target_id"])
        self.assertEqual("remote-session-a", member["session_id"])
        self.assertEqual("", member["runtime"])
        self.assertEqual("", member["cwd"])
        self.assertTrue(member["verification_receipt"])
        listed = groups.inspect_groups(self.store)
        self.assertEqual(1, listed["groups"][0]["member_count"])

        native = {**REMOTE_NATIVE, "native_id": str(uuid.uuid4())}
        added = groups.control_group(self.store, "team", "add", 0, "add-native", remote_members=[native])
        self.assertEqual(1, added["revision"])
        self.assertEqual(2, len(added["remote_members"]))
        self.assertEqual(2, groups.inspect_groups(self.store)["groups"][0]["member_count"])

        duplicate = groups.control_group(self.store, "team", "add", 1, "duplicate", remote_members=[native])
        self.assertEqual(1, duplicate["revision"])
        with self.assertRaisesRegex(BridgeError, "Remove the old remote member"):
            groups.control_group(
                self.store, "team", "add", 1, "conflict",
                remote_members=[{**native, "label": "changed"}],
            )

    def test_remove_requires_full_identity_and_delete_keeps_remote_archive(self):
        groups.control_group(self.store, "team", "create", 0, "create", remote_members=[REMOTE_BRIDGE])
        with self.assertRaises(BridgeError):
            groups.control_group(
                self.store, "team", "remove", 0, "remove-missing-identity",
                remote_members=[{"host_id": "gpu-a", "target_kind": "bridge", "session_id": "remote-session-a"}],
            )
        removed = groups.control_group(
            self.store, "team", "remove", 0, "remove",
            remote_members=[REMOTE_BRIDGE],
        )
        self.assertEqual(1, removed["revision"])
        self.assertEqual([], removed["remote_members"])

        groups.control_group(self.store, "team", "add", 1, "re-add", remote_members=[REMOTE_BRIDGE])
        deleted = groups.control_group(self.store, "team", "delete", 2, "delete")
        self.assertTrue(deleted["deleted"])
        self.assertEqual([], deleted["members"])
        self.assertEqual(1, len(deleted["remote_members"]))
        self.assertEqual([], groups.inspect_groups(self.store)["groups"])
        self.assertEqual(1, len(groups.inspect_groups(self.store, "team")["remote_members"]))
        with self.assertRaisesRegex(BridgeError, "group was deleted"):
            groups.broadcast(self.store, "team", "message", 3, "deleted")

    def test_bridge_remote_member_rejects_runtime_and_native_rejects_session_id(self):
        for member in (
            {**REMOTE_BRIDGE, "runtime": "codex"},
            {**REMOTE_BRIDGE, "session_id": None},
            {**REMOTE_NATIVE, "session_id": "both"},
            {**REMOTE_NATIVE, "runtime": "other"},
        ):
            with self.subTest(member=member), self.assertRaises(BridgeError):
                groups.control_group(self.store, "bad", "create", 0, "bad", remote_members=[member])


class RemoteBroadcastTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.profile = Profile("fixture", "codex", "fixed", executable=sys.executable)
        self.config = Config({"fixture": self.profile})

    def local_session(self, name):
        sid = self.store.open(self.profile, str(self.root), "read-only", "open-" + name)["session_id"]
        self.store.worker_ready(sid, "g", 123, "fixture", "native-" + sid)
        return sid

    def test_broadcast_freezes_targets_and_allocates_monotonic_stream_sequences(self):
        groups.control_group(self.store, "team", "create", 0, "create", remote_members=[REMOTE_BRIDGE])

        first = groups.broadcast(self.store, "team", "first", 0, "send-1")
        [recipient] = first["recipients"]
        self.assertEqual(1, first["pending_count"])
        self.assertEqual("pending", recipient["status"])
        self.assertEqual(1, recipient["target_seq"])

        with self.store.connection() as db:
            broadcast = dict(db.execute("SELECT * FROM group_broadcasts").fetchone())
            stream = dict(db.execute("SELECT * FROM target_streams").fetchone())
            outbox = dict(db.execute("SELECT * FROM peer_outbox").fetchone())
            recipients = [dict(row) for row in db.execute("SELECT * FROM broadcast_recipients")]
            group_row = dict(db.execute("SELECT * FROM session_groups WHERE id='team'").fetchone())
        snapshot = json.loads(broadcast["recipient_snapshot"])
        expected_target = {
            "host_id": "gpu-a", "node_id": "node-a", "state_generation": "gen-a",
            "kind": "bridge", "runtime": "", "target_id": "remote-session-a",
        }
        self.assertEqual([expected_target], snapshot)
        self.assertEqual(compute_stream_id(outbox_authority(outbox), expected_target), outbox["stream_id"])
        self.assertEqual(2, stream["next_seq"])
        self.assertEqual(0, stream["released_seq"])
        self.assertEqual("pending", outbox["status"])
        self.assertEqual(0, outbox["revision"])
        self.assertEqual("message", outbox["seq_kind"])
        self.assertEqual(1, outbox["target_seq"])
        self.assertEqual("gpu-a", outbox["host_id"])
        self.assertEqual("node-a", outbox["node_id"])
        self.assertEqual("gen-a", outbox["state_generation"])
        self.assertEqual(1, len(recipients))
        self.assertEqual(outbox["delivery_id"], recipients[0]["delivery_id"])
        self.assertIsNotNone(group_row["last_activity_at"])

        self.assertEqual(first, groups.broadcast(self.store, "team", "first", 0, "send-1"))
        second = groups.broadcast(self.store, "team", "second", 0, "send-2")
        self.assertEqual(2, second["recipients"][0]["target_seq"])
        with self.store.connection() as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM peer_outbox").fetchone()[0])
            self.assertEqual(3, db.execute("SELECT next_seq FROM target_streams").fetchone()[0])
        with self.assertRaisesRegex(BridgeError, "different content"):
            groups.broadcast(self.store, "team", "changed", 0, "send-1")

    def test_unified_broadcast_status_merges_local_and_remote_recipients(self):
        sid = self.local_session("local")
        groups.control_group(
            self.store, "team", "create", 0, "create", session_ids=[sid], remote_members=[REMOTE_BRIDGE],
        )
        result = groups.broadcast(self.store, "team", "hello", 0, "send")

        self.assertEqual(1, result["queued_count"])
        self.assertEqual(1, result["pending_count"])
        status = groups.broadcast_status(self.store, result["broadcast_id"])
        self.assertEqual({"queued": 1, "pending": 1}, status["counts"])
        bridge = next(item for item in status["recipients"] if item["kind"] == "bridge" and item["host_id"] == "local")
        remote = next(item for item in status["recipients"] if item["host_id"] == "gpu-a")
        self.assertEqual(sid, bridge["session_id"])
        self.assertIsNotNone(bridge["task_id"])
        self.assertEqual("pending", remote["status"])
        self.assertEqual(1, remote["target_seq"])
        with self.assertRaises(BridgeError):
            groups.broadcast_status(self.store, "missing")

    def test_remote_native_rejects_budget_scope_and_native_member_with_cwd(self):
        native = {**REMOTE_NATIVE, "native_id": str(uuid.uuid4()), "cwd": str(self.root)}
        groups.control_group(self.store, "team", "create", 0, "create", remote_members=[native])
        with self.assertRaisesRegex(BridgeError, "Remote native sessions"):
            groups.broadcast(self.store, "team", "hello", 0, "send", 10, token_budget=100)
        with self.store.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM peer_outbox").fetchone()[0])

    def test_local_native_budget_still_uses_existing_error(self):
        native = {"runtime": "codex", "native_id": str(uuid.uuid4()), "cwd": str(self.root), "label": ""}
        groups.control_group(self.store, "team", "create", 0, "create", native_members=[native])
        with self.assertRaisesRegex(BridgeError, "Native sessions execute outside"):
            groups.broadcast(self.store, "team", "hello", 0, "send", 10, token_budget=100)


def outbox_authority(outbox):
    return json.loads(outbox["payload"])["authority_ref"]


class RemoteGroupBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def test_broker_resolves_member_identity_and_rejects_mismatch(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state = Path(temp.name)
        (state / "config.json").write_text(json.dumps({
            "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
            "worker_launcher": "detached", "telegram_database": None,
            "hosts": {"gpu-a": {
                "ssh_target": "gpu-a.internal",
                "remote_executable": "/usr/local/bin/agent-bridge",
                "pinned": {"node_id": "resolved-node", "state_generation": "resolved-generation"},
            }},
        }))
        from agent_bridge.broker import Broker

        broker = Broker(state)
        member = {"host_id": "gpu-a", "target_kind": "bridge", "session_id": "s"}
        group = await broker.call("agent_group", {
            "group_id": "team", "action": "create", "expected_revision": 0,
            "request_id": "create", "remote_members": [member],
        })
        [stored] = group["remote_members"]
        self.assertEqual("resolved-node", stored["node_id"])
        self.assertEqual("resolved-generation", stored["state_generation"])

        with self.assertRaisesRegex(BridgeError, "does not match"):
            await broker.call("agent_group", {
                "group_id": "other", "action": "create", "expected_revision": 0,
                "request_id": "other-create",
                "remote_members": [{**member, "node_id": "wrong"}],
            })


if __name__ == "__main__":
    unittest.main()
