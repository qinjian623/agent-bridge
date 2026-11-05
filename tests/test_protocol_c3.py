import tempfile
import unittest
from pathlib import Path

from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import (
    PeerErrorCode,
    normalize_ack_request_params,
    normalize_notify_parent_params,
    normalize_outbox_item,
    validate_object_ref,
)
from agent_bridge.store import Store

NODE = ("host-a", "node-a", "gen-1")


def ref(local_id="x"):
    return {"host_id": "h", "node_id": "n", "state_generation": "g", "local_id": local_id}


def operation():
    return {"node_id": "n", "state_generation": "g", "operation_id": "op"}


class StoreC3Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))

    def test_schema_tables_exist(self):
        with self.store.connection() as db:
            names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table in (
            "peer_event_mirrors",
            "peer_event_cursors",
            "peer_parent_routes",
            "mirrored_parent_notifications",
        ):
            self.assertIn(table, names)
        Store(Path(self.temp.name))  # re-open is idempotent

    def test_parent_routes(self):
        self.assertIsNone(self.store.lookup_parent_route(*NODE, "child"))
        self.store.register_parent_route(*NODE, "child", ref("p"))
        self.store.register_parent_route(*NODE, "child", ref("p"))  # idempotent
        self.assertEqual(self.store.lookup_parent_route(*NODE, "child")["parent_ref"], ref("p"))
        with self.assertRaises(BridgeError) as caught:
            self.store.register_parent_route(*NODE, "child", ref("other"))
        self.assertEqual(caught.exception.code, PeerErrorCode.PARENT_ROUTE_CONFLICT)
        self.assertIsNone(self.store.lookup_parent_route("host-a", "node-a", "gen-2", "child"))

    def test_event_cursor_never_rewinds(self):
        self.assertEqual(self.store.get_peer_event_cursor(*NODE), 0)
        self.store.advance_peer_event_cursor(*NODE, 18)
        self.store.advance_peer_event_cursor(*NODE, 5)
        self.assertEqual(self.store.get_peer_event_cursor(*NODE), 18)
        self.assertEqual(self.store.get_peer_event_cursor("host-a", "node-a", "gen-2"), 0)
        with self.assertRaises(BridgeError):
            self.store.advance_peer_event_cursor(*NODE, -1)

    def test_mirrored_events(self):
        with self.store.connection(write=True) as db:
            local_seq = db.execute("INSERT INTO events(kind,data,created) VALUES('x','{}',1)").lastrowid
        self.assertIsNone(self.store.get_mirrored_event(*NODE, 18))
        row = self.store.mirror_peer_event(*NODE, 18, "fp1", local_seq)
        self.assertEqual(row["local_seq"], local_seq)
        again = self.store.mirror_peer_event(*NODE, 18, "fp1", local_seq)
        self.assertEqual(again["local_seq"], local_seq)
        self.assertEqual(self.store.get_mirrored_event(*NODE, 18)["fingerprint"], "fp1")
        with self.assertRaises(BridgeError) as caught:
            self.store.mirror_peer_event(*NODE, 18, "fp2", local_seq)
        self.assertEqual(caught.exception.code, PeerErrorCode.OPERATION_CONFLICT)

    def test_pulled_request_receipts(self):
        self.assertIsNone(self.store.get_pulled_request_receipt(*NODE, "op"))
        first = self.store.record_pulled_request_receipt(*NODE, "op", 3, "fp", "accepted", {"broadcast_id": "b"})
        self.assertEqual(first["result"], {"broadcast_id": "b"})
        self.assertIsNone(first["acked_at"])
        again = self.store.record_pulled_request_receipt(*NODE, "op", 3, "fp", "rejected", {})
        self.assertEqual(again["outcome"], "accepted")
        with self.assertRaises(BridgeError):
            self.store.record_pulled_request_receipt(*NODE, "op", 3, "other", "accepted", {})
        acked = self.store.update_pulled_request_receipt_acked(*NODE, "op", acked_at=42.0)
        self.assertEqual(acked["acked_at"], 42.0)
        self.assertEqual(self.store.update_pulled_request_receipt_acked(*NODE, "op", acked_at=99.0)["acked_at"], 42.0)
        with self.assertRaises(BridgeError):
            self.store.update_pulled_request_receipt_acked(*NODE, "missing")
        self.assertIsNone(self.store.get_pulled_request_receipt("host-a", "node-a", "gen-2", "op"))

    def test_ack_outbox_persists_result_and_outcome(self):
        self.store.add_outbox("op-1", "agent_broadcast", {"a": 1})
        row = self.store.ack_outbox("op-1", {"broadcast_id": "b"}, "accepted")
        self.assertEqual(row["status"], "acked")
        self.assertEqual(row["outcome"], "accepted")
        self.assertEqual(row["result"], {"broadcast_id": "b"})
        self.assertEqual(self.store.ack_outbox("op-1", {"broadcast_id": "b"}, "accepted")["status"], "acked")
        with self.assertRaises(BridgeError) as caught:
            self.store.ack_outbox("op-1", {"broadcast_id": "c"}, "accepted")
        self.assertEqual(caught.exception.code, PeerErrorCode.OPERATION_CONFLICT)
        with self.assertRaises(BridgeError) as caught:
            self.store.ack_outbox("op-1", {"broadcast_id": "b"}, "rejected")
        self.assertEqual(caught.exception.code, PeerErrorCode.OPERATION_CONFLICT)


class ProtocolC3Tests(unittest.TestCase):
    def test_object_ref_validation(self):
        self.assertEqual(validate_object_ref({**ref(), "extra": 1}), ref())
        for bad in (None, "x", {}, {**ref(), "local_id": ""}, {**ref(), "host_id": 3}):
            with self.assertRaises(BridgeError):
                validate_object_ref(bad)

    def test_normalize_outbox_item(self):
        item = normalize_outbox_item({"operation_id": "o", "method": "agent_broadcast", "payload": {}, "created": 1})
        self.assertIsNone(item["sender_ref"])
        self.assertIsNone(item["seq"])
        self.assertEqual(item["created"], 1.0)
        full = normalize_outbox_item(
            {
                "operation_id": "o",
                "method": "agent_broadcast",
                "payload": {"k": 1},
                "created": 2.5,
                "binding_id": "b",
                "request_id": "r",
                "seq": 4,
                "sender_ref": ref("s"),
                "authority_ref": ref("a"),
                "authorization_revision": 3,
                "fingerprint": "f",
                "group_id": "g",
                "native_id": "n",
            }
        )
        self.assertEqual(full["sender_ref"], ref("s"))
        self.assertEqual(full["authorization_revision"], 3)
        for patch in (
            {"operation_id": ""},
            {"payload": []},
            {"created": "now"},
            {"seq": 0},
            {"sender_ref": {"host_id": "h"}},
            {"authority_ref": "x"},
        ):
            data = {"operation_id": "o", "method": "m", "payload": {}, "created": 1, **patch}
            with self.assertRaises(BridgeError):
                normalize_outbox_item(data)

    def test_normalize_ack_request_params(self):
        params = normalize_ack_request_params({"operation_id": "o", "outcome": "accepted", "result": {"x": 1}})
        self.assertNotIn("operation", params)
        params = normalize_ack_request_params(
            {"operation": operation(), "operation_id": "o", "outcome": "rejected", "result": {}}
        )
        self.assertEqual(params["operation"], operation())
        for bad in (
            {"outcome": "accepted", "result": {}},
            {"operation_id": "o", "outcome": "", "result": {}},
            {"operation_id": "o", "outcome": "accepted", "result": None},
            {"operation_id": "o", "outcome": "accepted", "result": {}, "operation": {"node_id": "n"}},
        ):
            with self.assertRaises(BridgeError):
                normalize_ack_request_params(bad)

    def test_normalize_notify_parent_params(self):
        event = {"seq": 18, "type": "task.completed", "ref": ref("t"), "payload": {}, "created": 1.0}
        base = {"operation": operation(), "parent_ref": ref("p"), "event": event}
        self.assertNotIn("result", normalize_notify_parent_params(base))
        artifact = {"host_id": "h", "artifact_id": "a", "sha256": "s", "size": 10}
        self.assertEqual(normalize_notify_parent_params({**base, "result": artifact})["result"], artifact)
        for patch in (
            {"parent_ref": {"host_id": "h"}},
            {"operation": None},
            {"event": {**event, "ref": {}}},
            {"event": {**event, "seq": -1}},
            {"result": {**artifact, "size": "x"}},
        ):
            with self.assertRaises(BridgeError):
                normalize_notify_parent_params({**base, **patch})


if __name__ == "__main__":
    unittest.main()
