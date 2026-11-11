import sqlite3
import tempfile
import unittest
from pathlib import Path

from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import compute_stream_id
from agent_bridge.store import Store, encode

AUTHORITY = {"host_id": "coordinator", "node_id": "node-coordinator", "state_generation": "gen-c"}
TARGET = {
    "host_id": "gpu-a",
    "node_id": "node-a",
    "state_generation": "gen-a",
    "kind": "bridge",
    "runtime": "",
    "target_id": "session-a",
}
OTHER_TARGET = {**TARGET, "target_id": "session-b"}

LEGACY_PEER_INBOX = """
CREATE TABLE peer_inbox (
 delivery_id TEXT PRIMARY KEY, host_id TEXT NOT NULL, node_id TEXT NOT NULL,
 state_generation TEXT NOT NULL, target_seq INTEGER NOT NULL,
 seq_kind TEXT NOT NULL DEFAULT 'delivery', operation_id TEXT NOT NULL,
 payload TEXT NOT NULL, status TEXT NOT NULL, receipt TEXT, created REAL NOT NULL,
 UNIQUE(host_id,node_id,state_generation,target_seq)
);
CREATE INDEX peer_inbox_operation ON peer_inbox(operation_id);
"""

LEGACY_NODE_OUTBOX = """
CREATE TABLE node_outbox (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT UNIQUE NOT NULL,
 group_id TEXT, native_id TEXT, method TEXT NOT NULL, payload TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', created REAL NOT NULL,
 pulled_at REAL, acked_at REAL
);
CREATE INDEX node_outbox_pending ON node_outbox(status,seq);
"""


class PhaseCStoreMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.path = self.state / "bridge.sqlite"

    def legacy_database(self):
        self.state.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as db:
            db.executescript(LEGACY_PEER_INBOX)
            db.executescript(LEGACY_NODE_OUTBOX)
            db.execute(
                "INSERT INTO peer_inbox VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "legacy-1",
                    "gpu-a",
                    "node-a",
                    "gen-a",
                    3,
                    "delivery",
                    "op-1",
                    encode({"message": "old one"}),
                    "received",
                    encode({"accepted_at": 1.0}),
                    1.0,
                ),
            )
            db.execute(
                "INSERT INTO peer_inbox VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "legacy-2",
                    "gpu-a",
                    "node-a",
                    "gen-a",
                    5,
                    "tombstone",
                    "op-2",
                    encode({"message": "old two"}),
                    "cancelled",
                    None,
                    2.0,
                ),
            )
            db.execute(
                "INSERT INTO node_outbox(operation_id,method,payload,status,created) VALUES(?,?,?,?,?)",
                ("outbox-1", "agent_broadcast", encode({"group_id": "g"}), "pending", 3.0),
            )

    def columns(self, table):
        with Store(self.state).connection() as db:
            return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}

    def test_legacy_peer_inbox_is_rebuilt_without_data_loss(self):
        self.legacy_database()
        store = Store(self.state)

        self.assertIn("ordering_mode", self.columns("peer_inbox"))
        self.assertIn("stream_id", self.columns("peer_inbox"))
        self.assertIn("ack_seq", self.columns("peer_inbox"))
        first = store.get_inbox_delivery("legacy-1")
        second = store.get_inbox_delivery("legacy-2")
        self.assertEqual({"message": "old one"}, first["payload"])
        self.assertEqual({"accepted_at": 1.0}, first["receipt"])
        self.assertEqual("legacy", first["ordering_mode"])
        self.assertIsNone(first["stream_id"])
        self.assertIsNone(first["authority_ref"])
        self.assertEqual(0, first["revision"])
        self.assertEqual("tombstone", second["seq_kind"])
        with store.connection() as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM peer_inbox").fetchone()[0])
            names = {
                row[0]
                for row in db.execute("SELECT name FROM sqlite_master WHERE type='index'")
            }
            self.assertIn("peer_inbox_legacy_seq", names)
            self.assertIn("peer_inbox_ordered_seq", names)
            self.assertIn("peer_inbox_operation", names)
            self.assertIsNone(
                db.execute("SELECT 1 FROM sqlite_master WHERE name='peer_inbox_c'").fetchone()
            )

    def test_legacy_node_outbox_gains_phase_c_columns_and_member_index(self):
        self.legacy_database()
        store = Store(self.state)

        self.assertTrue(
            {
                "binding_id",
                "request_id",
                "sender_ref",
                "authority_ref",
                "authorization_revision",
                "fingerprint",
                "result",
                "outcome",
            }
            <= self.columns("node_outbox")
        )
        with store.connection() as db:
            row = db.execute("SELECT * FROM node_outbox WHERE operation_id='outbox-1'").fetchone()
            self.assertEqual("agent_broadcast", row["method"])
            self.assertEqual(encode({"group_id": "g"}), row["payload"])
            self.assertIsNone(row["binding_id"])
            self.assertIsNone(row["result"])
            index = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='node_outbox_member_request'"
            ).fetchone()
            self.assertIsNotNone(index)
            self.assertIn("WHERE binding_id IS NOT NULL", index["sql"])

    def test_member_request_index_requires_non_null_binding_and_is_unique(self):
        store = Store(self.state)
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO node_outbox(operation_id,method,payload,status,created,binding_id,request_id) "
                "VALUES('a','agent_broadcast','{}','pending',1.0,NULL,NULL)"
            )
            db.execute(
                "INSERT INTO node_outbox(operation_id,method,payload,status,created,binding_id,request_id) "
                "VALUES('b','agent_broadcast','{}','pending',1.0,NULL,NULL)"
            )
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO node_outbox(operation_id,method,payload,status,created,binding_id,request_id) "
                "VALUES('c','agent_broadcast','{}','pending',1.0,'binding-1','request-1')"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            with store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO node_outbox(operation_id,method,payload,status,created,binding_id,request_id) "
                    "VALUES('d','agent_broadcast','{}','pending',1.0,'binding-1','request-1')"
                )

    def test_peer_inbox_ordering_constraints_are_scoped_to_mode(self):
        store = Store(self.state)
        insert = (
            "INSERT INTO peer_inbox(delivery_id,host_id,node_id,state_generation,target_seq,seq_kind,"
            "operation_id,payload,status,created,ordering_mode,stream_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
        )
        with store.connection(write=True) as db:
            db.execute(insert, ("ordered-1", "h", "n", "g", 1, "message", "op-a", "{}", "received", 1.0, "ordered", "s1"))
            db.execute(insert, ("ordered-2", "h", "n", "g", 1, "message", "op-b", "{}", "received", 1.0, "ordered", "s2"))
        with self.assertRaises(sqlite3.IntegrityError):
            with store.connection(write=True) as db:
                db.execute(
                    insert, ("ordered-3", "h", "n", "g", 1, "message", "op-c", "{}", "received", 1.0, "ordered", "s1")
                )
        with self.assertRaises(sqlite3.IntegrityError):
            with store.connection(write=True) as db:
                db.execute(
                    insert, ("ordered-4", "h", "n", "g", 2, "message", "op-d", "{}", "received", 1.0, "ordered", None)
                )


class PhaseCCommitLogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)

    def test_record_commit_assigns_sequential_ack_seq_and_replays_natural_keys(self):
        store = Store(self.state)
        self.assertEqual(1, store.record_commit("open:session-a", "peer.deliver"))
        self.assertEqual(2, store.record_commit("cancel:session-a", "peer.cancel"))
        self.assertEqual(2, store.record_commit("cancel:session-a", "peer.cancel"))
        self.assertEqual(3, store.record_commit("open:session-b", "peer.deliver"))
        with store.connection() as db:
            rows = db.execute(
                "SELECT ack_seq,mutation_key,method FROM peer_commit_log ORDER BY ack_seq"
            ).fetchall()
        self.assertEqual([1, 2, 3], [row["ack_seq"] for row in rows])
        self.assertEqual(["open:session-a", "cancel:session-a", "open:session-b"], [row["mutation_key"] for row in rows])

    def legacy_delivery(self, store, seq):
        from agent_bridge.node_identity import FileNodeIdentityStore

        identity = FileNodeIdentityStore(self.state).load_or_create()
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO peer_inbox(delivery_id,host_id,node_id,state_generation,target_seq,seq_kind,"
                "operation_id,payload,status,created) VALUES(?,?,?,?,?,'message',?,'{}','received',1.0)",
                (f"delivery-{seq}", "h", identity.node_id, identity.state_generation, seq, f"op-{seq}"),
            )

    def test_commit_log_starts_above_verified_identity_floor(self):
        from agent_bridge.node_identity import FileNodeIdentityStore

        store = Store(self.state)
        self.legacy_delivery(store, 7)
        FileNodeIdentityStore(self.state).advance(7)
        store = Store(self.state)
        self.assertEqual(8, store.record_commit("after-floor", "peer.deliver"))
        reopened = Store(self.state)
        self.assertEqual(9, reopened.record_commit("after-reopen", "peer.deliver"))
        with reopened.connection() as db:
            floors = db.execute(
                "SELECT ack_seq FROM peer_commit_log WHERE mutation_key='migration_floor'"
            ).fetchall()
        self.assertEqual([7], [row["ack_seq"] for row in floors])

    def test_legacy_hash_floor_is_preserved_above_an_existing_commit_log(self):
        from agent_bridge.node_identity import FileNodeIdentityStore

        store = Store(self.state)
        self.assertEqual(1, store.record_commit("old-global-commit", "peer.workspace"))
        legacy_hash = 3156441702022342537
        self.legacy_delivery(store, legacy_hash)
        identity = FileNodeIdentityStore(self.state)
        identity.advance(legacy_hash)
        reopened = Store(self.state)
        self.assertEqual(legacy_hash + 1, reopened.record_commit("new-deliver", "peer.deliver"))
        self.assertEqual("ready", identity.load_or_create().state.value)
        self.assertEqual(legacy_hash, identity.load_or_create().max_confirmed_seq)

    def test_identity_floor_without_a_matching_legacy_delivery_marks_rollback(self):
        from agent_bridge.node_identity import FileNodeIdentityStore

        store = Store(self.state)
        self.legacy_delivery(store, 4)
        store.record_commit("older", "peer.workspace")
        identity = FileNodeIdentityStore(self.state)
        identity.advance(5)
        reopened = Store(self.state)
        self.assertEqual(1, reopened.max_peer_commit_seq())
        self.assertEqual("state_corrupted", identity.load_or_create().state.value)
        Store(self.state)
        self.assertEqual(1, reopened.max_peer_commit_seq())

    def test_empty_restored_database_cannot_seed_from_identity_file(self):
        from agent_bridge.node_identity import FileNodeIdentityStore

        identity = FileNodeIdentityStore(self.state)
        identity.advance(5)
        restored = Store(self.state)
        self.assertEqual(0, restored.max_peer_commit_seq())
        self.assertEqual("state_corrupted", identity.load_or_create().state.value)

    def test_migration_never_clears_an_already_corrupted_identity(self):
        from agent_bridge.node_identity import FileNodeIdentityStore

        store = Store(self.state)
        self.legacy_delivery(store, 5)
        identity = FileNodeIdentityStore(self.state)
        identity.advance(5)
        with self.assertRaises(BridgeError):
            identity.verify_database(4)
        reopened = Store(self.state)
        self.assertEqual(0, reopened.max_peer_commit_seq())
        self.assertEqual("state_corrupted", identity.load_or_create().state.value)

    def test_commit_log_starts_above_existing_acked_sequence(self):
        store = Store(self.state)
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO peer_inbox(delivery_id,host_id,node_id,state_generation,target_seq,seq_kind,"
                "operation_id,payload,status,created,ordering_mode,stream_id,ack_seq) "
                "VALUES('d1','h','n','g',1,'message','op','{}','received',1.0,'ordered','s1',9)"
            )
        migrated = Store(self.state)
        self.assertEqual(10, migrated.record_commit("after-acked", "peer.deliver"))

    def test_record_commit_rejects_invalid_keys(self):
        store = Store(self.state)
        for key, method in (("", "peer.deliver"), ("key", ""), (None, "peer.deliver"), ("key", None)):
            with self.assertRaises(BridgeError):
                store.record_commit(key, method)


class PhaseCStreamTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = Store(self.state)

    def test_target_stream_is_created_once_and_allocates_sequential_sequences(self):
        stream = self.store.get_or_create_target_stream(AUTHORITY, TARGET)
        self.assertEqual(compute_stream_id(AUTHORITY, TARGET), stream["stream_id"])
        self.assertEqual(1, stream["next_seq"])
        self.assertEqual(0, stream["released_seq"])
        self.assertEqual(AUTHORITY, stream["authority_ref"])
        self.assertEqual(TARGET, stream["target_ref"])
        again = self.store.get_or_create_target_stream(AUTHORITY, TARGET)
        self.assertEqual(stream, again)
        different = self.store.get_or_create_target_stream(AUTHORITY, OTHER_TARGET)
        self.assertNotEqual(stream["stream_id"], different["stream_id"])

        self.assertEqual([1, 2, 3], [self.store.allocate_target_seq(stream["stream_id"]) for _ in range(3)])
        self.store.advance_released_seq(stream["stream_id"], 2)
        self.store.advance_released_seq(stream["stream_id"], 1)
        row = self.store.get_or_create_target_stream(AUTHORITY, TARGET)
        self.assertEqual(4, row["next_seq"])
        self.assertEqual(2, row["released_seq"])
        self.assertEqual(4, self.store.allocate_target_seq(stream["stream_id"]))
        with self.assertRaisesRegex(BridgeError, "below the allocated next_seq"):
            self.store.advance_released_seq(stream["stream_id"], 5)

    def test_target_stream_rejects_unknown_stream_and_invalid_refs(self):
        with self.assertRaisesRegex(BridgeError, "target_streams"):
            self.store.allocate_target_seq("missing")
        with self.assertRaisesRegex(BridgeError, "target_streams"):
            self.store.advance_released_seq("missing", 1)
        with self.assertRaises(BridgeError):
            self.store.get_or_create_target_stream(AUTHORITY, {"kind": "bridge"})
        with self.assertRaises(BridgeError):
            self.store.get_or_create_target_stream("not-json", TARGET)

    def test_incoming_stream_progress_is_monotonic(self):
        stream = self.store.get_or_create_incoming_stream(AUTHORITY, TARGET)
        self.assertEqual(compute_stream_id(AUTHORITY, TARGET), stream["stream_id"])
        self.assertEqual(0, stream["admitted_seq"])
        self.assertEqual(stream, self.store.get_or_create_incoming_stream(AUTHORITY, TARGET))
        self.store.advance_admitted_seq(stream["stream_id"], 1)
        self.store.advance_admitted_seq(stream["stream_id"], 3)
        self.store.advance_admitted_seq(stream["stream_id"], 2)
        row = self.store.get_or_create_incoming_stream(AUTHORITY, TARGET)
        self.assertEqual(3, row["admitted_seq"])
        with self.assertRaisesRegex(BridgeError, "incoming_streams"):
            self.store.advance_admitted_seq("missing", 1)


class PhaseCSchemaIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))

    def test_phase_c_tables_and_foreign_keys_exist(self):
        expected = {
            "remote_group_members",
            "group_broadcasts",
            "broadcast_recipients",
            "target_streams",
            "peer_outbox",
            "delivery_resolutions",
            "pulled_request_receipts",
            "incoming_streams",
            "ordered_targets",
            "peer_commit_log",
            "native_send_queue",
            "native_target_counters",
            "member_bindings",
        }
        with self.store.connection() as db:
            tables = {
                row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            self.assertTrue(expected <= tables)
            references = {
                table: {row[2] for row in db.execute(f"PRAGMA foreign_key_list({table})")}
                for table in (
                    "remote_group_members",
                    "group_broadcasts",
                    "broadcast_recipients",
                    "peer_outbox",
                    "delivery_resolutions",
                )
            }
            self.assertIn("session_groups", references["remote_group_members"])
            self.assertIn("session_groups", references["group_broadcasts"])
            self.assertIn("group_broadcasts", references["broadcast_recipients"])
            self.assertIn("target_streams", references["peer_outbox"])
            self.assertIn("group_broadcasts", references["peer_outbox"])
            self.assertIn("peer_outbox", references["delivery_resolutions"])
            constraints = {
                row[0]
                for row in db.execute("SELECT name FROM sqlite_master WHERE type='index'")
            }
            self.assertTrue({"peer_outbox_head", "native_send_queue_head"} <= constraints)

    def test_foreign_key_chain_and_checks_are_enforced(self):
        stream = self.store.get_or_create_target_stream(AUTHORITY, TARGET)
        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO session_groups(id,revision,deleted,created) VALUES('group-1',0,0,1.0)"
            )
            db.execute(
                "INSERT INTO group_broadcasts(id,group_id,group_revision,message,recipient_snapshot,"
                "request_key,created) VALUES('broadcast-1','group-1',0,'hello','[]','request-1',1.0)"
            )
            db.execute(
                "INSERT INTO peer_outbox(delivery_id,broadcast_id,group_id,group_revision,stream_id,target_seq,"
                "host_id,node_id,state_generation,target_kind,runtime,target_id,operation_id,payload,"
                "fingerprint,seq_kind,created,updated) "
                "VALUES('delivery-1','broadcast-1','group-1',0,?,1,'gpu-a','node-a','gen-a','bridge','',"
                "'session-a','op-1','{}','fp','message',1.0,1.0)",
                (stream["stream_id"],),
            )
            db.execute(
                "INSERT INTO delivery_resolutions(id,delivery_id,request_id,fingerprint,group_revision,"
                "delivery_revision,remote_revision,request_json,resolution,evidence,actor,status,created,updated) "
                "VALUES('resolution-1','delivery-1','request-2','fp',0,0,0,'{}','received','checked','admin',"
                "'pending',1.0,1.0)"
            )
            self.assertEqual(0, len(db.execute("PRAGMA foreign_key_check").fetchall()))
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO peer_outbox(delivery_id,stream_id,target_seq,host_id,node_id,state_generation,"
                    "target_kind,runtime,target_id,operation_id,payload,fingerprint,seq_kind,created,updated) "
                    "VALUES('delivery-2','missing',1,'gpu-a','node-a','gen-a','bridge','','session-a',"
                    "'op-2','{}','fp','message',1.0,1.0)"
                )
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store.connection(write=True) as db:
                db.execute("INSERT INTO target_streams(stream_id,authority_ref,target_ref,next_seq,released_seq) "
                           "VALUES('bad','{}','{}',1,1)")


if __name__ == "__main__":
    unittest.main()
