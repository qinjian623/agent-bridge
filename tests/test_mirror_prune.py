import contextlib
import fcntl
import hashlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from agent_bridge.cli import build_parser, main
from agent_bridge.config import Profile
from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import IDENTITY_FILENAME, FileNodeIdentityStore
from agent_bridge.peer_protocol import DeliverySource
from agent_bridge.store import Store, encode

HOST_A = "host-a"
HOST_B = "host-b"
NODE_A = "node-a"
NODE_B = "node-b"
GEN_A = "gen-a"
GEN_B = "gen-b"


def _raw_event(
    *,
    host_id: str = HOST_A,
    node_id: str = NODE_A,
    generation: str = GEN_A,
    seq: int = 1,
    local_id: str = "task-1",
    kind: str = "tool_completed",
    payload: dict | None = None,
    created: float = 100.0,
) -> dict:
    return {
        "seq": seq,
        "type": kind,
        "ref": {
            "host_id": host_id,
            "node_id": node_id,
            "state_generation": generation,
            "local_id": local_id,
        },
        "payload": payload if payload is not None else {"ok": True, "text": "x" * 64},
        "created": created,
    }


def _wrap_hop(inner_event: dict, *, host_id: str, node_id: str, generation: str, seq: int) -> dict:
    return {
        "seq": seq,
        "type": inner_event["type"],
        "ref": {
            "host_id": host_id,
            "node_id": node_id,
            "state_generation": generation,
            "local_id": str(seq),
        },
        "payload": {
            "source": inner_event["ref"],
            "remote_seq": inner_event["seq"],
            "type": inner_event["type"],
            "payload": inner_event["payload"],
            "created": inner_event["created"],
        },
        "created": inner_event["created"] + 1.0,
    }


def _nested_echo_event(origin_event: dict, hops: int, *, base_seq: int = 1000) -> dict:
    current = origin_event
    for step in range(hops):
        if step % 2 == 0:
            host_id, node_id, generation = HOST_B, NODE_B, GEN_B
        else:
            host_id, node_id, generation = HOST_A, NODE_A, GEN_A
        current = _wrap_hop(
            current,
            host_id=host_id,
            node_id=node_id,
            generation=generation,
            seq=base_seq + step,
        )
    return current


def _insert_legacy_mirror(
    store: Store,
    peer_event: dict,
    *,
    origin_node_id: str | None = None,
    origin_state_generation: str | None = None,
    origin_seq: int | None = None,
    origin_fingerprint: str | None = None,
) -> int:
    source = peer_event["ref"]
    fingerprint = hashlib.sha256(encode(peer_event).encode("utf-8")).hexdigest()
    data = {
        "source": source,
        "remote_seq": peer_event["seq"],
        "type": peer_event["type"],
        "payload": peer_event["payload"],
        "created": peer_event["created"],
    }
    with store.connection(write=True) as db:
        local_seq = store.event(db, peer_event["type"], data)
        db.execute(
            "INSERT INTO peer_event_mirrors("
            "host_id,node_id,state_generation,remote_seq,local_seq,fingerprint,created,"
            "origin_node_id,origin_state_generation,origin_seq,origin_fingerprint"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                source["host_id"],
                source["node_id"],
                source["state_generation"],
                peer_event["seq"],
                local_seq,
                fingerprint,
                peer_event["created"],
                origin_node_id,
                origin_state_generation,
                origin_seq,
                origin_fingerprint,
            ),
        )
    return local_seq


class MirrorPruneTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = Store(self.state)

    def test_dry_run_is_non_mutating_and_reports_accurate_plan(self):
        identity = FileNodeIdentityStore(self.state).load_or_create()
        with self.store.connection(write=True) as db:
            local_only_seq = self.store.event(db, "task_queued", {"task": "local-work"})

        origin = _raw_event(seq=5, payload={"blob": "a" * 300})
        direct_seq = _insert_legacy_mirror(
            self.store,
            origin,
            origin_node_id=NODE_A,
            origin_state_generation=GEN_A,
            origin_seq=5,
            origin_fingerprint="fp-direct",
        )
        echo_1 = _wrap_hop(origin, host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=50)
        echo_1_seq = _insert_legacy_mirror(self.store, echo_1)
        self_echo = _wrap_hop(
            _raw_event(host_id="local", node_id=identity.node_id, generation=identity.state_generation, seq=1),
            host_id=HOST_B,
            node_id=NODE_B,
            generation=GEN_B,
            seq=51,
        )
        self_echo_seq = _insert_legacy_mirror(self.store, self_echo)

        summary = self.store.prune_duplicate_mirrors(dry_run=True, vacuum=True, batch_size=1)

        self.assertTrue(summary["dry_run"])
        self.assertTrue(summary["vacuum"])
        self.assertFalse(summary["vacuumed"])
        self.assertFalse(summary["broker_running"])
        self.assertEqual(identity.node_id, summary["local_node_id"])
        self.assertEqual(3, summary["total_mirrors"])
        self.assertEqual(1, summary["kept_mirrors"])
        self.assertEqual(0, summary["protected_mirrors"])
        self.assertEqual(2, summary["pruned_mirrors"])
        self.assertEqual(1, summary["pruned_duplicates"])
        self.assertEqual(1, summary["pruned_self_origins"])
        self.assertEqual(1, summary["repaired_origins"])
        self.assertEqual(2, summary["pruned_events"])
        self.assertGreater(summary["reclaimable_data_bytes"], 400)

        with self.store.connection() as db:
            self.assertEqual(3, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])
            seqs = {row[0] for row in db.execute("SELECT seq FROM events").fetchall()}
        self.assertEqual({local_only_seq, direct_seq, echo_1_seq, self_echo_seq}, seqs)

    def test_prefers_direct_origin_even_if_wrapped_copy_arrived_first(self):
        origin = _raw_event(seq=9, payload={"blob": "payload" * 40})
        wrapped_first = _nested_echo_event(origin, 3, base_seq=200)
        wrapped_first_seq = _insert_legacy_mirror(self.store, wrapped_first)
        direct_second_seq = _insert_legacy_mirror(self.store, origin)

        result = self.store.prune_duplicate_mirrors(dry_run=False, batch_size=1)

        self.assertEqual(2, result["total_mirrors"])
        self.assertEqual(1, result["kept_mirrors"])
        self.assertEqual(1, result["pruned_mirrors"])
        self.assertEqual(1, result["pruned_duplicates"])
        self.assertEqual(1, result["pruned_events"])

        with self.store.connection() as db:
            remaining = db.execute("SELECT local_seq, remote_seq, origin_node_id, origin_seq FROM peer_event_mirrors").fetchall()
            event_seqs = [row[0] for row in db.execute("SELECT seq FROM events").fetchall()]
        self.assertEqual(1, len(remaining))
        self.assertEqual(direct_second_seq, remaining[0]["local_seq"])
        self.assertEqual(9, remaining[0]["remote_seq"])
        self.assertEqual(NODE_A, remaining[0]["origin_node_id"])
        self.assertEqual(9, remaining[0]["origin_seq"])
        self.assertEqual([direct_second_seq], event_seqs)
        self.assertNotEqual(wrapped_first_seq, remaining[0]["local_seq"])

    def test_repairs_depth_above_32_truncated_origins_and_distinguishes_from_self_origin(self):
        identity = FileNodeIdentityStore(self.state).load_or_create()
        remote_origin = _raw_event(host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=17, payload={"k": "v"})

        # Simulate a Mac v0.2.6 row whose 40-hop nesting stopped at depth 32 on an intermediate hop matching local node_id.
        deep_wrapped = _nested_echo_event(remote_origin, 40, base_seq=1000)
        deep_seq = _insert_legacy_mirror(
            self.store,
            deep_wrapped,
            origin_node_id=identity.node_id,
            origin_state_generation=identity.state_generation,
            origin_seq=8888,
            origin_fingerprint="truncated-depth-32-fp",
        )

        # Standalone depth-40 row with no direct copy must be REPAIRED and KEPT, NOT deleted as self-origin.
        repaired = self.store.prune_duplicate_mirrors(dry_run=False)
        self.assertEqual(1, repaired["kept_mirrors"])
        self.assertEqual(0, repaired["pruned_mirrors"])
        self.assertEqual(0, repaired["pruned_self_origins"])
        self.assertEqual(1, repaired["repaired_origins"])

        expected_fp = hashlib.sha256(encode(remote_origin).encode("utf-8")).hexdigest()
        row = self.store.get_mirrored_event_by_origin(NODE_B, GEN_B, 17)
        self.assertIsNotNone(row)
        self.assertEqual(deep_seq, row["local_seq"])
        self.assertEqual(expected_fp, row["origin_fingerprint"])

        # Now add an earlier/direct copy and verify the depth-40 duplicate IS pruned cleanly.
        direct_seq = _insert_legacy_mirror(self.store, remote_origin)
        pruned = self.store.prune_duplicate_mirrors(dry_run=False)
        self.assertEqual(2, pruned["total_mirrors"])
        self.assertEqual(1, pruned["kept_mirrors"])
        self.assertEqual(1, pruned["pruned_duplicates"])
        kept = self.store.get_mirrored_event_by_origin(NODE_B, GEN_B, 17)
        self.assertEqual(direct_seq, kept["local_seq"])

    def test_preserves_deliveries_mirrored_notifications_native_queue_and_cursors(self):
        identity = FileNodeIdentityStore(self.state).load_or_create()
        self.store.advance_peer_event_cursor(HOST_A, NODE_A, GEN_A, 999)
        self.store.parent_control("p1", "parent_register", 0, "register-p1", "native-p1", str(self.state))

        origin = _raw_event(seq=11)
        unprotected_direct_seq = _insert_legacy_mirror(self.store, origin)
        delivered_echo = _wrap_hop(origin, host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=101)
        delivered_seq = _insert_legacy_mirror(self.store, delivered_echo)
        notified_echo = _wrap_hop(origin, host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=102)
        notified_seq = _insert_legacy_mirror(self.store, notified_echo)
        queued_echo = _wrap_hop(origin, host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=103)
        queued_seq = _insert_legacy_mirror(self.store, queued_echo)

        # Even a self-origin mirror referenced by deliveries must never be pruned.
        self_origin = _raw_event(
            host_id="local", node_id=identity.node_id, generation=identity.state_generation, seq=4
        )
        protected_self_seq = _insert_legacy_mirror(
            self.store, _wrap_hop(self_origin, host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=104)
        )
        unprotected_self_seq = _insert_legacy_mirror(
            self.store, _wrap_hop(self_origin, host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=105)
        )

        with self.store.connection(write=True) as db:
            db.execute("INSERT INTO deliveries(event_seq,parent_id) VALUES(?,'p1')", (delivered_seq,))
            db.execute(
                "INSERT INTO mirrored_parent_notifications(local_seq,parent_ref,operation_id,status,receipt,updated) "
                "VALUES(?,?,?,?,?,?)",
                (notified_seq, "parent-ref", "op-1", "received", "{}", 1.0),
            )
            db.execute(
                "INSERT INTO native_send_queue("
                "id,runtime,native_id,local_seq,delivery_source,delivery_id,message,status,created"
                ") VALUES(?,?,?,?,?,?,?,?,?)",
                ("q1", "codex", "native-1", 1, str(DeliverySource.PARENT_NOTIFICATION), str(queued_seq), "hi", "pending", 1.0),
            )
            db.execute("INSERT INTO deliveries(event_seq,parent_id) VALUES(?,'p1')", (protected_self_seq,))

        result = self.store.prune_duplicate_mirrors(dry_run=False, batch_size=2)

        self.assertEqual(6, result["total_mirrors"])
        self.assertEqual(4, result["protected_mirrors"])
        self.assertEqual(4, result["kept_mirrors"])
        self.assertEqual(2, result["pruned_mirrors"])
        self.assertEqual(1, result["pruned_duplicates"])
        self.assertEqual(1, result["pruned_self_origins"])

        with self.store.connection() as db:
            remaining = {row[0] for row in db.execute("SELECT local_seq FROM peer_event_mirrors").fetchall()}
            event_seqs = {row[0] for row in db.execute("SELECT seq FROM events").fetchall()}
        self.assertEqual({delivered_seq, notified_seq, queued_seq, protected_self_seq}, remaining)
        self.assertEqual({delivered_seq, notified_seq, queued_seq, protected_self_seq}, event_seqs)
        self.assertNotIn(unprotected_direct_seq, remaining)
        self.assertNotIn(unprotected_self_seq, remaining)
        self.assertEqual(999, self.store.get_peer_event_cursor(HOST_A, NODE_A, GEN_A))

    def test_keep_self_origins_retains_one_copy_and_does_not_create_identity_file(self):
        self.assertFalse((self.state / IDENTITY_FILENAME).exists())
        self_node = "explicit-self-node"
        base = _raw_event(host_id="local", node_id=self_node, generation=GEN_A, seq=1)
        seq_1 = _insert_legacy_mirror(self.store, base)
        _insert_legacy_mirror(self.store, _wrap_hop(base, host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=20))

        result = self.store.prune_duplicate_mirrors(
            dry_run=False,
            prune_self_origins=False,
            local_node_id=self_node,
        )

        self.assertFalse((self.state / IDENTITY_FILENAME).exists())
        self.assertEqual(2, result["total_mirrors"])
        self.assertEqual(1, result["kept_mirrors"])
        self.assertEqual(1, result["pruned_mirrors"])
        self.assertEqual(1, result["pruned_duplicates"])
        self.assertEqual(0, result["pruned_self_origins"])
        with self.store.connection() as db:
            remaining = [row[0] for row in db.execute("SELECT local_seq FROM peer_event_mirrors").fetchall()]
        self.assertEqual([seq_1], remaining)

    def test_vacuum_shrinks_database_and_preserves_autoincrement_after_tail_delete(self):
        origin = _raw_event(seq=1, payload={"blob": "z" * 4000})
        first_seq = _insert_legacy_mirror(self.store, origin)
        last_seq = first_seq
        for index in range(120):
            echo = _nested_echo_event(origin, 4, base_seq=500 + index * 10)
            last_seq = _insert_legacy_mirror(self.store, echo)

        checkpoint_conn = sqlite3.connect(self.store.path, isolation_level=None)
        try:
            checkpoint_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            checkpoint_conn.close()

        result = self.store.prune_duplicate_mirrors(dry_run=False, vacuum=True, batch_size=25)

        self.assertTrue(result["vacuumed"])
        self.assertEqual(121, result["total_mirrors"])
        self.assertEqual(1, result["kept_mirrors"])
        self.assertEqual(120, result["pruned_mirrors"])
        self.assertEqual(120, result["pruned_events"])
        self.assertLess(result["db_bytes_after"], result["db_bytes_before"])
        self.assertEqual(0, result["wal_bytes_after"])

        # Tail mirrored rows (> first_seq .. last_seq) were deleted; next AUTOINCREMENT seq must still be > last_seq.
        with self.store.connection(write=True) as db:
            next_seq = self.store.event(db, "task_queued", {"ok": True})
        self.assertGreater(next_seq, last_seq)

    def test_vacuum_refuses_when_broker_or_worker_lock_is_held(self):
        origin = _raw_event(seq=1)
        _insert_legacy_mirror(self.store, origin)
        _insert_legacy_mirror(self.store, _wrap_hop(origin, host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=2))

        broker_lock = (self.state / "broker.lock").open("a+")
        self.addCleanup(broker_lock.close)
        fcntl.flock(broker_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

        dry = self.store.prune_duplicate_mirrors(dry_run=True, vacuum=True)
        self.assertTrue(dry["broker_running"])
        self.assertFalse(dry["vacuumed"])

        with self.assertRaises(BridgeError) as caught:
            self.store.prune_duplicate_mirrors(dry_run=False, vacuum=True)
        self.assertEqual("broker_running", caught.exception.code)
        with self.store.connection() as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])

        # Release broker lock, hold an active worker lock instead.
        broker_lock.close()
        sid = self.store.open(Profile("test", "codex", "m"), str(self.state), "read-only", "k")["session_id"]
        worker_lock = (self.state / f"{sid}.lock").open("a+")
        self.addCleanup(worker_lock.close)
        fcntl.flock(worker_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

        with self.assertRaises(BridgeError) as worker_caught:
            self.store.prune_duplicate_mirrors(dry_run=False, vacuum=True)
        self.assertEqual("worker_running", worker_caught.exception.code)
        with self.store.connection() as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])

    def test_invalid_batch_size_rejected(self):
        for bad in (0, -3, True):
            with self.assertRaises(BridgeError) as caught:
                self.store.prune_duplicate_mirrors(batch_size=bad)
            self.assertEqual("peer_invalid_params", caught.exception.code)

    def test_cli_mirrors_prune_surface(self):
        parser = build_parser()
        args = parser.parse_args(["mirrors", "prune", "--dry-run", "--vacuum", "--keep-self-origins", "--batch-size", "25"])
        self.assertEqual(
            ("mirrors", "prune", True, True, True, False, 25),
            (
                args.command,
                args.mirrors_command,
                args.dry_run,
                args.vacuum,
                args.keep_self_origins,
                args.no_repair_origins,
                args.batch_size,
            ),
        )

        origin = _raw_event(seq=2, payload={"blob": "q" * 2048})
        _insert_legacy_mirror(self.store, origin)
        for index in range(10):
            _insert_legacy_mirror(self.store, _nested_echo_event(origin, 3, base_seq=100 + index * 10))

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            main(["--state", str(self.state), "mirrors", "prune", "--dry-run"])
        dry_payload = json.loads(stdout.getvalue())
        self.assertTrue(dry_payload["dry_run"])
        self.assertEqual(10, dry_payload["pruned_mirrors"])

        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "agent_bridge.cli",
                "--state",
                str(self.state),
                "mirrors",
                "prune",
                "--vacuum",
                "--batch-size",
                "4",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, proc.returncode, proc.stderr)
        real_payload = json.loads(proc.stdout)
        self.assertFalse(real_payload["dry_run"])
        self.assertTrue(real_payload["vacuumed"])
        self.assertEqual(10, real_payload["pruned_mirrors"])
        self.assertEqual(1, real_payload["kept_mirrors"])

        # Running CLI with --vacuum while broker.lock is held returns non-zero cleanly without deleting rows.
        broker_lock = os.open(self.state / "broker.lock", os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, broker_lock)
        fcntl.flock(broker_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        blocked = subprocess.run(
            [sys.executable, "-m", "agent_bridge.cli", "--state", str(self.state), "mirrors", "prune", "--vacuum"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(0, blocked.returncode)
        self.assertIn("broker is running", blocked.stderr)


if __name__ == "__main__":
    unittest.main()
