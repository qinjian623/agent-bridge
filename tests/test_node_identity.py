import json
import os
import stat
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import (
    IDENTITY_FILE_MODE,
    IDENTITY_FILENAME,
    FileNodeIdentityStore,
)
from agent_bridge.peer_protocol import NodeRef, NodeState, PeerErrorCode


class FileNodeIdentityStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = FileNodeIdentityStore(self.state)

    def test_path_is_the_identity_file_in_the_state_directory(self):
        self.assertEqual(self.store.path(), self.state / IDENTITY_FILENAME)

    def test_load_or_create_makes_a_private_atomic_identity(self):
        identity = self.store.load_or_create()

        self.assertEqual(identity.max_confirmed_seq, 0)
        self.assertEqual(identity.state, NodeState.READY)
        self.assertEqual(identity.schema_version, 1)
        uuid.UUID(identity.node_id)
        int(identity.state_generation, 16)
        self.assertEqual(stat.S_IMODE(self.store.path().stat().st_mode), IDENTITY_FILE_MODE)
        self.assertEqual(list(self.state.glob(f".{IDENTITY_FILENAME}.*")), [])

        raw = json.loads(self.store.path().read_text(encoding="utf-8"))
        self.assertEqual(raw["state"], "ready")
        self.assertEqual(raw["node_id"], identity.node_id)
        self.assertEqual(raw["max_confirmed_seq"], 0)

        self.assertEqual(self.store.load_or_create(), identity)
        self.assertEqual(FileNodeIdentityStore(self.state).load_or_create(), identity)

    def test_create_mode_is_0600_even_with_a_restrictive_umask(self):
        previous = os.umask(0o777)
        try:
            self.store.load_or_create()
        finally:
            os.umask(previous)
        self.assertEqual(stat.S_IMODE(self.store.path().stat().st_mode), IDENTITY_FILE_MODE)

    def test_rejects_identity_file_with_wrong_permissions(self):
        self.store.load_or_create()
        self.store.path().chmod(0o644)

        with self.assertRaisesRegex(BridgeError, "0600") as caught:
            FileNodeIdentityStore(self.state).load_or_create()
        self.assertEqual(caught.exception.code, "invalid_identity")

    def test_rejects_identity_file_owned_by_another_user(self):
        self.store.load_or_create()

        with patch("agent_bridge.node_identity.os.getuid", return_value=os.getuid() + 1):
            with self.assertRaisesRegex(BridgeError, "owned") as caught:
                FileNodeIdentityStore(self.state).load_or_create()
        self.assertEqual(caught.exception.code, "invalid_identity")

    def test_rejects_malformed_identity_file(self):
        path = self.state / IDENTITY_FILENAME
        path.write_text("{not json", encoding="utf-8")
        path.chmod(IDENTITY_FILE_MODE)

        with self.assertRaises(BridgeError) as caught:
            self.store.load_or_create()
        self.assertEqual(caught.exception.code, "invalid_identity")

    def test_advance_only_raises_the_sequence_and_persists_it(self):
        self.assertEqual(self.store.advance(7).max_confirmed_seq, 7)
        self.assertEqual(self.store.advance(3).max_confirmed_seq, 7)
        self.assertEqual(self.store.advance(7).max_confirmed_seq, 7)
        self.assertEqual(FileNodeIdentityStore(self.state).load_or_create().max_confirmed_seq, 7)

        with self.assertRaises(BridgeError) as caught:
            self.store.advance(-1)
        self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)

    def test_verify_database_detects_rollback_and_persists_corruption(self):
        self.store.advance(5)

        with self.assertRaisesRegex(BridgeError, "Database rollback detected") as caught:
            self.store.verify_database(4)
        self.assertEqual(caught.exception.code, PeerErrorCode.STATE_CORRUPTED)

        self.assertEqual(self.store.load_or_create().state, NodeState.STATE_CORRUPTED)
        self.assertEqual(FileNodeIdentityStore(self.state).load_or_create().state, NodeState.STATE_CORRUPTED)
        self.assertEqual(self.store.verify_database(5).state, NodeState.STATE_CORRUPTED)

    def test_verify_database_accepts_an_equal_or_newer_database(self):
        self.store.advance(3)

        self.assertEqual(self.store.verify_database(3).state, NodeState.READY)
        self.assertEqual(self.store.verify_database(9).state, NodeState.READY)
        self.assertEqual(self.store.load_or_create().state, NodeState.READY)

    def test_verify_database_compares_coordinator_sequence_as_an_additional_floor(self):
        self.store.advance(3)
        self.assertEqual(NodeState.READY, self.store.verify_database(5, last_acked_seq=2).state)
        with self.assertRaises(BridgeError) as caught:
            self.store.verify_database(5, last_acked_seq=6)
        self.assertEqual(PeerErrorCode.STATE_CORRUPTED, caught.exception.code)
        self.assertEqual(NodeState.STATE_CORRUPTED, self.store.load_or_create().state)

    def test_independent_instances_cannot_overwrite_a_newer_confirmed_sequence(self):
        self.store.load_or_create()
        other = FileNodeIdentityStore(self.state)
        entered = threading.Event()
        release = threading.Event()
        newer_done = threading.Event()
        real_write = self.store._write

        def slow_write(identity):
            entered.set()
            if not release.wait(5):
                raise AssertionError("Timed out waiting to complete the older write")
            real_write(identity)

        def newer_write():
            try:
                return other.advance(2)
            finally:
                newer_done.set()

        with patch.object(self.store, "_write", side_effect=slow_write), ThreadPoolExecutor(max_workers=2) as pool:
            older = pool.submit(self.store.advance, 1)
            self.assertTrue(entered.wait(5))
            newer = pool.submit(newer_write)
            try:
                self.assertFalse(newer_done.wait(0.05))
            finally:
                release.set()
            older.result(timeout=5)
            newer.result(timeout=5)
        self.assertEqual(2, self.store.load_or_create().max_confirmed_seq)

    def test_rotate_generation_requires_the_current_node_id(self):
        self.store.load_or_create()

        with self.assertRaises(BridgeError) as caught:
            self.store.rotate_generation("wrong-node")
        self.assertEqual(caught.exception.code, PeerErrorCode.IDENTITY_MISMATCH)

    def test_rotate_generation_recovers_and_preserves_the_sequence(self):
        identity = self.store.load_or_create()
        self.store.advance(4)
        with self.assertRaises(BridgeError):
            self.store.verify_database(1)

        rotated = self.store.rotate_generation(identity.node_id)

        self.assertEqual(rotated.node_id, identity.node_id)
        self.assertNotEqual(rotated.state_generation, identity.state_generation)
        self.assertEqual(rotated.max_confirmed_seq, 4)
        self.assertEqual(rotated.state, NodeState.READY)
        self.assertEqual(FileNodeIdentityStore(self.state).load_or_create(), rotated)

    def test_node_ref_tracks_the_current_generation(self):
        identity = self.store.load_or_create()
        self.assertEqual(
            self.store.node_ref("gpu-a"),
            NodeRef(host_id="gpu-a", node_id=identity.node_id, state_generation=identity.state_generation),
        )

        rotated = self.store.rotate_generation(identity.node_id)
        self.assertEqual(
            self.store.node_ref("gpu-a"),
            NodeRef(host_id="gpu-a", node_id=identity.node_id, state_generation=rotated.state_generation),
        )


if __name__ == "__main__":
    unittest.main()
