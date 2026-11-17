from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent_bridge.config import Config, normalize_projects
from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import (
    MAX_WORKSPACE_CHUNK_BYTES,
    PeerErrorCode,
    compute_input_digest,
    compute_manifest_digest,
    normalize_inspect_params,
    normalize_manifest,
    normalize_workspace_params,
)
from agent_bridge.store import Store

SHA_A = "a" * 64
SHA_B = "b" * 64
OPERATION = {"node_id": "n", "state_generation": "g", "operation_id": "op"}
AUTHORITY = {"host_id": "h", "node_id": "n", "state_generation": "g", "local_id": "l"}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def prepare_params(**override):
    params = {
        "operation": OPERATION, "action": "prepare", "workspace_id": "ws-1", "project_id": "bridge",
        "manifest_sha256": SHA_A, "manifest_size": 10, "delivery_mode": "patch", "source_base_commit": "abc",
        "estimated_total_bytes": 0, "authority_ref": AUTHORITY,
    }
    params.update(override)
    return params


def chunk_params(payload=b"hello", **override):
    params = {
        "operation": OPERATION, "action": "chunk", "workspace_id": "ws-1", "blob_sha256": SHA_A, "offset": 0,
        "size": len(payload), "chunk_sha256": sha(payload), "data": base64.b64encode(payload).decode(),
    }
    params.update(override)
    return params


def manifest(**override):
    value = {
        "schema": 1, "workspace_id": "ws-1", "project_id": "bridge", "workspace_mode": "git",
        "source_base_commit": "abc", "delivery_mode": "snapshot", "selected_files": ["src/a.py", "old.py"],
        "input_digest": "x",
        "files": [
            {"path": "src/a.py", "op": "put", "size": 3, "before_sha256": SHA_B, "sha256": SHA_A, "mode": 0o644},
            {"path": "old.py", "op": "delete", "before_sha256": SHA_B},
        ],
        "patch": None, "total_bytes": 3, "file_count": 2,
    }
    value.update(override)
    return value


class WorkspaceParamsTest(unittest.TestCase):
    def assertInvalid(self, params):
        with self.assertRaises(BridgeError) as caught:
            normalize_workspace_params(params)
        self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)

    def test_prepare_valid_and_defaults(self):
        result = normalize_workspace_params(prepare_params(source_base_commit=None))
        self.assertEqual(result["workspace_mode"], "git")
        self.assertIsNone(result["source_base_commit"])
        self.assertEqual(result["authority_ref"], AUTHORITY)

    def test_prepare_invalid(self):
        for override in (
            {"workspace_id": ""}, {"project_id": None}, {"manifest_sha256": "xyz"}, {"manifest_sha256": "A" * 64},
            {"manifest_size": 0}, {"manifest_size": True}, {"delivery_mode": "zip"}, {"workspace_mode": "svn"},
            {"estimated_total_bytes": -1}, {"authority_ref": {"host_id": "h"}}, {"action": "nope"},
            {"operation": {"node_id": "n"}},
        ):
            with self.subTest(override=override):
                self.assertInvalid(prepare_params(**override))

    def test_chunk_valid_and_invalid(self):
        self.assertEqual(normalize_workspace_params(chunk_params())["size"], 5)
        for override in (
            {"offset": -1}, {"size": 0}, {"size": MAX_WORKSPACE_CHUNK_BYTES + 1}, {"size": 4},
            {"data": "!!!!"}, {"data": 5}, {"chunk_sha256": "bad"}, {"blob_sha256": ""},
        ):
            with self.subTest(override=override):
                self.assertInvalid(chunk_params(**override))

    def test_chunk_max_size(self):
        payload = b"x" * MAX_WORKSPACE_CHUNK_BYTES
        self.assertEqual(normalize_workspace_params(chunk_params(payload))["size"], MAX_WORKSPACE_CHUNK_BYTES)

    def test_seal_and_abort(self):
        seal = {"operation": OPERATION, "action": "seal", "workspace_id": "w", "manifest_sha256": SHA_A,
                "expected_revision": 0}
        self.assertEqual(normalize_workspace_params(seal)["expected_revision"], 0)
        self.assertInvalid({**seal, "expected_revision": -1})
        self.assertInvalid({**seal, "manifest_sha256": "1"})
        abort = {"operation": OPERATION, "action": "abort", "workspace_id": "w", "reason": "r", "expected_revision": 2}
        self.assertEqual(normalize_workspace_params(abort)["reason"], "r")
        self.assertInvalid({**abort, "reason": ""})
        self.assertInvalid({**abort, "expected_revision": "2"})

    def test_inspect_workspace(self):
        result = normalize_inspect_params({"kind": "workspace", "section": "base", "project_id": "p", "base_commit": "c"})
        self.assertEqual(result["section"], "base")
        result = normalize_inspect_params({"kind": "workspace", "id": "ws", "section": "missing", "after": 3, "limit": 100})
        self.assertEqual((result["after"], result["limit"]), (3, 100))
        for bad in (
            {"kind": "workspace", "section": "base"},
            {"kind": "workspace", "id": "ws"},
            {"kind": "workspace", "id": "ws", "section": "missing", "limit": 101},
            {"kind": "workspace", "id": "ws", "section": "other"},
            {"kind": "session", "id": "s", "section": "base"},
            {"kind": "bogus"},
        ):
            with self.subTest(bad=bad), self.assertRaises(BridgeError):
                normalize_inspect_params(bad)
        self.assertEqual(normalize_inspect_params({"kind": "session", "id": "s"}), {"kind": "session", "id": "s"})


class ManifestTest(unittest.TestCase):
    def assertInvalid(self, value):
        with self.assertRaises(BridgeError):
            normalize_manifest(value)

    def test_valid_manifest(self):
        result = normalize_manifest(manifest())
        self.assertEqual(result["files"][1], {
            "path": "old.py", "op": "delete", "size": 0, "before_sha256": SHA_B, "sha256": None, "mode": 0,
        })
        self.assertEqual(normalize_manifest(result), result)

    def test_unsafe_paths(self):
        for path in ("../x", "a/../b", "/abs", ".git/config", "a/.git/x", "a//b", "./a", "a/", "a\0b", ""):
            with self.subTest(path=path):
                self.assertInvalid(manifest(
                    selected_files=[path],
                    files=[{"path": path, "op": "put", "size": 1, "sha256": SHA_A, "mode": 0o644}], file_count=1,
                ))

    def test_duplicate_and_conflicting_paths(self):
        put = {"op": "put", "size": 1, "sha256": SHA_A, "mode": 0o644}
        self.assertInvalid(manifest(files=[{"path": "a", **put}, {"path": "a", **put}], file_count=2))
        self.assertInvalid(manifest(files=[{"path": "a", **put}, {"path": "a/b", **put}], file_count=2))

    def test_invalid_modes_and_fields(self):
        base = {"path": "a", "op": "put", "size": 1, "sha256": SHA_A}
        for mode in (0o4755, 0o2755, 0o1777, -1, "644", True, None):
            with self.subTest(mode=mode):
                self.assertInvalid(manifest(files=[{**base, "mode": mode}], file_count=1, selected_files=["a"]))
        for mode in (0, 0o600, 0o755, 0o777):
            normalize_manifest(manifest(files=[{**base, "mode": mode}], file_count=1, selected_files=["a"]))
        self.assertInvalid(manifest(files=[{**base, "mode": 0o644, "op": "move"}], file_count=1))
        self.assertInvalid(manifest(schema=2))
        self.assertInvalid(manifest(file_count=5))
        self.assertInvalid(manifest(delivery_mode="zip"))

    def test_patch_files_must_be_selected(self):
        self.assertInvalid(manifest(delivery_mode="patch", selected_files=["src/a.py"]))
        normalize_manifest(manifest(delivery_mode="patch"))

    def test_manifest_digest_deterministic_and_ignores_input_digest(self):
        first = compute_manifest_digest(manifest(input_digest="one"))
        self.assertEqual(first, compute_manifest_digest(manifest(input_digest="two")))
        self.assertNotEqual(first, compute_manifest_digest(manifest(total_bytes=4)))
        canonical = {k: v for k, v in manifest().items() if k != "input_digest"}
        expected = hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
        self.assertEqual(first, expected)

    def test_input_digest(self):
        files = [
            {"path": "b", "op": "put", "size": 1, "sha256": SHA_A, "mode": 0o644, "before_sha256": None},
            {"path": "gone", "op": "delete", "size": 0, "sha256": None, "mode": 0, "before_sha256": SHA_B},
            {"path": "a", "op": "put", "size": 2, "sha256": SHA_B, "mode": 0o755, "before_sha256": None},
        ]
        digest = compute_input_digest(files)
        self.assertEqual(digest, compute_input_digest(list(reversed(files))))
        expected = hashlib.sha256(json.dumps([
            {"mode": 0o755, "path": "a", "sha256": SHA_B, "size": 2},
            {"mode": 0o644, "path": "b", "sha256": SHA_A, "size": 1},
        ], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(digest, expected)
        files[0]["mode"] = 0o600
        self.assertNotEqual(digest, compute_input_digest(files))

    def test_input_digest_sorts_by_utf8_bytes(self):
        # U+FF5E sorts after U+1F600 by code point? No: UTF-8 byte order equals code point order; check stability.
        files = [
            {"path": p, "op": "put", "size": 0, "sha256": SHA_A, "mode": 0o644} for p in ("\uff5e", "\U0001f600", "z")
        ]
        self.assertEqual(compute_input_digest(files), compute_input_digest(files[::-1]))


class ConfigProjectsTest(unittest.TestCase):
    def test_projects_resolved_and_validated(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "proj"
            root.mkdir()
            link = Path(tmp) / "link"
            link.symlink_to(root)
            self.assertEqual(normalize_projects({"p": str(link)}), {"p": str(root.resolve())})
            for bad in ({"p": str(Path(tmp) / "missing")}, {"": str(root)}, {"p": 3}, {"p": " "}, []):
                with self.subTest(bad=bad), self.assertRaises(BridgeError):
                    normalize_projects(bad)
            self.assertEqual(Config(profiles={}).projects, {})


class WorkspaceStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name))

    def create(self, workspace_id="ws-1", **override):
        args = {
            "workspace_id": workspace_id, "authority_ref": AUTHORITY, "project_id": "bridge",
            "project_config_digest": "cfg", "project_root": "/srv/bridge", "manifest_sha256": SHA_A,
            "delivery_mode": "patch", "source_base_commit": "abc", "metadata": {"k": 1},
        }
        args.update(override)
        return self.store.create_workspace_transfer(**args)

    def make_session(self, session_id="s1"):
        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,created) VALUES(?,?,?,?,?)",
                (session_id, "p", "/", "read-only", time.time()),
            )

    def test_schema_created_and_migration_idempotent(self):
        with self.store.connection() as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"workspace_transfers", "workspace_chunks", "workspace_blobs"} <= tables)
        self.create()
        reopened = Store(Path(self.tmp.name))
        self.assertEqual(reopened.get_workspace_transfer("ws-1")["project_id"], "bridge")

    def test_migrates_database_without_workspace_tables(self):
        with self.store.connection(write=True) as db:
            db.execute("DROP TABLE workspace_chunks")
            db.execute("DROP TABLE workspace_transfers")
            db.execute("DROP TABLE workspace_blobs")
        reopened = Store(Path(self.tmp.name))
        self.assertEqual(self.create()["status"], "receiving")
        self.assertIsNotNone(reopened.get_workspace_transfer("ws-1"))

    def test_create_and_get(self):
        row = self.create()
        self.assertEqual((row["status"], row["revision"]), ("receiving", 0))
        self.assertEqual(row["authority_ref"], AUTHORITY)
        self.assertEqual(row["metadata"], {"k": 1})
        self.assertIsNone(row["attached_session_id"])
        self.assertIsNone(self.store.get_workspace_transfer("missing"))

    def test_create_idempotent_and_conflict(self):
        first = self.create()
        self.assertEqual(self.create(), first)
        for override in ({"manifest_sha256": SHA_B}, {"project_id": "other"}, {"authority_ref": {**AUTHORITY, "node_id": "x"}}):
            with self.subTest(override=override), self.assertRaises(BridgeError) as caught:
                self.create(**override)
            self.assertEqual(caught.exception.code, PeerErrorCode.WORKSPACE_CONFLICT)

    def test_status_update_revision_cas(self):
        self.create()
        self.assertEqual(self.store.update_workspace_transfer_status("ws-1", "verified", 0), 1)
        # replay of the same change is idempotent
        self.assertEqual(self.store.update_workspace_transfer_status("ws-1", "verified", 0), 1)
        # stale revision with a different status conflicts
        with self.assertRaises(BridgeError) as caught:
            self.store.update_workspace_transfer_status("ws-1", "aborted", 0)
        self.assertEqual(caught.exception.code, PeerErrorCode.WORKSPACE_CONFLICT)
        self.assertEqual(self.store.update_workspace_transfer_status("ws-1", "preparing", 1), 2)
        self.assertEqual(
            self.store.update_workspace_transfer_status("ws-1", "ready", 2, ready_path="/r", input_digest="d"), 3
        )
        row = self.store.get_workspace_transfer("ws-1")
        self.assertEqual((row["status"], row["ready_path"], row["input_digest"], row["revision"]), ("ready", "/r", "d", 3))
        self.assertEqual(self.store.update_workspace_transfer_status("ws-1", "failed", 3, error="boom"), 4)
        self.assertEqual(self.store.get_workspace_transfer("ws-1")["error"], "boom")
        with self.assertRaises(BridgeError) as caught:
            self.store.update_workspace_transfer_status("missing", "ready", 0)
        self.assertEqual(caught.exception.code, "not_found")

    def test_concurrent_overlapping_chunks_cannot_poison_upload(self):
        self.create()
        barrier = threading.Barrier(2)

        def record(offset, size):
            barrier.wait(timeout=5)
            try:
                self.store.record_workspace_chunk("ws-1", SHA_A, offset, size, SHA_B, "/chunk")
                return "accepted"
            except BridgeError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda span: record(*span), [(0, 10), (5, 5)]))
        self.assertCountEqual(["accepted", PeerErrorCode.WORKSPACE_CONFLICT], results)
        chunks = self.store.get_workspace_chunks("ws-1", SHA_A)
        self.assertEqual(1, len(chunks))
        if chunks[0]["offset"] == 5:
            self.store.record_workspace_chunk("ws-1", SHA_A, 0, 5, SHA_A, "/prefix")
        self.assertEqual(10, sum(chunk["size"] for chunk in self.store.get_workspace_chunks("ws-1", SHA_A)))

    def test_chunks_idempotent_and_conflict(self):
        self.create()
        self.store.record_workspace_chunk("ws-1", SHA_A, 5, 5, SHA_B, "/c1")
        self.store.record_workspace_chunk("ws-1", SHA_A, 0, 5, SHA_A, "/c0")
        self.store.record_workspace_chunk("ws-1", SHA_A, 0, 5, SHA_A, "/c0")
        chunks = self.store.get_workspace_chunks("ws-1", SHA_A)
        self.assertEqual([c["offset"] for c in chunks], [0, 5])
        with self.assertRaises(BridgeError) as caught:
            self.store.record_workspace_chunk("ws-1", SHA_A, 0, 5, SHA_B, "/other")
        self.assertEqual(caught.exception.code, PeerErrorCode.WORKSPACE_CONFLICT)
        with self.assertRaises(BridgeError):
            self.store.record_workspace_chunk("nope", SHA_A, 0, 5, SHA_A, "/c0")
        self.assertEqual(self.store.get_workspace_chunks("ws-1", SHA_B), [])

    def test_blobs(self):
        self.assertFalse(self.store.has_workspace_blob(SHA_A))
        self.store.record_workspace_blob(SHA_A, 0, "/blob")
        self.store.record_workspace_blob(SHA_A, 0, "/blob")
        self.assertTrue(self.store.has_workspace_blob(SHA_A))
        self.assertEqual(self.store.get_workspace_blob(SHA_A)["stored_path"], "/blob")
        with self.assertRaises(BridgeError):
            self.store.record_workspace_blob(SHA_A, 9, "/blob")

    def test_attach(self):
        self.create()
        self.make_session("s1")
        self.make_session("s2")
        with self.assertRaises(BridgeError) as caught:
            self.store.attach_workspace_transfer("ws-1", "s1")
        self.assertEqual(caught.exception.code, PeerErrorCode.WORKSPACE_CONFLICT)
        self.store.update_workspace_transfer_status("ws-1", "ready", 0, ready_path="/r", input_digest="d")
        self.store.attach_workspace_transfer("ws-1", "s1")
        self.store.attach_workspace_transfer("ws-1", "s1")
        row = self.store.get_workspace_transfer("ws-1")
        self.assertEqual((row["status"], row["attached_session_id"], row["revision"]), ("attached", "s1", 2))
        with self.assertRaises(BridgeError) as caught:
            self.store.attach_workspace_transfer("ws-1", "s2")
        self.assertEqual(caught.exception.code, PeerErrorCode.WORKSPACE_ALREADY_ATTACHED)
        with self.assertRaises(BridgeError) as caught:
            self.store.update_workspace_transfer_status("ws-1", "aborted", 2)
        self.assertEqual(caught.exception.code, PeerErrorCode.WORKSPACE_ALREADY_ATTACHED)

    def test_transaction_participation(self):
        self.create()
        with self.store.connection(write=True) as db:
            self.store.update_workspace_transfer_status("ws-1", "verified", 0, db=db)
            self.store.record_workspace_blob(SHA_A, 1, "/b", db=db)
        self.assertEqual(self.store.get_workspace_transfer("ws-1")["status"], "verified")
        with self.assertRaises(RuntimeError), self.store.connection(write=True) as db:
            self.store.update_workspace_transfer_status("ws-1", "aborted", 1, db=db)
            raise RuntimeError
        self.assertEqual(self.store.get_workspace_transfer("ws-1")["status"], "verified")


if __name__ == "__main__":
    unittest.main()
