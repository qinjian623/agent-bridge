from __future__ import annotations

import base64
import hashlib
import json
import stat
import subprocess
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from agent_bridge.broker import Broker
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import (
    MAX_WORKSPACE_CHUNK_BYTES,
    PeerErrorCode,
    compute_input_digest,
    compute_manifest_digest,
    normalize_manifest,
)
from agent_bridge.store import Store, encode
from agent_bridge.workspace import chunk_bytes, create_workspace_manifest

AUTHORITY = {"host_id": "coord", "node_id": "coord-node", "state_generation": "gen-1", "local_id": "req-1"}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run_git(root, *args):
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True)
    if result.returncode:
        raise AssertionError(result.stderr.decode(errors="replace"))
    return result.stdout


def make_repo(root: Path, marker: str | None = None) -> str:
    root.mkdir(parents=True)
    run_git(root, "init", "-q")
    run_git(root, "config", "user.email", "workspace@example.invalid")
    run_git(root, "config", "user.name", "Workspace Test")
    if marker is not None:
        # Keep otherwise-identical fixtures from sharing a commit id (and thus
        # from accidentally containing one another's base commit objects).
        (root / marker).write_text(marker + "\n")
    (root / "README.md").write_text("readme\n")
    (root / "src").mkdir()
    (root / "src" / "a.py").write_text("base a\n")
    (root / "old.py").write_text("old\n")
    (root / "bin").mkdir()
    entry = root / "bin" / "run.sh"
    entry.write_text("#!/bin/sh\necho hi\n")
    entry.chmod(0o755)
    run_git(root, "add", "-A")
    run_git(root, "commit", "-q", "-m", "base")
    return run_git(root, "rev-parse", "HEAD").decode().strip()


def modify_repo(root: Path) -> None:
    (root / "src" / "a.py").write_text("selected a\n")
    (root / "new.txt").write_text("new file\n")
    (root / "old.py").unlink()


SELECTION = ["src/a.py", "new.txt", "old.py"]


def workspace_git_worktrees(root: Path) -> str:
    return run_git(root, "worktree", "list", "--porcelain").decode()


class ManifestBuildTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / "repo"
        self.base = make_repo(self.repo)
        modify_repo(self.repo)

    def base_bytes(self, name: str) -> bytes:
        return run_git(self.repo, "cat-file", "blob", f"{self.base}:{name}")

    def source_bytes(self, entry, selected):
        if entry["path"] in selected:
            return (self.repo / entry["path"]).read_bytes()
        return self.base_bytes(entry["path"])

    def test_chunk_bytes_aligns_on_fixed_boundaries_and_hashes(self):
        payload = b"x" * (MAX_WORKSPACE_CHUNK_BYTES + 3)
        chunks = chunk_bytes(payload)
        self.assertEqual([offset for offset, _, _ in chunks], [0, MAX_WORKSPACE_CHUNK_BYTES])
        self.assertEqual(chunks[0][1], payload[:MAX_WORKSPACE_CHUNK_BYTES])
        self.assertEqual(chunks[1][1], payload[MAX_WORKSPACE_CHUNK_BYTES:])
        self.assertEqual(chunks[1][2], sha256(payload[MAX_WORKSPACE_CHUNK_BYTES:]))
        self.assertEqual(chunk_bytes(b""), [])
        self.assertEqual(chunk_bytes(b"abcdef", 2), [(0, b"ab", sha256(b"ab")), (2, b"cd", sha256(b"cd")), (4, b"ef", sha256(b"ef"))])
        with self.assertRaises(BridgeError):
            chunk_bytes(b"x", 0)
        with self.assertRaises(BridgeError):
            chunk_bytes("text")  # type: ignore[arg-type]

    def test_patch_manifest_lists_only_the_selected_differences(self):
        manifest = create_workspace_manifest(
            "bridge", "ws-patch", self.repo, "patch", base_commit=self.base, selected_files=SELECTION,
        )
        self.assertEqual(
            [(entry["path"], entry["op"]) for entry in manifest["files"]],
            [("new.txt", "put"), ("old.py", "delete"), ("src/a.py", "put")],
        )
        by_path = {entry["path"]: entry for entry in manifest["files"]}
        self.assertEqual(by_path["src/a.py"]["before_sha256"], sha256(b"base a\n"))
        self.assertEqual(by_path["src/a.py"]["sha256"], sha256(b"selected a\n"))
        self.assertEqual(
            by_path["src/a.py"]["mode"],
            stat.S_IMODE((self.repo / "src" / "a.py").stat().st_mode),
        )
        self.assertEqual(by_path["old.py"]["before_sha256"], sha256(b"old\n"))
        self.assertIsNone(by_path["new.txt"]["before_sha256"])
        self.assertEqual(manifest["selected_files"], SELECTION)
        self.assertEqual(manifest["source_base_commit"], self.base)
        self.assertEqual(manifest["file_count"], 3)
        self.assertEqual(manifest["total_bytes"], len(b"new file\n") + len(b"selected a\n"))
        self.assertEqual(normalize_manifest(manifest), manifest)
        self.assertEqual(compute_manifest_digest(normalize_manifest(manifest)), compute_manifest_digest(manifest))

    def test_snapshot_manifest_carries_the_complete_final_input(self):
        manifest = create_workspace_manifest(
            "bridge", "ws-snapshot", self.repo, "snapshot", base_commit=self.base, selected_files=SELECTION,
        )
        by_path = {entry["path"]: entry for entry in manifest["files"]}
        self.assertEqual(
            set(by_path), {"README.md", "bin/run.sh", "new.txt", "old.py", "src/a.py"},
        )
        self.assertEqual(by_path["old.py"]["op"], "delete")
        self.assertEqual(by_path["old.py"]["before_sha256"], sha256(b"old\n"))
        self.assertEqual(by_path["README.md"]["mode"], 0o644)
        self.assertEqual(by_path["bin/run.sh"]["mode"], 0o755)
        self.assertEqual(by_path["src/a.py"]["sha256"], sha256(b"selected a\n"))
        for entry in manifest["files"]:
            if entry["op"] != "put":
                continue
            content = self.source_bytes(entry, set(SELECTION))
            self.assertEqual(entry["sha256"], sha256(content))
            self.assertEqual(entry["size"], len(content))
        self.assertEqual(manifest["file_count"], 5)
        self.assertEqual(manifest["input_digest"], compute_input_digest(manifest["files"]))
        self.assertEqual(
            manifest["total_bytes"],
            sum(entry["size"] for entry in manifest["files"] if entry["op"] == "put"),
        )

    def test_patch_and_snapshot_digest_the_same_complete_input(self):
        patch = create_workspace_manifest(
            "bridge", "ws-1", self.repo, "patch", base_commit=self.base, selected_files=SELECTION,
        )
        snapshot = create_workspace_manifest(
            "bridge", "ws-2", self.repo, "snapshot", base_commit=self.base, selected_files=SELECTION,
        )
        self.assertEqual(patch["input_digest"], snapshot["input_digest"])
        self.assertNotEqual(compute_manifest_digest(patch), compute_manifest_digest(snapshot))

    def test_manifest_selection_validation_and_patch_base_requirement(self):
        with self.assertRaises(BridgeError):
            create_workspace_manifest("bridge", "ws", self.repo, "patch")
        with self.assertRaises(BridgeError):
            create_workspace_manifest(
                "bridge", "ws", self.repo, "snapshot", base_commit=self.base, selected_files=["../escape"],
            )
        with self.assertRaises(BridgeError):
            create_workspace_manifest(
                "bridge", "ws", self.repo, "snapshot", base_commit=self.base, selected_files=["src/a.py", "src/a.py"],
            )
        with self.assertRaises(BridgeError):
            create_workspace_manifest(
                "bridge", "../ws", self.repo, "snapshot", base_commit=self.base, selected_files=[],
            )


class ServiceWorkspaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_workspace_replay_repairs_a_missing_seal_commit_record(self):
        manifest = self.snapshot_manifest()
        real_record_commit = self.store.record_commit

        def fail_seal_record(key, method, **kwargs):
            if key.startswith("peer.workspace:seal:"):
                raise OSError("synthetic crash after ready status, before commit record")
            return real_record_commit(key, method, **kwargs)

        with patch.object(self.store, "record_commit", side_effect=fail_seal_record):
            with self.assertRaises(OSError):
                await self.prepare_and_seal(manifest)
        workspace_id = manifest["workspace_id"]
        ready = self.store.get_workspace_transfer(workspace_id)
        self.assertEqual("ready", ready["status"])
        previous_max = self.store.max_peer_commit_seq()
        params = {
            "action": "seal", "workspace_id": workspace_id,
            "manifest_sha256": ready["manifest_sha256"], "expected_revision": ready["revision"],
        }
        replay = await self.action(**params)
        self.assertEqual("ready", replay["status"])
        self.assertEqual(previous_max + 1, self.store.max_peer_commit_seq())
        await self.action(**params)
        self.assertEqual(previous_max + 1, self.store.max_peer_commit_seq())

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name) / "state"
        self.state.mkdir()
        self.repo = Path(self.temp.name) / "repo"
        self.base = make_repo(self.repo)
        modify_repo(self.repo)
        self.store = Store(self.state)
        self.identity = FileNodeIdentityStore(self.state)
        self.config = Config(
            profiles={"worker": Profile("worker", "codex", "worker-model")},
            projects={"bridge": str(self.repo)},
        )
        self.service = DirectPeerService(
            self.identity, host_id="gpu-a", store=self.store, config=self.config,
        )
        self.counter = 0

    def operation(self):
        identity = self.identity.load_or_create()
        self.counter += 1
        return {
            "node_id": identity.node_id,
            "state_generation": identity.state_generation,
            "operation_id": f"workspace-op-{self.counter}",
        }

    async def peer(self, method, params):
        return await self.service.handle(
            {"v": 1, "id": uuid.uuid4().hex, "method": method, "params": params}
        )

    async def action_response(self, **payload):
        return await self.peer("peer.workspace", {"operation": self.operation(), **payload})

    async def action(self, **payload):
        response = await self.action_response(**payload)
        self.assertTrue(response["ok"], response)
        return response["result"]

    async def inspect(self, **params):
        response = await self.peer("peer.inspect", params)
        self.assertTrue(response["ok"], response)
        return response["result"]

    def snapshot_manifest(self, workspace_id="ws-service"):
        return create_workspace_manifest(
            "bridge", workspace_id, self.repo, "snapshot",
            base_commit=self.base, selected_files=SELECTION,
        )

    def patch_manifest(self, workspace_id="ws-patch"):
        return create_workspace_manifest(
            "bridge", workspace_id, self.repo, "patch",
            base_commit=self.base, selected_files=SELECTION,
        )

    async def prepare(self, manifest):
        workspace_id = manifest["workspace_id"]
        data = encode(manifest).encode("utf-8")
        digest = compute_manifest_digest(manifest)
        result = await self.action(
            action="prepare",
            workspace_id=workspace_id,
            project_id=manifest["project_id"],
            manifest_sha256=digest,
            manifest_size=len(data),
            delivery_mode=manifest["delivery_mode"],
            workspace_mode="git",
            source_base_commit=manifest["source_base_commit"],
            estimated_total_bytes=len(data) + sum(
                entry["size"] for entry in manifest["files"] if entry["op"] == "put"
            ),
            authority_ref=AUTHORITY,
        )
        return workspace_id, digest, data, result

    async def upload_blob(self, workspace_id, digest, data):
        for offset, chunk, chunk_sha256 in chunk_bytes(data):
            await self.action(
                action="chunk",
                workspace_id=workspace_id,
                blob_sha256=digest,
                offset=offset,
                size=len(chunk),
                chunk_sha256=chunk_sha256,
                data=base64.b64encode(chunk).decode("ascii"),
            )

    def entry_bytes(self, manifest, entry):
        if entry["path"] in set(manifest["selected_files"]):
            return (self.repo / entry["path"]).read_bytes()
        return run_git(self.repo, "cat-file", "blob", f"{manifest['source_base_commit']}:{entry['path']}")

    async def upload_all_files(self, manifest):
        seen = set()
        for entry in manifest["files"]:
            if entry["op"] != "put" or entry["size"] == 0 or entry["sha256"] in seen:
                continue
            seen.add(entry["sha256"])
            await self.upload_blob(manifest["workspace_id"], entry["sha256"], self.entry_bytes(manifest, entry))

    async def missing(self, workspace_id):
        items = []
        after = 0
        while True:
            result = await self.inspect(
                kind="workspace", id=workspace_id, section="missing", after=after, limit=100,
            )
            items.extend(result["items"])
            if not result.get("has_more"):
                return items
            after = result["cursor"]

    async def seal(self, workspace_id, digest, expected_revision=None):
        if expected_revision is None:
            status = await self.inspect(kind="workspace", id=workspace_id, section="status")
            expected_revision = status["items"][0]["revision"]
        return await self.action(
            action="seal", workspace_id=workspace_id, manifest_sha256=digest,
            expected_revision=expected_revision,
        )

    async def prepare_and_seal(self, manifest):
        workspace_id, digest, data, _ = await self.prepare(manifest)
        await self.upload_blob(workspace_id, digest, data)
        await self.upload_all_files(manifest)
        sealed = await self.seal(workspace_id, digest)
        return workspace_id, digest, sealed

    def workspace_ref(self, workspace_id, digest, manifest):
        return {
            "workspace_id": workspace_id,
            "manifest_sha256": digest,
            "input_digest": manifest["input_digest"],
        }

    async def deliver(self, workspace_ref, target_seq=1, delivery_id=None):
        response = await self.peer(
            "peer.deliver",
            {
                "operation": self.operation(),
                "delivery_id": delivery_id or f"delivery-{uuid.uuid4().hex}",
                "target": {"kind": "bridge", "id": ""},
                "target_seq": target_seq,
                "seq_kind": "message",
                "message": "",
                "open": {
                    "profile": "worker",
                    "access": "workspace-write",
                    "workspace_ref": workspace_ref,
                },
            },
        )
        return response

    async def test_snapshot_prepare_upload_seal_builds_the_complete_input(self):
        manifest = self.snapshot_manifest()
        workspace_id, digest, data, prepared = await self.prepare(manifest)
        self.assertEqual((prepared["action"], prepared["status"], prepared["revision"]), ("prepare", "receiving", 0))

        await self.upload_blob(workspace_id, digest, data)
        status = await self.inspect(kind="workspace", id=workspace_id, section="status")
        self.assertEqual(status["items"][0]["status"], "verified")

        missing = await self.missing(workspace_id)
        self.assertEqual(
            {(item["sha256"], item["size"]) for item in missing},
            {
                (entry["sha256"], entry["size"])
                for entry in manifest["files"]
                if entry["op"] == "put" and entry["size"] > 0
            },
        )
        await self.upload_all_files(manifest)
        self.assertEqual(await self.missing(workspace_id), [])

        sealed = await self.seal(workspace_id, digest)
        self.assertEqual(sealed["status"], "ready")
        self.assertEqual(sealed["input_digest"], manifest["input_digest"])
        ready = Path(sealed["ready_path"])
        self.assertTrue(ready.is_dir())
        self.assertEqual((ready / "README.md").read_text(), "readme\n")
        self.assertEqual((ready / "src" / "a.py").read_text(), "selected a\n")
        self.assertEqual((ready / "new.txt").read_text(), "new file\n")
        self.assertFalse((ready / "old.py").exists())
        self.assertEqual(stat.S_IMODE((ready / "bin" / "run.sh").stat().st_mode), 0o755)
        self.assertEqual(run_git(ready, "rev-parse", "HEAD").decode().strip(), run_git(ready, "rev-parse", "HEAD").decode().strip())
        row = self.store.get_workspace_transfer(workspace_id)
        self.assertEqual((row["status"], row["input_digest"]), ("ready", manifest["input_digest"]))
        self.assertEqual(row["ready_path"], str(ready))

    async def test_patch_prepare_upload_seal_creates_a_detached_worktree_with_the_input_tree(self):
        manifest = self.patch_manifest()
        workspace_id, digest, sealed = await self.prepare_and_seal(manifest)
        self.assertEqual(sealed["status"], "ready")
        ready = Path(sealed["ready_path"])
        self.assertEqual(run_git(ready, "rev-parse", "HEAD").decode().strip(), self.base)
        self.assertEqual((ready / "src" / "a.py").read_text(), "selected a\n")
        self.assertEqual((ready / "README.md").read_text(), "readme\n")
        self.assertEqual((ready / "new.txt").read_text(), "new file\n")
        self.assertFalse((ready / "old.py").exists())
        self.assertEqual(stat.S_IMODE((ready / "bin" / "run.sh").stat().st_mode), 0o755)
        input_tree = run_git(ready, "write-tree").decode().strip()
        self.assertEqual(run_git(ready, "diff", "--name-only", input_tree), b"")
        self.assertIn(str(ready), workspace_git_worktrees(self.repo))

    async def test_manifest_blob_verification_progression_and_chunk_replay(self):
        manifest = self.patch_manifest()
        workspace_id, digest, data, _ = await self.prepare(manifest)
        first_missing = await self.missing(workspace_id)
        [chunk_item] = first_missing
        self.assertEqual(chunk_item["blob_sha256"], digest)
        self.assertEqual(chunk_item["offset"], 0)
        chunk = data[:chunk_item["size"]]
        chunk_sha256 = sha256(chunk)
        encoded = base64.b64encode(chunk).decode("ascii")
        result = await self.action(
            action="chunk", workspace_id=workspace_id, blob_sha256=digest, offset=0,
            size=len(chunk), chunk_sha256=chunk_sha256, data=encoded,
        )
        self.assertEqual(result["received"], len(chunk))
        replay = await self.action(
            action="chunk", workspace_id=workspace_id, blob_sha256=digest, offset=0,
            size=len(chunk), chunk_sha256=chunk_sha256, data=encoded,
        )
        self.assertEqual(replay["received"], len(chunk))
        self.assertEqual(await self.missing(workspace_id), [
            {
                "sha256": entry["sha256"], "size": entry["size"],
            }
            for entry in manifest["files"] if entry["op"] == "put"
        ])
        status = await self.inspect(kind="workspace", id=workspace_id, section="status")
        self.assertEqual((status["items"][0]["status"], status["items"][0]["revision"]), ("verified", 1))
        after_verified = await self.action(
            action="chunk", workspace_id=workspace_id, blob_sha256=digest, offset=0,
            size=len(chunk), chunk_sha256=chunk_sha256, data=encoded,
        )
        self.assertEqual(after_verified["received"], len(chunk))

    async def test_prepare_replay_is_idempotent_and_conflicting_parameters_fail(self):
        manifest = self.patch_manifest()
        workspace_id, digest, data, first = await self.prepare(manifest)
        _, _, _, replay = await self.prepare(manifest)
        self.assertEqual((replay["status"], replay["revision"]), (first["status"], first["revision"]))
        self.assertEqual(self.store.get_workspace_transfer(workspace_id)["manifest_sha256"], digest)
        response = await self.action_response(
            action="prepare", workspace_id=workspace_id, project_id="bridge",
            manifest_sha256="f" * 64, manifest_size=len(data), delivery_mode="patch",
            workspace_mode="git", source_base_commit=manifest["source_base_commit"],
            estimated_total_bytes=0, authority_ref=AUTHORITY,
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)

    async def test_chunk_overlap_and_digest_conflicts_are_rejected(self):
        manifest = self.patch_manifest()
        workspace_id, _, _, _ = await self.prepare(manifest)
        payload = b"0123456789"
        digest = sha256(payload)
        await self.action(
            action="chunk", workspace_id=workspace_id, blob_sha256=digest, offset=0,
            size=10, chunk_sha256=sha256(payload), data=base64.b64encode(payload).decode(),
        )
        response = await self.action_response(
            action="chunk", workspace_id=workspace_id, blob_sha256=digest, offset=0,
            size=10, chunk_sha256=sha256(b"different!"), data=base64.b64encode(b"different!").decode(),
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)
        overlap = b"abcde"
        response = await self.action_response(
            action="chunk", workspace_id=workspace_id, blob_sha256=digest, offset=5,
            size=5, chunk_sha256=sha256(overlap), data=base64.b64encode(overlap).decode(),
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)
        corrupt = b"0123456789"
        response = await self.action_response(
            action="chunk", workspace_id=workspace_id, blob_sha256=digest, offset=20,
            size=10, chunk_sha256=sha256(b"other"), data=base64.b64encode(corrupt).decode(),
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)

    async def test_multichunk_manifest_acknowledges_incomplete_prefixes(self):
        names = []
        bulk = self.repo / "bulk"
        bulk.mkdir()
        for index in range(2200):
            name = f"bulk/{index:04d}.txt"
            (self.repo / name).write_text("")
            names.append(name)
        manifest = create_workspace_manifest(
            "bridge", "ws-large-manifest", self.repo, "snapshot",
            base_commit=self.base, selected_files=names,
        )
        data = encode(manifest).encode("utf-8")
        chunks = chunk_bytes(data)
        self.assertGreater(len(chunks), 1)
        workspace_id, digest, _, _ = await self.prepare(manifest)
        for index, (offset, chunk, chunk_sha256) in enumerate(chunks):
            result = await self.action(
                action="chunk", workspace_id=workspace_id, blob_sha256=digest,
                offset=offset, size=len(chunk), chunk_sha256=chunk_sha256,
                data=base64.b64encode(chunk).decode("ascii"),
            )
            if index < len(chunks) - 1:
                self.assertNotIn("status", result)
                self.assertEqual(self.store.get_workspace_transfer(workspace_id)["status"], "receiving")
            else:
                self.assertEqual(result.get("status"), "verified")
        self.assertEqual(self.store.get_workspace_transfer(workspace_id)["status"], "verified")

    async def test_rejected_tail_replacement_preserves_accepted_chunk_and_upload(self):
        name = "tail-first.bin"
        prefix = b"a" * MAX_WORKSPACE_CHUNK_BYTES
        tail = b"0123456789"
        content = prefix + tail
        (self.repo / name).write_bytes(content)
        manifest = create_workspace_manifest(
            "bridge", "ws-tail-first", self.repo, "snapshot",
            base_commit=self.base, selected_files=[*SELECTION, name],
        )
        workspace_id, digest, data, _ = await self.prepare(manifest)
        await self.upload_blob(workspace_id, digest, data)
        blob = sha256(content)
        tail_payload = {
            "action": "chunk", "workspace_id": workspace_id, "blob_sha256": blob,
            "offset": len(prefix), "size": len(tail), "chunk_sha256": sha256(tail),
            "data": base64.b64encode(tail).decode("ascii"),
        }
        await self.action(**tail_payload)
        [accepted] = self.store.get_workspace_chunks(workspace_id, blob)
        self.assertEqual(tail, Path(accepted["stored_path"]).read_bytes())
        conflict_payload = {
            **tail_payload, "chunk_sha256": sha256(b"different!"),
            "data": base64.b64encode(b"different!").decode("ascii"),
        }
        response = await self.action_response(**conflict_payload)
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)
        self.assertEqual([accepted], self.store.get_workspace_chunks(workspace_id, blob))
        self.assertEqual(tail, Path(accepted["stored_path"]).read_bytes())

        def send(payload):
            try:
                return "ok", self.service._workspace_chunk(self.store, payload)
            except BridgeError as error:
                return "error", error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(send, (tail_payload, conflict_payload)))
        self.assertEqual(1, sum(kind == "ok" for kind, _ in outcomes))
        self.assertIn(("error", PeerErrorCode.WORKSPACE_CONFLICT), outcomes)
        self.assertEqual([accepted], self.store.get_workspace_chunks(workspace_id, blob))
        self.assertEqual(tail, Path(accepted["stored_path"]).read_bytes())

        await self.action(
            action="chunk", workspace_id=workspace_id, blob_sha256=blob,
            offset=0, size=len(prefix), chunk_sha256=sha256(prefix),
            data=base64.b64encode(prefix).decode("ascii"),
        )
        recorded = self.store.get_workspace_blob(blob)
        self.assertIsNotNone(recorded)
        self.assertEqual(content, Path(recorded["stored_path"]).read_bytes())
        await self.upload_all_files(manifest)
        sealed = await self.seal(workspace_id, digest)
        self.assertEqual(content, (Path(sealed["ready_path"]) / name).read_bytes())

    async def test_patch_seal_attaches_a_new_file_inside_a_new_directory(self):
        directory = self.repo / "newdir"
        directory.mkdir()
        (directory / "file.py").write_text("new input\n")
        manifest = create_workspace_manifest(
            "bridge", "ws-new-directory", self.repo, "patch",
            base_commit=self.base, selected_files=["newdir/file.py"],
        )
        workspace_id, digest, sealed = await self.prepare_and_seal(manifest)
        ready = Path(sealed["ready_path"])
        self.assertEqual("new input\n", (ready / "newdir" / "file.py").read_text())
        response = await self.deliver(self.workspace_ref(workspace_id, digest, manifest), target_seq=1)
        self.assertTrue(response["ok"], response)
        session = self.store.get_session(response["result"]["session_id"])
        self.assertEqual(session["cwd"], str(ready))

    async def test_seal_rejects_new_files_through_symlink_or_file_parents(self):
        (self.repo / "link").symlink_to("../outside", target_is_directory=True)
        run_git(self.repo, "add", "-A")
        run_git(self.repo, "commit", "-qm", "symlink base")
        base = run_git(self.repo, "rev-parse", "HEAD").decode().strip()
        for workspace_id, path in (
            ("ws-symlink-parent", "link/file.py"),
            ("ws-file-parent", "README.md/file.py"),
        ):
            with self.subTest(path=path):
                data = b"must not be written\n"
                entry = {
                    "path": path, "op": "put", "size": len(data), "before_sha256": None,
                    "sha256": sha256(data), "mode": 0o644,
                }
                manifest = normalize_manifest({
                    "schema": 1, "workspace_id": workspace_id, "project_id": "bridge",
                    "workspace_mode": "git", "source_base_commit": base,
                    "delivery_mode": "patch", "selected_files": [path],
                    "input_digest": compute_input_digest([entry]), "files": [entry],
                    "patch": None, "total_bytes": len(data), "file_count": 1,
                })
                pending, digest, manifest_data, _ = await self.prepare(manifest)
                await self.upload_blob(pending, digest, manifest_data)
                await self.upload_blob(pending, entry["sha256"], data)
                status = await self.inspect(kind="workspace", id=pending, section="status")
                response = await self.action_response(
                    action="seal", workspace_id=pending, manifest_sha256=digest,
                    expected_revision=status["items"][0]["revision"],
                )
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)
                self.assertEqual(self.store.get_workspace_transfer(pending)["status"], "failed")

    async def test_prepare_limits_are_enforced(self):
        params = {
            "action": "prepare", "workspace_id": "ws-limit", "project_id": "bridge",
            "manifest_sha256": "a" * 64, "manifest_size": 32 * 1024 * 1024 + 1,
            "delivery_mode": "snapshot", "workspace_mode": "git", "source_base_commit": None,
            "estimated_total_bytes": 0, "authority_ref": AUTHORITY,
        }
        response = await self.peer("peer.workspace", {"operation": self.operation(), **params})
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED)
        oversized = await self.peer(
            "peer.workspace",
            {
                "operation": self.operation(),
                **{**params, "workspace_id": "ws-limit-2", "manifest_size": 10,
                   "estimated_total_bytes": 1024 * 1024 * 1024 + 1},
            },
        )
        self.assertFalse(oversized["ok"])
        self.assertEqual(oversized["error"]["code"], PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED)

    async def test_seal_before_verification_and_abort_lifecycle(self):
        manifest = self.patch_manifest()
        workspace_id, digest, _, _ = await self.prepare(manifest)
        response = await self.action_response(
            action="seal", workspace_id=workspace_id, manifest_sha256=digest, expected_revision=0,
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)
        aborted = await self.action(
            action="abort", workspace_id=workspace_id, reason="user cancelled", expected_revision=0,
        )
        self.assertEqual((aborted["status"], aborted["revision"]), ("aborted", 1))
        row = self.store.get_workspace_transfer(workspace_id)
        self.assertEqual((row["status"], row["error"]), ("aborted", "user cancelled"))
        replay = await self.action(
            action="abort", workspace_id=workspace_id, reason="user cancelled", expected_revision=0,
        )
        self.assertEqual(replay["revision"], 1)
        response = await self.action_response(
            action="abort", workspace_id=workspace_id, reason="again", expected_revision=5,
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)

    async def test_ready_abort_removes_the_worktree(self):
        manifest = self.patch_manifest()
        workspace_id, digest, sealed = await self.prepare_and_seal(manifest)
        ready = Path(sealed["ready_path"])
        row = self.store.get_workspace_transfer(workspace_id)
        aborted = await self.action(
            action="abort", workspace_id=workspace_id, reason="not needed", expected_revision=row["revision"],
        )
        self.assertEqual(aborted["status"], "aborted")
        self.assertFalse(ready.exists())
        self.assertNotIn(str(ready), workspace_git_worktrees(self.repo))

    async def test_inspect_base_status_and_missing_unknowns(self):
        base = await self.inspect(
            kind="workspace", section="base", project_id="bridge", base_commit=self.base,
        )
        self.assertEqual(base["items"][0]["exists"], True)
        self.assertEqual(base["items"][0]["project_id"], "bridge")
        self.assertEqual(len(base["items"][0]["project_config_digest"]), 64)
        absent = await self.inspect(
            kind="workspace", section="base", project_id="bridge", base_commit="f" * 40,
        )
        self.assertFalse(absent["items"][0]["exists"])
        for params in (
            {"kind": "workspace", "section": "base", "project_id": "missing"},
            {"kind": "workspace", "id": "missing", "section": "status"},
            {"kind": "workspace", "id": "missing", "section": "missing"},
        ):
            response = await self.peer("peer.inspect", params)
            self.assertFalse(response["ok"])
            self.assertEqual(response["error"]["code"], PeerErrorCode.OPERATION_NOT_FOUND)

    async def test_deliver_attaches_the_ready_workspace_atomically(self):
        manifest = self.snapshot_manifest()
        workspace_id, digest, sealed = await self.prepare_and_seal(manifest)
        workspace_ref = self.workspace_ref(workspace_id, digest, manifest)
        response = await self.deliver(workspace_ref, target_seq=1)
        self.assertTrue(response["ok"], response)
        result = response["result"]
        self.assertEqual(result["status"], "accepted_remote")
        session = self.store.get_session(result["session_id"])
        self.assertEqual(session["cwd"], sealed["ready_path"])
        self.assertEqual(session["access"], "workspace-write")
        self.assertEqual(session["workspace"]["kind"], "git")
        self.assertEqual(session["workspace"]["workspace_id"], workspace_id)
        self.assertEqual(session["workspace"]["manifest_sha256"], digest)
        self.assertEqual(session["workspace"]["input_digest"], manifest["input_digest"])
        self.assertEqual(session["workspace"]["source_base_commit"], manifest["source_base_commit"])
        self.assertRegex(session["workspace"]["snapshot_tree"], r"^[0-9a-f]{40}$")
        row = self.store.get_workspace_transfer(workspace_id)
        self.assertEqual((row["status"], row["attached_session_id"]), ("attached", result["session_id"]))

        again = await self.deliver(workspace_ref, target_seq=2)
        self.assertFalse(again["ok"])
        self.assertEqual(again["error"]["code"], PeerErrorCode.WORKSPACE_ALREADY_ATTACHED)
        attach_row = self.store.get_workspace_transfer(workspace_id)
        aborted = await self.peer(
            "peer.workspace",
            {
                "operation": self.operation(),
                "action": "abort", "workspace_id": workspace_id, "reason": "late",
                "expected_revision": attach_row["revision"],
            },
        )
        self.assertFalse(aborted["ok"])
        self.assertEqual(aborted["error"]["code"], PeerErrorCode.WORKSPACE_ALREADY_ATTACHED)

    async def test_deliver_rejects_mismatched_and_non_ready_references(self):
        manifest = self.snapshot_manifest()
        workspace_id, digest, _ = await self.prepare_and_seal(manifest)
        wrong = {
            "workspace_id": workspace_id, "manifest_sha256": digest, "input_digest": "0" * 64,
        }
        response = await self.deliver(wrong, target_seq=1)
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)

        pending = self.snapshot_manifest("ws-pending")
        pending_id, pending_digest, _, _ = await self.prepare(pending)
        pending_ref = {
            "workspace_id": pending_id, "manifest_sha256": pending_digest,
            "input_digest": pending["input_digest"],
        }
        response = await self.deliver(pending_ref, target_seq=2)
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_CONFLICT)

    async def test_seal_rechecks_the_project_mapping(self):
        manifest = self.patch_manifest()
        workspace_id, digest, data, _ = await self.prepare(manifest)
        await self.upload_blob(workspace_id, digest, data)
        replacement = Path(self.temp.name) / "replacement"
        replacement.mkdir()
        self.config.projects["bridge"] = str(replacement)
        response = await self.peer(
            "peer.workspace",
            {
                "operation": self.operation(),
                "action": "seal", "workspace_id": workspace_id, "manifest_sha256": digest,
                "expected_revision": 1,
            },
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.WORKSPACE_PROJECT_CHANGED)
        self.assertEqual(self.store.get_workspace_transfer(workspace_id)["status"], "verified")


class ServicePeerClient:
    def __init__(self, service):
        self.service = service
        self.calls = []
        self.deliver_params = []

    async def _call(self, method, params):
        self.calls.append((method, params))
        response = await self.service.handle(
            {"v": 1, "id": uuid.uuid4().hex, "method": f"peer.{method}", "params": params}
        )
        if not response["ok"]:
            raise BridgeError(response["error"]["code"], response["error"]["message"])
        return response["result"]

    async def inspect(self, params):
        return await self._call("inspect", params)

    async def workspace(self, params):
        return await self._call("workspace", params)

    async def deliver(self, params):
        self.deliver_params.append(params)
        return {
            "delivery_id": params["delivery_id"],
            "accepted": True,
            "task_id": None,
            "session_id": "remote-session",
            "target_seq": params["target_seq"],
            "receipt": {"accepted_at": 1.0},
            "status": "accepted_remote",
        }


class ServicePeerFactory:
    def __init__(self, client):
        self.peer = client
        self.requested = []

    def client(self, host_id):
        self.requested.append(host_id)
        return self.peer


class BrokerWorkspaceDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name) / "state"
        self.state.mkdir()
        self.source = Path(self.temp.name) / "source"
        self.base = make_repo(self.source)
        modify_repo(self.source)
        self.project = Path(self.temp.name) / "project"
        make_repo(self.project, marker="project-only.txt")
        identity = FileNodeIdentityStore(self.state).load_or_create()
        (self.state / "config.json").write_text(json.dumps({
            "profiles": {"fixed": {"runtime": "codex", "model": "fixed-model"}},
            "hosts": {
                "gpu-a": {
                    "ssh_target": "gpu-a.internal",
                    "remote_executable": "/usr/local/bin/agent-bridge",
                    "pinned": {
                        "node_id": identity.node_id,
                        "state_generation": identity.state_generation,
                    },
                }
            },
            "projects": {"bridge": str(self.project)},
            "worker_launcher": "detached",
            "telegram_database": None,
        }))
        self.broker = Broker(self.state)
        self.service = DirectPeerService(
            FileNodeIdentityStore(self.state),
            host_id="gpu-a",
            store=self.broker.store,
            config=self.broker.config,
        )
        self.client = ServicePeerClient(self.service)
        self.broker.peer_factory = ServicePeerFactory(self.client)

    def args(self, request_id, extra=None):
        value = {
            "host_id": "gpu-a",
            "profile": "fixed",
            "cwd": str(self.source),
            "project_id": "bridge",
            "access": "workspace-write",
            "base": self.base,
            "snapshot_files": SELECTION,
            "request_id": request_id,
        }
        value.update(extra or {})
        return value

    async def test_remote_snapshot_dispatch_uploads_and_seals_before_delivering(self):
        opened = await self.broker.call("agent_open", self.args("snap-1"))
        self.assertEqual(opened["session_id"], "remote-session")
        [deliver] = self.client.deliver_params
        ref = deliver["open"]["workspace_ref"]
        self.assertEqual(set(ref), {"workspace_id", "manifest_sha256", "input_digest"})
        transfer = self.broker.store.get_workspace_transfer(ref["workspace_id"])
        self.assertEqual(transfer["delivery_mode"], "snapshot")
        self.assertEqual(transfer["status"], "ready")
        self.assertEqual(transfer["manifest_sha256"], ref["manifest_sha256"])
        self.assertEqual(transfer["input_digest"], ref["input_digest"])
        ready = Path(transfer["ready_path"])
        self.assertEqual((ready / "README.md").read_text(), "readme\n")
        self.assertEqual((ready / "src" / "a.py").read_text(), "selected a\n")
        self.assertEqual((ready / "new.txt").read_text(), "new file\n")
        self.assertFalse((ready / "old.py").exists())
        self.assertEqual(stat.S_IMODE((ready / "bin" / "run.sh").stat().st_mode), 0o755)
        self.assertEqual(deliver["target"], {"kind": "bridge", "id": ""})
        self.assertEqual(deliver["open"]["access"], "workspace-write")
        actions = [params["action"] for method, params in self.client.calls if method == "workspace"]
        self.assertEqual(actions[0], "prepare")
        self.assertEqual(actions[-1], "seal")
        self.assertIn("chunk", actions)

    async def test_remote_patch_dispatch_reuses_an_available_base_commit(self):
        self.broker.config.projects["bridge"] = str(self.source)
        opened = await self.broker.call("agent_open", self.args("patch-1"))
        self.assertEqual(opened["session_id"], "remote-session")
        [deliver] = self.client.deliver_params
        ref = deliver["open"]["workspace_ref"]
        transfer = self.broker.store.get_workspace_transfer(ref["workspace_id"])
        self.assertEqual(transfer["delivery_mode"], "patch")
        ready = Path(transfer["ready_path"])
        self.assertEqual(run_git(ready, "rev-parse", "HEAD").decode().strip(), self.base)
        self.assertEqual((ready / "src" / "a.py").read_text(), "selected a\n")
        self.assertFalse((ready / "old.py").exists())
        # Retrying the identical open returns the stored operation result without a second delivery.
        replay = await self.broker.call("agent_open", self.args("patch-1"))
        self.assertEqual(replay, opened)
        self.assertEqual(len(self.client.deliver_params), 1)

    async def test_remote_snapshot_dispatch_uploads_a_multi_chunk_file(self):
        payload = b"a" * (MAX_WORKSPACE_CHUNK_BYTES + 1)
        (self.source / "large.bin").write_bytes(payload)
        opened = await self.broker.call(
            "agent_open",
            self.args("large-file", {"snapshot_files": [*SELECTION, "large.bin"]}),
        )
        self.assertEqual(opened["session_id"], "remote-session")
        [deliver] = self.client.deliver_params
        ref = deliver["open"]["workspace_ref"]
        transfer = self.broker.store.get_workspace_transfer(ref["workspace_id"])
        self.assertEqual(transfer["status"], "ready")
        self.assertEqual(payload, (Path(transfer["ready_path"]) / "large.bin").read_bytes())

    async def test_remote_read_only_open_is_unchanged(self):
        opened = await self.broker.call(
            "agent_open",
            {
                "host_id": "gpu-a",
                "profile": "fixed",
                "cwd": str(self.source),
                "access": "read-only",
                "request_id": "read-1",
            },
        )
        self.assertEqual(opened["session_id"], "remote-session")
        [deliver] = self.client.deliver_params
        self.assertNotIn("workspace_ref", deliver["open"])
        self.assertFalse(any(method == "workspace" for method, _ in self.client.calls))


if __name__ == "__main__":
    unittest.main()
