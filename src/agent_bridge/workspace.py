from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from .errors import BridgeError
from .peer_protocol import (
    PeerErrorCode,
    compute_input_digest,
    compute_manifest_digest,
    validate_manifest_path,
)

MAX_WORKSPACE_CHUNK_BYTES = 256 * 1024
MAX_WORKSPACE_FILE_BYTES = 64 * 1024 * 1024
MAX_WORKSPACE_TOTAL_BYTES = 1024 * 1024 * 1024
MAX_WORKSPACE_FILES = 100_000
MAX_WORKSPACE_MANIFEST_BYTES = 32 * 1024 * 1024


def git(directory, *args, stdout=subprocess.PIPE):
    result = subprocess.run(
        ["git", "-C", str(directory), "--literal-pathspecs", *args], stdout=stdout, stderr=subprocess.PIPE, timeout=60
    )
    if result.returncode:
        raise BridgeError("git_error", result.stderr.decode(errors="replace")[-2000:])
    return result.stdout


def selected_path(root, name):
    if not isinstance(name, str) or not name or "\x00" in name:
        raise BridgeError("invalid_snapshot", "Snapshot paths must be individual relative file names")
    try:
        name.encode("utf-8")
    except UnicodeEncodeError:
        raise BridgeError("invalid_snapshot", "Selected snapshot paths must be valid UTF-8") from None
    path = Path(name)
    if path.is_absolute() or any(part in {"..", ".git"} for part in path.parts) or path.as_posix() != name:
        raise BridgeError("invalid_snapshot", "Snapshot paths must stay inside the project and exclude .git")
    target = root
    for part in path.parts:
        target = target / part
        if target.is_symlink():
            raise BridgeError("invalid_snapshot", "Snapshot paths cannot traverse symlinks")
    return target


def save_manifest(path, value):
    temporary = path.with_suffix(".tmp")
    # Filesystem surrogate escapes must remain reversible JSON escapes, never invalid UTF-8 bytes.
    temporary.write_bytes(json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8", "backslashreplace"))
    temporary.chmod(0o600)
    temporary.replace(path)


def file_identity(value):
    return (
        (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns, value.st_mode)
        if value
        else None
    )


def snapshot(state, root, key, names, commit, mode):
    if not isinstance(names, list) or any(not isinstance(name, str) for name in names) or len(set(names)) != len(names):
        raise BridgeError("invalid_snapshot", "snapshot_files must be a list of unique relative file names")
    folder = state / "snapshots" / key
    expected = {"source": str(root), "base_commit": commit, "selected_files": names, "kind": mode}
    manifest = folder / "workspace.json"
    if manifest.exists():
        saved = json.loads(manifest.read_text())
        if any(saved.get(key) != value for key, value in expected.items()):
            raise BridgeError("snapshot_conflict", "This request ID already has a different workspace snapshot")
        return folder, saved
    folder.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.TemporaryDirectory(dir=folder.parent) as temporary:
        stage = Path(temporary)
        files, identities = {}, {}
        for name in names:
            path = selected_path(root, name)
            try:
                before = path.stat()
            except FileNotFoundError:
                if commit and git(root, "ls-tree", "-z", commit, "--", name):
                    files[name] = {"deleted": True}
                    identities[name] = None
                    continue
                raise BridgeError("invalid_snapshot", f"Snapshot file does not exist: {name}") from None
            if not stat.S_ISREG(before.st_mode):
                raise BridgeError("invalid_snapshot", "Select individual regular files, not directories or devices")
            target = stage / "files" / name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(path, target, follow_symlinks=False)
            if file_identity(path.stat()) != file_identity(before) or target.is_symlink():
                raise BridgeError("snapshot_changed", "A selected file changed during capture; submit a new request")
            with target.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            files[name] = {"sha256": digest, "mode": stat.S_IMODE(before.st_mode)}
            identities[name] = before
        for name, before in identities.items():
            path = selected_path(root, name)
            if file_identity(path.stat() if path.exists() else None) != file_identity(before):
                raise BridgeError("snapshot_changed", "A selected file changed during capture; submit a new request")
        saved = {**expected, "files": files, "ready": False, "manifest": str(manifest)}
        save_manifest(stage / "workspace.json", saved)
        os.rename(stage, folder)
    return folder, saved


def apply_snapshot(folder, metadata, target):
    for name, record in metadata["files"].items():
        path = selected_path(target, name)
        if record.get("deleted"):
            path.unlink(missing_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(folder / "files" / name, path)
            path.chmod(record["mode"])


def worktree(state, source, request_id, base="HEAD", snapshot_files=None):
    source = Path(source).expanduser().resolve()
    root = Path(git(source, "rev-parse", "--show-toplevel").decode().strip()).resolve()
    if source != root:
        raise BridgeError("invalid_workspace", "Writing sessions require the repository root")
    if base.startswith("-"):
        raise BridgeError("invalid_base", "Invalid Git revision")
    commit = git(root, "rev-parse", "--verify", base + "^{commit}").decode().strip()
    key = hashlib.sha256(request_id.encode()).hexdigest()[:24]
    folder, metadata = snapshot(state, root, key, snapshot_files or [], commit, "git")
    target = state / "worktrees" / key
    if target.exists():
        if git(target, "rev-parse", "--git-common-dir") != git(
            root, "rev-parse", "--path-format=absolute", "--git-common-dir"
        ):
            raise BridgeError("worktree_conflict", "Worktree path already exists for a different repository")
    else:
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        git(root, "worktree", "add", "--detach", str(target), commit)
    if not metadata["ready"]:
        apply_snapshot(folder, metadata, target)
        if metadata["selected_files"]:
            git(target, "add", "-A", "-f", "--", *metadata["selected_files"])
        tree = git(target, "write-tree").decode().strip()
        # Keep snapshot blobs reachable without committing or changing the source index.
        ref = "refs/cli-agent-bridge/snapshots/" + hashlib.sha256(str(state).encode()).hexdigest()[:8] + "-" + key
        git(target, "update-ref", ref, tree)
        metadata.update(ready=True, snapshot_tree=tree, snapshot_ref=ref)
        save_manifest(folder / "workspace.json", metadata)
    return str(target), metadata


def directory_copy(state, source, request_id, snapshot_files):
    root = Path(source).expanduser().resolve()
    if not root.is_dir():
        raise BridgeError("invalid_workspace", "Copy source must be an existing directory")
    key = hashlib.sha256(request_id.encode()).hexdigest()[:24]
    folder, metadata = snapshot(state, root, key, snapshot_files, None, "copy")
    target = state / "copies" / key
    if metadata["ready"] and not target.is_dir():
        raise BridgeError(
            "workspace_missing", "The retained directory copy is missing; use a new request to recreate it"
        )
    target.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not metadata["ready"]:
        apply_snapshot(folder, metadata, target)
        metadata["ready"] = True
        save_manifest(folder / "workspace.json", metadata)
    return str(target), metadata


def chunk_bytes(data: bytes, chunk_size: int = MAX_WORKSPACE_CHUNK_BYTES) -> list[tuple[int, bytes, str]]:
    """Split `data` into aligned content-addressed chunks: (offset, bytes, sha256(chunk))."""
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "data must be bytes")
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int):
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "chunk_size must be an integer")
    if not 1 <= chunk_size <= MAX_WORKSPACE_CHUNK_BYTES:
        raise BridgeError(
            PeerErrorCode.INVALID_PARAMS,
            f"chunk_size must be between 1 and {MAX_WORKSPACE_CHUNK_BYTES}",
        )
    raw = bytes(data)
    return [
        (offset, raw[offset:offset + chunk_size], hashlib.sha256(raw[offset:offset + chunk_size]).hexdigest())
        for offset in range(0, len(raw), chunk_size)
    ]


def _resolve_manifest_commit(root: Path, value: str) -> str:
    if not isinstance(value, str) or not value or value.startswith("-"):
        raise BridgeError("invalid_base", "A valid Git base revision is required")
    return git(root, "rev-parse", "--verify", value + "^{commit}").decode().strip()


def _manifest_repo_root(root: Path) -> Path:
    resolved = Path(root).expanduser().resolve()
    if not resolved.is_dir():
        raise BridgeError("invalid_workspace", "Workspace root must be an existing directory")
    try:
        top = Path(git(resolved, "rev-parse", "--show-toplevel").decode().strip()).resolve()
    except BridgeError as error:
        raise BridgeError("invalid_workspace", "Git workspace delivery requires a Git repository") from error
    if top != resolved:
        raise BridgeError("invalid_workspace", "Workspace delivery requires the repository root")
    return resolved


def _base_inventory(root: Path, commit: str) -> dict[str, dict[str, Any]]:
    """All supported tracked files at `commit`: path -> {sha256, size, mode}.

    Symlinks, submodules, directories and special Git modes are rejected rather than silently skipped (design 4.3).
    """
    output = git(root, "ls-tree", "-r", "-z", "--full-tree", commit)
    inventory: dict[str, dict[str, Any]] = {}
    for record in output.split(b"\0"):
        if not record:
            continue
        meta, raw_path = record.split(b"\t", 1)
        mode, kind, object_id = meta.split(b" ")
        try:
            name = raw_path.decode("utf-8")
        except UnicodeDecodeError:
            raise BridgeError("invalid_snapshot", "Tracked paths must be valid UTF-8") from None
        validate_manifest_path(name)
        if kind != b"blob" or mode not in {b"100644", b"100755"}:
            raise BridgeError(
                PeerErrorCode.WORKSPACE_CONFLICT,
                f"Unsupported tracked entry in the base commit: {name!r}",
            )
        content = git(root, "cat-file", "blob", object_id.decode("ascii"))
        inventory[name] = {
            "sha256": hashlib.sha256(content).hexdigest(),
            "size": len(content),
            "mode": 0o755 if mode == b"100755" else 0o644,
        }
    return inventory


def _selected_record(root: Path, name: str) -> dict[str, Any] | None:
    path = selected_path(root, name)
    try:
        before = path.lstat()
    except FileNotFoundError:
        return None
    if path.is_symlink() or not stat.S_ISREG(before.st_mode):
        raise BridgeError("invalid_snapshot", "Select individual regular files, not directories or devices")
    mode = stat.S_IMODE(before.st_mode)
    if mode & ~0o777:
        raise BridgeError("invalid_snapshot", "Files may only carry normal permission bits (0..0777)")
    try:
        content = path.read_bytes()
    except OSError as error:
        raise BridgeError("invalid_snapshot", f"Could not read selected file {name!r}: {error}") from error
    return {
        "sha256": hashlib.sha256(content).hexdigest(),
        "size": len(content),
        "mode": mode,
    }


def _normalize_selection(selected_files: list[str] | None) -> list[str]:
    if selected_files is None:
        return []
    if not isinstance(selected_files, list):
        raise BridgeError("invalid_snapshot", "selected_files must be a list of relative file names")
    names = []
    for name in selected_files:
        validate_manifest_path(name, "selected_files[]")
        names.append(name)
    if len(set(names)) != len(names):
        raise BridgeError("invalid_snapshot", "selected_files must contain unique paths")
    return names


def _check_workspace_limits(files: list[dict[str, Any]], final_files: dict[str, dict[str, Any]]) -> int:
    if len(files) > MAX_WORKSPACE_FILES or len(final_files) > MAX_WORKSPACE_FILES:
        raise BridgeError(
            PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED,
            f"Workspace exceeds the {MAX_WORKSPACE_FILES}-file limit",
        )
    total = 0
    for record in final_files.values():
        size = record["size"]
        if size > MAX_WORKSPACE_FILE_BYTES:
            raise BridgeError(
                PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED,
                f"Workspace file exceeds the {MAX_WORKSPACE_FILE_BYTES}-byte limit",
            )
        total += size
    if total > MAX_WORKSPACE_TOTAL_BYTES:
        raise BridgeError(
            PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED,
            f"Workspace exceeds the {MAX_WORKSPACE_TOTAL_BYTES}-byte total limit",
        )
    return total


def create_workspace_manifest(
    project_id: str,
    workspace_id: str,
    root: Path,
    delivery_mode: str,
    base_commit: str | None = None,
    selected_files: list[str] | None = None,
    authority_ref: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build an immutable workspace manifest (design 4.3) without reading anything after capture.

    Patch mode lists only the selected differences from the fixed base commit; snapshot mode lists the complete
    final input (base tracked files overlaid with the selected current bytes). Both digest the complete final
    input, never just the transferred difference. `authority_ref` belongs to the caller's identity envelope and
    is intentionally not covered by the immutable manifest digest.
    """
    if not isinstance(project_id, str) or not project_id:
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "project_id must be a non-empty string")
    if (
        not isinstance(workspace_id, str)
        or not workspace_id
        or "\0" in workspace_id
        or "/" in workspace_id
        or "\\" in workspace_id
        or workspace_id in {".", ".."}
    ):
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "workspace_id must be a safe path component")
    if delivery_mode not in {"patch", "snapshot"}:
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "delivery_mode must be patch or snapshot")
    selection = _normalize_selection(selected_files)
    resolved_root = Path(root).expanduser().resolve()

    base: dict[str, dict[str, Any]] = {}
    source_base_commit: str | None = None
    if base_commit is not None:
        resolved_root = _manifest_repo_root(resolved_root)
        source_base_commit = _resolve_manifest_commit(resolved_root, base_commit)
        base = _base_inventory(resolved_root, source_base_commit)
    elif delivery_mode == "patch":
        raise BridgeError("invalid_workspace", "Patch delivery requires a base commit")
    else:
        resolved_root = Path(root).expanduser().resolve()
        if resolved_root.is_dir():
            try:
                resolved_root = _manifest_repo_root(resolved_root)
                default_base = _resolve_manifest_commit(resolved_root, "HEAD")
            except BridgeError:
                default_base = None
            if default_base is not None:
                base = _base_inventory(resolved_root, default_base)

    final: dict[str, dict[str, Any]] = {
        path: {"sha256": record["sha256"], "size": record["size"], "mode": record["mode"]}
        for path, record in base.items()
    }
    files: list[dict[str, Any]] = []

    for name in selection:
        current = _selected_record(resolved_root, name)
        before = base.get(name)
        if current is None:
            if before is None:
                raise BridgeError("invalid_snapshot", f"Selected file does not exist: {name}")
            files.append({
                "path": name, "op": "delete", "size": 0,
                "before_sha256": before["sha256"], "sha256": None, "mode": 0,
            })
            final.pop(name, None)
            continue
        changed = (
            before is None
            or before["sha256"] != current["sha256"]
            or before["size"] != current["size"]
            or before["mode"] != current["mode"]
        )
        if delivery_mode == "patch":
            if changed:
                files.append({
                    "path": name, "op": "put", "size": current["size"],
                    "before_sha256": before["sha256"] if before else None,
                    "sha256": current["sha256"], "mode": current["mode"],
                })
        final[name] = current

    if delivery_mode == "snapshot":
        files = [
            {
                "path": path, "op": "put", "size": record["size"], "before_sha256": None,
                "sha256": record["sha256"], "mode": record["mode"],
            }
            for path, record in final.items()
        ]
        files.extend(
            {
                "path": name, "op": "delete", "size": 0, "before_sha256": base[name]["sha256"],
                "sha256": None, "mode": 0,
            }
            for name in selection
            if name not in final
        )
    files.sort(key=lambda entry: entry["path"].encode("utf-8"))

    _check_workspace_limits(files, final)
    final_entries = [
        {
            "path": path, "op": "put", "size": record["size"],
            "before_sha256": None, "sha256": record["sha256"], "mode": record["mode"],
        }
        for path, record in final.items()
    ]
    manifest: dict[str, Any] = {
        "schema": 1,
        "workspace_id": workspace_id,
        "project_id": project_id,
        "workspace_mode": "git",
        "source_base_commit": source_base_commit,
        "delivery_mode": delivery_mode,
        "selected_files": selection,
        "input_digest": compute_input_digest(final_entries),
        "files": files,
        "patch": None,
        "total_bytes": sum(entry["size"] for entry in files if entry["op"] == "put"),
        "file_count": len(files),
    }
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(encoded) > MAX_WORKSPACE_MANIFEST_BYTES:
        raise BridgeError(
            PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED,
            f"Workspace manifest exceeds the {MAX_WORKSPACE_MANIFEST_BYTES}-byte limit",
        )
    # Compute eagerly so an unsendable manifest fails at generation time.
    compute_manifest_digest(manifest)
    return manifest


def _display_path(value):
    return json.dumps(os.fsdecode(value), ensure_ascii=False)[1:-1].encode("utf-8", "backslashreplace").decode("utf-8")


def capture_patch(directory, destination, workspace=None):
    if workspace and workspace.get("kind") == "copy":
        changes, current = {}, set()
        root = Path(directory)
        for path in root.rglob("*"):
            name = path.relative_to(root).as_posix()
            if path.is_symlink():
                changes[name] = {"symlink": os.readlink(path)}
                current.add(name)
            elif path.is_file():
                current.add(name)
                with path.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                mode = stat.S_IMODE(path.stat().st_mode)
                original = workspace["files"].get(name, {})
                if original.get("sha256") != digest or original.get("mode") != mode:
                    changes[name] = {"sha256": digest, "mode": mode, "path": str(path)}
        for name in workspace["files"].keys() - current:
            changes[name] = {"deleted": True}
        destination = destination.with_suffix(".files.json")
        save_manifest(destination, changes)
        names = list(changes)
        result = {"patch": None, "changes": str(destination), "workspace": directory,
                  "changed_files": [_display_path(name) for name in names]}
        if result["changed_files"] != names:
            result["changed_files_raw_hex"] = [os.fsencode(name).hex() for name in names]
        return result
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
            temporary = Path(stream.name)
            git(directory, "diff", "--binary", (workspace or {}).get("snapshot_tree", "HEAD"), stdout=stream)
            raw_status = git(directory, "status", "--porcelain=v1")
            status = raw_status.decode("utf-8", "backslashreplace")
            untracked = [name for name in git(directory, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0") if name]
            for name in untracked:
                diff = subprocess.run(
                    ["git", "diff", "--no-index", "--binary", "--", "/dev/null", name],
                    cwd=directory, stdout=stream, stderr=subprocess.PIPE, timeout=60,
                )
                if diff.returncode not in {0, 1}:
                    raise BridgeError("git_error", "Could not capture an untracked file")
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    result = {
        "patch": str(destination),
        "status": status,
        "untracked_files": [_display_path(name) for name in untracked],
        "base_snapshot": (workspace or {}).get("snapshot_tree", "HEAD"),
    }
    if result["untracked_files"] != [os.fsdecode(name) for name in untracked]:
        result["untracked_files_raw_hex"] = [name.hex() for name in untracked]
    if status.encode("utf-8") != raw_status:
        result["status_raw_hex"] = raw_status.hex()
    return result
