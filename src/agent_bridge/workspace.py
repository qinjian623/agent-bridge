from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

from .errors import BridgeError


def git(directory, *args):
    result = subprocess.run(
        ["git", "-C", str(directory), "--literal-pathspecs", *args], capture_output=True, timeout=60
    )
    if result.returncode:
        raise BridgeError("git_error", result.stderr.decode(errors="replace")[-2000:])
    return result.stdout


def selected_path(root, name):
    if not isinstance(name, str) or not name or "\x00" in name:
        raise BridgeError("invalid_snapshot", "Snapshot paths must be individual relative file names")
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
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2))
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
        return {"patch": None, "changes": str(destination), "workspace": directory, "changed_files": list(changes)}
    patch = git(directory, "diff", "--binary", (workspace or {}).get("snapshot_tree", "HEAD"))
    status = git(directory, "status", "--porcelain=v1").decode(errors="replace")
    untracked = [
        name
        for name in git(directory, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")
        if name
    ]
    for name in untracked:
        diff = subprocess.run(
            ["git", "diff", "--no-index", "--binary", "--", "/dev/null", name],
            cwd=directory, capture_output=True, timeout=60,
        )
        if diff.returncode not in {0, 1}:
            raise BridgeError("git_error", "Could not capture an untracked file")
        patch += diff.stdout
    destination.write_bytes(patch)
    destination.chmod(0o600)
    return {
        "patch": str(destination),
        "status": status,
        "untracked_files": untracked,
        "base_snapshot": (workspace or {}).get("snapshot_tree", "HEAD"),
    }
