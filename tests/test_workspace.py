import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from agent_bridge import workspace as workspace_module
from agent_bridge.errors import BridgeError
from agent_bridge.store import encode
from agent_bridge.workspace import capture_patch, directory_copy, git, worktree


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        git(self.source, "init", "-q")
        for name, content in {
            "tracked.txt": "base\n",
            "deleted.txt": "delete me\n",
            "untouched.txt": "original\n",
        }.items():
            (self.source / name).write_text(content)
        git(self.source, "add", ".")
        git(self.source, "-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture")
        self.state = self.root / "state"

    def test_selected_dirty_deleted_binary_and_untracked_files_have_stable_snapshot(self):
        (self.source / "tracked.txt").write_text("selected input\n")
        (self.source / "deleted.txt").unlink()
        (self.source / "untouched.txt").write_text("unselected dirty\n")
        (self.source / "new.bin").write_bytes(b"\x00\x01\xfe")
        git(self.source, "add", "tracked.txt")
        original_index = git(self.source, "diff", "--cached", "--binary")
        selected = ["tracked.txt", "deleted.txt", "new.bin"]
        directory, metadata = worktree(self.state, self.source, "snapshot", snapshot_files=selected)
        target = Path(directory)
        self.assertEqual("selected input\n", (target / "tracked.txt").read_text())
        self.assertFalse((target / "deleted.txt").exists())
        self.assertEqual("original\n", (target / "untouched.txt").read_text())
        self.assertEqual(b"\x00\x01\xfe", (target / "new.bin").read_bytes())
        (target / "tracked.txt").write_text("child change\n")
        (target / "created.py").write_text("answer = 42\n")
        result = capture_patch(directory, self.state / "result.patch", metadata)
        patch = Path(result["patch"]).read_text()
        self.assertIn("-selected input", patch)
        self.assertNotIn("-base", patch)
        self.assertIn("+answer = 42", patch)
        self.assertEqual(["created.py"], result["untracked_files"])
        (self.source / "tracked.txt").write_text("source changed after capture\n")
        again, saved = worktree(self.state, self.source, "snapshot", snapshot_files=selected)
        self.assertEqual(directory, again)
        self.assertEqual(metadata["snapshot_tree"], saved["snapshot_tree"])
        self.assertEqual("child change\n", (target / "tracked.txt").read_text())
        self.assertEqual(original_index, git(self.source, "diff", "--cached", "--binary"))

    def test_snapshot_rejects_traversal_symlinks_directories_and_missing_files(self):
        (self.source / "link").symlink_to(self.root)
        for index, name in enumerate(
            ("../outside", ".git/config", "link/file", "missing", "/tmp/file", ".", "./tracked.txt")
        ):
            with self.assertRaises(BridgeError, msg=name):
                worktree(self.state, self.source, str(index), snapshot_files=[name])

    def test_non_git_copy_is_explicit_and_preserves_source(self):
        plain = self.root / "plain"
        plain.mkdir()
        (plain / "file.txt").write_text("input")
        (plain / "private.txt").write_text("excluded")
        directory, metadata = directory_copy(self.state, plain, "copy", ["file.txt"])
        target = Path(directory)
        self.assertFalse((target / "private.txt").exists())
        (target / "file.txt").unlink()
        (target / "new.txt").write_text("output")
        result = capture_patch(directory, self.state / "copy.patch", metadata)
        changes = json.loads(Path(result["changes"]).read_text())
        self.assertTrue(changes["file.txt"]["deleted"])
        self.assertIn("new.txt", changes)
        self.assertEqual("input", (plain / "file.txt").read_text())
        self.assertEqual(directory, directory_copy(self.state, plain, "copy", ["file.txt"])[0])

    def test_copy_records_permission_only_changes_and_modes_of_changed_and_new_files(self):
        script = self.source / "run.sh"
        script.write_text("#!/bin/sh\necho ok\n")
        script.chmod(0o644)
        directory, metadata = directory_copy(self.state, self.source, "copy-mode", ["run.sh"])
        target = Path(directory) / "run.sh"
        self.assertEqual([], capture_patch(directory, self.state / "unchanged.patch", metadata)["changed_files"])
        target.chmod(0o755)
        result = capture_patch(directory, self.state / "mode.patch", metadata)
        changes = json.loads(Path(result["changes"]).read_text())
        self.assertEqual(["run.sh"], result["changed_files"])
        self.assertEqual(metadata["files"]["run.sh"]["sha256"], changes["run.sh"]["sha256"])
        self.assertEqual(0o755, changes["run.sh"]["mode"])
        target.write_text("#!/bin/sh\necho changed\n")
        target.chmod(0o600)
        new = Path(directory) / "new.sh"
        new.write_text("new")
        new.chmod(0o700)
        changes = json.loads(Path(capture_patch(directory, self.state / "content.patch", metadata)["changes"]).read_text())
        self.assertNotEqual(metadata["files"]["run.sh"]["sha256"], changes["run.sh"]["sha256"])
        self.assertEqual(0o600, changes["run.sh"]["mode"])
        self.assertEqual(0o700, changes["new.sh"]["mode"])
        self.assertEqual(0o644, script.stat().st_mode & 0o777)

    def test_snapshot_patch_applies_to_same_input_without_source_changes(self):
        (self.source / "tracked.txt").write_text("input snapshot\n")
        directory, metadata = worktree(self.state, self.source, "one", snapshot_files=["tracked.txt"])
        destination, _ = worktree(self.state, self.source, "two", snapshot_files=["tracked.txt"])
        (Path(directory) / "tracked.txt").write_text("output\n")
        (Path(directory) / "new.bin").write_bytes(b"\x00\xfe\x02")
        (Path(directory) / "new.txt").write_text("new text\n")
        expected = git(directory, "diff", "--binary", metadata["snapshot_tree"])
        names = git(directory, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0")[:-1]
        for name in names:
            diff = subprocess.run(
                ["git", "diff", "--no-index", "--binary", "--", "/dev/null", name],
                cwd=directory, capture_output=True, timeout=60,
            )
            self.assertEqual(1, diff.returncode)
            expected += diff.stdout
        result = capture_patch(directory, self.state / "changes.patch", metadata)
        self.assertEqual(expected, Path(result["patch"]).read_bytes())
        self.assertEqual(0o600, Path(result["patch"]).stat().st_mode & 0o777)
        subprocess.run(["git", "-C", destination, "apply", result["patch"]], check=True)
        self.assertEqual("output\n", (Path(destination) / "tracked.txt").read_text())
        self.assertEqual(b"\x00\xfe\x02", (Path(destination) / "new.bin").read_bytes())
        self.assertEqual("new text\n", (Path(destination) / "new.txt").read_text())
        self.assertEqual("input snapshot\n", (self.source / "tracked.txt").read_text())

    def test_capture_failure_keeps_previous_patch_and_removes_partial_file(self):
        directory, metadata = worktree(self.state, self.source, "failed-capture")
        (Path(directory) / "tracked.txt").write_text("output\n")
        for name in ("first.txt", "second.txt"):
            (Path(directory) / name).write_text("new file\n")
        destination = self.state / "existing.patch"
        destination.write_bytes(b"previous complete patch\n")
        destination.chmod(0o640)
        original_run = subprocess.run
        for failure in ("git_error", "timeout"):
            with self.subTest(failure=failure):
                files_before = set(self.state.iterdir())
                calls = 0

                def run(argv, failure=failure, **kwargs):
                    nonlocal calls
                    if "--no-index" in argv:
                        calls += 1
                        stream = kwargs["stdout"]
                        self.assertEqual(0o600, os.fstat(stream.fileno()).st_mode & 0o777)
                        if calls == 2:
                            os.write(stream.fileno(), b"partial second diff\n")
                            if failure == "timeout":
                                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
                            return subprocess.CompletedProcess(argv, 2, stderr=b"git failed")
                    return original_run(argv, **kwargs)

                with patch("agent_bridge.workspace.subprocess.run", side_effect=run):
                    with self.assertRaises(BridgeError if failure == "git_error" else subprocess.TimeoutExpired):
                        capture_patch(directory, destination, metadata)
                self.assertEqual(2, calls)
                self.assertEqual(b"previous complete patch\n", destination.read_bytes())
                self.assertEqual(0o640, destination.stat().st_mode & 0o777)
                self.assertEqual(files_before, set(self.state.iterdir()))

    def test_capture_preserves_raw_git_paths_contents_and_symlink_targets(self):
        git(self.source, "config", "core.quotePath", "false")
        tracked = (b"tracked-\xff.txt", b"tracked-\\xff.txt")
        for name in tracked:
            with open(os.fsencode(self.source) + b"/" + name, "wb") as stream:
                stream.write(b"original\n")
        git(self.source, "add", "--", *tracked)
        git(self.source, "-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "raw names")
        directory, metadata = worktree(self.state, self.source, "raw-capture")
        destination, _ = worktree(self.state, self.source, "raw-apply")
        raw_root = os.fsencode(directory)
        for name in tracked:
            with open(raw_root + b"/" + name, "wb") as stream:
                stream.write(b"modified tracked\n")
        untracked = {
            b"new-\xff.txt": b"binary\x00\xff\n",
            b"new-\\xff.txt": b"literal escape\n",
            b"new-line\n.txt": b"newline path\n",
            "新文件.txt".encode(): b"unicode name\n",
        }
        for name, content in untracked.items():
            with open(raw_root + b"/" + name, "wb") as stream:
                stream.write(content)
        link_name, link_target = b"link-\xff", b"target-\xff\n"
        os.symlink(link_target, raw_root + b"/" + link_name)
        raw_status = git(directory, "status", "--porcelain=v1")
        result = capture_patch(directory, self.state / "raw.patch", metadata)
        names = [bytes.fromhex(value) for value in result["untracked_files_raw_hex"]]
        self.assertEqual(set(untracked) | {link_name}, set(names))
        displays = dict(zip(names, result["untracked_files"], strict=True))
        self.assertEqual("new-\\udcff.txt", displays[b"new-\xff.txt"])
        self.assertEqual("new-\\\\xff.txt", displays[b"new-\\xff.txt"])
        self.assertEqual("new-line\\n.txt", displays[b"new-line\n.txt"])
        self.assertEqual("新文件.txt", displays["新文件.txt".encode()])
        self.assertEqual(len(names), len(set(result["untracked_files"])))
        self.assertEqual(raw_status, bytes.fromhex(result["status_raw_hex"]))
        # This is the same UTF-8 JSON and SQLite transport used by task results.
        serialized = encode(result)
        serialized.encode("utf-8")
        with closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE results(value TEXT)")
            db.execute("INSERT INTO results VALUES(?)", (serialized,))
            self.assertEqual(result, json.loads(db.execute("SELECT value FROM results").fetchone()[0]))
        patch = Path(result["patch"]).read_bytes()
        self.assertIn(b"GIT binary patch", patch)
        subprocess.run(["git", "-C", destination, "apply", result["patch"]], check=True)
        for name in tracked:
            with open(os.fsencode(destination) + b"/" + name, "rb") as stream:
                self.assertEqual(b"modified tracked\n", stream.read())
            with open(os.fsencode(self.source) + b"/" + name, "rb") as stream:
                self.assertEqual(b"original\n", stream.read())
        for name, content in untracked.items():
            for root in (raw_root, os.fsencode(destination)):
                with open(root + b"/" + name, "rb") as stream:
                    self.assertEqual(content, stream.read())
        self.assertEqual(link_target, os.readlink(os.fsencode(destination) + b"/" + link_name))
        self.assertEqual(link_target, os.readlink(raw_root + b"/" + link_name))

    def test_copy_capture_keeps_raw_names_and_symlink_targets_in_valid_json(self):
        directory, metadata = directory_copy(self.state, self.source, "raw-copy", ["tracked.txt"])
        raw_root = os.fsencode(directory)
        files = {b"raw-\xff": b"raw", b"raw-\\xff": b"literal", b"line\nname": b"line"}
        for name, content in files.items():
            with open(raw_root + b"/" + name, "wb") as stream:
                stream.write(content)
        link_name, link_target = b"link-\xff", b"../target-\xfe\n"
        os.symlink(link_target, raw_root + b"/" + link_name)
        (Path(directory) / "新文件.txt").write_text("normal unicode")
        result = capture_patch(directory, self.state / "raw-copy.patch", metadata)
        encoded = encode(result).encode("utf-8")
        self.assertEqual(result, json.loads(encoded))
        raw_manifest = Path(result["changes"]).read_bytes()
        self.assertIn(b"\\udcff", raw_manifest)
        self.assertIn("新文件.txt".encode(), raw_manifest)
        changes = json.loads(raw_manifest.decode("utf-8"))
        raw_names = {os.fsencode(name): record for name, record in changes.items()}
        self.assertEqual(set(files) | {link_name, "新文件.txt".encode()}, set(raw_names))
        self.assertEqual(link_target, os.fsencode(raw_names[link_name]["symlink"]))
        names = [bytes.fromhex(value) for value in result["changed_files_raw_hex"]]
        self.assertEqual(set(raw_names), set(names))
        self.assertEqual(len(names), len(set(result["changed_files"])))
        for name, content in files.items():
            self.assertEqual(raw_root + b"/" + name, os.fsencode(raw_names[name]["source_path"]))
            with open(os.fsencode(raw_names[name]["path"]), "rb") as stream:
                self.assertEqual(content, stream.read())
        self.assertEqual(link_target, os.readlink(raw_root + b"/" + link_name))
        self.assertEqual("base\n", (self.source / "tracked.txt").read_text())

    def test_copy_capture_retains_changed_and_new_files_after_workspace_changes(self):
        plain = self.root / "retained-source"
        plain.mkdir()
        (plain / "file.txt").write_text("input\n")
        (plain / "unchanged.txt").write_text("same\n")
        directory, metadata = directory_copy(
            self.state, plain, "retained-copy", ["file.txt", "unchanged.txt"],
        )
        target = Path(directory)
        (target / "file.txt").write_text("task output\n")
        (target / "file.txt").chmod(0o640)
        (target / "new.txt").write_text("new output\n")
        (target / "new.txt").chmod(0o644)
        result = capture_patch(directory, self.state / "retained.patch", metadata)
        changes = json.loads(Path(result["changes"]).read_text())
        self.assertNotIn("unchanged.txt", changes)
        for name, content, mode in (
            ("file.txt", b"task output\n", 0o640),
            ("new.txt", b"new output\n", 0o644),
        ):
            record = changes[name]
            retained = Path(record["path"])
            self.assertNotEqual(str(retained), str(target / name))
            self.assertEqual(str(target / name), record["source_path"])
            self.assertEqual(mode, record["mode"])
            self.assertEqual(content, retained.read_bytes())
            self.assertEqual(record["sha256"], hashlib.sha256(content).hexdigest())
        (target / "file.txt").write_text("later task\n")
        (target / "new.txt").unlink()
        self.assertEqual(b"task output\n", Path(changes["file.txt"]["path"]).read_bytes())
        self.assertEqual(b"new output\n", Path(changes["new.txt"]["path"]).read_bytes())

    def test_copy_capture_rejects_symlink_in_capture_root_ancestors(self):
        ancestor = self.root / "root-link"
        ancestor.symlink_to(self.root, target_is_directory=True)
        directory, metadata = directory_copy(self.state, self.source, "ancestor-link", ["tracked.txt"])
        relative = Path(directory).relative_to(self.root)
        with self.assertRaises(BridgeError) as caught:
            capture_patch(str(ancestor / relative), self.state / "ancestor.patch", metadata)
        self.assertEqual("invalid_snapshot", caught.exception.code)
        self.assertFalse((self.state / "ancestor.files.json").exists())

    def test_copy_capture_rejects_fifo_replacement_without_waiting_for_a_writer(self):
        code = '''
import os, sys, tempfile
from pathlib import Path
from agent_bridge.errors import BridgeError
from agent_bridge.workspace import _capture_root, _open_source_regular
with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / "file"
    path.write_text("original")
    root = _capture_root(Path(directory))
    try:
        path.unlink()
        os.mkfifo(path)
        try:
            _open_source_regular(root, "file")
        except BridgeError as error:
            assert error.code == "snapshot_changed"
        else:
            raise AssertionError("FIFO accepted as a regular file")
    finally:
        os.close(root)
'''
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=5)
        self.assertEqual(0, result.returncode, result.stderr)

    def test_copy_capture_rejects_parent_symlink_swap_before_hashing(self):
        plain = self.root / "swap-before-source"
        (plain / "parent").mkdir(parents=True)
        (plain / "parent" / "file.txt").write_text("input\n")
        outside = self.root / "swap-before-outside"
        outside.mkdir()
        (outside / "file.txt").write_text("outside secret\n")
        directory, metadata = directory_copy(
            self.state, plain, "swap-before", ["parent/file.txt"],
        )
        target = Path(directory)
        (target / "parent" / "file.txt").write_text("workspace output\n")
        original = workspace_module._regular_digest
        swapped = False

        def digest(root_fd, name):
            nonlocal swapped
            if not swapped:
                swapped = True
                (target / "parent").rename(target / "parent-original")
                (target / "parent").symlink_to(outside, target_is_directory=True)
            return original(root_fd, name)

        destination = self.state / "swap-before.patch"
        with patch("agent_bridge.workspace._regular_digest", side_effect=digest):
            with self.assertRaises(BridgeError) as caught:
                capture_patch(directory, destination, metadata)
        self.assertTrue(swapped)
        self.assertEqual("snapshot_changed", caught.exception.code)
        self.assertFalse(destination.with_suffix(".files.json").exists())
        self.assertFalse(list(self.state.glob("swap-before.patch.files-*")))
        for path in self.state.rglob("*"):
            if path.is_file() and not path.is_symlink():
                self.assertNotIn(b"outside secret", path.read_bytes())

    def test_copy_capture_rejects_parent_symlink_swap_between_hash_and_retention(self):
        plain = self.root / "swap-after-source"
        (plain / "parent").mkdir(parents=True)
        (plain / "parent" / "file.txt").write_text("input\n")
        outside = self.root / "swap-after-outside"
        outside.mkdir()
        (outside / "file.txt").write_text("outside secret\n")
        directory, metadata = directory_copy(
            self.state, plain, "swap-after", ["parent/file.txt"],
        )
        target = Path(directory)
        (target / "parent" / "file.txt").write_text("workspace output\n")
        original = workspace_module._retain_regular
        swapped = False

        def retain(root_fd, name, retained, digest, mode):
            nonlocal swapped
            if not swapped:
                swapped = True
                (target / "parent").rename(target / "parent-original")
                (target / "parent").symlink_to(outside, target_is_directory=True)
            return original(root_fd, name, retained, digest, mode)

        destination = self.state / "swap-after.patch"
        with patch("agent_bridge.workspace._retain_regular", side_effect=retain):
            with self.assertRaises(BridgeError) as caught:
                capture_patch(directory, destination, metadata)
        self.assertTrue(swapped)
        self.assertEqual("snapshot_changed", caught.exception.code)
        self.assertFalse(destination.with_suffix(".files.json").exists())
        self.assertFalse(list(self.state.glob("swap-after.patch.files-*")))
        for path in self.state.rglob("*"):
            if path.is_file() and not path.is_symlink():
                self.assertNotIn(b"outside secret", path.read_bytes())

    def test_non_utf8_selected_snapshot_input_is_rejected_explicitly(self):
        with self.assertRaises(BridgeError) as caught:
            directory_copy(self.state, self.source, "bad-selected", [os.fsdecode(b"raw-\xff")])
        self.assertEqual("invalid_snapshot", caught.exception.code)
