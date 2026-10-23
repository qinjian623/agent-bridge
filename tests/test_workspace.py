import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from agent_bridge.errors import BridgeError
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
        result = capture_patch(directory, self.state / "changes.patch", metadata)
        subprocess.run(["git", "-C", destination, "apply", result["patch"]], check=True)
        self.assertEqual("output\n", (Path(destination) / "tracked.txt").read_text())
        self.assertEqual(b"\x00\xfe\x02", (Path(destination) / "new.bin").read_bytes())
        self.assertEqual("input snapshot\n", (self.source / "tracked.txt").read_text())
