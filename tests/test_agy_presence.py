import fcntl
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_bridge.agy_presence import inactive_reason, unsupported_platform_reason


class AgYPresenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.proc = self.root / "proc"
        self.proc.mkdir()
        (self.proc / "locks").touch()
        self.home = self.root / "native"
        (self.home / "presence").mkdir(parents=True)
        self.executable = self.root / "bin/agy"

    def check(self, exclude_pid=None):
        return inactive_reason("target", str(self.executable), native_home=self.home, proc_root=self.proc,
                               exclude_pid=exclude_pid)

    def process(self, pid, args, *, binary="agy-real", state="S"):
        path = self.proc / str(pid)
        path.mkdir()
        (path / "fd").mkdir()
        (path / "stat").write_text(f"{pid} ({binary}) {state} 1 0 0\n")
        (path / "comm").write_text(binary + "\n")
        (path / "cmdline").write_bytes(b"\0".join(a.encode() for a in args) + b"\0")
        (path / "exe").symlink_to(self.root / "bin" / binary)
        return path

    def presence(self, pid, native_id):
        path = self.home / "presence" / (native_id + ".lock")
        path.touch()
        (self.proc / str(pid) / "fd/3").symlink_to(path)
        with (self.proc / "locks").open("a") as file:
            file.write(f"1: FLOCK ADVISORY WRITE {pid} 00:00:{path.stat().st_ino} 0 EOF\n")
        return path

    def test_no_process_or_stale_unlocked_presence_allows_start(self):
        self.assertIsNone(self.check())
        (self.home / "presence/target.lock").touch()
        self.assertIsNone(self.check())

    def test_both_conversation_argument_forms_are_busy_without_native_lock(self):
        for i, args in enumerate((["agy-real", "--conversation", "target"],
                                  ["agy-real", "--conversation=target"])):
            with self.subTest(args=args):
                p = self.process(100 + i, args)
                self.assertIn("conversation is active", self.check())
                for entry in (p / "fd").iterdir():
                    entry.unlink()
                (p / "fd").rmdir()
                for entry in p.iterdir():
                    entry.unlink()
                p.rmdir()

    def test_other_explicit_conversation_and_non_agy_process_are_ignored(self):
        self.process(101, ["agy-real", "--conversation=other"])
        self.process(102, ["python3", "--conversation=target"], binary="python3")
        self.assertIsNone(self.check())

    def test_continue_and_unidentified_agy_are_busy_unless_presence_identifies_other_session(self):
        for i, args in enumerate((["agy-real", "-c"], ["agy-real", "--continue"], ["agy-real"])):
            self.process(200 + i, args)
            self.assertIn("unconfirmed conversation", self.check())
            self.presence(200 + i, "other-" + str(i))
            self.assertIsNone(self.check())

    def test_open_fd_without_held_lock_does_not_identify_continue_session(self):
        self.process(201, ["agy-real", "-c"])
        path = self.home / "presence/other.lock"
        path.touch()
        (self.proc / "201/fd/3").symlink_to(path)
        self.assertIn("unconfirmed conversation", self.check())

    def test_held_native_lock_is_busy_even_without_visible_agy_argv(self):
        path = self.home / "presence/target.lock"
        path.touch()
        with path.open("rb") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIn("lock is held", self.check())

    def test_exclude_pid_only_allows_its_verified_native_lock_and_not_another_process(self):
        self.process(301, ["agy-real", "--conversation=target"])
        path = self.presence(301, "target")
        with path.open("rb") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIsNone(self.check(exclude_pid=301))
            self.process(302, ["agy-real", "--conversation=target"])
            self.assertIn("pid 302", self.check(exclude_pid=301))

    def test_exclude_pid_never_ignores_unknown_lock_owner(self):
        self.process(301, ["agy-real", "--conversation=target"])
        path = self.home / "presence/target.lock"
        path.touch()
        with path.open("rb") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIn("lock is held", self.check(exclude_pid=301))

    def test_missing_lock_owner_information_never_allows_excluded_process(self):
        self.process(301, ["agy-real", "--conversation=target"])
        path = self.home / "presence/target.lock"
        path.touch()
        (self.proc / "locks").unlink()
        with path.open("rb") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIn("lock is held", self.check(exclude_pid=301))

    def test_zombie_and_current_check_process_are_not_active_agy(self):
        self.process(401, ["agy-real", "--conversation=target"], state="Z")
        self.process(os.getpid(), ["agy-real", "--conversation=target"])
        self.assertIsNone(self.check())

    def test_unreadable_live_state_and_missing_proc_are_busy(self):
        p = self.process(501, ["agy-real", "--conversation=target"])
        (p / "stat").write_text("malformed")
        self.assertIn("Cannot confirm", self.check())
        self.assertIn("Cannot inspect", inactive_reason("target", str(self.executable),
                                                       native_home=self.home, proc_root=self.root / "missing"))

    def test_empty_argv_for_live_agy_is_not_mistaken_for_closed(self):
        self.process(502, [])
        self.assertIn("Cannot confirm", self.check())

    def test_permission_error_during_process_inspection_is_not_offline(self):
        self.process(503, ["agy-real", "--conversation=target"])
        original = Path.read_bytes

        def protected(path):
            if path == self.proc / "503/cmdline":
                raise PermissionError("not readable")
            return original(path)

        with patch.object(Path, "read_bytes", protected):
            self.assertIn("Cannot confirm", self.check())

    def test_unreadable_exe_for_systemd_does_not_block_agy_resume(self):
        self.process(1180, ["/usr/lib/systemd/systemd", "--user"], binary="systemd")
        original = os.readlink

        def protected(path, *args, **kwargs):
            if Path(path) == self.proc / "1180/exe":
                raise PermissionError("systemd exe is protected")
            return original(path, *args, **kwargs)

        with patch("agent_bridge.agy_presence.os.readlink", protected):
            self.assertIsNone(self.check())

    def test_unreadable_exe_for_confirmed_agy_still_blocks_resume(self):
        self.process(1181, ["agy-real", "--conversation=target"])
        original = os.readlink

        def protected(path, *args, **kwargs):
            if Path(path) == self.proc / "1181/exe":
                raise PermissionError("AGY exe is protected")
            return original(path, *args, **kwargs)

        with patch("agent_bridge.agy_presence.os.readlink", protected):
            self.assertIn("Cannot confirm", self.check())

    def test_unreadable_exe_and_missing_comm_do_not_imply_non_agy(self):
        p = self.process(1182, ["/usr/lib/systemd/systemd", "--user"], binary="systemd")
        (p / "comm").unlink()
        original = os.readlink

        def protected(path, *args, **kwargs):
            if Path(path) == self.proc / "1182/exe":
                raise PermissionError("exe is protected")
            return original(path, *args, **kwargs)

        with patch("agent_bridge.agy_presence.os.readlink", protected):
            self.assertIn("Cannot confirm", self.check())

    def test_presence_probe_failure_is_not_offline(self):
        (self.home / "presence/target.lock").touch()
        with patch("agent_bridge.agy_presence.fcntl.flock", side_effect=PermissionError("unreadable")):
            self.assertIn("Cannot inspect", self.check())

    def test_custom_script_executable_is_recognized(self):
        self.process(601, ["python3", str(self.executable), "--conversation=target"], binary="python3")
        self.assertIn("conversation is active", self.check())

    def test_platform_capability_is_explicit_and_does_not_claim_darwin_owner_support(self):
        self.assertIsNone(unsupported_platform_reason("linux"))
        self.assertIsNone(unsupported_platform_reason("linux2"))
        for platform_name, label in (("darwin", "macOS"), ("freebsd14", "freebsd14")):
            with self.subTest(platform=platform_name):
                reason = unsupported_platform_reason(platform_name)
                self.assertIn("unsupported", reason)
                self.assertIn(label, reason)
                self.assertIn("flock ownership", reason)

    def test_darwin_without_proc_fixture_fails_closed_before_inspection(self):
        with patch("agent_bridge.agy_presence.sys.platform", "darwin"), \
             patch("agent_bridge.agy_presence._target_lock_reason", side_effect=AssertionError("no Linux lock probe")), \
             patch.object(Path, "iterdir", side_effect=AssertionError("no /proc scan")):
            for excluded in (None, 123):
                reason = inactive_reason("target", "agy", native_home=self.home, exclude_pid=excluded)
                self.assertIn("unsupported on macOS", reason)

    def test_explicit_proc_fixture_keeps_linux_semantics_on_darwin(self):
        with patch("agent_bridge.agy_presence.sys.platform", "darwin"):
            self.assertIsNone(self.check())
            self.process(801, ["agy-real", "--conversation=target"])
            self.assertIn("conversation is active", self.check())
            self.assertIsNone(self.check(exclude_pid=801))

    def test_unverifiable_flock_is_not_treated_as_excluded_owner_on_darwin(self):
        path = self.home / "presence/target.lock"
        path.touch()
        with path.open("rb") as handle, patch("agent_bridge.agy_presence.sys.platform", "darwin"):
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIn("unsupported on macOS", inactive_reason("target", "agy", native_home=self.home,
                                                                exclude_pid=os.getpid()))


if __name__ == "__main__":
    unittest.main()
