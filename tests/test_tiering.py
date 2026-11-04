"""Real files, Git trees and subprocesses; no bridge service or model calls."""

from __future__ import annotations

import fcntl
import importlib.util
import io
import json
import os
import re
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import unittest
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).parents[1] / "scripts/tiering.py"
SPEC = importlib.util.spec_from_file_location("tiering", SCRIPT)
tiering = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tiering)
START = datetime(2026, 10, 28, 12, tzinfo=UTC)


def at(seconds=0):
    return (START + timedelta(seconds=seconds)).isoformat()


def dispatch(task_id="task-1", **changes):
    return {
        "event": "task",
        "time": at(),
        "task_id": task_id,
        "profile": "codex-sol",
        "model": "actual-model",
        "effort": "high",
        "goal": "修复空输入",
        "allowed_files": ["allowed.py"],
        "test_command": "python -B -m unittest",
        "acceptance": ["空输入返回空列表"],
        "snapshot_tree": "saved-tree",
        "status": "queued",
        "attempt_id": None,
        **changes,
    }


def result(task_id="task-1", status="completed", seconds=10, **changes):
    return {
        "event": "task",
        "time": at(seconds),
        "task_id": task_id,
        "status": status,
        "attempt_id": "attempt-" + task_id,
        "reason": "执行结束",
        **changes,
    }


def review(task_id="task-1", decision="accepted", seconds=30, **changes):
    return {
        "event": "review",
        "time": at(seconds),
        "task_id": task_id,
        "attempt_id": "attempt-" + task_id,
        "decision": decision,
        "reason": "独立检查完成并已阅读 diff",
        "test_exit_code": 0,
        "out_of_scope_files": [],
        **changes,
    }


def python_command(code):
    return shlex.join([sys.executable, "-B", "-c", code])


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.PIPE).decode().strip()


def is_running(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()[0] not in {"Z", "X"}
    except FileNotFoundError:
        return False


def mixed_events():
    events = []
    for task_id, status, duration in (
        ("accepted", "completed", 30),
        ("rejected", "completed", 50),
        ("failed", "failed", 5),
        ("cancelled", "cancelled", 20),
        ("interrupted", "interrupted", 40),
        ("pending", "completed", 10),
        ("uncertain", "recovery_needed", 8),
        ("running", "running", 7),
    ):
        events.append(dispatch(task_id))
        events.append(result(task_id, status, seconds=duration if status != "completed" else 10))
        if task_id in {"accepted", "rejected"}:
            events.append(
                review(
                    task_id,
                    decision=task_id,
                    seconds=duration,
                    test_exit_code=0 if task_id == "accepted" else 1,
                )
            )
    events.append(dispatch("only-dispatched"))
    return events


def cli(*args, file=None, env=None):
    prefix = [sys.executable, "-B", str(SCRIPT)]
    if file is not None:
        prefix += ["--file", str(file)]
    return subprocess.run([*prefix, *args], capture_output=True, text=True, env=env, timeout=15)


class TieringTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.temporary_directory.name)
        self.ledger = self.tmp_path / "records" / "events.jsonl"

    def tearDown(self):
        self.temporary_directory.cleanup()

    def create_repository(self, name=None):
        if name is None:
            name = f"worktree_{uuid.uuid4().hex[:8]}"
        root = self.tmp_path / name
        root.mkdir(exist_ok=True)
        (root / "allowed.py").write_text("original\n")
        (root / "outside.txt").write_text("outside\n")
        (root / "binary.bin").write_bytes(b"\x00\xff\x01")
        (root / ".gitignore").write_text("ignored*\n")
        (root / "link").symlink_to("outside.txt")
        git(root, "init", "-q")
        git(root, "config", "core.filemode", "false")
        git(root, "add", ".")
        return root, git(root, "write-tree")

    def test_default_and_override_paths(self):
        old_env = os.environ.copy()
        try:
            os.environ["HOME"] = str(self.tmp_path)
            os.environ.pop("AGENT_TIERING_FILE", None)
            self.assertEqual(tiering.event_file(), self.tmp_path / ".local/state/agent-tiering/events.jsonl")
            os.environ["AGENT_TIERING_FILE"] = str(self.tmp_path / "environment.jsonl")
            self.assertEqual(tiering.event_file().name, "environment.jsonl")
            self.assertEqual(tiering.event_file(self.tmp_path / "explicit.jsonl").name, "explicit.jsonl")
        finally:
            os.environ.clear()
            os.environ.update(old_env)

    def test_permissions_and_unicode_round_trip(self):
        tiering.append_event(dispatch(), self.ledger)
        self.assertEqual(tiering.read_events(self.ledger), [dispatch()])
        for path, expected in ((self.ledger.parent, 0o700), (self.ledger, 0o600), (self.ledger.with_suffix(".lock"), 0o600)):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), expected)
            path.chmod(0o777)
        tiering.append_event(result(), self.ledger)
        self.assertEqual(stat.S_IMODE(self.ledger.parent.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.ledger.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.ledger.with_suffix(".lock").stat().st_mode), 0o600)
        self.assertTrue(self.ledger.read_bytes().endswith(b"\n"))

    def test_flush_and_fsync_happen_under_exclusive_lock(self):
        calls = []
        original_flock, original_fsync = fcntl.flock, os.fsync

        def flock(fd, operation):
            calls.append(operation)
            return original_flock(fd, operation)

        def fsync(fd):
            self.assertEqual(calls[-1], fcntl.LOCK_EX)
            self.assertEqual(json.loads(self.ledger.read_text()), dispatch())
            calls.append("fsync")
            original_fsync(fd)

        with mock.patch.object(tiering.fcntl, "flock", side_effect=flock), \
             mock.patch.object(tiering.os, "fsync", side_effect=fsync):
            tiering.append_event(dispatch(), self.ledger)
        self.assertEqual(calls, [fcntl.LOCK_EX, "fsync", fcntl.LOCK_UN])

    def test_real_fixed_lock_blocks_another_writer(self):
        self.ledger.parent.mkdir(parents=True, exist_ok=True)
        code = (
            "import json,runpy,sys; m=runpy.run_path(sys.argv[1]); "
            "print('ready',flush=True); m['append_event'](json.loads(sys.argv[3]),sys.argv[2])"
        )
        with self.ledger.with_suffix(".lock").open("wb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            process = subprocess.Popen(
                [sys.executable, "-B", "-c", code, str(SCRIPT), str(self.ledger), json.dumps(dispatch())],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                self.assertEqual(process.stdout.readline(), "ready\n")
                with self.assertRaises(subprocess.TimeoutExpired):
                    process.wait(timeout=0.15)
                self.assertFalse(self.ledger.exists())
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
                _, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0, stderr)
        self.assertEqual(tiering.read_events(self.ledger), [dispatch()])

    def test_concurrent_process_writes_preserve_all_complete_lines(self):
        code = (
            "import json,runpy,sys; m=runpy.run_path(sys.argv[1]); "
            "base=json.loads(sys.argv[3]); "
            "[m['append_event'](dict(base,task_id=sys.argv[4]+'-'+str(i),goal='并发记录'*2000),sys.argv[2]) "
            "for i in range(8)]"
        )
        processes = []
        try:
            for worker in range(4):
                processes.append(
                    subprocess.Popen(
                        [sys.executable, "-B", "-c", code, str(SCRIPT), str(self.ledger), json.dumps(dispatch()), str(worker)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                )
            for process in processes:
                _, stderr = process.communicate(timeout=20)
                self.assertEqual(process.returncode, 0, stderr)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        events = tiering.read_events(self.ledger)
        self.assertEqual(len(events), 32)
        self.assertEqual(len({event["task_id"] for event in events}), 32)
        self.assertTrue(all(event["goal"] == "并发记录" * 2000 for event in events))

    def test_corrupt_line_stops_read_report_and_append_without_repair(self):
        for bad in [b'{"event":', b"not json\n", b"\xff\n", b"\n", b"[]\n"]:
            with self.subTest(bad=bad):
                if self.ledger.exists():
                    self.ledger.unlink()
                self.ledger.parent.mkdir(parents=True, exist_ok=True)
                data = json.dumps(dispatch()).encode() + b"\n" + bad + json.dumps(dispatch("later")).encode() + b"\n"
                self.ledger.write_bytes(data)
                with self.assertRaisesRegex(tiering.TieringError, rf"{re.escape(str(self.ledger))}:2:"):
                    tiering.read_events(self.ledger)
                with self.assertRaisesRegex(tiering.TieringError, rf"{re.escape(str(self.ledger))}:2:"):
                    tiering.append_event(result(), self.ledger)
                out_stream, err_stream = io.StringIO(), io.StringIO()
                with mock.patch("sys.stdout", out_stream), mock.patch("sys.stderr", err_stream):
                    code = tiering.main(["report", "--file", str(self.ledger)])
                self.assertEqual(code, 2)
                self.assertIn(str(self.ledger) + ":2:", err_stream.getvalue())
                self.assertEqual(out_stream.getvalue(), "")
                self.assertEqual(self.ledger.read_bytes(), data)

    def test_valid_json_without_final_newline_is_corrupt(self):
        self.ledger.parent.mkdir(parents=True, exist_ok=True)
        self.ledger.write_text(json.dumps(dispatch()))
        with self.assertRaisesRegex(tiering.TieringError, r":1:.*换行"):
            tiering.read_events(self.ledger)

    def test_write_failure_reports_file_and_line_without_truncation(self):
        tiering.append_event(dispatch(), self.ledger)

        def fail(fd):
            raise OSError("disk failure")

        with mock.patch.object(tiering.os, "fsync", side_effect=fail):
            with self.assertRaisesRegex(tiering.TieringError, rf"{re.escape(str(self.ledger))}:2:.*写入失败"):
                tiering.append_event(result(), self.ledger)
        self.assertEqual(tiering.read_events(self.ledger), [dispatch(), result()])

    def test_invalid_dispatch_is_not_written(self):
        cases = [
            {"event": "other"},
            {"event": []},
            {"time": "2026-10-28T12:00:00"},
            {"status": "completed"},
            {"attempt_id": "made-up"},
            {"goal": ""},
            {"allowed_files": ["../outside"]},
            {"allowed_files": ["/absolute"]},
            {"allowed_files": ["a/../b"]},
            {"allowed_files": [".git/config"]},
            {"allowed_files": ["a//b"]},
            {"acceptance": "not a list"},
            {"secret": "unexpected field"},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                if self.ledger.exists():
                    self.ledger.unlink()
                with self.assertRaises(tiering.TieringError):
                    tiering.append_event(dispatch(**changes), self.ledger)
                self.assertFalse(self.ledger.exists())

    def test_allowed_file_change_passes_and_does_not_change_index(self):
        root, tree = self.create_repository()
        original_index = (root / ".git/index").read_bytes()
        (root / "allowed.py").write_text("fixed\n")
        checked = tiering.check(root, tree, ["allowed.py"], python_command("print('independent success')"))
        self.assertTrue(checked["clean"])
        self.assertTrue(checked["stable"])
        self.assertEqual(checked["changed_files"], ["allowed.py"])
        self.assertEqual(checked["out_of_scope_files"], [])
        self.assertEqual(checked["polluted_files"], [])
        self.assertEqual(checked["test_exit_code"], 0)
        self.assertIn("independent success", checked["test_output"])
        self.assertEqual((root / ".git/index").read_bytes(), original_index)

    def test_every_file_change_is_checked_even_if_git_would_hide_it(self):
        changes = ["modify", "new", "ignored", "delete", "binary", "symlink", "mode", "staged"]
        names = {
            "modify": "outside.txt",
            "new": "new.txt",
            "ignored": "ignored-secret.txt",
            "delete": "outside.txt",
            "binary": "binary.bin",
            "symlink": "link",
            "mode": "allowed.py",
            "staged": "outside.txt",
        }
        for change in changes:
            with self.subTest(change=change):
                root, tree = self.create_repository()
                name = names[change]
                path = root / name
                if change == "delete":
                    path.unlink()
                elif change == "symlink":
                    path.unlink()
                    path.symlink_to("allowed.py")
                elif change == "mode":
                    path.chmod(0o755)  # core.filemode=false must not hide this.
                else:
                    path.write_bytes(b"\x00changed\xff")
                if change == "staged":
                    git(root, "add", name)
                if change == "modify":
                    git(root, "update-index", "--assume-unchanged", name)
                checked = tiering.check(root, tree, [], python_command("pass"))
                self.assertEqual(checked["changed_files"], [name])
                self.assertEqual(checked["out_of_scope_files"], [name])
                self.assertTrue(checked["stable"])
                self.assertFalse(checked["clean"])

    def test_rename_checks_old_and_new_names(self):
        cases = [["outside.txt"], ["renamed.txt"], ["outside.txt", "renamed.txt"]]
        for allowed in cases:
            with self.subTest(allowed=allowed):
                root, tree = self.create_repository()
                (root / "outside.txt").rename(root / "renamed.txt")
                git(root, "add", "-A")
                checked = tiering.check(root, tree, allowed, python_command("pass"))
                self.assertEqual(checked["changed_files"], ["outside.txt", "renamed.txt"])
                self.assertEqual(checked["out_of_scope_files"], sorted({"outside.txt", "renamed.txt"} - set(allowed)))
                self.assertEqual(checked["clean"], (len(allowed) == 2))

    def test_names_with_spaces_tabs_newlines_and_unicode(self):
        root, tree = self.create_repository()
        names = ["a space", "a\ttab", "a\nnewline", "中文", "--option", " ", "\n"]
        for name in names:
            (root / name).write_text("new")
        checked = tiering.check(root, tree, names, python_command("pass"))
        self.assertEqual(checked["changed_files"], sorted(names))
        self.assertTrue(checked["clean"])

    def test_input_snapshot_includes_preexisting_dirty_added_and_deleted_files(self):
        root, original = self.create_repository()
        (root / "outside.txt").write_text("preexisting modification")
        (root / "ignored-carried.txt").write_text("preexisting ignored addition")
        (root / "binary.bin").unlink()
        git(root, "add", "-A", "-f")
        snapshot = git(root, "write-tree")
        self.assertNotEqual(snapshot, original)
        self.assertTrue(bool(git(root, "diff", "--name-only", original)))
        checked = tiering.check(root, snapshot, [], python_command("pass"))
        self.assertTrue(checked["clean"])
        self.assertEqual(checked["changed_files"], [])
        (root / "ignored-carried.txt").write_text("agent changed it")
        checked = tiering.check(root, snapshot, [], python_command("pass"))
        self.assertEqual(checked["out_of_scope_files"], ["ignored-carried.txt"])

    def test_symbolic_link_is_not_followed(self):
        root, _ = self.create_repository()
        external = self.tmp_path / "external"
        external.mkdir()
        (root / "link").unlink()
        (root / "link").symlink_to(external, target_is_directory=True)
        git(root, "add", "link")
        tree = git(root, "write-tree")
        (external / "outside-worktree").write_text("must not traverse")
        self.assertTrue(tiering.check(root, tree, [], python_command("pass"))["clean"])

    def test_command_exit_code_and_combined_output(self):
        for exit_code in [0, 3, 127]:
            with self.subTest(exit_code=exit_code):
                root, tree = self.create_repository()
                command = python_command(
                    f"import sys; print('stdout'); print('stderr',file=sys.stderr); sys.exit({exit_code})"
                )
                checked = tiering.check(root, tree, [], command)
                self.assertEqual(checked["test_exit_code"], exit_code)
                self.assertTrue("stdout" in checked["test_output"] and "stderr" in checked["test_output"])
                self.assertEqual(checked["clean"], (exit_code == 0))

    def test_missing_test_program_fails(self):
        root, tree = self.create_repository()
        checked = tiering.check(root, tree, [], "nonexistent-tiering-test-command-12345")
        self.assertEqual(checked["test_exit_code"], 127)
        self.assertFalse(checked["clean"])

    def test_test_process_launch_error_is_reported_without_acceptance(self):
        root, tree = self.create_repository()
        original = subprocess.Popen

        def popen(command, **kwargs):
            if isinstance(command, str):
                raise OSError("cannot launch shell")
            return original(command, **kwargs)

        with mock.patch.object(tiering.subprocess, "Popen", side_effect=popen):
            checked = tiering.check(root, tree, [], "true")
        self.assertIsNone(checked["test_exit_code"])
        self.assertEqual(checked["test_error"], "cannot launch shell")
        self.assertFalse(checked["clean"])

    def test_ledger_preserves_whitespace_only_file_names(self):
        event = dispatch(allowed_files=[" ", "\n"])
        tiering.append_event(event, self.ledger)
        self.assertEqual(tiering.read_events(self.ledger), [event])

    def test_command_output_is_bounded(self):
        root, tree = self.create_repository()
        checked = tiering.check(root, tree, [], python_command("print('x'*100000); print('THE END')"))
        self.assertLessEqual(len(checked["test_output"]), tiering.OUTPUT_LIMIT)
        self.assertTrue(checked["output_truncated"])
        self.assertTrue(checked["test_output"].endswith("THE END\n"))

    def test_test_pollution_invalidates_check_including_allowed_files(self):
        cases = [
            ("from pathlib import Path; Path('allowed.py').write_text('test pollution')", "allowed.py"),
            ("from pathlib import Path; Path('ignored-cache').write_text('cache')", "ignored-cache"),
            ("from pathlib import Path; Path('outside.txt').unlink()", "outside.txt"),
            ("import os; os.chmod('allowed.py',0o600)", "allowed.py"),
            ("from pathlib import Path; p=Path('allowed.py'); s=p.read_bytes(); p.write_bytes(s)", "allowed.py"),
        ]
        for code, polluted in cases:
            with self.subTest(polluted=polluted):
                root, tree = self.create_repository()
                (root / "allowed.py").chmod(0o644)
                checked = tiering.check(
                    root, tree, ["allowed.py", "ignored-cache", "outside.txt"], python_command(code)
                )
                self.assertEqual(checked["test_exit_code"], 0)
                self.assertEqual(checked["out_of_scope_files"], [])
                self.assertEqual(checked["polluted_files"], [polluted])
                self.assertFalse(checked["stable"])
                self.assertFalse(checked["clean"])

    def test_timeout_and_normal_exit_cleanup_entire_process_group(self):
        for timeout in [True, False]:
            with self.subTest(timeout=timeout):
                root, tree = self.create_repository()
                pid_file = self.tmp_path / f"child_{timeout}.pid"
                code = (
                    "import subprocess,sys,time; from pathlib import Path; "
                    "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
                    f"Path({str(pid_file)!r}).write_text(str(p.pid)); print('started',flush=True); "
                    + ("time.sleep(60)" if timeout else "pass")
                )
                checked = tiering.check(root, tree, [], python_command(code), timeout=0.5 if timeout else 5)
                pid = int(pid_file.read_text())
                try:
                    subprocess.run(
                        [sys.executable, "-c", "import time; time.sleep(.05)"], check=True, timeout=2
                    )
                    self.assertFalse(is_running(pid))
                    self.assertEqual(checked["timed_out"], timeout)
                    self.assertEqual(checked["test_exit_code"], (-signal.SIGKILL if timeout else 0))
                    self.assertNotEqual(checked["clean"], timeout)
                    self.assertIn("started", checked["test_output"])
                finally:
                    if is_running(pid):
                        os.kill(pid, signal.SIGKILL)

    def test_invalid_timeout(self):
        root, tree = self.create_repository()
        for timeout in [0, -1, float("inf"), float("nan")]:
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(tiering.TieringError, "timeout"):
                    tiering.check(root, tree, [], "true", timeout=timeout)

    def test_invalid_snapshot_or_subdirectory_does_not_run_command(self):
        root, _ = self.create_repository()
        marker = self.tmp_path / "must-not-run"
        command = python_command(f"from pathlib import Path; Path({str(marker)!r}).touch()")
        with self.assertRaisesRegex(tiering.TieringError, "Git"):
            tiering.check(root, "does-not-exist", [], command)
        subdirectory = root / "subdirectory"
        subdirectory.mkdir()
        with self.assertRaisesRegex(tiering.TieringError, "根目录"):
            tiering.check(subdirectory, "does-not-exist", [], command)
        self.assertFalse(marker.exists())

    def test_special_file_and_submodule_are_reported_as_unchecked(self):
        root, tree = self.create_repository()
        os.mkfifo(root / "fifo")
        with self.assertRaisesRegex(tiering.TieringError, "特殊文件"):
            tiering.check(root, tree, [], "true")
        (root / "fifo").unlink()
        git(root, "update-index", "--add", "--cacheinfo", "160000," + "a" * 40 + ",submodule")
        tree = git(root, "write-tree")
        with self.assertRaisesRegex(tiering.TieringError, "子模块"):
            tiering.check(root, tree, [], "true")

    def test_report_counts_rates_and_arithmetic_mean_include_all_determined_results(self):
        report = tiering.build_report(mixed_events())
        group, = report["groups"]
        expected = {
            "dispatched": 9,
            "completed": 3,
            "accepted": 1,
            "rejected": 1,
            "failed": 1,
            "cancelled": 1,
            "interrupted": 1,
            "interrupted_or_cancelled": 2,
            "pending_review": 1,
            "waiting": 1,
            "needs_check": 2,
            "determined": 5,
            "pass_rate": 0.2,
            "average_duration_seconds": 29,
        }
        self.assertEqual({key: group[key] for key in expected}, expected)
        self.assertEqual(len(report["warnings"]), 2)
        self.assertIn("样本较少", group["sample_note"])
        text = tiering.format_report(report)
        self.assertTrue("20.0%" in text and "29.00" in text)
        self.assertTrue("待审" in text and "待核对" in text and report["cutoff"] in text)

    def test_zero_denominator_pending_review_and_empty_ledger(self):
        group, = tiering.build_report([dispatch(), result()])["groups"]
        self.assertEqual(group["pending_review"], 1)
        self.assertEqual(group["determined"], 0)
        self.assertIsNone(group["pass_rate"])
        self.assertIsNone(group["average_duration_seconds"])
        self.assertEqual(group["rate_note"], "暂无已确定结果")
        self.assertIn("暂无已确定结果", tiering.format_report(tiering.build_report([dispatch()])))
        self.assertEqual(tiering.read_events(self.ledger), [])
        self.assertEqual(tiering.build_report([])["groups"], [])
        self.assertIn("暂无已确定结果", tiering.format_report(tiering.build_report([])))

    def test_ten_determined_results_remove_small_sample_note(self):
        events = [event for index in range(10) for event in (dispatch(str(index)), result(str(index), "failed"))]
        group, = tiering.build_report(events)["groups"]
        self.assertEqual(group["determined"], 10)
        self.assertEqual(group["pass_rate"], 0)
        self.assertEqual(group["sample_note"], "")

    def test_grouping_uses_actual_profile_model_and_effort(self):
        events = [
            dispatch("a"),
            dispatch("b", profile="another-profile"),
            dispatch("c", model="another-model"),
            dispatch("d", effort="low"),
        ]
        groups = tiering.build_report(events)["groups"]
        self.assertEqual(len(groups), 4)
        self.assertTrue(all(group["dispatched"] == 1 for group in groups))

    def test_duplicates_do_not_count_again_or_move_end_time(self):
        events = [dispatch(), result(), review()]
        duplicates = [dict(event, time=at(100)) for event in events]
        group, = tiering.build_report(events + events + duplicates)["groups"]
        self.assertEqual(group["dispatched"], 1)
        self.assertEqual(group["completed"], 1)
        self.assertEqual(group["accepted"], 1)
        self.assertEqual(group["average_duration_seconds"], 30)

    def test_conflicting_or_incomplete_records_stop_aggregation(self):
        cases = [
            [dispatch(), dispatch(goal="changed requirements")],
            [dispatch(), result(), result(status="failed")],
            [dispatch(), result(), result(attempt_id="another-attempt")],
            [dispatch(), result(), review(), review(decision="rejected", test_exit_code=1)],
            [dispatch(), result(), review(attempt_id="another-attempt")],
            [dispatch(), result(status="failed"), review()],
            [dispatch(), review()],
            [result()],
            [review()],
            [dispatch(), result(seconds=-1)],
            [dispatch(), result(status="running"), result(attempt_id="another-attempt")],
        ]
        for events in cases:
            with self.subTest(events=events):
                with self.assertRaisesRegex(tiering.TieringError, r"events:\d+:"):
                    tiering.build_report(events)

    def test_failed_missing_tests_or_unchecked_scope_cannot_be_accepted(self):
        cases = [
            {"test_exit_code": 1},
            {"test_exit_code": None},
            {"test_exit_code": True},
            {"out_of_scope_files": ["outside.txt"]},
            {"out_of_scope_files": None},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                with self.assertRaises(tiering.TieringError):
                    tiering.build_report([dispatch(), result(), review(**changes)])

    def test_manual_test_requires_explicit_dispatch_without_automated_command(self):
        group, = tiering.build_report([
            dispatch(test_command=None, acceptance=["人工阅读文档并核对引用"]),
            result(),
            review(test_exit_code=None, reason="约定无自动测试，已人工核对文档和引用"),
        ])["groups"]
        self.assertEqual(group["accepted"], 1)

    def test_cutoff_uses_known_results_only_and_understands_timezones(self):
        report = tiering.build_report([dispatch(), result(), review()], cutoff=at(20))
        self.assertEqual(report["cutoff"], at(20))
        self.assertEqual(report["groups"][0]["pending_review"], 1)
        self.assertIsNone(report["groups"][0]["pass_rate"])
        group, = tiering.build_report([
            dispatch(time="2026-10-28T20:00:00+08:00"),
            result(),
            review(),
        ])["groups"]
        self.assertEqual(group["average_duration_seconds"], 30)

    def test_cli_pass_test_failure_and_startup_failure(self):
        root, tree = self.create_repository()
        for task_id in ("pass", "test-failure", "startup-failure"):
            command = python_command("print('ok')" if task_id == "pass" else "raise SystemExit(4)")
            recorded = cli(
                "record-task",
                "--task-id",
                task_id,
                "--time",
                at(),
                "--profile",
                "fixed",
                "--model",
                "actual-model",
                "--effort",
                "high",
                "--goal",
                "修复输入",
                "--allowed-files",
                "allowed.py",
                "--test-command",
                command,
                "--acceptance",
                "输入正确",
                "只修改允许文件",
                "--snapshot-tree",
                tree,
                file=self.ledger,
            )
            self.assertEqual(recorded.returncode, 0, recorded.stderr)
            status = "failed" if task_id == "startup-failure" else "completed"
            args = ["record-task", "--task-id", task_id, "--time", at(10), "--status", status, "--reason", "执行结果"]
            if status == "completed":
                args += ["--attempt-id", "attempt-" + task_id]
            recorded = cli(*args, file=self.ledger)
            self.assertEqual(recorded.returncode, 0, recorded.stderr)
            if status == "failed":
                self.assertIsNone(json.loads(recorded.stdout)["attempt_id"])
                continue
            checked = cli(
                "check",
                "--worktree",
                str(root),
                "--snapshot-tree",
                tree,
                "--allowed-files",
                "allowed.py",
                "--test-command",
                command,
            )
            check_result = json.loads(checked.stdout)
            self.assertEqual(checked.returncode, (0 if task_id == "pass" else 1))
            self.assertEqual(check_result["test_exit_code"], (0 if task_id == "pass" else 4))
            recorded = cli(
                "record-review",
                "--task-id",
                task_id,
                "--attempt-id",
                "attempt-" + task_id,
                "--time",
                at(30),
                "--decision",
                "accepted" if task_id == "pass" else "rejected",
                "--reason",
                "已独立检查并阅读修改",
                "--test-exit-code",
                str(check_result["test_exit_code"]),
                "--out-of-scope-files",
                file=self.ledger,
            )
            self.assertEqual(recorded.returncode, 0, recorded.stderr)
        output = cli("report", "--format", "json", file=self.ledger)
        self.assertEqual(output.returncode, 0, output.stderr)
        group, = json.loads(output.stdout)["groups"]
        self.assertEqual(
            (group["dispatched"], group["completed"], group["accepted"], group["rejected"], group["failed"]),
            (3, 2, 1, 1, 1),
        )
        self.assertAlmostEqual(group["pass_rate"], 1 / 3, places=5)
        self.assertAlmostEqual(group["average_duration_seconds"], 70 / 3, places=5)
        events = tiering.read_events(self.ledger)
        self.assertEqual(len(events), 8)
        self.assertFalse(any(e["event"] == "review" and e["task_id"] == "startup-failure" for e in events))
        self.assertIn("33.3%", cli("report", file=self.ledger).stdout)

    def test_cli_env_file_override_and_file_after_subcommand(self):
        env = dict(os.environ, AGENT_TIERING_FILE=str(self.ledger))
        self.assertEqual(cli("report", "--format", "json", env=env).returncode, 0)
        explicit = self.ledger.parent / "explicit.jsonl"
        output = cli("report", "--file", str(explicit), "--format", "json", env=env)
        self.assertEqual(output.returncode, 0)
        self.assertEqual(json.loads(output.stdout)["groups"], [])

    def test_cli_check_errors_are_not_clean_results(self):
        root, _ = self.create_repository()
        output = cli("check", "--worktree", str(root), "--snapshot-tree", "missing", "--test-command", "true")
        self.assertEqual(output.returncode, 2)
        self.assertTrue("Git" in output.stderr and not output.stdout)


if __name__ == "__main__":
    unittest.main()
