"""Real files, Git trees and subprocesses; no bridge service or model calls."""

import fcntl
import importlib.util
import json
import os
import shlex
import signal
import stat
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts/tiering.py"
SPEC = importlib.util.spec_from_file_location("tiering", SCRIPT)
tiering = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tiering)
START = datetime(2026, 10, 28, 12, tzinfo=UTC)


def at(seconds=0):
    return (START + timedelta(seconds=seconds)).isoformat()


def dispatch(task_id="task-1", **changes):
    return {
        "event": "task", "time": at(), "task_id": task_id,
        "profile": "codex-sol", "model": "actual-model", "effort": "high",
        "goal": "修复空输入", "allowed_files": ["allowed.py"],
        "test_command": "python -B -m unittest", "acceptance": ["空输入返回空列表"],
        "snapshot_tree": "saved-tree", "status": "queued", "attempt_id": None, **changes,
    }


def result(task_id="task-1", status="completed", seconds=10, **changes):
    return {
        "event": "task", "time": at(seconds), "task_id": task_id,
        "status": status, "attempt_id": "attempt-" + task_id, "reason": "执行结束", **changes,
    }


def review(task_id="task-1", decision="accepted", seconds=30, **changes):
    return {
        "event": "review", "time": at(seconds), "task_id": task_id,
        "attempt_id": "attempt-" + task_id, "decision": decision,
        "reason": "独立检查完成并已阅读 diff", "test_exit_code": 0,
        "out_of_scope_files": [], **changes,
    }


def python_command(code):
    return shlex.join([sys.executable, "-B", "-c", code])


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.PIPE).decode().strip()


@pytest.fixture
def ledger(tmp_path):
    return tmp_path / "records" / "events.jsonl"


@pytest.fixture
def repository(tmp_path):
    root = tmp_path / "worktree"
    root.mkdir()
    (root / "allowed.py").write_text("original\n")
    (root / "outside.txt").write_text("outside\n")
    (root / "binary.bin").write_bytes(b"\x00\xff\x01")
    (root / ".gitignore").write_text("ignored*\n")
    (root / "link").symlink_to("outside.txt")
    git(root, "init", "-q")
    git(root, "config", "core.filemode", "false")
    git(root, "add", ".")
    # Git trees suffice: no commit, branch, service or model is needed.
    return root, git(root, "write-tree")


def test_default_and_override_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("AGENT_TIERING_FILE", raising=False)
    assert tiering.event_file() == tmp_path / ".local/state/agent-tiering/events.jsonl"
    monkeypatch.setenv("AGENT_TIERING_FILE", str(tmp_path / "environment.jsonl"))
    assert tiering.event_file().name == "environment.jsonl"
    assert tiering.event_file(tmp_path / "explicit.jsonl").name == "explicit.jsonl"


def test_permissions_and_unicode_round_trip(ledger):
    tiering.append_event(dispatch(), ledger)
    assert tiering.read_events(ledger) == [dispatch()]
    for path, expected in ((ledger.parent, 0o700), (ledger, 0o600), (ledger.with_suffix(".lock"), 0o600)):
        assert stat.S_IMODE(path.stat().st_mode) == expected
        path.chmod(0o777)
    tiering.append_event(result(), ledger)
    assert stat.S_IMODE(ledger.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(ledger.stat().st_mode) == 0o600
    assert stat.S_IMODE(ledger.with_suffix(".lock").stat().st_mode) == 0o600
    assert ledger.read_bytes().endswith(b"\n")


def test_flush_and_fsync_happen_under_exclusive_lock(monkeypatch, ledger):
    calls = []
    original_flock, original_fsync = fcntl.flock, os.fsync

    def flock(fd, operation):
        calls.append(operation)
        return original_flock(fd, operation)

    def fsync(fd):
        assert calls[-1] == fcntl.LOCK_EX
        # A second descriptor sees the complete line only after the explicit flush.
        assert json.loads(ledger.read_text()) == dispatch()
        calls.append("fsync")
        original_fsync(fd)

    monkeypatch.setattr(tiering.fcntl, "flock", flock)
    monkeypatch.setattr(tiering.os, "fsync", fsync)
    tiering.append_event(dispatch(), ledger)
    assert calls == [fcntl.LOCK_EX, "fsync", fcntl.LOCK_UN]


def test_real_fixed_lock_blocks_another_writer(ledger):
    ledger.parent.mkdir()
    code = (
        "import json,runpy,sys; m=runpy.run_path(sys.argv[1]); "
        "print('ready',flush=True); m['append_event'](json.loads(sys.argv[3]),sys.argv[2])"
    )
    with ledger.with_suffix(".lock").open("wb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        process = subprocess.Popen(
            [sys.executable, "-B", "-c", code, str(SCRIPT), str(ledger), json.dumps(dispatch())],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            assert process.stdout.readline() == "ready\n"
            with pytest.raises(subprocess.TimeoutExpired):
                process.wait(timeout=0.15)
            assert not ledger.exists()
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            _, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, stderr
    assert tiering.read_events(ledger) == [dispatch()]


def test_concurrent_process_writes_preserve_all_complete_lines(ledger):
    code = (
        "import json,runpy,sys; m=runpy.run_path(sys.argv[1]); "
        "base=json.loads(sys.argv[3]); "
        "[m['append_event'](dict(base,task_id=sys.argv[4]+'-'+str(i),goal='并发记录'*2000),sys.argv[2]) "
        "for i in range(8)]"
    )
    processes = []
    try:
        for worker in range(4):
            processes.append(subprocess.Popen(
                [sys.executable, "-B", "-c", code, str(SCRIPT), str(ledger), json.dumps(dispatch()), str(worker)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            ))
        for process in processes:
            _, stderr = process.communicate(timeout=20)
            assert process.returncode == 0, stderr
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
    events = tiering.read_events(ledger)
    assert len(events) == 32
    assert len({event["task_id"] for event in events}) == 32
    assert all(event["goal"] == "并发记录" * 2000 for event in events)


@pytest.mark.parametrize("bad", [b'{"event":', b"not json\n", b"\xff\n", b"\n", b"[]\n"])
def test_corrupt_line_stops_read_report_and_append_without_repair(ledger, bad, capsys):
    ledger.parent.mkdir()
    data = json.dumps(dispatch()).encode() + b"\n" + bad + json.dumps(dispatch("later")).encode() + b"\n"
    ledger.write_bytes(data)
    with pytest.raises(tiering.TieringError, match=rf"{ledger}:2:"):
        tiering.read_events(ledger)
    with pytest.raises(tiering.TieringError, match=rf"{ledger}:2:"):
        tiering.append_event(result(), ledger)
    assert tiering.main(["report", "--file", str(ledger)]) == 2
    captured = capsys.readouterr()
    assert str(ledger) + ":2:" in captured.err
    assert not captured.out
    assert ledger.read_bytes() == data


def test_valid_json_without_final_newline_is_corrupt(ledger):
    ledger.parent.mkdir()
    ledger.write_text(json.dumps(dispatch()))
    with pytest.raises(tiering.TieringError, match=r":1:.*换行"):
        tiering.read_events(ledger)


def test_write_failure_reports_file_and_line_without_truncation(monkeypatch, ledger):
    tiering.append_event(dispatch(), ledger)

    def fail(fd):
        raise OSError("disk failure")

    monkeypatch.setattr(tiering.os, "fsync", fail)
    with pytest.raises(tiering.TieringError, match=rf"{ledger}:2:.*写入失败"):
        tiering.append_event(result(), ledger)
    assert tiering.read_events(ledger) == [dispatch(), result()]


@pytest.mark.parametrize("changes", [
    {"event": "other"}, {"event": []}, {"time": "2026-10-28T12:00:00"}, {"status": "completed"},
    {"attempt_id": "made-up"}, {"goal": ""}, {"allowed_files": ["../outside"]},
    {"allowed_files": ["/absolute"]}, {"allowed_files": ["a/../b"]},
    {"allowed_files": [".git/config"]}, {"allowed_files": ["a//b"]},
    {"acceptance": "not a list"}, {"secret": "unexpected field"},
])
def test_invalid_dispatch_is_not_written(ledger, changes):
    with pytest.raises(tiering.TieringError):
        tiering.append_event(dispatch(**changes), ledger)
    assert not ledger.exists()


def test_allowed_file_change_passes_and_does_not_change_index(repository):
    root, tree = repository
    original_index = (root / ".git/index").read_bytes()
    (root / "allowed.py").write_text("fixed\n")
    checked = tiering.check(root, tree, ["allowed.py"], python_command("print('independent success')"))
    assert checked["clean"]
    assert checked["stable"]
    assert checked["changed_files"] == ["allowed.py"]
    assert checked["out_of_scope_files"] == checked["polluted_files"] == []
    assert checked["test_exit_code"] == 0
    assert "independent success" in checked["test_output"]
    assert (root / ".git/index").read_bytes() == original_index


@pytest.mark.parametrize("change", ["modify", "new", "ignored", "delete", "binary", "symlink", "mode", "staged"])
def test_every_file_change_is_checked_even_if_git_would_hide_it(repository, change):
    root, tree = repository
    names = {
        "modify": "outside.txt", "new": "new.txt", "ignored": "ignored-secret.txt", "delete": "outside.txt",
        "binary": "binary.bin", "symlink": "link", "mode": "allowed.py", "staged": "outside.txt",
    }
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
    assert checked["changed_files"] == checked["out_of_scope_files"] == [name]
    assert checked["stable"]
    assert not checked["clean"]


@pytest.mark.parametrize("allowed", [["outside.txt"], ["renamed.txt"], ["outside.txt", "renamed.txt"]])
def test_rename_checks_old_and_new_names(repository, allowed):
    root, tree = repository
    (root / "outside.txt").rename(root / "renamed.txt")
    git(root, "add", "-A")
    checked = tiering.check(root, tree, allowed, python_command("pass"))
    assert checked["changed_files"] == ["outside.txt", "renamed.txt"]
    assert checked["out_of_scope_files"] == sorted({"outside.txt", "renamed.txt"} - set(allowed))
    assert checked["clean"] == (len(allowed) == 2)


def test_names_with_spaces_tabs_newlines_and_unicode(repository):
    root, tree = repository
    names = ["a space", "a\ttab", "a\nnewline", "中文", "--option", " ", "\n"]
    for name in names:
        (root / name).write_text("new")
    checked = tiering.check(root, tree, names, python_command("pass"))
    assert checked["changed_files"] == sorted(names)
    assert checked["clean"]


def test_input_snapshot_includes_preexisting_dirty_added_and_deleted_files(repository):
    root, original = repository
    (root / "outside.txt").write_text("preexisting modification")
    (root / "ignored-carried.txt").write_text("preexisting ignored addition")
    (root / "binary.bin").unlink()
    git(root, "add", "-A", "-f")
    snapshot = git(root, "write-tree")
    assert snapshot != original
    assert git(root, "diff", "--name-only", original)
    checked = tiering.check(root, snapshot, [], python_command("pass"))
    assert checked["clean"]
    assert checked["changed_files"] == []
    (root / "ignored-carried.txt").write_text("agent changed it")
    checked = tiering.check(root, snapshot, [], python_command("pass"))
    assert checked["out_of_scope_files"] == ["ignored-carried.txt"]


def test_symbolic_link_is_not_followed(repository, tmp_path):
    root, _ = repository
    external = tmp_path / "external"
    external.mkdir()
    (root / "link").unlink()
    (root / "link").symlink_to(external, target_is_directory=True)
    git(root, "add", "link")
    tree = git(root, "write-tree")
    (external / "outside-worktree").write_text("must not traverse")
    assert tiering.check(root, tree, [], python_command("pass"))["clean"]


@pytest.mark.parametrize("exit_code", [0, 3, 127])
def test_command_exit_code_and_combined_output(repository, exit_code):
    root, tree = repository
    command = python_command(f"import sys; print('stdout'); print('stderr',file=sys.stderr); sys.exit({exit_code})")
    checked = tiering.check(root, tree, [], command)
    assert checked["test_exit_code"] == exit_code
    assert "stdout" in checked["test_output"] and "stderr" in checked["test_output"]
    assert checked["clean"] == (exit_code == 0)


def test_missing_test_program_fails(repository):
    root, tree = repository
    checked = tiering.check(root, tree, [], "nonexistent-tiering-test-command-12345")
    assert checked["test_exit_code"] == 127
    assert not checked["clean"]


def test_test_process_launch_error_is_reported_without_acceptance(repository, monkeypatch):
    root, tree = repository
    original = subprocess.Popen

    def popen(command, **kwargs):
        if isinstance(command, str):
            raise OSError("cannot launch shell")
        return original(command, **kwargs)

    monkeypatch.setattr(tiering.subprocess, "Popen", popen)
    checked = tiering.check(root, tree, [], "true")
    assert checked["test_exit_code"] is None
    assert checked["test_error"] == "cannot launch shell"
    assert not checked["clean"]


def test_ledger_preserves_whitespace_only_file_names(ledger):
    event = dispatch(allowed_files=[" ", "\n"])
    tiering.append_event(event, ledger)
    assert tiering.read_events(ledger) == [event]


def test_command_output_is_bounded(repository):
    root, tree = repository
    checked = tiering.check(root, tree, [], python_command("print('x'*100000); print('THE END')"))
    assert len(checked["test_output"]) <= tiering.OUTPUT_LIMIT
    assert checked["output_truncated"]
    assert checked["test_output"].endswith("THE END\n")


@pytest.mark.parametrize("code, polluted", [
    ("from pathlib import Path; Path('allowed.py').write_text('test pollution')", "allowed.py"),
    ("from pathlib import Path; Path('ignored-cache').write_text('cache')", "ignored-cache"),
    ("from pathlib import Path; Path('outside.txt').unlink()", "outside.txt"),
    ("import os; os.chmod('allowed.py',0o600)", "allowed.py"),
    ("from pathlib import Path; p=Path('allowed.py'); s=p.read_bytes(); p.write_bytes(s)", "allowed.py"),
])
def test_test_pollution_invalidates_check_including_allowed_files(repository, code, polluted):
    root, tree = repository
    (root / "allowed.py").chmod(0o644)
    checked = tiering.check(root, tree, ["allowed.py", "ignored-cache", "outside.txt"], python_command(code))
    assert checked["test_exit_code"] == 0
    assert checked["out_of_scope_files"] == []
    assert checked["polluted_files"] == [polluted]
    assert not checked["stable"] and not checked["clean"]


def is_running(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()[0] not in {"Z", "X"}
    except FileNotFoundError:
        return False


@pytest.mark.parametrize("timeout", [True, False])
def test_timeout_and_normal_exit_cleanup_entire_process_group(repository, tmp_path, timeout):
    root, tree = repository
    pid_file = tmp_path / "child.pid"
    code = (
        "import subprocess,sys,time; from pathlib import Path; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        f"Path({str(pid_file)!r}).write_text(str(p.pid)); print('started',flush=True); "
        + ("time.sleep(60)" if timeout else "pass")
    )
    checked = tiering.check(root, tree, [], python_command(code), timeout=0.5 if timeout else 5)
    pid = int(pid_file.read_text())
    try:
        # The child may briefly be a zombie; it must no longer execute.
        subprocess.run(
            [sys.executable, "-c", "import time; time.sleep(.05)"], check=True, timeout=2
        )
        assert not is_running(pid)
        assert checked["timed_out"] == timeout
        assert checked["test_exit_code"] == (-signal.SIGKILL if timeout else 0)
        assert checked["clean"] != timeout
        assert "started" in checked["test_output"]
    finally:
        if is_running(pid):
            os.kill(pid, signal.SIGKILL)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout(repository, timeout):
    root, tree = repository
    with pytest.raises(tiering.TieringError, match="timeout"):
        tiering.check(root, tree, [], "true", timeout=timeout)


def test_invalid_snapshot_or_subdirectory_does_not_run_command(repository, tmp_path):
    root, _ = repository
    marker = tmp_path / "must-not-run"
    command = python_command(f"from pathlib import Path; Path({str(marker)!r}).touch()")
    with pytest.raises(tiering.TieringError, match="Git"):
        tiering.check(root, "does-not-exist", [], command)
    subdirectory = root / "subdirectory"
    subdirectory.mkdir()
    with pytest.raises(tiering.TieringError, match="根目录"):
        tiering.check(subdirectory, "does-not-exist", [], command)
    assert not marker.exists()


def test_special_file_and_submodule_are_reported_as_unchecked(repository):
    root, tree = repository
    os.mkfifo(root / "fifo")
    with pytest.raises(tiering.TieringError, match="特殊文件"):
        tiering.check(root, tree, [], "true")
    (root / "fifo").unlink()
    # A gitlink requires a commit-shaped OID, but no commit needs to be made.
    git(root, "update-index", "--add", "--cacheinfo", "160000," + "a" * 40 + ",submodule")
    tree = git(root, "write-tree")
    with pytest.raises(tiering.TieringError, match="子模块"):
        tiering.check(root, tree, [], "true")


def mixed_events():
    events = []
    for task_id, status, duration in (
        ("accepted", "completed", 30), ("rejected", "completed", 50), ("failed", "failed", 5),
        ("cancelled", "cancelled", 20), ("interrupted", "interrupted", 40), ("pending", "completed", 10),
        ("uncertain", "recovery_needed", 8), ("running", "running", 7),
    ):
        events.append(dispatch(task_id))
        events.append(result(task_id, status, seconds=duration if status != "completed" else 10))
        if task_id in {"accepted", "rejected"}:
            events.append(review(task_id, decision=task_id, seconds=duration, test_exit_code=0 if task_id == "accepted" else 1))
    events.append(dispatch("only-dispatched"))
    return events


def test_report_counts_rates_and_arithmetic_mean_include_all_determined_results():
    report = tiering.build_report(mixed_events())
    group, = report["groups"]
    expected = {
        "dispatched": 9, "completed": 3, "accepted": 1, "rejected": 1, "failed": 1,
        "cancelled": 1, "interrupted": 1, "interrupted_or_cancelled": 2,
        "pending_review": 1, "waiting": 1, "needs_check": 2, "determined": 5,
        "pass_rate": 0.2, "average_duration_seconds": 29,
    }
    assert {key: group[key] for key in expected} == expected
    assert len(report["warnings"]) == 2
    assert "样本较少" in group["sample_note"]
    text = tiering.format_report(report)
    assert "20.0%" in text and "29.00" in text
    assert "待审" in text and "待核对" in text and report["cutoff"] in text


def test_zero_denominator_pending_review_and_empty_ledger(ledger):
    group, = tiering.build_report([dispatch(), result()])["groups"]
    assert group["pending_review"] == 1
    assert group["determined"] == 0
    assert group["pass_rate"] is group["average_duration_seconds"] is None
    assert group["rate_note"] == "暂无已确定结果"
    assert "暂无已确定结果" in tiering.format_report(tiering.build_report([dispatch()]))
    assert tiering.read_events(ledger) == []
    assert tiering.build_report([])["groups"] == []
    assert "暂无已确定结果" in tiering.format_report(tiering.build_report([]))


def test_ten_determined_results_remove_small_sample_note():
    events = [event for index in range(10) for event in (dispatch(str(index)), result(str(index), "failed"))]
    group, = tiering.build_report(events)["groups"]
    assert group["determined"] == 10
    assert group["pass_rate"] == 0
    assert group["sample_note"] == ""


def test_grouping_uses_actual_profile_model_and_effort():
    events = [dispatch("a"), dispatch("b", profile="another-profile"),
              dispatch("c", model="another-model"), dispatch("d", effort="low")]
    groups = tiering.build_report(events)["groups"]
    assert len(groups) == 4
    assert all(group["dispatched"] == 1 for group in groups)


def test_duplicates_do_not_count_again_or_move_end_time():
    events = [dispatch(), result(), review()]
    duplicates = [dict(event, time=at(100)) for event in events]
    group, = tiering.build_report(events + events + duplicates)["groups"]
    assert group["dispatched"] == group["completed"] == group["accepted"] == 1
    assert group["average_duration_seconds"] == 30


@pytest.mark.parametrize("events", [
    [dispatch(), dispatch(goal="changed requirements")],
    [dispatch(), result(), result(status="failed")],
    [dispatch(), result(), result(attempt_id="another-attempt")],
    [dispatch(), result(), review(), review(decision="rejected", test_exit_code=1)],
    [dispatch(), result(), review(attempt_id="another-attempt")],
    [dispatch(), result(status="failed"), review()],
    [dispatch(), review()], [result()], [review()],
    [dispatch(), result(seconds=-1)],
    [dispatch(), result(status="running"), result(attempt_id="another-attempt")],
])
def test_conflicting_or_incomplete_records_stop_aggregation(events):
    with pytest.raises(tiering.TieringError, match=r"events:\d+:"):
        tiering.build_report(events)


@pytest.mark.parametrize("changes", [
    {"test_exit_code": 1}, {"test_exit_code": None}, {"test_exit_code": True},
    {"out_of_scope_files": ["outside.txt"]}, {"out_of_scope_files": None},
])
def test_failed_missing_tests_or_unchecked_scope_cannot_be_accepted(changes):
    with pytest.raises(tiering.TieringError):
        tiering.build_report([dispatch(), result(), review(**changes)])


def test_manual_test_requires_explicit_dispatch_without_automated_command():
    group, = tiering.build_report([
        dispatch(test_command=None, acceptance=["人工阅读文档并核对引用"]), result(),
        review(test_exit_code=None, reason="约定无自动测试，已人工核对文档和引用"),
    ])["groups"]
    assert group["accepted"] == 1


def test_cutoff_uses_known_results_only_and_understands_timezones():
    report = tiering.build_report([dispatch(), result(), review()], cutoff=at(20))
    assert report["cutoff"] == at(20)
    assert report["groups"][0]["pending_review"] == 1
    assert report["groups"][0]["pass_rate"] is None
    group, = tiering.build_report([
        dispatch(time="2026-10-28T20:00:00+08:00"), result(), review(),
    ])["groups"]
    assert group["average_duration_seconds"] == 30


def cli(*args, file=None, env=None):
    prefix = [sys.executable, "-B", str(SCRIPT)]
    if file is not None:
        prefix += ["--file", str(file)]
    return subprocess.run([*prefix, *args], capture_output=True, text=True, env=env, timeout=15)


def test_cli_pass_test_failure_and_startup_failure(repository, ledger):
    root, tree = repository
    for task_id in ("pass", "test-failure", "startup-failure"):
        command = python_command("print('ok')" if task_id == "pass" else "raise SystemExit(4)")
        recorded = cli(
            "record-task", "--task-id", task_id, "--time", at(), "--profile", "fixed",
            "--model", "actual-model", "--effort", "high", "--goal", "修复输入",
            "--allowed-files", "allowed.py", "--test-command", command,
            "--acceptance", "输入正确", "只修改允许文件", "--snapshot-tree", tree, file=ledger,
        )
        assert recorded.returncode == 0, recorded.stderr
        status = "failed" if task_id == "startup-failure" else "completed"
        args = ["record-task", "--task-id", task_id, "--time", at(10), "--status", status, "--reason", "执行结果"]
        if status == "completed":
            args += ["--attempt-id", "attempt-" + task_id]
        recorded = cli(*args, file=ledger)
        assert recorded.returncode == 0, recorded.stderr
        if status == "failed":
            assert json.loads(recorded.stdout)["attempt_id"] is None
            continue
        checked = cli("check", "--worktree", str(root), "--snapshot-tree", tree,
                      "--allowed-files", "allowed.py", "--test-command", command)
        check_result = json.loads(checked.stdout)
        assert checked.returncode == (0 if task_id == "pass" else 1)
        assert check_result["test_exit_code"] == (0 if task_id == "pass" else 4)
        recorded = cli(
            "record-review", "--task-id", task_id, "--attempt-id", "attempt-" + task_id,
            "--time", at(30), "--decision", "accepted" if task_id == "pass" else "rejected",
            "--reason", "已独立检查并阅读修改", "--test-exit-code", str(check_result["test_exit_code"]),
            "--out-of-scope-files", file=ledger,
        )
        assert recorded.returncode == 0, recorded.stderr
    output = cli("report", "--format", "json", file=ledger)
    assert output.returncode == 0, output.stderr
    group, = json.loads(output.stdout)["groups"]
    assert (group["dispatched"], group["completed"], group["accepted"], group["rejected"], group["failed"]) == (3, 2, 1, 1, 1)
    assert group["pass_rate"] == pytest.approx(1 / 3)
    assert group["average_duration_seconds"] == pytest.approx(70 / 3)
    events = tiering.read_events(ledger)
    assert len(events) == 8
    assert not any(e["event"] == "review" and e["task_id"] == "startup-failure" for e in events)
    assert "33.3%" in cli("report", file=ledger).stdout


def test_cli_env_file_override_and_file_after_subcommand(ledger):
    env = dict(os.environ, AGENT_TIERING_FILE=str(ledger))
    assert cli("report", "--format", "json", env=env).returncode == 0
    explicit = ledger.parent / "explicit.jsonl"
    output = cli("report", "--file", str(explicit), "--format", "json", env=env)
    assert output.returncode == 0
    assert json.loads(output.stdout)["groups"] == []


def test_cli_check_errors_are_not_clean_results(repository):
    root, _ = repository
    output = cli("check", "--worktree", str(root), "--snapshot-tree", "missing", "--test-command", "true")
    assert output.returncode == 2
    assert "Git" in output.stderr and not output.stdout
