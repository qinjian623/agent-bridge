"""Local task/review ledger and independent checks (docs/agent-tiering.md, v1).

This script never dispatches agents or submits bridge reviews. ``clean`` means
the command passed, changes stayed in scope, and files stayed stable during the
check; the main agent must still read the diff and decide whether to accept.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import signal
import stat
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from statistics import mean

DISPATCH_FIELDS = {
    "profile", "model", "effort", "goal", "allowed_files", "test_command", "acceptance", "snapshot_tree"
}
SETTLED = {"completed", "failed", "interrupted", "cancelled"}
WAITING = {"queued", "assigned", "running", "waiting_input"}
OUTPUT_LIMIT = 4096


class TieringError(ValueError):
    """An incomplete check or a ledger that must be inspected by a human."""


def event_file(file=None):
    return Path(file or os.environ.get("AGENT_TIERING_FILE") or "~/.local/state/agent-tiering/events.jsonl").expanduser()


def timestamp(value):
    try:
        result = datetime.fromisoformat(value)
        if result.utcoffset() is None:
            raise ValueError("time requires a timezone")
        return result
    except (TypeError, ValueError) as exc:
        raise TieringError(f"无效的带时区时间: {value!r}") from exc


def relative_file(name):
    if not isinstance(name, str) or not name or "\0" in name:
        raise TieringError("文件名必须是 worktree 内的相对文件名")
    path = PurePosixPath(name)
    if path.is_absolute() or path.as_posix() != name or name == "." or any(p in {"..", ".git"} for p in path.parts):
        raise TieringError(f"无效的相对文件名: {name!r}")
    return name


def validate_event(event):
    if not isinstance(event, dict):
        raise TieringError("每行必须是 JSON 对象")
    common = {"event", "time", "task_id"}
    if (not common <= event.keys() or not isinstance(event["event"], str)
            or event["event"] not in {"task", "review"}):
        raise TieringError("记录必须包含 event、time、task_id，event 只能是 task 或 review")
    timestamp(event["time"])

    def text_field(key):
        if not isinstance(event.get(key), str) or not event[key].strip():
            raise TieringError(f"{key} 必须是非空字符串")

    def strings(key, paths=False, nullable=False):
        value = event[key]
        if nullable and value is None:
            return
        if not isinstance(value, list) or any(not isinstance(s, str) or not (s if paths else s.strip()) for s in value):
            raise TieringError(f"{key} 必须是字符串列表")
        if paths:
            for name in value:
                relative_file(name)

    text_field("task_id")
    dispatch = event["event"] == "task" and bool(DISPATCH_FIELDS & event.keys())
    fields = common | {"attempt_id"}
    if event["event"] == "task":
        fields |= {"status"} | (DISPATCH_FIELDS if dispatch else {"reason"})
    else:
        fields |= {"decision", "reason", "test_exit_code", "out_of_scope_files"}
    if fields != event.keys():
        raise TieringError(f"字段缺失或多余: 缺少 {sorted(fields - event.keys())}，多余 {sorted(event.keys() - fields)}")
    if event["attempt_id"] is not None:
        text_field("attempt_id")
    if event["event"] == "task":
        text_field("status")
        if dispatch:
            if event["status"] != "queued" or event["attempt_id"] is not None:
                raise TieringError("派发行必须是 queued，attempt_id 必须为 null")
            for key in DISPATCH_FIELDS - {"allowed_files", "acceptance", "test_command"}:
                text_field(key)
            strings("allowed_files", paths=True)
            strings("acceptance")
            if event["test_command"] is not None:
                text_field("test_command")
        else:
            text_field("reason")
            if event["status"] == "completed" and event["attempt_id"] is None:
                raise TieringError("completed 必须带有真实 attempt_id")
    else:
        text_field("attempt_id")
        text_field("reason")
        if not isinstance(event["decision"], str) or event["decision"] not in {"accepted", "rejected"}:
            raise TieringError("decision 只能是 accepted 或 rejected")
        code = event["test_exit_code"]
        if code is not None and type(code) is not int:
            raise TieringError("test_exit_code 必须是整数或 null")
        strings("out_of_scope_files", paths=True, nullable=True)
        if event["decision"] == "accepted" and (code not in {0, None} or event["out_of_scope_files"] != []):
            raise TieringError("测试失败或文件范围未检查通过，不能记为 accepted")


def aggregate(events, source="events"):
    """Reduce by task, reject contradictions, and ignore duplicate payloads."""
    tasks, seen = {}, set()
    for line, event in enumerate(events, 1):
        try:
            validate_event(event)
            signature = json.dumps({k: v for k, v in event.items() if k != "time"}, sort_keys=True, ensure_ascii=True)
            if signature in seen:
                continue
            seen.add(signature)
            task_id = event["task_id"]
            if "profile" in event:
                if task_id in tasks:
                    raise TieringError(f"任务 {task_id} 的派发要求矛盾")
                tasks[task_id] = {"dispatch": event, "last": event, "review": None}
                continue
            if task_id not in tasks:
                raise TieringError(f"任务 {task_id} 缺少派发行，无法汇总")
            task = tasks[task_id]
            previous = task["review"] or task["last"]
            if timestamp(event["time"]) < timestamp(previous["time"]):
                raise TieringError(f"任务 {task_id} 的时间倒退")
            if event["event"] == "review":
                if task["review"] is not None:
                    raise TieringError(f"任务 {task_id} 的审查结论矛盾")
                if task["last"]["status"] != "completed" or event["attempt_id"] != task["last"]["attempt_id"]:
                    raise TieringError(f"任务 {task_id} 的 review 没有对应的 completed 尝试")
                if (event["decision"] == "accepted" and event["test_exit_code"] is None
                        and task["dispatch"]["test_command"] is not None):
                    raise TieringError(f"任务 {task_id} 的约定测试未运行，不能记为 accepted")
                task["review"] = event
            else:
                if task["review"] or task["last"]["status"] in SETTLED:
                    raise TieringError(f"任务 {task_id} 的执行结果矛盾；第一版不支持重试")
                attempt = task["last"]["attempt_id"]
                if attempt is not None and event["attempt_id"] != attempt:
                    raise TieringError(f"任务 {task_id} 的 attempt_id 矛盾")
                task["last"] = event
        except TieringError as exc:
            raise TieringError(f"{source}:{line}: {exc}") from exc
    return tasks


@contextmanager
def ledger_lock(path, exclusive):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor = os.open(path.with_suffix(".lock"), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "r+b") as lock:
        os.fchmod(lock.fileno(), 0o600)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def read_unlocked(path):
    events = []
    if not path.exists():
        return events
    with path.open("rb") as stream:
        for line, raw in enumerate(stream, 1):
            try:
                if not raw.endswith(b"\n"):
                    raise ValueError("行末缺少换行，记录可能未写完")
                event = json.loads(raw.decode("utf-8"))
                validate_event(event)
            except (ValueError, TypeError) as exc:
                raise TieringError(f"{path}:{line}: 损坏或无效的记录: {exc}") from exc
            events.append(event)
    return events


def read_events(file=None):
    path = event_file(file)
    if not path.parent.exists():
        return []
    with ledger_lock(path, exclusive=False):
        return read_unlocked(path)


def append_event(event, file=None):
    path = event_file(file)
    with ledger_lock(path, exclusive=True):
        events = read_unlocked(path)
        line = len(events) + 1
        aggregate([*events, event], source=str(path))
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
                os.fchmod(stream.fileno(), 0o600)
                payload = json.dumps(event, ensure_ascii=True, allow_nan=False) + "\n"
                if stream.write(payload) != len(payload):
                    raise OSError("未完整写入一行")
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as exc:
            raise TieringError(f"{path}:{line}: 写入失败，停止后续派发并人工核对: {exc}") from exc
    return event


def git(root, *args):
    result = subprocess.run(
        ["git", "--no-optional-locks", "-C", str(root), *args], capture_output=True, timeout=60
    )
    if result.returncode:
        raise TieringError(f"Git 检查未完成: {result.stderr.decode(errors='replace')[-2000:]}")
    return result.stdout


def snapshot_files(root, tree):
    result = {}
    for entry in git(root, "ls-tree", "-r", "-z", "--full-tree", tree).split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.split()
        name = os.fsdecode(name)
        if kind != b"blob" or mode not in {b"100644", b"100755", b"120000"}:
            raise TieringError(f"无法完整检查 {name}：不支持子模块或特殊 Git 对象")
        result[name] = (mode.decode(), oid.decode())
    return result


def file_identity(value):
    return (value.st_mode, value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def working_files(root, algorithm):
    """Read every file, including ignored files; never traverse symlinks/.git."""
    result = {}

    def visit(directory):
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                if path == root / ".git":
                    continue
                before = path.lstat()
                if stat.S_ISDIR(before.st_mode):
                    visit(path)
                    continue
                digest = hashlib.new(algorithm)
                if stat.S_ISLNK(before.st_mode):
                    data = os.fsencode(os.readlink(path))
                    digest.update(f"blob {len(data)}\0".encode() + data)
                    mode = "120000"
                elif stat.S_ISREG(before.st_mode):
                    mode = "100755" if before.st_mode & 0o111 else "100644"
                    digest.update(f"blob {before.st_size}\0".encode())
                    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                    with os.fdopen(descriptor, "rb") as stream:
                        while chunk := stream.read(1024 * 1024):
                            digest.update(chunk)
                else:
                    raise TieringError(f"无法完整检查特殊文件: {path}")
                if file_identity(before) != file_identity(path.lstat()):
                    raise TieringError(f"检查期间文件发生变化，停止写入后重新检查: {path}")
                result[path.relative_to(root).as_posix()] = (mode, digest.hexdigest(), file_identity(before))

    visit(root)
    return result


def run_test(root, command, timeout):
    """Use an external output file so inherited pipes cannot hang cleanup."""
    with tempfile.TemporaryFile() as output:
        try:
            process = subprocess.Popen(
                command, shell=True, cwd=root, stdin=subprocess.DEVNULL,
                stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
            )
        except OSError as exc:
            return {"test_exit_code": None, "test_output": "", "output_truncated": False,
                    "timed_out": False, "test_error": str(exc)}
        timed_out = False
        try:
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
        finally:
            # Also stop background children left behind after a successful shell exit.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        size = output.seek(0, os.SEEK_END)
        output.seek(max(0, size - OUTPUT_LIMIT))
        return {
            "test_exit_code": process.returncode,
            "test_output": output.read(OUTPUT_LIMIT).decode("utf-8", errors="replace"),
            "output_truncated": size > OUTPUT_LIMIT,
            "timed_out": timed_out,
            "test_error": None,
        }


def check(worktree, snapshot_tree, allowed_files, test_command, timeout=60):
    if not isinstance(test_command, str) or not test_command.strip():
        raise TieringError("独立检查必须提供约定的 test_command")
    if not math.isfinite(timeout) or timeout <= 0:
        raise TieringError("timeout 必须是有限的正数")
    allowed = {relative_file(name) for name in allowed_files}
    root = Path(worktree).resolve()
    if Path(os.fsdecode(git(root, "rev-parse", "--show-toplevel")).removesuffix("\n")).resolve() != root:
        raise TieringError("worktree 必须是 Git 工作目录的根目录")
    tree = git(root, "rev-parse", "--verify", "--end-of-options", snapshot_tree + "^{tree}").decode().strip()
    baseline = snapshot_files(root, tree)
    algorithm = git(root, "rev-parse", "--show-object-format").decode().strip()
    before = working_files(root, algorithm)
    test = run_test(root, test_command, timeout)
    after = working_files(root, algorithm)
    changed = sorted(
        name for name in baseline.keys() | before.keys() | after.keys()
        if baseline.get(name) != before.get(name, ())[:2] or baseline.get(name) != after.get(name, ())[:2]
    )
    polluted = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
    out_of_scope = sorted(set(changed) - allowed)
    return {
        "worktree": str(root), "snapshot_tree": tree, "test_command": test_command, **test,
        "changed_files": changed, "out_of_scope_files": out_of_scope,
        "polluted_files": polluted, "stable": not polluted,
        "clean": test["test_exit_code"] == 0 and not test["timed_out"] and not out_of_scope and not polluted,
    }


def build_report(events, cutoff=None):
    cutoff = cutoff or datetime.now(UTC).isoformat()
    end = timestamp(cutoff)
    # Validate the whole ledger even when reporting an earlier cutoff.
    aggregate(events)
    tasks = aggregate([event for event in events if timestamp(event["time"]) <= end])
    groups, warnings = {}, []
    for task_id, task in tasks.items():
        dispatch, last, review = task["dispatch"], task["last"], task["review"]
        key = tuple(dispatch[k] for k in ("profile", "model", "effort"))
        if key not in groups:
            groups[key] = {
                **dict(zip(("profile", "model", "effort"), key, strict=True)),
                **dict.fromkeys(("dispatched", "completed", "accepted", "rejected", "failed", "interrupted",
                                "cancelled", "waiting", "pending_review", "needs_check"), 0),
                "durations": [],
            }
        group = groups[key]
        group["dispatched"] += 1
        status = last["status"]
        if status == "completed":
            group["completed"] += 1
        finished = None
        if review:
            group[review["decision"]] += 1
            finished = review
        elif status in {"failed", "interrupted", "cancelled"}:
            group[status] += 1
            finished = last
        elif status == "completed":
            group["pending_review"] += 1
        elif last is not dispatch and status in WAITING:
            group["waiting"] += 1
        else:
            group["needs_check"] += 1
            warnings.append(f"任务 {task_id} 待核对：{'只有派发行' if last is dispatch else status}")
        if finished:
            group["durations"].append((timestamp(finished["time"]) - timestamp(dispatch["time"])).total_seconds())
    for group in groups.values():
        count = sum(group[k] for k in ("accepted", "rejected", "failed", "interrupted", "cancelled"))
        durations = group.pop("durations")
        group.update(
            determined=count, interrupted_or_cancelled=group["interrupted"] + group["cancelled"],
            pass_rate=group["accepted"] / count if count else None,
            average_duration_seconds=mean(durations) if durations else None,
            rate_note="" if count else "暂无已确定结果",
            sample_note="样本较少（已确定结果少于 10 条），不轻易判断配置优劣" if count < 10 else "",
        )
    return {"cutoff": cutoff, "groups": [groups[k] for k in sorted(groups)], "warnings": warnings}


def format_report(report):
    lines = [f"统计截止时间: {report['cutoff']}",
             "配置 | 模型 | 推理强度 | 派发 | completed | 通过 | 拒绝 | 失败 | 中断/取消 | 等待 | 待审 | 待核对 | 通过率 | 平均耗时(秒)"]
    for group in report["groups"]:
        rate = f"{group['pass_rate']:.1%}" if group["pass_rate"] is not None else group["rate_note"]
        duration = group["average_duration_seconds"]
        values = [str(group[k]) for k in ("profile", "model", "effort", "dispatched", "completed", "accepted",
                  "rejected", "failed", "interrupted_or_cancelled", "waiting", "pending_review", "needs_check")]
        lines.append(" | ".join([*values, rate, f"{duration:.2f}" if duration is not None else "—"]))
        if group["sample_note"]:
            lines.append(f"  {group['profile']}: {group['sample_note']}")
    if not report["groups"]:
        lines.append("暂无任务；暂无已确定结果")
    lines.extend(report["warnings"])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", help="JSONL 文件；也可用 AGENT_TIERING_FILE")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("record-task", "record-review", "check", "report"):
        sub = commands.add_parser(name)
        sub.add_argument("--file", default=argparse.SUPPRESS, help="JSONL 文件")
        if name in {"record-task", "record-review"}:
            sub.add_argument("--task-id", required=True)
            sub.add_argument("--attempt-id")
            sub.add_argument("--time", help="带时区的 ISO 8601 时间；默认现在")
            sub.add_argument("--reason")
        if name == "record-task":
            sub.add_argument("--status", default="queued")
            for field in ("profile", "model", "effort", "goal", "snapshot-tree", "test-command"):
                sub.add_argument("--" + field)
            sub.add_argument("--allowed-file", "--allowed-files", dest="allowed_files", nargs="*", action="extend", default=[])
            sub.add_argument("--acceptance", nargs="+", action="extend", default=[])
        elif name == "record-review":
            sub.add_argument("--decision", choices=("accepted", "rejected"), required=True)
            sub.add_argument("--test-exit-code", type=int)
            sub.add_argument("--out-of-scope-files", nargs="*")
        elif name == "check":
            sub.add_argument("--worktree", required=True)
            sub.add_argument("--snapshot-tree", required=True)
            sub.add_argument("--allowed-file", "--allowed-files", dest="allowed_files", nargs="*", action="extend", default=[])
            sub.add_argument("--test-command", required=True)
            sub.add_argument("--timeout", type=float, default=60)
        else:
            sub.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            result = check(args.worktree, args.snapshot_tree, args.allowed_files, args.test_command, args.timeout)
        elif args.command == "report":
            result = build_report(read_events(args.file))
            if args.format == "text":
                print(format_report(result))
                return 0
        else:
            result = {"event": "task" if args.command == "record-task" else "review",
                      "time": args.time or datetime.now(UTC).isoformat(), "task_id": args.task_id,
                      "attempt_id": args.attempt_id}
            if args.command == "record-task":
                result["status"] = args.status
                if any(getattr(args, key) is not None for key in DISPATCH_FIELDS - {"allowed_files", "acceptance"}) or args.allowed_files or args.acceptance:
                    result.update({key: getattr(args, key) for key in DISPATCH_FIELDS})
                else:
                    result["reason"] = args.reason
            else:
                result.update({key: getattr(args, key) for key in ("decision", "reason", "test_exit_code", "out_of_scope_files")})
            append_event(result, args.file)
        print(json.dumps(result, ensure_ascii=True, allow_nan=False))
        return 1 if args.command == "check" and not result["clean"] else 0
    except (TieringError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"tiering: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
