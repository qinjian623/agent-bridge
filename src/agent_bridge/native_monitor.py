"""Discovery and normalization across Pi, Claude, Antigravity (AGY), and Codex."""
from __future__ import annotations

import fcntl
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

MONITORED_RUNTIMES = ("pi", "claude", "antigravity", "codex")
VALID_STATUS_FILTERS = ("active", "running", "idle", "waiting_input", "recovery_needed", "stopped")
RUNTIME_ALIASES = {
    "agy": "antigravity",
    "antigravity": "antigravity",
    "pi": "pi",
    "claude": "claude",
    "codex": "codex",
}
RUNTIME_LABELS = {
    "pi": "pi",
    "claude": "claude",
    "antigravity": "agy",
    "codex": "codex",
}
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_TITLE_RE = re.compile(r'^\s*title:\s*"(.*)"\s*$')
_CODEX_VALUE_FLAGS = {
    "-m",
    "--model",
    "-c",
    "--config",
    "-C",
    "--cd",
    "-p",
    "--profile",
    "-s",
    "--sandbox",
    "-a",
    "--ask-for-approval",
    "--listen",
    "--output-schema",
    "-i",
    "--image",
    "-o",
    "--output-last-message",
}
_NATIVE_CACHE_TTL_SECONDS = 0.75
_NATIVE_CACHE_LOCK = threading.Lock()
_NATIVE_CACHE: dict[str, Any] = {"ts": 0.0, "ppid_map": {}, "procs": [], "lock_owners": {}}
_CODEX_TITLE_CACHE: dict[str, tuple[int, int, dict[str, str]]] = {}


def normalize_runtime(value: str | None) -> str | None:
    """Normalize runtime names and profile prefixes into a canonical monitored runtime."""
    if not value:
        return None
    text = str(value).strip().lower()
    if text in RUNTIME_ALIASES:
        return RUNTIME_ALIASES[text]
    for prefix, canonical in (
        ("antigravity", "antigravity"),
        ("agy", "antigravity"),
        ("claude", "claude"),
        ("codex", "codex"),
        ("pi", "pi"),
    ):
        if text.startswith(prefix):
            return canonical
    return text


def display_runtime(runtime: str | None) -> str:
    """Return the user-facing short label for a runtime."""
    canonical = normalize_runtime(runtime)
    if canonical is None:
        return "-"
    return RUNTIME_LABELS.get(canonical, canonical)


def parse_runtime_filter(value: str | None) -> str | None:
    """Validate an explicit runtime query filter against monitored runtimes."""
    if not value:
        return None
    text = str(value).strip().lower()
    if not text or text == "all":
        return None
    canonical = RUNTIME_ALIASES.get(text)
    if canonical not in MONITORED_RUNTIMES:
        raise ValueError(f"Unsupported runtime {value!r}")
    return canonical


def normalize_status_filter(value: str | None) -> str | None:
    """Validate and normalize status filters, mapping 'waiting' -> 'waiting_input'."""
    if not value:
        return None
    text = str(value).strip().lower()
    if not text or text == "all":
        return None
    if text == "waiting":
        return "waiting_input"
    if text not in VALID_STATUS_FILTERS:
        raise ValueError(f"Unsupported status {value!r}")
    return text


def matches_bridge_status(session: dict[str, Any], status_filter: str | None) -> bool:
    """Apply consistent bridge session status filtering shared across TUI/API/SSE."""
    if not status_filter:
        return True
    obs = str(session.get("observed") or "stopped").lower()
    if status_filter == "active":
        return (
            obs in ("running", "idle", "waiting_input", "starting", "recovery_needed")
            or bool(session.get("alive"))
            or bool(session.get("waiting_question"))
        )
    if status_filter == "waiting_input":
        return obs == "waiting_input" or bool(session.get("waiting_question"))
    return obs == status_filter


def matches_native_status(native_proc: dict[str, Any], status_filter: str | None) -> bool:
    """Apply consistent native process status filtering shared across TUI/API/SSE."""
    if not status_filter:
        return True
    obs = str(native_proc.get("observed") or native_proc.get("status") or "active").lower()
    if status_filter == "active":
        return native_proc.get("kind") != "daemon"
    return obs == status_filter


def _flag_value(args: list[str], *flags: str) -> str | None:
    for idx, arg in enumerate(args):
        for flag in flags:
            if arg == flag and idx + 1 < len(args):
                return args[idx + 1]
            if arg.startswith(flag + "="):
                return arg.split("=", 1)[1]
    return None


def _pid_holds_same_lock_file(proc_root: Path, pid: int, lock_stat: os.stat_result) -> bool | None:
    """Verify via /proc/<pid>/fd whether pid actually holds an fd to the exact lock file.

    Returns True/False when /proc/<pid>/fd is inspectable, or None if unreadable/absent.
    Checking fd.stat() handles Btrfs subvolumes (where st_dev differs from /proc/locks)
    while rejecting cross-filesystem inode collisions.
    """
    fd_dir = proc_root / str(pid) / "fd"
    if not fd_dir.is_dir():
        return None
    try:
        entries = list(fd_dir.iterdir())
    except OSError:
        return None
    if not entries:
        return False
    for fd_entry in entries:
        try:
            fst = fd_entry.stat()
            if fst.st_ino == lock_stat.st_ino and fst.st_dev == lock_stat.st_dev:
                return True
        except OSError:
            continue
    return False


def _active_agy_lock_owners(proc_root: Path, native_home: Path) -> dict[int, list[str]]:
    """Map PID -> active Antigravity conversation IDs from /proc/locks + presence/*.lock."""
    locks_path = proc_root / "locks"
    presence_dir = native_home / "presence"
    if not locks_path.exists() or not presence_dir.exists():
        return {}

    inode_locks: dict[int, list[tuple[int, int, int]]] = {}
    try:
        for line in locks_path.read_text(errors="replace").splitlines():
            fields = line.split()
            if len(fields) < 6 or fields[1:4] != ["FLOCK", "ADVISORY", "WRITE"]:
                continue
            try:
                pid = int(fields[4])
                maj_hex, min_hex, ino_str = fields[5].split(":", 2)
                dev_maj = int(maj_hex, 16)
                dev_min = int(min_hex, 16)
                inode = int(ino_str)
            except (ValueError, IndexError):
                continue
            inode_locks.setdefault(inode, []).append((pid, dev_maj, dev_min))
    except OSError:
        return {}

    if not inode_locks:
        return {}

    pid_sessions: dict[int, list[str]] = {}
    try:
        lock_files = list(presence_dir.glob("*.lock"))
    except OSError:
        return {}

    for lock_file in lock_files:
        try:
            lock_stat = lock_file.stat()
        except OSError:
            continue
        candidates = inode_locks.get(lock_stat.st_ino)
        if not candidates:
            continue
        # Confirm an active FLOCK lock non-blockingly when on real /proc.
        try:
            with lock_file.open("rb") as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    if str(proc_root) == "/proc":
                        continue
                except BlockingIOError:
                    pass
        except OSError:
            continue

        conv_id = lock_file.stem
        lock_maj = os.major(lock_stat.st_dev)
        lock_min = os.minor(lock_stat.st_dev)
        for owner_pid, dev_maj, dev_min in candidates:
            fd_match = _pid_holds_same_lock_file(proc_root, owner_pid, lock_stat)
            if fd_match is False:
                continue
            if fd_match is None and (dev_maj, dev_min) != (lock_maj, lock_min):
                continue
            sessions_for_pid = pid_sessions.setdefault(owner_pid, [])
            if conv_id not in sessions_for_pid:
                sessions_for_pid.append(conv_id)

    for convs in pid_sessions.values():
        convs.sort()
    return pid_sessions


def _agy_titles(native_home: Path, conv_ids: set[str]) -> dict[str, str]:
    titles: dict[str, str] = {}
    ann_dir = native_home / "annotations"
    if not conv_ids or not ann_dir.exists():
        return titles
    for cid in conv_ids:
        if Path(cid).name != cid:
            continue
        pbtxt = ann_dir / f"{cid}.pbtxt"
        try:
            for line in pbtxt.read_text(errors="replace").splitlines()[:20]:
                match = _TITLE_RE.match(line)
                if match and match.group(1).strip():
                    titles[cid] = match.group(1).strip()
                    break
        except OSError:
            continue
    return titles


def _codex_titles(codex_home: Path, session_ids: set[str]) -> dict[str, str]:
    idx_file = codex_home / "session_index.jsonl"
    if not session_ids:
        return {}
    try:
        st = idx_file.stat()
    except OSError:
        return {}
    if not idx_file.is_file():
        return {}
    cache_key = str(idx_file)
    with _NATIVE_CACHE_LOCK:
        cached = _CODEX_TITLE_CACHE.get(cache_key)
    if cached is not None and cached[0] == st.st_mtime_ns and cached[1] == st.st_size:
        parsed_index = cached[2]
    else:
        parsed_index = {}
        try:
            for line in idx_file.read_text(errors="replace").splitlines():
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(item, dict):
                    continue
                sid = item.get("id")
                name = item.get("thread_name")
                if isinstance(sid, str) and isinstance(name, str) and name.strip():
                    parsed_index[sid] = name.strip()
        except OSError:
            return {}
        with _NATIVE_CACHE_LOCK:
            _CODEX_TITLE_CACHE[cache_key] = (st.st_mtime_ns, st.st_size, parsed_index)
    return {sid: parsed_index[sid] for sid in session_ids if sid in parsed_index}


def _has_ancestor(pid: int, ppid_map: dict[int, int], targets: set[int]) -> bool:
    if pid in targets:
        return True
    cur = ppid_map.get(pid, 0)
    seen: set[int] = set()
    while cur > 1 and cur not in seen:
        if cur in targets:
            return True
        seen.add(cur)
        cur = ppid_map.get(cur, 0)
    return False


def _scan_proc_linux(proc_root: Path) -> tuple[dict[int, int], list[dict[str, Any]]]:
    uid = os.getuid()
    ppid_map: dict[int, int] = {}
    records: list[dict[str, Any]] = []
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return ppid_map, records

    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            if entry.stat().st_uid != uid:
                continue
            stat_txt = (entry / "stat").read_text(errors="replace")
            rest = stat_txt.rsplit(") ", 1)[1].split()
            state = rest[0]
            ppid = int(rest[1])
            ppid_map[pid] = ppid
            if state == "Z":
                continue
            raw_cmd = (entry / "cmdline").read_bytes()
            args = [part.decode("utf-8", "replace") for part in raw_cmd.split(b"\0") if part]
            if not args:
                continue
            comm = (entry / "comm").read_text(errors="replace").strip()
            try:
                exe = os.readlink(entry / "exe").removesuffix(" (deleted)")
            except OSError:
                exe = ""
            try:
                cwd = os.readlink(entry / "cwd")
            except OSError:
                cwd = ""
            records.append(
                {
                    "pid": pid,
                    "ppid": ppid,
                    "comm": comm,
                    "exe": exe,
                    "exename": Path(exe).name if exe else "",
                    "cwd": cwd,
                    "args": args,
                }
            )
        except (OSError, ValueError, IndexError):
            continue
    return ppid_map, records


def _scan_ps_posix() -> tuple[dict[int, int], list[dict[str, Any]]]:
    ppid_map: dict[int, int] = {}
    records: list[dict[str, Any]] = []
    try:
        output = subprocess.check_output(
            ["ps", "-axo", "pid=,ppid=,comm=,args="],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return ppid_map, records

    for line in output.splitlines():
        parts = line.strip().split(None, 3)
        if len(parts) < 4:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        ppid_map[pid] = ppid
        comm = Path(parts[2]).name
        args = parts[3].split()
        records.append(
            {
                "pid": pid,
                "ppid": ppid,
                "comm": comm,
                "exe": parts[2],
                "exename": comm,
                "cwd": "",
                "args": args,
            }
        )
    return ppid_map, records


def _is_internal_bridge_or_proxy(proc: dict[str, Any]) -> bool:
    """Identify bridge daemons/workers, proxies, and system helpers without inspecting prompt text."""
    args: list[str] = proc["args"]
    comm: str = proc["comm"]
    exe: str = proc["exe"]
    exename: str = proc["exename"]
    if comm in {"systemd-inhibit", "ChatGPT"}:
        return True
    cmd0 = Path(args[0]).name if args else ""
    if cmd0 in {"agent-bridge", "codex-async"} or exename in {"agent-bridge", "codex-async"}:
        return True

    interp = cmd0.startswith("python") or exename.startswith("python")
    if interp:
        idx = 1
        while idx < len(args):
            arg = args[idx]
            if arg == "-m" and idx + 1 < len(args):
                mod = args[idx + 1]
                return mod == "agent_bridge" or mod.startswith("agent_bridge.")
            if arg in {"-c", "-W", "-X"}:
                idx += 2
                continue
            if arg.startswith("-"):
                idx += 1
                continue
            arg_path = Path(arg)
            return "agent_bridge" in arg_path.parts or "codex-async" in arg_path.parts
        return False

    if cmd0 in {"node", "nodejs"} or exename in {"node", "nodejs"}:
        for arg in args[1:4]:
            if arg.startswith("-"):
                continue
            arg_path = Path(arg)
            if arg_path.name == "proxy.mjs" or "codex-async" in arg_path.parts:
                return True
            if exe and "claude-openai-proxy" in exe:
                return True
            break

    return False


def _codex_subcommand(args: list[str]) -> str:
    """Return the first positional subcommand of a codex invocation, skipping known option values."""
    idx = 1
    while idx < len(args):
        arg = args[idx]
        if arg == "--":
            break
        if arg in _CODEX_VALUE_FLAGS:
            idx += 2
            continue
        if arg.startswith("-"):
            idx += 1
            continue
        return arg
    return ""


def _classify_process(
    proc: dict[str, Any],
    agy_bin_pids: set[int],
    agy_bin_ppids: set[int],
) -> tuple[str, str] | None:
    """Return (runtime, profile_hint) if process is a pi/claude/agy/codex agent process."""
    args: list[str] = proc["args"]
    comm: str = proc["comm"]
    exe: str = proc["exe"]
    exename: str = proc["exename"]
    pid: int = proc["pid"]
    ppid: int = proc["ppid"]
    cmd0 = Path(args[0]).name if args else ""

    if _is_internal_bridge_or_proxy(proc):
        return None

    # Antigravity (AGY)
    is_agy_bin = (
        comm in {"agy", "agy-real", "antigravity"}
        or cmd0 in {"agy", "agy-real", "antigravity"}
        or exename.startswith(("agy-real", "antigravity"))
    )
    is_agy_wrapper = cmd0 in {"bash", "sh", "zsh"} and len(args) > 1 and Path(args[1]).name in {"agy", "agy-real"}
    if is_agy_wrapper:
        # Suppress shell wrapper regardless of whether wrapper spawned agy-real or vice versa.
        if ppid in agy_bin_pids or pid in agy_bin_ppids:
            return None
        return "antigravity", "agy"
    if is_agy_bin:
        if ppid in agy_bin_pids and comm == "agy":
            return None
        return "antigravity", "agy"

    # Codex CLI / app-server
    if comm == "codex" or exename == "codex" or cmd0 == "codex":
        return "codex", "codex"

    # Claude Code CLI
    claude_names = {"claude", "claude-ollama", "claude-openai", "claude-deepseek"}
    if comm in claude_names or cmd0 in claude_names or "/claude/versions/" in exe:
        hint = cmd0 if cmd0 in claude_names else "claude"
        return "claude", hint

    # Pi Coding Agent
    pi_names = {"pi", "pi-ollama", "pi-deepseek"}
    if (
        comm in pi_names
        or cmd0 in pi_names
        or any("pi-coding-agent/dist/cli.js" in arg for arg in args[:4])
    ):
        hint = cmd0 if cmd0 in pi_names else (comm if comm in pi_names else "pi")
        return "pi", hint

    return None


def collect_native_sessions(
    *,
    proc_root: Path | None = None,
    home: Path | None = None,
    exclude_pids: set[int] | None = None,
    exclude_native_ids: set[tuple[str, str]] | None = None,
    group_labels: dict[tuple[str, str], str] | None = None,
    platform_name: str | None = None,
) -> list[dict[str, Any]]:
    """Discover active native CLI processes across pi, claude, antigravity (agy), and codex."""
    use_cache = proc_root is None and home is None and platform_name is None
    resolved_home = Path(home) if home is not None else Path.home()
    resolved_proc = Path(proc_root) if proc_root is not None else Path("/proc")
    plat = platform_name or sys.platform
    agy_home = resolved_home / ".gemini/antigravity-cli"
    codex_home = resolved_home / ".codex"

    if use_cache:
        now = time.monotonic()
        with _NATIVE_CACHE_LOCK:
            if now - _NATIVE_CACHE["ts"] < _NATIVE_CACHE_TTL_SECONDS:
                ppid_map = _NATIVE_CACHE["ppid_map"]
                procs = _NATIVE_CACHE["procs"]
                lock_owners = _NATIVE_CACHE["lock_owners"]
            else:
                if plat.startswith("linux") and resolved_proc.is_dir():
                    ppid_map, procs = _scan_proc_linux(resolved_proc)
                else:
                    ppid_map, procs = _scan_ps_posix()
                lock_owners = _active_agy_lock_owners(resolved_proc, agy_home) if resolved_proc.exists() else {}
                _NATIVE_CACHE.update(
                    {"ts": now, "ppid_map": ppid_map, "procs": procs, "lock_owners": lock_owners}
                )
    else:
        if proc_root is not None or (plat.startswith("linux") and resolved_proc.is_dir()):
            ppid_map, procs = _scan_proc_linux(resolved_proc)
        else:
            ppid_map, procs = _scan_ps_posix()
        lock_owners = _active_agy_lock_owners(resolved_proc, agy_home) if resolved_proc.exists() else {}

    excluded = set(exclude_pids or ())
    excluded.add(os.getpid())
    # Also exclude any live bridge worker processes visible in cmdline.
    for proc in procs:
        args = proc["args"]
        if len(args) >= 4 and args[1:3] == ["-m", "agent_bridge"] and "worker" in args[3:]:
            excluded.add(proc["pid"])
        elif _is_internal_bridge_or_proxy(proc) and "worker" in args:
            excluded.add(proc["pid"])

    agy_bin_pids: set[int] = set()
    agy_bin_ppids: set[int] = set()
    for proc in procs:
        args = proc["args"]
        cmd0 = Path(args[0]).name if args else ""
        if (
            proc["comm"] in {"agy", "agy-real", "antigravity"}
            or cmd0 in {"agy", "agy-real", "antigravity"}
            or proc["exename"].startswith(("agy-real", "antigravity"))
        ):
            agy_bin_pids.add(proc["pid"])
            if proc["ppid"] > 1:
                agy_bin_ppids.add(proc["ppid"])

    raw_items: list[dict[str, Any]] = []
    fallback_holders: list[tuple[dict[str, Any], list[str]]] = []
    agy_ids: set[str] = set()
    codex_ids: set[str] = set()
    seen_bridge_native = set(exclude_native_ids or ())
    labels = group_labels or {}

    for proc in procs:
        pid = proc["pid"]
        held_locks = lock_owners.get(pid) or []
        unmanaged_locks = [cid for cid in held_locks if ("antigravity", cid) not in seen_bridge_native]
        has_unmanaged_lock = bool(unmanaged_locks)
        if _has_ancestor(pid, ppid_map, excluded) and not has_unmanaged_lock:
            continue
        classified = _classify_process(proc, agy_bin_pids, agy_bin_ppids)
        if classified is None:
            if has_unmanaged_lock:
                fallback_holders.append((proc, unmanaged_locks))
            continue
        rt, profile_hint = classified
        if rt != "antigravity" and has_unmanaged_lock:
            fallback_holders.append((proc, unmanaged_locks))
        args = proc["args"]
        cwd = proc["cwd"]
        model = _flag_value(args, "--model", "-m") or ""
        provider = _flag_value(args, "--provider") or ""
        native_id: str | None = None
        native_ids: list[str] = []
        mode = "interactive"
        kind = "interactive"

        if rt == "antigravity":
            native_id = _flag_value(args, "--conversation")
            if native_id:
                native_ids = [native_id, *(cid for cid in held_locks if cid != native_id)]
            elif held_locks:
                native_id = held_locks[0]
                native_ids = list(held_locks)
            if "--input-format" in args or "--print" in args or "-p" in args:
                mode = "headless"
                kind = "worker"
        elif rt == "codex":
            subcmd = _codex_subcommand(args)
            if subcmd in {"app-server", "exec-server"}:
                mode = "daemon"
                kind = "daemon"
            elif subcmd == "exec":
                mode = "headless"
                kind = "worker"
            for idx, arg in enumerate(args):
                if arg == "resume" and idx + 1 < len(args) and _UUID_RE.match(args[idx + 1]):
                    native_id = args[idx + 1]
                    break
        elif rt == "claude":
            native_id = _flag_value(args, "--resume", "--session-id")
            if "-p" in args or "--print" in args or _flag_value(args, "--output-format") == "stream-json":
                mode = "headless"
                kind = "worker"
            if not model:
                settings_raw = _flag_value(args, "--settings")
                if settings_raw and settings_raw.startswith("{"):
                    try:
                        settings_obj = json.loads(settings_raw)
                        env_obj = settings_obj.get("env") if isinstance(settings_obj, dict) else None
                        if isinstance(env_obj, dict):
                            model = str(env_obj.get("ANTHROPIC_MODEL") or "")
                    except ValueError:
                        pass
        elif rt == "pi":
            native_id = _flag_value(args, "--session")
            if "-p" in args or "--print" in args or _flag_value(args, "--mode") in {"rpc", "json"}:
                mode = "headless"
                kind = "worker"

        if native_ids:
            native_ids = [cid for cid in native_ids if (rt, cid) not in seen_bridge_native]
            if not native_ids:
                continue
            native_id = native_ids[0]
        elif native_id and (rt, native_id) in seen_bridge_native:
            continue

        if rt == "antigravity" and native_ids:
            agy_ids.update(native_ids)
        elif rt == "codex" and native_id:
            codex_ids.add(native_id)

        raw_items.append(
            {
                "runtime": rt,
                "profile_hint": profile_hint,
                "pid": pid,
                "ppid": proc["ppid"],
                "cwd": cwd,
                "model": model,
                "provider": provider,
                "native_id": native_id,
                "native_ids": native_ids,
                "mode": mode,
                "kind": kind,
                "args": args,
            }
        )

    # Fallback: if a suppressed wrapper or excluded process holds an unmanaged AGY lock
    # not already claimed by a classified Antigravity process, surface it directly.
    for proc, unmanaged_locks in fallback_holders:
        remaining = [cid for cid in unmanaged_locks if cid not in agy_ids]
        if not remaining:
            continue
        agy_ids.update(remaining)
        raw_items.append(
            {
                "runtime": "antigravity",
                "profile_hint": "agy",
                "pid": proc["pid"],
                "ppid": proc["ppid"],
                "cwd": proc["cwd"],
                "model": _flag_value(proc["args"], "--model", "-m") or "",
                "provider": _flag_value(proc["args"], "--provider") or "",
                "native_id": remaining[0],
                "native_ids": remaining,
                "mode": "interactive",
                "kind": "interactive",
                "args": proc["args"],
            }
        )

    agy_title_map = _agy_titles(agy_home, agy_ids)
    codex_title_map = _codex_titles(codex_home, codex_ids)

    results: list[dict[str, Any]] = []
    for item in raw_items:
        rt = item["runtime"]
        pid = item["pid"]
        native_id = item["native_id"]
        mode = item["mode"]
        kind = item["kind"]
        profile_hint = item["profile_hint"]

        title = labels.get((rt, native_id)) if native_id else None
        if not title and rt == "antigravity" and native_id:
            title = agy_title_map.get(native_id)
        if not title and rt == "codex" and native_id:
            title = codex_title_map.get(native_id)
        if not title:
            if mode == "daemon":
                subcmd = _codex_subcommand(item["args"]) or "daemon"
                title = f"codex {subcmd}"
            elif native_id:
                short_id = Path(native_id).stem[:8] if rt == "pi" else native_id[:8]
                title = f"{display_runtime(rt)} #{short_id}"
            elif mode == "headless":
                title = f"{display_runtime(rt)} worker (pid {pid})"
            else:
                title = f"{display_runtime(rt)} (pid {pid})"

        status = "running" if mode in {"headless", "daemon"} else "active"
        results.append(
            {
                "id": f"native:{rt}:{pid}",
                "source": "native",
                "managed": False,
                "runtime": rt,
                "display_runtime": display_runtime(rt),
                "kind": kind,
                "mode": mode,
                "name": title,
                "host_id": "local",
                "profile": profile_hint,
                "model": item["model"],
                "provider": item["provider"],
                "native_id": native_id,
                "native_ids": item.get("native_ids") or ([native_id] if native_id else []),
                "cwd": item["cwd"],
                "pid": pid,
                "ppid": item["ppid"],
                "worker_pid": pid,
                "desired": "open",
                "observed": status,
                "status": status,
                "alive": True,
                "waiting_question": False,
            }
        )

    order = {"worker": 0, "interactive": 1, "daemon": 2}
    rt_order = {rt: i for i, rt in enumerate(MONITORED_RUNTIMES)}
    results.sort(key=lambda r: (order.get(r["kind"], 3), rt_order.get(r["runtime"], 9), r["pid"]))
    return results


def _empty_runtime_bucket(rt: str) -> dict[str, Any]:
    return {
        "runtime": rt,
        "display_name": display_runtime(rt),
        "bridge_total": 0,
        "bridge_active": 0,
        "bridge_running": 0,
        "bridge_waiting": 0,
        "bridge_recovery": 0,
        "native_active": 0,
        "native_workers": 0,
        "native_interactive": 0,
        "native_daemons": 0,
        "total_live": 0,
        "total_cost_usd": 0.0,
        "known_cost_sessions": 0,
        "cost_complete": True,
    }


def summarize_runtimes(
    bridge_sessions: list[dict[str, Any]],
    native_sessions: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build per-runtime breakdown across pi, claude, antigravity (agy), and codex."""
    summary: dict[str, dict[str, Any]] = {}
    ordered_keys = list(MONITORED_RUNTIMES)
    extra_keys = sorted(
        {
            rt
            for s in (*bridge_sessions, *native_sessions)
            if (rt := normalize_runtime(s.get("runtime"))) and rt not in MONITORED_RUNTIMES
        }
    )
    for rt in [*ordered_keys, *extra_keys]:
        summary[rt] = _empty_runtime_bucket(rt)

    for s in bridge_sessions:
        rt = normalize_runtime(s.get("runtime")) or "unknown"
        bucket = summary.setdefault(rt, _empty_runtime_bucket(rt))
        bucket["bridge_total"] += 1
        obs = s.get("observed")
        if obs in ("running", "idle", "waiting_input") or s.get("alive"):
            bucket["bridge_active"] += 1
        if obs == "running":
            bucket["bridge_running"] += 1
        if obs == "waiting_input" or s.get("waiting_question"):
            bucket["bridge_waiting"] += 1
        if obs == "recovery_needed":
            bucket["bridge_recovery"] += 1
        if "cost_usd" in s:
            if isinstance(s.get("cost_usd"), (int, float)):
                bucket["total_cost_usd"] += float(s["cost_usd"])
                bucket["known_cost_sessions"] += 1
                if s.get("cost_complete") is False:
                    bucket["cost_complete"] = False
            elif s.get("has_attempts") or s.get("cost_complete") is False:
                bucket["cost_complete"] = False
        else:
            attempt = s.get("attempt")
            if isinstance(attempt, dict):
                if isinstance(attempt.get("cost_usd"), (int, float)):
                    bucket["total_cost_usd"] += float(attempt["cost_usd"])
                    bucket["known_cost_sessions"] += 1
                    if attempt.get("cost_complete") is False:
                        bucket["cost_complete"] = False
                else:
                    bucket["cost_complete"] = False
            elif s.get("has_attempts"):
                bucket["cost_complete"] = False

    for n in native_sessions:
        rt = normalize_runtime(n.get("runtime")) or "unknown"
        bucket = summary.setdefault(rt, _empty_runtime_bucket(rt))
        kind = n.get("kind")
        if kind == "daemon":
            bucket["native_daemons"] += 1
        else:
            bucket["native_active"] += 1
            if kind == "worker":
                bucket["native_workers"] += 1
            else:
                bucket["native_interactive"] += 1

    for bucket in summary.values():
        bucket["total_live"] = bucket["bridge_active"] + bucket["native_active"]
        bucket["total_cost_usd"] = round(bucket["total_cost_usd"], 4)

    return summary


def _unified_sort_key(item: dict[str, Any]) -> tuple[int, int, float, int, str]:
    status = str(item.get("status") or "stopped").lower()
    if item.get("waiting_question") or status == "waiting_input":
        tier = 0
    elif status == "running":
        tier = 1
    elif status in ("active", "idle", "starting") or item.get("alive"):
        tier = 2 if item.get("kind") != "daemon" else 4
    elif status == "recovery_needed":
        tier = 3
    elif item.get("kind") == "daemon":
        tier = 4
    else:
        tier = 5
    source_rank = 0 if item.get("source") == "bridge" else 1
    created = item.get("created")
    recency = 0.0
    if isinstance(created, (int, float)) and not isinstance(created, bool):
        try:
            f_created = float(created)
            if math.isfinite(f_created):
                recency = -f_created
        except OverflowError:
            recency = 0.0
    raw_pid = item.get("pid")
    pid = raw_pid if isinstance(raw_pid, int) and not isinstance(raw_pid, bool) else 0
    return (tier, source_rank, recency, pid, str(item.get("id") or ""))


def unified_sessions(
    snapshot: dict[str, Any],
    *,
    runtime: str | None = None,
    source: str | None = None,
    status: str | None = None,
    host_id: str | None = None,
    offset: int = 0,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Return a consistently shaped session list merging Bridge and Native records."""
    rt_filter = parse_runtime_filter(runtime)
    src_filter = source.lower() if source and source.lower() != "all" else None
    st_filter = normalize_status_filter(status)
    start = max(0, int(offset or 0))
    items: list[dict[str, Any]] = []

    if src_filter in (None, "bridge"):
        for s in snapshot.get("sessions", []):
            rt = normalize_runtime(s.get("runtime"))
            if rt_filter and rt != rt_filter:
                continue
            if host_id and s.get("host_id", "local") != host_id:
                continue
            if not matches_bridge_status(s, st_filter):
                continue
            obs = str(s.get("observed") or "stopped")
            attempt = s.get("attempt") or {}
            cost_usd = s.get("cost_usd") if isinstance(s.get("cost_usd"), (int, float)) else attempt.get("cost_usd")
            items.append(
                {
                    "id": s.get("id"),
                    "source": "bridge",
                    "managed": True,
                    "runtime": rt or s.get("runtime"),
                    "display_runtime": display_runtime(rt or s.get("runtime")),
                    "name": s.get("name"),
                    "status": obs,
                    "mode": "bridge-worker",
                    "host_id": s.get("host_id", "local"),
                    "pid": s.get("worker_pid"),
                    "alive": bool(s.get("alive")),
                    "native_id": s.get("native_id"),
                    "cwd": s.get("cwd"),
                    "model": s.get("model"),
                    "profile": s.get("profile"),
                    "cost_usd": cost_usd,
                    "cost_complete": s.get("cost_complete"),
                    "task": s.get("task"),
                    "waiting_question": bool(s.get("waiting_question")),
                    "created": s.get("created"),
                }
            )

    if src_filter in (None, "native"):
        for n in snapshot.get("native_sessions", []):
            rt = normalize_runtime(n.get("runtime"))
            if rt_filter and rt != rt_filter:
                continue
            if host_id and n.get("host_id", "local") != host_id:
                continue
            if not matches_native_status(n, st_filter):
                continue
            obs = str(n.get("observed") or n.get("status") or "active")
            items.append(
                {
                    "id": n.get("id"),
                    "source": "native",
                    "managed": False,
                    "runtime": rt or n.get("runtime"),
                    "display_runtime": display_runtime(rt or n.get("runtime")),
                    "name": n.get("name"),
                    "status": obs,
                    "mode": n.get("mode"),
                    "kind": n.get("kind"),
                    "host_id": n.get("host_id", "local"),
                    "pid": n.get("pid"),
                    "alive": True,
                    "native_id": n.get("native_id"),
                    "native_ids": n.get("native_ids") or ([n["native_id"]] if n.get("native_id") else []),
                    "cwd": n.get("cwd"),
                    "model": n.get("model"),
                    "profile": n.get("profile"),
                    "cost_usd": None,
                    "task": None,
                    "waiting_question": False,
                    "created": None,
                }
            )

    items.sort(key=_unified_sort_key)
    if limit is not None and limit >= 0:
        return items[start : start + limit]
    return items[start:]


def filter_snapshot_dict(
    snapshot: dict[str, Any],
    *,
    runtime: str | None = None,
    source: str | None = None,
    status: str | None = None,
    session_limit: int | None = None,
    after_seq: int | None = None,
    event_limit: int | None = None,
) -> dict[str, Any]:
    """Apply consistent runtime, source, status, limit, and cursor filtering to a snapshot dict."""
    rt_filter = parse_runtime_filter(runtime)
    src_filter = source.lower() if source and source.lower() != "all" else None
    if src_filter is not None and src_filter not in {"bridge", "native"}:
        raise ValueError(f"Unsupported source {source!r}")
    st_filter = normalize_status_filter(status)

    sessions = list(snapshot.get("sessions") or [])
    native_sessions = list(snapshot.get("native_sessions") or [])
    events = list(snapshot.get("events") or [])
    metrics = dict(snapshot.get("metrics") or {})

    if rt_filter:
        sessions = [s for s in sessions if normalize_runtime(s.get("runtime")) == rt_filter]
        native_sessions = [n for n in native_sessions if normalize_runtime(n.get("runtime")) == rt_filter]
        events = [e for e in events if normalize_runtime(e.get("runtime")) == rt_filter]

    if src_filter == "bridge":
        native_sessions = []
    elif src_filter == "native":
        sessions = []
        events = []

    if st_filter:
        sessions = [s for s in sessions if matches_bridge_status(s, st_filter)]
        native_sessions = [n for n in native_sessions if matches_native_status(n, st_filter)]
        matching_session_ids = {s.get("id") for s in sessions if s.get("id")}
        events = [e for e in events if e.get("session_id") in matching_session_ids]

    if after_seq is not None:
        events = [e for e in events if int(e.get("seq", 0)) > after_seq]
        events.sort(key=lambda item: int(item.get("seq", 0)))

    if event_limit is not None and event_limit >= 0:
        events = events[:event_limit]

    if session_limit is not None and session_limit >= 0:
        sessions = sessions[:session_limit]
        native_sessions = native_sessions[:session_limit]

    result = dict(snapshot)
    result["sessions"] = sessions
    result["native_sessions"] = native_sessions
    result["events"] = events
    result["metrics"] = metrics
    return result


def is_event_window_truncated(
    raw_snapshot: dict[str, Any] | Any,
    post_events: list[dict[str, Any]],
    *,
    req_limit: int,
    wide_limit: int,
    has_filter: bool,
    source: str | None = None,
    include_events: bool = True,
) -> bool:
    """Detect whether a collector's event window truncated or dropped matching/older events."""
    if not include_events or source == "native" or not isinstance(raw_snapshot, dict):
        return False
    if "events_truncated" in raw_snapshot:
        return bool(raw_snapshot.get("events_truncated"))
    pre_events = list(raw_snapshot.get("events") or [])
    raw_ver = int(raw_snapshot.get("collector_version") or 0)
    raw_latest = int((raw_snapshot.get("metrics") or {}).get("latest_event_seq") or 0)
    valid_seqs = {
        int(ev.get("seq", 0))
        for ev in pre_events
        if 1 <= int(ev.get("seq", 0)) <= raw_latest
    }
    compat_ignored_limit = (
        raw_ver < 3
        and raw_latest > 0
        and (not pre_events or len(valid_seqs) != raw_latest)
    )
    hit_limit = len(pre_events) >= int(wide_limit) and (
        raw_ver < 3 or (has_filter and len(post_events) < len(pre_events))
    )
    return bool(hit_limit or compat_ignored_limit)


def should_widen_events(
    raw_snapshot: dict[str, Any] | Any,
    post_events: list[dict[str, Any]],
    *,
    req_limit: int,
    max_limit: int,
    has_filter: bool,
    source: str | None = None,
    include_events: bool = True,
) -> bool:
    """Return True when re-collecting at max_limit may uncover masked or unpaginated events."""
    if not include_events or source == "native" or not isinstance(raw_snapshot, dict):
        return False
    if "events_truncated" in raw_snapshot or int(req_limit) >= int(max_limit):
        return False
    return is_event_window_truncated(
        raw_snapshot,
        post_events,
        req_limit=req_limit,
        wide_limit=req_limit,
        has_filter=has_filter,
        source=source,
        include_events=include_events,
    )
