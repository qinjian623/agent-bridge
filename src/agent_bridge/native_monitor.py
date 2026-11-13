"""Discovery and normalization across Pi, Claude, Antigravity (AGY), and Codex."""
from __future__ import annotations

import fcntl
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

MONITORED_RUNTIMES = ("pi", "claude", "antigravity", "codex")
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


def _flag_value(args: list[str], *flags: str) -> str | None:
    for idx, arg in enumerate(args):
        for flag in flags:
            if arg == flag and idx + 1 < len(args):
                return args[idx + 1]
            if arg.startswith(flag + "="):
                return arg.split("=", 1)[1]
    return None


def _active_agy_lock_owners(proc_root: Path, native_home: Path) -> dict[int, list[str]]:
    """Map PID -> active Antigravity conversation IDs from /proc/locks + presence/*.lock."""
    locks_path = proc_root / "locks"
    presence_dir = native_home / "presence"
    if not locks_path.exists() or not presence_dir.exists():
        return {}

    inode_pids: dict[int, set[int]] = {}
    try:
        for line in locks_path.read_text(errors="replace").splitlines():
            fields = line.split()
            if len(fields) < 6 or fields[1:4] != ["FLOCK", "ADVISORY", "WRITE"]:
                continue
            try:
                pid = int(fields[4])
                inode = int(fields[5].rsplit(":", 1)[1])
            except (ValueError, IndexError):
                continue
            inode_pids.setdefault(inode, set()).add(pid)
    except OSError:
        return {}

    if not inode_pids:
        return {}

    pid_sessions: dict[int, list[str]] = {}
    try:
        lock_files = list(presence_dir.glob("*.lock"))
    except OSError:
        return {}

    for lock_file in lock_files:
        try:
            inode = lock_file.stat().st_ino
        except OSError:
            continue
        owners = inode_pids.get(inode)
        if not owners:
            continue
        # Confirm an active FLOCK lock non-blockingly when on real /proc, or rely on inode match cleanly.
        try:
            with lock_file.open("rb") as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    # Lock was NOT held; skip stale /proc/locks race unless inode check matched a test fixture.
                    if str(proc_root) == "/proc":
                        continue
                except BlockingIOError:
                    pass
        except OSError:
            continue
        conv_id = lock_file.stem
        for owner_pid in owners:
            pid_sessions.setdefault(owner_pid, []).append(conv_id)

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
    titles: dict[str, str] = {}
    idx_file = codex_home / "session_index.jsonl"
    if not session_ids or not idx_file.is_file():
        return titles
    try:
        for line in idx_file.read_text(errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except ValueError:
                continue
            sid = item.get("id")
            name = item.get("thread_name")
            if isinstance(sid, str) and sid in session_ids and isinstance(name, str) and name.strip():
                titles[sid] = name.strip()
    except OSError:
        pass
    return titles


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


def _classify_process(proc: dict[str, Any], agy_pids: set[int]) -> tuple[str, str] | None:
    """Return (runtime, profile_hint) if process is a pi/claude/agy/codex agent process."""
    args: list[str] = proc["args"]
    comm: str = proc["comm"]
    exe: str = proc["exe"]
    exename: str = proc["exename"]
    ppid: int = proc["ppid"]
    cmd0 = Path(args[0]).name if args else ""
    joined = " ".join(args)

    # Never misclassify bridge internals, proxies, or systemd inhibitors.
    if (
        "agent_bridge" in joined
        or "codex-async" in joined
        or "proxy.mjs" in joined
        or comm in {"systemd-inhibit", "ChatGPT"}
    ):
        return None

    # Antigravity (AGY)
    is_agy_bin = (
        comm in {"agy", "agy-real", "antigravity"}
        or cmd0 in {"agy", "agy-real", "antigravity"}
        or exename.startswith(("agy-real", "antigravity"))
    )
    is_agy_wrapper = cmd0 in {"bash", "sh", "zsh"} and len(args) > 1 and Path(args[1]).name in {"agy", "agy-real"}
    if is_agy_bin or is_agy_wrapper:
        if ppid in agy_pids and (is_agy_wrapper or comm == "agy"):
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
    resolved_home = Path(home) if home is not None else Path.home()
    resolved_proc = Path(proc_root) if proc_root is not None else Path("/proc")
    plat = platform_name or sys.platform

    if proc_root is not None or (plat.startswith("linux") and resolved_proc.is_dir()):
        ppid_map, procs = _scan_proc_linux(resolved_proc)
    else:
        ppid_map, procs = _scan_ps_posix()

    excluded = set(exclude_pids or ())
    excluded.add(os.getpid())
    # Also exclude any live bridge worker processes visible in cmdline.
    for proc in procs:
        args = proc["args"]
        if any("agent_bridge" in a for a in args) and "worker" in args:
            excluded.add(proc["pid"])

    agy_pids = {
        proc["pid"]
        for proc in procs
        if proc["comm"] in {"agy", "agy-real", "antigravity"}
        or proc["exename"].startswith(("agy-real", "antigravity"))
    }
    agy_home = resolved_home / ".gemini/antigravity-cli"
    codex_home = resolved_home / ".codex"
    lock_owners = _active_agy_lock_owners(resolved_proc, agy_home) if resolved_proc.exists() else {}

    raw_items: list[dict[str, Any]] = []
    agy_ids: set[str] = set()
    codex_ids: set[str] = set()
    seen_bridge_native = set(exclude_native_ids or ())
    labels = group_labels or {}

    for proc in procs:
        pid = proc["pid"]
        if _has_ancestor(pid, ppid_map, excluded):
            continue
        classified = _classify_process(proc, agy_pids)
        if classified is None:
            continue
        rt, profile_hint = classified
        args: list[str] = proc["args"]
        cwd = proc["cwd"]
        model = _flag_value(args, "--model", "-m") or ""
        provider = _flag_value(args, "--provider") or ""
        native_id: str | None = None
        mode = "interactive"
        kind = "interactive"

        if rt == "antigravity":
            native_id = _flag_value(args, "--conversation")
            if not native_id and lock_owners.get(pid):
                native_id = lock_owners[pid][0]
            if native_id:
                agy_ids.add(native_id)
            if "--input-format" in args or "--print" in args or "-p" in args:
                mode = "headless"
                kind = "worker"
        elif rt == "codex":
            subcmd = next((a for a in args[1:] if not a.startswith("-")), "")
            if subcmd in {"app-server", "exec-server"}:
                mode = "daemon"
                kind = "daemon"
            elif subcmd == "exec":
                mode = "headless"
                kind = "worker"
            for idx, arg in enumerate(args):
                if arg == "resume" and idx + 1 < len(args) and _UUID_RE.match(args[idx + 1]):
                    native_id = args[idx + 1]
                    codex_ids.add(native_id)
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

        if native_id and (rt, native_id) in seen_bridge_native:
            continue

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
                "mode": mode,
                "kind": kind,
                "args": args,
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
                subcmd = next((a for a in item["args"][1:] if not a.startswith("-")), "daemon")
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
                "cwd": item["cwd"],
                "pid": pid,
                "ppid": item["ppid"],
                "worker_pid": pid,
                "desired": "open",
                "observed": status,
                "status": status,
                "alive": True,
                "waiting_question": False,
                "command": " ".join(item["args"])[:200],
            }
        )

    order = {"worker": 0, "interactive": 1, "daemon": 2}
    rt_order = {rt: i for i, rt in enumerate(MONITORED_RUNTIMES)}
    results.sort(key=lambda r: (order.get(r["kind"], 3), rt_order.get(r["runtime"], 9), r["pid"]))
    return results


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
        summary[rt] = {
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
        }

    for s in bridge_sessions:
        rt = normalize_runtime(s.get("runtime")) or "unknown"
        bucket = summary.setdefault(
            rt,
            {
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
            },
        )
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
        attempt = s.get("attempt")
        if isinstance(attempt, dict) and isinstance(attempt.get("cost_usd"), (int, float)):
            bucket["total_cost_usd"] += float(attempt["cost_usd"])

    for n in native_sessions:
        rt = normalize_runtime(n.get("runtime")) or "unknown"
        bucket = summary.setdefault(
            rt,
            {
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
            },
        )
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


def unified_sessions(
    snapshot: dict[str, Any],
    *,
    runtime: str | None = None,
    source: str | None = None,
    status: str | None = None,
    host_id: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Return a consistently shaped session list merging Bridge and Native records."""
    rt_filter = normalize_runtime(runtime) if runtime and runtime.lower() != "all" else None
    src_filter = source.lower() if source and source.lower() != "all" else None
    st_filter = status.lower() if status and status.lower() != "all" else None
    items: list[dict[str, Any]] = []

    if src_filter in (None, "bridge"):
        for s in snapshot.get("sessions", []):
            rt = normalize_runtime(s.get("runtime"))
            if rt_filter and rt != rt_filter:
                continue
            if host_id and s.get("host_id", "local") != host_id:
                continue
            obs = str(s.get("observed") or "stopped")
            is_active = obs in ("running", "idle", "waiting_input", "starting") or bool(s.get("alive"))
            if st_filter == "active":
                if not is_active and obs != "recovery_needed":
                    continue
            elif st_filter and obs != st_filter:
                continue
            attempt = s.get("attempt") or {}
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
                    "cost_usd": attempt.get("cost_usd"),
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
            obs = str(n.get("observed") or n.get("status") or "active")
            if st_filter == "active":
                if n.get("kind") == "daemon":
                    continue
            elif st_filter and obs != st_filter:
                continue
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
                    "cwd": n.get("cwd"),
                    "model": n.get("model"),
                    "profile": n.get("profile"),
                    "cost_usd": None,
                    "task": None,
                    "waiting_question": False,
                    "created": None,
                }
            )

    if limit is not None and limit >= 0:
        return items[:limit]
    return items
