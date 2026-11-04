"""Monitor collector and terminal dashboard for cli-agent-bridge."""
from __future__ import annotations

import json
import os
import re
import shutil
import socket
import sqlite3
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config import state_directory
from .hosts import ConfigHostRegistry
from .node_identity import FileNodeIdentityStore


def _is_pid_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _format_relative_time(epoch: float | None) -> str:
    if not epoch:
        return "-"
    delta = time.time() - epoch
    if delta < 0:
        return "now"
    if delta < 60:
        return f"{int(delta)}s ago"
    if delta < 3600:
        return f"{int(delta // 60)}m ago"
    if delta < 86400:
        return f"{int(delta // 3600)}h ago"
    return f"{int(delta // 86400)}d ago"


def _format_bytes(size: int | float | None) -> str:
    if size is None:
        return "-"
    size = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TB"


@dataclass
class MonitorSnapshot:
    timestamp: float
    coordinator: dict[str, Any]
    hosts: list[dict[str, Any]]
    sessions: list[dict[str, Any]]
    pending_questions: list[dict[str, Any]]
    groups: list[dict[str, Any]]
    broadcasts: list[dict[str, Any]]
    peer_queues: dict[str, Any]
    events: list[dict[str, Any]]
    metrics: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def derive_session_name(
    sid: str,
    profile_name: str = "",
    instruction: str | None = None,
    explicit_name: str | None = None,
    group_label: str | None = None,
) -> str:
    """Derive a human-readable, recognizable name for an agent session."""
    if explicit_name and explicit_name.strip():
        return explicit_name.strip()
    if group_label and group_label.strip():
        return group_label.strip()
    if not instruction or not instruction.strip():
        short_prof = profile_name.split("-")[0] if "-" in profile_name else (profile_name or "agent")
        return f"{short_prof} #{sid[:6]}"

    # 1. Recognized architectural phases
    if re.search(r"Phase\s*B", instruction, re.I):
        return "Phase B 跨机派发"
    if re.search(r"Phase\s*A3", instruction, re.I) or "SshPeerTransport" in instruction:
        return "Phase A3 管道与中继"
    if re.search(r"Phase\s*A2", instruction, re.I) or "HostRegistry" in instruction:
        return "Phase A2 节点配置"
    if re.search(r"Phase\s*A1", instruction, re.I) or "NodeIdentityStore" in instruction:
        return "Phase A1 节点身份"
    if re.search(r"Phase\s*A", instruction, re.I):
        return "Phase A 多机底座"

    # 2. Structured operation_id at line start (e.g. "operation_id: language-t10-cpu-conformance")
    m_op = re.search(r"(?:^|\n)\s*(?:role:\s*[^;\n]+;\s*)?operation_id:\s*([a-zA-Z0-9_\-]+)", instruction)
    if m_op and m_op.group(1).lower() != "str":
        op = m_op.group(1)
        # Strip trailing date suffixes like -20261104-01
        op_clean = re.sub(r"-\d{8}(-[a-z0-9]+)?$", "", op)
        return op_clean

    # 3. Explicit goal
    m_goal = re.search(r"(?:^|\n)\s*goal:\s*([^\n]+)", instruction)
    if m_goal:
        return m_goal.group(1)[:28].strip()

    first_line = instruction.strip().split("\n")[0][:28].strip()
    return first_line or f"{profile_name} #{sid[:6]}"


class MonitorCollector:
    """Collects multi-agent status, communications, and cluster health directly from local store."""

    def __init__(self, state: Path | str | None = None) -> None:
        self.state = state_directory(state)
        self.db_path = self.state / "bridge.sqlite"

    def _open_db(self) -> sqlite3.Connection | None:
        if not self.db_path.exists():
            return None
        try:
            db = sqlite3.connect(f"file:{self.db_path.resolve()}?mode=ro", uri=True, timeout=2.0)
            db.row_factory = sqlite3.Row
            return db
        except sqlite3.Error:
            try:
                db = sqlite3.connect(str(self.db_path), timeout=2.0)
                db.row_factory = sqlite3.Row
                return db
            except sqlite3.Error:
                return None

    def _has_table(self, db: sqlite3.Connection, table_name: str) -> bool:
        cur = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cur.fetchone() is not None

    def collect(self, limit: int = 15, include_events: bool = True) -> dict[str, Any]:
        now = time.time()
        hostname = socket.gethostname()

        # Coordinator identity
        coord_identity: dict[str, Any] = {
            "host_id": hostname,
            "node_id": "unknown",
            "state_generation": "unknown",
            "node_state": "ready",
            "state_dir": str(self.state),
            "db_path": str(self.db_path),
            "db_size": self.db_path.stat().st_size if self.db_path.exists() else 0,
        }
        try:
            store = FileNodeIdentityStore(self.state)
            ident = store.load_or_create()
            coord_identity["node_id"] = ident.node_id
            coord_identity["state_generation"] = ident.state_generation
            coord_identity["node_state"] = ident.state.value
        except Exception:
            pass

        # Hosts from registry
        hosts: list[dict[str, Any]] = []
        try:
            registry = ConfigHostRegistry(self.state)
            hosts = registry.status()
        except Exception:
            pass

        db = self._open_db()
        sessions: list[dict[str, Any]] = []
        pending_questions: list[dict[str, Any]] = []
        groups: list[dict[str, Any]] = []
        broadcasts: list[dict[str, Any]] = []
        peer_queues: dict[str, Any] = {"inbox": {}, "outbox": {}, "operations": {}}
        events: list[dict[str, Any]] = []
        metrics: dict[str, Any] = {
            "total_sessions": 0,
            "active_sessions": 0,
            "running_tasks": 0,
            "waiting_questions": 0,
            "total_broadcasts": 0,
            "total_cost_usd": 0.0,
        }

        if db is not None:
            try:
                # 1. Pending Questions / Approvals
                if self._has_table(db, "questions"):
                    cur = db.execute(
                        "SELECT id, session_id, task_id, attempt_id, data, status, created "
                        "FROM questions WHERE status='pending' ORDER BY created ASC"
                    )
                    for row in cur.fetchall():
                        raw_data = row["data"]
                        parsed = {}
                        if isinstance(raw_data, str):
                            try:
                                parsed = json.loads(raw_data)
                            except Exception:
                                pass
                        inp = parsed.get("input", {}) if isinstance(parsed.get("input"), dict) else {}
                        pending_questions.append(
                            {
                                "id": row["id"],
                                "session_id": row["session_id"],
                                "task_id": row["task_id"],
                                "attempt_id": row["attempt_id"],
                                "type": parsed.get("type", "unknown"),
                                "tool": parsed.get("tool", inp.get("tool", "-")),
                                "command": inp.get("command"),
                                "description": inp.get("description"),
                                "created": row["created"],
                                "created_rel": _format_relative_time(row["created"]),
                            }
                        )

                # 2. Sessions & Active Tasks
                custom_names: dict[str, str] = {}
                if self._has_table(db, "session_names"):
                    c_cur = db.execute("SELECT session_id, name FROM session_names")
                    custom_names = {r[0]: r[1] for r in c_cur.fetchall()}

                group_labels: dict[str, str] = {}
                if self._has_table(db, "native_group_members"):
                    g_cur = db.execute("SELECT native_id, label FROM native_group_members")
                    for r in g_cur.fetchall():
                        if r[0] and r[1]:
                            group_labels[r[0]] = r[1]

                session_names_map: dict[str, str] = {}

                if self._has_table(db, "sessions"):
                    cur = db.execute(
                        "SELECT id, profile, cwd, access, parent, native_id, desired, observed, "
                        "revision, worker_pid, worker_start, generation, error, created "
                        "FROM sessions ORDER BY created DESC"
                    )
                    question_sessions = {q["session_id"] for q in pending_questions}
                    for row in cur.fetchall():
                        sid = row["id"]
                        worker_pid = row["worker_pid"]
                        alive = _is_pid_alive(worker_pid)
                        profile_raw = row["profile"]
                        profile_name = profile_raw
                        runtime = ""
                        model_name = ""
                        if profile_raw and profile_raw.startswith("{"):
                            try:
                                p_data = json.loads(profile_raw)
                                profile_name = p_data.get("name", profile_name)
                                model_name = p_data.get("model", "")
                                runtime = p_data.get("runtime", "")
                            except Exception:
                                pass
                        if not runtime:
                            runtime = profile_name.split("-")[0] if "-" in profile_name else profile_name

                        # Join latest task
                        task_info = None
                        if self._has_table(db, "tasks"):
                            t_cur = db.execute(
                                "SELECT id, instruction, status, validation, reservation, created "
                                "FROM tasks WHERE session_id=? ORDER BY created DESC LIMIT 1",
                                (sid,),
                            )
                            t_row = t_cur.fetchone()
                            if t_row:
                                task_info = {
                                    "id": t_row["id"],
                                    "instruction": t_row["instruction"][:80],
                                    "status": t_row["status"],
                                    "validation": t_row["validation"],
                                    "reservation": t_row["reservation"],
                                    "created": t_row["created"],
                                }

                        # Join latest attempt for cost
                        attempt_info = None
                        cost_usd = 0.0
                        if task_info and self._has_table(db, "attempts"):
                            a_cur = db.execute(
                                "SELECT id, status, usage, started, finished "
                                "FROM attempts WHERE task_id=? ORDER BY started DESC LIMIT 1",
                                (task_info["id"],),
                            )
                            a_row = a_cur.fetchone()
                            if a_row:
                                raw_usage = a_row["usage"]
                                if raw_usage and isinstance(raw_usage, str):
                                    try:
                                        usage_dict = json.loads(raw_usage)
                                        cost_usd = float(usage_dict.get("reported_session_cost") or 0.0)
                                    except Exception:
                                        pass
                                attempt_info = {
                                    "id": a_row["id"],
                                    "status": a_row["status"],
                                    "cost_usd": cost_usd,
                                    "started": a_row["started"],
                                    "finished": a_row["finished"],
                                }
                                metrics["total_cost_usd"] += cost_usd

                        observed = row["observed"]
                        if sid in question_sessions:
                            observed = "waiting_input"

                        if observed in ("running", "idle", "waiting_input") or alive:
                            metrics["active_sessions"] += 1
                        if task_info and task_info["status"] == "running":
                            metrics["running_tasks"] += 1

                        native_id = row["native_id"]
                        inst = task_info["instruction"] if task_info else ""
                        s_name = derive_session_name(
                            sid=sid,
                            profile_name=profile_name,
                            instruction=inst,
                            explicit_name=custom_names.get(sid),
                            group_label=group_labels.get(native_id) if native_id else None,
                        )
                        session_names_map[sid] = s_name

                        sessions.append(
                            {
                                "id": sid,
                                "name": s_name,
                                "profile": profile_name,
                                "model": model_name,
                                "runtime": runtime,
                                "desired": row["desired"],
                                "observed": observed,
                                "worker_pid": worker_pid,
                                "alive": alive,
                                "revision": row["revision"],
                                "created": row["created"],
                                "created_rel": _format_relative_time(row["created"]),
                                "task": task_info,
                                "attempt": attempt_info,
                                "waiting_question": sid in question_sessions,
                            }
                        )

                for q in pending_questions:
                    q["session_name"] = session_names_map.get(q["session_id"], q["session_id"][:8])

                metrics["total_sessions"] = len(sessions)
                metrics["waiting_questions"] = len(pending_questions)

                # 3. Groups
                if self._has_table(db, "session_groups"):
                    cur = db.execute("SELECT id, revision, created FROM session_groups WHERE deleted=0")
                    for row in cur.fetchall():
                        gid = row["id"]
                        b_members = 0
                        n_members = 0
                        if self._has_table(db, "session_group_members"):
                            b_members = db.execute(
                                "SELECT count(*) FROM session_group_members WHERE group_id=?", (gid,)
                            ).fetchone()[0]
                        if self._has_table(db, "native_group_members"):
                            n_members = db.execute(
                                "SELECT count(*) FROM native_group_members WHERE group_id=?", (gid,)
                            ).fetchone()[0]
                        groups.append(
                            {
                                "id": gid,
                                "revision": row["revision"],
                                "created": row["created"],
                                "created_rel": _format_relative_time(row["created"]),
                                "bridge_members": b_members,
                                "native_members": n_members,
                                "total_members": b_members + n_members,
                            }
                        )

                # 4. Broadcasts
                if self._has_table(db, "native_broadcast_deliveries"):
                    cur = db.execute(
                        "SELECT broadcast_id, group_id, message, min(created) as created, count(*) as total, "
                        "sum(case when status='pending' then 1 else 0 end) as pending_cnt, "
                        "sum(case when status='sending' then 1 else 0 end) as sending_cnt, "
                        "sum(case when status='received' then 1 else 0 end) as received_cnt, "
                        "sum(case when status='uncertain' then 1 else 0 end) as uncertain_cnt, "
                        "sum(case when status='failed' then 1 else 0 end) as failed_cnt "
                        "FROM native_broadcast_deliveries GROUP BY broadcast_id ORDER BY min(created) DESC LIMIT 10"
                    )
                    for row in cur.fetchall():
                        broadcasts.append(
                            {
                                "broadcast_id": row["broadcast_id"],
                                "group_id": row["group_id"],
                                "message_preview": (row["message"] or "")[:60],
                                "total": row["total"],
                                "pending": row["pending_cnt"],
                                "sending": row["sending_cnt"],
                                "received": row["received_cnt"],
                                "uncertain": row["uncertain_cnt"],
                                "failed": row["failed_cnt"],
                                "created": row["created"],
                                "created_rel": _format_relative_time(row["created"]),
                            }
                        )
                    metrics["total_broadcasts"] = len(broadcasts)

                # 5. Peer queues (Phase B/C)
                if self._has_table(db, "peer_inbox"):
                    cur = db.execute("SELECT status, count(*) FROM peer_inbox GROUP BY status")
                    peer_queues["inbox"] = {r[0]: r[1] for r in cur.fetchall()}
                if self._has_table(db, "node_outbox"):
                    cur = db.execute("SELECT status, count(*) FROM node_outbox GROUP BY status")
                    peer_queues["outbox"] = {r[0]: r[1] for r in cur.fetchall()}
                if self._has_table(db, "peer_operations"):
                    cur = db.execute("SELECT status, count(*) FROM peer_operations GROUP BY status")
                    peer_queues["operations"] = {r[0]: r[1] for r in cur.fetchall()}

                # 6. Events
                if include_events and self._has_table(db, "events"):
                    cur = db.execute(
                        "SELECT seq, session_id, task_id, attempt_id, kind, data, created "
                        "FROM events ORDER BY seq DESC LIMIT ?",
                        (limit,),
                    )
                    for row in cur.fetchall():
                        raw_data = row["data"]
                        parsed = {}
                        if isinstance(raw_data, str):
                            try:
                                parsed = json.loads(raw_data)
                            except Exception:
                                pass
                        summary = ""
                        kind = row["kind"]
                        if kind == "tool_started":
                            summary = f"{parsed.get('tool', 'tool')}: {str(parsed.get('command') or '')[:50]}"
                        elif kind == "tool_completed":
                            is_err = parsed.get("is_error")
                            summary = f"tool done (error={is_err})" if is_err else "tool done (ok)"
                        elif kind == "input_required":
                            summary = f"approval needed: {parsed.get('tool')}"
                        elif kind == "task_finished":
                            summary = f"status={parsed.get('status')}"
                        elif kind in ("task_queued", "attempt_started"):
                            summary = parsed.get("instruction", "")[:40] if "instruction" in parsed else ""

                        events.append(
                            {
                                "seq": row["seq"],
                                "session_id": row["session_id"],
                                "session_name": session_names_map.get(
                                    row["session_id"], row["session_id"][:8] if row["session_id"] else "-"
                                ),
                                "task_id": row["task_id"],
                                "kind": kind,
                                "summary": summary,
                                "created": row["created"],
                                "created_rel": _format_relative_time(row["created"]),
                            }
                        )

            finally:
                db.close()

        metrics["total_cost_usd"] = round(metrics["total_cost_usd"], 4)

        return MonitorSnapshot(
            timestamp=now,
            coordinator=coord_identity,
            hosts=hosts,
            sessions=sessions,
            pending_questions=pending_questions,
            groups=groups,
            broadcasts=broadcasts,
            peer_queues=peer_queues,
            events=events,
            metrics=metrics,
        ).as_dict()


# ---------------------------------------------------------------- TUI Renderer

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
RED = "\033[31m"
WHITE = "\033[37m"
BG_YELLOW = "\033[43;30m"
BG_RED = "\033[41;37m"
CLEAR_SCREEN = "\033[2J\033[H"


def render_tui(snapshot: dict[str, Any], width: int = 100) -> str:
    lines: list[str] = []
    width = max(80, width)

    metrics = snapshot["metrics"]
    coord = snapshot["coordinator"]
    now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(snapshot["timestamp"]))

    # 1. Header Banner
    title = " cli-agent-bridge Monitor "
    border_len = (width - len(title) - 2) // 2
    header_bar = f"{CYAN}┌{'─' * border_len}{BOLD}{WHITE}{title}{RESET}{CYAN}{'─' * (width - border_len - len(title) - 2)}┐{RESET}"
    lines.append(header_bar)

    coord_info = (
        f"{CYAN}│{RESET} Coordinator: {BOLD}{coord.get('host_id')}{RESET} | "
        f"Node: {DIM}{coord.get('node_id', '')[:8]}...{RESET} | "
        f"DB: {DIM}{_format_bytes(coord.get('db_size'))}{RESET} | "
        f"Time: {now_str}"
    )
    # Pad to width
    plain_len = len(f" Coordinator: {coord.get('host_id')} | Node: {coord.get('node_id', '')[:8]}... | DB: {_format_bytes(coord.get('db_size'))} | Time: {now_str}")
    pad = max(0, width - plain_len - 2)
    lines.append(f"{coord_info}{' ' * pad}{CYAN}│{RESET}")

    metrics_str = (
        f" Active Agents: {GREEN}{BOLD}{metrics.get('active_sessions', 0)}{RESET}/{metrics.get('total_sessions', 0)} | "
        f"Running Tasks: {BLUE}{BOLD}{metrics.get('running_tasks', 0)}{RESET} | "
        f"Pending Approvals: {RED if metrics.get('waiting_questions') else GREEN}{BOLD}{metrics.get('waiting_questions', 0)}{RESET} | "
        f"Cost: {YELLOW}${metrics.get('total_cost_usd', 0.0):.4f}{RESET}"
    )
    plain_m = f" Active Agents: {metrics.get('active_sessions', 0)}/{metrics.get('total_sessions', 0)} | Running Tasks: {metrics.get('running_tasks', 0)} | Pending Approvals: {metrics.get('waiting_questions', 0)} | Cost: ${metrics.get('total_cost_usd', 0.0):.4f}"
    pad_m = max(0, width - len(plain_m) - 2)
    lines.append(f"{CYAN}│{RESET}{metrics_str}{' ' * pad_m}{CYAN}│{RESET}")
    lines.append(f"{CYAN}└{'─' * (width - 2)}┘{RESET}")

    # 2. Urgent Banner: Pending Approvals
    pending_q = snapshot.get("pending_questions", [])
    if pending_q:
        lines.append(f"\n{BG_YELLOW}{BOLD} ⚠ ACTION REQUIRED: {len(pending_q)} PENDING APPROVAL(S) {RESET}")
        for q in pending_q:
            s_display = q.get("session_name") or q["session_id"][:8]
            lines.append(
                f"  • {BOLD}Question [{q['id'][:8]}]{RESET} for {CYAN}{s_display}{RESET} ({DIM}{q['session_id'][:8]}{RESET}): "
                f"Tool={BOLD}{q['tool']}{RESET} | Command={YELLOW}{q.get('command') or q.get('description') or '-'}{RESET} "
                f"({DIM}{q['created_rel']}{RESET})"
            )
            lines.append(f"    {DIM}↳ Approve with: agent_control action=respond question_id={q['id']} answer={{\"decision\":\"allow\"}}{RESET}")

    # 3. Cluster Nodes Section
    hosts = snapshot.get("hosts", [])
    lines.append(f"\n{BOLD}{CYAN}▶ Cluster Nodes ({len(hosts)}){RESET}")
    lines.append(f"{DIM}  {'Host ID':<18} {'State':<12} {'Target':<22} {'Node ID':<18} {'Last Contact'}{RESET}")
    lines.append(f"  {DIM}{'─' * 18} {'─' * 12} {'─' * 22} {'─' * 18} {'─' * 15}{RESET}")
    if not hosts:
        lines.append(f"  {DIM}(Only local coordinator configured; add hosts in config.json to monitor remote servers){RESET}")
    else:
        for h in hosts:
            state = h.get("state", "offline")
            st_color = GREEN if state == "online" else (YELLOW if state == "offline" else RED)
            lines.append(
                f"  {BOLD}{h.get('host_id', '-'):<18}{RESET} "
                f"{st_color}{state:<12}{RESET} "
                f"{h.get('ssh_target', '-'):<22} "
                f"{DIM}{h.get('node_id', '-')[:16]:<18}{RESET} "
                f"{_format_relative_time(h.get('last_contact'))}"
            )

    # 4. Active Agents & Sessions Section
    sessions = snapshot.get("sessions", [])
    lines.append(f"\n{BOLD}{CYAN}▶ Agents & Sessions ({len(sessions)}){RESET}")
    lines.append(f"{DIM}  {'Agent / Session Name':<30} {'Runtime':<9} {'Model/Profile':<16} {'Status':<14} {'PID':<8} {'Cost':<7} {'ID':<10}{RESET}")
    lines.append(f"  {DIM}{'─' * 30} {'─' * 9} {'─' * 16} {'─' * 14} {'─' * 8} {'─' * 7} {'─' * 10}{RESET}")
    if not sessions:
        lines.append(f"  {DIM}(No sessions created yet){RESET}")
    else:
        for s in sessions[:12]:
            obs = s.get("observed", "stopped")
            if s.get("waiting_question"):
                status_str = f"{BG_YELLOW}{BOLD}WAIT_INPUT{RESET}"
            elif obs == "running":
                status_str = f"{GREEN}{BOLD}RUNNING{RESET}"
            elif obs == "idle":
                status_str = f"{BLUE}IDLE{RESET}"
            elif obs == "waiting_input":
                status_str = f"{YELLOW}WAIT_INPUT{RESET}"
            else:
                status_str = f"{DIM}{obs}{RESET}"

            pid_str = str(s.get("worker_pid") or "-")
            if s.get("alive"):
                pid_str = f"{GREEN}{pid_str}●{RESET}"

            cost = f"${s['attempt']['cost_usd']:.3f}" if s.get("attempt") and s["attempt"].get("cost_usd") is not None else "-"
            s_name = s.get("name") or s["id"][:8]

            lines.append(
                f"  {BOLD}{s_name[:28]:<30}{RESET} "
                f"{CYAN}{s['runtime']:<9}{RESET} "
                f"{s['profile'][:14]:<16} "
                f"{status_str:<22} "
                f"{pid_str:<16} "
                f"{cost:<7} "
                f"{DIM}{s['id'][:8]}{RESET}"
            )

    # 5. Communications & Broadcasts Section
    broadcasts = snapshot.get("broadcasts", [])
    groups = snapshot.get("groups", [])
    lines.append(f"\n{BOLD}{CYAN}▶ Communications & Groups ({len(groups)} groups, {len(broadcasts)} broadcasts){RESET}")
    if broadcasts:
        lines.append(f"{DIM}  {'Broadcast ID':<16} {'Group':<14} {'Progress':<18} {'Message Preview'}{RESET}")
        lines.append(f"  {DIM}{'─' * 16} {'─' * 14} {'─' * 18} {'─' * 40}{RESET}")
        for b in broadcasts[:5]:
            prog = f"{GREEN}{b['received']}{RESET}/{b['total']} recv"
            if b.get("uncertain"):
                prog += f" {RED}({b['uncertain']} uncert){RESET}"
            lines.append(
                f"  {b['broadcast_id'][:14]:<16} "
                f"{b['group_id'][:12]:<14} "
                f"{prog:<26} "
                f"{b['message_preview'][:40]}"
            )
    else:
        lines.append(f"  {DIM}(No broadcast activity recorded){RESET}")

    # 6. Recent Event Tail
    events = snapshot.get("events", [])
    lines.append(f"\n{BOLD}{CYAN}▶ Live Events (Recent {len(events)}){RESET}")
    lines.append(f"{DIM}  {'Seq':<6} {'Time':<10} {'Kind':<18} {'Session':<16} {'Detail / Summary'}{RESET}")
    lines.append(f"  {DIM}{'─' * 6} {'─' * 10} {'─' * 18} {'─' * 16} {'─' * 45}{RESET}")
    if not events:
        lines.append(f"  {DIM}(No events yet){RESET}")
    else:
        for e in events[:8]:
            kind = e["kind"]
            k_color = GREEN if "finish" in kind or "complete" in kind else (YELLOW if "input" in kind or "wait" in kind else BLUE)
            s_name = e.get("session_name") or (e.get("session_id") or "-")[:14]
            lines.append(
                f"  {DIM}{e['seq']:<6}{RESET} "
                f"{e['created_rel']:<10} "
                f"{k_color}{kind:<18}{RESET} "
                f"{CYAN}{s_name[:14]:<16}{RESET} "
                f"{e.get('summary', '')[:45]}"
            )

    return "\n".join(lines)


def run_tui_monitor(
    state: Path | str | None = None,
    interval: float = 2.0,
    once: bool = False,
    output_format: str = "text",
) -> None:
    collector = MonitorCollector(state)
    if once:
        snapshot = collector.collect(limit=20)
        if output_format == "json":
            print(json.dumps(snapshot, indent=2))
        else:
            term_width = shutil.get_terminal_size((100, 30)).columns
            print(render_tui(snapshot, width=term_width))
        return

    # Interactive live loop
    print(CLEAR_SCREEN, end="", flush=True)
    try:
        while True:
            term_width = shutil.get_terminal_size((100, 30)).columns
            snapshot = collector.collect(limit=15)
            output = render_tui(snapshot, width=term_width)
            # Home cursor and redraw
            print("\033[H" + output + f"\n\n{DIM}[Press Ctrl+C to exit monitor | Refreshing every {interval}s]{RESET}\033[J", flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n{DIM}Monitor stopped.{RESET}")


def set_session_name(state: Path | str | None, session_id: str, name: str) -> str:
    """Explicitly assign a recognizable human name or alias to an agent session."""
    state_dir = state_directory(state)
    db_path = state_dir / "bridge.sqlite"
    if not db_path.exists():
        raise ValueError(f"State database not found: {db_path}")

    # Resolve session_id if given as prefix
    with sqlite3.connect(str(db_path)) as db:
        cur = db.execute(
            "SELECT id FROM sessions WHERE id = ? OR id LIKE ? ORDER BY created DESC",
            (session_id, f"{session_id}%"),
        )
        rows = cur.fetchall()
        if not rows:
            raise ValueError(f"Session not found matching: {session_id}")
        full_sid = rows[0][0]

        db.execute(
            "CREATE TABLE IF NOT EXISTS session_names ("
            "  session_id TEXT PRIMARY KEY, name TEXT NOT NULL, created REAL NOT NULL"
            ")"
        )
        db.execute(
            "INSERT INTO session_names(session_id, name, created) VALUES(?, ?, ?) "
            "ON CONFLICT(session_id) DO UPDATE SET name=excluded.name",
            (full_sid, name.strip(), time.time()),
        )
        return full_sid
