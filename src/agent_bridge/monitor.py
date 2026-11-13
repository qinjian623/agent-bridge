"""Monitor collector and terminal dashboard for cli-agent-bridge."""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import socket
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .config import state_directory
from .errors import BridgeError
from .hosts import ConfigHostRegistry
from .native_monitor import (
    MONITORED_RUNTIMES,
    collect_native_sessions,
    display_runtime,
    normalize_runtime,
    summarize_runtimes,
)
from .node_identity import FileNodeIdentityStore
from .store import Store


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
    native_sessions: list[dict[str, Any]] = field(default_factory=list)
    runtimes: dict[str, Any] = field(default_factory=dict)

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
    """Collects multi-agent status, communications, and cluster health across all runtimes."""

    def __init__(
        self,
        state: Path | str | None = None,
        *,
        host_registry=None,
        proc_root: Path | str | None = None,
        native_home: Path | str | None = None,
    ) -> None:
        self.state = state_directory(state)
        self.db_path = self.state / "bridge.sqlite"
        self.host_registry = host_registry
        self.proc_root = Path(proc_root) if proc_root is not None else None
        self.native_home = Path(native_home) if native_home is not None else None

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

    def collect(
        self,
        limit: int = 15,
        include_events: bool = True,
        *,
        runtime: str | None = None,
        source: str | None = None,
        status: str | None = None,
        session_limit: int | None = None,
        include_native: bool = True,
        after_seq: int | None = None,
    ) -> dict[str, Any]:
        filter_runtime_arg = runtime
        if self.host_registry is None and (self.state / "broker.sock").exists():
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                from .broker import request

                rpc_args: dict[str, Any] = {"limit": limit, "include_events": include_events}
                if runtime is not None:
                    rpc_args["runtime"] = runtime
                if source is not None:
                    rpc_args["source"] = source
                if status is not None:
                    rpc_args["status"] = status
                if session_limit is not None:
                    rpc_args["session_limit"] = session_limit
                if not include_native:
                    rpc_args["include_native"] = False
                if after_seq is not None:
                    rpc_args["after_seq"] = after_seq

                async def live_snapshot():
                    return await asyncio.wait_for(request(self.state, "agent_monitor", rpc_args), 2)

                try:
                    return asyncio.run(live_snapshot())
                except (BridgeError, OSError, ValueError, TimeoutError):
                    pass
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
            registry = self.host_registry if self.host_registry is not None else ConfigHostRegistry(self.state)
            for st in registry.status():
                host_id = st.host_id
                cfg = None
                try:
                    cfg = registry.get(host_id)
                except Exception:
                    pass
                node_id = st.node.node_id if st.node else None
                state_gen = st.node.state_generation if st.node else None
                state_val = st.state.value if hasattr(st.state, "value") else str(st.state)
                hosts.append(
                    {
                        "host_id": host_id,
                        "state": state_val,
                        "ssh_target": cfg.ssh_target if cfg else "-",
                        "remote_executable": cfg.remote_executable if cfg else "-",
                        "projects": dict(cfg.projects) if cfg and cfg.projects else {},
                        "node_id": node_id,
                        "state_generation": state_gen,
                        "last_contact": st.last_contact,
                        "last_contact_rel": _format_relative_time(st.last_contact),
                        "protocol_version": st.protocol_version,
                        "detail": st.detail,
                    }
                )
        except Exception:
            pass

        db = self._open_db()
        sessions: list[dict[str, Any]] = []
        pending_questions: list[dict[str, Any]] = []
        groups: list[dict[str, Any]] = []
        broadcasts: list[dict[str, Any]] = []
        peer_queues: dict[str, Any] = {"inbox": {}, "outbox": {}, "member_outbox": {}, "operations": {}}
        events: list[dict[str, Any]] = []
        native_group_labels: dict[tuple[str, str], str] = {}
        session_runtime_map: dict[str, str] = {}
        live_worker_pids: set[int] = set()
        live_bridge_native_ids: set[tuple[str, str]] = set()
        metrics: dict[str, Any] = {
            "total_hosts": len(hosts) + 1,
            "online_hosts": sum(1 for h in hosts if h.get("state") == "online") + 1,
            "total_sessions": 0,
            "active_sessions": 0,
            "running_tasks": 0,
            "waiting_questions": 0,
            "total_broadcasts": 0,
            "total_cost_usd": 0.0,
            "native_sessions": 0,
            "native_workers": 0,
            "native_daemons": 0,
            "total_all_active": 0,
            "schema_version": "1",
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
                native_group_labels: dict[tuple[str, str], str] = {}
                if self._has_table(db, "native_group_members"):
                    g_cur = db.execute("SELECT runtime, native_id, label FROM native_group_members")
                    for r in g_cur.fetchall():
                        if r[1] and r[2]:
                            group_labels[r[1]] = r[2]
                            rt_norm = normalize_runtime(r[0]) or str(r[0] or "")
                            native_group_labels[(rt_norm, r[1])] = r[2]

                session_hosts: dict[str, str] = {}
                if self._has_table(db, "peer_outbox"):
                    for r in db.execute(
                        "SELECT host_id, target_id FROM peer_outbox WHERE target_kind='bridge' AND target_id IS NOT NULL"
                    ).fetchall():
                        session_hosts[r["target_id"]] = r["host_id"]
                if self._has_table(db, "remote_group_members"):
                    for r in db.execute(
                        "SELECT host_id, target_id FROM remote_group_members WHERE target_kind='bridge' AND target_id IS NOT NULL"
                    ).fetchall():
                        session_hosts[r["target_id"]] = r["host_id"]

                session_names_map: dict[str, str] = {}
                session_runtime_map: dict[str, str] = {}
                live_worker_pids: set[int] = set()
                live_bridge_native_ids: set[tuple[str, str]] = set()

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
                        session_rt = ""
                        model_name = ""
                        if profile_raw and profile_raw.startswith("{"):
                            try:
                                p_data = json.loads(profile_raw)
                                profile_name = p_data.get("name", profile_name)
                                model_name = p_data.get("model", "")
                                session_rt = p_data.get("runtime", "")
                            except Exception:
                                pass
                        session_rt = normalize_runtime(session_rt) or normalize_runtime(profile_name) or (
                            profile_name.split("-")[0] if "-" in profile_name else profile_name
                        )

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

                        is_live = observed in ("running", "idle", "waiting_input", "starting") or alive
                        if observed in ("running", "idle", "waiting_input") or alive:
                            metrics["active_sessions"] += 1
                        if task_info and task_info["status"] == "running":
                            metrics["running_tasks"] += 1

                        native_id = row["native_id"]
                        if alive and isinstance(worker_pid, int) and worker_pid > 0:
                            live_worker_pids.add(worker_pid)
                        if is_live and native_id and session_rt:
                            live_bridge_native_ids.add((session_rt, native_id))

                        inst = task_info["instruction"] if task_info else ""
                        s_name = derive_session_name(
                            sid=sid,
                            profile_name=profile_name,
                            instruction=inst,
                            explicit_name=custom_names.get(sid),
                            group_label=group_labels.get(native_id) if native_id else None,
                        )
                        session_names_map[sid] = s_name
                        session_runtime_map[sid] = session_rt

                        sessions.append(
                            {
                                "id": sid,
                                "source": "bridge",
                                "managed": True,
                                "name": s_name,
                                "host_id": session_hosts.get(sid, "local"),
                                "profile": profile_name,
                                "model": model_name,
                                "runtime": session_rt,
                                "display_runtime": display_runtime(session_rt),
                                "cwd": row["cwd"],
                                "native_id": native_id,
                                "error": row["error"],
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

                    def _session_priority(item: dict[str, Any]) -> tuple[int, float]:
                        obs_val = item.get("observed")
                        if item.get("waiting_question") or obs_val == "waiting_input":
                            tier = 0
                        elif obs_val == "running":
                            tier = 1
                        elif obs_val == "recovery_needed":
                            tier = 2
                        elif obs_val in ("idle", "starting") or item.get("alive"):
                            tier = 3
                        else:
                            tier = 4
                        return (tier, -(item.get("created") or 0.0))

                    sessions.sort(key=_session_priority)

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
                        r_members = 0
                        remote_hosts: list[str] = []
                        if self._has_table(db, "session_group_members"):
                            b_members = db.execute(
                                "SELECT count(*) FROM session_group_members WHERE group_id=?", (gid,)
                            ).fetchone()[0]
                        if self._has_table(db, "native_group_members"):
                            n_members = db.execute(
                                "SELECT count(*) FROM native_group_members WHERE group_id=?", (gid,)
                            ).fetchone()[0]
                        if self._has_table(db, "remote_group_members"):
                            cur_r = db.execute(
                                "SELECT host_id, count(*) FROM remote_group_members WHERE group_id=? GROUP BY host_id",
                                (gid,),
                            )
                            for r_host, r_cnt in cur_r.fetchall():
                                r_members += r_cnt
                                remote_hosts.append(f"{r_host}:{r_cnt}")
                        groups.append(
                            {
                                "id": gid,
                                "revision": row["revision"],
                                "created": row["created"],
                                "created_rel": _format_relative_time(row["created"]),
                                "bridge_members": b_members,
                                "native_members": n_members,
                                "remote_members": r_members,
                                "remote_hosts": remote_hosts,
                                "total_members": b_members + n_members + r_members,
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
                if self._has_table(db, "peer_outbox"):
                    cur = db.execute("SELECT status, count(*) FROM peer_outbox GROUP BY status")
                    peer_queues["outbox"] = {r[0]: r[1] for r in cur.fetchall()}
                if self._has_table(db, "node_outbox"):
                    cur = db.execute("SELECT status, count(*) FROM node_outbox GROUP BY status")
                    peer_queues["member_outbox"] = {r[0]: r[1] for r in cur.fetchall()}
                if self._has_table(db, "peer_operations"):
                    cur = db.execute("SELECT status, count(*) FROM peer_operations GROUP BY status")
                    peer_queues["operations"] = {r[0]: r[1] for r in cur.fetchall()}

                # 6. Events
                if include_events and self._has_table(db, "events"):
                    if after_seq is not None:
                        cur = db.execute(
                            "SELECT seq, session_id, task_id, attempt_id, kind, data, created "
                            "FROM events WHERE seq > ? ORDER BY seq ASC LIMIT ?",
                            (int(after_seq), limit),
                        )
                    else:
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
                                "runtime": session_runtime_map.get(row["session_id"]),
                                "task_id": row["task_id"],
                                "kind": kind,
                                "summary": summary,
                                "created": row["created"],
                                "created_rel": _format_relative_time(row["created"]),
                            }
                        )

            finally:
                db.close()

        native_sessions: list[dict[str, Any]] = []
        if include_native:
            native_sessions = collect_native_sessions(
                proc_root=self.proc_root,
                home=self.native_home,
                exclude_pids=live_worker_pids,
                exclude_native_ids=live_bridge_native_ids,
                group_labels=native_group_labels,
            )

        runtimes = summarize_runtimes(sessions, native_sessions)
        metrics["total_cost_usd"] = round(metrics["total_cost_usd"], 4)
        metrics["native_sessions"] = sum(1 for n in native_sessions if n.get("kind") != "daemon")
        metrics["native_workers"] = sum(1 for n in native_sessions if n.get("kind") == "worker")
        metrics["native_daemons"] = sum(1 for n in native_sessions if n.get("kind") == "daemon")
        metrics["total_all_active"] = metrics["active_sessions"] + metrics["native_sessions"]

        rt_filter = (
            normalize_runtime(filter_runtime_arg)
            if filter_runtime_arg and filter_runtime_arg.lower() != "all"
            else None
        )
        src_filter = source.lower() if source and source.lower() != "all" else None
        st_filter = status.lower() if status and status.lower() != "all" else None

        if rt_filter:
            sessions = [s for s in sessions if normalize_runtime(s.get("runtime")) == rt_filter]
            native_sessions = [n for n in native_sessions if normalize_runtime(n.get("runtime")) == rt_filter]
            events = [e for e in events if normalize_runtime(e.get("runtime")) == rt_filter]

        if src_filter == "bridge":
            native_sessions = []
        elif src_filter == "native":
            sessions = []

        if st_filter:
            if st_filter == "active":
                sessions = [
                    s
                    for s in sessions
                    if s.get("observed") in ("running", "idle", "waiting_input", "starting", "recovery_needed")
                    or s.get("alive")
                ]
                native_sessions = [n for n in native_sessions if n.get("kind") != "daemon"]
            else:
                sessions = [s for s in sessions if str(s.get("observed") or "").lower() == st_filter]
                native_sessions = [n for n in native_sessions if str(n.get("observed") or "").lower() == st_filter]

        if session_limit is not None and session_limit >= 0:
            sessions = sessions[:session_limit]
            native_sessions = native_sessions[:session_limit]

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
            native_sessions=native_sessions,
            runtimes=runtimes,
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
    total_nodes = len(hosts) + 1
    lines.append(f"\n{BOLD}{CYAN}▶ Cluster Nodes ({total_nodes}){RESET}")
    lines.append(f"{DIM}  {'Host ID':<18} {'Role':<13} {'State':<12} {'Target':<20} {'Node ID':<18} {'Last Contact'}{RESET}")
    lines.append(f"  {DIM}{'─' * 18} {'─' * 13} {'─' * 12} {'─' * 20} {'─' * 18} {'─' * 15}{RESET}")
    lines.append(
        f"  {BOLD}{coord.get('host_id', 'coordinator')[:18]:<18}{RESET} "
        f"{BLUE}{'coordinator':<13}{RESET} "
        f"{GREEN}{'online':<12}{RESET} "
        f"{'local':<20} "
        f"{DIM}{str(coord.get('node_id', '-'))[:16]:<18}{RESET} "
        f"active"
    )
    for h in hosts:
        state = h.get("state", "offline")
        st_color = GREEN if state == "online" else (YELLOW if state == "offline" else RED)
        node_id_str = str(h.get("node_id") or "-")
        lines.append(
            f"  {BOLD}{h.get('host_id', '-'):<18}{RESET} "
            f"{CYAN}{'worker':<13}{RESET} "
            f"{st_color}{state:<12}{RESET} "
            f"{h.get('ssh_target', '-'):<20} "
            f"{DIM}{node_id_str[:16]:<18}{RESET} "
            f"{h.get('last_contact_rel') or _format_relative_time(h.get('last_contact'))}"
        )

    # 3b. Runtime Coverage (pi · claude · agy · codex)
    runtimes = snapshot.get("runtimes") or {}
    lines.append(f"\n{BOLD}{CYAN}▶ Runtimes (Pi · Claude · AGY · Codex){RESET}")
    lines.append(f"{DIM}  {'Runtime':<10} {'Bridge Live/All':<17} {'Native Live':<13} {'Running':<9} {'Waiting':<9} {'Cost'}{RESET}")
    lines.append(f"  {DIM}{'─' * 10} {'─' * 17} {'─' * 13} {'─' * 9} {'─' * 9} {'─' * 10}{RESET}")
    for rt_key in MONITORED_RUNTIMES:
        info = runtimes.get(rt_key, {})
        disp = info.get("display_name") or display_runtime(rt_key)
        b_live = f"{info.get('bridge_active', 0)}/{info.get('bridge_total', 0)}"
        n_live = str(info.get("native_active", 0))
        if info.get("native_daemons", 0):
            n_live += f" (+{info['native_daemons']}d)"
        running_cnt = str(info.get("bridge_running", 0) + info.get("native_workers", 0))
        waiting_cnt = str(info.get("bridge_waiting", 0) + info.get("bridge_recovery", 0))
        rt_cost = f"${float(info.get('total_cost_usd', 0.0)):.3f}"
        lines.append(
            f"  {BOLD}{disp:<10}{RESET} "
            f"{b_live:<17} "
            f"{GREEN if info.get('native_active') else DIM}{n_live:<13}{RESET} "
            f"{running_cnt:<9} "
            f"{YELLOW if (info.get('bridge_waiting') or info.get('bridge_recovery')) else DIM}{waiting_cnt:<9}{RESET} "
            f"{rt_cost}"
        )

    # 4. Active Agents & Sessions Section
    sessions = snapshot.get("sessions", [])
    lines.append(f"\n{BOLD}{CYAN}▶ Bridge Sessions ({len(sessions)}){RESET}")
    lines.append(f"{DIM}  {'Agent / Session Name':<28} {'Host':<10} {'Runtime':<9} {'Model/Profile':<15} {'Status':<14} {'PID':<8} {'Cost':<7} {'ID':<10}{RESET}")
    lines.append(f"  {DIM}{'─' * 28} {'─' * 10} {'─' * 9} {'─' * 15} {'─' * 14} {'─' * 8} {'─' * 7} {'─' * 10}{RESET}")
    if not sessions:
        lines.append(f"  {DIM}(No sessions created yet){RESET}")
    else:
        for s in sessions[:20]:
            obs = s.get("observed", "stopped")
            if s.get("waiting_question"):
                status_str = f"{BG_YELLOW}{BOLD}WAIT_INPUT{RESET}"
            elif obs == "running":
                status_str = f"{GREEN}{BOLD}RUNNING{RESET}"
            elif obs == "idle":
                status_str = f"{BLUE}IDLE{RESET}"
            elif obs == "waiting_input":
                status_str = f"{YELLOW}WAIT_INPUT{RESET}"
            elif obs == "recovery_needed":
                status_str = f"{RED}RECOVERY{RESET}"
            else:
                status_str = f"{DIM}{obs}{RESET}"

            pid_str = str(s.get("worker_pid") or "-")
            if s.get("alive"):
                pid_str = f"{GREEN}{pid_str}●{RESET}"

            cost = f"${s['attempt']['cost_usd']:.3f}" if s.get("attempt") and s["attempt"].get("cost_usd") is not None else "-"
            s_name = s.get("name") or s["id"][:8]
            host_tag = s.get("host_id", "local")[:9]
            rt_tag = (s.get("display_runtime") or s.get("runtime") or "-")[:9]

            lines.append(
                f"  {BOLD}{s_name[:28]:<28}{RESET} "
                f"{DIM}{host_tag:<10}{RESET} "
                f"{rt_tag:<9} "
                f"{(s.get('model') or s.get('profile') or '-')[:15]:<15} "
                f"{status_str:<14} "
                f"{pid_str:<8} "
                f"{cost:<7} "
                f"{DIM}{s.get('id', '-')[:8]:<10}{RESET}"
            )

    # 4b. Native CLI Processes & Workers
    native_sessions = snapshot.get("native_sessions", [])
    lines.append(f"\n{BOLD}{CYAN}▶ Native CLI Processes & Workers ({len(native_sessions)}){RESET}")
    lines.append(f"{DIM}  {'Title / Native Session':<28} {'Runtime':<9} {'Mode':<12} {'Model':<20} {'PID':<8} {'CWD'}{RESET}")
    lines.append(f"  {DIM}{'─' * 28} {'─' * 9} {'─' * 12} {'─' * 20} {'─' * 8} {'─' * 28}{RESET}")
    if not native_sessions:
        lines.append(f"  {DIM}(No live native pi/claude/agy/codex processes){RESET}")
    else:
        for n in native_sessions[:20]:
            rt_tag = (n.get("display_runtime") or n.get("runtime") or "-")[:9]
            mode_tag = (n.get("mode") or "-")[:12]
            m_color = GREEN if mode_tag == "headless" else (CYAN if mode_tag == "interactive" else DIM)
            lines.append(
                f"  {BOLD}{(n.get('name') or '-')[:28]:<28}{RESET} "
                f"{rt_tag:<9} "
                f"{m_color}{mode_tag:<12}{RESET} "
                f"{(n.get('model') or '-')[:20]:<20} "
                f"{str(n.get('pid') or '-'):<8} "
                f"{DIM}{str(n.get('cwd') or '-')[:36]}{RESET}"
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
    *,
    runtime: str | None = None,
    source: str | None = None,
    active_only: bool = False,
    session_limit: int | None = None,
    include_native: bool = True,
) -> None:
    collector = MonitorCollector(state)
    collect_kwargs: dict[str, Any] = {}
    if runtime and runtime != "all":
        collect_kwargs["runtime"] = runtime
    if source and source != "all":
        collect_kwargs["source"] = source
    if active_only:
        collect_kwargs["status"] = "active"
    if session_limit is not None:
        collect_kwargs["session_limit"] = session_limit
    if not include_native:
        collect_kwargs["include_native"] = False

    if once:
        snapshot = collector.collect(limit=20, **collect_kwargs)
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
            snapshot = collector.collect(limit=15, **collect_kwargs)
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

    store = Store(state_dir)
    # Resolve session_id if given as prefix
    with store.connection() as db:
        rows = db.execute(
            "SELECT id FROM sessions WHERE id = ? OR id LIKE ? ORDER BY created DESC",
            (session_id, f"{session_id}%"),
        ).fetchall()
    if not rows:
        raise ValueError(f"Session not found matching: {session_id}")
    full_sid = rows[0]["id"]
    store.set_session_name(full_sid, name)
    return full_sid
