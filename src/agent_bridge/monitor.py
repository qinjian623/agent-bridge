"""Monitor collector and terminal dashboard for cli-agent-bridge."""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import shutil
import socket
import sqlite3
import threading
import time
import unicodedata
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
    filter_snapshot_dict,
    is_event_window_truncated,
    matches_bridge_status,
    normalize_runtime,
    normalize_status_filter,
    parse_runtime_filter,
    should_widen_events,
    snapshot_cost_metrics,
    summarize_runtimes,
)
from .node_identity import FileNodeIdentityStore
from .store import Store

_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]+")


def _sanitize_tui_text(value: Any, max_cols: int | None = None) -> str:
    """Strip terminal control/escape/newline sequences and cap by display width."""
    text = _CONTROL_CHAR_RE.sub(" ", str(value or "")).strip()
    if max_cols is not None:
        return _truncate_display_width(text, max_cols)
    return text


def _char_display_width(ch: str) -> int:
    if unicodedata.category(ch) in ("Mn", "Me", "Cf"):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1


def _iter_clusters_with_width(text: str) -> list[tuple[str, int]]:
    """Split text into base+combining/VS clusters with conservative terminal display width."""
    clusters: list[list[Any]] = []
    for ch in text:
        cat = unicodedata.category(ch)
        if cat in ("Mn", "Me", "Cf"):
            if clusters:
                clusters[-1][0] += ch
                # VS16 (U+FE0F) upgrades narrow emoji bases like '❤' (1 -> 2 cols).
                if ch == "\ufe0f" and clusters[-1][1] == 1:
                    clusters[-1][1] = 2
            else:
                clusters.append([ch, 0])
        else:
            w = 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
            clusters.append([ch, w])
    return [(c[0], int(c[1])) for c in clusters]


def _display_width(text: str) -> int:
    return sum(w for _, w in _iter_clusters_with_width(text))


def _truncate_display_width(text: str, max_cols: int) -> str:
    if max_cols <= 0:
        return ""
    out: list[str] = []
    cols = 0
    for cluster, w in _iter_clusters_with_width(text):
        if cols + w > max_cols:
            break
        out.append(cluster)
        cols += w
    return "".join(out)


_ANSI_SGR_RE = re.compile(r"(\x1b\[[0-9;]*m)")


def _clip_ansi_to_width(line: str, max_cols: int) -> str:
    """Clip a single line containing SGR ANSI escapes to at most max_cols display columns."""
    if max_cols <= 0:
        return ""
    out: list[str] = []
    cols = 0
    clipped = False
    open_sgr = False
    open_sgr_before_base = False
    base_rollback_len = 0
    prev_base_width = 0
    for token in _ANSI_SGR_RE.split(line):
        if not token:
            continue
        if _ANSI_SGR_RE.fullmatch(token):
            out.append(token)
            open_sgr = token != RESET
            continue
        for ch in token:
            cat = unicodedata.category(ch)
            if cat in ("Mn", "Me", "Cf"):
                if ch == "\ufe0f" and prev_base_width == 1:
                    if cols + 1 > max_cols:
                        # Do not leave the narrow base char split from its VS16 presentation selector.
                        del out[base_rollback_len:]
                        open_sgr = open_sgr_before_base
                        cols -= 1
                        clipped = True
                        break
                    cols += 1
                    prev_base_width = 2
                out.append(ch)
                continue
            w = 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
            if cols + w > max_cols:
                clipped = True
                break
            base_rollback_len = len(out)
            open_sgr_before_base = open_sgr
            prev_base_width = w
            out.append(ch)
            cols += w
        if clipped:
            break
    if clipped and open_sgr:
        out.append(RESET)
    return "".join(out)


def _pad_display(text: str, width_cols: int) -> str:
    trimmed = _truncate_display_width(text, width_cols)
    return trimmed + (" " * max(0, width_cols - _display_width(trimmed)))


def _is_finite_number(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(float(value))


def _parse_usage_dict(raw_usage: str | dict[str, Any] | None) -> dict[str, Any] | None:
    if isinstance(raw_usage, dict):
        return raw_usage
    if isinstance(raw_usage, str) and raw_usage.strip():
        try:
            parsed = json.loads(raw_usage)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _extract_usage_cost(raw_usage: str | dict[str, Any] | None) -> tuple[float | None, bool, float | None, bool]:
    """Return (per_attempt_usd, is_complete, reported_session_cost, delta_comparable) from attempt usage JSON."""
    usage_dict = _parse_usage_dict(raw_usage)
    if not usage_dict:
        return None, False, None, True

    reported_raw = usage_dict.get("reported_session_cost")
    reported_session = float(reported_raw) if _is_finite_number(reported_raw) else None
    overall_complete = bool(usage_dict.get("complete") is True)

    cat = usage_dict.get("catalog_estimate")
    if isinstance(cat, dict) and _is_finite_number(cat.get("usd")):
        complete = bool(cat.get("complete") is True and usage_dict.get("complete") is not False)
        cat_source = cat.get("source")
        # Configured/custom price sheets (priced_usage) differ from native SDK reported_session_cost,
        # so do not treat discrepancies against reported_session_cost as missing-spend cost gaps.
        comparable = (
            cat.get("price_date") is None
            and cat_source in (None, "", "claude_cli_model_usage", "pi_local_model_catalog")
        )
        return float(cat["usd"]), complete, reported_session, comparable
    if _is_finite_number(cat):
        return float(cat), overall_complete, reported_session, True

    billed = usage_dict.get("actual_billed_usd")
    if _is_finite_number(billed):
        return float(billed), overall_complete, reported_session, True

    return None, overall_complete, reported_session, True


def _extract_usage_scope(raw_usage: str | dict[str, Any] | None) -> str | None:
    usage_dict = _parse_usage_dict(raw_usage)
    if not usage_dict:
        return None
    scope = usage_dict.get("reported_cost_scope")
    return str(scope) if isinstance(scope, str) and scope else None


def _is_pid_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _format_relative_time(epoch: float | None) -> str:
    if not epoch or isinstance(epoch, bool) or not isinstance(epoch, (int, float)):
        return "-"
    try:
        epoch_f = float(epoch)
    except (OverflowError, TypeError, ValueError):
        return "-"
    if not math.isfinite(epoch_f):
        return "-"
    delta = time.time() - epoch_f
    if not math.isfinite(delta):
        return "-"
    if delta < 0:
        return "now"
    if delta < 60:
        return f"{int(delta)}s ago"
    if delta < 3600:
        return f"{int(delta // 60)}m ago"
    if delta < 86400:
        return f"{int(delta // 3600)}h ago"
    days = delta // 86400
    if not math.isfinite(days):
        return "-"
    return f"{int(days)}d ago"


def _format_bytes(size: int | float | None) -> str:
    if size is None or isinstance(size, bool):
        return "-"
    try:
        size = float(size)
    except (OverflowError, TypeError, ValueError):
        return "-"
    if not math.isfinite(size):
        return "-"
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
    collector_version: int = 3

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


class _RowHashAggregate:
    """Order-independent 128-bit BLAKE2b XOR checksum over full SQLite row tuples."""

    def __init__(self) -> None:
        self._acc = 0

    def step(self, *args: Any) -> None:
        h = hashlib.blake2b(digest_size=16)
        for val in args:
            if val is None:
                h.update(b"N;")
            elif isinstance(val, int):
                raw = str(val).encode("ascii")
                h.update(b"I" + str(len(raw)).encode("ascii") + b":" + raw)
            elif isinstance(val, float):
                raw = repr(val).encode("ascii")
                h.update(b"F" + str(len(raw)).encode("ascii") + b":" + raw)
            else:
                raw = str(val).encode("utf-8", "surrogatepass")
                h.update(b"S" + str(len(raw)).encode("ascii") + b":" + raw)
        self._acc ^= int.from_bytes(h.digest(), "little")

    def finalize(self) -> str:
        return format(self._acc, "032x")


_TASK_FP_EXPR = "bridge_fp(rowid, id, session_id, instruction, status, validation, active_attempt, reservation, created)"
_ATTEMPT_FP_EXPR = "bridge_fp(rowid, id, session_id, task_id, status, usage, started, finished)"
_WORKER_STOP_FP_EXPR = "bridge_fp(seq, session_id, created)"


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
        self._db_lock = threading.Lock()
        self._db_inode: int | None = None
        self._task_watermark: int = 0
        self._task_watermark_id: str | None = None
        self._task_watermark_fp: str = ""
        self._task_total_count: int = 0
        self._task_event_wm: int = 0
        self._mutable_task_rowids: set[int] = set()
        self._cached_tasks_by_session: dict[str, dict[str, Any]] = {}
        self._attempt_watermark: int = 0
        self._attempt_watermark_id: str | None = None
        self._attempt_watermark_fp: str = ""
        self._attempt_stop_events_fp: str = ""
        self._attempt_done_count: int = 0
        self._attempt_max_sort_key: tuple[float, int] | None = None
        self._cached_done_costs: dict[str, dict[str, Any]] = {}
        self._cached_done_attempts: dict[tuple[str, str], dict[str, Any]] = {}

    def _reset_attempt_cache(self) -> None:
        self._attempt_watermark = 0
        self._attempt_watermark_id = None
        self._attempt_watermark_fp = ""
        self._attempt_stop_events_fp = ""
        self._attempt_done_count = 0
        self._attempt_max_sort_key = None
        self._cached_done_costs = {}
        self._cached_done_attempts = {}

    def _open_db(self) -> sqlite3.Connection | None:
        if not self.db_path.exists():
            return None
        for kwargs in (
            {"database": f"file:{self.db_path.resolve()}?mode=ro", "uri": True},
            {"database": str(self.db_path)},
        ):
            try:
                db = sqlite3.connect(timeout=2.0, **kwargs)
                db.row_factory = sqlite3.Row
                db.create_aggregate("bridge_fp", -1, _RowHashAggregate)
                return db
            except sqlite3.Error:
                continue
        return None

    def _has_table(self, db: sqlite3.Connection, table_name: str) -> bool:
        cur = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cur.fetchone() is not None

    @staticmethod
    def _frozen_task_filter(rowids: set[int]) -> tuple[str, tuple[str, ...]]:
        """Exclude mutable task rowids in one bound JSON parameter without SQLite variable-count cliffs."""
        if not rowids:
            return "", ()
        return " AND rowid NOT IN (SELECT value FROM json_each(?))", (json.dumps(sorted(rowids), separators=(",", ":")),)

    @staticmethod
    def _is_valid_broker_snapshot_reply(data: Any) -> bool:
        """Verify that a broker IPC snapshot payload has valid schema, nested elements, and types."""
        def _is_str(val: Any) -> bool:
            return isinstance(val, str)

        def _is_bool(val: Any) -> bool:
            return isinstance(val, bool)

        def _is_int(val: Any) -> bool:
            return isinstance(val, int) and not isinstance(val, bool) and -(1 << 63) <= val <= (1 << 63) - 1

        def _is_count(val: Any) -> bool:
            return _is_int(val) and val >= 0

        def _is_num(val: Any) -> bool:
            if isinstance(val, bool) or not isinstance(val, (int, float)):
                return False
            try:
                f = float(val)
            except OverflowError:
                return False
            return math.isfinite(f)

        def _is_timestamp(val: Any) -> bool:
            return _is_num(val) and 0.0 <= float(val) <= 253402300799.0

        def _is_cost(val: Any) -> bool:
            return _is_num(val) and 0.0 <= float(val) <= 1e12

        def _is_dict(val: Any) -> bool:
            return isinstance(val, dict)

        def _opt(obj: dict[str, Any], pred: Any, *keys: str) -> bool:
            return all((obj.get(k) is None or pred(obj.get(k))) for k in keys)

        def _if_present(obj: dict[str, Any], pred: Any, *keys: str) -> bool:
            return all((k not in obj or pred(obj[k])) for k in keys)

        if not isinstance(data, dict):
            return False

        # Reject any non-str dict keys, integers outside signed 64-bit, or non-finite floats anywhere in payload.
        stack: list[Any] = [data]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                for k, v in cur.items():
                    if not isinstance(k, str):
                        return False
                    stack.append(v)
            elif isinstance(cur, list):
                stack.extend(cur)
            elif isinstance(cur, bool) or cur is None or isinstance(cur, str):
                continue
            elif isinstance(cur, int):
                if not (-(1 << 63) <= cur <= (1 << 63) - 1):
                    return False
            elif isinstance(cur, float):
                if not math.isfinite(cur):
                    return False
            else:
                return False

        if (
            not _if_present(data, _is_timestamp, "timestamp")
            or not _if_present(data, _is_int, "collector_version")
            or not _if_present(data, _is_bool, "events_truncated")
        ):
            return False

        coord = data.get("coordinator")
        hosts = data.get("hosts")
        sessions = data.get("sessions")
        events = data.get("events")
        metrics = data.get("metrics")
        if not (
            isinstance(coord, dict)
            and isinstance(hosts, list)
            and isinstance(sessions, list)
            and isinstance(events, list)
            and isinstance(metrics, dict)
        ):
            return False
        if not _opt(coord, _is_str, "host_id", "node_id", "state_generation", "node_state", "state_dir", "db_path"):
            return False
        if not _opt(coord, _is_int, "db_size"):
            return False

        for item in hosts:
            if not isinstance(item, dict):
                return False
            if not _is_str(item.get("host_id")) or not _is_str(item.get("state")):
                return False
            if not _opt(item, _is_str, "ssh_target", "remote_executable", "node_id", "state_generation", "last_contact_rel", "detail"):
                return False
            if not _opt(item, _is_timestamp, "last_contact") or not _opt(item, _is_int, "protocol_version") or not _if_present(item, _is_dict, "projects"):
                return False

        for item in sessions:
            if not isinstance(item, dict) or not _is_str(item.get("id")):
                return False
            if not _opt(item, _is_str, "name", "host_id", "runtime", "display_runtime", "model", "profile", "status", "observed", "source", "native_id", "cwd"):
                return False
            if not _if_present(item, _is_bool, "alive", "waiting_question", "cost_complete", "has_attempts", "managed"):
                return False
            if (
                not _opt(item, _is_int, "worker_pid", "revision")
                or not _opt(item, _is_cost, "cost_usd")
                or not _opt(item, _is_timestamp, "created")
                or not _opt(item, _is_dict, "attempt", "task")
            ):
                return False
            if isinstance(item.get("attempt"), dict) and (
                not _opt(item["attempt"], _is_cost, "cost_usd")
                or not _if_present(item["attempt"], _is_bool, "cost_complete")
            ):
                return False

        for item in events:
            if not isinstance(item, dict):
                return False
            if not _is_int(item.get("seq")) or not _is_str(item.get("kind")):
                return False
            if not _opt(item, _is_str, "session_id", "session_name", "runtime", "task_id", "summary", "created_rel"):
                return False
            if not _opt(item, _is_timestamp, "created"):
                return False

        if not _if_present(
            metrics,
            _is_int,
            "latest_event_seq",
            "total_hosts",
            "online_hosts",
            "total_sessions",
            "active_sessions",
            "running_tasks",
            "waiting_questions",
            "total_broadcasts",
            "native_sessions",
            "native_workers",
            "native_daemons",
            "total_all_active",
        ):
            return False
        if not _if_present(
            metrics, _is_count,
            "known_cost_sessions", "known_cost_attempt_sessions", "unknown_cost_sessions",
        ):
            return False
        if not _opt(metrics, _is_cost, "total_cost_usd") or not _if_present(metrics, _is_bool, "cost_complete", "has_cost_gaps", "has_attempts"):
            return False

        if "native_sessions" in data:
            native_sessions = data["native_sessions"]
            if not isinstance(native_sessions, list):
                return False
            for item in native_sessions:
                if not isinstance(item, dict):
                    return False
                if not _opt(item, _is_str, "id", "runtime", "display_runtime", "mode", "name", "model", "cwd", "kind", "native_id", "host_id", "status", "observed", "profile"):
                    return False
                if not _opt(item, _is_int, "pid"):
                    return False
                if "native_ids" in item:
                    nids = item["native_ids"]
                    if not isinstance(nids, list) or not all(_is_str(x) for x in nids):
                        return False

        if "runtimes" in data:
            runtimes = data["runtimes"]
            if not isinstance(runtimes, dict):
                return False
            for rt_key, info in runtimes.items():
                if not _is_str(rt_key) or not isinstance(info, dict):
                    return False
                if not _if_present(info, _is_str, "runtime", "display_name"):
                    return False
                if not _if_present(
                    info,
                    _is_int,
                    "bridge_active",
                    "bridge_total",
                    "bridge_running",
                    "bridge_waiting",
                    "bridge_recovery",
                    "bridge_stopped",
                    "native_active",
                    "native_workers",
                    "native_interactive",
                    "native_daemons",
                    "total_live",
                ):
                    return False
                if not _if_present(info, _is_count, "known_cost_sessions", "known_cost_attempt_sessions", "unknown_cost_sessions"):
                    return False
                if not _if_present(info, _is_cost, "total_cost_usd") or not _if_present(info, _is_bool, "cost_complete", "has_attempts", "has_cost_gaps"):
                    return False

        if "pending_questions" in data:
            pending_questions = data["pending_questions"]
            if not isinstance(pending_questions, list):
                return False
            for q in pending_questions:
                if not isinstance(q, dict):
                    return False
                if not _opt(q, _is_str, "id", "session_id", "session_name", "tool", "command", "description", "created_rel") or not _opt(q, _is_timestamp, "created"):
                    return False

        if "groups" in data:
            groups = data["groups"]
            if not isinstance(groups, list):
                return False
            for g in groups:
                if not isinstance(g, dict):
                    return False
                group_identity = g.get("id")
                if group_identity is None:
                    group_identity = g.get("group_id")
                if not _is_str(group_identity):
                    return False
                if (
                    not _opt(g, _is_str, "id", "group_id", "created_rel")
                    or not _opt(g, _is_timestamp, "created")
                    or not _if_present(g, _is_int, "bridge_members", "native_members", "remote_members", "total_members")
                ):
                    return False
                if "remote_hosts" in g:
                    rhosts = g["remote_hosts"]
                    if not isinstance(rhosts, list) or not all(_is_str(x) for x in rhosts):
                        return False

        if "broadcasts" in data:
            broadcasts = data["broadcasts"]
            if not isinstance(broadcasts, list):
                return False
            for b in broadcasts:
                if not isinstance(b, dict):
                    return False
                if not _is_int(b.get("received")) or not _is_int(b.get("total")):
                    return False
                if not _if_present(b, _is_int, "pending", "sending", "uncertain", "failed"):
                    return False
                if not _opt(b, _is_str, "broadcast_id", "group_id", "message_preview", "created_rel") or not _opt(b, _is_timestamp, "created"):
                    return False

        if "peer_queues" in data:
            peer_queues = data["peer_queues"]
            if not isinstance(peer_queues, dict) or not _if_present(peer_queues, _is_dict, "inbox", "outbox", "member_outbox", "operations"):
                return False

        return True

    @staticmethod
    def _attach_provenance(
        snapshot: dict[str, Any],
        *,
        source: str,
        broker_connected: bool | None,
        detail: str,
        legacy: bool = False,
        transport: str | None = None,
        served_by: str | None = None,
    ) -> dict[str, Any]:
        """Attach what is positively known about how this snapshot was produced."""
        if broker_connected is True:
            connection_state = "connected"
        elif source == "local_db_fallback":
            connection_state = "fallback"
        elif broker_connected is False:
            connection_state = "unavailable"
        else:
            connection_state = "unknown"
        out = dict(snapshot)
        raw_provenance = out.get("provenance")
        provenance = dict(raw_provenance) if isinstance(raw_provenance, dict) else {}
        provenance.update(
            {
                "source": source,
                "snapshot_source": source,
                "broker_connected": broker_connected,
                "broker_state": connection_state,
                "connection_state": connection_state,
                "legacy": bool(provenance.get("legacy")) or bool(legacy),
                "detail": detail,
                "transport": transport,
                "served_by": served_by,
            }
        )
        out["provenance"] = provenance
        raw_coordinator = out.get("coordinator")
        coordinator = dict(raw_coordinator) if isinstance(raw_coordinator, dict) else {}
        coordinator.update(
            {
                "snapshot_source": source,
                "broker_connected": broker_connected,
                "connection_state": connection_state,
            }
        )
        out["coordinator"] = coordinator
        return out

    def _finalize_broker_reply(
        self,
        reply: dict[str, Any],
        *,
        limit: int,
        include_native: bool,
        rt_filter: str | None,
        src_filter: str | None,
        st_filter: str | None,
        session_limit: int | None,
        after_seq: int | None,
    ) -> dict[str, Any]:
        """Backfill legacy fields and apply client-side snapshot filters to a broker IPC reply."""
        is_legacy = (
            not reply.get("collector_version")
            or "runtimes" not in reply
            or "native_sessions" not in reply
        )
        out = dict(reply)
        if "runtimes" not in out or "native_sessions" not in out:
            sessions_list = [dict(s) for s in (out.get("sessions") or [])]
            live_pids = {
                int(s["worker_pid"])
                for s in sessions_list
                if s.get("alive") and isinstance(s.get("worker_pid"), int)
            }
            live_nids = {
                (normalize_runtime(s.get("runtime")) or "", str(s["native_id"]))
                for s in sessions_list
                if s.get("native_id")
                and (s.get("alive") or s.get("observed") in ("running", "idle", "waiting_input", "starting"))
            }
            native_list = (
                collect_native_sessions(
                    proc_root=self.proc_root,
                    home=self.native_home,
                    exclude_pids=live_pids,
                    exclude_native_ids=live_nids,
                )
                if include_native
                else []
            )
            for s in sessions_list:
                s_rt = normalize_runtime(s.get("runtime")) or s.get("runtime") or ""
                s["runtime"] = s_rt
                s.setdefault("display_runtime", display_runtime(s_rt))
                s.setdefault("source", "bridge")
                s.setdefault("managed", True)
            out["sessions"] = sessions_list
            out["native_sessions"] = native_list
            out["runtimes"] = summarize_runtimes(sessions_list, native_list)
            rep_metrics = dict(out.get("metrics") or {})
            rep_metrics.setdefault(
                "native_sessions", sum(1 for n in native_list if n.get("kind") != "daemon")
            )
            rep_metrics.setdefault(
                "native_workers", sum(1 for n in native_list if n.get("kind") == "worker")
            )
            rep_metrics.setdefault(
                "native_daemons", sum(1 for n in native_list if n.get("kind") == "daemon")
            )
            rep_metrics.setdefault(
                "total_all_active",
                int(rep_metrics.get("active_sessions") or 0) + int(rep_metrics["native_sessions"]),
            )
            out["metrics"] = rep_metrics
        if is_legacy:
            out["legacy_broker"] = True
        apply_filters = any(
            x is not None
            for x in (rt_filter, src_filter, st_filter, session_limit, after_seq)
        )
        return filter_snapshot_dict(
            out,
            runtime=rt_filter,
            source=src_filter,
            status=st_filter,
            session_limit=session_limit,
            after_seq=after_seq,
            event_limit=limit if apply_filters else None,
        )

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
        rt_filter = parse_runtime_filter(filter_runtime_arg)
        src_filter = source.lower() if source and source.lower() != "all" else None
        if src_filter is not None and src_filter not in {"bridge", "native"}:
            raise ValueError(f"Unsupported source {source!r}")
        st_filter = normalize_status_filter(status)
        broker_socket_present = (self.state / "broker.sock").exists()
        broker_attempted = False
        broker_failure: str | None = None

        if self.host_registry is None and broker_socket_present:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                from .broker import request

                broker_attempted = True
                withhold_session_limit = st_filter is not None and session_limit is not None
                eff_session_limit = None if withhold_session_limit else session_limit
                rpc_args: dict[str, Any] = {"limit": limit, "include_events": include_events}
                if filter_runtime_arg is not None:
                    rpc_args["runtime"] = filter_runtime_arg
                if source is not None:
                    rpc_args["source"] = source
                if status is not None:
                    rpc_args["status"] = status
                if eff_session_limit is not None:
                    rpc_args["session_limit"] = eff_session_limit
                if not include_native:
                    rpc_args["include_native"] = False
                if after_seq is not None:
                    rpc_args["after_seq"] = after_seq

                async def live_snapshot(call_args: dict[str, Any]):
                    return await asyncio.wait_for(request(self.state, "agent_monitor", call_args), 2)

                broker_reply_errors = (
                    BridgeError,
                    OSError,
                    ValueError,
                    TimeoutError,
                    TypeError,
                    KeyError,
                    AttributeError,
                    ArithmeticError,
                )

                try:
                    reply = asyncio.run(live_snapshot(rpc_args))
                    if self._is_valid_broker_snapshot_reply(reply):
                        filtered = self._finalize_broker_reply(
                            reply,
                            limit=limit,
                            include_native=include_native,
                            rt_filter=rt_filter,
                            src_filter=src_filter,
                            st_filter=st_filter,
                            session_limit=eff_session_limit,
                            after_seq=after_seq,
                        )
                        has_ev_filter = any(
                            (
                                rt_filter,
                                src_filter not in (None, "all"),
                                st_filter is not None,
                                after_seq is not None,
                            )
                        )
                        wide_limit = max(int(limit), 500)
                        widened = should_widen_events(
                            reply,
                            list(filtered.get("events") or []),
                            req_limit=limit,
                            max_limit=wide_limit,
                            has_filter=has_ev_filter,
                            source=src_filter,
                            include_events=include_events,
                        )
                        widening_failed = False
                        truncated: bool | None = None
                        if widened:
                            wide_rpc = dict(rpc_args)
                            wide_rpc["limit"] = wide_limit
                            try:
                                wide_reply = asyncio.run(live_snapshot(wide_rpc))
                                if self._is_valid_broker_snapshot_reply(wide_reply):
                                    wide_filtered = self._finalize_broker_reply(
                                        wide_reply,
                                        limit=limit,
                                        include_native=include_native,
                                        rt_filter=rt_filter,
                                        src_filter=src_filter,
                                        st_filter=st_filter,
                                        session_limit=eff_session_limit,
                                        after_seq=after_seq,
                                    )
                                    truncated = is_event_window_truncated(
                                        wide_reply,
                                        list(wide_filtered.get("events") or []),
                                        req_limit=limit,
                                        wide_limit=wide_limit,
                                        has_filter=has_ev_filter,
                                        source=src_filter,
                                        include_events=include_events,
                                    )
                                    reply = wide_reply
                                    filtered = wide_filtered
                                else:
                                    widening_failed = True
                            except broker_reply_errors:
                                widening_failed = True
                        if widening_failed:
                            truncated = True
                        elif truncated is None:
                            truncated = is_event_window_truncated(
                                reply,
                                list(filtered.get("events") or []),
                                req_limit=limit,
                                wide_limit=limit,
                                has_filter=has_ev_filter,
                                source=src_filter,
                                include_events=include_events,
                            )
                        if widened and len(filtered.get("events") or []) > limit:
                            filtered = dict(filtered)
                            filtered["events"] = list(filtered.get("events") or [])[:limit]
                        if withhold_session_limit and session_limit is not None:
                            filtered = dict(filtered)
                            filtered["sessions"] = list(filtered.get("sessions") or [])[:session_limit]
                            filtered["native_sessions"] = list(filtered.get("native_sessions") or [])[:session_limit]
                        raw_ver = int(reply.get("collector_version") or 0)
                        if truncated or raw_ver < 3:
                            filtered = dict(filtered)
                            filtered["events_truncated"] = bool(truncated)
                        legacy_reply = bool(filtered.get("legacy_broker")) or raw_ver < 3
                        filtered = self._attach_provenance(
                            filtered,
                            source="broker_rpc",
                            broker_connected=True,
                            detail=(
                                "Legacy broker reply received over the Unix socket; broker connectivity is confirmed, "
                                "but snapshot metadata is incomplete."
                                if legacy_reply
                                else "Snapshot received from the local broker over its Unix socket."
                            ),
                            legacy=legacy_reply,
                            transport="unix_socket",
                            served_by="broker",
                        )
                        return filtered
                    if isinstance(reply, dict) and "sessions" not in reply:
                        return reply
                    broker_failure = "invalid_snapshot"
                except broker_reply_errors:
                    broker_failure = "rpc_failed"
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
            if hasattr(registry, "snapshot"):
                pairs: list[tuple[Any, Any]] = list(registry.snapshot())
            else:
                pairs = []
                for st in registry.status():
                    cfg = None
                    try:
                        cfg = registry.get(st.host_id)
                    except Exception:
                        pass
                    pairs.append((cfg, st))
            for cfg, st in pairs:
                host_id = st.host_id
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
        remote_hosts = [h for h in hosts if h.get("host_id") != "local"]
        local_host_entry = next((h for h in hosts if h.get("host_id") == "local"), None)
        local_online = 1 if (local_host_entry is None or local_host_entry.get("state") == "online") else 0
        metrics: dict[str, Any] = {
            "total_hosts": len(remote_hosts) + 1,
            "online_hosts": sum(1 for h in remote_hosts if h.get("state") == "online") + local_online,
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

        with self._db_lock:
            db = self._open_db()
            if db is not None:
                try:
                    db.execute("BEGIN DEFERRED")
                    try:
                        db_inode: int | None = self.db_path.stat().st_ino
                    except OSError:
                        db_inode = None
                    if db_inode != self._db_inode:
                        self._db_inode = db_inode
                        self._task_watermark = 0
                        self._task_watermark_id = None
                        self._task_watermark_fp = ""
                        self._task_total_count = 0
                        self._task_event_wm = 0
                        self._mutable_task_rowids = set()
                        self._cached_tasks_by_session = {}
                        self._reset_attempt_cache()

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

                    session_names_map: dict[str, str] = {}
                    session_runtime_map: dict[str, str] = {}
                    live_worker_pids: set[int] = set()
                    live_bridge_native_ids: set[tuple[str, str]] = set()

                    latest_tasks: dict[str, dict[str, Any]] = {}
                    inflight_sessions: set[str] = set()
                    mutable_statuses = {"pending", "queued", "assigned", "running", "waiting_input", "recovery_needed"}
                    has_events_table = self._has_table(db, "events")
                    if self._has_table(db, "tasks"):
                        t_wm = self._task_watermark
                        if t_wm > 0:
                            frozen_clause, m_params = self._frozen_task_filter(self._mutable_task_rowids)
                            t_count_row = db.execute(
                                "SELECT COUNT(*), COALESCE(MAX(rowid), 0), "
                                "       (SELECT COUNT(*) FROM tasks WHERE rowid <= ?), "
                                "       (SELECT id FROM tasks WHERE rowid = ?), "
                                f"      (SELECT {_TASK_FP_EXPR} FROM tasks WHERE rowid <= ?{frozen_clause}) "
                                "FROM tasks",
                                (t_wm, t_wm, t_wm, *m_params),
                            ).fetchone()
                            cur_task_count = int(t_count_row[0]) if t_count_row else 0
                            cur_task_max_rowid = int(t_count_row[1]) if t_count_row else 0
                            cur_task_le_wm = int(t_count_row[2]) if t_count_row else 0
                            cur_task_wm_id = t_count_row[3] if t_count_row else None
                            cur_task_fp = str(t_count_row[4] or "") if t_count_row else ""
                        else:
                            t_count_row = db.execute("SELECT COUNT(*), COALESCE(MAX(rowid), 0) FROM tasks").fetchone()
                            cur_task_count = int(t_count_row[0]) if t_count_row else 0
                            cur_task_max_rowid = int(t_count_row[1]) if t_count_row else 0
                            cur_task_le_wm = 0
                            cur_task_wm_id = None
                            cur_task_fp = ""
                        cur_ev_max = 0
                        if has_events_table:
                            ev_row = db.execute("SELECT COALESCE(MAX(seq), 0) FROM events").fetchone()
                            cur_ev_max = int(ev_row[0]) if ev_row else 0

                        # Fall back to cold scan if DB shrank/VACUUMed/replaced below watermark or events reset.
                        if (
                            t_wm > 0
                            and (
                                cur_task_count < self._task_total_count
                                or cur_task_le_wm != self._task_total_count
                                or cur_task_max_rowid < t_wm
                                or cur_task_wm_id != self._task_watermark_id
                                or cur_task_fp != self._task_watermark_fp
                                or cur_ev_max < self._task_event_wm
                            )
                        ):
                            t_wm = 0

                        if t_wm == 0:
                            t_cur = db.execute(
                                "SELECT rowid, id, session_id, instruction, status, validation, active_attempt, reservation, created "
                                "FROM ("
                                "  SELECT rowid, id, session_id, instruction, status, validation, active_attempt, reservation, created, "
                                "         ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created DESC, rowid DESC) AS rn "
                                "  FROM tasks"
                                ") WHERE rn = 1"
                            )
                            new_t_wm = cur_task_max_rowid
                            cached_tasks: dict[str, dict[str, Any]] = {}
                            mutable_rowids: set[int] = set()
                            rows_to_process = list(t_cur.fetchall())
                            # Also seed mutable_rowids for non-latest mutable tasks (e.g., queued tasks)
                            for m_row in db.execute(
                                "SELECT rowid FROM tasks WHERE status IN ('pending','queued','assigned','running','waiting_input','recovery_needed')"
                            ):
                                mutable_rowids.add(int(m_row[0]))
                        else:
                            cached_tasks = {k: dict(v) for k, v in self._cached_tasks_by_session.items()}
                            mutable_rowids = set(self._mutable_task_rowids)
                            touched_task_ids: list[str] = []
                            touched_sids: list[str] = []
                            if has_events_table and cur_ev_max > self._task_event_wm:
                                touched_task_ids = [
                                    r[0]
                                    for r in db.execute(
                                        "SELECT DISTINCT task_id FROM events WHERE seq > ? AND task_id IS NOT NULL",
                                        (self._task_event_wm,),
                                    ).fetchall()
                                    if r[0]
                                ]
                                touched_sids = [
                                    r[0]
                                    for r in db.execute(
                                        "SELECT DISTINCT session_id FROM events WHERE seq > ? AND session_id IS NOT NULL AND task_id IS NULL",
                                        (self._task_event_wm,),
                                    ).fetchall()
                                    if r[0]
                                ]
                            clauses = ["rowid > ?"]
                            params: list[Any] = [t_wm]
                            if mutable_rowids:
                                m_list = sorted(mutable_rowids)
                                clauses.append(f"rowid IN ({','.join('?' for _ in m_list)})")
                                params.extend(m_list)
                            if touched_task_ids:
                                if len(touched_task_ids) <= 200:
                                    clauses.append(f"id IN ({','.join('?' for _ in touched_task_ids)})")
                                    params.extend(touched_task_ids)
                                else:
                                    clauses = ["1=1"]
                                    params = []
                            if touched_sids and clauses != ["1=1"]:
                                if len(touched_sids) <= 200:
                                    clauses.append(f"session_id IN ({','.join('?' for _ in touched_sids)})")
                                    params.extend(touched_sids)
                                else:
                                    clauses = ["1=1"]
                                    params = []
                            rows_to_process = db.execute(
                                "SELECT rowid, id, session_id, instruction, status, validation, active_attempt, reservation, created "
                                f"FROM tasks WHERE {' OR '.join(clauses)} "
                                "ORDER BY created DESC, rowid DESC",
                                tuple(params),
                            ).fetchall()
                            new_t_wm = max(t_wm, cur_task_max_rowid)

                        task_sid_index = {
                            info["id"]: sid for sid, info in cached_tasks.items()
                        }
                        recompute_sids: set[str] = set()
                        for t_row in rows_to_process:
                            t_rowid = int(t_row["rowid"])
                            if t_rowid > new_t_wm:
                                new_t_wm = t_rowid
                            t_id = t_row["id"]
                            t_sid = t_row["session_id"]
                            t_status = t_row["status"]
                            if t_status in mutable_statuses:
                                mutable_rowids.add(t_rowid)
                            else:
                                mutable_rowids.discard(t_rowid)

                            old_sid = task_sid_index.get(t_id)
                            if old_sid is not None and old_sid != t_sid:
                                task_sid_index.pop(t_id, None)
                                cur_old = cached_tasks.get(old_sid)
                                if cur_old is not None and cur_old["id"] == t_id:
                                    cached_tasks.pop(old_sid, None)
                                recompute_sids.add(old_sid)

                            cand = {
                                "id": t_id,
                                "instruction": (t_row["instruction"] or "")[:80],
                                "status": t_status,
                                "validation": t_row["validation"],
                                "active_attempt": t_row["active_attempt"],
                                "reservation": t_row["reservation"],
                                "created": t_row["created"],
                                "rowid": t_rowid,
                            }
                            prev = cached_tasks.get(t_sid)
                            if (
                                prev is None
                                or (cand["created"], cand["rowid"]) >= (prev["created"], prev["rowid"])
                                or cand["id"] == prev["id"]
                            ):
                                if prev is not None and prev["id"] != t_id:
                                    task_sid_index.pop(prev["id"], None)
                                cached_tasks[t_sid] = cand
                                task_sid_index[t_id] = t_sid
                            elif task_sid_index.get(t_id) == t_sid:
                                task_sid_index.pop(t_id, None)

                        for old_sid in recompute_sids:
                            repl = db.execute(
                                "SELECT rowid, id, instruction, status, validation, active_attempt, reservation, created "
                                "FROM tasks WHERE session_id=? ORDER BY created DESC, rowid DESC LIMIT 1",
                                (old_sid,),
                            ).fetchone()
                            if repl is None:
                                prev_old = cached_tasks.pop(old_sid, None)
                                if prev_old is not None and task_sid_index.get(prev_old["id"]) == old_sid:
                                    task_sid_index.pop(prev_old["id"], None)
                            else:
                                r_rowid = int(repl["rowid"])
                                if repl["status"] in mutable_statuses:
                                    mutable_rowids.add(r_rowid)
                                prev_old = cached_tasks.get(old_sid)
                                if prev_old is not None and prev_old["id"] != repl["id"] and task_sid_index.get(prev_old["id"]) == old_sid:
                                    task_sid_index.pop(prev_old["id"], None)
                                cached_tasks[old_sid] = {
                                    "id": repl["id"],
                                    "instruction": (repl["instruction"] or "")[:80],
                                    "status": repl["status"],
                                    "validation": repl["validation"],
                                    "active_attempt": repl["active_attempt"],
                                    "reservation": repl["reservation"],
                                    "created": repl["created"],
                                    "rowid": r_rowid,
                                }
                                task_sid_index[repl["id"]] = old_sid

                        new_t_wm_id: str | None = None
                        new_t_wm_fp = ""
                        new_t_wm_count = cur_task_count
                        if new_t_wm > 0:
                            end_frozen_clause, end_m_params = self._frozen_task_filter(mutable_rowids)
                            wm_row = db.execute(
                                "SELECT (SELECT id FROM tasks WHERE rowid = ?), "
                                "       (SELECT COUNT(*) FROM tasks WHERE rowid <= ?), "
                                f"      {_TASK_FP_EXPR} "
                                f"FROM tasks WHERE rowid <= ?{end_frozen_clause}",
                                (new_t_wm, new_t_wm, new_t_wm, *end_m_params),
                            ).fetchone()
                            new_t_wm_id = wm_row[0] if wm_row else None
                            new_t_wm_count = int(wm_row[1] or 0) if wm_row else 0
                            new_t_wm_fp = str(wm_row[2] or "") if wm_row else ""
                        self._task_watermark = new_t_wm
                        self._task_watermark_id = new_t_wm_id
                        self._task_watermark_fp = new_t_wm_fp
                        self._task_total_count = new_t_wm_count
                        self._task_event_wm = cur_ev_max
                        self._mutable_task_rowids = mutable_rowids
                        self._cached_tasks_by_session = cached_tasks
                        latest_task_active_attempt = {
                            (sid, info["id"]): info["active_attempt"]
                            for sid, info in cached_tasks.items()
                            if info.get("active_attempt")
                        }
                        latest_tasks = {
                            sid: {k: v for k, v in info.items() if k not in ("rowid", "active_attempt")}
                            for sid, info in cached_tasks.items()
                        }

                        for r in db.execute(
                            "SELECT DISTINCT session_id FROM tasks WHERE status IN ('assigned','running','waiting_input')"
                        ):
                            inflight_sessions.add(r[0])

                    latest_task_pairs = {(sid, t["id"]) for sid, t in latest_tasks.items()}
                    if not self._has_table(db, "tasks"):
                        latest_task_active_attempt: dict[tuple[str, str], str] = {}
                    latest_attempts: dict[tuple[str, str], dict[str, Any]] = {}
                    session_costs: dict[str, dict[str, Any]] = {}
                    if self._has_table(db, "attempts"):
                        a_wm = self._attempt_watermark
                        if a_wm > 0:
                            stop_fp_expr = (
                                f"(SELECT {_WORKER_STOP_FP_EXPR} FROM events WHERE kind='worker_stopped')"
                                if has_events_table
                                else "''"
                            )
                            cnt_row = db.execute(
                                f"SELECT COUNT(*), {_ATTEMPT_FP_EXPR}, "
                                "       (SELECT id FROM attempts WHERE rowid = ? AND finished IS NOT NULL), "
                                f"      {stop_fp_expr} "
                                "FROM attempts WHERE finished IS NOT NULL AND rowid <= ?",
                                (a_wm, a_wm),
                            ).fetchone()
                            cur_done = int(cnt_row[0]) if cnt_row else 0
                            cur_fp = str(cnt_row[1] or "") if cnt_row else ""
                            cur_wm_id = cnt_row[2] if cnt_row else None
                            cur_stop_fp = str(cnt_row[3] or "") if (cnt_row and has_events_table) else ""
                            if (
                                cur_done != self._attempt_done_count
                                or cur_fp != self._attempt_watermark_fp
                                or cur_wm_id != self._attempt_watermark_id
                                or cur_stop_fp != self._attempt_stop_events_fp
                            ):
                                a_wm = 0
                                self._reset_attempt_cache()

                        attempt_sql = (
                            "SELECT a.rowid AS a_rowid, a.session_id AS session_id, a.id AS id, "
                            "       a.task_id AS task_id, a.status AS status, a.usage AS usage, "
                            "       a.started AS started, a.finished AS finished "
                            "FROM attempts a "
                        )
                        fetched_rows = db.execute(
                            attempt_sql
                            + ("WHERE a.rowid > ? OR a.finished IS NULL " if a_wm > 0 else "")
                            + "ORDER BY COALESCE(a.started, a.finished, 1e18) ASC, a.rowid ASC",
                            (a_wm,) if a_wm > 0 else (),
                        ).fetchall()

                        def _attempt_sort_key(started: Any, finished: Any, rowid: int) -> tuple[float, int]:
                            if started is not None:
                                primary = float(started)
                            elif finished is not None:
                                primary = float(finished)
                            else:
                                primary = 1e18
                            return (primary, int(rowid))

                        if a_wm > 0 and self._attempt_max_sort_key is not None:
                            out_of_order = any(
                                _attempt_sort_key(r["started"], r["finished"], int(r["a_rowid"])) < self._attempt_max_sort_key
                                for r in fetched_rows
                            )
                            if out_of_order:
                                # Late/backfilled attempt sorts earlier than already-cached attempts (including equal started with lower rowid): rebuild chronologically.
                                a_wm = 0
                                self._reset_attempt_cache()
                                fetched_rows = db.execute(
                                    attempt_sql + "ORDER BY COALESCE(a.started, a.finished, 1e18) ASC, a.rowid ASC"
                                ).fetchall()

                        # Copy immutable completed state cleanly into working accumulators
                        done_costs: dict[str, dict[str, Any]] = {
                            sid: dict(val) for sid, val in self._cached_done_costs.items()
                        }
                        done_attempts: dict[tuple[str, str], dict[str, Any]] = dict(self._cached_done_attempts)
                        worker_stop_times: dict[str, list[float]] = {}
                        run_stop_events_fp = self._attempt_stop_events_fp if a_wm > 0 else ""
                        if has_events_table and fetched_rows:
                            stop_hasher = _RowHashAggregate()
                            for ev_row in db.execute(
                                "SELECT seq, session_id, created FROM events WHERE kind='worker_stopped' ORDER BY seq ASC"
                            ):
                                stop_hasher.step(ev_row["seq"], ev_row["session_id"], ev_row["created"])
                                if ev_row["session_id"] and ev_row["created"] is not None:
                                    worker_stop_times.setdefault(ev_row["session_id"], []).append(
                                        float(ev_row["created"])
                                    )
                            run_stop_events_fp = stop_hasher.finalize()
                        new_done_max_rowid = a_wm
                        newly_done_count = 0

                        def _build_latest_attempt_entry(row: sqlite3.Row) -> dict[str, Any]:
                            attempt_usd, usage_complete, reported_sess, _ = _extract_usage_cost(row["usage"])
                            attempt_finished = row["finished"] is not None
                            unstarted_cancelled = (
                                attempt_finished
                                and row["started"] is None
                                and row["status"] == "cancelled"
                                and attempt_usd is None
                                and reported_sess is None
                            )
                            if unstarted_cancelled:
                                att_display: float | None = 0.0
                                att_complete = True
                            else:
                                att_display = attempt_usd if attempt_usd is not None else reported_sess
                                att_complete = bool(usage_complete and attempt_finished and att_display is not None)
                            return {
                                "id": row["id"],
                                "status": row["status"],
                                "cost_usd": round(att_display, 6) if att_display is not None else None,
                                "cost_complete": att_complete,
                                "started": row["started"],
                                "finished": row["finished"],
                                "_rowid": int(row["a_rowid"]),
                            }

                        def _record_attempt(
                            target_costs: dict[str, dict[str, Any]],
                            target_latest: dict[tuple[str, str], dict[str, Any]],
                            row: sqlite3.Row,
                        ) -> None:
                            t_id = row["task_id"]
                            a_sid = row["session_id"]
                            attempt_usd, usage_complete, reported_sess, delta_comparable = _extract_usage_cost(
                                row["usage"]
                            )
                            cur_scope = _extract_usage_scope(row["usage"])
                            attempt_finished = row["finished"] is not None
                            unstarted_cancelled = (
                                attempt_finished
                                and row["started"] is None
                                and row["status"] == "cancelled"
                                and attempt_usd is None
                                and reported_sess is None
                            )
                            sc = target_costs.get(a_sid)
                            if sc is None:
                                sc = {
                                    "running_usd": None,
                                    "total_attempts": 0,
                                    "cost_gap": False,
                                    "has_inflight_attempt": False,
                                    "scope_id": None,
                                    "saw_unscoped_cost": False,
                                    "scope_base_usd": 0.0,
                                    "last_reported_in_scope": None,
                                    "scope_delta_since_reported": 0.0,
                                    "scope_noncomparable_delta": 0.0,
                                    "scope_uses_deltas": False,
                                    "last_attempt_time": None,
                                }
                                target_costs[a_sid] = sc
                            sc["total_attempts"] += 1
                            if not attempt_finished:
                                sc["has_inflight_attempt"] = True
                            if unstarted_cancelled:
                                # Cancelled before starting: $0 spent and no cost gap; preserve client scope/time.
                                if sc["running_usd"] is None:
                                    sc["running_usd"] = 0.0
                            else:
                                cur_time = (
                                    float(row["started"])
                                    if row["started"] is not None
                                    else (float(row["finished"]) if row["finished"] is not None else float("inf"))
                                )
                                prev_time = sc["last_attempt_time"]
                                has_cost_telemetry = (
                                    cur_scope is not None
                                    or reported_sess is not None
                                    or attempt_usd is not None
                                )
                                stop_between = prev_time is not None and any(
                                    prev_time <= stop_t <= cur_time for stop_t in worker_stop_times.get(a_sid, ())
                                )
                                # Attempts without usage/scope (e.g. failed before emitting telemetry) must
                                # not look like a client scope switch to/from None and split cumulative spend.
                                scope_changed = (
                                    has_cost_telemetry
                                    and prev_time is not None
                                    and cur_scope != sc["scope_id"]
                                    and (sc["scope_id"] is not None or sc["saw_unscoped_cost"])
                                )
                                reported_drop = (
                                    reported_sess is not None
                                    and sc["last_reported_in_scope"] is not None
                                    and float(reported_sess) < float(sc["last_reported_in_scope"])
                                )
                                restarted = stop_between or scope_changed or reported_drop
                                if restarted:
                                    # Worker / SDK client restarted: seal pre-restart spend before integrating new worker costs.
                                    sc["scope_base_usd"] = float(sc["running_usd"] or 0.0)
                                    sc["scope_id"] = cur_scope
                                    sc["saw_unscoped_cost"] = has_cost_telemetry and cur_scope is None
                                    sc["last_reported_in_scope"] = None
                                    sc["scope_delta_since_reported"] = 0.0
                                    sc["scope_noncomparable_delta"] = 0.0
                                    sc["scope_uses_deltas"] = True
                                elif has_cost_telemetry:
                                    if cur_scope is None:
                                        sc["saw_unscoped_cost"] = True
                                    elif sc["scope_id"] is None:
                                        sc["scope_id"] = cur_scope

                                if sc["scope_uses_deltas"]:
                                    if attempt_usd is not None and delta_comparable:
                                        pending_delta = float(sc.get("scope_delta_since_reported") or 0.0)
                                        pending_noncomp = float(sc.get("scope_noncomparable_delta") or 0.0)
                                        prev_reported = sc["last_reported_in_scope"]
                                        sc["running_usd"] = float(sc["running_usd"] or 0.0) + float(attempt_usd)
                                        if reported_sess is not None:
                                            if pending_noncomp > 1e-9:
                                                sc["cost_gap"] = True
                                            expected_with_cur = float(prev_reported or 0.0) + pending_delta + float(attempt_usd)
                                            if float(reported_sess) > expected_with_cur + pending_noncomp + 1e-9:
                                                excess = float(reported_sess) - (expected_with_cur + pending_noncomp)
                                                sc["running_usd"] = max(
                                                    sc["running_usd"] + excess,
                                                    sc["scope_base_usd"] + float(reported_sess),
                                                )
                                                sc["cost_gap"] = True
                                            elif float(reported_sess) < expected_with_cur - 1e-9:
                                                sc["cost_gap"] = True
                                            sc["last_reported_in_scope"] = float(reported_sess)
                                            sc["scope_delta_since_reported"] = max(
                                                0.0,
                                                expected_with_cur - float(reported_sess),
                                            )
                                            sc["scope_noncomparable_delta"] = 0.0
                                        else:
                                            sc["scope_delta_since_reported"] = pending_delta + float(attempt_usd)
                                        if not usage_complete or not attempt_finished:
                                            sc["cost_gap"] = True
                                    elif reported_sess is not None:
                                        pending_noncomp = float(sc.get("scope_noncomparable_delta") or 0.0)
                                        if sc["last_reported_in_scope"] is not None:
                                            raw_diff = float(reported_sess) - float(sc["last_reported_in_scope"])
                                            pending_delta = float(sc.get("scope_delta_since_reported") or 0.0)
                                            if pending_noncomp > 1e-9:
                                                # Earlier custom-catalog attempt lacked reported_session_cost;
                                                # native cumulative diff cannot safely verify or add on top of it cleanly.
                                                net_diff = raw_diff - pending_delta - pending_noncomp
                                                sc["cost_gap"] = True
                                                sc["running_usd"] = float(sc["running_usd"] or 0.0) + max(0.0, net_diff)
                                                sc["last_reported_in_scope"] = float(reported_sess)
                                                sc["scope_delta_since_reported"] = max(
                                                    0.0,
                                                    pending_delta - max(0.0, raw_diff),
                                                )
                                                sc["scope_noncomparable_delta"] = 0.0
                                            else:
                                                net_diff = raw_diff - pending_delta
                                                if net_diff < -1e-9:
                                                    sc["cost_gap"] = True
                                                sc["running_usd"] = float(sc["running_usd"] or 0.0) + max(0.0, net_diff)
                                                sc["last_reported_in_scope"] = float(reported_sess)
                                                sc["scope_delta_since_reported"] = max(0.0, -net_diff)
                                            if not usage_complete or not attempt_finished:
                                                sc["cost_gap"] = True
                                        elif attempt_usd is not None:
                                            # Non-comparable custom catalog per-attempt delta alongside first post-restart
                                            # reported_session_cost: count attempt_usd once without cross-catalog comparison.
                                            sc["running_usd"] = float(sc["running_usd"] or 0.0) + float(attempt_usd)
                                            sc["last_reported_in_scope"] = float(reported_sess)
                                            sc["scope_delta_since_reported"] = 0.0
                                            if pending_noncomp > 1e-9:
                                                sc["cost_gap"] = True
                                                sc["scope_noncomparable_delta"] = 0.0
                                            if not usage_complete or not attempt_finished:
                                                sc["cost_gap"] = True
                                        else:
                                            # Post-restart reported_session_cost without an in-scope baseline or
                                            # per-attempt delta cannot prove what earlier turns/workers it includes.
                                            pending_delta = float(sc.get("scope_delta_since_reported") or 0.0)
                                            sc["running_usd"] = max(
                                                float(sc["running_usd"] or 0.0),
                                                sc["scope_base_usd"] + float(reported_sess),
                                            )
                                            sc["last_reported_in_scope"] = float(reported_sess)
                                            sc["scope_delta_since_reported"] = max(
                                                0.0,
                                                pending_delta - float(reported_sess),
                                            )
                                            sc["scope_noncomparable_delta"] = 0.0
                                            sc["cost_gap"] = True
                                    elif attempt_usd is not None:
                                        sc["running_usd"] = float(sc["running_usd"] or 0.0) + float(attempt_usd)
                                        if delta_comparable:
                                            sc["scope_delta_since_reported"] = float(
                                                sc.get("scope_delta_since_reported") or 0.0
                                            ) + float(attempt_usd)
                                        else:
                                            sc["scope_noncomparable_delta"] = float(
                                                sc.get("scope_noncomparable_delta") or 0.0
                                            ) + float(attempt_usd)
                                        if not usage_complete or not attempt_finished:
                                            sc["cost_gap"] = True
                                    else:
                                        sc["cost_gap"] = True
                                else:
                                    if reported_sess is not None:
                                        pending_delta = float(sc.get("scope_delta_since_reported") or 0.0)
                                        pending_noncomp = float(sc.get("scope_noncomparable_delta") or 0.0)
                                        prev_reported = sc["last_reported_in_scope"]
                                        comparable_attempt_usd = (
                                            float(attempt_usd)
                                            if (attempt_usd is not None and delta_comparable)
                                            else 0.0
                                        )
                                        expected_with_cur = (
                                            float(prev_reported or 0.0) + pending_delta + comparable_attempt_usd
                                        )
                                        if pending_noncomp > 1e-9:
                                            # Track unabsorbed comparable deltas and custom-catalog floor independently:
                                            # - below expected_comparable: keep custom delta in scope_base_usd and retain the
                                            #   remaining unabsorbed comparable delta in scope_delta_since_reported.
                                            # - at/above expected_comparable: preserve only the custom portion exceeding native_extra.
                                            sc["cost_gap"] = True
                                            if float(reported_sess) < expected_with_cur - 1e-9:
                                                sc["scope_base_usd"] += pending_noncomp
                                                sc["scope_noncomparable_delta"] = 0.0
                                                sc["scope_delta_since_reported"] = expected_with_cur - float(reported_sess)
                                                sc["running_usd"] = max(
                                                    float(sc["running_usd"] or 0.0) + comparable_attempt_usd,
                                                    sc["scope_base_usd"] + float(reported_sess),
                                                )
                                            else:
                                                native_extra = float(reported_sess) - expected_with_cur
                                                custom_excess = max(0.0, pending_noncomp - native_extra)
                                                sc["scope_base_usd"] += custom_excess
                                                sc["scope_noncomparable_delta"] = 0.0
                                                sc["scope_delta_since_reported"] = 0.0
                                                sc["running_usd"] = sc["scope_base_usd"] + float(reported_sess)
                                            sc["last_reported_in_scope"] = float(reported_sess)
                                        elif float(reported_sess) < expected_with_cur - 1e-9:
                                            # Cumulative reported_session_cost failed to cover prior or current deltas.
                                            sc["cost_gap"] = True
                                            sc["running_usd"] = max(
                                                float(sc["running_usd"] or 0.0) + comparable_attempt_usd,
                                                sc["scope_base_usd"] + float(reported_sess),
                                            )
                                            sc["last_reported_in_scope"] = float(reported_sess)
                                            sc["scope_delta_since_reported"] = max(
                                                0.0,
                                                expected_with_cur - float(reported_sess),
                                            )
                                        else:
                                            # reported_session_cost is cumulative within the current client scope;
                                            # prioritize it even when the same attempt also has a delta estimate.
                                            sc["running_usd"] = sc["scope_base_usd"] + float(reported_sess)
                                            sc["last_reported_in_scope"] = float(reported_sess)
                                            sc["scope_delta_since_reported"] = 0.0
                                            if not usage_complete or not attempt_finished:
                                                sc["cost_gap"] = True
                                    elif attempt_usd is not None:
                                        # Per-attempt delta (catalog_estimate / actual_billed_usd) adds sequentially.
                                        sc["running_usd"] = float(sc["running_usd"] or 0.0) + float(attempt_usd)
                                        if delta_comparable:
                                            sc["scope_delta_since_reported"] = float(
                                                sc.get("scope_delta_since_reported") or 0.0
                                            ) + float(attempt_usd)
                                        else:
                                            sc["scope_noncomparable_delta"] = float(
                                                sc.get("scope_noncomparable_delta") or 0.0
                                            ) + float(attempt_usd)
                                        if not usage_complete or not attempt_finished:
                                            sc["cost_gap"] = True
                                    else:
                                        sc["cost_gap"] = True
                                sc["last_attempt_time"] = (
                                    float(row["finished"]) if row["finished"] is not None else cur_time
                                )

                            cur_rowid = int(row["a_rowid"])
                            prev_att = target_latest.get((a_sid, t_id))
                            # Cost iteration is sorted by wall-clock timestamps; never let clock rollback
                            # overwrite a newer-created attempt (higher a_rowid) with an older cancelled one.
                            if prev_att is None or cur_rowid >= int(prev_att.get("_rowid", -1)):
                                target_latest[(a_sid, t_id)] = _build_latest_attempt_entry(row)

                        has_live_rows = any(a_row["finished"] is None for a_row in fetched_rows)
                        if not has_live_rows:
                            # All fetched rows are finished and ordered by (started, rowid): safe to fold into permanent cache.
                            max_sort_key_seen = self._attempt_max_sort_key
                            new_done_wm_id = self._attempt_watermark_id
                            for a_row in fetched_rows:
                                r_rowid = int(a_row["a_rowid"])
                                if r_rowid > a_wm:
                                    _record_attempt(done_costs, done_attempts, a_row)
                                    newly_done_count += 1
                                    if r_rowid > new_done_max_rowid:
                                        new_done_max_rowid = r_rowid
                                        new_done_wm_id = a_row["id"]
                                    sort_key = _attempt_sort_key(a_row["started"], a_row["finished"], r_rowid)
                                    if max_sort_key_seen is None or sort_key > max_sort_key_seen:
                                        max_sort_key_seen = sort_key
                            if newly_done_count > 0:
                                fp_row = db.execute(
                                    f"SELECT {_ATTEMPT_FP_EXPR} FROM attempts WHERE finished IS NOT NULL AND rowid <= ?",
                                    (new_done_max_rowid,),
                                ).fetchone()
                                self._attempt_watermark = new_done_max_rowid
                                self._attempt_watermark_id = new_done_wm_id
                                self._attempt_watermark_fp = str(fp_row[0] or "") if fp_row else ""
                                self._attempt_stop_events_fp = run_stop_events_fp
                                self._attempt_done_count += newly_done_count
                                self._attempt_max_sort_key = max_sort_key_seen
                                self._cached_done_costs = done_costs
                                self._cached_done_attempts = done_attempts

                        session_costs = {sid: dict(val) for sid, val in done_costs.items()}
                        raw_latest_attempts = dict(done_attempts)
                        if has_live_rows:
                            # Replay all post-watermark / live attempts together in strict SQL chronological order.
                            for a_row in fetched_rows:
                                _record_attempt(session_costs, raw_latest_attempts, a_row)
                        latest_attempts = {
                            pair: {k: v for k, v in att.items() if k != "_rowid"}
                            for pair, att in raw_latest_attempts.items()
                            if pair in latest_task_pairs
                        }
                        # Also honor explicit tasks.active_attempt when set so it always takes precedence.
                        for pair, active_aid in latest_task_active_attempt.items():
                            cur_att = latest_attempts.get(pair)
                            if cur_att is None or cur_att.get("id") != active_aid:
                                a_active_row = db.execute(
                                    attempt_sql + "WHERE a.id = ?",
                                    (active_aid,),
                                ).fetchone()
                                if a_active_row is not None:
                                    built = _build_latest_attempt_entry(a_active_row)
                                    latest_attempts[pair] = {k: v for k, v in built.items() if k != "_rowid"}

                    all_cost_complete = True
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

                            task_info = latest_tasks.get(sid)
                            attempt_info = latest_attempts.get((sid, task_info["id"])) if task_info else None
                            sc = session_costs.get(sid)
                            has_inflight = sid in inflight_sessions or (
                                task_info is not None and task_info.get("status") in ("assigned", "running", "waiting_input")
                            )
                            if sc is not None and sc["total_attempts"] > 0:
                                sess_has_attempts = True
                                if sc["running_usd"] is not None:
                                    sess_cost_usd: float | None = round(float(sc["running_usd"]), 6)
                                    sess_cost_complete = bool(
                                        not sc["cost_gap"] and not sc["has_inflight_attempt"] and not has_inflight
                                    )
                                else:
                                    sess_cost_usd = None
                                    sess_cost_complete = False
                                if sess_cost_usd is not None:
                                    metrics["total_cost_usd"] += sess_cost_usd
                                if not sess_cost_complete:
                                    all_cost_complete = False
                            else:
                                sess_has_attempts = bool(task_info is not None and task_info.get("status") not in (None, "cancelled"))
                                sess_cost_complete = not has_inflight
                                sess_cost_usd = 0.0 if not has_inflight else None
                                if has_inflight:
                                    all_cost_complete = False

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
                                    "host_id": "local",
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
                                    "cost_usd": sess_cost_usd,
                                    "cost_complete": sess_cost_complete,
                                    "has_attempts": sess_has_attempts,
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

                    metrics.update(snapshot_cost_metrics(sessions))

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

                    # 6. Events (filter by runtime/source/status before enforcing limit, and expose latest DB seq cursor)
                    if include_events and self._has_table(db, "events"):
                        max_row = db.execute("SELECT max(seq) FROM events").fetchone()
                        if max_row and max_row[0] is not None:
                            metrics["latest_event_seq"] = int(max_row[0])
                        has_ev_session_filter = bool(rt_filter or st_filter)
                        if src_filter == "native":
                            matching_sids: list[str] = []
                            cur = ()
                        else:
                            matching_sids = (
                                [
                                    s["id"]
                                    for s in sessions
                                    if (not rt_filter or normalize_runtime(s.get("runtime")) == rt_filter)
                                    and (not st_filter or matches_bridge_status(s, st_filter))
                                ]
                                if has_ev_session_filter
                                else []
                            )
                            if has_ev_session_filter and not matching_sids:
                                cur = ()
                            elif has_ev_session_filter and len(matching_sids) <= 900:
                                placeholders = ",".join("?" for _ in matching_sids)
                                if after_seq is not None:
                                    cur = db.execute(
                                        f"SELECT seq, session_id, task_id, attempt_id, kind, data, created "
                                        f"FROM events WHERE session_id IN ({placeholders}) AND seq > ? ORDER BY seq ASC LIMIT ?",
                                        (*matching_sids, int(after_seq), limit),
                                    )
                                else:
                                    cur = db.execute(
                                        f"SELECT seq, session_id, task_id, attempt_id, kind, data, created "
                                        f"FROM events WHERE session_id IN ({placeholders}) ORDER BY seq DESC LIMIT ?",
                                        (*matching_sids, limit),
                                    )
                            elif after_seq is not None:
                                cur = db.execute(
                                    "SELECT seq, session_id, task_id, attempt_id, kind, data, created "
                                    "FROM events WHERE seq > ? ORDER BY seq ASC"
                                    + ("" if has_ev_session_filter else " LIMIT ?"),
                                    (int(after_seq),) if has_ev_session_filter else (int(after_seq), limit),
                                )
                            else:
                                cur = db.execute(
                                    "SELECT seq, session_id, task_id, attempt_id, kind, data, created "
                                    "FROM events ORDER BY seq DESC"
                                    + ("" if has_ev_session_filter else " LIMIT ?"),
                                    () if has_ev_session_filter else (limit,),
                                )
                        matching_sid_set = set(matching_sids) if has_ev_session_filter else None
                        for row in cur:
                            ev_sid = row["session_id"]
                            if matching_sid_set is not None and ev_sid not in matching_sid_set:
                                continue
                            ev_runtime = session_runtime_map.get(ev_sid)
                            if rt_filter and normalize_runtime(ev_runtime) != rt_filter:
                                continue
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
                                    "runtime": ev_runtime,
                                    "task_id": row["task_id"],
                                    "kind": kind,
                                    "summary": summary,
                                    "created": row["created"],
                                    "created_rel": _format_relative_time(row["created"]),
                                }
                            )
                            if len(events) >= limit:
                                break
                    elif self._has_table(db, "events"):
                        max_row = db.execute("SELECT max(seq) FROM events").fetchone()
                        if max_row and max_row[0] is not None:
                            metrics["latest_event_seq"] = int(max_row[0])

                finally:
                    try:
                        db.rollback()
                    except sqlite3.Error:
                        pass
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
        metrics["cost_complete"] = all(b.get("cost_complete", True) for b in runtimes.values()) and (
            all_cost_complete if db is not None else True
        )
        metrics["native_sessions"] = sum(1 for n in native_sessions if n.get("kind") != "daemon")
        metrics["native_workers"] = sum(1 for n in native_sessions if n.get("kind") == "worker")
        metrics["native_daemons"] = sum(1 for n in native_sessions if n.get("kind") == "daemon")
        metrics["total_all_active"] = metrics["active_sessions"] + metrics["native_sessions"]
        metrics.setdefault("latest_event_seq", 0)

        snapshot = MonitorSnapshot(
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
        if any(x is not None for x in (rt_filter, src_filter, st_filter, session_limit)):
            snapshot = filter_snapshot_dict(
                snapshot,
                runtime=rt_filter,
                source=src_filter,
                status=st_filter,
                session_limit=session_limit,
            )
        if self.host_registry is not None:
            provenance_source = "local_db"
            provenance_connected: bool | None = None
            provenance_detail = (
                "Snapshot collected directly from the local database; broker serving status is not reported."
            )
        elif broker_attempted:
            provenance_source = "local_db_fallback"
            provenance_connected = False
            provenance_detail = (
                "Broker RPC failed (" + str(broker_failure or "unavailable") +
                "); data was read directly from the local database. Broker connectivity is not verified."
            )
        elif broker_socket_present:
            provenance_source = "local_db_fallback"
            provenance_connected = None
            provenance_detail = (
                "A broker socket exists but could not be queried from this context; data was read directly "
                "from the local database. Broker connectivity is unknown."
            )
        else:
            provenance_source = "local_db_fallback"
            provenance_connected = False
            provenance_detail = (
                "No broker socket is available; data was read directly from the local database. "
                "Broker connectivity is unavailable."
            )
        snapshot = self._attach_provenance(
            snapshot,
            source=provenance_source,
            broker_connected=provenance_connected,
            detail=provenance_detail,
            transport=None,
            served_by=None,
        )
        return snapshot


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
    now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(snapshot.get("timestamp") or time.time()))
    compact = width < 96

    # 1. Header Banner
    title = " cli-agent-bridge Monitor "
    border_len = (width - len(title) - 2) // 2
    header_bar = f"{CYAN}┌{'─' * border_len}{BOLD}{WHITE}{title}{RESET}{CYAN}{'─' * (width - border_len - len(title) - 2)}┐{RESET}"
    lines.append(header_bar)

    host_clean = _sanitize_tui_text(coord.get("host_id") or "-", 18 if compact else 24)
    node_clean = _sanitize_tui_text(str(coord.get("node_id") or "")[:8], 8)
    coord_plain = f" Coord: {host_clean} | Node: {node_clean} | DB: {_format_bytes(coord.get('db_size'))} | {now_str}" if compact else f" Coordinator: {host_clean} | Node: {node_clean}... | DB: {_format_bytes(coord.get('db_size'))} | Time: {now_str}"
    if _display_width(coord_plain) > width - 2:
        coord_plain = _pad_display(coord_plain, width - 2)
    if compact:
        coord_info = (
            f"{CYAN}│{RESET} Coord: {BOLD}{host_clean}{RESET} | "
            f"Node: {DIM}{node_clean}{RESET} | "
            f"DB: {DIM}{_format_bytes(coord.get('db_size'))}{RESET} | {now_str}"
        )
    else:
        coord_info = (
            f"{CYAN}│{RESET} Coordinator: {BOLD}{host_clean}{RESET} | "
            f"Node: {DIM}{node_clean}...{RESET} | "
            f"DB: {DIM}{_format_bytes(coord.get('db_size'))}{RESET} | "
            f"Time: {now_str}"
        )
    pad = max(0, width - _display_width(coord_plain) - 2)
    lines.append(f"{coord_info}{' ' * pad}{CYAN}│{RESET}")

    cost_complete = metrics.get("cost_complete", True)
    total_cost_val = metrics.get("total_cost_usd")
    cost_has_attempts = metrics.get("has_attempts")
    if not isinstance(cost_has_attempts, bool):
        cost_has_attempts = any(
            isinstance(s, dict) and (s.get("has_attempts") or isinstance(s.get("attempt"), dict))
            for s in snapshot.get("sessions", [])
        )
        if not cost_has_attempts and metrics.get("total_sessions", 0) > len(snapshot.get("sessions", [])):
            cost_has_attempts = None
    known_cost_attempts = metrics.get("known_cost_attempt_sessions")
    known_coverage = isinstance(known_cost_attempts, int) and not isinstance(known_cost_attempts, bool) and known_cost_attempts >= 0
    has_cost_gaps = metrics.get("has_cost_gaps")
    if not isinstance(has_cost_gaps, bool):
        unknown_cost_sessions = metrics.get("unknown_cost_sessions")
        if isinstance(unknown_cost_sessions, int) and not isinstance(unknown_cost_sessions, bool):
            has_cost_gaps = unknown_cost_sessions > 0
        else:
            has_cost_gaps = cost_complete is False or (
                metrics.get("cost_complete") is not True
                and metrics.get("total_sessions", 0) > len(snapshot.get("sessions", []))
            )
    cost_complete = cost_complete is not False and not has_cost_gaps
    if total_cost_val is None or (
        not cost_complete
        and float(total_cost_val or 0.0) == 0.0
        and (not known_coverage or (cost_has_attempts is not False and known_cost_attempts == 0))
    ):
        cost_label = "unknown"
    elif not cost_complete:
        cost_label = f"${float(total_cost_val or 0.0):.4f}*"
    else:
        cost_label = f"${float(total_cost_val or 0.0):.4f}"
    if compact:
        metrics_str = (
            f" Agents: {GREEN}{BOLD}{metrics.get('active_sessions', 0)}{RESET}/{metrics.get('total_sessions', 0)} | "
            f"Tasks: {BLUE}{BOLD}{metrics.get('running_tasks', 0)}{RESET} | "
            f"Wait: {RED if metrics.get('waiting_questions') else GREEN}{BOLD}{metrics.get('waiting_questions', 0)}{RESET} | "
            f"Cost: {YELLOW}{cost_label}{RESET}"
        )
        plain_m = f" Agents: {metrics.get('active_sessions', 0)}/{metrics.get('total_sessions', 0)} | Tasks: {metrics.get('running_tasks', 0)} | Wait: {metrics.get('waiting_questions', 0)} | Cost: {cost_label}"
    else:
        metrics_str = (
            f" Active Agents: {GREEN}{BOLD}{metrics.get('active_sessions', 0)}{RESET}/{metrics.get('total_sessions', 0)} | "
            f"Running Tasks: {BLUE}{BOLD}{metrics.get('running_tasks', 0)}{RESET} | "
            f"Pending Approvals: {RED if metrics.get('waiting_questions') else GREEN}{BOLD}{metrics.get('waiting_questions', 0)}{RESET} | "
            f"Cost: {YELLOW}{cost_label}{RESET}"
        )
        plain_m = f" Active Agents: {metrics.get('active_sessions', 0)}/{metrics.get('total_sessions', 0)} | Running Tasks: {metrics.get('running_tasks', 0)} | Pending Approvals: {metrics.get('waiting_questions', 0)} | Cost: {cost_label}"
    if _display_width(plain_m) > width - 2:
        metrics_str = _clip_ansi_to_width(metrics_str, width - 2)
        pad_m = max(0, width - _display_width(_ANSI_SGR_RE.sub("", metrics_str)) - 2)
    else:
        pad_m = max(0, width - _display_width(plain_m) - 2)
    lines.append(f"{CYAN}│{RESET}{metrics_str}{' ' * pad_m}{CYAN}│{RESET}")
    lines.append(f"{CYAN}└{'─' * (width - 2)}┘{RESET}")

    # 2. Urgent Banner: Pending Approvals
    pending_q = snapshot.get("pending_questions", [])
    if pending_q:
        sess_revs = {
            s.get("id"): s.get("revision")
            for s in snapshot.get("sessions", [])
            if isinstance(s, dict) and s.get("id")
        }
        lines.append(f"\n{BG_YELLOW}{BOLD} ⚠ ACTION REQUIRED: {len(pending_q)} PENDING APPROVAL(S) {RESET}")
        for q in pending_q:
            q_id = _sanitize_tui_text(q.get("id", ""), 36)
            s_id = _sanitize_tui_text(q.get("session_id", ""), 48)
            s_rev = sess_revs.get(q.get("session_id"))
            rev_txt = str(s_rev) if isinstance(s_rev, int) else "<rev>"
            s_display = _sanitize_tui_text(q.get("session_name") or s_id[:8], 18 if compact else 32)
            q_tool = _sanitize_tui_text(q.get("tool", "-"), 14 if compact else 24)
            q_cmd = _sanitize_tui_text(q.get("command") or q.get("description") or "-", max(12, width - 48))
            q_rel = _sanitize_tui_text(q.get("created_rel") or "-", 12)
            lines.append(
                f"  • {BOLD}[{q_id[:8]}]{RESET} {CYAN}{s_display}{RESET}: "
                f"{BOLD}{q_tool}{RESET} | {YELLOW}{q_cmd}{RESET} ({DIM}{q_rel}{RESET})"
            )
            if width >= 195:
                lines.append(
                    f"    {DIM}↳ agent_control session_id={s_id} action=respond expected_revision={rev_txt} "
                    f'request_id=<uuid> question_id={q_id} answer={{"decision":"allow"}}{RESET}'
                )
            elif width >= 115:
                lines.append(
                    f"    {DIM}↳ agent_control session_id={s_id} action=respond question_id={q_id}{RESET}"
                )
                lines.append(
                    f'      {DIM}expected_revision={rev_txt} request_id=<uuid> answer={{"decision":"allow"}}{RESET}'
                )
            else:
                lines.append(
                    f"    {DIM}↳ agent_control action=respond expected_revision={rev_txt} request_id=<uuid>{RESET}"
                )
                lines.append(f"      {DIM}session_id={s_id}{RESET}")
                lines.append(f"      {DIM}question_id={q_id}{RESET}")
                lines.append(f'      {DIM}answer={{"decision":"allow"}}{RESET}')

    # 3. Cluster Nodes Section
    hosts = snapshot.get("hosts", [])
    total_nodes = len(hosts) + 1
    ch_w, cr_w, cs_w, ct_w, cn_w, cl_w = (14, 11, 9, 14, 12, 9) if compact else (18, 13, 12, 20, 18, 15)
    lines.append(f"\n{BOLD}{CYAN}▶ Cluster Nodes ({total_nodes}){RESET}")
    lines.append(f"{DIM}  {'Host ID':<{ch_w}} {'Role':<{cr_w}} {'State':<{cs_w}} {'Target':<{ct_w}} {'Node ID':<{cn_w}} {'Contact' if compact else 'Last Contact'}{RESET}")
    lines.append(f"  {DIM}{'─' * ch_w} {'─' * cr_w} {'─' * cs_w} {'─' * ct_w} {'─' * cn_w} {'─' * cl_w}{RESET}")
    lines.append(
        f"  {BOLD}{_pad_display(host_clean, ch_w)}{RESET} "
        f"{BLUE}{_pad_display('coordinator', cr_w)}{RESET} "
        f"{GREEN}{_pad_display('online', cs_w)}{RESET} "
        f"{_pad_display('local', ct_w)} "
        f"{DIM}{_pad_display(_sanitize_tui_text(coord.get('node_id') or '-', cn_w - 2), cn_w)}{RESET} "
        f"active"
    )
    for h in hosts:
        state = _sanitize_tui_text(h.get("state", "offline"), cs_w)
        st_color = GREEN if state == "online" else (YELLOW if state == "offline" else RED)
        h_id = _pad_display(_sanitize_tui_text(h.get("host_id", "-"), ch_w), ch_w)
        h_tgt = _pad_display(_sanitize_tui_text(h.get("ssh_target", "-"), ct_w), ct_w)
        node_id_str = _pad_display(_sanitize_tui_text(h.get("node_id") or "-", cn_w - 2), cn_w)
        rel_str = _sanitize_tui_text(h.get("last_contact_rel") or _format_relative_time(h.get("last_contact")), cl_w)
        lines.append(
            f"  {BOLD}{h_id}{RESET} "
            f"{CYAN}{_pad_display('worker', cr_w)}{RESET} "
            f"{st_color}{_pad_display(state, cs_w)}{RESET} "
            f"{h_tgt} "
            f"{DIM}{node_id_str}{RESET} "
            f"{rel_str}"
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
        rt_cost_val = float(info.get("total_cost_usd", 0.0) or 0.0)
        rt_complete = info.get("cost_complete", True) is not False and not info.get("has_cost_gaps", False)
        rt_known = info.get("known_cost_attempt_sessions")
        rt_coverage = isinstance(rt_known, int) and not isinstance(rt_known, bool) and rt_known >= 0
        if not rt_complete and rt_cost_val == 0.0 and (not rt_coverage or (info.get("has_attempts") is not False and rt_known == 0)):
            rt_cost = "unknown"
        elif not rt_complete:
            rt_cost = f"${rt_cost_val:.3f}*"
        else:
            rt_cost = f"${rt_cost_val:.3f}"
        lines.append(
            f"  {BOLD}{_pad_display(_sanitize_tui_text(disp, 10), 10)}{RESET} "
            f"{b_live:<17} "
            f"{GREEN if info.get('native_active') else DIM}{n_live:<13}{RESET} "
            f"{running_cnt:<9} "
            f"{YELLOW if (info.get('bridge_waiting') or info.get('bridge_recovery')) else DIM}{waiting_cnt:<9}{RESET} "
            f"{rt_cost}"
        )

    # 4. Active Agents & Sessions Section
    host_w = 7 if compact else 9
    rt_w = 6 if compact else 8
    model_w = 10 if compact else 14
    status_w = 10 if compact else 11
    pid_w = 6 if compact else 7
    cost_w = 7
    id_cols = 0 if compact else 9
    fixed_cols = 2 + host_w + 1 + rt_w + 1 + model_w + 1 + status_w + 1 + pid_w + 1 + cost_w + id_cols
    name_w = max(12, min(28, width - fixed_cols - 1))
    sessions = snapshot.get("sessions", [])
    lines.append(f"\n{BOLD}{CYAN}▶ Bridge Sessions ({len(sessions)}){RESET}")
    name_hdr = _pad_display("Name" if compact else "Agent / Session Name", name_w)
    hdr = (
        f"  {name_hdr} {'Host':<{host_w}} {'RT':<{rt_w}} "
        f"{'Model':<{model_w}} {'Status':<{status_w}} {'PID':<{pid_w}} {'Cost':<{cost_w}}"
    )
    sep = (
        f"  {'─' * name_w} {'─' * host_w} {'─' * rt_w} {'─' * model_w} "
        f"{'─' * status_w} {'─' * pid_w} {'─' * cost_w}"
    )
    if not compact:
        hdr += f" {'ID':<8}"
        sep += f" {'─' * 8}"
    lines.append(f"{DIM}{hdr}{RESET}")
    lines.append(f"{DIM}{sep}{RESET}")
    if not sessions:
        lines.append(f"  {DIM}(No sessions created yet){RESET}")
    else:
        for s in sessions[:20]:
            obs = _sanitize_tui_text(s.get("observed", "stopped"), status_w)
            status_txt = _pad_display("WAIT_INPUT" if s.get("waiting_question") else obs.upper() if obs in {"running", "idle"} else ("WAIT_INPUT" if obs == "waiting_input" else ("RECOVERY" if obs == "recovery_needed" else obs)), status_w)
            if s.get("waiting_question"):
                status_str = f"{BG_YELLOW}{BOLD}{status_txt}{RESET}"
            elif obs == "running":
                status_str = f"{GREEN}{BOLD}{status_txt}{RESET}"
            elif obs == "idle":
                status_str = f"{BLUE}{status_txt}{RESET}"
            elif obs == "waiting_input":
                status_str = f"{YELLOW}{status_txt}{RESET}"
            elif obs == "recovery_needed":
                status_str = f"{RED}{status_txt}{RESET}"
            else:
                status_str = f"{DIM}{status_txt}{RESET}"

            raw_pid = str(s.get("worker_pid") or "-")
            pid_plain = _pad_display(f"{raw_pid}●" if s.get("alive") else raw_pid, pid_w)
            pid_str = f"{GREEN}{pid_plain}{RESET}" if s.get("alive") else pid_plain

            att = s.get("attempt")
            cost_val = s.get("cost_usd") if s.get("cost_usd") is not None else (att.get("cost_usd") if isinstance(att, dict) else None)
            if cost_val is not None:
                suffix = "*" if s.get("cost_complete") is False else ""
                cost = f"${float(cost_val):.3f}{suffix}"
            elif s.get("has_attempts"):
                cost = "unknown"
            else:
                cost = "-"
            s_name = _pad_display(_sanitize_tui_text(s.get("name") or s["id"][:8], name_w), name_w)
            host_tag = _pad_display(_sanitize_tui_text(s.get("host_id", "local"), host_w), host_w)
            rt_tag = _pad_display(_sanitize_tui_text(s.get("display_runtime") or s.get("runtime") or "-", rt_w), rt_w)
            mod_tag = _pad_display(_sanitize_tui_text(s.get("model") or s.get("profile") or "-", model_w), model_w)
            row_line = (
                f"  {BOLD}{s_name}{RESET} "
                f"{DIM}{host_tag}{RESET} "
                f"{rt_tag} "
                f"{mod_tag} "
                f"{status_str} "
                f"{pid_str} "
                f"{_pad_display(cost, cost_w)}"
            )
            if not compact:
                sid_tag = _pad_display(_sanitize_tui_text(s.get("id", "-"), 8), 8)
                row_line += f" {DIM}{sid_tag}{RESET}"
            lines.append(row_line)

    # 4b. Native CLI Processes & Workers
    native_sessions = snapshot.get("native_sessions", [])
    n_rt_w = 6 if compact else 8
    n_mode_w = 10 if compact else 11
    n_pid_w = 7 if compact else 8
    n_model_w = 12 if compact else 16
    n_name_w = 18 if compact else min(28, max(18, width - 70))
    n_cwd_w = max(8, width - (2 + n_name_w + 1 + n_rt_w + 1 + n_mode_w + 1 + n_model_w + 1 + n_pid_w + 1))
    lines.append(f"\n{BOLD}{CYAN}▶ Native CLI Processes & Workers ({len(native_sessions)}){RESET}")
    lines.append(
        f"{DIM}  {'Title / Session' if compact else 'Title / Native Session':<{n_name_w}} {'RT':<{n_rt_w}} {'Mode':<{n_mode_w}} "
        f"{'Model':<{n_model_w}} {'PID':<{n_pid_w}} {'CWD'}{RESET}"
    )
    lines.append(
        f"  {DIM}{'─' * n_name_w} {'─' * n_rt_w} {'─' * n_mode_w} {'─' * n_model_w} {'─' * n_pid_w} {'─' * min(n_cwd_w, 28)}{RESET}"
    )
    if not native_sessions:
        lines.append(f"  {DIM}(No live native pi/claude/agy/codex processes){RESET}")
    else:
        for n in native_sessions[:20]:
            rt_tag = _pad_display(_sanitize_tui_text(n.get("display_runtime") or n.get("runtime") or "-", n_rt_w), n_rt_w)
            mode_clean = _sanitize_tui_text(n.get("mode") or "-", n_mode_w)
            m_color = GREEN if mode_clean == "headless" else (CYAN if mode_clean == "interactive" else DIM)
            n_title = _pad_display(_sanitize_tui_text(n.get("name") or "-", n_name_w), n_name_w)
            n_model = _pad_display(_sanitize_tui_text(n.get("model") or "-", n_model_w), n_model_w)
            n_cwd = _sanitize_tui_text(n.get("cwd") or "-", n_cwd_w)
            lines.append(
                f"  {BOLD}{n_title}{RESET} "
                f"{rt_tag} "
                f"{m_color}{_pad_display(mode_clean, n_mode_w)}{RESET} "
                f"{n_model} "
                f"{_pad_display(_sanitize_tui_text(n.get('pid') or '-', n_pid_w), n_pid_w)} "
                f"{DIM}{n_cwd}{RESET}"
            )

    # 5. Communications & Broadcasts Section
    broadcasts = snapshot.get("broadcasts", [])
    groups = snapshot.get("groups", [])
    b_w, g_w, p_w = (12, 12, 14) if compact else (16, 14, 18)
    b_prev_w = max(12, width - (2 + b_w + 1 + g_w + 1 + p_w + 1))
    lines.append(f"\n{BOLD}{CYAN}▶ Communications & Groups ({len(groups)} groups, {len(broadcasts)} broadcasts){RESET}")
    if broadcasts:
        lines.append(f"{DIM}  {'Broadcast ID':<{b_w}} {'Group':<{g_w}} {'Progress':<{p_w}} {'Message Preview'}{RESET}")
        lines.append(f"  {DIM}{'─' * b_w} {'─' * g_w} {'─' * p_w} {'─' * min(b_prev_w, 40)}{RESET}")
        for b in broadcasts[:5]:
            prog_txt = f"{b['received']}/{b['total']} recv"
            if b.get("uncertain"):
                prog_txt += f" ({b['uncertain']} uncert)"
            b_id = _pad_display(_sanitize_tui_text(b.get("broadcast_id", ""), b_w), b_w)
            g_id = _pad_display(_sanitize_tui_text(b.get("group_id", ""), g_w), g_w)
            preview = _sanitize_tui_text(b.get("message_preview", ""), b_prev_w)
            lines.append(
                f"  {b_id} "
                f"{g_id} "
                f"{GREEN}{_pad_display(prog_txt, p_w)}{RESET} "
                f"{preview}"
            )
    else:
        lines.append(f"  {DIM}(No broadcast activity recorded){RESET}")

    # 6. Recent Event Tail
    events = snapshot.get("events", [])
    e_sq, e_tm, e_kd, e_sn = (5, 8, 14, 12) if compact else (6, 10, 18, 16)
    e_sm = max(12, width - (2 + e_sq + 1 + e_tm + 1 + e_kd + 1 + e_sn + 1))
    lines.append(f"\n{BOLD}{CYAN}▶ Live Events (Recent {len(events)}){RESET}")
    lines.append(f"{DIM}  {'Seq':<{e_sq}} {'Time':<{e_tm}} {'Kind':<{e_kd}} {'Session':<{e_sn}} {'Detail / Summary'}{RESET}")
    lines.append(f"  {DIM}{'─' * e_sq} {'─' * e_tm} {'─' * e_kd} {'─' * e_sn} {'─' * min(e_sm, 45)}{RESET}")
    if not events:
        lines.append(f"  {DIM}(No events yet){RESET}")
    else:
        for e in events[:8]:
            kind = _sanitize_tui_text(e["kind"], e_kd)
            k_color = GREEN if "finish" in kind or "complete" in kind else (YELLOW if "input" in kind or "wait" in kind else BLUE)
            s_name = _sanitize_tui_text(e.get("session_name") or str(e.get("session_id") or "-")[:e_sn], e_sn)
            summary = _sanitize_tui_text(e.get("summary", ""), e_sm)
            lines.append(
                f"  {DIM}{_pad_display(str(e['seq']), e_sq)}{RESET} "
                f"{_pad_display(_sanitize_tui_text(e.get('created_rel') or '-', e_tm), e_tm)} "
                f"{k_color}{_pad_display(kind, e_kd)}{RESET} "
                f"{CYAN}{_pad_display(s_name, e_sn)}{RESET} "
                f"{summary}"
            )

    return "\n".join(_clip_ansi_to_width(line, width) for line in lines)


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
