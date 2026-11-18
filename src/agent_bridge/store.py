from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import unicodedata
import uuid
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

from . import artifacts
from .codex_parent import read_resume_config
from .config import Config, Profile
from .errors import BridgeError
from .parent_usage import meter_usage, refresh_meter, start_meter
from .peer_protocol import MAX_FRAME_BYTES, DeliverySource, PeerErrorCode, compute_stream_id, unwrap_peer_event


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def identifier():
    return str(uuid.uuid4())


def validate_session_name(name: str | None) -> str | None:
    """Return the canonical trimmed session name, or None when no name was supplied."""
    if name is None:
        return None
    if not isinstance(name, str):
        raise BridgeError("invalid_request", "name must be a string")
    value = name.strip()
    if not 1 <= len(value) <= 128:
        raise BridgeError("invalid_request", "name must be 1 to 128 characters after trimming whitespace")
    if any(character in "\x00\n\r" or unicodedata.category(character) == "Cc" for character in value):
        raise BridgeError("invalid_request", "name must not contain NUL, newline, or control characters")
    return value


def task_input(instruction, reservation, dependencies=None):
    if not isinstance(instruction, str) or not instruction.strip() or len(instruction) > 200_000:
        raise BridgeError("invalid_instruction", "instruction must be nonempty and at most 200000 characters")
    if type(reservation) is not int or reservation < 0:
        raise BridgeError("invalid_budget", "reservation must be a nonnegative token count")
    if dependencies is not None and (
        not isinstance(dependencies, list) or any(not isinstance(value, str) or not value for value in dependencies)
    ):
        raise BridgeError("invalid_dependencies", "dependencies must be a list of task IDs")
    return list(dict.fromkeys(dependencies or []))


def validate_task_timeout(value):
    if value is None:
        return None
    try:
        valid = type(value) in {int, float} and math.isfinite(value) and value > 0
    except OverflowError:
        valid = False
    if not valid:
        raise BridgeError("invalid_task_timeout", "task_timeout must be a finite positive number of seconds")
    return value


def check_task_timeout_support(session, details):
    if details.get("task_timeout") is not None and session["worker_pid"] is not None and not session["worker_task_timeout"]:
        raise BridgeError("worker_upgrade_required", "This worker does not support per-task timeouts; keep its current work and use a worker running the updated code")


def check_retry(task, instruction):
    if not (task["status"] in {"failed", "interrupted", "cancelled"} or task["validation"] == "rejected"):
        raise BridgeError("invalid_retry", "Retry requires a settled failed/interrupted/cancelled or rejected task")
    if instruction != task["instruction"]:
        raise BridgeError("invalid_retry", "Retry preserves the task instruction; use a new task to change its objective")


WORKSPACE_SCHEMA = """
CREATE TABLE IF NOT EXISTS workspace_transfers (
    workspace_id TEXT PRIMARY KEY, authority_ref TEXT NOT NULL,
    project_id TEXT NOT NULL, project_config_digest TEXT NOT NULL,
    project_root TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
    source_base_commit TEXT, delivery_mode TEXT NOT NULL,
    status TEXT NOT NULL, input_digest TEXT,
    metadata TEXT NOT NULL, ready_path TEXT, attached_session_id TEXT REFERENCES sessions(id),
    revision INTEGER NOT NULL DEFAULT 0, error TEXT,
    created REAL NOT NULL, updated REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS workspace_chunks (
    workspace_id TEXT NOT NULL REFERENCES workspace_transfers(workspace_id),
    blob_sha256 TEXT NOT NULL, offset INTEGER NOT NULL CHECK(offset>=0),
    size INTEGER NOT NULL CHECK(size>0), chunk_sha256 TEXT NOT NULL,
    stored_path TEXT NOT NULL,
    PRIMARY KEY(workspace_id,blob_sha256,offset)
);
CREATE TABLE IF NOT EXISTS workspace_blobs (
    sha256 TEXT PRIMARY KEY, size INTEGER NOT NULL CHECK(size>=0),
    stored_path TEXT NOT NULL, verified_at REAL NOT NULL
);
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
 id TEXT PRIMARY KEY, profile TEXT NOT NULL, cwd TEXT NOT NULL, access TEXT NOT NULL,
 parent TEXT, native_id TEXT, desired TEXT NOT NULL DEFAULT 'open', observed TEXT NOT NULL DEFAULT 'stopped',
 revision INTEGER NOT NULL DEFAULT 0, worker_pid INTEGER, worker_start TEXT,
 generation TEXT, error TEXT, created REAL NOT NULL,
 worker_task_timeout INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS session_names (
 session_id TEXT PRIMARY KEY, name TEXT NOT NULL, created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id),
 instruction TEXT NOT NULL, dependencies TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
 validation TEXT NOT NULL DEFAULT 'pending', active_attempt TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0,
 reservation INTEGER NOT NULL, result TEXT, error TEXT, created REAL NOT NULL,
 details TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS attempts (
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), session_id TEXT NOT NULL REFERENCES sessions(id),
 status TEXT NOT NULL, native_turn_id TEXT, result TEXT, usage TEXT, started REAL, finished REAL
);
CREATE TABLE IF NOT EXISTS requests (
 id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS api_requests (
 id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, response TEXT
);
CREATE TABLE IF NOT EXISTS events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, task_id TEXT, attempt_id TEXT,
 kind TEXT NOT NULL, data TEXT NOT NULL, created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), attempt_id TEXT REFERENCES attempts(id),
 kind TEXT NOT NULL, metadata TEXT NOT NULL, created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS questions (
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL, task_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
 data TEXT NOT NULL, answer TEXT, status TEXT NOT NULL DEFAULT 'pending', created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS reviews (
 task_id TEXT NOT NULL, attempt_id TEXT NOT NULL, decision TEXT NOT NULL, evidence TEXT NOT NULL, created REAL NOT NULL,
 PRIMARY KEY(task_id,attempt_id)
);
CREATE TABLE IF NOT EXISTS parents (
 id TEXT PRIMARY KEY, runtime TEXT NOT NULL, native_id TEXT NOT NULL, cwd TEXT NOT NULL,
 desired TEXT NOT NULL DEFAULT 'open', revision INTEGER NOT NULL DEFAULT 0, notify INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS parent_usage (
 parent_id TEXT PRIMARY KEY REFERENCES parents(id), state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quota_cooldowns (
 quota_group TEXT PRIMARY KEY, blocked_until REAL, failures INTEGER NOT NULL, revision INTEGER NOT NULL,
 reason TEXT NOT NULL, updated REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
 event_seq INTEGER PRIMARY KEY REFERENCES events(seq), parent_id TEXT NOT NULL REFERENCES parents(id),
 status TEXT NOT NULL DEFAULT 'pending', receipt TEXT, error TEXT,
 native_status TEXT NOT NULL DEFAULT 'unknown', native_turn_id TEXT, queue_cleanup TEXT,
 cleanup_last_attempt REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS session_groups (
 id TEXT PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 0,
 deleted INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL,
 updated_at REAL, last_activity_at REAL, deleted_at REAL, delete_reason TEXT
);
CREATE TABLE IF NOT EXISTS group_prune_runs (
 id TEXT PRIMARY KEY,
 request_id TEXT NOT NULL UNIQUE,
 fingerprint TEXT NOT NULL,
 cutoff REAL NOT NULL,
 status TEXT NOT NULL,
 created REAL NOT NULL,
 finished REAL
);
CREATE TABLE IF NOT EXISTS group_prune_items (
 run_id TEXT NOT NULL REFERENCES group_prune_runs(id),
 group_id TEXT NOT NULL REFERENCES session_groups(id),
 expected_revision INTEGER NOT NULL,
 member_snapshot TEXT NOT NULL,
 check_result TEXT NOT NULL,
 status TEXT NOT NULL,
 reason TEXT,
 PRIMARY KEY(run_id, group_id)
);
CREATE TABLE IF NOT EXISTS session_group_members (
 group_id TEXT NOT NULL REFERENCES session_groups(id), session_id TEXT NOT NULL REFERENCES sessions(id),
 PRIMARY KEY(group_id,session_id)
);
CREATE TABLE IF NOT EXISTS native_group_members (
 group_id TEXT NOT NULL REFERENCES session_groups(id), runtime TEXT NOT NULL, native_id TEXT NOT NULL,
 cwd TEXT NOT NULL, label TEXT NOT NULL,
 PRIMARY KEY(group_id,runtime,native_id)
);
CREATE TABLE IF NOT EXISTS native_broadcast_deliveries (
 id TEXT PRIMARY KEY, broadcast_id TEXT NOT NULL, group_id TEXT NOT NULL REFERENCES session_groups(id),
 runtime TEXT NOT NULL, native_id TEXT NOT NULL, cwd TEXT NOT NULL, message TEXT NOT NULL,
 status TEXT NOT NULL, receipt TEXT, native_turn_id TEXT, processing_status TEXT, error TEXT, created REAL NOT NULL, last_attempt REAL NOT NULL DEFAULT 0,
 UNIQUE(broadcast_id,runtime,native_id)
);
CREATE INDEX IF NOT EXISTS native_broadcast_pending ON native_broadcast_deliveries(status,last_attempt);
CREATE INDEX IF NOT EXISTS task_queue ON tasks(status,created);
CREATE INDEX IF NOT EXISTS event_session ON events(session_id,seq);
CREATE TABLE IF NOT EXISTS remote_group_members (
 group_id TEXT NOT NULL REFERENCES session_groups(id),
 host_id TEXT NOT NULL, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 target_kind TEXT NOT NULL CHECK(target_kind IN ('bridge','native')),
 runtime TEXT NOT NULL DEFAULT '', target_id TEXT NOT NULL,
 cwd TEXT NOT NULL DEFAULT '', label TEXT NOT NULL DEFAULT '',
 joined_revision INTEGER NOT NULL, verified_at REAL NOT NULL,
 verification_receipt TEXT NOT NULL, created REAL NOT NULL,
 CHECK((target_kind='bridge' AND runtime='') OR
       (target_kind='native' AND runtime IN ('codex','antigravity'))),
 PRIMARY KEY(group_id,host_id,node_id,state_generation,target_kind,runtime,target_id)
);
CREATE TABLE IF NOT EXISTS group_broadcasts (
 id TEXT PRIMARY KEY, group_id TEXT NOT NULL REFERENCES session_groups(id),
 group_revision INTEGER NOT NULL, sender_ref TEXT, message TEXT NOT NULL,
 recipient_snapshot TEXT NOT NULL, request_key TEXT UNIQUE NOT NULL,
 created REAL NOT NULL, settled_at REAL
);
CREATE TABLE IF NOT EXISTS broadcast_recipients (
 broadcast_id TEXT NOT NULL REFERENCES group_broadcasts(id),
 recipient_key TEXT NOT NULL, target_ref TEXT NOT NULL,
 delivery_id TEXT UNIQUE, task_id TEXT, status TEXT NOT NULL,
 send_status TEXT, processing_status TEXT, error TEXT,
 updated REAL NOT NULL,
 PRIMARY KEY(broadcast_id,recipient_key)
);
CREATE TABLE IF NOT EXISTS target_streams (
 stream_id TEXT PRIMARY KEY, authority_ref TEXT NOT NULL, target_ref TEXT NOT NULL,
 next_seq INTEGER NOT NULL DEFAULT 1 CHECK(next_seq>=1),
 released_seq INTEGER NOT NULL DEFAULT 0 CHECK(released_seq>=0),
 CHECK(next_seq>released_seq)
);
CREATE TABLE IF NOT EXISTS peer_outbox (
 delivery_id TEXT PRIMARY KEY,
 broadcast_id TEXT REFERENCES group_broadcasts(id),
 group_id TEXT REFERENCES session_groups(id), group_revision INTEGER,
 stream_id TEXT NOT NULL REFERENCES target_streams(stream_id),
 target_seq INTEGER NOT NULL CHECK(target_seq>=1),
 host_id TEXT NOT NULL, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 target_kind TEXT NOT NULL, runtime TEXT NOT NULL DEFAULT '', target_id TEXT NOT NULL,
 operation_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, fingerprint TEXT NOT NULL,
 seq_kind TEXT NOT NULL CHECK(seq_kind IN ('message','tombstone')),
 status TEXT NOT NULL DEFAULT 'pending',
 send_status TEXT, processing_status TEXT, receipt TEXT, error TEXT,
 revision INTEGER NOT NULL DEFAULT 0, cancel_requested INTEGER NOT NULL DEFAULT 0,
 blocked_by TEXT, created REAL NOT NULL, updated REAL NOT NULL,
 last_attempt REAL NOT NULL DEFAULT 0,
 UNIQUE(stream_id,target_seq)
);
CREATE INDEX IF NOT EXISTS peer_outbox_head ON peer_outbox(stream_id,target_seq,status);
CREATE TABLE IF NOT EXISTS delivery_resolutions (
 id TEXT PRIMARY KEY, delivery_id TEXT NOT NULL REFERENCES peer_outbox(delivery_id),
 request_id TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL,
 group_revision INTEGER NOT NULL, delivery_revision INTEGER NOT NULL,
 remote_revision INTEGER NOT NULL, request_json TEXT NOT NULL,
 resolution TEXT NOT NULL CHECK(resolution IN ('received','not_received')),
 evidence TEXT NOT NULL, actor TEXT NOT NULL, status TEXT NOT NULL,
 remote_result TEXT, created REAL NOT NULL, updated REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pulled_request_receipts (
 host_id TEXT NOT NULL, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 operation_id TEXT NOT NULL, source_seq INTEGER NOT NULL,
 fingerprint TEXT NOT NULL, outcome TEXT NOT NULL,
 result TEXT NOT NULL, acked_at REAL, created REAL NOT NULL,
 PRIMARY KEY(host_id,node_id,state_generation,operation_id)
);
CREATE TABLE IF NOT EXISTS peer_operations (
 operation_id TEXT PRIMARY KEY, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 fingerprint TEXT NOT NULL, method TEXT NOT NULL, params TEXT NOT NULL, status TEXT NOT NULL,
 result TEXT, error TEXT, created REAL NOT NULL, updated REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS peer_operation_identity ON peer_operations(node_id,state_generation,operation_id);
CREATE TABLE IF NOT EXISTS peer_inbox (
 delivery_id TEXT PRIMARY KEY,
 host_id TEXT NOT NULL, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 target_seq INTEGER NOT NULL,
 seq_kind TEXT NOT NULL, operation_id TEXT NOT NULL,
 payload TEXT NOT NULL, status TEXT NOT NULL, receipt TEXT, created REAL NOT NULL,
 ordering_mode TEXT NOT NULL DEFAULT 'legacy' CHECK(ordering_mode IN ('legacy','bootstrap','ordered')),
 stream_id TEXT,
 authority_ref TEXT, target_kind TEXT, runtime TEXT, target_id TEXT,
 revision INTEGER NOT NULL DEFAULT 0,
 ack_seq INTEGER,
 CHECK(ordering_mode='legacy' OR
       (ordering_mode='bootstrap' AND stream_id IS NULL AND target_seq=0) OR
       (ordering_mode='ordered' AND stream_id IS NOT NULL AND target_seq>=1))
);
CREATE TABLE IF NOT EXISTS incoming_streams (
 stream_id TEXT PRIMARY KEY, authority_ref TEXT NOT NULL, target_ref TEXT NOT NULL,
 admitted_seq INTEGER NOT NULL DEFAULT 0 CHECK(admitted_seq>=0)
);
CREATE TABLE IF NOT EXISTS ordered_targets (
 target_kind TEXT NOT NULL, runtime TEXT NOT NULL DEFAULT '', target_id TEXT NOT NULL,
 authority_ref TEXT NOT NULL, enabled_at REAL NOT NULL,
 PRIMARY KEY(target_kind,runtime,target_id)
);
CREATE TABLE IF NOT EXISTS peer_commit_log (
 ack_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 mutation_key TEXT NOT NULL UNIQUE, method TEXT NOT NULL, created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS native_send_queue (
 id TEXT PRIMARY KEY, runtime TEXT NOT NULL, native_id TEXT NOT NULL,
 local_seq INTEGER NOT NULL,
 delivery_source TEXT NOT NULL,
 delivery_id TEXT NOT NULL, message TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending',
 receipt TEXT, native_turn_id TEXT, processing_status TEXT,
 error TEXT, revision INTEGER NOT NULL DEFAULT 0,
 created REAL NOT NULL, last_attempt REAL NOT NULL DEFAULT 0,
 UNIQUE(runtime,native_id,local_seq), UNIQUE(delivery_source,delivery_id)
);
CREATE INDEX IF NOT EXISTS native_send_queue_head ON native_send_queue(runtime,native_id,local_seq,status);
CREATE TABLE IF NOT EXISTS native_target_counters (
 runtime TEXT NOT NULL, native_id TEXT NOT NULL, next_seq INTEGER NOT NULL DEFAULT 1,
 PRIMARY KEY(runtime,native_id)
);
CREATE TABLE IF NOT EXISTS node_outbox (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT UNIQUE NOT NULL,
 group_id TEXT, native_id TEXT, method TEXT NOT NULL, payload TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', created REAL NOT NULL,
 pulled_at REAL, acked_at REAL,
 binding_id TEXT, request_id TEXT, sender_ref TEXT, authority_ref TEXT,
 authorization_revision INTEGER, fingerprint TEXT, result TEXT, outcome TEXT
);
CREATE INDEX IF NOT EXISTS node_outbox_pending ON node_outbox(status,seq);
CREATE TABLE IF NOT EXISTS peer_event_mirrors (
 host_id TEXT NOT NULL, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 remote_seq INTEGER NOT NULL, local_seq INTEGER NOT NULL UNIQUE REFERENCES events(seq),
 fingerprint TEXT NOT NULL, created REAL NOT NULL,
 origin_node_id TEXT, origin_state_generation TEXT, origin_seq INTEGER, origin_fingerprint TEXT,
 PRIMARY KEY(host_id,node_id,state_generation,remote_seq)
);
CREATE TABLE IF NOT EXISTS peer_event_cursors (
 host_id TEXT NOT NULL, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 after_seq INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(host_id,node_id,state_generation)
);
CREATE TABLE IF NOT EXISTS peer_parent_routes (
 host_id TEXT NOT NULL, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 child_id TEXT NOT NULL, parent_ref TEXT NOT NULL, created REAL NOT NULL,
 PRIMARY KEY(host_id,node_id,state_generation,child_id)
);
CREATE INDEX IF NOT EXISTS peer_parent_routes_node
 ON peer_parent_routes(node_id,state_generation,child_id);
CREATE TABLE IF NOT EXISTS mirrored_parent_notifications (
 local_seq INTEGER PRIMARY KEY REFERENCES events(seq), parent_ref TEXT NOT NULL,
 operation_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL,
 receipt TEXT, updated REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS member_bindings (
 id TEXT PRIMARY KEY, token_digest TEXT NOT NULL UNIQUE,
 sender_ref TEXT NOT NULL, authority_ref TEXT NOT NULL, group_id TEXT NOT NULL,
 allowed_methods TEXT NOT NULL, authorization_revision INTEGER NOT NULL,
 pid INTEGER NOT NULL, process_start TEXT NOT NULL,
 expires_at REAL NOT NULL, revoked_at REAL, created REAL NOT NULL
);
""" + WORKSPACE_SCHEMA


class Store:
    PEER_MIRROR_MIGRATION_BATCH = 500

    def __init__(self, state: Path):
        self.state = state
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = state / "bridge.sqlite"
        with self.connection(timeout=30) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)
            db.execute("BEGIN IMMEDIATE")
            if "processing_status" not in {row[1] for row in db.execute("PRAGMA table_info(native_broadcast_deliveries)")}:
                db.execute("ALTER TABLE native_broadcast_deliveries ADD COLUMN processing_status TEXT")
            if "notify" not in {row[1] for row in db.execute("PRAGMA table_info(parents)")}:
                db.execute("ALTER TABLE parents ADD COLUMN notify INTEGER NOT NULL DEFAULT 1")
            if "workspace" not in {row[1] for row in db.execute("PRAGMA table_info(sessions)")}:
                db.execute("ALTER TABLE sessions ADD COLUMN workspace TEXT NOT NULL DEFAULT '{}'")
            if "process_scope" not in {row[1] for row in db.execute("PRAGMA table_info(sessions)")}:
                db.execute("ALTER TABLE sessions ADD COLUMN process_scope TEXT")
            if "worker_task_timeout" not in {row[1] for row in db.execute("PRAGMA table_info(sessions)")}:
                db.execute("ALTER TABLE sessions ADD COLUMN worker_task_timeout INTEGER NOT NULL DEFAULT 0")
            if "worker_version" not in {row[1] for row in db.execute("PRAGMA table_info(sessions)")}:
                db.execute("ALTER TABLE sessions ADD COLUMN worker_version TEXT")
            if "resume_config_file" not in {row[1] for row in db.execute("PRAGMA table_info(parents)")}:
                db.execute("ALTER TABLE parents ADD COLUMN resume_config_file TEXT")
            if "first_output" not in {row[1] for row in db.execute("PRAGMA table_info(attempts)")}:
                db.execute("ALTER TABLE attempts ADD COLUMN first_output REAL")
            if "expires" not in {row[1] for row in db.execute("PRAGMA table_info(questions)")}:
                db.execute("ALTER TABLE questions ADD COLUMN expires REAL")
            if "reservation" not in {row[1] for row in db.execute("PRAGMA table_info(attempts)")}:
                db.execute("ALTER TABLE attempts ADD COLUMN reservation INTEGER NOT NULL DEFAULT 0")
                db.execute("UPDATE attempts SET reservation=(SELECT reservation FROM tasks WHERE id=attempts.task_id)")
            if "last_attempt" not in {row[1] for row in db.execute("PRAGMA table_info(deliveries)")}:
                db.execute("ALTER TABLE deliveries ADD COLUMN last_attempt REAL NOT NULL DEFAULT 0")
            delivery_columns = {row[1] for row in db.execute("PRAGMA table_info(deliveries)")}
            for column, ddl in (
                ("native_status", "TEXT NOT NULL DEFAULT 'unknown'"),
                ("native_turn_id", "TEXT"),
                ("queue_cleanup", "TEXT"),
                ("cleanup_last_attempt", "REAL NOT NULL DEFAULT 0"),
            ):
                if column not in delivery_columns:
                    db.execute(f"ALTER TABLE deliveries ADD COLUMN {column} {ddl}")
            # An older Broker may ACK after a newer worker has already added these columns.
            db.execute(
                "UPDATE deliveries SET queue_cleanup='pending' WHERE status='processed' AND queue_cleanup IS NULL"
            )
            group_columns = {row[1] for row in db.execute("PRAGMA table_info(session_groups)")}
            for column, ddl in (
                ("updated_at", "REAL"),
                ("last_activity_at", "REAL"),
                ("deleted_at", "REAL"),
                ("delete_reason", "TEXT"),
            ):
                if column not in group_columns:
                    db.execute(f"ALTER TABLE session_groups ADD COLUMN {column} {ddl}")
            if "last_activity_at" not in group_columns:
                db.execute(
                    """UPDATE session_groups SET last_activity_at=max(created, COALESCE((SELECT max(t) FROM (
                        SELECT e.created AS t FROM events e
                        WHERE e.kind IN ('group_changed','group_broadcast') AND json_valid(e.data)
                          AND json_extract(e.data,'$.group_id')=session_groups.id
                        UNION ALL SELECT b.created FROM group_broadcasts b WHERE b.group_id=session_groups.id
                        UNION ALL SELECT r.updated FROM broadcast_recipients r
                          JOIN group_broadcasts b ON b.id=r.broadcast_id WHERE b.group_id=session_groups.id
                    )), created))"""
                )
            db.execute("UPDATE session_groups SET last_activity_at=created WHERE last_activity_at IS NULL")
            db.execute("UPDATE session_groups SET updated_at=created WHERE updated_at IS NULL")
            db.execute("CREATE INDEX IF NOT EXISTS session_groups_prune ON session_groups(deleted, last_activity_at)")
            self._migrate_node_outbox(db)
            self._migrate_peer_inbox(db)
            self._migrate_native_send_queue(db)
            self._migrate_peer_event_mirrors_columns(db)
            self._migrate_workspace_tables(db)
            if "request_id" not in {row[1] for row in db.execute("PRAGMA table_info(peer_operations)")}:
                db.execute("ALTER TABLE peer_operations ADD COLUMN request_id TEXT")
            db.execute("CREATE INDEX IF NOT EXISTS peer_operations_request ON peer_operations(request_id)")
            self._initialize_commit_log(db)
        self._finish_peer_event_mirrors_migration()
        self.path.chmod(0o600)

    @classmethod
    def _origin_mirror_values(cls, row, *, force: bool = False):
        if force or row["origin_node_id"] is None:
            data = cls._decode_json(row["data"])
            if isinstance(data, dict):
                synthetic = {
                    "seq": row["remote_seq"],
                    "type": row["kind"] or "event",
                    "ref": {
                        "host_id": row["host_id"],
                        "node_id": row["node_id"],
                        "state_generation": row["state_generation"],
                        "local_id": str(row["remote_seq"]),
                    },
                    "payload": data,
                    "created": float(row["created"] or 0.0),
                }
                origin_event, wrapped = unwrap_peer_event(synthetic)
                if wrapped:
                    return (
                        origin_event["ref"]["node_id"],
                        origin_event["ref"]["state_generation"],
                        origin_event["seq"],
                        hashlib.sha256(encode(origin_event).encode("utf-8")).hexdigest(),
                    )
        return (row["node_id"], row["state_generation"], row["remote_seq"], row["fingerprint"])

    @classmethod
    def _migrate_peer_event_mirrors_batch(cls, db, limit: int | None = None) -> int:
        """Backfill up to `limit` legacy mirror rows without loading large event logs into memory."""
        batch_limit = cls.PEER_MIRROR_MIGRATION_BATCH if limit is None else limit
        if batch_limit <= 0:
            raise ValueError("limit must be a positive integer")
        pending = db.execute(
            "SELECT m.host_id,m.node_id,m.state_generation,m.remote_seq,m.local_seq,"
            "m.fingerprint,m.origin_node_id,e.kind,e.data,e.created "
            "FROM peer_event_mirrors m LEFT JOIN events e ON e.seq=m.local_seq "
            "WHERE m.origin_node_id IS NULL OR ("
            "m.origin_fingerprint IS NULL AND m.origin_node_id=m.node_id "
            "AND m.origin_state_generation=m.state_generation AND m.origin_seq=m.remote_seq"
            ") ORDER BY m.local_seq DESC LIMIT ?",
            (batch_limit,),
        ).fetchall()
        if not pending:
            return 0
        updates = [
            (
                *cls._origin_mirror_values(row),
                row["host_id"],
                row["node_id"],
                row["state_generation"],
                row["remote_seq"],
            )
            for row in pending
        ]
        db.executemany(
            "UPDATE peer_event_mirrors SET origin_node_id=?,origin_state_generation=?,origin_seq=?,origin_fingerprint=? "
            "WHERE host_id=? AND node_id=? AND state_generation=? AND remote_seq=?",
            updates,
        )
        return len(updates)

    def migrate_peer_event_mirrors_batch(self, limit: int | None = None) -> int:
        """Backfill one bounded batch of legacy mirror origin columns in its own transaction."""
        with self.connection(write=True) as db:
            return self._migrate_peer_event_mirrors_batch(db, limit=limit)

    @staticmethod
    def _migrate_peer_event_mirrors_columns(db):
        columns = {row[1] for row in db.execute("PRAGMA table_info(peer_event_mirrors)")}
        additions = (
            ("origin_node_id", "TEXT"),
            ("origin_state_generation", "TEXT"),
            ("origin_seq", "INTEGER"),
            ("origin_fingerprint", "TEXT"),
        )
        for name, sql_type in additions:
            if name not in columns:
                db.execute(f"ALTER TABLE peer_event_mirrors ADD COLUMN {name} {sql_type}")

    def _finish_peer_event_mirrors_migration(self, limit: int | None = None):
        """Ensure peer mirror indexes and bounded startup backfill run in separate short transactions."""
        indexes = {
            "peer_event_mirrors_origin": (
                "CREATE INDEX IF NOT EXISTS peer_event_mirrors_origin "
                "ON peer_event_mirrors(origin_node_id,origin_state_generation,origin_seq)"
            ),
            "peer_event_mirrors_missing_origin": (
                "CREATE INDEX IF NOT EXISTS peer_event_mirrors_missing_origin "
                "ON peer_event_mirrors(local_seq DESC) "
                "WHERE origin_node_id IS NULL OR ("
                "origin_fingerprint IS NULL AND origin_node_id=node_id "
                "AND origin_state_generation=state_generation AND origin_seq=remote_seq)"
            ),
        }
        with self.connection() as db:
            existing = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='peer_event_mirrors'"
                )
            }
            has_pending = (
                db.execute(
                    "SELECT 1 FROM peer_event_mirrors WHERE origin_node_id IS NULL OR ("
                    "origin_fingerprint IS NULL AND origin_node_id=node_id "
                    "AND origin_state_generation=state_generation AND origin_seq=remote_seq) LIMIT 1"
                ).fetchone()
                is not None
            )
        for name, ddl in indexes.items():
            if name not in existing:
                with self.connection(write=True, timeout=30) as db:
                    db.execute(ddl)
        if not has_pending:
            return
        remaining = self.PEER_MIRROR_MIGRATION_BATCH if limit is None else limit
        while remaining > 0:
            moved = self.migrate_peer_event_mirrors_batch(limit=min(50, remaining))
            if not moved:
                break
            remaining -= moved

    @classmethod
    def _migrate_peer_event_mirrors(cls, db, limit: int | None = None):
        cls._migrate_peer_event_mirrors_columns(db)
        db.execute(
            "CREATE INDEX IF NOT EXISTS peer_event_mirrors_origin "
            "ON peer_event_mirrors(origin_node_id,origin_state_generation,origin_seq)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS peer_event_mirrors_missing_origin "
            "ON peer_event_mirrors(local_seq DESC) "
            "WHERE origin_node_id IS NULL OR ("
            "origin_fingerprint IS NULL AND origin_node_id=node_id "
            "AND origin_state_generation=state_generation AND origin_seq=remote_seq)"
        )
        cls._migrate_peer_event_mirrors_batch(db, limit=limit)

    @staticmethod
    def _migrate_workspace_tables(db):
        for statement in WORKSPACE_SCHEMA.split(";"):
            if statement.strip():
                db.execute(statement)

    @staticmethod
    def _migrate_node_outbox(db):
        columns = {row[1] for row in db.execute("PRAGMA table_info(node_outbox)")}
        additions = (
            ("binding_id", "TEXT"),
            ("request_id", "TEXT"),
            ("sender_ref", "TEXT"),
            ("authority_ref", "TEXT"),
            ("authorization_revision", "INTEGER"),
            ("fingerprint", "TEXT"),
            ("result", "TEXT"),
            ("outcome", "TEXT"),
        )
        for name, sql_type in additions:
            if name not in columns:
                db.execute(f"ALTER TABLE node_outbox ADD COLUMN {name} {sql_type}")
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS node_outbox_member_request "
            "ON node_outbox(binding_id,request_id) WHERE binding_id IS NOT NULL"
        )

    @staticmethod
    def _migrate_peer_inbox(db):
        columns = {row[1] for row in db.execute("PRAGMA table_info(peer_inbox)")}
        if "ordering_mode" not in columns:
            db.execute("DROP TABLE IF EXISTS peer_inbox_c")
            db.execute(
                "CREATE TABLE peer_inbox_c ("
                "delivery_id TEXT PRIMARY KEY,"
                "host_id TEXT NOT NULL, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,"
                "target_seq INTEGER NOT NULL,"
                "seq_kind TEXT NOT NULL, operation_id TEXT NOT NULL,"
                "payload TEXT NOT NULL, status TEXT NOT NULL, receipt TEXT, created REAL NOT NULL,"
                "ordering_mode TEXT NOT NULL DEFAULT 'legacy' "
                "CHECK(ordering_mode IN ('legacy','bootstrap','ordered')),"
                "stream_id TEXT,"
                "authority_ref TEXT, target_kind TEXT, runtime TEXT, target_id TEXT,"
                "revision INTEGER NOT NULL DEFAULT 0,"
                "ack_seq INTEGER,"
                "CHECK(ordering_mode='legacy' OR "
                "(ordering_mode='bootstrap' AND stream_id IS NULL AND target_seq=0) OR "
                "(ordering_mode='ordered' AND stream_id IS NOT NULL AND target_seq>=1)))"
            )
            db.execute(
                "INSERT INTO peer_inbox_c("
                "delivery_id,host_id,node_id,state_generation,target_seq,seq_kind,operation_id,"
                "payload,status,receipt,created,ordering_mode,stream_id,authority_ref,target_kind,"
                "runtime,target_id,revision,ack_seq) "
                "SELECT delivery_id,host_id,node_id,state_generation,target_seq,seq_kind,operation_id,"
                "payload,status,receipt,created,'legacy',NULL,NULL,NULL,NULL,NULL,0,NULL "
                "FROM peer_inbox"
            )
            db.execute("DROP TABLE peer_inbox")
            db.execute("ALTER TABLE peer_inbox_c RENAME TO peer_inbox")
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS peer_inbox_legacy_seq "
            "ON peer_inbox(host_id,node_id,state_generation,target_seq) WHERE ordering_mode='legacy'"
        )
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS peer_inbox_ordered_seq "
            "ON peer_inbox(stream_id,target_seq) WHERE ordering_mode='ordered'"
        )
        db.execute("CREATE INDEX IF NOT EXISTS peer_inbox_operation ON peer_inbox(operation_id)")

    @staticmethod
    def _migrate_native_send_queue(db):
        """Import legacy native broadcast rows into the ordered send queue, exactly once.

        Rows are imported per physical target in insertion order, so the historical write order is preserved
        without guessing from UUIDs. `sending` becomes `uncertain`: after a restart the native write may or may
        not have happened. Re-running the migration is a no-op because `(delivery_source, delivery_id)` is unique.
        """
        legacy = db.execute(
            "SELECT d.* FROM native_broadcast_deliveries d "
            "LEFT JOIN native_send_queue q "
            "ON q.delivery_source IN (?,?) AND q.delivery_id=d.id "
            "WHERE q.id IS NULL ORDER BY d.runtime,d.native_id,d.rowid",
            Store.NATIVE_SEND_LEGACY_SOURCES,
        ).fetchall()
        counters = {}
        for row in legacy:
            key = (row["runtime"], row["native_id"])
            if key not in counters:
                saved = db.execute(
                    "SELECT next_seq FROM native_target_counters WHERE runtime=? AND native_id=?", key
                ).fetchone()
                counters[key] = saved["next_seq"] if saved else 1
            local_seq = counters[key]
            status = "uncertain" if row["status"] == "sending" else row["status"]
            db.execute(
                "INSERT OR IGNORE INTO native_send_queue("
                "id,runtime,native_id,local_seq,delivery_source,delivery_id,message,status,receipt,"
                "native_turn_id,processing_status,error,revision,created,last_attempt) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)",
                (
                    row["id"], row["runtime"], row["native_id"], local_seq,
                    "native_broadcast", row["id"], row["message"], status, row["receipt"],
                    row["native_turn_id"], row["processing_status"], row["error"], row["created"],
                    row["last_attempt"],
                ),
            )
            counters[key] = local_seq + 1
        for (runtime, native_id), next_seq in counters.items():
            db.execute(
                "INSERT INTO native_target_counters(runtime,native_id,next_seq) VALUES(?,?,?) "
                "ON CONFLICT(runtime,native_id) DO UPDATE SET next_seq=MAX(next_seq,excluded.next_seq)",
                (runtime, native_id, next_seq),
            )

    def _initialize_commit_log(self, db):
        """Preserve a legacy counter only when its acknowledged delivery still exists in the DB."""
        from .node_identity import FileNodeIdentityStore

        committed = self.max_peer_commit_seq(db=db)
        acknowledged = db.execute("SELECT COALESCE(MAX(ack_seq),0) FROM peer_inbox").fetchone()[0]
        floor = max(committed, acknowledged)
        identity_store = FileNodeIdentityStore(self.state)
        if identity_store.path().exists():
            identity = identity_store.load_or_create()
            if identity.state.value == "state_corrupted":
                return
            if identity.max_confirmed_seq > floor:
                legacy = db.execute(
                    "SELECT COALESCE(MAX(target_seq),0) FROM peer_inbox WHERE ack_seq IS NULL "
                    "AND node_id=? AND state_generation=?",
                    (identity.node_id, identity.state_generation),
                ).fetchone()[0]
                if legacy < identity.max_confirmed_seq:
                    try:
                        identity_store.verify_database(floor)
                    except BridgeError as error:
                        if error.code != PeerErrorCode.STATE_CORRUPTED:
                            raise
                    return
                floor = identity.max_confirmed_seq
        if floor > committed:
            key = "migration_floor" if committed == 0 else f"legacy_floor:{floor}"
            db.execute(
                "INSERT INTO peer_commit_log(ack_seq,mutation_key,method,created) VALUES(?,?,?,?)",
                (floor, key, "migration", time.time()),
            )

    def create_group_prune_run(
        self, run_id: str, request_id: str, fingerprint: str, cutoff: float, now: float | None = None
    ) -> dict:
        with self.connection(write=True) as db:
            row = db.execute("SELECT * FROM group_prune_runs WHERE request_id=?", (request_id,)).fetchone()
            if row is not None:
                if row["fingerprint"] != fingerprint:
                    raise BridgeError("request_conflict", "request_id was already used with different parameters")
                return dict(row)
            db.execute(
                "INSERT INTO group_prune_runs(id,request_id,fingerprint,cutoff,status,created) VALUES(?,?,?,?,?,?)",
                (run_id, request_id, fingerprint, cutoff, "running", time.time() if now is None else now),
            )
            return dict(db.execute("SELECT * FROM group_prune_runs WHERE id=?", (run_id,)).fetchone())

    def get_group_prune_run(self, run_id: str) -> dict | None:
        with self.connection() as db:
            row = db.execute("SELECT * FROM group_prune_runs WHERE id=?", (run_id,)).fetchone()
            return dict(row) if row else None

    def get_group_prune_run_by_request_id(self, request_id: str) -> dict | None:
        with self.connection() as db:
            row = db.execute("SELECT * FROM group_prune_runs WHERE request_id=?", (request_id,)).fetchone()
            return dict(row) if row else None

    def record_group_prune_item(
        self,
        run_id: str,
        group_id: str,
        expected_revision: int,
        member_snapshot: str | dict,
        check_result: str,
        status: str,
        reason: str | None = None,
    ) -> None:
        if not isinstance(member_snapshot, str):
            member_snapshot = json.dumps(member_snapshot, sort_keys=True, separators=(",", ":"))
        with self.connection(write=True) as db:
            db.execute(
                "INSERT OR REPLACE INTO group_prune_items(run_id,group_id,expected_revision,member_snapshot,"
                "check_result,status,reason) VALUES(?,?,?,?,?,?,?)",
                (run_id, group_id, expected_revision, member_snapshot, check_result, status, reason),
            )

    def get_group_prune_items(self, run_id: str) -> list[dict]:
        with self.connection() as db:
            rows = db.execute("SELECT * FROM group_prune_items WHERE run_id=? ORDER BY group_id", (run_id,))
            return [dict(row) for row in rows]

    def finish_group_prune_run(self, run_id: str, status: str = "completed", now: float | None = None) -> None:
        with self.connection(write=True) as db:
            db.execute(
                "UPDATE group_prune_runs SET status=?, finished=? WHERE id=?",
                (status, time.time() if now is None else now, run_id),
            )

    def prune_group_cas(
        self, group_id: str, expected_revision: int, delete_reason: str = "pruned", now: float | None = None
    ) -> dict:
        ts = time.time() if now is None else now
        with self.connection(write=True) as db:
            cursor = db.execute(
                "UPDATE session_groups SET deleted=1, deleted_at=:now, delete_reason=:reason, revision=revision+1 "
                "WHERE id=:group_id AND revision=:expected_revision AND deleted=0",
                {"now": ts, "reason": delete_reason, "group_id": group_id, "expected_revision": expected_revision},
            )
            row = db.execute("SELECT * FROM session_groups WHERE id=?", (group_id,)).fetchone()
            if cursor.rowcount == 0:
                if row is None:
                    raise BridgeError("not_found", f"session_groups: {group_id}")
                if row["deleted"]:
                    raise BridgeError("group_deleted", "This group was deleted")
                raise BridgeError(PeerErrorCode.REVISION_CONFLICT, f"stale revision, expected {expected_revision}")
            return dict(row)

    def touch_group_activity(self, group_id: str, timestamp: float | None = None, updated: bool = False, db=None) -> None:
        ts = time.time() if timestamp is None else timestamp
        sql = "UPDATE session_groups SET last_activity_at=?" + (", updated_at=?" if updated else "") + " WHERE id=?"
        args = (ts, ts, group_id) if updated else (ts, group_id)
        if db is not None:
            db.execute(sql, args)
            return
        with self.connection(write=True) as own:
            own.execute(sql, args)

    @contextmanager
    def connection(self, *, write=False, timeout=10):
        db = sqlite3.connect(self.path, timeout=timeout)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def required(db, table, key):
        if table not in {"sessions", "tasks", "attempts", "questions", "session_groups"}:
            raise ValueError("Invalid table")
        row = db.execute(f"SELECT * FROM {table} WHERE id=?", (key,)).fetchone()
        if row is None:
            raise BridgeError("not_found", f"{table}: {key}")
        return dict(row)

    @staticmethod
    def event(db, kind, data, session=None, task=None, attempt=None):
        return db.execute(
            "INSERT INTO events(session_id,task_id,attempt_id,kind,data,created) VALUES(?,?,?,?,?,?)",
            (session, task, attempt, kind, encode(data), time.time()),
        ).lastrowid

    @staticmethod
    def previous(db, request_id, payload):
        if not isinstance(request_id, str) or not request_id or len(request_id) > 200:
            raise BridgeError("invalid_request_id", "A nonempty request_id of at most 200 characters is required")
        fingerprint = hashlib.sha256(encode(payload).encode()).hexdigest()
        row = db.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
        if row:
            if row["fingerprint"] != fingerprint:
                raise BridgeError("request_conflict", "request_id was already used for different content")
            return fingerprint, json.loads(row["result"])
        return fingerprint, None

    @staticmethod
    def remember(db, request_id, fingerprint, result):
        db.execute("INSERT INTO requests VALUES(?,?,?)", (request_id, fingerprint, encode(result)))
        return result

    def open(
        self,
        profile: Profile,
        cwd: str,
        access: str,
        request_id: str,
        parent: str | None = None,
        workspace=None,
        name: str | None = None,
    ):
        directory = Path(cwd).expanduser().resolve()
        if not directory.is_dir():
            raise BridgeError("invalid_directory", "Working directory must exist")
        if access not in {"read-only", "workspace-write"}:
            raise BridgeError("invalid_access", "access must be read-only or workspace-write")
        name = validate_session_name(name)
        payload = {
            "op": "open",
            "profile": asdict(profile),
            "cwd": str(directory),
            "access": access,
            "parent": parent,
            "workspace": workspace,
        }
        if name is not None:
            payload["name"] = name
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(db, request_id, payload)
            if old is not None:
                return old
            session = identifier()
            native = identifier() if profile.runtime == "claude" else None
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,parent,native_id,created,workspace) VALUES(?,?,?,?,?,?,?,?)",
                (
                    session,
                    encode(asdict(profile)),
                    str(directory),
                    access,
                    parent,
                    native,
                    time.time(),
                    encode(workspace or {}),
                ),
            )
            if name is not None:
                db.execute(
                    "INSERT INTO session_names(session_id,name,created) VALUES(?,?,?)",
                    (session, name, time.time()),
                )
            self.event(db, "session_opened", {"name": name} if name is not None else {}, session)
            result = {"session_id": session, "revision": 0}
            if name is not None:
                result["name"] = name
            return self.remember(db, request_id, fingerprint, result)

    def create_session(
        self,
        db,
        profile: Profile,
        cwd: str,
        access: str,
        parent: str | None = None,
        workspace=None,
        name: str | None = None,
    ):
        """Insert one session inside an existing transaction (peer.deliver composite open, ADR-13).

        Unlike `open`, this does not allocate a `requests` row: the caller's peer operation record is the
        idempotency key, so session creation and task enqueueing can share one SQLite transaction.
        """
        directory = Path(cwd).expanduser().resolve()
        if not directory.is_dir():
            raise BridgeError("invalid_directory", "Working directory must exist")
        if access not in {"read-only", "workspace-write"}:
            raise BridgeError("invalid_access", "access must be read-only or workspace-write")
        name = validate_session_name(name)
        session = identifier()
        native = identifier() if profile.runtime == "claude" else None
        db.execute(
            "INSERT INTO sessions(id,profile,cwd,access,parent,native_id,created,workspace) VALUES(?,?,?,?,?,?,?,?)",
            (
                session,
                encode(asdict(profile)),
                str(directory),
                access,
                parent,
                native,
                time.time(),
                encode(workspace or {}),
            ),
        )
        if name is not None:
            db.execute(
                "INSERT INTO session_names(session_id,name,created) VALUES(?,?,?)",
                (session, name, time.time()),
            )
        self.event(db, "session_opened", {"name": name} if name is not None else {}, session)
        return session, str(directory)

    def dispatch(
        self, session_id: str, instruction: str, request_id: str, dependencies=None, reservation=0, details=None
    ):
        dependencies = task_input(instruction, reservation, dependencies)
        details = dict(details or {})
        timeout = validate_task_timeout(details.get("task_timeout"))
        if timeout is None:
            details.pop("task_timeout", None)
        payload = {
            "op": "dispatch",
            "session": session_id,
            "instruction": instruction,
            "dependencies": dependencies,
            "reservation": reservation,
            "details": details,
        }
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(db, request_id, payload)
            if old is not None:
                return old
            session = self.required(db, "sessions", session_id)
            if session["desired"] == "closed":
                raise BridgeError("session_closed", "Reopen this session explicitly before submitting work")
            for dependency in dependencies:
                self.required(db, "tasks", dependency)
            result = self.enqueue_task(db, session_id, instruction, dependencies, reservation, details)
            return self.remember(db, request_id, fingerprint, result)

    def enqueue_task(self, db, session_id, instruction, dependencies, reservation, details):
        details = dict(details or {})
        timeout = validate_task_timeout(details.get("task_timeout"))
        if timeout is None:
            details.pop("task_timeout", None)
        else:
            check_task_timeout_support(self.required(db, "sessions", session_id), details)
        task = identifier()
        db.execute(
            "INSERT INTO tasks(id,session_id,instruction,dependencies,reservation,created,details) VALUES(?,?,?,?,?,?,?)",
            (task, session_id, instruction, encode(dependencies), reservation, time.time(), encode(details)),
        )
        self.event(db, "task_queued", {}, session_id, task)
        return {"task_id": task, "session_id": session_id, "status": "queued"}

    def validate_dispatch(self, instruction, reservation, dependencies=None, retry_task_id=None):
        with self.connection() as db:
            if retry_task_id:
                task = self.required(db, "tasks", retry_task_id)
                check_retry(task, instruction)
                if dependencies is not None and dependencies != json.loads(task["dependencies"]):
                    raise BridgeError("invalid_retry", "Retry preserves dependencies")
                if reservation is None:
                    reservation = task["reservation"]
            if reservation is None:
                reservation = 0
            dependencies = task_input(instruction, reservation, dependencies)
            for dependency in dependencies:
                self.required(db, "tasks", dependency)
        return reservation

    def set_session_name(self, session_id, name):
        """Validate and upsert a display name for an existing Bridge session."""
        value = validate_session_name(name)
        if value is None:
            raise BridgeError("invalid_request", "name is required")
        with self.connection(write=True) as db:
            self.required(db, "sessions", session_id)
            db.execute(
                "INSERT INTO session_names(session_id,name,created) VALUES(?,?,?) "
                "ON CONFLICT(session_id) DO UPDATE SET name=excluded.name",
                (session_id, value, time.time()),
            )
        return value

    def get_session(self, session_id):
        with self.connection() as db:
            row = self.required(db, "sessions", session_id)
            naming = db.execute("SELECT name FROM session_names WHERE session_id=?", (session_id,)).fetchone()
        row["profile"] = json.loads(row["profile"])
        row["workspace"] = json.loads(row["workspace"])
        if naming is not None:
            row["name"] = naming["name"]
        return row

    def get_task(self, task_id):
        with self.connection() as db:
            row = self.required(db, "tasks", task_id)
            row["attempts"] = [
                dict(x) for x in db.execute("SELECT * FROM attempts WHERE task_id=? ORDER BY started,id", (task_id,))
            ]
        row["dependencies"] = json.loads(row["dependencies"])
        row["details"] = json.loads(row["details"])
        row["result"] = json.loads(row["result"]) if row["result"] else None
        for attempt in row["attempts"]:
            for key in ("result", "usage"):
                attempt[key] = json.loads(attempt[key]) if attempt[key] else None
        return row

    def sessions(self):
        with self.connection() as db:
            rows = db.execute(
                "SELECT s.id,s.desired,s.observed,s.revision,s.worker_pid,s.worker_start,s.generation,s.error,s.profile,sn.name "
                "FROM sessions s LEFT JOIN session_names sn ON sn.session_id=s.id ORDER BY s.created"
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            profile = json.loads(value.pop("profile"))
            name = value.pop("name")
            value.update({key: profile.get(key) for key in ("runtime", "provider", "model", "effort", "service_tier")})
            value["profile"] = profile["name"]
            if name is not None:
                value["name"] = name
            result.append(value)
        return result

    def control(self, session_id, action, revision, request_id, continue_task_id=None):
        if action not in {"pause", "pause_drain", "resume", "close", "reopen"}:
            raise BridgeError("invalid_action", "Unknown session control action")
        payload = {
            "op": "control",
            "session": session_id,
            "action": action,
            "revision": revision,
            "continue": continue_task_id,
        }
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(db, request_id, payload)
            if old is not None:
                return old
            session = self.required(db, "sessions", session_id)
            if session["revision"] != revision:
                raise BridgeError("stale_revision", "Session changed; inspect it before controlling it")
            if session["desired"] == "closed" and action != "reopen":
                raise BridgeError("session_closed", "Use reopen for a closed session")
            if action == "reopen" and session["desired"] != "closed":
                raise BridgeError("invalid_state", "Only a closed session can be reopened")
            desired = {
                "pause": "paused",
                "pause_drain": "draining",
                "resume": "open",
                "reopen": "open",
                "close": "closed",
            }[action]
            if desired == "draining" and not db.execute(
                "SELECT 1 FROM tasks WHERE session_id=? AND status IN ('assigned','running','waiting_input')", (session_id,)
            ).fetchone():
                desired = "paused"
            db.execute("UPDATE sessions SET desired=?,revision=revision+1 WHERE id=?", (desired, session_id))
            if action in {"resume", "reopen"} and session["worker_pid"] is None:
                if db.execute(
                    "SELECT 1 FROM tasks WHERE session_id=? AND status='recovery_needed'", (session_id,)
                ).fetchone():
                    raise BridgeError(
                        "recovery_required", "Inspect the uncertain task and explicitly resolve it before resuming"
                    )
                db.execute("UPDATE sessions SET observed='stopped',error=NULL WHERE id=?", (session_id,))
            if action in {"pause", "close"}:
                assigned = db.execute(
                    "SELECT id,active_attempt FROM tasks WHERE session_id=? AND status='assigned'", (session_id,)
                ).fetchall()
                for task in assigned:
                    db.execute(
                        "UPDATE attempts SET status='cancelled',finished=? WHERE id=?",
                        (time.time(), task["active_attempt"]),
                    )
                    db.execute(
                        "UPDATE tasks SET status=?,active_attempt=NULL WHERE id=?",
                        ("cancelled" if action == "close" else "queued", task["id"]),
                    )
                db.execute(
                    "UPDATE questions SET status='expired' WHERE session_id=? AND status='pending'", (session_id,)
                )
            if action == "close":
                db.execute("UPDATE tasks SET status='cancelled' WHERE session_id=? AND status='queued'", (session_id,))
            if continue_task_id:
                task = self.required(db, "tasks", continue_task_id)
                if action != "resume" or task["session_id"] != session_id or task["status"] != "interrupted":
                    raise BridgeError(
                        "invalid_continuation", "Only an interrupted task in this session can be continued"
                    )
                db.execute("UPDATE tasks SET status='queued',cancel_requested=0 WHERE id=?", (continue_task_id,))
            self.event(
                db,
                "session_control_requested",
                {"action": action, "revision": revision + 1},
                session_id,
                continue_task_id,
            )
            return self.remember(
                db, request_id, fingerprint, {"session_id": session_id, "desired": desired, "revision": revision + 1}
            )

    def cancel(self, task_id, request_id):
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(db, request_id, {"op": "cancel", "task": task_id})
            if old is not None:
                return old
            task = self.required(db, "tasks", task_id)
            if task["status"] in {"queued", "assigned"}:
                db.execute("UPDATE tasks SET status='cancelled',cancel_requested=1 WHERE id=?", (task_id,))
                if task["active_attempt"]:
                    db.execute(
                        "UPDATE attempts SET status='cancelled',finished=? WHERE id=?",
                        (time.time(), task["active_attempt"]),
                    )
            elif task["status"] in {"running", "waiting_input"}:
                db.execute("UPDATE tasks SET cancel_requested=1 WHERE id=?", (task_id,))
                db.execute("UPDATE questions SET status='expired' WHERE task_id=? AND status='pending'", (task_id,))
            self.event(db, "cancel_requested", {}, task["session_id"], task_id)
            return self.remember(db, request_id, fingerprint, {"task_id": task_id, "cancel_requested": True})

    def queue_context(self, db):
        active = db.execute(
            "SELECT t.id,t.session_id,s.profile,t.reservation,a.usage FROM tasks t JOIN sessions s ON s.id=t.session_id LEFT JOIN attempts a ON a.id=t.active_attempt WHERE t.status IN ('assigned','running','waiting_input')"
        ).fetchall()
        return active, self.accounting(db)

    def queue_reason(self, db, task, config, active, accounting):
        session = self.required(db, "sessions", task["session_id"])
        if session["desired"] != "open":
            return {"code": "session_state", "desired": session["desired"]}
        busy = [row["id"] for row in active if row["session_id"] == session["id"]]
        if busy:
            return {"code": "session_busy", "task_ids": busy}
        earlier = db.execute(
            "SELECT id FROM tasks WHERE session_id=? AND status='queued' AND (created,id)<(?,?) ORDER BY created,id LIMIT 1",
            (session["id"], task["created"], task["id"]),
        ).fetchone()
        if earlier:
            return {"code": "session_queue", "task_id": earlier["id"]}
        dependencies = [self.required(db, "tasks", dep) for dep in json.loads(task["dependencies"])]
        pending = [
            {key: row[key] for key in ("id", "status", "validation")}
            for row in dependencies if row["validation"] != "accepted"
        ]
        if pending:
            return {"code": "dependencies", "tasks": pending}
        if session["observed"] != "idle":
            return {"code": "worker_state", "observed": session["observed"], "error": session["error"]}
        try:
            check_task_timeout_support(session, json.loads(task["details"]))
        except BridgeError as error:
            return {"code": error.code, "message": str(error)}
        if len(active) >= config.parallel_limit:
            return {"code": "parallel_limit", "limit": config.parallel_limit}
        group = json.loads(session["profile"])["quota_group"]
        cooldown = db.execute("SELECT * FROM quota_cooldowns WHERE quota_group=?", (group,)).fetchone()
        if cooldown and (cooldown["blocked_until"] is None or cooldown["blocked_until"] > time.time()):
            return {"code": "quota_wait", "quota_group": group, "until": cooldown["blocked_until"], "reason": cooldown["reason"]}
        if sum(json.loads(row["profile"])["quota_group"] == group for row in active) >= config.quota_limit:
            return {"code": "quota_limit", "quota_group": group, "limit": config.quota_limit}
        if config.token_budget is not None:
            if accounting["unreserved_unknown_attempts"]:
                return {"code": "usage_unknown"}
            if task["reservation"] <= 0:
                return {"code": "reservation_required"}
            committed = sum(
                max(row["reservation"], json.loads(row["usage"] or "{}").get("total_tokens") or 0) for row in active
            )
            available = config.token_budget - accounting["charged_tokens"] - committed
            if task["reservation"] > available:
                return {"code": "token_budget", "reservation": task["reservation"], "available": max(0, available)}
        return None

    def waiting_reason(self, task_id, config):
        with self.connection() as db:
            task = self.required(db, "tasks", task_id)
            if task["status"] != "queued":
                return None
            return self.queue_reason(db, task, config, *self.queue_context(db)) or {"code": "ready"}

    def claim(self, config: Config):
        """Reserve capacity and claim one task atomically across competing schedulers."""
        with self.connection(write=True) as db:
            active, accounting = self.queue_context(db)
            for task in db.execute("SELECT * FROM tasks WHERE status='queued' ORDER BY created,id").fetchall():
                if self.queue_reason(db, task, config, active, accounting):
                    continue
                session = self.required(db, "sessions", task["session_id"])
                attempt = identifier()
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,reservation) VALUES(?,?,?,'assigned',?)",
                    (attempt, task["id"], session["id"], task["reservation"]),
                )
                db.execute(
                    "UPDATE tasks SET status='assigned',active_attempt=?,validation='pending' WHERE id=?",
                    (attempt, task["id"]),
                )
                db.execute("UPDATE sessions SET revision=revision+1 WHERE id=?", (session["id"],))
                self.event(db, "task_assigned", {}, session["id"], task["id"], attempt)
                return {"task_id": task["id"], "session_id": session["id"], "attempt_id": attempt}
        return None

    def retry(self, task_id, session_id, instruction, request_id, reservation=None, task_timeout=None):
        task_timeout = validate_task_timeout(task_timeout)
        payload = {"op": "retry", "task": task_id, "session": session_id, "instruction": instruction}
        if reservation is not None:
            payload["reservation"] = reservation
        if task_timeout is not None:
            payload["task_timeout"] = task_timeout
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(db, request_id, payload)
            if old is not None:
                return old
            task = self.required(db, "tasks", task_id)
            target = self.required(db, "sessions", session_id)
            if target["desired"] == "closed":
                raise BridgeError("session_closed", "Reopen the target session before retrying")
            check_retry(task, instruction)
            reservation = task["reservation"] if reservation is None else reservation
            task_input(instruction, reservation)
            details = json.loads(task["details"])
            if task_timeout is not None:
                details["task_timeout"] = task_timeout
            check_task_timeout_support(target, details)
            db.execute(
                "UPDATE tasks SET session_id=?,reservation=?,details=?,status='queued',validation='pending',active_attempt=NULL,cancel_requested=0,result=NULL,error=NULL WHERE id=?",
                (session_id, reservation, encode(details), task_id),
            )
            self.event(db, "retry_requested", {"previous_session_id": task["session_id"]}, session_id, task_id)
            return self.remember(
                db, request_id, fingerprint, {"task_id": task_id, "session_id": session_id, "status": "queued"}
            )

    def resolve(self, session_id, task_id, revision, request_id, resolution, evidence):
        if resolution not in {"failed", "cancelled", "interrupted"} or not evidence or not evidence.strip():
            raise BridgeError("invalid_resolution", "Provide failed/cancelled/interrupted and an inspection note")
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(
                db,
                request_id,
                {
                    "op": "resolve",
                    "session": session_id,
                    "task": task_id,
                    "revision": revision,
                    "resolution": resolution,
                    "evidence": evidence,
                },
            )
            if old is not None:
                return old
            session = self.required(db, "sessions", session_id)
            task = self.required(db, "tasks", task_id)
            if session["revision"] != revision:
                raise BridgeError("stale_revision", "Inspect the session before resolving an uncertain task")
            if task["session_id"] != session_id or task["status"] != "recovery_needed" or session["worker_pid"]:
                raise BridgeError("invalid_resolution", "Resolve only an uncertain task after its worker has stopped")
            db.execute("UPDATE tasks SET status=?,error=? WHERE id=?", (resolution, evidence, task_id))
            db.execute("UPDATE sessions SET revision=revision+1 WHERE id=?", (session_id,))
            self.event(
                db,
                "recovery_resolved",
                {"resolution": resolution, "evidence": evidence},
                session_id,
                task_id,
                task["active_attempt"],
            )
            return self.remember(
                db, request_id, fingerprint, {"task_id": task_id, "status": resolution, "revision": revision + 1}
            )

    def worker_starting(self, session_id, generation, pid, start, process_scope=None, *, worker_version=None):
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            if session["desired"] != "open" or session["observed"] not in {"stopped", "starting"}:
                return False
            db.execute(
                "UPDATE sessions SET generation=?,worker_pid=?,worker_start=?,process_scope=?,worker_task_timeout=0,worker_version=?,observed='starting' WHERE id=?",
                (generation, pid, start, process_scope, worker_version, session_id),
            )
            return True

    def worker_ready(self, session_id, generation, pid, start, native_id, *, task_timeout_supported=False, worker_version=None):
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            if session["generation"] not in {None, generation}:
                raise BridgeError("stale_worker", "A different worker owns this session")
            if session["native_id"] and session["native_id"] != native_id:
                raise BridgeError("session_mismatch", "Native session identity changed")
            db.execute(
                "UPDATE sessions SET generation=?,worker_pid=?,worker_start=?,native_id=?,worker_task_timeout=?,worker_version=COALESCE(?,worker_version),observed='idle',error=NULL WHERE id=?",
                (generation, pid, start, native_id, int(task_timeout_supported), worker_version, session_id),
            )
            self.event(db, "worker_ready", {}, session_id)

    def begin_attempt(self, session_id, generation):
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            if session["generation"] != generation or session["desired"] != "open":
                return None
            row = db.execute(
                "SELECT * FROM tasks WHERE session_id=? AND status='assigned' ORDER BY created LIMIT 1", (session_id,)
            ).fetchone()
            if not row:
                return None
            task = dict(row)
            db.execute("UPDATE tasks SET status='running' WHERE id=?", (task["id"],))
            db.execute(
                "UPDATE attempts SET status='running',started=? WHERE id=?", (time.time(), task["active_attempt"])
            )
            db.execute("UPDATE sessions SET observed='running' WHERE id=?", (session_id,))
            self.event(db, "attempt_started", {}, session_id, task["id"], task["active_attempt"])
            return task

    def native_started(self, session_id, task_id, attempt_id, native_turn_id):
        with self.connection(write=True) as db:
            db.execute("UPDATE attempts SET native_turn_id=? WHERE id=?", (native_turn_id, attempt_id))
            self.event(db, "native_started", {"native_turn_id": native_turn_id}, session_id, task_id, attempt_id)

    def finish(self, session_id, generation, task_id, attempt_id, status, result, usage=None):
        if status not in {"completed", "failed", "cancelled", "interrupted", "recovery_needed"}:
            raise ValueError("Invalid terminal state")
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            task = self.required(db, "tasks", task_id)
            if session["generation"] != generation or task["active_attempt"] != attempt_id:
                raise BridgeError("stale_worker", "This worker no longer owns the attempt")
            if task["status"] not in {"running", "waiting_input"}:
                return False
            if usage is None:
                previous = db.execute("SELECT usage FROM attempts WHERE id=?", (attempt_id,)).fetchone()[0]
                usage = {**json.loads(previous or "{}"), "complete": False, "scope": "partial_attempt"}
            elif status == "recovery_needed":
                usage = {**usage, "complete": False, "scope": "partial_attempt"}
            if status in {"failed", "recovery_needed"} and result.get("provider_limit"):
                self.cooldown(db, json.loads(session["profile"])["quota_group"], result["provider_limit"])
            artifact = self.save_artifact(db, task_id, attempt_id, "result", result)
            result = {
                **artifacts.brief(result),
                "artifact": artifact,
                "answer_truncated": len(result.get("answer") or "") > 4000,
                "native_usage_total": result.get("native_usage_total"),
            }
            db.execute(
                "UPDATE attempts SET status=?,result=?,usage=?,finished=? WHERE id=?",
                (status, encode(result), encode(usage) if usage is not None else None, time.time(), attempt_id),
            )
            db.execute(
                "UPDATE tasks SET status=?,result=?,error=? WHERE id=?",
                (status, encode(result), result.get("error"), task_id),
            )
            db.execute(
                "UPDATE sessions SET observed='idle',desired=CASE WHEN desired='draining' THEN 'paused' ELSE desired END WHERE id=?",
                (session_id,),
            )
            db.execute("UPDATE questions SET status='expired' WHERE attempt_id=? AND status='pending'", (attempt_id,))
            seq = self.event(
                db, "task_finished", {"status": status, "validation": "pending"}, session_id, task_id, attempt_id
            )
            self.notify_parent(db, session, seq)
            return True

    @staticmethod
    def notify_parent(db, session, seq):
        if session["parent"] and db.execute("SELECT 1 FROM parents WHERE id=? AND notify=1", (session["parent"],)).fetchone():
            db.execute("INSERT INTO deliveries(event_seq,parent_id) VALUES(?,?)", (seq, session["parent"]))

    def notify_queued_failure(self, db, session, error):
        for task in db.execute("SELECT id FROM tasks WHERE session_id=? AND status='queued'", (session["id"],)).fetchall():
            seq = self.event(
                db, "task_start_failed", {"status": "queued", "error": error}, session["id"], task["id"],
            )
            self.notify_parent(db, session, seq)

    def worker_start_failed(self, session_id, error):
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            if session["worker_pid"] or session["observed"] == "recovery_needed":
                return
            db.execute("UPDATE sessions SET observed='recovery_needed',error=? WHERE id=?", (error, session_id))
            self.event(db, "worker_start_failed", {"error": error}, session_id)
            self.notify_queued_failure(db, session, error)

    def worker_stopped(self, session_id, generation, error=None):
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            if session["generation"] != generation:
                return
            active = db.execute(
                "SELECT id,active_attempt FROM tasks WHERE session_id=? AND status IN ('running','waiting_input')",
                (session_id,),
            ).fetchall()
            for task in active:
                previous = db.execute("SELECT usage FROM attempts WHERE id=?", (task["active_attempt"],)).fetchone()[0]
                usage = {**json.loads(previous or "{}"), "complete": False, "scope": "partial_attempt"}
                db.execute(
                    "UPDATE tasks SET status='recovery_needed',error=? WHERE id=?",
                    (error or "Worker disappeared during execution", task["id"]),
                )
                db.execute(
                    "UPDATE attempts SET status='recovery_needed',finished=?,usage=? WHERE id=?",
                    (time.time(), encode(usage), task["active_attempt"]),
                )
                seq = self.event(
                    db, "task_recovery_needed", {"status": "recovery_needed", "error": error},
                    session_id, task["id"], task["active_attempt"],
                )
                self.notify_parent(db, session, seq)
            for task in db.execute(
                "SELECT id,active_attempt FROM tasks WHERE session_id=? AND status='assigned'", (session_id,)
            ).fetchall():
                db.execute(
                    "UPDATE attempts SET status='cancelled',finished=? WHERE id=?",
                    (time.time(), task["active_attempt"]),
                )
                db.execute("UPDATE tasks SET status='queued',active_attempt=NULL WHERE id=?", (task["id"],))
            db.execute("UPDATE questions SET status='expired' WHERE session_id=? AND status='pending'", (session_id,))
            observed = "recovery_needed" if active or error else "stopped"
            if observed == "recovery_needed" and session["observed"] != "recovery_needed":
                self.notify_queued_failure(db, session, error or "Worker disappeared during execution")
            db.execute(
                "UPDATE sessions SET observed=?,worker_pid=NULL,worker_task_timeout=0,worker_version=NULL,error=?,desired=CASE WHEN desired='draining' THEN 'paused' ELSE desired END WHERE id=?",
                (observed, error, session_id),
            )
            self.event(db, "worker_stopped", {"observed": observed, "error": error}, session_id)

    def review(self, task_id, attempt_id, decision, evidence, request_id, revision):
        if decision not in {"accepted", "rejected"} or not isinstance(evidence, str) or not evidence.strip():
            raise BridgeError("invalid_review", "Provide accepted/rejected and concrete verification evidence")
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(
                db,
                request_id,
                {
                    "op": "review",
                    "task": task_id,
                    "attempt": attempt_id,
                    "decision": decision,
                    "evidence": evidence,
                    "revision": revision,
                },
            )
            if old is not None:
                return old
            task = self.required(db, "tasks", task_id)
            session = self.required(db, "sessions", task["session_id"])
            if session["revision"] != revision:
                raise BridgeError("stale_revision", "Inspect this session before reviewing its result")
            if task["active_attempt"] != attempt_id or task["status"] != "completed":
                raise BridgeError("invalid_review", "Only the current completed attempt can be reviewed")
            if db.execute("SELECT 1 FROM reviews WHERE task_id=? AND attempt_id=?", (task_id, attempt_id)).fetchone():
                raise BridgeError("already_reviewed", "This attempt already has a review")
            db.execute("INSERT INTO reviews VALUES(?,?,?,?,?)", (task_id, attempt_id, decision, evidence, time.time()))
            db.execute("UPDATE tasks SET validation=? WHERE id=?", (decision, task_id))
            db.execute("UPDATE sessions SET revision=revision+1 WHERE id=?", (task["session_id"],))
            self.event(
                db,
                "task_reviewed",
                {"decision": decision, "evidence": evidence},
                task["session_id"],
                task_id,
                attempt_id,
            )
            return self.remember(
                db, request_id, fingerprint, {"task_id": task_id, "validation": decision, "revision": revision + 1}
            )

    @staticmethod
    def accounting(db):
        known = charged = uncertain = 0
        unreserved = 0
        estimates = []
        for row in db.execute(
            "SELECT usage,reservation FROM attempts WHERE finished IS NOT NULL AND started IS NOT NULL"
        ):
            usage = json.loads(row["usage"]) if row["usage"] else {}
            estimates.append(usage.get("catalog_estimate", {}))
            tokens = usage.get("total_tokens")
            known += tokens or 0
            if tokens is None or usage.get("complete") is False:
                uncertain += 1
                unreserved += int(row["reservation"] <= 0)
                charged += max(tokens or 0, row["reservation"])
            else:
                charged += tokens
        reserved = db.execute(
            "SELECT COALESCE(SUM(reservation),0) FROM tasks WHERE status IN ('assigned','running','waiting_input')"
        ).fetchone()[0]
        inflight = sum(
            (json.loads(row[0]).get("total_tokens") or 0)
            for row in db.execute("SELECT usage FROM attempts WHERE finished IS NULL AND usage IS NOT NULL")
        )
        meters = [meter_usage(json.loads(row[0])) for row in db.execute("SELECT state FROM parent_usage")]
        parent_tokens = sum(meter["total_tokens"] for meter in meters)
        estimates.extend(meter.get("catalog_estimate", {}) for meter in meters)
        parent_count = db.execute("SELECT COUNT(*) FROM parents").fetchone()[0]
        unknown_parents = parent_count - len(meters) + sum(not meter["complete"] for meter in meters)
        return {
            "reported_tokens": known + parent_tokens,
            "child_reported_tokens": known,
            "parent_reported_tokens": parent_tokens,
            "charged_tokens": charged + parent_tokens,
            "reserved_tokens": reserved,
            "inflight_tokens": inflight,
            "uncertain_attempts": uncertain,
            "unreserved_unknown_attempts": unreserved + unknown_parents,
            "unmetered_or_incomplete_parents": unknown_parents,
            "scope": "child_attempts_and_registered_parents" if parent_count else "child_attempts_only",
            "complete": bool(parent_count) and not (uncertain or unknown_parents or reserved or inflight),
            "actual_billed_usd": None,
            "catalog_estimate": {
                "reported_usd": sum(estimate.get("usd") or 0 for estimate in estimates)
                if any(estimate.get("usd") is not None for estimate in estimates)
                else None,
                "complete": not (uncertain or reserved or inflight)
                and bool(estimates)
                and all(estimate.get("complete") and estimate.get("usd") is not None for estimate in estimates)
                and not unknown_parents
                and bool(parent_count),
                "sources": sorted({estimate["source"] for estimate in estimates if estimate.get("source")}),
            },
        }

    def budget(self):
        with self.connection() as db:
            return self.accounting(db)

    @staticmethod
    def cooldown(db, group, limit):
        previous = db.execute("SELECT * FROM quota_cooldowns WHERE quota_group=?", (group,)).fetchone()
        failures = previous["failures"] + 1 if previous else 1
        delay = limit.get("retry_after_seconds")
        until = (
            None
            if limit.get("manual_reset")
            else time.time() + (delay if delay is not None else min(900, 60 * 2 ** min(failures - 1, 4)))
        )
        if previous:
            until = (
                None if previous["blocked_until"] is None or until is None else max(until, previous["blocked_until"])
            )
        db.execute(
            "INSERT INTO quota_cooldowns VALUES(?,?,?,?,?,?) ON CONFLICT(quota_group) DO UPDATE SET blocked_until=excluded.blocked_until,failures=excluded.failures,revision=excluded.revision,reason=excluded.reason,updated=excluded.updated",
            (
                group,
                until,
                failures,
                previous["revision"] + 1 if previous else 0,
                "provider_retry_after"
                if delay is not None
                else "manual_quota_reset"
                if until is None
                else "local_exponential_backoff",
                time.time(),
            ),
        )

    def quotas(self):
        with self.connection() as db:
            return [dict(row) for row in db.execute("SELECT * FROM quota_cooldowns")]

    def quota_resume(self, group, revision, request_id, evidence):
        if not isinstance(evidence, str) or not evidence.strip():
            raise BridgeError("invalid_quota", "Provide evidence that the provider quota is available again")
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(
                db, request_id, {"op": "quota_resume", "group": group, "revision": revision, "evidence": evidence}
            )
            if old is not None:
                return old
            row = db.execute("SELECT * FROM quota_cooldowns WHERE quota_group=?", (group,)).fetchone()
            if not row or row["revision"] != revision:
                raise BridgeError("stale_revision", "Inspect the quota cooldown before resuming")
            db.execute(
                "UPDATE quota_cooldowns SET blocked_until=0,failures=0,revision=revision+1,reason=?,updated=? WHERE quota_group=?",
                (evidence, time.time(), group),
            )
            self.event(db, "quota_resumed", {"quota_group": group, "evidence": evidence})
            return self.remember(db, request_id, fingerprint, {"quota_group": group, "revision": revision + 1})

    def record(self, kind, data, session_id, task_id=None, attempt_id=None):
        with self.connection(write=True) as db:
            if kind == "first_output":
                changed = db.execute(
                    "UPDATE attempts SET first_output=? WHERE id=? AND session_id=? AND first_output IS NULL AND status='running'",
                    (time.time(), attempt_id, session_id),
                ).rowcount
                if not changed:
                    return None
            if task_id and (kind in {"tool_started", "tool_completed"} or len(encode(data)) > 8192):
                artifact = self.save_artifact(db, task_id, attempt_id, kind, data)
                data = {
                    key: artifacts.brief(data[key], 1000)
                    for key in ("id", "type", "tool", "command", "status", "exitCode", "is_error")
                    if key in data
                }
                data["artifact"] = artifact
            return self.event(db, kind, data, session_id, task_id, attempt_id)

    def save_artifact(self, db, task_id, attempt_id, kind, value):
        metadata = artifacts.save(self.state, value)
        db.execute(
            "INSERT INTO artifacts VALUES(?,?,?,?,?,?)",
            (metadata["artifact_id"], task_id, attempt_id, kind, encode(metadata), time.time()),
        )
        return metadata

    def read_artifact(self, task_id, artifact_id, offset=0, limit=4000):
        with self.connection() as db:
            row = db.execute(
                "SELECT metadata FROM artifacts WHERE task_id=? AND id=?", (task_id, artifact_id)
            ).fetchone()
            if not row:
                raise BridgeError("not_found", "Artifact does not belong to this task")
            return artifacts.read(self.state, artifact_id, json.loads(row[0]), offset, limit)

    def public_task(self, task_id, attempt_id=None):
        task = self.get_task(task_id)
        task["attempt_count"] = len(task["attempts"])
        if attempt_id:
            task["attempts"] = [attempt for attempt in task["attempts"] if attempt["id"] == attempt_id]
            if not task["attempts"]:
                raise BridgeError("not_found", "Attempt does not belong to this task")
        else:
            task["attempts"] = task["attempts"][-20:]
        task = artifacts.brief(task)
        with self.connection() as db:
            task["artifacts"] = [
                {"kind": row["kind"], "attempt_id": row["attempt_id"], **json.loads(row["metadata"])}
                for row in db.execute(
                    "SELECT kind,attempt_id,metadata FROM artifacts WHERE task_id=? ORDER BY created DESC LIMIT 20",
                    (task_id,),
                )
            ]
        return task

    def progress_usage(self, session_id, generation, task_id, attempt_id, usage):
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            task = self.required(db, "tasks", task_id)
            if (
                session["generation"] == generation
                and task["active_attempt"] == attempt_id
                and task["status"] in {"running", "waiting_input"}
            ):
                db.execute("UPDATE attempts SET usage=? WHERE id=?", (encode(usage), attempt_id))

    def question(self, session_id, task_id, attempt_id, data, expires=None):
        with self.connection(write=True) as db:
            task = self.required(db, "tasks", task_id)
            session = self.required(db, "sessions", session_id)
            if (
                task["active_attempt"] != attempt_id
                or task["status"] not in {"running", "waiting_input"}
                or session["desired"] not in {"open", "draining"}
            ):
                raise BridgeError("expired_question", "This attempt cannot request input")
            key = identifier()
            db.execute(
                "INSERT INTO questions(id,session_id,task_id,attempt_id,data,created,expires) VALUES(?,?,?,?,?,?,?)",
                (key, session_id, task_id, attempt_id, encode(data), time.time(), expires),
            )
            db.execute("UPDATE tasks SET status='waiting_input' WHERE id=?", (task_id,))
            db.execute("UPDATE sessions SET observed='waiting_input',revision=revision+1 WHERE id=?", (session_id,))
            seq = self.event(
                db, "input_required", {**data, "question_id": key, "expires": expires}, session_id, task_id, attempt_id
            )
            self.notify_parent(db, session, seq)
            return key

    def get_question(self, question_id):
        with self.connection(write=True) as db:
            db.execute(
                "UPDATE questions SET status='expired' WHERE id=? AND status='pending' AND expires<=?",
                (question_id, time.time()),
            )
            row = self.required(db, "questions", question_id)
        row["answer"] = json.loads(row["answer"]) if row["answer"] else None
        return row

    def parent_control(
        self,
        parent_id,
        action,
        revision,
        request_id,
        native_id=None,
        cwd=None,
        event_seq=None,
        usage_path=None,
        usage_since=None,
        notify=True,
        usage_price=None,
        resume_config_file=None,
    ):
        with self.connection(write=True) as db:
            payload = {
                "op": "parent",
                "parent": parent_id,
                "action": action,
                "revision": revision,
                "native": native_id,
                "cwd": cwd,
                "event": event_seq,
                "usage_path": usage_path,
                "usage_since": usage_since,
                "notify": notify,
                "usage_price": usage_price,
                "resume_config_file": resume_config_file,
            }
            fingerprint, old = self.previous(db, request_id, payload)
            if old is not None:
                return old
            row = db.execute("SELECT * FROM parents WHERE id=?", (parent_id,)).fetchone()
            if action == "parent_register":
                if row or revision != 0 or not native_id or not cwd or not Path(cwd).is_dir():
                    raise BridgeError(
                        "invalid_parent", "Register a new parent with its exact native ID and existing cwd"
                    )
                if (
                    db.execute("SELECT 1 FROM parents WHERE native_id=?", (native_id,)).fetchone()
                    or db.execute("SELECT 1 FROM sessions WHERE native_id=?", (native_id,)).fetchone()
                ):
                    raise BridgeError(
                        "invalid_parent", "This native session is already registered as a parent or child"
                    )
                meter = start_meter(usage_path, native_id, usage_since, usage_price) if usage_path else None
                if resume_config_file:
                    read_resume_config(resume_config_file)
                db.execute(
                    "INSERT INTO parents(id,runtime,native_id,cwd,notify,resume_config_file) VALUES(?,'codex',?,?,?,?)",
                    (
                        parent_id, native_id, str(Path(cwd).resolve()), int(notify),
                        str(Path(resume_config_file).expanduser().resolve()) if resume_config_file else None,
                    ),
                )
                if meter is not None:
                    db.execute("INSERT INTO parent_usage VALUES(?,?)", (parent_id, encode(meter)))
                return self.remember(
                    db,
                    request_id,
                    fingerprint,
                    {
                        "parent_id": parent_id,
                        "revision": 0,
                        "desired": "open",
                        "notify": notify,
                        "usage": meter_usage(meter) if meter is not None else None,
                    },
                )
            if not row or row["revision"] != revision:
                raise BridgeError("stale_revision", "Inspect the registered parent before controlling it")
            if action == "parent_meter_stop":
                usage = db.execute("SELECT state FROM parent_usage WHERE parent_id=?", (parent_id,)).fetchone()
                if not usage:
                    raise BridgeError("invalid_meter", "This parent has no usage meter")
                meter = refresh_meter(json.loads(usage[0]))
                meter["stopped"] = True
                meter["stopped_at"] = time.time()
                db.execute("UPDATE parent_usage SET state=? WHERE parent_id=?", (encode(meter), parent_id))
                desired = row["desired"]
            elif action == "parent_ack":
                changed = db.execute(
                    "UPDATE deliveries SET status='processed',queue_cleanup='pending' WHERE event_seq=? AND parent_id=? AND status IN ('sending','uncertain','accepted','wake_pending')",
                    (event_seq, parent_id),
                ).rowcount
                if not changed:
                    raise BridgeError(
                        "invalid_delivery", "Only a delivery sent to this parent can be acknowledged"
                    )
                # A parent can handle an event through agent_result before its queued notice is read.
                desired = row["desired"]
            else:
                desired = {
                    "parent_pause": "paused",
                    "parent_resume": "open",
                    "parent_close": "closed",
                    "parent_reopen": "open",
                }.get(action)
                if not desired or row["desired"] == "closed" and action != "parent_reopen":
                    raise BridgeError("invalid_parent", "Closed parents require explicit parent_reopen")
            db.execute("UPDATE parents SET desired=?,revision=revision+1 WHERE id=?", (desired, parent_id))
            return self.remember(
                db, request_id, fingerprint, {"parent_id": parent_id, "desired": desired, "revision": revision + 1}
            )

    def get_parent(self, parent_id):
        with self.connection() as db:
            row = db.execute("SELECT * FROM parents WHERE id=?", (parent_id,)).fetchone()
            if not row:
                raise BridgeError("not_found", "Parent is not registered")
            usage = db.execute("SELECT state FROM parent_usage WHERE parent_id=?", (parent_id,)).fetchone()
            return {
                **dict(row),
                "usage": meter_usage(json.loads(usage[0])) if usage else None,
                "deliveries": [
                    dict(d)
                    for d in db.execute("SELECT * FROM deliveries WHERE parent_id=? ORDER BY event_seq", (parent_id,))
                ],
            }

    def refresh_parent_usage(self):
        with self.connection() as db:
            rows = db.execute("SELECT parent_id,state FROM parent_usage").fetchall()
        for row in rows:
            previous = json.loads(row["state"])
            if previous["stopped"]:
                continue
            updated = refresh_meter(previous)
            with self.connection(write=True) as db:
                # A concurrent refresh or explicit stop must not be overwritten.
                db.execute(
                    "UPDATE parent_usage SET state=? WHERE parent_id=? AND state=?",
                    (encode(updated), row["parent_id"], row["state"]),
                )

    def respond(self, session_id, question_id, answer, revision, request_id):
        if not isinstance(answer, dict):
            raise BridgeError("invalid_answer", "answer must match the native request's response object")
        with self.connection(write=True) as db:
            fingerprint, old = self.previous(
                db,
                request_id,
                {
                    "op": "respond",
                    "session": session_id,
                    "question": question_id,
                    "answer": answer,
                    "revision": revision,
                },
            )
            if old is not None:
                return old
            question = self.required(db, "questions", question_id)
            session = self.required(db, "sessions", session_id)
            if session["revision"] != revision:
                raise BridgeError("stale_revision", "Inspect the session before answering")
            if (
                question["session_id"] != session_id
                or question["status"] != "pending"
                or question["expires"] is not None and question["expires"] <= time.time()
                or session["desired"] not in {"open", "draining"}
            ):
                raise BridgeError("expired_question", "This request is no longer pending")
            db.execute("UPDATE questions SET status='answered',answer=? WHERE id=?", (encode(answer), question_id))
            pending = db.execute(
                "SELECT 1 FROM questions WHERE attempt_id=? AND status='pending'", (question["attempt_id"],)
            ).fetchone()
            state = "waiting_input" if pending else "running"
            db.execute(
                "UPDATE tasks SET status=? WHERE id=? AND status IN ('running','waiting_input')",
                (state, question["task_id"]),
            )
            db.execute("UPDATE sessions SET observed=?,revision=revision+1 WHERE id=?", (state, session_id))
            self.event(
                db,
                "input_answered",
                {"question_id": question_id},
                session_id,
                question["task_id"],
                question["attempt_id"],
            )
            return self.remember(db, request_id, fingerprint, {"question_id": question_id, "revision": revision + 1})

    def events(self, after=0, session_id=None, task_id=None, limit=100, *, exclude_mirrored=False):
        if not isinstance(after, int) or after < 0:
            raise BridgeError("invalid_cursor", "Event cursor must be a nonnegative integer")
        clauses, values = ["seq>?"], [after]
        for key, value in (("session_id", session_id), ("task_id", task_id)):
            if value:
                clauses.append(key + "=?")
                values.append(value)
        if exclude_mirrored:
            clauses.extend([
                "NOT EXISTS (SELECT 1 FROM peer_event_mirrors m WHERE m.local_seq=events.seq)",
                "NOT EXISTS (SELECT 1 FROM mirrored_parent_notifications n WHERE n.local_seq=events.seq)",
                "kind!='peer_event_conflict'",
            ])
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM events WHERE " + " AND ".join(clauses) + " ORDER BY seq LIMIT ?",
                (*values, min(limit, 200)),
            ).fetchall()
        return [{**dict(row), "data": json.loads(row["data"])} for row in rows]

    # ---------------------------------------------------------------- multi-machine phase B storage

    @staticmethod
    def _decode_json(value):
        if value is None:
            return None
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value

    @classmethod
    def _peer_operation(cls, row):
        value = dict(row)
        value["params"] = cls._decode_json(value.get("params"))
        value["result"] = cls._decode_json(value.get("result"))
        value["error"] = cls._decode_json(value.get("error"))
        return value

    @classmethod
    def _peer_inbox(cls, row):
        value = dict(row)
        value["payload"] = cls._decode_json(value.get("payload"))
        value["receipt"] = cls._decode_json(value.get("receipt"))
        return value

    @classmethod
    def _node_outbox(cls, row):
        value = dict(row)
        value["payload"] = cls._decode_json(value.get("payload"))
        value["result"] = cls._decode_json(value.get("result"))
        value["sender_ref"] = cls._decode_json(value.get("sender_ref"))
        value["authority_ref"] = cls._decode_json(value.get("authority_ref"))
        return value

    # ---------------------------------------------------------------- multi-machine phase C streams and commit log

    @staticmethod
    def _stream_ref(value, name):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError as error:
                raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"{name} must be a JSON object") from error
        if not isinstance(value, dict):
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"{name} must be an object")
        return value

    def _stream_identity(self, authority_ref, target_ref):
        authority = self._stream_ref(authority_ref, "authority_ref")
        target = self._stream_ref(target_ref, "target_ref")
        try:
            stream_id = compute_stream_id(authority, target)
        except (KeyError, TypeError) as error:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"Invalid stream reference: {error}") from error
        return stream_id, encode(authority), encode(target)

    @classmethod
    def _stream_row(cls, row):
        value = dict(row)
        value["authority_ref"] = cls._decode_json(value.get("authority_ref"))
        value["target_ref"] = cls._decode_json(value.get("target_ref"))
        return value

    @staticmethod
    def _stream_id(stream_id):
        if not isinstance(stream_id, str) or not stream_id:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "stream_id must be a non-empty string")
        return stream_id

    def record_commit(self, mutation_key: str, method: str, *, db=None) -> int:
        """Append one committed mutation and return its durable, monotonically increasing ack_seq.

        `mutation_key` is the natural key for the mutation, so replaying the same key returns the original
        sequence instead of allocating a second one. Pass `db` to participate in an existing transaction.
        """
        if not isinstance(mutation_key, str) or not mutation_key:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "mutation_key must be a non-empty string")
        if not isinstance(method, str) or not method:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "method must be a non-empty string")
        if db is not None:
            return self._record_commit(db, mutation_key, method)
        with self.connection(write=True) as connection:
            return self._record_commit(connection, mutation_key, method)

    def max_peer_commit_seq(self, *, db=None) -> int:
        if db is not None:
            return db.execute("SELECT COALESCE(MAX(ack_seq),0) FROM peer_commit_log").fetchone()[0]
        with self.connection() as connection:
            return self.max_peer_commit_seq(db=connection)

    @staticmethod
    def _record_commit(db, mutation_key: str, method: str) -> int:
        existing = db.execute(
            "SELECT ack_seq FROM peer_commit_log WHERE mutation_key=?", (mutation_key,)
        ).fetchone()
        if existing is not None:
            return existing["ack_seq"]
        cursor = db.execute(
            "INSERT INTO peer_commit_log(mutation_key,method,created) VALUES(?,?,?)",
            (mutation_key, method, time.time()),
        )
        return cursor.lastrowid

    def get_or_create_target_stream(self, authority_ref: dict | str, target_ref: dict | str) -> dict:
        """Return the coordination-side ordered stream for one authority/target pair."""
        stream_id, encoded_authority, encoded_target = self._stream_identity(authority_ref, target_ref)
        with self.connection(write=True) as db:
            db.execute(
                "INSERT INTO target_streams(stream_id,authority_ref,target_ref) VALUES(?,?,?) "
                "ON CONFLICT(stream_id) DO NOTHING",
                (stream_id, encoded_authority, encoded_target),
            )
            row = db.execute("SELECT * FROM target_streams WHERE stream_id=?", (stream_id,)).fetchone()
        return self._stream_row(row)

    def allocate_target_seq(self, stream_id: str) -> int:
        """Allocate the next target sequence for a stream; monotonic and gap-free per stream."""
        stream_id = self._stream_id(stream_id)
        with self.connection(write=True) as db:
            row = db.execute("SELECT next_seq FROM target_streams WHERE stream_id=?", (stream_id,)).fetchone()
            if row is None:
                raise BridgeError("not_found", f"target_streams: {stream_id}")
            seq = row["next_seq"]
            db.execute("UPDATE target_streams SET next_seq=? WHERE stream_id=?", (seq + 1, stream_id))
            return seq

    def advance_released_seq(self, stream_id: str, seq: int) -> None:
        """Advance a stream's released sequence, never moving it backwards."""
        stream_id = self._stream_id(stream_id)
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "seq must be a non-negative integer")
        with self.connection(write=True) as db:
            if db.execute("SELECT 1 FROM target_streams WHERE stream_id=?", (stream_id,)).fetchone() is None:
                raise BridgeError("not_found", f"target_streams: {stream_id}")
            try:
                db.execute(
                    "UPDATE target_streams SET released_seq=MAX(released_seq,?) WHERE stream_id=?",
                    (seq, stream_id),
                )
            except sqlite3.IntegrityError as error:
                raise BridgeError(
                    PeerErrorCode.INVALID_PARAMS, "released seq must be below the allocated next_seq"
                ) from error

    def get_or_create_incoming_stream(self, authority_ref: dict | str, target_ref: dict | str) -> dict:
        """Return the execution-side admission stream for one authority/target pair."""
        stream_id, encoded_authority, encoded_target = self._stream_identity(authority_ref, target_ref)
        with self.connection(write=True) as db:
            db.execute(
                "INSERT INTO incoming_streams(stream_id,authority_ref,target_ref) VALUES(?,?,?) "
                "ON CONFLICT(stream_id) DO NOTHING",
                (stream_id, encoded_authority, encoded_target),
            )
            row = db.execute("SELECT * FROM incoming_streams WHERE stream_id=?", (stream_id,)).fetchone()
        return self._stream_row(row)

    def advance_admitted_seq(self, stream_id: str, seq: int) -> None:
        """Advance an incoming stream's admitted sequence, never moving it backwards."""
        stream_id = self._stream_id(stream_id)
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "seq must be a non-negative integer")
        with self.connection(write=True) as db:
            if db.execute("SELECT 1 FROM incoming_streams WHERE stream_id=?", (stream_id,)).fetchone() is None:
                raise BridgeError("not_found", f"incoming_streams: {stream_id}")
            db.execute(
                "UPDATE incoming_streams SET admitted_seq=MAX(admitted_seq,?) WHERE stream_id=?",
                (seq, stream_id),
            )

    def record_operation(
        self,
        operation_id,
        node_id=None,
        state_generation=None,
        fingerprint=None,
        method=None,
        params=None,
        status="pending",
        result=None,
        error=None,
        request_id=None,
        *,
        db=None,
    ):
        """Persist one idempotency record for `(node_id, state_generation, operation_id)` (ADR-04).

        Re-recording the same operation with an identical fingerprint returns the existing row, which lets a
        crash/retry path inspect the stored outcome instead of running the work twice. Pass `db` to join a
        caller's transaction when the operation result must commit together with its external effect.
        """
        if not isinstance(operation_id, str) or not operation_id:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "operation_id must be a non-empty string")
        now = time.time()
        encoded_params = encode(params if params is not None else {})
        encoded_result = encode(result) if result is not None else None
        encoded_error = None if error is None else (error if isinstance(error, str) else encode(error))
        stored_request_id = request_id if isinstance(request_id, str) and request_id else None

        def record(connection):
            existing = connection.execute(
                "SELECT * FROM peer_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if existing is not None:
                if fingerprint is not None and existing["fingerprint"] != fingerprint:
                    raise BridgeError(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "operation_id was already used for different parameters",
                    )
                if stored_request_id is not None and not existing["request_id"]:
                    connection.execute(
                        "UPDATE peer_operations SET request_id=? WHERE operation_id=?",
                        (stored_request_id, operation_id),
                    )
                    existing = connection.execute(
                        "SELECT * FROM peer_operations WHERE operation_id=?", (operation_id,)
                    ).fetchone()
                return self._peer_operation(existing)
            connection.execute(
                "INSERT INTO peer_operations(operation_id,node_id,state_generation,fingerprint,method,params,status,result,error,created,updated,request_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    node_id or "",
                    state_generation or "",
                    fingerprint or "",
                    method or "",
                    encoded_params,
                    status,
                    encoded_result,
                    encoded_error,
                    now,
                    now,
                    stored_request_id,
                ),
            )
            row = connection.execute("SELECT * FROM peer_operations WHERE operation_id=?", (operation_id,)).fetchone()
            return self._peer_operation(row)

        if db is not None:
            return record(db)
        with self.connection(write=True) as connection:
            return record(connection)

    def get_operation(self, operation_id, node_id=None, state_generation=None, fingerprint=None):
        """Return one operation record, optionally constrained to the ADR-04 identity namespace.

        A supplied fingerprint only matches when it equals the stored one, so callers can distinguish a replay
        from a conflicting reuse of the same operation_id.
        """
        if not isinstance(operation_id, str) or not operation_id:
            return None
        with self.connection() as db:
            row = db.execute("SELECT * FROM peer_operations WHERE operation_id=?", (operation_id,)).fetchone()
        if row is None:
            return None
        if node_id is not None and row["node_id"] != node_id:
            return None
        if state_generation is not None and row["state_generation"] != state_generation:
            return None
        if fingerprint is not None and row["fingerprint"] != fingerprint:
            return None
        return self._peer_operation(row)

    def update_operation(self, operation_id, status=None, result=None, error=None, params=None):
        """Update an existing operation record and touch `updated`; returns the refreshed row."""
        assignments = ["updated=?"]
        values = [time.time()]
        for column, value in (("status", status), ("params", params)):
            if value is not None:
                assignments.append(f"{column}=?")
                values.append(encode(value) if column == "params" else value)
        if result is not None:
            assignments.append("result=?")
            values.append(encode(result))
        if error is not None:
            assignments.append("error=?")
            values.append(error if isinstance(error, str) else encode(error))
        values.append(operation_id)
        with self.connection(write=True) as db:
            changed = db.execute(
                "UPDATE peer_operations SET " + ",".join(assignments) + " WHERE operation_id=?", values
            ).rowcount
            if not changed:
                raise BridgeError(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown operation: {operation_id}")
            row = db.execute("SELECT * FROM peer_operations WHERE operation_id=?", (operation_id,)).fetchone()
            return self._peer_operation(row)

    def record_inbox_delivery(
        self,
        delivery_id,
        host_id,
        node_id,
        state_generation,
        target_seq,
        operation_id,
        payload,
        status="received",
        receipt=None,
        seq_kind="delivery",
    ):
        """Persist one received delivery intent (ADR-07/ADR-08).

        The `(host_id, node_id, state_generation, target_seq)` unique constraint prevents two different
        deliveries from silently occupying the same sequence slot. Re-recording the same delivery_id is
        idempotent so a resent frame cannot create a duplicate.
        """
        for name, value in (
            ("delivery_id", delivery_id),
            ("host_id", host_id),
            ("node_id", node_id),
            ("state_generation", state_generation),
            ("operation_id", operation_id),
        ):
            if not isinstance(value, str) or not value:
                raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"{name} must be a non-empty string")
        if isinstance(target_seq, bool) or not isinstance(target_seq, int) or target_seq < 0:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "target_seq must be a non-negative integer")
        if not isinstance(seq_kind, str) or not seq_kind:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "seq_kind must be a non-empty string")
        encoded_payload = encode(payload)
        encoded_receipt = None if receipt is None else encode(receipt)
        with self.connection(write=True) as db:
            existing = db.execute("SELECT * FROM peer_inbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if existing is not None:
                if existing["operation_id"] != operation_id:
                    raise BridgeError(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "delivery_id was already used for a different operation",
                    )
                return self._peer_inbox(existing)
            try:
                db.execute(
                    "INSERT INTO peer_inbox(delivery_id,host_id,node_id,state_generation,target_seq,seq_kind,operation_id,payload,status,receipt,created) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        delivery_id,
                        host_id,
                        node_id,
                        state_generation,
                        target_seq,
                        seq_kind,
                        operation_id,
                        encoded_payload,
                        status,
                        encoded_receipt,
                        time.time(),
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise BridgeError(
                    PeerErrorCode.SEQUENCE_GAP,
                    f"target_seq {target_seq} is already recorded for this node",
                ) from error
            row = db.execute("SELECT * FROM peer_inbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            return self._peer_inbox(row)

    def get_inbox_delivery(self, delivery_id):
        if not isinstance(delivery_id, str) or not delivery_id:
            return None
        with self.connection() as db:
            row = db.execute("SELECT * FROM peer_inbox WHERE delivery_id=?", (delivery_id,)).fetchone()
        return None if row is None else self._peer_inbox(row)

    def add_outbox(
        self,
        operation_id,
        method,
        payload,
        group_id=None,
        native_id=None,
        status="pending",
        *,
        binding_id=None,
        request_id=None,
        sender_ref=None,
        authority_ref=None,
        authorization_revision=None,
        fingerprint=None,
    ):
        """Enqueue one outbound request; duplicate operation_ids are idempotent (ADR-04)."""
        if not isinstance(operation_id, str) or not operation_id:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "operation_id must be a non-empty string")
        if not isinstance(method, str) or not method:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "method must be a non-empty string")
        encoded_payload = encode(payload)
        encoded_sender = sender_ref if isinstance(sender_ref, str) else (
            None if sender_ref is None else encode(sender_ref)
        )
        encoded_authority = authority_ref if isinstance(authority_ref, str) else (
            None if authority_ref is None else encode(authority_ref)
        )
        with self.connection(write=True) as db:
            existing = db.execute("SELECT * FROM node_outbox WHERE operation_id=?", (operation_id,)).fetchone()
            if existing is None and binding_id is not None and request_id is not None:
                existing = db.execute(
                    "SELECT * FROM node_outbox WHERE binding_id=? AND request_id=?", (binding_id, request_id)
                ).fetchone()
                if existing is not None and existing["operation_id"] != operation_id:
                    raise BridgeError(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "request_id was already used by a different member operation",
                    )
            if existing is not None:
                if existing["payload"] != encoded_payload or existing["method"] != method:
                    raise BridgeError(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "operation_id was already used for a different outbox request",
                    )
                return existing["seq"]
            # Reject an unsendable request before it can block a durable member outbox.
            wire_fields = {
                "operation_id": operation_id, "method": method, "payload": payload,
                "group_id": group_id, "native_id": native_id, "binding_id": binding_id,
                "request_id": request_id, "sender_ref": encoded_sender, "authority_ref": encoded_authority,
                "authorization_revision": authorization_revision, "fingerprint": fingerprint,
            }
            if len(encode(wire_fields).encode("utf-8")) > MAX_FRAME_BYTES - 4096:
                raise BridgeError(PeerErrorCode.FRAME_TOO_LARGE, "Member request is too large for one relay frame")
            cursor = db.execute(
                "INSERT INTO node_outbox(operation_id,group_id,native_id,method,payload,status,created,"
                "binding_id,request_id,sender_ref,authority_ref,authorization_revision,fingerprint) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_id, group_id, native_id, method, encoded_payload, status, time.time(),
                    binding_id, request_id, encoded_sender, encoded_authority, authorization_revision, fingerprint,
                ),
            )
            return cursor.lastrowid

    def get_node_outbox(self, operation_id):
        """Return one locally persisted member outbox row, decoded, or None."""
        if not isinstance(operation_id, str) or not operation_id:
            return None
        with self.connection() as db:
            row = db.execute("SELECT * FROM node_outbox WHERE operation_id=?", (operation_id,)).fetchone()
        return None if row is None else self._node_outbox(row)

    def pull_outbox(self, after=0, limit=100):
        """Return pending/pulled outbox rows after `after`, marking them pulled for retry safety."""
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "after must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "limit must be a positive integer")
        limit = min(limit, 100)
        now = time.time()
        with self.connection(write=True) as db:
            rows = db.execute(
                "SELECT * FROM node_outbox WHERE seq>? AND status IN ('pending','pulled') ORDER BY seq LIMIT ?",
                (after, limit),
            ).fetchall()
            if rows:
                db.executemany(
                    "UPDATE node_outbox SET status='pulled',pulled_at=? WHERE seq=?",
                    [(now, row["seq"]) for row in rows],
                )
                marks = ",".join("?" * len(rows))
                rows = db.execute(
                    f"SELECT * FROM node_outbox WHERE seq IN ({marks}) ORDER BY seq",
                    [row["seq"] for row in rows],
                ).fetchall()
        return [self._node_outbox(row) for row in rows]

    def ack_outbox(self, operation_id, result=None, outcome="accepted", status="acked"):
        """Mark one outbox request acknowledged, persisting `result` and `outcome`.

        Replaying the same outcome/result is idempotent; a different stored outcome/result is a conflict.
        """
        if not isinstance(operation_id, str) or not operation_id:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "operation_id must be a non-empty string")
        if not isinstance(outcome, str) or not outcome:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "outcome must be a non-empty string")
        encoded_result = None if result is None else encode(result)
        with self.connection(write=True) as db:
            row = db.execute("SELECT * FROM node_outbox WHERE operation_id=?", (operation_id,)).fetchone()
            if row is None:
                raise BridgeError(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown outbox operation: {operation_id}")
            if row["status"] == "acked" and row["outcome"] is not None:
                if row["outcome"] != outcome or (encoded_result is not None and row["result"] != encoded_result):
                    raise BridgeError(
                        PeerErrorCode.OPERATION_CONFLICT, "outbox operation was already acknowledged differently"
                    )
                return self._node_outbox(row)
            db.execute(
                "UPDATE node_outbox SET status=?,acked_at=?,result=?,outcome=? WHERE operation_id=?",
                (status, time.time(), encoded_result, outcome, operation_id),
            )
            row = db.execute("SELECT * FROM node_outbox WHERE operation_id=?", (operation_id,)).fetchone()
            return self._node_outbox(row)

    # ---------------------------------------------------------------- multi-machine phase C event mirror

    @staticmethod
    def _non_empty_text(value, name):
        if not isinstance(value, str) or not value:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"{name} must be a non-empty string")
        return value

    @classmethod
    def _peer_identity(cls, host_id, node_id, state_generation):
        return (
            cls._non_empty_text(host_id, "host_id"),
            cls._non_empty_text(node_id, "node_id"),
            cls._non_empty_text(state_generation, "state_generation"),
        )

    @staticmethod
    def _seq_value(value, name, minimum=0):
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"{name} must be an integer >= {minimum}")
        return value

    def register_parent_route(self, host_id, node_id, state_generation, child_id, parent_ref):
        """Persist the authorised parent route of one remote child; a different parent is a route conflict."""
        identity = self._peer_identity(host_id, node_id, state_generation)
        if not isinstance(child_id, str) or not child_id:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "child_id must be a non-empty string")
        if not isinstance(parent_ref, dict) or not parent_ref:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "parent_ref must be a non-empty object")
        encoded = encode(parent_ref)
        with self.connection(write=True) as db:
            row = db.execute(
                "SELECT * FROM peer_parent_routes WHERE host_id=? AND node_id=? AND state_generation=? AND child_id=?",
                (*identity, child_id),
            ).fetchone()
            if row is not None:
                if row["parent_ref"] != encoded:
                    raise BridgeError(PeerErrorCode.PARENT_ROUTE_CONFLICT, "child already routes to a different parent")
                return self._parent_route(row)
            db.execute(
                "INSERT INTO peer_parent_routes(host_id,node_id,state_generation,child_id,parent_ref,created) "
                "VALUES(?,?,?,?,?,?)",
                (*identity, child_id, encoded, time.time()),
            )
            row = db.execute(
                "SELECT * FROM peer_parent_routes WHERE host_id=? AND node_id=? AND state_generation=? AND child_id=?",
                (*identity, child_id),
            ).fetchone()
            return self._parent_route(row)

    @classmethod
    def _parent_route(cls, row):
        value = dict(row)
        value["parent_ref"] = cls._decode_json(value["parent_ref"])
        return value

    def lookup_parent_route(self, host_id, node_id, state_generation, child_id):
        identity = self._peer_identity(host_id, node_id, state_generation)
        if not isinstance(child_id, str) or not child_id:
            return None
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM peer_parent_routes WHERE host_id=? AND node_id=? AND state_generation=? AND child_id=?",
                (*identity, child_id),
            ).fetchone()
        return None if row is None else self._parent_route(row)

    def lookup_parent_route_by_node(self, node_id, state_generation, child_id):
        node_id = self._non_empty_text(node_id, "node_id")
        state_generation = self._non_empty_text(state_generation, "state_generation")
        if not isinstance(child_id, str) or not child_id:
            return None
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM peer_parent_routes WHERE node_id=? AND state_generation=? AND child_id=? "
                "ORDER BY created, host_id LIMIT 1",
                (node_id, state_generation, child_id),
            ).fetchone()
        return None if row is None else self._parent_route(row)

    def get_peer_event_cursor(self, host_id, node_id, state_generation) -> int:
        identity = self._peer_identity(host_id, node_id, state_generation)
        with self.connection() as db:
            row = db.execute(
                "SELECT after_seq FROM peer_event_cursors WHERE host_id=? AND node_id=? AND state_generation=?",
                identity,
            ).fetchone()
        return 0 if row is None else row["after_seq"]

    def advance_peer_event_cursor(self, host_id, node_id, state_generation, after_seq, *, db=None):
        """Move the durable cursor forward only; a lower value never rewinds it. Returns the stored cursor."""
        identity = self._peer_identity(host_id, node_id, state_generation)
        after_seq = self._seq_value(after_seq, "after_seq")
        if db is not None:
            return self._advance_peer_event_cursor(db, identity, after_seq)
        with self.connection(write=True) as connection:
            return self._advance_peer_event_cursor(connection, identity, after_seq)

    @staticmethod
    def _advance_peer_event_cursor(db, identity, after_seq):
        db.execute(
            "INSERT INTO peer_event_cursors(host_id,node_id,state_generation,after_seq) VALUES(?,?,?,?) "
            "ON CONFLICT(host_id,node_id,state_generation) DO UPDATE SET after_seq=MAX(after_seq,excluded.after_seq)",
            (*identity, after_seq),
        )
        return db.execute(
            "SELECT after_seq FROM peer_event_cursors WHERE host_id=? AND node_id=? AND state_generation=?",
            identity,
        ).fetchone()["after_seq"]

    def mirror_peer_event(
        self,
        host_id,
        node_id,
        state_generation,
        remote_seq,
        fingerprint,
        local_seq,
        *,
        db=None,
        origin_node_id=None,
        origin_state_generation=None,
        origin_seq=None,
        origin_fingerprint=None,
    ):
        """Map a remote event seq to a local events.seq. Same fingerprint is a duplicate; different is a conflict."""
        identity = self._peer_identity(host_id, node_id, state_generation)
        remote_seq = self._seq_value(remote_seq, "remote_seq")
        local_seq = self._seq_value(local_seq, "local_seq", 1)
        if not isinstance(fingerprint, str) or not fingerprint:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "fingerprint must be a non-empty string")
        resolved_origin_node = self._non_empty_text(origin_node_id or identity[1], "origin_node_id")
        resolved_origin_gen = self._non_empty_text(origin_state_generation or identity[2], "origin_state_generation")
        resolved_origin_seq = self._seq_value(remote_seq if origin_seq is None else origin_seq, "origin_seq")
        resolved_origin_fp = origin_fingerprint if origin_node_id is not None else fingerprint
        origin = (resolved_origin_node, resolved_origin_gen, resolved_origin_seq, resolved_origin_fp)
        if db is not None:
            return self._mirror_peer_event(db, identity, remote_seq, fingerprint, local_seq, origin)
        with self.connection(write=True) as connection:
            return self._mirror_peer_event(connection, identity, remote_seq, fingerprint, local_seq, origin)

    @staticmethod
    def _mirror_peer_event(db, identity, remote_seq, fingerprint, local_seq, origin):
        row = db.execute(
            "SELECT * FROM peer_event_mirrors WHERE host_id=? AND node_id=? AND state_generation=? AND remote_seq=?",
            (*identity, remote_seq),
        ).fetchone()
        if row is not None:
            if row["fingerprint"] != fingerprint:
                raise BridgeError(PeerErrorCode.OPERATION_CONFLICT, "remote event seq was mirrored with different content")
            return dict(row)
        db.execute(
            "INSERT INTO peer_event_mirrors(host_id,node_id,state_generation,remote_seq,local_seq,fingerprint,created,"
            "origin_node_id,origin_state_generation,origin_seq,origin_fingerprint) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (*identity, remote_seq, local_seq, fingerprint, time.time(), *origin),
        )
        return dict(
            db.execute(
                "SELECT * FROM peer_event_mirrors WHERE host_id=? AND node_id=? AND state_generation=? "
                "AND remote_seq=?",
                (*identity, remote_seq),
            ).fetchone()
        )

    def get_mirrored_event(self, host_id, node_id, state_generation, remote_seq):
        identity = self._peer_identity(host_id, node_id, state_generation)
        remote_seq = self._seq_value(remote_seq, "remote_seq")
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM peer_event_mirrors WHERE host_id=? AND node_id=? AND state_generation=? AND remote_seq=?",
                (*identity, remote_seq),
            ).fetchone()
        return None if row is None else dict(row)

    def get_mirrored_event_by_origin(self, origin_node_id, origin_state_generation, origin_seq, *, db=None):
        origin_node_id = self._non_empty_text(origin_node_id, "origin_node_id")
        origin_state_generation = self._non_empty_text(origin_state_generation, "origin_state_generation")
        origin_seq = self._seq_value(origin_seq, "origin_seq")
        query = (
            "SELECT * FROM peer_event_mirrors WHERE origin_node_id=? AND origin_state_generation=? "
            "AND origin_seq=? ORDER BY CASE WHEN node_id=origin_node_id "
            "AND state_generation=origin_state_generation AND remote_seq=origin_seq THEN 0 ELSE 1 END, "
            "local_seq LIMIT 1"
        )
        if db is not None:
            row = db.execute(query, (origin_node_id, origin_state_generation, origin_seq)).fetchone()
            return None if row is None else dict(row)
        with self.connection() as connection:
            row = connection.execute(query, (origin_node_id, origin_state_generation, origin_seq)).fetchone()
        return None if row is None else dict(row)

    def prune_duplicate_mirrors(
        self,
        *,
        dry_run: bool = True,
        batch_size: int = 500,
        prune_self_origins: bool = True,
        repair_origins: bool = True,
        vacuum: bool = False,
        local_node_id: str | None = None,
    ) -> dict:
        """Inspect or prune duplicate peer event mirrors while keeping notifications, deliveries, and cursors intact."""
        from .mirror_prune import prune_duplicate_mirrors

        return prune_duplicate_mirrors(
            self,
            dry_run=dry_run,
            batch_size=batch_size,
            prune_self_origins=prune_self_origins,
            repair_origins=repair_origins,
            vacuum=vacuum,
            local_node_id=local_node_id,
        )

    @classmethod
    def _pulled_receipt(cls, row):
        value = dict(row)
        value["result"] = cls._decode_json(value["result"])
        return value

    def record_pulled_request_receipt(
        self, host_id, node_id, state_generation, operation_id, source_seq, fingerprint, outcome, result, *, db=None
    ):
        """Save the coordinator-side receipt; same fingerprint reuses the stored outcome/result."""
        identity = self._peer_identity(host_id, node_id, state_generation)
        source_seq = self._seq_value(source_seq, "source_seq")
        for name, value in (("operation_id", operation_id), ("fingerprint", fingerprint), ("outcome", outcome)):
            if not isinstance(value, str) or not value:
                raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"{name} must be a non-empty string")
        if db is not None:
            return self._record_pulled_request_receipt(
                db, identity, operation_id, source_seq, fingerprint, outcome, result
            )
        with self.connection(write=True) as connection:
            return self._record_pulled_request_receipt(
                connection, identity, operation_id, source_seq, fingerprint, outcome, result
            )

    def _record_pulled_request_receipt(self, db, identity, operation_id, source_seq, fingerprint, outcome, result):
        row = db.execute(
            "SELECT * FROM pulled_request_receipts WHERE host_id=? AND node_id=? AND state_generation=? "
            "AND operation_id=?",
            (*identity, operation_id),
        ).fetchone()
        if row is None:
            db.execute(
                "INSERT INTO pulled_request_receipts(host_id,node_id,state_generation,operation_id,source_seq,"
                "fingerprint,outcome,result,created) VALUES(?,?,?,?,?,?,?,?,?)",
                (*identity, operation_id, source_seq, fingerprint, outcome, encode(result), time.time()),
            )
            row = db.execute(
                "SELECT * FROM pulled_request_receipts WHERE host_id=? AND node_id=? AND state_generation=? "
                "AND operation_id=?",
                (*identity, operation_id),
            ).fetchone()
        elif row["fingerprint"] != fingerprint:
            raise BridgeError(PeerErrorCode.OPERATION_CONFLICT, "pulled request was already recorded differently")
        return self._pulled_receipt(row)

    def get_pulled_request_receipt(self, host_id, node_id, state_generation, operation_id):
        identity = self._peer_identity(host_id, node_id, state_generation)
        if not isinstance(operation_id, str) or not operation_id:
            return None
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM pulled_request_receipts WHERE host_id=? AND node_id=? AND state_generation=? "
                "AND operation_id=?",
                (*identity, operation_id),
            ).fetchone()
        return None if row is None else self._pulled_receipt(row)

    def update_pulled_request_receipt_acked(self, host_id, node_id, state_generation, operation_id, acked_at=None):
        identity = self._peer_identity(host_id, node_id, state_generation)
        with self.connection(write=True) as db:
            changed = db.execute(
                "UPDATE pulled_request_receipts SET acked_at=COALESCE(acked_at,?) WHERE host_id=? AND node_id=? "
                "AND state_generation=? AND operation_id=?",
                (time.time() if acked_at is None else acked_at, *identity, operation_id),
            ).rowcount
            if not changed:
                raise BridgeError(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown pulled request: {operation_id}")
            row = db.execute(
                "SELECT * FROM pulled_request_receipts WHERE host_id=? AND node_id=? AND state_generation=? "
                "AND operation_id=?",
                (*identity, operation_id),
            ).fetchone()
            return self._pulled_receipt(row)

    # ---------------------------------------------------------------- multi-machine phase C native send queue

    NATIVE_SEND_BLOCKING_STATUSES = ("pending", "sending", "uncertain")
    NATIVE_SEND_LEGACY_SOURCES = (str(DeliverySource.LOCAL_BROADCAST), "native_broadcast")

    @staticmethod
    def _native_send_text(value, name):
        if not isinstance(value, str) or not value:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"{name} must be a non-empty string")
        return value

    @classmethod
    def _allocate_native_local_seq(cls, db, runtime, native_id):
        row = db.execute(
            "SELECT next_seq FROM native_target_counters WHERE runtime=? AND native_id=?",
            (runtime, native_id),
        ).fetchone()
        if row is None:
            db.execute(
                "INSERT INTO native_target_counters(runtime,native_id,next_seq) VALUES(?,?,2)",
                (runtime, native_id),
            )
            return 1
        seq = row["next_seq"]
        db.execute(
            "UPDATE native_target_counters SET next_seq=? WHERE runtime=? AND native_id=?",
            (seq + 1, runtime, native_id),
        )
        return seq

    def allocate_native_local_seq(self, runtime: str, native_id: str) -> int:
        """Allocate the next gap-free local write sequence for one physical native target, starting at 1."""
        runtime = self._native_send_text(runtime, "runtime")
        native_id = self._native_send_text(native_id, "native_id")
        with self.connection(write=True) as db:
            return self._allocate_native_local_seq(db, runtime, native_id)

    def enqueue_native_send(
        self,
        runtime: str,
        native_id: str,
        delivery_source: str,
        delivery_id: str,
        message: str,
        status: str = "pending",
        *,
        item_id: str | None = None,
        db=None,
    ) -> dict:
        """Persist one write to a native target, ordered by `(runtime, native_id, local_seq)`.

        `(delivery_source, delivery_id)` is the idempotency key: replaying the same logical delivery returns the
        existing row instead of allocating another sequence. Pass `db` to join a caller's transaction; groups use
        that to commit the legacy row and the queue row atomically.
        """
        runtime = self._native_send_text(runtime, "runtime")
        native_id = self._native_send_text(native_id, "native_id")
        delivery_source = self._native_send_text(delivery_source, "delivery_source")
        delivery_id = self._native_send_text(delivery_id, "delivery_id")
        if not isinstance(message, str):
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "message must be a string")
        status = self._native_send_text(status, "status")

        def enqueue(connection):
            existing = connection.execute(
                "SELECT * FROM native_send_queue WHERE delivery_source=? AND delivery_id=?",
                (delivery_source, delivery_id),
            ).fetchone()
            if existing is not None:
                return dict(existing)
            local_seq = self._allocate_native_local_seq(connection, runtime, native_id)
            queue_id = item_id or identifier()
            connection.execute(
                "INSERT INTO native_send_queue("
                "id,runtime,native_id,local_seq,delivery_source,delivery_id,message,status,created) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (queue_id, runtime, native_id, local_seq, delivery_source, delivery_id, message, status, time.time()),
            )
            row = connection.execute("SELECT * FROM native_send_queue WHERE id=?", (queue_id,)).fetchone()
            return dict(row)

        if db is not None:
            return enqueue(db)
        with self.connection(write=True) as connection:
            return enqueue(connection)

    def get_native_send_queue_item(self, item_id: str) -> dict | None:
        if not isinstance(item_id, str) or not item_id:
            return None
        with self.connection() as db:
            row = db.execute("SELECT * FROM native_send_queue WHERE id=?", (item_id,)).fetchone()
        return None if row is None else dict(row)

    def get_native_send_by_delivery(self, delivery_source: str, delivery_id: str) -> dict | None:
        if not isinstance(delivery_source, str) or not delivery_source:
            return None
        if not isinstance(delivery_id, str) or not delivery_id:
            return None
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM native_send_queue WHERE delivery_source=? AND delivery_id=?",
                (delivery_source, delivery_id),
            ).fetchone()
        return None if row is None else dict(row)

    @classmethod
    def _claim_native_send_head(cls, db, allowed, now, limit=None):
        heads = db.execute(
            "SELECT runtime,native_id,MIN(local_seq) AS local_seq FROM native_send_queue "
            "WHERE status IN (?,?,?) GROUP BY runtime,native_id "
            "ORDER BY MIN(last_attempt),MIN(created),runtime,native_id",
            cls.NATIVE_SEND_BLOCKING_STATUSES,
        ).fetchall()
        claimed = []
        for head in heads:
            if limit is not None and len(claimed) >= limit:
                break
            row = db.execute(
                "SELECT * FROM native_send_queue WHERE runtime=? AND native_id=? AND local_seq=?",
                (head["runtime"], head["native_id"], head["local_seq"]),
            ).fetchone()
            if row is None or row["status"] != "pending":
                continue
            if allowed is not None and row["delivery_source"] not in allowed:
                continue
            if not db.execute(
                "UPDATE native_send_queue SET status='sending',last_attempt=?,revision=revision+1 "
                "WHERE id=? AND status='pending'",
                (now, row["id"]),
            ).rowcount:
                continue
            claimed.append(dict(db.execute("SELECT * FROM native_send_queue WHERE id=?", (row["id"],)).fetchone()))
        return claimed

    def claim_eligible_native_sends(self, limit: int = 10, *, delivery_sources=None) -> list[dict]:
        """Claim at most one pending head per target, up to `limit` targets, in fair order.

        A target whose oldest unfinished item is `sending` or `uncertain` is blocked: later items for that target
        must never overtake an in-flight or unconfirmed native write. Other targets are unaffected.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "limit must be a non-negative integer")
        if limit == 0:
            return []
        allowed = None if delivery_sources is None else {str(source) for source in delivery_sources}
        with self.connection(write=True) as db:
            return self._claim_native_send_head(db, allowed, time.time(), limit)

    def claim_native_send_head(self, delivery_source: str, delivery_id: str) -> dict | None:
        """Claim one delivery only when it is the pending head of its target's queue.

        Used by the parent-notification path, which must share the same per-target ordering as broadcasts
        without claiming work that belongs to another target.
        """
        delivery_source = self._native_send_text(delivery_source, "delivery_source")
        delivery_id = self._native_send_text(delivery_id, "delivery_id")
        with self.connection(write=True) as db:
            item = db.execute(
                "SELECT * FROM native_send_queue WHERE delivery_source=? AND delivery_id=?",
                (delivery_source, delivery_id),
            ).fetchone()
            if item is None or item["status"] != "pending":
                return None
            head = db.execute(
                "SELECT id FROM native_send_queue WHERE runtime=? AND native_id=? AND status IN (?,?,?) "
                "ORDER BY local_seq LIMIT 1",
                (item["runtime"], item["native_id"], *self.NATIVE_SEND_BLOCKING_STATUSES),
            ).fetchone()
            if head is None or head["id"] != item["id"]:
                return None
            if not db.execute(
                "UPDATE native_send_queue SET status='sending',last_attempt=?,revision=revision+1 "
                "WHERE id=? AND status='pending'",
                (time.time(), item["id"]),
            ).rowcount:
                return None
            return dict(db.execute("SELECT * FROM native_send_queue WHERE id=?", (item["id"],)).fetchone())

    @classmethod
    def _sync_native_send_legacy(cls, db, row):
        if row["delivery_source"] in cls.NATIVE_SEND_LEGACY_SOURCES:
            db.execute(
                "UPDATE native_broadcast_deliveries SET status=?,receipt=?,native_turn_id=?,"
                "processing_status=?,error=? WHERE id=?",
                (
                    row["status"], row["receipt"], row["native_turn_id"],
                    row["processing_status"], row["error"], row["delivery_id"],
                ),
            )
        elif row["delivery_source"] == str(DeliverySource.PARENT_NOTIFICATION):
            try:
                event_seq = int(row["delivery_id"])
            except (TypeError, ValueError):
                return
            # `deliveries` has its own accepted/wake_pending/processed state machine; notifications.py owns
            # those transitions. Only mirror the unambiguous transient states, and never undo a parent_ack.
            if row["status"] in ("pending", "sending", "uncertain"):
                db.execute(
                    "UPDATE deliveries SET status=?,receipt=COALESCE(?,receipt),error=? "
                    "WHERE event_seq=? AND status!='processed'",
                    (row["status"], row["receipt"], row["error"], event_seq),
                )
            else:
                db.execute(
                    "UPDATE deliveries SET receipt=COALESCE(?,receipt),error=? "
                    "WHERE event_seq=? AND status!='processed'",
                    (row["receipt"], row["error"], event_seq),
                )

    def update_native_send(
        self,
        item_id: str,
        status: str,
        receipt: str = None,
        native_turn_id: str = None,
        processing_status: str = None,
        error: str = None,
        *,
        expected_status: str | None = None,
        db=None,
    ) -> dict:
        """Update one queue row and mirror it to the legacy table for existing queries.

        `expected_status` gives callers compare-and-swap semantics: when the row moved on (for example a parent
        acknowledged an uncertain notification), the update is skipped and the current row is returned.
        """
        item_id = self._native_send_text(item_id, "item_id")
        status = self._native_send_text(status, "status")

        def update(connection):
            existing = connection.execute("SELECT * FROM native_send_queue WHERE id=?", (item_id,)).fetchone()
            if existing is None:
                raise BridgeError("not_found", f"native_send_queue: {item_id}")
            if expected_status is not None and existing["status"] != expected_status:
                return dict(existing)
            connection.execute(
                "UPDATE native_send_queue SET status=?,receipt=?,native_turn_id=?,processing_status=?,"
                "error=?,revision=revision+1 WHERE id=?",
                (status, receipt, native_turn_id, processing_status, error, item_id),
            )
            row = dict(connection.execute("SELECT * FROM native_send_queue WHERE id=?", (item_id,)).fetchone())
            self._sync_native_send_legacy(connection, row)
            return row

        if db is not None:
            return update(db)
        with self.connection(write=True) as connection:
            return update(connection)


    # ---------------------------------------------------------------- workspace transfers (design 4.4)

    @staticmethod
    def _workspace_transfer_row(row):
        if row is None:
            return None
        value = dict(row)
        value["authority_ref"] = Store._decode_json(value.get("authority_ref"))
        value["metadata"] = Store._decode_json(value.get("metadata"))
        return value

    def _with_db(self, db, action):
        if db is not None:
            return action(db)
        with self.connection(write=True) as connection:
            return action(connection)

    def create_workspace_transfer(
        self,
        workspace_id: str,
        authority_ref: dict,
        project_id: str,
        project_config_digest: str,
        project_root: str,
        manifest_sha256: str,
        delivery_mode: str,
        source_base_commit: str | None = None,
        metadata: dict | None = None,
        *,
        db=None,
    ) -> dict:
        """Record the initial `receiving` transfer; an identical replay returns the stored row."""
        encoded_authority = encode(authority_ref)

        def create(connection):
            now = time.time()
            connection.execute(
                "INSERT OR IGNORE INTO workspace_transfers(workspace_id,authority_ref,project_id,project_config_digest,"
                "project_root,manifest_sha256,source_base_commit,delivery_mode,status,metadata,revision,created,updated) "
                "VALUES(?,?,?,?,?,?,?,?,'receiving',?,0,?,?)",
                (workspace_id, encoded_authority, project_id, project_config_digest, project_root, manifest_sha256,
                 source_base_commit, delivery_mode, encode(metadata or {}), now, now),
            )
            row = connection.execute("SELECT * FROM workspace_transfers WHERE workspace_id=?", (workspace_id,)).fetchone()
            if (row["authority_ref"], row["project_id"], row["manifest_sha256"], row["delivery_mode"],
                    row["source_base_commit"], row["project_config_digest"]) != (
                    encoded_authority, project_id, manifest_sha256, delivery_mode, source_base_commit,
                    project_config_digest):
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "workspace_id already bound to a different transfer")
            return self._workspace_transfer_row(row)

        return self._with_db(db, create)

    def get_workspace_transfer(self, workspace_id: str) -> dict | None:
        with self.connection() as db:
            return self._workspace_transfer_row(
                db.execute("SELECT * FROM workspace_transfers WHERE workspace_id=?", (workspace_id,)).fetchone()
            )

    def update_workspace_transfer_status(
        self,
        workspace_id: str,
        status: str,
        expected_revision: int,
        ready_path: str | None = None,
        input_digest: str | None = None,
        error: str | None = None,
        db=None,
    ) -> int:
        """Revision-CAS status change; returns the new revision.

        Replaying a change that already took effect (same status, revision moved past `expected_revision`)
        returns the current revision. Any other revision mismatch is a workspace_conflict.
        """

        def update(connection):
            row = connection.execute(
                "SELECT status,revision,attached_session_id FROM workspace_transfers WHERE workspace_id=?",
                (workspace_id,),
            ).fetchone()
            if row is None:
                raise BridgeError("not_found", f"workspace_transfers: {workspace_id}")
            if row["revision"] != expected_revision:
                if row["status"] == status and row["revision"] > expected_revision:
                    return row["revision"]
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "workspace revision conflict")
            if row["attached_session_id"] is not None and status != row["status"]:
                raise BridgeError(PeerErrorCode.WORKSPACE_ALREADY_ATTACHED, "workspace is already attached")
            connection.execute(
                "UPDATE workspace_transfers SET status=?,ready_path=COALESCE(?,ready_path),"
                "input_digest=COALESCE(?,input_digest),error=?,revision=revision+1,updated=? WHERE workspace_id=?",
                (status, ready_path, input_digest, error, time.time(), workspace_id),
            )
            return expected_revision + 1

        return self._with_db(db, update)

    def record_workspace_chunk(
        self, workspace_id: str, blob_sha256: str, offset: int, size: int, chunk_sha256: str, stored_path: str,
        db=None,
    ) -> None:
        """Record one verified chunk. Same bytes replay is a no-op; different bytes at that offset conflict."""

        def record(connection):
            if connection.execute(
                "SELECT 1 FROM workspace_transfers WHERE workspace_id=?", (workspace_id,)
            ).fetchone() is None:
                raise BridgeError("not_found", f"workspace_transfers: {workspace_id}")
            existing = connection.execute(
                "SELECT size,chunk_sha256 FROM workspace_chunks WHERE workspace_id=? AND blob_sha256=? AND offset=?",
                (workspace_id, blob_sha256, offset),
            ).fetchone()
            if existing is not None:
                if (existing["size"], existing["chunk_sha256"]) != (size, chunk_sha256):
                    raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "chunk differs from the stored bytes")
                return
            if connection.execute(
                "SELECT 1 FROM workspace_chunks WHERE workspace_id=? AND blob_sha256=? "
                "AND offset<? AND offset+size>? LIMIT 1",
                (workspace_id, blob_sha256, offset + size, offset),
            ).fetchone() is not None:
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "chunk overlaps a stored chunk")
            connection.execute(
                "INSERT INTO workspace_chunks(workspace_id,blob_sha256,offset,size,chunk_sha256,stored_path) "
                "VALUES(?,?,?,?,?,?)",
                (workspace_id, blob_sha256, offset, size, chunk_sha256, stored_path),
            )

        self._with_db(db, record)

    def get_workspace_chunks(self, workspace_id: str, blob_sha256: str) -> list[dict]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM workspace_chunks WHERE workspace_id=? AND blob_sha256=? ORDER BY offset",
                (workspace_id, blob_sha256),
            ).fetchall()
        return [dict(row) for row in rows]

    def has_workspace_blob(self, sha256: str) -> bool:
        return self.get_workspace_blob(sha256) is not None

    def get_workspace_blob(self, sha256: str) -> dict | None:
        with self.connection() as db:
            row = db.execute("SELECT * FROM workspace_blobs WHERE sha256=?", (sha256,)).fetchone()
        return None if row is None else dict(row)

    def record_workspace_blob(self, sha256: str, size: int, stored_path: str, db=None) -> None:
        """Mark a blob verified in the shared cache; replaying the same digest and size is a no-op."""

        def record(connection):
            connection.execute(
                "INSERT OR IGNORE INTO workspace_blobs(sha256,size,stored_path,verified_at) VALUES(?,?,?,?)",
                (sha256, size, stored_path, time.time()),
            )
            if connection.execute("SELECT size FROM workspace_blobs WHERE sha256=?", (sha256,)).fetchone()["size"] != size:
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "blob size differs from the verified cache entry")

        self._with_db(db, record)

    def attach_workspace_transfer(self, workspace_id: str, session_id: str, db=None) -> None:
        """Bind a `ready` workspace to one session; replaying the same session is a no-op."""

        def attach(connection):
            row = connection.execute(
                "SELECT status,attached_session_id FROM workspace_transfers WHERE workspace_id=?", (workspace_id,)
            ).fetchone()
            if row is None:
                raise BridgeError("not_found", f"workspace_transfers: {workspace_id}")
            if row["attached_session_id"] is not None:
                if row["attached_session_id"] == session_id:
                    return
                raise BridgeError(PeerErrorCode.WORKSPACE_ALREADY_ATTACHED, "workspace is already attached")
            if row["status"] != "ready":
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "only a ready workspace can be attached")
            connection.execute(
                "UPDATE workspace_transfers SET attached_session_id=?,status='attached',revision=revision+1,updated=? "
                "WHERE workspace_id=?",
                (session_id, time.time(), workspace_id),
            )

        self._with_db(db, attach)
