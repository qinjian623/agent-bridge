from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

from . import artifacts
from .codex_parent import read_resume_config
from .config import Config, Profile
from .errors import BridgeError
from .parent_usage import meter_usage, refresh_meter, start_meter
from .peer_protocol import PeerErrorCode


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def identifier():
    return str(uuid.uuid4())


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


def check_retry(task, instruction):
    if not (task["status"] in {"failed", "interrupted", "cancelled"} or task["validation"] == "rejected"):
        raise BridgeError("invalid_retry", "Retry requires a settled failed/interrupted/cancelled or rejected task")
    if instruction != task["instruction"]:
        raise BridgeError("invalid_retry", "Retry preserves the task instruction; use a new task to change its objective")


SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
 id TEXT PRIMARY KEY, profile TEXT NOT NULL, cwd TEXT NOT NULL, access TEXT NOT NULL,
 parent TEXT, native_id TEXT, desired TEXT NOT NULL DEFAULT 'open', observed TEXT NOT NULL DEFAULT 'stopped',
 revision INTEGER NOT NULL DEFAULT 0, worker_pid INTEGER, worker_start TEXT,
 generation TEXT, error TEXT, created REAL NOT NULL
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
 status TEXT NOT NULL DEFAULT 'pending', receipt TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS session_groups (
 id TEXT PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 0,
 deleted INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL
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
CREATE TABLE IF NOT EXISTS peer_operations (
 operation_id TEXT PRIMARY KEY, node_id TEXT NOT NULL, state_generation TEXT NOT NULL,
 fingerprint TEXT NOT NULL, method TEXT NOT NULL, params TEXT NOT NULL, status TEXT NOT NULL,
 result TEXT, error TEXT, created REAL NOT NULL, updated REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS peer_operation_identity ON peer_operations(node_id,state_generation,operation_id);
CREATE TABLE IF NOT EXISTS peer_inbox (
 delivery_id TEXT PRIMARY KEY, host_id TEXT NOT NULL, node_id TEXT NOT NULL,
 state_generation TEXT NOT NULL, target_seq INTEGER NOT NULL,
 seq_kind TEXT NOT NULL DEFAULT 'delivery', operation_id TEXT NOT NULL,
 payload TEXT NOT NULL, status TEXT NOT NULL, receipt TEXT, created REAL NOT NULL,
 UNIQUE(host_id,node_id,state_generation,target_seq)
);
CREATE INDEX IF NOT EXISTS peer_inbox_operation ON peer_inbox(operation_id);
CREATE TABLE IF NOT EXISTS node_outbox (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT UNIQUE NOT NULL,
 group_id TEXT, native_id TEXT, method TEXT NOT NULL, payload TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', created REAL NOT NULL,
 pulled_at REAL, acked_at REAL
);
CREATE INDEX IF NOT EXISTS node_outbox_pending ON node_outbox(status,seq);
"""


class Store:
    def __init__(self, state: Path):
        self.state = state
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = state / "bridge.sqlite"
        with self.connection() as db:
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
        self.path.chmod(0o600)

    @contextmanager
    def connection(self, *, write=False):
        db = sqlite3.connect(self.path, timeout=10)
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

    def open(self, profile: Profile, cwd: str, access: str, request_id: str, parent: str | None = None, workspace=None):
        directory = Path(cwd).expanduser().resolve()
        if not directory.is_dir():
            raise BridgeError("invalid_directory", "Working directory must exist")
        if access not in {"read-only", "workspace-write"}:
            raise BridgeError("invalid_access", "access must be read-only or workspace-write")
        payload = {
            "op": "open",
            "profile": asdict(profile),
            "cwd": str(directory),
            "access": access,
            "parent": parent,
            "workspace": workspace,
        }
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
            self.event(db, "session_opened", {}, session)
            return self.remember(db, request_id, fingerprint, {"session_id": session, "revision": 0})

    def create_session(self, db, profile: Profile, cwd: str, access: str, parent: str | None = None, workspace=None):
        """Insert one session inside an existing transaction (peer.deliver composite open, ADR-13).

        Unlike `open`, this does not allocate a `requests` row: the caller's peer operation record is the
        idempotency key, so session creation and task enqueueing can share one SQLite transaction.
        """
        directory = Path(cwd).expanduser().resolve()
        if not directory.is_dir():
            raise BridgeError("invalid_directory", "Working directory must exist")
        if access not in {"read-only", "workspace-write"}:
            raise BridgeError("invalid_access", "access must be read-only or workspace-write")
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
        self.event(db, "session_opened", {}, session)
        return session, str(directory)

    def dispatch(
        self, session_id: str, instruction: str, request_id: str, dependencies=None, reservation=0, details=None
    ):
        dependencies = task_input(instruction, reservation, dependencies)
        details = details or {}
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

    def get_session(self, session_id):
        with self.connection() as db:
            row = self.required(db, "sessions", session_id)
        row["profile"] = json.loads(row["profile"])
        row["workspace"] = json.loads(row["workspace"])
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
                "SELECT id,desired,observed,revision,worker_pid,worker_start,generation,error,profile FROM sessions ORDER BY created"
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            profile = json.loads(value.pop("profile"))
            value.update({key: profile.get(key) for key in ("runtime", "provider", "model", "effort", "service_tier")})
            value["profile"] = profile["name"]
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
            self.event(db, "session_control_requested", {"action": action, "revision": revision + 1}, session_id)
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

    def retry(self, task_id, session_id, instruction, request_id, reservation=None):
        payload = {"op": "retry", "task": task_id, "session": session_id, "instruction": instruction}
        if reservation is not None:
            payload["reservation"] = reservation
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
            db.execute(
                "UPDATE tasks SET session_id=?,reservation=?,status='queued',validation='pending',active_attempt=NULL,cancel_requested=0,result=NULL,error=NULL WHERE id=?",
                (session_id, reservation, task_id),
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

    def worker_starting(self, session_id, generation, pid, start, process_scope=None):
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            if session["desired"] != "open" or session["observed"] not in {"stopped", "starting"}:
                return False
            db.execute(
                "UPDATE sessions SET generation=?,worker_pid=?,worker_start=?,process_scope=?,observed='starting' WHERE id=?",
                (generation, pid, start, process_scope, session_id),
            )
            return True

    def worker_ready(self, session_id, generation, pid, start, native_id):
        with self.connection(write=True) as db:
            session = self.required(db, "sessions", session_id)
            if session["generation"] not in {None, generation}:
                raise BridgeError("stale_worker", "A different worker owns this session")
            if session["native_id"] and session["native_id"] != native_id:
                raise BridgeError("session_mismatch", "Native session identity changed")
            db.execute(
                "UPDATE sessions SET generation=?,worker_pid=?,worker_start=?,native_id=?,observed='idle',error=NULL WHERE id=?",
                (generation, pid, start, native_id, session_id),
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
                "UPDATE sessions SET observed=?,worker_pid=NULL,error=?,desired=CASE WHEN desired='draining' THEN 'paused' ELSE desired END WHERE id=?",
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
            return artifacts.read(json.loads(row[0]), offset, limit)

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
            self.event(db, "input_required", {"question_id": key, "expires": expires, **data}, session_id, task_id, attempt_id)
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
                    "UPDATE deliveries SET status='processed' WHERE event_seq=? AND parent_id=? AND status IN ('sending','uncertain','accepted','wake_pending')",
                    (event_seq, parent_id),
                ).rowcount
                if not changed:
                    raise BridgeError(
                        "invalid_delivery", "Only a delivery sent to this parent can be acknowledged"
                    )
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

    def events(self, after=0, session_id=None, task_id=None, limit=100):
        if not isinstance(after, int) or after < 0:
            raise BridgeError("invalid_cursor", "Event cursor must be a nonnegative integer")
        clauses, values = ["seq>?"], [after]
        for key, value in (("session_id", session_id), ("task_id", task_id)):
            if value:
                clauses.append(key + "=?")
                values.append(value)
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
        return value

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
    ):
        """Persist one idempotency record for `(node_id, state_generation, operation_id)` (ADR-04).

        Re-recording the same operation with an identical fingerprint returns the existing row, which lets a
        crash/retry path inspect the stored outcome instead of running the work twice.
        """
        if not isinstance(operation_id, str) or not operation_id:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "operation_id must be a non-empty string")
        now = time.time()
        encoded_params = encode(params if params is not None else {})
        encoded_result = encode(result) if result is not None else None
        encoded_error = None if error is None else (error if isinstance(error, str) else encode(error))
        with self.connection(write=True) as db:
            existing = db.execute(
                "SELECT * FROM peer_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if existing is not None:
                if fingerprint is not None and existing["fingerprint"] != fingerprint:
                    raise BridgeError(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "operation_id was already used for different parameters",
                    )
                return self._peer_operation(existing)
            db.execute(
                "INSERT INTO peer_operations(operation_id,node_id,state_generation,fingerprint,method,params,status,result,error,created,updated) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
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
                ),
            )
            row = db.execute("SELECT * FROM peer_operations WHERE operation_id=?", (operation_id,)).fetchone()
            return self._peer_operation(row)

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

    def add_outbox(self, operation_id, method, payload, group_id=None, native_id=None, status="pending"):
        """Enqueue one outbound request; duplicate operation_ids are idempotent (ADR-04)."""
        if not isinstance(operation_id, str) or not operation_id:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "operation_id must be a non-empty string")
        if not isinstance(method, str) or not method:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "method must be a non-empty string")
        encoded_payload = encode(payload)
        with self.connection(write=True) as db:
            existing = db.execute("SELECT * FROM node_outbox WHERE operation_id=?", (operation_id,)).fetchone()
            if existing is not None:
                if existing["payload"] != encoded_payload or existing["method"] != method:
                    raise BridgeError(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "operation_id was already used for a different outbox request",
                    )
                return existing["seq"]
            cursor = db.execute(
                "INSERT INTO node_outbox(operation_id,group_id,native_id,method,payload,status,created) VALUES(?,?,?,?,?,?,?)",
                (operation_id, group_id, native_id, method, encoded_payload, status, time.time()),
            )
            return cursor.lastrowid

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

    def ack_outbox(self, operation_id, result=None, status="acked"):
        """Mark one outbox request acknowledged; `result` is accepted for API symmetry (no column is needed)."""
        if not isinstance(operation_id, str) or not operation_id:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "operation_id must be a non-empty string")
        with self.connection(write=True) as db:
            changed = db.execute(
                "UPDATE node_outbox SET status=?,acked_at=? WHERE operation_id=?",
                (status, time.time(), operation_id),
            ).rowcount
            if not changed:
                raise BridgeError(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown outbox operation: {operation_id}")
            row = db.execute("SELECT * FROM node_outbox WHERE operation_id=?", (operation_id,)).fetchone()
            return self._node_outbox(row)

