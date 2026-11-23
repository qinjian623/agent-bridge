"""Historical peer-event mirror deduplication, origin repair, and safe SQLite compaction."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .errors import BridgeError
from .node_identity import IDENTITY_FILENAME, FileNodeIdentityStore
from .peer_protocol import TOOL_DETAIL_EVENT_TYPES, DeliverySource, PeerErrorCode, peer_error, unwrap_peer_event
from .store import encode

if TYPE_CHECKING:
    from .store import Store

_SQL_VAR_CHUNK = 400
_EXPIRED_DETAIL = "_mirror_detail_expired"
_EXPIRED_DETAIL_PREFIX = '{"_mirror_detail_expired":true,'
_MIRROR_ENVELOPE = {"source", "remote_seq", "type", "payload", "created"}
_PROTECTED_SQL = (
    "EXISTS (SELECT 1 FROM deliveries d WHERE d.event_seq=m.local_seq) "
    "OR EXISTS (SELECT 1 FROM mirrored_parent_notifications n WHERE n.local_seq=m.local_seq) "
    "OR EXISTS (SELECT 1 FROM native_send_queue q "
    "WHERE q.delivery_source=? AND q.delivery_id=CAST(m.local_seq AS TEXT))"
)


def _validate_batch_size(batch_size: int) -> int:
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "batch_size must be a positive integer")
    return batch_size


def _resolve_local_identity(store: Store, local_node_id: str | None) -> tuple[str | None, str | None]:
    if local_node_id is not None:
        resolved = store._non_empty_text(local_node_id, "local_node_id")
        if (store.state / IDENTITY_FILENAME).exists():
            identity = FileNodeIdentityStore(store.state).load_or_create()
            if identity.node_id == resolved:
                return resolved, identity.state_generation
        return resolved, None
    if (store.state / IDENTITY_FILENAME).exists():
        identity = FileNodeIdentityStore(store.state).load_or_create()
        return identity.node_id, identity.state_generation
    return None, None


def _db_file_sizes(store: Store) -> tuple[int, int]:
    db_bytes = store.path.stat().st_size if store.path.exists() else 0
    wal_path = Path(f"{store.path}-wal")
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    return db_bytes, wal_bytes


def _is_broker_running(state: Path) -> bool:
    path = state / "broker.lock"
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return False
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    finally:
        os.close(descriptor)
    return False


def _open_exclusive_lock(path: Path, *, create: bool, code: str, message: str) -> int | None:
    flags = os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileNotFoundError:
        return None
    try:
        if create:
            os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        os.close(descriptor)
        raise BridgeError(code, message) from error
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


@contextlib.contextmanager
def _maintenance_locks(store: Store):
    """Hold exclusive broker and existing session locks so VACUUM never races a live writer."""
    with store.connection() as db:
        session_ids = [row[0] for row in db.execute("SELECT id FROM sessions").fetchall()]
    with contextlib.ExitStack() as stack:
        broker_fd = _open_exclusive_lock(
            store.state / "broker.lock",
            create=True,
            code="broker_running",
            message="Cannot run VACUUM while the broker is running; stop the broker first or omit --vacuum",
        )
        if broker_fd is not None:
            stack.callback(os.close, broker_fd)
        for session_id in session_ids:
            lock_path = store.state / f"{session_id}.lock"
            if lock_path.parent != store.state or lock_path.is_symlink():
                continue
            worker_fd = _open_exclusive_lock(
                lock_path,
                create=False,
                code="worker_running",
                message="Cannot run VACUUM while live workers are active; wait for workers to stop first or omit --vacuum",
            )
            if worker_fd is not None:
                stack.callback(os.close, worker_fd)
        yield


def _create_plan_db() -> sqlite3.Connection:
    plan_db = sqlite3.connect("")
    plan_db.row_factory = sqlite3.Row
    plan_db.executescript(
        """
        CREATE TABLE mirrors (
            local_seq INTEGER PRIMARY KEY,
            origin_node_id TEXT NOT NULL,
            origin_state_generation TEXT NOT NULL,
            origin_seq INTEGER NOT NULL,
            origin_fingerprint TEXT NOT NULL,
            protected INTEGER NOT NULL,
            is_direct INTEGER NOT NULL,
            is_self INTEGER NOT NULL,
            self_proven INTEGER NOT NULL,
            needs_repair INTEGER NOT NULL,
            has_event INTEGER NOT NULL,
            data_bytes INTEGER NOT NULL,
            action TEXT NOT NULL DEFAULT 'keep'
        );
        CREATE INDEX idx_mirrors_origin
            ON mirrors(origin_node_id, origin_state_generation, origin_seq, origin_fingerprint);
        CREATE INDEX idx_mirrors_action_seq
            ON mirrors(action, local_seq);
        """
    )
    return plan_db


def _origin_for_row(store: Store, row: sqlite3.Row, *, repair_origins: bool) -> tuple[str, str, int, str]:
    if (
        (not repair_origins or _has_expired_detail(row["data"]))
        and row["origin_node_id"] is not None
        and row["origin_state_generation"] is not None
        and row["origin_seq"] is not None
        and row["origin_fingerprint"] is not None
    ):
        return (
            row["origin_node_id"],
            row["origin_state_generation"],
            int(row["origin_seq"]),
            row["origin_fingerprint"],
        )
    return store._origin_mirror_values(row, force=repair_origins)


def _has_expired_detail(data: Any) -> bool:
    return isinstance(data, str) and data.startswith(_EXPIRED_DETAIL_PREFIX)


def _expired_detail(row: sqlite3.Row) -> str | None:
    if _has_expired_detail(row["data"]):
        return None
    try:
        data = json.loads(row["data"])
    except (TypeError, ValueError, RecursionError):
        return None
    if not isinstance(data, dict) or data.keys() != _MIRROR_ENVELOPE:
        return None
    source = data["source"]
    if (
        not isinstance(source, dict)
        or (source.get("node_id"), source.get("state_generation"), data["remote_seq"])
        != (row["origin_node_id"], row["origin_state_generation"], row["origin_seq"])
        or data["type"] != row["kind"]
        or not isinstance(data["payload"], dict)
    ):
        return None
    origin_event = {
        "ref": source, "seq": data["remote_seq"], "type": data["type"],
        "payload": data["payload"], "created": data["created"],
    }
    try:
        fingerprint = hashlib.sha256(encode(origin_event).encode("utf-8")).hexdigest()
    except (RecursionError, UnicodeError):
        return None
    if fingerprint != row["origin_fingerprint"]:
        return None
    del data["payload"]
    data[_EXPIRED_DETAIL] = True
    return encode(data)


def restore_expired_mirror_detail(db, local_seq: int, origin_event: dict[str, Any], *, result=None) -> bool:
    """Restore a verified detail before adding a late notification reference in the same transaction."""
    row = db.execute(
        "SELECT m.origin_fingerprint FROM events e JOIN peer_event_mirrors m ON m.local_seq=e.seq "
        "WHERE e.seq=? AND SUBSTR(e.data,1,?)=?",
        (local_seq, len(_EXPIRED_DETAIL_PREFIX), _EXPIRED_DETAIL_PREFIX),
    ).fetchone()
    if row is None:
        return False
    if hashlib.sha256(encode(origin_event).encode("utf-8")).hexdigest() != row["origin_fingerprint"]:
        raise peer_error(PeerErrorCode.OPERATION_CONFLICT, "remote event was mirrored with different content")
    data = {
        "source": origin_event["ref"], "remote_seq": origin_event["seq"], "type": origin_event["type"],
        "payload": origin_event["payload"], "created": origin_event["created"],
    }
    if result is not None:
        data["result"] = result
    db.execute("UPDATE events SET data=? WHERE seq=?", (encode(data), local_seq))
    return True


def expire_mirror_details(
    database: Path, *, days: int = 30, limit: int = 500, after: int = 0, apply: bool = False,
) -> dict[str, Any]:
    """Expire at most one page of old tool mirror details while keeping replay keys and event sequences."""
    if isinstance(days, bool) or not isinstance(days, int) or days < 1:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "days must be a positive integer")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "limit must be an integer from 1 to 500")
    if isinstance(after, bool) or not isinstance(after, int) or not 0 <= after < 2**63:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "after must be a non-negative SQLite sequence")
    now = time.time()
    retention_seconds = days * 86_400
    cutoff = 0.0 if retention_seconds >= now else now - retention_seconds
    uri = database.resolve().as_uri() + ("?mode=rw" if apply else "?mode=ro")
    with contextlib.closing(sqlite3.connect(uri, uri=True, timeout=2)) as db, db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
        seqs = [row[0] for row in db.execute(
            "SELECT local_seq FROM peer_event_mirrors WHERE local_seq>? ORDER BY local_seq LIMIT ?",
            (after, limit + 1),
        )]
        page = seqs[:limit]
        next_after = page[-1] if page else after
        updates = []
        reclaimable_bytes = 0
        if page:
            kinds = ",".join("?" for _ in TOOL_DETAIL_EVENT_TYPES)
            # Visit only this page of mirrors even when local event sequences are sparse.
            rows = db.execute(
                "SELECT m.local_seq,m.origin_node_id,m.origin_state_generation,m.origin_seq,m.origin_fingerprint,"
                "e.kind,e.data FROM peer_event_mirrors m CROSS JOIN events e ON e.seq=m.local_seq "
                "JOIN peer_event_cursors c ON c.host_id=m.host_id AND c.node_id=m.node_id "
                "AND c.state_generation=m.state_generation AND c.after_seq>=m.remote_seq "
                f"WHERE m.local_seq>? AND m.local_seq<=? AND e.kind IN ({kinds}) "
                "AND m.created<? AND e.created<? AND e.session_id IS NULL AND e.task_id IS NULL "
                "AND e.attempt_id IS NULL AND m.origin_fingerprint IS NOT NULL AND NOT (" + _PROTECTED_SQL + ")",
                (after, next_after, *TOOL_DETAIL_EVENT_TYPES, cutoff, cutoff, str(DeliverySource.PARENT_NOTIFICATION)),
            )
            for row in rows:
                detail = _expired_detail(row)
                if detail is not None:
                    saved = len(row["data"].encode("utf-8")) - len(detail.encode("utf-8"))
                    if saved > 0:
                        updates.append((detail, row["local_seq"]))
                        reclaimable_bytes += saved
            if apply:
                db.executemany("UPDATE events SET data=? WHERE seq=?", updates)
    return {
        "dry_run": not apply, "days": days, "cutoff": cutoff, "limit": limit,
        "scanned_mirrors": len(page), "eligible_mirrors": len(updates),
        "expired_mirrors": len(updates) if apply else 0,
        "reclaimable_data_bytes": reclaimable_bytes,
        "next_after": next_after, "has_more": len(seqs) > limit,
    }


def _origin_event_from_row(store: Store, row: sqlite3.Row) -> dict[str, Any] | None:
    data = store._decode_json(row["data"])
    if not isinstance(data, dict):
        return None
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
    return origin_event if wrapped else None


def _mirror_content_fingerprint(store: Store, row: sqlite3.Row) -> str | None:
    data = store._decode_json(row["data"])
    if data is None:
        return None
    origin_event = _origin_event_from_row(store, row)
    if origin_event is not None:
        return hashlib.sha256(encode(origin_event).encode("utf-8")).hexdigest()
    return hashlib.sha256(
        encode({"kind": row["kind"], "data": data, "created": row["created"]}).encode("utf-8")
    ).hexdigest()


def _self_origin_proven(
    store: Store,
    db: sqlite3.Connection,
    *,
    local_seq: int,
    expected_origin: tuple[str, str, int, str],
    local_node_id: str | None,
    local_state_generation: str | None,
) -> bool:
    origin_node, origin_generation, origin_seq, origin_fingerprint = expected_origin
    if (
        local_node_id is None
        or local_state_generation is None
        or origin_node != local_node_id
        or origin_generation != local_state_generation
    ):
        return False
    mirror = db.execute(
        "SELECT m.host_id,m.node_id,m.state_generation,m.remote_seq,"
        "m.origin_node_id,m.origin_state_generation,m.origin_seq,m.origin_fingerprint,"
        "e.kind,e.data,e.created "
        "FROM peer_event_mirrors m LEFT JOIN events e ON e.seq=m.local_seq "
        "WHERE m.local_seq=?",
        (local_seq,),
    ).fetchone()
    if mirror is None:
        return False
    origin_event = _origin_event_from_row(store, mirror)
    if origin_event is None:
        return False
    if (
        int(origin_event["seq"]) != origin_seq
        or origin_event["ref"]["node_id"] != local_node_id
        or origin_event["ref"]["state_generation"] != local_state_generation
        or hashlib.sha256(encode(origin_event).encode("utf-8")).hexdigest() != origin_fingerprint
    ):
        return False
    original = db.execute(
        "SELECT e.seq,e.kind,e.data,e.created,e.session_id,e.task_id,e.attempt_id "
        "FROM events e WHERE e.seq=? AND NOT EXISTS ("
        "SELECT 1 FROM peer_event_mirrors m WHERE m.local_seq=e.seq)",
        (origin_seq,),
    ).fetchone()
    if original is None:
        return False
    local_id = original["task_id"] or original["session_id"] or original["attempt_id"] or str(original["seq"])
    return (
        origin_event["ref"]["local_id"] == local_id
        and origin_event["type"] == original["kind"]
        and origin_event["payload"] == store._decode_json(original["data"])
        and float(origin_event["created"]) == float(original["created"])
    )


def _scan_mirrors_into_plan(
    store: Store,
    plan_db: sqlite3.Connection,
    *,
    batch_size: int,
    repair_origins: bool,
    local_node_id: str | None,
    local_state_generation: str | None,
) -> None:
    with store.connection() as db:
        upper_row = db.execute("SELECT COALESCE(MAX(local_seq), 0) FROM peer_event_mirrors").fetchone()
        upper_seq = int(upper_row[0]) if upper_row else 0
    if upper_seq <= 0:
        return

    parent_source = str(DeliverySource.PARENT_NOTIFICATION)
    last_seq = 0
    while last_seq < upper_seq:
        with store.connection() as db:
            rows = db.execute(
                "SELECT m.host_id,m.node_id,m.state_generation,m.remote_seq,m.local_seq,"
                "m.fingerprint,m.origin_node_id,m.origin_state_generation,m.origin_seq,m.origin_fingerprint,"
                "e.kind,e.data,e.created,"
                "CASE WHEN e.seq IS NULL THEN 0 ELSE 1 END AS has_event,"
                "LENGTH(CAST(COALESCE(e.data,'') AS BLOB)) AS data_bytes,"
                "CASE WHEN (" + _PROTECTED_SQL + ") THEN 1 ELSE 0 END AS protected "
                "FROM peer_event_mirrors m "
                "LEFT JOIN events e ON e.seq=m.local_seq "
                "WHERE m.local_seq>? AND m.local_seq<=? "
                "ORDER BY m.local_seq LIMIT ?",
                (parent_source, last_seq, upper_seq, batch_size),
            ).fetchall()
            if not rows:
                break

            batch = []
            for row in rows:
                orig_node, orig_gen, orig_seq, orig_fp = _origin_for_row(
                    store, row, repair_origins=repair_origins
                )
                needs_repair = int(
                    (row["origin_node_id"], row["origin_state_generation"], row["origin_seq"], row["origin_fingerprint"])
                    != (orig_node, orig_gen, orig_seq, orig_fp)
                )
                is_direct = int(
                    orig_node == row["node_id"]
                    and orig_gen == row["state_generation"]
                    and orig_seq == row["remote_seq"]
                )
                is_self = int(local_node_id is not None and orig_node == local_node_id)
                self_proven = 0
                if is_self and local_state_generation is not None and orig_gen == local_state_generation:
                    self_proven = int(
                        _self_origin_proven(
                            store,
                            db,
                            local_seq=row["local_seq"],
                            expected_origin=(orig_node, orig_gen, orig_seq, orig_fp),
                            local_node_id=local_node_id,
                            local_state_generation=local_state_generation,
                        )
                    )
                batch.append(
                    (
                        row["local_seq"],
                        orig_node,
                        orig_gen,
                        orig_seq,
                        orig_fp,
                        row["protected"],
                        is_direct,
                        is_self,
                        self_proven,
                        needs_repair,
                        row["has_event"],
                        row["data_bytes"],
                    )
                )
                last_seq = row["local_seq"]

            plan_db.executemany(
                "INSERT INTO mirrors("
                "local_seq,origin_node_id,origin_state_generation,origin_seq,origin_fingerprint,"
                "protected,is_direct,is_self,self_proven,needs_repair,has_event,data_bytes"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                batch,
            )
        plan_db.commit()


def _classify_plan(plan_db: sqlite3.Connection, *, prune_self_origins: bool) -> dict[str, int]:
    prune_self_flag = 1 if prune_self_origins else 0
    plan_db.execute(
        """
        UPDATE mirrors
        SET action = 'prune'
        WHERE local_seq IN (
            SELECT local_seq FROM (
                SELECT local_seq,
                       CASE
                           WHEN protected = 1 THEN 'keep'
                           WHEN ? = 1 AND is_self = 1 AND self_proven = 0 THEN 'keep'
                           WHEN ? = 1 AND is_self = 1 THEN 'prune'
                           WHEN ROW_NUMBER() OVER (
                               PARTITION BY origin_node_id, origin_state_generation, origin_seq, origin_fingerprint
                               ORDER BY protected DESC, is_direct DESC, local_seq ASC
                           ) = 1 THEN 'keep'
                           ELSE 'prune'
                       END AS planned_action
                FROM mirrors
            )
            WHERE planned_action = 'prune'
        )
        """,
        (prune_self_flag, prune_self_flag),
    )
    plan_db.commit()
    summary = plan_db.execute(
        """
        SELECT
            COUNT(*) AS total_mirrors,
            COALESCE(SUM(CASE WHEN action = 'keep' THEN 1 ELSE 0 END), 0) AS kept_mirrors,
            COALESCE(SUM(CASE WHEN protected = 1 THEN 1 ELSE 0 END), 0) AS protected_mirrors,
            COALESCE(SUM(CASE WHEN action = 'prune' THEN 1 ELSE 0 END), 0) AS pruned_mirrors,
            COALESCE(SUM(CASE WHEN action = 'prune' AND ? = 1 AND is_self = 1 THEN 1 ELSE 0 END), 0) AS pruned_self_origins,
            COALESCE(SUM(CASE WHEN action = 'prune' AND NOT (? = 1 AND is_self = 1) THEN 1 ELSE 0 END), 0) AS pruned_duplicates,
            COALESCE(SUM(CASE WHEN action = 'keep' AND needs_repair = 1 THEN 1 ELSE 0 END), 0) AS repaired_origins,
            COALESCE(SUM(CASE WHEN action = 'prune' AND has_event = 1 THEN 1 ELSE 0 END), 0) AS pruned_events,
            COALESCE(SUM(CASE WHEN action = 'prune' THEN data_bytes ELSE 0 END), 0) AS reclaimable_data_bytes
        FROM mirrors
        """,
        (prune_self_flag, prune_self_flag),
    ).fetchone()
    return {key: int(summary[key]) for key in summary.keys()}


def _apply_origin_repairs(store: Store, plan_db: sqlite3.Connection, *, batch_size: int) -> int:
    repaired = 0
    last_seq = 0
    while True:
        rows = plan_db.execute(
            "SELECT local_seq,origin_node_id,origin_state_generation,origin_seq,origin_fingerprint "
            "FROM mirrors WHERE action='keep' AND needs_repair=1 AND local_seq>? "
            "ORDER BY local_seq LIMIT ?",
            (last_seq, batch_size),
        ).fetchall()
        if not rows:
            break
        updates = [
            (
                row["origin_node_id"],
                row["origin_state_generation"],
                row["origin_seq"],
                row["origin_fingerprint"],
                row["local_seq"],
            )
            for row in rows
        ]
        last_seq = rows[-1]["local_seq"]
        with store.connection(write=True, timeout=30) as db:
            db.executemany(
                "UPDATE peer_event_mirrors "
                "SET origin_node_id=?,origin_state_generation=?,origin_seq=?,origin_fingerprint=? "
                "WHERE local_seq=?",
                updates,
            )
        repaired += len(updates)
    return repaired


def _max_event_seq(db: sqlite3.Connection) -> int:
    max_seq = int(db.execute("SELECT COALESCE(MAX(seq), 0) FROM events").fetchone()[0])
    has_seq_table = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
    ).fetchone()
    if has_seq_table:
        row = db.execute("SELECT seq FROM sqlite_sequence WHERE name='events'").fetchone()
        if row is not None and row[0] is not None:
            max_seq = max(max_seq, int(row[0]))
    return max_seq


def _ensure_min_event_sequence(db: sqlite3.Connection, min_seq: int) -> None:
    if min_seq <= 0:
        return
    has_seq_table = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
    ).fetchone()
    if not has_seq_table:
        return
    row = db.execute("SELECT seq FROM sqlite_sequence WHERE name='events'").fetchone()
    if row is None:
        db.execute("INSERT INTO sqlite_sequence(name, seq) VALUES('events', ?)", (min_seq,))
    elif int(row[0]) < min_seq:
        db.execute("UPDATE sqlite_sequence SET seq=? WHERE name='events'", (min_seq,))


def _apply_mirror_prunes(
    store: Store,
    plan_db: sqlite3.Connection,
    *,
    batch_size: int,
    prune_self_origins: bool,
    repair_origins: bool,
    local_node_id: str | None,
) -> tuple[int, int, int, int]:
    with store.connection() as db:
        max_seq = _max_event_seq(db)

    parent_source = str(DeliverySource.PARENT_NOTIFICATION)
    deleted_mirrors = 0
    deleted_duplicates = 0
    deleted_self_origins = 0
    deleted_events = 0
    last_seq = 0

    while True:
        rows = plan_db.execute(
            "SELECT local_seq,origin_node_id,origin_state_generation,origin_seq,origin_fingerprint,"
            "is_self,self_proven FROM mirrors WHERE action='prune' AND local_seq>? "
            "ORDER BY local_seq LIMIT ?",
            (last_seq, batch_size),
        ).fetchall()
        if not rows:
            break
        last_seq = rows[-1]["local_seq"]
        plan_by_seq = {int(row["local_seq"]): row for row in rows}
        all_seqs = list(plan_by_seq)
        current_node_id, current_generation = (
            _resolve_local_identity(store, local_node_id) if prune_self_origins else (None, None)
        )

        with store.connection(write=True, timeout=30) as db:
            for start in range(0, len(all_seqs), _SQL_VAR_CHUNK):
                chunk = all_seqs[start : start + _SQL_VAR_CHUNK]
                placeholders = ",".join("?" for _ in chunk)
                current_rows = db.execute(
                    "SELECT m.local_seq,m.host_id,m.node_id,m.state_generation,m.remote_seq,"
                    "m.fingerprint,m.origin_node_id,m.origin_state_generation,m.origin_seq,"
                    "m.origin_fingerprint,e.kind,e.data,e.created,"
                    "CASE WHEN (" + _PROTECTED_SQL + ") THEN 1 ELSE 0 END AS protected "
                    "FROM peer_event_mirrors m LEFT JOIN events e ON e.seq=m.local_seq "
                    f"WHERE m.local_seq IN ({placeholders})",
                    (parent_source, *chunk),
                ).fetchall()
                current_by_seq = {int(row["local_seq"]): row for row in current_rows}
                eligible = []
                for seq in chunk:
                    plan_row = plan_by_seq[seq]
                    current = current_by_seq.get(seq)
                    if current is None:
                        continue
                    plan_origin = (
                        plan_row["origin_node_id"],
                        plan_row["origin_state_generation"],
                        int(plan_row["origin_seq"]),
                        plan_row["origin_fingerprint"],
                    )
                    if _origin_for_row(store, current, repair_origins=repair_origins) != plan_origin:
                        continue
                    if int(current["protected"]):
                        continue
                    if prune_self_origins and int(plan_row["is_self"]) and int(plan_row["self_proven"]):
                        if not _self_origin_proven(
                            store,
                            db,
                            local_seq=seq,
                            expected_origin=plan_origin,
                            local_node_id=current_node_id,
                            local_state_generation=current_generation,
                        ):
                            continue
                        eligible.append(seq)
                        continue
                    candidate_fingerprint = _mirror_content_fingerprint(store, current)
                    if candidate_fingerprint is None:
                        continue
                    keep_seqs = [
                        int(keep[0])
                        for keep in plan_db.execute(
                            "SELECT local_seq FROM mirrors WHERE origin_node_id=? "
                            "AND origin_state_generation=? AND origin_seq=? AND origin_fingerprint=? "
                            "AND action='keep'",
                            plan_origin,
                        ).fetchall()
                    ]
                    if not keep_seqs:
                        continue
                    keep_placeholders = ",".join("?" for _ in keep_seqs)
                    keep_rows = db.execute(
                        "SELECT m.local_seq,m.host_id,m.node_id,m.state_generation,m.remote_seq,"
                        "m.fingerprint,m.origin_node_id,m.origin_state_generation,m.origin_seq,"
                        "m.origin_fingerprint,e.kind,e.data,e.created "
                        "FROM peer_event_mirrors m LEFT JOIN events e ON e.seq=m.local_seq "
                        f"WHERE m.local_seq IN ({keep_placeholders})",
                        keep_seqs,
                    ).fetchall()
                    if not any(
                        _mirror_content_fingerprint(store, keep) == candidate_fingerprint
                        for keep in keep_rows
                    ):
                        continue
                    eligible.append(seq)
                if not eligible:
                    continue
                ep = ",".join("?" for _ in eligible)
                db.execute(f"DELETE FROM peer_event_mirrors WHERE local_seq IN ({ep})", eligible)
                deleted_mirrors += len(eligible)
                for seq in eligible:
                    plan_row = plan_by_seq[seq]
                    if prune_self_origins and int(plan_row["is_self"]) and int(plan_row["self_proven"]):
                        deleted_self_origins += 1
                    else:
                        deleted_duplicates += 1
                deleted_events += db.execute(
                    f"DELETE FROM events WHERE seq IN ({ep}) "
                    "AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.event_seq=events.seq) "
                    "AND NOT EXISTS (SELECT 1 FROM mirrored_parent_notifications n WHERE n.local_seq=events.seq) "
                    "AND NOT EXISTS (SELECT 1 FROM peer_event_mirrors m WHERE m.local_seq=events.seq)",
                    eligible,
                ).rowcount

    if deleted_events > 0 and max_seq > 0:
        with store.connection(write=True, timeout=30) as db:
            _ensure_min_event_sequence(db, max_seq)

    return deleted_mirrors, deleted_duplicates, deleted_self_origins, deleted_events


def _vacuum_store(store: Store) -> None:
    connection = sqlite3.connect(store.path, timeout=30, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("VACUUM")
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()


def prune_duplicate_mirrors(
    store: Store,
    *,
    dry_run: bool = True,
    batch_size: int = 500,
    prune_self_origins: bool = True,
    repair_origins: bool = True,
    vacuum: bool = False,
    local_node_id: str | None = None,
) -> dict[str, Any]:
    """Deduplicate historical peer event mirrors while preserving deliveries, notifications, and cursors."""
    batch_size = _validate_batch_size(batch_size)
    resolved_local_node_id, resolved_local_state_generation = _resolve_local_identity(store, local_node_id)
    db_bytes_before, wal_bytes_before = _db_file_sizes(store)
    broker_running = _is_broker_running(store.state)

    lock_context = _maintenance_locks(store) if (vacuum and not dry_run) else contextlib.nullcontext()
    with lock_context:
        if vacuum and not dry_run:
            broker_running = False
        with contextlib.closing(_create_plan_db()) as plan_db:
            _scan_mirrors_into_plan(
                store,
                plan_db,
                batch_size=batch_size,
                repair_origins=repair_origins,
                local_node_id=resolved_local_node_id,
                local_state_generation=resolved_local_state_generation,
            )
            stats = _classify_plan(plan_db, prune_self_origins=prune_self_origins)

            vacuumed = False
            if not dry_run:
                stats["repaired_origins"] = _apply_origin_repairs(store, plan_db, batch_size=batch_size)
                (
                    stats["pruned_mirrors"],
                    stats["pruned_duplicates"],
                    stats["pruned_self_origins"],
                    stats["pruned_events"],
                ) = _apply_mirror_prunes(
                    store,
                    plan_db,
                    batch_size=batch_size,
                    prune_self_origins=prune_self_origins,
                    repair_origins=repair_origins,
                    local_node_id=local_node_id,
                )
                stats["kept_mirrors"] = stats["total_mirrors"] - stats["pruned_mirrors"]
                if vacuum:
                    _vacuum_store(store)
                    vacuumed = True

    db_bytes_after, wal_bytes_after = _db_file_sizes(store)
    return {
        "dry_run": bool(dry_run),
        "vacuum": bool(vacuum),
        "vacuumed": vacuumed,
        "broker_running": broker_running,
        "local_node_id": resolved_local_node_id,
        "prune_self_origins": bool(prune_self_origins),
        "repair_origins": bool(repair_origins),
        "batch_size": batch_size,
        **stats,
        "db_bytes_before": db_bytes_before,
        "db_bytes_after": db_bytes_after,
        "wal_bytes_before": wal_bytes_before,
        "wal_bytes_after": wal_bytes_after,
    }
