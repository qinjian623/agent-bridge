"""Historical peer-event mirror deduplication, origin repair, and safe SQLite compaction."""

from __future__ import annotations

import contextlib
import fcntl
import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .errors import BridgeError
from .node_identity import IDENTITY_FILENAME, FileNodeIdentityStore
from .peer_protocol import DeliverySource, PeerErrorCode, peer_error

if TYPE_CHECKING:
    from .store import Store

_SQL_VAR_CHUNK = 400


def _validate_batch_size(batch_size: int) -> int:
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "batch_size must be a positive integer")
    return batch_size


def _resolve_local_node_id(store: Store, local_node_id: str | None) -> str | None:
    if local_node_id is not None:
        return store._non_empty_text(local_node_id, "local_node_id")
    if (store.state / IDENTITY_FILENAME).exists():
        return FileNodeIdentityStore(store.state).load_or_create().node_id
    return None


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
            needs_repair INTEGER NOT NULL,
            has_event INTEGER NOT NULL,
            data_bytes INTEGER NOT NULL,
            action TEXT NOT NULL DEFAULT 'keep'
        );
        CREATE INDEX idx_mirrors_origin
            ON mirrors(origin_node_id, origin_state_generation, origin_seq);
        CREATE INDEX idx_mirrors_action_seq
            ON mirrors(action, local_seq);
        """
    )
    return plan_db


def _origin_for_row(store: Store, row: sqlite3.Row, *, repair_origins: bool) -> tuple[str, str, int, str]:
    if (
        not repair_origins
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


def _scan_mirrors_into_plan(
    store: Store,
    plan_db: sqlite3.Connection,
    *,
    batch_size: int,
    repair_origins: bool,
    local_node_id: str | None,
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
                "CASE WHEN ("
                "  EXISTS (SELECT 1 FROM deliveries d WHERE d.event_seq=m.local_seq)"
                "  OR EXISTS (SELECT 1 FROM mirrored_parent_notifications n WHERE n.local_seq=m.local_seq)"
                "  OR EXISTS (SELECT 1 FROM native_send_queue q WHERE q.delivery_source=? AND q.delivery_id=CAST(m.local_seq AS TEXT))"
                ") THEN 1 ELSE 0 END AS protected "
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
                    needs_repair,
                    row["has_event"],
                    row["data_bytes"],
                )
            )
            last_seq = row["local_seq"]

        plan_db.executemany(
            "INSERT INTO mirrors("
            "local_seq,origin_node_id,origin_state_generation,origin_seq,origin_fingerprint,"
            "protected,is_direct,is_self,needs_repair,has_event,data_bytes"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
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
                           WHEN ? = 1 AND is_self = 1 THEN 'prune'
                           WHEN ROW_NUMBER() OVER (
                               PARTITION BY origin_node_id, origin_state_generation, origin_seq
                               ORDER BY protected DESC, is_direct DESC, local_seq ASC
                           ) = 1 THEN 'keep'
                           ELSE 'prune'
                       END AS planned_action
                FROM mirrors
            )
            WHERE planned_action = 'prune'
        )
        """,
        (prune_self_flag,),
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
            "SELECT local_seq,is_self FROM mirrors WHERE action='prune' AND local_seq>? "
            "ORDER BY local_seq LIMIT ?",
            (last_seq, batch_size),
        ).fetchall()
        if not rows:
            break
        last_seq = rows[-1]["local_seq"]
        kind_by_seq = {int(row["local_seq"]): int(row["is_self"]) for row in rows}
        all_seqs = list(kind_by_seq.keys())

        with store.connection(write=True, timeout=30) as db:
            for start in range(0, len(all_seqs), _SQL_VAR_CHUNK):
                chunk = all_seqs[start : start + _SQL_VAR_CHUNK]
                placeholders = ",".join("?" for _ in chunk)
                eligible = [
                    int(row[0])
                    for row in db.execute(
                        f"SELECT m.local_seq FROM peer_event_mirrors m "
                        f"WHERE m.local_seq IN ({placeholders}) "
                        f"AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.event_seq=m.local_seq) "
                        f"AND NOT EXISTS (SELECT 1 FROM mirrored_parent_notifications n WHERE n.local_seq=m.local_seq) "
                        f"AND NOT EXISTS (SELECT 1 FROM native_send_queue q "
                        f"  WHERE q.delivery_source=? AND q.delivery_id=CAST(m.local_seq AS TEXT))",
                        (*chunk, parent_source),
                    ).fetchall()
                ]
                if not eligible:
                    continue
                ep = ",".join("?" for _ in eligible)
                db.execute(f"DELETE FROM peer_event_mirrors WHERE local_seq IN ({ep})", eligible)
                deleted_mirrors += len(eligible)
                for seq in eligible:
                    if prune_self_origins and kind_by_seq.get(seq):
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
    resolved_local_node_id = _resolve_local_node_id(store, local_node_id)
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
