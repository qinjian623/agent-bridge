from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .agy_presence import inactive_reason as native_inactive_reason
from .config import safe_error
from .errors import BridgeError
from .peer_protocol import (
    DeliverySource,
    PeerDeliveryStatus,
    PeerErrorCode,
    canonical_operation_id,
    compute_stream_id,
    normalize_group_prune_params,
    normalize_remote_member,
)
from .store import Store, encode, identifier, task_input
from .worker import alive, owned_processes

TERMINAL_RECIPIENT_STATUSES = (
    PeerDeliveryStatus.RELEASED,
    PeerDeliveryStatus.CANCELLED,
    PeerDeliveryStatus.FAILED,
)
TERMINAL_TASK_STATUSES = frozenset({"completed", "failed", "cancelled"})
# "pending_review" is not written by the current worker, but older or hand-edited state can contain it.
UNSETTLED_TASK_STATUSES = frozenset({
    "queued", "assigned", "running", "waiting_input", "interrupted", "recovery_needed", "pending_review",
})
# A native delivery settles only on an explicit terminal status or a terminal processing outcome.
TERMINAL_NATIVE_STATUSES = frozenset({"skipped", "failed", "cancelled"})
TERMINAL_PROCESSING_STATUSES = frozenset({"completed", "failed"})
NATIVE_QUEUE_BLOCKING_STATUSES = ("pending", "sending", "uncertain")
ACTIVE_OBSERVED_STATES = frozenset({"running", "waiting_input", "interrupted", "recovery_needed"})
ACTIVE_STATES = frozenset({"active", "unknown"})
_GROUP_TASK_WHERE = (
    "(CASE WHEN json_valid(t.details) THEN json_extract(t.details,'$.group_id') END=? "
    "OR t.id IN (SELECT r.task_id FROM broadcast_recipients r "
    "JOIN group_broadcasts b ON b.id=r.broadcast_id "
    "WHERE b.group_id=? AND r.task_id IS NOT NULL))"
)


def validate_group_id(group_id):
    if not isinstance(group_id, str) or not group_id.strip() or group_id != group_id.strip() or len(group_id) > 128:
        raise BridgeError("invalid_group", "Specify one nonempty group_id of at most 128 characters, without surrounding whitespace")


def _remote_member_view(row):
    item = dict(row)
    item["kind"] = item["target_kind"]
    if item["target_kind"] == "bridge":
        item["session_id"] = item["target_id"]
    else:
        item["native_id"] = item["target_id"]
    return item


def group_view(db, group_id):
    row = Store.required(db, "session_groups", group_id)
    created = row["created"]
    return {
        "group_id": row["id"], "revision": row["revision"], "deleted": bool(row["deleted"]),
        "deleted_at": row["deleted_at"], "delete_reason": row["delete_reason"],
        "created": created,
        "updated_at": created if row["updated_at"] is None else row["updated_at"],
        "last_activity_at": created if row["last_activity_at"] is None else row["last_activity_at"],
        "members": [dict(member) for member in db.execute(
            "SELECT m.session_id,s.desired,s.observed FROM session_group_members m JOIN sessions s ON s.id=m.session_id WHERE m.group_id=? ORDER BY m.session_id",
            (group_id,),
        )] + [{"kind": "native", **dict(member),
               "transport": "codex_queue" if member["runtime"] == "codex" else "antigravity_resume"}
              for member in db.execute(
                  "SELECT runtime,native_id,cwd,label FROM native_group_members WHERE group_id=? ORDER BY runtime,native_id",
                  (group_id,),
              )],
        "remote_members": [_remote_member_view(member) for member in db.execute(
            "SELECT * FROM remote_group_members WHERE group_id=? "
            "ORDER BY host_id,node_id,state_generation,target_kind,runtime,target_id",
            (group_id,),
        )],
    }


def _recipient_is_settled(db, recipient):
    """Resolve a frozen broadcast recipient to its live work item; stored status alone goes stale."""
    if recipient["task_id"]:
        task = db.execute("SELECT status FROM tasks WHERE id=?", (recipient["task_id"],)).fetchone()
        return task is not None and task["status"] in TERMINAL_TASK_STATUSES
    if recipient["delivery_id"]:
        outbox = db.execute(
            "SELECT status FROM peer_outbox WHERE delivery_id=?", (recipient["delivery_id"],),
        ).fetchone()
        if outbox is not None:
            return outbox["status"] in TERMINAL_RECIPIENT_STATUSES
        native = db.execute(
            "SELECT status,processing_status FROM native_broadcast_deliveries WHERE id=?",
            (recipient["delivery_id"],),
        ).fetchone()
        if native is not None:
            return (
                native["status"] in TERMINAL_NATIVE_STATUSES
                or native["processing_status"] in TERMINAL_PROCESSING_STATUSES
            )
        queue = db.execute(
            "SELECT status,processing_status FROM native_send_queue WHERE delivery_source=? AND delivery_id=?",
            (str(DeliverySource.LOCAL_BROADCAST), recipient["delivery_id"]),
        ).fetchone()
        if queue is not None:
            return (
                queue["status"] in TERMINAL_NATIVE_STATUSES
                or queue["processing_status"] in TERMINAL_PROCESSING_STATUSES
            )
        # A delivery row disappeared without a provable terminal outcome.
        return False
    return recipient["status"] in {"skipped", *TERMINAL_RECIPIENT_STATUSES}


def _unsettled_work(db, group_id):
    """Return every durable work item that makes this group unsafe to delete or prune."""
    items = [{"type": "task", "id": row["id"], "status": row["status"]}
             for row in db.execute(
                 "SELECT t.id,t.status FROM tasks t WHERE " + _GROUP_TASK_WHERE + " ORDER BY t.id",
                 (group_id, group_id),
             ) if row["status"] in UNSETTLED_TASK_STATUSES]
    items.extend(
        {"type": "question", "id": row["id"]}
        for row in db.execute(
            "SELECT q.id FROM questions q JOIN tasks t ON t.id=q.task_id "
            "WHERE q.status='pending' AND " + _GROUP_TASK_WHERE + " ORDER BY q.id",
            (group_id, group_id),
        )
    )
    recipients = db.execute(
        "SELECT r.broadcast_id,r.recipient_key,r.task_id,r.delivery_id,r.status "
        "FROM broadcast_recipients r JOIN group_broadcasts b ON b.id=r.broadcast_id "
        "WHERE b.group_id=? ORDER BY r.broadcast_id,r.recipient_key",
        (group_id,),
    ).fetchall()
    items.extend(
        {"type": "broadcast_recipient", "id": row["recipient_key"], "broadcast_id": row["broadcast_id"]}
        for row in recipients if not _recipient_is_settled(db, row)
    )
    items.extend(
        {"type": "outbox", "id": row["delivery_id"], "status": row["status"]}
        for row in db.execute(
            "SELECT delivery_id,status FROM peer_outbox WHERE group_id=? "
            f"AND status NOT IN ({','.join('?' for _ in TERMINAL_RECIPIENT_STATUSES)}) ORDER BY delivery_id",
            (group_id, *TERMINAL_RECIPIENT_STATUSES),
        )
    )
    items.extend(
        {"type": "node_outbox", "id": row["seq"], "status": row["status"]}
        for row in db.execute(
            "SELECT seq,status FROM node_outbox WHERE group_id=? AND status!='acked' ORDER BY seq",
            (group_id,),
        )
    )
    return items


def _native_queue_blocking(db, runtime, native_id):
    return db.execute(
        "SELECT status FROM native_send_queue WHERE runtime=? AND native_id=? "
        f"AND status IN ({','.join('?' for _ in NATIVE_QUEUE_BLOCKING_STATUSES)}) LIMIT 1",
        (runtime, native_id, *NATIVE_QUEUE_BLOCKING_STATUSES),
    ).fetchone()


def _activity_state(db, group, now):
    """Derive the conservative activity classification from current members and durable work."""
    group_id = group["id"]
    unsettled = _unsettled_work(db, group_id)
    bridge = [dict(row) for row in db.execute(
        "SELECT m.session_id,s.desired,s.observed,s.worker_pid,s.worker_start,s.process_scope "
        "FROM session_group_members m JOIN sessions s ON s.id=m.session_id "
        "WHERE m.group_id=? ORDER BY m.session_id",
        (group_id,),
    )]
    natives = [dict(row) for row in db.execute(
        "SELECT runtime,native_id FROM native_group_members WHERE group_id=? ORDER BY runtime,native_id",
        (group_id,),
    )]
    remotes = [dict(row) for row in db.execute(
        "SELECT host_id,node_id,state_generation,target_kind,runtime,target_id "
        "FROM remote_group_members WHERE group_id=? "
        "ORDER BY host_id,node_id,state_generation,target_kind,runtime,target_id",
        (group_id,),
    )]
    active_reasons = []
    unknown_reasons = []
    if unsettled:
        active_reasons.append(f"{len(unsettled)} unsettled work item(s) must settle first")
    for member in bridge:
        sid = member["session_id"]
        observed = member["observed"]
        if member["desired"] == "open":
            active_reasons.append(f"bridge session {sid} is open")
        if observed in ACTIVE_OBSERVED_STATES:
            active_reasons.append(f"bridge session {sid} is observed {observed}")
        elif observed != "stopped":
            # starting/idle mean the worker process exists; paused does not imply exited.
            active_reasons.append(f"bridge session {sid} is not stopped (observed {observed})")
        elif member["worker_pid"] and alive(member):
            active_reasons.append(f"bridge session {sid} still owns a live worker process")
        elif member["process_scope"]:
            processes = owned_processes(member["process_scope"])
            if processes["status"] == "running":
                active_reasons.append(f"bridge session {sid} still owns worker processes")
            elif processes["status"] != "stopped":
                unknown_reasons.append(f"bridge session {sid} process state cannot be verified")
    for member in natives:
        runtime, native_id = member["runtime"], member["native_id"]
        blocked = _native_queue_blocking(db, runtime, native_id)
        if blocked is not None:
            active_reasons.append(f"native session {runtime}:{native_id} has a {blocked['status']} delivery")
        else:
            # There is no durable process proof for an idle native session; never claim it is stopped.
            unknown_reasons.append(f"native session {runtime}:{native_id} process state cannot be reliably verified")
    for member in remotes:
        unknown_reasons.append(
            f"remote member {member['host_id']}:{member['target_kind']}:{member['target_id']} "
            "cannot be verified from this node"
        )
    has_members = bool(bridge or natives or remotes)
    if active_reasons:
        state, reasons = "active", active_reasons + unknown_reasons
    elif unknown_reasons:
        state, reasons = "unknown", unknown_reasons
    elif not has_members:
        state, reasons = "inactive", ["group has no members and no unsettled work"]
    elif not natives and not remotes and all(
        member["desired"] == "closed" and member["observed"] == "stopped" for member in bridge
    ):
        state, reasons = "completed", ["all members are closed and stopped; all work is settled"]
    else:
        state, reasons = "inactive", ["all members are stopped; no unsettled work"]
    if group["deleted"]:
        state, reasons = "deleted", ["group is soft-deleted"]
    return {
        "activity_state": state,
        "active": state in ACTIVE_STATES,
        "activity_reasons": reasons,
        "last_activity_at": group["created"] if group["last_activity_at"] is None else group["last_activity_at"],
        "checked_at": now,
        "stale": state == "unknown" or (state != "deleted" and bool(unknown_reasons)),
        "unsettled_count": len(unsettled),
        "member_count": len(bridge) + len(natives) + len(remotes),
        "remote_member_count": len(remotes),
    }


def inspect_groups(store, group_id=None, active_only=False):
    if not isinstance(active_only, bool):
        raise BridgeError("invalid_request", "active_only must be a boolean")
    if group_id is not None:
        validate_group_id(group_id)
    with store.connection() as db:
        db.execute("BEGIN")
        now = time.time()
        if group_id is not None:
            row = Store.required(db, "session_groups", group_id)
            view = group_view(db, group_id)
            view.update(_activity_state(db, row, now))
            return view
        result = []
        for row in db.execute("SELECT * FROM session_groups WHERE deleted=0 ORDER BY created,id").fetchall():
            view = group_view(db, row["id"])
            view.update(_activity_state(db, row, now))
            if active_only and view["activity_state"] not in ACTIVE_STATES:
                continue
            result.append(view)
        return {"groups": result}


def native_members_input(members, action):
    if members is None:
        return []
    if not isinstance(members, list):
        raise BridgeError("invalid_members", "native_members must be a list of runtime/native_id/cwd/label objects")
    result = {}
    for member in members:
        if not isinstance(member, dict) or set(member) - {"runtime", "native_id", "cwd", "label"}:
            raise BridgeError("invalid_members", "Native members accept runtime, native_id, cwd and label only")
        runtime, native_id = member.get("runtime"), member.get("native_id")
        if not isinstance(runtime, str) or runtime not in {"codex", "antigravity"} or not isinstance(native_id, str):
            raise BridgeError("invalid_members", "Specify codex or antigravity and the original native UUID")
        try:
            if str(uuid.UUID(native_id)) != native_id:
                raise ValueError()
        except ValueError:
            raise BridgeError("invalid_members", "native_id must be a canonical UUID") from None
        item = {"runtime": runtime, "native_id": native_id}
        if action != "remove":
            cwd, label = member.get("cwd"), member.get("label", "")
            if not isinstance(cwd, str) or not Path(cwd).is_absolute():
                raise BridgeError("invalid_members", "Native members require an absolute cwd")
            if not isinstance(label, str) or len(label) > 128:
                raise BridgeError("invalid_members", "Native member label must be at most 128 characters")
            item.update(cwd=cwd, label=label)
        key = runtime, native_id
        if key in result and result[key] != item:
            raise BridgeError("invalid_members", "Conflicting details for the same native session")
        result[key] = item
    return [result[key] for key in sorted(result)]


def remote_members_input(members, action):
    if members is None:
        return []
    if not isinstance(members, list):
        raise BridgeError("invalid_members", "remote_members must be a list of remote member objects")
    result = {}
    for raw in members:
        try:
            member = normalize_remote_member(raw, action)
        except BridgeError as error:
            raise BridgeError("invalid_members", str(error)) from error
        if member["target_kind"] == "bridge" and (member["runtime"] != "" or member["cwd"] != ""):
            raise BridgeError("invalid_members", "Bridge remote members must not set runtime or cwd")
        key = (
            member["host_id"], member["node_id"], member["state_generation"],
            member["target_kind"], member["runtime"], member["target_id"],
        )
        if key in result and result[key] != member:
            raise BridgeError("member_conflict", "Conflicting details for the same remote member")
        result[key] = member
    return [result[key] for key in sorted(result)]


def control_group(
    store, group_id, action, revision, request_id, session_ids=None, native_members=None, remote_members=None,
):
    validate_group_id(group_id)
    if action not in {"create", "add", "remove", "delete"}:
        raise BridgeError("invalid_action", "Group action must be create, add, remove or delete")
    if type(revision) is not int or revision < 0:
        raise BridgeError("invalid_revision", "Provide the group's nonnegative expected_revision")
    if session_ids is not None and (
        not isinstance(session_ids, list)
        or any(not isinstance(sid, str) or not sid.strip() for sid in session_ids)
    ):
        raise BridgeError("invalid_members", "session_ids must be a list of bridge session IDs")
    members = sorted(set(session_ids or []))
    natives = native_members_input(native_members, action)
    remotes = remote_members_input(remote_members, action)
    if (action in {"add", "remove"} and not members and not natives and not remotes) or (
        action == "delete" and (
            session_ids is not None or native_members is not None or remote_members is not None
        )
    ):
        raise BridgeError("invalid_members", "Add/remove require members; delete does not accept members")
    payload = {"op": "group", "group_id": group_id, "action": action, "revision": revision, "session_ids": members}
    if native_members is not None:
        payload["native_members"] = natives
    if remote_members is not None:
        payload["remote_members"] = remotes
    with store.connection(write=True) as db:
        fingerprint, old = store.previous(db, request_id, payload)
        if old is not None:
            return old
        now = time.time()
        if action == "create":
            if revision != 0:
                raise BridgeError("stale_revision", "A new group requires expected_revision=0")
            if db.execute("SELECT 1 FROM session_groups WHERE id=?", (group_id,)).fetchone():
                raise BridgeError("group_exists", "This group_id already exists or was deleted; use a new ID")
            db.execute(
                "INSERT INTO session_groups(id,created,updated_at,last_activity_at) VALUES(?,?,?,?)",
                (group_id, now, now, now),
            )
        else:
            row = store.required(db, "session_groups", group_id)
            if row["deleted"]:
                raise BridgeError("group_deleted", "This group was deleted")
            if row["revision"] != revision:
                raise BridgeError("stale_revision", "Group membership changed; inspect the group before changing it")
        for sid in members:
            store.required(db, "sessions", sid)
        if action in {"create", "add"}:
            for member in natives:
                if not Path(member["cwd"]).is_dir():
                    raise BridgeError("invalid_members", "Native member cwd must be an existing directory")
                if db.execute("SELECT 1 FROM sessions WHERE native_id=?", (member["native_id"],)).fetchone():
                    raise BridgeError("native_owned_by_bridge", "Use the existing bridge session_id for this native session")
                old_member = db.execute(
                    "SELECT cwd,label FROM native_group_members WHERE group_id=? AND runtime=? AND native_id=?",
                    (group_id, member["runtime"], member["native_id"]),
                ).fetchone()
                if old_member and dict(old_member) != {k: member[k] for k in ("cwd", "label")}:
                    raise BridgeError("member_conflict", "Remove the old member before changing its directory or label")
            for member in remotes:
                old_member = db.execute(
                    "SELECT cwd,label FROM remote_group_members WHERE group_id=? AND host_id=? AND node_id=? "
                    "AND state_generation=? AND target_kind=? AND runtime=? AND target_id=?",
                    (group_id, member["host_id"], member["node_id"], member["state_generation"],
                     member["target_kind"], member["runtime"], member["target_id"]),
                ).fetchone()
                if old_member and dict(old_member) != {k: member[k] for k in ("cwd", "label")}:
                    raise BridgeError("member_conflict", "Remove the old remote member before changing its directory or label")
        changed = action in {"create", "delete"}
        if action in {"create", "add"}:
            for sid in members:
                changed |= bool(db.execute(
                    "INSERT OR IGNORE INTO session_group_members(group_id,session_id) VALUES(?,?)", (group_id, sid),
                ).rowcount)
            for member in natives:
                changed |= bool(db.execute(
                    "INSERT OR IGNORE INTO native_group_members(group_id,runtime,native_id,cwd,label) VALUES(?,?,?,?,?)",
                    (group_id, member["runtime"], member["native_id"], member["cwd"], member["label"]),
                ).rowcount)
            joined_revision = revision if action == "create" else revision + 1
            for member in remotes:
                receipt = encode({"member": member, "verified_at": now, "source": "agent_group"})
                changed |= bool(db.execute(
                    "INSERT OR IGNORE INTO remote_group_members("
                    "group_id,host_id,node_id,state_generation,target_kind,runtime,target_id,cwd,label,"
                    "joined_revision,verified_at,verification_receipt,created) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (group_id, member["host_id"], member["node_id"], member["state_generation"],
                     member["target_kind"], member["runtime"], member["target_id"], member["cwd"], member["label"],
                     joined_revision, now, receipt, now),
                ).rowcount)
        elif action == "remove":
            for sid in members:
                changed |= bool(db.execute(
                    "DELETE FROM session_group_members WHERE group_id=? AND session_id=?", (group_id, sid),
                ).rowcount)
            for member in natives:
                changed |= bool(db.execute(
                    "DELETE FROM native_group_members WHERE group_id=? AND runtime=? AND native_id=?",
                    (group_id, member["runtime"], member["native_id"]),
                ).rowcount)
            for member in remotes:
                changed |= bool(db.execute(
                    "DELETE FROM remote_group_members WHERE group_id=? AND host_id=? AND node_id=? "
                    "AND state_generation=? AND target_kind=? AND runtime=? AND target_id=?",
                    (group_id, member["host_id"], member["node_id"], member["state_generation"],
                     member["target_kind"], member["runtime"], member["target_id"]),
                ).rowcount)
        else:
            if _unsettled_work(db, group_id):
                raise BridgeError(PeerErrorCode.GROUP_PRUNE_BLOCKED, "Cannot delete group with unsettled work")
            # Soft delete keeps the complete member snapshot (local, native and remote) for history and prune.
            db.execute(
                "UPDATE session_groups SET deleted=1,deleted_at=?,delete_reason='deleted',"
                "revision=revision+1,updated_at=? WHERE id=?",
                (now, now, group_id),
            )
        if changed:
            if action in {"add", "remove"}:
                db.execute(
                    "UPDATE session_groups SET revision=revision+1,updated_at=?,last_activity_at=? WHERE id=?",
                    (now, now, group_id),
                )
            store.event(db, "group_changed", {"group_id": group_id, "action": action, "session_ids": members,
                                               "native_members": natives, "remote_members": remotes})
        return store.remember(db, request_id, fingerprint, group_view(db, group_id))


def resolve_authority_ref(store, value=None):
    """Return the coordinator NodeRef used as the authority for ordered target streams."""
    if value is None:
        from .node_identity import FileNodeIdentityStore

        return FileNodeIdentityStore(store.state).node_ref("local").as_dict()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as error:
            raise BridgeError("invalid_authority", "authority_ref must be a JSON object") from error
    if not isinstance(value, dict):
        raise BridgeError("invalid_authority", "authority_ref must be an object")
    keys = ("host_id", "node_id", "state_generation")
    if any(not isinstance(value.get(key), str) or not value.get(key) for key in keys):
        raise BridgeError("invalid_authority", "authority_ref requires host_id, node_id and state_generation")
    return {key: value[key] for key in keys}


authority_ref = resolve_authority_ref


def target_ref(authority, kind, runtime, target_id, *, host_id=None, node_id=None, state_generation=None):
    return {
        "host_id": host_id or authority["host_id"],
        "node_id": node_id or authority["node_id"],
        "state_generation": state_generation or authority["state_generation"],
        "kind": kind,
        "runtime": runtime,
        "target_id": target_id,
    }


def _recipient_key(target):
    return encode(target)


def _local_target(member, authority):
    if member.get("kind") == "native":
        return target_ref(authority, "native", member["runtime"], member["native_id"])
    return target_ref(authority, "bridge", "", member["session_id"])


def _remote_target(member, authority):
    if not member["node_id"] or not member["state_generation"]:
        raise BridgeError(
            "invalid_members",
            f"Resolve the node identity for remote host {member['host_id']} before broadcasting",
        )
    return target_ref(
        authority, member["target_kind"], member["runtime"], member["target_id"],
        host_id=member["host_id"], node_id=member["node_id"], state_generation=member["state_generation"],
    )


def _delivery_payload(delivery_id, group_id, broadcast_id, target, target_seq, stream_id, authority,
                      message, reservation, group_revision):
    payload = {
        "delivery_id": delivery_id,
        "group_id": group_id,
        "broadcast_id": broadcast_id,
        "target": {"kind": target["kind"], "id": target["target_id"]},
        "target_seq": target_seq,
        "seq_kind": "message",
        "message": message,
        "expected_group_revision": group_revision,
        "reservation": reservation,
        "stream_id": stream_id,
        "authority_ref": authority,
        "ordering_mode": "ordered",
    }
    if target["kind"] == "native":
        payload["target"]["runtime"] = target["runtime"]
    return payload


def update_broadcast_recipient(db, delivery_id, status, *, send_status=None, processing_status=None,
                               error=None):
    db.execute(
        "UPDATE broadcast_recipients SET status=?,send_status=?,processing_status=?,error=?,updated=? "
        "WHERE delivery_id=?",
        (status, send_status, processing_status, error, time.time(), delivery_id),
    )


def settle_broadcast(db, broadcast_id):
    rows = db.execute("SELECT status FROM broadcast_recipients WHERE broadcast_id=?", (broadcast_id,)).fetchall()
    if rows and all(row["status"] in TERMINAL_RECIPIENT_STATUSES for row in rows):
        db.execute(
            "UPDATE group_broadcasts SET settled_at=COALESCE(settled_at,?) WHERE id=?",
            (time.time(), broadcast_id),
        )


@contextmanager
def _transaction(store, db):
    """Use an existing write transaction when one is supplied, otherwise open one."""
    if db is not None:
        yield db
        return
    with store.connection(write=True) as connection:
        yield connection


def broadcast(store, group_id, message, revision, request_id, reservation=0, *, token_budget=None,
              authority_ref=None, sender_ref=None, db=None):
    validate_group_id(group_id)
    task_input(message, reservation)
    if type(revision) is not int or revision < 0:
        raise BridgeError("invalid_revision", "Provide the group's nonnegative expected_revision")
    payload = {"op": "broadcast", "group_id": group_id, "message": message, "revision": revision, "reservation": reservation}
    with _transaction(store, db) as db:
        fingerprint, old = store.previous(db, request_id, payload)
        if old is not None:
            return old
        if token_budget is not None and reservation <= 0:
            raise BridgeError("reservation_required", "A positive token reservation per member is required while a budget is enabled")
        group = group_view(db, group_id)
        if group["deleted"]:
            raise BridgeError("group_deleted", "This group was deleted")
        if group["revision"] != revision:
            raise BridgeError("stale_revision", "Group membership changed; inspect the group before broadcasting")
        if not group["members"] and not group["remote_members"]:
            raise BridgeError("empty_group", "This group has no members; nothing was sent")
        if token_budget is not None and any(m.get("kind") == "native" for m in group["members"]):
            raise BridgeError("native_budget_unsupported", "Native sessions execute outside bridge token accounting; cannot broadcast to them while a token budget is enabled")
        if token_budget is not None and any(m["target_kind"] == "native" for m in group["remote_members"]):
            raise BridgeError("peer_budget_scope", "Remote native sessions cannot be metered by the coordinator's token budget")

        authority = resolve_authority_ref(store, authority_ref)
        broadcast_id = identifier()
        recipients = []
        target_rows = []
        seen = set()

        def add_target(target):
            key = _recipient_key(target)
            if key not in seen:
                seen.add(key)
                target_rows.append(target)
            return key

        for member in group["members"]:
            add_target(_local_target(member, authority))
        for member in group["remote_members"]:
            add_target(_remote_target(member, authority))

        details = {"kind": "broadcast", "learn_from_result": False, "group_id": group_id,
                   "group_revision": revision, "broadcast_id": broadcast_id}
        created = time.time()
        db.execute(
            "INSERT INTO group_broadcasts(id,group_id,group_revision,sender_ref,message,recipient_snapshot,"
            "request_key,created) VALUES(?,?,?,?,?,?,?,?)",
            (broadcast_id, group_id, revision,
             encode(sender_ref) if isinstance(sender_ref, dict) else (
                 encode(sender_ref.as_dict()) if hasattr(sender_ref, "as_dict") else sender_ref
             ),
             message, encode(target_rows), request_id, created),
        )

        # Membership, all task/outbox inserts and the retry receipt commit in one transaction.
        for member in group["members"]:
            target = _local_target(member, authority)
            key = _recipient_key(target)
            if member.get("kind") == "native":
                delivery_id = identifier()
                reason = None
                if db.execute("SELECT 1 FROM sessions WHERE native_id=?", (member["native_id"],)).fetchone():
                    reason = {"code": "native_owned_by_bridge", "message": "Use its bridge session_id"}
                status = "skipped" if reason else "pending"
                db.execute(
                    "INSERT INTO native_broadcast_deliveries(id,broadcast_id,group_id,runtime,native_id,cwd,message,status,error,created) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (delivery_id, broadcast_id, group_id, member["runtime"], member["native_id"], member["cwd"],
                     message, status, encode(reason) if reason else None, time.time()),
                )
                # Same transaction and same (runtime,native_id) order as the legacy row: no sender may start
                # between these two writes, and no later message can overtake this one.
                store.enqueue_native_send(
                    member["runtime"], member["native_id"], DeliverySource.LOCAL_BROADCAST,
                    delivery_id, message, status=status, item_id=delivery_id, db=db,
                )
                recipients.append({"delivery_id": delivery_id, "runtime": member["runtime"],
                                   "native_id": member["native_id"], "status": status,
                                   **({"reason": reason} if reason else {})})
                db.execute(
                    "INSERT INTO broadcast_recipients(broadcast_id,recipient_key,target_ref,delivery_id,task_id,"
                    "status,send_status,processing_status,error,updated) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (broadcast_id, key, encode(target), delivery_id, None, status, status, None,
                     encode(reason) if reason else None, time.time()),
                )
                continue
            sid = member["session_id"]
            reason = None
            if member["desired"] != "open":
                reason = {"code": "session_state", "desired": member["desired"]}
            elif member["observed"] == "recovery_needed":
                reason = {"code": "worker_state", "observed": member["observed"]}
            if reason:
                recipients.append({"session_id": sid, "status": "skipped", "reason": reason})
                db.execute(
                    "INSERT INTO broadcast_recipients(broadcast_id,recipient_key,target_ref,delivery_id,task_id,"
                    "status,send_status,processing_status,error,updated) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (broadcast_id, key, encode(target), None, None, "skipped", "skipped", None,
                     encode(reason), time.time()),
                )
            else:
                queued = store.enqueue_task(db, sid, message, [], reservation, details)
                recipients.append(queued)
                db.execute(
                    "INSERT INTO broadcast_recipients(broadcast_id,recipient_key,target_ref,delivery_id,task_id,"
                    "status,send_status,processing_status,error,updated) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (broadcast_id, key, encode(target), None, queued["task_id"], queued["status"], queued["status"],
                     None, None, time.time()),
                )

        for member in group["remote_members"]:
            target = _remote_target(member, authority)
            key = _recipient_key(target)
            stream_id = compute_stream_id(authority, target)
            stream = db.execute("SELECT next_seq FROM target_streams WHERE stream_id=?", (stream_id,)).fetchone()
            if stream is None:
                target_seq = 1
                db.execute(
                    "INSERT INTO target_streams(stream_id,authority_ref,target_ref,next_seq,released_seq) "
                    "VALUES(?,?,?,?,0)",
                    (stream_id, encode(authority), encode(target), target_seq + 1),
                )
            else:
                target_seq = stream["next_seq"]
                db.execute(
                    "UPDATE target_streams SET next_seq=? WHERE stream_id=?", (target_seq + 1, stream_id),
                )
            delivery_id = identifier()
            operation_id = canonical_operation_id(
                authority, member["state_generation"], "peer.deliver", delivery_id + ":message",
            )
            delivery_payload = _delivery_payload(
                delivery_id, group_id, broadcast_id, target, target_seq, stream_id, authority,
                message, reservation, revision,
            )
            if member["target_kind"] == "native" and member["cwd"]:
                delivery_payload["cwd"] = member["cwd"]
            delivery_payload["operation_id"] = operation_id
            delivery_fingerprint = hashlib.sha256(encode(delivery_payload).encode("utf-8")).hexdigest()
            db.execute(
                "INSERT INTO peer_outbox(delivery_id,broadcast_id,group_id,group_revision,stream_id,target_seq,"
                "host_id,node_id,state_generation,target_kind,runtime,target_id,operation_id,payload,fingerprint,"
                "seq_kind,status,revision,created,updated) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (delivery_id, broadcast_id, group_id, revision, stream_id, target_seq,
                 member["host_id"], member["node_id"], member["state_generation"], member["target_kind"],
                 member["runtime"], member["target_id"], operation_id, encode(delivery_payload),
                 delivery_fingerprint, "message", "pending", 0, time.time(), time.time()),
            )
            recipients.append({
                "delivery_id": delivery_id, "host_id": member["host_id"], "node_id": member["node_id"],
                "state_generation": member["state_generation"], "target_kind": member["target_kind"],
                "kind": member["target_kind"], "runtime": member["runtime"],
                "target_id": member["target_id"], "status": "pending",
                "target_seq": target_seq, "stream_id": stream_id,
                **({"session_id": member["target_id"]} if member["target_kind"] == "bridge" else
                   {"native_id": member["target_id"]}),
            })
            db.execute(
                "INSERT INTO broadcast_recipients(broadcast_id,recipient_key,target_ref,delivery_id,task_id,"
                "status,send_status,processing_status,error,updated) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (broadcast_id, key, encode(target), delivery_id, None, "pending", "pending", None, None, time.time()),
            )

        db.execute("UPDATE session_groups SET last_activity_at=? WHERE id=?", (time.time(), group_id))
        queued = sum(row["status"] == "queued" for row in recipients)
        result = {"broadcast_id": broadcast_id, "group_id": group_id, "group_revision": revision,
                  "queued_count": queued, "skipped_count": sum(r["status"] == "skipped" for r in recipients),
                  "recipients": recipients}
        if any("delivery_id" in r for r in recipients):
            result["pending_count"] = sum(r["status"] == "pending" for r in recipients)
        store.event(db, "group_broadcast", result)
        return store.remember(db, request_id, fingerprint, result)


def _decode(value):
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def broadcast_status(store, broadcast_id):
    """Unified per-recipient status across local bridge tasks, local native queue and remote outbox."""
    if not isinstance(broadcast_id, str) or not broadcast_id:
        raise BridgeError("invalid_broadcast", "broadcast_id must be a non-empty string")
    with store.connection() as db:
        db.execute("BEGIN")
        row = db.execute("SELECT * FROM group_broadcasts WHERE id=?", (broadcast_id,)).fetchone()
        if row is None:
            raise BridgeError("not_found", f"group_broadcasts: {broadcast_id}")
        stored = [dict(item) for item in db.execute(
            "SELECT * FROM broadcast_recipients WHERE broadcast_id=? ORDER BY target_ref", (broadcast_id,),
        )]
        recipients = []
        for item in stored:
            target = _decode(item["target_ref"]) or {}
            entry = {
                **target,
                "recipient_key": item["recipient_key"],
                "target_ref": target,
                "delivery_id": item["delivery_id"],
                "task_id": item["task_id"],
                "status": item["status"],
                "send_status": item["send_status"],
                "processing_status": item["processing_status"],
                "error": _decode(item["error"]),
                "updated": item["updated"],
            }
            if target.get("kind") == "bridge":
                entry["session_id"] = target.get("target_id")
            elif target.get("kind") == "native":
                entry["native_id"] = target.get("target_id")
            if item["delivery_id"]:
                outbox = db.execute(
                    "SELECT status,send_status,processing_status,error,receipt,target_seq,stream_id,revision "
                    "FROM peer_outbox WHERE delivery_id=?", (item["delivery_id"],),
                ).fetchone()
                legacy = None
                if outbox is None:
                    legacy = db.execute(
                        "SELECT status,receipt,native_turn_id,processing_status,error "
                        "FROM native_broadcast_deliveries WHERE id=?", (item["delivery_id"],),
                    ).fetchone()
                if outbox is not None:
                    entry.update(
                        status=outbox["status"], send_status=outbox["send_status"],
                        processing_status=outbox["processing_status"], error=_decode(outbox["error"]),
                        receipt=_decode(outbox["receipt"]), target_seq=outbox["target_seq"],
                        stream_id=outbox["stream_id"], revision=outbox["revision"],
                    )
                elif legacy is not None:
                    entry.update(
                        status=legacy["status"], send_status=legacy["status"],
                        processing_status=legacy["processing_status"], error=_decode(legacy["error"]),
                        receipt=_decode(legacy["receipt"]), native_turn_id=legacy["native_turn_id"],
                    )
            elif item["task_id"]:
                task = db.execute("SELECT status,error FROM tasks WHERE id=?", (item["task_id"],)).fetchone()
                if task is not None:
                    entry["status"] = task["status"]
                    if task["error"]:
                        entry["error"] = _decode(task["error"])
            recipients.append(entry)
        counts = {}
        for item in recipients:
            counts[item["status"]] = counts.get(item["status"], 0) + 1
        return {
            "broadcast_id": row["id"],
            "group_id": row["group_id"],
            "group_revision": row["group_revision"],
            "message": row["message"],
            "sender_ref": _decode(row["sender_ref"]),
            "recipient_snapshot": _decode(row["recipient_snapshot"]),
            "created": row["created"],
            "settled_at": row["settled_at"],
            "counts": counts,
            "recipients": recipients,
        }


def _member_snapshot(db, group_id):
    view = group_view(db, group_id)
    return {"members": view["members"], "remote_members": view["remote_members"]}


def _bridge_stop_reason(db, group_id):
    for session in db.execute(
        "SELECT s.id,s.desired,s.observed,s.worker_pid,s.worker_start,s.process_scope "
        "FROM session_group_members m JOIN sessions s ON s.id=m.session_id "
        "WHERE m.group_id=? ORDER BY s.id",
        (group_id,),
    ):
        sid = session["id"]
        if session["desired"] != "closed":
            return f"bridge session {sid} is not closed (desired={session['desired']})"
        if session["observed"] != "stopped":
            return f"bridge session {sid} is not stopped (observed={session['observed']})"
        if session["worker_pid"] and alive(session):
            return f"bridge session {sid} still owns a live worker process"
        if session["process_scope"]:
            processes = owned_processes(session["process_scope"])
            if processes["status"] == "running":
                return f"bridge session {sid} still owns worker processes"
            if processes["status"] != "stopped":
                return f"bridge session {sid} process state cannot be verified"
    return None


def _native_stop_reason(db, group_id):
    for member in db.execute(
        "SELECT runtime,native_id FROM native_group_members WHERE group_id=? ORDER BY runtime,native_id",
        (group_id,),
    ):
        runtime, native_id = member["runtime"], member["native_id"]
        blocked = _native_queue_blocking(db, runtime, native_id)
        if blocked is not None:
            return f"native session {runtime}:{native_id} has a {blocked['status']} delivery or send lock"
        if db.execute("SELECT 1 FROM sessions WHERE native_id=?", (native_id,)).fetchone():
            return f"native session {runtime}:{native_id} is now owned by a bridge session"
        if runtime == "antigravity":
            try:
                reason = native_inactive_reason(native_id, "agy")
            except (OSError, ValueError) as error:
                return f"native AGY session {native_id} state cannot be verified: {safe_error(error)}"
            if reason:
                return f"native AGY session {native_id}: {reason}"
    return None


def _peer_client(broker, host_id):
    factory = getattr(broker, "peer_factory", None)
    if factory is None:
        return None
    if hasattr(factory, "client"):
        return factory.client(host_id)
    if callable(factory):
        return factory(host_id)
    return factory


async def _remote_member_reason(broker, member):
    host_id, target_id = member["host_id"], member["target_id"]
    try:
        client = _peer_client(broker, host_id)
    except Exception as error:
        return f"remote host {host_id} is unavailable: {safe_error(error)}"
    if client is None:
        return f"remote member {host_id}:{target_id} cannot be verified without a peer client"
    try:
        result = client.inspect({"kind": "session", "id": target_id})
        if asyncio.iscoroutine(result):
            result = await result
    except Exception as error:
        return f"remote host {host_id} is unreachable: {safe_error(error)}"
    items = result.get("items") if isinstance(result, dict) else None
    if not isinstance(items, list):
        return f"remote host {host_id} returned no verifiable session state"
    matches = [
        item for item in items
        if isinstance(item, dict) and (item.get("id") == target_id or item.get("native_id") == target_id)
    ]
    if not matches:
        return f"remote {member['target_kind']} {target_id} on {host_id} is not confirmed stopped"
    item = matches[0]
    if item.get("desired") not in {None, "closed"}:
        return f"remote {member['target_kind']} {target_id} on {host_id} is not closed"
    if item.get("observed") != "stopped":
        return (
            f"remote {member['target_kind']} {target_id} on {host_id} is not stopped "
            f"(observed={item.get('observed')})"
        )
    return None


async def _prune_block_reason(store, broker, group_id):
    """ACV read-only gate: durable work, local member stop proof, then remote probes outside the DB lock."""
    with store.connection() as db:
        db.execute("BEGIN")
        unsettled = _unsettled_work(db, group_id)
        if unsettled:
            kinds = {}
            for item in unsettled:
                kinds[item["type"]] = kinds.get(item["type"], 0) + 1
            detail = ", ".join(f"{count} {kind}" for kind, count in sorted(kinds.items()))
            return f"{len(unsettled)} unsettled work item(s) must settle first: {detail}"
        reason = _bridge_stop_reason(db, group_id)
        if reason:
            return reason
        reason = _native_stop_reason(db, group_id)
        if reason:
            return reason
        remotes = [dict(row) for row in db.execute(
            "SELECT host_id,node_id,state_generation,target_kind,runtime,target_id "
            "FROM remote_group_members WHERE group_id=? "
            "ORDER BY host_id,node_id,state_generation,target_kind,runtime,target_id",
            (group_id,),
        )]
    if not remotes:
        return None
    if broker is None:
        return "remote members require coordinator peer access"
    reasons = []
    for member in remotes:
        reason = await _remote_member_reason(broker, member)
        if reason:
            reasons.append(reason)
    return "; ".join(reasons) if reasons else None


def _prune_result(run, dry_run, items):
    summary = {"total": len(items)}
    for item in items:
        summary[item["status"]] = summary.get(item["status"], 0) + 1
    return {
        "run_id": run["id"], "cutoff": run["cutoff"], "dry_run": dry_run,
        "items": items, "summary": summary,
    }


async def prune_groups(store, broker, older_than, dry_run=True, request_id=None):
    """Authoritative Coordinator Verification (ADR-25): read-only probes, then a local CAS soft delete."""
    params = normalize_group_prune_params({
        "older_than": older_than, "dry_run": dry_run, "request_id": request_id,
    })
    seconds, dry, request_id = params["older_than"], params["dry_run"], params["request_id"]
    cutoff = time.time() - seconds
    fingerprint = hashlib.sha256(
        encode({"older_than": seconds, "dry_run": dry}).encode("utf-8")
    ).hexdigest()
    proposed_run_id = identifier()
    run = store.create_group_prune_run(proposed_run_id, request_id, fingerprint, cutoff)
    if run["id"] != proposed_run_id:
        # A retried request returns the original fixed run and never widens its candidate set.
        return _prune_result(run, dry, store.get_group_prune_items(run["id"]))
    run_id = run["id"]
    cutoff = run["cutoff"]
    try:
        with store.connection() as db:
            db.execute("BEGIN")
            candidates = [dict(row) for row in db.execute(
                "SELECT * FROM session_groups WHERE deleted=0 AND last_activity_at<? "
                "ORDER BY last_activity_at,id",
                (cutoff,),
            )]
        for group in candidates:
            group_id, revision = group["id"], group["revision"]
            with store.connection() as db:
                db.execute("BEGIN")
                snapshot = _member_snapshot(db, group_id)
            try:
                reason = await _prune_block_reason(store, broker, group_id)
            except Exception as error:
                reason = f"prune verification failed: {safe_error(error)}"
            if reason is None and not dry:
                # Re-read the facts that can change without a revision bump before the local CAS.
                with store.connection() as db:
                    db.execute("BEGIN")
                    current = db.execute(
                        "SELECT revision,deleted,last_activity_at,created FROM session_groups WHERE id=?",
                        (group_id,),
                    ).fetchone()
                    still_unsettled = current is not None and _unsettled_work(db, group_id)
                if (
                    current is None or current["deleted"] or current["revision"] != revision
                    or (current["last_activity_at"] if current["last_activity_at"] is not None else current["created"])
                    != (group["last_activity_at"] if group["last_activity_at"] is not None else group["created"])
                    or still_unsettled
                ):
                    reason = "group activity or membership changed after verification"
            if reason is not None:
                store.record_group_prune_item(run_id, group_id, revision, snapshot, "blocked", "skipped", reason)
                continue
            if dry:
                store.record_group_prune_item(run_id, group_id, revision, snapshot, "eligible", "dry_run")
                continue
            try:
                store.prune_group_cas(group_id, revision, delete_reason="pruned")
            except BridgeError as error:
                store.record_group_prune_item(
                    run_id, group_id, revision, snapshot, "blocked", "skipped", safe_error(error),
                )
                continue
            with store.connection(write=True) as db:
                store.event(db, "group_pruned", {
                    "group_id": group_id, "run_id": run_id, "delete_reason": "pruned", "cutoff": cutoff,
                })
            store.record_group_prune_item(run_id, group_id, revision, snapshot, "pruned", "pruned")
    except BaseException:
        store.finish_group_prune_run(run_id, status="failed")
        raise
    else:
        store.finish_group_prune_run(run_id)
    return _prune_result(run, dry, store.get_group_prune_items(run_id))
