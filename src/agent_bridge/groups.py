from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path

from .errors import BridgeError
from .peer_protocol import (
    DeliverySource,
    PeerDeliveryStatus,
    canonical_operation_id,
    compute_stream_id,
    normalize_remote_member,
)
from .store import Store, encode, identifier, task_input

TERMINAL_RECIPIENT_STATUSES = (
    PeerDeliveryStatus.RELEASED,
    PeerDeliveryStatus.CANCELLED,
    PeerDeliveryStatus.FAILED,
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
    return {
        "group_id": row["id"], "revision": row["revision"], "deleted": bool(row["deleted"]),
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


def inspect_groups(store, group_id=None):
    if group_id is not None:
        validate_group_id(group_id)
    with store.connection() as db:
        db.execute("BEGIN")
        if group_id is not None:
            return group_view(db, group_id)
        return {"groups": [dict(row) for row in db.execute(
            "SELECT g.id AS group_id,g.revision,"
            "(SELECT COUNT(*) FROM session_group_members WHERE group_id=g.id)+"
            "(SELECT COUNT(*) FROM native_group_members WHERE group_id=g.id)+"
            "(SELECT COUNT(*) FROM remote_group_members WHERE group_id=g.id) AS member_count "
            "FROM session_groups g WHERE g.deleted=0 ORDER BY g.created,g.id"
        )]}


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
        if action == "create":
            if revision != 0:
                raise BridgeError("stale_revision", "A new group requires expected_revision=0")
            if db.execute("SELECT 1 FROM session_groups WHERE id=?", (group_id,)).fetchone():
                raise BridgeError("group_exists", "This group_id already exists or was deleted; use a new ID")
            db.execute("INSERT INTO session_groups(id,created) VALUES(?,?)", (group_id, time.time()))
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
            now = time.time()
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
            db.execute("DELETE FROM session_group_members WHERE group_id=?", (group_id,))
            db.execute("DELETE FROM native_group_members WHERE group_id=?", (group_id,))
            # Remote members stay archived with the soft-deleted group so the frozen history remains inspectable.
            db.execute("UPDATE session_groups SET deleted=1 WHERE id=?", (group_id,))
        if changed:
            if action != "create":
                db.execute("UPDATE session_groups SET revision=revision+1 WHERE id=?", (group_id,))
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


def broadcast(store, group_id, message, revision, request_id, reservation=0, *, token_budget=None,
              authority_ref=None, sender_ref=None):
    validate_group_id(group_id)
    task_input(message, reservation)
    if type(revision) is not int or revision < 0:
        raise BridgeError("invalid_revision", "Provide the group's nonnegative expected_revision")
    payload = {"op": "broadcast", "group_id": group_id, "message": message, "revision": revision, "reservation": reservation}
    with store.connection(write=True) as db:
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
