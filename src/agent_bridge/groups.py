from __future__ import annotations

import time
import uuid
from pathlib import Path

from .errors import BridgeError
from .store import Store, encode, identifier, task_input


def validate_group_id(group_id):
    if not isinstance(group_id, str) or not group_id.strip() or group_id != group_id.strip() or len(group_id) > 128:
        raise BridgeError("invalid_group", "Specify one nonempty group_id of at most 128 characters, without surrounding whitespace")


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
            "(SELECT COUNT(*) FROM native_group_members WHERE group_id=g.id) AS member_count "
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


def control_group(store, group_id, action, revision, request_id, session_ids=None, native_members=None):
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
    if (action in {"add", "remove"} and not members and not natives) or (
        action == "delete" and (session_ids is not None or native_members is not None)
    ):
        raise BridgeError("invalid_members", "Add/remove require members; delete does not accept members")
    payload = {"op": "group", "group_id": group_id, "action": action, "revision": revision, "session_ids": members}
    if native_members is not None:
        payload["native_members"] = natives
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
        else:
            db.execute("DELETE FROM session_group_members WHERE group_id=?", (group_id,))
            db.execute("DELETE FROM native_group_members WHERE group_id=?", (group_id,))
            db.execute("UPDATE session_groups SET deleted=1 WHERE id=?", (group_id,))
        if changed:
            if action != "create":
                db.execute("UPDATE session_groups SET revision=revision+1 WHERE id=?", (group_id,))
            store.event(db, "group_changed", {"group_id": group_id, "action": action, "session_ids": members,
                                               "native_members": natives})
        return store.remember(db, request_id, fingerprint, group_view(db, group_id))


def broadcast(store, group_id, message, revision, request_id, reservation=0, *, token_budget=None):
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
        if not group["members"]:
            raise BridgeError("empty_group", "This group has no members; nothing was sent")
        if token_budget is not None and any(m.get("kind") == "native" for m in group["members"]):
            raise BridgeError("native_budget_unsupported", "Native sessions execute outside bridge token accounting; cannot broadcast to them while a token budget is enabled")
        broadcast_id = identifier()
        recipients = []
        details = {"kind": "broadcast", "learn_from_result": False, "group_id": group_id,
                   "group_revision": revision, "broadcast_id": broadcast_id}
        # Membership, all task inserts and the retry receipt commit in one transaction.
        for member in group["members"]:
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
                recipients.append({"delivery_id": delivery_id, "runtime": member["runtime"],
                                   "native_id": member["native_id"], "status": status,
                                   **({"reason": reason} if reason else {})})
                continue
            sid = member["session_id"]
            reason = None
            if member["desired"] != "open":
                reason = {"code": "session_state", "desired": member["desired"]}
            elif member["observed"] == "recovery_needed":
                reason = {"code": "worker_state", "observed": member["observed"]}
            if reason:
                recipients.append({"session_id": sid, "status": "skipped", "reason": reason})
            else:
                recipients.append(store.enqueue_task(db, sid, message, [], reservation, details))
        queued = sum(row["status"] == "queued" for row in recipients)
        result = {"broadcast_id": broadcast_id, "group_id": group_id, "group_revision": revision,
                  "queued_count": queued, "skipped_count": sum(r["status"] == "skipped" for r in recipients),
                  "recipients": recipients}
        if any("delivery_id" in r for r in recipients):
            result["pending_count"] = sum(r["status"] == "pending" for r in recipients)
        store.event(db, "group_broadcast", result)
        return store.remember(db, request_id, fingerprint, result)
