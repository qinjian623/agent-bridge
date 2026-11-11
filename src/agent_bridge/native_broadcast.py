"""Durable deliveries to existing native sessions, separate from bridge workers."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from .config import safe_error
from .errors import BridgeError
from .groups import native_members_input, validate_group_id
from .peer_protocol import DeliverySource
from .store import encode

LEGACY_SOURCES = (str(DeliverySource.LOCAL_BROADCAST), "native_broadcast")
QUEUE_SOURCES = (*LEGACY_SOURCES, str(DeliverySource.PEER_DELIVER), str(DeliverySource.MIGRATION))
# Legacy rows written by an older build may still be the only evidence of an in-flight write; map them onto the
# queue states that refuse to overtake an unconfirmed native write.
LEGACY_STATUS = {"sending": "uncertain", "unconfirmed": "uncertain"}


def inbox(store, group_id, runtime, native_id, after=0, limit=10):
    """Read saved messages for one current native member, without claiming receipt."""
    validate_group_id(group_id)
    native_members_input([{"runtime": runtime, "native_id": native_id}], "remove")
    if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 10:
        raise BridgeError("invalid_pagination", "after must be nonnegative; limit must be between 1 and 10")
    with store.connection() as db:
        db.execute("BEGIN")
        group = store.required(db, "session_groups", group_id)
        if group["deleted"]:
            raise BridgeError("group_deleted", "This group was deleted")
        if not db.execute(
            "SELECT 1 FROM native_group_members WHERE group_id=? AND runtime=? AND native_id=?",
            (group_id, runtime, native_id),
        ).fetchone():
            raise BridgeError("not_group_member", "This native session is not a member of the specified group")
        rows = [dict(row) for row in db.execute(
            "SELECT rowid AS sequence,id AS delivery_id,broadcast_id,message,status AS delivery_status,created "
            "FROM native_broadcast_deliveries WHERE group_id=? AND runtime=? AND native_id=? AND rowid>? "
            "ORDER BY rowid LIMIT ?", (group_id, runtime, native_id, after, limit + 1),
        )]
        page = rows[:limit]
        return {"group_id": group_id, "runtime": runtime, "native_id": native_id, "messages": page,
                "cursor": page[-1]["sequence"] if page else after, "has_more": len(rows) > limit,
                "receipt_changed": False}


async def send(config, delivery, *, inspect=False):
    if delivery["runtime"] != "codex":
        return {"status": "failed", "error": "No verified noninterrupting queue for this runtime"}
    if not config.codex_async_python:
        return {"status": "deferred", "error": "codex_async_python is not configured"}
    try:
        process = await asyncio.create_subprocess_exec(
            config.codex_async_python, str(Path(__file__).with_name("codex_queue.py")),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
    except OSError as error:
        return {"status": "deferred", "error": "Queue helper did not start: " + safe_error(error)}
    try:
        payload = {**delivery, "inspect": inspect, "telegram_database": config.telegram_database}
        stdout, stderr = await asyncio.wait_for(process.communicate(encode(payload).encode()), 45)
        if process.returncode:
            raise RuntimeError("Queue helper failed: " + safe_error(stderr.decode(errors="replace")))
        return json.loads(stdout)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def send_agy(store, config, row, *, queue_id=None):
    item_id = queue_id or row["id"]
    payload = {**row, "telegram_database": config.telegram_database,
               "async_state": str(Path(config.telegram_database).parent if config.telegram_database else Path.home() / ".local/state/codex-async"),
               "timeout": config.task_timeout}
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "agent_bridge.agy_wake", stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, limit=1024 * 1024,
        )
    except OSError as error:
        return {"status": "deferred", "error": "AGY wake helper did not start: " + safe_error(error)}
    received = None
    try:
        process.stdin.write(encode(payload).encode())
        await process.stdin.drain()
        process.stdin.close()
        async with asyncio.timeout(config.task_timeout + 90):
            last = None
            while line := await process.stdout.readline():
                reply = json.loads(line)
                if reply.get("status") == "received":
                    if reply.get("receipt") != row["id"] or not isinstance(reply.get("turn_id"), str) or not reply["turn_id"].isdigit():
                        raise ValueError("Invalid native AGY user_input receipt")
                    if received and received["turn_id"] != reply["turn_id"]:
                        raise ValueError("AGY changed an already confirmed native user_input step")
                    received = reply
                    store.update_native_send(
                        item_id, status="received", receipt=reply["receipt"],
                        native_turn_id=reply["turn_id"], processing_status=reply.get("processing_status"),
                        error=reply.get("error"),
                    )
                last = reply
            await process.wait()
            if last is None or process.returncode:
                raise RuntimeError("AGY wake helper stopped without a final response")
            if received and (last.get("status") != "received" or last.get("processing_status") not in {"completed", "failed"}):
                raise RuntimeError("AGY wake helper did not confirm a terminal processing outcome")
            return last
    except Exception as error:
        if received:
            result = {**received, "processing_status": "failed", "error": "AGY stopped after receipt: " + safe_error(error)}
            store.update_native_send(
                item_id, status="received", receipt=received["receipt"],
                native_turn_id=received["turn_id"], processing_status="failed",
                error=result["error"], expected_status="received",
            )
            return result
        raise
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 8)
            except TimeoutError:
                process.kill()
                await process.wait()


def _legacy_delivery(store, row):
    if row["delivery_source"] not in LEGACY_SOURCES:
        return None
    with store.connection() as db:
        found = db.execute(
            "SELECT * FROM native_broadcast_deliveries WHERE id=?", (row["delivery_id"],)
        ).fetchone()
    return None if found is None else dict(found)


def _native_owned_by_bridge(store, native_id):
    with store.connection() as db:
        return db.execute("SELECT 1 FROM sessions WHERE native_id=?", (native_id,)).fetchone() is not None


async def refresh_queue_receipt(store, config, row, sender=None):
    """Inspect an existing Codex submission; never enqueue or resume anything."""
    if config is None or row["runtime"] != "codex" or row["status"] not in {"enqueued", "uncertain"}:
        return row
    sender = sender or send
    status, receipt, turn_id = row["status"], row["receipt"], row["native_turn_id"]
    try:
        reply = await sender(config, row, inspect=True)
        reported = reply.get("status")
        if reported == "unconfirmed":
            status, error = "uncertain", reply.get("error") or "Native receipt is not confirmed"
        elif reported in {"enqueued", "received"}:
            found = reply.get("receipt")
            if not isinstance(found, str) or not found or receipt and receipt != found:
                status, error = "uncertain", "Native receipt does not match the persisted submission"
            elif reported == "received" and not (isinstance(reply.get("turn_id"), str) and reply["turn_id"]):
                status, error = "uncertain", "Native receipt has no matching turn"
            else:
                status, receipt = reported, found
                turn_id = reply.get("turn_id") or turn_id
                error = reply.get("error")
        else:
            error = reply.get("error") or "Native receipt inspection is unavailable"
    except Exception as exc:
        error = safe_error(exc)
    return store.update_native_send(
        row["id"], status=status, receipt=receipt, native_turn_id=turn_id,
        processing_status=row["processing_status"], error=error, expected_status=row["status"],
    )


async def deliver(store, config, sender=send, *, tasks=None):
    capacity = max(0, config.parallel_limit - len(tasks)) if tasks is not None else 10
    rows = store.claim_eligible_native_sends(capacity, delivery_sources=QUEUE_SOURCES)

    async def process_row(row, item_id):
        try:
            result = await send_agy(store, config, row, queue_id=item_id) if row["runtime"] == "antigravity" and sender is send else await sender(config, row)
            status, receipt, error = result["status"], result.get("receipt"), result.get("error")
            if status == "enqueued" and isinstance(receipt, str) and receipt:
                pass
            elif status == "received" and receipt == row["id"] and str(result.get("turn_id", "")).isdigit():
                pass
            elif status in {"deferred", "failed", "uncertain"}:
                status = "pending" if status == "deferred" else status
            else:
                raise ValueError("Queue helper did not confirm a valid receipt")
        except Exception as exc:
            status, receipt, error = "uncertain", None, safe_error(exc)
            result = {}
        store.update_native_send(
            item_id, status=status, receipt=receipt, native_turn_id=result.get("turn_id"),
            processing_status=result.get("processing_status"), error=error, expected_status="sending",
        )

    pending = []
    for claimed in rows:
        legacy = _legacy_delivery(store, claimed)
        if legacy is not None and legacy["status"] != "pending":
            # A legacy-only writer (older build, recovery, or an explicit tool) moved this delivery on.
            # Adopt its fact rather than sending again; `sending` and `unconfirmed` become uncertain.
            store.update_native_send(
                claimed["id"], status=LEGACY_STATUS.get(legacy["status"], legacy["status"]),
                receipt=legacy["receipt"], native_turn_id=legacy["native_turn_id"],
                processing_status=legacy["processing_status"], error=legacy["error"],
                expected_status="sending",
            )
            continue
        if _native_owned_by_bridge(store, claimed["native_id"]):
            store.update_native_send(
                claimed["id"], status="failed",
                error="Native session is now owned by bridge; inspect before sending",
                expected_status="sending",
            )
            continue
        row = claimed if legacy is None else {**legacy, **claimed}
        if claimed["delivery_source"] == str(DeliverySource.PEER_DELIVER):
            inbox = store.get_inbox_delivery(claimed["delivery_id"])
            payload = inbox.get("payload") if inbox else None
            if not isinstance(payload, dict):
                store.update_native_send(
                    claimed["id"], status="failed", error="Original peer delivery metadata is missing",
                    expected_status="sending",
                )
                continue
            row = {**row, "cwd": payload.get("cwd"), "group_id": payload.get("group_id", "")}
            if row["runtime"] == "antigravity" and (
                not isinstance(row["cwd"], str) or not Path(row["cwd"]).is_absolute() or not Path(row["cwd"]).is_dir()
            ):
                store.update_native_send(
                    claimed["id"], status="failed", error="Remote AGY member needs its existing native working directory",
                    expected_status="sending",
                )
                continue
        task = asyncio.create_task(process_row(row, claimed["id"]))
        if tasks is not None:
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        else:
            pending.append(task)
    if pending:
        await asyncio.gather(*pending)


def view(store, broadcast_id):
    if not isinstance(broadcast_id, str) or not broadcast_id:
        raise BridgeError("invalid_broadcast", "Specify a broadcast_id")
    with store.connection() as db:
        db.execute("BEGIN")
        row = db.execute(
            "SELECT data FROM events WHERE kind='group_broadcast' AND json_extract(data,'$.broadcast_id')=?",
            (broadcast_id,),
        ).fetchone()
        if not row:
            raise BridgeError("not_found", "Unknown broadcast_id")
        result = json.loads(row["data"])
        for member in result["recipients"]:
            if "delivery_id" in member:
                delivery = db.execute(
                    "SELECT status,receipt,native_turn_id,processing_status,error FROM native_broadcast_deliveries WHERE id=?", (member["delivery_id"],),
                ).fetchone()
                # Remote deliveries live in peer_outbox/scheduler state, not the legacy native table.
                if delivery is not None:
                    member.update(dict(delivery))
            elif "task_id" in member:
                task = db.execute("SELECT status FROM tasks WHERE id=?", (member["task_id"],)).fetchone()
                member["status"] = task["status"]
        # These counts describe current states, not the original enqueue response.
        for key in ("queued_count", "skipped_count", "pending_count"):
            result.pop(key, None)
        result["counts"] = {}
        for member in result["recipients"]:
            status = member["status"]
            result["counts"][status] = result["counts"].get(status, 0) + 1
        return result


async def inspect(store, config, broadcast_id, *, refresh=False, sender=send):
    if not isinstance(refresh, bool):
        raise BridgeError("invalid_request", "refresh must be a boolean")
    result = view(store, broadcast_id)
    if not refresh:
        return result
    with store.connection() as db:
        rows = [dict(row) for row in db.execute(
            "SELECT * FROM native_broadcast_deliveries WHERE broadcast_id=? AND runtime='codex' AND status IN ('enqueued','uncertain','unconfirmed')",
            (broadcast_id,),
        )]

    async def check(row):
        try:
            reply = await sender(config, row, inspect=True)
            status = reply["status"]
            if status not in {"enqueued", "received", "unconfirmed"}:
                raise ValueError(reply.get("error", "Native receipt could not be inspected"))
            if status == "enqueued" and not reply.get("receipt"):
                raise ValueError("Native queue did not return a receipt")
            if status == "received" and not reply.get("turn_id"):
                raise ValueError("Native history did not return a matching turn")
            if status == "unconfirmed" and row["status"] == "uncertain":
                status = "uncertain"
            error = reply.get("error")
            receipt = reply.get("receipt") or row["receipt"]
            turn_id = reply.get("turn_id") or row["native_turn_id"]
        except Exception as exc:
            status, receipt, error = row["status"], row["receipt"], safe_error(exc)
            turn_id = row["native_turn_id"]
        queued = store.get_native_send_by_delivery(DeliverySource.LOCAL_BROADCAST, row["id"])
        if queued is None:
            queued = store.get_native_send_by_delivery("native_broadcast", row["id"])
        if queued is None:
            with store.connection(write=True) as db:
                db.execute(
                    "UPDATE native_broadcast_deliveries SET status=?,receipt=?,native_turn_id=?,error=? WHERE id=? AND status=?",
                    (status, receipt, turn_id, error, row["id"], row["status"]),
                )
        else:
            # Absence from the native queue is never proof of non-delivery.
            queue_status = "uncertain" if status == "unconfirmed" else status
            store.update_native_send(
                queued["id"], status=queue_status, receipt=receipt, native_turn_id=turn_id,
                processing_status=row["processing_status"], error=error,
                expected_status=queued["status"],
            )

    await asyncio.gather(*(check(row) for row in rows))
    return view(store, broadcast_id)
