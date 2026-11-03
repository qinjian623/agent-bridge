"""Durable deliveries to existing native sessions, separate from bridge workers."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

from .config import safe_error
from .errors import BridgeError
from .groups import native_members_input, validate_group_id
from .store import encode


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


async def send_agy(store, config, row):
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
                    with store.connection(write=True) as db:
                        db.execute(
                            "UPDATE native_broadcast_deliveries SET status='received',receipt=?,native_turn_id=?,processing_status=?,error=? WHERE id=? AND status IN ('sending','received','uncertain')",
                            (reply["receipt"], reply["turn_id"], reply.get("processing_status"), reply.get("error"), row["id"]),
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
            with store.connection(write=True) as db:
                db.execute("UPDATE native_broadcast_deliveries SET processing_status='failed',error=? WHERE id=? AND status='received'",
                           (result["error"], row["id"]))
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


async def deliver(store, config, sender=send, *, tasks=None):
    capacity = max(0, config.parallel_limit - len(tasks)) if tasks is not None else 10
    with store.connection() as db:
        rows = [dict(row) for row in db.execute(
            "SELECT * FROM native_broadcast_deliveries WHERE status='pending' ORDER BY last_attempt,created LIMIT ?", (capacity,),
        )]

    async def process_row(row):
        try:
            result = await send_agy(store, config, row) if row["runtime"] == "antigravity" and sender is send else await sender(config, row)
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
        with store.connection(write=True) as db:
            db.execute(
                "UPDATE native_broadcast_deliveries SET status=?,receipt=?,native_turn_id=?,processing_status=?,error=? WHERE id=? AND status='sending'",
                (status, receipt, result.get("turn_id"), result.get("processing_status"), error, row["id"]),
            )

    pending = []
    for row in rows:
        with store.connection(write=True) as db:
            if not db.execute(
                "UPDATE native_broadcast_deliveries SET status='sending',last_attempt=? WHERE id=? AND status='pending'",
                (time.time(), row["id"]),
            ).rowcount:
                continue
            if db.execute("SELECT 1 FROM sessions WHERE native_id=?", (row["native_id"],)).fetchone():
                db.execute(
                    "UPDATE native_broadcast_deliveries SET status='failed',error='Native session is now owned by bridge; inspect before sending' WHERE id=?",
                    (row["id"],),
                )
                continue
        task = asyncio.create_task(process_row(row))
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
        with store.connection(write=True) as db:
            db.execute(
                "UPDATE native_broadcast_deliveries SET status=?,receipt=?,native_turn_id=?,error=? WHERE id=? AND status=?",
                (status, receipt, turn_id, error, row["id"], row["status"]),
            )

    await asyncio.gather(*(check(row) for row in rows))
    return view(store, broadcast_id)
