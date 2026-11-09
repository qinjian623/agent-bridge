"""Run with the installed codex-async Python environment; no Telegram API calls."""

import json
import os
import sys
from contextlib import ExitStack
from pathlib import Path

if __package__:
    from .codex_queue import inspect_receipt
else:
    from codex_queue import inspect_receipt


def read_resume_config(filename):
    if not filename:
        return None
    path = Path(filename).expanduser().resolve()
    if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077:
        raise ValueError("Parent resume config must be owned by you with mode 600")
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or set(data) - {"mcp_servers", "plugins", "apps", "features"}:
        raise ValueError("Parent resume config accepts only MCP, plugin, app and multi_agent settings")
    if not isinstance(data.get("features", {}), dict) or set(data.get("features", {})) - {"multi_agent"}:
        raise ValueError("Parent resume config cannot change permissions or model settings")
    return data


def receipt_status(client, job):
    return inspect_receipt(client, {
        "native_id": job["thread_id"], "receipt": job.get("receipt"),
    }, message_id="cli-agent-bridge:" + job["event_id"])


def retract(client, job):
    receipt = job.get("receipt")
    try:
        result = receipt_status(client, job)
    except Exception as error:
        cleanup = "uncertain" if job.get("cleanup_check_only") else "pending"
        return {"queue_cleanup": cleanup, "native_status": "unconfirmed", "receipt": receipt,
                "error": f"Native queue inspection failed ({type(error).__name__})"}
    if result["status"] == "received":
        return {"queue_cleanup": "already_started", "native_status": "received", "receipt": receipt,
                "native_turn_id": result["turn_id"]}
    if result["status"] != "enqueued":
        return {"queue_cleanup": "uncertain", "native_status": "unconfirmed", "receipt": receipt,
                "error": result.get("error", "Native queue entry was not confirmed")}
    receipt = result["receipt"]
    if job.get("cleanup_check_only"):
        return {"queue_cleanup": "uncertain", "native_status": "enqueued", "receipt": receipt,
                "error": "Native queue entry remains; read-only cleanup check did not delete it"}
    try:
        deleted = client.request("thread/queue/delete", {
            "threadId": job["thread_id"], "queuedSubmissionId": receipt,
        })
        if deleted.get("deleted") is True:
            return {"queue_cleanup": "removed", "native_status": "removed", "receipt": receipt}
    except Exception:
        pass
    # A failed reply may race with the original client taking this item; only inspect after the attempt.
    try:
        result = receipt_status(client, {**job, "receipt": receipt})
        if result["status"] == "received":
            return {"queue_cleanup": "already_started", "native_status": "received", "receipt": receipt,
                    "native_turn_id": result["turn_id"]}
    except Exception:
        pass
    return {"queue_cleanup": "uncertain", "native_status": "unconfirmed", "receipt": receipt,
            "error": "Native queue deletion was not confirmed; original notification was not resent"}


def advance(client, job, busy_error):
    if job.get("retract"):
        return retract(client, job)
    if job.get("inspect"):
        return {"native_status": client.request("thread/read", {"threadId": job["thread_id"]})["thread"]["status"]}
    if not job.get("receipt"):
        receipt = client.enqueue(job["thread_id"], job["message"], "cli-agent-bridge:" + job["event_id"])
        return {"receipt": receipt, "ready": False, "native_status": "enqueued"}
    status = client.request("thread/read", {"threadId": job["thread_id"]})["thread"]["status"]
    if status.get("activeFlags"):
        return {
            "deferred": "Native parent requires attention: " + ", ".join(status["activeFlags"]),
            "parent_status": status,
        }
    try:
        result = receipt_status(client, job)
    except ValueError as error:
        return {"deferred": str(error), "native_status": "unconfirmed", "ready": False}
    if result["status"] == "received":
        return {"receipt": job["receipt"], "ready": True, "native_status": "received",
                "native_turn_id": result["turn_id"]}
    if result["status"] != "enqueued":
        return {"receipt": job["receipt"], "ready": False, "native_status": "unconfirmed",
                "error": result.get("error")}
    if status["type"] == "notLoaded":
        try:
            # Omitting settings preserves the parent's own approvals, model and workspace.
            params = {"threadId": job["thread_id"], "excludeTurns": True}
            config = read_resume_config(job.get("resume_config_file"))
            if config is not None:
                params["config"] = config
            client.request("thread/resume", params)
        except busy_error:
            return {"deferred": "Original native client still owns this session"}
    elif status["type"] not in {"idle", "active"}:
        return {"deferred": "Native parent requires attention: " + status["type"]}
    return {"receipt": job["receipt"], "ready": False, "native_status": "enqueued"}


def main():
    from telegram_guard import delivery_status

    job = json.load(sys.stdin)
    if not job.get("inspect") and not job.get("retract"):
        external = delivery_status(job.get("telegram_database"), job["thread_id"])
        if external != "open":
            print(json.dumps({"deferred": "Telegram delivery state: " + external}))
            return
    with ExitStack() as stack:
        try:
            from codex_async.app_server import CodexAppServer, ThreadBusyError

            client = stack.enter_context(CodexAppServer(Path(job["cwd"]), timeout=30, sandbox=None, daemon=True, start_daemon=False))
        except Exception as error:
            # No enqueue was attempted; this setup failure is safe to retry.
            result = {"deferred": f"Parent client setup failed ({type(error).__name__})"}
            if job.get("retract"):
                result.update(queue_cleanup="uncertain" if job.get("cleanup_check_only") else "pending",
                              native_status="unconfirmed")
            print(json.dumps(result))
            return
        print(json.dumps(advance(client, job, ThreadBusyError)))


if __name__ == "__main__":
    main()
