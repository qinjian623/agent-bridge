"""Run with the installed codex-async Python environment; no Telegram API calls."""

import json
import os
import sys
from contextlib import ExitStack
from pathlib import Path


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


def advance(client, job, busy_error):
    if job.get("inspect"):
        return {"native_status": client.request("thread/read", {"threadId": job["thread_id"]})["thread"]["status"]}
    if not job.get("receipt"):
        receipt = client.enqueue(job["thread_id"], job["message"], "cli-agent-bridge:" + job["event_id"])
        return {"receipt": receipt, "ready": False}
    status = client.request("thread/read", {"threadId": job["thread_id"]})["thread"]["status"]
    if status.get("activeFlags"):
        return {
            "deferred": "Native parent requires attention: " + ", ".join(status["activeFlags"]),
            "native_status": status,
        }
    params = {"threadId": job["thread_id"], "limit": 100}
    cursors = set()
    while True:
        queued = client.request("thread/queue/list", params)
        pending = any(item["id"] == job["receipt"] for item in queued["data"])
        cursor = queued.get("nextCursor")
        if pending or not cursor:
            break
        if cursor in cursors:
            return {"deferred": "Native queue pagination repeated a cursor"}
        cursors.add(cursor)
        params = {**params, "cursor": cursor}
    if pending and status["type"] == "notLoaded":
        try:
            # Omitting settings preserves the parent's own approvals, model and workspace.
            params = {"threadId": job["thread_id"], "excludeTurns": True}
            config = read_resume_config(job.get("resume_config_file"))
            if config is not None:
                params["config"] = config
            client.request("thread/resume", params)
        except busy_error:
            return {"deferred": "Original native client still owns this session"}
    elif pending and status["type"] not in {"idle", "active"}:
        return {"deferred": "Native parent requires attention: " + status["type"]}
    return {"receipt": job["receipt"], "ready": True}


def main():
    from telegram_guard import delivery_status

    job = json.load(sys.stdin)
    external = delivery_status(job.get("telegram_database"), job["thread_id"])
    if not job.get("inspect") and external != "open":
        print(json.dumps({"deferred": "Telegram delivery state: " + external}))
        return
    with ExitStack() as stack:
        try:
            from codex_async.app_server import CodexAppServer, ThreadBusyError

            client = stack.enter_context(CodexAppServer(Path(job["cwd"]), timeout=30, sandbox=None, daemon=True, start_daemon=False))
        except Exception as error:
            # No enqueue was attempted; this setup failure is safe to retry.
            print(json.dumps({"deferred": f"Parent client setup failed ({type(error).__name__})"}))
            return
        print(json.dumps(advance(client, job, ThreadBusyError)))


if __name__ == "__main__":
    main()
