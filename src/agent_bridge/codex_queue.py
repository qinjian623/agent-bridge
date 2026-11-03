"""Enqueue on an existing native thread; never start, resume, steer or interrupt it.

Run with the installed codex-async Python environment, like codex_parent.py.
"""

import json
import sys
from contextlib import ExitStack
from pathlib import Path


def client_id(delivery_id):
    return "cli-agent-bridge:group:" + delivery_id


def inspect_receipt(client, job):
    wanted = client_id(job["id"])
    params = {"threadId": job["native_id"], "limit": 100}
    cursors = set()
    while True:
        page = client.request("thread/queue/list", params)
        for item in page["data"]:
            if item.get("clientUserMessageId") == wanted:
                return {"status": "enqueued", "receipt": item["id"]}
        cursor = page.get("nextCursor")
        if not cursor:
            break
        if cursor in cursors:
            raise ValueError("Native queue repeated a pagination cursor")
        cursors.add(cursor)
        params["cursor"] = cursor
    # Absence from the queue is not acknowledgement: it could have been deleted.
    params = {"threadId": job["native_id"], "limit": 20, "sortDirection": "desc", "itemsView": "full"}
    cursors = set()
    for _ in range(5):
        page = client.request("thread/turns/list", params)
        for turn in page["data"]:
            if any(item.get("type") == "userMessage" and item.get("clientId") == wanted
                   for item in turn.get("items", [])):
                return {"status": "received", "receipt": job.get("receipt"), "turn_id": turn["id"]}
        cursor = page.get("nextCursor")
        if not cursor or cursor in cursors:
            break
        cursors.add(cursor)
        params["cursor"] = cursor
    # shortcut: scan only the latest 100 turns; older messages stay unconfirmed, never resent.
    return {"status": "unconfirmed", "error": "Not in the queue; no matching native user message in the latest 100 turns"}


def advance(client, job):
    if job.get("inspect"):
        return inspect_receipt(client, job)
    try:
        thread = client.request("thread/read", {"threadId": job["native_id"], "includeTurns": False})["thread"]
        if thread["id"] != job["native_id"] or Path(thread["cwd"]).resolve() != Path(job["cwd"]).resolve():
            return {"status": "failed", "error": "Native thread identity or directory does not match the group member"}
    except Exception as error:
        return {"status": "deferred", "error": "Native thread preflight failed: " + type(error).__name__}
    # Once enqueue is attempted, a missing response is uncertain and must not be retried.
    receipt = client.enqueue(job["native_id"], job["message"], client_id(job["id"]))
    return {"status": "enqueued", "receipt": receipt}


def main():
    from telegram_guard import delivery_status

    job = json.load(sys.stdin)
    if job["runtime"] != "codex":
        print(json.dumps({"status": "failed", "error": "No verified queue transport for this runtime"}))
        return
    if not job.get("inspect"):
        external = delivery_status(job.get("telegram_database"), job["native_id"])
        if external != "open":
            print(json.dumps({"status": "deferred", "error": "Telegram delivery state: " + external}))
            return
    with ExitStack() as stack:
        try:
            from codex_async.app_server import CodexAppServer

            client = stack.enter_context(CodexAppServer(
                Path(job["cwd"]), timeout=15, sandbox=None, daemon=True, start_daemon=False,
            ))
        except Exception as error:
            print(json.dumps({"status": "deferred", "error": "Shared Codex client unavailable: " + type(error).__name__}))
            return
        print(json.dumps(advance(client, job)))


if __name__ == "__main__":
    main()
