#!/usr/bin/env python3
"""Protocol fixture for process crash/restart tests; never contacts a provider."""

import json
import sys
import threading
import time
import uuid
from pathlib import Path

lock = threading.Lock()
thread_id = None
turn_id = None
stopped = threading.Event()


def emit(value):
    with lock:
        print(json.dumps(value), flush=True)


def complete(turn):
    time.sleep(2)
    emit(
        {
            "method": "item/completed",
            "params": {
                "threadId": thread_id,
                "turnId": turn,
                "item": {"type": "agentMessage", "id": "answer", "phase": "final_answer", "text": "fixture complete"},
            },
        }
    )
    emit(
        {
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": {"id": turn, "status": "interrupted" if stopped.is_set() else "completed"},
            },
        }
    )


for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    reply = {}
    if method in {"thread/start", "thread/resume"}:
        thread_id = request["params"].get("threadId") or str(uuid.uuid4())
        reply = {"thread": {"id": thread_id}}
    elif method == "model/list":
        second = bool(request["params"].get("cursor"))
        reply = {
            "data": [{"model": "new-model" if second else "fixed-model", "inputModalities": ["text"]}],
            "nextCursor": None if second else "second",
        }
    elif method == "turn/start":
        turn_id = str(uuid.uuid4())
        with Path("invocations.txt").open("a") as file:
            file.write(turn_id + "\n")
        reply = {"turn": {"id": turn_id}}
    elif method == "turn/interrupt":
        stopped.set()
    if "id" in request:
        emit({"id": request["id"], "result": reply})
    if method == "turn/start":
        threading.Thread(target=complete, args=(turn_id,), daemon=True).start()
