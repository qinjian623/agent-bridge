"""Restore a closed original AGY session for one message; never attach to a terminal."""

import fcntl
import json
import os
import queue
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from contextlib import closing
from pathlib import Path

from .agy_presence import inactive_reason


def native_baseline(home, native_id):
    conversation = home / "conversations" / (native_id + ".db")
    summary = home / "conversation_summaries.db"
    with closing(sqlite3.connect(summary.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        row = db.execute("SELECT nesting_depth FROM conversation_summaries WHERE conversation_id=?", (native_id,)).fetchone()
    if not row or row[0] not in (None, 0):
        raise ValueError("Original native main session does not exist")
    with closing(sqlite3.connect(conversation.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        return db.execute("SELECT COALESCE(MAX(idx),-1) FROM steps").fetchone()[0]


def wake(job, emit):
    native_id = job["native_id"]
    if str(uuid.UUID(native_id)) != native_id:
        raise ValueError("Invalid native UUID")
    home = Path(job.get("native_home") or Path.home() / ".gemini/antigravity-cli")
    executable = job.get("executable", "agy")
    locks = Path(job["async_state"]) / "antigravity-locks"
    locks.mkdir(mode=0o700, parents=True, exist_ok=True)
    def blocked(exclude_pid=None):
        return inactive_reason(native_id, executable, native_home=home, exclude_pid=exclude_pid)

    with (locks / (native_id + ".lock")).open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "deferred", "error": "Original session already has an active bridge worker"}
        reason = blocked()
        if reason:
            return {"status": "deferred", "error": reason}
        try:
            baseline = native_baseline(home, native_id)
        except (OSError, sqlite3.Error, ValueError):
            return {"status": "failed", "error": "Original native main-session records are missing or unreadable"}
        events, errors = queue.Queue(), []
        env = dict(os.environ)
        for key in ("CODEX_THREAD_ID", "AGENT_BRIDGE_ROLE", "AGENT_BRIDGE_TOKEN"):
            env.pop(key, None)
        # Do not override model/effort/authentication or bypass native tool approval.
        command = [executable, "--conversation", native_id, "--input-format", "stream-json",
                   "--output-format", "stream-json"]
        process = None
        written = received = False
        result = None
        readers = []
        try:
            process = subprocess.Popen(command, cwd=job["cwd"], env=env, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)

            def read_events():
                try:
                    for line in process.stdout:
                        events.put(json.loads(line))
                except (ValueError, UnicodeError):
                    events.put({"event": "invalid"})
                finally:
                    events.put(None)

            def read_errors():
                for line in process.stderr:
                    if re.search(r"soft[ -]denied|permission denied|requires? approval", line, re.I):
                        errors.append("Native permission denied")

            for fn in (read_events, read_errors):
                reader = threading.Thread(target=fn, daemon=True)
                reader.start()
                readers.append(reader)
            initial = events.get(timeout=60)
            if not isinstance(initial, dict) or initial.get("event") != "init" or initial.get("conversation_id") != native_id:
                return {"status": "failed", "error": "AGY did not initialize the original native session"}
            reason = blocked(process.pid)
            if reason:
                return {"status": "deferred", "error": reason}
            # Check again immediately before the sole write, including native locks.
            reason = blocked(process.pid)
            if reason:
                return {"status": "deferred", "error": reason}
            message = f"[cli-agent-bridge delivery={job['id']} group={job['group_id']}]\n{job['message']}"
            written = True
            process.stdin.write(json.dumps({"event": "user", "message": {"content": message}}, ensure_ascii=False) + "\n")
            process.stdin.close()
            deadline = time.monotonic() + min(float(job.get("timeout", 3600)), 60)
            overall = time.monotonic() + float(job.get("timeout", 3600))
            receipt = None
            while True:
                if time.monotonic() >= min(deadline, overall):
                    raise queue.Empty()
                event = events.get(timeout=max(.01, min(deadline, overall) - time.monotonic()))
                if event is None:
                    break
                if not isinstance(event, dict) or result is not None:
                    raise ValueError("Invalid events after terminal AGY result")
                if event.get("event") == "step_update":
                    step = event.get("step_update", {})
                    if step.get("conversation_id") != native_id:
                        raise ValueError("AGY step belongs to another native session")
                    index = step.get("step_index")
                    if step.get("step_type") == "user_input" and step.get("state") == "DONE" and type(index) is int and index > baseline and not received:
                        received = True
                        receipt = {"status": "received", "receipt": job["id"], "turn_id": str(index), "processing_status": "running"}
                        emit(receipt)
                        deadline = overall
                elif event.get("event") == "result":
                    result = event.get("result", {})
                    if result.get("conversation_id") != native_id:
                        raise ValueError("AGY result belongs to another native session")
                    deadline = min(deadline, time.monotonic() + 10)
                elif event.get("event") not in {"init"}:
                    raise ValueError("Unexpected AGY protocol event")
            process.wait(timeout=10)
            for reader in readers:
                reader.join(timeout=5)
            if not received:
                return {"status": "uncertain", "error": "AGY did not confirm a new native user_input step"}
            successful = (result and result.get("status") == "SUCCESS" and bool(result.get("response"))
                          and not errors and process.returncode == 0 and not any(r.is_alive() for r in readers))
            return {**receipt, "processing_status": "completed" if successful else "failed",
                    "error": None if successful else "Native AGY did not finish successfully; message was received and will not be resent"}
        except (OSError, ValueError, queue.Empty, subprocess.SubprocessError) as error:
            if received:
                return {**receipt, "processing_status": "failed", "error": "AGY stopped after receipt: " + type(error).__name__}
            return {"status": "uncertain" if written else "deferred", "error": "AGY wake failed: " + type(error).__name__}
        finally:
            if process:
                try:
                    # This is the new process group we created, including its MCP children.
                    os.killpg(process.pid, signal.SIGTERM)
                    if process.poll() is None:
                        process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                except ProcessLookupError:
                    pass
                finally:
                    # The CLI exiting does not prove its MCP/tool descendants exited.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                for handle in (process.stdin, process.stdout, process.stderr):
                    if handle and not handle.closed:
                        handle.close()


def main():
    def stop(*_):
        raise SystemExit(143)

    signal.signal(signal.SIGTERM, stop)
    job = json.load(sys.stdin)

    def emit(value):
        print(json.dumps(value), flush=True)

    emit(wake(job, emit))


if __name__ == "__main__":
    main()
