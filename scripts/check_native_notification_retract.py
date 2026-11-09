"""Check notification retraction against one private Native stdio instance; never use the shared daemon."""

import argparse
import ast
import asyncio
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

from agent_bridge.codex_parent import advance, receipt_status
from agent_bridge.config import Config, safe_error
from agent_bridge.notifications import cleanup_processed, deliver
from agent_bridge.store import Store


class QueueClient:
    """Record metadata and reject every operation outside this test's thread and queue IDs."""

    METHODS = {"thread/start", "thread/read", "thread/turns/list", "thread/queue/add", "thread/queue/list", "thread/queue/delete"}

    def __init__(self, native, report):
        self.native, self.report = native, report
        self.thread_id = None
        self.receipts = set()
        self.race_receipt = None

    def request(self, method, params):
        assert method in self.METHODS, "Forbidden Native method"
        if method == "thread/start":
            assert self.thread_id is None and set(params) == {"cwd"}, "Only one unmodified test thread is allowed"
        else:
            assert self.thread_id and params.get("threadId") == self.thread_id, "Wrong Native thread"
        if method == "thread/queue/delete":
            receipt = params["queuedSubmissionId"]
            assert receipt in self.receipts, "Cannot delete an unowned queue entry"
            if self.race_receipt == receipt:
                self.race_receipt = None
                assert self.request(method, params)["deleted"] is True
                self.report["injected_predelete_race"] = True
        item = {"method": method, "id": self.native.sequence + 1}
        item.update({key: params[key] for key in ("threadId", "queuedSubmissionId", "clientUserMessageId") if key in params})
        self.report["rpc"].append(item)
        result = self.native.request(method, params)
        if method == "thread/start":
            thread = result["thread"]
            assert thread["turns"] == [] and thread["status"]["type"] == "idle", "New test thread is not empty and idle"
            self.thread_id = thread["id"]
            uuid.UUID(self.thread_id)
            item["threadId"] = self.thread_id
            item.update(empty=True, idle=True)
        elif method == "thread/queue/add":
            receipt = result["queuedSubmission"]["id"]
            assert isinstance(receipt, str) and receipt
            self.receipts.add(receipt)
            item["receipt"] = receipt
        elif method == "thread/queue/delete":
            assert isinstance(result.get("deleted"), bool)
            item["deleted"] = result["deleted"]
        elif method == "thread/turns/list":
            item["empty"] = not result["data"]
        elif method == "thread/read":
            item["empty"] = not result["thread"].get("turns")
            item["idle"] = result["thread"]["status"]["type"] == "idle"
        return result

    def enqueue(self, thread_id, text, message_id):
        return self.request("thread/queue/add", {
            "threadId": thread_id, "clientUserMessageId": message_id,
            "input": [{"type": "text", "text": text}],
        })["queuedSubmission"]["id"]

    def empty_turns(self):
        try:
            result = self.request("thread/turns/list", {
                "threadId": self.thread_id, "limit": 1, "itemsView": "full",
            })
        except Exception as error:
            rejection = native_rejection(error)
            if not (rejection.get("code") == -32600 and rejection.get("message") == (
                f"thread {self.thread_id} is not materialized yet; thread/turns/list is unavailable before first user message"
            )):
                raise
            self.report["rpc"][-1]["unmaterialized_before_first_user_message"] = True
            thread = self.request("thread/read", {"threadId": self.thread_id, "includeTurns": True})["thread"]
            assert thread["status"]["type"] == "idle" and thread["turns"] == []
            self.report["unmaterialized_history_verified_by_thread_read"] = True
        else:
            assert result["data"] == [], "Unexpected Native turn: no model execution is allowed"
        assert not any(e.get("method", "").startswith("turn/") for e in self.native.events), "Unexpected turn event"

    def queue(self):
        items, cursor, seen = {}, None, set()
        while True:
            params = {"threadId": self.thread_id, "limit": 100}
            if cursor:
                params["cursor"] = cursor
            page = self.request("thread/queue/list", params)
            for item in page["data"]:
                assert item["id"] in self.receipts, "Unowned item appeared in the private test queue"
                items[item["id"]] = item["clientUserMessageId"]
            cursor = page.get("nextCursor")
            if not cursor:
                return items
            assert cursor not in seen, "Queue pagination loop"
            seen.add(cursor)

    def writes(self):
        return sum(item["method"] in {"thread/queue/add", "thread/queue/delete"} for item in self.report["rpc"])


def native_rejection(error):
    message = str(error)
    if message.startswith("App Server rejected ") and ": " in message:
        try:
            result = ast.literal_eval(message.split(": ", 1)[1])
            if isinstance(result, dict):
                return result
        except (SyntaxError, ValueError):
            pass
    return {}


def describe_error(error):
    rejection = native_rejection(error)
    result = {"error_type": type(error).__name__}
    if rejection:
        result["native_error_code"] = rejection.get("code")
    message = rejection.get("message", str(error))
    result["error_message"] = re.sub(r"(?<!\w)/(?:home|tmp|run|etc|Users|private)/[^\s'\"<>]+", "<path>", safe_error(message))[:240]
    return result


async def check(native, root, report):
    client = QueueClient(native, report)
    report["phase"] = "thread_start"
    client.request("thread/start", {"cwd": str(root / "cwd")})
    report["thread_id"] = client.thread_id
    client.empty_turns()
    assert not client.queue()
    store = Store(root / "bridge-state")
    config = Config({}, codex_async_python=sys.executable, telegram_database=None)
    parent_id = "private-notification-check"
    store.parent_control(parent_id, "parent_register", 0, "register-test-parent", client.thread_id, str(root / "cwd"))
    nonce = uuid.uuid4().hex
    marker_events = ["marker-a:" + nonce, "marker-b:" + nonce]
    markers = {
        client.enqueue(client.thread_id, "PRIVATE QUEUE MARKER; DO NOT EXECUTE", "cli-agent-bridge:" + event):
        "cli-agent-bridge:" + event for event in marker_events
    }

    def event():
        with store.connection(write=True) as db:
            seq = store.event(db, "synthetic_notification_check", {"status": "test_only"})
            db.execute("INSERT INTO deliveries(event_seq,parent_id) VALUES(?,?)", (seq, parent_id))
        report["synthetic_bridge_events_created"] += 1
        return seq

    def delivery(seq):
        return next(item for item in store.get_parent(parent_id)["deliveries"] if item["event_seq"] == seq)

    async def sender(config, parent, message, seq, receipt=None):
        assert parent["native_id"] == client.thread_id
        assert receipt is None, "Do not run the helper's resume-capable polling path"
        return advance(client, {
            "thread_id": client.thread_id, "event_id": str(seq), "receipt": receipt, "message": message,
        }, RuntimeError)

    async def retractor(config, parent, seq, receipt=None, *, check_only=False):
        assert parent["native_id"] == client.thread_id
        report["cleanup_calls"].append({"event_seq": seq, "check_only": check_only})
        return advance(client, {
            "thread_id": client.thread_id, "event_id": str(seq), "receipt": receipt,
            "retract": True, "cleanup_check_only": check_only,
        }, RuntimeError)

    report["phase"] = "ack_retract"
    first = event()
    await deliver(store, config, sender, retractor)
    before = delivery(first)
    assert before["status"] == "wake_pending" and before["native_status"] == "enqueued"
    first_job = {"thread_id": client.thread_id, "event_id": str(first), "receipt": before["receipt"]}
    assert receipt_status(client, first_job)["status"] == "enqueued"
    assert client.queue() == {**markers, before["receipt"]: "cli-agent-bridge:" + str(first)}
    client.empty_turns()
    ack_args = (parent_id, "parent_ack", 0, "ack-first", None, None, first)
    ack = store.parent_control(*ack_args)
    await cleanup_processed(store, config, retractor)
    assert delivery(first)["queue_cleanup"] == "removed" and delivery(first)["native_status"] == "removed"
    assert client.queue() == markers
    writes = client.writes()
    assert store.parent_control(*ack_args) == ack
    await cleanup_processed(store, config, retractor)
    assert client.writes() == writes
    client.empty_turns()
    report.update(ack_before_enqueued=True, ack_removed_exact_notice=True, markers_preserved=True, duplicate_ack_no_write=True)

    report["phase"] = "delete_false_race"
    second = event()
    await deliver(store, config, sender, retractor)
    second_receipt = delivery(second)["receipt"]
    client.race_receipt = second_receipt
    revision = store.get_parent(parent_id)["revision"]
    store.parent_control(parent_id, "parent_ack", revision, "ack-second", event_seq=second)
    await cleanup_processed(store, config, retractor)
    assert delivery(second)["queue_cleanup"] == "uncertain"
    assert any(item.get("queuedSubmissionId") == second_receipt and item.get("deleted") is False for item in report["rpc"])
    assert client.queue() == markers
    client.empty_turns()
    report["native_delete_false_observed"] = True

    report["phase"] = "readonly_recheck"
    await asyncio.sleep(31)
    writes = client.writes()
    await cleanup_processed(store, config, retractor)
    assert report["cleanup_calls"][-1] == {"event_seq": second, "check_only": True}
    assert delivery(second)["queue_cleanup"] == "uncertain" and client.writes() == writes
    # A still-present marker also proves that check_only refuses a possible delete.
    marker_receipt = next(iter(markers))
    result = advance(client, {
        "thread_id": client.thread_id, "event_id": marker_events[0], "receipt": marker_receipt,
        "retract": True, "cleanup_check_only": True,
    }, RuntimeError)
    assert result["queue_cleanup"] == "uncertain" and result["native_status"] == "enqueued"
    assert client.writes() == writes and client.queue() == markers
    client.empty_turns()
    report["uncertain_recheck_readonly"] = True

    report["phase"] = "cleanup_test_markers"
    for receipt in markers:
        assert client.request("thread/queue/delete", {"threadId": client.thread_id, "queuedSubmissionId": receipt})["deleted"] is True
    assert not client.queue()
    client.empty_turns()
    with store.connection() as db:
        assert db.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM sessions").fetchone()[0] == 0
    report.update(test_queue_empty=True, native_turns_empty=True, bridge_tasks_created=False, worker_started=False)


def worker(root):
    from codex_async.app_server import CodexAppServer

    report = {"passed": False, "rpc": [], "cleanup_calls": [], "phase": "initialize", "synthetic_bridge_events_created": 0,
              "production_helper_main_tested": False, "shared_daemon_used": False, "credentials_copied": False}
    native = CodexAppServer(root / "cwd", timeout=12, sandbox=None, daemon=False, start_daemon=False)
    try:
        assert Path(os.environ["CODEX_HOME"]).resolve() == (root / "native-home").resolve()
        assert not list((root / "native-home").iterdir())
        with native:
            assert native.process is not None and native.connection is None
            report["native_pid"] = native.process.pid
            assert os.getpgid(native.process.pid) == os.getpgrp()
            print(json.dumps({"native_pid": native.process.pid}), flush=True)
            asyncio.run(check(native, root, report))
            report["passed"] = True
    except Exception as error:
        report.update(describe_error(error))
        report["authentication_required"] = any(word in str(error).lower() for word in ("unauth", "authenticat", "log in", "login", "sign in", "api key"))
    finally:
        native.close()
        if native.process is not None:
            report["native_pid"] = native.process.pid
            report["native_exited"] = native.process.poll() is not None
    print(json.dumps(report), flush=True)
    return 0 if report["passed"] else 1


def main(args):
    os.umask(0o077)
    if args.worker_root:
        return worker(Path(args.worker_root))
    report_path = Path(args.report).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    binary = Path(args.codex_executable).expanduser().resolve()
    assert binary.name == "codex" and os.access(binary, os.X_OK)
    with binary.open("rb") as file:
        assert file.read(2) != b"#!", "Use the Native binary rather than a proxy/authentication shell wrapper"
    with tempfile.TemporaryDirectory(prefix="private-native-retract-", dir=report_path.parent) as temp:
        root = Path(temp)
        for name in ("cwd", "native-home"):
            (root / name).mkdir(mode=0o700)
        env = {"HOME": str(Path.home()), "PATH": str(binary.parent) + os.pathsep + os.defpath,
               "CODEX_HOME": str(root / "native-home"), "LANG": "C.UTF-8",
               "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
        child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--worker-root", str(root)],
                                 cwd=root / "cwd", env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 text=True, start_new_session=True)
        timed_out = False
        try:
            stdout, stderr = child.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(child.pid, signal.SIGTERM)
            try:
                stdout, stderr = child.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                stdout, stderr = child.communicate(timeout=5)
        finally:
            # This session was created for the check; never leave its Native descendant behind on timeout.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        lines = [json.loads(line) for line in stdout.splitlines() if line.startswith("{")]
        report = next((item for item in reversed(lines) if "passed" in item), {"passed": False})
        if lines and "native_pid" in lines[0]:
            report.setdefault("native_pid", lines[0]["native_pid"])
        report.update(timed_out=timed_out, worker_exited=child.poll() is not None, worker_stderr_empty=not stderr)
        report["isolated_native_pass"] = report["passed"]
        if timed_out and report.get("native_pid"):
            status = Path(f"/proc/{report['native_pid']}/stat")
            report["native_exited"] = not status.exists() or status.read_text().split(") ", 1)[1].startswith("Z ")
        if not timed_out:
            assert report.get("native_exited", True), "Private Native did not exit"
        report_path.write_text(json.dumps(report, indent=2) + "\n")
    report["temporary_state_removed"] = True
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"report": str(report_path), "passed": report["passed"], "phase": report.get("phase"),
                      "native_exited": report.get("native_exited"), "timed_out": timed_out,
                      "isolated_native_pass": report["isolated_native_pass"]}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex-executable", help="Absolute path to the installed Native codex binary")
    parser.add_argument("--report", help="Private JSON result path; Native state and contents are not retained")
    parser.add_argument("--worker-root", help=argparse.SUPPRESS)
    options = parser.parse_args()
    if not options.worker_root and (not options.codex_executable or not options.report):
        parser.error("--codex-executable and --report are required")
    raise SystemExit(main(options))
