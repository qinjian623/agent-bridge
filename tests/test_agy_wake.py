import fcntl
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from agent_bridge import agy_wake

FAKE_AGY = r'''
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

args = sys.argv[1:]
native_id = args[args.index("--conversation") + 1]
assert args[args.index("--input-format") + 1] == "stream-json"
assert args[args.index("--output-format") + 1] == "stream-json"
assert "--model" not in args and "--effort" not in args
assert "--sandbox" not in args and "--mode" not in args
assert "--dangerously-skip-permissions" not in args
assert not any(k in os.environ for k in ("CODEX_THREAD_ID", "AGENT_BRIDGE_ROLE", "AGENT_BRIDGE_TOKEN"))
scenario = os.environ.get("FAKE_AGY_SCENARIO", "success")
record = Path(os.environ["FAKE_AGY_RECORD"])

def save(value):
    with record.open("a") as stream:
        stream.write(json.dumps(value) + "\n")

def emit(value):
    print(json.dumps(value), flush=True)

save({"phase": "started", "native_id": native_id, "pid": os.getpid(), "args": args})
emit({"event": "init", "conversation_id": "wrong-session" if scenario == "wrong_init" else native_id})
raw = sys.stdin.readline()
if not raw:
    raise SystemExit(0)
message = json.loads(raw)
assert message["event"] == "user"
save({"phase": "input", "native_id": native_id, "content": message["message"]["content"]})
if scenario == "invalid_json":
    print("not json", flush=True)
    raise SystemExit(0)
if scenario not in ("no_ack", "exit_without_ack"):
    index = 10 if scenario == "replay" else 11
    if index > 10:
        home = Path(os.environ["FAKE_AGY_NATIVE_HOME"])
        with sqlite3.connect(home / "conversations" / (native_id + ".db")) as db:
            db.execute("INSERT INTO steps(idx) VALUES(?)", (index,))
    emit({"event": "step_update", "step_update": {
        "conversation_id": "wrong-session" if scenario == "wrong_step" else native_id,
        "step_type": "user_input", "state": "DONE", "step_index": index,
    }})
if scenario == "slow":
    release = Path(os.environ["FAKE_AGY_RELEASE"])
    deadline = time.monotonic() + 3
    while not release.exists():
        if time.monotonic() >= deadline:
            raise SystemExit(9)
        time.sleep(.01)
if scenario == "permission_denied":
    print("Native permission denied", file=sys.stderr, flush=True)
if scenario == "orphan":
    import subprocess
    ready = record.with_suffix(".child-ready")
    child = subprocess.Popen([sys.executable, "-c",
        "import signal,time;from pathlib import Path;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        "Path(" + repr(str(ready)) + ").touch();time.sleep(60)"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    save({"phase": "descendant", "pid": child.pid})
    deadline = time.monotonic() + 2
    while not ready.exists():
        if time.monotonic() >= deadline:
            raise SystemExit(9)
        time.sleep(.01)
if scenario != "exit_without_ack":
    emit({"event": "result", "result": {
        "conversation_id": native_id,
        "status": "ERROR" if scenario == "error_result" else "SUCCESS",
        "response": "failed" if scenario == "error_result" else "done",
    }})
'''


class AgyWakeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.native_id = str(uuid.uuid4())
        self.home = self.root / "native"
        (self.home / "conversations").mkdir(parents=True)
        self.conversation = self.home / "conversations" / (self.native_id + ".db")
        with sqlite3.connect(self.conversation) as db:
            db.execute("CREATE TABLE steps(idx INTEGER PRIMARY KEY)")
            db.execute("INSERT INTO steps(idx) VALUES(10)")
        with sqlite3.connect(self.home / "conversation_summaries.db") as db:
            db.execute("CREATE TABLE conversation_summaries(conversation_id TEXT PRIMARY KEY,nesting_depth INTEGER)")
            db.execute("INSERT INTO conversation_summaries VALUES(?,0)", (self.native_id,))
        self.executable = self.root / "fake-agy"
        self.executable.write_text("#!" + sys.executable + "\n" + FAKE_AGY)
        self.executable.chmod(0o700)
        self.record = self.root / "calls.jsonl"
        self.release = self.root / "release"
        self.job = {
            "id": str(uuid.uuid4()), "runtime": "antigravity", "native_id": self.native_id,
            "cwd": str(self.root), "group_id": "nu-test", "message": "only this native session",
            "telegram_database": None, "async_state": str(self.root / "async"),
            "native_home": str(self.home), "executable": str(self.executable), "timeout": 5,
        }
        self.environment = patch.dict(os.environ, {
            "FAKE_AGY_RECORD": str(self.record), "FAKE_AGY_NATIVE_HOME": str(self.home),
            "FAKE_AGY_RELEASE": str(self.release), "FAKE_AGY_SCENARIO": "success",
            "CODEX_THREAD_ID": "inherited-thread", "AGENT_BRIDGE_ROLE": "child", "AGENT_BRIDGE_TOKEN": "not-for-native",
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.presence = patch.object(agy_wake, "inactive_reason", return_value=None)
        self.inactive = self.presence.start()
        self.addCleanup(self.presence.stop)
        self.receipts = []

    def calls(self):
        return [json.loads(line) for line in self.record.read_text().splitlines()] if self.record.exists() else []

    def wake(self, scenario="success", emit=None):
        with patch.dict(os.environ, {"FAKE_AGY_SCENARIO": scenario}):
            return agy_wake.wake(self.job, emit or self.receipts.append)

    def test_restores_original_id_preserves_model_and_confirms_new_input(self):
        result = self.wake()
        self.assertEqual("received", result["status"])
        self.assertEqual("completed", result["processing_status"])
        self.assertEqual(self.job["id"], result["receipt"])
        self.assertEqual("11", result["turn_id"])
        self.assertEqual(["running"], [r["processing_status"] for r in self.receipts])
        calls = self.calls()
        self.assertEqual(["started", "input"], [call["phase"] for call in calls])
        self.assertEqual({self.native_id}, {call["native_id"] for call in calls})
        self.assertIn("delivery=" + self.job["id"], calls[1]["content"])
        self.assertIn(self.job["message"], calls[1]["content"])
        with sqlite3.connect(self.conversation) as db:
            self.assertEqual(11, db.execute("SELECT MAX(idx) FROM steps").fetchone()[0])
        self.assertFalse(Path("/proc", str(calls[0]["pid"])).exists())

    def test_existing_active_or_unknown_native_owner_never_starts_cli(self):
        self.inactive.return_value = "Original session is active or ownership cannot be verified"
        result = self.wake()
        self.assertEqual("deferred", result["status"])
        self.assertEqual([], self.calls())
        self.assertEqual([], self.receipts)

    def test_shared_lock_busy_does_not_launch_a_second_bridge_or_telegram_worker(self):
        locks = Path(self.job["async_state"]) / "antigravity-locks"
        locks.mkdir(parents=True)
        with (locks / (self.native_id + ".lock")).open("a") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.wake()
        self.assertEqual("deferred", result["status"])
        self.assertEqual([], self.calls())
        self.inactive.assert_not_called()

    def test_native_appearing_after_init_or_immediately_before_input_prevents_write(self):
        for checks in ([None, "another client appeared"], [None, None, "another client appeared"]):
            with self.subTest(checks=checks):
                self.inactive.side_effect = checks
                result = self.wake()
                self.assertEqual("deferred", result["status"])
                calls = self.calls()
                self.assertTrue(all(call["phase"] == "started" for call in calls))
                self.assertTrue(all(not Path("/proc", str(call["pid"])).exists() for call in calls))
                self.record.unlink()

    def test_missing_native_records_fail_without_creating_a_replacement(self):
        self.conversation.unlink()
        result = self.wake()
        self.assertEqual("failed", result["status"])
        self.assertEqual([], self.calls())
        self.assertFalse(self.conversation.exists())

    def test_subagent_record_cannot_be_restored_as_original_main_session(self):
        with sqlite3.connect(self.home / "conversation_summaries.db") as db:
            db.execute("UPDATE conversation_summaries SET nesting_depth=1")
        result = self.wake()
        self.assertEqual("failed", result["status"])
        self.assertEqual([], self.calls())

    def test_wrong_init_id_fails_before_any_input_and_cleans_up_own_process(self):
        result = self.wake("wrong_init")
        self.assertEqual("failed", result["status"])
        self.assertEqual(["started"], [call["phase"] for call in self.calls()])
        self.assertFalse(Path("/proc", str(self.calls()[0]["pid"])).exists())

    def test_no_ack_replayed_step_and_invalid_protocol_remain_uncertain(self):
        for scenario in ("no_ack", "replay", "invalid_json", "exit_without_ack", "wrong_step"):
            with self.subTest(scenario=scenario):
                result = self.wake(scenario)
                self.assertEqual("uncertain", result["status"])
                self.assertEqual([], self.receipts)
                self.assertFalse(Path("/proc", str(self.calls()[-2]["pid"])).exists())

    def test_receipt_is_emitted_before_long_processing_and_blocks_second_delivery(self):
        attempts = []

        def received(receipt):
            self.receipts.append(receipt)
            self.assertEqual("running", receipt["processing_status"])
            self.assertTrue(Path("/proc", str(self.calls()[0]["pid"])).exists())
            other_job = {**self.job, "id": str(uuid.uuid4()), "group_id": "another-group"}
            attempts.append(agy_wake.wake(other_job, self.fail))
            self.release.touch()

        result = self.wake("slow", received)
        self.assertEqual("completed", result["processing_status"])
        self.assertEqual("deferred", attempts[0]["status"])
        self.assertEqual(1, sum(call["phase"] == "started" for call in self.calls()))
        self.assertEqual(1, len(self.receipts))

    def test_failed_processing_or_permission_denial_does_not_erase_receipt(self):
        for scenario in ("error_result", "permission_denied"):
            with self.subTest(scenario=scenario):
                with sqlite3.connect(self.conversation) as db:
                    db.execute("DELETE FROM steps WHERE idx>10")
                self.receipts.clear()
                result = self.wake(scenario)
                self.assertEqual("received", result["status"])
                self.assertEqual("failed", result["processing_status"])
                self.assertEqual(1, len(self.receipts))
                self.assertEqual(self.job["id"], result["receipt"])

    def test_success_cleanup_stops_own_descendant_even_if_it_ignores_sigterm(self):
        result = self.wake("orphan")
        self.assertEqual("completed", result["processing_status"])
        descendant = next(call["pid"] for call in self.calls() if call["phase"] == "descendant")

        def child_is_live():
            try:
                state = Path("/proc", str(descendant), "stat").read_text().split(") ", 1)[1].split()[0]
                return state != "Z"
            except FileNotFoundError:
                return False

        if child_is_live():
            self.addCleanup(subprocess.run, ["kill", "-KILL", str(descendant)], check=False, capture_output=True)
        deadline = time.monotonic() + 1
        while child_is_live() and time.monotonic() < deadline:
            time.sleep(.005)
        self.assertFalse(child_is_live())


if __name__ == "__main__":
    unittest.main()
