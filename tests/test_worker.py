import asyncio
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.runtimes import PiRuntime
from agent_bridge.store import Store
from agent_bridge.worker import Worker
from agent_bridge.workspace import directory_copy


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.profile = Profile("fixture", "codex", "fixed", executable=sys.executable)
        self.config = Config({"fixture": self.profile}, worker_launcher="detached", interrupt_timeout=.1)

    def session(self, access="read-only"):
        return self.store.open(self.profile, str(self.root), access, "open")["session_id"]

    async def run_worker(self, sid, *, slow_close=False):
        worker = Worker(self.root, self.config, sid)
        begin = worker.store.begin_attempt
        owners = []

        def begin_with_assignment(*args):
            worker.store.claim(self.config)
            return begin(*args)

        async def connect():
            owners.append(asyncio.current_task())

        async def run(instruction, on_started, on_event):
            await on_started("turn")
            (self.root / "output.txt").write_text("retained change")
            worker.shutdown = True
            return "completed", {"answer": "done", "native_usage_total": {"totalTokens": 150}}, {"total_tokens": 30, "complete": True}

        async def close():
            owners.append(asyncio.current_task())
            if slow_close:
                await asyncio.sleep(60)

        agent = SimpleNamespace(native_id="native", connect=connect, run=run, close=close)
        with patch("agent_bridge.worker.runtime", return_value=agent) as factory, patch.object(worker.store, "begin_attempt", begin_with_assignment):
            await asyncio.wait_for(worker.run(), 2)
        self.assertIs(owners[0], owners[-1])
        return factory.call_args.args[1]

    async def test_patch_capture_failure_is_failed_and_preserves_result_usage_and_files(self):
        sid = self.session("workspace-write")
        tid = self.store.dispatch(sid, "task", "dispatch")["task_id"]
        # This is intentionally not a Git directory; exercise the real capture failure.
        await self.run_worker(sid)
        task = self.store.get_task(tid)
        self.assertEqual("failed", task["status"])
        self.assertIn("Patch capture failed", task["error"])
        self.assertTrue(task["result"]["files"]["error"])
        self.assertEqual("done", task["result"]["answer"])
        self.assertEqual(30, task["attempts"][0]["usage"]["total_tokens"])
        self.assertEqual("retained change", (self.root / "output.txt").read_text())

    async def test_latest_crashed_attempt_does_not_restore_an_older_success_counter(self):
        sid = self.session()
        self.store.worker_ready(sid, "old", 123, "fixture", "native")
        for index in range(2):
            tid = self.store.dispatch(sid, "task", str(index))["task_id"]
            aid = self.store.claim(self.config)["attempt_id"]
            self.store.begin_attempt(sid, "old")
            if index == 0:
                self.store.finish(sid, "old", tid, aid, "completed", {"answer": "done", "native_usage_total": {"totalTokens": 100}})
        self.store.worker_stopped(sid, "old", "crashed")
        self.store.resolve(sid, tid, self.store.get_session(sid)["revision"], "resolve", "failed", "fixture inspected")
        self.store.control(sid, "resume", self.store.get_session(sid)["revision"], "resume")
        self.store.dispatch(sid, "new task", "new")
        loaded = await self.run_worker(sid)
        self.assertTrue(loaded["has_native_history"])
        self.assertIsNone(loaded["native_usage_total"])

    async def test_native_cleanup_has_a_deadline_and_does_not_change_completed_task(self):
        sid = self.session()
        tid = self.store.dispatch(sid, "task", "dispatch")["task_id"]
        await self.run_worker(sid, slow_close=True)
        self.assertEqual("completed", self.store.get_task(tid)["status"])
        session = self.store.get_session(sid)
        self.assertIsNone(session["worker_pid"])
        self.assertIn("Native cleanup failed", session["error"])
        self.assertIn("TimeoutError", session["error"])

    async def check_connect_failure(self, error):
        self.store.parent_control("parent", "parent_register", 0, "register", "parent-native", str(self.root))
        sid = self.store.open(self.profile, str(self.root), "read-only", "open", "parent")["session_id"]
        tid = self.store.dispatch(sid, "task", "dispatch")["task_id"]
        worker = Worker(self.root, self.config, sid)
        agent = SimpleNamespace(connect=AsyncMock(side_effect=error), close=AsyncMock())
        with patch("agent_bridge.worker.runtime", return_value=agent):
            await worker.run()
        task = self.store.get_task(tid)
        self.assertEqual("queued", task["status"])
        self.assertEqual([], task["attempts"])
        self.assertEqual("recovery_needed", self.store.get_session(sid)["observed"])
        self.assertTrue(self.store.get_session(sid)["error"])
        self.store.worker_stopped(sid, worker.generation, "CLI unavailable")
        self.store.worker_start_failed(sid, "CLI unavailable")
        events = [e for e in self.store.events(task_id=tid) if e["kind"] == "task_start_failed"]
        self.assertEqual(1, len(events))
        self.assertEqual("queued", events[0]["data"]["status"])
        self.assertEqual([events[0]["seq"]], [d["event_seq"] for d in self.store.get_parent("parent")["deliveries"]])
        self.assertIsNone(self.store.claim(self.config))

    async def test_connect_failure_notifies_parent_and_keeps_unstarted_work_queued(self):
        await self.check_connect_failure(ConnectionError("CLI unavailable"))

    async def test_empty_timeout_error_still_stops_relaunch_and_notifies_parent(self):
        await self.check_connect_failure(TimeoutError())

    async def test_pi_final_statistics_failure_preserves_confirmed_answer_usage_and_files(self):
        source = self.root / "source"
        source.mkdir()
        (source / "file.txt").write_text("input")
        cwd, metadata = directory_copy(self.root, source, "copy", ["file.txt"])
        profile = Profile("pi", "pi", "fixed", provider="fixture", executable=sys.executable)
        config = Config({"pi": profile}, worker_launcher="detached")
        sid = self.store.open(profile, cwd, "workspace-write", "open", workspace=metadata)["session_id"]
        tid = self.store.dispatch(sid, "task", "dispatch")["task_id"]
        worker = Worker(self.root, config, sid)
        agent = PiRuntime(profile, self.store.get_session(sid), self.root, worker.question)
        events = asyncio.Queue()
        stats_calls = 0

        async def request(value):
            nonlocal stats_calls
            if value["type"] == "get_session_stats":
                stats_calls += 1
                if stats_calls == 1:
                    return {"tokens": {"total": 0}, "cost": 0}
                worker.shutdown = True
                raise TimeoutError("Final statistics unavailable")
            (Path(cwd) / "file.txt").write_text("verified change")
            await events.put({"type": "message_end", "message": {"role": "assistant", "model": "fixed",
                "content": [{"type": "text", "text": "verified answer"}], "stopReason": "stop", "usage": {"totalTokens": 12}}})
            await events.put({"type": "agent_settled"})
            return {"disposition": "started"}

        agent.client = SimpleNamespace(events=events, event=events.get, request=request, close=AsyncMock())
        agent.connect = AsyncMock()
        begin = worker.store.begin_attempt

        def assign(*args):
            worker.store.claim(config)
            return begin(*args)

        with patch("agent_bridge.worker.runtime", return_value=agent), patch.object(worker.store, "begin_attempt", assign):
            await asyncio.wait_for(worker.run(), 1)
        task = self.store.get_task(tid)
        self.assertEqual("completed", task["status"])
        self.assertEqual("verified answer", task["result"]["answer"])
        self.assertIn("statistics unavailable", task["result"]["statistics_error"])
        usage = task["attempts"][0]["usage"]
        self.assertEqual(12, usage["total_tokens"])
        self.assertFalse(usage["complete"])
        self.assertIsNone(usage["catalog_estimate"]["usd"])
        files = task["result"]["files"]
        self.assertEqual(["file.txt"], files["changed_files"])
        self.assertIn("file.txt", json.loads(Path(files["changes"]).read_text()))
        self.assertEqual("input", (source / "file.txt").read_text())

    async def check_late_interrupt(self, failure=None):
        sid = self.session()
        first = self.store.dispatch(sid, "first", "first")["task_id"]
        second = self.store.dispatch(sid, "second", "second")["task_id"]
        worker = Worker(self.root, self.config, sid)
        begin = worker.store.begin_attempt
        interrupted = asyncio.Event()
        interrupt_finished = False

        def assign(*args):
            worker.store.claim(self.config)
            return begin(*args)

        async def run(instruction, on_started, on_event):
            await on_started(instruction)
            if instruction == "first":
                self.store.cancel(first, "cancel-first")
                await interrupted.wait()
                return "interrupted", {"answer": "A stopped"}, {}
            self.assertTrue(interrupt_finished, "A's untargeted abort must finish before B starts")
            await asyncio.sleep(.35)  # Cross A's old interrupt deadline and the monitor's next poll.
            worker.shutdown = True
            return "completed", {"answer": "healthy B"}, {}

        async def interrupt():
            nonlocal interrupt_finished
            interrupted.set()
            await asyncio.sleep(.03 if failure != "timeout" else 60)
            worker.store.claim(self.config)
            interrupt_finished = True
            if failure:
                raise BridgeError("native_error", "Old turn already completed")

        agent = SimpleNamespace(native_id="native", connect=AsyncMock(), run=run, interrupt=interrupt, close=AsyncMock())
        with patch("agent_bridge.worker.runtime", return_value=agent), patch.object(worker.store, "begin_attempt", assign):
            await asyncio.wait_for(worker.run(), 2)
        self.assertEqual("cancelled", self.store.get_task(first)["status"])
        later = self.store.get_task(second)
        self.assertEqual(0, later["cancel_requested"])
        if failure:
            self.assertEqual("queued", later["status"])
            self.assertTrue(all(attempt["started"] is None for attempt in later["attempts"]))
            self.assertEqual("recovery_needed", self.store.get_session(sid)["observed"])
        else:
            self.assertEqual("completed", later["status"])
            self.assertEqual("healthy B", later["result"]["answer"])

    async def test_next_task_waits_for_previous_interrupt_and_has_its_own_deadline(self):
        await self.check_late_interrupt()

    async def test_late_interrupt_error_cannot_start_or_cancel_next_task(self):
        await self.check_late_interrupt("error")

    async def test_late_interrupt_timeout_leaves_next_task_unstarted(self):
        await self.check_late_interrupt("timeout")

    async def test_explicit_cancel_and_close_take_priority_over_timeout_and_budget(self):
        for action in ("cancel", "close"):
            with self.subTest(action=action):
                sid = self.store.open(self.profile, str(self.root), "read-only", "open-" + action)["session_id"]
                self.store.worker_ready(sid, "g", 1, "fixture", "native-" + action)
                tid = self.store.dispatch(sid, "task", "dispatch-" + action)["task_id"]
                self.store.claim(self.config)
                worker = Worker(self.root, self.config, sid)
                worker.task = self.store.begin_attempt(sid, "g")
                if action == "cancel":
                    self.store.cancel(tid, "cancel")
                else:
                    self.store.control(sid, "close", self.store.get_session(sid)["revision"], "close")
                worker.started = time.monotonic() - self.config.task_timeout - 1
                worker.config.token_budget = 0
                worker.native_accepted.set()
                triggered = asyncio.Event()
                worker.agent = SimpleNamespace(interrupt=AsyncMock(side_effect=triggered.set))
                monitor = asyncio.create_task(worker.watch_control(SimpleNamespace(cancel=lambda: None)))
                try:
                    await asyncio.wait_for(triggered.wait(), 1)
                    self.assertEqual("cancelled", worker.stop_reason)
                finally:
                    monitor.cancel()
                    await asyncio.gather(monitor, return_exceptions=True)
                    worker.config.token_budget = None
