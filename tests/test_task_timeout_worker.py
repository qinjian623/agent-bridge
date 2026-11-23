import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from agent_bridge.config import Config, Profile
from agent_bridge.store import Store
from agent_bridge.worker import Worker
from agent_bridge.workspace import directory_copy, git, worktree


class TaskTimeoutWorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.profile = Profile("fixture", "codex", "model", executable=sys.executable)
        self.config = Config({"fixture": self.profile}, worker_launcher="detached", task_timeout=.02, interrupt_timeout=.2)
        self.store = Store(self.root)

    async def execute(self, worker, agent):
        begin = worker.store.begin_attempt
        def assigned(*args, **kwargs):
            worker.store.claim(self.config)
            return begin(*args, **kwargs)
        with patch("agent_bridge.worker.runtime", return_value=agent), patch.object(worker.store, "begin_attempt", assigned):
            await asyncio.wait_for(worker.run(), 3)

    async def test_override_allows_longer_task_then_next_task_uses_default_again(self):
        sid = self.store.open(self.profile, str(self.root), "read-only", "open")["session_id"]
        first = self.store.dispatch(sid, "long", "long", details={"task_timeout": 1})["task_id"]
        second = self.store.dispatch(sid, "default", "default")["task_id"]
        worker = Worker(self.root, self.config, sid)
        interrupted = asyncio.Event()
        observed = []
        async def run(instruction, on_started, on_event):
            await on_started(instruction)
            observed.append(worker.task_timeout)
            if instruction == "long":
                await asyncio.sleep(.25)
                self.assertFalse(interrupted.is_set(), "Default timeout must not terminate the overridden task")
                return "completed", {"answer": "long task completed"}, {"total_tokens": 1, "complete": True}
            await interrupted.wait()
            worker.shutdown = True
            return "interrupted", {"answer": "saved default work", "error": "This operation was aborted"}, {}
        agent = SimpleNamespace(native_id="native", connect=AsyncMock(), run=run,
                                interrupt=AsyncMock(side_effect=interrupted.set), close=AsyncMock())
        await self.execute(worker, agent)
        self.assertEqual([1, .02], observed)
        self.assertEqual(.02, self.config.task_timeout)
        self.assertEqual("completed", self.store.get_task(first)["status"])
        later = self.store.get_task(second)
        self.assertEqual("interrupted", later["status"])
        self.assertEqual(.02, later["result"]["termination"]["task_timeout_seconds"])
        agent.interrupt.assert_awaited_once()

    async def test_started_attempt_freezes_override_instead_of_rereading_task_metadata(self):
        sid = self.store.open(self.profile, str(self.root), "read-only", "open")["session_id"]
        tid = self.store.dispatch(sid, "long", "long", details={"task_timeout": 1})["task_id"]
        worker = Worker(self.root, self.config, sid)
        async def run(instruction, on_started, on_event):
            await on_started("turn")
            with self.store.connection(write=True) as db:
                db.execute("UPDATE tasks SET details=? WHERE id=?", (json.dumps({"task_timeout": .001}), tid))
            await asyncio.sleep(.25)
            worker.shutdown = True
            return "completed", {"answer": "completed under the original deadline"}, {}
        agent = SimpleNamespace(native_id="native", connect=AsyncMock(), run=run, interrupt=AsyncMock(), close=AsyncMock())
        await self.execute(worker, agent)
        self.assertEqual(1, worker.task_timeout)
        self.assertEqual("completed", self.store.get_task(tid)["status"])
        agent.interrupt.assert_not_awaited()

    async def test_approval_expiration_uses_the_attempt_override(self):
        worker = Worker(self.root, self.config, "fixture-session")
        worker.task = {"id": "task", "active_attempt": "attempt"}
        worker.started = time.monotonic()
        worker.task_timeout = 20
        with patch.object(worker.store, "question", return_value="question") as question, patch.object(
            worker.store, "get_question", return_value={"status": "answered", "answer": {"ok": True}},
        ):
            before = time.time()
            self.assertEqual({"ok": True}, await worker.question({"type": "permission"}))
        expiration = question.call_args.args[-1]
        self.assertGreater(expiration - before, 19)
        self.assertLessEqual(expiration - before, 20.1)
        self.assertEqual(.02, self.config.task_timeout)

    async def test_non_utf8_git_output_survives_timeout_collection_and_sqlite_result(self):
        source = self.root / "source"
        source.mkdir()
        git(source, "init", "-q")
        git(source, "config", "user.name", "Test")
        git(source, "config", "user.email", "test@example.invalid")
        (source / "input.txt").write_text("input")
        git(source, "add", "input.txt")
        git(source, "commit", "-qm", "initial")
        cwd, metadata = worktree(self.root, source, "raw-name")
        sid = self.store.open(self.profile, cwd, "workspace-write", "raw-session", workspace=metadata)["session_id"]
        tid = self.store.dispatch(sid, "work", "raw-task")["task_id"]
        worker = Worker(self.root, self.config, sid)
        stopped = asyncio.Event()
        raw_name = b"result-\xff.txt"
        raw_path = os.fsencode(cwd) + b"/" + raw_name
        async def run(instruction, on_started, on_event):
            await on_started("turn")
            with open(raw_path, "wb") as stream:
                stream.write(b"retained work\n")
            await stopped.wait()
            worker.shutdown = True
            return "interrupted", {"answer": "work preserved", "error": "aborted"}, {"total_tokens": 3, "complete": True}
        agent = SimpleNamespace(native_id="native", connect=AsyncMock(), run=run,
                                interrupt=AsyncMock(side_effect=stopped.set), close=AsyncMock())
        await self.execute(worker, agent)
        task = self.store.get_task(tid)
        self.assertEqual("interrupted", task["status"])
        self.assertIn("Bridge task_timeout", task["error"])
        self.assertEqual("work preserved", task["result"]["answer"])
        files = task["result"]["files"]
        self.assertNotIn("error", files)
        self.assertEqual([raw_name.hex()], files["untracked_files_raw_hex"])
        self.assertIn("\\udcff", files["untracked_files"][0])
        json.dumps(task["result"], ensure_ascii=False).encode("utf-8")
        self.assertIn(b"retained work", Path(files["patch"]).read_bytes())
        self.assertIn(raw_name, os.listdir(os.fsencode(cwd)))
        with open(raw_path, "rb") as stream:
            self.assertEqual(b"retained work\n", stream.read())
        self.assertEqual([b".git", b"input.txt"], sorted(os.listdir(os.fsencode(source))))

    async def test_collection_failure_keeps_known_native_result_and_timeout_context(self):
        for native_status in ("completed", "interrupted"):
            with self.subTest(native_status=native_status):
                source = self.root / ("source-" + native_status)
                source.mkdir()
                (source / "file.txt").write_text("input")
                cwd, metadata = directory_copy(self.root, source, native_status, ["file.txt"])
                sid = self.store.open(self.profile, cwd, "workspace-write", native_status, workspace=metadata)["session_id"]
                tid = self.store.dispatch(sid, "work", "dispatch-" + native_status)["task_id"]
                worker = Worker(self.root, self.config, sid)
                interrupted = asyncio.Event()
                async def run(instruction, on_started, on_event, current=worker, stopped=interrupted, status=native_status):
                    await on_started("turn")
                    await stopped.wait()
                    current.shutdown = True
                    return status, {"answer": "preserved native answer", "error": None if status == "completed" else "aborted"}, {"total_tokens": 9, "complete": True}
                agent = SimpleNamespace(native_id="native", connect=AsyncMock(), run=run,
                                        interrupt=AsyncMock(side_effect=interrupted.set), close=AsyncMock())
                error = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")
                with patch("agent_bridge.worker.capture_patch", side_effect=error):
                    await self.execute(worker, agent)
                task = self.store.get_task(tid)
                self.assertEqual("failed" if native_status == "completed" else "interrupted", task["status"])
                self.assertEqual("preserved native answer", task["result"]["answer"])
                self.assertEqual("task_timeout", task["result"]["termination"]["cause"])
                self.assertIn("utf-8", task["result"]["files"]["error"])
                self.assertEqual(9, task["attempts"][0]["usage"]["total_tokens"])
                if native_status == "interrupted":
                    self.assertIn("Bridge task_timeout", task["error"])
                    self.assertEqual("aborted", task["result"]["native_error"])


if __name__ == "__main__":
    unittest.main()
