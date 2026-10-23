import asyncio
import json
import os
import shutil
import signal
import sys
import tempfile
import unittest
from pathlib import Path

from agent_bridge.broker import request
from agent_bridge.worker import owned_processes, process_identity, process_scope


class ProcessScopeTests(unittest.TestCase):
    def test_process_inventory_checks_nested_groups_and_preserves_unknown_external_jobs(self):
        self.assertTrue(Path(process_scope(os.getpid())).is_dir())
        self.assertEqual("unknown", owned_processes(None)["status"])
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            child = root / "child"
            child.mkdir()
            (root / "cgroup.procs").write_text("")
            (child / "cgroup.procs").write_text(str(os.getpid()))
            report = owned_processes(root)
            self.assertEqual("running", report["status"])
            self.assertEqual(os.getpid(), report["processes"][0]["pid"])
            self.assertEqual("unknown", report["external_jobs"])
            (child / "cgroup.procs").write_text("")
            self.assertEqual("stopped", owned_processes(root)["status"])
            (root / "cgroup.procs").unlink()
            self.assertEqual("unknown", owned_processes(root)["status"])
        self.assertEqual("stopped", owned_processes(root)["status"])


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name)
        fake = self.state / "fake-codex"
        shutil.copyfile(Path(__file__).parent / "fixtures/fake_codex.py", fake)
        fake.chmod(0o700)
        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": str(fake)}},
                    "worker_launcher": "detached",
                    "telegram_database": None,
                }
            )
        )
        self.workers = []
        self.process = None
        self.addAsyncCleanup(self.cleanup)
        await self.start_broker()

    async def start_broker(self):
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "agent_bridge.cli",
            "--state",
            str(self.state),
            "serve",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        for _ in range(100):
            if (self.state / "broker.sock").exists():
                return
            await asyncio.sleep(0.05)
        self.fail("broker did not start")

    async def cleanup(self):
        for pid, start in self.workers:
            if process_identity(pid) == start:
                os.kill(pid, signal.SIGTERM)
        for _ in range(60):
            if all(process_identity(pid) != start for pid, start in self.workers):
                break
            await asyncio.sleep(0.1)
        if self.process and self.process.returncode is None:
            self.process.terminate()
            await self.process.wait()
        self.temp.cleanup()

    async def wait_state(self, tid, states):
        for _ in range(100):
            task = await request(self.state, "agent_result", {"task_id": tid})
            if task["status"] in states:
                return task
            await asyncio.sleep(0.05)
        self.fail(str(task))

    async def test_broker_and_client_disconnect_leave_native_worker_running(self):
        opened = await request(
            self.state, "agent_open", {"profile": "fixture", "cwd": str(self.state), "request_id": "open"}
        )
        sid = opened["session_id"]
        dispatched = await request(
            self.state, "agent_dispatch", {"session_id": sid, "instruction": "fixture", "request_id": "dispatch"}
        )
        tid = dispatched["task_id"]
        await self.wait_state(tid, {"running"})
        worker = await request(self.state, "agent_inspect", {"session_id": sid})
        self.workers.append((worker["worker_pid"], worker["worker_start"]))
        self.process.terminate()
        await self.process.wait()
        self.assertEqual(worker["worker_start"], process_identity(worker["worker_pid"]))
        await asyncio.sleep(2.2)
        await self.start_broker()
        task = await self.wait_state(tid, {"completed"})
        after = await request(self.state, "agent_inspect", {"session_id": sid})
        self.assertEqual(worker["worker_pid"], after["worker_pid"])
        self.assertEqual(1, len(task["attempts"]))
        self.assertEqual(1, len((self.state / "invocations.txt").read_text().splitlines()))
        self.assertEqual("fixture complete", task["result"]["answer"])

    async def test_dead_worker_recovery_does_not_send_prompt_again(self):
        opened = await request(
            self.state, "agent_open", {"profile": "fixture", "cwd": str(self.state), "request_id": "open"}
        )
        sid = opened["session_id"]
        task = await request(
            self.state, "agent_dispatch", {"session_id": sid, "instruction": "fixture", "request_id": "dispatch"}
        )
        await self.wait_state(task["task_id"], {"running"})
        for _ in range(50):
            if (self.state / "invocations.txt").exists():
                break
            await asyncio.sleep(0.02)
        worker = await request(self.state, "agent_inspect", {"session_id": sid})
        os.kill(worker["worker_pid"], signal.SIGKILL)
        result = await self.wait_state(task["task_id"], {"recovery_needed"})
        await asyncio.sleep(0.4)
        self.assertEqual(1, len(result["attempts"]))
        self.assertEqual(1, len((self.state / "invocations.txt").read_text().splitlines()))
