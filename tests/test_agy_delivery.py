import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from agent_bridge import groups, native_broadcast
from agent_bridge.broker import Broker
from agent_bridge.config import Config
from agent_bridge.store import Store

FAKE_HELPER = r'''
import json
import os
import sys
import time
from pathlib import Path

row = json.load(sys.stdin)
mode = os.environ.get("FAKE_AGY_DRIVER_MODE", "slow_completed")
record = Path(os.environ["FAKE_AGY_DRIVER_RECORD"])
with record.open("a") as stream:
    stream.write(json.dumps({"id": row["id"], "native_id": row["native_id"], "pid": os.getpid()}) + "\n")
reply = {"status": "received", "receipt": row["id"], "turn_id": "11", "processing_status": "running"}
if mode == "wrong_receipt":
    reply["receipt"] = "not-this-delivery"
if mode == "wrong_turn":
    reply["turn_id"] = "not-a-native-step"
if mode == "integer_turn":
    reply["turn_id"] = 11
print(json.dumps(reply), flush=True)
if mode in {"wrong_receipt", "wrong_turn", "integer_turn", "missing_final"}:
    raise SystemExit(0)
if mode == "deferred_after_receipt":
    print(json.dumps({"status": "deferred", "error": "unexpected final disposition"}), flush=True)
    raise SystemExit(0)
if mode == "conflicting_final":
    reply.update(turn_id="99", processing_status="completed")
    print(json.dumps(reply), flush=True)
    raise SystemExit(0)
release = Path(os.environ["FAKE_AGY_DRIVER_RELEASE"])
deadline = time.monotonic() + 3
while not release.exists():
    if time.monotonic() >= deadline:
        raise SystemExit(9)
    time.sleep(.01)
reply["processing_status"] = "failed" if mode == "slow_failed" else "completed"
reply["error"] = "processing failed" if mode == "slow_failed" else None
print(json.dumps(reply), flush=True)
'''


class AgyDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.config = Config({}, parallel_limit=2, task_timeout=5)
        self.members = [dict(runtime=runtime, native_id=str(uuid.uuid4()), cwd=str(self.root))
                        for runtime in ("antigravity", "codex")]
        groups.control_group(self.store, "team", "create", 0, "create", native_members=self.members)
        self.script = self.root / "fake_helper.py"
        self.script.write_text(FAKE_HELPER)
        self.record = self.root / "helpers.jsonl"
        self.release = self.root / "release"
        self.environment = patch.dict(os.environ, {
            "FAKE_AGY_DRIVER_RECORD": str(self.record), "FAKE_AGY_DRIVER_RELEASE": str(self.release),
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.real_spawn = asyncio.create_subprocess_exec
        self.processes = []
        self.codex_calls = []

    def rows(self):
        with self.store.connection() as db:
            return [dict(row) for row in db.execute("SELECT * FROM native_broadcast_deliveries ORDER BY created")]

    def agy_row(self, broadcast_id):
        return next(row for row in self.rows() if row["broadcast_id"] == broadcast_id and row["runtime"] == "antigravity")

    def broadcast(self, request_id="send"):
        return groups.broadcast(self.store, "team", "fixture only", 0, request_id)

    async def spawn_fake(self, *args, **kwargs):
        self.assertEqual((sys.executable, "-m", "agent_bridge.agy_wake"), args)
        process = await self.real_spawn(sys.executable, str(self.script), **kwargs)
        self.processes.append(process)
        return process

    async def codex_sender(self, config, row):
        self.assertEqual("codex", row["runtime"])
        self.codex_calls.append(row["native_id"])
        return {"status": "enqueued", "receipt": "codex-" + row["id"]}

    async def wait_until(self, predicate, timeout=2):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("timed out waiting for isolated fake-helper state")
            await asyncio.sleep(.005)

    async def slow_delivery(self, mode):
        sent = self.broadcast()
        tasks = set()
        with patch.dict(os.environ, {"FAKE_AGY_DRIVER_MODE": mode}), \
             patch.object(native_broadcast.asyncio, "create_subprocess_exec", self.spawn_fake), \
             patch.object(native_broadcast, "send", self.codex_sender):
            sender = native_broadcast.send
            try:
                await asyncio.wait_for(native_broadcast.deliver(self.store, self.config, sender, tasks=tasks), .5)
                self.assertFalse(self.release.exists())
                await self.wait_until(lambda: self.agy_row(sent["broadcast_id"])["status"] == "received"
                                      and bool(self.codex_calls))
                agy = self.agy_row(sent["broadcast_id"])
                self.assertEqual("running", agy["processing_status"])
                self.assertEqual(agy["id"], agy["receipt"])
                self.assertEqual("11", agy["native_turn_id"])
                self.assertTrue(tasks)
                self.assertTrue(all(p.returncode is None for p in self.processes))
                self.assertEqual({"received": 1, "enqueued": 1}, native_broadcast.view(self.store, sent["broadcast_id"])["counts"])
                await native_broadcast.deliver(self.store, self.config, sender, tasks=tasks)
                self.assertEqual(sent, self.broadcast())
                self.release.touch()
                await asyncio.wait_for(asyncio.gather(*list(tasks)), 2)
            finally:
                self.release.touch()
                if tasks:
                    await asyncio.wait_for(asyncio.gather(*list(tasks), return_exceptions=True), 2)
        self.assertEqual(1, len(self.record.read_text().splitlines()))
        self.assertEqual([self.members[1]["native_id"]], self.codex_calls)
        return self.agy_row(sent["broadcast_id"])

    async def test_background_deliver_does_not_wait_for_agy_processing_or_block_codex(self):
        row = await self.slow_delivery("slow_completed")
        self.assertEqual("received", row["status"])
        self.assertEqual("completed", row["processing_status"])

    async def test_processing_failure_preserves_early_received_state_without_retry(self):
        row = await self.slow_delivery("slow_failed")
        self.assertEqual("received", row["status"])
        self.assertEqual("failed", row["processing_status"])
        self.assertIn("processing failed", row["error"])

    async def test_wrong_native_receipt_or_turn_is_uncertain_and_never_marked_received(self):
        with patch.object(native_broadcast.asyncio, "create_subprocess_exec", self.spawn_fake), \
             patch.object(native_broadcast, "send", self.codex_sender):
            sender = native_broadcast.send
            for mode in ("wrong_receipt", "wrong_turn", "integer_turn"):
                with self.subTest(mode=mode), patch.dict(os.environ, {"FAKE_AGY_DRIVER_MODE": mode}):
                    sent = self.broadcast(mode)
                    await native_broadcast.deliver(self.store, self.config, sender)
                    row = self.agy_row(sent["broadcast_id"])
                    self.assertEqual("uncertain", row["status"])
                    self.assertIsNone(row["receipt"])
                    self.assertIsNone(row["native_turn_id"])
            await native_broadcast.deliver(self.store, self.config, sender)
        self.assertEqual(3, len(self.record.read_text().splitlines()))

    async def test_helper_exit_without_terminal_processing_result_cannot_stay_running(self):
        with patch.object(native_broadcast.asyncio, "create_subprocess_exec", self.spawn_fake), \
             patch.object(native_broadcast, "send", self.codex_sender):
            sender = native_broadcast.send
            for mode in ("missing_final", "deferred_after_receipt"):
                with self.subTest(mode=mode), patch.dict(os.environ, {"FAKE_AGY_DRIVER_MODE": mode}):
                    sent = self.broadcast(mode)
                    await native_broadcast.deliver(self.store, self.config, sender)
                    row = self.agy_row(sent["broadcast_id"])
                    self.assertEqual("received", row["status"])
                    self.assertEqual("failed", row["processing_status"])
                    self.assertEqual(row["id"], row["receipt"])

    async def test_conflicting_final_native_step_cannot_replace_the_first_receipt(self):
        with patch.object(native_broadcast.asyncio, "create_subprocess_exec", self.spawn_fake), \
             patch.object(native_broadcast, "send", self.codex_sender), \
             patch.dict(os.environ, {"FAKE_AGY_DRIVER_MODE": "conflicting_final"}):
            sender = native_broadcast.send
            sent = self.broadcast()
            await native_broadcast.deliver(self.store, self.config, sender)
        row = self.agy_row(sent["broadcast_id"])
        self.assertEqual("received", row["status"])
        self.assertEqual("11", row["native_turn_id"])
        self.assertEqual("failed", row["processing_status"])

    async def test_broker_restart_preserves_receipt_marks_running_processing_unknown_and_does_not_resend(self):
        (self.root / "config.json").write_text(json.dumps({"profiles": {}, "telegram_database": None}))
        sent = self.broadcast()
        original = self.agy_row(sent["broadcast_id"])
        with self.store.connection(write=True) as db:
            db.execute("UPDATE native_broadcast_deliveries SET status='received',receipt=id,native_turn_id='11',processing_status='running' WHERE runtime='antigravity'")
            db.execute("UPDATE native_broadcast_deliveries SET status='enqueued',receipt='codex' WHERE runtime='codex'")
        restarted = Broker(self.root)
        row = self.agy_row(sent["broadcast_id"])
        self.assertEqual("received", row["status"])
        self.assertEqual(original["id"], row["receipt"])
        self.assertEqual("11", row["native_turn_id"])
        self.assertEqual("uncertain", row["processing_status"])

        async def forbidden(*args):
            self.fail("a received message must not be sent again after restart")

        tasks = set()
        await native_broadcast.deliver(restarted.store, restarted.config, forbidden, tasks=tasks)
        self.assertFalse(tasks)
        self.assertEqual(sent, self.broadcast())
        self.assertFalse(self.record.exists())


if __name__ == "__main__":
    unittest.main()
