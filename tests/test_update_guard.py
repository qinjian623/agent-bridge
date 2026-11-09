import asyncio
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agent_bridge import __version__, update_guard
from agent_bridge.broker import Broker
from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService


def idle(_):
    return {"busy": False, "reasons": []}


class UpdateGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)

    def test_lease_survives_new_reader_and_is_private_without_raw_token(self):
        lease = update_guard.prepare(self.state, idle)
        path = self.state / "update-maintenance.json"
        value = json.loads(path.read_text())
        self.assertEqual(0o600, path.stat().st_mode & 0o777)
        self.assertNotIn(lease["token"], path.read_text())
        self.assertEqual(hashlib.sha256(lease["token"].encode()).hexdigest(), value["token_hash"])
        self.assertIsNotNone(update_guard.maintenance(Path(str(self.state))))
        with self.assertRaises(BridgeError) as error, update_guard.operation(self.state):
            self.fail("Maintenance allowed new work")
        self.assertEqual("update_busy", error.exception.code)
        with update_guard.operation(self.state, readonly=True):
            pass
        update_guard.finish(self.state, lease["token"])
        with update_guard.operation(self.state):
            pass

    def test_busy_or_existing_lease_cannot_prepare(self):
        with self.assertRaises(BridgeError):
            update_guard.prepare(self.state, lambda _: {"busy": True, "reasons": ["active task"]})
        self.assertFalse((self.state / "update-maintenance.json").exists())
        update_guard.prepare(self.state, idle)
        with self.assertRaises(BridgeError):
            update_guard.prepare(self.state, idle)

    def test_an_inflight_operation_blocks_prepare(self):
        with update_guard.operation(self.state), self.assertRaises(BridgeError):
            update_guard.prepare(self.state, idle)
        self.assertIsNone(update_guard.maintenance(self.state))

    def test_another_process_inflight_operation_blocks_prepare(self):
        code = "from pathlib import Path; from agent_bridge.update_guard import operation; import sys\nwith operation(Path(sys.argv[1])):\n print('locked',flush=True)\n sys.stdin.read()\n"
        process = subprocess.Popen([sys.executable, "-c", code, str(self.state)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual("locked", process.stdout.readline().strip())
            with self.assertRaises(BridgeError):
                update_guard.prepare(self.state, idle)
        finally:
            process.communicate(timeout=5)
        self.assertEqual(0, process.returncode)

    def test_wrong_token_cannot_finish_and_duplicate_finish_is_safe(self):
        lease = update_guard.prepare(self.state, idle)
        with self.assertRaises(BridgeError) as error:
            update_guard.finish(self.state, "different-token")
        self.assertEqual("forbidden", error.exception.code)
        self.assertIsNotNone(update_guard.maintenance(self.state))
        self.assertEqual({"maintenance": False}, update_guard.finish(self.state, lease["token"]))
        self.assertEqual({"maintenance": False}, update_guard.finish(self.state, lease["token"]))

    def test_expiry_releases_maintenance_without_updater(self):
        with patch("agent_bridge.update_guard.time.time", return_value=100):
            update_guard.prepare(self.state, idle)
        with patch("agent_bridge.update_guard.time.time", return_value=281), update_guard.operation(self.state):
            pass

    def test_insecure_malformed_and_symlink_leases_fail_closed(self):
        path = self.state / "update-maintenance.json"
        path.write_text("bad-json")
        path.chmod(0o600)
        with self.assertRaises(BridgeError):
            update_guard.maintenance(self.state)
        path.chmod(0o644)
        with self.assertRaises(BridgeError):
            update_guard.maintenance(self.state)
        path.unlink()
        target = self.state / "target"
        target.write_text("{}")
        path.symlink_to(target)
        with self.assertRaises((BridgeError, OSError)):
            with update_guard.operation(self.state):
                self.fail("Symlink lease accepted")

    def test_bad_ttl_does_not_create_lease(self):
        for ttl in (True, 0, 301, "180"):
            with self.subTest(ttl=ttl), self.assertRaises(BridgeError):
                update_guard.prepare(self.state, idle, ttl=ttl)


class BrokerUpdateGuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        (self.state / "config.json").write_text(json.dumps({"profiles": {}, "worker_launcher": "detached"}))
        self.broker = Broker(self.state)

    async def test_mutating_api_is_rejected_but_health_remains_available(self):
        lease = update_guard.prepare(self.state, idle)
        with patch.object(self.broker, "_api_call", new_callable=AsyncMock, return_value={"result": {}}) as call:
            with self.assertRaises(BridgeError):
                await self.broker.api_call("agent_dispatch", {})
            call.assert_not_called()
            await self.broker.api_call("agent_hosts", {})
            call.assert_awaited_once()
        await self.broker.api_call("agent_update_finish", {"token": lease["token"]})

    async def test_notification_and_scheduler_loops_do_not_start_work_during_lease(self):
        update_guard.prepare(self.state, idle)
        with patch("agent_bridge.notifications.deliver", new_callable=AsyncMock) as deliver, \
                patch.object(self.broker.store, "claim") as claim:
            jobs = [asyncio.create_task(self.broker.notifications()), asyncio.create_task(self.broker.scheduler())]
            await asyncio.sleep(0.02)
            self.broker.stopping.set()
            await asyncio.gather(*jobs)
            deliver.assert_not_called()
            claim.assert_not_called()

    async def test_restarted_broker_obeys_persisted_lease(self):
        update_guard.prepare(self.state, idle)
        restarted = Broker(self.state)
        with self.assertRaises(BridgeError):
            await restarted.api_call("agent_open", {})
        result = await restarted.api_call("agent_hosts", {})
        self.assertEqual([], result["result"]["hosts"])

    async def test_peer_rejects_work_before_execution_and_keeps_health_available(self):
        update_guard.prepare(self.state, idle)
        service = DirectPeerService(FileNodeIdentityStore(self.state), store=self.broker.store)
        with patch.object(service, "_deliver") as deliver:
            result = await service.handle({"id": "test", "method": "peer.deliver", "params": {}})
            self.assertFalse(result["ok"])
            self.assertEqual("peer_host_offline", result["error"]["code"])
            deliver.assert_not_called()
        result = await service.handle({"id": "health", "method": "peer.hello", "params": {"protocol_versions": [1]}})
        self.assertTrue(result["ok"])

    async def test_version_health_during_maintenance_does_not_launch_work(self):
        update_guard.prepare(self.state, idle)
        result = await self.broker.api_call("agent_update_status", {})
        self.assertEqual(__version__, result["result"]["version"])
        self.assertTrue(result["result"]["maintenance"])

    async def test_parent_receipt_inspection_cannot_launch_helper_during_maintenance(self):
        update_guard.prepare(self.state, idle)
        with patch("agent_bridge.notifications.send", new_callable=AsyncMock) as helper:
            with self.assertRaises(BridgeError):
                await self.broker.api_call("agent_inspect", {"parent_id": "synthetic-parent"})
            helper.assert_not_called()
