import asyncio
import fcntl
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from agent_bridge import __version__, update_guard, updater
from agent_bridge import broker as broker_mod
from agent_bridge.broker import Broker, _wait_host_interval, request
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.worker import Worker


def _write_config(state: Path, *, profiles=None):
    (state / "config.json").write_text(
        json.dumps(
            {
                "profiles": profiles or {
                    "fixture": {"runtime": "pi", "provider": "p", "model": "m", "quota_group": "q"}
                },
                "worker_launcher": "detached",
                "telegram_database": None,
            }
        )
    )


def _setup_managed_releases(state: Path):
    root = state / "managed"
    old = root / "releases/v0.2.8-old"
    new = root / "releases/v0.2.9-new"
    old.mkdir(parents=True, mode=0o700)
    new.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    (root / "releases").chmod(0o700)
    (old / "release.json").write_text(json.dumps({"tag": "v0.2.8"}))
    (new / "release.json").write_text(json.dumps({"tag": "v0.2.9"}))
    (root / "current").symlink_to(old)
    settings_path = state / "update.json"
    settings_path.write_text(
        json.dumps(
            {
                "repository": updater.REPOSITORY,
                "root": str(root),
                "service_kind": "systemd",
                "service_name": "cli-agent-bridge.service",
                "bootstrap_python": sys.executable,
            }
        )
    )
    settings_path.chmod(0o600)
    return root, old, new


class BrokerOtaDrainTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        _write_config(self.state)
        self.broker = Broker(self.state)
        children_patch = patch.object(updater, "_children", return_value=[])
        children_patch.start()
        self.addCleanup(children_patch.stop)

    async def test_prepare_waits_for_native_tasks_and_runs_snapshot_off_event_loop(self):
        release_native = asyncio.Event()
        native_task = asyncio.create_task(release_native.wait())
        self.broker._native_tasks.add(native_task)
        native_task.add_done_callback(self.broker._native_tasks.discard)

        threads = []
        stop_ticker = asyncio.Event()
        ticks = 0

        async def ticker():
            nonlocal ticks
            while not stop_ticker.is_set():
                ticks += 1
                await asyncio.sleep(0.01)

        def slow_snapshot(state, **kwargs):
            threads.append(threading.current_thread() is threading.main_thread())
            time.sleep(0.06)
            return {"busy": False, "reasons": [], "incompatible_workers": []}

        ticker_task = asyncio.create_task(ticker())
        lease = None
        try:
            with patch.object(updater, "busy_snapshot", side_effect=slow_snapshot):
                prepare_task = asyncio.create_task(
                    self.broker.api_call("agent_update_prepare", {"hot": True, "drain_timeout": 3.0})
                )
                await asyncio.sleep(0.05)
                self.assertTrue(self.broker.update_draining.is_set())
                self.assertFalse(prepare_task.done())
                self.assertEqual([], threads)
                ticks_before = ticks
                release_native.set()
                lease = (await asyncio.wait_for(prepare_task, 2))["result"]
                ticks_after = ticks
            self.assertEqual([False], threads)
            self.assertGreaterEqual(ticks_after - ticks_before, 2)
        finally:
            release_native.set()
            stop_ticker.set()
            await asyncio.gather(native_task, ticker_task, return_exceptions=True)
            if lease is not None:
                await self.broker.api_call("agent_update_finish", {"token": lease["token"]})

    async def test_wait_host_interval_wakes_promptly_after_draining_clears(self):
        stopping = asyncio.Event()
        draining = asyncio.Event()
        draining.set()

        async def clear_soon():
            await asyncio.sleep(0.02)
            draining.clear()

        cleaner = asyncio.create_task(clear_soon())
        try:
            start = time.monotonic()
            await _wait_host_interval(stopping, draining, 30.0, paused=True, keep_going=lambda: True)
            elapsed = time.monotonic() - start
            self.assertLess(elapsed, 0.4)
        finally:
            await asyncio.gather(cleaner, return_exceptions=True)

        start = time.monotonic()
        await _wait_host_interval(stopping, draining, 30.0, paused=True, keep_going=lambda: True)
        self.assertLess(time.monotonic() - start, 0.05)

    async def test_api_call_allows_readonly_reads_during_draining_and_skips_parent_refresh(self):
        profile = self.broker.config.profiles["fixture"]
        sid = self.broker.store.open(profile, str(self.state), "read-only", "open-ro")["session_id"]
        tid = self.broker.store.dispatch(sid, "task", "disp-ro")["task_id"]
        self.broker.store.record_operation(
            "op-1", "node-a", "gen-a", "fp", "agent_dispatch", {}, status="completed", result={"ok": True}
        )

        self.broker.update_draining.set()
        try:
            with patch.object(self.broker.store, "refresh_parent_usage", MagicMock()) as refresh_mock:
                groups_res = await self.broker.api_call("agent_groups", {})
                result_res = await self.broker.api_call("agent_result", {"task_id": tid})
                op_res = await self.broker.api_call("agent_operation", {"operation_id": "op-1", "wait": 0})
                inspect_res = await self.broker.api_call("agent_inspect", {})
                self.assertIn("groups", groups_res["result"])
                self.assertEqual(tid, result_res["result"]["id"])
                self.assertEqual("completed", op_res["result"]["status"])
                self.assertIn("sessions", inspect_res["result"])
                refresh_mock.assert_not_called()

                with self.assertRaises(BridgeError) as error:
                    await self.broker.api_call("agent_inspect", {"parent_id": "p1"})
                self.assertEqual("update_busy", error.exception.code)

                self.broker.update_draining.clear()
                await self.broker.api_call("agent_inspect", {})
                refresh_mock.assert_called_once()
        finally:
            self.broker.update_draining.clear()

    async def test_prepare_drains_inflight_operation_and_resumes_on_finish(self):
        op_started = asyncio.Event()
        release_op = asyncio.Event()

        async def inflight():
            with update_guard.operation(self.state):
                op_started.set()
                await release_op.wait()

        holder = asyncio.create_task(inflight())
        sched = asyncio.create_task(self.broker.scheduler())
        try:
            await asyncio.wait_for(op_started.wait(), 1)
            prepare_task = asyncio.create_task(
                self.broker.api_call("agent_update_prepare", {"hot": True, "drain_timeout": 3.0})
            )
            await asyncio.sleep(0.05)
            # While draining, mutating RPCs fail closed with update_busy, readonly stays responsive.
            with self.assertRaises(BridgeError) as error:
                await self.broker.api_call("agent_hosts_reload", {})
            self.assertEqual("update_busy", error.exception.code)
            self.assertIn("hosts", (await self.broker.api_call("agent_hosts", {}))["result"])
            self.assertFalse(prepare_task.done())

            release_op.set()
            await asyncio.wait_for(holder, 1)
            lease = (await asyncio.wait_for(prepare_task, 2))["result"]
            self.assertIsNotNone(update_guard.maintenance(self.state))
            self.assertTrue(lease["hot"])

            await self.broker.api_call("agent_update_finish", {"token": lease["token"]})
            self.assertIsNone(update_guard.maintenance(self.state))
            self.assertFalse(self.broker.update_draining.is_set())
            self.assertIn("hosts", (await self.broker.api_call("agent_hosts", {}))["result"])
        finally:
            release_op.set()
            self.broker.stopping.set()
            await asyncio.gather(holder, sched, return_exceptions=True)

    async def test_agent_wait_wakes_immediately_when_prepare_starts(self):
        waiter = asyncio.create_task(self.broker.api_call("agent_wait", {"timeout": 30}))
        await asyncio.sleep(0.05)
        start = time.monotonic()
        lease = (await self.broker.api_call("agent_update_prepare", {"drain_timeout": 3.0}))["result"]
        waited = (await asyncio.wait_for(waiter, 1))["result"]
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 0.8)
        self.assertTrue(waited["timed_out"])
        await self.broker.api_call("agent_update_finish", {"token": lease["token"]})

    async def test_hot_prepare_allows_live_worker_while_non_hot_fails_fast(self):
        sid = self.broker.store.open(
            Profile(**Config.load(self.state / "config.json").profiles["fixture"].__dict__),
            str(self.state),
            "read-only",
            "open-1",
        )["session_id"]
        with self.broker.store.connection(write=True) as db:
            db.execute(
                "UPDATE sessions SET worker_pid=123,worker_start='boot:1',worker_version='0.2.8',observed='idle' WHERE id=?",
                (sid,),
            )
            db.execute(
                "INSERT INTO tasks(id,session_id,instruction,dependencies,reservation,status,created) "
                "VALUES('t1',?,'run','[]',0,'running',0)",
                (sid,),
            )
        with patch("agent_bridge.worker.process_identity", return_value="boot:1"):
            start = time.monotonic()
            with self.assertRaises(BridgeError) as error:
                await self.broker.api_call("agent_update_prepare", {"hot": False, "drain_timeout": 5.0})
            self.assertEqual("update_busy", error.exception.code)
            self.assertLess(time.monotonic() - start, 0.5)
            self.assertFalse(self.broker.update_draining.is_set())

            lease = (await self.broker.api_call("agent_update_prepare", {"hot": True}))["result"]
            self.assertTrue(lease["hot"])
            await self.broker.api_call("agent_update_finish", {"token": lease["token"]})

    async def test_prepare_resets_update_draining_on_timeout(self):
        with update_guard.operation(self.state):
            start = time.monotonic()
            with self.assertRaises(BridgeError) as error:
                await self.broker.api_call("agent_update_prepare", {"hot": True, "drain_timeout": 0.15})
            self.assertEqual("update_busy", error.exception.code)
            self.assertGreaterEqual(time.monotonic() - start, 0.12)
        self.assertFalse(self.broker.update_draining.is_set())
        self.assertIsNone(update_guard.maintenance(self.state))
        self.assertIn("added", (await self.broker.api_call("agent_hosts_reload", {}))["result"])

    async def test_prepare_drains_direct_peer_service_mutating_rpc(self):
        service = DirectPeerService(
            FileNodeIdentityStore(self.state),
            store=self.broker.store,
            config=self.broker.config,
        )
        rpc_started = asyncio.Event()
        release_rpc = asyncio.Event()

        async def slow_handle(_request):
            rpc_started.set()
            await release_rpc.wait()
            return {"id": "rpc-1", "ok": True, "result": {"accepted": True}}

        service._handle = slow_handle
        rpc_task = asyncio.create_task(service.handle({"id": "rpc-1", "method": "peer.deliver", "params": {}}))
        try:
            await asyncio.wait_for(rpc_started.wait(), 1)
            prepare_task = asyncio.create_task(
                self.broker.api_call("agent_update_prepare", {"hot": True, "drain_timeout": 3.0})
            )
            await asyncio.sleep(0.05)
            self.assertFalse(prepare_task.done())
            self.assertTrue(self.broker.update_draining.is_set())

            release_rpc.set()
            rpc_reply = await asyncio.wait_for(rpc_task, 1)
            self.assertTrue(rpc_reply["ok"])

            lease = (await asyncio.wait_for(prepare_task, 2))["result"]
            blocked = await service.handle({"id": "rpc-2", "method": "peer.deliver", "params": {}})
            self.assertFalse(blocked["ok"])
            self.assertEqual("peer_host_offline", blocked["error"]["code"])
            await self.broker.api_call("agent_update_finish", {"token": lease["token"]})
        finally:
            release_rpc.set()
            await asyncio.gather(rpc_task, return_exceptions=True)


class RequestRestartRetryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        _write_config(self.state)
        self.broker = Broker(self.state)

    async def test_request_fails_immediately_when_no_update_is_active(self):
        start = time.monotonic()
        with self.assertRaises(FileNotFoundError):
            await request(self.state, "agent_hosts", {})
        self.assertLess(time.monotonic() - start, 0.2)

    async def test_non_idempotent_write_never_retries_on_disconnect_during_maintenance(self):
        lease = update_guard.prepare(self.state, lambda _: {"busy": False, "reasons": []})
        self.addCleanup(update_guard.finish, self.state, lease["token"])
        updater._write(
            self.state / "update-status.json",
            {"status": "activating", "target_version": __version__, "checked_at": time.time()},
        )
        for exc_type in (EOFError, ConnectionResetError):
            with self.subTest(exc_type=exc_type):
                with patch.object(
                    broker_mod, "_single_request", new=AsyncMock(side_effect=exc_type("disconnected"))
                ) as single:
                    with self.assertRaises(exc_type):
                        await request(
                            self.state,
                            "agent_dispatch",
                            {"session_id": "s1", "instruction": "hi"},
                            retry_timeout=1.0,
                        )
                    self.assertEqual(1, single.await_count)

    async def test_update_status_never_retries_on_busy_or_missing_socket(self):
        lease = update_guard.prepare(self.state, lambda _: {"busy": False, "reasons": []})
        self.addCleanup(update_guard.finish, self.state, lease["token"])
        updater._write(
            self.state / "update-status.json",
            {"status": "activating", "target_version": __version__, "checked_at": time.time()},
        )
        for exc in (FileNotFoundError("no sock"), BridgeError("update_busy", "busy")):
            with self.subTest(exc=type(exc).__name__):
                with patch.object(broker_mod, "_single_request", new=AsyncMock(side_effect=exc)) as single:
                    with self.assertRaises(type(exc)):
                        await request(self.state, "agent_update_status", {}, retry_timeout=1.0)
                    self.assertEqual(1, single.await_count)

    async def test_stale_activating_journal_without_maintenance_fails_immediately(self):
        updater._write(
            self.state / "update-status.json",
            {"status": "activating", "target_version": __version__, "checked_at": time.time() - 300},
        )
        start = time.monotonic()
        with self.assertRaises(FileNotFoundError):
            await request(self.state, "agent_hosts", {}, retry_timeout=2.0)
        self.assertLess(time.monotonic() - start, 0.2)

    async def test_request_retries_across_maintenance_socket_gap_until_new_broker_is_up(self):
        sock = self.state / "broker.sock"
        lease = update_guard.prepare(self.state, lambda _: {"busy": False, "reasons": []})
        updater._write(
            self.state / "update-status.json",
            {"status": "activating", "target_version": __version__, "checked_at": time.time()},
        )

        async def restart_server():
            await asyncio.sleep(0.35)
            new_broker = Broker(self.state)
            server = await asyncio.start_unix_server(new_broker.connection, str(sock), limit=2 * 1024 * 1024)
            sock.chmod(0o600)
            update_guard.finish(self.state, lease["token"])
            updater._write(self.state / "update-status.json", {"status": "updated", "version": __version__})
            return server, new_broker

        starter = asyncio.create_task(restart_server())
        try:
            result = await request(self.state, "agent_hosts", {}, retry_timeout=3.0)
            self.assertIn("hosts", result)
        finally:
            server, _ = await starter
            server.close()
            await server.wait_closed()


class WorkerRollingUpgradeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        _write_config(self.state)
        self.root, self.old, self.new = _setup_managed_releases(self.state)
        self.config = Config.load(self.state / "config.json")

    async def test_registration_stays_pending_while_the_worker_is_alive(self):
        broker = Broker(self.state)
        sid = self._open_session(self.config.profiles["fixture"])
        busy = sqlite3.OperationalError("database is locked")
        busy.sqlite_errorcode = sqlite3.SQLITE_BUSY
        get_session = broker.store.get_session
        reads = 0

        def session_state(session_id):
            nonlocal reads
            reads += 1
            if reads == 1:
                raise busy
            return get_session(session_id)

        async def still_running(session_id):
            broker.store.worker_starting(session_id, "late-worker", os.getpid(), "start")
            return False

        with (
            patch.object(broker_mod, "_WORKER_START_PROBE_DELAY", 0),
            patch.object(broker.store, "get_session", side_effect=session_state),
            patch.object(broker, "_systemd_worker_exited", side_effect=still_running) as probe,
        ):
            await asyncio.wait_for(broker._wait_worker_registration(sid), 2)
        probe.assert_awaited_once_with(sid)
        self.assertEqual(("starting", os.getpid()), (get_session(sid)["observed"], get_session(sid)["worker_pid"]))

    async def test_confirmed_worker_exit_keeps_the_task_queued_and_marks_startup_failed(self):
        broker = Broker(self.state)
        sid = self._open_session(self.config.profiles["fixture"])
        task_id = broker.store.dispatch(sid, "never sent", "start-exited")["task_id"]
        child = SimpleNamespace(poll=lambda: 1)

        async def launch(session_id):
            await broker._wait_worker_registration(session_id, child)

        with patch.object(broker, "launch", side_effect=launch):
            await broker.ensure_worker(sid)
        self.assertEqual("recovery_needed", broker.store.get_session(sid)["observed"])
        self.assertIn("exited before registering", broker.store.get_session(sid)["error"])
        self.assertEqual("queued", broker.store.get_task(task_id)["status"])
        self.assertEqual([], broker.store.get_task(task_id)["attempts"])

    async def test_unavailable_systemd_status_is_not_proof_of_worker_exit(self):
        broker = Broker(self.state)
        for code, fields, expected in (
            (1, b"", False),
            (0, b"LoadState=loaded\nActiveState=activating\n", False),
            (0, b"LoadState=not-found\nActiveState=inactive\n", True),
        ):
            with self.subTest(code=code, fields=fields):
                proc = SimpleNamespace(returncode=code, communicate=AsyncMock(return_value=(fields, b"")))
                with patch.object(broker_mod.asyncio, "create_subprocess_exec", AsyncMock(return_value=proc)):
                    self.assertEqual(expected, await broker._systemd_worker_exited("fixture-session"))

    async def test_launch_limit_includes_workers_connecting_to_the_native_cli(self):
        broker = Broker(self.state)
        profile = self.config.profiles["fixture"]
        registered = [self._open_session(profile) for _ in range(2)]
        for sid in registered:
            broker.store.worker_starting(sid, sid, os.getpid(), "start")
        pending = self._open_session(profile)
        broker.launching.update({registered[0], pending})
        queued = [self._open_session(profile) for _ in range(4)]
        for sid in queued:
            broker.store.dispatch(sid, "ready work", "dispatch-" + sid)

        launched = AsyncMock()
        sweeps = 0

        async def wait(_):
            nonlocal sweeps
            sweeps += 1
            await asyncio.sleep(0)
            if sweeps == 1:
                launched.assert_not_awaited()
                sid = registered[0]
                broker.store.worker_ready(sid, sid, os.getpid(), "start", "native-" + sid)
                broker.launching.discard(sid)
            else:
                launched.assert_awaited_once_with(queued[0])
                broker.stopping.set()

        with (
            patch.object(broker_mod, "alive", return_value=True),
            patch.object(broker, "ensure_worker", launched),
            patch.object(broker, "_loop_wait", side_effect=wait),
        ):
            await asyncio.wait_for(broker.scheduler(), 2)

    async def test_active_task_finishes_before_worker_retires_and_unstarted_assigned_task_requeues_cleanly(self):
        profile = self.config.profiles["fixture"]
        worker_v1 = Worker(self.state, self.config, sid := self._open_session(profile))
        t1 = worker_v1.store.dispatch(sid, "task one", "req-1")["task_id"]
        t2 = worker_v1.store.dispatch(sid, "task two", "req-2")["task_id"]

        first_started = asyncio.Event()
        allow_first_finish = asyncio.Event()
        seen_versions = []
        race_claimed = []

        orig_v1_begin = worker_v1.store.begin_attempt
        v1_claimed = False

        def begin_attempt_v1(*args, **kwargs):
            nonlocal v1_claimed
            if not v1_claimed:
                v1_claimed = True
                worker_v1.store.claim(self.config)
            return orig_v1_begin(*args, **kwargs)

        worker_v1.store.begin_attempt = begin_attempt_v1
        orig_retire = worker_v1._should_retire_for_release

        def retire_with_claim_race():
            retire = orig_retire()
            if retire and not race_claimed:
                # Simulate Broker.claim assigning t2 right as the idle worker is about to retire.
                worker_v1.store.claim(self.config)
                race_claimed.append(worker_v1.store.get_task(t2)["status"])
            return retire

        worker_v1._should_retire_for_release = retire_with_claim_race

        async def run_first(instruction, started_native, _event):
            seen_versions.append(worker_v1.store.get_session(sid)["worker_version"])
            await started_native("turn-1")
            first_started.set()
            await allow_first_finish.wait()
            return "completed", {"answer": "done-1"}, None

        agent1 = SimpleNamespace(
            native_id="native-1",
            connect=AsyncMock(),
            run=AsyncMock(side_effect=run_first),
            interrupt=AsyncMock(),
            close=AsyncMock(),
        )
        task1_runner = None
        try:
            with patch("agent_bridge.worker.runtime", return_value=agent1):
                task1_runner = asyncio.create_task(worker_v1.run())
                await asyncio.wait_for(first_started.wait(), 2)
                # Broker OTA happens mid-task (switch current symlink + clear maintenance).
                (self.root / "current").unlink()
                (self.root / "current").symlink_to(self.new)
                await asyncio.sleep(0.05)
                # Active task must NOT be interrupted by release change.
                self.assertFalse(worker_v1.shutdown)
                self.assertEqual("running", worker_v1.store.get_task(t1)["status"])
                allow_first_finish.set()
                await asyncio.wait_for(task1_runner, 2)
        finally:
            allow_first_finish.set()
            if task1_runner is not None and not task1_runner.done():
                task1_runner.cancel()
                await asyncio.gather(task1_runner, return_exceptions=True)

        self.assertEqual([__version__], seen_versions)
        self.assertEqual(["assigned"], race_claimed)
        self.assertEqual("completed", worker_v1.store.get_task(t1)["status"])
        self.assertEqual("queued", worker_v1.store.get_task(t2)["status"])
        session = worker_v1.store.get_session(sid)
        self.assertEqual("open", session["desired"])
        self.assertEqual("stopped", session["observed"])
        self.assertIsNone(session["worker_pid"])
        self.assertIsNone(session["worker_version"])

        # New worker picks up t2 WITHOUT injecting "Continue the interrupted task..."
        worker_v2 = Worker(self.state, self.config, sid, release_changed=lambda: False)
        orig_v2_begin = worker_v2.store.begin_attempt
        v2_claimed = False

        def begin_attempt_v2(*args, **kwargs):
            nonlocal v2_claimed
            if not v2_claimed:
                v2_claimed = True
                worker_v2.store.claim(self.config)
            return orig_v2_begin(*args, **kwargs)

        worker_v2.store.begin_attempt = begin_attempt_v2
        received_instructions = []

        async def run_second(instruction, started_native, _event):
            received_instructions.append(instruction)
            await started_native("turn-2")
            worker_v2.shutdown = True
            return "completed", {"answer": "done-2"}, None

        agent2 = SimpleNamespace(
            native_id="native-1",
            connect=AsyncMock(),
            run=AsyncMock(side_effect=run_second),
            interrupt=AsyncMock(),
            close=AsyncMock(),
        )
        with patch("agent_bridge.worker.runtime", return_value=agent2):
            await asyncio.wait_for(worker_v2.run(), 2)
        self.assertEqual(["task two"], received_instructions)
        self.assertEqual("completed", worker_v2.store.get_task(t2)["status"])

    async def test_wait_worker_released_blocks_until_lock_unlocked(self):
        profile = self.config.profiles["fixture"]
        sid = self._open_session(profile)
        lock_path = self.state / f"{sid}.lock"
        broker = Broker(self.state)
        handle = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            waiter = asyncio.create_task(broker._wait_worker_released(sid, timeout=2.0))
            await asyncio.sleep(0.15)
            self.assertFalse(waiter.done())
            fcntl.flock(handle, fcntl.LOCK_UN)
            self.assertTrue(await asyncio.wait_for(waiter, 1.0))
        finally:
            os.close(handle)

    async def test_idle_worker_waits_for_maintenance_then_retires_cleanly(self):
        profile = self.config.profiles["fixture"]
        sid = self._open_session(profile)
        worker = Worker(self.state, self.config, sid)
        ready = asyncio.Event()

        async def on_connect():
            ready.set()

        agent = SimpleNamespace(
            native_id="native-idle",
            connect=AsyncMock(side_effect=on_connect),
            run=AsyncMock(),
            interrupt=AsyncMock(),
            close=AsyncMock(),
        )
        lease = update_guard.prepare(self.state, lambda _: {"busy": False, "reasons": []})
        (self.root / "current").unlink()
        (self.root / "current").symlink_to(self.new)

        with patch("agent_bridge.worker.runtime", return_value=agent):
            runner = asyncio.create_task(worker.run())
            await asyncio.wait_for(ready.wait(), 2)
            # Maintenance still active: idle worker stays alive rather than bouncing mid-switch/rollback.
            await asyncio.sleep(0.25)
            self.assertFalse(runner.done())

            update_guard.finish(self.state, lease["token"])
            await asyncio.wait_for(runner, 2)

        session = worker.store.get_session(sid)
        self.assertEqual("open", session["desired"])
        self.assertEqual("stopped", session["observed"])
        self.assertIsNone(session["worker_pid"])
        self.assertIsNone(session["error"])
        # Lock file is fully released and immediately re-acquirable.
        broker = Broker(self.state)
        await broker._wait_worker_released(sid, timeout=0.5)

    async def test_wait_worker_released_timeout_defers_ensure_worker_without_recovery_needed(self):
        profile = self.config.profiles["fixture"]
        sid = self._open_session(profile)
        broker = Broker(self.state)
        tid = broker.store.dispatch(sid, "hi", "disp-busy")["task_id"]
        lock_path = self.state / f"{sid}.lock"
        handle = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertFalse(await broker._wait_worker_released(sid, timeout=0.15))
            await broker.ensure_worker(sid)
        finally:
            os.close(handle)
        session = broker.store.get_session(sid)
        self.assertEqual("stopped", session["observed"])
        self.assertIsNone(session["error"])
        self.assertEqual("queued", broker.store.get_task(tid)["status"])

    async def test_rollback_while_maintenance_active_does_not_retire_worker(self):
        profile = self.config.profiles["fixture"]
        sid = self._open_session(profile)
        worker = Worker(self.state, self.config, sid)
        ready = asyncio.Event()

        async def on_connect():
            ready.set()

        agent = SimpleNamespace(
            native_id="native-rollback",
            connect=AsyncMock(side_effect=on_connect),
            run=AsyncMock(),
            interrupt=AsyncMock(),
            close=AsyncMock(),
        )
        lease = update_guard.prepare(self.state, lambda _: {"busy": False, "reasons": []})
        runner = None
        try:
            with patch("agent_bridge.worker.runtime", return_value=agent):
                runner = asyncio.create_task(worker.run())
                await asyncio.wait_for(ready.wait(), 2)
                (self.root / "current").unlink()
                (self.root / "current").symlink_to(self.new)
                await asyncio.sleep(0.15)
                self.assertFalse(runner.done())

                (self.root / "current").unlink()
                (self.root / "current").symlink_to(self.old)
                update_guard.finish(self.state, lease["token"])
                await asyncio.sleep(0.25)
                self.assertFalse(runner.done())
                worker.shutdown = True
                await asyncio.wait_for(runner, 2)
        finally:
            if update_guard.maintenance(self.state) is not None:
                update_guard.finish(self.state, lease["token"])
            worker.shutdown = True
            if runner is not None and not runner.done():
                await asyncio.gather(runner, return_exceptions=True)

    async def test_worker_run_releases_lock_before_marking_worker_stopped(self):
        profile = self.config.profiles["fixture"]
        sid = self._open_session(profile)
        worker = Worker(self.state, self.config, sid)
        lock_free = []
        orig_stopped = worker.store.worker_stopped

        def checked_worker_stopped(*args, **kwargs):
            fd = os.open(self.state / f"{sid}.lock", os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_free.append(True)
                fcntl.flock(fd, fcntl.LOCK_UN)
            except BlockingIOError:
                lock_free.append(False)
            finally:
                os.close(fd)
            return orig_stopped(*args, **kwargs)

        worker.store.worker_stopped = checked_worker_stopped

        async def on_connect():
            worker.shutdown = True

        agent = SimpleNamespace(
            native_id="native-lock-order",
            connect=AsyncMock(side_effect=on_connect),
            run=AsyncMock(),
            interrupt=AsyncMock(),
            close=AsyncMock(),
        )
        with patch("agent_bridge.worker.runtime", return_value=agent):
            await asyncio.wait_for(worker.run(), 2)
        self.assertEqual([True], lock_free)

    async def test_worker_event_retries_transient_sqlite_operational_errors(self):
        profile = self.config.profiles["fixture"]
        sid = self._open_session(profile)
        worker = Worker(self.state, self.config, sid)
        self.assertTrue(
            worker.store.worker_starting(
                sid, worker.generation, os.getpid(), "boot:1", worker_version=__version__
            )
        )
        worker.store.worker_ready(
            sid,
            worker.generation,
            os.getpid(),
            "boot:1",
            "native-ev",
            task_timeout_supported=True,
            worker_version=__version__,
        )
        tid = worker.store.dispatch(sid, "task", "disp-ev")["task_id"]
        worker.store.claim(self.config)
        worker.task = worker.store.begin_attempt(sid, worker.generation)
        self.assertIsNotNone(worker.task)

        orig_record = worker.store.record
        calls = {"count": 0}

        def flaky_record(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] < 3:
                raise sqlite3.OperationalError("database is locked")
            return orig_record(*args, **kwargs)

        worker.store.record = flaky_record
        await worker.event("first_output", {"source": "test"})
        self.assertEqual(3, calls["count"])
        with worker.store.connection() as db:
            kinds = [r["kind"] for r in db.execute("SELECT kind FROM events WHERE task_id=?", (tid,)).fetchall()]
        self.assertIn("first_output", kinds)

    async def test_release_change_detector_survives_corrupt_update_json(self):
        (self.state / "update.json").write_text("{corrupt-json")
        detector = updater.release_change_detector(self.state)
        self.assertFalse(detector())

    def _open_session(self, profile):
        from agent_bridge.store import Store

        store = Store(self.state)
        return store.open(profile, str(self.state), "read-only", f"open-{os.getpid()}-{time.monotonic()}")["session_id"]


if __name__ == "__main__":
    unittest.main()
