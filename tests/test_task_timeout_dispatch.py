import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agent_bridge.broker import Broker
from agent_bridge.config import Config, Profile
from agent_bridge.discovery import ModelDirectory, model_record
from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import SESSION_EFFORT_CAPABILITY, PeerErrorCode, ok
from agent_bridge.peer_transport import DirectPeerClient
from agent_bridge.store import Store, validate_task_timeout


class ServiceClient:
    def __init__(self, service):
        self.service = service
        self.requests = []

    async def hello(self, params):
        response = await self.service.handle({"v": 1, "id": "hello", "method": "peer.hello", "params": params})
        if not response["ok"]:
            raise BridgeError(response["error"]["code"], response["error"]["message"])
        return response["result"]

    async def deliver(self, params):
        self.requests.append(params)
        response = await self.service.handle({"v": 1, "id": "request", "method": "peer.deliver", "params": params})
        if not response["ok"]:
            raise BridgeError(response["error"]["code"], response["error"]["message"])
        return response["result"]


class Factory:
    def __init__(self, client):
        self.peer = client
        self.requested = []

    def client(self, host_id):
        self.requested.append(host_id)
        return self.peer


class TaskTimeoutDispatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.remote = Store(self.root / "remote")
        self.identity = FileNodeIdentityStore(self.remote.state)
        self.node = self.identity.load_or_create()
        self.profile = Profile("fixed", "codex", "fixture", executable=sys.executable, verified=True)
        self.capability_probe = AsyncMock(return_value={"features": ["task_timeout_v1"], "version": "fixture"})
        self.service = DirectPeerService(
            self.identity, host_id="peer", store=self.remote, config=Config({"fixed": self.profile}),
            capability_probe=self.capability_probe,
        )
        self.client = ServiceClient(self.service)
        self.factory = Factory(self.client)
        state = self.root / "local"
        state.mkdir()
        (state / "config.json").write_text(json.dumps({
            "profiles": {"fixed": {"runtime": "codex", "model": "fixture", "executable": sys.executable, "verified": True}},
            "worker_launcher": "detached", "telegram_database": None,
            "hosts": {"peer": {"ssh_target": "fixture", "remote_executable": "fixture", "pinned": {
                "node_id": self.node.node_id, "state_generation": self.node.state_generation,
            }}},
        }))
        self.broker = Broker(state, peer_factory=self.factory)
        self.local = self.broker.store
        self.sid = self.local.open(self.profile, str(self.root), "read-only", "existing")["session_id"]

    def local_args(self, request_id, **extra):
        return {"session_id": self.sid, "instruction": "compile fixture", "request_id": request_id, **extra}

    def remote_args(self, request_id, **extra):
        return {"host_id": "peer", "profile": "fixed", "cwd": str(self.root), "instruction": "compile fixture", "request_id": request_id, **extra}

    async def test_local_override_is_persisted_and_other_tasks_keep_node_default(self):
        longer = await self.broker.call("agent_dispatch", self.local_args("long", requirements={"task_timeout": 21600}))
        default = await self.broker.call("agent_dispatch", self.local_args("default"))
        explicit_null = await self.broker.call("agent_dispatch", self.local_args("null", requirements={"task_timeout": None}))
        self.assertEqual(21600, self.local.get_task(longer["task_id"])["details"]["task_timeout"])
        for task in (default, explicit_null):
            self.assertNotIn("task_timeout", self.local.get_task(task["task_id"])["details"])
        self.assertEqual(3600, self.broker.config.task_timeout)

    async def test_timeout_replay_is_idempotent_and_changes_conflict(self):
        args = self.local_args("replay", requirements={"task_timeout": 7200})
        first = await self.broker.api_call("agent_dispatch", args)
        self.assertEqual(first, await self.broker.api_call("agent_dispatch", args))
        with self.assertRaises(BridgeError) as caught:
            await self.broker.api_call("agent_dispatch", {**args, "requirements": {"task_timeout": 10800}})
        self.assertEqual("request_conflict", caught.exception.code)
        with self.local.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])

    async def test_invalid_timeouts_have_no_local_or_remote_creation_side_effects(self):
        invalid = [True, False, 0, -1, "7200", float("nan"), float("inf"), -float("inf"), 10 ** 1000]
        for index, value in enumerate(invalid):
            for remote in (False, True):
                with self.subTest(value=repr(value)[:30], remote=remote):
                    args = self.remote_args(f"invalid-{index}", access="workspace-write") if remote else {
                        "profile": "fixed", "cwd": str(self.root / "nonexistent"), "access": "workspace-write",
                        "instruction": "compile fixture", "request_id": f"invalid-{index}",
                    }
                    with patch.object(self.broker, "_prepare_remote_workspace", new_callable=AsyncMock) as prepare:
                        with self.assertRaises(BridgeError) as caught:
                            await self.broker.call("agent_dispatch", {**args, "requirements": {"task_timeout": value}})
                    self.assertEqual("invalid_task_timeout", caught.exception.code)
                    prepare.assert_not_awaited()
        self.assertEqual([], self.factory.requested)
        self.assertEqual([], self.client.requests)
        for store, sessions in ((self.local, 1), (self.remote, 0)):
            with store.connection() as db:
                self.assertEqual(sessions, db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0])
                self.assertEqual(0, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])
        self.assertFalse((self.broker.state / "worktrees").exists())

    async def test_store_retry_inherits_or_overrides_without_changing_running_attempt(self):
        created = self.local.dispatch(self.sid, "compile fixture", "create", details={"task_timeout": 7200})
        task_id = created["task_id"]
        self.local.cancel(task_id, "cancel")
        retried = await self.broker.call("agent_dispatch", self.local_args("retry", retry_task_id=task_id))
        self.assertEqual(task_id, retried["task_id"])
        self.assertEqual(7200, self.local.get_task(task_id)["details"]["task_timeout"])
        self.local.worker_ready(self.sid, "worker", 123, "fixture", "native", task_timeout_supported=True)
        attempt = self.local.claim(self.broker.config)
        running = self.local.begin_attempt(self.sid, "worker")
        self.assertEqual(attempt["attempt_id"], running["active_attempt"])
        with self.assertRaises(BridgeError) as caught:
            await self.broker.call("agent_dispatch", self.local_args("bad-live-retry", retry_task_id=task_id, requirements={"task_timeout": 21600}))
        self.assertEqual("invalid_retry", caught.exception.code)
        self.assertEqual(7200, self.local.get_task(task_id)["details"]["task_timeout"])
        self.local.finish(self.sid, "worker", task_id, attempt["attempt_id"], "interrupted", {"error": "fixture"})
        args = self.local_args("retry-longer", retry_task_id=task_id, requirements={"task_timeout": 21600})
        result = await self.broker.api_call("agent_dispatch", args)
        self.assertEqual(result, await self.broker.api_call("agent_dispatch", args))
        self.assertEqual(21600, self.local.get_task(task_id)["details"]["task_timeout"])
        with self.assertRaises(BridgeError) as caught:
            await self.broker.api_call("agent_dispatch", {**args, "requirements": {"task_timeout": 10800}})
        self.assertEqual("request_conflict", caught.exception.code)

    async def test_remote_composite_and_existing_session_persist_timeout_atomically(self):
        args = self.remote_args("remote-long", requirements={"task_timeout": 21600})
        first = await self.broker.call("agent_dispatch", args)
        self.assertEqual(first, await self.broker.call("agent_dispatch", args))
        self.assertEqual(1, len(self.client.requests))
        self.assertEqual(21600, self.client.requests[0]["task_timeout"])
        self.assertEqual(21600, self.remote.get_task(first["task_id"])["details"]["task_timeout"])
        second = await self.broker.call("agent_dispatch", self.remote_args(
            "remote-next", session_id=first["session_id"], requirements={"task_timeout": 10800},
        ))
        self.assertEqual(10800, self.remote.get_task(second["task_id"])["details"]["task_timeout"])
        default = await self.broker.call("agent_dispatch", self.remote_args("remote-default", session_id=first["session_id"]))
        self.assertNotIn("task_timeout", self.client.requests[-1])
        self.assertNotIn("task_timeout", self.remote.get_task(default["task_id"])["details"])
        self.assertEqual(3600, self.service._config.task_timeout)

    async def test_remote_retry_is_rejected_before_any_remote_operation(self):
        with patch.object(self.broker, "_prepare_remote_workspace", new_callable=AsyncMock) as prepare:
            with self.assertRaises(BridgeError) as caught:
                await self.broker.call("agent_dispatch", self.remote_args(
                    "retry-remote", retry_task_id="original", access="workspace-write", requirements={"task_timeout": 21600},
                ))
        self.assertEqual("unsupported_remote_retry", caught.exception.code)
        prepare.assert_not_awaited()
        self.assertEqual([], self.factory.requested)

    async def test_older_remote_node_is_rejected_before_workspace_or_task_creation(self):
        hello = AsyncMock(return_value={"capabilities": []})
        with patch.object(self.client, "hello", hello), patch.object(self.broker, "_prepare_remote_workspace", new_callable=AsyncMock) as prepare:
            with self.assertRaises(BridgeError) as caught:
                await self.broker.call("agent_dispatch", self.remote_args(
                    "old-node", access="workspace-write", requirements={"task_timeout": 21600},
                ))
        self.assertEqual("unsupported_task_timeout", caught.exception.code)
        hello.assert_awaited_once()
        prepare.assert_not_awaited()
        self.assertEqual([], self.client.requests)
        with self.remote.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0])

    async def test_remote_reasoning_effort_is_validated_and_persisted_by_target(self):
        self.service._models = ModelDirectory(self.root)
        self.capability_probe.return_value["features"].append(SESSION_EFFORT_CAPABILITY)
        args = self.remote_args("remote-effort", model="next-model", effort="xhigh")
        with patch(
            "agent_bridge.discovery.native_models", new_callable=AsyncMock,
            return_value=([model_record("next-model", reasoning_efforts=["xhigh"])], "target-directory"),
        ):
            first = await self.broker.api_call("agent_dispatch", args)
            self.assertEqual(first, await self.broker.api_call("agent_dispatch", args))
            rejected = await self.broker.api_call(
                "agent_dispatch", {**args, "request_id": "remote-invalid", "effort": "max"},
            )
            self.assertEqual("unsupported_model_settings", rejected["error"]["code"])
        self.assertEqual("xhigh", self.client.requests[0]["open"]["effort"])
        self.assertEqual(2, len(self.client.requests))
        result = first["result"]
        profile = self.remote.get_session(result["session_id"])["profile"]
        self.assertEqual(("next-model", "xhigh"), (profile["model"], profile["effort"]))
        self.assertEqual("queued", self.remote.get_task(result["task_id"])["status"])
        with self.remote.connection() as db:
            for table in ("sessions", "tasks", "peer_inbox"):
                self.assertEqual(1, db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    async def test_older_remote_node_rejects_effort_before_workspace_transfer(self):
        with patch.object(self.broker, "_prepare_remote_workspace", new_callable=AsyncMock) as prepare:
            with self.assertRaises(BridgeError) as caught:
                await self.broker.call("agent_dispatch", self.remote_args(
                    "old-effort", effort="xhigh", access="workspace-write",
                ))
        self.assertEqual("unsupported_session_effort", caught.exception.code)
        prepare.assert_not_awaited()
        self.assertEqual([], self.client.requests)
        with self.local.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM peer_operations").fetchone()[0])
        with self.remote.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0])

    async def test_old_worker_rejects_override_but_accepts_default_work(self):
        self.local.worker_ready(self.sid, "old", 123, "fixture", "native")
        with self.assertRaises(BridgeError) as caught:
            await self.broker.call("agent_dispatch", self.local_args("old-worker", requirements={"task_timeout": 7200}))
        self.assertEqual("worker_upgrade_required", caught.exception.code)
        default = await self.broker.call("agent_dispatch", self.local_args("default-old"))
        self.assertEqual("queued", self.local.get_task(default["task_id"])["status"])
        with self.local.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])

    async def test_capability_is_checked_again_on_the_final_business_connection(self):
        await self.assert_final_business_connection_rejects_downgrade(
            "task_timeout_v1", {"requirements": {"task_timeout": 21600}}, "unsupported_task_timeout",
        )

    async def test_effort_capability_is_checked_again_on_the_final_business_connection(self):
        await self.assert_final_business_connection_rejects_downgrade(
            SESSION_EFFORT_CAPABILITY, {"effort": "xhigh"}, "unsupported_session_effort",
        )

    async def assert_final_business_connection_rejects_downgrade(self, capability, options, error_code):
        calls = []
        node = {"host_id": "peer", "node_id": self.node.node_id, "state_generation": self.node.state_generation}
        async def call(request, timeout, **kwargs):
            calls.append(request["method"])
            self.assertEqual("peer.hello", request["method"], "An unsupported node must never receive the delivery")
            return ok(request["id"], {
                "protocol_version": 1, "node": node, "node_state": "ready", "max_confirmed_seq": 0,
                "capabilities": [capability] if len(calls) == 1 else [], "broker_version": "fixture",
            })
        transport = AsyncMock()
        transport.call.side_effect = call
        client = DirectPeerClient(transport, registry=self.broker.host_registry, host_id="peer")
        self.factory.peer = client
        with self.assertRaises(BridgeError) as caught:
            await self.broker.call("agent_dispatch", self.remote_args("downgraded", **options))
        self.assertEqual(error_code, caught.exception.code)
        self.assertEqual(["peer.hello", "peer.hello"], calls)
        with self.remote.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0])
        with self.local.connection() as db:
            self.assertEqual("uncertain", db.execute("SELECT status FROM peer_operations").fetchone()[0])

    async def test_queued_override_waits_for_a_supporting_worker(self):
        task = self.local.dispatch(self.sid, "compile fixture", "queued", details={"task_timeout": 21600})
        self.local.worker_ready(self.sid, "old", 123, "fixture", "native")
        self.assertIsNone(self.local.claim(self.broker.config))
        reason = self.local.waiting_reason(task["task_id"], self.broker.config)
        self.assertEqual("worker_upgrade_required", reason["code"])
        self.local.worker_ready(self.sid, "old", 123, "fixture", "native", task_timeout_supported=True)
        self.assertEqual(task["task_id"], self.local.claim(self.broker.config)["task_id"])
        self.local.worker_stopped(self.sid, "old")
        self.assertEqual(0, self.local.get_session(self.sid)["worker_task_timeout"])
        self.assertTrue(self.local.worker_starting(self.sid, "new-generation", 456, "new-fixture"))
        self.assertEqual(0, self.local.get_session(self.sid)["worker_task_timeout"])

    async def test_schema_upgrade_marks_an_existing_worker_unsupported_without_changing_it(self):
        self.local.worker_ready(self.sid, "old-generation", 123, "fixture", "native")
        with self.local.connection(write=True) as db:
            db.execute("ALTER TABLE sessions DROP COLUMN worker_task_timeout")
        upgraded = Store(self.local.state)
        row = upgraded.get_session(self.sid)
        self.assertEqual(0, row["worker_task_timeout"])
        self.assertEqual(("old-generation", 123, "idle"), (row["generation"], row["worker_pid"], row["observed"]))

    async def test_remote_operation_replay_cannot_change_the_timeout(self):
        first = await self.broker.call("agent_dispatch", self.remote_args("remote-replay", requirements={"task_timeout": 21600}))
        params = self.client.requests[0]
        replay = await self.client.deliver(params)
        self.assertEqual(first["task_id"], replay["task_id"])
        with self.assertRaises(BridgeError) as caught:
            await self.client.deliver({**params, "task_timeout": 10800})
        self.assertEqual(PeerErrorCode.OPERATION_CONFLICT, caught.exception.code)
        self.assertEqual(21600, self.remote.get_task(first["task_id"])["details"]["task_timeout"])
        with self.remote.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])

    async def test_raw_remote_invalid_override_rolls_back_without_new_session(self):
        with self.assertRaises(BridgeError) as caught:
            await self.client.deliver({
                "operation": {"node_id": self.node.node_id, "state_generation": self.node.state_generation, "operation_id": "bad"},
                "delivery_id": "bad", "target": {"kind": "bridge", "id": ""}, "target_seq": 1,
                "message": "compile fixture", "task_timeout": False,
                "open": {"profile": "fixed", "cwd": str(self.root), "access": "read-only"},
            })
        self.assertEqual("invalid_task_timeout", caught.exception.code)
        with self.remote.connection() as db:
            for table in ("sessions", "tasks", "peer_inbox", "peer_operations"):
                self.assertEqual(0, db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def raw_timeout_delivery(self):
        return {
            "operation": {"node_id": self.node.node_id, "state_generation": self.node.state_generation, "operation_id": "probe"},
            "delivery_id": "probe", "target": {"kind": "bridge", "id": ""}, "target_seq": 1,
            "message": "compile fixture", "task_timeout": 21600,
            "open": {"profile": "fixed", "cwd": str(self.root), "access": "read-only"},
        }

    async def test_new_relay_without_supporting_broker_does_not_advertise_or_deliver(self):
        probes = [
            None,
            AsyncMock(side_effect=BridgeError("unknown_method", "old Broker")),
            AsyncMock(side_effect=FileNotFoundError("no Broker socket")),
            AsyncMock(side_effect=TimeoutError("Broker unavailable")),
        ]
        for index, probe in enumerate(probes):
            with self.subTest(case=index):
                self.service._capability_probe = probe
                hello = await self.client.hello({"protocol_versions": [1]})
                self.assertNotIn("task_timeout_v1", hello["capabilities"])
                with patch.object(self.service, "_ready_identity") as ready:
                    with self.assertRaises(BridgeError) as caught:
                        await self.client.deliver(self.raw_timeout_delivery())
                self.assertEqual("unsupported_task_timeout", caught.exception.code)
                ready.assert_not_called()
        with self.remote.connection() as db:
            for table in ("sessions", "tasks", "peer_inbox", "peer_operations"):
                self.assertEqual(0, db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    async def test_broker_support_is_rechecked_after_the_relay_hello(self):
        self.service._capability_probe = AsyncMock(side_effect=[
            {"features": ["task_timeout_v1"], "version": "new"}, {"features": [], "version": "old"},
        ])
        hello = await self.client.hello({"protocol_versions": [1]})
        self.assertIn("task_timeout_v1", hello["capabilities"])
        with self.assertRaises(BridgeError) as caught:
            await self.client.deliver(self.raw_timeout_delivery())
        self.assertEqual("unsupported_task_timeout", caught.exception.code)
        with self.remote.connection() as db:
            for table in ("sessions", "tasks", "peer_inbox", "peer_operations"):
                self.assertEqual(0, db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    async def test_running_broker_exposes_internal_read_only_capabilities(self):
        with patch("agent_bridge.update_guard.maintenance", return_value={"fixture": True}):
            response = await self.broker.api_call("agent_capabilities", {})
        self.assertEqual(["task_timeout_v1", SESSION_EFFORT_CAPABILITY], response["result"]["features"])
        self.assertIsInstance(response["result"]["version"], str)
        with self.assertRaises(BridgeError) as caught:
            await self.broker.api_call("agent_capabilities", {"change": True})
        self.assertEqual("invalid_request", caught.exception.code)

    def test_timeout_validation_accepts_null_default_and_finite_positive_numbers(self):
        self.assertIsNone(validate_task_timeout(None))
        self.assertEqual(7200, validate_task_timeout(7200))
        self.assertEqual(.25, validate_task_timeout(.25))
        self.assertEqual(3600, Config({}).task_timeout)


if __name__ == "__main__":
    unittest.main()
