import asyncio
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from agent_bridge.broker import Broker, request
from agent_bridge.config import Config, Profile
from agent_bridge.discovery import model_record
from agent_bridge.errors import BridgeError
from agent_bridge.routing import choose, task_samples
from agent_bridge.store import Store
from agent_bridge.worker import Worker
from agent_bridge.workspace import directory_copy, worktree


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {"fixed": {"runtime": "codex", "model": "fixed-model", "executable": sys.executable}},
                    "hosts": {
                        "gpu-a": {
                            "ssh_target": "gpu-a.internal",
                            "remote_executable": "/usr/local/bin/agent-bridge",
                            "pinned": {"node_id": "node-a", "state_generation": "generation-a"},
                        }
                    },
                    "worker_launcher": "detached",
                    "telegram_database": None,
                }
            )
        )
        self.broker = Broker(self.state)
        # Metadata and persistence tests use real MCP/socket transport, no inference.
        self.server = await asyncio.start_unix_server(self.broker.connection, str(self.state / "broker.sock"))
        self.addAsyncCleanup(self.close_server)

    async def close_server(self):
        self.server.close()
        await self.server.wait_closed()

    async def test_worker_launcher_failure_notifies_parent_once_without_starting_task(self):
        store = self.broker.store
        store.parent_control("parent", "parent_register", 0, "register", "native-parent", str(self.state))
        sid = store.open(self.broker.config.profiles["fixed"], str(self.state), "read-only", "open", "parent")["session_id"]
        tid = store.dispatch(sid, "task", "dispatch")["task_id"]
        with patch.object(self.broker, "launch", AsyncMock(side_effect=OSError("systemd unavailable"))):
            await self.broker.ensure_worker(sid)
            await self.broker.ensure_worker(sid)
        self.assertEqual("recovery_needed", store.get_session(sid)["observed"])
        self.assertEqual("queued", store.get_task(tid)["status"])
        self.assertEqual([], store.get_task(tid)["attempts"])
        self.assertEqual(1, len(store.get_parent("parent")["deliveries"]))
        self.assertIsNone(store.claim(self.broker.config))

    async def test_actual_mcp_client_preserves_original_tools_and_adds_group_tools(self):
        params = StdioServerParameters(
            command=sys.executable, args=["-m", "agent_bridge.cli", "--state", str(self.state), "mcp"]
        )
        async with stdio_client(params) as (reader, writer), ClientSession(reader, writer) as session:
            await session.initialize()
            tools = await session.list_tools()
            self.assertEqual({"agent_catalog", "agent_open", "agent_dispatch", "agent_control", "agent_cancel",
                                      "agent_inspect", "agent_wait", "agent_result", "agent_review", "agent_hosts", "agent_hosts_reload",
                              "agent_monitor", "agent_operation",
                              "agent_groups", "agent_group", "agent_group_prune", "agent_broadcast", "agent_broadcast_status",
                              "agent_group_inbox", "agent_delivery_resolve"}, {t.name for t in tools.tools})
            for name in ("agent_open", "agent_dispatch"):
                properties = next(t for t in tools.tools if t.name == name).inputSchema["properties"]
                self.assertIn("model", properties)
                self.assertIsNone(properties["effort"]["default"])
            schema = next(t for t in tools.tools if t.name == "agent_dispatch").inputSchema["properties"]
            self.assertIsNone(schema["access"]["default"])
            self.assertIsNone(schema["reservation"]["default"])
            with patch(
                "agent_bridge.discovery.native_models",
                new_callable=AsyncMock,
                return_value=([model_record("new-model", reasoning_efforts=["xhigh"])], "test-directory"),
            ):
                catalog = await session.call_tool("agent_catalog", {"profile": "fixed", "refresh": True})
                self.assertFalse(catalog.isError, catalog)
                model = json.loads(catalog.content[0].text)["discovery"][0]["models"][0]
                self.assertEqual("new-model", model["model"])
                self.assertEqual(["xhigh"], model["reasoning_efforts"])
            result = await session.call_tool(
                "agent_open", {"profile": "fixed", "cwd": str(self.state), "request_id": "mcp-open"}
            )
            self.assertFalse(result.isError, result)
            opened = json.loads(result.content[0].text)
            inspected = await session.call_tool("agent_inspect", {"session_id": opened["session_id"]})
            self.assertFalse(inspected.isError, inspected)
            self.assertEqual("fixed-model", json.loads(inspected.content[0].text)["profile"]["model"])
            dispatched = await session.call_tool("agent_dispatch", {
                "profile": "fixed", "model": "new-model", "effort": "xhigh", "cwd": str(self.state),
                "instruction": "fixture", "request_id": "mcp-effort",
            })
            self.assertFalse(dispatched.isError, dispatched)
            dispatched = json.loads(dispatched.content[0].text)
            session_id = dispatched["session_id"]
            inspected = await session.call_tool("agent_inspect", {"session_id": session_id})
            self.assertFalse(inspected.isError, inspected)
            profile = json.loads(inspected.content[0].text)["profile"]
            self.assertEqual(("new-model", "xhigh"), (profile["model"], profile["effort"]))
            changed = await session.call_tool("agent_dispatch", {
                "session_id": session_id, "effort": "xhigh", "instruction": "fixture",
                "request_id": "mcp-effort-existing",
            })
            self.assertTrue(changed.isError, changed)
            self.assertIn("Reasoning effort can only be set", changed.content[0].text)
        self.assertEqual(2, len(self.broker.store.sessions()))
        self.assertEqual("queued", self.broker.store.get_task(dispatched["task_id"])["status"])
        with self.broker.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])

    async def test_null_effort_replays_without_a_new_session(self):
        args = {"profile": "fixed", "cwd": str(self.state), "request_id": "omit-effort"}
        opened = await request(self.state, "agent_open", args)
        with_null = {**args, "effort": None}
        self.assertEqual(opened, await request(self.state, "agent_open", with_null))
        self.assertEqual({"result": opened}, await self.broker.api_call("agent_open", with_null))
        self.assertEqual(1, len(self.broker.store.sessions()))

    async def test_legacy_broker_cannot_silently_drop_requested_effort(self):
        args = {"profile": "fixed", "cwd": str(self.state), "request_id": "legacy-effort", "effort": "xhigh"}
        methods = []

        async def serve(reader, writer):
            payload = json.loads(await reader.readline())
            method = payload["method"]
            methods.append(method)
            if method == "agent_open":
                params = {key: value for key, value in payload["params"].items() if key != "effort"}
                response = await self.broker.api_call(method, params)
            else:
                response = {"error": BridgeError("unknown_method", method).as_dict()}
            writer.write(json.dumps(response).encode() + b"\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        self.server.close()
        await self.server.wait_closed()
        socket_path = self.state / "broker.sock"
        socket_path.unlink(missing_ok=True)
        self.server = await asyncio.start_unix_server(serve, str(socket_path))
        with self.assertRaises(BridgeError) as caught:
            await request(self.state, "agent_open", args)
        self.assertEqual(["agent_open_with_effort"], methods)
        self.assertEqual("unsupported_session_effort", caught.exception.code)
        self.assertEqual([], self.broker.store.sessions())

    async def test_agent_hosts_tool_lists_registered_hosts_without_network(self):
        params = StdioServerParameters(
            command=sys.executable, args=["-m", "agent_bridge.cli", "--state", str(self.state), "mcp"]
        )
        async with stdio_client(params) as (reader, writer), ClientSession(reader, writer) as session:
            await session.initialize()
            tools = {tool.name: tool for tool in (await session.list_tools()).tools}
            self.assertTrue(tools["agent_hosts"].annotations.readOnlyHint)
            self.assertEqual(tools["agent_hosts"].inputSchema.get("properties", {}), {})

            listed = await session.call_tool("agent_hosts", {})
            self.assertFalse(listed.isError, listed)
            hosts = json.loads(listed.content[0].text)["hosts"]
            self.assertEqual(["gpu-a"], [host["host_id"] for host in hosts])
            self.assertEqual(
                {
                    "host_id",
                    "state",
                    "node_id",
                    "state_generation",
                    "last_contact",
                    "protocol_version",
                    "detail",
                },
                set(hosts[0]),
            )
            self.assertEqual("offline", hosts[0]["state"])
            self.assertEqual("node-a", hosts[0]["node_id"])
            self.assertEqual("generation-a", hosts[0]["state_generation"])
            self.assertIsNone(hosts[0]["last_contact"])
            self.assertIsNone(hosts[0]["protocol_version"])

            filtered = await self.broker.call("agent_hosts", {"host_id": "gpu-a"})
            self.assertEqual(hosts, filtered["hosts"])

            with self.assertRaises(BridgeError) as caught:
                await self.broker.call("agent_hosts", {"host_id": "missing"})
            self.assertEqual(caught.exception.code, "peer_host_unknown")

    async def test_agent_monitor_marks_broker_served_snapshot(self):
        snapshot = await self.broker.call("agent_monitor", {"include_native": False})
        provenance = snapshot["provenance"]
        self.assertEqual("broker", provenance["snapshot_source"])
        self.assertIs(provenance["broker_connected"], True)
        self.assertEqual("connected", provenance["connection_state"])
        self.assertEqual("unix_socket", provenance["transport"])
        self.assertEqual("broker", provenance["served_by"])
        self.assertEqual("broker", snapshot["coordinator"]["snapshot_source"])
        self.assertIs(snapshot["coordinator"]["broker_connected"], True)
        self.assertEqual("connected", snapshot["coordinator"]["connection_state"])

    async def test_child_client_cannot_control_owner(self):
        with (
            patch.dict(os.environ, {"AGENT_BRIDGE_ROLE": "child"}),
            self.assertRaisesRegex(BridgeError, "may not administer"),
        ):
            await request(self.state, "agent_catalog", {})

    async def test_wrong_token_rejected(self):
        with (
            patch.dict(os.environ, {"AGENT_BRIDGE_TOKEN": "wrong"}),
            self.assertRaisesRegex(BridgeError, "token is invalid"),
        ):
            await request(self.state, "agent_catalog", {})

    async def test_api_retries_return_original_model_even_after_config_change(self):
        original = self.broker.config.profiles["fixed"]
        self.broker.config.profiles["fixed"] = Profile(**{**original.__dict__, "verified": True})
        args = {"cwd": str(self.state), "instruction": "do once", "request_id": "auto"}
        first = await request(self.state, "agent_dispatch", args)
        self.broker.config.profiles = {}
        repeated = await request(self.state, "agent_dispatch", args)
        self.assertEqual(first, repeated)
        with self.assertRaisesRegex(BridgeError, "different API parameters"):
            await request(self.state, "agent_dispatch", {**args, "instruction": "changed"})

    async def test_invalid_dispatch_creates_no_session_or_workspace(self):
        (self.state / "input.txt").write_text("source")
        cases = [
            {"instruction": ""}, {"reservation": -1}, {"reservation": True},
            {"dependencies": ["missing"]}, {"dependencies": "missing"},
            {"retry_task_id": "missing"}, {"requirements": "speed"}, {"requirements": []},
            {"requirements": {"unsupported": True}}, {"access": "invalid"},
        ]
        for index, changes in enumerate(cases):
            args = {"profile": "fixed", "cwd": str(self.state), "access": "workspace-write",
                    "workspace_mode": "copy", "snapshot_files": ["input.txt"], "instruction": "task",
                    "request_id": str(index), **changes}
            with self.subTest(changes=changes), self.assertRaises(BridgeError):
                await request(self.state, "agent_dispatch", args)
            self.assertEqual([], self.broker.store.sessions())
            self.assertFalse((self.state / "copies").exists())
            self.assertFalse((self.state / "snapshots").exists())
        self.broker.config.token_budget = 100
        with self.assertRaisesRegex(BridgeError, "positive token reservation"):
            await request(self.state, "agent_dispatch", {"profile": "fixed", "cwd": str(self.state), "instruction": "task", "request_id": "budget"})
        self.assertEqual([], self.broker.store.sessions())

    async def test_copy_mode_worker_results_keep_immutable_captured_bytes(self):
        source = self.state / "copy-source"
        source.mkdir()
        (source / "file.txt").write_text("input\n")
        cwd, metadata = directory_copy(self.state, source, "copy-retention", ["file.txt"])
        store = self.broker.store
        sid = store.open(
            self.broker.config.profiles["fixed"], cwd, "workspace-write", "copy-open", workspace=metadata,
        )["session_id"]

        async def run_task(label, body):
            task_id = store.dispatch(sid, label, "dispatch-" + label)["task_id"]
            worker = Worker(self.state, self.broker.config, sid)
            begin = worker.store.begin_attempt

            def assign(*args):
                worker.store.claim(self.broker.config)
                return begin(*args)

            async def connect():
                return None

            async def run(instruction, on_started, on_event):
                await on_started("turn")
                body(Path(cwd))
                worker.shutdown = True
                return "completed", {"answer": label}, {"total_tokens": 1, "complete": True}

            async def close():
                return None

            agent = SimpleNamespace(native_id="native", connect=connect, run=run, close=close)
            with (
                patch("agent_bridge.worker.runtime", return_value=agent),
                patch.object(worker.store, "begin_attempt", assign),
            ):
                await asyncio.wait_for(worker.run(), 10)
            self.assertEqual("completed", store.get_task(task_id)["status"])
            return task_id

        first = await run_task("task-one", lambda root: (root / "file.txt").write_text("first output\n"))
        first_task = await self.broker.call("agent_result", {"task_id": first})
        changes = json.loads(Path(first_task["result"]["files"]["changes"]).read_text())
        record = changes["file.txt"]
        retained = Path(record["path"])
        self.assertEqual(b"first output\n", retained.read_bytes())
        self.assertEqual(hashlib.sha256(b"first output\n").hexdigest(), record["sha256"])
        artifact_id = first_task["result"]["artifact"]["artifact_id"]

        async def assert_first_result_bytes():
            task = await self.broker.call("agent_result", {"task_id": first})
            saved = json.loads(Path(task["result"]["files"]["changes"]).read_text())["file.txt"]
            self.assertEqual(b"first output\n", Path(saved["path"]).read_bytes())
            full = await self.broker.call(
                "agent_result", {"task_id": first, "artifact_id": artifact_id, "limit": 65536},
            )
            saved = json.loads(Path(json.loads(full["content"])["files"]["changes"]).read_text())["file.txt"]
            self.assertEqual(b"first output\n", Path(saved["path"]).read_bytes())
            self.assertEqual(record["sha256"], saved["sha256"])

        await assert_first_result_bytes()

        def overwrite_and_delete(root):
            (root / "file.txt").write_text("second output\n")
            (root / "file.txt").unlink()

        await run_task("task-two", overwrite_and_delete)
        self.assertFalse((Path(cwd) / "file.txt").exists())
        await assert_first_result_bytes()

    async def test_failed_dispatch_closes_a_session_created_before_a_state_race(self):
        with patch.object(self.broker.store, "dispatch", side_effect=BridgeError("state_changed", "fixture")):
            with self.assertRaises(BridgeError):
                await request(self.state, "agent_dispatch", {"profile": "fixed", "cwd": str(self.state), "instruction": "task", "request_id": "race"})
        self.assertEqual(["closed"], [s["desired"] for s in self.broker.store.sessions()])

    async def test_open_name_is_recorded_and_existing_session_name_is_rejected(self):
        opened = await request(
            self.state,
            "agent_open",
            {
                "profile": "fixed",
                "cwd": str(self.state),
                "request_id": "named-open",
                "name": "  Integration Pipeline  ",
            },
        )
        self.assertEqual("Integration Pipeline", opened["name"])
        sid = opened["session_id"]
        self.assertEqual("Integration Pipeline", self.broker.store.get_session(sid)["name"])
        events = self.broker.store.events(session_id=sid)
        self.assertEqual(
            {"name": "Integration Pipeline"},
            next(event for event in events if event["kind"] == "session_opened")["data"],
        )
        with self.assertRaisesRegex(BridgeError, "Cannot set name on an existing session"):
            await request(
                self.state,
                "agent_dispatch",
                {"session_id": sid, "instruction": "task", "request_id": "named-dispatch", "name": "Other"},
            )

    async def test_dispatch_created_session_records_name_in_its_fingerprint(self):
        args = {
            "profile": "fixed",
            "cwd": str(self.state),
            "instruction": "task",
            "request_id": "named-dispatch-open",
            "name": "  Dispatch Named  ",
        }
        result = await request(self.state, "agent_dispatch", args)
        self.assertEqual("queued", result["status"])
        self.assertEqual("Dispatch Named", self.broker.store.get_session(result["session_id"])["name"])
        with self.assertRaisesRegex(BridgeError, "different API parameters"):
            await request(self.state, "agent_dispatch", {**args, "name": "Different"})

    async def test_omitted_name_and_explicit_none_share_a_request_fingerprint(self):
        args = {"profile": "fixed", "cwd": str(self.state), "instruction": "task", "request_id": "none-name"}
        first = await request(self.state, "agent_dispatch", args)
        self.assertEqual(first, await request(self.state, "agent_dispatch", {**args, "name": None}))

    async def test_explicit_access_must_match_but_omitted_access_inherits(self):
        opened = await request(self.state, "agent_open", {"profile": "fixed", "cwd": str(self.state), "access": "workspace-write", "workspace_mode": "copy", "snapshot_files": [], "request_id": "write"})
        sid = opened["session_id"]
        with self.assertRaisesRegex(BridgeError, "access cannot change"):
            await request(self.state, "agent_dispatch", {"session_id": sid, "instruction": "task", "access": "read-only", "request_id": "readonly"})
        for index, access in enumerate((None, "workspace-write")):
            result = await request(self.state, "agent_dispatch", {"session_id": sid, "instruction": "task", "access": access, "request_id": "inherit" + str(index)})
            self.assertEqual("queued", result["status"])

    async def test_retry_accepts_new_reservation_when_budget_was_enabled_later(self):
        store = self.broker.store
        sid = store.open(self.broker.config.profiles["fixed"], str(self.state), "read-only", "open")["session_id"]
        store.worker_ready(sid, "g", 123, "start", "native")
        tid = store.dispatch(sid, "task", "dispatch")["task_id"]
        aid = store.claim(self.broker.config)["attempt_id"]
        store.begin_attempt(sid, "g")
        store.finish(sid, "g", tid, aid, "failed", {"error": "fixture"}, {"total_tokens": 1, "complete": True})
        self.broker.config.token_budget = 100
        args = {"session_id": sid, "retry_task_id": tid, "instruction": "task", "request_id": "retry"}
        with self.assertRaisesRegex(BridgeError, "positive token reservation"):
            await request(self.state, "agent_dispatch", args)
        args.update(reservation=20, request_id="funded-retry")
        result = await request(self.state, "agent_dispatch", args)
        self.assertEqual(result, await request(self.state, "agent_dispatch", args))
        self.assertEqual(20, store.get_task(tid)["reservation"])
        self.assertEqual("ready", store.waiting_reason(tid, self.broker.config)["code"])

    async def test_invalid_parameters_are_repeatable_and_unexpected_errors_never_replay(self):
        args = {"requirements": "speed", "instruction": "task", "request_id": "bad-type"}
        for _ in range(2):
            with self.assertRaisesRegex(BridgeError, "requirements must be an object"):
                await request(self.state, "agent_dispatch", args)
        with patch.object(self.broker, "call", new_callable=AsyncMock, side_effect=RuntimeError("result lost")) as call:
            for _ in range(2):
                with self.assertRaisesRegex(BridgeError, "Result not confirmed"):
                    await request(self.state, "agent_cancel", {"task_id": "task", "request_id": "uncertain"})
            self.assertEqual(1, call.await_count)
        with self.broker.store.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM api_requests WHERE response IS NULL").fetchone()[0])

    async def test_max_length_request_id_can_auto_open_a_session(self):
        result = await request(self.state, "agent_dispatch", {"profile": "fixed", "cwd": str(self.state), "instruction": "task", "request_id": "a" * 200})
        self.assertEqual("queued", result["status"])


class WorkspaceAndRoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root / "state")

    def test_parallel_worktrees_preserve_dirty_source(self):
        source = self.root / "source"
        source.mkdir()
        subprocess.run(["git", "init", "-q", str(source)], check=True)
        (source / "file.txt").write_text("committed")
        subprocess.run(["git", "-C", str(source), "add", "file.txt"], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(source),
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            check=True,
        )
        (source / "file.txt").write_text("dirty")
        (source / "private.txt").write_text("untracked")
        first, _ = worktree(self.store.state, source, "first")
        second, _ = worktree(self.store.state, source, "second")
        self.assertEqual("committed", (Path(first) / "file.txt").read_text())
        (Path(first) / "file.txt").write_text("changed")
        self.assertEqual("committed", (Path(second) / "file.txt").read_text())
        self.assertEqual("dirty", (source / "file.txt").read_text())
        self.assertEqual("untracked", (source / "private.txt").read_text())
        self.assertEqual(first, worktree(self.store.state, source, "first")[0])

    def test_requirements_filter_and_unknown_estimates(self):
        profile = Profile(
            "fixed", "codex", "m", provider="allowed", verified=True, executable=sys.executable, context_window=100
        )
        config = Config({"fixed": profile})
        for req in (
            {"providers": ["forbidden"]},
            {"input_types": ["image"]},
            {"tools": ["unsupported"]},
            {"input_tokens": 101},
        ):
            with self.assertRaisesRegex(BridgeError, "No eligible"):
                choose(config, self.store, req)
        selection = choose(config, self.store, {})
        self.assertIn("Unmeasured", selection["reason"])
        self.assertNotIn("score", selection["candidates"][0])
        self.assertTrue(choose(config, self.store, {"execute_locally": True})["execute_locally"])

    def test_cross_model_retry_keeps_quality_with_the_model_that_produced_it(self):
        profiles = {name: Profile(name, "codex", name, verified=True, executable=sys.executable) for name in ("a", "b")}
        config = Config(profiles)
        sessions = {}
        for name, profile in profiles.items():
            sid = self.store.open(profile, str(self.root), "read-only", name)["session_id"]
            self.store.worker_ready(sid, "g", 123, "start", name)
            sessions[name] = sid
        tid = self.store.dispatch(sessions["a"], "task", "dispatch")["task_id"]
        aid = self.store.claim(config)["attempt_id"]
        self.store.begin_attempt(sessions["a"], "g")
        self.store.finish(sessions["a"], "g", tid, aid, "failed", {"error": "A failed"}, {"total_tokens": 10, "complete": True})
        self.store.retry(tid, sessions["b"], "task", "retry")
        bid = self.store.claim(config)["attempt_id"]
        self.store.begin_attempt(sessions["b"], "g")
        self.store.finish(sessions["b"], "g", tid, bid, "completed", {"answer": "B passed"}, {"total_tokens": 20, "complete": True})
        self.store.review(tid, bid, "accepted", "checked B", "review", 1)
        for name, quality in (("a", False), ("b", True)):
            samples = task_samples(self.store, profiles[name], "general")
            self.assertEqual([quality], [s["quality"] for s in samples])
            self.assertIsNone(samples[0]["tokens"])
        self.assertEqual("b", choose(config, self.store, {})["profile"])
        self.assertEqual(30, self.store.budget()["reported_tokens"])

    def test_unstarted_cross_model_retry_does_not_erase_the_original_failure(self):
        profiles = {name: Profile(name, "codex", name, verified=True, executable=sys.executable) for name in ("a", "b")}
        config = Config(profiles)
        sessions = {}
        for name, profile in profiles.items():
            sid = self.store.open(profile, str(self.root), "read-only", name)["session_id"]
            self.store.worker_ready(sid, "g", 123, "start", name)
            sessions[name] = sid
        tid = self.store.dispatch(sessions["a"], "task", "dispatch")["task_id"]
        aid = self.store.claim(config)["attempt_id"]
        self.store.begin_attempt(sessions["a"], "g")
        self.store.finish(sessions["a"], "g", tid, aid, "failed", {"error": "A failed"}, {"total_tokens": 10, "complete": True})
        self.store.retry(tid, sessions["b"], "task", "retry")
        for state in ("queued", "assigned", "cancelled"):
            if state == "assigned":
                self.store.claim(config)
            elif state == "cancelled":
                self.store.cancel(tid, "cancel")
            with self.subTest(state=state):
                self.assertEqual(state, self.store.get_task(tid)["status"])
                samples = task_samples(self.store, profiles["a"], "general")
                self.assertEqual([False], [s["quality"] for s in samples])
                self.assertIsNone(samples[0]["tokens"])
                self.assertEqual([], task_samples(self.store, profiles["b"], "general"))
                self.assertEqual("b", choose(config, self.store, {})["profile"])

    def test_balanced_cost_speed_and_quality_filter_use_comparable_totals(self):
        profiles = {
            name: Profile(name, "codex", name, verified=True, executable=sys.executable)
            for name in ("cheap", "fast", "worse", "bad")
        }
        config = Config(profiles)
        values = {"cheap": (10, 20, True), "fast": (30, 2, True), "worse": (40, 40, True), "bad": (1, 1, False)}

        def samples(store, profile, kind):
            tokens, seconds, quality = values[profile.name]
            return [{"tokens": tokens, "seconds": seconds, "quality": quality, "usd": None}]

        with patch("agent_bridge.routing.task_samples", samples):
            self.assertEqual("fast", choose(config, self.store, {})["profile"])
            self.assertEqual("cheap", choose(config, self.store, {"strategy": "cost"})["profile"])
            fast = choose(config, self.store, {"strategy": "speed"})
            self.assertEqual("fast", fast["profile"])
            self.assertEqual("bad", fast["excluded"][0]["profile"])
            self.assertNotIn("score", next(c for c in fast["candidates"] if c["profile"] == "worse"))
