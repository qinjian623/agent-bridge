import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from agent_bridge import groups
from agent_bridge.broker import Broker, request
from agent_bridge.errors import BridgeError
from agent_bridge.mcp_server import server as mcp_server
from agent_bridge.peer_protocol import PROTOCOL_VERSION, HostState, NodeRef
from agent_bridge.peer_transport import DefaultPeerClientFactory


def host(name, **changes):
    value = {
        "ssh_target": name + ".internal",
        "remote_executable": "/usr/local/bin/agent-bridge",
        "pinned": {"node_id": "node-" + name, "state_generation": "generation-" + name},
    }
    value.update(changes)
    return value


class FakeFactory:
    def __init__(self):
        self.retire = AsyncMock()
        self.release_retired = Mock()
        self.client = Mock(side_effect=AssertionError("Configuration reload must not send peer requests"))


class CloseOnceTransport:
    def __init__(self, config, number, trace):
        self.node = config.pinned.as_dict()
        self.number, self.trace = number, trace
        self.fail_close = number == 1
        self.closed = False

    async def call(self, request, timeout):
        assert not self.closed, "Closed transport was reused"
        self.trace.append((self.number, request["method"]))
        if request["method"] == "peer.hello":
            result = {"protocol_version": PROTOCOL_VERSION, "node": self.node, "node_state": "ready"}
        else:
            assert request["method"] == "peer.inspect" and request["params"] == {"kind": "session"}
            result = {"items": [{"id": "closed-session", "desired": "closed", "observed": "stopped"}]}
        return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": result}

    async def close(self):
        if self.fail_close:
            self.fail_close = False
            self.trace.append((self.number, "close-failed"))
            raise OSError("first close failed")
        self.closed = True
        self.trace.append((self.number, "closed"))


class DynamicHostTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.state = Path(temp.name)
        self.payload = {
            "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
            "worker_launcher": "detached",
            "telegram_database": None,
            "hosts": {"gpu-a": host("gpu-a")},
        }
        self.write_hosts(self.payload["hosts"])
        self.factory = FakeFactory()
        self.broker = Broker(self.state, peer_factory=self.factory)

    def write_hosts(self, hosts):
        self.payload["hosts"] = hosts
        (self.state / "config.json").write_text(json.dumps(self.payload))

    def assert_changes(self, result, *, added=(), updated=(), removed=()):
        self.assertEqual(sorted(added), sorted(result["added"]))
        self.assertEqual(sorted(updated), sorted(result["updated"]))
        self.assertEqual(sorted(removed), sorted(result["removed"]))

    def retired_hosts(self):
        return {host_id for call in self.factory.retire.await_args_list for host_id in call.args[0]}

    async def wait_for(self, predicate):
        async with asyncio.timeout(3):
            while not predicate():
                await asyncio.sleep(0.01)

    def inspect_routes(self, *, sessions=(), tasks=None, offline=False):
        tasks = tasks or {}
        client = Mock()
        node = {"host_id": "gpu-a", "node_id": "node-gpu-a", "state_generation": "generation-gpu-a"}
        client.hello = AsyncMock(return_value={"node": node, "node_state": "ready", "protocol_version": 1})

        async def inspect(params):
            if offline:
                raise BridgeError("peer_host_offline", "offline fixture")
            if params["kind"] == "node":
                return {"items": [{**node, "node_state": "ready", "protocol_version": 1}]}
            if params["kind"] == "session":
                return {"items": list(sessions)}
            task = tasks.get(params.get("id"))
            return {"items": [] if task is None else [task]}

        client.inspect = AsyncMock(side_effect=inspect)
        self.factory.client.side_effect = None
        self.factory.client.return_value = client
        return client

    def use_close_failure_factory(self):
        transports, trace = [], []

        def transport(config):
            result = CloseOnceTransport(config, len(transports) + 1, trace)
            transports.append(result)
            return result

        factory = DefaultPeerClientFactory(self.broker.host_registry, transport_factory=transport)
        for component in (self.broker, self.broker.delivery_scheduler, self.broker.outbox_puller, self.broker.event_mirror):
            component.peer_factory = factory
        self.addAsyncCleanup(factory.close)
        self.broker.store.register_parent_route("gpu-a", "node-gpu-a", "generation-gpu-a", "closed-session", {
            "host_id": "local", "node_id": "parent-node", "state_generation": "parent-generation", "local_id": "parent",
        })
        return factory, transports, trace

    async def test_add_remove_are_visible_without_replacing_broker_or_shared_objects(self):
        registry, config, factory = self.broker.host_registry, self.broker.config, self.broker.peer_factory
        registry.mark("gpu-a", HostState.ONLINE)
        previous_status = registry.status("gpu-a")[0]
        self.write_hosts({"gpu-a": host("gpu-a"), "gpu-b": host("gpu-b")})

        added = await self.broker.call("agent_hosts_reload", {})
        self.assert_changes(added, added=["gpu-b"])
        self.assertIs(registry, self.broker.host_registry)
        self.assertIs(config, self.broker.config)
        self.assertIs(factory, self.broker.peer_factory)
        self.assertIs(previous_status, registry.status("gpu-a")[0])
        self.assertEqual({"gpu-a", "gpu-b"}, set(config.hosts))
        self.assertEqual({"gpu-a", "gpu-b"}, {item["host_id"] for item in (await self.broker.call("agent_hosts", {}))["hosts"]})
        self.assertEqual({"gpu-a", "gpu-b"}, {item["host_id"] for item in (await self.broker.call("agent_monitor", {}))["hosts"]})
        self.assertNotIn("gpu-a", self.retired_hosts())

        self.write_hosts({"gpu-a": host("gpu-a")})
        self.assert_changes(await self.broker.reload_hosts(), removed=["gpu-b"])
        self.assertEqual({"gpu-b"}, self.retired_hosts())
        self.assertEqual(["gpu-a"], [item["host_id"] for item in (await self.broker.call("agent_hosts", {}))["hosts"]])
        self.factory.client.assert_not_called()

    async def test_repeated_reload_does_not_retire_connections_or_change_status(self):
        self.broker.host_registry.mark("gpu-a", HostState.ONLINE)
        previous_status = self.broker.host_registry.status("gpu-a")[0]
        self.assert_changes(await self.broker.reload_hosts())
        self.assert_changes(await self.broker.reload_hosts())
        self.assertEqual(set(), self.retired_hosts())
        self.assertIs(previous_status, self.broker.host_registry.status("gpu-a")[0])

    async def test_reload_with_host_filter_never_waits_for_its_own_host_lock(self):
        for args in ({"host_id": "gpu-a"}, {"remote_members": [{"host_id": "gpu-a"}]}):
            with self.subTest(args=args):
                self.write_hosts({"gpu-a": host("gpu-a")})
                await self.broker.reload_hosts()
                self.write_hosts({})
                try:
                    result = await asyncio.wait_for(self.broker.call("agent_hosts_reload", args), 0.5)
                except BridgeError as error:
                    self.assertEqual("invalid_request", error.code)
                    self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
                else:
                    self.assert_changes(result, removed=["gpu-a"])
                self.assertEqual(set(), self.broker._hosts_changing)

    async def test_change_retires_only_changed_host_and_updates_shared_config(self):
        self.write_hosts({"gpu-a": host("gpu-a"), "gpu-b": host("gpu-b")})
        await self.broker.reload_hosts()
        self.factory.retire.reset_mock()
        self.factory.release_retired.reset_mock()
        self.broker.host_registry.mark("gpu-b", HostState.ONLINE)
        untouched_status = self.broker.host_registry.status("gpu-b")[0]
        self.write_hosts({"gpu-a": host("gpu-a", ssh_target="new-gpu-a.internal"), "gpu-b": host("gpu-b")})

        self.assert_changes(await self.broker.reload_hosts(), updated=["gpu-a"])
        self.assertEqual({"gpu-a"}, self.retired_hosts())
        self.assertEqual("new-gpu-a.internal", self.broker.config.hosts["gpu-a"].ssh_target)
        self.assertEqual("new-gpu-a.internal", self.broker.host_registry.get("gpu-a").ssh_target)
        self.assertIs(untouched_status, self.broker.host_registry.status("gpu-b")[0])
        self.assertEqual({"gpu-a"}, {item for call in self.factory.release_retired.call_args_list for item in call.args[0]})

    async def test_invalid_config_never_removes_old_hosts_or_retires_clients(self):
        self.broker.host_registry.mark("gpu-a", HostState.ONLINE)
        previous_status = self.broker.host_registry.status("gpu-a")[0]
        cached_node = NodeRef("gpu-a", "node-gpu-a", "generation-gpu-a")
        self.broker.outbox_puller._nodes["gpu-a"] = cached_node
        for bad in ("{broken", json.dumps({**self.payload, "hosts": {"gpu-b": {"ssh_target": "missing-executable"}}})):
            with self.subTest(config=bad):
                (self.state / "config.json").write_text(bad)
                with self.assertRaises(BridgeError):
                    await self.broker.reload_hosts()
                self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
                self.assertEqual({"gpu-a"}, set(self.broker.config.hosts))
                self.assertIs(previous_status, self.broker.host_registry.status("gpu-a")[0])
                self.assertIs(cached_node, self.broker.outbox_puller._nodes["gpu-a"])
                self.assertEqual(set(), self.retired_hosts())

    async def test_pin_file_change_is_loaded_without_replacing_registry(self):
        registry = self.broker.host_registry
        (self.state / "host_pins.json").write_text(json.dumps({"version": 1, "hosts": {
            "gpu-a": {"host_id": "gpu-a", "node_id": "replacement-node", "state_generation": "replacement-generation"},
        }}))

        self.assert_changes(await self.broker.reload_hosts(), updated=["gpu-a"])
        self.assertIs(registry, self.broker.host_registry)
        self.assertEqual(NodeRef("gpu-a", "replacement-node", "replacement-generation"), registry.get("gpu-a").pinned)

    async def test_active_remote_group_prevents_partial_batch_removal(self):
        groups.control_group(self.broker.store, "team", "create", 0, "create-team", remote_members=[{
            "host_id": "gpu-a", "node_id": "node-gpu-a", "state_generation": "generation-gpu-a",
            "target_kind": "bridge", "session_id": "remote-session",
        }])
        self.write_hosts({"gpu-b": host("gpu-b")})

        with self.assertRaises(BridgeError) as caught:
            await self.broker.reload_hosts()
        self.assertEqual("host_in_use", caught.exception.code)
        self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
        self.assertEqual(set(), self.retired_hosts())
        self.assertEqual("remote-session", groups.inspect_groups(self.broker.store, "team")["remote_members"][0]["target_id"])

    async def test_unsettled_operation_prevents_endpoint_change_without_replaying_it(self):
        self.broker.store.record_operation(
            "unconfirmed-dispatch", "node-gpu-a", "generation-gpu-a", "fingerprint", "peer.deliver",
            {"host_id": "gpu-a"}, status="uncertain",
        )
        self.write_hosts({"gpu-a": host("gpu-a", ssh_target="replacement.internal")})
        with self.assertRaises(BridgeError) as caught:
            await self.broker.reload_hosts()
        self.assertEqual("host_in_use", caught.exception.code)
        self.assertEqual("gpu-a.internal", self.broker.host_registry.get("gpu-a").ssh_target)
        self.assertEqual("uncertain", self.broker.store.get_operation("unconfirmed-dispatch")["status"])
        self.factory.client.assert_not_called()

    async def test_legacy_operation_without_host_name_still_blocks_its_pinned_node(self):
        self.broker.store.record_operation(
            "legacy-unconfirmed-dispatch", "node-gpu-a", "generation-gpu-a", "fingerprint", "peer.deliver",
            {"operation": {"node_id": "node-gpu-a", "state_generation": "generation-gpu-a"}}, status="uncertain",
        )
        self.write_hosts({})
        with self.assertRaises(BridgeError) as caught:
            await self.broker.reload_hosts()
        self.assertEqual("host_in_use", caught.exception.code)
        self.assertEqual("uncertain", self.broker.store.get_operation("legacy-unconfirmed-dispatch")["status"])
        self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
        self.assertEqual(set(), self.retired_hosts())

    async def test_pending_delivery_prevents_removal_after_member_leaves_group(self):
        member = {
            "host_id": "gpu-a", "node_id": "node-gpu-a", "state_generation": "generation-gpu-a",
            "target_kind": "bridge", "session_id": "remote-session",
        }
        groups.control_group(self.broker.store, "team", "create", 0, "create-team", remote_members=[member])
        broadcast = groups.broadcast(self.broker.store, "team", "test notice", 0, "queued-notice")
        groups.control_group(self.broker.store, "team", "remove", 0, "remove-member", remote_members=[member])
        with self.broker.store.connection() as db:
            before = dict(db.execute("SELECT * FROM peer_outbox").fetchone())
        self.write_hosts({})

        with self.assertRaises(BridgeError) as caught:
            await self.broker.reload_hosts()
        self.assertEqual("host_in_use", caught.exception.code)
        with self.broker.store.connection() as db:
            self.assertEqual(before, dict(db.execute("SELECT * FROM peer_outbox").fetchone()))
        self.assertEqual("pending", groups.broadcast_status(self.broker.store, broadcast["broadcast_id"])["recipients"][0]["status"])
        self.assertEqual(set(), self.retired_hosts())

    async def test_parent_notification_route_prevents_removal_without_losing_route(self):
        parent = {"host_id": "local", "node_id": "parent-node", "state_generation": "parent-generation", "local_id": "parent"}
        self.broker.store.register_parent_route("gpu-a", "node-gpu-a", "generation-gpu-a", "remote-task", parent)
        self.inspect_routes(tasks={"remote-task": {"id": "remote-task", "status": "running", "validation": "pending"}})
        self.write_hosts({})
        with self.assertRaises(BridgeError) as caught:
            await self.broker.reload_hosts()
        self.assertEqual("host_in_use", caught.exception.code)
        self.assertEqual(parent, self.broker.store.lookup_parent_route("gpu-a", "node-gpu-a", "generation-gpu-a", "remote-task")["parent_ref"])
        self.assertEqual(set(), self.retired_hosts())

    async def test_reviewed_task_and_closed_session_allow_removal_but_preserve_history(self):
        parent = {"host_id": "local", "node_id": "parent-node", "state_generation": "parent-generation", "local_id": "parent"}
        for child in ("closed-session", "reviewed-task"):
            self.broker.store.register_parent_route("gpu-a", "node-gpu-a", "generation-gpu-a", child, parent)
        client = self.inspect_routes(
            sessions=[{"id": "closed-session", "desired": "closed", "observed": "stopped"}],
            tasks={"reviewed-task": {"id": "reviewed-task", "status": "completed", "validation": "accepted"}},
        )
        self.write_hosts({})

        self.assert_changes(await self.broker.reload_hosts(), removed=["gpu-a"])
        self.assertEqual([], self.broker.host_registry.list())
        for child in ("closed-session", "reviewed-task"):
            self.assertEqual(parent, self.broker.store.lookup_parent_route("gpu-a", "node-gpu-a", "generation-gpu-a", child)["parent_ref"])
        client.deliver.assert_not_called()
        client.control.assert_not_called()
        client.cancel.assert_not_called()

    async def test_completed_but_unreviewed_route_cannot_be_removed(self):
        parent = {"host_id": "local", "node_id": "parent-node", "state_generation": "parent-generation", "local_id": "parent"}
        self.broker.store.register_parent_route("gpu-a", "node-gpu-a", "generation-gpu-a", "unreviewed-task", parent)
        self.inspect_routes(tasks={"unreviewed-task": {"id": "unreviewed-task", "status": "completed", "validation": "pending"}})
        self.write_hosts({})

        with self.assertRaises(BridgeError) as caught:
            await self.broker.reload_hosts()
        self.assertEqual("host_in_use", caught.exception.code)
        self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
        self.assertEqual(set(), self.retired_hosts())

    async def test_offline_route_check_preserves_registration_and_history(self):
        parent = {"host_id": "local", "node_id": "parent-node", "state_generation": "parent-generation", "local_id": "parent"}
        self.broker.store.register_parent_route("gpu-a", "node-gpu-a", "generation-gpu-a", "remote-task", parent)
        self.inspect_routes(offline=True)
        self.write_hosts({})

        with self.assertRaises(BridgeError) as caught:
            await self.broker.reload_hosts()
        self.assertEqual("host_in_use", caught.exception.code)
        self.assertEqual(parent, self.broker.store.lookup_parent_route("gpu-a", "node-gpu-a", "generation-gpu-a", "remote-task")["parent_ref"])
        self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
        self.assertEqual(set(), self.retired_hosts())

    async def test_failed_close_is_finished_before_rechecking_routes_and_removing_host(self):
        factory, transports, trace = self.use_close_failure_factory()
        factory.client("gpu-a")
        self.write_hosts({})
        with self.assertRaisesRegex(OSError, "first close failed"):
            await self.broker.reload_hosts()
        self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
        with self.assertRaises(BridgeError) as frozen:
            factory.client("gpu-a")
        self.assertEqual("host_busy", frozen.exception.code)

        self.assert_changes(await self.broker.reload_hosts(), removed=["gpu-a"])
        self.assertEqual(2, len(transports))
        self.assertLess(trace.index((1, "closed")), trace.index((2, "peer.hello")))
        self.assertTrue(all(item.closed for item in transports))
        self.assertEqual(set(), self.broker._hosts_changing)
        self.assertIsNone(self.broker.host_reload_error)
        self.assertIsNotNone(self.broker.store.lookup_parent_route(
            "gpu-a", "node-gpu-a", "generation-gpu-a", "closed-session",
        ))

    async def test_reverted_config_unfreezes_original_host_after_failed_close(self):
        factory, transports, trace = self.use_close_failure_factory()
        previous_client = factory.client("gpu-a")
        self.write_hosts({})
        with self.assertRaisesRegex(OSError, "first close failed"):
            await self.broker.reload_hosts()
        self.write_hosts({"gpu-a": host("gpu-a")})

        self.assert_changes(await self.broker.reload_hosts())
        restored_client = factory.client("gpu-a")
        self.assertIsNot(previous_client, restored_client)
        inspected = await restored_client.inspect({"kind": "session"})
        self.assertEqual("closed-session", inspected["items"][0]["id"])
        self.assertTrue(transports[0].closed)
        self.assertLess(trace.index((1, "closed")), trace.index((2, "peer.hello")))
        self.assertEqual(set(), self.broker._hosts_changing)
        self.assertIsNone(self.broker.host_reload_error)
        self.assertEqual("gpu-a.internal", self.broker.host_registry.get("gpu-a").ssh_target)

    async def test_reload_clears_only_changed_host_polling_caches(self):
        self.write_hosts({"gpu-a": host("gpu-a"), "gpu-b": host("gpu-b")})
        await self.broker.reload_hosts()
        for name in ("gpu-a", "gpu-b"):
            node = NodeRef(name, "node-" + name, "generation-" + name)
            self.broker.outbox_puller._nodes[name] = node
            self.broker.outbox_puller._temp_after[name] = 12
            self.broker.outbox_puller._scan_upper[name] = 21
            self.broker.event_mirror._nodes[name] = node
            self.broker.event_mirror._paused.add(name)
        self.write_hosts({"gpu-a": host("gpu-a", ssh_target="replacement.internal", pinned={
            "node_id": "replacement-node", "state_generation": "replacement-generation",
        }), "gpu-b": host("gpu-b")})

        await self.broker.reload_hosts()
        for cached in (
            self.broker.outbox_puller._nodes, self.broker.outbox_puller._temp_after,
            self.broker.outbox_puller._scan_upper, self.broker.event_mirror._nodes,
        ):
            self.assertNotIn("gpu-a", cached)
            self.assertIn("gpu-b", cached)
        self.assertEqual({"gpu-b"}, self.broker.event_mirror._paused)
        self.assertEqual(12, self.broker.outbox_puller._temp_after["gpu-b"])
        self.assertEqual(21, self.broker.outbox_puller._scan_upper["gpu-b"])

    async def test_same_node_ssh_change_does_not_clear_event_conflict_pause(self):
        self.broker.event_mirror._paused.add("gpu-a")
        self.write_hosts({"gpu-a": host("gpu-a", ssh_target="replacement.internal")})
        await self.broker.reload_hosts()
        self.assertEqual({"gpu-a"}, self.broker.event_mirror._paused)

    async def test_removed_then_readded_identity_does_not_inherit_old_mirror_pause(self):
        self.broker.event_mirror._paused.add("gpu-a")
        self.write_hosts({})
        await self.broker.reload_hosts()
        self.write_hosts({"gpu-a": host("gpu-a", pinned={
            "node_id": "replacement-node", "state_generation": "replacement-generation",
        })})
        self.assert_changes(await self.broker.reload_hosts(), added=["gpu-a"])
        self.assertNotIn("gpu-a", self.broker.event_mirror._paused)
        client = Mock(events=AsyncMock(return_value={"items": [], "cursor": 0, "has_more": False}))
        self.factory.client.side_effect = None
        self.factory.client.return_value = client
        self.assertEqual(0, await self.broker.event_mirror.run_once("gpu-a"))
        client.events.assert_awaited_once()

    async def test_removed_host_poll_loops_exit_without_waiting_full_interval(self):
        for component in (self.broker.outbox_puller, self.broker.event_mirror):
            with self.subTest(component=type(component).__name__):
                self.write_hosts({"gpu-a": host("gpu-a", pull_interval=30)})
                await self.broker.reload_hosts()
                polled = asyncio.Event()

                async def run_once(host_id, polled=polled):
                    polled.set()
                    return 0

                with patch.object(component, "run_once", new=AsyncMock(side_effect=run_once)) as run:
                    task = asyncio.create_task(component._host_loop(self.broker.host_registry.get("gpu-a")))
                    try:
                        await asyncio.wait_for(polled.wait(), 1)
                        self.write_hosts({})
                        await self.broker.reload_hosts()
                        await asyncio.wait_for(task, 1)
                        run.assert_awaited_once_with("gpu-a")
                    finally:
                        if not task.done():
                            task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

    async def test_reload_waits_for_complete_poller_ack_before_retiring_connection(self):
        ack_started, release_ack = asyncio.Event(), asyncio.Event()
        trace = []

        async def acknowledge(params):
            trace.append("ack-started")
            ack_started.set()
            await release_ack.wait()
            trace.append("ack-completed")
            return {"operation_id": params["operation_id"], "status": "acked"}

        async def retire(host_ids):
            trace.append("retired")

        client = Mock()
        client.pull_requests = AsyncMock(return_value={"items": [{
            "seq": 1, "operation_id": "old-poll-request", "method": "unsupported", "payload": {},
        }], "cursor": 1, "has_more": False, "scan_upper_seq": 1})
        client.ack_request = AsyncMock(side_effect=acknowledge)
        self.factory.client.side_effect = None
        self.factory.client.return_value = client
        self.factory.retire.side_effect = retire
        poller = asyncio.create_task(self.broker.outbox_puller.run_once("gpu-a"))
        refresh = None
        try:
            await asyncio.wait_for(ack_started.wait(), 1)
            self.write_hosts({})
            refresh = asyncio.create_task(self.broker.reload_hosts())
            await self.wait_for(lambda: "gpu-a" in self.broker._hosts_changing)
            self.assertFalse(refresh.done())
            self.assertEqual("gpu-a", self.broker.host_registry.get("gpu-a").host_id)
            self.factory.retire.assert_not_awaited()

            release_ack.set()
            self.assertEqual(1, await asyncio.wait_for(poller, 1))
            self.assert_changes(await asyncio.wait_for(refresh, 1), removed=["gpu-a"])
            receipt = self.broker.store.get_pulled_request_receipt(
                "gpu-a", "node-gpu-a", "generation-gpu-a", "old-poll-request",
            )
            self.assertIsNotNone(receipt["acked_at"])
            self.assertEqual(["ack-started", "ack-completed", "retired"], trace)
            client.ack_request.assert_awaited_once()
        finally:
            release_ack.set()
            await asyncio.gather(*(task for task in (poller, refresh) if task is not None), return_exceptions=True)

    async def test_config_changed_while_waiting_for_poll_is_revalidated_before_applying(self):
        lock = self.broker.outbox_puller._poll_locks.setdefault("gpu-a", asyncio.Lock())
        await lock.acquire()
        self.write_hosts({"gpu-a": host("gpu-a", ssh_target="first-replacement.internal")})
        refresh = asyncio.create_task(self.broker.reload_hosts())
        try:
            await self.wait_for(lambda: "gpu-a" in self.broker._hosts_changing)
            self.write_hosts({"gpu-a": host("gpu-a", ssh_target="latest-replacement.internal")})
        finally:
            lock.release()
        with self.assertRaises(BridgeError) as caught:
            await asyncio.wait_for(refresh, 1)
        self.assertEqual("config_changed", caught.exception.code)
        self.assertEqual("gpu-a.internal", self.broker.host_registry.get("gpu-a").ssh_target)
        self.assertEqual(set(), self.retired_hosts())
        self.assertEqual(set(), self.broker._hosts_changing)

        self.assert_changes(await self.broker.reload_hosts(), updated=["gpu-a"])
        self.assertEqual("latest-replacement.internal", self.broker.host_registry.get("gpu-a").ssh_target)

    async def test_reload_waits_for_whole_multistep_api_without_interrupting_dispatch(self):
        uploading, release_upload = asyncio.Event(), asyncio.Event()
        trace = []
        client = Mock()
        client.workspace = AsyncMock(return_value={})

        async def prepare(host_id, peer, node, args):
            await peer.workspace({"action": "first-chunk"})
            uploading.set()
            await release_upload.wait()
            await peer.workspace({"action": "last-chunk"})
            trace.append("uploaded")
            return args

        async def deliver(params):
            trace.append("delivered")
            return {"session_id": "new-remote-session", "status": "opened"}

        async def retire(host_ids):
            trace.append("retired")

        client.deliver = AsyncMock(side_effect=deliver)
        self.factory.client.side_effect = None
        self.factory.client.return_value = client
        self.factory.retire.side_effect = retire
        refresh = None
        with patch.object(self.broker, "_prepare_remote_workspace", new=AsyncMock(side_effect=prepare)):
            opened = asyncio.create_task(self.broker.call("agent_open", {
                "host_id": "gpu-a", "profile": "fixture", "cwd": str(self.state),
                "access": "workspace-write", "request_id": "multistep-open",
            }))
            try:
                await asyncio.wait_for(uploading.wait(), 1)
                self.write_hosts({})
                refresh = asyncio.create_task(self.broker.reload_hosts())
                await self.wait_for(lambda: "gpu-a" in self.broker._hosts_changing)
                self.assertFalse(refresh.done())
                self.factory.retire.assert_not_awaited()
                with self.assertRaises(BridgeError) as caught:
                    await self.broker.call("agent_catalog", {"host_id": "gpu-a"})
                self.assertEqual("peer_host_offline", caught.exception.code)

                release_upload.set()
                result = await asyncio.wait_for(opened, 1)
                self.assertEqual("new-remote-session", result["session_id"])
                self.assertEqual("completed", self.broker.store.get_operation(result["operation_id"])["status"])
                self.assert_changes(await asyncio.wait_for(refresh, 1), removed=["gpu-a"])
                self.assertEqual(["uploaded", "delivered", "retired"], trace)
                self.assertEqual(2, client.workspace.await_count)
                client.deliver.assert_awaited_once()
            finally:
                release_upload.set()
                await asyncio.gather(*(task for task in (opened, refresh) if task is not None), return_exceptions=True)

    async def test_watcher_reports_invalid_config_then_recovers_without_restart(self):
        watcher = asyncio.create_task(self.broker.watch_hosts())
        try:
            (self.state / "config.json").write_text("{broken")
            await self.wait_for(lambda: bool(self.broker.host_reload_error))
            self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
            self.assertTrue((await self.broker.call("agent_hosts", {}))["config_error"])

            self.write_hosts({"gpu-a": host("gpu-a"), "gpu-b": host("gpu-b")})
            await self.wait_for(lambda: len(self.broker.host_registry.list()) == 2)
            self.assertIsNone(self.broker.host_reload_error)
            self.assertIsNone((await self.broker.call("agent_hosts", {}))["config_error"])
        finally:
            self.broker.stopping.set()
            await asyncio.wait_for(watcher, 1)

    async def test_watcher_recovers_after_unexpected_refresh_exception(self):
        original_read = self.broker.host_registry.read_configured_hosts
        failed = False

        def read():
            nonlocal failed
            if not failed:
                failed = True
                raise RuntimeError("temporary configuration read failure")
            return original_read()

        with patch.object(self.broker.host_registry, "read_configured_hosts", side_effect=read):
            watcher = asyncio.create_task(self.broker.watch_hosts())
            try:
                await self.wait_for(lambda: bool(self.broker.host_reload_error))
                self.assertFalse(watcher.done())
                self.assertEqual(["gpu-a"], [item.host_id for item in self.broker.host_registry.list()])
                self.write_hosts({"gpu-a": host("gpu-a"), "gpu-b": host("gpu-b")})
                await self.wait_for(lambda: len(self.broker.host_registry.list()) == 2)
                self.assertIsNone(self.broker.host_reload_error)
            finally:
                self.broker.stopping.set()
                await asyncio.wait_for(watcher, 1)

    async def test_mcp_exposes_reload_but_child_cannot_call_admin_socket(self):
        tools = {tool.name: tool for tool in await mcp_server(self.state).list_tools()}
        self.assertIn("agent_hosts_reload", list(tools))
        self.assertFalse(tools["agent_hosts_reload"].annotations and tools["agent_hosts_reload"].annotations.readOnlyHint)
        self.assertEqual({}, tools["agent_hosts_reload"].inputSchema.get("properties", {}))
        self.assertTrue(tools["agent_hosts"].annotations.readOnlyHint)
        with patch.dict(os.environ, {"AGENT_BRIDGE_ROLE": "child"}), patch(
            "agent_bridge.broker.asyncio.open_unix_connection", new_callable=AsyncMock,
        ) as connection:
            with self.assertRaises(BridgeError) as caught:
                await request(self.state, "agent_hosts_reload", {})
            self.assertEqual("forbidden", caught.exception.code)
            connection.assert_not_awaited()
