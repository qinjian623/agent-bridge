import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from agent_bridge.errors import BridgeError
from agent_bridge.hosts import ConfigHostRegistry, HostConfig
from agent_bridge.peer_protocol import MAX_FRAME_BYTES, PROTOCOL_VERSION, HostState, NodeRef, PeerErrorCode
from agent_bridge.peer_transport import (
    DEFAULT_PEER_TIMEOUT,
    DefaultPeerClientFactory,
    DirectPeerClient,
    SshPeerTransport,
)

OPERATION = {"node_id": "node-a", "state_generation": "gen-a", "operation_id": "op-1"}

HELLO_PARAMS = {"protocol_versions": [1], "coordinator_id": "coordinator", "coordinator_generation": "gen-c"}

METHOD_CASES = (
    ("hello", HELLO_PARAMS),
    ("catalog", {"refresh": True}),
    ("inspect", {"kind": "node"}),
    ("pull_requests", {"after": 3, "limit": 5}),
    ("ack_request", {"operation_id": "op-1", "outcome": "accepted", "result": {}}),
    (
        "deliver",
        {
            "operation": OPERATION,
            "delivery_id": "delivery-1",
            "group_id": "group-1",
            "broadcast_id": "broadcast-1",
            "target": {"kind": "bridge", "id": "session-1"},
            "target_seq": 1,
            "seq_kind": "message",
            "message": "run",
            "expected_group_revision": 0,
        },
    ),
    ("lookup_delivery", {"delivery_id": "delivery-1"}),
    ("control", {"operation": OPERATION, "session_id": "session-1", "action": "pause"}),
    ("cancel", {"operation": OPERATION, "task_id": "task-1"}),
    (
        "review",
        {
            "operation": OPERATION,
            "task_id": "task-1",
            "attempt_id": "attempt-1",
            "decision": "accepted",
            "evidence": "checked",
        },
    ),
    ("events", {"after": 9, "limit": 20}),
    ("result", {"task_id": "task-1", "offset": 0}),
    ("inbox", {"group_id": "group-1", "runtime": "codex", "native_id": "native-1"}),
    (
        "notify_parent",
        {
            "operation": OPERATION,
            "parent_ref": {"host_id": "coordinator", "node_id": "parent", "state_generation": "g", "local_id": "p"},
            "event": {"seq": 1, "type": "done", "ref": {}, "payload": {}, "created": 1.0},
        },
    ),
)


def peer_request(method="peer.hello", request_id="frame-1", params=None):
    return {"v": PROTOCOL_VERSION, "id": request_id, "method": method, "params": params if params is not None else {}}


def hello_result(node_id="node-a", generation="gen-a", state="ready"):
    return {
        "protocol_version": PROTOCOL_VERSION,
        "node": {"host_id": "gpu-a", "node_id": node_id, "state_generation": generation},
        "node_state": state,
        "max_confirmed_seq": 0,
        "capabilities": ["peer.hello"],
        "broker_version": "0.1.0",
    }


def make_host(host_id="gpu-a", **changes):
    values = {
        "host_id": host_id,
        "ssh_target": f"{host_id}.internal",
        "remote_executable": "/usr/local/bin/agent-bridge",
    }
    values.update(changes)
    return HostConfig(**values)


class FakeStdin:
    def __init__(self, process):
        self.process = process
        self.closed = False

    def write(self, data):
        if self.process.write_error is not None:
            raise self.process.write_error
        self.process.buffer += data
        while b"\n" in self.process.buffer:
            line, _, self.process.buffer = self.process.buffer.partition(b"\n")
            self.process.handle_line(line + b"\n")

    async def drain(self):
        if self.process.drain_error is not None:
            raise self.process.drain_error

    def close(self):
        self.closed = True


class FakeStdout:
    def __init__(self, process):
        self.process = process

    async def readline(self):
        return await self.process.responses.get()


class FakeProcess:
    def __init__(self, handler=None):
        self.handler = handler
        self.responses = asyncio.Queue()
        self.stdin = FakeStdin(self)
        self.stdout = FakeStdout(self)
        self.stderr = None
        self.returncode = None
        self.buffer = b""
        self.sent_lines = []
        self.write_error = None
        self.drain_error = None
        self.terminated = False
        self.killed = False
        self.waited = False

    def handle_line(self, line):
        self.sent_lines.append(line)
        if self.handler is None:
            return
        response = self.handler(json.loads(line))
        if response is None:
            return
        if isinstance(response, bytes):
            self.responses.put_nowait(response)
        else:
            self.responses.put_nowait(json.dumps(response).encode("utf-8") + b"\n")

    def terminate(self):
        self.terminated = True
        self.returncode = -15
        self.responses.put_nowait(b"")

    def kill(self):
        self.killed = True
        self.returncode = -9
        self.responses.put_nowait(b"")

    async def wait(self):
        self.waited = True
        return self.returncode


class RecordingTransport:
    def __init__(self, result=None, error=None):
        self.result = result if result is not None else {}
        self.error = error
        self.calls = []
        self.closed = False

    async def call(self, request, timeout):
        self.calls.append((request, timeout))
        if self.error is not None:
            return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": False, "error": self.error}
        return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": self.result}

    async def close(self):
        self.closed = True


class FixedTransport(RecordingTransport):
    def __init__(self, result):
        super().__init__(result=result)


class SshPeerTransportTests(unittest.IsolatedAsyncioTestCase):
    def spawn(self, process):
        patcher = patch(
            "agent_bridge.peer_transport.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        )
        mocked = patcher.start()
        self.addCleanup(patcher.stop)
        return mocked

    async def test_spawns_ssh_with_an_argument_list_and_exchanges_one_frame(self):
        def handler(request):
            self.assertEqual(request["method"], "peer.hello")
            return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": {"pong": True}}

        process = FakeProcess(handler)
        spawn = self.spawn(process)
        transport = SshPeerTransport(
            make_host(remote_state="/srv/agent-bridge", ssh_options=("-o", "BatchMode=yes")),
        )

        response = await transport.call(peer_request(), 1)

        self.assertEqual(response["result"], {"pong": True})
        spawn.assert_awaited_once()
        self.assertEqual(
            spawn.await_args.args,
            (
                "ssh",
                "-o",
                "BatchMode=yes",
                "gpu-a.internal",
                "/usr/local/bin/agent-bridge",
                "relay",
                "--state",
                "/srv/agent-bridge",
            ),
        )
        self.assertNotIn("shell", spawn.await_args.kwargs)
        frame = json.loads(process.sent_lines[0])
        self.assertEqual(frame, peer_request())
        self.assertEqual(process.sent_lines[0].count(b"\n"), 1)
        await transport.close()

    async def test_constructor_accepts_individual_transport_fields(self):
        transport = SshPeerTransport("gpu-b", "user@gpu-b", "/opt/agent-bridge")

        self.assertEqual(transport.host_id, "gpu-b")
        self.assertEqual(transport.argv, ("ssh", "user@gpu-b", "/opt/agent-bridge", "relay"))

        keyword = SshPeerTransport(
            host_id="gpu-c",
            ssh_target="user@gpu-c",
            remote_executable="/opt/agent-bridge",
            remote_state="/srv/state",
            ssh_options=["-p", "2222"],
        )
        self.assertEqual(
            keyword.argv,
            ("ssh", "-p", "2222", "user@gpu-c", "/opt/agent-bridge", "relay", "--state", "/srv/state"),
        )

    async def test_reuses_one_process_for_multiple_sequential_frames(self):
        def handler(request):
            return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": {"n": int(request["id"])}}

        process = FakeProcess(handler)
        spawn = self.spawn(process)
        transport = SshPeerTransport(make_host())

        first = await transport.call(peer_request(request_id="1"), 1)
        second = await transport.call(peer_request(request_id="2"), 1)

        self.assertEqual(first["result"], {"n": 1})
        self.assertEqual(second["result"], {"n": 2})
        spawn.assert_awaited_once()
        await transport.close()

    async def test_timeout_is_host_offline_and_terminates_the_child(self):
        process = FakeProcess(handler=None)
        self.spawn(process)
        transport = SshPeerTransport(make_host())

        with self.assertRaises(BridgeError) as caught:
            await transport.call(peer_request(), 0.05)

        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)
        self.assertTrue(process.killed)
        self.assertTrue(process.waited)
        self.assertIsNone(transport._process)

    async def test_closed_stdout_is_host_offline(self):
        process = FakeProcess(handler=None)
        process.responses.put_nowait(b"")
        self.spawn(process)
        transport = SshPeerTransport(make_host())

        with self.assertRaises(BridgeError) as caught:
            await transport.call(peer_request(), 1)

        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)
        self.assertIn("unknown", str(caught.exception))

    async def test_process_exit_without_a_frame_is_host_offline(self):
        process = FakeProcess(handler=None)
        process.returncode = 255
        process.responses.put_nowait(b"")
        self.spawn(process)
        transport = SshPeerTransport(make_host())

        with self.assertRaises(BridgeError) as caught:
            await transport.call(peer_request(), 1)

        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)

    async def test_pipe_errors_are_host_offline(self):
        process = FakeProcess(handler=None)
        process.write_error = BrokenPipeError("pipe closed")
        self.spawn(process)
        transport = SshPeerTransport(make_host())

        with self.assertRaises(BridgeError) as caught:
            await transport.call(peer_request(), 1)

        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)
        self.assertTrue(process.killed)
        self.assertTrue(process.stdin.closed)

    async def test_drain_errors_are_host_offline(self):
        process = FakeProcess(handler=None)
        process.drain_error = ConnectionResetError("reset")
        self.spawn(process)
        transport = SshPeerTransport(make_host())

        with self.assertRaises(BridgeError) as caught:
            await transport.call(peer_request(), 1)

        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)

    async def test_start_failure_is_host_offline(self):
        spawn = patch(
            "agent_bridge.peer_transport.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=FileNotFoundError("ssh missing")),
        )
        with spawn:
            transport = SshPeerTransport(make_host())
            with self.assertRaises(BridgeError) as caught:
                await transport.call(peer_request(), 1)

        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)

    async def test_malformed_and_mismatched_frames_are_protocol_errors(self):
        cases = (
            (b"{not json\n", PeerErrorCode.INVALID_PARAMS),
            (b'{"v":1,"id":"other","ok":true,"result":{}}\n', PeerErrorCode.INVALID_PARAMS),
            (b"[]\n", PeerErrorCode.INVALID_PARAMS),
        )
        for payload, code in cases:
            with self.subTest(payload=payload):
                process = FakeProcess(handler=None)
                process.responses.put_nowait(payload)
                self.spawn(process)
                transport = SshPeerTransport(make_host())
                with self.assertRaises(BridgeError) as caught:
                    await transport.call(peer_request(), 1)
                self.assertEqual(caught.exception.code, code)

    async def test_oversized_frames_are_rejected_before_spawning(self):
        spawn = self.spawn(FakeProcess())
        transport = SshPeerTransport(make_host())
        request = peer_request(params={"blob": "x" * MAX_FRAME_BYTES})

        with self.assertRaises(BridgeError) as caught:
            await transport.call(request, 1)

        self.assertEqual(caught.exception.code, PeerErrorCode.FRAME_TOO_LARGE)
        spawn.assert_not_awaited()

    async def test_oversized_reply_is_rejected(self):
        process = FakeProcess(handler=None)
        process.responses.put_nowait(b"x" * (MAX_FRAME_BYTES + 10) + b"\n")
        self.spawn(process)
        transport = SshPeerTransport(make_host())

        with self.assertRaises(BridgeError) as caught:
            await transport.call(peer_request(), 1)

        self.assertEqual(caught.exception.code, PeerErrorCode.FRAME_TOO_LARGE)

    async def test_invalid_timeout_is_rejected(self):
        transport = SshPeerTransport(make_host())

        for timeout in (0, -1, float("nan"), True):
            with self.subTest(timeout=timeout):
                with self.assertRaises(BridgeError) as caught:
                    await transport.call(peer_request(), timeout)
                self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)

    async def test_close_terminates_gracefully_and_is_idempotent(self):
        def handler(request):
            return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": {}}

        process = FakeProcess(handler)
        self.spawn(process)
        transport = SshPeerTransport(make_host())
        await transport.call(peer_request(), 1)

        await transport.close()
        await transport.close()

        self.assertTrue(process.terminated)
        self.assertTrue(process.waited)
        self.assertTrue(process.stdin.closed)
        self.assertIsNone(transport._process)


class DirectPeerClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_fourteen_methods_map_to_peer_names(self):
        for name, params in METHOD_CASES:
            with self.subTest(method=name):
                transport = RecordingTransport(result={"method_result": name})
                client = DirectPeerClient(transport, timeout=12.5)

                result = await getattr(client, name)(params)

                self.assertEqual(result, {"method_result": name})
                self.assertEqual(len(transport.calls), 1)
                request, timeout = transport.calls[0]
                self.assertEqual(timeout, 12.5)
                self.assertEqual(request["v"], PROTOCOL_VERSION)
                self.assertEqual(request["method"], f"peer.{name}")
                self.assertEqual(request["params"], params)
                self.assertEqual(len(request["id"]), 32)
                int(request["id"], 16)

    async def test_uses_a_fresh_frame_id_per_call(self):
        transport = RecordingTransport(result={})
        client = DirectPeerClient(transport)

        await client.hello(HELLO_PARAMS)
        await client.hello(HELLO_PARAMS)

        self.assertNotEqual(transport.calls[0][0]["id"], transport.calls[1][0]["id"])
        self.assertEqual(transport.calls[0][1], DEFAULT_PEER_TIMEOUT)

    async def test_error_responses_raise_bridge_error_with_the_peer_code(self):
        transport = RecordingTransport(error={"code": PeerErrorCode.HOST_OFFLINE, "message": "gone"})
        client = DirectPeerClient(transport)

        with self.assertRaises(BridgeError) as caught:
            await client.catalog({})

        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)
        self.assertEqual(str(caught.exception), "gone")

    async def test_missing_error_body_still_raises(self):
        class BadTransport(RecordingTransport):
            async def call(self, request, timeout):
                return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": False}

        client = DirectPeerClient(BadTransport())

        with self.assertRaises(BridgeError) as caught:
            await client.inspect({"kind": "node"})

        self.assertEqual(caught.exception.code, "peer_error")


class DefaultPeerClientFactoryTests(unittest.IsolatedAsyncioTestCase):
    def factory(self, pinned, result):
        registry = ConfigHostRegistry({"gpu-a": make_host(pinned=pinned)})
        transport = FixedTransport(result)
        created = []

        def transport_factory(config):
            created.append(config)
            return transport

        factory = DefaultPeerClientFactory(registry, transport_factory=transport_factory)
        return registry, factory, transport, created

    async def test_matching_pin_marks_the_host_online(self):
        pinned = NodeRef("gpu-a", "node-a", "gen-a")
        registry, factory, _, created = self.factory(pinned, hello_result())

        client = factory.client("gpu-a")
        result = await client.hello(HELLO_PARAMS)

        self.assertEqual(result["node"]["node_id"], "node-a")
        self.assertEqual([config.host_id for config in created], ["gpu-a"])
        status = registry.status("gpu-a")[0]
        self.assertEqual(status.state, HostState.ONLINE)
        self.assertIsNotNone(status.last_contact)

    async def test_first_handshake_without_a_pin_is_accepted_and_not_auto_pinned(self):
        registry, factory, _, _ = self.factory(None, hello_result())

        await factory.client("gpu-a").hello(HELLO_PARAMS)

        status = registry.status("gpu-a")[0]
        self.assertEqual(status.state, HostState.ONLINE)
        self.assertIsNone(registry.get("gpu-a").pinned)

    async def test_changed_node_id_marks_identity_changed_and_raises(self):
        pinned = NodeRef("gpu-a", "node-a", "gen-a")
        registry, factory, _, _ = self.factory(pinned, hello_result(node_id="node-evil"))

        with self.assertRaises(BridgeError) as caught:
            await factory.client("gpu-a").hello(HELLO_PARAMS)

        self.assertEqual(caught.exception.code, PeerErrorCode.IDENTITY_MISMATCH)
        status = registry.status("gpu-a")[0]
        self.assertEqual(status.state, HostState.IDENTITY_CHANGED)
        self.assertIn("identity changed", status.detail)

    async def test_changed_generation_marks_identity_changed_and_raises(self):
        pinned = NodeRef("gpu-a", "node-a", "gen-a")
        registry, factory, _, _ = self.factory(pinned, hello_result(generation="gen-b"))

        with self.assertRaises(BridgeError) as caught:
            await factory.client("gpu-a").hello(HELLO_PARAMS)

        self.assertEqual(caught.exception.code, PeerErrorCode.IDENTITY_MISMATCH)
        self.assertEqual(registry.status("gpu-a")[0].state, HostState.IDENTITY_CHANGED)

    async def test_corrupted_node_marks_state_corrupted_and_raises(self):
        pinned = NodeRef("gpu-a", "node-a", "gen-a")
        registry, factory, _, _ = self.factory(
            pinned, hello_result(state="state_corrupted"),
        )

        with self.assertRaises(BridgeError) as caught:
            await factory.client("gpu-a").hello(HELLO_PARAMS)

        self.assertEqual(caught.exception.code, PeerErrorCode.STATE_CORRUPTED)
        self.assertEqual(registry.status("gpu-a")[0].state, HostState.STATE_CORRUPTED)

    async def test_a_corrupted_error_response_also_marks_the_host(self):
        registry = ConfigHostRegistry({"gpu-a": make_host(pinned=NodeRef("gpu-a", "node-a", "gen-a"))})
        transport = RecordingTransport(error={"code": PeerErrorCode.STATE_CORRUPTED, "message": "rollback"})
        factory = DefaultPeerClientFactory(registry, lambda config: transport)

        with self.assertRaises(BridgeError) as caught:
            await factory.client("gpu-a").hello(HELLO_PARAMS)

        self.assertEqual(caught.exception.code, PeerErrorCode.STATE_CORRUPTED)
        self.assertEqual(registry.status("gpu-a")[0].state, HostState.STATE_CORRUPTED)

    async def test_non_hello_calls_do_not_run_the_handshake_check(self):
        pinned = NodeRef("gpu-a", "node-a", "gen-a")
        registry, factory, _, _ = self.factory(pinned, {"profiles": []})

        result = await factory.client("gpu-a").catalog({})

        self.assertEqual(result, {"profiles": []})
        self.assertEqual(registry.status("gpu-a")[0].state, HostState.OFFLINE)

    async def test_unknown_hosts_are_rejected_before_creating_a_transport(self):
        registry = ConfigHostRegistry({})
        created = []
        factory = DefaultPeerClientFactory(registry, transport_factory=created.append)

        with self.assertRaises(BridgeError) as caught:
            factory.client("missing")

        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_UNKNOWN)
        self.assertEqual(created, [])

    async def test_identity_check_requires_a_host_id(self):
        registry = ConfigHostRegistry({"gpu-a": make_host()})

        with self.assertRaises(BridgeError) as caught:
            DirectPeerClient(RecordingTransport(), registry=registry)

        self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)


if __name__ == "__main__":
    unittest.main()
