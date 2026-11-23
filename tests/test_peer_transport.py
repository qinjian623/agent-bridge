import asyncio
import json
import shlex
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


class HandshakeTransport(RecordingTransport):
    def __init__(self, hello=None):
        super().__init__()
        self.hello = hello if hello is not None else hello_result()

    async def call(self, request, timeout):
        self.calls.append((request, timeout))
        await asyncio.sleep(0)
        result = self.hello if request["method"] == "peer.hello" else {"profiles": []}
        return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": result}


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
                "/usr/local/bin/agent-bridge relay --state /srv/agent-bridge",
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
        self.assertEqual(transport.argv, ("ssh", "user@gpu-b", "/opt/agent-bridge relay"))

        keyword = SshPeerTransport(
            host_id="gpu-c",
            ssh_target="user@gpu-c",
            remote_executable="/opt/agent-bridge",
            remote_state="/srv/state",
            ssh_options=["-p", "2222"],
        )
        self.assertEqual(
            keyword.argv,
            ("ssh", "-p", "2222", "user@gpu-c", "/opt/agent-bridge relay --state /srv/state"),
        )

    def test_remote_command_quotes_spaces_quotes_and_shell_expansion(self):
        executable = "/opt/agent bridge/bin/agent-$(touch /tmp/never-run)-'quoted'"
        state = "/srv/state with spaces/$HOME;$(touch /tmp/never-run)/`id`"
        transport = SshPeerTransport(
            make_host(
                remote_executable=executable,
                remote_state=state,
                ssh_options=("-o", "BatchMode=yes"),
            )
        )

        self.assertEqual(
            transport.argv,
            (
                "ssh",
                "-o",
                "BatchMode=yes",
                "gpu-a.internal",
                shlex.join((executable, "relay", "--state", state)),
            ),
        )
        self.assertEqual(shlex.split(transport.argv[-1]), [executable, "relay", "--state", state])
        self.assertIn(shlex.quote(executable), transport.argv[-1])
        self.assertIn(shlex.quote(state), transport.argv[-1])
        self.assertIn("$HOME", shlex.split(transport.argv[-1])[-1])

    def test_simple_remote_paths_keep_the_existing_command_tokens(self):
        transport = SshPeerTransport(make_host())

        self.assertEqual(transport.argv[-1], "/usr/local/bin/agent-bridge relay")
        self.assertEqual(shlex.split(transport.argv[-1]), ["/usr/local/bin/agent-bridge", "relay"])

    def test_rejects_ssh_option_target_and_control_characters(self):
        cases = (
            {"ssh_target": "-oProxyCommand=touch /tmp/never-run"},
            {"ssh_target": "gpu-a\n-oProxyCommand=touch /tmp/never-run"},
            {"remote_executable": "/opt/agent\nbridge"},
            {"remote_state": "/srv/state\r"},
            {"ssh_options": ("-o", "BatchMode=yes\nProxyCommand=touch /tmp/never-run")},
        )
        for changes in cases:
            with self.subTest(changes=changes), self.assertRaises(BridgeError) as caught:
                SshPeerTransport(make_host(**changes))
            self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)

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

    async def test_non_hello_calls_verify_identity_before_sending(self):
        pinned = NodeRef("gpu-a", "node-a", "gen-a")
        registry = ConfigHostRegistry({"gpu-a": make_host(pinned=pinned)})
        transport = HandshakeTransport()
        factory = DefaultPeerClientFactory(registry, lambda config: transport)

        result = await factory.client("gpu-a").catalog({})

        self.assertEqual(result, {"profiles": []})
        self.assertEqual([r[0]["method"] for r in transport.calls], ["peer.hello", "peer.catalog"])
        status = registry.status("gpu-a")[0]
        self.assertEqual(status.state, HostState.ONLINE)
        self.assertEqual(status.protocol_version, PROTOCOL_VERSION)
        self.assertIsNotNone(status.last_contact)

    async def test_polling_uses_identity_only_hello_and_refuses_a_changed_pin(self):
        pinned = NodeRef("gpu-a", "node-a", "gen-a")
        registry = ConfigHostRegistry({"gpu-a": make_host(pinned=pinned)})
        transport = HandshakeTransport()
        client = DefaultPeerClientFactory(registry, lambda config: transport).client("gpu-a")

        await client.pull_requests({"after": 0})
        self.assertIs(transport.calls[0][0]["params"]["identity_only"], True)
        self.assertEqual("peer.pull_requests", transport.calls[1][0]["method"])
        await client.lookup_delivery({"delivery_id": "delivery-1"})
        self.assertIs(transport.calls[2][0]["params"]["identity_only"], True)
        self.assertEqual("peer.lookup_delivery", transport.calls[3][0]["method"])
        transport.hello = hello_result(generation="gen-b")
        with self.assertRaises(BridgeError) as caught:
            await client.events({"after": 0})

        self.assertEqual(PeerErrorCode.IDENTITY_MISMATCH, caught.exception.code)
        self.assertEqual(5, len(transport.calls))
        self.assertIs(transport.calls[4][0]["params"]["identity_only"], True)

    async def test_explicit_identity_only_hello_does_not_change_later_ordinary_requests(self):
        registry = ConfigHostRegistry({"gpu-a": make_host()})
        transport = HandshakeTransport()
        client = DefaultPeerClientFactory(registry, lambda config: transport).client("gpu-a")

        await client.hello({**HELLO_PARAMS, "identity_only": True})
        await client.catalog({})

        self.assertIs(transport.calls[0][0]["params"]["identity_only"], True)
        self.assertNotIn("identity_only", transport.calls[1][0]["params"])

    async def test_reuses_one_client_and_transport_for_a_host(self):
        _, factory, transport, created = self.factory(None, hello_result())

        first = factory.client("gpu-a")
        second = factory.client("gpu-a")

        self.assertIs(first, second)
        self.assertIs(first._transport, transport)
        self.assertEqual(len(created), 1)

    async def test_retire_closes_only_changed_hosts_and_blocks_old_client_references(self):
        registry = ConfigHostRegistry({name: make_host(name) for name in ("gpu-a", "gpu-b")})
        created = []

        def make_transport(config):
            transport = HandshakeTransport()
            created.append((config, transport))
            return transport

        factory = DefaultPeerClientFactory(registry, make_transport)
        old = factory.client("gpu-a")
        unaffected = factory.client("gpu-b")
        await factory.retire(["gpu-a"])
        with self.assertRaises(BridgeError) as caught:
            factory.client("gpu-a")
        self.assertEqual(caught.exception.code, "host_busy")
        with self.assertRaises(BridgeError) as caught:
            await old.catalog({})
        self.assertEqual(caught.exception.code, "host_busy")
        self.assertTrue(created[0][1].closed)
        self.assertEqual(created[0][1].calls, [])
        self.assertFalse(created[1][1].closed)
        self.assertIs(factory.client("gpu-b"), unaffected)

        changed = make_host("gpu-a", ssh_target="replacement-target")
        registry.replace_hosts({"gpu-a": changed, "gpu-b": registry.get("gpu-b")})
        factory.release_retired(["gpu-a"])
        replacement = factory.client("gpu-a")
        self.assertIsNot(replacement, old)
        self.assertEqual(created[-1][0].ssh_target, "replacement-target")
        await replacement.catalog({})
        with self.assertRaises(BridgeError):
            await old.catalog({})
        self.assertEqual(created[0][1].calls, [])
        await factory.close()

    async def test_retire_drains_current_write_and_rejects_queued_or_new_requests(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class InflightTransport(HandshakeTransport):
            async def call(self, request, timeout):
                if request["method"] == "peer.deliver":
                    self.calls.append((request, timeout))
                    entered.set()
                    await release.wait()
                    return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True,
                            "result": {"accepted": True}}
                return await super().call(request, timeout)

        registry = ConfigHostRegistry({"gpu-a": make_host()})
        transport = InflightTransport()
        factory = DefaultPeerClientFactory(registry, lambda config: transport)
        client = factory.client("gpu-a")
        write = asyncio.create_task(client.deliver({"operation": OPERATION}))
        await asyncio.wait_for(entered.wait(), 1)
        queued = asyncio.create_task(client.catalog({}))
        await asyncio.sleep(0)
        retiring = asyncio.create_task(factory.retire(["gpu-a"]))
        await asyncio.sleep(0)
        self.assertFalse(retiring.done())
        self.assertFalse(transport.closed)
        self.assertFalse(write.done())
        with self.assertRaises(BridgeError) as caught:
            await asyncio.wait_for(client.catalog({}), .2)
        self.assertEqual(caught.exception.code, "host_busy")
        with self.assertRaises(BridgeError):
            factory.client("gpu-a")

        release.set()
        self.assertEqual(await write, {"accepted": True})
        with self.assertRaises(BridgeError) as caught:
            await queued
        self.assertEqual(caught.exception.code, "host_busy")
        await retiring
        self.assertTrue(transport.closed)
        self.assertEqual([item[0]["method"] for item in transport.calls], ["peer.hello", "peer.deliver"])

    async def test_removed_hosts_cannot_reconnect_after_release(self):
        registry = ConfigHostRegistry({"gpu-a": make_host()})
        transport = HandshakeTransport()
        factory = DefaultPeerClientFactory(registry, lambda config: transport)
        old = factory.client("gpu-a")
        await factory.retire(["gpu-a"])
        registry.replace_hosts({})
        factory.release_retired(["gpu-a"])

        with self.assertRaises(BridgeError) as caught:
            factory.client("gpu-a")
        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_UNKNOWN)
        with self.assertRaises(BridgeError):
            await old.hello(HELLO_PARAMS)
        self.assertEqual(transport.calls, [])

    async def test_cancelled_retirement_keeps_the_write_running_and_host_frozen(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class InflightTransport(HandshakeTransport):
            async def call(self, request, timeout):
                if request["method"] == "peer.deliver":
                    self.calls.append((request, timeout))
                    entered.set()
                    await release.wait()
                    return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True,
                            "result": {"accepted": True}}
                return await super().call(request, timeout)

        registry = ConfigHostRegistry({"gpu-a": make_host()})
        transport = InflightTransport()
        factory = DefaultPeerClientFactory(registry, lambda config: transport)
        write = asyncio.create_task(factory.client("gpu-a").deliver({"operation": OPERATION}))
        await asyncio.wait_for(entered.wait(), 1)
        retiring = asyncio.create_task(factory.retire(["gpu-a"]))
        await asyncio.sleep(0)
        retiring.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await retiring
        self.assertFalse(write.done())
        self.assertFalse(transport.closed)
        with self.assertRaises(BridgeError) as caught:
            factory.client("gpu-a")
        self.assertEqual(caught.exception.code, "host_busy")
        release.set()
        self.assertEqual(await write, {"accepted": True})
        await factory.retire(["gpu-a"])
        self.assertTrue(transport.closed)

    async def test_hosts_keep_separate_connections_and_close_is_repeatable(self):
        registry = ConfigHostRegistry({name: make_host(name) for name in ("gpu-a", "gpu-b")})
        created = []

        def make_transport(config):
            transport = HandshakeTransport()
            created.append((config.host_id, transport))
            return transport

        factory = DefaultPeerClientFactory(registry, make_transport)
        first = factory.client("gpu-a")
        second = factory.client("gpu-b")
        self.assertIsNot(first, second)
        self.assertEqual([name for name, _ in created], ["gpu-a", "gpu-b"])

        await factory.close()
        await factory.close()

        self.assertTrue(all(transport.closed for _, transport in created))
        self.assertIsNot(factory.client("gpu-a"), first)
        self.assertEqual(len(created), 3)
        await factory.close()

    async def test_identity_mismatch_prevents_reads_and_writes(self):
        for name, params in (("catalog", {}), ("deliver", {"operation": OPERATION})):
            with self.subTest(method=name):
                registry = ConfigHostRegistry({"gpu-a": make_host(pinned=NodeRef("gpu-a", "node-a", "gen-a"))})
                transport = HandshakeTransport(hello_result(node_id="other"))
                factory = DefaultPeerClientFactory(registry, lambda config, transport=transport: transport)

                with self.assertRaises(BridgeError) as caught:
                    await getattr(factory.client("gpu-a"), name)(params)

                self.assertEqual(caught.exception.code, PeerErrorCode.IDENTITY_MISMATCH)
                self.assertEqual([r[0]["method"] for r in transport.calls], ["peer.hello"])
                self.assertEqual(registry.status("gpu-a")[0].state, HostState.IDENTITY_CHANGED)
                self.assertTrue(transport.closed)

    async def test_sticky_identity_failure_blocks_requests_until_re_registration(self):
        registry, factory, transport, _ = self.factory(NodeRef("gpu-a", "node-a", "gen-a"), hello_result(node_id="other"))
        client = factory.client("gpu-a")
        with self.assertRaises(BridgeError):
            await client.catalog({})

        transport.result = hello_result()
        with self.assertRaises(BridgeError) as caught:
            await client.catalog({})

        self.assertEqual(caught.exception.code, PeerErrorCode.IDENTITY_MISMATCH)
        self.assertEqual(registry.status("gpu-a")[0].state, HostState.IDENTITY_CHANGED)
        self.assertEqual([r[0]["method"] for r in transport.calls], ["peer.hello", "peer.hello"])

    async def test_concurrent_calls_keep_handshake_and_request_together(self):
        registry = ConfigHostRegistry({"gpu-a": make_host()})
        transport = HandshakeTransport()
        factory = DefaultPeerClientFactory(registry, lambda config: transport)

        await asyncio.gather(factory.client("gpu-a").catalog({}), factory.client("gpu-a").inspect({"kind": "node"}))

        self.assertEqual(
            [r[0]["method"] for r in transport.calls],
            ["peer.hello", "peer.catalog", "peer.hello", "peer.inspect"],
        )

    async def test_connection_loss_is_offline_and_does_not_replay_uncertain_write(self):
        def respond(request):
            result = hello_result() if request["method"] == "peer.hello" else {"profiles": []}
            return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": result}

        def lose_write(request):
            return respond(request) if request["method"] == "peer.hello" else b""

        first, second = FakeProcess(lose_write), FakeProcess(respond)
        registry = ConfigHostRegistry({"gpu-a": make_host(pinned=NodeRef("gpu-a", "node-a", "gen-a"))})
        factory = DefaultPeerClientFactory(registry)
        with patch("agent_bridge.peer_transport.asyncio.create_subprocess_exec", new=AsyncMock(side_effect=[first, second])) as spawn:
            client = factory.client("gpu-a")
            with self.assertRaises(BridgeError) as caught:
                await client.deliver({"operation": OPERATION})
            self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)
            self.assertEqual(registry.status("gpu-a")[0].state, HostState.OFFLINE)
            self.assertTrue(first.killed)

            await factory.client("gpu-a").catalog({})
            self.assertEqual(spawn.await_count, 2)
            self.assertEqual([json.loads(line)["method"] for line in first.sent_lines], ["peer.hello", "peer.deliver"])
            self.assertEqual([json.loads(line)["method"] for line in second.sent_lines], ["peer.hello", "peer.catalog"])
            self.assertEqual(registry.status("gpu-a")[0].state, HostState.ONLINE)
            await factory.close()

    async def test_reconnected_host_must_match_pin_before_another_write(self):
        def lost(request):
            return b""

        def changed(request):
            return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": hello_result(node_id="changed")}

        first, second = FakeProcess(lost), FakeProcess(changed)
        registry = ConfigHostRegistry({"gpu-a": make_host(pinned=NodeRef("gpu-a", "node-a", "gen-a"))})
        factory = DefaultPeerClientFactory(registry)
        with patch("agent_bridge.peer_transport.asyncio.create_subprocess_exec", new=AsyncMock(side_effect=[first, second])):
            with self.assertRaises(BridgeError):
                await factory.client("gpu-a").catalog({})
            with self.assertRaises(BridgeError) as caught:
                await factory.client("gpu-a").deliver({"operation": OPERATION})

            self.assertEqual(caught.exception.code, PeerErrorCode.IDENTITY_MISMATCH)
            self.assertEqual([json.loads(line)["method"] for line in second.sent_lines], ["peer.hello"])
            self.assertEqual(registry.status("gpu-a")[0].state, HostState.IDENTITY_CHANGED)
            await factory.close()

    async def test_ssh_exit_after_hello_does_not_send_business_frame_on_new_connection(self):
        def exit_after_hello(request):
            first.returncode = 255
            return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": hello_result()}

        first = FakeProcess(exit_after_hello)
        registry = ConfigHostRegistry({"gpu-a": make_host(pinned=NodeRef("gpu-a", "node-a", "gen-a"))})
        factory = DefaultPeerClientFactory(registry)
        with patch("agent_bridge.peer_transport.asyncio.create_subprocess_exec", new=AsyncMock(return_value=first)) as spawn:
            with self.assertRaises(BridgeError) as caught:
                await factory.client("gpu-a").deliver({"operation": OPERATION})

            self.assertEqual(caught.exception.code, PeerErrorCode.HOST_OFFLINE)
            self.assertIn("not sent", str(caught.exception))
            self.assertEqual([json.loads(line)["method"] for line in first.sent_lines], ["peer.hello"])
            spawn.assert_awaited_once()
            self.assertEqual(registry.status("gpu-a")[0].state, HostState.OFFLINE)
            await factory.close()

    async def test_protocol_failure_marks_offline_and_closes_connection(self):
        for response in ({"protocol_version": PROTOCOL_VERSION}, {**hello_result(), "protocol_version": 999}):
            with self.subTest(response=response):
                registry = ConfigHostRegistry({"gpu-a": make_host()})
                transport = HandshakeTransport(response)
                factory = DefaultPeerClientFactory(registry, lambda config, transport=transport: transport)
                with self.assertRaises(BridgeError):
                    await factory.client("gpu-a").catalog({})
                self.assertEqual(registry.status("gpu-a")[0].state, HostState.OFFLINE)
                self.assertTrue(transport.closed)

    async def test_business_error_keeps_verified_connection_online(self):
        class RejectedBusinessTransport(HandshakeTransport):
            async def call(self, request, timeout):
                if request["method"] != "peer.hello":
                    return {
                        "v": PROTOCOL_VERSION, "id": request["id"], "ok": False,
                        "error": {"code": PeerErrorCode.REVISION_CONFLICT, "message": "stale revision"},
                    }
                return await super().call(request, timeout)

        registry = ConfigHostRegistry({"gpu-a": make_host()})
        transport = RejectedBusinessTransport()
        factory = DefaultPeerClientFactory(registry, lambda config: transport)
        with self.assertRaises(BridgeError) as caught:
            await factory.client("gpu-a").control({})
        self.assertEqual(caught.exception.code, PeerErrorCode.REVISION_CONFLICT)
        self.assertEqual(registry.status("gpu-a")[0].state, HostState.ONLINE)
        self.assertFalse(transport.closed)

    async def test_malformed_business_reply_marks_offline(self):
        def respond(request):
            if request["method"] != "peer.hello":
                return b"not-json\n"
            return {"v": PROTOCOL_VERSION, "id": request["id"], "ok": True, "result": hello_result()}

        registry = ConfigHostRegistry({"gpu-a": make_host()})
        process = FakeProcess(respond)
        factory = DefaultPeerClientFactory(registry)
        with patch("agent_bridge.peer_transport.asyncio.create_subprocess_exec", new=AsyncMock(return_value=process)):
            with self.assertRaises(BridgeError) as caught:
                await factory.client("gpu-a").catalog({})
            self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)
            self.assertEqual(registry.status("gpu-a")[0].state, HostState.OFFLINE)
            self.assertTrue(process.killed)
            await factory.close()

    async def test_start_failure_marks_offline(self):
        registry = ConfigHostRegistry({"gpu-a": make_host()})
        registry.mark("gpu-a", HostState.ONLINE)
        factory = DefaultPeerClientFactory(registry)
        with patch("agent_bridge.peer_transport.asyncio.create_subprocess_exec", new=AsyncMock(side_effect=OSError("SSH unavailable"))):
            with self.assertRaises(BridgeError):
                await factory.client("gpu-a").catalog({})
        self.assertEqual(registry.status("gpu-a")[0].state, HostState.OFFLINE)
        await factory.close()

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
