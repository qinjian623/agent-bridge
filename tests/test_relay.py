import asyncio
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

from agent_bridge.cli import build_parser
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import MAX_FRAME_BYTES, PeerErrorCode, ok
from agent_bridge.peer_transport import DirectRelayServer


def frame(request):
    return (json.dumps(request) + "\n").encode("utf-8")


def request(method="peer.hello", params=None, request_id="1", version=1):
    return {"v": version, "id": request_id, "method": method, "params": params if params is not None else {}}


class FakeService:
    def __init__(self, response=None):
        self.response = response
        self.requests = []

    async def handle(self, peer_request):
        self.requests.append(peer_request)
        if self.response is not None:
            return self.response
        return ok(peer_request["id"], {"method": peer_request["method"]})


class FlushSink(io.BytesIO):
    def __init__(self):
        super().__init__()
        self.flushes = 0

    def flush(self):
        self.flushes += 1
        super().flush()


async def run_relay(server, payload, sink=None):
    stdin = io.BytesIO(payload)
    stdout = sink or io.BytesIO()
    await server.serve(stdin, stdout)
    return stdout


class DirectPeerServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.identity = FileNodeIdentityStore(self.state)
        self.service = DirectPeerService(self.identity, host_id="gpu-a")

    async def test_hello_negotiates_the_version_and_returns_the_node_identity(self):
        response = await self.service.handle(request("peer.hello", {"protocol_versions": [1]}))

        self.assertTrue(response["ok"])
        self.assertEqual(response["id"], "1")
        result = response["result"]
        identity = self.identity.load_or_create()
        self.assertEqual(result["protocol_version"], 1)
        self.assertEqual(
            result["node"],
            {"host_id": "gpu-a", "node_id": identity.node_id, "state_generation": identity.state_generation},
        )
        self.assertEqual(result["node_state"], "ready")
        self.assertEqual(result["max_confirmed_seq"], 0)
        self.assertEqual(
            result["capabilities"],
            [
                "peer.hello",
                "peer.inspect",
                "peer.catalog",
                "peer.pull_requests",
                "peer.ack_request",
                "peer.deliver",
                "peer.lookup_delivery",
                "peer.cancel",
                "peer.result",
                "peer.events",
                "peer.notify_parent",
                "peer.workspace",
                "peer.resolve_delivery",
                "groups_v1",
                "ordered_delivery_v1",
                "member_outbox_v1",
                "event_mirror_v1",
                "workspace_v1",
                "session_name_v1",
                "prune_guard_v1",
            ],
        )
        self.assertTrue(result["broker_version"])

    async def test_hello_rejects_unsupported_versions(self):
        response = await self.service.handle(request("peer.hello", {"protocol_versions": [99]}))

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.VERSION_MISMATCH)

    async def test_hello_validates_protocol_versions(self):
        for params in ({}, {"protocol_versions": "1"}, {"protocol_versions": [True]}, {"protocol_versions": [1.0]}):
            with self.subTest(params=params):
                response = await self.service.handle(request("peer.hello", params))
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], PeerErrorCode.INVALID_PARAMS)

    async def test_hello_checks_last_acked_seq_for_a_rollback(self):
        self.identity.advance(5)

        response = await self.service.handle(
            request("peer.hello", {"protocol_versions": [1], "last_acked_seq": 2})
        )

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.STATE_CORRUPTED)
        self.assertEqual(self.identity.load_or_create().state.value, "state_corrupted")

    async def test_hello_accepts_a_matching_last_acked_seq(self):
        self.identity.advance(5)

        response = await self.service.handle(
            request("peer.hello", {"protocol_versions": [1], "last_acked_seq": 5})
        )

        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["max_confirmed_seq"], 5)

    async def test_inspect_node_returns_the_current_node_state(self):
        response = await self.service.handle(request("peer.inspect", {"kind": "node"}))

        self.assertTrue(response["ok"])
        [item] = response["result"]["items"]
        identity = self.identity.load_or_create()
        self.assertEqual(item["host_id"], "gpu-a")
        self.assertEqual(item["node_id"], identity.node_id)
        self.assertEqual(item["state_generation"], identity.state_generation)
        self.assertEqual(item["node_state"], "ready")
        self.assertEqual(item["max_confirmed_seq"], 0)

    async def test_inspect_accepts_known_kinds_and_rejects_unknown_ones(self):
        for kind in ("session", "task", "group", "operation"):
            with self.subTest(kind=kind):
                response = await self.service.handle(request("peer.inspect", {"kind": kind}))
                self.assertTrue(response["ok"])
                self.assertEqual(response["result"], {"items": []})

        response = await self.service.handle(request("peer.inspect", {"kind": "nonsense"}))
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.INVALID_PARAMS)

    async def test_unimplemented_whitelisted_methods_are_reported_as_not_implemented(self):
        response = await self.service.handle(request("peer.pull_requests", {"after": 0}))

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.NOT_IMPLEMENTED)

    async def test_unknown_methods_and_envelopes_are_rejected(self):
        unknown = await self.service.handle(request("agent_open", {}, request_id="x"))
        self.assertFalse(unknown["ok"])
        self.assertEqual(unknown["id"], "x")
        self.assertEqual(unknown["error"]["code"], PeerErrorCode.METHOD_NOT_ALLOWED)

        wrong_version = await self.service.handle(request("peer.hello", {"protocol_versions": [1]}, version=2))
        self.assertFalse(wrong_version["ok"])
        self.assertEqual(wrong_version["error"]["code"], PeerErrorCode.VERSION_MISMATCH)

        bad_params = await self.service.handle({"v": 1, "id": "p", "method": "peer.hello", "params": []})
        self.assertFalse(bad_params["ok"])
        self.assertEqual(bad_params["error"]["code"], PeerErrorCode.INVALID_PARAMS)

    async def test_non_object_requests_are_rejected(self):
        response = await self.service.handle(["not", "a", "request"])

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.INVALID_PARAMS)


class DirectRelayServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatches_whitelisted_methods_to_the_local_service(self):
        service = FakeService()
        server = DirectRelayServer(service)
        payload = frame(request("peer.hello", {"protocol_versions": [1]}, request_id="frame-7"))

        stdout = await run_relay(server, payload)

        response = json.loads(stdout.getvalue())
        self.assertTrue(response["ok"])
        self.assertEqual(response["id"], "frame-7")
        self.assertEqual(len(service.requests), 1)
        self.assertEqual(service.requests[0]["method"], "peer.hello")

    async def test_unknown_methods_are_filtered_before_the_service(self):
        service = FakeService()
        server = DirectRelayServer(service)

        stdout = await run_relay(server, frame(request("agent_open", {}, request_id="bad")))

        response = json.loads(stdout.getvalue())
        self.assertFalse(response["ok"])
        self.assertEqual(response["id"], "bad")
        self.assertEqual(response["error"]["code"], PeerErrorCode.METHOD_NOT_ALLOWED)
        self.assertEqual(service.requests, [])

    async def test_multiple_frames_and_blank_lines_are_handled_in_order(self):
        service = FakeService()
        server = DirectRelayServer(service)
        payload = (
            frame(request("peer.hello", {"protocol_versions": [1]}, request_id="1"))
            + b"\n"
            + frame(request("peer.inspect", {"kind": "node"}, request_id="2"))
        )

        stdout = await run_relay(server, payload)

        responses = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual([response["id"] for response in responses], ["1", "2"])
        self.assertTrue(all(response["ok"] for response in responses))
        self.assertEqual([entry["id"] for entry in service.requests], ["1", "2"])

    async def test_malformed_frames_return_invalid_params(self):
        cases = (
            (b"{not json\n", PeerErrorCode.INVALID_PARAMS),
            (b"[]\n", PeerErrorCode.INVALID_PARAMS),
            (b"\xff\xfe\n", PeerErrorCode.INVALID_PARAMS),
        )
        for payload, code in cases:
            with self.subTest(payload=payload):
                stdout = await run_relay(DirectRelayServer(FakeService()), payload)
                response = json.loads(stdout.getvalue())
                self.assertFalse(response["ok"])
                self.assertEqual(response["error"]["code"], code)

    async def test_oversized_request_frames_are_rejected(self):
        payload = b"x" * (MAX_FRAME_BYTES + 10) + b"\n"

        stdout = await run_relay(DirectRelayServer(FakeService()), payload)

        response = json.loads(stdout.getvalue())
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], PeerErrorCode.FRAME_TOO_LARGE)

    async def test_oversized_responses_become_a_frame_too_large_error(self):
        huge = {"ok": True, "id": "big", "result": {"blob": "x" * (MAX_FRAME_BYTES + 10)}}
        service = FakeService(response=huge)

        stdout = await run_relay(DirectRelayServer(service), frame(request("peer.inspect", {"kind": "node"})))

        response = json.loads(stdout.getvalue())
        self.assertFalse(response["ok"])
        self.assertEqual(response["id"], "big")
        self.assertEqual(response["error"]["code"], PeerErrorCode.FRAME_TOO_LARGE)

    async def test_each_response_is_written_on_one_line_and_flushed(self):
        service = FakeService()
        sink = FlushSink()

        await run_relay(DirectRelayServer(service), frame(request("peer.hello", {"protocol_versions": [1]})), sink)

        self.assertEqual(sink.flushes, 1)
        written = sink.getvalue()
        self.assertEqual(written.count(b"\n"), 1)
        self.assertTrue(written.endswith(b"\n"))


class CliRelayParserTests(unittest.TestCase):
    def test_relay_accepts_state_before_or_after_the_subcommand(self):
        parser = build_parser()

        after = parser.parse_args(["relay", "--state", "/tmp/after"])
        before = parser.parse_args(["--state", "/tmp/before", "relay"])
        default = parser.parse_args(["relay"])

        self.assertEqual(after.command, "relay")
        self.assertEqual(after.state, "/tmp/after")
        self.assertEqual(before.command, "relay")
        self.assertEqual(before.state, "/tmp/before")
        self.assertIsNone(default.state)


class RelayCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_cli_relay_serves_hello_over_stdio_and_exits_at_eof(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state = Path(temp.name)

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "agent_bridge.cli",
            "--state",
            str(state),
            "relay",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        payload = frame(request("peer.hello", {"protocol_versions": [1]}, request_id="cli-1"))
        stdout, stderr = await asyncio.wait_for(process.communicate(payload), 30)

        self.assertEqual(process.returncode, 0, stderr.decode())
        lines = stdout.splitlines()
        self.assertEqual(len(lines), 1)
        response = json.loads(lines[0])
        self.assertTrue(response["ok"])
        self.assertEqual(response["id"], "cli-1")
        identity = FileNodeIdentityStore(state).load_or_create()
        self.assertEqual(response["result"]["node"]["node_id"], identity.node_id)
        self.assertEqual(response["result"]["node"]["state_generation"], identity.state_generation)


if __name__ == "__main__":
    unittest.main()
