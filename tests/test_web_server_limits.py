"""Regression tests for Web monitor request limits and default Host derivation."""

from __future__ import annotations

import contextlib
import io
import json
import socket
import tempfile
import threading
import time
import unittest
from http.client import HTTPConnection
from pathlib import Path
from unittest.mock import patch

import agent_bridge.web_monitor as web_monitor
from agent_bridge.monitor import MonitorCollector
from agent_bridge.web_monitor import (
    DEFAULT_ALLOWED_HOSTS,
    HTTP_REQUEST_TIMEOUT_SECONDS,
    MAX_CONCURRENT_HTTP_REQUESTS,
    BoundedThreadingHTTPServer,
    WebMonitorHandler,
    WebSessionManager,
    loopback_allowed_hosts,
    run_web_monitor,
)

TOKEN = "test-admin-token-0123456789-abcdefghijklmnopqrstuvwxyz"
ROTATED_TOKEN = "rotated-admin-token-0123456789-abcdefghijklmnopqrstuvwxyz"


class ServerTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.state = Path(self.tmp.name)
        token_path = self.state / "admin.token"
        token_path.write_text(TOKEN)
        token_path.chmod(0o600)
        WebMonitorHandler.collector = MonitorCollector(self.state)
        WebMonitorHandler.state = self.state
        WebMonitorHandler.allowed_hosts = set(DEFAULT_ALLOWED_HOSTS)
        WebMonitorHandler.public_origin = None
        WebMonitorHandler.sessions = WebSessionManager()
        self.server: BoundedThreadingHTTPServer | None = None
        self.server_thread: threading.Thread | None = None
        self.port = 0
        self.sockets: list[socket.socket] = []

    def tearDown(self) -> None:
        for sock in self.sockets:
            sock.close()
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.server_thread is not None:
            self.server_thread.join(timeout=3)
        WebMonitorHandler.collector = None
        WebMonitorHandler.state = None
        WebMonitorHandler.allowed_hosts = set(DEFAULT_ALLOWED_HOSTS)
        WebMonitorHandler.public_origin = None
        WebMonitorHandler.sessions = WebSessionManager()
        self.tmp.cleanup()

    def start_server(
        self,
        *,
        max_concurrent_requests: int = MAX_CONCURRENT_HTTP_REQUESTS,
        request_timeout: float = HTTP_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self.server = BoundedThreadingHTTPServer(
            ("127.0.0.1", 0),
            WebMonitorHandler,
            max_concurrent_requests=max_concurrent_requests,
            request_timeout=request_timeout,
        )
        self.port = int(self.server.server_address[1])
        WebMonitorHandler.allowed_hosts = loopback_allowed_hosts(self.port)
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        self.server_thread.start()

    def connect(self) -> socket.socket:
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=2)
        self.sockets.append(sock)
        return sock

    def request(
        self,
        method: str,
        path: str,
        *,
        host: str = "localhost",
        body: object | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 2,
    ) -> tuple[int, dict[str, str], bytes]:
        request_headers = {"Host": host}
        request_headers.update(headers or {})
        payload: bytes | None = None
        if body is not None:
            payload = body if isinstance(body, bytes) else json.dumps(body).encode()
            request_headers.setdefault("Content-Type", "application/json")
            request_headers["Content-Length"] = str(len(payload))
        connection = HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            connection.request(method, path, body=payload, headers=request_headers)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def wait_for(self, predicate, *, timeout: float = 3, message: str = "condition") -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        self.fail(f"Timed out waiting for {message}")

    def read_until(self, sock: socket.socket, marker: bytes, *, timeout: float = 3) -> bytes:
        sock.settimeout(timeout)
        data = bytearray()
        while marker not in data:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data.extend(chunk)
        return bytes(data)

    def read_until_closed(self, sock: socket.socket, *, timeout: float = 3) -> bytes:
        sock.settimeout(timeout)
        data = bytearray()
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                return bytes(data)
            data.extend(chunk)

    def slot_count(self) -> int:
        assert self.server is not None
        return self.server._request_slots._value


class WebServerLimitTests(ServerTestBase):
    def test_server_limits_follow_module_constants(self) -> None:
        with (
            patch.object(web_monitor, "HTTP_REQUEST_TIMEOUT_SECONDS", 0.3),
            patch.object(web_monitor, "MAX_CONCURRENT_HTTP_REQUESTS", 2),
        ):
            server = BoundedThreadingHTTPServer(("127.0.0.1", 0), WebMonitorHandler)
            try:
                self.assertEqual(0.3, server.request_timeout)
                self.assertEqual(2, server._request_slots._value)
            finally:
                server.server_close()

    def test_partial_body_times_out_and_releases_capacity(self) -> None:
        self.start_server(max_concurrent_requests=1, request_timeout=0.2)
        slow = self.connect()
        slow.sendall(
            b"POST /auth/login HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Origin: http://localhost\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 1\r\n\r\n"
        )
        self.wait_for(lambda: self.slot_count() == 0, message="slow body to occupy the only slot")

        status, _, _ = self.request("GET", "/login")
        self.assertEqual(503, status)

        response = self.read_until_closed(slow)
        self.assertIn(b" 408 ", response.split(b"\r\n", 1)[0])
        self.wait_for(lambda: self.slot_count() == 1, message="timed-out body to release its slot")

        status, headers, body = self.request(
            "POST",
            "/auth/login",
            body={"token": TOKEN},
            headers={"Origin": "http://localhost"},
        )
        self.assertEqual(200, status, body)
        self.assertIn("bridge_session=", headers.get("Set-Cookie", ""))
        self.assertIn("csrf_token", json.loads(body))

    def test_partial_headers_time_out_and_release_capacity(self) -> None:
        self.start_server(max_concurrent_requests=1, request_timeout=0.2)
        slow = self.connect()
        slow.sendall(b"GET /login HTTP/1.1\r\nHost: localhost\r\n")
        self.wait_for(lambda: self.slot_count() == 0, message="partial headers to occupy the only slot")

        started = time.monotonic()
        self.read_until_closed(slow)
        self.assertLess(time.monotonic() - started, 2)
        self.wait_for(lambda: self.slot_count() == 1, message="timed-out headers to release their slot")

        status, _, body = self.request("GET", "/login")
        self.assertEqual(200, status, body)

    def test_slow_trickled_headers_hit_absolute_deadline(self) -> None:
        self.start_server(max_concurrent_requests=1, request_timeout=0.12)
        slow = self.connect()
        payload = (
            b"GET /login HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"X-Trickle: " + b"a" * 12 + b"\r\n\r\n"
        )

        def trickle() -> None:
            for byte in payload:
                try:
                    slow.sendall(bytes((byte,)))
                except OSError:
                    return
                time.sleep(0.025)

        sender = threading.Thread(target=trickle, daemon=True)
        sender.start()
        try:
            self.wait_for(
                lambda: self.slot_count() == 0,
                message="trickled request to occupy the only slot",
            )
            started = time.monotonic()
            self.wait_for(
                lambda: self.slot_count() == 1,
                timeout=0.6,
                message="trickled headers to hit the absolute deadline",
            )
            self.assertLess(time.monotonic() - started, 0.6)
            response = self.read_until_closed(slow, timeout=1)
            self.assertTrue(response == b"" or b" 408 " in response.split(b"\r\n", 1)[0])
        finally:
            slow.close()
            sender.join(timeout=2)

        status, _, body = self.request("GET", "/login")
        self.assertEqual(200, status, body)

    def test_slow_trickled_body_hits_absolute_deadline(self) -> None:
        self.start_server(max_concurrent_requests=1, request_timeout=0.12)
        slow = self.connect()
        body = json.dumps({"token": TOKEN}).encode()
        slow.sendall(
            b"POST /auth/login HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n"
        )

        def trickle() -> None:
            for byte in body:
                try:
                    slow.sendall(bytes((byte,)))
                except OSError:
                    return
                time.sleep(0.025)

        sender = threading.Thread(target=trickle, daemon=True)
        sender.start()
        try:
            self.wait_for(
                lambda: self.slot_count() == 0,
                message="trickled body to occupy the only slot",
            )
            response = self.read_until_closed(slow, timeout=1)
            self.assertIn(b" 408 ", response.split(b"\r\n", 1)[0])
            self.wait_for(
                lambda: self.slot_count() == 1,
                message="timed-out trickled body to release its slot",
            )
        finally:
            slow.close()
            sender.join(timeout=2)

        status, _, body = self.request("GET", "/login")
        self.assertEqual(200, status, body)

    def test_overload_refuses_new_connection_and_recovers(self) -> None:
        self.start_server(max_concurrent_requests=1, request_timeout=5)
        slow = self.connect()
        slow.sendall(b"GET /login HTTP/1.1\r\nHost: localhost\r\n")
        self.wait_for(lambda: self.slot_count() == 0, message="slow request to occupy the only slot")

        status, _, _ = self.request("GET", "/login")
        self.assertEqual(503, status)

        slow.close()
        self.wait_for(lambda: self.slot_count() == 1, message="slow client close to release its slot")
        status, _, body = self.request("GET", "/login")
        self.assertEqual(200, status, body)

    def test_long_lived_sse_client_occupies_and_releases_request_slot(self) -> None:
        baseline = web_monitor._ACTIVE_SSE_CLIENTS
        self.start_server(max_concurrent_requests=1, request_timeout=0.5)
        stream = self.connect()
        stream.sendall(
            b"GET /api/v1/stream?max_seconds=3&interval=0.2 HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Authorization: Bearer " + TOKEN.encode() + b"\r\n"
            b"Connection: close\r\n\r\n"
        )
        headers = self.read_until(stream, b"\r\n\r\n")
        self.assertIn(b" 200 ", headers.split(b"\r\n", 1)[0])
        self.assertIn(b"text/event-stream", headers.lower())
        self.wait_for(
            lambda: web_monitor._ACTIVE_SSE_CLIENTS == baseline + 1,
            message="SSE client count to increase",
        )
        self.wait_for(lambda: self.slot_count() == 0, message="SSE client to hold its request slot")
        time.sleep(0.7)
        self.assertEqual(baseline + 1, web_monitor._ACTIVE_SSE_CLIENTS)
        self.assertEqual(0, self.slot_count())

        status, _, _ = self.request("GET", "/login")
        self.assertEqual(503, status)

        stream.close()
        self.wait_for(
            lambda: web_monitor._ACTIVE_SSE_CLIENTS == baseline,
            timeout=4,
            message="SSE client count to return to baseline",
        )
        self.wait_for(lambda: self.slot_count() == 1, message="SSE slot to be released")
        status, _, body = self.request("GET", "/login")
        self.assertEqual(200, status, body)

    def test_sse_token_rotation_ends_stream_and_releases_slot(self) -> None:
        baseline = web_monitor._ACTIVE_SSE_CLIENTS
        self.start_server(max_concurrent_requests=1, request_timeout=2)
        stream = self.connect()
        stream.sendall(
            b"GET /api/v1/stream?max_seconds=4&interval=0.2 HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Authorization: Bearer " + TOKEN.encode() + b"\r\n"
            b"Connection: close\r\n\r\n"
        )
        headers = self.read_until(stream, b"\r\n\r\n")
        self.assertIn(b" 200 ", headers.split(b"\r\n", 1)[0])
        self.wait_for(
            lambda: web_monitor._ACTIVE_SSE_CLIENTS == baseline + 1,
            message="SSE client count to increase",
        )

        token_path = self.state / "admin.token"
        token_path.write_text(ROTATED_TOKEN)
        token_path.chmod(0o600)
        self.read_until_closed(stream, timeout=4)
        self.wait_for(
            lambda: web_monitor._ACTIVE_SSE_CLIENTS == baseline,
            timeout=4,
            message="token rotation to release the SSE client",
        )
        self.wait_for(lambda: self.slot_count() == 1, message="rotated SSE slot to be released")


class WebMonitorRunTests(ServerTestBase):
    def test_default_hosts_follow_the_ephemeral_bound_port(self) -> None:
        class CapturingServer(BoundedThreadingHTTPServer):
            instance: CapturingServer | None = None
            started = threading.Event()

            def __init__(self, address: tuple[str, int], handler) -> None:
                super().__init__(address, handler)
                type(self).instance = self

            def serve_forever(self, poll_interval: float = 0.5) -> None:
                type(self).started.set()
                super().serve_forever(poll_interval)

        CapturingServer.started.clear()
        CapturingServer.instance = None
        runner: threading.Thread | None = None
        with (
            patch("agent_bridge.web_monitor.BoundedThreadingHTTPServer", CapturingServer),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            runner = threading.Thread(
                target=run_web_monitor,
                kwargs={"state": self.state, "host": "127.0.0.1", "port": 0},
                daemon=True,
            )
            runner.start()
            self.assertTrue(CapturingServer.started.wait(3), "Web monitor did not start")
            server = CapturingServer.instance
            assert server is not None
            port = int(server.server_address[1])
            try:
                self.assertIn(f"127.0.0.1:{port}", WebMonitorHandler.allowed_hosts)
                self.assertIn(f"localhost:{port}", WebMonitorHandler.allowed_hosts)

                connection = HTTPConnection("127.0.0.1", port, timeout=2)
                try:
                    connection.request("GET", "/login", headers={"Host": f"127.0.0.1:{port}"})
                    response = connection.getresponse()
                    self.assertEqual(200, response.status, response.read())
                finally:
                    connection.close()

                connection = HTTPConnection("127.0.0.1", port, timeout=2)
                try:
                    connection.request("GET", "/login", headers={"Host": f"evil.example:{port}"})
                    response = connection.getresponse()
                    self.assertEqual(400, response.status, response.read())
                finally:
                    connection.close()
            finally:
                server.shutdown()
                if runner is not None:
                    runner.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
