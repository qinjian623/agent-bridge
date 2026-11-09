"""Unit tests for cli-agent-bridge Monitor (TUI, Web, Collector and CLI)."""
from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import time
import unittest
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

from agent_bridge.cli import build_parser
from agent_bridge.errors import BridgeError
from agent_bridge.hosts import ConfigHostRegistry, HostConfig
from agent_bridge.monitor import MonitorCollector, render_tui
from agent_bridge.peer_protocol import HostState, NodeRef
from agent_bridge.store import Store
from agent_bridge.web_monitor import (
    DEFAULT_ALLOWED_HOSTS,
    HTML_PAGE,
    LOGIN_PAGE,
    MAX_BODY_BYTES,
    WebMonitorHandler,
    WebSessionManager,
    normalize_allowed_hosts,
    read_admin_token,
    run_web_monitor,
)

TOKEN = "test-admin-token-0123456789-abcdefghijklmnopqrstuvwxyz"


class FakeSocket:
    def __init__(self, request_bytes: bytes) -> None:
        self.rfile = BytesIO(request_bytes)
        self.wfile = BytesIO()

    def makefile(self, mode: str, *args: Any) -> BytesIO:
        return self.rfile if "r" in mode else self.wfile

    def sendall(self, data: bytes) -> None:
        self.wfile.write(data)


class HTTPResponse:
    def __init__(self, status: int, headers: dict[str, list[str]], body: str) -> None:
        self.status = status
        self.headers = headers
        self.body = body

    def header(self, name: str) -> str:
        values = self.headers.get(name.lower(), [])
        return values[0] if values else ""

    def json(self) -> dict[str, Any]:
        return json.loads(self.body)


def parse_response(raw: bytes) -> HTTPResponse:
    text = raw.decode("utf-8", "replace")
    head, _, body = text.partition("\r\n\r\n")
    lines = head.split("\r\n")
    status = int(lines[0].split(" ")[1])
    headers: dict[str, list[str]] = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        headers.setdefault(name.strip().lower(), []).append(value.strip())
    return HTTPResponse(status, headers, body)


class FakeBroker:
    """Minimal Unix-socket broker used to verify web approval forwarding."""

    def __init__(self, state: Path, response: Any) -> None:
        self.path = state / "broker.sock"
        self.response = response
        self.requests: list[dict[str, Any]] = []
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.bind(str(self.path))
        self._socket.listen(1)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        try:
            connection, _ = self._socket.accept()
            with connection:
                data = b""
                while b"\n" not in data:
                    chunk = connection.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                if data:
                    self.requests.append(json.loads(data.split(b"\n", 1)[0]))
                if self.response is not None:
                    body = self.response if isinstance(self.response, bytes) else json.dumps(self.response).encode()
                    connection.sendall(body + b"\n")
        except OSError:
            pass
        finally:
            self._socket.close()

    def close(self) -> None:
        self._socket.close()
        self._thread.join(timeout=2)


class MonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.state = Path(self.tmp.name)
        self._write_token(TOKEN)
        WebMonitorHandler.collector = MonitorCollector(self.state)
        WebMonitorHandler.state = self.state
        WebMonitorHandler.allowed_hosts = set(DEFAULT_ALLOWED_HOSTS)
        WebMonitorHandler.public_origin = None
        WebMonitorHandler.sessions = WebSessionManager()

    def tearDown(self) -> None:
        WebMonitorHandler.collector = None
        WebMonitorHandler.state = None
        WebMonitorHandler.allowed_hosts = set(DEFAULT_ALLOWED_HOSTS)
        WebMonitorHandler.public_origin = None
        WebMonitorHandler.sessions = WebSessionManager()
        self.tmp.cleanup()

    # ------------------------------------------------------------------ helpers

    def _write_token(self, value: str, mode: int = 0o600) -> None:
        path = self.state / "admin.token"
        path.write_text(value)
        path.chmod(mode)

    def send(
        self,
        method: str,
        path: str,
        *,
        host: str = "localhost:8080",
        token: str | None = None,
        cookie: str | None = None,
        csrf: str | None = None,
        origin: str | None = None,
        body: Any = None,
        content_length: int | None = None,
    ) -> HTTPResponse:
        payload = b""
        if body is not None:
            payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        lines = [f"{method} {path} HTTP/1.1", f"Host: {host}"]
        if token is not None:
            lines.append(f"Authorization: Bearer {token}")
        if cookie is not None:
            lines.append(f"Cookie: bridge_session={cookie}")
        if csrf is not None:
            lines.append(f"X-CSRF-Token: {csrf}")
        if origin is not None:
            lines.append(f"Origin: {origin}")
        if body is not None or content_length is not None:
            lines.append("Content-Type: application/json")
        length = len(payload) if content_length is None else content_length
        lines.append(f"Content-Length: {length}")
        raw = ("\r\n".join(lines) + "\r\n\r\n").encode() + payload
        sock = FakeSocket(raw)
        WebMonitorHandler(sock, ("127.0.0.1", 12345), None)  # type: ignore[arg-type]
        return parse_response(sock.wfile.getvalue())

    def login(self, token: str | None = None, origin: str = "http://localhost:8080") -> tuple[str, dict[str, Any]]:
        response = self.send(
            "POST",
            "/auth/login",
            body={"token": TOKEN if token is None else token},
            origin=origin,
        )
        self.assertEqual(200, response.status, response.body)
        payload = response.json()
        cookie = response.header("set-cookie")
        session_id = cookie.split(";", 1)[0].split("=", 1)[1]
        return session_id, payload

    def _create_pending_question(self) -> None:
        store = Store(self.state)
        with store.connection(write=True) as db:
            now = time.time()
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,desired,observed,revision,created) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("sess-1", "claude-deepseek", str(self.state), "read-only", "open", "idle", 1, now),
            )
            db.execute(
                "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,reservation,created) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("task-1", "sess-1", "Approve something", "[]", "running", "pending", 0, now),
            )
            db.execute(
                "INSERT INTO questions(id,session_id,task_id,attempt_id,data,status,created) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    "q-1",
                    "sess-1",
                    "task-1",
                    "att-1",
                    json.dumps({"type": "permission", "tool": "Bash", "input": {"command": "ls -la"}}),
                    "pending",
                    now,
                ),
            )

    def _pending_question(self) -> str:
        store = Store(self.state)
        with store.connection() as db:
            return db.execute("SELECT status FROM questions WHERE id='q-1'").fetchone()[0]

    @staticmethod
    def _respond_payload() -> dict[str, Any]:
        return {
            "session_id": "sess-1",
            "question_id": "q-1",
            "decision": "allow",
            "expected_revision": 1,
            "request_id": "web-respond-1",
        }

    # ------------------------------------------------------------------ collector / TUI / CLI

    def test_collector_empty_state(self) -> None:
        collector = MonitorCollector(self.state)
        snapshot = collector.collect(limit=10)
        self.assertIn("timestamp", snapshot)
        self.assertIn("coordinator", snapshot)
        self.assertIn("hosts", snapshot)
        self.assertIn("sessions", snapshot)
        self.assertIn("pending_questions", snapshot)
        self.assertIn("metrics", snapshot)
        self.assertEqual(snapshot["metrics"]["total_sessions"], 0)
        self.assertEqual(snapshot["metrics"]["active_sessions"], 0)
        self.assertEqual(snapshot["metrics"]["waiting_questions"], 0)

    def test_collector_with_sessions_tasks_and_approvals(self) -> None:
        self._create_pending_question()
        collector = MonitorCollector(self.state)
        snapshot = collector.collect(limit=10)

        self.assertEqual(snapshot["metrics"]["total_sessions"], 1)
        self.assertEqual(snapshot["metrics"]["active_sessions"], 1)
        self.assertEqual(snapshot["metrics"]["waiting_questions"], 1)
        self.assertEqual(len(snapshot["pending_questions"]), 1)
        question = snapshot["pending_questions"][0]
        self.assertEqual(question["id"], "q-1")
        self.assertEqual(question["tool"], "Bash")
        self.assertEqual(question["command"], "ls -la")

        session = snapshot["sessions"][0]
        self.assertEqual(session["observed"], "waiting_input")
        self.assertTrue(session["waiting_question"])

        tui = render_tui(snapshot, width=100)
        self.assertIn("cli-agent-bridge Monitor", tui)
        self.assertIn("ACTION REQUIRED", tui)
        self.assertIn("sess-1", tui)
        self.assertIn("Bash", tui)

    def test_cli_parser_monitor_arguments(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["monitor", "--once", "--format", "json"])
        self.assertEqual(args.command, "monitor")
        self.assertTrue(args.once)
        self.assertEqual(args.format, "json")
        self.assertIsNone(args.allowed_hosts)
        self.assertIsNone(args.public_origin)

        web_args = parser.parse_args(
            [
                "monitor",
                "--web",
                "--port",
                "9090",
                "--host",
                "0.0.0.0",
                "--allowed-hosts",
                "localhost,example.com",
                "--allowed-hosts",
                "10.0.0.2:8080",
                "--public-origin",
                "https://monitor.example.com",
            ]
        )
        self.assertTrue(web_args.web)
        self.assertEqual(web_args.port, 9090)
        self.assertEqual(web_args.host, "0.0.0.0")
        self.assertEqual(["localhost", "example.com", "10.0.0.2:8080"], web_args.allowed_hosts)
        self.assertEqual("https://monitor.example.com", web_args.public_origin)
        self.assertEqual(
            {"localhost", "example.com", "10.0.0.2:8080"},
            normalize_allowed_hosts(web_args.allowed_hosts),
        )

    def test_set_session_name_and_cli(self) -> None:
        from agent_bridge.monitor import set_session_name

        store = Store(self.state)
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,desired,observed,revision,created) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("sess-custom", "claude-deepseek", str(self.state), "read-only", "open", "idle", 1, time.time()),
            )
        set_session_name(self.state, "sess-custom", "Custom Pipeline Agent")
        collector = MonitorCollector(self.state)
        snapshot = collector.collect()
        session_map = {session["id"]: session["name"] for session in snapshot["sessions"]}
        self.assertEqual("Custom Pipeline Agent", session_map.get("sess-custom"))

    # ------------------------------------------------------------------ admin token safety

    def test_read_admin_token_accepts_secure_file(self) -> None:
        self.assertEqual(TOKEN, read_admin_token(self.state))

    def test_read_admin_token_rejects_insecure_permissions(self) -> None:
        self._write_token(TOKEN, mode=0o644)
        with self.assertRaises(BridgeError) as context:
            read_admin_token(self.state)
        self.assertEqual("permission_denied", context.exception.code)

    def test_read_admin_token_rejects_missing_file(self) -> None:
        (self.state / "admin.token").unlink()
        with self.assertRaises(BridgeError) as context:
            read_admin_token(self.state)
        self.assertEqual("permission_denied", context.exception.code)

    def test_read_admin_token_rejects_non_regular_and_empty_files(self) -> None:
        token_path = self.state / "admin.token"
        token_path.unlink()
        token_path.mkdir()
        with self.assertRaises(BridgeError):
            read_admin_token(self.state)

        token_path.rmdir()
        self._write_token("", mode=0o600)
        with self.assertRaises(BridgeError):
            read_admin_token(self.state)

        self._write_token(TOKEN + " with-space", mode=0o600)
        with self.assertRaises(ValueError):
            read_admin_token(self.state)

    def test_read_admin_token_rejects_foreign_owner(self) -> None:
        with patch("agent_bridge.web_monitor.os.getuid", return_value=os.getuid() + 1):
            with self.assertRaises(BridgeError) as context:
                read_admin_token(self.state)
        self.assertEqual("permission_denied", context.exception.code)

    def test_run_web_monitor_forwards_host_and_origin_configuration(self) -> None:
        class FakeServer:
            def __init__(self, address: tuple[str, int], handler: Any) -> None:
                self.address = address
                self.handler = handler
                FakeServer.instance = self

            def serve_forever(self) -> None:
                raise KeyboardInterrupt

            def server_close(self) -> None:
                pass

        with patch("agent_bridge.web_monitor.ThreadingHTTPServer", FakeServer):
            run_web_monitor(
                self.state,
                host="0.0.0.0",
                port=9999,
                allowed_hosts=["monitor.example.com", "127.0.0.1:9999"],
                public_origin="https://monitor.example.com/",
            )
        self.assertEqual(("0.0.0.0", 9999), FakeServer.instance.address)
        self.assertEqual(
            {"monitor.example.com", "127.0.0.1:9999"},
            WebMonitorHandler.allowed_hosts,
        )
        self.assertEqual("https://monitor.example.com", WebMonitorHandler.public_origin)

    def test_run_web_monitor_refuses_insecure_startup(self) -> None:
        with tempfile.TemporaryDirectory() as empty:
            with self.assertRaises(BridgeError):
                run_web_monitor(Path(empty))
        with self.assertRaises(ValueError):
            run_web_monitor(self.state, public_origin="not an origin")

    # ------------------------------------------------------------------ login / session lifecycle

    def test_login_issues_http_only_same_site_cookie_and_csrf(self) -> None:
        response = self.send(
            "POST",
            "/auth/login",
            body={"token": TOKEN},
            origin="http://localhost:8080",
        )
        self.assertEqual(200, response.status)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertIn("csrf_token", payload)
        self.assertGreater(payload["expires_at"], time.time() + 7 * 3600)
        self.assertNotIn(TOKEN, response.body)
        cookie = response.header("set-cookie")
        session_id = cookie.split(";", 1)[0].split("=", 1)[1]
        self.assertNotIn(session_id, response.body)
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=Strict", cookie)
        self.assertIn("Path=/", cookie)
        self.assertNotIn("Secure", cookie)

    def test_login_failure_returns_401(self) -> None:
        response = self.send(
            "POST",
            "/auth/login",
            body={"token": "wrong-token"},
            origin="http://localhost:8080",
        )
        self.assertEqual(401, response.status)
        self.assertFalse(response.json()["ok"])
        self.assertNotIn("wrong-token", response.body)

    def test_login_cookie_is_secure_with_https_public_origin(self) -> None:
        WebMonitorHandler.public_origin = "https://monitor.example.com"
        response = self.send(
            "POST",
            "/auth/login",
            body={"token": TOKEN},
            origin="https://monitor.example.com",
        )
        self.assertEqual(200, response.status)
        self.assertIn("; Secure", response.header("set-cookie"))

    def test_token_rotation_invalidates_existing_sessions_and_old_headers(self) -> None:
        session_id, _ = self.login()
        self.assertEqual(200, self.send("GET", "/api/snapshot", cookie=session_id).status)

        rotated = "rotated-admin-token-value"
        self._write_token(rotated)

        self.assertEqual(401, self.send("GET", "/api/snapshot", cookie=session_id).status)
        self.assertEqual(401, self.send("GET", "/api/snapshot", token=TOKEN).status)
        self.assertEqual(200, self.send("GET", "/api/snapshot", token=rotated).status)
        old_login = self.send(
            "POST", "/auth/login", body={"token": TOKEN}, origin="http://localhost:8080"
        )
        self.assertEqual(401, old_login.status)

    def test_missing_token_file_rejects_access(self) -> None:
        session_id, _ = self.login()
        (self.state / "admin.token").unlink()
        self.assertEqual(401, self.send("GET", "/api/snapshot", cookie=session_id).status)
        self.assertEqual(401, self.send("GET", "/api/snapshot", token=TOKEN).status)
        response = self.send(
            "POST", "/auth/login", body={"token": TOKEN}, origin="http://localhost:8080"
        )
        self.assertEqual(503, response.status)

    def test_logout_revokes_session_and_clears_cookie(self) -> None:
        session_id, _ = self.login()
        self.assertEqual(200, self.send("GET", "/api/snapshot", cookie=session_id).status)

        response = self.send(
            "POST",
            "/auth/logout",
            cookie=session_id,
            origin="http://localhost:8080",
        )
        self.assertEqual(200, response.status)
        self.assertIn("Max-Age=0", response.header("set-cookie"))
        self.assertEqual(401, self.send("GET", "/api/snapshot", cookie=session_id).status)

        self.assertEqual(
            401,
            self.send("POST", "/auth/logout", origin="http://localhost:8080").status,
        )

    def test_auth_session_returns_csrf_and_expiry(self) -> None:
        session_id, login_payload = self.login()
        response = self.send("GET", "/auth/session", cookie=session_id)
        self.assertEqual(200, response.status)
        payload = response.json()
        self.assertEqual(login_payload["csrf_token"], payload["csrf_token"])
        self.assertEqual(login_payload["expires_at"], payload["expires_at"])
        self.assertNotIn(TOKEN, response.body)
        self.assertEqual(401, self.send("GET", "/auth/session").status)

    # ------------------------------------------------------------------ read endpoints and headers

    def test_snapshot_requires_bearer_or_cookie_and_sets_security_headers(self) -> None:
        unauthenticated = self.send("GET", "/api/snapshot")
        self.assertEqual(401, unauthenticated.status)
        self.assertNotIn("access-control-allow-origin", unauthenticated.headers)

        session_id, _ = self.login()
        for kwargs in ({"token": TOKEN}, {"cookie": session_id}):
            response = self.send("GET", "/api/snapshot", **kwargs)
            self.assertEqual(200, response.status)
            self.assertIn("metrics", response.json())
            self.assertEqual("no-store", response.header("cache-control"))
            self.assertEqual("nosniff", response.header("x-content-type-options"))
            self.assertEqual("no-referrer", response.header("referrer-policy"))
            self.assertIn("default-src 'self'", response.header("content-security-policy"))
            self.assertIn("frame-ancestors 'none'", response.header("content-security-policy"))
            self.assertIn("base-uri 'none'", response.header("content-security-policy"))
            self.assertNotIn("access-control-allow-origin", response.headers)

    def test_root_redirects_unauthenticated_and_serves_dashboard_when_authenticated(self) -> None:
        redirect = self.send("GET", "/")
        self.assertEqual(302, redirect.status)
        self.assertEqual("/login", redirect.header("location"))

        login_page = self.send("GET", "/login")
        self.assertEqual(200, login_page.status)
        self.assertIn('id="loginForm"', login_page.body)
        self.assertNotIn(TOKEN, login_page.body)
        self.assertEqual("no-store", login_page.header("cache-control"))

        session_id, _ = self.login()
        dashboard = self.send("GET", "/", cookie=session_id)
        self.assertEqual(200, dashboard.status)
        self.assertIn("cli-agent-bridge Monitor", dashboard.body)
        self.assertIn("text/html", dashboard.header("content-type"))

    def test_unknown_route_and_unsupported_method(self) -> None:
        self.assertEqual(404, self.send("GET", "/missing").status)
        self.assertEqual(405, self.send("POST", "/api/snapshot", token=TOKEN).status)

    # ------------------------------------------------------------------ Host / Origin / size checks

    def test_host_header_validation(self) -> None:
        rejected = self.send("GET", "/login", host="evil.example")
        self.assertEqual(400, rejected.status)
        self.assertEqual("no-store", rejected.header("cache-control"))

        self.assertEqual(200, self.send("GET", "/login", host="localhost").status)
        self.assertEqual(200, self.send("GET", "/login", host="127.0.0.1:8080").status)

        WebMonitorHandler.allowed_hosts = {"monitor.example.com"}
        self.assertEqual(200, self.send("GET", "/login", host="monitor.example.com").status)
        self.assertEqual(400, self.send("GET", "/login", host="localhost").status)

    def test_login_origin_validation(self) -> None:
        rejected = self.send(
            "POST",
            "/auth/login",
            body={"token": TOKEN},
            origin="https://evil.example",
        )
        self.assertEqual(403, rejected.status)
        accepted = self.send(
            "POST",
            "/auth/login",
            body={"token": TOKEN},
            origin="http://127.0.0.1:8080",
        )
        self.assertEqual(200, accepted.status)
        # Non-browser clients may omit Origin for login.
        no_origin = self.send("POST", "/auth/login", body={"token": TOKEN})
        self.assertEqual(200, no_origin.status)

    def test_cookie_writes_require_matching_origin(self) -> None:
        self._create_pending_question()
        session_id, payload = self.login()
        body = self._respond_payload()

        missing = self.send(
            "POST",
            "/api/respond",
            cookie=session_id,
            csrf=payload["csrf_token"],
            body=body,
        )
        self.assertEqual(403, missing.status)
        mismatched = self.send(
            "POST",
            "/api/respond",
            cookie=session_id,
            csrf=payload["csrf_token"],
            origin="https://evil.example",
            body=body,
        )
        self.assertEqual(403, mismatched.status)
        self.assertEqual("pending", self._pending_question())

    def test_body_size_limit_rejects_oversized_requests(self) -> None:
        response = self.send(
            "POST",
            "/auth/login",
            body={"token": TOKEN},
            origin="http://localhost:8080",
            content_length=MAX_BODY_BYTES + 1,
        )
        self.assertEqual(413, response.status)
        self.assertEqual("no-store", response.header("cache-control"))

        logout = self.send(
            "POST",
            "/auth/logout",
            origin="http://localhost:8080",
            content_length=MAX_BODY_BYTES + 1,
        )
        self.assertEqual(413, logout.status)

    # ------------------------------------------------------------------ CSRF and Broker forwarding

    def test_respond_cookie_requires_csrf_token(self) -> None:
        self._create_pending_question()
        session_id, payload = self.login()
        body = self._respond_payload()

        missing = self.send(
            "POST",
            "/api/respond",
            cookie=session_id,
            origin="http://localhost:8080",
            body=body,
        )
        self.assertEqual(403, missing.status)
        mismatched = self.send(
            "POST",
            "/api/respond",
            cookie=session_id,
            csrf="wrong-csrf",
            origin="http://localhost:8080",
            body=body,
        )
        self.assertEqual(403, mismatched.status)
        unauthenticated = self.send(
            "POST",
            "/api/respond",
            csrf=payload["csrf_token"],
            origin="http://localhost:8080",
            body=body,
        )
        self.assertEqual(401, unauthenticated.status)
        self.assertEqual("pending", self._pending_question())

    def test_respond_forwards_to_broker_and_does_not_write_store(self) -> None:
        self._create_pending_question()
        session_id, payload = self.login()
        broker = FakeBroker(self.state, {"result": {"question_id": "q-1", "revision": 2}})
        try:
            response = self.send(
                "POST",
                "/api/respond",
                cookie=session_id,
                csrf=payload["csrf_token"],
                origin="http://localhost:8080",
                body=self._respond_payload(),
            )
        finally:
            broker.close()

        self.assertEqual(200, response.status)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(1, len(broker.requests))
        request = broker.requests[0]
        self.assertEqual("agent_control", request["method"])
        self.assertEqual(TOKEN, request["token"])
        for args in (request["params"], request["args"]):
            self.assertEqual(TOKEN, args["token"])
            self.assertEqual("respond", args["action"])
            self.assertEqual("sess-1", args["session_id"])
            self.assertEqual("q-1", args["question_id"])
            self.assertEqual({"decision": "allow"}, args["answer"])
            self.assertEqual(1, args["expected_revision"])
            self.assertEqual("web-respond-1", args["request_id"])
        # The handler must never fall back to Store.respond().
        self.assertEqual("pending", self._pending_question())

    def test_respond_returns_503_when_broker_socket_is_unavailable(self) -> None:
        self._create_pending_question()
        session_id, payload = self.login()
        response = self.send(
            "POST",
            "/api/respond",
            cookie=session_id,
            csrf=payload["csrf_token"],
            origin="http://localhost:8080",
            body=self._respond_payload(),
        )
        self.assertEqual(503, response.status)
        self.assertEqual({"ok": False, "error": "Broker unavailable"}, response.json())
        self.assertEqual("pending", self._pending_question())

    def test_respond_maps_stale_revision_to_409(self) -> None:
        self._create_pending_question()
        session_id, payload = self.login()
        broker = FakeBroker(
            self.state,
            {"error": {"code": "stale_revision", "message": "Inspect the session before answering"}},
        )
        try:
            response = self.send(
                "POST",
                "/api/respond",
                cookie=session_id,
                csrf=payload["csrf_token"],
                origin="http://localhost:8080",
                body=self._respond_payload(),
            )
        finally:
            broker.close()

        self.assertEqual(409, response.status)
        self.assertFalse(response.json()["ok"])
        self.assertEqual("pending", self._pending_question())

    def test_respond_with_bearer_token_does_not_require_csrf(self) -> None:
        self._create_pending_question()
        broker = FakeBroker(self.state, {"result": {"question_id": "q-1", "revision": 2}})
        try:
            response = self.send(
                "POST",
                "/api/respond",
                token=TOKEN,
                origin="http://localhost:8080",
                body=self._respond_payload(),
            )
        finally:
            broker.close()
        self.assertEqual(200, response.status)
        self.assertTrue(response.json()["ok"])

    # ------------------------------------------------------------------ frontend safety

    def test_dashboard_uses_dom_text_apis_instead_of_inner_html(self) -> None:
        self.assertNotIn("innerHTML", HTML_PAGE)
        self.assertIn("textContent", HTML_PAGE)
        self.assertIn("createTextNode", HTML_PAGE)
        self.assertIn("addEventListener", HTML_PAGE)
        self.assertNotIn(TOKEN, LOGIN_PAGE)
        self.assertIn("hostTable", HTML_PAGE)
        self.assertIn("statNodes", HTML_PAGE)
        self.assertIn("Cluster Nodes (Execution Hosts)", HTML_PAGE)

    def test_collector_cluster_hosts(self) -> None:
        (self.state / "config.json").write_text(
            json.dumps({
                "hosts": {
                    "gpu-1": {
                        "ssh_target": "gpu-1.internal",
                        "remote_executable": "/usr/local/bin/agent-bridge",
                        "projects": {"bridge": "/srv/bridge"},
                    }
                }
            })
        )
        (self.state / "host_pins.json").write_text(
            json.dumps({
                "version": 1,
                "hosts": {
                    "gpu-1": {
                        "host_id": "gpu-1",
                        "node_id": "node-gpu-12345",
                        "state_generation": "gen-gpu-67890",
                    }
                }
            })
        )
        collector = MonitorCollector(self.state)
        snapshot = collector.collect(limit=5)
        self.assertEqual(2, snapshot["metrics"]["total_hosts"])
        self.assertEqual(1, len(snapshot["hosts"]))
        host = snapshot["hosts"][0]
        self.assertEqual("gpu-1", host["host_id"])
        self.assertEqual("gpu-1.internal", host["ssh_target"])
        self.assertEqual("node-gpu-12345", host["node_id"])
        self.assertEqual({"bridge": "/srv/bridge"}, host["projects"])

        tui = render_tui(snapshot, width=120)
        self.assertIn("Cluster Nodes (2)", tui)
        self.assertIn("coordinator", tui)
        self.assertIn("gpu-1", tui)

    def test_collector_uses_live_registry_without_resetting_host_status(self) -> None:
        registry = ConfigHostRegistry({
            "macos-node": HostConfig("macos-node", "macos-node", "/opt/agent-bridge", pinned=NodeRef("macos-node", "node-c", "gen-c")),
        })
        registry.mark("macos-node", HostState.ONLINE)
        snapshot = MonitorCollector(self.state, host_registry=registry).collect()
        self.assertEqual(snapshot["hosts"][0]["state"], "online")
        self.assertIsNotNone(snapshot["hosts"][0]["last_contact"])
        self.assertEqual(snapshot["metrics"]["online_hosts"], 2)

    def test_standalone_collector_reads_broker_snapshot(self) -> None:
        (self.state / "broker.sock").touch()
        expected = {"hosts": [{"host_id": "macos-node", "state": "online"}]}
        with patch("agent_bridge.broker.request", new_callable=AsyncMock, return_value=expected) as request:
            result = MonitorCollector(self.state).collect(limit=3, include_events=False)
        self.assertEqual(result, expected)
        request.assert_awaited_once_with(self.state, "agent_monitor", {"limit": 3, "include_events": False})

    def test_standalone_collector_keeps_local_view_if_broker_is_unavailable(self) -> None:
        (self.state / "broker.sock").touch()
        with patch("agent_bridge.broker.request", new_callable=AsyncMock, side_effect=ConnectionRefusedError):
            snapshot = MonitorCollector(self.state).collect()
        self.assertIn("hosts", snapshot)
        self.assertIn("sessions", snapshot)

    def test_host_ping_endpoint_forwards_to_broker(self) -> None:
        broker = FakeBroker(self.state, {"result": {"entries": [], "stale": False}})
        try:
            response = self.send(
                "POST",
                "/api/host/ping",
                token=TOKEN,
                origin="http://localhost:8080",
                body={"host_id": "gpu-1"},
            )
            self.assertEqual(1, len(broker.requests))
            req = broker.requests[0]
            self.assertEqual("agent_catalog", req.get("method"))
            self.assertEqual("gpu-1", req.get("params", {}).get("host_id"))
            self.assertEqual(200, response.status)
            self.assertTrue(response.json()["ok"])
        finally:
            broker.close()


if __name__ == "__main__":
    unittest.main()
