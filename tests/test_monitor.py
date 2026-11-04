"""Unit tests for cli-agent-bridge Monitor (TUI, Web, Collector and CLI)."""
from __future__ import annotations

import json
import tempfile
import time
import unittest
from io import BytesIO
from pathlib import Path
from typing import Any

from agent_bridge.cli import build_parser
from agent_bridge.monitor import MonitorCollector, render_tui
from agent_bridge.store import Store
from agent_bridge.web_monitor import WebMonitorHandler


class MonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.state = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

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
        store = Store(self.state)
        # Create a session
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
                ("task-1", "sess-1", "Inspect repository structure", "[]", "running", "pending", 0, now),
            )
            # Add a pending question
            q_data = json.dumps({"type": "permission", "tool": "Bash", "input": {"command": "ls -la"}})
            db.execute(
                "INSERT INTO questions(id,session_id,task_id,attempt_id,data,status,created) "
                "VALUES(?,?,?,?,?,?,?)",
                ("q-1", "sess-1", "task-1", "att-1", q_data, "pending", now),
            )
            # Add an event
            ev_data = json.dumps({"tool": "Bash", "command": "ls -la"})
            db.execute(
                "INSERT INTO events(session_id,task_id,attempt_id,kind,data,created) "
                "VALUES(?,?,?,?,?,?)",
                ("sess-1", "task-1", "att-1", "tool_started", ev_data, now),
            )

        collector = MonitorCollector(self.state)
        snapshot = collector.collect(limit=10)

        self.assertEqual(snapshot["metrics"]["total_sessions"], 1)
        self.assertEqual(snapshot["metrics"]["active_sessions"], 1)
        self.assertEqual(snapshot["metrics"]["waiting_questions"], 1)
        self.assertEqual(len(snapshot["pending_questions"]), 1)
        q = snapshot["pending_questions"][0]
        self.assertEqual(q["id"], "q-1")
        self.assertEqual(q["tool"], "Bash")
        self.assertEqual(q["command"], "ls -la")

        # Verify session was tagged as waiting_input
        s = snapshot["sessions"][0]
        self.assertEqual(s["observed"], "waiting_input")
        self.assertTrue(s["waiting_question"])

        # Test TUI rendering
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

        web_args = parser.parse_args(["monitor", "--web", "--port", "9090", "--host", "0.0.0.0"])
        self.assertTrue(web_args.web)
        self.assertEqual(web_args.port, 9090)
        self.assertEqual(web_args.host, "0.0.0.0")

    def test_web_monitor_html_serving(self) -> None:
        collector = MonitorCollector(self.state)
        WebMonitorHandler.collector = collector
        WebMonitorHandler.state = self.state

        class FakeSocket:
            def __init__(self, request_bytes: bytes) -> None:
                self.rfile = BytesIO(request_bytes)
                self.wfile = BytesIO()

            def makefile(self, mode: str, *args: Any) -> BytesIO:
                return self.rfile if "r" in mode else self.wfile

            def sendall(self, data: bytes) -> None:
                self.wfile.write(data)

        # 1. Test GET /
        sock = FakeSocket(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        WebMonitorHandler(sock, ("127.0.0.1", 12345), None)  # type: ignore
        response = sock.wfile.getvalue().decode()
        self.assertIn("HTTP/1.0 200 OK", response)
        self.assertIn("text/html", response)
        self.assertIn("cli-agent-bridge Monitor", response)

        # 2. Test GET /api/snapshot
        sock2 = FakeSocket(b"GET /api/snapshot HTTP/1.1\r\nHost: localhost\r\n\r\n")
        WebMonitorHandler(sock2, ("127.0.0.1", 12345), None)  # type: ignore
        resp2 = sock2.wfile.getvalue().decode()
        self.assertIn("HTTP/1.0 200 OK", resp2)
        self.assertIn("application/json", resp2)
        self.assertIn("metrics", resp2)

    def test_set_session_name_and_cli(self) -> None:
        from agent_bridge.monitor import MonitorCollector, set_session_name
        from agent_bridge.store import Store

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
        sess_map = {s["id"]: s["name"] for s in snapshot["sessions"]}
        self.assertEqual("Custom Pipeline Agent", sess_map.get("sess-custom"))


if __name__ == "__main__":
    unittest.main()
