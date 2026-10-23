import io
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_bridge import telegram_guard
from agent_bridge.codex_parent import advance, main, read_resume_config


class Busy(Exception):
    pass


class ParentTests(unittest.TestCase):
    def test_client_setup_failure_reports_deferred_before_any_enqueue(self):
        constructor = Mock(side_effect=ConnectionError("daemon unavailable"))
        app_server = types.ModuleType("codex_async.app_server")
        app_server.CodexAppServer = constructor
        app_server.ThreadBusyError = Busy
        output = io.StringIO()
        with patch.dict(sys.modules, {"codex_async": types.ModuleType("codex_async"), "codex_async.app_server": app_server, "telegram_guard": telegram_guard}), patch("sys.stdin", io.StringIO(json.dumps({"cwd": "/tmp", "thread_id": "parent", "message": "test"}))), patch("sys.stdout", output):
            main()
        self.assertIn("setup failed", json.loads(output.getvalue())["deferred"])
        self.assertEqual(1, constructor.call_count)

    def test_attention_flags_never_report_ready_or_resume(self):
        calls = []

        class Client:
            def request(self, method, params):
                calls.append(method)
                return {"thread": {"status": {"type": "active", "activeFlags": ["waitingOnApproval"]}}}

        result = advance(Client(), {"thread_id": "parent", "receipt": "receipt"}, Busy)
        self.assertIn("waitingOnApproval", result["deferred"])
        self.assertNotIn("ready", result)
        self.assertEqual(["thread/read"], calls)

    def test_cold_resume_does_not_change_parent_permissions_or_model(self):
        calls = []

        class Client:
            def request(self, method, params):
                calls.append((method, params))
                return {
                    "thread/read": {"thread": {"status": {"type": "notLoaded"}}},
                    "thread/queue/list": {"data": [{"id": "receipt"}]},
                    "thread/resume": {},
                }[method]

        result = advance(Client(), {"thread_id": "parent", "receipt": "receipt"}, Busy)
        self.assertTrue(result["ready"])
        self.assertEqual(("thread/resume", {"threadId": "parent", "excludeTurns": True}), calls[-1])

    def test_inspect_does_not_queue_or_wake(self):
        class Client:
            def request(self, method, params):
                if method != "thread/read":
                    raise AssertionError(method)
                return {"thread": {"status": {"type": "notLoaded"}}}

        self.assertEqual(
            {"native_status": {"type": "notLoaded"}}, advance(Client(), {"thread_id": "parent", "inspect": True}, Busy)
        )

    def test_cold_parent_checks_later_queue_pages_before_deciding_to_resume(self):
        for present in (True, False):
            client = Mock()
            client.request.side_effect = [
                {"thread": {"status": {"type": "notLoaded"}}},
                {"data": [{"id": str(i)} for i in range(100)], "nextCursor": "page2"},
                {"data": [{"id": "receipt"}] if present else [], "nextCursor": None}, {},
            ]
            with self.subTest(present=present):
                self.assertTrue(advance(client, {"thread_id": "parent", "receipt": "receipt"}, Busy)["ready"])
                calls = client.request.call_args_list
                self.assertEqual(("thread/queue/list", {"threadId": "parent", "limit": 100, "cursor": "page2"}), calls[2].args)
                self.assertEqual(present, any(c.args[0] == "thread/resume" for c in calls))

    def test_repeated_queue_cursor_defers_instead_of_claiming_ready(self):
        client = Mock()
        client.request.side_effect = [
            {"thread": {"status": {"type": "notLoaded"}}},
            {"data": [], "nextCursor": "same"}, {"data": [], "nextCursor": "same"},
        ]
        result = advance(client, {"thread_id": "parent", "receipt": "receipt"}, Busy)
        self.assertIn("repeated", result["deferred"])
        self.assertEqual(3, client.request.call_count)

    def test_cold_resume_restores_only_explicit_private_tool_configuration(self):
        calls = []

        class Client:
            def request(self, method, params):
                calls.append((method, params))
                return {
                    "thread/read": {"thread": {"status": {"type": "notLoaded"}}},
                    "thread/queue/list": {"data": [{"id": "receipt"}]},
                    "thread/resume": {},
                }[method]

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "resume.json"
            config = {"mcp_servers": {"bridge": {"args": ["--state", "test-state"]}}}
            path.write_text(json.dumps(config))
            path.chmod(0o600)
            advance(Client(), {"thread_id": "parent", "receipt": "receipt", "resume_config_file": str(path)}, Busy)
            self.assertEqual(
                ("thread/resume", {"threadId": "parent", "excludeTurns": True, "config": config}), calls[-1]
            )
            path.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "mode 600"):
                read_resume_config(path)
            path.chmod(0o600)
            for config in ({"model": "other"}, {"approval_policy": "never"}, {"features": {"shell_tool": True}}):
                path.write_text(json.dumps(config))
                with self.assertRaises(ValueError):
                    read_resume_config(path)
