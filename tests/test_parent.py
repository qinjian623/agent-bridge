import io
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from agent_bridge import telegram_guard
from agent_bridge.codex_parent import advance, main, read_resume_config


class Busy(Exception):
    pass


class ParentTests(unittest.TestCase):
    def setUp(self):
        self.job = {"thread_id": "parent", "receipt": "receipt", "event_id": "7"}
        self.item = {"id": "receipt", "clientUserMessageId": "cli-agent-bridge:7"}

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
                    "thread/queue/list": {"data": [item]},
                    "thread/resume": {},
                }[method]

        item = self.item
        result = advance(Client(), self.job, Busy)
        self.assertFalse(result["ready"])
        self.assertEqual("enqueued", result["native_status"])
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
                {"data": [self.item] if present else [], "nextCursor": None},
                {} if present else {"data": []},
            ]
            with self.subTest(present=present):
                result = advance(client, self.job, Busy)
                self.assertFalse(result["ready"])
                self.assertEqual("enqueued" if present else "unconfirmed", result["native_status"])
                calls = client.request.call_args_list
                self.assertEqual(("thread/queue/list", {"threadId": "parent", "limit": 100, "cursor": "page2"}), calls[2].args)
                self.assertEqual(present, any(c.args[0] == "thread/resume" for c in calls))

    def test_repeated_queue_cursor_defers_instead_of_claiming_ready(self):
        client = Mock()
        client.request.side_effect = [
            {"thread": {"status": {"type": "notLoaded"}}},
            {"data": [], "nextCursor": "same"}, {"data": [], "nextCursor": "same"},
        ]
        result = advance(client, self.job, Busy)
        self.assertIn("repeated", result["deferred"])
        self.assertEqual(3, client.request.call_count)

    def test_cold_resume_restores_only_explicit_private_tool_configuration(self):
        calls = []

        class Client:
            def request(self, method, params):
                calls.append((method, params))
                return {
                    "thread/read": {"thread": {"status": {"type": "notLoaded"}}},
                    "thread/queue/list": {"data": [item]},
                    "thread/resume": {},
                }[method]

        with tempfile.TemporaryDirectory() as folder:
            item = self.item
            path = Path(folder) / "resume.json"
            config = {"mcp_servers": {"bridge": {"args": ["--state", "test-state"]}}}
            path.write_text(json.dumps(config))
            path.chmod(0o600)
            advance(Client(), {**self.job, "resume_config_file": str(path)}, Busy)
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

    def test_active_and_idle_queue_entries_are_not_ready(self):
        for status in ("active", "idle"):
            client = Mock()
            client.request.side_effect = [
                {"thread": {"status": {"type": status}}}, {"data": [self.item]},
            ]
            with self.subTest(status=status):
                result = advance(client, self.job, Busy)
                self.assertEqual((False, "enqueued"), (result["ready"], result["native_status"]))
                self.assertEqual(["thread/read", "thread/queue/list"], [c.args[0] for c in client.request.call_args_list])
                client.enqueue.assert_not_called()

    def test_initial_enqueue_only_proves_queue_acceptance(self):
        client = Mock()
        client.enqueue.return_value = "receipt"
        job = {**self.job, "receipt": None, "message": "existing result"}
        result = advance(client, job, Busy)
        self.assertEqual({"receipt": "receipt", "ready": False, "native_status": "enqueued"}, result)
        client.enqueue.assert_called_once_with("parent", "existing result", "cli-agent-bridge:7")
        client.request.assert_not_called()

    def test_only_exact_native_user_message_proves_receipt(self):
        for kind, client_id, received in (
            ("userMessage", "cli-agent-bridge:7", True),
            ("userMessage", "cli-agent-bridge:70", False),
            ("agentMessage", "cli-agent-bridge:7", False),
        ):
            client = Mock()
            client.request.side_effect = [
                {"thread": {"status": {"type": "active"}}}, {"data": []},
                {"data": [{"id": "turn", "items": [{"type": kind, "clientId": client_id}]}]},
            ]
            with self.subTest(kind=kind, client_id=client_id):
                result = advance(client, self.job, Busy)
                self.assertEqual(received, result["ready"])
                self.assertEqual("received" if received else "unconfirmed", result["native_status"])
                self.assertEqual("turn" if received else None, result.get("native_turn_id"))
                client.enqueue.assert_not_called()

    def test_retract_matches_both_identifiers_across_pages_and_leaves_other_entries(self):
        client = Mock()
        client.request.side_effect = [
            {"data": [{"id": "other", "clientUserMessageId": "cli-agent-bridge:70"}], "nextCursor": "next"},
            {"data": [self.item]}, {"deleted": True},
        ]
        result = advance(client, {**self.job, "retract": True}, Busy)
        self.assertEqual({"queue_cleanup": "removed", "native_status": "removed", "receipt": "receipt"}, result)
        self.assertEqual([
            ("thread/queue/list", {"threadId": "parent", "limit": 100}),
            ("thread/queue/list", {"threadId": "parent", "limit": 100, "cursor": "next"}),
            ("thread/queue/delete", {"threadId": "parent", "queuedSubmissionId": "receipt"}),
        ], [c.args for c in client.request.call_args_list])
        client.enqueue.assert_not_called()

    def test_retract_recovers_missing_receipt_only_from_unique_exact_client_id(self):
        client = Mock()
        client.request.side_effect = [{"data": [self.item]}, {"deleted": True}]
        result = advance(client, {**self.job, "receipt": None, "retract": True}, Busy)
        self.assertEqual("removed", result["queue_cleanup"])
        self.assertEqual("receipt", result["receipt"])
        self.assertEqual("thread/queue/delete", client.request.call_args_list[-1].args[0])

    def test_retract_missing_receipt_and_duplicate_client_ids_do_not_select_any_entry(self):
        client = Mock()
        client.request.side_effect = [
            {"data": [self.item], "nextCursor": "next"},
            {"data": [{**self.item, "id": "duplicate"}]},
        ]
        result = advance(client, {**self.job, "receipt": None, "retract": True}, Busy)
        self.assertEqual("uncertain", result["queue_cleanup"])
        self.assertEqual(["thread/queue/list", "thread/queue/list"], [c.args[0] for c in client.request.call_args_list])

    def test_retract_never_deletes_ambiguous_or_conflicting_identifiers(self):
        cases = (
            {"data": [{**self.item, "id": "different"}]},
            {"data": [{**self.item, "clientUserMessageId": "other"}]},
            {"data": [self.item], "nextCursor": "next"},
        )
        for page in cases:
            client = Mock()
            client.request.side_effect = [page, {"data": [{**self.item, "id": "duplicate"}]}]
            with self.subTest(page=page):
                result = advance(client, {**self.job, "retract": True}, Busy)
                self.assertEqual(("uncertain", "unconfirmed"), (result["queue_cleanup"], result["native_status"]))
                self.assertTrue(all(c.args[0] == "thread/queue/list" for c in client.request.call_args_list))
                client.enqueue.assert_not_called()

    def test_retract_already_started_only_reports_original_turn(self):
        client = Mock()
        client.request.side_effect = [
            {"data": []},
            {"data": [{"id": "turn", "items": [{"type": "userMessage", "clientId": "cli-agent-bridge:7"}]}]},
        ]
        result = advance(client, {**self.job, "retract": True}, Busy)
        self.assertEqual(("already_started", "received", "turn"),
                         (result["queue_cleanup"], result["native_status"], result["native_turn_id"]))
        self.assertEqual(["thread/queue/list", "thread/turns/list"], [c.args[0] for c in client.request.call_args_list])

    def test_retract_delete_race_or_lost_reply_checks_read_only_and_never_resends(self):
        for delete_result in ({"deleted": False}, TimeoutError("reply lost")):
            for started in (True, False):
                client = Mock()
                turns = [{"id": "turn", "items": [{"type": "userMessage", "clientId": "cli-agent-bridge:7"}]}] if started else []
                client.request.side_effect = [{"data": [self.item]}, delete_result, {"data": []}, {"data": turns}]
                with self.subTest(delete_result=delete_result, started=started):
                    result = advance(client, {**self.job, "retract": True}, Busy)
                    self.assertEqual("already_started" if started else "uncertain", result["queue_cleanup"])
                    self.assertEqual("received" if started else "unconfirmed", result["native_status"])
                    self.assertEqual(["thread/queue/list", "thread/queue/delete", "thread/queue/list", "thread/turns/list"],
                                     [c.args[0] for c in client.request.call_args_list])
                    client.enqueue.assert_not_called()

    def test_retract_absence_or_unavailable_queue_is_uncertain_not_removed(self):
        for pages, cleanup in (([{"data": []}, {"data": []}], "uncertain"),
                               ([ConnectionError("disconnected")], "pending")):
            client = Mock()
            client.request.side_effect = pages
            with self.subTest(pages=pages):
                result = advance(client, {**self.job, "retract": True}, Busy)
                self.assertEqual((cleanup, "unconfirmed"), (result["queue_cleanup"], result["native_status"]))
                self.assertTrue(all(c.args[0] in {"thread/queue/list", "thread/turns/list"} for c in client.request.call_args_list))
                client.enqueue.assert_not_called()

    def test_retract_failed_delete_with_entry_still_queued_never_deletes_twice(self):
        for result in ({"deleted": False}, TimeoutError("reply lost")):
            client = Mock()
            client.request.side_effect = [{"data": [self.item]}, result, {"data": [self.item]}]
            with self.subTest(result=result):
                result = advance(client, {**self.job, "retract": True}, Busy)
                self.assertEqual(("uncertain", "unconfirmed", "receipt"),
                                 (result["queue_cleanup"], result["native_status"], result["receipt"]))
                self.assertEqual(["thread/queue/list", "thread/queue/delete", "thread/queue/list"],
                                 [c.args[0] for c in client.request.call_args_list])

    def test_cleanup_check_only_never_repeats_delete_even_when_entry_is_present(self):
        client = Mock()
        client.request.return_value = {"data": [self.item]}
        result = advance(client, {**self.job, "retract": True, "cleanup_check_only": True}, Busy)
        self.assertEqual(("uncertain", "enqueued", "receipt"),
                         (result["queue_cleanup"], result["native_status"], result["receipt"]))
        self.assertEqual(["thread/queue/list"], [c.args[0] for c in client.request.call_args_list])
        client.enqueue.assert_not_called()

    def test_cleanup_check_only_can_confirm_native_turn_without_mutation(self):
        client = Mock()
        client.request.side_effect = [
            {"data": []},
            {"data": [{"id": "turn", "items": [{"type": "userMessage", "clientId": "cli-agent-bridge:7"}]}]},
        ]
        result = advance(client, {**self.job, "retract": True, "cleanup_check_only": True}, Busy)
        self.assertEqual(("already_started", "received", "turn"),
                         (result["queue_cleanup"], result["native_status"], result["native_turn_id"]))
        self.assertEqual(["thread/queue/list", "thread/turns/list"], [c.args[0] for c in client.request.call_args_list])

    def test_cleanup_read_failure_preserves_previous_uncertainty(self):
        client = Mock()
        client.request.side_effect = ConnectionError("disconnected")
        result = advance(client, {**self.job, "retract": True, "cleanup_check_only": True}, Busy)
        self.assertEqual(("uncertain", "unconfirmed"), (result["queue_cleanup"], result["native_status"]))
        self.assertEqual(["thread/queue/list"], [c.args[0] for c in client.request.call_args_list])

    def test_cleanup_setup_failure_is_retryable_only_without_previous_delete_attempt(self):
        for check_only in (False, True):
            constructor = Mock(side_effect=ConnectionError("daemon unavailable"))
            app_server = types.ModuleType("codex_async.app_server")
            app_server.CodexAppServer, app_server.ThreadBusyError = constructor, Busy
            modules = {"codex_async": types.ModuleType("codex_async"), "codex_async.app_server": app_server,
                       "telegram_guard": telegram_guard}
            job = {**self.job, "cwd": "/tmp", "retract": True, "cleanup_check_only": check_only}
            output = io.StringIO()
            with self.subTest(check_only=check_only), patch.dict(sys.modules, modules), \
                    patch("sys.stdin", io.StringIO(json.dumps(job))), patch("sys.stdout", output):
                main()
            result = json.loads(output.getvalue())
            self.assertEqual("uncertain" if check_only else "pending", result["queue_cleanup"])
            self.assertIn("setup failed", result["deferred"])
            self.assertFalse(constructor.call_args.kwargs["start_daemon"])

    def test_retract_main_ignores_paused_telegram_and_never_starts_daemon(self):
        client = Mock()
        client.request.side_effect = [{"data": [self.item]}, {"deleted": True}]
        constructor = MagicMock()
        constructor.return_value.__enter__.return_value = client
        app_server = types.ModuleType("codex_async.app_server")
        app_server.CodexAppServer, app_server.ThreadBusyError = constructor, Busy
        job = {**self.job, "cwd": "/tmp", "retract": True}
        output = io.StringIO()
        modules = {"codex_async": types.ModuleType("codex_async"), "codex_async.app_server": app_server,
                   "telegram_guard": telegram_guard}
        with patch.dict(sys.modules, modules), patch("sys.stdin", io.StringIO(json.dumps(job))), \
                patch("sys.stdout", output), patch.object(telegram_guard, "delivery_status", return_value="paused") as external:
            main()
        external.assert_not_called()
        self.assertEqual("removed", json.loads(output.getvalue())["queue_cleanup"])
        self.assertFalse(constructor.call_args.kwargs["start_daemon"])
        self.assertIsNone(constructor.call_args.kwargs["sandbox"])
        self.assertTrue(all(c.args[0] in {"thread/queue/list", "thread/queue/delete"} for c in client.request.call_args_list))
