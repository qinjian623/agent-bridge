import json
import unittest
from unittest.mock import Mock

from scripts.check_native_notification_retract import QueueClient, describe_error


class NativeNotificationSafetyTests(unittest.TestCase):
    def setUp(self):
        self.native = Mock(sequence=0)
        self.report = {"rpc": []}
        self.client = QueueClient(self.native, self.report)
        self.client.thread_id = "owned-test-thread"

    def test_model_resume_and_daemon_methods_are_rejected_before_rpc(self):
        for method in ("turn/start", "thread/queue/start", "thread/resume", "turn/steer", "turn/interrupt", "daemon/start"):
            with self.subTest(method=method), self.assertRaisesRegex(AssertionError, "Forbidden"):
                self.client.request(method, {"threadId": "owned-test-thread"})
        self.native.request.assert_not_called()

    def test_other_threads_and_unowned_receipts_are_rejected(self):
        with self.assertRaisesRegex(AssertionError, "Wrong Native thread"):
            self.client.request("thread/queue/list", {"threadId": "unrelated"})
        with self.assertRaisesRegex(AssertionError, "unowned queue entry"):
            self.client.request("thread/queue/delete", {"threadId": "owned-test-thread", "queuedSubmissionId": "unowned"})
        self.native.request.assert_not_called()

    def test_thread_start_never_accepts_model_or_permissions_overrides(self):
        self.client.thread_id = None
        for key in ("model", "modelProvider", "sandbox", "approvalPolicy", "config"):
            with self.subTest(key=key), self.assertRaisesRegex(AssertionError, "unmodified"):
                self.client.request("thread/start", {"cwd": "/unused", key: "override"})
        self.native.request.assert_not_called()

    def test_a_second_thread_cannot_be_created(self):
        with self.assertRaisesRegex(AssertionError, "Only one"):
            self.client.request("thread/start", {"cwd": "/unused"})
        self.native.request.assert_not_called()

    def test_rpc_report_keeps_identifiers_without_input_text(self):
        self.native.request.return_value = {"queuedSubmission": {"id": "owned-receipt"}}
        self.client.enqueue("owned-test-thread", "input-must-not-be-recorded", "test-client-id")
        self.assertNotIn("input-must-not-be-recorded", json.dumps(self.report))
        self.assertEqual("owned-receipt", self.report["rpc"][0]["receipt"])
        self.assertEqual("test-client-id", self.report["rpc"][0]["clientUserMessageId"])

    def test_native_rejection_is_short_and_does_not_keep_paths_or_credentials(self):
        error = RuntimeError("App Server rejected thread/read: " + repr({
            "code": -32601, "message": "list_turns unsupported at /home/test/private with Bearer secret-value",
        }))
        result = describe_error(error)
        self.assertEqual(-32601, result["native_error_code"])
        self.assertNotIn("/home/test/private", result["error_message"])
        self.assertNotIn("secret-value", result["error_message"])

    def test_unexpected_history_errors_are_preserved_instead_of_treated_as_empty(self):
        error = RuntimeError("App Server rejected thread/turns/list: {'code': -32001, 'message': 'unavailable'}")
        self.native.request.side_effect = error
        with self.assertRaises(RuntimeError) as caught:
            self.client.empty_turns()
        self.assertIs(error, caught.exception)
        self.native.request.assert_called_once()
