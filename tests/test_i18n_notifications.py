import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_bridge.config import Config, Profile
from agent_bridge.i18n import use_language
from agent_bridge.notifications import deliver
from agent_bridge.peer_protocol import DeliverySource
from agent_bridge.store import Store


class NotificationLanguageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.store = Store(Path(self.temp.name))
        self.profile = Profile("fixture", "codex", "fixed")
        self.config = Config({"fixture": self.profile}, codex_async_python="fixture-helper")
        self.store.parent_control("parent", "parent_register", 0, "register", "native-parent", self.temp.name)
        self.sid = self.store.open(self.profile, self.temp.name, "read-only", "open", "parent")["session_id"]
        self.store.worker_ready(self.sid, "g", 1, "start", "native-child")
        self.task = self.store.dispatch(self.sid, "Original task / 原始任务", "dispatch")["task_id"]
        self.store.claim(self.config)
        attempt = self.store.begin_attempt(self.sid, "g")["active_attempt"]
        self.answer = "Child task / 原始模型回答"
        self.store.finish(self.sid, "g", self.task, attempt, "completed", {"answer": self.answer})
        self.messages = {}

    async def sender(self, config, parent, message, seq, receipt=None):
        self.messages[seq] = message
        return {"receipt": "fixture-receipt", "native_status": "received", "native_turn_id": "fixture-turn"}

    async def test_chinese_notice_retains_ids_commands_and_stop_rules(self):
        self.config.language = "zh-CN"
        await deliver(self.store, self.config, self.sender)
        self.assertEqual(1, len(self.messages))
        seq, message = next(iter(self.messages.items()))
        self.assertIn(f"[cli-agent-bridge event {seq} at ", message)
        self.assertIn(f"子任务 {self.task} 已完成", message)
        for text in ("agent_result", "agent_review", "parent_ack", "暂停或停止", "不授权恢复开发或重试工作"):
            self.assertIn(text, message)
        with self.store.connection() as db:
            row = db.execute("SELECT status,result,instruction FROM tasks WHERE id=?", (self.task,)).fetchone()
            self.assertEqual("completed", row["status"])
            self.assertEqual(self.answer, json.loads(row["result"])["answer"])
            self.assertEqual("Original task / 原始任务", row["instruction"])

    async def test_english_default_and_explicit_context_wins_over_config(self):
        self.config.language = "zh-CN"
        with use_language("en"):
            await deliver(self.store, self.config, self.sender)
        message = next(iter(self.messages.values()))
        self.assertIn(f"Child task {self.task} has completed.", message)
        self.assertIn("does not authorize resuming development or retrying work", message)

    async def test_environment_wins_over_config_without_changing_original_message(self):
        self.config.language = "en"
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "zh-CN"}):
            await deliver(self.store, self.config, self.sender)
        self.assertIn("已完成", next(iter(self.messages.values())))

    async def test_empty_environment_selects_english_for_direct_notifications(self):
        self.config.language = "zh-CN"
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": ""}):
            await deliver(self.store, self.config, self.sender)
        self.assertIn(f"Child task {self.task} has completed.", next(iter(self.messages.values())))

    async def test_locale_change_does_not_rewrite_or_duplicate_a_pending_notice(self):
        sent = []

        async def deferred(config, parent, message, seq, receipt=None):
            sent.append(message)
            return {"deferred": "fixture offline"}

        self.config.language = "en"
        await deliver(self.store, self.config, deferred)
        seq = self.store.get_parent("parent")["deliveries"][0]["event_seq"]
        before = self.store.get_native_send_by_delivery(DeliverySource.PARENT_NOTIFICATION, str(seq))
        self.config.language = "zh-CN"
        await deliver(self.store, self.config, self.sender)
        after = self.store.get_native_send_by_delivery(DeliverySource.PARENT_NOTIFICATION, str(seq))
        self.assertEqual(before["id"], after["id"])
        self.assertEqual(sent[0], self.messages[seq])
        self.assertEqual(before["message"], after["message"])
        with self.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM native_send_queue").fetchone()[0])

    async def test_input_request_in_chinese_is_not_a_completion(self):
        task = self.store.dispatch(self.sid, "need input", "input-task")["task_id"]
        attempt = self.store.claim(self.config)["attempt_id"]
        self.store.begin_attempt(self.sid, "g")
        question = self.store.question(self.sid, task, attempt, {"method": "permission"}, expires=time.time() + 60)
        seq = next(event["seq"] for event in self.store.events(task_id=task) if event["kind"] == "input_required")
        self.config.language = "zh-CN"
        await deliver(self.store, self.config, self.sender)
        message = self.messages[seq]
        self.assertIn("等待输入", message)
        self.assertIn(f"question_id={question}", message)
        self.assertIn(f"agent_inspect(task_id={task})", message)
        self.assertIn("不要审查结果", message)
        self.assertNotIn("agent_review", message)

    async def test_recovery_notice_does_not_authorize_replaying_work(self):
        task = self.store.dispatch(self.sid, "needs recovery", "recovery-task")["task_id"]
        attempt = self.store.claim(self.config)["attempt_id"]
        self.store.begin_attempt(self.sid, "g")
        self.store.finish(self.sid, "g", task, attempt, "recovery_needed", {"answer": "uncertain"})
        seq = self.store.events(task_id=task)[-1]["seq"]
        self.config.language = "zh-CN"
        await deliver(self.store, self.config, self.sender)
        self.assertIn("不要重放结果不明的工作", self.messages[seq])
        self.assertIn("recovery_needed", self.messages[seq])
