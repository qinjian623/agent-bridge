"""Terminal translation changes labels without touching snapshots or user text."""
from __future__ import annotations

import copy
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from agent_bridge import monitor
from agent_bridge.i18n import use_language


def snapshot():
    return {
        "timestamp": 1_700_000_000,
        "coordinator": {"host_id": "主机-Name", "node_id": "node-12345678", "db_size": 1024},
        "metrics": {"total_sessions": 0, "total_cost_usd": 0, "cost_complete": True},
        "hosts": [], "sessions": [], "native_sessions": [], "broadcasts": [], "groups": [], "events": [],
    }


def plain(text):
    return monitor._ANSI_SGR_RE.sub("", text)


class MonitorLanguageTests(unittest.TestCase):
    def test_full_metrics_use_complete_translated_labels(self):
        data = snapshot()
        with use_language("zh-CN"):
            chinese = plain(monitor.render_tui(data, width=140))
        self.assertIn("活跃代理:", chinese)
        self.assertIn("运行中任务:", chinese)
        self.assertNotIn("Active ", chinese)
        self.assertNotIn("Running ", chinese)
        with use_language("en"):
            english = plain(monitor.render_tui(data, width=140))
        self.assertIn("Active Agents:", english)
        self.assertIn("Running Tasks:", english)

    def test_titles_headers_empty_states_and_interleaved_languages(self):
        data = snapshot()
        for language in ("en", "zh-CN", "en", "zh-CN"):
            with self.subTest(language=language), use_language(language):
                text = plain(monitor.render_tui(data, width=140))
                if language == "en":
                    for label in ("cli-agent-bridge Monitor", "Cluster Nodes", "Bridge Sessions", "No events yet", "No sessions created yet"):
                        self.assertIn(label, text)
                else:
                    for label in ("cli-agent-bridge 监视器", "集群节点", "主机 ID", "代理 / 会话名称", "原生 CLI 进程与工作进程", "没有广播记录", "暂无事件", "尚未创建会话"):
                        self.assertIn(label, text)
                    self.assertNotIn("No sessions created yet", text)

    def test_chinese_columns_fit_terminal_display_width(self):
        data = snapshot()
        data["sessions"] = [{"id": "sid", "name": "中文e\u0301名字", "observed": "waiting_input"}]
        data["native_sessions"] = [{"name": "原生会话", "mode": "interactive", "runtime": "codex", "model": "Model"}]
        with use_language("zh-CN"):
            for width in (80, 95, 100, 140):
                with self.subTest(width=width):
                    text = plain(monitor.render_tui(data, width=width))
                    lines = text.splitlines()
                    self.assertEqual(monitor._display_width(lines[0]), width)
                    for line in lines:
                        self.assertLessEqual(monitor._display_width(line), width)
                    self.assertIn("等待输入", text)
                    self.assertIn("交互式", text)
                    header = next(line for line in lines if "主机 ID" in line)
                    separator = lines[lines.index(header) + 1]
                    self.assertEqual(monitor._display_width(header[:header.index("角色")]), separator.index(" ", 2) + 1)

    def test_abnormal_host_states_are_translated_without_changing_snapshot(self):
        data = snapshot()
        data["hosts"] = [{"host_id": "fixture", "state": state} for state in ("identity_changed", "state_corrupted")]
        original = copy.deepcopy(data)
        for width in (80, 100, 140):
            with self.subTest(width=width), use_language("zh-CN"):
                text = plain(monitor.render_tui(data, width=width))
                self.assertIn("身份变更", text)
                self.assertIn("状态损坏", text)
                self.assertNotIn("identity_changed", text)
                self.assertNotIn("state_corrupted", text)
                self.assertTrue(all(monitor._display_width(line) <= width for line in text.splitlines()))
        with use_language("en"):
            text = plain(monitor.render_tui(data, width=180))
            self.assertIn("identity_cha", text)
            self.assertIn("state_corrup", text)
        self.assertEqual(original, data)

    def test_raw_user_text_model_event_and_json_are_preserved(self):
        data = snapshot()
        data["sessions"] = [{"id": "session-Name", "name": "Name", "model": "Status", "observed": "running"}]
        data["native_sessions"] = [{"name": "Status", "model": "Running", "mode": "headless", "cwd": "/tmp/Name"}]
        data["broadcasts"] = [{"broadcast_id": "Name", "group_id": "Status", "received": 1, "total": 2, "message_preview": "Name Status Running"}]
        data["events"] = [{"seq": 1, "kind": "custom.Name", "session_name": "Status", "summary": "Name Status Running"}]
        data["pending_questions"] = [{"id": "question-Name", "session_id": "session-Name", "command": "Name Status Running"}]
        original = copy.deepcopy(data)
        with use_language("zh-CN"):
            text = plain(monitor.render_tui(data, width=160))
        self.assertIn("Name Status Running", text)
        self.assertIn("custom.Name", text)
        self.assertIn("Status", text)
        self.assertIn("Running", text)
        self.assertIn("运行中", text)
        self.assertIn("无界面", text)
        self.assertEqual(data, original)
        for language in ("en", "zh-CN"):
            output = io.StringIO()
            with patch.object(monitor, "MonitorCollector") as collector, use_language(language), redirect_stdout(output):
                collector.return_value.collect.return_value = data
                monitor.run_tui_monitor("/unused", once=True, output_format="json")
            self.assertEqual(json.loads(output.getvalue()), original)

    def test_refresh_exit_and_unknown_cost_labels(self):
        data = snapshot()
        data["metrics"] = {"total_sessions": 1, "cost_complete": False, "total_cost_usd": None}
        output = io.StringIO()
        with patch.object(monitor, "MonitorCollector") as collector, patch.object(monitor.time, "sleep", side_effect=KeyboardInterrupt), use_language("zh-CN"), redirect_stdout(output):
            collector.return_value.collect.return_value = data
            monitor.run_tui_monitor("/unused", interval=3)
        text = plain(output.getvalue())
        self.assertIn("未知", text)
        self.assertIn("按 Ctrl+C 退出监视器 | 每 3 秒刷新", text)
        self.assertIn("监视器已停止。", text)
