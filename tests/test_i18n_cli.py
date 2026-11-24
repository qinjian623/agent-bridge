"""Offline checks for language selection and CLI/initializer presentation."""
from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agent_bridge import cli, init_config, updater
from agent_bridge.i18n import resolve_language, use_language
from agent_bridge.monitor import _display_width


class CliLanguageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name) / "state"
        self.state.mkdir(mode=0o700)
        self.env = patch.dict(os.environ, {"LANG": "zh_CN.UTF-8"}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def run_cli(self, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        code = 0
        with redirect_stdout(stdout), redirect_stderr(stderr):
            try:
                cli.main(["--state", str(self.state), *args])
            except SystemExit as error:
                code = error.code
        return code, stdout.getvalue(), stderr.getvalue()

    def test_english_default_and_help_in_both_flag_positions(self):
        code, output, _ = self.run_cli("--help")
        self.assertEqual(code, 0)
        self.assertIn("Persistent local", output)
        self.assertIn("options:", output)
        for arguments in (("--lang", "zh-CN", "--help"), ("doctor", "--language=zh-CN", "--help")):
            with self.subTest(arguments=arguments):
                code, output, _ = self.run_cli(*arguments)
                self.assertEqual(code, 0)
                self.assertIn("用法：", output)
                self.assertIn("选项:", output)
                self.assertIn("显示帮助并退出", output)
                self.assertIn("--lang", output)
        self.assertEqual(resolve_language(), "en")
        self.assertIn("Persistent local", self.run_cli("--help")[1])

    def test_config_environment_flag_and_auto_precedence(self):
        (self.state / "config.json").write_text(json.dumps({"profiles": {}, "language": "zh-CN"}))
        self.assertIn("用法：", self.run_cli("--help")[1])
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "en"}):
            self.assertIn("usage:", self.run_cli("--help")[1])
            self.assertIn("用法：", self.run_cli("--lang", "zh-CN", "--help")[1])
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "auto", "LC_ALL": "en_US.UTF-8", "LC_MESSAGES": "zh_CN.UTF-8"}):
            self.assertIn("usage:", self.run_cli("--help")[1])
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "auto", "LC_MESSAGES": "zh_CN.UTF-8"}):
            self.assertIn("用法：", self.run_cli("--help")[1])
        self.assertIn("用法：", self.run_cli("--lang", "auto", "--help")[1])
        self.assertIn("usage:", self.run_cli("--lang", "en", "--help")[1])

    def test_unknown_or_empty_environment_uses_english_and_keeps_flags_strict(self):
        (self.state / "config.json").write_text(json.dumps({"profiles": {}, "language": "zh-CN"}))
        for language in ("de_DE.UTF-8", "", "   "):
            with self.subTest(language=language), patch.dict(os.environ, {"AGENT_BRIDGE_LANG": language}):
                code, output, _ = self.run_cli("--help")
                self.assertEqual(code, 0)
                self.assertIn("usage:", output)
                self.assertIn("用法：", self.run_cli("--lang", "zh-CN", "--help")[1])
                for invalid in ("de_DE.UTF-8", ""):
                    self.assertEqual(self.run_cli("--lang", invalid, "--help")[0], 2)
                seen = []
                with patch("agent_bridge.broker.Broker") as broker:
                    broker.return_value.serve = AsyncMock(side_effect=lambda seen=seen: seen.append(resolve_language()))
                    self.assertEqual(self.run_cli("serve")[0], 0)
                    broker.return_value.serve.assert_awaited_once_with()
                self.assertEqual(seen, ["en"])

    def test_language_flags_on_descendants_and_existing_abbreviations(self):
        for flag in ("--lang", "--language"):
            code, output, _ = self.run_cli("group", "prune", "--older-than", "7d", flag, "zh-CN", "--help")
            self.assertEqual(code, 0)
            self.assertIn("用法：", output)
            self.assertIn("正时长", output)
        result = {"status": "ok", "name": "Name"}
        with patch("agent_bridge.broker.request", AsyncMock(return_value=result)) as request:
            code, output, _ = self.run_cli("group", "prune", "--older-than", "7d", "--language", "zh-CN")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output), result)
        self.assertEqual(request.await_args.args[2]["older_than"], "7d")
        for arguments in (("--langu", "zh-CN", "--help"), ("monitor", "--langu", "zh-CN", "--help"),
                          ("group", "prune", "--older-than", "7d", "--langu", "zh-CN", "--help")):
            with self.subTest(arguments=arguments):
                code, output, _ = self.run_cli(*arguments)
                self.assertEqual(code, 0)
                self.assertIn("用法：", output)
        with patch("agent_bridge.monitor.run_tui_monitor") as monitor:
            code, _, _ = self.run_cli("monitor", "--interv", "3")
        self.assertEqual(code, 0)
        self.assertEqual(monitor.call_args.kwargs["interval"], 3.0)

    def test_help_survives_invalid_or_unreadable_configuration(self):
        path = self.state / "config.json"
        for contents in ("{invalid json", '{"profiles":{},"language":"invalid"}', '{"profiles":{},"language":[]}'):
            with self.subTest(contents=contents):
                path.write_text(contents)
                code, output, _ = self.run_cli("--help")
                self.assertEqual(code, 0)
                self.assertIn("usage:", output)
        with patch.object(Path, "read_text", side_effect=PermissionError("test config is unreadable")):
            code, output, _ = self.run_cli("--help")
        self.assertEqual(code, 0)
        self.assertIn("usage:", output)

    def test_chinese_help_wraps_by_display_columns(self):
        for width in (80, 100):
            for command in ((), ("monitor",), ("init",), ("integrate",), ("update",)):
                with self.subTest(width=width, command=command), patch.dict(os.environ, {"COLUMNS": str(width)}), patch.object(sys, "argv", ["agent-bridge"]):
                    code, output, _ = self.run_cli("--lang", "zh-CN", *command, "--help")
                    self.assertEqual(code, 0)
                    usage = output.split("\n\n", 1)[0].splitlines()
                    for line in output.splitlines():
                        if _display_width(line) > width:
                            # argparse treats the command alternatives in usage as one token.
                            self.assertTrue(command)
                            self.assertIn(line, usage)
                            self.assertNotRegex(line, r"[\u4e00-\u9fff]")

    def test_errors_translate_templates_and_keep_argument_values(self):
        for arguments, expected, raw in (
            (("serve", "--extra-Name"), "无法识别的参数", "--extra-Name"),
            (("unknown-command",), "无效选项", "unknown-command"),
            (("monitor", "--port", "Name"), "无效的 int 值", "Name"),
            (("monitor", "--port"), "需要一个参数值", "--port"),
            (("name",), "缺少必填参数", "session_id"),
            (("--state",), "需要一个参数值", "--state"),
        ):
            with self.subTest(arguments=arguments):
                code, _, error = self.run_cli("--lang", "zh-CN", *arguments)
                self.assertEqual(code, 2)
                self.assertIn(expected, error)
                self.assertIn(raw, error)
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "zh-CN"}):
            code, _, error = self.run_cli("--lang", "invalid", "--help")
            self.assertEqual(code, 2)
            self.assertIn("语言必须为", error)
        self.assertEqual(resolve_language(), "en")

    def test_updater_help_reuses_language_without_changing_json_results(self):
        code, output, _ = self.run_cli("update", "--lang", "zh-CN", "--help")
        self.assertEqual(code, 0)
        self.assertIn("安装已检查的发布 wheel", output)
        self.assertIn("等待所有正在运行的 worker", output)
        self.assertIn("--drain-workers", output)
        output = io.StringIO()
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "zh-CN"}), redirect_stdout(output), self.assertRaises(SystemExit) as exited:
            updater.main(["--help"])
        self.assertEqual(exited.exception.code, 0)
        self.assertIn("用法：", output.getvalue())
        result = {"status": "current", "name": "Name", "message": "Running"}
        with patch.object(updater, "Updater") as updater_class:
            updater_class.return_value.check.return_value = result
            code, output, _ = self.run_cli("update", "check", "--lang", "zh-CN")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output), result)

    def test_call_json_user_text_and_unknown_errors_are_unchanged(self):
        params = {"instruction": "Name Status Running", "language": "zh-CN"}
        result = {"status": "running", "name": "Name", "reply": "No events yet", "nested": params}
        request = AsyncMock(return_value=result)
        with patch("agent_bridge.broker.request", request):
            code, output, _ = self.run_cli("call", "agent_test", "--lang", "zh-CN", "--json", json.dumps(params))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output), result)
        request.assert_awaited_once_with(self.state, "agent_test", params)
        with patch("agent_bridge.monitor.set_session_name"):
            code, output, _ = self.run_cli("--lang", "zh-CN", "name", "sid-Name", "Name Status Running")
        self.assertEqual(code, 0)
        self.assertIn("会话 sid-Name 已命名为：Name Status Running", output)
        with patch("agent_bridge.broker.request", AsyncMock(side_effect=RuntimeError("External Name Status failure"))):
            code, _, error = self.run_cli("--lang", "zh-CN", "call", "agent_test", "--json", "{}")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(error), {"error": "External Name Status failure"})

    def test_common_init_error_and_doctor_warnings_are_localized(self):
        code, _, error = self.run_cli("init", "--lang", "zh-CN", "--non-interactive")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(error)["error"]["code"], "init_requires_manual_config")
        self.assertIn("首次配置需要手动指定", error)
        (self.state / "config.json").write_text(json.dumps({"profiles": {}, "worker_launcher": "systemd"}))
        env = {"platform": "linux", "systemd_user": False, "tools": {"uv": None, "ssh": None}}
        with patch.object(cli, "detect_environment", return_value=env):
            code, output, _ = self.run_cli("doctor", "--lang", "zh-CN")
        self.assertEqual(code, 0)
        result = json.loads(output)
        self.assertEqual(result["launcher"], "systemd")
        self.assertIn("worker_launcher 为 'systemd'", result["warnings"][0])
        self.assertIn("未找到 'uv'", result["warnings"][1])

    def test_language_context_restores_after_success_and_error(self):
        with use_language("zh-CN"):
            self.assertIn("usage:", self.run_cli("--lang", "en", "--help")[1])
            self.assertEqual(resolve_language(), "zh-CN")
            self.assertEqual(self.run_cli("--lang", "en", "bad-command")[0], 2)
            self.assertEqual(resolve_language(), "zh-CN")
        self.assertEqual(resolve_language(), "en")


class InitLanguageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.environment = {"platform": "linux", "systemd_user": False, "launchd_user": False,
                            "runtimes": {"codex": None}, "tools": {"uv": None, "ssh": None}}

    def test_wizard_translates_menus_and_keeps_user_values_and_choice_keys(self):
        raw = {"profiles": {"Name": {"runtime": "codex", "model": "Status"}},
               "projects": {"Running": str(self.state)}, "hosts": {}, "language": "zh-CN"}
        path = self.state / "config.json"
        path.write_text(json.dumps(raw))
        original = path.read_bytes()
        prompts, output = [], []
        replies = iter(("1", "b", "invalid", "q"))

        def answer(prompt):
            prompts.append(prompt)
            return next(replies)

        with patch.object(init_config, "detect_environment", return_value=self.environment), use_language("zh-CN"):
            result = init_config.run_init(self.state, cli.build_parser().parse_args(["init"]),
                                          input_fn=answer, output_fn=output.append, is_tty=True)
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(path.read_bytes(), original)
        self.assertIn("运行配置", "\n".join(output))
        self.assertIn("Name", "\n".join(output))
        self.assertIn("model=Status", "\n".join(output))
        self.assertIn("Running", "\n".join(output))
        self.assertIn("选择 [1-5]、[s]", prompts[0])
        self.assertIn("[a] 添加/编辑", prompts[1])
        self.assertIn("  选择无效。", output)

    def test_prompts_localize_validation_and_preserve_defaults(self):
        prompts, output = [], []
        replies = iter(("", "Name", "maybe", "y"))

        def answer(prompt):
            prompts.append(prompt)
            return next(replies)

        with use_language("zh-CN"):
            value = init_config._prompt(answer, output.append, "Profile name", required=True)
            confirmed = init_config._prompt_yes_no(answer, output.append, "No profiles configured yet. Save anyway?")
        self.assertEqual(value, "Name")
        self.assertTrue(confirmed)
        self.assertIn("运行配置名称", prompts[0])
        self.assertEqual(output, ["  此项必填。", "  请输入 y 或 n。"])

    def test_reconfiguration_preserves_language_and_private_config(self):
        raw = {"profiles": {"Name": {"runtime": "codex", "model": "Status"}}, "language": "zh-CN", "worker_launcher": "detached"}
        path = self.state / "config.json"
        path.write_text(json.dumps(raw))
        args = cli.build_parser().parse_args(["init", "--non-interactive", "--parallel-limit", "4"])
        with patch.object(init_config, "detect_environment", return_value=self.environment), use_language("en"):
            result = init_config.run_init(self.state, args)
        self.assertTrue(result["config_changed"])
        saved = json.loads(path.read_text())
        self.assertEqual(saved["language"], "zh-CN")
        self.assertEqual(saved["profiles"]["Name"]["model"], "Status")
        self.assertEqual(saved["parallel_limit"], 4)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
