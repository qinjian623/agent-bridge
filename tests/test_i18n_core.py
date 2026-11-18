import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_bridge.config import Config
from agent_bridge.errors import BridgeError
from agent_bridge.i18n import translate, use_language
from agent_bridge.i18n_core import MESSAGES
from agent_bridge.store import task_input, validate_session_name


class ConfigLanguageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "config.json"

    def load_config(self, **values):
        content = json.dumps({"profiles": {}, **values})
        self.path.write_text(content, encoding="utf-8")
        try:
            return Config.load(self.path)
        finally:
            self.assertEqual(content, self.path.read_text(encoding="utf-8"))

    def test_old_and_null_config_keep_none_without_adopting_process_language(self):
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "zh-CN"}):
            self.assertIsNone(Config(profiles={}).language)
            self.assertIsNone(self.load_config().language)
            self.assertIsNone(self.load_config(language=None).language)

    def test_explicit_languages_and_aliases_are_normalized(self):
        for value, expected in (
            ("en", "en"),
            ("zh-CN", "zh-CN"),
            ("auto", "auto"),
            ("zh_CN", "zh-CN"),
            ("zh_CN.UTF-8", "zh-CN"),
            ("en_US.UTF-8", "en"),
        ):
            with self.subTest(value=value):
                self.assertEqual(expected, self.load_config(language=value).language)

    def test_invalid_language_values_are_redacted_config_errors(self):
        for value in ("", "fr", "synthetic-private-language", 1, True, [], {}):
            with self.subTest(value=value):
                with self.assertRaises(BridgeError) as caught:
                    self.load_config(language=value)
                self.assertEqual("invalid_config", caught.exception.code)
                self.assertEqual("language must be en, zh-CN or auto", str(caught.exception))


class CoreMessagesTests(unittest.TestCase):
    def test_common_error_categories_have_chinese_presentation(self):
        examples = {
            "language must be en, zh-CN or auto": "语言必须为 en、zh-CN 或 auto",
            "Could not read config.json": "无法读取 config.json",
            "Admin token permissions must be 0600": "管理员令牌文件的权限必须为 0600",
            "Working directory must exist": "工作目录必须存在",
            "This request is no longer pending": "此请求已不再等待回答",
            "reservation must be a nonnegative token count": "reservation 必须为非负整数，表示预留的 token 数量",
            "Maintenance token does not match": "维护令牌不匹配",
            "Wheel SHA256 mismatch or ambiguous checksum": "wheel 的 SHA256 不匹配或校验值不明确",
        }
        with use_language("zh-CN"):
            for message, expected in examples.items():
                with self.subTest(message=message):
                    self.assertEqual(expected, translate(message))

    def test_real_validation_errors_stay_english_until_presentation(self):
        with use_language("zh-CN"):
            with self.assertRaises(BridgeError) as caught:
                task_input("synthetic task", -1)
            error = caught.exception
            self.assertEqual("invalid_budget", error.code)
            self.assertEqual("reservation must be a nonnegative token count", str(error))
            self.assertEqual(
                {"code": "invalid_budget", "message": "reservation must be a nonnegative token count"},
                error.as_dict(),
            )
            self.assertEqual("reservation 必须为非负整数，表示预留的 token 数量", translate(str(error)))
            with self.assertRaises(BridgeError) as caught:
                validate_session_name(17)
            self.assertEqual("name 必须为字符串", translate(str(caught.exception)))

    def test_explicit_english_and_unknown_messages_fall_back_unchanged(self):
        with use_language("zh-CN"):
            self.assertEqual("Working directory must exist", translate("Working directory must exist", language="en"))
            self.assertEqual("Unlisted synthetic failure", translate("Unlisted synthetic failure"))

    def test_user_data_and_embedded_english_are_never_rewritten(self):
        messages = (
            "synthetic user says Working directory must exist",
            "Executable not found: /synthetic/Working directory must exist",
            "Profile field 'synthetic-user-field' must be a string",
            "Project 'synthetic-user-project' root is not a directory",
            "{synthetic_user_field}",
        )
        with use_language("zh-CN"):
            for message in messages:
                with self.subTest(message=message):
                    error = BridgeError("synthetic_error_code", message)
                    self.assertEqual(message, translate(str(error)))
                    self.assertEqual({"code": "synthetic_error_code", "message": message}, error.as_dict())

    def test_only_known_config_field_errors_have_exact_translations(self):
        with use_language("zh-CN"):
            self.assertEqual(
                "配置字段 'runtime' 必须为字符串",
                translate("Profile field 'runtime' must be a string"),
            )
        self.assertTrue(all(isinstance(key, str) and isinstance(value, str) for key, value in MESSAGES.items()))


if __name__ == "__main__":
    unittest.main()
