import asyncio
import os
import string
import unittest
from collections import Counter
from unittest.mock import patch

from agent_bridge.i18n import (
    configured_language,
    get_catalog,
    resolve_language,
    translate,
    use_language,
    validate_language,
)


class LanguageTests(unittest.TestCase):
    def test_english_default_and_explicit_auto_locale(self):
        with patch.dict(os.environ, {"LANG": "zh_CN.UTF-8"}, clear=True):
            self.assertEqual("en", resolve_language())
            self.assertEqual("zh-CN", resolve_language("auto"))
            self.assertEqual("zh-CN", validate_language("zh_CN.UTF-8"))
            self.assertEqual("en", resolve_language("fr-FR"))
            with self.assertRaises(ValueError):
                validate_language("fr-FR")

    def test_auto_uses_posix_precedence_and_never_changes_environment(self):
        with patch.dict(os.environ, {"LC_ALL": "C.UTF-8", "LC_MESSAGES": "zh_CN.UTF-8", "LANG": "zh_CN.UTF-8"}, clear=True):
            before = dict(os.environ)
            self.assertEqual("en", resolve_language("auto"))
            with use_language("zh-CN"):
                self.assertEqual("zh-CN", resolve_language())
            self.assertEqual(before, dict(os.environ))

    def test_context_environment_config_precedence_and_restore_on_failure(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual("zh-CN", configured_language("zh-CN"))
            with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "en"}):
                self.assertEqual("en", configured_language("zh-CN"))
                with self.assertRaises(RuntimeError), use_language("zh-CN"):
                    self.assertEqual("zh-CN", configured_language("en"))
                    raise RuntimeError("fixture")
                self.assertEqual("en", resolve_language())

    def test_empty_environment_selects_english_before_config(self):
        with patch.dict(os.environ, {"AGENT_BRIDGE_LANG": "", "LANG": "zh_CN.UTF-8"}, clear=True):
            self.assertEqual("en", configured_language("zh-CN"))
            with use_language("zh-CN"):
                self.assertEqual("zh-CN", configured_language("en"))

    def test_template_formatting_fallback_and_catalog_copy(self):
        with use_language("zh-CN"):
            self.assertEqual("子任务 fixture", translate("Child task {task_id}", task_id="fixture"))
            self.assertEqual("Original fixture", translate("Original {value}", value="fixture"))
            catalog = get_catalog()
            catalog["Child task {task_id}"] = "changed"
            self.assertEqual("子任务 fixture", translate("Child task {task_id}", task_id="fixture"))
        with use_language("en"):
            self.assertEqual({}, get_catalog())

    def test_catalog_template_fields_conversions_and_specs_match(self):
        formatter = string.Formatter()

        def fields(text):
            return Counter((field, spec, conversion) for _, field, spec, conversion in formatter.parse(text) if field is not None)

        for original, translated in get_catalog("zh-CN").items():
            with self.subTest(original=original):
                self.assertIsInstance(translated, str)
                self.assertEqual(fields(original), fields(translated))


class ConcurrentLanguageTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_and_thread_language_choices_do_not_leak(self):
        async def run(language):
            with use_language(language):
                await asyncio.sleep(0)
                value = translate("Child task {task_id}", task_id="fixture")
                threaded = await asyncio.to_thread(translate, "Child task {task_id}", task_id="fixture")
                return value, threaded

        with patch.dict(os.environ, {}, clear=True):
            result = await asyncio.gather(run("en"), run("zh-CN"))
            self.assertEqual([("Child task fixture", "Child task fixture"), ("子任务 fixture", "子任务 fixture")], result)
            self.assertEqual("en", resolve_language())
