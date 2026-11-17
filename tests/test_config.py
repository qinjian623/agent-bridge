import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_bridge.config import Config
from agent_bridge.errors import BridgeError


class ConfigValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.path = self.home / "config.json"
        self.secret = "sk-test-secret-value-1234567890"

    def _write(self, payload):
        self.path.write_text(json.dumps(payload), encoding="utf-8")

    def test_unknown_profile_field_is_a_clear_redacted_bridge_error(self):
        self._write(
            {
                "profiles": {
                    "p1": {
                        "runtime": "codex",
                        "model": "gpt-5",
                        "unknown_option": self.secret,
                    }
                }
            }
        )
        with self.assertRaises(BridgeError) as caught:
            Config.load(self.path)
        self.assertEqual("invalid_config", caught.exception.code)
        message = str(caught.exception)
        self.assertIn("unknown fields", message)
        self.assertNotIn(self.secret, message)
        self.assertNotIn("Traceback", message)

    def test_unknown_top_level_field_is_a_clear_redacted_bridge_error(self):
        self._write({"profiles": {}, "unknown_option": self.secret})
        with self.assertRaises(BridgeError) as caught:
            Config.load(self.path)
        self.assertEqual("invalid_config", caught.exception.code)
        message = str(caught.exception)
        self.assertIn("unknown fields", message)
        self.assertNotIn(self.secret, message)

    def test_malformed_known_profile_fields_cannot_pass_validation(self):
        for field, value in {
            "executable": [self.secret],
            "provider": {"secret": self.secret},
            "env_file": [self.secret],
            "tools": "read",
            "input_types": [1],
            "verified": "false",
            "context_window": True,
        }.items():
            with self.subTest(field=field):
                self._write({"profiles": {"sample": {"runtime": "codex", "model": "sample", field: value}}})
                original = self.path.read_bytes()
                with self.assertRaises(BridgeError) as caught:
                    Config.load(self.path)
                self.assertEqual("invalid_config", caught.exception.code)
                self.assertNotIn(self.secret, str(caught.exception))
                self.assertEqual(original, self.path.read_bytes())

    def test_malformed_root_profiles_and_profile_values_are_bridge_errors(self):
        for payload in (
            [],
            {},
            {"profiles": []},
            {"profiles": {"p1": []}},
            {"profiles": {"p1": None}},
        ):
            with self.subTest(payload=payload):
                self._write(payload)
                with self.assertRaises(BridgeError) as caught:
                    Config.load(self.path)
                self.assertEqual("invalid_config", caught.exception.code)

    def test_malformed_top_level_types_are_bridge_errors_without_values(self):
        for payload in (
            {"profiles": {}, "parallel_limit": self.secret},
            {"profiles": {}, "worker_launcher": [self.secret]},
            {"profiles": {}, "task_timeout": {"secret": self.secret}},
        ):
            with self.subTest(field=next(key for key in payload if key != "profiles")):
                self._write(payload)
                with self.assertRaises(BridgeError) as caught:
                    Config.load(self.path)
                self.assertEqual("invalid_config", caught.exception.code)
                self.assertNotIn(self.secret, str(caught.exception))

    def test_valid_config_still_loads(self):
        self._write(
            {
                "profiles": {
                    "p1": {
                        "runtime": "codex",
                        "provider": "openai-codex",
                        "model": "gpt-5",
                        "name": "p1",
                    }
                },
                "worker_launcher": "detached",
            }
        )
        config = Config.load(self.path)
        self.assertEqual(["p1"], list(config.profiles))
        self.assertEqual("detached", config.worker_launcher)

    def test_invalid_json_and_names_do_not_leak_input(self):
        for content in (
            '{"profiles":',
            json.dumps({"profiles": {self.secret: []}}),
            json.dumps({"profiles": {"p": {"runtime": "codex", "model": "x", self.secret: 1}}}),
            json.dumps({"profiles": {}, self.secret: 1}),
            json.dumps({"profiles": {}, "hosts": {self.secret: []}}),
            json.dumps({"profiles": {}, "projects": {self.secret: "./missing-synthetic-project"}}),
        ):
            with self.subTest(case=content[:15]):
                self.path.write_text(content)
                with self.assertRaises(BridgeError) as caught:
                    Config.load(self.path)
                self.assertEqual("invalid_config", caught.exception.code)
                self.assertNotIn(self.secret, str(caught.exception))
                self.assertEqual(content, self.path.read_text())

    def test_counters_are_integers_and_numeric_values_exclude_booleans(self):
        for field, value in (
            ("parallel_limit", 1.5), ("quota_limit", True), ("parallel_limit", False),
            ("task_timeout", True), ("interrupt_timeout", False), ("token_budget", True),
            ("task_timeout", 10 ** 1000),
        ):
            with self.subTest(field=field, value=value):
                self._write({"profiles": {}, field: value})
                with self.assertRaises(BridgeError):
                    Config.load(self.path)

    def test_host_interval_overflow_is_a_redacted_config_error(self):
        self._write({"profiles": {}, "hosts": {"worker": {
            "ssh_target": "worker.invalid", "remote_executable": "/usr/local/bin/agent-bridge",
            "pull_interval": 10 ** 1000,
        }}})
        with self.assertRaises(BridgeError) as caught:
            Config.load(self.path)
        self.assertEqual("invalid_config", caught.exception.code)
        self.assertEqual("Invalid hosts configuration", str(caught.exception))

    def test_unresolvable_project_home_is_a_redacted_config_error(self):
        self._write({"profiles": {}, "projects": {"sample": "~missing-synthetic-user/project"}})
        with patch.object(Path, "expanduser", side_effect=RuntimeError(self.secret)):
            with self.assertRaises(BridgeError) as caught:
                Config.load(self.path)
        self.assertEqual("invalid_config", caught.exception.code)
        self.assertNotIn(self.secret, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
