"""Pure pricing-window checks; no bridge service, provider request or model execution."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).parents[1] / "skills/cli-agent-bridge/scripts/provider_windows.py"
SPEC = importlib.util.spec_from_file_location("provider_windows", SCRIPT)
provider_windows = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(provider_windows)


def policy(value: str, **kwargs):
    return provider_windows.policy_at(datetime.fromisoformat(value), **kwargs)


class ProviderWindowTests(unittest.TestCase):
    def test_ollama_weekday_peak_uses_half_open_utc_boundaries(self):
        for stamp, expected in (
            ("2026-11-09T11:59:59.999999+00:00", True),
            ("2026-11-09T12:00:00+00:00", False),
            ("2026-11-09T17:59:59.999999+00:00", False),
            ("2026-11-09T18:00:00+00:00", True),
        ):
            with self.subTest(stamp=stamp):
                self.assertEqual(expected, policy(stamp)["ollama_off_peak"])

    def test_ollama_uses_utc_weekend_even_when_beijing_date_differs(self):
        for stamp, expected in (
            ("2026-11-07T01:00:00+08:00", False),  # Saturday Beijing, Friday 17:00 UTC.
            ("2026-11-07T08:00:00+08:00", True),
            ("2026-11-08T15:00:00+00:00", True),
            ("2026-11-09T00:00:00+08:00", True),  # Monday Beijing, Sunday 16:00 UTC.
        ):
            with self.subTest(stamp=stamp):
                self.assertEqual(expected, policy(stamp)["ollama_off_peak"])

    def test_official_peak_uses_half_open_beijing_boundaries(self):
        for stamp, expected in (
            ("2026-11-09T08:59:59.999999+08:00", True),
            ("2026-11-09T09:00:00+08:00", False),
            ("2026-11-09T11:59:59.999999+08:00", False),
            ("2026-11-09T12:00:00+08:00", True),
            ("2026-11-09T13:59:59.999999+08:00", True),
            ("2026-11-09T14:00:00+08:00", False),
            ("2026-11-09T17:59:59.999999+08:00", False),
            ("2026-11-09T18:00:00+08:00", True),
        ):
            with self.subTest(stamp=stamp):
                self.assertEqual(expected, policy(stamp)["deepseek_off_peak"])

    def test_official_weekends_follow_beijing_date(self):
        for stamp in ("2026-11-07T10:00:00+08:00", "2026-11-08T15:00:00+08:00"):
            with self.subTest(stamp=stamp):
                self.assertTrue(policy(stamp)["deepseek_off_peak"])
        self.assertTrue(policy("2026-11-06T16:00:00+00:00")["deepseek_off_peak"])
        self.assertFalse(policy("2026-11-09T02:00:00+00:00")["deepseek_off_peak"])

    def test_confirmed_holiday_changes_only_the_official_window(self):
        ordinary = policy("2026-11-09T15:00:00+08:00")
        holiday = policy("2026-11-09T15:00:00+08:00", china_public_holiday=True)
        self.assertFalse(ordinary["deepseek_off_peak"])
        self.assertTrue(holiday["deepseek_off_peak"])
        self.assertEqual(ordinary["ollama_off_peak"], holiday["ollama_off_peak"])
        self.assertEqual(["claude-ollama"], ordinary["preferred_flash_profiles"])
        self.assertEqual(
            ["claude-ollama", "pi-deepseek", "claude-deepseek"], holiday["preferred_flash_profiles"],
        )

    def test_ollama_is_first_when_both_providers_are_off_peak(self):
        self.assertEqual(
            ["claude-ollama", "pi-deepseek", "claude-deepseek"],
            policy("2026-11-09T11:00:00+00:00")["preferred_flash_profiles"],
        )

    def test_official_profiles_are_selected_during_ollama_peak(self):
        value = policy("2026-11-09T12:00:00+00:00")
        self.assertFalse(value["ollama_off_peak"])
        self.assertTrue(value["deepseek_off_peak"])
        self.assertEqual(["pi-deepseek", "claude-deepseek"], value["preferred_flash_profiles"])

    def test_only_ollama_is_selected_during_official_peak(self):
        value = policy("2026-11-09T02:00:00+00:00")
        self.assertTrue(value["ollama_off_peak"])
        self.assertFalse(value["deepseek_off_peak"])
        self.assertEqual(["claude-ollama"], value["preferred_flash_profiles"])

    def test_same_instant_has_same_policy_regardless_of_input_timezone(self):
        reference = policy("2026-11-09T12:00:00+00:00")
        self.assertEqual(reference, policy("2026-11-09T20:00:00+08:00"))
        self.assertEqual(reference, policy("2026-11-09T07:00:00-05:00"))

    def test_naive_time_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "timezone"):
            provider_windows.policy_at(datetime(2026, 11, 9, 12))

    def test_weekly_schedule_always_has_at_least_one_off_peak_provider(self):
        start = datetime(2026, 11, 9, tzinfo=UTC)
        for hour in range(7 * 24):
            moment = start + timedelta(hours=hour)
            with self.subTest(moment=moment):
                value = provider_windows.policy_at(moment)
                self.assertTrue(value["ollama_off_peak"] or value["deepseek_off_peak"])
                self.assertTrue(value["preferred_flash_profiles"])


class ProviderWindowCliTests(unittest.TestCase):
    def invoke(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, timeout=5)

    def test_cli_reports_known_time_without_actions(self):
        result = self.invoke("--at", "2026-11-09T12:00:00Z")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(policy("2026-11-09T12:00:00+00:00"), json.loads(result.stdout))

    def test_cli_rejects_bad_or_naive_timestamp(self):
        for stamp in ("not-a-time", "2026-11-09T12:00:00"):
            with self.subTest(stamp=stamp):
                result = self.invoke("--at", stamp)
                self.assertEqual(2, result.returncode)
                self.assertEqual("", result.stdout)
                self.assertNotIn("Traceback", result.stderr)

    def test_cli_holiday_flag_applies_to_given_beijing_date(self):
        result = self.invoke("--at", "2026-11-09T15:00:00+08:00", "--china-public-holiday")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(
            policy("2026-11-09T15:00:00+08:00", china_public_holiday=True), json.loads(result.stdout),
        )

    def test_default_time_is_current_utc(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                self.assertEqual(UTC, tz)
                return datetime(2026, 11, 9, 12, tzinfo=UTC)

        with patch.object(provider_windows, "datetime", FixedDateTime), patch.object(sys, "argv", [str(SCRIPT)]), \
                patch("builtins.print") as output:
            provider_windows.main()
        self.assertEqual(policy("2026-11-09T12:00:00+00:00"), json.loads(output.call_args.args[0]))
