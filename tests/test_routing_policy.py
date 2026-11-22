from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from agent_bridge import routing
from agent_bridge.config import Config
from agent_bridge.errors import BridgeError


def example_config():
    weekdays = ["mon", "tue", "wed", "thu", "fri"]
    return {
        "profiles": {
            "official": {
                "runtime": "pi", "provider": "deepseek", "model": "flash", "verified": True,
                "executable": sys.executable, "quota_group": "official",
            },
            "cloud": {
                "runtime": "claude", "provider": "ollama-anthropic", "model": "flash", "verified": True,
                "executable": sys.executable, "quota_group": "cloud",
            },
        },
        "routing": {
            "calendars": {"cn": {"holidays": ["2026-10-01"]}},
            "windows": {
                "ollama": {"timezone": "UTC", "ranges": [{"days": weekdays, "start": "12:00", "end": "18:00"}]},
                "official": {
                    "timezone": "Asia/Shanghai", "calendar": "cn", "ranges": [
                        {"days": weekdays, "start": "09:00", "end": "12:00"},
                        {"days": weekdays, "start": "14:00", "end": "18:00"},
                    ],
                },
            },
            "preferences": [{
                "id": "flash-off-peak", "when": {"kinds": ["flash"]}, "prefer": [
                    {"match": {"runtime": "claude", "provider": "ollama-anthropic"}, "window": "ollama"},
                    {"match": {"provider": "deepseek"}, "window": "official"},
                ],
            }],
        },
    }


class RoutingPolicyTests(unittest.TestCase):
    def load(self, config):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            return Config.load(path)

    def test_config_rejects_invalid_windows_calendars_and_preferences(self):
        for section, key, value in (
            ("window", "timezone", "Invalid/Timezone"),
            ("window", "calendar", "missing"),
            ("range", "start", "24:00"),
            ("range", "end", "12:00"),
            ("range", "days", ["monday"]),
            ("calendar", "holidays", ["2026-02-30"]),
            ("calendar", "holidays", ["20261001"]),
            ("rule", "score_tolerance", True),
            ("rule", "score_tolerance", 2),
            ("rule", "when", {"kinds": []}),
            ("rule", "when", {"kinds": ["flash"], "strategies": ["unknown"]}),
            ("target", "window", "missing"),
            ("target", "match", {}),
            ("target", "match", {"profile": "missing"}),
            ("target", "typo", True),
        ):
            with self.subTest(section=section, key=key, value=value):
                config = example_config()
                policy = config["routing"]
                rule = policy["preferences"][0]
                window = policy["windows"]["ollama"]
                {
                    "calendar": policy["calendars"]["cn"], "window": window, "range": window["ranges"][0],
                    "rule": rule, "target": rule["prefer"][0],
                }[section][key] = value
                with self.assertRaises(BridgeError) as raised:
                    self.load(config)
                self.assertEqual("invalid_config", raised.exception.code)

    def test_old_config_has_no_routing_preferences(self):
        config = example_config()
        del config["routing"]
        self.assertEqual((), self.load(config).routing)

    def test_low_peak_windows_use_local_holidays_and_half_open_bounds(self):
        cloud, official = self.load(example_config()).routing[0].prefer
        for target, at, expected in (
            (cloud, "2026-09-30T11:59:59+00:00", True),
            (cloud, "2026-09-30T12:00:00+00:00", False),
            (cloud, "2026-09-30T18:00:00+00:00", True),
            (cloud, "2026-10-01T12:00:00+00:00", False),
            (cloud, "2026-10-03T12:00:00+00:00", True),
            (official, "2026-09-30T00:59:59+00:00", True),
            (official, "2026-09-30T01:00:00+00:00", False),
            (official, "2026-09-30T04:00:00+00:00", True),
            (official, "2026-09-30T06:00:00+00:00", False),
            (official, "2026-09-30T10:00:00+00:00", True),
            (official, "2026-10-01T01:00:00+00:00", True),
            (official, "2026-10-03T01:00:00+00:00", True),
        ):
            with self.subTest(window=target.window_name, at=at):
                self.assertIs(target.window.is_off_peak(datetime.fromisoformat(at)), expected)

    def test_overnight_ranges_follow_the_start_day(self):
        config = example_config()
        config["routing"]["windows"]["official"]["ranges"] = [
            {"days": ["thu", "fri"], "start": "22:00", "end": "06:00"},
        ]
        night = self.load(config).routing[0].prefer[1].window
        for at, expected in (
            ("2026-10-01T22:00:00+08:00", True),
            ("2026-10-02T04:00:00+08:00", True),
            ("2026-10-02T22:00:00+08:00", False),
            ("2026-10-03T05:59:59+08:00", False),
            ("2026-10-03T06:00:00+08:00", True),
        ):
            with self.subTest(at=at):
                self.assertIs(night.is_off_peak(datetime.fromisoformat(at)), expected)

    def test_preferences_apply_only_to_matching_eligible_candidates(self):
        config = self.load(example_config())
        store = Mock()
        store.quotas.return_value = []
        flash = {"kind": "flash"}
        with patch.object(routing, "task_samples", return_value=[]), patch.object(routing, "datetime") as clock:
            clock.now.return_value = datetime.fromisoformat("2026-10-01T08:00:00+00:00")
            result = routing.choose(config, store, flash)
            self.assertEqual("cloud", result["profile"])
            self.assertEqual(["ollama", "official"], result["preference"]["active_windows"])
            clock.now.assert_called_once()
            for requirements, name in (({}, None), ({**flash, "strategy": "speed"}, None), (flash, "official")):
                result = routing.choose(config, store, requirements, name)
                self.assertEqual("official", result["profile"])
                self.assertNotIn("preference", result)

            store.quotas.return_value = [{"quota_group": "cloud", "blocked_until": None}]
            result = routing.choose(config, store, flash)
            self.assertEqual("official", result["profile"])
            self.assertEqual("official", result["preference"]["window"])
            self.assertEqual("cloud", result["excluded"][0]["profile"])

            store.quotas.return_value = []
            clock.now.return_value = datetime.fromisoformat("2026-09-30T14:00:00+00:00")
            self.assertEqual("official", routing.choose(config, store, flash)["profile"])

    def test_measured_preferences_keep_pareto_filter_and_score_tolerance(self):
        config = self.load(example_config())
        store = Mock()
        store.quotas.return_value = []
        for tokens, seconds, expected in ((9, 12, "cloud"), (9, 18, "official"), (11, 11, "official")):
            with self.subTest(tokens=tokens, seconds=seconds):
                samples = {
                    "official": [{"quality": True, "tokens": 10, "seconds": 10, "usd": None}],
                    "cloud": [{"quality": True, "tokens": tokens, "seconds": seconds, "usd": None}],
                }
                with patch.object(routing, "task_samples", side_effect=lambda _, p, kind, data=samples: data[p.name]):
                    with patch.object(routing, "datetime") as clock:
                        clock.now.return_value = datetime.fromisoformat("2026-10-01T08:00:00+00:00")
                        result = routing.choose(config, store, {"kind": "flash"})
                self.assertEqual(expected, result["profile"])

    def test_preference_can_select_a_model_under_the_same_provider(self):
        raw = example_config()
        cloud = raw["profiles"]["cloud"]
        raw["profiles"]["cloud"] = {**cloud, "model": "other-model"}
        raw["profiles"]["cloud-flash"] = cloud
        raw["routing"]["preferences"][0]["prefer"][0]["match"]["model"] = "flash"
        config = self.load(raw)
        store = Mock()
        store.quotas.return_value = []
        with patch.object(routing, "task_samples", return_value=[]), patch.object(routing, "datetime") as clock:
            clock.now.return_value = datetime.fromisoformat("2026-10-01T08:00:00+00:00")
            self.assertEqual("cloud-flash", routing.choose(config, store, {"kind": "flash"})["profile"])
