import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.parent_usage import meter_usage, refresh_meter, start_meter
from agent_bridge.routing import task_samples
from agent_bridge.store import Store
from agent_bridge.usage import claude_usage, priced_usage, validate_price


class UsageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "session.jsonl"
        self.append("session_meta", {"id": "native"})
        self.append("turn_context", {"model": "model"})

    def append(self, kind, payload, timestamp=1):
        with self.path.open("a") as stream:
            stream.write(
                json.dumps(
                    {"type": kind, "payload": payload, "timestamp": datetime.fromtimestamp(timestamp, UTC).isoformat()}
                )
                + "\n"
            )

    def usage(self, total, timestamp):
        self.append(
            "event_msg",
            {
                "type": "token_count",
                "info": {
                    "total_token_usage": {
                        "input_tokens": total - 10,
                        "output_tokens": 10,
                        "cached_input_tokens": 20,
                        "total_tokens": total,
                        "reasoning_output_tokens": 4,
                    }
                },
            },
            timestamp,
        )

    def test_parent_incremental_restart_duplicate_and_partial_line(self):
        self.usage(100, 2)
        meter = start_meter(self.path, "native")
        self.usage(150, 3)
        self.usage(150, 3)
        meter = refresh_meter(meter)
        self.assertEqual(50, meter_usage(meter)["total_tokens"])
        # Serializing the checkpoint simulates a process restart.
        meter = refresh_meter(json.loads(json.dumps(meter)))
        self.assertEqual(50, meter_usage(meter)["total_tokens"])
        with self.path.open("ab") as stream:
            stream.write(b'{"type":"unrelated"')
        offset = meter["offset"]
        self.assertEqual(offset, refresh_meter(meter)["offset"])
        with self.path.open("ab") as stream:
            stream.write(b"}\n")
        self.assertGreater(refresh_meter(meter)["offset"], offset)
        self.assertEqual(50, meter_usage(start_meter(self.path, "native", since=2.5))["total_tokens"])

    def test_reset_replacement_and_wrong_identity_are_not_zero_usage(self):
        self.usage(100, 2)
        meter = start_meter(self.path, "native")
        self.usage(150, 3)
        self.usage(40, 4)
        result = meter_usage(refresh_meter(meter))
        self.assertFalse(result["complete"])
        self.assertEqual(50, result["total_tokens"])
        self.path.write_text("")
        self.assertFalse(refresh_meter(meter)["complete"])
        self.path.unlink()
        self.append("session_meta", {"id": "different"})
        with self.assertRaisesRegex(BridgeError, "does not match"):
            start_meter(self.path, "native")

    def test_malformed_native_record_does_not_stop_other_meters(self):
        meter = start_meter(self.path, "native")
        with self.path.open("a") as stream:
            stream.write("[1,2]\n")
        result = refresh_meter(meter)
        self.assertFalse(result["complete"])
        self.assertIn("Malformed", result["error"])

    def test_parent_meter_enters_shared_budget_without_notifications(self):
        store = Store(self.root / "state")
        self.usage(100, 2)
        args = ("parent", "parent_register", 0, "register", "native", str(self.root))
        registered = store.parent_control(*args, usage_path=str(self.path), notify=False)
        self.usage(150, 3)
        self.assertEqual(registered, store.parent_control(*args, usage_path=str(self.path), notify=False))
        store.refresh_parent_usage()
        self.assertEqual(50, store.budget()["parent_reported_tokens"])
        profile = Profile("test", "codex", "model")
        sid = store.open(profile, str(self.root), "read-only", "open", "parent")["session_id"]
        store.worker_ready(sid, "generation", 1, "start", "child-native")
        task = store.dispatch(sid, "task", "dispatch", reservation=30)["task_id"]
        config = Config({"test": profile}, token_budget=75)
        self.assertIsNone(store.claim(config))
        config.token_budget = 90
        self.assertIsNotNone(store.claim(config))
        attempt = store.begin_attempt(sid, "generation")["active_attempt"]
        store.finish(sid, "generation", task, attempt, "completed", {}, {"total_tokens": 20, "complete": True})
        self.assertEqual(70, store.budget()["reported_tokens"])
        self.assertTrue(store.budget()["complete"])
        self.assertEqual([], store.get_parent("parent")["deliveries"])
        store.review(task, attempt, "accepted", "independent check", "review", 1)
        self.assertIsNone(task_samples(store, profile, "general")[0]["tokens"])
        with self.assertRaisesRegex(BridgeError, "already registered"):
            store.parent_control("another", "parent_register", 0, "duplicate", "native", str(self.root))
        store.parent_control("parent", "parent_meter_stop", 0, "stop")
        self.usage(200, 4)
        store.refresh_parent_usage()
        self.assertEqual(70, store.budget()["reported_tokens"])
        self.assertEqual(70, task_samples(store, profile, "general")[0]["tokens"])

    def test_moving_a_task_does_not_assign_its_parent_overhead_to_the_remaining_task(self):
        store = Store(self.root / "state")
        self.usage(100, 2)
        store.parent_control("parent", "parent_register", 0, "register", "native", str(self.root), usage_path=str(self.path), notify=False)
        store.parent_control("other", "parent_register", 0, "register-other", "native-other", str(self.root), notify=False)
        profile = Profile("test", "codex", "model")
        config = Config({"test": profile})
        sid = store.open(profile, str(self.root), "read-only", "open", "parent")["session_id"]
        other = store.open(profile, str(self.root), "read-only", "open-other", "other")["session_id"]
        store.worker_ready(sid, "g", 1, "start", "child")
        store.worker_ready(other, "g", 2, "start", "other-child")
        tasks = []
        for status, tokens in (("failed", 10), ("completed", 20)):
            tid = store.dispatch(sid, "task", status)["task_id"]
            aid = store.claim(config)["attempt_id"]
            store.begin_attempt(sid, "g")
            store.finish(sid, "g", tid, aid, status, {}, {"total_tokens": tokens, "complete": True})
            if status == "completed":
                store.review(tid, aid, "accepted", "verified", "review", store.get_session(sid)["revision"])
            tasks.append(tid)
        self.usage(150, 3)
        store.parent_control("parent", "parent_meter_stop", 0, "stop")
        store.retry(tasks[0], other, "task", "retry")
        samples = task_samples(store, profile, "general")
        self.assertEqual([False, True], [s["quality"] for s in samples])
        self.assertTrue(all(s["tokens"] is None and s["seconds"] is None and s["usd"] is None for s in samples))
        self.assertEqual(50, store.get_parent("parent")["usage"]["total_tokens"])

    def test_claude_all_models_use_deltas_and_reset_remains_partial(self):
        before = {
            "m": {
                "inputTokens": 100,
                "outputTokens": 10,
                "cacheReadInputTokens": 40,
                "cacheCreationInputTokens": 0,
                "costUSD": 1,
            }
        }
        after = {
            "m": {
                "inputTokens": 140,
                "outputTokens": 20,
                "cacheReadInputTokens": 50,
                "cacheCreationInputTokens": 0,
                "costUSD": 2,
            },
            "aux": {
                "inputTokens": 3,
                "outputTokens": 2,
                "cacheReadInputTokens": 0,
                "cacheCreationInputTokens": 0,
                "costUSD": 0.1,
            },
        }
        usage = claude_usage(before, after, {"input_tokens": 1}, 2.1)
        self.assertEqual(65, usage["total_tokens"])
        self.assertTrue(usage["complete"])
        self.assertAlmostEqual(1.1, usage["catalog_estimate"]["usd"])
        self.assertEqual(0, claude_usage(after, after, {}, 2.1)["total_tokens"])
        self.assertFalse(claude_usage(after, before, {"input_tokens": 1}, 1)["complete"])
        self.assertIsNone(claude_usage(None, after, {}, 2.1)["total_tokens"])

    def test_prices_separate_cache_and_unknown_rates_from_zero(self):
        price = {
            "currency": "USD",
            "source": "test fixture",
            "as_of": "2026-10-22",
            "per_million": {"input_tokens": 2, "output_tokens": 10, "cache_read_tokens": 0.5},
        }
        usage = {
            "input_tokens": 1000,
            "output_tokens": 100,
            "cache_read_tokens": 800,
            "cache_write_tokens": 0,
            "complete": True,
            "input_includes_cache": True,
        }
        self.assertAlmostEqual(0.0018, priced_usage(usage, price)["catalog_estimate"]["usd"])
        self.assertIsNone(priced_usage(usage, None)["actual_billed_usd"])
        del price["per_million"]["cache_read_tokens"]
        self.assertIsNone(priced_usage(usage, price)["catalog_estimate"]["usd"])
        with self.assertRaises(BridgeError):
            validate_price({**price, "per_million": {"input_tokens": float("nan"), "output_tokens": 1}})
