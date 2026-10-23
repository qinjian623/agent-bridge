import sys
import tempfile
import time
import unittest
from pathlib import Path

from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.quota import provider_limit
from agent_bridge.routing import choose
from agent_bridge.store import Store


class QuotaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.store = Store(self.path)
        self.profiles = {
            name: Profile(name, "codex", name, quota_group=group, verified=True, executable=sys.executable)
            for name, group in (("one", "shared"), ("two", "shared"), ("other", "separate"))
        }
        self.config = Config(self.profiles)
        self.sessions = {}
        for name, profile in self.profiles.items():
            sid = self.store.open(profile, str(self.path), "read-only", name)["session_id"]
            self.store.worker_ready(sid, "g", 1, "start", "native-" + name)
            self.sessions[name] = sid

    def test_provider_delay_shared_across_profiles_survives_restart(self):
        first = self.store.dispatch(self.sessions["one"], "fail", "fail")["task_id"]
        self.store.claim(self.config)
        attempt = self.store.begin_attempt(self.sessions["one"], "g")["active_attempt"]
        limit = provider_limit({"status": 429, "headers": {"retry-after": "120"}})
        self.store.finish(self.sessions["one"], "g", first, attempt, "failed", {"provider_limit": limit})
        self.store.dispatch(self.sessions["two"], "queued", "queued")
        self.store = Store(self.path)
        self.assertIsNone(self.store.claim(self.config))
        self.assertGreater(self.store.quotas()[0]["blocked_until"], time.time() + 115)
        self.assertEqual("other", choose(self.config, self.store, {})["profile"])
        self.assertEqual("two", choose(self.config, self.store, {}, "two")["profile"])
        # Expiry permits queued work; the failed task is never replayed.
        with self.store.connection(write=True) as db:
            db.execute("UPDATE quota_cooldowns SET blocked_until=0")
        self.assertEqual(self.sessions["two"], self.store.claim(self.config)["session_id"])
        self.assertEqual(1, len(self.store.get_task(first)["attempts"]))

    def test_unknown_reset_needs_explicit_revision_and_evidence(self):
        with self.store.connection(write=True) as db:
            self.store.cooldown(db, "shared", provider_limit({"code": "insufficient_quota"}))
        self.store.dispatch(self.sessions["two"], "queued", "queued")
        self.assertIsNone(self.store.claim(self.config))
        with self.assertRaises(BridgeError):
            self.store.quota_resume("shared", 1, "stale", "provider available")
        result = self.store.quota_resume("shared", 0, "resume", "provider available")
        self.assertEqual(result, self.store.quota_resume("shared", 0, "resume", "provider available"))
        self.assertIsNotNone(self.store.claim(self.config))
        self.assertIsNone(provider_limit("invalid model name"))
