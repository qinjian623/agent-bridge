import asyncio
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from agent_bridge.broker import Broker, request


class RoutingDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.state = Path(temporary.name)
        fixture = {"model": "fixture", "executable": sys.executable, "verified": True}
        config = {
            "profiles": {
                "plain": {**fixture, "runtime": "pi", "provider": "deepseek"},
                "preferred-alias": {**fixture, "runtime": "claude", "provider": "ollama-anthropic"},
            },
            "worker_launcher": "detached",
            "routing": {
                "windows": {"all-day": {"timezone": "UTC", "ranges": []}},
                "preferences": [{
                    "id": "flash-off-peak",
                    "when": {"kinds": ["flash"]},
                    "prefer": [{
                        "match": {"runtime": "claude", "provider": "ollama-anthropic"},
                        "window": "all-day",
                    }],
                }],
            },
        }
        (self.state / "config.json").write_text(json.dumps(config), encoding="utf-8")
        self.broker = Broker(self.state)
        self.server = await asyncio.start_unix_server(self.broker.connection, str(self.state / "broker.sock"))
        self.addAsyncCleanup(self.close_server)

    async def close_server(self):
        self.server.close()
        await self.server.wait_closed()

    def dispatch_args(self, request_id, **kwargs):
        return {"cwd": str(self.state), "instruction": "fixture", "request_id": request_id,
                "requirements": {"kind": "flash"}, **kwargs}

    async def test_automatic_dispatch_persists_preference_and_replays_original_selection(self):
        args = self.dispatch_args("automatic")
        result = await request(self.state, "agent_dispatch", args)
        task = self.broker.store.get_task(result["task_id"])
        selection = task["details"]["selection"]
        self.assertEqual("queued", task["status"])
        self.assertEqual("preferred-alias", selection["profile"])
        self.assertEqual("preferred-alias", self.broker.store.get_session(result["session_id"])["profile"]["name"])
        preference = selection["preference"]
        self.assertEqual("flash-off-peak", preference["rule_id"])
        self.assertIs(preference["applied"], True)
        self.assertEqual(timedelta(0), datetime.fromisoformat(preference["evaluated_at_utc"]).utcoffset())

        with patch.object(self.broker.config, "routing", ()):
            self.assertEqual(result, await request(self.state, "agent_dispatch", args))
            self.assertEqual(1, len(self.broker.store.sessions()))
            fresh = await request(self.state, "agent_dispatch", self.dispatch_args("without-routing"))
        self.assertEqual(selection, self.broker.store.get_task(result["task_id"])["details"]["selection"])
        self.assertEqual("plain", self.broker.store.get_session(fresh["session_id"])["profile"]["name"])
        with self.broker.store.connection() as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])

    async def test_explicit_profile_and_existing_session_skip_preference(self):
        explicit = await request(self.state, "agent_dispatch", self.dispatch_args("explicit", profile="plain"))
        task = self.broker.store.get_task(explicit["task_id"])
        self.assertEqual("plain", task["details"]["selection"]["profile"])
        self.assertNotIn("preference", task["details"]["selection"])

        followup = await request(self.state, "agent_dispatch", self.dispatch_args("existing", session_id=explicit["session_id"]))
        self.assertEqual(explicit["session_id"], followup["session_id"])
        task = self.broker.store.get_task(followup["task_id"])
        self.assertIsNone(task["details"]["selection"])
        self.assertEqual(1, len(self.broker.store.sessions()))

    async def test_retry_without_session_keeps_legacy_selection(self):
        original = await request(self.state, "agent_dispatch", self.dispatch_args("original", profile="plain"))
        task_id = original["task_id"]
        self.broker.store.cancel(task_id, "cancel-original")

        retried = await request(self.state, "agent_dispatch", self.dispatch_args("retry", retry_task_id=task_id))
        self.assertEqual(task_id, retried["task_id"])
        self.assertNotEqual(original["session_id"], retried["session_id"])
        profile = self.broker.store.get_session(retried["session_id"])["profile"]
        self.assertEqual("plain", profile["name"])
