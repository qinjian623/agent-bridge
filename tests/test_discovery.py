import asyncio
import json
import shutil
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from urllib.error import HTTPError

from agent_bridge.broker import Broker
from agent_bridge.config import Profile
from agent_bridge.discovery import ModelDirectory, NoRedirect, deepseek_models, model_record, native_models
from agent_bridge.errors import BridgeError


class DiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.directory = ModelDirectory(self.root)
        self.profile = Profile(
            "test",
            "codex",
            "fixed-model",
            provider="openai-codex",
            executable=sys.executable,
            verified=True,
            verified_at="2026-10-22",
            context_window=999,
            effort="low",
        )
        self.rows = [model_record("fixed-model"), model_record("new-model", input_types=["text", "image"])]

    async def test_codex_pagination_uses_metadata_only_and_closes_process(self):
        executable = self.root / "fake-codex"
        shutil.copyfile(Path(__file__).parent / "fixtures" / "fake_codex.py", executable)
        executable.chmod(0o700)
        profile = replace(self.profile, executable=str(executable))
        result = await self.directory.get(profile)
        self.assertIsNone(result["error"])
        self.assertEqual(["fixed-model", "new-model"], [m["model"] for m in result["models"]])
        self.assertTrue(result["models"][0]["verified"])
        self.assertFalse(result["models"][1]["verified"])
        self.assertFalse((self.root / "invocations.txt").exists())
        await asyncio.sleep(0)
        self.assertFalse([t for t in asyncio.all_tasks() if "JsonProcess" in repr(t.get_coro())])

    async def test_cache_refresh_failure_and_model_policy_preservation(self):
        with patch("agent_bridge.discovery.native_models", new_callable=AsyncMock) as probe:
            probe.return_value = self.rows, "test-directory"
            first = await self.directory.get(self.profile)
            self.assertEqual(first, await self.directory.get(self.profile))
            probe.assert_awaited_once()
            new = await self.directory.resolve(self.profile, "new-model")
            self.assertEqual("new-model", new.model)
            self.assertEqual(self.profile.provider, new.provider)
            self.assertEqual(self.profile.quota_group, new.quota_group)
            self.assertEqual(self.profile.tools, new.tools)
            self.assertIsNone(new.effort)
            self.assertEqual(["text"], new.input_types)
            self.assertFalse(new.verified)
            self.assertIsNone(new.verified_at)
            self.assertIsNone(new.context_window)
            self.assertIsNone(new.price)
            probe.side_effect = RuntimeError("private-token-must-not-leak")
            failed = await self.directory.get(self.profile, refresh=True)
            self.assertTrue(failed["stale"])
            self.assertEqual(first["models"], failed["models"])
            self.assertNotIn("private-token", json.dumps(failed))
            with self.assertRaisesRegex(BridgeError, "refresh succeeds"):
                await self.directory.resolve(self.profile, "new-model")
            self.assertEqual(self.profile, await self.directory.resolve(self.profile, "fixed-model"))
            probe.side_effect = None
            self.assertIsNone((await self.directory.get(self.profile, refresh=True))["error"])

    async def test_model_id_provider_configuration_and_settings_are_not_substituted(self):
        rows = [model_record("different", reasoning_efforts=["high"], service_tiers=[])]
        with patch("agent_bridge.discovery.native_models", new_callable=AsyncMock, return_value=(rows, "test")):
            for model in ("missing", "--bad", " "):
                with self.assertRaises(BridgeError):
                    await self.directory.resolve(self.profile, model)
            with self.assertRaisesRegex(BridgeError, "requested reasoning effort"):
                await self.directory.resolve(self.profile, "different", "low")
            self.assertIsNone((await self.directory.resolve(self.profile, "different")).effort)
            tier = replace(self.profile, effort=None, service_tier="fast")
            with self.assertRaisesRegex(BridgeError, "service tier"):
                await self.directory.resolve(tier, "different")
            valid = replace(tier, service_tier="default")
            self.assertEqual("different", (await self.directory.resolve(valid, "different")).model)
        changed = replace(self.profile, provider="other")
        with patch("agent_bridge.discovery.native_models", new_callable=AsyncMock, side_effect=RuntimeError):
            result = await self.directory.get(changed)
            self.assertFalse(result["models"])
            self.assertFalse(result["stale"])

    async def test_explicit_effort_is_checked_even_when_using_the_configured_model(self):
        profile = replace(self.profile, price={
            "currency": "USD",
            "source": "fixture",
            "as_of": "2026-11-22",
            "per_million": {"input_tokens": 1, "output_tokens": 2},
        })
        rows = [model_record(
            profile.model, reasoning_efforts=["low", "high"], default_reasoning_effort="low",
        )]
        with patch("agent_bridge.discovery.native_models", new_callable=AsyncMock, return_value=(rows, "test")) as probe:
            self.assertEqual(profile, await self.directory.resolve(profile, None))
            probe.assert_not_awaited()
            self.assertEqual(profile, await self.directory.resolve(profile, None, "low"))
            self.assertEqual(
                replace(profile, effort="high", verified=False, verified_at=None),
                await self.directory.resolve(profile, None, "high"),
            )
            with self.assertRaisesRegex(BridgeError, "requested reasoning effort"):
                await self.directory.resolve(profile, None, "max")
            with self.assertRaisesRegex(BridgeError, "without whitespace"):
                await self.directory.resolve(profile, None, " high")
            probe.return_value = self.rows, "test"
            await self.directory.get(profile, refresh=True)
            with self.assertRaisesRegex(BridgeError, "does not report"):
                await self.directory.resolve(profile, None, "high")
            probe.side_effect = BridgeError("discovery_failed", "fixture")
            await self.directory.get(profile, refresh=True)
            with self.assertRaisesRegex(BridgeError, "directory refresh succeeds"):
                await self.directory.resolve(profile, None, "high")
            self.assertEqual(profile, await self.directory.resolve(profile, None))

    async def test_agy_tiers_are_discovered_and_do_not_inherit_the_previous_model_effort(self):
        fixture = self.root / "agy-models"
        fixture.write_text(
            "#!/bin/sh\n"
            "test \"$1\" = models || exit 2\n"
            "printf 'gemini-4-neon-xhigh\\tNeon (XHigh)\\nfixture-max\\tFixture Max\\nopaque-model\\n'\n"
        )
        fixture.chmod(0o700)
        base = replace(
            self.profile, runtime="antigravity", provider="google-antigravity",
            model="claude-sonnet-5-5-medium", effort="medium", executable=str(fixture),
        )
        record = (await self.directory.get(base))["models"]
        self.assertEqual([["xhigh"], ["max"], None], [m["reasoning_efforts"] for m in record])
        self.assertEqual(["xhigh", "max", None], [m["default_reasoning_effort"] for m in record])
        self.assertIsNone((await self.directory.resolve(base, "gemini-4-neon-xhigh")).effort)
        self.assertEqual("xhigh", (await self.directory.resolve(base, "gemini-4-neon-xhigh", "xhigh")).effort)
        self.assertEqual("max", (await self.directory.resolve(base, "fixture-max", "max")).effort)
        with self.assertRaisesRegex(BridgeError, "requested reasoning effort"):
            await self.directory.resolve(base, "gemini-4-neon-xhigh", "medium")
        with self.assertRaisesRegex(BridgeError, "does not report"):
            await self.directory.resolve(base, "opaque-model", "high")
        with self.assertRaisesRegex(BridgeError, "match the tier"):
            replace(base, model="gemini-4-neon-xhigh", effort="max").validate()

    async def test_concurrent_refreshes_share_one_query(self):
        async def probe(*_):
            await asyncio.sleep(0.02)
            return self.rows, "test"

        with patch("agent_bridge.discovery.native_models", new_callable=AsyncMock, side_effect=probe) as query:
            results = await asyncio.gather(*(self.directory.get(self.profile, True) for _ in range(3)))
            query.assert_awaited_once()
            self.assertEqual(results[0], results[2])

    async def test_pi_does_not_add_other_providers_or_publish_native_credentials(self):
        profile = replace(self.profile, runtime="pi", provider="deepseek")
        client = Mock(
            start=AsyncMock(),
            close=AsyncMock(),
            request=AsyncMock(
                return_value={
                    "models": [
                        {
                            "id": "deepseek-new",
                            "provider": "deepseek",
                            "headers": {"Authorization": "private-key"},
                            "input": ["text", "image"],
                            "contextWindow": 1000,
                            "reasoning": True,
                            "thinkingLevelMap": {"minimal": None, "low": "low", "medium": None, "xhigh": None, "max": "max"},
                        },
                        {"id": "plain", "provider": "deepseek", "reasoning": False},
                        {"id": "old", "provider": "deepseek", "reasoning": True},
                        {"id": "other", "provider": "other"},
                    ]
                }
            ),
        )
        with patch("agent_bridge.discovery.JsonProcess", return_value=client) as process:
            rows, _ = await native_models(profile, self.root)
        self.assertEqual(["deepseek-new", "plain", "old"], [m["model"] for m in rows])
        self.assertEqual([["off", "low", "high", "max"], ["off"], None], [m["reasoning_efforts"] for m in rows])
        self.assertNotIn("private-key", json.dumps(rows))
        self.assertIn("--no-session", process.call_args.args[0])
        client.close.assert_awaited_once()

    async def test_claude_uses_resolved_ids_and_only_supported_endpoints(self):
        profile = replace(self.profile, runtime="claude", provider="anthropic")
        client = Mock(
            connect=AsyncMock(),
            disconnect=AsyncMock(),
            get_server_info=AsyncMock(
                return_value={
                    "models": [{"value": "alias", "resolvedModel": "exact-model"}, {"value": "unresolved"}],
                    "account": {"secret": "private-account"},
                }
            ),
        )
        with (
            patch.dict("os.environ", {"ANTHROPIC_BASE_URL": "https://api.anthropic.com"}),
            patch("claude_agent_sdk.ClaudeSDKClient", return_value=client),
        ):
            rows, _ = await native_models(profile, self.root)
        self.assertEqual(["exact-model"], [m["model"] for m in rows])
        self.assertNotIn("private-account", json.dumps(rows))
        client.disconnect.assert_awaited_once()
        with patch.dict("os.environ", {"ANTHROPIC_BASE_URL": "https://other.invalid"}), self.assertRaises(BridgeError):
            await native_models(replace(profile, endpoint_host="other.invalid"), self.root)

    async def test_native_pagination_cycle_fails_and_closes(self):
        async def request(value):
            return {"data": [{"model": "duplicate"}], "nextCursor": "again"} if value["method"] == "model/list" else {}

        client = Mock(start=AsyncMock(), write=AsyncMock(), close=AsyncMock(), request=AsyncMock(side_effect=request))
        with (
            patch("agent_bridge.discovery.JsonProcess", return_value=client),
            self.assertRaisesRegex(BridgeError, "pagination"),
        ):
            await native_models(self.profile, self.root)
        client.close.assert_awaited_once()

    def test_deepseek_filters_anthropic_support_and_never_follows_redirects(self):
        response = Mock()
        response.read.return_value = json.dumps(
            {
                "data": [
                    {"id": "exact-model", "context_window": 1000, "api_capabilities": {"anthropic_messages": {}}},
                    {"id": "other-protocol"},
                ]
            }
        ).encode()
        opener = Mock()
        opener.open.return_value.__enter__ = Mock(return_value=response)
        opener.open.return_value.__exit__ = Mock(return_value=False)
        with patch("agent_bridge.discovery.build_opener", return_value=opener):
            rows = deepseek_models({"ANTHROPIC_API_KEY": "private-key"})
            req = opener.open.call_args.args[0]
            self.assertEqual("https://api.deepseek.com/models", req.full_url)
            self.assertEqual("Bearer private-key", req.get_header("Authorization"))
            self.assertEqual(["exact-model"], [m["model"] for m in rows])
            self.assertIsNone(NoRedirect().redirect_request(req, None, 302, "", {}, "https://other.invalid"))
            opener.open.side_effect = HTTPError(req.full_url, 401, "private-key", {}, None)
            with self.assertRaisesRegex(BridgeError, "HTTP 401") as error:
                deepseek_models({"ANTHROPIC_API_KEY": "private-key"})
            self.assertNotIn("private-key", str(error.exception))


class DiscoveryBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_pins_discovered_model_retries_and_rejects_session_model_change(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / "config.json").write_text(
                json.dumps(
                    {
                        "profiles": {
                            "base": {
                                "runtime": "codex",
                                "model": "old",
                                "provider": "openai-codex",
                                "executable": sys.executable,
                            }
                        },
                        "telegram_database": None,
                    }
                )
            )
            broker = Broker(state)
            rows = [model_record("discovered", context_window=1000)]
            args = {
                "profile": "base",
                "model": "discovered",
                "cwd": directory,
                "request_id": "once",
                "instruction": "small task",
                "requirements": {"input_tokens": 100},
            }
            with patch("agent_bridge.discovery.native_models", new_callable=AsyncMock, return_value=(rows, "test")):
                catalog = await broker.call("agent_catalog", {})
                self.assertEqual("discovered", catalog["discovery"][0]["models"][0]["model"])
                first = await broker.api_call("agent_dispatch", args)
                self.assertIn("result", first)
                task = broker.store.get_task(first["result"]["task_id"])
                session = broker.store.get_session(task["session_id"])
                self.assertEqual("discovered", session["profile"]["model"])
                self.assertEqual("discovered", task["details"]["selection"]["model"])
                broker.config.profiles = {}
                self.assertEqual(first, await broker.api_call("agent_dispatch", args))
                with self.assertRaisesRegex(BridgeError, "change its model"):
                    await broker.call(
                        "agent_dispatch", {**args, "session_id": session["id"], "model": "old", "request_id": "change"}
                    )
                self.assertEqual(1, len(broker.store.sessions()))
