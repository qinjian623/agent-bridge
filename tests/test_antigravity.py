import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_bridge.antigravity import AntigravityRuntime, turn_usage
from agent_bridge.config import Config, Profile
from agent_bridge.discovery import native_models
from agent_bridge.errors import BridgeError
from agent_bridge.quota import provider_limit
from agent_bridge.routing import choose
from agent_bridge.runtimes import runtime
from agent_bridge.store import Store
from agent_bridge.usage import priced_usage

FIXTURE = Path(__file__).parent / 'fixtures/fake_agy.py'


class AntigravityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.profile = Profile('agy', 'antigravity', 'fixed-model', provider='google-antigravity',
                               executable=str(FIXTURE), verified=True)
        self.agent = runtime(self.profile, {'cwd': str(self.root), 'access': 'workspace-write'}, self.root, self.no_question)
        self.events = []

    async def asyncTearDown(self):
        await self.agent.close()
        self.temp.cleanup()

    async def no_question(self, *_):
        self.fail('Antigravity must not send Claude SDK approval messages')

    async def started(self, *_):
        self.started_count += 1

    async def event(self, kind, data):
        self.events.append((kind, data))

    async def ask(self, instruction):
        self.started_count = 0
        answer = await asyncio.wait_for(self.agent.run(instruction, self.started, self.event), 3)
        self.assertEqual(1, self.started_count)
        return answer

    async def test_multiple_turns_resume_and_no_historical_usage_charged(self):
        await self.agent.connect()
        self.assertEqual('native-agy', self.agent.native_id)
        status, _, usage = await self.ask('remember orange')
        self.assertEqual('completed', status)
        self.assertEqual(110, usage['total_tokens'])
        self.assertTrue(usage['complete'])
        _, result, usage = await self.ask('recall')
        self.assertEqual('orange', result['answer'])
        self.assertEqual(110, usage['total_tokens'])
        native = self.agent.native_id
        await self.agent.close()
        self.agent = AntigravityRuntime(self.profile, {'cwd': str(self.root), 'access': 'workspace-write',
            'native_id': native, 'has_native_history': True}, self.root, self.no_question)
        await self.agent.connect()
        _, result, usage = await self.ask('recall')
        self.assertEqual('orange', result['answer'])
        self.assertEqual(110, usage['total_tokens'])  # Observed new steps, not cumulative 330.
        self.assertFalse(usage['complete'])
        _, _, usage = await self.ask('recall')
        self.assertEqual(110, usage['total_tokens'])
        self.assertFalse(usage['complete'])

    async def test_interrupt_waits_for_native_result_then_reconnects(self):
        await self.agent.connect()
        await self.ask('remember resume-marker')
        self.started_count = 0
        running = asyncio.create_task(self.agent.run('hold', self.started, self.event))
        for _ in range(100):
            if self.started_count:
                break
            await asyncio.sleep(.01)
        self.assertEqual(1, self.started_count)
        process = self.agent.client.process
        await self.agent.interrupt()
        status, _, _ = await asyncio.wait_for(running, 3)
        self.assertEqual('interrupted', status)
        self.assertIsNotNone(process.returncode)
        _, result, _ = await self.ask('recall')
        self.assertEqual('resume-marker', result['answer'])

    async def test_tools_are_deduplicated_and_permission_denials_fail_success_results(self):
        await self.agent.connect()
        status, _, _ = await self.ask('tool')
        self.assertEqual('completed', status)
        self.assertEqual(1, sum(k == 'tool_started' for k, _ in self.events))
        self.assertEqual(1, sum(k == 'tool_completed' for k, _ in self.events))
        for instruction in ('permission', 'stderr-denial', 'late-stderr-denial'):
            status, result, _ = await self.ask(instruction)
            self.assertEqual('failed', status)
            self.assertTrue(result['permission_denied'])

    async def test_extra_result_cannot_complete_current_task(self):
        await self.agent.connect()
        await self.ask('remember recent')
        with self.assertRaises(BridgeError):
            await self.ask('stale')

    async def test_nonzero_exit_overrides_native_success(self):
        await self.agent.connect()
        status, result, _ = await self.ask('bad-exit')
        self.assertEqual('failed', status)
        self.assertIn('nonzero', result['error'])

    async def test_stream_eof_malformed_and_wrong_identity_are_not_success(self):
        for instruction in ('no-result', 'bad-json', 'wrong-session', 'waiting'):
            with self.subTest(instruction=instruction):
                await self.agent.close()
                self.agent = AntigravityRuntime(self.profile, {'cwd': str(self.root), 'access': 'workspace-write'},
                                               self.root, self.no_question)
                await self.agent.connect()
                with self.assertRaises(BridgeError):
                    await self.ask(instruction)

    async def test_missing_reset_counters_stay_partial_and_thinking_is_not_added(self):
        await self.agent.connect()
        await self.ask('remember counters')
        for instruction in ('reset', 'missing-usage'):
            _, _, usage = await self.ask(instruction)
            self.assertEqual(110, usage['total_tokens'])
            self.assertEqual(2, usage['reasoning_tokens'])
            self.assertFalse(usage['complete'])
            self.assertFalse(usage['pricing_supported'])
        self.assertIsNone(turn_usage(None, None, {})['total_tokens'])
        estimate = priced_usage(usage, {'currency': 'USD', 'source': 'fixture', 'as_of': '2026-10-27',
                                        'per_million': {'input_tokens': 1, 'output_tokens': 2}})
        self.assertNotIn('catalog_estimate', estimate)
        self.assertIsNone(estimate['actual_billed_usd'])

    async def test_quota_is_reported_and_native_errors_are_not_retried(self):
        await self.agent.connect()
        status, result, _ = await self.ask('quota')
        self.assertEqual('failed', status)
        self.assertEqual(12, result['provider_limit']['retry_after_seconds'])
        self.assertIsNotNone(provider_limit('RESOURCE_EXHAUSTED'))

    async def test_quota_rejection_before_first_user_turn_is_confirmed_failure(self):
        await self.agent.connect()
        status, result, usage = await self.ask('quota-zero')
        self.assertEqual('failed', status)
        self.assertEqual(12, result['provider_limit']['retry_after_seconds'])
        self.assertEqual(0, usage['total_tokens'])

    async def test_model_confirmation_readonly_and_service_tier_fail_before_inference(self):
        self.agent = AntigravityRuntime(Profile('agy', 'antigravity', 'wrong-init', provider='google-antigravity',
            executable=str(FIXTURE)), {'cwd': str(self.root), 'access': 'workspace-write'}, self.root, self.no_question)
        with self.assertRaisesRegex(BridgeError, 'exact requested model'):
            await self.agent.connect()
        self.assertIsNone(self.agent.client)
        self.agent.session['access'] = 'read-only'
        with self.assertRaisesRegex(BridgeError, 'read-only'):
            await self.agent.connect()
        for changes in ({'provider': 'anthropic'}, {'service_tier': 'fast'}, {'input_types': ['image']},
                        {'model': 12}, {'effort': []}, {'endpoint_host': 'api.anthropic.com'},
                        {'model': 'claude-opus-5-5-high', 'effort': 'low'}):
            from dataclasses import replace
            with self.assertRaises(BridgeError):
                replace(self.profile, **changes).validate()

    async def test_directory_factory_command_and_readonly_routing(self):
        rows, source = await native_models(self.profile, self.root)
        self.assertEqual(['fixed-model', 'claude-opus-5-5-high'], [r['model'] for r in rows])
        self.assertEqual(['high'], rows[1]['reasoning_efforts'])
        self.assertEqual('Antigravity CLI agy models', source)
        self.assertIsInstance(self.agent, AntigravityRuntime)
        with patch('agent_bridge.config.shutil.which', return_value='/fake/agy') as which:
            Profile('agy', 'antigravity', 'fixed-model', provider='google-antigravity').command()
            which.assert_called_once_with('agy')
        store = Store(self.root / 'state')
        with self.assertRaisesRegex(BridgeError, 'read-only'):
            choose(Config(profiles={'agy': self.profile}), store, {}, access='read-only')
