import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from agent_bridge.broker import Broker
from agent_bridge.errors import BridgeError
from agent_bridge.worker import Worker


class AntigravityWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_broker_worker_usage_and_workspace_result_with_offline_cli(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / 'state'
            state.mkdir()
            fixture = Path(__file__).parent / 'fixtures/fake_agy.py'
            (state / 'config.json').write_text(json.dumps(dict(
                profiles={'agy': dict(runtime='antigravity', provider='google-antigravity',
                                      model='fixed-model', executable=str(fixture.resolve()))},
                worker_launcher='detached', telegram_database=None)))
            broker = Broker(state)
            with self.assertRaisesRegex(BridgeError, 'workspace-write'):
                await broker.call('agent_open', dict(profile='agy', cwd=folder, request_id='readonly'))
            self.assertEqual([], broker.store.sessions())
            opened = await broker.call('agent_open', dict(profile='agy', cwd=folder, request_id='open',
                access='workspace-write', workspace_mode='copy', snapshot_files=[]))
            sid = opened['session_id']
            first = await broker.call('agent_dispatch', dict(session_id=sid, instruction='remember bridge-value', request_id='one'))
            broker.store.claim(broker.config)
            worker = Worker(state, broker.config, sid)
            running = asyncio.create_task(worker.run())

            async def finished(tid):
                async with asyncio.timeout(5):
                    while True:
                        broker.store.claim(broker.config)
                        task = broker.store.get_task(tid)
                        if task['status'] == 'completed':
                            return task
                        if task['status'] in {'failed', 'recovery_needed'}:
                            self.fail(str(task))
                        await asyncio.sleep(.01)

            try:
                task = await finished(first['task_id'])
                self.assertEqual('remembered', task['result']['answer'])
                self.assertIn('files', task['result'])
                second = await broker.call('agent_dispatch', dict(session_id=sid, instruction='recall', request_id='two'))
                broker.store.claim(broker.config)
                task = await finished(second['task_id'])
                self.assertEqual('bridge-value', task['result']['answer'])
                self.assertEqual('native-agy', broker.store.get_session(sid)['native_id'])
                self.assertEqual(220, broker.store.budget()['reported_tokens'])
            finally:
                worker.shutdown = True
                await asyncio.wait_for(running, 3)
            self.assertIsNone(worker.agent.client)
