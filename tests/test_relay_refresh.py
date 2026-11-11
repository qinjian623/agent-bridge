import asyncio
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agent_bridge.cli import _relay_release_changed
from agent_bridge.config import Config
from agent_bridge.discovery import ModelDirectory
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import PeerErrorCode, ok
from agent_bridge.peer_transport import DirectRelayServer
from agent_bridge.store import Store


def request(method='peer.catalog', params=None, request_id='request'):
    return {'v': 1, 'id': request_id, 'method': method, 'params': params or {}}


def frame(value):
    return (json.dumps(value) + '\n').encode()


class RelayRefreshTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.path = self.state / 'config.json'
        self.write_config('old-model')
        self.models = ModelDirectory(self.state)
        self.service = DirectPeerService(
            FileNodeIdentityStore(self.state), store=Store(self.state),
            config=Config.load(self.path), config_path=self.path, model_directory=self.models,
        )

    def write_config(self, model):
        self.path.write_text(json.dumps({
            'profiles': {'worker': {'runtime': 'codex', 'model': model}}, 'telegram_database': None,
        }))

    async def test_catalog_reloads_changed_profiles_and_discovers_with_refresh(self):
        native = AsyncMock(side_effect=[
            ([{'model': 'old-model'}], 'fixture'),
            ([{'model': 'new-model'}], 'fixture'),
            ([{'model': 'new-model'}], 'fixture'),
        ])
        with patch('agent_bridge.discovery.native_models', native):
            first = await self.service.handle(request())
            self.write_config('new-model')
            second = await self.service.handle(request())
            cached = await self.service.handle(request())
            refreshed = await self.service.handle(request(params={'profile': 'worker', 'refresh': True}))
        self.assertTrue(first['ok'], first)
        self.assertEqual('old-model', first['result']['profiles'][0]['model'])
        self.assertEqual('new-model', second['result']['profiles'][0]['model'])
        self.assertEqual(second['result']['entries'], second['result']['profiles'])
        self.assertEqual('new-model', refreshed['result']['discovery'][0]['models'][0]['model'])
        self.assertEqual(second['result']['discovery'], cached['result']['discovery'])
        self.assertEqual(3, native.await_count)
        self.assertFalse(refreshed['result']['stale'])

    async def test_removed_profile_is_not_returned(self):
        self.path.write_text(json.dumps({'profiles': {}, 'telegram_database': None}))
        result = await self.service.handle(request(params={'profile': 'worker'}))
        self.assertTrue(result['ok'], result)
        self.assertEqual([], result['result']['profiles'])
        self.assertEqual([], result['result']['discovery'])

    async def test_bad_config_never_reuses_old_config_for_writes_and_can_recover(self):
        for invalid in ('{', '{"profiles": []}'):
            with self.subTest(invalid=invalid):
                self.path.write_text(invalid)
                with patch.object(self.service, '_deliver') as deliver:
                    response = await self.service.handle(request('peer.deliver'))
                self.assertFalse(response['ok'])
                self.assertEqual(PeerErrorCode.HOST_OFFLINE, response['error']['code'])
                deliver.assert_not_called()
        self.write_config('recovered-model')
        with patch('agent_bridge.discovery.native_models', AsyncMock(return_value=(
            [{'model': 'recovered-model'}], 'fixture',
        ))):
            response = await self.service.handle(request())
        self.assertTrue(response['ok'], response)
        self.assertEqual('recovered-model', response['result']['profiles'][0]['model'])

    async def test_discovery_failure_is_reported_without_false_freshness(self):
        with patch('agent_bridge.discovery.native_models', AsyncMock(side_effect=OSError('fixture failure'))):
            response = await self.service.handle(request(params={'refresh': True}))
        self.assertTrue(response['ok'], response)
        self.assertTrue(response['result']['stale'])
        self.assertEqual('old-model', response['result']['entries'][0]['model'])
        self.assertIsNotNone(response['result']['discovery'][0]['error'])

    async def test_refresh_requires_boolean(self):
        response = await self.service.handle(request(params={'refresh': 'yes'}))
        self.assertFalse(response['ok'])
        self.assertEqual(PeerErrorCode.INVALID_PARAMS, response['error']['code'])

    async def test_deleted_loaded_config_refuses_writes(self):
        self.path.unlink()
        with patch.object(self.service, '_deliver') as deliver:
            response = await self.service.handle(request('peer.deliver'))
        self.assertFalse(response['ok'])
        self.assertEqual(PeerErrorCode.HOST_OFFLINE, response['error']['code'])
        deliver.assert_not_called()

    async def test_unconfigured_node_allows_handshake_but_not_writes(self):
        self.path.unlink()
        service = DirectPeerService(
            FileNodeIdentityStore(self.state), store=Store(self.state), config_path=self.path,
        )
        hello = await service.handle(request('peer.hello', {'protocol_versions': [1]}))
        inspect = await service.handle(request('peer.inspect', {'kind': 'node'}))
        with patch.object(service, '_deliver') as deliver:
            response = await service.handle(request('peer.deliver'))
        self.assertTrue(hello['ok'], hello)
        self.assertTrue(inspect['ok'], inspect)
        self.assertFalse(response['ok'])
        self.assertEqual(PeerErrorCode.HOST_OFFLINE, response['error']['code'])
        deliver.assert_not_called()


class RelayRetirementTests(unittest.IsolatedAsyncioTestCase):
    async def test_release_change_before_frame_refuses_without_executing(self):
        service = AsyncMock()
        server = DirectRelayServer(service, retire=lambda: True)
        stdout = io.BytesIO()
        await server.serve(io.BytesIO(frame(request()) + frame(request(request_id='second'))), stdout)
        [response] = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertFalse(response['ok'])
        self.assertEqual('request', response['id'])
        self.assertEqual(PeerErrorCode.HOST_OFFLINE, response['error']['code'])
        self.assertIn('no request was executed', response['error']['message'])
        service.handle.assert_not_awaited()

    async def test_release_change_during_frame_delivers_response_before_exit(self):
        changed = False
        started, finish = asyncio.Event(), asyncio.Event()

        async def handle(value):
            started.set()
            await finish.wait()
            return ok(value['id'], {'committed': True})

        service = AsyncMock()
        service.handle.side_effect = handle
        server = DirectRelayServer(service, retire=lambda: changed)
        stdout = io.BytesIO()
        task = asyncio.create_task(server.serve(io.BytesIO(
            frame(request('peer.deliver')) + frame(request('peer.deliver', request_id='second')),
        ), stdout))
        await asyncio.wait_for(started.wait(), 1)
        changed = True
        self.assertFalse(task.done())
        finish.set()
        await asyncio.wait_for(task, 1)
        [response] = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual({'committed': True}, response['result'])
        self.assertEqual(1, service.handle.await_count)

    async def test_new_connection_uses_changed_release_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            root = state / 'managed'
            old, new = root / 'releases' / 'old', root / 'releases' / 'new'
            old.mkdir(parents=True, mode=0o700)
            new.mkdir(mode=0o700)
            root.chmod(0o700)
            (root / 'current').symlink_to(old)
            settings = {
                'repository': 'example/bridge', 'root': str(root), 'service_kind': 'systemd',
                'service_name': 'bridge', 'bootstrap_python': sys.executable,
            }
            path = state / 'update.json'
            path.write_text(json.dumps(settings))
            path.chmod(0o600)
            old_marker = _relay_release_changed(state)
            self.assertFalse(old_marker())
            (root / 'current').unlink()
            (root / 'current').symlink_to(new)
            self.assertTrue(old_marker())
            self.assertFalse(_relay_release_changed(state)())
            with patch('agent_bridge.cli.sys.prefix', str(old)):
                self.assertTrue(_relay_release_changed(state)())

    async def test_unmanaged_checkout_does_not_require_updater(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(_relay_release_changed(Path(directory))())
