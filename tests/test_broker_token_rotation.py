import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from agent_bridge import broker as broker_module
from agent_bridge import web_monitor
from agent_bridge.broker import Broker
from agent_bridge.config import MAX_TOKEN_BYTES, read_admin_token
from agent_bridge.errors import BridgeError


def write_token(state: Path, value: str, mode: int = 0o600) -> None:
    """Replace admin.token atomically, like a real rotation."""
    temporary = state / "admin.token.new"
    temporary.write_text(value)
    temporary.chmod(mode)
    os.replace(temporary, state / "admin.token")


class BrokerTokenRotationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        (self.state / "config.json").write_text(json.dumps({"profiles": {}}))
        self.broker = Broker(self.state)
        self.server = await asyncio.start_unix_server(
            self.broker.connection, str(self.state / "broker.sock"), limit=2 * 1024 * 1024
        )
        self.addAsyncCleanup(self._close)
        env = patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("AGENT_BRIDGE_TOKEN", None)
        os.environ.pop("AGENT_BRIDGE_ROLE", None)

    async def _close(self):
        self.server.close()
        await self.server.wait_closed()

    async def raw(self, token):
        reader, writer = await asyncio.open_unix_connection(str(self.state / "broker.sock"))
        writer.write((json.dumps({"version": 1, "token": token, "method": "agent_hosts", "params": {}}) + "\n").encode())
        await writer.drain()
        reply = json.loads(await reader.readline())
        writer.close()
        await writer.wait_closed()
        return reply

    async def assertForbidden(self, token):
        reply = await self.raw(token)
        self.assertEqual(reply.get("error", {}).get("code"), "forbidden", reply)

    async def assertAccepted(self, token):
        reply = await self.raw(token)
        self.assertNotIn("error", reply, reply)

    async def test_init_creates_secure_token_and_property_reads_it(self):
        path = self.state / "admin.token"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.broker.token, read_admin_token(self.state))
        await self.assertAccepted(self.broker.token)
        await self.assertForbidden("wrong")

    async def test_call_uses_current_file_token(self):
        result = await broker_module.request(self.state, "agent_hosts", {})
        self.assertIsNotNone(result)
        write_token(self.state, "rotated-via-call")
        self.assertIsNotNone(await broker_module.request(self.state, "agent_hosts", {}))

    async def test_rotation_rejects_old_and_accepts_new_without_restart(self):
        old = self.broker.token
        await self.assertAccepted(old)
        write_token(self.state, "new-token-123\n")
        self.assertEqual(self.broker.token, "new-token-123")
        await self.assertForbidden(old)
        await self.assertAccepted("new-token-123")
        write_token(self.state, "third-token")
        await self.assertForbidden("new-token-123")
        await self.assertAccepted("third-token")

    async def test_permission_tampering_rejected_and_recovers(self):
        token = self.broker.token
        (self.state / "admin.token").chmod(0o644)
        await self.assertForbidden(token)
        with self.assertRaises(BridgeError) as caught:
            _ = self.broker.token
        self.assertEqual(caught.exception.code, "forbidden")
        (self.state / "admin.token").chmod(0o600)
        await self.assertAccepted(token)

    async def test_missing_empty_malformed_oversized_and_symlink_rejected(self):
        token = self.broker.token
        path = self.state / "admin.token"
        path.unlink()
        await self.assertForbidden(token)
        await self.assertForbidden("")
        write_token(self.state, "")
        await self.assertForbidden(token)
        await self.assertForbidden("")
        write_token(self.state, "has space inside")
        await self.assertForbidden("has space inside")
        write_token(self.state, "x" * (MAX_TOKEN_BYTES + 1))
        await self.assertForbidden("x" * (MAX_TOKEN_BYTES + 1))
        real = self.state / "real.token"
        real.write_text(token)
        real.chmod(0o600)
        path.unlink()
        path.symlink_to(real)
        await self.assertForbidden(token)

    async def test_call_fails_closed_for_missing_token_file(self):
        (self.state / "admin.token").unlink()
        with self.assertRaises(BridgeError):
            await broker_module.request(self.state, "agent_hosts", {})

    async def test_env_token_still_takes_precedence_in_call(self):
        os.environ["AGENT_BRIDGE_TOKEN"] = "stale-env-token"
        with self.assertRaises(BridgeError) as caught:
            await broker_module.request(self.state, "agent_hosts", {})
        self.assertEqual(caught.exception.code, "forbidden")

    async def test_web_monitor_and_broker_rotate_in_lockstep(self):
        handler = SimpleNamespace(state=self.state)

        def forward(token):
            return web_monitor.WebMonitorHandler._broker_call(handler, token, {"action": "list"})

        old = read_admin_token(self.state)
        self.assertNotEqual((await asyncio.to_thread(forward, old)).get("error", {}).get("code"), "forbidden")
        write_token(self.state, "lockstep-new-token")
        new = read_admin_token(self.state)  # what the Web Monitor reads on its next request
        self.assertEqual(new, "lockstep-new-token")
        reply = await asyncio.to_thread(forward, old)
        self.assertEqual(reply["error"]["code"], "forbidden")
        reply = await asyncio.to_thread(forward, new)
        self.assertNotEqual(reply.get("error", {}).get("code"), "forbidden", reply)

    def test_web_monitor_reexports_shared_reader(self):
        self.assertIs(web_monitor.read_admin_token, read_admin_token)
        self.assertEqual(web_monitor.MAX_TOKEN_BYTES, MAX_TOKEN_BYTES)
        self.assertEqual(MAX_TOKEN_BYTES, 8192)


if __name__ == "__main__":
    unittest.main()
