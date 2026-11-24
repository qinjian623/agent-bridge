import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from agent_bridge import cli
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.store import SCHEMA, Store
from agent_bridge.worker import Worker


class WorkerStoreStartupTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state = Path(directory.name)
        self.profile = Profile("fixture", "codex", "fixed")
        self.config = Config({"fixture": self.profile})

    def test_broker_managed_worker_skips_database_initialization(self):
        broker_store = Store(self.state)
        session = broker_store.open(self.profile, str(self.state), "read-only", "open")["session_id"]
        with (
            broker_store.connection(write=True),
            patch.object(Store, "connection", side_effect=AssertionError("unexpected connection during worker init")),
        ):
            worker = Worker(self.state, self.config, session, database_initialized=True)
        self.assertEqual(worker.store.get_session(session)["id"], session)

    def test_standalone_worker_initializes_legacy_database(self):
        connection = sqlite3.connect(self.state / "bridge.sqlite")
        try:
            connection.executescript(SCHEMA)
            self.assertNotIn("worker_version", {row[1] for row in connection.execute("PRAGMA table_info(sessions)")})
        finally:
            connection.close()

        worker = Worker(self.state, self.config, "fixture")
        with worker.store.connection() as db:
            self.assertIn("worker_version", {row[1] for row in db.execute("PRAGMA table_info(sessions)")})

    def test_managed_worker_refuses_missing_database(self):
        with self.assertRaises(BridgeError) as raised:
            Worker(self.state, self.config, "fixture", database_initialized=True)
        self.assertEqual(raised.exception.code, "database_not_initialized")
        self.assertFalse((self.state / "bridge.sqlite").exists())

    def test_worker_command_passes_database_initialization_flag(self):
        Store(self.state)
        args = ["--state", str(self.state), "worker", "fixture", "--database-initialized"]
        with (
            patch.object(cli.os, "umask"),
            patch.object(Config, "load", return_value=self.config),
            patch.object(Store, "connection", side_effect=AssertionError("unexpected connection during worker init")),
            patch.object(Worker, "run", new_callable=AsyncMock) as run,
        ):
            cli.main(args)
        run.assert_awaited_once()

    def test_busy_registration_retries_before_connecting_to_native_agent(self):
        broker_store = Store(self.state)
        session = broker_store.open(self.profile, str(self.state), "read-only", "open")["session_id"]
        worker = Worker(self.state, self.config, session, database_initialized=True, release_changed=lambda: True)
        register = worker.store.worker_starting
        busy = sqlite3.OperationalError("database is locked")
        busy.sqlite_errorcode = sqlite3.SQLITE_BUSY | (1 << 8)
        agent = SimpleNamespace(native_id="fixture", connect=AsyncMock(), close=AsyncMock())

        def register_after_busy(*args, **kwargs):
            agent.connect.assert_not_awaited()
            if starts.call_count == 1:
                raise busy
            return register(*args, **kwargs)

        with (
            patch.object(worker.store, "worker_starting", side_effect=register_after_busy) as starts,
            patch("agent_bridge.worker.runtime", return_value=agent),
        ):
            asyncio.run(worker.run())
        self.assertEqual(2, starts.call_count)
        self.assertEqual(starts.call_args_list[0], starts.call_args_list[1])
        agent.connect.assert_awaited_once()
        self.assertEqual("stopped", broker_store.get_session(session)["observed"])
