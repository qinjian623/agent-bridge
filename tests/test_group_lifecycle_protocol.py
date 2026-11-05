from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import PeerErrorCode, normalize_group_prune_params, parse_older_than
from agent_bridge.store import Store


class ParseOlderThanTests(unittest.TestCase):
    def test_durations(self):
        self.assertEqual(parse_older_than("7d"), 7 * 86400)
        self.assertEqual(parse_older_than("12H"), 12 * 3600)
        self.assertEqual(parse_older_than("2w"), 2 * 604800)
        self.assertEqual(parse_older_than("100s"), 100)
        self.assertEqual(parse_older_than("90"), 90)
        self.assertEqual(parse_older_than("1.5h"), 5400)
        self.assertEqual(parse_older_than(60), 60)
        self.assertEqual(parse_older_than(0.5), 0.5)

    def test_invalid(self):
        for value in ("-5d", "abc", 0, -1, "0d", "", "5x", True, None):
            with self.subTest(value=value), self.assertRaises(BridgeError) as ctx:
                parse_older_than(value)  # type: ignore[arg-type]
            self.assertEqual(ctx.exception.code, "invalid_request")


class NormalizeParamsTests(unittest.TestCase):
    def test_defaults_and_values(self):
        self.assertEqual(
            normalize_group_prune_params({"older_than": "1d", "request_id": "r1"}),
            {"older_than": 86400.0, "dry_run": True, "request_id": "r1"},
        )
        self.assertFalse(normalize_group_prune_params({"older_than": 5, "dry_run": False, "request_id": "r"})["dry_run"])

    def test_invalid(self):
        for params in (
            {"request_id": "r"},
            {"older_than": "1d"},
            {"older_than": "1d", "request_id": ""},
            {"older_than": "1d", "request_id": 3},
            {"older_than": "1d", "request_id": "r", "dry_run": "yes"},
            {"older_than": "bad", "request_id": "r"},
        ):
            with self.subTest(params=params), self.assertRaises(BridgeError):
                normalize_group_prune_params(params)


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state = Path(self._tmp.name)
        self.store = Store(self.state)

    def add_group(self, group_id="g1", created=100.0, revision=0):
        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO session_groups(id,revision,deleted,created,updated_at,last_activity_at) VALUES(?,?,0,?,?,?)",
                (group_id, revision, created, created, created),
            )

    def group(self, group_id="g1"):
        with self.store.connection() as db:
            return dict(db.execute("SELECT * FROM session_groups WHERE id=?", (group_id,)).fetchone())


class SchemaMigrationTests(StoreTestCase):
    def test_fresh_schema(self):
        with self.store.connection() as db:
            columns = {row[1] for row in db.execute("PRAGMA table_info(session_groups)")}
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            indexes = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        self.assertTrue({"updated_at", "last_activity_at", "deleted_at", "delete_reason"} <= columns)
        self.assertTrue({"group_prune_runs", "group_prune_items"} <= tables)
        self.assertIn("session_groups_prune", indexes)

    def test_upgrade_from_old_schema(self):
        state = self.state / "old"
        state.mkdir()
        db = sqlite3.connect(state / "bridge.sqlite")
        db.executescript(
            """
            CREATE TABLE session_groups (
              id TEXT PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 0,
              deleted INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL
            );
            INSERT INTO session_groups(id,created) VALUES('old1',10.0),('old2',20.0);
            """
        )
        db.commit()
        db.close()
        store = Store(state)
        with store.connection() as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(session_groups)")}
            rows = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM session_groups")}
            indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        self.assertTrue({"updated_at", "last_activity_at", "deleted_at", "delete_reason"} <= columns)
        self.assertEqual(rows["old1"]["last_activity_at"], 10.0)
        self.assertEqual(rows["old1"]["updated_at"], 10.0)
        self.assertEqual(rows["old2"]["last_activity_at"], 20.0)
        self.assertIsNone(rows["old2"]["deleted_at"])
        self.assertIn("session_groups_prune", indexes)
        Store(state)  # idempotent re-open

    def test_upgrade_uses_latest_group_event(self):
        state = self.state / "old2"
        state.mkdir()
        db = sqlite3.connect(state / "bridge.sqlite")
        db.executescript(
            """
            CREATE TABLE session_groups (
              id TEXT PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 0,
              deleted INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL
            );
            INSERT INTO session_groups(id,created) VALUES('g',10.0);
            CREATE TABLE events (
              seq INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, task_id TEXT, attempt_id TEXT,
              kind TEXT NOT NULL, data TEXT NOT NULL, created REAL NOT NULL
            );
            """
        )
        db.execute("INSERT INTO events(kind,data,created) VALUES('group_changed',?,50.0)", (json.dumps({"group_id": "g"}),))
        db.commit()
        db.close()
        with Store(state).connection() as conn:
            self.assertEqual(conn.execute("SELECT last_activity_at FROM session_groups").fetchone()[0], 50.0)


class PruneRunTests(StoreTestCase):
    def test_run_lifecycle(self):
        run = self.store.create_group_prune_run("run1", "req1", "fp", 123.0, now=5.0)
        self.assertEqual((run["id"], run["status"], run["created"], run["finished"]), ("run1", "running", 5.0, None))
        again = self.store.create_group_prune_run("run2", "req1", "fp", 123.0, now=9.0)
        self.assertEqual(again["id"], "run1")
        with self.assertRaises(BridgeError) as ctx:
            self.store.create_group_prune_run("run3", "req1", "other", 123.0)
        self.assertEqual(ctx.exception.code, "request_conflict")
        self.assertEqual(self.store.get_group_prune_run("run1")["request_id"], "req1")
        self.assertIsNone(self.store.get_group_prune_run("nope"))
        self.assertEqual(self.store.get_group_prune_run_by_request_id("req1")["id"], "run1")
        self.assertIsNone(self.store.get_group_prune_run_by_request_id("nope"))

        self.add_group("g1")
        self.add_group("g2")
        self.store.record_group_prune_item("run1", "g2", 0, {"members": ["a"]}, "ok", "planned")
        self.store.record_group_prune_item("run1", "g1", 3, "[]", "blocked", "skipped", "busy")
        items = self.store.get_group_prune_items("run1")
        self.assertEqual([i["group_id"] for i in items], ["g1", "g2"])
        self.assertEqual(items[0]["reason"], "busy")
        self.assertEqual(json.loads(items[1]["member_snapshot"]), {"members": ["a"]})
        self.store.record_group_prune_item("run1", "g2", 0, "[]", "ok", "pruned")
        self.assertEqual(self.store.get_group_prune_items("run1")[1]["status"], "pruned")

        self.store.finish_group_prune_run("run1", now=77.0)
        done = self.store.get_group_prune_run("run1")
        self.assertEqual((done["status"], done["finished"]), ("completed", 77.0))


class PruneCasTests(StoreTestCase):
    def test_success(self):
        self.add_group(revision=2)
        row = self.store.prune_group_cas("g1", 2, "stale", now=500.0)
        self.assertEqual((row["deleted"], row["revision"], row["deleted_at"], row["delete_reason"]), (1, 3, 500.0, "stale"))
        self.assertEqual(self.group()["revision"], 3)

    def test_revision_conflict(self):
        self.add_group(revision=2)
        with self.assertRaises(BridgeError) as ctx:
            self.store.prune_group_cas("g1", 1)
        self.assertEqual(ctx.exception.code, PeerErrorCode.REVISION_CONFLICT)
        self.assertEqual(self.group()["deleted"], 0)

    def test_already_deleted(self):
        self.add_group()
        self.store.prune_group_cas("g1", 0)
        with self.assertRaises(BridgeError) as ctx:
            self.store.prune_group_cas("g1", 1)
        self.assertEqual(ctx.exception.code, "group_deleted")

    def test_missing(self):
        with self.assertRaises(BridgeError) as ctx:
            self.store.prune_group_cas("nope", 0)
        self.assertEqual(ctx.exception.code, "not_found")


class TouchActivityTests(StoreTestCase):
    def test_touch(self):
        self.add_group(created=100.0)
        self.store.touch_group_activity("g1", 200.0)
        g = self.group()
        self.assertEqual((g["last_activity_at"], g["updated_at"]), (200.0, 100.0))
        self.store.touch_group_activity("g1", 300.0, updated=True)
        g = self.group()
        self.assertEqual((g["last_activity_at"], g["updated_at"]), (300.0, 300.0))

    def test_touch_with_db(self):
        self.add_group()
        with self.store.connection(write=True) as db:
            self.store.touch_group_activity("g1", 400.0, db=db)
        self.assertEqual(self.group()["last_activity_at"], 400.0)


if __name__ == "__main__":
    unittest.main()
