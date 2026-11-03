import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from agent_bridge import codex_queue, groups, native_broadcast
from agent_bridge.broker import Broker
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.store import Store


class NativeGroupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.config = Config({})
        self.members = [dict(runtime="codex", native_id=str(uuid.uuid4()), cwd=str(self.root), label=str(i))
                        for i in range(4)]
        self.members[3]["runtime"] = "antigravity"
        self.outside = dict(runtime="codex", native_id=str(uuid.uuid4()), cwd=str(self.root))

    def create(self):
        return groups.control_group(self.store, "nu", "create", 0, "create", native_members=self.members)

    def broadcast(self, request_id="send"):
        return groups.broadcast(self.store, "nu", "group message", 0, request_id)

    def rows(self):
        with self.store.connection() as db:
            return [dict(r) for r in db.execute("SELECT * FROM native_broadcast_deliveries ORDER BY created")]

    def skip_agy_fixture_delivery(self):
        # These tests exercise only the fake Codex helper, never the installed AGY.
        with self.store.connection(write=True) as db:
            db.execute("UPDATE native_broadcast_deliveries SET status='skipped' WHERE runtime='antigravity'")

    def test_register_original_ids_without_bridge_sessions_and_manage_members(self):
        group = self.create()
        self.assertEqual({m["native_id"] for m in self.members}, {m["native_id"] for m in group["members"]})
        self.assertEqual({"codex_queue", "antigravity_resume"}, {m["transport"] for m in group["members"]})
        self.assertEqual(4, groups.inspect_groups(self.store)["groups"][0]["member_count"])
        self.assertEqual(group, groups.control_group(self.store, "nu", "add", 0, "same", native_members=self.members))
        with self.store.connection() as db:
            for table in ("sessions", "tasks", "parents", "deliveries"):
                self.assertEqual(0, db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        removed = groups.control_group(self.store, "nu", "remove", 0, "remove", native_members=[
            {k: self.members[0][k] for k in ("runtime", "native_id")},
        ])
        self.assertEqual((1, 3), (removed["revision"], len(removed["members"])))
        deleted = groups.control_group(self.store, "nu", "delete", 1, "delete")
        self.assertEqual([], deleted["members"])
        self.assertEqual([], groups.inspect_groups(self.store)["groups"])

    def test_duplicate_requests_keep_snapshot_and_outside_never_gets_a_delivery(self):
        self.create()
        groups.control_group(self.store, "other", "create", 0, "other", native_members=[self.outside])
        with ThreadPoolExecutor(8) as pool:
            results = list(pool.map(lambda _: self.broadcast(), range(16)))
        result = results[0]
        self.assertTrue(all(r == result for r in results))
        self.assertEqual((0, 4, 0), (result["queued_count"], result["pending_count"], result["skipped_count"]))
        self.assertEqual(4, len(self.rows()))
        self.assertNotIn(self.outside["native_id"], {r["native_id"] for r in self.rows()})
        groups.control_group(self.store, "nu", "add", 0, "add", native_members=[self.outside])
        self.assertEqual(result, self.broadcast())
        with self.assertRaisesRegex(BridgeError, "different content"):
            groups.broadcast(self.store, "nu", "changed", 0, "send")
        groups.control_group(self.store, "nu", "delete", 1, "delete")
        self.assertEqual(result, self.broadcast())
        self.assertEqual(4, len(self.rows()))

    def test_invalid_native_members_and_conflicting_metadata_are_atomic(self):
        self.create()
        for bad in ("not-a-list", [None], [{"runtime": "codex", "native_id": "*"}],
                    [{**self.outside, "runtime": "unknown"}], [{**self.outside, "cwd": "relative"}],
                    [{**self.outside, "cwd": str(self.root / "missing")}],
                    [{**self.outside, "label": "x" * 129}], [{**self.outside, "new_process": True}]):
            with self.subTest(bad=bad), self.assertRaises(BridgeError):
                groups.control_group(self.store, "nu", "add", 0, "invalid", native_members=bad)
        with self.assertRaisesRegex(BridgeError, "Conflicting details"):
            groups.control_group(self.store, "nu", "add", 0, "bad-duplicate", native_members=[
                self.outside, {**self.outside, "label": "changed"},
            ])
        with self.assertRaisesRegex(BridgeError, "Remove the old member"):
            groups.control_group(self.store, "nu", "add", 0, "bad-change", native_members=[
                self.outside, {**self.members[0], "label": "changed"},
            ])
        self.assertEqual(4, len(groups.inspect_groups(self.store, "nu")["members"]))

    def test_cannot_register_native_already_owned_by_bridge_or_bypass_budget(self):
        profile = Profile("test", "codex", "fixed", executable=sys.executable)
        sid = self.store.open(profile, str(self.root), "read-only", "open")["session_id"]
        self.store.worker_ready(sid, "g", 1, "fixture", self.members[0]["native_id"])
        with self.assertRaisesRegex(BridgeError, "existing bridge session_id"):
            self.create()
        with self.store.connection(write=True) as db:
            db.execute("UPDATE sessions SET native_id=NULL WHERE id=?", (sid,))
        self.create()
        with self.assertRaisesRegex(BridgeError, "outside bridge token accounting"):
            groups.broadcast(self.store, "nu", "message", 0, "budget", 10, token_budget=100)
        self.assertEqual([], self.rows())

    def test_failure_during_native_fanout_rolls_back_all_deliveries_and_retry_record(self):
        self.create()
        with patch("agent_bridge.groups.identifier", side_effect=["broadcast", "delivery", RuntimeError("insert failed")]):
            with self.assertRaisesRegex(RuntimeError, "insert failed"):
                self.broadcast()
        self.assertEqual([], self.rows())
        with self.store.connection() as db:
            self.assertIsNone(db.execute("SELECT * FROM requests WHERE id='send'").fetchone())
        self.assertEqual(4, self.broadcast()["pending_count"])

    def test_inbox_only_returns_original_recipient_and_group_without_changing_receipts(self):
        self.create()
        member = self.members[3]
        groups.control_group(self.store, "elsewhere", "create", 0, "elsewhere", native_members=[member, self.outside])
        first = self.broadcast()
        groups.broadcast(self.store, "elsewhere", "another group", 0, "other-send")
        before = self.rows()
        result = native_broadcast.inbox(self.store, "nu", member["runtime"], member["native_id"])
        self.assertEqual(["group message"], [m["message"] for m in result["messages"]])
        self.assertEqual(first["broadcast_id"], result["messages"][0]["broadcast_id"])
        self.assertEqual("pending", result["messages"][0]["delivery_status"])
        self.assertFalse(result["receipt_changed"])
        self.assertEqual(result, native_broadcast.inbox(self.store, "nu", member["runtime"], member["native_id"]))
        self.assertEqual(before, self.rows())
        with self.assertRaisesRegex(BridgeError, "not a member"):
            native_broadcast.inbox(self.store, "nu", "codex", self.outside["native_id"])

    def test_inbox_cursor_no_duplicates_and_rejects_removed_members_or_invalid_queries(self):
        self.create()
        member = self.members[3]
        for i in range(3):
            groups.broadcast(self.store, "nu", str(i), 0, "send-" + str(i))
        cursor, seen = 0, []
        for _ in range(3):
            page = native_broadcast.inbox(self.store, "nu", member["runtime"], member["native_id"], cursor, 1)
            seen += [m["message"] for m in page["messages"]]
            cursor = page["cursor"]
        self.assertEqual(["0", "1", "2"], seen)
        self.assertFalse(page["has_more"])
        self.assertEqual([], native_broadcast.inbox(self.store, "nu", member["runtime"], member["native_id"], cursor)["messages"])
        for after, limit in [(-1, 1), (False, 1), (0, 0), (0, 11), ("0", 1)]:
            with self.assertRaises(BridgeError):
                native_broadcast.inbox(self.store, "nu", member["runtime"], member["native_id"], after, limit)
        groups.control_group(self.store, "nu", "remove", 0, "remove", native_members=[member])
        with self.assertRaisesRegex(BridgeError, "not a member"):
            native_broadcast.inbox(self.store, "nu", member["runtime"], member["native_id"])
        groups.control_group(self.store, "nu", "delete", 1, "delete")
        with self.assertRaisesRegex(BridgeError, "group was deleted"):
            native_broadcast.inbox(self.store, "nu", member["runtime"], member["native_id"])

    async def test_only_safe_presend_failures_retry_and_lost_ack_never_resends(self):
        self.create()
        result = self.broadcast()
        calls = {}
        modes = {r["native_id"]: i for i, r in enumerate(self.rows())}

        async def sender(config, row):
            native_id = row["native_id"]
            calls[native_id] = calls.get(native_id, 0) + 1
            mode = modes[native_id]
            if mode == 0:
                raise ConnectionError("native enqueue may have succeeded; response lost")
            if mode == 1 and calls[native_id] == 1:
                return {"status": "deferred", "error": "client setup failed before enqueue"}
            return {"status": "enqueued", "receipt": "receipt-" + native_id}

        await asyncio.gather(*(native_broadcast.deliver(self.store, self.config, sender) for _ in range(3)))
        await native_broadcast.deliver(self.store, self.config, sender)
        current = native_broadcast.view(self.store, result["broadcast_id"])
        self.assertEqual({"uncertain": 1, "enqueued": 3}, current["counts"])
        for native_id, mode in modes.items():
            self.assertEqual(2 if mode == 1 else 1, calls[native_id])
        self.assertNotIn(self.outside["native_id"], calls)
        self.assertIn(self.members[3]["native_id"], calls)
        self.assertEqual(result, self.broadcast())

    async def test_spawn_failure_is_pending_but_invalid_ack_is_uncertain(self):
        self.create()
        self.broadcast()
        self.skip_agy_fixture_delivery()
        self.config.codex_async_python = str(self.root / "missing-python")
        await native_broadcast.deliver(self.store, self.config)
        self.assertEqual(3, sum(r["status"] == "pending" for r in self.rows()))

        async def malformed(config, row):
            return {"status": "enqueued"}

        await native_broadcast.deliver(self.store, self.config, malformed)
        self.assertEqual(3, sum(r["status"] == "uncertain" for r in self.rows()))

    async def test_status_refresh_reads_receipts_without_resending(self):
        self.create()
        result = self.broadcast()
        with self.store.connection(write=True) as db:
            db.execute("UPDATE native_broadcast_deliveries SET status='uncertain' WHERE runtime='codex'")
        statuses = dict(zip(sorted(m["native_id"] for m in self.members[:3]), ["received", "enqueued", "unconfirmed"], strict=True))

        async def reader(config, row, *, inspect=False):
            self.assertTrue(inspect)
            return {"status": statuses[row["native_id"]], "receipt": "original-receipt", "turn_id": "matched-turn"}

        current = await native_broadcast.inspect(self.store, self.config, result["broadcast_id"], refresh=True, sender=reader)
        self.assertEqual({"received": 1, "enqueued": 1, "uncertain": 1, "pending": 1}, current["counts"])
        self.assertEqual(result, self.broadcast())

    async def test_real_helper_process_uses_existing_daemon_and_never_starts_a_session(self):
        self.create()
        self.broadcast()
        self.skip_agy_fixture_delivery()
        module = self.root / "codex_async"
        module.mkdir()
        (module / "__init__.py").touch()
        calls = self.root / "calls.jsonl"
        (module / "app_server.py").write_text('''
import json
from pathlib import Path
class CodexAppServer:
    def __init__(self, cwd, *, timeout, sandbox, daemon, start_daemon):
        assert daemon is True and start_daemon is False and sandbox is None
        self.cwd = str(cwd)
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def request(self, method, params):
        assert method == "thread/read", method
        return {"thread": {"id": params["threadId"], "cwd": self.cwd, "status": {"type": "notLoaded"}}}
    def enqueue(self, native_id, message, client_id):
        with (Path(self.cwd) / "calls.jsonl").open("a") as f:
            f.write(json.dumps({"native_id": native_id, "client_id": client_id}) + "\\n")
        return "receipt-" + native_id
''')
        self.config.codex_async_python = sys.executable
        with patch.dict(os.environ, {"PYTHONPATH": str(self.root)}):
            await native_broadcast.deliver(self.store, self.config)
            await native_broadcast.deliver(self.store, self.config)
        lines = [json.loads(line) for line in calls.read_text().splitlines()]
        self.assertEqual(3, len(lines))
        self.assertEqual({m["native_id"] for m in self.members[:3]}, {r["native_id"] for r in lines})
        self.assertEqual(3, len({r["client_id"] for r in lines}))
        self.assertEqual(3, sum(r["status"] == "enqueued" for r in self.rows()))

    async def test_native_delivery_preserves_telegram_pause_and_removal(self):
        self.create()
        self.broadcast()
        self.skip_agy_fixture_delivery()
        guard = self.root / "telegram.sqlite"
        with sqlite3.connect(guard) as db:
            db.executescript("CREATE TABLE topics(thread_id TEXT,topic_id INTEGER);"
                             "CREATE TABLE removed_topics(thread_id TEXT,topic_id INTEGER);"
                             "CREATE TABLE inbox(thread_id TEXT,topic_id INTEGER,status TEXT);")
            db.execute("INSERT INTO inbox(thread_id,status) VALUES(?,'stopped')", (self.members[0]["native_id"],))
            db.execute("INSERT INTO removed_topics(thread_id) VALUES(?)", (self.members[1]["native_id"],))
            db.execute("INSERT INTO inbox(thread_id,status) VALUES(?,'uncertain')", (self.members[2]["native_id"],))
        self.config.telegram_database = str(guard)
        self.config.codex_async_python = sys.executable
        await native_broadcast.deliver(self.store, self.config)
        errors = {r["native_id"]: r["error"] for r in self.rows()}
        self.assertIn("paused", errors[self.members[0]["native_id"]])
        self.assertIn("closed", errors[self.members[1]["native_id"]])
        self.assertIn("paused", errors[self.members[2]["native_id"]])
        self.assertEqual(3, sum(r["status"] == "pending" for r in self.rows()))

    async def test_restart_recovers_api_snapshot_but_never_repeats_inflight_send(self):
        (self.root / "config.json").write_text(json.dumps({"profiles": {}, "telegram_database": None}))
        self.create()
        args = dict(group_id="nu", message="group message", expected_revision=0, request_id="send")
        broker = Broker(self.root)
        original = await broker.api_call("agent_broadcast", args)
        with self.store.connection(write=True) as db:
            db.execute("UPDATE api_requests SET response=NULL WHERE id='send'")
            db.execute("UPDATE native_broadcast_deliveries SET status='sending'")
        restarted = Broker(self.root)
        self.assertEqual(original, await restarted.api_call("agent_broadcast", args))

        async def forbidden(*args):
            self.fail("uncertain native deliveries must not be sent again")

        await native_broadcast.deliver(self.store, self.config, forbidden)
        current = await restarted.call("agent_broadcast_status", {"broadcast_id": original["result"]["broadcast_id"]})
        self.assertEqual({"uncertain": 4}, current["counts"])


class QueueClient:
    def __init__(self, job, pages=None):
        self.job, self.pages, self.calls = job, list(pages or []), []

    def request(self, method, params):
        self.calls.append((method, params.copy()))
        if method == "thread/read":
            return {"thread": {"id": self.job["native_id"], "cwd": self.job["cwd"], "status": {"type": "notLoaded"}}}
        if method not in {"thread/queue/list", "thread/turns/list"}:
            raise AssertionError("Unexpected native mutation: " + method)
        return self.pages.pop(0)

    def enqueue(self, thread, message, message_id):
        self.calls.append(("thread/queue/add", {"threadId": thread, "message": message, "clientUserMessageId": message_id}))
        return "receipt"


class NativeQueueHelperTests(unittest.TestCase):
    def setUp(self):
        self.job = {"id": "delivery", "runtime": "codex", "native_id": str(uuid.uuid4()), "cwd": "/tmp", "message": "test"}

    def test_native_notloaded_never_resumes_and_adds_only_to_original_queue(self):
        client = QueueClient(self.job)
        self.assertEqual({"status": "enqueued", "receipt": "receipt"}, codex_queue.advance(client, self.job))
        self.assertEqual(["thread/read", "thread/queue/add"], [m for m, _ in client.calls])
        self.assertEqual(self.job["native_id"], client.calls[-1][1]["threadId"])

    def test_wrong_cwd_fails_before_enqueue(self):
        client = QueueClient({**self.job, "cwd": "/"})
        self.assertEqual("failed", codex_queue.advance(client, self.job)["status"])
        self.assertEqual(["thread/read"], [m for m, _ in client.calls])

    def test_receipt_pagination_and_native_client_id_prove_receipt(self):
        client = QueueClient(self.job, [
            {"data": [{"id": "other", "clientUserMessageId": "other"}], "nextCursor": "next"},
            {"data": [{"id": "receipt", "clientUserMessageId": codex_queue.client_id("delivery")}], "nextCursor": None},
        ])
        self.assertEqual({"status": "enqueued", "receipt": "receipt"}, codex_queue.inspect_receipt(client, self.job))
        self.assertEqual("next", client.calls[-1][1]["cursor"])
        client = QueueClient(self.job, [
            {"data": [], "nextCursor": None},
            {"data": [{"id": "turn", "items": [{"type": "userMessage", "clientId": codex_queue.client_id("delivery")}]}]},
        ])
        result = codex_queue.inspect_receipt(client, self.job)
        self.assertEqual(("received", "turn"), (result["status"], result["turn_id"]))

    def test_disappeared_queue_item_and_similar_text_are_not_receipt(self):
        client = QueueClient(self.job, [
            {"data": [], "nextCursor": None},
            {"data": [{"id": "turn", "items": [{"type": "userMessage", "clientId": "other", "text": "test"}]}]},
        ])
        self.assertEqual("unconfirmed", codex_queue.inspect_receipt(client, self.job)["status"])

    def test_queue_pagination_loop_is_not_mistaken_for_delivery(self):
        client = QueueClient(self.job, [{"data": [], "nextCursor": "loop"}] * 2)
        with self.assertRaisesRegex(ValueError, "repeated a pagination cursor"):
            codex_queue.inspect_receipt(client, self.job)


if __name__ == "__main__":
    unittest.main()
