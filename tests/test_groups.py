import asyncio
import json
import os
import sys
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from agent_bridge import groups
from agent_bridge.broker import Broker, request
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.store import Store


class GroupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.profile = Profile("fixture", "codex", "fixed", executable=sys.executable)
        self.config = Config({"fixture": self.profile})
        self.sessions = [self.session(name) for name in ("one", "two", "outside")]

    def session(self, name, parent=None):
        sid = self.store.open(self.profile, str(self.root), "read-only", "open-" + name, parent)["session_id"]
        self.store.worker_ready(sid, "g", 123, "fixture", "native-" + sid)
        return sid

    def create(self, name="team", members=None):
        return groups.control_group(self.store, name, "create", 0, "create-" + name,
                                    self.sessions[:2] if members is None else members)

    def tasks(self):
        with self.store.connection() as db:
            return [dict(row) for row in db.execute("SELECT id,session_id,instruction FROM tasks ORDER BY rowid")]

    def test_group_membership_management_and_deleted_id_cannot_be_reused(self):
        first, second, outside = self.sessions
        group = self.create(members=[first, first])
        self.assertEqual([first], [m["session_id"] for m in group["members"]])
        self.assertEqual(group, groups.control_group(self.store, "team", "create", 0, "create-team", [first]))
        self.assertEqual(0, groups.control_group(self.store, "team", "add", 0, "duplicate-member", [first])["revision"])
        added = groups.control_group(self.store, "team", "add", 0, "add", [second])
        self.assertEqual(1, added["revision"])
        with self.assertRaisesRegex(BridgeError, "membership changed"):
            groups.control_group(self.store, "team", "remove", 0, "stale-remove", [first])
        with self.assertRaisesRegex(BridgeError, "sessions: missing"):
            groups.control_group(self.store, "team", "add", 1, "bad-member", [outside, "missing"])
        inspected = groups.inspect_groups(self.store, "team")
        self.assertEqual(added, {key: inspected[key] for key in added})
        removed = groups.control_group(self.store, "team", "remove", 1, "remove", [first])
        self.assertEqual([second], [m["session_id"] for m in removed["members"]])
        self.assertEqual(2, removed["revision"])
        deleted = groups.control_group(self.store, "team", "delete", 2, "delete")
        self.assertTrue(deleted["deleted"])
        self.assertEqual([second], [m["session_id"] for m in deleted["members"]])
        self.assertEqual([], groups.inspect_groups(self.store)["groups"])
        inspected = groups.inspect_groups(self.store, "team")
        self.assertEqual(deleted, {key: inspected[key] for key in deleted})
        with self.assertRaisesRegex(BridgeError, "already exists or was deleted"):
            groups.control_group(self.store, "team", "create", 0, "recreate")
        with self.assertRaisesRegex(BridgeError, "group was deleted"):
            groups.broadcast(self.store, "team", "message", 3, "deleted-send")

    def test_broadcast_only_reaches_explicit_members_and_retries_keep_original_snapshot(self):
        first, second, outside = self.sessions
        self.create()
        self.create("elsewhere", [second, outside])
        result = groups.broadcast(self.store, "team", "only this group", 0, "send", 10)
        self.assertEqual({first, second}, {r["session_id"] for r in result["recipients"]})
        self.assertEqual((2, 0), (result["queued_count"], result["skipped_count"]))
        self.assertEqual({first, second}, {t["session_id"] for t in self.tasks()})
        group = groups.control_group(self.store, "team", "add", 0, "add-outside", [outside])
        group = groups.control_group(self.store, "team", "remove", group["revision"], "remove-second", [second])
        self.assertEqual(result, groups.broadcast(Store(self.root), "team", "only this group", 0, "send", 10))
        self.assertEqual(2, len(self.tasks()))
        with self.assertRaisesRegex(BridgeError, "membership changed"):
            groups.broadcast(self.store, "team", "new message", 0, "stale-send")
        next_send = groups.broadcast(self.store, "team", "new message", group["revision"], "new-send")
        self.assertEqual({first, outside}, {r["session_id"] for r in next_send["recipients"]})
        for recipient in result["recipients"]:
            task = self.store.get_task(recipient["task_id"])
            self.assertEqual("only this group", task["instruction"])
            self.assertEqual(10, task["reservation"])
            self.assertEqual(result["broadcast_id"], task["details"]["broadcast_id"])
            self.assertFalse(task["details"]["learn_from_result"])

    def test_concurrent_duplicates_create_one_task_per_member(self):
        self.create()
        with ThreadPoolExecutor(8) as pool:
            results = list(pool.map(lambda _: groups.broadcast(self.store, "team", "same", 0, "same-id"), range(16)))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(2, len(self.tasks()))
        for group_id, message in (("team", "changed"), ("other", "same")):
            with self.assertRaisesRegex(BridgeError, "different content"):
                groups.broadcast(self.store, group_id, message, 0, "same-id")

    def test_missing_empty_unknown_or_invalid_group_never_means_all_sessions(self):
        self.create("empty", [])
        for name in (None, "", " ", "unknown", "*", "empty", [], "x" * 129):
            with self.subTest(group=name), self.assertRaises(BridgeError):
                groups.broadcast(self.store, name, "must not send", 0, "invalid")
        self.assertEqual([], self.tasks())
        for members in ("not-a-list", [None], [""]):
            with self.assertRaises(BridgeError):
                groups.control_group(self.store, "bad", "create", 0, "bad", members)
        with self.assertRaises(BridgeError):
            groups.control_group(self.store, "missing-member", "create", 0, "missing-member", ["not-a-session"])
        self.assertEqual(["empty"], [g["group_id"] for g in groups.inspect_groups(self.store)["groups"]])

    def test_paused_draining_closed_and_recovery_members_are_skipped_without_changing_them(self):
        active = self.sessions[0]
        skipped = [self.session(name) for name in ("paused", "draining", "closed", "recovery")]
        with self.store.connection(write=True) as db:
            for sid, desired in zip(skipped[:3], ("paused", "draining", "closed"), strict=True):
                db.execute("UPDATE sessions SET desired=? WHERE id=?", (desired, sid))
            db.execute("UPDATE sessions SET observed='recovery_needed' WHERE id=?", (skipped[3],))
        self.create(members=[active, *skipped])
        before = [self.store.get_session(sid) for sid in skipped]
        result = groups.broadcast(self.store, "team", "message", 0, "mixed")
        self.assertEqual((1, 4), (result["queued_count"], result["skipped_count"]))
        self.assertEqual([active], [t["session_id"] for t in self.tasks()])
        self.assertEqual(before, [self.store.get_session(sid) for sid in skipped])
        self.assertTrue(all("reason" in r for r in result["recipients"] if r["status"] == "skipped"))
        self.create("skipped", skipped)
        all_skipped = groups.broadcast(self.store, "skipped", "message", 0, "all-skipped")
        self.assertEqual((0, 4), (all_skipped["queued_count"], all_skipped["skipped_count"]))

    def test_write_failure_rolls_back_all_tasks_and_the_retry_receipt(self):
        self.create()
        enqueue = self.store.enqueue_task
        calls = 0

        def fail_second(*args):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("disk full")
            return enqueue(*args)

        with patch.object(self.store, "enqueue_task", side_effect=fail_second), self.assertRaisesRegex(OSError, "disk full"):
            groups.broadcast(self.store, "team", "message", 0, "send")
        self.assertEqual([], self.tasks())
        with self.store.connection() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM requests WHERE id='send'").fetchone())
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM events WHERE kind IN ('task_queued','group_broadcast')").fetchone()[0])
        self.assertEqual(2, groups.broadcast(self.store, "team", "message", 0, "send")["queued_count"])

    def test_message_reservation_and_revision_are_checked_before_any_enqueue(self):
        self.create()
        for message, reservation, revision in (("", 0, 0), ("x" * 200001, 0, 0), ("ok", -1, 0),
                                                ("ok", True, 0), ("ok", 1, True), ("ok", 1, None)):
            with self.subTest(message_length=len(message), reservation=reservation, revision=revision), self.assertRaises(BridgeError):
                groups.broadcast(self.store, "team", message, revision, "bad", reservation)
        with self.assertRaisesRegex(BridgeError, "positive token reservation"):
            groups.broadcast(self.store, "team", "message", 0, "unfunded", token_budget=100)
        self.assertEqual([], self.tasks())
        result = groups.broadcast(self.store, "team", "funded", 0, "funded", 60, token_budget=100)
        self.config.token_budget = 100
        first = self.store.claim(self.config)
        self.assertIn(first["task_id"], {r["task_id"] for r in result["recipients"]})
        self.assertIsNone(self.store.claim(self.config))

    def test_failures_are_not_replayed_and_parent_notifications_keep_original_recipients(self):
        self.store.parent_control("parent", "parent_register", 0, "parent", "native-parent", str(self.root))
        first, second = [self.session(name, "parent") for name in ("child1", "child2")]
        self.create(members=[first, second])
        result = groups.broadcast(self.store, "team", "message", 0, "send")
        for status in ("failed", "completed"):
            claimed = self.store.claim(self.config)
            self.store.begin_attempt(claimed["session_id"], "g")
            self.store.finish(claimed["session_id"], "g", claimed["task_id"], claimed["attempt_id"], status, {"answer": status})
        self.assertEqual(result, groups.broadcast(self.store, "team", "message", 0, "send"))
        self.assertEqual(2, len(self.tasks()))
        self.assertEqual(2, len(self.store.get_parent("parent")["deliveries"]))
        self.assertEqual({"failed", "completed"}, {self.store.get_task(r["task_id"])["status"] for r in result["recipients"]})
        groups.control_group(self.store, "team", "delete", 0, "delete")
        self.assertEqual(result, groups.broadcast(self.store, "team", "message", 0, "send"))

    def test_schema_upgrade_preserves_existing_tasks_and_parent_notifications(self):
        self.store.parent_control("parent", "parent_register", 0, "register", "native-parent", str(self.root))
        sid = self.session("child", "parent")
        task = self.store.dispatch(sid, "original task", "dispatch")["task_id"]
        attempt = self.store.claim(self.config)["attempt_id"]
        self.store.begin_attempt(sid, "g")
        self.store.finish(sid, "g", task, attempt, "completed", {"answer": "original answer"})
        original_task = self.store.get_task(task)
        original_parent = self.store.get_parent("parent")
        with self.store.connection(write=True) as db:
            db.execute("DROP TABLE session_group_members")
            db.execute("DROP TABLE session_groups")
        upgraded = Store(self.root)
        self.assertEqual(original_task, upgraded.get_task(task))
        self.assertEqual(original_parent, upgraded.get_parent("parent"))
        self.assertEqual({"groups": []}, groups.inspect_groups(upgraded))


class GroupApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        (self.state / "config.json").write_text(json.dumps({
            "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
            "worker_launcher": "detached", "telegram_database": None,
        }))
        self.broker = Broker(self.state)

        async def connection(reader, writer):
            await self.broker.connection(reader, writer)

        self.server = await asyncio.start_unix_server(connection, str(self.state / "broker.sock"))
        self.addAsyncCleanup(self.close_server)
        self.sessions = [self.broker.store.open(self.broker.config.profiles["fixture"], str(self.state), "read-only", "open-" + str(i))["session_id"] for i in range(3)]
        await request(self.state, "agent_group", {"group_id": "team", "action": "create", "expected_revision": 0,
                                               "request_id": "create", "session_ids": self.sessions[:2]})

    async def close_server(self):
        self.server.close()
        await self.server.wait_closed()

    async def test_socket_retries_and_broker_restart_recover_committed_fanout(self):
        args = {"group_id": "team", "message": "one per member", "expected_revision": 0, "request_id": "send"}
        results = await asyncio.gather(*(request(self.state, "agent_broadcast", args) for _ in range(8)))
        self.assertTrue(all(result == results[0] for result in results))
        with self.broker.store.connection(write=True) as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])
            # Model a crash after the fanout committed but before the API reply was saved.
            db.execute("UPDATE api_requests SET response=NULL WHERE id='send'")
        self.broker = Broker(self.state)
        with patch("agent_bridge.groups.broadcast", side_effect=AssertionError("must recover original receipt")):
            self.assertEqual(results[0], await request(self.state, "agent_broadcast", args))
        with self.assertRaisesRegex(BridgeError, "different API parameters"):
            await request(self.state, "agent_broadcast", {**args, "message": "changed"})
        with self.broker.store.connection() as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM tasks WHERE session_id=?", (self.sessions[2],)).fetchone()[0])

    async def test_bad_requests_return_stable_errors_and_cannot_fall_back_to_all(self):
        for index, args in enumerate(({}, {"group_id": ""}, {"group_id": "unknown"}, {"group_id": "team", "expected_revision": 4})):
            payload = {"message": "message", "expected_revision": 0, "request_id": "bad-" + str(index), **args}
            errors = []
            for _ in range(2):
                with self.assertRaises(BridgeError) as context:
                    await request(self.state, "agent_broadcast", payload)
                errors.append(context.exception.as_dict())
            self.assertEqual(errors[0], errors[1])
        with self.broker.store.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])

    async def test_child_and_wrong_token_cannot_broadcast_or_change_groups(self):
        for method, args in (("agent_group", {"group_id": "team", "action": "delete", "expected_revision": 0, "request_id": "delete"}),
                             ("agent_broadcast", {"group_id": "team", "message": "message", "expected_revision": 0, "request_id": "send"})):
            with patch.dict(os.environ, {"AGENT_BRIDGE_ROLE": "child"}), self.assertRaisesRegex(BridgeError, "may not administer"):
                await request(self.state, method, args)
            with patch.dict(os.environ, {"AGENT_BRIDGE_TOKEN": "wrong"}), self.assertRaisesRegex(BridgeError, "token is invalid"):
                await request(self.state, method, args)

    async def test_actual_mcp_group_tools_and_cli_query_use_the_same_state(self):
        params = StdioServerParameters(command=sys.executable, args=["-m", "agent_bridge.cli", "--state", str(self.state), "mcp"])
        async with stdio_client(params) as (reader, writer), ClientSession(reader, writer) as session:
            await session.initialize()
            tools = {t.name: t for t in (await session.list_tools()).tools}
            self.assertTrue(tools["agent_groups"].annotations.readOnlyHint)
            self.assertIn("group_id", tools["agent_broadcast"].inputSchema["required"])
            invalid = await session.call_tool("agent_broadcast", {"message": "invalid", "expected_revision": 0, "request_id": "missing-group"})
            self.assertTrue(invalid.isError)
            inspected = await session.call_tool("agent_groups", {"group_id": "team"})
            self.assertFalse(inspected.isError)
            self.assertEqual(2, len(json.loads(inspected.content[0].text)["members"]))
            created = await session.call_tool("agent_group", {"group_id": "mcp-team", "action": "create", "expected_revision": 0,
                                                              "request_id": "mcp-create", "session_ids": [self.sessions[0]]})
            self.assertFalse(created.isError)
            args = {"group_id": "mcp-team", "message": "MCP message", "expected_revision": 0, "request_id": "mcp-send"}
            sent = await session.call_tool("agent_broadcast", args)
            self.assertFalse(sent.isError, sent)
            result = json.loads(sent.content[0].text)
            self.assertEqual([self.sessions[0]], [r["session_id"] for r in result["recipients"]])
            repeated = await session.call_tool("agent_broadcast", args)
            self.assertEqual(result, json.loads(repeated.content[0].text))
            natives = [{"runtime": runtime, "native_id": str(uuid.uuid4()), "cwd": str(self.state)}
                       for runtime in ("codex", "antigravity")]
            native_args = {"group_id": "native", "action": "create", "expected_revision": 0,
                           "request_id": "native-create", "native_members": natives}
            created = await session.call_tool("agent_group", native_args)
            self.assertFalse(created.isError, created)
            self.assertEqual(2, len(json.loads(created.content[0].text)["members"]))
            sent = await session.call_tool("agent_broadcast", {"group_id": "native", "message": "fixture only",
                                                              "expected_revision": 0, "request_id": "native-send"})
            native_result = json.loads(sent.content[0].text)
            status = await session.call_tool("agent_broadcast_status", {"broadcast_id": native_result["broadcast_id"]})
            self.assertFalse(status.isError, status)
            self.assertEqual({"pending": 2}, json.loads(status.content[0].text)["counts"])
            self.assertTrue(tools["agent_group_inbox"].annotations.readOnlyHint)
            inbox = await session.call_tool("agent_group_inbox", {"group_id": "native", "runtime": "antigravity",
                                                                  "native_id": natives[1]["native_id"]})
            self.assertFalse(inbox.isError, inbox)
            self.assertEqual(["fixture only"], [m["message"] for m in json.loads(inbox.content[0].text)["messages"]])
        process = await asyncio.create_subprocess_exec(sys.executable, "-m", "agent_bridge.cli", "--state", str(self.state),
            "call", "agent_groups", "--json", '{"group_id":"mcp-team"}', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        self.assertEqual(0, process.returncode, stderr.decode())
        self.assertEqual([self.sessions[0]], [m["session_id"] for m in json.loads(stdout)["members"]])
