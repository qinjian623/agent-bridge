import asyncio
import contextlib
import io
import json
import os
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from agent_bridge import groups
from agent_bridge.broker import Broker
from agent_bridge.cli import build_parser
from agent_bridge.config import Profile
from agent_bridge.errors import BridgeError
from agent_bridge.mcp_server import server as mcp_server
from agent_bridge.peer_protocol import PeerErrorCode
from agent_bridge.store import Store

REMOTE_BRIDGE = {
    "host_id": "gpu-a", "node_id": "node-a", "state_generation": "gen-a",
    "target_kind": "bridge", "session_id": "remote-session-a", "label": "remote",
}


class FakeClient:
    def __init__(self, result=None, error=None, hook=None):
        self.result, self.error, self.hook = result, error, hook
        self.calls = []

    def inspect(self, payload):
        self.calls.append(payload)
        if self.hook is not None:
            self.hook(payload)
        if self.error is not None:
            raise self.error
        return self.result


class AsyncClient(FakeClient):
    async def inspect(self, payload):
        self.calls.append(payload)
        if self.hook is not None:
            self.hook(payload)
        if self.error is not None:
            raise self.error
        return self.result


class FakeFactory:
    def __init__(self, client=None, error=None):
        self.client_value, self.error = client, error

    def client(self, host_id):
        if self.error is not None:
            raise self.error
        return self.client_value


class DirectFactory:
    """A factory object without a client() hook; _peer_client treats it as the client itself."""

    def __init__(self, client):
        self.inner = client

    def inspect(self, payload):
        return self.inner.inspect(payload)


class CallableFactory:
    def __init__(self, client=None, error=None):
        self.client_value, self.error = client, error

    def __call__(self, host_id):
        if self.error is not None:
            raise self.error
        return self.client_value


def remote_group_result(target_id, desired="closed", observed="stopped"):
    return {"items": [{"id": target_id, "native_id": target_id, "desired": desired, "observed": observed}]}


class GroupFixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.profile = Profile("fixture", "codex", "fixed", executable=sys.executable)
        (self.root / "config.json").write_text(json.dumps({"profiles": {}}))
        self._session_counter = 0
        self._work_session = None

    def open_session(self, desired="open", observed="stopped", native_id=None, process_scope=None,
                     worker_pid=None):
        self._session_counter += 1
        sid = self.store.open(
            self.profile, str(self.root), "read-only", f"open-{self._session_counter}",
        )["session_id"]
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE sessions SET desired=?,observed=?,native_id=?,process_scope=?,worker_pid=? WHERE id=?",
                (desired, observed, native_id, process_scope, worker_pid, sid),
            )
        return sid

    def create_group(self, group_id="team", session_ids=None, native_members=None, remote_members=None,
                     desired="closed"):
        if session_ids is None and native_members is None and remote_members is None:
            session_ids = [self.open_session(desired=desired)]
        return groups.control_group(
            self.store, group_id, "create", 0, f"create-{group_id}",
            session_ids, native_members, remote_members,
        )

    def backdate(self, group_id, age=2 * 86400):
        timestamp = time.time() - age
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE session_groups SET created=?,updated_at=?,last_activity_at=? WHERE id=?",
                (timestamp, timestamp, timestamp, group_id),
            )
        return timestamp

    def native_member(self, runtime="codex"):
        return {"runtime": runtime, "native_id": str(uuid.uuid4()), "cwd": str(self.root), "label": ""}

    def work_session(self):
        if self._work_session is None:
            self._work_session = self.open_session(desired="closed")
        return self._work_session

    def add_task(self, group_id, status="queued", task_id=None, validation="pending"):
        task_id = task_id or "task-" + uuid.uuid4().hex
        session_id = self.work_session()
        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,reservation,created,"
                "details) VALUES(?,?,?,?,?,?,?,?,?)",
                (task_id, session_id, "work", "[]", status, validation, 0, time.time(),
                 json.dumps({"group_id": group_id})),
            )
        return task_id

    def add_question(self, group_id, task_id=None):
        task_id = task_id or self.add_task(group_id, status="completed")
        question_id = "question-" + uuid.uuid4().hex
        session_id = self.work_session()
        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO questions(id,session_id,task_id,attempt_id,data,status,created) "
                "VALUES(?,?,?,?,?,?,?)",
                (question_id, session_id, task_id, "attempt", "{}", "pending", time.time()),
            )
        return question_id

    def add_recipient(self, group_id, status="pending", task_id=None, delivery_id=None):
        broadcast_id = "broadcast-" + uuid.uuid4().hex
        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO group_broadcasts(id,group_id,group_revision,sender_ref,message,"
                "recipient_snapshot,request_key,created) VALUES(?,?,?,?,?,?,?,?)",
                (broadcast_id, group_id, 0, None, "message", "[]", "key-" + uuid.uuid4().hex, time.time()),
            )
            db.execute(
                "INSERT INTO broadcast_recipients(broadcast_id,recipient_key,target_ref,delivery_id,task_id,"
                "status,updated) VALUES(?,?,?,?,?,?,?)",
                (broadcast_id, "recipient", "{}", delivery_id, task_id, status, time.time()),
            )
        return broadcast_id

    def add_peer_outbox(self, group_id, status="pending"):
        delivery_id, stream_id = "delivery-" + uuid.uuid4().hex, "stream-" + uuid.uuid4().hex
        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO target_streams(stream_id,authority_ref,target_ref,next_seq,released_seq) "
                "VALUES(?,?,?,?,?)",
                (stream_id, "{}", "{}", 2, 0),
            )
            db.execute(
                "INSERT INTO peer_outbox(delivery_id,broadcast_id,group_id,group_revision,stream_id,target_seq,"
                "host_id,node_id,state_generation,target_kind,runtime,target_id,operation_id,payload,"
                "fingerprint,seq_kind,status,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (delivery_id, None, group_id, 0, stream_id, 1, "host", "node", "gen", "bridge", "",
                 "target", "operation-" + uuid.uuid4().hex, "{}", "fingerprint", "message", status,
                 time.time(), time.time()),
            )
        return delivery_id

    def add_node_outbox(self, group_id, status="pending"):
        operation_id = "node-operation-" + uuid.uuid4().hex
        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO node_outbox(operation_id,group_id,method,payload,status,created) "
                "VALUES(?,?,?,?,?,?)",
                (operation_id, group_id, "peer.deliver", "{}", status, time.time()),
            )
        return operation_id

    def group(self, group_id):
        return groups.inspect_groups(self.store, group_id)

    @staticmethod
    def item(result, group_id):
        return next(item for item in result["items"] if item["group_id"] == group_id)


class GroupLifecycleTests(GroupFixture, unittest.TestCase):
    def test_create_add_remove_updates_revision_and_activity_timestamps(self):
        before = time.time()
        created = self.create_group()
        self.assertEqual(0, created["revision"])
        self.assertFalse(created["deleted"])
        self.assertIsNone(created["deleted_at"])
        self.assertIsNone(created["delete_reason"])
        self.assertGreaterEqual(created["created"], before)
        self.assertEqual(created["created"], created["updated_at"])
        self.assertEqual(created["created"], created["last_activity_at"])
        first = created["members"][0]["session_id"]

        second = self.open_session(desired="closed")
        added = groups.control_group(self.store, "team", "add", 0, "add-second", [second])
        self.assertEqual(1, added["revision"])
        self.assertEqual(2, len(added["members"]))
        self.assertGreaterEqual(added["updated_at"], created["updated_at"])
        self.assertGreaterEqual(added["last_activity_at"], created["last_activity_at"])

        before_duplicate = dict(added)
        duplicate = groups.control_group(self.store, "team", "add", 1, "duplicate-second", [second])
        self.assertEqual(1, duplicate["revision"])
        self.assertEqual(before_duplicate["updated_at"], duplicate["updated_at"])
        self.assertEqual(before_duplicate["last_activity_at"], duplicate["last_activity_at"])

        removed = groups.control_group(self.store, "team", "remove", 1, "remove-first", [first])
        self.assertEqual(2, removed["revision"])
        self.assertEqual([second], [member["session_id"] for member in removed["members"]])
        self.assertGreaterEqual(removed["updated_at"], added["updated_at"])
        self.assertGreaterEqual(removed["last_activity_at"], added["last_activity_at"])

        with self.assertRaisesRegex(BridgeError, "stale_revision|membership changed"):
            groups.control_group(self.store, "team", "remove", 1, "stale-remove", [second])
        with self.assertRaisesRegex(BridgeError, "already exists or was deleted"):
            groups.control_group(self.store, "team", "create", 0, "recreate")
        with self.assertRaisesRegex(BridgeError, "requires expected_revision=0"):
            groups.control_group(self.store, "fresh", "create", 1, "bad-create")

    def test_broadcast_advances_last_activity_without_changing_revision(self):
        created = self.create_group(desired="open")
        session_id = created["members"][0]["session_id"]
        groups.control_group(self.store, "team", "add", 0, "add-idle", [self.open_session(observed="idle")])
        before = self.group("team")
        time.sleep(0.01)
        groups.broadcast(self.store, "team", "message", 1, "send")
        after = self.group("team")
        self.assertEqual(before["revision"], after["revision"])
        self.assertGreater(after["last_activity_at"], before["last_activity_at"])
        self.assertIn(session_id, {member["session_id"] for member in after["members"]})

    def test_soft_delete_preserves_member_snapshot_and_rejects_reuse(self):
        native = self.native_member()
        group = self.create_group(
            session_ids=[self.open_session(desired="closed")],
            native_members=[native],
            remote_members=[REMOTE_BRIDGE],
        )
        deleted = groups.control_group(self.store, "team", "delete", 0, "delete-team")
        self.assertTrue(deleted["deleted"])
        self.assertIsNotNone(deleted["deleted_at"])
        self.assertEqual("deleted", deleted["delete_reason"])
        self.assertEqual(1, deleted["revision"])
        self.assertGreaterEqual(deleted["deleted_at"], group["updated_at"])
        self.assertEqual(2, len(deleted["members"]))
        self.assertEqual(1, len(deleted["remote_members"]))
        with self.store.connection() as db:
            self.assertEqual(
                1, db.execute("SELECT COUNT(*) FROM session_group_members WHERE group_id='team'").fetchone()[0],
            )
            self.assertEqual(
                1, db.execute("SELECT COUNT(*) FROM native_group_members WHERE group_id='team'").fetchone()[0],
            )
            self.assertEqual(
                1, db.execute("SELECT COUNT(*) FROM remote_group_members WHERE group_id='team'").fetchone()[0],
            )
        self.assertEqual([], groups.inspect_groups(self.store)["groups"])
        detail = self.group("team")
        self.assertEqual("deleted", detail["activity_state"])
        self.assertFalse(detail["active"])
        self.assertTrue(detail["deleted"])
        with self.assertRaisesRegex(BridgeError, "group was deleted"):
            groups.control_group(self.store, "team", "add", 1, "add-after-delete",
                                 [self.open_session(desired="closed")])
        with self.assertRaisesRegex(BridgeError, "group was deleted"):
            groups.broadcast(self.store, "team", "message", 1, "broadcast-after-delete")

    def test_delete_is_blocked_by_unsettled_tasks_and_questions(self):
        self.create_group()
        self.add_task("team", status="queued")
        with self.assertRaisesRegex(BridgeError, "unsettled work") as caught:
            groups.control_group(self.store, "team", "delete", 0, "delete-task")
        self.assertEqual(PeerErrorCode.GROUP_PRUNE_BLOCKED, caught.exception.code)
        self.assertFalse(self.group("team")["deleted"])
        self.assertEqual(0, self.group("team")["revision"])

        with self.store.connection(write=True) as db:
            db.execute("UPDATE tasks SET status='completed' WHERE details LIKE '%team%'")
        self.add_question("team")
        with self.assertRaisesRegex(BridgeError, "unsettled work"):
            groups.control_group(self.store, "team", "delete", 0, "delete-question")
        with self.store.connection(write=True) as db:
            db.execute("UPDATE questions SET status='answered'")
        self.assertTrue(groups.control_group(self.store, "team", "delete", 0, "delete-settled")["deleted"])

    def test_delete_is_blocked_by_broadcast_outbox_and_node_outbox(self):
        for group_id, blocker, settle in (
            ("recipient", lambda: self.add_recipient("recipient"), lambda: self._settle_recipient("recipient")),
            ("outbox", lambda: self.add_peer_outbox("outbox"), lambda: self._settle("peer_outbox", "outbox")),
            ("node", lambda: self.add_node_outbox("node"), lambda: self._settle("node_outbox", "node")),
        ):
            with self.subTest(group_id=group_id):
                self.create_group(group_id)
                blocker()
                with self.assertRaisesRegex(BridgeError, "unsettled work"):
                    groups.control_group(self.store, group_id, "delete", 0, f"delete-{group_id}")
                self.assertFalse(self.group(group_id)["deleted"])
                settle()
                self.assertTrue(
                    groups.control_group(self.store, group_id, "delete", 0, f"delete-settled-{group_id}")["deleted"],
                )

    def _settle_recipient(self, group_id):
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE broadcast_recipients SET status='skipped' WHERE broadcast_id IN "
                "(SELECT id FROM group_broadcasts WHERE group_id=?)",
                (group_id,),
            )

    def _settle(self, table, group_id):
        status = "released" if table == "peer_outbox" else "acked"
        with self.store.connection(write=True) as db:
            db.execute(f"UPDATE {table} SET status=? WHERE group_id=?", (status, group_id))

    def test_terminal_work_does_not_block_delete(self):
        self.create_group()
        task_id = self.add_task("team", status="completed", validation="accepted")
        self.add_recipient("team", status="released", task_id=task_id)
        peer_delivery = self.add_peer_outbox("team", status="failed")
        with self.store.connection(write=True) as db:
            db.execute("UPDATE peer_outbox SET status='released' WHERE delivery_id=?", (peer_delivery,))
        self.add_node_outbox("team", status="acked")
        deleted = groups.control_group(self.store, "team", "delete", 0, "delete-terminal")
        self.assertTrue(deleted["deleted"])

    def test_activity_state_active_for_open_idle_and_unsettled_groups(self):
        self.create_group("open", session_ids=[self.open_session(desired="open")])
        open_group = self.group("open")
        self.assertEqual("active", open_group["activity_state"])
        self.assertTrue(open_group["active"])
        self.assertFalse(open_group["stale"])
        self.assertTrue(any("open" in reason for reason in open_group["activity_reasons"]))
        self.assertEqual(0, open_group["unsettled_count"])

        self.create_group("idle", session_ids=[self.open_session(desired="open", observed="idle")])
        self.assertEqual("active", self.group("idle")["activity_state"])

        self.create_group("busy")
        self.add_task("busy", status="running")
        busy = self.group("busy")
        self.assertEqual("active", busy["activity_state"])
        self.assertEqual(1, busy["unsettled_count"])
        self.assertTrue(any("unsettled" in reason for reason in busy["activity_reasons"]))

        before = self.group("busy")["last_activity_at"]
        time.sleep(0.01)
        self.group("busy")
        self.assertEqual(before, self.group("busy")["last_activity_at"])

    def test_activity_state_inactive_for_paused_and_empty_groups(self):
        self.create_group("paused", session_ids=[self.open_session(desired="paused")])
        paused = self.group("paused")
        self.assertEqual("inactive", paused["activity_state"])
        self.assertFalse(paused["active"])
        self.assertEqual(0, paused["unsettled_count"])
        self.assertTrue(paused["activity_reasons"])

        self.create_group("empty", session_ids=[])
        empty = self.group("empty")
        self.assertEqual("inactive", empty["activity_state"])
        self.assertFalse(empty["active"])
        self.assertEqual(0, empty["member_count"])
        self.assertIn("no members", empty["activity_reasons"][0])

    def test_activity_state_completed_for_closed_and_stopped_members(self):
        self.create_group()
        completed = self.group("team")
        self.assertEqual("completed", completed["activity_state"])
        self.assertFalse(completed["active"])
        self.assertTrue(any("closed and stopped" in reason for reason in completed["activity_reasons"]))

        reopened = groups.control_group(
            self.store, "team", "add", 0, "add-open", [self.open_session(desired="open")],
        )
        self.assertEqual(1, reopened["revision"])
        self.assertEqual("active", self.group("team")["activity_state"])

    def test_activity_state_unknown_for_native_remote_and_unverifiable_processes(self):
        self.create_group("native", native_members=[self.native_member()])
        native = self.group("native")
        self.assertEqual("unknown", native["activity_state"])
        self.assertTrue(native["active"])
        self.assertTrue(native["stale"])
        self.assertTrue(any("native session" in reason for reason in native["activity_reasons"]))
        self.assertEqual(1, native["member_count"])

        self.create_group("remote", session_ids=[], remote_members=[REMOTE_BRIDGE])
        remote = self.group("remote")
        self.assertEqual("unknown", remote["activity_state"])
        self.assertTrue(remote["active"])
        self.assertTrue(remote["stale"])
        self.assertEqual(1, remote["remote_member_count"])
        self.assertEqual(1, remote["member_count"])

        scope = self.root / "missing-cgroup"
        scope.mkdir()
        self.create_group(
            "scope", session_ids=[self.open_session(desired="closed", process_scope=str(scope))],
        )
        unverifiable = self.group("scope")
        self.assertEqual("unknown", unverifiable["activity_state"])
        self.assertTrue(unverifiable["stale"])
        self.assertTrue(any("cannot be verified" in reason for reason in unverifiable["activity_reasons"]))

    def test_activity_state_active_wins_over_unknown_and_deleted_wins_over_everything(self):
        self.create_group(
            "mixed", session_ids=[self.open_session(desired="open")], native_members=[self.native_member()],
        )
        mixed = self.group("mixed")
        self.assertEqual("active", mixed["activity_state"])
        self.assertTrue(mixed["stale"])
        self.assertTrue(any("open" in reason for reason in mixed["activity_reasons"]))
        self.assertTrue(any("native session" in reason for reason in mixed["activity_reasons"]))

        deleted = groups.control_group(self.store, "mixed", "delete", 0, "delete-mixed")
        self.assertTrue(deleted["deleted"])
        detail = self.group("mixed")
        self.assertEqual("deleted", detail["activity_state"])
        self.assertFalse(detail["active"])
        self.assertFalse(detail["stale"])

    def test_active_only_filters_list_but_never_explicit_detail(self):
        self.create_group("active", session_ids=[self.open_session(desired="open")])
        self.create_group("unknown", native_members=[self.native_member()])
        self.create_group("completed")
        listed = groups.inspect_groups(self.store)["groups"]
        self.assertEqual({"active", "unknown", "completed"}, {item["group_id"] for item in listed})
        filtered = groups.inspect_groups(self.store, active_only=True)["groups"]
        self.assertEqual({"active", "unknown"}, {item["group_id"] for item in filtered})
        detail = groups.inspect_groups(self.store, "completed", active_only=True)
        self.assertEqual("completed", detail["activity_state"])
        with self.assertRaisesRegex(BridgeError, "active_only must be a boolean"):
            groups.inspect_groups(self.store, active_only="yes")
        with self.assertRaisesRegex(BridgeError, "group_id"):
            groups.inspect_groups(self.store, group_id="  ")


class GroupPruneTests(GroupFixture, unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_reports_eligible_without_deleting(self):
        self.create_group()
        self.backdate("team")
        before = time.time()
        result = await groups.prune_groups(self.store, None, "1d", True, "dry-run")
        after = time.time()
        self.assertTrue(result["dry_run"])
        self.assertGreaterEqual(result["cutoff"], before - 86400)
        self.assertLessEqual(result["cutoff"], after - 86400)
        self.assertEqual({"total": 1, "dry_run": 1}, result["summary"])
        [item] = result["items"]
        self.assertEqual("team", item["group_id"])
        self.assertEqual(0, item["expected_revision"])
        self.assertEqual("eligible", item["check_result"])
        self.assertEqual("dry_run", item["status"])
        self.assertIsNone(item["reason"])
        snapshot = json.loads(item["member_snapshot"])
        self.assertEqual(1, len(snapshot["members"]))
        self.assertEqual([], snapshot["remote_members"])
        run = self.store.get_group_prune_run(result["run_id"])
        self.assertEqual("completed", run["status"])
        self.assertIsNotNone(run["finished"])
        self.assertFalse(self.group("team")["deleted"])
        with self.store.connection() as db:
            self.assertEqual(
                0, db.execute("SELECT COUNT(*) FROM events WHERE kind='group_pruned'").fetchone()[0],
            )

    async def test_real_prune_soft_deletes_preserves_members_and_emits_event(self):
        native = self.native_member()
        self.create_group(
            session_ids=[self.open_session(desired="closed")], native_members=[native],
        )
        self.backdate("team")
        result = await groups.prune_groups(self.store, None, 86400, False, "real-run")
        self.assertFalse(result["dry_run"])
        self.assertEqual({"total": 1, "pruned": 1}, result["summary"])
        [item] = result["items"]
        self.assertEqual("pruned", item["check_result"])
        self.assertEqual("pruned", item["status"])
        detail = self.group("team")
        self.assertTrue(detail["deleted"])
        self.assertEqual("pruned", detail["delete_reason"])
        self.assertIsNotNone(detail["deleted_at"])
        self.assertEqual(1, detail["revision"])
        self.assertEqual(2, len(detail["members"]))
        with self.store.connection() as db:
            event = db.execute("SELECT data FROM events WHERE kind='group_pruned'").fetchone()
            self.assertEqual("team", json.loads(event["data"])["group_id"])
            self.assertEqual(result["run_id"], json.loads(event["data"])["run_id"])
        self.assertEqual("completed", self.store.get_group_prune_run(result["run_id"])["status"])
        self.assertEqual([], groups.inspect_groups(self.store)["groups"])

    async def test_prune_skips_groups_with_unsettled_work(self):
        self.create_group()
        self.add_task("team", status="waiting_input")
        self.backdate("team")
        result = await groups.prune_groups(self.store, None, "1d", False, "unsettled-run")
        [item] = result["items"]
        self.assertEqual("blocked", item["check_result"])
        self.assertEqual("skipped", item["status"])
        self.assertIn("unsettled work", item["reason"])
        self.assertFalse(self.group("team")["deleted"])
        self.assertEqual({"total": 1, "skipped": 1}, result["summary"])

    async def test_prune_skips_members_that_are_not_provably_stopped(self):
        cases = (
            ("open", {"desired": "open"}, "not closed", None),
            ("paused", {"desired": "paused"}, "not closed", None),
            ("idle", {"observed": "idle"}, "not stopped", None),
            ("alive", {"worker_pid": 123}, "live worker", ("alive",)),
            ("processes", {"process_scope": "scope"}, "worker processes", ("owned_processes",)),
        )
        for group_id, session_state, reason, patched in cases:
            with self.subTest(group_id=group_id):
                session_id = self.open_session(
                    desired=session_state.get("desired", "closed"),
                    observed=session_state.get("observed", "stopped"),
                    worker_pid=session_state.get("worker_pid"),
                    process_scope=str(self.root) if "process_scope" in session_state else None,
                )
                self.create_group(group_id, session_ids=[session_id])
                self.backdate(group_id)
                if patched == ("alive",):
                    context = patch("agent_bridge.groups.alive", return_value=True)
                elif patched == ("owned_processes",):
                    context = patch(
                        "agent_bridge.groups.owned_processes",
                        return_value={"status": "running", "processes": [], "external_jobs": "unknown"},
                    )
                else:
                    context = _NullContext()
                with context:
                    result = await groups.prune_groups(
                        self.store, None, "1d", True, f"stopped-{group_id}",
                    )
                item = self.item(result, group_id)
                self.assertEqual("blocked", item["check_result"], item)
                self.assertIn(reason, item["reason"])
                self.assertFalse(self.group(group_id)["deleted"])

    async def test_prune_native_queue_ownership_and_agy_uncertainty(self):
        member = self.native_member()
        self.create_group("native", session_ids=[], native_members=[member])
        self.backdate("native")
        eligible = await groups.prune_groups(self.store, None, "1d", True, "native-ok")
        self.assertEqual("eligible", self.item(eligible, "native")["check_result"])

        with self.store.connection(write=True) as db:
            db.execute(
                "INSERT INTO native_send_queue(id,runtime,native_id,local_seq,delivery_source,delivery_id,"
                "message,status,created) VALUES(?,?,?,?,?,?,?,?,?)",
                ("queue-1", member["runtime"], member["native_id"], 1, "local_broadcast", "delivery-1",
                 "message", "pending", time.time()),
            )
        queued = await groups.prune_groups(self.store, None, "1d", True, "native-queue")
        queued_item = self.item(queued, "native")
        self.assertEqual("blocked", queued_item["check_result"])
        self.assertIn("delivery or send lock", queued_item["reason"])

        with self.store.connection(write=True) as db:
            db.execute("DELETE FROM native_send_queue")
        self.open_session(desired="closed", native_id=member["native_id"])
        owned = await groups.prune_groups(self.store, None, "1d", True, "native-owned")
        owned_item = self.item(owned, "native")
        self.assertEqual("blocked", owned_item["check_result"])
        self.assertIn("owned by a bridge session", owned_item["reason"])

        agy = self.native_member("antigravity")
        self.create_group("agy", session_ids=[], native_members=[agy])
        self.backdate("agy")
        with patch("agent_bridge.groups.native_inactive_reason", return_value="AGY lock is held"):
            locked = await groups.prune_groups(self.store, None, "1d", True, "agy-lock")
        locked_item = self.item(locked, "agy")
        self.assertEqual("blocked", locked_item["check_result"])
        self.assertIn("AGY lock is held", locked_item["reason"])
        with patch("agent_bridge.groups.native_inactive_reason", side_effect=OSError("no state")):
            unverifiable = await groups.prune_groups(self.store, None, "1d", True, "agy-unverified")
        unverifiable_item = self.item(unverifiable, "agy")
        self.assertEqual("blocked", unverifiable_item["check_result"])
        self.assertIn("cannot be verified", unverifiable_item["reason"])

    async def test_prune_remote_member_verification_success_and_failures(self):
        _, item = await self._prune_remote(
            "success", FakeFactory(AsyncClient(remote_group_result("remote-session-a"))),
        )
        self.assertEqual("eligible", item["check_result"])

        _, item = await self._prune_remote(
            "direct", DirectFactory(AsyncClient(remote_group_result("remote-session-a"))),
        )
        self.assertEqual("eligible", item["check_result"])

        _, item = await self._prune_remote(
            "callable", CallableFactory(AsyncClient(remote_group_result("remote-session-a"))),
        )
        self.assertEqual("eligible", item["check_result"])

        for group_id, factory, reason in (
            ("unavailable", FakeFactory(error=ConnectionError("ssh down")), "is unavailable"),
            ("unreachable", FakeFactory(FakeClient(error=TimeoutError("timed out"))), "is unreachable"),
            ("nostate", FakeFactory(FakeClient({"items": []})), "not confirmed stopped"),
            ("nomatch", FakeFactory(FakeClient({"items": [{"id": "other", "observed": "stopped"}]})),
             "not confirmed stopped"),
            ("notclosed", FakeFactory(FakeClient(remote_group_result("remote-session-a", desired="open"))),
             "is not closed"),
            ("notstopped", FakeFactory(FakeClient(remote_group_result("remote-session-a", observed="idle"))),
             "is not stopped"),
            ("malformed", FakeFactory(FakeClient({"unexpected": True})), "no verifiable session state"),
        ):
            with self.subTest(group_id=group_id):
                result, item = await self._prune_remote(group_id, factory)
                self.assertEqual("blocked", item["check_result"], item)
                self.assertIn(reason, item["reason"])
                self.assertFalse(self.group(group_id)["deleted"])
                self.assertGreaterEqual(result["summary"]["total"], 1)

        self.create_group("nofactory", session_ids=[], remote_members=[REMOTE_BRIDGE])
        self.backdate("nofactory")
        result = await groups.prune_groups(self.store, SimpleNamespace(), "1d", True, "remote-nofactory")
        self.assertIn("without a peer client", self.item(result, "nofactory")["reason"])
        _, item = await self._prune_remote("nobroker", None)
        self.assertIn("require coordinator peer access", item["reason"])

    async def _prune_remote(self, group_id, peer_factory):
        self.create_group(group_id, session_ids=[], remote_members=[REMOTE_BRIDGE])
        self.backdate(group_id)
        broker = None if peer_factory is None else SimpleNamespace(peer_factory=peer_factory)
        result = await groups.prune_groups(self.store, broker, "1d", True, f"remote-{group_id}")
        return result, self.item(result, group_id)

    async def test_remote_client_inspect_is_awaited_and_receives_session_probe(self):
        client = AsyncClient(remote_group_result("remote-session-a"))
        factory = FakeFactory(client)
        await self._prune_remote("probe", factory)
        self.assertEqual([{"kind": "session", "id": "remote-session-a"}], client.calls)

    async def test_prune_rechecks_activity_before_cas(self):
        client = AsyncClient(remote_group_result("remote-session-a"))
        changed = {"done": False}

        def hook(payload):
            if changed["done"]:
                return
            changed["done"] = True
            with self.store.connection(write=True) as db:
                db.execute("UPDATE session_groups SET revision=revision+1 WHERE id='team'")

        client.hook = hook
        self.create_group("team", session_ids=[], remote_members=[REMOTE_BRIDGE])
        self.backdate("team")
        result = await groups.prune_groups(
            self.store, SimpleNamespace(peer_factory=FakeFactory(client)), "1d", False, "changed",
        )
        [item] = result["items"]
        self.assertEqual("blocked", item["check_result"])
        self.assertIn("activity or membership changed", item["reason"])
        self.assertFalse(self.group("team")["deleted"])

    async def test_prune_rechecks_last_activity_without_a_revision_bump(self):
        client = AsyncClient(remote_group_result("remote-session-a"))
        changed = {"done": False}

        def hook(payload):
            if changed["done"]:
                return
            changed["done"] = True
            with self.store.connection(write=True) as db:
                db.execute("UPDATE session_groups SET last_activity_at=? WHERE id='team'", (time.time(),))

        client.hook = hook
        self.create_group("team", session_ids=[], remote_members=[REMOTE_BRIDGE])
        self.backdate("team")
        result = await groups.prune_groups(
            self.store, SimpleNamespace(peer_factory=FakeFactory(client)), "1d", False, "touched",
        )
        self.assertEqual("blocked", result["items"][0]["check_result"])
        self.assertFalse(self.group("team")["deleted"])

    async def test_candidate_selection_uses_fixed_cutoff_and_excludes_deleted_groups(self):
        self.create_group("old")
        self.backdate("old")
        self.create_group("empty", session_ids=[])
        self.backdate("empty")
        self.create_group("fresh")
        future = self.create_group("future")
        with self.store.connection(write=True) as db:
            db.execute("UPDATE session_groups SET last_activity_at=? WHERE id='future'", (time.time() + 3600,))
        self.create_group("gone")
        groups.control_group(self.store, "gone", "delete", 0, "delete-gone")
        self.backdate("gone")
        result = await groups.prune_groups(self.store, None, "1d", True, "candidates")
        self.assertEqual({"old", "empty"}, {item["group_id"] for item in result["items"]})
        self.assertEqual(2, result["summary"]["total"])
        self.assertEqual(0, future["revision"])

    async def test_request_id_is_idempotent_and_never_widens_candidates(self):
        self.create_group("first")
        self.backdate("first")
        first = await groups.prune_groups(self.store, None, "1d", True, "repeat")
        self.create_group("later")
        self.backdate("later")
        repeated = await groups.prune_groups(self.store, None, "1d", True, "repeat")
        self.assertEqual(first["run_id"], repeated["run_id"])
        self.assertEqual(first["cutoff"], repeated["cutoff"])
        self.assertEqual(first["items"], repeated["items"])
        self.assertEqual({"first"}, {item["group_id"] for item in repeated["items"]})
        with self.assertRaisesRegex(BridgeError, "request_conflict|different parameters"):
            await groups.prune_groups(self.store, None, "2d", True, "repeat")
        with self.assertRaisesRegex(BridgeError, "request_conflict|different parameters"):
            await groups.prune_groups(self.store, None, "1d", False, "repeat")

    async def test_invalid_prune_parameters_are_rejected_before_creating_a_run(self):
        invalid = (
            {"older_than": 0}, {"older_than": -1}, {"older_than": True}, {"older_than": "x"},
            {"older_than": ""}, {"older_than": None}, {"older_than": float("inf")},
            {"older_than": "1d", "dry_run": "yes"}, {"older_than": "1d", "request_id": ""},
            {"older_than": "1d", "request_id": None}, {"older_than": "1d", "request_id": 5},
        )
        for params in invalid:
            with self.subTest(params=params), self.assertRaises(BridgeError) as caught:
                await groups.prune_groups(
                    self.store, None, params["older_than"], params.get("dry_run", True),
                    params.get("request_id", "valid"),
                )
            self.assertEqual("invalid_request", caught.exception.code)
        with self.store.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM group_prune_runs").fetchone()[0])

    async def test_cas_failure_is_recorded_per_group_without_rolling_back_others(self):
        self.create_group("first")
        self.backdate("first", age=3 * 86400)
        self.create_group("second")
        self.backdate("second", age=2 * 86400)
        self.add_task("second", status="running")
        original = self.store.prune_group_cas

        def flaky(group_id, revision, delete_reason="pruned", now=None):
            if group_id == "first":
                raise BridgeError(PeerErrorCode.REVISION_CONFLICT, "stale revision")
            return original(group_id, revision, delete_reason=delete_reason, now=now)

        with patch.object(self.store, "prune_group_cas", side_effect=flaky):
            result = await groups.prune_groups(self.store, None, "1d", False, "cas-failure")
        items = {item["group_id"]: item for item in result["items"]}
        self.assertEqual("blocked", items["first"]["check_result"])
        self.assertIn("stale revision", items["first"]["reason"])
        self.assertEqual("blocked", items["second"]["check_result"])
        self.assertEqual({"total": 2, "skipped": 2}, result["summary"])
        self.assertFalse(self.group("first")["deleted"])
        self.assertFalse(self.group("second")["deleted"])
        self.assertEqual("completed", self.store.get_group_prune_run(result["run_id"])["status"])

    async def test_unexpected_group_failure_marks_the_run_failed(self):
        self.create_group()
        self.backdate("team")
        with patch("agent_bridge.groups._member_snapshot", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                await groups.prune_groups(self.store, None, "1d", False, "failed-run")
        run = self.store.get_group_prune_run_by_request_id("failed-run")
        self.assertEqual("failed", run["status"])
        self.assertIsNotNone(run["finished"])

    async def test_broker_and_mcp_and_cli_expose_prune_surfaces(self):
        self.create_group("active", session_ids=[self.open_session(desired="open")])
        self.create_group("old")
        self.backdate("old")
        broker = Broker(self.root)
        listed = await broker.api_call("agent_groups", {"active_only": True})
        self.assertEqual({"active"}, {item["group_id"] for item in listed["result"]["groups"]})
        detail = await broker.api_call("agent_groups", {"group_id": "old", "active_only": True})
        self.assertEqual("old", detail["result"]["group_id"])
        pruned = await broker.api_call(
            "agent_group_prune", {"older_than": "1d", "dry_run": True, "request_id": "broker-dry"},
        )
        self.assertEqual({"old"}, {item["group_id"] for item in pruned["result"]["items"]})
        self.assertTrue(pruned["result"]["dry_run"])

        tools = {tool.name: tool for tool in await mcp_server(self.root).list_tools()}
        self.assertIn("agent_group_prune", tools)
        self.assertTrue(tools["agent_groups"].annotations.readOnlyHint)
        group_schema = tools["agent_groups"].inputSchema["properties"]
        self.assertEqual(False, group_schema["active_only"]["default"])
        prune_schema = tools["agent_group_prune"].inputSchema["properties"]
        self.assertEqual(True, prune_schema["dry_run"]["default"])
        self.assertEqual({"older_than"}, set(tools["agent_group_prune"].inputSchema["required"]))

        parser = build_parser()
        args = parser.parse_args(["group", "prune", "--older-than", "7d"])
        self.assertEqual(("group", "prune", "7d", False), (args.command, args.group_command, args.older_than, args.dry_run))
        args = parser.parse_args(["group", "prune", "--older-than", "7d", "--dry-run"])
        self.assertTrue(args.dry_run)
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            parser.parse_args(["group", "prune"])

    async def test_cli_group_prune_runs_against_the_admin_socket(self):
        self.create_group()
        self.backdate("team")
        broker = Broker(self.root)

        async def connection(reader, writer):
            await broker.connection(reader, writer)

        server = await asyncio.start_unix_server(connection, str(self.root / "broker.sock"))
        env = {key: value for key, value in os.environ.items()
               if key not in {"AGENT_BRIDGE_ROLE", "AGENT_BRIDGE_TOKEN"}}
        try:
            dry = await self._cli(server, ["--older-than", "1d", "--dry-run"], env)
            self.assertTrue(dry["dry_run"])
            self.assertEqual("eligible", dry["items"][0]["check_result"])
            self.assertFalse(self.group("team")["deleted"])

            real = await self._cli(server, ["--older-than", "1d"], env)
            self.assertFalse(real["dry_run"])
            self.assertEqual("pruned", real["items"][0]["status"])
        finally:
            server.close()
            await server.wait_closed()
        detail = self.group("team")
        self.assertTrue(detail["deleted"])
        self.assertEqual("pruned", detail["delete_reason"])

    async def _cli(self, server, arguments, env):
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "agent_bridge.cli", "--state", str(self.root),
            "group", "prune", *arguments,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env,
        )
        stdout, stderr = await process.communicate()
        self.assertEqual(0, process.returncode, stderr.decode())
        return json.loads(stdout)


class _NullContext:
    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


if __name__ == "__main__":
    unittest.main()
