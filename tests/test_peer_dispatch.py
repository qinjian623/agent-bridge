import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from agent_bridge.broker import Broker
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import INLINE_RESULT_BYTES, PeerErrorCode
from agent_bridge.store import Store


class PeerStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = Store(self.state)

    def test_operations_are_idempotent_per_operation_id_and_fingerprint(self):
        created = self.store.record_operation(
            "op-1",
            node_id="node-a",
            state_generation="generation-a",
            fingerprint="fingerprint-1",
            method="peer.deliver",
            params={"message": "once"},
        )
        self.assertEqual("pending", created["status"])
        self.assertEqual({"message": "once"}, created["params"])
        self.assertIsNone(created["result"])

        replayed = self.store.record_operation(
            "op-1",
            node_id="node-b",
            state_generation="generation-b",
            fingerprint="fingerprint-1",
        )
        self.assertEqual(created["created"], replayed["created"])
        self.assertEqual("node-a", replayed["node_id"])

        with self.assertRaises(BridgeError) as caught:
            self.store.record_operation("op-1", fingerprint="fingerprint-2")
        self.assertEqual(PeerErrorCode.OPERATION_CONFLICT, caught.exception.code)

        with self.assertRaises(BridgeError) as caught:
            self.store.record_operation("")
        self.assertEqual(PeerErrorCode.INVALID_PARAMS, caught.exception.code)

    def test_operation_lookup_supports_the_identity_namespace_and_updates(self):
        self.store.record_operation(
            "op-1",
            node_id="node-a",
            state_generation="generation-a",
            fingerprint="fingerprint-1",
            method="peer.deliver",
            params={"message": "once"},
        )

        self.assertIsNotNone(
            self.store.get_operation(
                "op-1", node_id="node-a", state_generation="generation-a", fingerprint="fingerprint-1"
            )
        )
        self.assertIsNone(self.store.get_operation("op-1", node_id="node-b"))
        self.assertIsNone(self.store.get_operation("op-1", state_generation="generation-b"))
        self.assertIsNone(self.store.get_operation("op-1", fingerprint="fingerprint-2"))
        self.assertIsNone(self.store.get_operation("missing"))
        self.assertIsNone(self.store.get_operation(""))

        updated = self.store.update_operation(
            "op-1", status="completed", result={"task_id": "task-1"}, params={"message": "updated"}
        )
        self.assertEqual("completed", updated["status"])
        self.assertEqual({"task_id": "task-1"}, updated["result"])
        self.assertEqual({"message": "updated"}, updated["params"])

        with self.assertRaises(BridgeError) as caught:
            self.store.update_operation("missing", status="failed")
        self.assertEqual(PeerErrorCode.OPERATION_NOT_FOUND, caught.exception.code)

    def test_inbox_deliveries_are_unique_per_sequence_slot(self):
        created = self.store.record_inbox_delivery(
            "delivery-1",
            "gpu-a",
            "node-a",
            "generation-a",
            3,
            "op-1",
            {"message": "hello"},
            receipt={"accepted_at": 1.0},
        )
        self.assertEqual("received", created["status"])
        self.assertEqual({"message": "hello"}, created["payload"])
        self.assertEqual({"accepted_at": 1.0}, created["receipt"])

        self.assertEqual(
            created,
            self.store.record_inbox_delivery(
                "delivery-1", "gpu-a", "node-a", "generation-a", 3, "op-1", {"message": "hello"}
            ),
        )
        with self.assertRaises(BridgeError) as caught:
            self.store.record_inbox_delivery(
                "delivery-1", "gpu-a", "node-a", "generation-a", 3, "op-2", {"message": "hello"}
            )
        self.assertEqual(PeerErrorCode.OPERATION_CONFLICT, caught.exception.code)

        with self.assertRaises(BridgeError) as caught:
            self.store.record_inbox_delivery(
                "delivery-2", "gpu-a", "node-a", "generation-a", 3, "op-2", {"message": "other"}
            )
        self.assertEqual(PeerErrorCode.SEQUENCE_GAP, caught.exception.code)

        self.assertIsNone(self.store.get_inbox_delivery("missing"))
        with self.assertRaises(BridgeError) as caught:
            self.store.record_inbox_delivery("", "gpu-a", "node-a", "generation-a", 4, "op-3", {})
        self.assertEqual(PeerErrorCode.INVALID_PARAMS, caught.exception.code)

    def test_outbox_add_pull_and_ack_are_replay_safe(self):
        first = self.store.add_outbox("op-1", "peer.deliver", {"message": "one"}, group_id="group-1")
        self.assertEqual(1, first)
        self.assertEqual(first, self.store.add_outbox("op-1", "peer.deliver", {"message": "one"}))
        with self.assertRaises(BridgeError) as caught:
            self.store.add_outbox("op-1", "peer.deliver", {"message": "changed"})
        self.assertEqual(PeerErrorCode.OPERATION_CONFLICT, caught.exception.code)

        second = self.store.add_outbox("op-2", "agent_group_inbox", {"message": "two"})
        self.assertEqual(2, second)

        page = self.store.pull_outbox(0, limit=1)
        self.assertEqual([1], [row["seq"] for row in page])
        self.assertEqual("pulled", page[0]["status"])
        self.assertIsNotNone(page[0]["pulled_at"])
        self.assertEqual({"message": "one"}, page[0]["payload"])

        remaining = self.store.pull_outbox(1, limit=10)
        self.assertEqual([2], [row["seq"] for row in remaining])

        acked = self.store.ack_outbox("op-1")
        self.assertEqual("acked", acked["status"])
        self.assertIsNotNone(acked["acked_at"])
        self.assertEqual([2], [row["seq"] for row in self.store.pull_outbox(0, limit=10)])

        with self.assertRaises(BridgeError) as caught:
            self.store.ack_outbox("missing")
        self.assertEqual(PeerErrorCode.OPERATION_NOT_FOUND, caught.exception.code)
        for call in (
            lambda: self.store.add_outbox("", "peer.deliver", {}),
            lambda: self.store.add_outbox("op-3", "", {}),
            lambda: self.store.pull_outbox(-1),
            lambda: self.store.pull_outbox(0, limit=0),
            lambda: self.store.ack_outbox(""),
        ):
            with self.assertRaises(BridgeError) as caught:
                call()
            self.assertEqual(PeerErrorCode.INVALID_PARAMS, caught.exception.code)

    def test_existing_databases_are_migrated_without_losing_rows(self):
        state = Path(self.temp.name) / "legacy"
        state.mkdir()
        with sqlite3.connect(state / "bridge.sqlite") as db:
            db.executescript(
                """
                CREATE TABLE sessions (
                 id TEXT PRIMARY KEY, profile TEXT NOT NULL, cwd TEXT NOT NULL, access TEXT NOT NULL,
                 parent TEXT, native_id TEXT, desired TEXT NOT NULL DEFAULT 'open',
                 observed TEXT NOT NULL DEFAULT 'stopped', revision INTEGER NOT NULL DEFAULT 0,
                 worker_pid INTEGER, worker_start TEXT, generation TEXT, error TEXT, created REAL NOT NULL
                );
                INSERT INTO sessions(id,profile,cwd,access,desired,observed,revision,created)
                VALUES('legacy-session','{}','/tmp','read-only','open','stopped',0,1.0);
                """
            )

        store = Store(state)
        with store.connection() as db:
            columns = {row[1] for row in db.execute("PRAGMA table_info(sessions)")}
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0])
        self.assertLessEqual({"workspace", "process_scope"}, columns)
        self.assertLessEqual({"peer_operations", "peer_inbox", "node_outbox"}, tables)

        # Opening the migrated database again must be a no-op rather than a duplicate-column error.
        Store(state)
        with Store(state).connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0])


class DirectPeerServicePhaseBTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = Store(self.state)
        self.identity = FileNodeIdentityStore(self.state)
        self.profile = Profile(
            "worker", "codex", "worker-model", env_file=str(self.state / "private-env.json")
        )
        self.config = Config({"worker": self.profile})
        self.service = DirectPeerService(
            self.identity, host_id="gpu-a", store=self.store, config=self.config
        )

    def operation(self, operation_id="op-1"):
        identity = self.identity.load_or_create()
        return {
            "node_id": identity.node_id,
            "state_generation": identity.state_generation,
            "operation_id": operation_id,
        }

    def deliver_params(self, **changes):
        params = {
            "operation": self.operation("op-1"),
            "delivery_id": "delivery-1",
            "group_id": "group-1",
            "broadcast_id": "broadcast-1",
            "target": {"kind": "bridge", "id": ""},
            "target_seq": 5,
            "seq_kind": "message",
            "message": "do the work",
            "expected_group_revision": 0,
            "open": {"profile": "worker", "cwd": str(self.state), "access": "read-only"},
        }
        params.update(changes)
        return params

    async def peer(self, method, params):
        return await self.service.handle({"v": 1, "id": "test", "method": method, "params": params})

    def assert_error(self, response, code):
        self.assertFalse(response["ok"], response)
        self.assertEqual(code, response["error"]["code"])

    def complete_task(self, result, request_id):
        session_id = self.store.open(
            self.config.profiles["worker"], str(self.state), "read-only", request_id
        )["session_id"]
        self.store.worker_ready(session_id, "generation", 123, "start", "native")
        task_id = self.store.dispatch(session_id, "finish me", "dispatch:" + request_id)["task_id"]
        attempt_id = self.store.claim(self.config)["attempt_id"]
        self.store.begin_attempt(session_id, "generation")
        self.store.finish(
            session_id, "generation", task_id, attempt_id, "completed", result,
            {"total_tokens": 1, "complete": True},
        )
        return task_id

    async def test_catalog_returns_only_sanitized_entries(self):
        response = await self.peer("peer.catalog", {})

        self.assertTrue(response["ok"], response)
        result = response["result"]
        self.assertEqual({"entries", "stale", "fetched_at"}, set(result))
        self.assertFalse(result["stale"])
        self.assertIsInstance(result["fetched_at"], float)
        [entry] = result["entries"]
        self.assertEqual(self.profile.public(), entry)
        self.assertNotIn("env_file", entry)
        self.assertEqual("worker", entry["name"])

        filtered = await self.peer("peer.catalog", {"profile": "missing"})
        self.assertEqual([], filtered["result"]["entries"])
        self.assert_error(await self.peer("peer.catalog", {"profile": 7}), PeerErrorCode.INVALID_PARAMS)

    async def test_deliver_opens_and_dispatches_in_one_idempotent_operation(self):
        params = self.deliver_params()
        response = await self.peer("peer.deliver", params)

        self.assertTrue(response["ok"], response)
        result = response["result"]
        self.assertTrue(result["accepted"])
        self.assertEqual("accepted_remote", result["status"])
        self.assertEqual("delivery-1", result["delivery_id"])
        self.assertEqual(5, result["target_seq"])
        self.assertIn("accepted_at", result["receipt"])
        session_id = result["session_id"]
        task_id = result["task_id"]
        self.assertIsNotNone(session_id)
        self.assertIsNotNone(task_id)

        session = self.store.get_session(session_id)
        self.assertEqual("worker", session["profile"]["name"])
        self.assertEqual(str(self.state.resolve()), session["cwd"])
        task = self.store.get_task(task_id)
        self.assertEqual("queued", task["status"])
        self.assertEqual("do the work", task["instruction"])
        self.assertEqual("op-1", task["details"]["operation_id"])
        self.assertEqual("delivery-1", task["details"]["delivery_id"])

        operation = self.store.get_operation("op-1")
        self.assertEqual("completed", operation["status"])
        self.assertEqual(result, operation["result"])
        inbox = self.store.get_inbox_delivery("delivery-1")
        self.assertEqual("received", inbox["status"])
        self.assertEqual("op-1", inbox["operation_id"])
        self.assertEqual(5, inbox["target_seq"])
        self.assertEqual(5, self.identity.load_or_create().max_confirmed_seq)

        replay = await self.peer("peer.deliver", params)
        self.assertTrue(replay["ok"], replay)
        self.assertEqual(result, replay["result"])
        self.assertEqual(1, len(self.store.sessions()))
        self.assertEqual([], self.store.get_task(task_id)["attempts"])

        conflict = await self.peer("peer.deliver", {**params, "message": "something else"})
        self.assert_error(conflict, PeerErrorCode.OPERATION_CONFLICT)
        self.assertEqual(1, len(self.store.sessions()))

    async def test_deliver_writes_a_validated_open_name_with_the_session(self):
        response = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-named"),
                delivery_id="delivery-named",
                target_seq=6,
                open={
                    "profile": "worker",
                    "cwd": str(self.state),
                    "access": "read-only",
                    "name": "  Remote Worker  ",
                },
            ),
        )
        self.assertTrue(response["ok"], response)
        result = response["result"]
        self.assertEqual("Remote Worker", result["name"])
        self.assertEqual("Remote Worker", self.store.get_session(result["session_id"])["name"])
        opened = [
            event for event in self.store.events(session_id=result["session_id"])
            if event["kind"] == "session_opened"
        ]
        self.assertEqual({"name": "Remote Worker"}, opened[0]["data"])

        invalid = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-bad-name"),
                delivery_id="delivery-bad-name",
                target_seq=7,
                open={
                    "profile": "worker",
                    "cwd": str(self.state),
                    "access": "read-only",
                    "name": "bad\nname",
                },
            ),
        )
        self.assert_error(invalid, "invalid_request")
        self.assertEqual(1, len(self.store.sessions()))

    async def test_deliver_rejects_identity_mismatch_and_corrupted_state(self):
        mismatch = self.deliver_params(
            operation={
                "node_id": "other-node",
                "state_generation": self.identity.load_or_create().state_generation,
                "operation_id": "op-mismatch",
            }
        )
        self.assert_error(await self.peer("peer.deliver", mismatch), PeerErrorCode.IDENTITY_MISMATCH)
        self.assertEqual([], self.store.sessions())
        self.assertIsNone(self.store.get_operation("op-mismatch"))

        self.identity.advance(5)
        with self.assertRaises(BridgeError) as caught:
            self.identity.verify_database(2)
        self.assertEqual(PeerErrorCode.STATE_CORRUPTED, caught.exception.code)

        corrupted = self.deliver_params(operation=self.operation("op-corrupted"), target_seq=6)
        self.assert_error(await self.peer("peer.deliver", corrupted), PeerErrorCode.STATE_CORRUPTED)
        self.assertEqual([], self.store.sessions())
        self.assertIsNone(self.store.get_operation("op-corrupted"))

    async def test_deliver_accepts_existing_sessions_and_rejects_closed_or_unknown_ones(self):
        session_id = self.store.open(
            self.config.profiles["worker"], str(self.state), "read-only", "open-existing"
        )["session_id"]
        response = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-existing"),
                delivery_id="delivery-existing",
                target={"kind": "bridge", "id": session_id},
                open=None,
            ),
        )
        self.assertTrue(response["ok"], response)
        self.assertEqual(session_id, response["result"]["session_id"])
        self.assertEqual(
            session_id, self.store.get_task(response["result"]["task_id"])["session_id"]
        )

        unknown = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-unknown"),
                delivery_id="delivery-unknown",
                target_seq=6,
                target={"kind": "bridge", "id": "missing"},
                open=None,
            ),
        )
        self.assert_error(unknown, "not_found")

        self.store.control(session_id, "close", 0, "close-existing")
        closed = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-closed"),
                delivery_id="delivery-closed",
                target_seq=7,
                target={"kind": "bridge", "id": session_id},
                open=None,
            ),
        )
        self.assert_error(closed, "session_closed")

        no_open = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-no-open"),
                delivery_id="delivery-no-open",
                target_seq=8,
                open=None,
            ),
        )
        self.assert_error(no_open, PeerErrorCode.INVALID_PARAMS)

    async def test_deliver_keeps_sequence_and_delivery_slots_unique(self):
        first = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-first"),
                delivery_id="delivery-first",
                target_seq=3,
            ),
        )
        self.assertTrue(first["ok"], first)

        same_slot = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-second"),
                delivery_id="delivery-second",
                target_seq=3,
                message="second",
            ),
        )
        self.assert_error(same_slot, PeerErrorCode.SEQUENCE_GAP)
        self.assertIsNone(self.store.get_operation("op-second"))
        self.assertEqual(1, len(self.store.sessions()))

        reused_delivery = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-third"),
                delivery_id="delivery-first",
                target_seq=4,
                message="third",
            ),
        )
        self.assert_error(reused_delivery, PeerErrorCode.OPERATION_CONFLICT)
        self.assertIsNone(self.store.get_operation("op-third"))

    async def test_tombstone_delivery_cancels_without_opening_a_session(self):
        response = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-tombstone"),
                delivery_id="delivery-tombstone",
                target_seq=8,
                seq_kind="tombstone",
                message=None,
                open=None,
            ),
        )

        self.assertTrue(response["ok"], response)
        self.assertEqual("cancelled", response["result"]["status"])
        self.assertIsNone(response["result"]["session_id"])
        self.assertIsNone(response["result"]["task_id"])
        self.assertEqual([], self.store.sessions())
        self.assertEqual("cancelled", self.store.get_inbox_delivery("delivery-tombstone")["status"])
        self.assertEqual("cancelled", self.store.get_operation("op-tombstone")["status"])
        self.assertEqual(8, self.identity.load_or_create().max_confirmed_seq)

    async def test_lookup_delivery_resolves_lost_acknowledgements(self):
        delivered = await self.peer("peer.deliver", self.deliver_params())
        result = delivered["result"]

        by_delivery = await self.peer("peer.lookup_delivery", {"delivery_id": "delivery-1"})
        self.assertTrue(by_delivery["ok"], by_delivery)
        self.assertTrue(by_delivery["result"]["found"])
        self.assertEqual("completed", by_delivery["result"]["operation_status"])
        self.assertEqual(result["task_id"], by_delivery["result"]["delivery"]["task_id"])
        self.assertEqual(result["session_id"], by_delivery["result"]["delivery"]["session_id"])
        self.assertEqual(5, by_delivery["result"]["delivery"]["target_seq"])

        by_operation = await self.peer("peer.lookup_delivery", {"operation": self.operation("op-1")})
        self.assertTrue(by_operation["result"]["found"])
        self.assertEqual(result["task_id"], by_operation["result"]["delivery"]["task_id"])

        missing = await self.peer("peer.lookup_delivery", {"delivery_id": "missing"})
        self.assertEqual({"found": False}, missing["result"])
        self.assert_error(
            await self.peer("peer.lookup_delivery", {}), PeerErrorCode.INVALID_PARAMS
        )

    async def test_cancel_targets_a_task_operation_or_delivery_idempotently(self):
        delivered = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-delivered"),
                delivery_id="delivery-delivered",
                target_seq=1,
            ),
        )
        task_id = delivered["result"]["task_id"]

        by_task = await self.peer(
            "peer.cancel", {"operation": self.operation("cancel-task"), "task_id": task_id}
        )
        self.assertTrue(by_task["ok"], by_task)
        self.assertEqual({"target": task_id, "status": "cancelled"}, by_task["result"])
        self.assertEqual("cancelled", self.store.get_task(task_id)["status"])
        replay = await self.peer(
            "peer.cancel", {"operation": self.operation("cancel-task"), "task_id": task_id}
        )
        self.assertEqual(by_task["result"], replay["result"])
        conflict = await self.peer(
            "peer.cancel", {"operation": self.operation("cancel-task"), "task_id": "other"}
        )
        self.assert_error(conflict, PeerErrorCode.OPERATION_CONFLICT)

        second = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-second"),
                delivery_id="delivery-second",
                target_seq=2,
                message="second",
            ),
        )
        by_delivery = await self.peer(
            "peer.cancel",
            {"operation": self.operation("cancel-delivery"), "delivery_id": "delivery-second"},
        )
        self.assertTrue(by_delivery["ok"], by_delivery)
        self.assertEqual("cancelled", by_delivery["result"]["status"])
        self.assertEqual(2, by_delivery["result"]["tombstone_seq"])
        self.assertEqual("cancelled", self.store.get_operation("op-second")["status"])
        self.assertEqual(
            "cancelled", self.store.get_task(second["result"]["task_id"])["status"]
        )

        third = await self.peer(
            "peer.deliver",
            self.deliver_params(
                operation=self.operation("op-third"),
                delivery_id="delivery-third",
                target_seq=3,
                message="third",
            ),
        )
        by_operation = await self.peer(
            "peer.cancel",
            {"operation": self.operation("cancel-operation"), "operation_id": "op-third"},
        )
        self.assertTrue(by_operation["ok"], by_operation)
        self.assertEqual("op-third", by_operation["result"]["target"])
        self.assertEqual("cancelled", self.store.get_operation("op-third")["status"])
        self.assertEqual(
            "cancelled", self.store.get_task(third["result"]["task_id"])["status"]
        )

        self.assert_error(
            await self.peer("peer.cancel", {"operation": self.operation("cancel-none")}),
            PeerErrorCode.INVALID_PARAMS,
        )
        self.assert_error(
            await self.peer(
                "peer.cancel",
                {
                    "operation": self.operation("cancel-two"),
                    "task_id": task_id,
                    "delivery_id": "delivery-delivered",
                },
            ),
            PeerErrorCode.INVALID_PARAMS,
        )
        self.assert_error(
            await self.peer(
                "peer.cancel",
                {"operation": self.operation("cancel-missing"), "delivery_id": "missing"},
            ),
            PeerErrorCode.OPERATION_NOT_FOUND,
        )
        self.assert_error(
            await self.peer(
                "peer.cancel",
                {"operation": self.operation("cancel-missing-task"), "task_id": "missing"},
            ),
            PeerErrorCode.OPERATION_NOT_FOUND,
        )

    async def test_result_returns_inline_summaries_and_paged_artifact_refs(self):
        small_task = self.complete_task({"answer": "all good"}, "small-result")
        small = await self.peer("peer.result", {"task_id": small_task})
        self.assertTrue(small["ok"], small)
        self.assertEqual("completed", small["result"]["status"])
        self.assertEqual("all good", small["result"]["summary"])
        [small_ref] = small["result"]["artifacts"]
        self.assertEqual("gpu-a", small_ref["host_id"])
        self.assertLessEqual(small_ref["size"], INLINE_RESULT_BYTES)

        answer = "x" * (INLINE_RESULT_BYTES + 100)
        large_task = self.complete_task({"answer": answer}, "large-result")
        large = await self.peer("peer.result", {"task_id": large_task})
        self.assertTrue(large["ok"], large)
        self.assertNotIn("summary", large["result"])
        [large_ref] = large["result"]["artifacts"]
        self.assertGreater(large_ref["size"], INLINE_RESULT_BYTES)
        self.assertEqual(64, len(large_ref["sha256"]))

        chunk = await self.peer(
            "peer.result",
            {"task_id": large_task, "artifact_id": large_ref["artifact_id"], "offset": 0, "limit": 100},
        )
        self.assertTrue(chunk["ok"], chunk)
        self.assertEqual(large_ref["artifact_id"], chunk["result"]["chunk"]["artifact_id"])
        self.assertEqual(0, chunk["result"]["chunk"]["offset"])
        self.assertEqual(100, len(chunk["result"]["chunk"]["data"]))
        self.assertFalse(chunk["result"]["chunk"]["eof"])

        self.assert_error(
            await self.peer("peer.result", {"task_id": "missing"}), "not_found"
        )
        self.assert_error(
            await self.peer("peer.result", {"task_id": large_task, "artifact_id": 3}),
            PeerErrorCode.INVALID_PARAMS,
        )

    async def test_events_page_with_a_cursor_and_identity_refs(self):
        delivered = await self.peer("peer.deliver", self.deliver_params())
        result = delivered["result"]

        first = await self.peer("peer.events", {"after": 0, "limit": 1})
        self.assertTrue(first["ok"], first)
        self.assertEqual(1, len(first["result"]["items"]))
        self.assertTrue(first["result"]["has_more"])
        item = first["result"]["items"][0]
        self.assertEqual("session_opened", item["type"])
        identity = self.identity.load_or_create()
        self.assertEqual(
            {
                "host_id": "gpu-a",
                "node_id": identity.node_id,
                "state_generation": identity.state_generation,
                "local_id": result["session_id"],
            },
            item["ref"],
        )

        second = await self.peer(
            "peer.events", {"after": first["result"]["cursor"], "limit": 10}
        )
        self.assertTrue(second["ok"], second)
        self.assertFalse(second["result"]["has_more"])
        self.assertEqual(["task_queued"], [entry["type"] for entry in second["result"]["items"]])
        self.assertEqual(result["task_id"], second["result"]["items"][0]["ref"]["local_id"])
        self.assertEqual(
            second["result"]["cursor"], second["result"]["items"][-1]["seq"]
        )

        self.assert_error(
            await self.peer("peer.events", {"after": -1}), PeerErrorCode.INVALID_PARAMS
        )
        self.assert_error(
            await self.peer("peer.events", {"after": 0, "limit": 0}),
            PeerErrorCode.INVALID_PARAMS,
        )


class FakePeerClient:
    def __init__(self):
        self.calls = []
        self.catalog_result = {"entries": [], "stale": False, "fetched_at": 1.0}
        self.deliver_results = []
        self.cancel_results = []
        self.lookup_results = []

    async def hello(self, params):
        self.calls.append(("hello", params))
        return {
            "protocol_version": 1,
            "node": {"host_id": "gpu-a", "node_id": "node-a", "state_generation": "generation-a"},
            "node_state": "ready",
            "max_confirmed_seq": 0,
            "capabilities": [],
            "broker_version": "test",
        }

    async def catalog(self, params):
        self.calls.append(("catalog", params))
        return self.catalog_result

    async def deliver(self, params):
        self.calls.append(("deliver", params))
        return self.deliver_results.pop(0)

    async def cancel(self, params):
        self.calls.append(("cancel", params))
        return self.cancel_results.pop(0)

    async def lookup_delivery(self, params):
        self.calls.append(("lookup_delivery", params))
        return self.lookup_results.pop(0)

    def named(self, name):
        return [params for method, params in self.calls if method == name]


class FakePeerFactory:
    def __init__(self, client, host_ids=("gpu-a",)):
        self.peer_client = client
        self.host_ids = set(host_ids)
        self.requested = []

    def client(self, host_id):
        if host_id not in self.host_ids:
            raise BridgeError(PeerErrorCode.HOST_UNKNOWN, f"Unknown host: {host_id!r}")
        self.requested.append(host_id)
        return self.peer_client


class CoordinatorRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {
                        "fixed": {
                            "runtime": "codex",
                            "model": "fixed-model",
                            "executable": sys.executable,
                        }
                    },
                    "hosts": {
                        "gpu-a": {
                            "ssh_target": "gpu-a.internal",
                            "remote_executable": "/usr/local/bin/agent-bridge",
                            "pinned": {"node_id": "node-a", "state_generation": "generation-a"},
                        }
                    },
                    "worker_launcher": "detached",
                    "telegram_database": None,
                }
            )
        )
        self.client = FakePeerClient()
        self.factory = FakePeerFactory(self.client)
        self.broker = Broker(self.state, peer_factory=self.factory)

    async def test_remote_catalog_uses_the_registered_host_and_rejects_unknown_hosts(self):
        self.client.catalog_result = {
            "entries": [{"name": "fixed"}],
            "stale": False,
            "fetched_at": 123.0,
        }
        result = await self.broker.call(
            "agent_catalog", {"host_id": "gpu-a", "refresh": True, "profile": "fixed"}
        )
        self.assertEqual({**self.client.catalog_result, "host_id": "gpu-a"}, result)
        self.assertEqual([{"refresh": True, "profile": "fixed"}], self.client.named("catalog"))
        self.assertEqual(["gpu-a"], self.factory.requested)

        with self.assertRaises(BridgeError) as caught:
            await self.broker.call("agent_catalog", {"host_id": "missing"})
        self.assertEqual(PeerErrorCode.HOST_UNKNOWN, caught.exception.code)

    async def test_remote_open_is_one_stable_operation_and_readable_via_agent_operation(self):
        self.client.deliver_results.append(
            {
                "delivery_id": "delivery-open",
                "status": "accepted_remote",
                "session_id": "remote-session",
                "target_seq": 7,
            }
        )
        args = {
            "host_id": "gpu-a",
            "profile": "fixed",
            "cwd": str(self.state),
            "access": "read-only",
            "request_id": "open-1",
        }
        opened = await self.broker.call("agent_open", args)

        self.assertEqual("gpu-a", opened["host_id"])
        self.assertEqual("remote-session", opened["session_id"])
        [params] = self.client.named("deliver")
        self.assertEqual(
            {
                "node_id": "node-a",
                "state_generation": "generation-a",
                "operation_id": opened["operation_id"],
            },
            params["operation"],
        )
        self.assertEqual({"kind": "bridge", "id": ""}, params["target"])
        self.assertEqual("message", params["seq_kind"])
        self.assertNotIn("message", params)
        self.assertEqual("fixed", params["open"]["profile"])
        self.assertEqual(str(self.state), params["open"]["cwd"])
        self.assertEqual("read-only", params["open"]["access"])
        self.assertNotIn("request_id", params["open"])
        self.assertTrue(params["delivery_id"].startswith("delivery-"))
        self.assertGreaterEqual(params["target_seq"], 0)

        replayed = await self.broker.call("agent_open", args)
        self.assertEqual(opened, replayed)
        self.assertEqual(1, len(self.client.named("deliver")))

        view = await self.broker.call(
            "agent_operation", {"operation_id": opened["operation_id"]}
        )
        self.assertEqual("completed", view["status"])
        self.assertEqual("gpu-a", view["host_id"])
        self.assertEqual("remote-session", view["result"]["session_id"])

    async def test_remote_open_forwards_a_validated_session_name(self):
        self.client.deliver_results.append(
            {
                "delivery_id": "delivery-named",
                "status": "accepted_remote",
                "session_id": "remote-named",
                "target_seq": 8,
                "name": "Remote Name",
            }
        )
        args = {
            "host_id": "gpu-a",
            "profile": "fixed",
            "cwd": str(self.state),
            "request_id": "open-named",
            "name": "  Remote Name  ",
        }
        opened = await self.broker.call("agent_open", args)
        [params] = self.client.named("deliver")
        self.assertEqual("Remote Name", params["open"]["name"])
        self.assertEqual(opened, await self.broker.call("agent_open", args))
        self.assertEqual(1, len(self.client.named("deliver")))

    async def test_remote_open_treats_explicit_none_name_as_omitted(self):
        self.client.deliver_results.append(
            {
                "delivery_id": "delivery-none",
                "status": "accepted_remote",
                "session_id": "remote-none",
                "target_seq": 9,
            }
        )
        args = {"host_id": "gpu-a", "profile": "fixed", "cwd": str(self.state), "request_id": "open-none"}
        first = await self.broker.call("agent_open", args)
        self.assertEqual(first, await self.broker.call("agent_open", {**args, "name": None}))
        [params] = self.client.named("deliver")
        self.assertNotIn("name", params["open"])
        self.assertEqual(1, len(self.client.named("deliver")))

    async def test_remote_dispatch_opens_atomically_or_targets_an_existing_session(self):
        self.client.deliver_results.append(
            {
                "delivery_id": "delivery-dispatch",
                "status": "accepted_remote",
                "session_id": "new-session",
                "task_id": "new-task",
            }
        )
        dispatched = await self.broker.call(
            "agent_dispatch",
            {
                "host_id": "gpu-a",
                "profile": "fixed",
                "cwd": str(self.state),
                "instruction": "run the tests",
                "request_id": "dispatch-1",
            },
        )
        self.assertEqual("new-task", dispatched["task_id"])
        [params] = self.client.named("deliver")
        self.assertEqual("run the tests", params["message"])
        self.assertEqual("fixed", params["open"]["profile"])
        self.assertEqual("", params["target"]["id"])

        self.client.deliver_results.append(
            {
                "delivery_id": "delivery-existing",
                "status": "accepted_remote",
                "session_id": "existing-session",
                "task_id": "existing-task",
            }
        )
        await self.broker.call(
            "agent_dispatch",
            {
                "host_id": "gpu-a",
                "session_id": "existing-session",
                "instruction": "run again",
                "request_id": "dispatch-2",
            },
        )
        params = self.client.named("deliver")[-1]
        self.assertEqual({"kind": "bridge", "id": "existing-session"}, params["target"])
        self.assertNotIn("open", params)
        self.assertEqual("run again", params["message"])

        with self.assertRaises(BridgeError) as caught:
            await self.broker.call(
                "agent_dispatch",
                {"host_id": "gpu-a", "instruction": "no profile", "request_id": "dispatch-3"},
            )
        self.assertEqual("invalid_request", caught.exception.code)

        with self.assertRaises(BridgeError) as caught:
            await self.broker.call(
                "agent_dispatch",
                {
                    "host_id": "gpu-a",
                    "profile": "fixed",
                    "cwd": str(self.state),
                    "instruction": "budgeted",
                    "request_id": "dispatch-4",
                    "requirements": {"hard_budget": True},
                },
            )
        self.assertEqual("hard_budget_unsupported", caught.exception.code)
        self.assertEqual(2, len(self.client.named("deliver")))

    async def test_remote_cancel_routes_by_task_operation_or_delivery(self):
        self.client.cancel_results.append({"target": "remote-task", "status": "cancelled"})
        by_task = await self.broker.call(
            "agent_cancel",
            {"host_id": "gpu-a", "task_id": "remote-task", "request_id": "cancel-task"},
        )
        self.assertEqual("cancelled", by_task["status"])
        self.assertEqual("remote-task", by_task["target"])
        self.assertEqual("gpu-a", by_task["host_id"])
        [params] = self.client.named("cancel")
        self.assertEqual("remote-task", params["task_id"])
        self.assertEqual("gpu-a", params["host_id"])
        self.assertEqual("generation-a", params["operation"]["state_generation"])

        replayed = await self.broker.call(
            "agent_cancel",
            {"host_id": "gpu-a", "task_id": "remote-task", "request_id": "cancel-task"},
        )
        self.assertEqual(by_task, replayed)
        self.assertEqual(1, len(self.client.named("cancel")))

        self.client.cancel_results.append(
            {"target": "delivery-9", "status": "cancelled", "tombstone_seq": 9}
        )
        by_delivery = await self.broker.call(
            "agent_cancel",
            {"host_id": "gpu-a", "delivery_id": "delivery-9", "request_id": "cancel-delivery"},
        )
        self.assertEqual("delivery-9", by_delivery["target"])
        self.assertEqual(9, by_delivery["tombstone_seq"])
        self.assertEqual("delivery-9", self.client.named("cancel")[-1]["delivery_id"])

        for args in (
            {"host_id": "gpu-a"},
            {"host_id": "gpu-a", "task_id": "a", "delivery_id": "b"},
        ):
            with self.subTest(args=args), self.assertRaises(BridgeError) as caught:
                await self.broker.call("agent_cancel", args)
            self.assertEqual("invalid_request", caught.exception.code)

    async def test_local_cancel_does_not_touch_the_peer_client(self):
        session_id = self.broker.store.open(
            self.broker.config.profiles["fixed"], str(self.state), "read-only", "local-open"
        )["session_id"]
        task_id = self.broker.store.dispatch(session_id, "local work", "local-dispatch")["task_id"]

        result = await self.broker.call(
            "agent_cancel", {"task_id": task_id, "request_id": "local-cancel"}
        )

        self.assertEqual(task_id, result["task_id"])
        self.assertEqual("cancelled", self.broker.store.get_task(task_id)["status"])
        self.assertEqual([], self.client.named("cancel"))

    async def test_agent_operation_refreshes_remote_state_and_reports_errors(self):
        self.broker.store.record_operation(
            "remote-op",
            "node-a",
            "generation-a",
            "fingerprint",
            "agent_dispatch",
            {"host_id": "gpu-a"},
            status="pending",
        )
        self.client.lookup_results.append(
            {
                "found": True,
                "operation_status": "accepted_remote",
                "delivery": {
                    "delivery_id": "delivery-x",
                    "task_id": "task-x",
                    "session_id": "session-x",
                },
            }
        )
        view = await self.broker.call("agent_operation", {"operation_id": "remote-op"})
        self.assertEqual("completed", view["status"])
        self.assertEqual("gpu-a", view["host_id"])
        self.assertEqual("task-x", view["result"]["task_id"])
        [lookup] = self.client.named("lookup_delivery")
        self.assertEqual(
            {
                "node_id": "node-a",
                "state_generation": "generation-a",
                "operation_id": "remote-op",
            },
            lookup["operation"],
        )

        self.broker.store.record_operation(
            "waiting-op", "node-a", "generation-a", "fingerprint", "agent_dispatch",
            {"host_id": "gpu-a"}, status="pending",
        )
        self.client.lookup_results.append({"found": False})
        waiting = await self.broker.call(
            "agent_operation", {"operation_id": "waiting-op", "wait": 0}
        )
        self.assertEqual("pending", waiting["status"])

        self.broker.store.record_operation(
            "failed-op", "node-a", "generation-a", "fingerprint", "agent_dispatch", {},
            status="failed", error={"code": "peer_error", "message": "boom"},
        )
        failed = await self.broker.call("agent_operation", {"operation_id": "failed-op"})
        self.assertEqual("failed", failed["status"])
        self.assertEqual("boom", failed["error"]["message"])

        with self.assertRaises(BridgeError) as caught:
            await self.broker.call("agent_operation", {"operation_id": "missing"})
        self.assertEqual(PeerErrorCode.OPERATION_NOT_FOUND, caught.exception.code)
        with self.assertRaises(BridgeError) as caught:
            await self.broker.call("agent_operation", {"operation_id": "remote-op", "wait": -1})
        self.assertEqual("invalid_request", caught.exception.code)


if __name__ == "__main__":
    unittest.main()
