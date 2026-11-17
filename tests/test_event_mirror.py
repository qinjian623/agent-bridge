import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_bridge.broker import Broker, EventMirror
from agent_bridge.config import Config, Profile
from agent_bridge.hosts import HostConfig
from agent_bridge.node_identity import FileNodeIdentityStore
from agent_bridge.notifications import deliver
from agent_bridge.peer_api import DirectPeerService
from agent_bridge.peer_protocol import PeerErrorCode, unwrap_peer_event
from agent_bridge.store import Store, encode

HOST_A = "gpu-a"
NODE_A = "node-a"
GEN_A = "gen-a"
HOST_B = "gpu-b"
NODE_B = "node-b"
GEN_B = "gen-b"


class FakeHostRegistry:
    def __init__(self, configs):
        self.configs = {config.host_id: config for config in configs}

    def get(self, host_id):
        return self.configs[host_id]

    def list(self):
        return list(self.configs.values())


class FakeClient:
    def __init__(self, node, events=()):
        self.node = node
        self.events_response = list(events)
        self.event_params = []
        self.notified = []

    async def hello(self, params):
        return {"node": self.node}

    async def events(self, params):
        self.event_params.append(dict(params))
        if not self.events_response:
            return {"items": [], "cursor": params["after"], "has_more": False}
        result = self.events_response.pop(0)
        return result(params) if callable(result) else result

    async def notify_parent(self, params):
        self.notified.append(dict(params))
        return {"mirror_seq": 99, "duplicate": False}


class FakeFactory:
    def __init__(self, clients):
        self.clients = clients

    def client(self, host_id):
        return self.clients[host_id]


def host_config(host_id, node_id, generation):
    return HostConfig(
        host_id=host_id,
        ssh_target=host_id + ".internal",
        remote_executable="/usr/local/bin/agent-bridge",
        pinned=None,
    )


def event(host_id=HOST_A, node_id=NODE_A, generation=GEN_A, seq=18, local_id="remote-task",
          event_type="task.completed", payload=None):
    return {
        "seq": seq,
        "type": event_type,
        "ref": {
            "host_id": host_id,
            "node_id": node_id,
            "state_generation": generation,
            "local_id": local_id,
        },
        "payload": payload if payload is not None else {"status": "completed"},
        "created": 1.0,
    }


def fingerprint(value):
    return hashlib.sha256(encode(value).encode("utf-8")).hexdigest()


class MirrorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.store = Store(self.root)
        self.identity = FileNodeIdentityStore(self.root)
        self.local_node = self.identity.node_ref("local")
        self.registry = FakeHostRegistry([
            host_config(HOST_A, NODE_A, GEN_A),
            host_config(HOST_B, NODE_B, GEN_B),
        ])

    def mirror(self, clients):
        return EventMirror(
            self.store, FakeFactory(clients), self.registry,
            Config({}, codex_async_python="fixture"),
        )

    def local_parent(self, parent_id="parent-1", native_id="native-parent"):
        self.store.parent_control(parent_id, "parent_register", 0, "register-" + parent_id, native_id, str(self.root))
        return {
            "host_id": self.local_node.host_id,
            "node_id": self.local_node.node_id,
            "state_generation": self.local_node.state_generation,
            "local_id": parent_id,
        }

    def remote_parent(self, parent_id="remote-parent"):
        return {
            "host_id": HOST_B,
            "node_id": NODE_B,
            "state_generation": GEN_B,
            "local_id": parent_id,
        }


class EventMirrorTests(MirrorTestCase):
    async def test_local_parent_route_mirrors_event_and_delivers_notification(self):
        parent_ref = self.local_parent()
        self.store.register_parent_route(HOST_A, NODE_A, GEN_A, "remote-task", parent_ref)
        item = event()
        client = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [item], "cursor": 18, "has_more": False},
        ])
        mirror = self.mirror({HOST_A: client})

        processed = await mirror.run_once(HOST_A)

        self.assertEqual(1, processed)
        self.assertEqual([{"after": 0, "limit": 100}], client.event_params)
        self.assertEqual(18, self.store.get_peer_event_cursor(HOST_A, NODE_A, GEN_A))
        mirrored = self.store.get_mirrored_event(HOST_A, NODE_A, GEN_A, 18)
        self.assertEqual(fingerprint(item), mirrored["fingerprint"])
        with self.store.connection() as db:
            local = dict(db.execute("SELECT * FROM events WHERE seq=?", (mirrored["local_seq"],)).fetchone())
            notification = dict(
                db.execute("SELECT * FROM mirrored_parent_notifications WHERE local_seq=?", (mirrored["local_seq"],)).fetchone()
            )
            delivery = dict(
                db.execute("SELECT * FROM deliveries WHERE event_seq=?", (mirrored["local_seq"],)).fetchone()
            )
        self.assertEqual("task.completed", local["kind"])
        data = json.loads(local["data"])
        self.assertEqual(item["ref"], data["source"])
        self.assertEqual(18, data["remote_seq"])
        self.assertEqual(parent_ref, json.loads(notification["parent_ref"]))
        self.assertEqual("received", notification["status"])
        self.assertEqual("parent-1", delivery["parent_id"])
        self.assertEqual("pending", delivery["status"])

        # The same delivery function used for locally produced events consumes the mirrored notification.
        calls = []

        async def sender(config, parent, message, event_seq, receipt=None):
            calls.append(event_seq)
            return {"receipt": "submission", "ready": True, "native_status": "received", "native_turn_id": "turn-1"}

        await deliver(self.store, Config({}, codex_async_python="fixture"), sender)
        self.assertEqual([mirrored["local_seq"]], calls)

    async def test_replayed_event_is_deduplicated(self):
        parent_ref = self.local_parent()
        self.store.register_parent_route(HOST_A, NODE_A, GEN_A, "remote-task", parent_ref)
        item = event()
        response = {"items": [item], "cursor": 18, "has_more": False}
        client = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [response, response])
        mirror = self.mirror({HOST_A: client})

        await mirror.run_once(HOST_A)
        await mirror.run_once(HOST_A)

        with self.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM mirrored_parent_notifications").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0])

    async def test_mirrored_notices_name_the_remote_host_task_and_outcome(self):
        parent_ref = self.local_parent()
        cases = [
            (18, "remote-failed", "task_finished", {"status": "failed", "validation": "pending"},
             ["Remote child task gpu-a/remote-failed", "failed", "agent_result"], ["agent_review"]),
            (19, "remote-completed", "task_finished", {"status": "completed"},
             ["Remote child task gpu-a/remote-completed", "completed", "agent_result", "agent_review"], []),
            (20, "remote-recovery", "task_recovery_needed",
             {"status": "recovery_needed", "error": "connection lost"},
             ["Remote child task gpu-a/remote-recovery", "recovery_needed", "resolve"], ["agent_review"]),
            (21, "remote-start", "task_start_failed", {"status": "queued", "error": "CLI unavailable"},
             ["Remote child task gpu-a/remote-start", "has not started", "agent_inspect", "resume"],
             ["agent_review"]),
        ]
        items = []
        for seq, child, kind, payload, _, _ in cases:
            self.store.register_parent_route(HOST_A, NODE_A, GEN_A, child, parent_ref)
            items.append(event(seq=seq, local_id=child, event_type=kind, payload=payload))
        client = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": items, "cursor": 21, "has_more": False},
        ])
        await self.mirror({HOST_A: client}).run_once(HOST_A)

        messages = {}

        async def sender(config, parent, message, event_seq, receipt=None):
            messages[event_seq] = message
            return {"ready": True, "receipt": "sent", "native_status": "received", "native_turn_id": "turn-1"}

        await deliver(self.store, Config({}, codex_async_python="fixture"), sender)
        for seq, _, _, _, includes, excludes in cases:
            mirrored = self.store.get_mirrored_event(HOST_A, NODE_A, GEN_A, seq)
            message = messages[mirrored["local_seq"]]
            for text in includes:
                self.assertIn(text, message)
            for text in excludes:
                self.assertNotIn(text, message)
            self.assertNotIn("None", message)

    async def test_local_and_mirrored_notices_keep_their_formats_and_deliver_once(self):
        parent_ref = self.local_parent()
        profile = Profile("local", "codex", "fixed")
        session_id = self.store.open(
            profile, str(self.root), "read-only", "local-open", parent="parent-1",
        )["session_id"]
        self.store.worker_ready(session_id, "g", 123, "start", "native-local")
        task_id = self.store.dispatch(session_id, "local work", "local-dispatch")["task_id"]
        config = Config({"local": profile}, codex_async_python="fixture")
        self.store.claim(config)
        attempt_id = self.store.begin_attempt(session_id, "g")["active_attempt"]
        self.store.finish(session_id, "g", task_id, attempt_id, "completed", {"answer": "done"})

        self.store.register_parent_route(HOST_A, NODE_A, GEN_A, "remote-task", parent_ref)
        item = event(seq=18, local_id="remote-task", event_type="task_finished",
                     payload={"status": "failed", "validation": "pending"})
        client = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [item], "cursor": 18, "has_more": False},
        ])
        await self.mirror({HOST_A: client}).run_once(HOST_A)

        calls = []

        async def sender(config, parent, message, event_seq, receipt=None):
            calls.append((event_seq, message))
            return {"ready": True, "receipt": "sent", "native_status": "received", "native_turn_id": "turn-1"}

        await deliver(self.store, config, sender)
        await deliver(self.store, config, sender)
        self.assertEqual(2, len(calls))
        messages = dict(calls)
        local_seq = self.store.events(task_id=task_id)[-1]["seq"]
        mirrored = self.store.get_mirrored_event(HOST_A, NODE_A, GEN_A, 18)
        self.assertIn(f"Child task {task_id} has completed", messages[local_seq])
        self.assertIn("Remote child task gpu-a/remote-task", messages[mirrored["local_seq"]])
        self.assertIn("failed", messages[mirrored["local_seq"]])

    async def test_remote_parent_route_calls_notify_parent_on_that_host(self):
        parent_ref = self.remote_parent()
        self.store.register_parent_route(HOST_A, NODE_A, GEN_A, "remote-task", parent_ref)
        item = event()
        source = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [item], "cursor": 18, "has_more": False},
        ])
        parent = FakeClient({"node_id": NODE_B, "state_generation": GEN_B})
        mirror = self.mirror({HOST_A: source, HOST_B: parent})

        processed = await mirror.run_once(HOST_A)

        self.assertEqual(1, processed)
        self.assertEqual(1, len(parent.notified))
        params = parent.notified[0]
        self.assertEqual(parent_ref, params["parent_ref"])
        self.assertEqual(item, params["event"])
        self.assertEqual(NODE_B, params["operation"]["node_id"])
        self.assertEqual(GEN_B, params["operation"]["state_generation"])
        mirrored = self.store.get_mirrored_event(HOST_A, NODE_A, GEN_A, 18)
        with self.store.connection() as db:
            row = db.execute(
                "SELECT * FROM mirrored_parent_notifications WHERE local_seq=?", (mirrored["local_seq"],)
            ).fetchone()
        self.assertEqual("sent", row["status"])
        self.assertEqual(parent_ref, json.loads(row["parent_ref"]))
        self.assertEqual(18, self.store.get_peer_event_cursor(HOST_A, NODE_A, GEN_A))

    async def test_fingerprint_conflict_pauses_the_source_without_overwriting(self):
        with self.store.connection(write=True) as db:
            local_seq = db.execute("INSERT INTO events(kind,data,created) VALUES('old','{}',1)").lastrowid
        self.store.mirror_peer_event(HOST_A, NODE_A, GEN_A, 18, "stored-fingerprint", local_seq)
        item = event()
        client = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [item], "cursor": 18, "has_more": False},
        ])
        mirror = self.mirror({HOST_A: client})

        processed = await mirror.run_once(HOST_A)
        second = await mirror.run_once(HOST_A)

        self.assertEqual(0, processed)
        self.assertEqual(0, second)
        self.assertIn(HOST_A, mirror._paused)
        self.assertEqual(0, self.store.get_peer_event_cursor(HOST_A, NODE_A, GEN_A))
        with self.store.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM events WHERE kind='task.completed'").fetchone()[0])
            conflict = db.execute("SELECT data FROM events WHERE kind='peer_event_conflict'").fetchone()
        self.assertIsNotNone(conflict)
        self.assertEqual(18, json.loads(conflict["data"])["remote_seq"])

    async def test_same_remote_seq_on_two_nodes_maps_to_distinct_local_events(self):
        item_a = event(host_id=HOST_A, node_id=NODE_A, generation=GEN_A, seq=5, local_id="task-a")
        item_b = event(host_id=HOST_B, node_id=NODE_B, generation=GEN_B, seq=5, local_id="task-b")
        client_a = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [item_a], "cursor": 5, "has_more": False},
        ])
        client_b = FakeClient({"node_id": NODE_B, "state_generation": GEN_B}, [
            {"items": [item_b], "cursor": 5, "has_more": False},
        ])
        mirror = self.mirror({HOST_A: client_a, HOST_B: client_b})

        await mirror.run_once(HOST_A)
        await mirror.run_once(HOST_B)

        first = self.store.get_mirrored_event(HOST_A, NODE_A, GEN_A, 5)
        second = self.store.get_mirrored_event(HOST_B, NODE_B, GEN_B, 5)
        self.assertNotEqual(first["local_seq"], second["local_seq"])
        self.assertEqual(5, self.store.get_peer_event_cursor(HOST_A, NODE_A, GEN_A))
        self.assertEqual(5, self.store.get_peer_event_cursor(HOST_B, NODE_B, GEN_B))

    async def test_events_without_a_parent_route_do_not_create_deliveries(self):
        item = event()
        client = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [item], "cursor": 18, "has_more": False},
        ])
        await self.mirror({HOST_A: client}).run_once(HOST_A)
        with self.store.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0])
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM mirrored_parent_notifications").fetchone()[0])
        self.assertEqual(18, self.store.get_peer_event_cursor(HOST_A, NODE_A, GEN_A))

    async def test_conflict_on_later_event_keeps_cursor_at_last_good_event(self):
        good = event(seq=1, local_id="task-1")
        bad = event(seq=2, local_id="task-2")
        with self.store.connection(write=True) as db:
            existing_seq = db.execute("INSERT INTO events(kind,data,created) VALUES('old','{}',1)").lastrowid
        self.store.mirror_peer_event(HOST_A, NODE_A, GEN_A, 2, "different", existing_seq)
        client = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [good, bad], "cursor": 2, "has_more": False},
        ])
        await self.mirror({HOST_A: client}).run_once(HOST_A)
        self.assertEqual(1, self.store.get_peer_event_cursor(HOST_A, NODE_A, GEN_A))

    async def test_reexported_own_event_from_unpatched_peer_is_ignored(self):
        echoed = {
            "seq": 50,
            "type": "task_finished",
            "ref": {
                "host_id": HOST_A,
                "node_id": NODE_A,
                "state_generation": GEN_A,
                "local_id": "50",
            },
            "payload": {
                "source": dict(self.local_node.as_dict(), local_id="local-task"),
                "remote_seq": 7,
                "type": "task_finished",
                "payload": {"status": "completed"},
                "created": 1.0,
            },
            "created": 2.0,
        }
        client = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [echoed], "cursor": 50, "has_more": False},
        ])
        processed = await self.mirror({HOST_A: client}).run_once(HOST_A)
        self.assertEqual(1, processed)
        self.assertEqual(50, self.store.get_peer_event_cursor(HOST_A, NODE_A, GEN_A))
        with self.store.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])

    async def test_same_origin_event_arriving_with_new_sender_and_seq_is_deduplicated(self):
        parent_ref = self.local_parent()
        self.store.register_parent_route(HOST_A, NODE_A, GEN_A, "remote-task", parent_ref)
        origin = event(host_id=HOST_A, node_id=NODE_A, generation=GEN_A, seq=18, local_id="remote-task")
        reexported_by_b = {
            "seq": 50,
            "type": origin["type"],
            "ref": {"host_id": HOST_B, "node_id": NODE_B, "state_generation": GEN_B, "local_id": "50"},
            "payload": {
                "source": origin["ref"],
                "remote_seq": origin["seq"],
                "type": origin["type"],
                "payload": origin["payload"],
                "created": origin["created"],
            },
            "created": 9.0,
        }
        client_a = FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [
            {"items": [origin], "cursor": 18, "has_more": False},
        ])
        client_b = FakeClient({"node_id": NODE_B, "state_generation": GEN_B}, [
            {"items": [reexported_by_b], "cursor": 50, "has_more": False},
        ])
        mirror = self.mirror({HOST_A: client_a, HOST_B: client_b})
        await mirror.run_once(HOST_A)
        await mirror.run_once(HOST_B)
        self.assertEqual(50, self.store.get_peer_event_cursor(HOST_B, NODE_B, GEN_B))
        with self.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0])

    async def test_migration_backfills_origin_columns_and_tolerates_historical_duplicates(self):
        origin = event(host_id=HOST_A, node_id=NODE_A, generation=GEN_A, seq=18, local_id="remote-task")
        wrapped_payload = {
            "source": origin["ref"],
            "remote_seq": origin["seq"],
            "type": origin["type"],
            "payload": origin["payload"],
            "created": origin["created"],
        }
        with self.store.connection(write=True) as db:
            s1 = self.store.event(db, origin["type"], wrapped_payload)
            s2 = self.store.event(db, origin["type"], {"source": {"host_id": HOST_B, "node_id": NODE_B, "state_generation": GEN_B, "local_id": "50"}, "remote_seq": 50, "type": origin["type"], "payload": wrapped_payload, "created": 9.0})
            db.execute(
                "INSERT INTO peer_event_mirrors(host_id,node_id,state_generation,remote_seq,local_seq,fingerprint,created) VALUES(?,?,?,?,?,?,?)",
                (HOST_A, NODE_A, GEN_A, 18, s1, "fp-a", 1.0),
            )
            db.execute(
                "INSERT INTO peer_event_mirrors(host_id,node_id,state_generation,remote_seq,local_seq,fingerprint,created) VALUES(?,?,?,?,?,?,?)",
                (HOST_B, NODE_B, GEN_B, 50, s2, "fp-b", 2.0),
            )
        reopened = Store(self.root)
        reexported_by_c = {
            "seq": 77,
            "type": origin["type"],
            "ref": {"host_id": "gpu-c", "node_id": "node-c", "state_generation": "gen-c", "local_id": "77"},
            "payload": wrapped_payload,
            "created": 10.0,
        }
        self.registry.configs["gpu-c"] = host_config("gpu-c", "node-c", "gen-c")
        client_c = FakeClient({"node_id": "node-c", "state_generation": "gen-c"}, [
            {"items": [reexported_by_c], "cursor": 77, "has_more": False},
        ])
        await EventMirror(reopened, FakeFactory({"gpu-c": client_c}), self.registry, Config({}, codex_async_python="fixture")).run_once("gpu-c")
        with reopened.connection() as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    async def test_wrapped_parent_route_resolves_by_node_when_host_alias_differs(self):
        parent_ref = self.local_parent()
        self.store.register_parent_route(HOST_A, NODE_A, GEN_A, "remote-task", parent_ref)
        origin = event(host_id="alias-on-relay", node_id=NODE_A, generation=GEN_A, seq=18, local_id="remote-task")
        wrapped = {
            "seq": 5,
            "type": origin["type"],
            "ref": {"host_id": HOST_B, "node_id": NODE_B, "state_generation": GEN_B, "local_id": "relay-task"},
            "payload": {
                "source": origin["ref"],
                "remote_seq": origin["seq"],
                "type": origin["type"],
                "payload": origin["payload"],
                "created": origin["created"],
            },
            "created": 200.0,
        }
        client = FakeClient({"node_id": NODE_B, "state_generation": GEN_B}, [{"items": [wrapped], "cursor": 5, "has_more": False}])
        mirror = self.mirror({HOST_B: client})

        processed = await mirror.run_once(HOST_B)

        self.assertEqual(1, processed)
        with self.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM mirrored_parent_notifications").fetchone()[0])

    async def test_rotate_generation_ignores_own_previous_generation_events(self):
        old_gen_event = event(host_id=HOST_A, node_id=self.local_node.node_id, generation="old-gen", seq=4)
        client = FakeClient(
            {"node_id": self.local_node.node_id, "state_generation": "old-gen"},
            [{"items": [old_gen_event], "cursor": 4, "has_more": False}],
        )
        mirror = self.mirror({HOST_A: client})

        processed = await mirror.run_once(HOST_A)

        self.assertEqual(1, processed)
        self.assertEqual(4, self.store.get_peer_event_cursor(HOST_A, self.local_node.node_id, "old-gen"))
        with self.store.connection() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])

    async def test_corrupt_wrapped_relay_before_direct_origin_pauses_relay_not_origin(self):
        origin_tampered = event(payload={"status": "tampered"})
        wrapped_bad = {
            "seq": 9,
            "type": origin_tampered["type"],
            "ref": {"host_id": HOST_B, "node_id": NODE_B, "state_generation": GEN_B, "local_id": "relay-task"},
            "payload": {
                "source": origin_tampered["ref"],
                "remote_seq": origin_tampered["seq"],
                "type": origin_tampered["type"],
                "payload": origin_tampered["payload"],
                "created": origin_tampered["created"],
            },
            "created": 201.0,
        }
        direct_good = event(payload={"status": "completed"})
        mirror = self.mirror({
            HOST_B: FakeClient({"node_id": NODE_B, "state_generation": GEN_B}, [{"items": [wrapped_bad], "cursor": 9, "has_more": False}]),
            HOST_A: FakeClient({"node_id": NODE_A, "state_generation": GEN_A}, [{"items": [direct_good], "cursor": 18, "has_more": False}]),
        })

        self.assertEqual(1, await mirror.run_once(HOST_B))
        self.assertEqual(1, await mirror.run_once(HOST_A))

        self.assertIn(HOST_B, mirror._paused)
        self.assertNotIn(HOST_A, mirror._paused)
        self.assertEqual(0, await mirror.run_once(HOST_B))
        with self.store.connection() as db:
            conflict = db.execute("SELECT data FROM events WHERE kind='peer_event_conflict'").fetchone()
            self.assertIsNotNone(conflict)
            direct_mirror = self.store.get_mirrored_event(HOST_A, NODE_A, GEN_A, 18)
            self.assertIsNotNone(direct_mirror)
            self.assertEqual(fingerprint(direct_good), direct_mirror["origin_fingerprint"])

    async def test_migration_backfills_non_wrapped_origin_fingerprint(self):
        with self.store.connection(write=True) as db:
            local_seq = self.store.event(db, "task.completed", {"summary": "done"})
            self.store.mirror_peer_event(
                HOST_A, NODE_A, GEN_A, 18, "legacy-fp-1", local_seq,
                origin_node_id=NODE_A, origin_state_generation=GEN_A, origin_seq=18, origin_fingerprint=None, db=db,
            )
            db.execute(
                "UPDATE peer_event_mirrors SET origin_fingerprint=NULL WHERE host_id=? AND remote_seq=?",
                (HOST_A, 18),
            )

        reopened = Store(self.root)
        mirrored = reopened.get_mirrored_event(HOST_A, NODE_A, GEN_A, 18)
        self.assertIsNotNone(mirrored)
        self.assertEqual("legacy-fp-1", mirrored["origin_fingerprint"])

    async def test_migration_backfills_non_wrapped_legacy_row_with_null_origin_columns(self):
        with self.store.connection(write=True) as db:
            local_seq = self.store.event(db, "task_completed", {"summary": "done"})
            db.execute(
                "INSERT INTO peer_event_mirrors("
                "host_id,node_id,state_generation,remote_seq,local_seq,fingerprint,created"
                ") VALUES(?,?,?,?,?,?,?)",
                (HOST_A, NODE_A, GEN_A, 19, local_seq, "legacy-fp-direct", 5.0),
            )

        reopened = Store(self.root)
        mirrored = reopened.get_mirrored_event_by_origin(NODE_A, GEN_A, 19)
        self.assertIsNotNone(mirrored)
        self.assertEqual("legacy-fp-direct", mirrored["origin_fingerprint"])

    async def test_migration_runs_bounded_batches_and_finishes_in_broker_backfill(self):
        with self.store.connection(write=True) as db:
            seqs = []
            for remote_seq in range(1, 8):
                local_seq = self.store.event(db, "task_completed", {"summary": f"item-{remote_seq}"})
                db.execute(
                    "INSERT INTO peer_event_mirrors("
                    "host_id,node_id,state_generation,remote_seq,local_seq,fingerprint,created"
                    ") VALUES(?,?,?,?,?,?,?)",
                    (HOST_A, NODE_A, GEN_A, remote_seq, local_seq, f"fp-{remote_seq}", float(remote_seq)),
                )
                seqs.append(remote_seq)

        (self.root / "config.json").write_text('{"profiles": {}}\n')
        with patch.object(Store, "PEER_MIRROR_MIGRATION_BATCH", 3):
            reopened = Store(self.root)
            with reopened.connection() as db:
                done = db.execute(
                    "SELECT remote_seq FROM peer_event_mirrors WHERE origin_node_id IS NOT NULL ORDER BY remote_seq"
                ).fetchall()
                pending = db.execute(
                    "SELECT COUNT(*) FROM peer_event_mirrors WHERE origin_node_id IS NULL"
                ).fetchone()[0]
            self.assertEqual([5, 6, 7], [row[0] for row in done])
            self.assertEqual(4, pending)

            broker = Broker(self.root)
            await broker.mirror_backfill()
            with broker.store.connection() as db:
                remaining = db.execute(
                    "SELECT COUNT(*) FROM peer_event_mirrors WHERE origin_node_id IS NULL OR origin_fingerprint IS NULL"
                ).fetchone()[0]
            self.assertEqual(0, remaining)
            for remote_seq in seqs:
                mirrored = broker.store.get_mirrored_event_by_origin(NODE_A, GEN_A, remote_seq)
                self.assertIsNotNone(mirrored)
                self.assertEqual(f"fp-{remote_seq}", mirrored["origin_fingerprint"])

    async def test_unwrap_peer_event_handles_deep_historical_envelopes(self):
        current = event(host_id=HOST_A, node_id=NODE_A, generation=GEN_A, seq=18, payload={"status": "origin"})
        for hop in range(1, 46):
            current = {
                "seq": 100 + hop,
                "type": current["type"],
                "ref": {"host_id": f"relay-{hop}", "node_id": f"node-{hop}", "state_generation": "gen-r", "local_id": str(hop)},
                "payload": {
                    "source": current["ref"],
                    "remote_seq": current["seq"],
                    "type": current["type"],
                    "payload": current["payload"],
                    "created": current["created"],
                },
                "created": 100.0 + hop,
            }
        origin_event, wrapped = unwrap_peer_event(current)
        self.assertTrue(wrapped)
        self.assertEqual(18, origin_event["seq"])
        self.assertEqual(NODE_A, origin_event["ref"]["node_id"])
        self.assertEqual({"status": "origin"}, origin_event["payload"])


class NotifyParentHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.store = Store(self.root)
        self.identity = FileNodeIdentityStore(self.root)
        self.service = DirectPeerService(self.identity, host_id=HOST_B, store=self.store)
        self.store.parent_control("parent-1", "parent_register", 0, "register", "native-parent", str(self.root))

    def operation(self, operation_id="notify-1"):
        identity = self.identity.load_or_create()
        return {
            "node_id": identity.node_id,
            "state_generation": identity.state_generation,
            "operation_id": operation_id,
        }

    def params(self, **changes):
        parent_ref = {
            "host_id": HOST_B, "node_id": NODE_B, "state_generation": GEN_B, "local_id": "parent-1",
        }
        values = {
            "operation": self.operation(),
            "parent_ref": parent_ref,
            "event": event(host_id=HOST_A, node_id=NODE_A, generation=GEN_A),
        }
        values.update(changes)
        return values

    async def test_notify_parent_is_deduplicated_by_operation_and_source(self):
        first = await self.service.handle(
            {"v": 1, "id": "1", "method": "peer.notify_parent", "params": self.params()}
        )
        self.assertTrue(first["ok"], first)
        self.assertFalse(first["result"]["duplicate"])
        replay = await self.service.handle(
            {"v": 1, "id": "2", "method": "peer.notify_parent", "params": self.params()}
        )
        self.assertTrue(replay["ok"], replay)
        self.assertTrue(replay["result"]["duplicate"])
        self.assertEqual(first["result"]["mirror_seq"], replay["result"]["mirror_seq"])
        with self.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM mirrored_parent_notifications").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])

    async def test_notify_parent_conflicts_for_a_different_event_with_the_same_operation(self):
        await self.service.handle(
            {"v": 1, "id": "1", "method": "peer.notify_parent", "params": self.params()}
        )
        conflicting = await self.service.handle(
            {
                "v": 1,
                "id": "2",
                "method": "peer.notify_parent",
                "params": self.params(event=event(seq=19)),
            }
        )
        self.assertFalse(conflicting["ok"])
        self.assertEqual(PeerErrorCode.OPERATION_CONFLICT, conflicting["error"]["code"])

    async def test_notify_parent_deduplicates_by_source_with_a_different_operation(self):
        first = await self.service.handle(
            {"v": 1, "id": "1", "method": "peer.notify_parent", "params": self.params()}
        )
        replay = await self.service.handle(
            {
                "v": 1,
                "id": "2",
                "method": "peer.notify_parent",
                "params": self.params(operation=self.operation("notify-2")),
            }
        )
        self.assertTrue(replay["ok"], replay)
        self.assertTrue(replay["result"]["duplicate"])
        self.assertEqual(first["result"]["mirror_seq"], replay["result"]["mirror_seq"])

    async def test_notify_parent_refuses_to_route_one_event_to_two_parents(self):
        self.store.parent_control("parent-2", "parent_register", 0, "register-2", "native-parent-2", str(self.root))
        await self.service.handle(
            {"v": 1, "id": "1", "method": "peer.notify_parent", "params": self.params()}
        )
        params = self.params(
            operation=self.operation("notify-2"),
            parent_ref={
                "host_id": HOST_B, "node_id": NODE_B, "state_generation": GEN_B, "local_id": "parent-2",
            },
        )
        response = await self.service.handle(
            {"v": 1, "id": "2", "method": "peer.notify_parent", "params": params}
        )
        self.assertFalse(response["ok"])
        self.assertEqual(PeerErrorCode.PARENT_ROUTE_CONFLICT, response["error"]["code"])

    async def test_notify_parent_rejects_an_unregistered_local_parent(self):
        params = self.params(
            parent_ref={
                "host_id": HOST_B, "node_id": NODE_B, "state_generation": GEN_B, "local_id": "missing",
            }
        )
        response = await self.service.handle(
            {"v": 1, "id": "1", "method": "peer.notify_parent", "params": params}
        )
        self.assertFalse(response["ok"])
        self.assertEqual(PeerErrorCode.OPERATION_NOT_FOUND, response["error"]["code"])

    async def test_notify_parent_deduplicates_renumbered_wrapped_origin_event(self):
        first = await self.service.handle(
            {"v": 1, "id": "1", "method": "peer.notify_parent", "params": self.params()}
        )
        self.assertTrue(first["ok"], first)
        origin = event(host_id=HOST_A, node_id=NODE_A, generation=GEN_A)
        wrapped = {
            "seq": 50,
            "type": origin["type"],
            "ref": {"host_id": "gpu-c", "node_id": "node-c", "state_generation": "gen-c", "local_id": "50"},
            "payload": {
                "source": origin["ref"],
                "remote_seq": origin["seq"],
                "type": origin["type"],
                "payload": origin["payload"],
                "created": origin["created"],
            },
            "created": 5.0,
        }
        replay = await self.service.handle(
            {
                "v": 1,
                "id": "2",
                "method": "peer.notify_parent",
                "params": self.params(operation=self.operation("notify-2"), event=wrapped),
            }
        )
        self.assertTrue(replay["ok"], replay)
        self.assertTrue(replay["result"]["duplicate"])
        self.assertEqual(first["result"]["mirror_seq"], replay["result"]["mirror_seq"])
        with self.store.connection() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM peer_event_mirrors").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0])

    async def test_wrapped_notify_parent_replays_operation_id_and_rejects_content_mismatch(self):
        origin = event(host_id=HOST_A, node_id=NODE_A, generation=GEN_A, seq=18)
        wrapped = {
            "seq": 5,
            "type": origin["type"],
            "ref": {"host_id": HOST_B, "node_id": NODE_B, "state_generation": GEN_B, "local_id": "relay-task"},
            "payload": {
                "source": origin["ref"],
                "remote_seq": origin["seq"],
                "type": origin["type"],
                "payload": origin["payload"],
                "created": origin["created"],
            },
            "created": 200.0,
        }
        req = {"v": 1, "id": "w-1", "method": "peer.notify_parent", "params": self.params(
            operation=self.operation("notify-wrapped"),
            event=wrapped,
        )}
        first = await self.service.handle(req)
        self.assertTrue(first["ok"], first)
        self.assertFalse(first["result"]["duplicate"])

        replay = await self.service.handle({**req, "id": "w-2"})
        self.assertTrue(replay["ok"], replay)
        self.assertTrue(replay["result"]["duplicate"])
        self.assertEqual(first["result"]["mirror_seq"], replay["result"]["mirror_seq"])

        tampered = {**wrapped, "payload": {**wrapped["payload"], "payload": {"status": "tampered"}}}
        conflict = await self.service.handle(
            {"v": 1, "id": "w-3", "method": "peer.notify_parent", "params": self.params(
                operation=self.operation("notify-wrapped"),
                event=tampered,
            )}
        )
        self.assertFalse(conflict["ok"])
        self.assertEqual(PeerErrorCode.OPERATION_CONFLICT, conflict["error"]["code"])


if __name__ == "__main__":
    unittest.main()
