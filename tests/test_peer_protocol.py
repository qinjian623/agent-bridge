import asyncio
import unittest

from agent_bridge import hosts, node_identity, peer_api, peer_transport  # noqa: F401
from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import (
    CAPABILITIES,
    PEER_METHODS,
    PROTOCOL_VERSION,
    BroadcastRecipient,
    DeliveryResolution,
    DeliverySource,
    GroupBroadcast,
    GroupRef,
    NativeSendQueueItem,
    NodeRef,
    ObjectRef,
    OperationStatus,
    PeerDeliveryStatus,
    PeerErrorCode,
    PeerOutboxItem,
    RemoteMember,
    ResolveDeliveryResult,
    SendQueueStatus,
    TargetRef,
    can_advance_native_queue,
    can_advance_peer_stream,
    canonical_operation_id,
    compute_stream_id,
    fail,
    is_head_blocking,
    is_peer_outbox_head_blocking,
    negotiate_version,
    normalize_remote_member,
    ok,
    require_method,
)


class PeerProtocolContract(unittest.TestCase):
    def test_whitelist_rejects_unknown_methods(self):
        for name in ("agent_open", "peer.shell", "", None, "peer.quota_set"):
            with self.assertRaises(BridgeError) as caught:
                require_method(name)
            self.assertEqual(caught.exception.code, PeerErrorCode.METHOD_NOT_ALLOWED)

    def test_every_method_has_client_and_service_surface(self):
        client = {name for name in dir(peer_transport.PeerClient) if not name.startswith("_")}
        self.assertEqual(client, {name.removeprefix("peer.") for name in PEER_METHODS})

    def test_sixteen_methods_with_phase_c_additions(self):
        self.assertEqual(len(PEER_METHODS), 16)
        for name in ("peer.workspace", "peer.resolve_delivery"):
            self.assertTrue(PEER_METHODS[name].mutating, name)
            self.assertTrue(PEER_METHODS[name].admin_only, name)
            self.assertIn("operation", PEER_METHODS[name].params.__annotations__, name)
        for name in PEER_METHODS:
            self.assertTrue(hasattr(peer_transport.DirectPeerClient, name.removeprefix("peer.")), name)

    def test_phase_c_error_codes_statuses_and_capabilities(self):
        codes = {
            "WORKSPACE_CONFLICT": "workspace_conflict",
            "WORKSPACE_ALREADY_ATTACHED": "workspace_already_attached",
            "WORKSPACE_LIMIT_EXCEEDED": "workspace_limit_exceeded",
            "WORKSPACE_PROJECT_CHANGED": "workspace_project_changed",
            "GROUP_PRUNE_BLOCKED": "group_prune_blocked",
            "NATIVE_STOP_UNVERIFIABLE": "native_stop_unverifiable",
            "PARENT_ROUTE_CONFLICT": "parent_route_conflict",
        }
        for name, value in codes.items():
            self.assertEqual(getattr(PeerErrorCode, name).value, value)
        for name in ("SENDING", "TOMBSTONE_PENDING", "RELEASED", "RESOLVING", "FAILED"):
            self.assertEqual(getattr(PeerDeliveryStatus, name).value, name.lower())
        self.assertEqual(CAPABILITIES, (
            "groups_v1", "ordered_delivery_v1", "member_outbox_v1", "event_mirror_v1",
            "workspace_v1", "session_name_v1", "prune_guard_v1", "task_timeout_v1", "session_effort_v1",
        ))

    def test_workspace_method_is_implemented_and_validates_its_operation(self):
        service = peer_api.DirectPeerService(None)  # type: ignore[arg-type]
        response = asyncio.run(service.handle({"v": 1, "id": "1", "method": "peer.workspace", "params": {}}))
        self.assertEqual(response["error"]["code"], PeerErrorCode.INVALID_PARAMS)

    def test_mutating_methods_carry_operation_or_natural_key(self):
        for spec in PEER_METHODS.values():
            if spec.mutating and spec.name != "peer.ack_request":
                self.assertIn("operation", spec.params.__annotations__, spec.name)

    def test_refs_are_namespaced(self):
        a, b = NodeRef("gpu-a", "n1", "g1"), NodeRef("gpu-b", "n1", "g1")
        self.assertNotEqual(ObjectRef(a, "x"), ObjectRef(b, "x"))
        self.assertEqual(ObjectRef(a, "x").as_dict()["local_id"], "x")

    def test_version_and_envelopes(self):
        self.assertEqual(negotiate_version([0, PROTOCOL_VERSION]), PROTOCOL_VERSION)
        with self.assertRaises(BridgeError):
            negotiate_version([99])
        self.assertTrue(ok("1", {})["ok"])
        self.assertEqual(fail("1", BridgeError("x", "m"))["error"], {"code": "x", "message": "m"})

    def test_terminal_statuses(self):
        self.assertTrue(OperationStatus.COMPLETED.terminal)
        self.assertFalse(OperationStatus.UNCERTAIN.terminal)


class PhaseCRefs(unittest.TestCase):
    A = NodeRef("h1", "n1", "g1")
    D = NodeRef("h2", "n2", "g2")

    def test_group_ref(self):
        self.assertEqual(
            GroupRef(self.A, "grp").as_dict(),
            {"authority": self.A.as_dict(), "group_id": "grp"},
        )

    def test_target_ref_validation(self):
        t = TargetRef(self.D, "native", "nid", "codex")
        self.assertEqual(
            t.as_dict(),
            {"host_id": "h2", "node_id": "n2", "state_generation": "g2",
             "kind": "native", "runtime": "codex", "target_id": "nid"},
        )
        self.assertEqual(TargetRef(self.D, "bridge", "s1").runtime, "")
        for args in (
            (self.D, "other", "x", ""),
            (self.D, "bridge", "x", "codex"),
            (self.D, "native", "x", ""),
            (self.D, "native", "x", "claude"),
        ):
            with self.assertRaises((ValueError, BridgeError)):
                TargetRef(*args)

    def test_stream_id(self):
        t = TargetRef(self.D, "bridge", "s1")
        sid = compute_stream_id(self.A, t)
        self.assertEqual(len(sid), 64)
        self.assertEqual(sid, compute_stream_id(self.A, t))
        self.assertEqual(sid, compute_stream_id(self.A.as_dict(), t.as_dict()))
        self.assertNotEqual(sid, compute_stream_id(self.A, TargetRef(self.D, "bridge", "s2")))
        self.assertNotEqual(sid, compute_stream_id(self.A, TargetRef(NodeRef("h2", "n2", "g3"), "bridge", "s1")))
        self.assertNotEqual(sid, compute_stream_id(NodeRef("h1", "n1", "g9"), t))
        # no ambiguity between authority and target fields
        self.assertNotEqual(
            compute_stream_id(NodeRef("a", "bc", "g"), TargetRef(self.D, "bridge", "s")),
            compute_stream_id(NodeRef("ab", "c", "g"), TargetRef(self.D, "bridge", "s")),
        )

    def test_canonical_operation_id(self):
        base = canonical_operation_id(self.A, "g2", "peer.deliver", "r1")
        self.assertEqual(base, canonical_operation_id(self.A.as_dict(), "g2", "peer.deliver", "r1"))
        self.assertEqual(len(base), 64)
        for other in (
            canonical_operation_id(NodeRef("h1", "n9", "g1"), "g2", "peer.deliver", "r1"),
            canonical_operation_id(NodeRef("h1", "n1", "g8"), "g2", "peer.deliver", "r1"),
            canonical_operation_id(self.A, "g3", "peer.deliver", "r1"),
            canonical_operation_id(self.A, "g2", "peer.control", "r1"),
            canonical_operation_id(self.A, "g2", "peer.deliver", "r2"),
        ):
            self.assertNotEqual(base, other)


class NativeSendQueueContract(unittest.TestCase):
    def test_send_queue_status_values(self):
        self.assertEqual(
            {s.value for s in SendQueueStatus},
            {"pending", "sending", "enqueued", "received", "uncertain", "failed", "cancelled"},
        )

    def test_delivery_source_values(self):
        self.assertEqual(
            {s.value for s in DeliverySource},
            {"local_broadcast", "peer_deliver", "parent_notification", "migration"},
        )

    def test_native_send_queue_item_fields(self):
        self.assertEqual(
            set(NativeSendQueueItem.__annotations__),
            {
                "id", "runtime", "native_id", "local_seq", "delivery_source", "delivery_id", "message",
                "status", "receipt", "native_turn_id", "processing_status", "error", "revision",
                "created", "last_attempt",
            },
        )

    def test_head_blocking_and_advance_are_complementary(self):
        blocking = {"pending", "sending", "uncertain"}
        for status in SendQueueStatus:
            self.assertEqual(is_head_blocking(status), status.value in blocking)
            self.assertEqual(can_advance_native_queue(status), status.value not in blocking)
        self.assertFalse(is_head_blocking("bogus"))
        self.assertFalse(can_advance_native_queue("bogus"))


class RemoteMemberContract(unittest.TestCase):
    NATIVE = {"host_id": "h", "kind": "native", "runtime": "codex", "native_id": "n1", "cwd": "/srv", "label": "L"}
    BRIDGE = {"host_id": "h", "target_kind": "bridge", "session_id": "s1", "label": "B"}

    def assertInvalid(self, member, action="add"):
        with self.assertRaises(BridgeError) as caught:
            normalize_remote_member(member, action)
        self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)

    def test_native_add_with_kind_alias(self):
        self.assertEqual(
            normalize_remote_member(self.NATIVE),
            {
                "host_id": "h", "node_id": "", "state_generation": "", "target_kind": "native",
                "runtime": "codex", "target_id": "n1", "cwd": "/srv", "label": "L",
            },
        )

    def test_bridge_add_normalizes_cwd(self):
        normalized = normalize_remote_member({**self.BRIDGE, "cwd": "/ignored"}, "create")
        self.assertEqual(normalized["target_id"], "s1")
        self.assertEqual(normalized["runtime"], "")
        self.assertEqual(normalized["cwd"], "")

    def test_bridge_invalid(self):
        self.assertInvalid({"host_id": "h", "target_kind": "bridge"})
        self.assertInvalid({**self.BRIDGE, "session_id": ""})
        self.assertInvalid({**self.BRIDGE, "runtime": "codex"})
        self.assertInvalid({**self.BRIDGE, "native_id": "n"})

    def test_native_invalid(self):
        self.assertInvalid({**self.NATIVE, "runtime": "claude"})
        self.assertInvalid({k: v for k, v in self.NATIVE.items() if k != "runtime"})
        self.assertInvalid({**self.NATIVE, "native_id": ""})
        self.assertInvalid({**self.NATIVE, "session_id": "s"})

    def test_kind_invalid(self):
        self.assertInvalid({**self.BRIDGE, "target_kind": "other"})
        self.assertInvalid({"host_id": "h", "session_id": "s"})
        self.assertInvalid({**self.BRIDGE, "kind": "native"})
        self.assertInvalid({**self.BRIDGE, "host_id": ""})
        self.assertInvalid("not a dict")  # type: ignore[arg-type]

    def test_remove_requires_identity(self):
        self.assertInvalid(self.BRIDGE, "remove")
        self.assertInvalid({**self.BRIDGE, "node_id": "n"}, "remove")
        self.assertInvalid({**self.NATIVE, "state_generation": "g"}, "remove")
        normalized = normalize_remote_member({**self.NATIVE, "node_id": "n", "state_generation": "g"}, "remove")
        self.assertEqual((normalized["node_id"], normalized["state_generation"]), ("n", "g"))

    def test_add_accepts_optional_identity(self):
        normalized = normalize_remote_member({**self.BRIDGE, "node_id": "n", "state_generation": "g"}, "add")
        self.assertEqual((normalized["node_id"], normalized["state_generation"]), ("n", "g"))

    def test_peer_api_uses_same_type(self):
        self.assertIs(peer_api.RemoteMember, RemoteMember)


class PeerOutboxContract(unittest.TestCase):
    def test_outbox_item_fields(self):
        self.assertEqual(
            set(PeerOutboxItem.__annotations__),
            {
                "delivery_id", "broadcast_id", "group_id", "group_revision", "stream_id", "target_seq", "host_id",
                "node_id", "state_generation", "target_kind", "runtime", "target_id", "operation_id", "payload",
                "fingerprint", "seq_kind", "status", "send_status", "processing_status", "receipt", "error",
                "revision", "cancel_requested", "blocked_by", "created", "updated", "last_attempt",
            },
        )

    def test_head_blocking_and_advance_partition(self):
        blocking = {
            "pending", "waiting_for_host", "sending", "accepted_remote", "uncertain", "tombstone_pending", "resolving",
        }
        advance = {"released", "cancelled", "failed"}
        for status in PeerDeliveryStatus:
            self.assertEqual(is_peer_outbox_head_blocking(status), status.value in blocking, status)
            self.assertEqual(can_advance_peer_stream(status), status.value in advance, status)
        self.assertFalse(is_peer_outbox_head_blocking("bogus"))
        self.assertFalse(can_advance_peer_stream("bogus"))

    def test_broadcast_and_resolution_records(self):
        self.assertEqual(
            set(GroupBroadcast.__annotations__),
            {"id", "group_id", "group_revision", "sender_ref", "message", "recipient_snapshot", "request_key",
             "created", "settled_at"},
        )
        self.assertEqual(
            set(BroadcastRecipient.__annotations__),
            {"broadcast_id", "recipient_key", "target_ref", "delivery_id", "task_id", "status", "send_status",
             "processing_status", "error", "updated"},
        )
        self.assertEqual(
            set(DeliveryResolution.__annotations__),
            {"id", "delivery_id", "request_id", "fingerprint", "group_revision", "delivery_revision",
             "remote_revision", "request_json", "resolution", "evidence", "actor", "status", "remote_result",
             "created", "updated"},
        )
        self.assertEqual(ResolveDeliveryResult.__annotations__["receipt"].__forward_arg__, "NotRequired[str | None]")


if __name__ == "__main__":
    unittest.main()
