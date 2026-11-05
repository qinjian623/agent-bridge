import asyncio
import unittest

from agent_bridge import hosts, node_identity, peer_api, peer_transport  # noqa: F401
from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import (
    CAPABILITIES,
    PEER_METHODS,
    PROTOCOL_VERSION,
    NodeRef,
    ObjectRef,
    OperationStatus,
    PeerDeliveryStatus,
    PeerErrorCode,
    fail,
    negotiate_version,
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
            "workspace_v1", "session_name_v1", "prune_guard_v1",
        ))

    def test_unimplemented_phase_c_methods_are_rejected_by_service(self):
        for name in ("peer.workspace", "peer.resolve_delivery"):
            service = peer_api.DirectPeerService(None)  # type: ignore[arg-type]
            response = asyncio.run(service.handle({"v": 1, "id": "1", "method": name, "params": {}}))
            self.assertEqual(response["error"]["code"], PeerErrorCode.NOT_IMPLEMENTED)

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


if __name__ == "__main__":
    unittest.main()
