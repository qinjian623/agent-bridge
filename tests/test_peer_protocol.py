import unittest

from agent_bridge import hosts, node_identity, peer_api, peer_transport  # noqa: F401
from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import (
    PEER_METHODS,
    PROTOCOL_VERSION,
    NodeRef,
    ObjectRef,
    OperationStatus,
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
