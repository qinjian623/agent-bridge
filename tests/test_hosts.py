import json
import stat
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from agent_bridge.config import Config
from agent_bridge.errors import BridgeError
from agent_bridge.hosts import (
    HOST_PINS_FILE_MODE,
    HOST_PINS_FILENAME,
    ConfigHostRegistry,
    HostConfig,
)
from agent_bridge.peer_protocol import PROTOCOL_VERSION, HostState, NodeRef, PeerErrorCode


def make_host(host_id="gpu-a", **changes):
    values = {
        "host_id": host_id,
        "ssh_target": f"{host_id}.internal",
        "remote_executable": "/usr/local/bin/agent-bridge",
    }
    values.update(changes)
    return HostConfig(**values)


class ConfigHostParsingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.config_path = self.state / "config.json"

    def write_config(self, hosts):
        payload = {
            "profiles": {
                "test": {
                    "runtime": "codex",
                    "provider": "openai-codex",
                    "model": "gpt-test",
                }
            },
            "hosts": hosts,
        }
        self.config_path.write_text(json.dumps(payload), encoding="utf-8")

    def test_config_load_parses_optional_hosts(self):
        self.write_config(
            {
                "gpu-a": {
                    "ssh_target": "gpu-a.internal",
                    "remote_executable": "/usr/local/bin/agent-bridge",
                    "remote_state": "/srv/agent-bridge",
                    "pinned": {"node_id": "node-1", "state_generation": "gen-1"},
                    "ssh_options": ["StrictHostKeyChecking=yes"],
                    "pull_interval": 5,
                    "projects": {"proj": "/srv/proj"},
                },
                "gpu-b": {
                    "host_id": "gpu-b",
                    "ssh_target": "user@gpu-b",
                    "remote_executable": "/opt/agent-bridge",
                },
            }
        )

        config = Config.load(self.config_path)

        self.assertEqual(set(config.hosts), {"gpu-a", "gpu-b"})
        gpu_a = config.hosts["gpu-a"]
        self.assertEqual(gpu_a.ssh_target, "gpu-a.internal")
        self.assertEqual(gpu_a.remote_executable, "/usr/local/bin/agent-bridge")
        self.assertEqual(gpu_a.remote_state, "/srv/agent-bridge")
        self.assertEqual(gpu_a.pinned, NodeRef("gpu-a", "node-1", "gen-1"))
        self.assertEqual(gpu_a.ssh_options, ("StrictHostKeyChecking=yes",))
        self.assertEqual(gpu_a.pull_interval, 5.0)
        self.assertEqual(gpu_a.projects, {"proj": "/srv/proj"})

        gpu_b = config.hosts["gpu-b"]
        self.assertIsNone(gpu_b.remote_state)
        self.assertIsNone(gpu_b.pinned)
        self.assertEqual(gpu_b.ssh_options, ())
        self.assertEqual(gpu_b.pull_interval, 2.0)
        self.assertEqual(gpu_b.projects, {})

    def test_config_without_hosts_defaults_to_an_empty_mapping(self):
        self.config_path.write_text(
            json.dumps({"profiles": {"test": {"runtime": "codex", "model": "gpt-test"}}}), encoding="utf-8"
        )

        config = Config.load(self.config_path)

        self.assertEqual(config.hosts, {})

    def test_invalid_hosts_are_rejected(self):
        cases = (
            [],
            {"GPU_A": {"ssh_target": "gpu-a", "remote_executable": "/bin/agent-bridge"}},
            {"gpu-a": {"remote_executable": "/bin/agent-bridge"}},
            {"gpu-a": {"ssh_target": "gpu-a", "remote_executable": ""}},
            {"gpu-a": {"ssh_target": "gpu-a", "remote_executable": "/bin/agent-bridge", "unknown": 1}},
            {
                "gpu-a": {
                    "ssh_target": "gpu-a",
                    "remote_executable": "/bin/agent-bridge",
                    "pinned": {"host_id": "gpu-b", "node_id": "n", "state_generation": "g"},
                }
            },
            {"gpu-a": {"ssh_target": "gpu-a", "remote_executable": "/bin/agent-bridge", "ssh_options": "-o"}},
            {"gpu-a": {"ssh_target": "gpu-a", "remote_executable": "/bin/agent-bridge", "pull_interval": 0}},
        )
        for hosts in cases:
            with self.subTest(hosts=hosts):
                self.write_config(hosts)
                with self.assertRaises(BridgeError) as caught:
                    Config.load(self.config_path)
                self.assertEqual(caught.exception.code, "invalid_config")


class ConfigHostRegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.node_a = NodeRef("gpu-a", "node-a", "generation-a")
        self.hosts = {
            "gpu-a": make_host("gpu-a"),
            "gpu-b": make_host("gpu-b", pinned=NodeRef("gpu-b", "node-b", "generation-b")),
        }
        self.registry = ConfigHostRegistry(self.hosts)

    def test_get_list_and_unknown_host_rejection(self):
        self.assertEqual(self.registry.get("gpu-a"), self.hosts["gpu-a"])
        self.assertEqual([host.host_id for host in self.registry.list()], ["gpu-a", "gpu-b"])

        with self.assertRaises(BridgeError) as caught:
            self.registry.get("missing")
        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_UNKNOWN)

    def test_initial_status_is_offline_and_uses_the_configured_pin(self):
        statuses = self.registry.status()
        self.assertEqual([status.host_id for status in statuses], ["gpu-a", "gpu-b"])
        self.assertEqual(statuses[0].state, HostState.OFFLINE)
        self.assertIsNone(statuses[0].node)
        self.assertIsNone(statuses[0].last_contact)
        self.assertIsNone(statuses[0].protocol_version)
        self.assertEqual(statuses[1].node, self.hosts["gpu-b"].pinned)
        self.assertEqual(self.registry.status("gpu-a"), [statuses[0]])
        self.assertEqual(ConfigHostRegistry().status(), [])
        self.assertEqual(ConfigHostRegistry().list(), [])

    def test_status_of_unknown_host_is_rejected(self):
        with self.assertRaises(BridgeError) as caught:
            self.registry.status("missing")
        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_UNKNOWN)

    def test_mark_online_records_contact_time(self):
        self.registry.mark("gpu-a", HostState.ONLINE)

        status = self.registry.status("gpu-a")[0]
        self.assertEqual(status.state, HostState.ONLINE)
        self.assertIsNotNone(status.last_contact)
        self.assertEqual(status.protocol_version, PROTOCOL_VERSION)
        self.assertIsNone(status.detail)

    def test_mark_accepts_a_state_value_and_rejects_unknown_states(self):
        self.registry.mark("gpu-a", "offline", "connection closed")

        status = self.registry.status("gpu-a")[0]
        self.assertEqual(status.state, HostState.OFFLINE)
        self.assertEqual(status.detail, "connection closed")

        with self.assertRaises(BridgeError) as caught:
            self.registry.mark("gpu-a", "exploded")
        self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)

    def test_mark_of_unknown_host_is_rejected(self):
        with self.assertRaises(BridgeError) as caught:
            self.registry.mark("missing", HostState.ONLINE)
        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_UNKNOWN)

    def test_identity_changed_is_sticky_until_pin(self):
        self.registry.mark("gpu-a", HostState.IDENTITY_CHANGED, "SSH host key mismatch")
        self.registry.mark("gpu-a", HostState.ONLINE, "must be ignored")

        status = self.registry.status("gpu-a")[0]
        self.assertEqual(status.state, HostState.IDENTITY_CHANGED)
        self.assertEqual(status.detail, "SSH host key mismatch")
        self.assertIsNone(status.last_contact)

        self.registry.pin("gpu-a", self.node_a)
        pinned = self.registry.status("gpu-a")[0]
        self.assertEqual(pinned.state, HostState.OFFLINE)
        self.assertEqual(pinned.node, self.node_a)
        self.assertIsNone(pinned.detail)
        self.assertEqual(self.registry.get("gpu-a").pinned, self.node_a)

    def test_state_corrupted_is_sticky_until_pin(self):
        self.registry.mark("gpu-a", HostState.STATE_CORRUPTED, "database rollback")
        for state in (HostState.OFFLINE, HostState.ONLINE):
            self.registry.mark("gpu-a", state)

        status = self.registry.status("gpu-a")[0]
        self.assertEqual(status.state, HostState.STATE_CORRUPTED)
        self.assertEqual(status.detail, "database rollback")

        self.registry.pin("gpu-a", self.node_a)
        self.assertEqual(self.registry.status("gpu-a")[0].state, HostState.OFFLINE)

    def test_pin_rejects_unknown_hosts_and_non_node_refs(self):
        with self.assertRaises(BridgeError) as caught:
            self.registry.pin("missing", self.node_a)
        self.assertEqual(caught.exception.code, PeerErrorCode.HOST_UNKNOWN)

        with self.assertRaises(BridgeError) as caught:
            self.registry.pin("gpu-a", {"host_id": "gpu-a"})
        self.assertEqual(caught.exception.code, PeerErrorCode.INVALID_PARAMS)

    def test_pin_persists_to_the_state_directory(self):
        registry = ConfigHostRegistry(self.hosts, self.state)
        node = NodeRef("gpu-a", "node-new", "generation-new")

        registry.pin("gpu-a", node)

        path = self.state / HOST_PINS_FILENAME
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), HOST_PINS_FILE_MODE)
        reloaded = ConfigHostRegistry(self.hosts, self.state)
        self.assertEqual(reloaded.get("gpu-a").pinned, node)
        self.assertEqual(reloaded.get("gpu-b").pinned, self.hosts["gpu-b"].pinned)

    def test_state_directory_loads_hosts_from_config_json(self):
        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "hosts": {
                        "gpu-a": {
                            "ssh_target": "gpu-a.internal",
                            "remote_executable": "/usr/local/bin/agent-bridge",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

        registry = ConfigHostRegistry(self.state)

        self.assertEqual([host.host_id for host in registry.list()], ["gpu-a"])
        self.assertEqual(registry.get("gpu-a").ssh_target, "gpu-a.internal")

    def test_replace_hosts_adds_removes_and_preserves_unchanged_live_status(self):
        self.registry.mark("gpu-a", HostState.ONLINE)
        original_status = self.registry.status("gpu-a")[0]
        new_host = make_host("gpu-c")

        self.registry.replace_hosts({"gpu-a": self.hosts["gpu-a"], "gpu-c": new_host})

        self.assertEqual([host.host_id for host in self.registry.list()], ["gpu-a", "gpu-c"])
        self.assertIs(self.registry.status("gpu-a")[0], original_status)
        self.assertEqual(self.registry.status("gpu-c")[0].state, HostState.OFFLINE)
        with self.assertRaises(BridgeError):
            self.registry.get("gpu-b")
        self.registry.replace_hosts({})
        self.assertEqual(self.registry.status(), [])

    def test_replace_hosts_resets_changed_connections_to_offline(self):
        self.registry.mark("gpu-b", HostState.ONLINE)
        changed = replace(self.hosts["gpu-b"], ssh_target="new-target")

        self.registry.replace_hosts({"gpu-b": changed})

        status = self.registry.status("gpu-b")[0]
        self.assertEqual(status.state, HostState.OFFLINE)
        self.assertEqual(status.node, changed.pinned)
        self.assertIsNone(status.last_contact)
        self.assertIsNone(status.protocol_version)

    def test_replace_hosts_clears_sticky_failure_only_for_a_new_nonempty_pin(self):
        for state in (HostState.IDENTITY_CHANGED, HostState.STATE_CORRUPTED):
            with self.subTest(state=state):
                original = self.hosts["gpu-b"]
                registry = ConfigHostRegistry({"gpu-b": original})
                registry.mark("gpu-b", state, "requires re-registration")
                status = registry.status("gpu-b")[0]
                changed = replace(original, ssh_options=("ConnectTimeout=5",))
                registry.replace_hosts({"gpu-b": changed})
                self.assertIs(registry.status("gpu-b")[0], status)
                registry.replace_hosts({"gpu-b": replace(changed, pinned=None)})
                self.assertIs(registry.status("gpu-b")[0], status)
                registry.replace_hosts({"gpu-b": changed})
                self.assertIs(registry.status("gpu-b")[0], status)
                repinned = replace(changed, pinned=NodeRef("gpu-b", "new-node", "new-generation"))
                registry.replace_hosts({"gpu-b": repinned})
                self.assertEqual(registry.status("gpu-b")[0].state, HostState.OFFLINE)
                self.assertEqual(registry.status("gpu-b")[0].node, repinned.pinned)

    def test_read_configured_hosts_applies_disk_pins_without_changing_live_registry(self):
        registry = ConfigHostRegistry(self.hosts, self.state)
        registry.mark("gpu-a", HostState.ONLINE)
        status = registry.status("gpu-a")[0]
        (self.state / "config.json").write_text(json.dumps({"hosts": {
            "gpu-c": {"ssh_target": "gpu-c", "remote_executable": "/bin/agent-bridge"},
        }}))
        (self.state / HOST_PINS_FILENAME).write_text(json.dumps({"hosts": {
            "gpu-c": {"node_id": "node-c", "state_generation": "generation-c"},
        }}))

        configured = registry.read_configured_hosts()

        self.assertEqual(list(configured), ["gpu-c"])
        self.assertEqual(configured["gpu-c"].pinned, NodeRef("gpu-c", "node-c", "generation-c"))
        self.assertEqual(registry.list(), list(self.hosts.values()))
        self.assertIs(registry.status("gpu-a")[0], status)

    def test_missing_or_invalid_configured_hosts_leave_live_state_unchanged(self):
        registry = ConfigHostRegistry(self.hosts, self.state)
        registry.mark("gpu-a", HostState.ONLINE)
        status = registry.status("gpu-a")[0]
        path = self.state / "config.json"
        for contents in (None, "{", json.dumps({"hosts": {"bad": {}}})):
            with self.subTest(contents=contents):
                if contents is not None:
                    path.write_text(contents)
                with self.assertRaises(BridgeError):
                    registry.read_configured_hosts()
                self.assertEqual(registry.list(), list(self.hosts.values()))
                self.assertIs(registry.status("gpu-a")[0], status)
        path.write_text(json.dumps({"hosts": {"gpu-a": {
            "ssh_target": "gpu-a", "remote_executable": "/bin/agent-bridge",
        }}}))
        (self.state / HOST_PINS_FILENAME).write_text("{")
        with self.assertRaises(BridgeError) as caught:
            registry.read_configured_hosts()
        self.assertEqual(caught.exception.code, "invalid_host_pins")
        self.assertIs(registry.status("gpu-a")[0], status)


if __name__ == "__main__":
    unittest.main()
