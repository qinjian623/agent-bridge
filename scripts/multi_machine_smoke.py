"""Multi-machine end-to-end smoke test between coordinator and remote node (macos-node)."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from agent_bridge.broker import Broker
from agent_bridge.peer_protocol import (
    PROTOCOL_VERSION,
    NodeRef,
)


def run_git(cwd: Path, *args: str) -> str:
    res = subprocess.run(["git", "-C", str(cwd), *args], capture_output=True, text=True, check=True)
    return res.stdout.strip()


async def main():
    print("=== Multi-Machine Smoke Test (omarchy -> macos-node) ===")
    ts = int(time.time())
    
    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp)
        (state / "config.json").write_text(json.dumps({
            "profiles": {
                "local-mock": {
                    "runtime": "codex",
                    "model": "gpt-6.1-sol",
                    "executable": sys.executable,
                }
            },
            "hosts": {
                "macos-node": {
                    "ssh_target": "macos-node",
                    "remote_executable": "/Users/user/gits/cli-agent-bridge/.venv/bin/agent-bridge",
                    "projects": {
                        "bridge": "/Users/user/gits/cli-agent-bridge"
                    }
                }
            },
            "projects": {
                "bridge": "/home/user/gits/cli-agent-bridge"
            },
            "worker_launcher": "systemd",
        }))

        broker = Broker(state)

        # -------------------------------------------------------------
        # 1. Host Discovery & Identity Handshake
        # -------------------------------------------------------------
        print("\n[1/7] Testing Host Handshake (peer.hello)...")
        client = broker.peer_factory.client("macos-node")
        hello = await client.hello({
            "protocol_versions": [PROTOCOL_VERSION],
            "coordinator_id": "coordinator-example",
            "coordinator_generation": "smoke-test",
        })
        node_info = hello["node"]
        print(f"  Connected to host {node_info['host_id']}:")
        print(f"    node_id: {node_info['node_id']}")
        print(f"    state_generation: {node_info['state_generation']}")
        print(f"    capabilities: {len(hello['capabilities'])} negotiated")
        assert "groups_v1" in hello["capabilities"]
        assert "workspace_v1" in hello["capabilities"]

        # Pin host identity and verify handshake against pinned identity
        broker.host_registry.pin(
            "macos-node",
            NodeRef("macos-node", node_info["node_id"], node_info["state_generation"])
        )
        # Next handshake verifies pinned identity and transitions host to ONLINE
        await client.hello({
            "protocol_versions": [PROTOCOL_VERSION],
            "coordinator_id": "coordinator-example",
            "coordinator_generation": "smoke-test",
        })
        hosts_status = await broker.call("agent_hosts", {})
        remote_status = next(h for h in hosts_status["hosts"] if h["host_id"] == "macos-node")
        print(f"  Host status after pin and verify: state={remote_status['state']}, node_id={remote_status['node_id']}")
        assert remote_status["state"] == "online"
        assert remote_status["node_id"] == node_info["node_id"]

        # -------------------------------------------------------------
        # 2. Remote Catalog Discovery (agent_catalog)
        # -------------------------------------------------------------
        print("\n[2/7] Testing Remote Catalog Discovery (agent_catalog)...")
        catalog = await broker.call("agent_catalog", {"host_id": "macos-node"})
        profiles = [entry["name"] for entry in catalog["entries"]]
        print(f"  Remote profiles discovered: {profiles}")
        assert "codex-sol" in profiles
        assert "claude" in profiles
        assert "agy" in profiles

        # -------------------------------------------------------------
        # 3. Remote Workspace Base Inspection (peer.inspect)
        # -------------------------------------------------------------
        print("\n[3/7] Testing Workspace Base Inspection (peer.inspect)...")
        local_head = run_git(Path("/home/user/gits/cli-agent-bridge"), "rev-parse", "HEAD")
        inspect_res = await client.inspect({
            "kind": "workspace",
            "section": "base",
            "project_id": "bridge",
            "base_commit": local_head,
        })
        base_item = inspect_res["items"][0]
        print(f"  Remote project 'bridge' base inspection: exists={base_item.get('exists')}, "
              f"project_id={base_item.get('project_id')}, digest={base_item.get('project_config_digest')}")
        assert base_item["exists"] is True

        # -------------------------------------------------------------
        # 4. Remote Read-Only Open (agent_open)
        # -------------------------------------------------------------
        print("\n[4/7] Testing Remote Read-Only Session Open (agent_open)...")
        ro_open = await broker.call("agent_open", {
            "host_id": "macos-node",
            "profile": "codex-sol",
            "cwd": "/home/user/gits/cli-agent-bridge",
            "project_id": "bridge",
            "access": "read-only",
            "request_id": f"smoke-ro-{ts}",
        })
        print(f"  Read-only open status: {ro_open['status']}, session_id: {ro_open['session_id']}")
        assert ro_open["status"] == "accepted_remote"
        ro_session_id = ro_open["session_id"]
        assert ro_session_id is not None

        # -------------------------------------------------------------
        # 5. Remote Workspace Transfer (workspace-write)
        # -------------------------------------------------------------
        print("\n[5/7] Testing Remote Workspace Transfer & Worktree Preparation...")

        ws_open = await broker.call("agent_open", {
            "host_id": "macos-node",
            "profile": "codex-sol",
            "cwd": "/home/user/gits/cli-agent-bridge",
            "project_id": "bridge",
            "access": "workspace-write",
            "base": local_head,
            "snapshot_files": ["README.md", "pyproject.toml"],
            "request_id": f"smoke-ws-{ts}",
        })
        print(f"  Workspace open status: {ws_open['status']}, session_id: {ws_open['session_id']}")
        assert ws_open["status"] == "accepted_remote"
        ws_session_id = ws_open["session_id"]

        # Verify worktree exists on macos-node
        remote_worktrees = subprocess.check_output(
            ["ssh", "macos-node", "git", "-C", "~/gits/cli-agent-bridge", "worktree", "list"],
            text=True
        )
        print("  Remote git worktrees:")
        for line in remote_worktrees.strip().splitlines():
            print(f"    {line}")
        assert "workspaces/ws-" in remote_worktrees

        # -------------------------------------------------------------
        # 6. Cross-Machine Group & Broadcast (agent_group, agent_broadcast)
        # -------------------------------------------------------------
        print("\n[6/7] Testing Cross-Machine Session Group & Broadcast...")
        # Create a local session
        local_prof = broker.config.profiles["local-mock"]
        local_sid = broker.store.open(
            local_prof, "/home/user/gits/cli-agent-bridge", "read-only", f"smoke-local-{ts}"
        )["session_id"]
        broker.store.worker_ready(local_sid, "g", 123, "local-mock", "native-local")

        group_id = f"smoke-cluster-grp-{ts}"
        group = await broker.call("agent_group", {
            "action": "create",
            "group_id": group_id,
            "expected_revision": 0,
            "request_id": f"smoke-grp-create-{ts}",
            "session_ids": [local_sid],
            "remote_members": [{
                "host_id": "macos-node",
                "target_kind": "bridge",
                "session_id": ws_session_id,
                "label": "macos-node-worker"
            }],
        })
        print(f"  Group created: {group['group_id']} (rev {group['revision']})")
        print(f"    Local members: {[m.get('session_id') for m in group['members']]}")
        print(f"    Remote members: {[m['session_id'] for m in group['remote_members']]}")
        assert len(group["members"]) == 1
        assert len(group["remote_members"]) == 1

        # Broadcast to group
        bcast = await broker.call("agent_broadcast", {
            "group_id": group_id,
            "message": "Cluster broadcast test: ping from omarchy coordinator to macos-node node",
            "expected_revision": 0,
            "request_id": f"smoke-bcast-{ts}",
        })
        print(f"  Broadcast recorded: {bcast['broadcast_id']}")

        # Pump DeliveryScheduler to deliver remote broadcast to macos-node
        pumped = await broker.delivery_scheduler.pump("macos-node")
        print(f"  DeliveryScheduler pumped {len(pumped)} items to macos-node")
        assert len(pumped) >= 1

        # Check broadcast status
        bcast_status = await broker.call("agent_broadcast_status", {
            "broadcast_id": bcast["broadcast_id"]
        })
        print(f"  Broadcast counts: {bcast_status['counts']}")
        for m in bcast_status.get("recipients", []):
            print(f"    recipient {m.get('session_id') or m.get('target_id')}: status={m.get('status')}")

        # -------------------------------------------------------------
        # 7. Remote Member Outbox Pull (OutboxPuller)
        # -------------------------------------------------------------
        print("\n[7/7] Testing OutboxPuller (macos-node -> Coordinator)...")
        # On macos-node, generate an outbox event from macos-node-worker
        remote_script = f"""
import json
from pathlib import Path
from agent_bridge.store import Store, encode

store = Store(Path('~/.local/state/cli-agent-bridge').expanduser())
sender_ref = {{
    'host_id': 'macos-node',
    'node_id': '{node_info['node_id']}',
    'state_generation': '{node_info['state_generation']}',
    'local_id': '{ws_session_id}',
}}
payload = {{
    'group_id': '{group_id}',
    'revision': 0,
    'message': 'Pong: Response from macos-node node to cluster broadcast',
}}
op_id = 'smoke-outbox-op-{ts}'
fp = 'fp-{ts}'
store.add_outbox(
    op_id,
    'agent_broadcast',
    payload,
    group_id=f'{group_id}',
    sender_ref=sender_ref,
    fingerprint=fp,
)
print('Remote outbox item recorded')
"""
        subprocess.run(
            ["ssh", "macos-node", "/Users/user/gits/cli-agent-bridge/.venv/bin/python", "-"],
            input=remote_script,
            text=True,
            check=True
        )

        # Pull from macos-node using OutboxPuller
        pulled = await broker.outbox_puller.run_once("macos-node")
        print(f"  OutboxPuller processed {pulled} items from macos-node")
        assert pulled >= 1

        # Verify receipt stored in coordinator store
        with broker.store.connection() as db:
            receipt = db.execute(
                "SELECT * FROM pulled_request_receipts WHERE operation_id=?",
                (f"smoke-outbox-op-{ts}",)
            ).fetchone()
            assert receipt is not None
            print(f"  Pulled receipt confirmed in coordinator store: outcome={receipt['outcome']}")
            assert receipt["outcome"] == "accepted"

        # -------------------------------------------------------------
        # Cleanup
        # -------------------------------------------------------------
        print("\nCleaning up remote test workspaces...")
        subprocess.run(
            ["ssh", "macos-node", "rm -rf ~/.local/state/cli-agent-bridge/workspaces/* && git -C ~/gits/cli-agent-bridge worktree prune"],
            check=True
        )

        print("\nAll 7 Multi-Machine Smoke Tests Passed Successfully!")


if __name__ == "__main__":
    asyncio.run(main())
