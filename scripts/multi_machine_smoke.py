"""Parameterized multi-machine end-to-end smoke test between coordinator and a registered remote host."""

from __future__ import annotations

import argparse
import asyncio
import json
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Any

from agent_bridge.broker import Broker
from agent_bridge.peer_protocol import PROTOCOL_VERSION, NodeRef


def run_git(cwd: Path, *args: str) -> str:
    res = subprocess.run(["git", "-C", str(cwd), *args], capture_output=True, text=True, check=True)
    return res.stdout.strip()


async def resolve_session_workspace(
    broker: Broker,
    client: Any,
    operation_id: str,
    session_id: str,
) -> dict[str, Any]:
    """Identify the single remote workspace transfer owned by this smoke session."""
    operation = broker.store.get_operation(operation_id)
    if not isinstance(operation, dict):
        raise AssertionError(f"Missing coordinator operation record for {operation_id!r}")
    params = operation.get("params") if isinstance(operation.get("params"), dict) else {}
    open_spec = params.get("open") if isinstance(params.get("open"), dict) else {}
    workspace_ref = open_spec.get("workspace_ref") if isinstance(open_spec.get("workspace_ref"), dict) else {}
    workspace_id = workspace_ref.get("workspace_id")
    if not isinstance(workspace_id, str) or not workspace_id.startswith("ws-"):
        raise AssertionError(f"Expected remote workspace_id for session {session_id!r}, got {workspace_id!r}")

    status = await client.inspect({"kind": "workspace", "id": workspace_id, "section": "status"})
    items = status.get("items") if isinstance(status, dict) else None
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
        raise AssertionError(f"Expected single workspace status item for {workspace_id!r}, got {status!r}")
    transfer = items[0]
    if transfer.get("workspace_id") != workspace_id:
        raise AssertionError(f"Workspace status mismatch: expected {workspace_id!r}, got {transfer!r}")
    if transfer.get("attached_session_id") != session_id:
        raise AssertionError(
            f"Workspace {workspace_id!r} attached to {transfer.get('attached_session_id')!r}, expected {session_id!r}"
        )
    ready_path = transfer.get("ready_path")
    if not isinstance(ready_path, str) or not ready_path:
        raise AssertionError(f"Workspace {workspace_id!r} has no ready_path: {transfer!r}")
    parsed = PurePosixPath(ready_path)
    if not parsed.is_absolute() or parsed.name != workspace_id or parsed.parent.name != "workspaces":
        raise AssertionError(f"Unexpected remote workspace path for {workspace_id!r}: {ready_path!r}")
    return {
        "workspace_id": workspace_id,
        "ready_path": ready_path,
        "delivery_mode": transfer.get("delivery_mode") or "patch",
    }


def cleanup_remote_workspace(
    ssh_target: str,
    remote_project: str,
    workspace: dict[str, Any],
    *,
    run_fn: Any = subprocess.run,
) -> None:
    """Remove only the exact remote workspace created for this smoke run."""
    workspace_id = workspace.get("workspace_id")
    ready_path = workspace.get("ready_path")
    delivery_mode = workspace.get("delivery_mode") or "patch"
    if not isinstance(workspace_id, str) or not workspace_id.startswith("ws-"):
        raise ValueError(f"Invalid workspace_id for cleanup: {workspace_id!r}")
    if not isinstance(ready_path, str) or not ready_path:
        raise ValueError(f"Invalid ready_path for cleanup: {ready_path!r}")
    parsed = PurePosixPath(ready_path)
    if not parsed.is_absolute() or parsed.name != workspace_id or parsed.parent.name != "workspaces":
        raise ValueError(f"Refusing to remove non-workspace path: {ready_path!r}")
    if delivery_mode == "patch":
        cmd = f"git -C {shlex.quote(remote_project)} worktree remove --force {shlex.quote(ready_path)}"
    else:
        cmd = f"rm -rf -- {shlex.quote(ready_path)}"
    run_fn(["ssh", ssh_target, cmd], check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-id", default="worker-node")
    parser.add_argument("--ssh-target", required=True)
    parser.add_argument("--remote-executable", required=True)
    parser.add_argument("--remote-python", required=True)
    parser.add_argument("--remote-project", required=True)
    parser.add_argument("--remote-profile", default="codex")
    parser.add_argument("--local-project", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--coordinator-id", default="coordinator")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    print(f"=== Multi-Machine Smoke Test ({args.coordinator_id} -> {args.host_id}) ===")
    ts = int(time.time())
    local_project = Path(args.local_project).expanduser().resolve()

    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp)
        (state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {
                        "local-mock": {
                            "runtime": "codex",
                            "model": "mock-model",
                            "executable": sys.executable,
                        }
                    },
                    "hosts": {
                        args.host_id: {
                            "ssh_target": args.ssh_target,
                            "remote_executable": args.remote_executable,
                            "projects": {"bridge": args.remote_project},
                        }
                    },
                    "projects": {"bridge": str(local_project)},
                    "worker_launcher": "detached" if sys.platform == "darwin" else "systemd",
                }
            )
        )

        broker = Broker(state)

        print("\n[1/7] Testing Host Handshake (peer.hello)...")
        client = broker.peer_factory.client(args.host_id)
        hello = await client.hello(
            {
                "protocol_versions": [PROTOCOL_VERSION],
                "coordinator_id": args.coordinator_id,
                "coordinator_generation": "smoke-test",
            }
        )
        node_info = hello["node"]
        print(f"  Connected to host {node_info['host_id']}:")
        print(f"    node_id: {node_info['node_id']}")
        print(f"    state_generation: {node_info['state_generation']}")
        print(f"    capabilities: {len(hello['capabilities'])} negotiated")
        assert "groups_v1" in hello["capabilities"]
        assert "workspace_v1" in hello["capabilities"]

        broker.host_registry.pin(
            args.host_id,
            NodeRef(args.host_id, node_info["node_id"], node_info["state_generation"]),
        )
        await client.hello(
            {
                "protocol_versions": [PROTOCOL_VERSION],
                "coordinator_id": args.coordinator_id,
                "coordinator_generation": "smoke-test",
            }
        )
        hosts_status = await broker.call("agent_hosts", {})
        host_status = next(h for h in hosts_status["hosts"] if h["host_id"] == args.host_id)
        print(f"  Host status after pin and verify: state={host_status['state']}, node_id={host_status['node_id']}")
        assert host_status["state"] == "online"
        assert host_status["node_id"] == node_info["node_id"]

        print("\n[2/7] Testing Remote Catalog Discovery (agent_catalog)...")
        catalog = await broker.call("agent_catalog", {"host_id": args.host_id})
        profiles = [entry["name"] for entry in catalog["entries"]]
        print(f"  Remote profiles discovered: {profiles}")
        assert args.remote_profile in profiles

        print("\n[3/7] Testing Workspace Base Inspection (peer.inspect)...")
        local_head = run_git(local_project, "rev-parse", "HEAD")
        inspect_res = await client.inspect(
            {
                "kind": "workspace",
                "section": "base",
                "project_id": "bridge",
                "base_commit": local_head,
            }
        )
        base_item = inspect_res["items"][0]
        print(
            f"  Remote project 'bridge' base inspection: exists={base_item.get('exists')}, "
            f"project_id={base_item.get('project_id')}, digest={base_item.get('project_config_digest')}"
        )
        assert base_item["exists"] is True

        print("\n[4/7] Testing Remote Read-Only Session Open (agent_open)...")
        ro_open = await broker.call(
            "agent_open",
            {
                "host_id": args.host_id,
                "profile": args.remote_profile,
                "cwd": str(local_project),
                "project_id": "bridge",
                "access": "read-only",
                "request_id": f"smoke-ro-{ts}",
            },
        )
        print(f"  Read-only open status: {ro_open['status']}, session_id: {ro_open['session_id']}")
        assert ro_open["status"] == "accepted_remote"
        assert ro_open["session_id"] is not None

        def _remote_worktree_paths() -> set[str]:
            out = subprocess.check_output(
                ["ssh", args.ssh_target, f"git -C {shlex.quote(args.remote_project)} worktree list --porcelain"],
                text=True,
            )
            return {
                line[len("worktree ") :].strip()
                for line in out.splitlines()
                if line.startswith("worktree ")
            }

        print("\n[5/7] Testing Remote Workspace Transfer & Worktree Preparation...")
        ws_open = await broker.call(
            "agent_open",
            {
                "host_id": args.host_id,
                "profile": args.remote_profile,
                "cwd": str(local_project),
                "project_id": "bridge",
                "access": "workspace-write",
                "base": local_head,
                "snapshot_files": ["README.md", "pyproject.toml"],
                "request_id": f"smoke-ws-{ts}",
            },
        )
        print(f"  Workspace open status: {ws_open['status']}, session_id: {ws_open['session_id']}")
        assert ws_open["status"] == "accepted_remote"
        ws_session_id = ws_open["session_id"]

        ws_workspace = await resolve_session_workspace(broker, client, ws_open["operation_id"], ws_session_id)
        print(f"  Created remote workspace ({ws_workspace['delivery_mode']}): {ws_workspace['ready_path']}")
        if ws_workspace["delivery_mode"] == "patch":
            remote_worktrees = _remote_worktree_paths()
            assert ws_workspace["ready_path"] in remote_worktrees, (
                f"Expected exact worktree {ws_workspace['ready_path']!r} in {sorted(remote_worktrees)}"
            )

        try:
            print("\n[6/7] Testing Cross-Machine Session Group & Broadcast...")
            local_prof = broker.config.profiles["local-mock"]
            local_sid = broker.store.open(
                local_prof, str(local_project), "read-only", f"smoke-local-{ts}"
            )["session_id"]
            broker.store.worker_ready(local_sid, "g", 123, "local-mock", "native-local")

            group_id = f"smoke-cluster-grp-{ts}"
            group = await broker.call(
                "agent_group",
                {
                    "action": "create",
                    "group_id": group_id,
                    "expected_revision": 0,
                    "request_id": f"smoke-grp-create-{ts}",
                    "session_ids": [local_sid],
                    "remote_members": [
                        {
                            "host_id": args.host_id,
                            "target_kind": "bridge",
                            "session_id": ws_session_id,
                            "label": "remote-worker",
                        }
                    ],
                },
            )
            print(f"  Group created: {group['group_id']} (rev {group['revision']})")
            assert len(group["members"]) == 1
            assert len(group["remote_members"]) == 1

            bcast = await broker.call(
                "agent_broadcast",
                {
                    "group_id": group_id,
                    "message": f"Cluster broadcast test: ping from {args.coordinator_id} to {args.host_id}",
                    "expected_revision": 0,
                    "request_id": f"smoke-bcast-{ts}",
                },
            )
            print(f"  Broadcast recorded: {bcast['broadcast_id']}")

            pumped = await broker.delivery_scheduler.pump(args.host_id)
            print(f"  DeliveryScheduler pumped {len(pumped)} items to {args.host_id}")
            assert len(pumped) >= 1

            bcast_status = await broker.call("agent_broadcast_status", {"broadcast_id": bcast["broadcast_id"]})
            print(f"  Broadcast counts: {bcast_status['counts']}")

            print(f"\n[7/7] Testing OutboxPuller ({args.host_id} -> Coordinator)...")
            remote_script = f"""
from pathlib import Path
from agent_bridge.store import Store

store = Store(Path('~/.local/state/cli-agent-bridge').expanduser())
sender_ref = {{
    'host_id': {args.host_id!r},
    'node_id': {node_info['node_id']!r},
    'state_generation': {node_info['state_generation']!r},
    'local_id': {ws_session_id!r},
}}
payload = {{
    'group_id': {group_id!r},
    'revision': 0,
    'message': 'Pong: Response from remote node to cluster broadcast',
}}
store.add_outbox(
    {f'smoke-outbox-op-{ts}'!r},
    'agent_broadcast',
    payload,
    group_id={group_id!r},
    sender_ref=sender_ref,
    fingerprint={f'fp-{ts}'!r},
)
print('Remote outbox item recorded')
"""
            subprocess.run(
                ["ssh", args.ssh_target, shlex.quote(args.remote_python), "-"],
                input=remote_script,
                text=True,
                check=True,
            )

            pulled = await broker.outbox_puller.run_once(args.host_id)
            print(f"  OutboxPuller processed {pulled} items from {args.host_id}")
            assert pulled >= 1

            with broker.store.connection() as db:
                receipt = db.execute(
                    "SELECT * FROM pulled_request_receipts WHERE operation_id=?",
                    (f"smoke-outbox-op-{ts}",),
                ).fetchone()
                assert receipt is not None
                print(f"  Pulled receipt confirmed in coordinator store: outcome={receipt['outcome']}")
                assert receipt["outcome"] == "accepted"
        finally:
            print("\nCleaning up created remote test workspace...")
            cleanup_remote_workspace(args.ssh_target, args.remote_project, ws_workspace)
        print("\nAll 7 Multi-Machine Smoke Tests Passed Successfully!")


if __name__ == "__main__":
    asyncio.run(main())
