"""Tests for scripts/multi_machine_smoke.py exact workspace resolution and cleanup."""

from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.multi_machine_smoke import cleanup_remote_workspace, resolve_session_workspace


class _FakeStore:
    def __init__(self, operation: dict | None) -> None:
        self._operation = operation

    def get_operation(self, operation_id: str) -> dict | None:
        return self._operation


class _FakeBroker:
    def __init__(self, operation: dict | None) -> None:
        self.store = _FakeStore(operation)


class _FakeClient:
    def __init__(self, inspect_response: dict) -> None:
        self.inspect_response = inspect_response
        self.inspect_calls: list[dict] = []

    async def inspect(self, params: dict) -> dict:
        self.inspect_calls.append(dict(params))
        return self.inspect_response


class MultiMachineSmokeCleanupTests(unittest.TestCase):
    def test_resolve_session_workspace_requires_exact_session_and_path(self) -> None:
        broker = _FakeBroker(
            {
                "operation_id": "op-1",
                "params": {
                    "open": {
                        "workspace_ref": {
                            "workspace_id": "ws-mine123",
                        }
                    }
                },
            }
        )
        client = _FakeClient(
            {
                "items": [
                    {
                        "workspace_id": "ws-mine123",
                        "attached_session_id": "sess-mine",
                        "delivery_mode": "patch",
                        "ready_path": "/remote/state/workspaces/ws-mine123",
                    }
                ]
            }
        )
        details = asyncio.run(resolve_session_workspace(broker, client, "op-1", "sess-mine"))
        self.assertEqual(
            details,
            {
                "workspace_id": "ws-mine123",
                "ready_path": "/remote/state/workspaces/ws-mine123",
                "delivery_mode": "patch",
            },
        )
        self.assertEqual(
            client.inspect_calls,
            [{"kind": "workspace", "id": "ws-mine123", "section": "status"}],
        )

    def test_resolve_session_workspace_rejects_foreign_session(self) -> None:
        broker = _FakeBroker(
            {
                "operation_id": "op-1",
                "params": {"open": {"workspace_ref": {"workspace_id": "ws-other999"}}},
            }
        )
        client = _FakeClient(
            {
                "items": [
                    {
                        "workspace_id": "ws-other999",
                        "attached_session_id": "sess-concurrent",
                        "ready_path": "/remote/state/workspaces/ws-other999",
                    }
                ]
            }
        )
        with self.assertRaisesRegex(AssertionError, "sess-concurrent"):
            asyncio.run(resolve_session_workspace(broker, client, "op-1", "sess-mine"))

    def test_cleanup_removes_only_exact_worktree_leaving_concurrent_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            workspaces = root / "state" / "workspaces"
            workspaces.mkdir(parents=True)
            subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.email", "smoke@example.invalid"], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Smoke Test"], check=True)
            (repo / "README.md").write_text("hi\n")
            subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-m", "init"], check=True, capture_output=True)

            mine = (workspaces / "ws-mine123").resolve()
            concurrent = (workspaces / "ws-concurrent456").resolve()
            subprocess.run(
                ["git", "-C", str(repo), "worktree", "add", "--detach", str(mine), "HEAD"],
                check=True,
                capture_output=True,
            )
            subprocess.run(
                ["git", "-C", str(repo), "worktree", "add", "--detach", str(concurrent), "HEAD"],
                check=True,
                capture_output=True,
            )

            recorded_cmds: list[list[str]] = []

            def fake_local_ssh(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
                self.assertEqual(cmd[:2], ["ssh", "worker@host"])
                recorded_cmds.append(cmd)
                self.assertNotIn("worktree prune", cmd[2])
                return subprocess.run(["bash", "-c", cmd[2]], check=check, capture_output=True, text=True)

            cleanup_remote_workspace(
                "worker@host",
                str(repo),
                {
                    "workspace_id": "ws-mine123",
                    "ready_path": str(mine),
                    "delivery_mode": "patch",
                },
                run_fn=fake_local_ssh,
            )

            self.assertEqual(len(recorded_cmds), 1)
            self.assertIn(str(mine), recorded_cmds[0][2])
            self.assertFalse(mine.exists())
            self.assertTrue(concurrent.is_dir())
            listed = subprocess.check_output(
                ["git", "-C", str(repo), "worktree", "list", "--porcelain"],
                text=True,
            )
            self.assertNotIn(str(mine), listed)
            self.assertIn(str(concurrent), listed)

    def test_cleanup_refuses_non_workspace_path(self) -> None:
        with patch("scripts.multi_machine_smoke.subprocess.run") as run_mock:
            with self.assertRaisesRegex(ValueError, "non-workspace path"):
                cleanup_remote_workspace(
                    "worker@host",
                    "/repo",
                    {
                        "workspace_id": "ws-abc",
                        "ready_path": "/repo/other/ws-abc",
                        "delivery_mode": "patch",
                    },
                    run_fn=run_mock,
                )
            run_mock.assert_not_called()

    def test_cleanup_snapshot_mode_removes_only_exact_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspaces = Path(tmp) / "workspaces"
            workspaces.mkdir(parents=True)
            mine = workspaces / "ws-snap123"
            concurrent = workspaces / "ws-concurrent789"
            mine.mkdir()
            (mine / "file.txt").write_text(json.dumps({"ok": True}))
            concurrent.mkdir()
            (concurrent / "keep.txt").write_text("keep")

            def fake_local_ssh(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
                self.assertNotIn("prune", cmd[2])
                return subprocess.run(["bash", "-c", cmd[2]], check=check, capture_output=True, text=True)

            cleanup_remote_workspace(
                "worker@host",
                "/unused/repo",
                {
                    "workspace_id": "ws-snap123",
                    "ready_path": str(mine),
                    "delivery_mode": "snapshot",
                },
                run_fn=fake_local_ssh,
            )

            self.assertFalse(mine.exists())
            self.assertTrue((concurrent / "keep.txt").is_file())
