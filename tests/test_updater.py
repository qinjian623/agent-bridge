import asyncio
import base64
import fcntl
import hashlib
import io
import json
import os
import plistlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

from agent_bridge import updater
from agent_bridge.store import Store


class UpdateFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name).resolve()
        self.state = self.home / "state"
        self.root = self.home / "managed"
        self.state.mkdir(mode=0o700)
        self.root.mkdir(mode=0o700)
        self.settings = {
            "repository": updater.REPOSITORY,
            "root": str(self.root),
            "service_kind": "systemd",
            "service_name": "cli-agent-bridge.service",
            "bootstrap_python": sys.executable,
        }
        updater._write(self.state / "update.json", self.settings)
        updater._write(self.state / "config.json", {"profiles": {}})
        self.updater = updater.Updater(self.state)
        self.old = self.root / "releases/old"
        self.new = self.root / "releases/new"
        self.old.mkdir(parents=True, mode=0o700)
        self.new.mkdir(mode=0o700)
        self.old.parent.chmod(0o700)
        updater._write(self.old / "release.json", {"tag": "v0.1.0"})
        updater._write(self.new / "release.json", {"tag": "v0.2.0"})
        self.updater._switch(self.old)
        self.digest = hashlib.sha256((self.state / "config.json").read_bytes()).hexdigest()

    def database(self):
        return Store(self.state)


class BusySnapshotTests(UpdateFixture):
    def test_empty_state_is_idle_without_creating_or_modifying_store(self):
        self.database()
        before = (self.state / "bridge.sqlite").read_bytes()
        result = updater.busy_snapshot(self.state)
        self.assertFalse(result["busy"], result)
        self.assertEqual(before, (self.state / "bridge.sqlite").read_bytes())

    def test_missing_database_is_unknown_and_never_created(self):
        result = updater.busy_snapshot(self.state)
        self.assertTrue(result["busy"])
        self.assertEqual(["update_state_unknown"], result["reasons"])
        self.assertFalse((self.state / "bridge.sqlite").exists())

    def test_paused_historical_queue_is_preserved_but_does_not_block_upgrade(self):
        store = self.database()
        with store.connection(write=True) as db:
            for name, desired in (("paused", "paused"), ("closed", "closed")):
                db.execute(
                    "INSERT INTO sessions(id,profile,cwd,access,desired,created) VALUES(?,?,?,?,?,0)",
                    (name, "test", str(self.home), "read-only", desired),
                )
                db.execute(
                    "INSERT INTO tasks(id,session_id,instruction,dependencies,reservation,created) VALUES(?,?,?,?,?,?)",
                    (name, name, "not executed", "[]", 0, time.time()),
                )
        self.assertFalse(updater.busy_snapshot(self.state)["busy"])
        with store.connection() as db:
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM tasks WHERE status='queued'").fetchone()[0])

    def test_open_queue_and_active_task_are_allowed_in_hot_mode_and_blocked_when_draining_workers(self):
        store = self.database()
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,desired,created) VALUES('s','test',?,'read-only','open',0)",
                (str(self.home),),
            )
            db.execute(
                "INSERT INTO tasks(id,session_id,instruction,dependencies,reservation,created) VALUES('t','s','x','[]',0,0)"
            )
        hot = updater.busy_snapshot(self.state)
        self.assertFalse(hot["busy"], hot)
        self.assertEqual(1, hot["counts"]["open_queued_tasks"])
        self.assertIn("open_queued_tasks", updater.busy_snapshot(self.state, hot=False)["reasons"])
        with store.connection(write=True) as db:
            db.execute("UPDATE sessions SET desired='paused'")
            db.execute("UPDATE tasks SET status='running'")
        hot_active = updater.busy_snapshot(self.state)
        self.assertFalse(hot_active["busy"], hot_active)
        self.assertEqual(1, hot_active["counts"]["active_tasks"])
        self.assertIn("active_tasks", updater.busy_snapshot(self.state, hot=False)["reasons"])

    def test_live_worker_is_allowed_in_hot_mode_while_native_helper_still_blocks(self):
        store = self.database()
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,worker_pid,worker_start,worker_version,created) "
                "VALUES('s','test',?,'read-only',123,'boot:1','0.2.8',0)",
                (str(self.home),),
            )
        with (
            patch("agent_bridge.worker.process_identity", return_value="boot:1"),
            patch.object(updater, "_children", return_value=["ssh", "python"]),
        ):
            drain_result = updater.busy_snapshot(self.state, broker_pid=456, hot=False)
            hot_result = updater.busy_snapshot(self.state, broker_pid=456, hot=True)
        self.assertIn("live_workers", drain_result["reasons"])
        self.assertIn("broker_helpers", drain_result["reasons"])
        self.assertNotIn("live_workers", hot_result["reasons"])
        self.assertEqual(["broker_helpers"], hot_result["reasons"])

    def test_incompatible_live_worker_blocks_hot_update_when_min_worker_version_increases(self):
        store = self.database()
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,worker_pid,worker_start,worker_version,created) "
                "VALUES('old','test',?,'read-only',123,'boot:1',NULL,0)",
                (str(self.home),),
            )
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,worker_pid,worker_start,worker_version,created) "
                "VALUES('new','test',?,'read-only',124,'boot:2','0.2.9',0)",
                (str(self.home),),
            )
        with patch("agent_bridge.worker.process_identity", side_effect=lambda pid: f"boot:{pid - 122}"):
            compat_old = updater.busy_snapshot(self.state, min_worker_version="0.2.0")
            blocked = updater.busy_snapshot(self.state, min_worker_version="0.2.9")
        self.assertFalse(compat_old["busy"], compat_old)
        self.assertTrue(blocked["busy"])
        self.assertEqual(["incompatible_workers"], blocked["reasons"])
        self.assertEqual(1, blocked["counts"]["incompatible_workers"])

    def test_reused_pid_and_ssh_relay_do_not_count_as_workers_or_helper(self):
        store = self.database()
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,worker_pid,worker_start,created) VALUES('s','test',?,'read-only',123,'old',0)",
                (str(self.home),),
            )
        with (
            patch("agent_bridge.worker.process_identity", return_value="new"),
            patch.object(updater, "_children", return_value=["ssh"]),
        ):
            self.assertFalse(updater.busy_snapshot(self.state, broker_pid=456)["busy"])

    def test_inflight_api_request_prevents_update(self):
        store = self.database()
        with store.connection(write=True) as db:
            db.execute("INSERT INTO api_requests VALUES('r','f',NULL)")
        self.assertIn("api_inflight", updater.busy_snapshot(self.state)["reasons"])

    def test_macos_process_sampling_failures_keep_update_state_unknown(self):
        self.database()
        for failure in ("exit", "timeout", "read"):
            with self.subTest(failure=failure):
                process = MagicMock(pid=123, returncode=2 if failure == "exit" else 0)
                process.__enter__.return_value = process
                process.communicate.return_value = ("", None)
                if failure == "timeout":
                    process.communicate.side_effect = [
                        subprocess.TimeoutExpired("ps", 5, output="private-output", stderr="private-stderr"),
                        ("", None),
                    ]
                elif failure == "read":
                    process.communicate.side_effect = OSError("private-stderr")
                with (
                    patch.object(updater.sys, "platform", "darwin"),
                    patch.object(updater.subprocess, "Popen", return_value=process),
                ):
                    result = updater.busy_snapshot(self.state, broker_pid=os.getpid())
                self.assertTrue(result["busy"])
                self.assertEqual(["update_state_unknown"], result["reasons"])
                self.assertNotIn("broker_helpers", result["counts"])
                self.assertNotIn("private-", json.dumps(result))
                if failure != "exit":
                    process.kill.assert_called_once_with()
                if failure == "timeout":
                    self.assertEqual([call(timeout=5), call()], process.communicate.call_args_list)
                process.__exit__.assert_called_once()


class ChildProcessTests(unittest.TestCase):
    def test_linux_ignores_zombies_and_disappeared_processes_but_keeps_live_descendants(self):
        files = {
            "/proc/10/task/10/children": "11 12 13 16",
            "/proc/11/stat": "11 (defunct helper) Z 10 0",
            "/proc/12/stat": "12 (ssh client) S 10 0",
            "/proc/12/comm": "ssh\n",
            "/proc/12/task/12/children": "14",
            "/proc/13/stat": "13 (antigravity) R 10 0",
            "/proc/13/comm": "antigravity\n",
            "/proc/13/task/13/children": "",
            "/proc/14/stat": "14 (python) S 12 0",
            "/proc/14/comm": "python\n",
            "/proc/14/task/14/children": "",
        }

        def read(path, *args, **kwargs):
            try:
                return files[str(path)]
            except KeyError as error:
                raise FileNotFoundError(str(path)) from error

        with (
            patch.object(updater.sys, "platform", "linux"),
            patch.object(updater.Path, "read_text", autospec=True, side_effect=read) as reader,
        ):
            names = updater._children(10)
        self.assertEqual(["ssh", "antigravity", "python"], names)
        self.assertNotIn("/proc/11/comm", [str(call.args[0]) for call in reader.call_args_list])

    def test_macos_ignores_zombie_status_suffixes_and_reads_full_executable_without_arguments(self):
        processes = "\n".join(
            (
                "10 1 Ss /Users/user/managed/bin/python",
                "11 10 Z+ /opt/homebrew/bin/python3",
                "12 10 S /usr/bin/ssh",
                "13 10 S+ /opt/homebrew/bin/antigravity-real",
                "14 12 Z /opt/homebrew/bin/python3",
                "15 12 S /opt/homebrew/bin/python3",
                "16 999 S /opt/homebrew/bin/unrelated",
                "17 10 S /bin/ps",
                "18 10 S /bin/ps",
            )
        )
        process = MagicMock(pid=18, returncode=0)
        process.__enter__.return_value = process
        process.communicate.return_value = (processes, None)
        with (
            patch.object(updater.sys, "platform", "darwin"),
            patch.object(updater.subprocess, "Popen", return_value=process) as popen,
        ):
            names = updater._children(10)
        self.assertEqual(["ssh", "antigravity-real", "ps", "python3"], names)
        popen.assert_called_once_with(
            ["ps", "-ww", "-axo", "pid=,ppid=,stat=,comm="],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        process.communicate.assert_called_once_with(timeout=5)

    def test_macos_current_process_with_only_sampler_has_no_children(self):
        process = MagicMock(pid=123, returncode=0)
        process.__enter__.return_value = process
        process.communicate.return_value = (f"123 {os.getpid()} S /bin/ps\n", None)
        with (
            patch.object(updater.sys, "platform", "darwin"),
            patch.object(updater.subprocess, "Popen", return_value=process),
        ):
            self.assertEqual([], updater._children(os.getpid()))


class ReleaseValidationTests(UpdateFixture):
    def release(self):
        return {
            "id": 1,
            "tag_name": "v0.2.0",
            "draft": False,
            "prerelease": False,
            "assets": [
                {"name": "cli_agent_bridge-0.2.0-py3-none-any.whl", "size": 123},
                {"name": "SHA256SUMS", "size": 90},
            ],
        }

    def test_only_exact_wheel_from_configured_repository_is_selected(self):
        with patch.object(updater, "_run", return_value=json.dumps(self.release())) as run:
            release = updater.latest_release(updater.REPOSITORY)
        self.assertEqual("cli_agent_bridge-0.2.0-py3-none-any.whl", release["wheel"])
        self.assertEqual(
            ["gh", "api", "--hostname", "github.com", "repos/qinjian623/cli-agent-bridge/releases/latest"],
            run.call_args.args[0],
        )

    def test_unstable_tags_and_missing_duplicate_or_oversized_assets_are_rejected(self):
        cases = []
        for field, value in (
            ("tag_name", "../../evil"),
            ("tag_name", "v1.0.0-rc1"),
            ("draft", True),
            ("prerelease", True),
            ("assets", []),
        ):
            data = self.release()
            data[field] = value
            cases.append(data)
        data = self.release()
        data["assets"].append(data["assets"][0])
        cases.append(data)
        data = self.release()
        data["assets"][0]["size"] = 100_000_001
        cases.append(data)
        cases.extend(([], {"id": 0}, {"id": 1, "tag_name": "v0.2.0", "assets": [None]}))
        for data in cases:
            with (
                self.subTest(data=data),
                patch.object(updater, "_run", return_value=json.dumps(data)),
                self.assertRaises(ValueError),
            ):
                updater.latest_release(updater.REPOSITORY)

    def test_checksum_must_match_once_and_symlinks_are_rejected(self):
        name = "cli_agent_bridge-0.2.0-py3-none-any.whl"
        (self.home / name).write_bytes(b"wheel")
        digest = hashlib.sha256(b"wheel").hexdigest()
        (self.home / "SHA256SUMS").write_text(f"{digest}  {name}\n")
        self.assertEqual(digest, updater.verify_wheel(self.home, name))
        for text in (f"{'0' * 64}  {name}\n", f"{digest}  {name}\n{digest}  {name}\n", ""):
            (self.home / "SHA256SUMS").write_text(text)
            with self.assertRaises(ValueError):
                updater.verify_wheel(self.home, name)
        (self.home / "SHA256SUMS").unlink()
        (self.home / "real-checksums").write_text(f"{digest}  {name}\n")
        (self.home / "SHA256SUMS").symlink_to(self.home / "real-checksums")
        with self.assertRaises(ValueError):
            updater.verify_wheel(self.home, name)

    def test_private_settings_and_managed_current_are_required(self):
        (self.state / "update.json").chmod(0o644)
        with self.assertRaises(ValueError):
            updater.Updater(self.state)
        (self.state / "update.json").chmod(0o600)
        self.updater._switch(self.home)
        with self.assertRaises(ValueError):
            self.updater.installed()

    def test_release_versions_compare_numerically(self):
        with patch.object(updater, "latest_release", return_value={"tag": "v0.10.0"}):
            self.assertTrue(self.updater.check()["update_available"])
        updater._write(self.old / "release.json", {"tag": "v1.0.0"})
        with patch.object(updater, "latest_release", return_value={"tag": "v0.10.0"}):
            self.assertFalse(self.updater.check()["update_available"])


class ActivationTests(UpdateFixture):
    def prepare(self, *args):
        if args[1] == "agent_update_prepare":
            return {"token": "only-in-memory", "expires_at": time.time() + 180}
        self.assertEqual("agent_update_finish", args[1])
        self.assertEqual({"token": "only-in-memory"}, args[2])
        return {"maintenance": False}

    def test_update_switches_after_prepare_and_preserves_dirty_source_and_database(self):
        source = self.home / "source"
        source.mkdir()
        dirty = source / "uncommitted.txt"
        dirty.write_text("keep me")
        database = self.state / "bridge.sqlite"
        database.write_bytes(b"not copied or restored")
        with (
            patch("agent_bridge.broker.request", new=AsyncMock(side_effect=self.prepare)) as request,
            patch.object(updater, "busy_snapshot", return_value={"busy": False}),
            patch.object(self.updater, "_restart") as restart,
            patch.object(self.updater, "_healthy", new=AsyncMock()),
        ):
            result = asyncio.run(self.updater._activate(self.new, self.digest))
        self.assertEqual("updated", result["status"])
        self.assertEqual(self.new, self.updater.current.resolve())
        restart.assert_called_once()
        self.assertEqual(["agent_update_prepare", "agent_update_finish"], [c.args[1] for c in request.call_args_list])
        self.assertEqual("keep me", dirty.read_text())
        self.assertEqual(b"not copied or restored", database.read_bytes())

    def test_new_start_failure_restores_previous_program_without_database_rollback(self):
        database = self.state / "bridge.sqlite"
        database.write_bytes(b"new schema stays")
        with (
            patch("agent_bridge.broker.request", new=AsyncMock(side_effect=self.prepare)),
            patch.object(updater, "busy_snapshot", return_value={"busy": False}),
            patch.object(self.updater, "_restart", side_effect=[RuntimeError("failed"), None]) as restart,
            patch.object(self.updater, "_healthy", new=AsyncMock()),
        ):
            result = asyncio.run(self.updater._activate(self.new, self.digest))
        self.assertEqual("rolled_back", result["status"])
        self.assertEqual(self.old, self.updater.current.resolve())
        self.assertEqual(2, restart.call_count)
        self.assertEqual(b"new schema stays", database.read_bytes())
        self.assertFalse(self.new.exists())

    def test_uncertain_restart_is_not_repeated_or_unlocked(self):
        with (
            patch("agent_bridge.broker.request", new=AsyncMock(side_effect=self.prepare)) as request,
            patch.object(updater, "busy_snapshot", return_value={"busy": False}),
            patch.object(self.updater, "_restart", side_effect=updater.UpdateUncertain()) as restart,
        ):
            with self.assertRaises(updater.UpdateUncertain):
                asyncio.run(self.updater._activate(self.new, self.digest))
        restart.assert_called_once()
        self.assertEqual(["agent_update_prepare"], [c.args[1] for c in request.call_args_list])
        self.assertTrue(self.new.exists())

    def test_uncertain_rollback_keeps_candidate_files_for_possible_live_process(self):
        with (
            patch("agent_bridge.broker.request", new=AsyncMock(side_effect=self.prepare)),
            patch.object(updater, "busy_snapshot", return_value={"busy": False}),
            patch.object(self.updater, "_restart", side_effect=[RuntimeError(), updater.UpdateUncertain()]),
        ):
            with self.assertRaises(updater.UpdateUncertain):
                asyncio.run(self.updater._activate(self.new, self.digest))
        self.assertTrue(self.new.exists())

    def test_configuration_change_or_work_arriving_after_staging_defers_before_switch(self):
        for changed in (True, False):
            with self.subTest(changed=changed):
                target = self.new if changed else self.root / "releases/another"
                target.mkdir(exist_ok=True)
                with (
                    patch("agent_bridge.broker.request", new=AsyncMock(side_effect=self.prepare)),
                    patch.object(updater, "busy_snapshot", return_value={"busy": True, "reasons": ["live_workers"]}),
                    patch.object(self.updater, "_restart") as restart,
                ):
                    result = asyncio.run(self.updater._activate(target, "changed" if changed else self.digest))
                self.assertEqual("deferred", result["status"])
                self.assertEqual(self.old, self.updater.current.resolve())
                restart.assert_not_called()
                self.assertFalse(target.exists())

    def test_busy_run_never_downloads_or_restarts(self):
        with (
            patch.object(self.updater, "service_managed", return_value=True),
            patch.object(self.updater, "check", return_value={"update_available": True}),
            patch.object(updater, "busy_snapshot", return_value={"busy": True, "reasons": ["live_workers"]}),
            patch.object(self.updater, "stage") as stage,
        ):
            self.assertEqual("deferred", self.updater.run()["status"])
        stage.assert_not_called()
        self.assertEqual("deferred", json.loads((self.state / "update-status.json").read_text())["status"])

    def test_concurrent_updater_does_not_start_second_download(self):
        lock = (self.root / "update.lock").open("a")
        (self.root / "update.lock").chmod(0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with patch.object(self.updater, "check") as check:
                self.assertEqual("another_updater", self.updater.run()["reason"])
                check.assert_not_called()
        finally:
            lock.close()

    def test_previous_uncertain_status_never_restarts_again(self):
        updater._write(self.state / "update-status.json", {"status": "uncertain"})
        with patch.object(self.updater, "_restart") as restart:
            self.assertEqual("uncertain", self.updater.run()["status"])
        restart.assert_not_called()

    def test_service_migration_is_required_before_automatic_updates(self):
        with (
            patch.object(self.updater, "service_managed", return_value=False),
            patch.object(self.updater, "stage") as stage,
        ):
            self.assertEqual("broker_service_not_migrated", self.updater.run()["reason"])
        stage.assert_not_called()

    def test_candidate_requires_worker_drain_forces_non_hot_activation(self):
        (self.new / "release.json").write_text(
            json.dumps({"tag": "v0.2.0", "requires_worker_drain": True, "min_worker_version": "0.2.5"})
        )
        seen = {}

        def prepare(*args):
            if args[1] == "agent_update_prepare":
                seen["prepare_args"] = args[2]
                return {"token": "only-in-memory", "expires_at": time.time() + 180}
            return {"maintenance": False}

        with (
            patch("agent_bridge.broker.request", new=AsyncMock(side_effect=prepare)),
            patch.object(updater, "busy_snapshot", return_value={"busy": False}) as snapshot,
            patch.object(self.updater, "_restart"),
            patch.object(self.updater, "_healthy", new=AsyncMock()),
        ):
            result = asyncio.run(self.updater._activate(self.new, self.digest))
        self.assertEqual("updated", result["status"])
        self.assertFalse(result["hot"])
        self.assertEqual({"hot": False, "min_worker_version": "0.2.5"}, seen["prepare_args"])
        self.assertFalse(snapshot.call_args.kwargs["hot"])
        self.assertEqual("0.2.5", snapshot.call_args.kwargs["min_worker_version"])

    def test_non_hot_prepare_falls_back_to_empty_args_on_legacy_broker(self):
        from agent_bridge.errors import BridgeError

        seen_prepare_args = []

        def prepare(*args):
            if args[1] == "agent_update_prepare":
                seen_prepare_args.append(args[2])
                if args[2]:
                    raise BridgeError("invalid_request", "unexpected params")
                return {"token": "only-in-memory", "expires_at": time.time() + 180}
            return {"maintenance": False}

        with (
            patch("agent_bridge.broker.request", new=AsyncMock(side_effect=prepare)),
            patch.object(updater, "busy_snapshot", return_value={"busy": False}),
            patch.object(self.updater, "_restart"),
            patch.object(self.updater, "_healthy", new=AsyncMock()),
        ):
            result = asyncio.run(self.updater._activate(self.new, self.digest, hot=False))
        self.assertEqual("updated", result["status"])
        self.assertEqual([{"hot": False}, {}], seen_prepare_args)

    def test_run_drain_workers_flag_forwards_hot_false(self):
        with patch.object(updater, "Updater") as mock_cls:
            mock_cls.return_value.run.return_value = {"status": "up_to_date"}
            updater.main(["--state", str(self.state), "--drain-workers", "run"])
        mock_cls.return_value.run.assert_called_once_with(drain_workers=True)

    def test_stage_persists_preflight_compatibility_metadata(self):
        release = {"tag": "v0.2.0", "wheel": "cli_agent_bridge-0.2.0-py3-none-any.whl", "release_id": 1}
        generated = {}

        def command(argv, **_):
            if argv[:3] == ["gh", "release", "download"]:
                download = Path(argv[argv.index("--dir") + 1])
                (download / release["wheel"]).write_bytes(b"test-wheel")
                (download / "SHA256SUMS").write_text(
                    hashlib.sha256(b"test-wheel").hexdigest() + "  " + release["wheel"] + "\n"
                )
            elif argv[:2] == ["uv", "venv"]:
                generated["target"] = Path(argv[-1])
                (generated["target"] / "bin").mkdir()
            elif argv[0] == str(generated["target"] / "bin/python"):
                skill = generated["target"] / "lib/python/site-packages/agent_bridge/skill"
                skill.mkdir(parents=True)
                (skill / "SKILL.md").write_text("packaged skill")
                return str(skill)
            return ""

        compat = {"requires_worker_drain": False, "min_worker_version": "0.2.5"}
        with (
            patch.object(updater, "_run", side_effect=command),
            patch.object(self.updater, "preflight", return_value=compat),
        ):
            target = self.updater.stage(release)
        saved = json.loads((target / "release.json").read_text())
        self.assertFalse(saved["requires_worker_drain"])
        self.assertEqual("0.2.5", saved["min_worker_version"])

    def test_bootstrap_only_stages_without_service_or_current_changes(self):
        with (
            patch.object(
                self.updater, "check", return_value={"update_available": True, "available": {"tag": "v0.2.0"}}
            ),
            patch.object(self.updater, "stage", return_value=self.new),
            patch.object(self.updater, "_restart") as restart,
        ):
            result = self.updater.bootstrap()
        self.assertFalse(result["activated"])
        self.assertEqual(self.old, self.updater.current.resolve())
        restart.assert_not_called()


class TimerAndLauncherTests(UpdateFixture):
    def test_launcher_uses_physical_release_python_and_preserves_arguments(self):
        binary = self.new / "bin"
        binary.mkdir()
        python = binary / "python"
        python.write_text('#!/bin/sh\nprintf "%s\\n" "$0" "$@"\n')
        python.chmod(0o700)
        self.updater._switch(self.new)
        script = self.root / "launcher"
        script.write_text(updater._launcher(self.root, Path(sys.executable), "agent_bridge.cli"))
        script.chmod(0o700)
        result = subprocess.check_output([str(script), "--state", "with spaces", "serve"], text=True).splitlines()
        self.assertEqual([str(python), "-m", "agent_bridge.cli", "--state", "with spaces", "serve"], result)

    def test_linux_timer_is_hourly_with_jitter_and_does_not_enable_by_default(self):
        (self.state / "update.json").unlink()
        with patch.object(updater.Path, "home", return_value=self.home), patch.object(updater, "_run") as run:
            result = updater.install(self.state, service_kind="systemd", root=self.root)
        self.assertFalse(result["enabled"])
        self.assertFalse(result["broker_service_migrated"])
        run.assert_not_called()
        timer = Path(result["timer"]).read_text()
        self.assertIn("OnUnitActiveSec=3600s", timer)
        self.assertIn("RandomizedDelaySec=5m", timer)
        service = (Path(result["timer"]).parent / "cli-agent-bridge-update.service").read_text()
        self.assertIn('"--jitter" "300"', service)
        self.assertNotIn("TOKEN", service)

    def test_macos_launchagent_has_hourly_interval_and_correct_state(self):
        (self.state / "update.json").unlink()
        with patch.object(updater.Path, "home", return_value=self.home), patch.object(updater, "_run") as run:
            result = updater.install(self.state, service_kind="launchd", root=self.root)
        value = plistlib.loads(Path(result["timer"]).read_bytes())
        self.assertEqual(3600, value["StartInterval"])
        self.assertIn(str(self.state), value["ProgramArguments"])
        self.assertNotIn("KeepAlive", value)
        run.assert_not_called()

    def test_enabling_requires_service_migration_and_existing_settings_are_not_overwritten(self):
        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater.Updater, "service_managed", return_value=False),
        ):
            with self.assertRaises(ValueError):
                updater.install(self.state, repository="wrong/repo", root=self.root)
        self.assertEqual(updater.REPOSITORY, json.loads((self.state / "update.json").read_text())["repository"])

    def test_errors_do_not_print_credentials_or_external_stderr(self):
        failed = Mock(returncode=1, stderr="secret-token", stdout="secret-token")
        with patch.object(updater.subprocess, "run", return_value=failed), self.assertRaises(RuntimeError) as error:
            updater._run(["gh", "api", "repos/example/release"])
        self.assertNotIn("secret-token", str(error.exception))

    def test_partial_stage_is_removed_if_install_or_preflight_fails(self):
        release = {"tag": "v0.2.0", "wheel": "cli_agent_bridge-0.2.0-py3-none-any.whl", "release_id": 1}
        with patch.object(updater, "_run", side_effect=RuntimeError("download failed")):
            with self.assertRaises(RuntimeError):
                self.updater.stage(release)
        self.assertEqual({"old", "new"}, {p.name for p in (self.root / "releases").iterdir()})

    def test_service_controller_is_linux_or_macos_without_shell_interpolation(self):
        with patch.object(updater, "_run") as run:
            self.updater._restart()
            self.assertEqual(["systemctl", "--user", "restart", "cli-agent-bridge.service"], run.call_args.args[0])
            self.updater.settings["service_kind"] = "launchd"
            self.updater.settings["service_name"] = "com.cli-agent-bridge.broker"
            self.updater._restart()
            self.assertEqual(
                ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/com.cli-agent-bridge.broker"],
                run.call_args.args[0],
            )


class CrashAndHealthTests(UpdateFixture):
    def test_interrupt_after_pointer_switch_keeps_a_journal_and_never_replays_restart(self):
        lease = {"token": "temporary-token", "expires_at": time.time() + 180, "broker_pid": 123}
        with (
            patch("agent_bridge.broker.request", new=AsyncMock(return_value=lease)),
            patch.object(updater, "busy_snapshot", return_value={"busy": False}),
            patch.object(self.updater, "_restart", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                asyncio.run(self.updater._activate(self.new, self.digest))
        journal = json.loads((self.state / "update-status.json").read_text())
        self.assertEqual("activating", journal["status"])
        self.assertEqual("0.2.0", journal["target_version"])
        self.assertNotIn("temporary-token", json.dumps(journal))
        self.assertEqual(self.new, self.updater.current.resolve())
        second = updater.Updater(self.state)
        with (
            patch.object(second, "_actual", new=AsyncMock(return_value={"version": "0.1.0", "maintenance": False})),
            patch.object(second, "_restart") as restart,
            patch.object(second, "check") as check,
        ):
            result = second.run()
        self.assertEqual("uncertain", result["status"])
        restart.assert_not_called()
        check.assert_not_called()
        self.assertTrue(self.new.exists())

    def test_interrupted_switch_recovers_only_after_target_version_is_healthy_and_unlocked(self):
        self.updater._switch(self.new)
        updater._write(self.state / "update-status.json", {"status": "activating", "target_version": "0.2.0"})
        with (
            patch.object(
                self.updater, "_actual", new=AsyncMock(return_value={"version": "0.2.0", "maintenance": False})
            ),
            patch.object(self.updater, "_restart") as restart,
        ):
            result = self.updater.run()
        self.assertEqual("updated", result["status"])
        self.assertTrue(result["recovered"])
        restart.assert_not_called()

    def test_current_symlink_alone_does_not_prove_running_version_is_current(self):
        with (
            patch.object(self.updater, "service_managed", return_value=True),
            patch.object(
                self.updater, "check", return_value={"update_available": False, "installed": {"tag": "v0.1.0"}}
            ),
            patch.object(self.updater, "_actual", new=AsyncMock(return_value={"version": "0.0.9"})),
            patch.object(self.updater, "_restart") as restart,
        ):
            self.assertEqual("uncertain", self.updater.run()["status"])
        restart.assert_not_called()

    def test_health_verifies_broker_version_and_selftest_pid(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        for result in ({"version": "old", "pid": 123}, {"version": "0.2.0", "pid": 456}):
            with (
                self.subTest(result=result),
                patch("agent_bridge.broker.request", new=AsyncMock(return_value=result)),
                self.assertRaises(RuntimeError),
            ):
                asyncio.run(self.updater._healthy(self.state, "0.2.0", process))
        with patch("agent_bridge.broker.request", new=AsyncMock(return_value={"version": "0.2.0", "pid": 123})):
            asyncio.run(self.updater._healthy(self.state, "0.2.0", process))

    def test_prepare_connection_failure_defers_and_removes_only_unstarted_candidate(self):
        with (
            patch("agent_bridge.broker.request", new=AsyncMock(side_effect=ConnectionRefusedError)),
            patch.object(self.updater, "_restart") as restart,
        ):
            result = asyncio.run(self.updater._activate(self.new, self.digest))
        self.assertEqual("deferred", result["status"])
        self.assertFalse(self.new.exists())
        self.assertTrue(self.old.exists())
        restart.assert_not_called()

    def test_stage_exports_packaged_skill_and_never_installs_over_source_checkout(self):
        release = {"tag": "v0.2.0", "wheel": "cli_agent_bridge-0.2.0-py3-none-any.whl", "release_id": 1}
        generated = {}

        def command(argv, **_):
            if argv[:3] == ["gh", "release", "download"]:
                download = Path(argv[argv.index("--dir") + 1])
                (download / release["wheel"]).write_bytes(b"test-wheel")
                (download / "SHA256SUMS").write_text(
                    hashlib.sha256(b"test-wheel").hexdigest() + "  " + release["wheel"] + "\n"
                )
                self.assertIn("github.com/" + updater.REPOSITORY, argv)
            elif argv[:2] == ["uv", "venv"]:
                generated["target"] = Path(argv[-1])
                (generated["target"] / "bin").mkdir()
            elif argv[0] == str(generated["target"] / "bin/python"):
                skill = generated["target"] / "lib/python/site-packages/agent_bridge/skill"
                skill.mkdir(parents=True)
                (skill / "SKILL.md").write_text("packaged skill")
                return str(skill)
            return ""

        with patch.object(updater, "_run", side_effect=command), patch.object(self.updater, "preflight"):
            target = self.updater.stage(release)
        self.assertEqual("packaged skill", (target / "skill/SKILL.md").read_text())
        self.assertTrue((target / "skill").is_symlink())
        self.assertTrue(self.old.exists())
        self.assertEqual(self.old, self.updater.current.resolve())

    def test_cli_failure_has_nonzero_exit_and_only_sanitized_status(self):
        with (
            patch.object(updater, "Updater", side_effect=RuntimeError("never-save-secret")),
            patch("builtins.print") as output,
        ):
            with self.assertRaises(SystemExit) as error:
                updater.main(["--state", str(self.state), "check"])
        self.assertEqual(1, error.exception.code)
        self.assertNotIn("never-save-secret", str(output.call_args))
        result = json.loads((self.state / "update-status.json").read_text())
        self.assertEqual("failed", result["status"])
        self.assertNotIn("never-save-secret", json.dumps(result))


class MirrorUrlReleaseTests(UpdateFixture):
    def _start_server(self, routes: dict[str, tuple[int, bytes, dict[str, str]]]):
        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                entry = routes.get(self.path)
                if entry is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                code, body, extra_headers = entry
                self.send_response(code)
                for hk, hv in extra_headers.items():
                    self.send_header(hk, hv)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        port = server.server_address[1]
        return f"http://127.0.0.1:{port}/cli-agent-bridge"

    def test_validate_release_url_accepts_tailscale_loopback_https_and_rejects_unsafe(self):
        self.assertEqual(
            "http://192.0.2.20:8088/cli-agent-bridge",
            updater.validate_release_url("http://192.0.2.20:8088/cli-agent-bridge/"),
        )
        self.assertEqual(
            "http://cn-peer-relay.taild20a97.ts.net:8088",
            updater.validate_release_url("http://cn-peer-relay.taild20a97.ts.net:8088"),
        )
        self.assertEqual(
            "http://[fd7a:115c:a1e0::b93a:f93b]:8088",
            updater.validate_release_url("http://[fd7a:115c:a1e0::b93a:f93b]:8088/"),
        )
        self.assertEqual(
            "https://releases.example.com/ota",
            updater.validate_release_url("https://releases.example.com/ota/"),
        )
        for bad in (
            "",
            "   ",
            "file:///tmp/ota",
            "http://82.156.182.75:8088/cli-agent-bridge",
            "http://192.0.2.10:8088/cli-agent-bridge",
            "http://example.com/cli-agent-bridge",
            "https://user:pass@releases.example.com/ota",
            "https://releases.example.com/ota?token=1",
            "https://releases.example.com/ota#frag",
            "https://releases.example.com/../evil",
            "http://192.0.2.20:8088/ota\nnewline",
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                updater.validate_release_url(bad)

    def test_install_and_set_url_manage_release_url_without_overwriting_other_fields(self):
        (self.state / "update.json").unlink()
        with patch.object(updater.Path, "home", return_value=self.home), patch.object(updater, "_run"):
            res = updater.install(
                self.state,
                service_kind="systemd",
                root=self.root,
                release_url="http://192.0.2.20:8088/cli-agent-bridge/",
            )
        saved = json.loads(Path(res["settings"]).read_text())
        self.assertEqual("http://192.0.2.20:8088/cli-agent-bridge", saved["release_url"])

        with patch("builtins.print"):
            updater.main(["--state", str(self.state), "set-url", "--release-url", "https://mirror.example.com/ota/"])
        self.assertEqual(
            "https://mirror.example.com/ota",
            json.loads((self.state / "update.json").read_text())["release_url"],
        )
        with patch("builtins.print"):
            updater.main(["--state", str(self.state), "set-url", "--release-url", ""])
        self.assertNotIn("release_url", json.loads((self.state / "update.json").read_text()))

        with patch("builtins.print"), self.assertRaises(SystemExit) as missing:
            updater.main(["--state", str(self.state), "set-url"])
        self.assertEqual(1, missing.exception.code)
        self.assertFalse((self.state / "update-status.json").exists())

        for preserved in (
            {"status": "activating", "target_version": "0.2.11"},
            {"status": "uncertain", "target_version": "0.2.11"},
        ):
            updater._write(self.state / "update-status.json", preserved)
            lock_fd = os.open(self.root / "update.lock", os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch("builtins.print") as lock_out, self.assertRaises(SystemExit) as locked_err:
                    updater.main(
                        ["--state", str(self.state), "set-url", "--release-url", "https://mirror.example.com/ota"]
                    )
                self.assertEqual(1, locked_err.exception.code)
                self.assertEqual("BlockingIOError", json.loads(lock_out.call_args.args[0])["error_type"])
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            with patch("builtins.print"), self.assertRaises(SystemExit):
                updater.main(["--state", str(self.state), "--release-url", "http://192.0.2.20:8088/ota", "check"])
            with (
                patch("builtins.print"),
                patch.object(updater, "latest_release", side_effect=RuntimeError("boom")),
                self.assertRaises(SystemExit),
            ):
                updater.main(["--state", str(self.state), "check"])
            saved_status = json.loads((self.state / "update-status.json").read_text())
            self.assertEqual(preserved["status"], saved_status["status"])
            self.assertEqual("0.2.11", saved_status["target_version"])

        # Simulate a concurrent run() acquiring update.lock and writing 'activating' during a failing check().
        (self.state / "update-status.json").unlink()
        hold_lock_fd = os.open(self.root / "update.lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            def concurrent_writer_holds_lock(*_, **__):
                fcntl.flock(hold_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                updater._write(self.state / "update-status.json", {"status": "activating", "target_version": "0.2.12"})
                raise RuntimeError("check failed while activating acquired lock")

            with (
                patch("builtins.print"),
                patch.object(updater, "latest_release", side_effect=concurrent_writer_holds_lock),
                self.assertRaises(SystemExit),
            ):
                updater.main(["--state", str(self.state), "check"])
            status_after_lock = json.loads((self.state / "update-status.json").read_text())
            self.assertEqual("activating", status_after_lock["status"])
            self.assertEqual("0.2.12", status_after_lock["target_version"])
        finally:
            fcntl.flock(hold_lock_fd, fcntl.LOCK_UN)
            os.close(hold_lock_fd)

        # Even if concurrent run() released the lock right before _record_cli_failure acquired it, 'activating' wins.
        (self.state / "update-status.json").unlink()

        def concurrent_writer_released_before_record(*_, **__):
            with updater._acquire_update_lock(self.root):
                updater._write(self.state / "update-status.json", {"status": "activating", "target_version": "0.2.13"})
            raise RuntimeError("check failed after activating wrote and unlocked")

        with (
            patch("builtins.print"),
            patch.object(updater, "latest_release", side_effect=concurrent_writer_released_before_record),
            self.assertRaises(SystemExit),
        ):
            updater.main(["--state", str(self.state), "check"])
        status_after_release = json.loads((self.state / "update-status.json").read_text())
        self.assertEqual("activating", status_after_release["status"])
        self.assertEqual("0.2.13", status_after_release["target_version"])

        # When update.json cannot be parsed, lock location is unknown and existing status must not be overwritten.
        (self.state / "update.json").write_text("{corrupt")
        with patch("builtins.print"), self.assertRaises(SystemExit) as corrupt_err:
            updater.main(["--state", str(self.state), "check"])
        self.assertEqual(1, corrupt_err.exception.code)
        status_after_corrupt = json.loads((self.state / "update-status.json").read_text())
        self.assertEqual("activating", status_after_corrupt["status"])
        self.assertEqual("0.2.13", status_after_corrupt["target_version"])

    def _start_proxy_trap(self) -> tuple[str, list[str]]:
        proxy_hits: list[str] = []

        class _ProxyHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                proxy_hits.append(self.path)
                body = b"intercepted"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002
                return

        proxy = ThreadingHTTPServer(("127.0.0.1", 0), _ProxyHandler)
        thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(proxy.shutdown)
        self.addCleanup(proxy.server_close)
        return f"http://127.0.0.1:{proxy.server_address[1]}", proxy_hits

    @staticmethod
    def _build_minimal_wheel(version: str = "0.2.0", skill_text: str = "mirror skill") -> bytes:
        dist_info = f"cli_agent_bridge-{version}.dist-info"
        files = {
            "agent_bridge/__init__.py": f'__version__ = "{version}"\n'.encode(),
            "agent_bridge/skill/SKILL.md": skill_text.encode(),
            f"{dist_info}/METADATA": (
                f"Metadata-Version: 2.1\nName: cli-agent-bridge\nVersion: {version}\n".encode()
            ),
            f"{dist_info}/WHEEL": (
                b"Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
            ),
        }
        buf = io.BytesIO()
        rows = []
        with zipfile.ZipFile(buf, "w") as zf:
            for name, data in files.items():
                zf.writestr(name, data)
                h = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
                rows.append(f"{name},sha256={h},{len(data)}")
            rows.append(f"{dist_info}/RECORD,,")
            zf.writestr(f"{dist_info}/RECORD", ("\n".join(rows) + "\n").encode())
        return buf.getvalue()

    def test_http_mirror_check_and_stage_verify_checksum_and_bypass_proxy(self):
        tag = "v0.2.0"
        wheel_name = "cli_agent_bridge-0.2.0-py3-none-any.whl"
        wheel_bytes = b"mirror-wheel-payload"
        digest = hashlib.sha256(wheel_bytes).hexdigest()
        sums_bytes = f"{digest}  {wheel_name}\n".encode()
        manifest = updater.build_release_manifest(tag, len(wheel_bytes), len(sums_bytes), release_id=42)
        base_url = self._start_server(
            {
                "/cli-agent-bridge/latest.json": (200, json.dumps(manifest).encode(), {}),
                f"/cli-agent-bridge/releases/{tag}/{wheel_name}": (200, wheel_bytes, {}),
                f"/cli-agent-bridge/releases/{tag}/SHA256SUMS": (200, sums_bytes, {}),
            }
        )
        proxy_url, proxy_hits = self._start_proxy_trap()
        updater.set_release_url(self.state, base_url)
        client = updater.Updater(self.state)

        generated = {}

        def fake_uv_command(argv, **_):
            if argv[0] == "gh":
                raise AssertionError("Mirror update must never invoke gh CLI")
            if argv[:2] == ["uv", "venv"]:
                generated["target"] = Path(argv[-1])
                (generated["target"] / "bin").mkdir()
            elif argv[0] == str(generated["target"] / "bin/python"):
                skill = generated["target"] / "lib/python/site-packages/agent_bridge/skill"
                skill.mkdir(parents=True)
                (skill / "SKILL.md").write_text("mirror skill")
                return str(skill)
            return ""

        with (
            patch.dict(
                os.environ,
                {"HTTP_PROXY": proxy_url, "http_proxy": proxy_url, "NO_PROXY": "", "no_proxy": ""},
            ),
            patch.object(updater, "_run", side_effect=fake_uv_command),
            patch.object(client, "preflight", return_value={"requires_worker_drain": False}),
        ):
            # Confirm an unpatched opener uses the proxy when NO_PROXY is cleared.
            with urllib.request.build_opener().open(f"{base_url}/latest.json") as intercepted:
                self.assertEqual(b"intercepted", intercepted.read())
            self.assertEqual(1, len(proxy_hits))
            proxy_hits.clear()

            chk = client.check()
            self.assertTrue(chk["update_available"])
            self.assertEqual(tag, chk["available"]["tag"])
            staged = client.stage(chk["available"])
        self.assertEqual([], proxy_hits)
        self.assertEqual("mirror skill", (staged / "skill/SKILL.md").read_text())
        self.assertEqual(digest, json.loads((staged / "release.json").read_text())["sha256"])

    @unittest.skipUnless(shutil.which("uv"), "uv CLI is required")
    def test_http_mirror_stages_real_wheel_via_uv(self):
        tag = "v0.2.0"
        wheel_name = "cli_agent_bridge-0.2.0-py3-none-any.whl"
        wheel_bytes = self._build_minimal_wheel("0.2.0", "real wheel skill")
        digest = hashlib.sha256(wheel_bytes).hexdigest()
        sums_bytes = f"{digest}  {wheel_name}\n".encode()
        manifest = updater.build_release_manifest(tag, len(wheel_bytes), len(sums_bytes), release_id=7)
        base_url = self._start_server(
            {
                "/cli-agent-bridge/latest.json": (200, json.dumps(manifest).encode(), {}),
                f"/cli-agent-bridge/releases/{tag}/{wheel_name}": (200, wheel_bytes, {}),
                f"/cli-agent-bridge/releases/{tag}/SHA256SUMS": (200, sums_bytes, {}),
            }
        )
        updater.set_release_url(self.state, base_url)
        client = updater.Updater(self.state)
        with patch.object(client, "preflight", return_value={"requires_worker_drain": False}):
            chk = client.check()
            staged = client.stage(chk["available"])
        version = subprocess.check_output(
            [str(staged / "bin/python"), "-c", "import agent_bridge; print(agent_bridge.__version__)"],
            text=True,
        ).strip()
        self.assertEqual("0.2.0", version)
        self.assertEqual("real wheel skill", (staged / "skill/SKILL.md").read_text())
        self.assertEqual(digest, json.loads((staged / "release.json").read_text())["sha256"])

    def test_http_mirror_rejects_public_dns_trickle_timeout_and_unsupported_cli_flag(self):
        with (
            patch.object(
                socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 8088))],
            ),
            self.assertRaises(ValueError),
        ):
            updater.latest_release(updater.REPOSITORY, release_url="http://spoofed.ts.net:8088")

        class _SlowAndRedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                try:
                    if self.path == "/slow-headers":
                        self.connection.sendall(b"HTTP/1.1 200 OK\r\nX-Slow: ")
                        for _ in range(15):
                            time.sleep(0.06)
                            self.connection.sendall(b"a")
                        self.connection.sendall(b"\r\nContent-Length: 2\r\n\r\nok")
                    elif self.path == "/slow-redirect":
                        self.connection.sendall(
                            b"HTTP/1.1 302 Found\r\n"
                            b"Location: /ok\r\n"
                            b"Content-Length: 5000000\r\n\r\n"
                        )
                        for _ in range(10):
                            time.sleep(0.1)
                            self.connection.sendall(b"z" * 1024)
                    elif self.path == "/huge-cross-redirect":
                        self.connection.sendall(
                            b"HTTP/1.1 302 Found\r\n"
                            b"Location: http://192.0.2.20:8088/cli-agent-bridge/latest.json\r\n"
                            b"Content-Length: 5000000\r\n\r\n"
                        )
                    elif self.path == "/ok":
                        self.connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfinal")
                    else:
                        self.send_response(200)
                        self.send_header("Content-Length", "100")
                        self.end_headers()
                        for _ in range(10):
                            self.wfile.write(b"x" * 10)
                            self.wfile.flush()
                            time.sleep(0.1)
                except OSError:
                    return

            def log_message(self, format, *args):  # noqa: A002
                return

        slow = ThreadingHTTPServer(("127.0.0.1", 0), _SlowAndRedirectHandler)
        thread = threading.Thread(target=slow.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(slow.shutdown)
        self.addCleanup(slow.server_close)
        slow_base = f"http://127.0.0.1:{slow.server_address[1]}"

        for path in ("/slow-body", "/slow-headers"):
            started = time.monotonic()
            with self.subTest(path=path), self.assertRaises(TimeoutError):
                updater._download_bytes(f"{slow_base}{path}", max_bytes=1024, timeout=0.2)
            self.assertLess(time.monotonic() - started, 0.7)

        # Same-origin redirect skips huge/slow 302 body and follows to target within timeout.
        started = time.monotonic()
        self.assertEqual(b"final", updater._download_bytes(f"{slow_base}/slow-redirect", max_bytes=1024, timeout=0.4))
        self.assertLess(time.monotonic() - started, 0.4)

        with self.assertRaises(ValueError):
            updater._download_bytes(f"{slow_base}/huge-cross-redirect", max_bytes=1024, timeout=0.4)

        # Hanging DNS lookup must also respect total download deadline without waiting for resolver completion.
        dns_release = threading.Event()

        def slow_dns(*_, **__):
            dns_release.wait(timeout=2.0)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.20", 8088))]

        try:
            started = time.monotonic()
            with patch.object(socket, "getaddrinfo", side_effect=slow_dns), self.assertRaises(TimeoutError):
                updater._download_bytes("http://slow-dns.ts.net:8088/latest.json", max_bytes=1024, timeout=0.15)
            self.assertLess(time.monotonic() - started, 0.5)
        finally:
            dns_release.set()

        # Stalled TLS handshake (do_handshake_on_connect=False tracked socket) must abort at deadline.
        tls_stop = threading.Event()
        tls_srv = socket.create_server(("127.0.0.1", 0))
        self.addCleanup(tls_stop.set)
        self.addCleanup(tls_srv.close)

        def accept_and_stall_tls():
            try:
                conn, _ = tls_srv.accept()
            except OSError:
                return
            try:
                tls_stop.wait(timeout=2.0)
            finally:
                conn.close()

        threading.Thread(target=accept_and_stall_tls, daemon=True).start()
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            updater._download_bytes(
                f"https://127.0.0.1:{tls_srv.getsockname()[1]}/latest.json",
                max_bytes=1024,
                timeout=0.2,
            )
        self.assertLess(time.monotonic() - started, 0.6)

        with patch("builtins.print"), patch.object(updater, "latest_release") as fetch, self.assertRaises(SystemExit) as err:
            updater.main(["--state", str(self.state), "--release-url", "http://192.0.2.20:8088/ota", "check"])
        self.assertEqual(1, err.exception.code)
        fetch.assert_not_called()
        self.assertFalse((self.state / "update-status.json").exists())

    def test_http_mirror_rejects_tampered_wheel_redirects_and_oversized_assets(self):
        tag = "v0.2.0"
        wheel_name = "cli_agent_bridge-0.2.0-py3-none-any.whl"
        bad_wheel = b"tampered-bytes"
        wrong_sums = f"{'0' * 64}  {wheel_name}\n".encode()
        manifest = updater.build_release_manifest(tag, len(bad_wheel), len(wrong_sums))
        base_url = self._start_server(
            {
                "/cli-agent-bridge/latest.json": (200, json.dumps(manifest).encode(), {}),
                f"/cli-agent-bridge/releases/{tag}/{wheel_name}": (200, bad_wheel, {}),
                f"/cli-agent-bridge/releases/{tag}/SHA256SUMS": (200, wrong_sums, {}),
                "/redirect/latest.json": (
                    302,
                    b"",
                    {"Location": "http://192.0.2.20:8088/cli-agent-bridge/latest.json"},
                ),
                "/oversized/latest.json": (200, b"x" * 300_000, {}),
                "/broken/latest.json": (500, b"boom", {}),
            }
        )
        root_origin = base_url.rsplit("/cli-agent-bridge", 1)[0]
        updater.set_release_url(self.state, base_url)
        client = updater.Updater(self.state)
        chk = client.check()
        with self.assertRaises(ValueError):
            client.stage(chk["available"])
        self.assertEqual({"old", "new"}, {p.name for p in (self.root / "releases").iterdir()})

        with self.assertRaises(ValueError):
            client.stage({"tag": tag, "wheel": "cli_agent_bridge-0.2.0-evil.whl"})
        with self.assertRaises(ValueError):
            updater.latest_release(updater.REPOSITORY, release_url=f"{root_origin}/redirect")
        with self.assertRaises(ValueError):
            updater.latest_release(updater.REPOSITORY, release_url=f"{root_origin}/oversized")

        updater.set_release_url(self.state, f"{root_origin}/broken")
        with patch("builtins.print"), self.assertRaises(SystemExit) as http_err:
            updater.main(["--state", str(self.state), "check"])
        self.assertEqual(1, http_err.exception.code)
        self.assertEqual("failed", json.loads((self.state / "update-status.json").read_text())["status"])

    @staticmethod
    def _load_publish_module():
        import importlib.util

        script_path = Path(__file__).resolve().parent.parent / "scripts/publish_ota_relay.py"
        spec = importlib.util.spec_from_file_location("publish_ota_relay", script_path)
        mod = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(mod)
        return mod

    def _make_release_tar(self, mod, tag: str, wheel_bytes: bytes, *, release_id: int = 99) -> tuple[str, str, bytes]:
        stage_src = self.home / f"src-{tag}-{len(wheel_bytes)}-{release_id}"
        stage_src.mkdir(exist_ok=True)
        wheel = f"cli_agent_bridge-{tag[1:]}-py3-none-any.whl"
        digest = hashlib.sha256(wheel_bytes).hexdigest()
        sums_bytes = f"{digest}  {wheel}\n".encode()
        manifest = updater.build_release_manifest(tag, len(wheel_bytes), len(sums_bytes), release_id=release_id)
        (stage_src / wheel).write_bytes(wheel_bytes)
        (stage_src / "SHA256SUMS").write_bytes(sums_bytes)
        (stage_src / "latest.json").write_text(json.dumps(manifest))
        return wheel, digest, mod.build_tar_payload(stage_src, ("latest.json", wheel, "SHA256SUMS"))

    def test_remote_relay_apply_script_verifies_immutable_monotonic_and_concurrent_publish(self):
        mod = self._load_publish_module()

        tag = "v0.2.11"
        wheel_bytes = b"relay-atomic-wheel"
        wheel, digest, tar_bytes = self._make_release_tar(mod, tag, wheel_bytes, release_id=99)

        remote_root = self.home / "remote-ota"
        proc = subprocess.run(
            [sys.executable, "-c", mod._REMOTE_APPLY_SCRIPT, str(remote_root), tag, wheel],
            input=tar_bytes,
            capture_output=True,
            check=True,
        )
        out = json.loads(proc.stdout.decode("utf-8"))
        self.assertEqual(digest, out["sha256"])
        self.assertFalse(out["immutable_preserved"])
        inode_before = (remote_root / "releases" / tag).stat().st_ino
        self.assertEqual(tag, json.loads((remote_root / "latest.json").read_text())["tag_name"])
        self.assertEqual(wheel_bytes, (remote_root / "releases" / tag / wheel).read_bytes())

        # Re-publishing the identical tag must preserve the immutable tag directory in-place (no 404 swap window).
        repub = subprocess.run(
            [sys.executable, "-c", mod._REMOTE_APPLY_SCRIPT, str(remote_root), tag, wheel],
            input=tar_bytes,
            capture_output=True,
            check=True,
        )
        repub_out = json.loads(repub.stdout.decode("utf-8"))
        self.assertTrue(repub_out["immutable_preserved"])
        self.assertEqual(inode_before, (remote_root / "releases" / tag).stat().st_ino)

        # Attempting to mutate an already-published tag with different contents must fail cleanly.
        _, _, mutated_tar = self._make_release_tar(mod, tag, b"different-wheel-bytes", release_id=100)
        conflict = subprocess.run(
            [sys.executable, "-c", mod._REMOTE_APPLY_SCRIPT, str(remote_root), tag, wheel],
            input=mutated_tar,
            capture_output=True,
        )
        self.assertEqual(2, conflict.returncode)
        self.assertEqual(wheel_bytes, (remote_root / "releases" / tag / wheel).read_bytes())
        self.assertEqual([], [p for p in (remote_root / "releases").iterdir() if p.name.startswith(".stage-")])

        # Publishing a newer release advances latest.json, and re-publishing an older tag never rolls back latest.json.
        wheel12, _, tar12 = self._make_release_tar(mod, "v0.2.12", b"wheel-0.2.12", release_id=101)
        subprocess.run(
            [sys.executable, "-c", mod._REMOTE_APPLY_SCRIPT, str(remote_root), "v0.2.12", wheel12],
            input=tar12,
            capture_output=True,
            check=True,
        )
        self.assertEqual("v0.2.12", json.loads((remote_root / "latest.json").read_text())["tag_name"])

        rollback_try = subprocess.run(
            [sys.executable, "-c", mod._REMOTE_APPLY_SCRIPT, str(remote_root), tag, wheel],
            input=tar_bytes,
            capture_output=True,
        )
        self.assertEqual(3, rollback_try.returncode)
        self.assertEqual("v0.2.12", json.loads((remote_root / "latest.json").read_text())["tag_name"])

        # Concurrent publishes of v0.2.13 and v0.2.14 serialize via .publish.lock and end at v0.2.14.
        wheel13, _, tar13 = self._make_release_tar(mod, "v0.2.13", b"wheel-0.2.13", release_id=102)
        wheel14, _, tar14 = self._make_release_tar(mod, "v0.2.14", b"wheel-0.2.14", release_id=103)
        barrier = threading.Barrier(2)
        outcomes: dict[str, int] = {}

        def _worker(rel_tag: str, rel_wheel: str, payload: bytes) -> None:
            barrier.wait(timeout=5)
            res = subprocess.run(
                [sys.executable, "-c", mod._REMOTE_APPLY_SCRIPT, str(remote_root), rel_tag, rel_wheel],
                input=payload,
                capture_output=True,
            )
            outcomes[rel_tag] = res.returncode

        t1 = threading.Thread(target=_worker, args=("v0.2.13", wheel13, tar13))
        t2 = threading.Thread(target=_worker, args=("v0.2.14", wheel14, tar14))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)
        self.assertEqual(0, outcomes.get("v0.2.14"))
        self.assertIn(outcomes.get("v0.2.13"), {0, 3})
        self.assertEqual("v0.2.14", json.loads((remote_root / "latest.json").read_text())["tag_name"])
        self.assertEqual(b"wheel-0.2.14", (remote_root / "releases" / "v0.2.14" / wheel14).read_bytes())

    def test_publish_release_verify_url_downloads_assets_and_sanitizes_failures(self):
        mod = self._load_publish_module()
        tag = "v0.2.11"
        wheel = "cli_agent_bridge-0.2.11-py3-none-any.whl"
        wheel_bytes = b"verified-mirror-wheel"
        digest = hashlib.sha256(wheel_bytes).hexdigest()
        sums_bytes = f"{digest}  {wheel}\n".encode()

        remote_root = self.home / "served-ota"
        remote_root.mkdir()

        class _DiskHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                prefix = "/cli-agent-bridge/"
                if not self.path.startswith(prefix):
                    self.send_response(404)
                    self.end_headers()
                    return
                rel = self.path[len(prefix) :]
                file_path = remote_root / rel
                if not file_path.is_file() or (serve_missing_wheel and rel.endswith(".whl")):
                    self.send_response(404)
                    self.end_headers()
                    return
                data = file_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format, *args):  # noqa: A002
                return

        serve_missing_wheel = False
        srv = ThreadingHTTPServer(("127.0.0.1", 0), _DiskHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.shutdown)
        self.addCleanup(srv.server_close)
        mirror_url = f"http://127.0.0.1:{srv.server_address[1]}/cli-agent-bridge"

        orig_run = subprocess.run

        def fake_subprocess(cmd, **kwargs):
            if cmd[:2] == ["gh", "api"]:
                return subprocess.CompletedProcess(
                    cmd,
                    0,
                    stdout=json.dumps({"id": 77, "tag_name": tag, "draft": False, "prerelease": False}),
                    stderr="",
                )
            if cmd[:3] == ["gh", "release", "download"]:
                dest = Path(cmd[cmd.index("--dir") + 1])
                (dest / wheel).write_bytes(wheel_bytes)
                (dest / "SHA256SUMS").write_bytes(sums_bytes)
                return subprocess.CompletedProcess(cmd, 0, stdout=b"", stderr=b"")
            if cmd[0] == "ssh":
                return orig_run(
                    [sys.executable, "-c", mod._REMOTE_APPLY_SCRIPT, str(remote_root), tag, wheel],
                    input=kwargs["input"],
                    capture_output=True,
                    check=True,
                )
            raise AssertionError(f"Unexpected command: {cmd}")

        with patch.object(mod.subprocess, "run", side_effect=fake_subprocess):
            result = mod.publish_release(
                host="relay",
                remote_dir=str(remote_root),
                tag=tag,
                verify_url=mirror_url,
            )
            self.assertEqual(digest, result["sha256"])
            self.assertEqual(mirror_url, result["verified_url"])

            serve_missing_wheel = True
            with self.assertRaises(RuntimeError) as verify_err:
                mod.publish_release(
                    host="relay",
                    remote_dir=str(remote_root),
                    tag=tag,
                    verify_url=mirror_url,
                )
            self.assertIn("SSH upload", str(verify_err.exception))

        # Top-level main() must sanitize gh stderr tokens/secrets.
        def failing_gh(cmd, **_):
            raise subprocess.CalledProcessError(4, cmd, output="", stderr="SECRET_GH_TOKEN_SHOULD_NOT_LEAK")

        with (
            patch.object(mod.subprocess, "run", side_effect=failing_gh),
            patch("builtins.print") as printed,
            self.assertRaises(SystemExit) as cli_exit,
        ):
            mod.main(["--tag", tag, "--verify-url", ""])
        self.assertEqual(1, cli_exit.exception.code)
        printed_text = " ".join(str(c) for c in printed.call_args_list)
        self.assertNotIn("SECRET_GH_TOKEN_SHOULD_NOT_LEAK", printed_text)
        payload = json.loads(printed.call_args.args[0])
        self.assertEqual("failed", payload["status"])
        self.assertEqual("RuntimeError", payload["error_type"])


if __name__ == "__main__":
    unittest.main()
