import asyncio
import fcntl
import hashlib
import json
import os
import plistlib
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from agent_bridge import updater
from agent_bridge.store import Store


class UpdateFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
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

    def test_open_queue_and_active_task_in_paused_session_are_busy(self):
        store = self.database()
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,desired,created) VALUES('s','test',?,'read-only','open',0)",
                (str(self.home),),
            )
            db.execute(
                "INSERT INTO tasks(id,session_id,instruction,dependencies,reservation,created) VALUES('t','s','x','[]',0,0)"
            )
        self.assertIn("open_queued_tasks", updater.busy_snapshot(self.state)["reasons"])
        with store.connection(write=True) as db:
            db.execute("UPDATE sessions SET desired='paused'")
            db.execute("UPDATE tasks SET status='running'")
        self.assertIn("active_tasks", updater.busy_snapshot(self.state)["reasons"])

    def test_live_worker_and_native_helper_are_busy_even_without_running_task(self):
        store = self.database()
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,worker_pid,worker_start,created) VALUES('s','test',?,'read-only',123,'boot:1',0)",
                (str(self.home),),
            )
        with (
            patch("agent_bridge.worker.process_identity", return_value="boot:1"),
            patch.object(updater, "_children", return_value=["ssh", "python"]),
        ):
            result = updater.busy_snapshot(self.state, broker_pid=456)
        self.assertIn("live_workers", result["reasons"])
        self.assertIn("broker_helpers", result["reasons"])

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


if __name__ == "__main__":
    unittest.main()
