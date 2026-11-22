import asyncio
import base64
import fcntl
import hashlib
import io
import json
import os
import plistlib
import select
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

from agent_bridge import updater
from agent_bridge.errors import BridgeError
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
                "10 1 Ss /Users/alice/managed/bin/python",
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
    def test_source_change_preserves_settings_and_requires_explicit_mirror_removal(self):
        path = self.state / "update.json"
        original = {**self.settings, "interval": 1800, "release_url": "http://127.0.0.1:8088/bridge"}
        updater._write(path, original)
        before = path.read_bytes()
        with patch.object(updater, "latest_release") as fetch:
            with self.assertRaises(BridgeError) as caught:
                updater.change_update_source(self.state, "example/public-bridge")
            self.assertEqual("mirror_configured", caught.exception.code)
            self.assertEqual(before, path.read_bytes())
            with updater._acquire_update_lock(self.root), self.assertRaises(BlockingIOError):
                updater.change_update_source(self.state, "example/public-bridge", clear_mirror=True)
            reply = updater.change_update_source(self.state, "example/public-bridge", clear_mirror=True)
        fetch.assert_not_called()
        self.assertTrue(reply["mirror_removed"])
        self.assertEqual(self.old.resolve(), self.updater.current.resolve())
        self.assertEqual(
            {**self.settings, "repository": "example/public-bridge", "interval": 1800},
            updater.load_settings(self.state),
        )
        # A timer that loaded the old source before the migration re-reads it under the lock.
        with patch.object(updater, "latest_release", return_value={"tag": "v0.1.0"}) as fetch:
            self.assertEqual("up_to_date", self.updater.bootstrap()["status"])
        fetch.assert_called_once_with("example/public-bridge")

    def test_legacy_mirror_settings_load_and_reconfiguration_preserve_source(self):
        for url in ("http://127.0.0.1:8088/bridge", "https://mirror.example.invalid/bridge/"):
            with self.subTest(url=url):
                updater._write(self.state / "update.json", {**self.settings, "release_url": url})
                self.assertEqual(url, updater.load_settings(self.state)["release_url"])
                for update_existing in (False, True):
                    with patch.object(updater.Path, "home", return_value=self.home):
                        *_, settings = updater.validate_update_install(
                            self.state, root=self.root, update_existing=update_existing,
                            interval=7200 if update_existing else 3600,
                        )
                    self.assertEqual(url, settings["release_url"])

    def test_mirror_url_validation_and_errors_do_not_echo_input(self):
        for url in (
            "https://mirror.example.invalid/bridge", "http://127.0.0.1:8088/bridge",
            "http://localhost/bridge", "http://100.64.0.8/bridge",
            "http://node.example.ts.net/bridge", "http://[fd7a:115c:a1e0::1]/bridge",
            "http://[::ffff:127.0.0.1]/bridge",
        ):
            self.assertEqual(url, updater.validate_release_url(url))
        for url in (
            "http://mirror.example.invalid/secret", "http://169.254.169.254/secret",
            "http://192.168.1.1/secret", "http://node.ts.net.evil.invalid/secret",
            "https://user:secret@mirror.example.invalid/bridge",
            "https://mirror.example.invalid:secret/bridge", "https://mirror.example.invalid/bridge?secret=1",
            "https://mirror.example.invalid/bridge#secret", "https://mirror.example.invalid/%2e%2e/secret",
            "https://[broken/secret", "file:///secret", None,
        ):
            with self.subTest(url=url), self.assertRaises(ValueError) as caught:
                updater.validate_release_url(url)
            self.assertNotIn("secret", str(caught.exception))

    def test_release_url_validators_reject_del_and_c1_before_url_normalization(self):
        for validate, base in (
            (updater.validate_release_url, "http://localhost/bridge"),
            (updater._validate_github_url, "https://github.com/example/repo"),
        ):
            for value in range(0x7F, 0xA0):
                char = chr(value)
                for url in (char + base, base + char, base + "/" + char + "/asset"):
                    with self.subTest(validator=validate.__name__, value=value, url=url), self.assertRaises(ValueError):
                        validate(url)

    def test_mirror_check_does_not_fall_back_to_github(self):
        base = "http://127.0.0.1:8088/bridge"
        payload = updater.build_release_manifest("v0.2.0", 123, 90)
        with patch.object(updater, "_download_bytes", return_value=json.dumps(payload).encode()) as dl:
            release = updater.latest_release(updater.REPOSITORY, release_url=base)
        self.assertEqual("v0.2.0", release["tag"])
        dl.assert_called_once_with(f"{base}/latest.json", max_bytes=262_144, timeout=30, mirror_base=base)
        error = urllib.error.HTTPError(f"{base}/latest.json", 404, "Missing", {}, None)
        with patch.object(updater, "_download_bytes", side_effect=error) as dl:
            with self.assertRaises(urllib.error.HTTPError):
                updater.latest_release(updater.REPOSITORY, release_url=base)
        self.assertEqual(1, dl.call_count)
        self.updater.settings["release_url"] = base
        with patch.object(updater, "latest_release", return_value=release) as latest:
            self.updater.check()
        latest.assert_called_once_with(updater.REPOSITORY, release_url=base)

        updater._write(self.state / "update.json", self.settings)
        client = updater.Updater(self.state)
        with patch.object(updater, "latest_release", return_value=release) as latest:
            client.check()
        latest.assert_called_once_with(updater.REPOSITORY)

    def test_mirror_redirect_stays_on_configured_origin(self):
        base = "http://127.0.0.1:8088/bridge"
        with updater._DownloadDeadline(2) as guard:
            handler = updater._GitHubRedirectHandler(guard, mirror_base=base)
            request = urllib.request.Request(f"{base}/latest.json")
            result = handler.redirect_request(request, None, 302, "Moved", {}, f"{base}/releases/latest.json")
            self.assertEqual(f"{base}/releases/latest.json", result.full_url)
            for target in (
                "http://127.0.0.2:8088/bridge/latest.json", "http://127.0.0.1:8089/bridge/latest.json",
                "https://127.0.0.1:8088/bridge/latest.json", "http://mirror.example.invalid/latest.json",
            ):
                with self.subTest(target=target), self.assertRaises(ValueError):
                    handler.redirect_request(request, None, 302, "Moved", {}, target)

    def test_http_mirror_dns_answers_are_checked_before_any_connect(self):
        answers = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("100.64.0.8", 80)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("8.8.8.8", 80)),
        ]
        with (
            updater._DownloadDeadline(2) as guard,
            patch.object(updater, "_resolve_addrs_with_deadline", return_value=answers),
            patch.object(updater.socket, "socket") as create,
            self.assertRaises(ValueError),
        ):
            updater._open_guarded_socket("node.example.ts.net", 80, guard, tailscale_only=True)
        create.assert_not_called()

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
        manifest = updater.build_release_manifest("v0.2.0", 123, 90)
        with patch.object(updater, "_download_bytes", return_value=json.dumps(manifest).encode()) as dl:
            release = updater.latest_release(updater.REPOSITORY)
        self.assertEqual("cli_agent_bridge-0.2.0-py3-none-any.whl", release["wheel"])
        self.assertEqual(
            "https://github.com/qinjian623/agent-bridge/releases/latest/download/latest.json",
            dl.call_args.args[0],
        )

    def test_latest_release_falls_back_to_github_api_on_404_manifest(self):
        import urllib.error

        not_found = urllib.error.HTTPError(
            "https://github.com/qinjian623/agent-bridge/releases/latest/download/latest.json",
            404,
            "Not Found",
            {},
            None,
        )
        with patch.object(
            updater,
            "_download_bytes",
            side_effect=[not_found, json.dumps(self.release()).encode()],
        ) as dl:
            release = updater.latest_release(updater.REPOSITORY)
        self.assertEqual("v0.2.0", release["tag"])
        self.assertEqual(1, release["release_id"])
        self.assertEqual(
            "https://api.github.com/repos/qinjian623/agent-bridge/releases/latest",
            dl.call_args_list[1].args[0],
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
                patch.object(updater, "_download_bytes", return_value=json.dumps(data).encode()),
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

    def test_failed_rollback_symlink_switch_keeps_maintenance_and_candidate(self):
        switch = self.updater._switch

        def fail_restore(target):
            if target == self.old:
                raise OSError("synthetic disk failure")
            switch(target)

        with (
            patch("agent_bridge.broker.request", new=AsyncMock(side_effect=self.prepare)) as request,
            patch.object(updater, "busy_snapshot", return_value={"busy": False}),
            patch.object(self.updater, "_switch", side_effect=fail_restore),
            patch.object(self.updater, "_restart", side_effect=RuntimeError("failed")) as restart,
            self.assertRaises(updater.UpdateUncertain),
        ):
            asyncio.run(self.updater._activate(self.new, self.digest))
        restart.assert_called_once()
        self.assertEqual(["agent_update_prepare"], [c.args[1] for c in request.call_args_list])
        self.assertEqual(self.new, self.updater.current.resolve())
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
        sums = hashlib.sha256(b"test-wheel").hexdigest() + "  " + release["wheel"] + "\n"

        def download(url, **_):
            if url.endswith("/" + release["wheel"]):
                return b"test-wheel"
            if url.endswith("/SHA256SUMS"):
                return sums.encode()
            raise AssertionError(f"Unexpected URL: {url}")

        def command(argv, **_):
            if argv[:2] == ["uv", "venv"]:
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
            patch.object(updater, "_download_bytes", side_effect=download),
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
    def test_scheduled_updater_can_find_uv_from_an_external_install_directory(self):
        uv_path = self.home / "custom tools/bin/uv"
        for kind in ("systemd", "launchd"):
            with self.subTest(kind=kind):
                (self.state / "update.json").unlink(missing_ok=True)
                with (
                    patch.object(updater.Path, "home", return_value=self.home),
                    patch.object(updater.shutil, "which", return_value=str(uv_path)),
                    patch.object(updater, "_run") as run,
                ):
                    result = updater.install(self.state, service_kind=kind, root=self.root)
                run.assert_not_called()
                timer = Path(result["timer"])
                if kind == "systemd":
                    service = timer.with_name("cli-agent-bridge-update.service").read_text()
                    self.assertIn(f'Environment="PATH={uv_path.parent}:', service)
                else:
                    env = plistlib.loads(timer.read_bytes())["EnvironmentVariables"]
                    self.assertTrue(env["PATH"].startswith(f"{uv_path.parent}:"))

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
        with patch.object(updater, "_download_bytes", side_effect=RuntimeError("download failed")):
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

    def test_existing_managed_launchers_survive_install_from_versioned_release_python(self):
        stable_python = self.home / "stable-python"
        stable_python.write_text("#!/bin/sh\n")
        stable_python.chmod(0o700)
        binary = self.root / "bin"
        binary.mkdir(mode=0o700)
        original: dict[Path, bytes] = {}
        for filename, module in updater._LAUNCHER_MODULES:
            launcher = binary / filename
            updater._write(launcher, updater._launcher(self.root, stable_python, module))
            launcher.chmod(0o700)
            original[launcher] = launcher.read_bytes()

        release_python = self.new / "bin/python"
        with (
            patch.object(updater.sys, "executable", str(release_python)),
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run") as run,
        ):
            result = updater.install(
                self.state,
                service_kind="systemd",
                root=self.root,
                update_existing=True,
            )

        self.assertEqual(str(binary / "agent-bridge"), result["launcher"])
        for launcher, data in original.items():
            self.assertEqual(data, launcher.read_bytes())
        run.assert_not_called()

    def test_managed_launcher_validation_rejects_arbitrary_shell_without_mutating_it(self):
        binary = self.root / "bin"
        binary.mkdir(mode=0o700)
        launcher = binary / "agent-bridge"
        content = "#!/bin/sh\nset -eu\nexec /bin/sh -c 'touch /tmp/unexpected'\n"
        launcher.write_text(content)
        launcher.chmod(0o700)

        with self.assertRaisesRegex(ValueError, "unrelated managed launcher"):
            updater._ensure_launchers(self.root)

        self.assertEqual(content, launcher.read_text())

    def test_launchd_service_name_with_plist_suffix_uses_one_normalized_label(self):
        launcher = str(self.root / "bin/agent-bridge")
        self.updater.settings["service_kind"] = "launchd"
        self.updater.settings["service_name"] = "com.example.bridge.plist"
        label = "com.example.bridge"
        with patch.object(
            updater,
            "_run",
            return_value=f"\tstate = running\n\tpid = 321\n\tprogram = {launcher}\n",
        ) as run:
            self.assertTrue(self.updater.service_managed())
            self.updater._restart()

        self.assertEqual(
            [
                ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
                ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{label}"],
            ],
            [invocation.args[0] for invocation in run.call_args_list],
        )

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run") as run,
        ):
            result = updater.install_broker_service(
                self.state,
                root=self.root,
                service_kind="launchd",
                service_name="com.example.bridge.plist",
                enable=False,
            )
        service_path = self.home / "Library/LaunchAgents/com.example.bridge.plist"
        self.assertEqual(str(service_path), result["service_path"])
        self.assertEqual(label, plistlib.loads(service_path.read_bytes())["Label"])
        run.assert_not_called()

    def test_update_install_preserves_recorded_bootstrap_python_after_ota(self):
        recorded = self.home / "stable-python"
        recorded.write_text("#!/bin/sh\n")
        updater._write(
            self.state / "update.json",
            {**self.settings, "bootstrap_python": str(recorded)},
        )
        release_python = self.new / "bin/python"
        with (
            patch.object(updater.sys, "executable", str(release_python)),
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run") as run,
        ):
            _, _, _, settings = updater.validate_update_install(
                self.state,
                service_kind="systemd",
                root=self.root,
            )
            self.assertEqual(str(recorded), settings["bootstrap_python"])
            _, _, _, reconfigured = updater.validate_update_install(
                self.state,
                service_kind="systemd",
                root=self.root,
                update_existing=True,
            )
            self.assertEqual(str(recorded), reconfigured["bootstrap_python"])
            updater.install(self.state, service_kind="systemd", root=self.root)

        saved = json.loads((self.state / "update.json").read_text())
        self.assertEqual(str(recorded), saved["bootstrap_python"])
        self.assertEqual(
            updater._launcher(self.root, recorded, "agent_bridge.cli"),
            (self.root / "bin/agent-bridge").read_text(),
        )
        run.assert_not_called()


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
        sums = hashlib.sha256(b"test-wheel").hexdigest() + "  " + release["wheel"] + "\n"
        seen_urls: list[str] = []

        def download(url, **_):
            seen_urls.append(url)
            if url.endswith("/" + release["wheel"]):
                return b"test-wheel"
            if url.endswith("/SHA256SUMS"):
                return sums.encode()
            raise AssertionError(f"Unexpected URL: {url}")

        def command(argv, **_):
            if argv[:2] == ["uv", "venv"]:
                generated["target"] = Path(argv[-1])
                (generated["target"] / "bin").mkdir()
            elif argv[0] == str(generated["target"] / "bin/python"):
                skill = generated["target"] / "lib/python/site-packages/agent_bridge/skill"
                skill.mkdir(parents=True)
                (skill / "SKILL.md").write_text("packaged skill")
                return str(skill)
            return ""

        with (
            patch.object(updater, "_download_bytes", side_effect=download),
            patch.object(updater, "_run", side_effect=command),
            patch.object(self.updater, "preflight"),
        ):
            target = self.updater.stage(release)
        self.assertEqual(
            [
                f"https://github.com/{updater.REPOSITORY}/releases/download/v0.2.0/{release['wheel']}",
                f"https://github.com/{updater.REPOSITORY}/releases/download/v0.2.0/SHA256SUMS",
            ],
            seen_urls,
        )
        self.assertEqual("packaged skill", (target / "skill/SKILL.md").read_text())
        self.assertTrue((target / "skill").is_symlink())
        self.assertTrue(self.old.exists())
        self.assertEqual(self.old, self.updater.current.resolve())

    def test_stage_mirror_downloads_wheel_and_checksum_only_from_configured_source(self):
        release = {"tag": "v0.2.0", "wheel": "cli_agent_bridge-0.2.0-py3-none-any.whl"}
        sums = (hashlib.sha256(b"test-wheel").hexdigest() + "  " + release["wheel"] + "\n").encode()
        for base in ("http://127.0.0.1:8088/bridge", "https://mirror.example.invalid/bridge"):
            with self.subTest(base=base):
                self.updater.settings["release_url"] = base
                generated = {}

                def download(url, base=base, **options):
                    self.assertEqual(base, options["mirror_base"])
                    self.assertTrue(url.startswith(f"{base}/releases/v0.2.0/"))
                    return sums if url.endswith("/SHA256SUMS") else b"test-wheel"

                def command(argv, generated=generated, **_):
                    if argv[:2] == ["uv", "venv"]:
                        generated["target"] = Path(argv[-1])
                        (generated["target"] / "bin").mkdir()
                    elif argv[0] == str(generated["target"] / "bin/python"):
                        skill = generated["target"] / "lib/python/site-packages/agent_bridge/skill"
                        skill.mkdir(parents=True)
                        return str(skill)
                    return ""

                with (
                    patch.object(updater, "_download_bytes", side_effect=download) as dl,
                    patch.object(updater, "_run", side_effect=command),
                    patch.object(self.updater, "preflight"),
                ):
                    target = self.updater.stage(release)
                self.assertEqual([
                    f"{base}/releases/v0.2.0/{release['wheel']}", f"{base}/releases/v0.2.0/SHA256SUMS",
                ], [c.args[0] for c in dl.call_args_list])
                self.assertTrue((target / "skill").is_symlink())

    def test_mirror_checksum_failure_does_not_install_or_keep_candidate(self):
        self.updater.settings["release_url"] = "http://127.0.0.1:8088/bridge"
        release = {"tag": "v0.2.0", "wheel": "cli_agent_bridge-0.2.0-py3-none-any.whl"}

        def download(url, **_):
            return ("0" * 64 + "  " + release["wheel"]).encode() if url.endswith("/SHA256SUMS") else b"wheel"

        before = set((self.root / "releases").iterdir())
        with (
            patch.object(updater, "_download_bytes", side_effect=download),
            patch.object(updater, "_run") as run,
            self.assertRaises(ValueError),
        ):
            self.updater.stage(release)
        run.assert_not_called()
        self.assertEqual(before, set((self.root / "releases").iterdir()))
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


class GitHubHttpsReleaseTests(UpdateFixture):
    def test_configured_loopback_http_mirror_bypasses_proxy_and_rejects_origin_change(self):
        seen = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                seen.append(self.path)
                if self.path == "/bridge/latest.json":
                    self.send_response(302)
                    self.send_header("Location", "/bridge/manifest.json")
                    body = b""
                elif self.path == "/bridge/escape":
                    self.send_response(302)
                    self.send_header("Location", "http://127.0.0.2/elsewhere")
                    body = b""
                else:
                    self.send_response(200)
                    body = b"fixture manifest"
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_address[1]}/bridge"
        with patch.dict(os.environ, {
            "HTTP_PROXY": "http://proxy.invalid:9", "http_proxy": "http://proxy.invalid:9",
            "HTTPS_PROXY": "http://proxy.invalid:9", "NO_PROXY": "", "no_proxy": "",
        }):
            self.assertEqual(b"fixture manifest", updater._download_bytes(f"{base}/latest.json", max_bytes=1024, timeout=2, mirror_base=base))
            with self.assertRaises(ValueError):
                updater._download_bytes(f"{base}/escape", max_bytes=1024, timeout=2, mirror_base=base)
        self.assertEqual(["/bridge/latest.json", "/bridge/manifest.json", "/bridge/escape"], seen)

    def _tls_contexts(self) -> tuple[ssl.SSLContext, ssl.SSLContext]:
        cert = self.home / "test-cert.pem"
        key = self.home / "test-key.pem"
        openssl_cfg = self.home / "openssl.cnf"
        openssl_cfg.write_text(
            "[req]\ndistinguished_name=dn\nx509_extensions=v3\nprompt=no\n"
            "[dn]\nCN=github.com\n"
            "[v3]\nsubjectAltName=IP:127.0.0.1,DNS:localhost,DNS:github.com,DNS:release-assets.githubusercontent.com\n"
        )
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(cert),
                "-days",
                "1",
                "-config",
                str(openssl_cfg),
            ],
            check=True,
            capture_output=True,
        )
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
        client_ctx = ssl.create_default_context(cafile=str(cert))
        return server_ctx, client_ctx

    def _start_tls_server(self, routes: dict[str, tuple[int, bytes, dict[str, str]]]):
        server_ctx, client_ctx = self._tls_contexts()

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                entry = routes.get(self.path)
                if entry is None:
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
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
        server.socket = server_ctx.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        port = server.server_address[1]
        return f"https://127.0.0.1:{port}", f"https://localhost:{port}", client_ctx

    @staticmethod
    def _build_minimal_wheel(version: str = "0.2.0", skill_text: str = "github wheel skill") -> bytes:
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

    def test_validate_github_url_enforces_https_and_github_hosts(self):
        self.assertEqual(
            "https://github.com/qinjian623/agent-bridge/releases/latest/download/latest.json",
            updater._validate_github_url(
                "https://github.com/qinjian623/agent-bridge/releases/latest/download/latest.json"
            ),
        )
        self.assertEqual(
            "https://release-assets.githubusercontent.com/asset/foo.whl?sp=r&sig=abc",
            updater._validate_github_url(
                "https://release-assets.githubusercontent.com/asset/foo.whl?sp=r&sig=abc",
                allow_query=True,
            ),
        )
        for bad in (
            "",
            "http://github.com/qinjian623/agent-bridge",
            "http://198.51.100.10:8088/cli-agent-bridge",
            "http://127.0.0.1:8088/latest.json",
            "file:///tmp/latest.json",
            "https://evil.example.com/latest.json",
            "https://user:pass@github.com/a/b",
            "https://github.com/a/b?token=1",
            "https://github.com/a/b#frag",
            "https://github.com/a/../evil",
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                updater._validate_github_url(bad)

    @unittest.skipUnless(shutil.which("openssl") and shutil.which("uv"), "openssl and uv are required")
    def test_github_https_release_cdn_redirect_and_real_wheel_stage(self):
        tag = "v0.2.0"
        wheel_name = "cli_agent_bridge-0.2.0-py3-none-any.whl"
        wheel_bytes = self._build_minimal_wheel("0.2.0", "github https skill")
        digest = hashlib.sha256(wheel_bytes).hexdigest()
        sums_bytes = f"{digest}  {wheel_name}\n".encode()
        manifest = updater.build_release_manifest(tag, len(wheel_bytes), len(sums_bytes))
        routes: dict[str, tuple[int, bytes, dict[str, str]]] = {}
        base_web, base_cdn, client_ctx = self._start_tls_server(routes)
        routes.update(
            {
                f"/{updater.REPOSITORY}/releases/latest/download/latest.json": (
                    302,
                    b"",
                    {"Location": f"/{updater.REPOSITORY}/releases/download/{tag}/latest.json"},
                ),
                f"/{updater.REPOSITORY}/releases/download/{tag}/latest.json": (
                    200,
                    json.dumps(manifest).encode(),
                    {},
                ),
                f"/{updater.REPOSITORY}/releases/download/{tag}/{wheel_name}": (
                    302,
                    b"",
                    {"Location": f"{base_cdn}/cdn/{wheel_name}?sp=r&sig=valid"},
                ),
                f"/cdn/{wheel_name}?sp=r&sig=valid": (200, wheel_bytes, {}),
                f"/{updater.REPOSITORY}/releases/download/{tag}/SHA256SUMS": (200, sums_bytes, {}),
            }
        )
        client = updater.Updater(self.state)
        with (
            patch.object(updater, "GITHUB_WEB_ORIGIN", base_web),
            patch.object(updater, "_GITHUB_RELEASE_HOSTS", frozenset({"127.0.0.1", "localhost"})),
            patch.object(updater, "_GITHUB_CDN_HOSTS", frozenset({"localhost"})),
            patch.object(updater, "_DEFAULT_SSL_CONTEXT", client_ctx),
            patch.object(client, "preflight", return_value={"requires_worker_drain": False}),
        ):
            chk = client.check()
            self.assertTrue(chk["update_available"])
            staged = client.stage(chk["available"])
        version = subprocess.check_output(
            [str(staged / "bin/python"), "-c", "import agent_bridge; print(agent_bridge.__version__)"],
            text=True,
        ).strip()
        self.assertEqual("0.2.0", version)
        self.assertEqual("github https skill", (staged / "skill/SKILL.md").read_text())
        self.assertEqual(digest, json.loads((staged / "release.json").read_text())["sha256"])

    @unittest.skipUnless(shutil.which("openssl"), "openssl is required")
    def test_github_https_rejects_unsafe_redirects_oversized_and_timeouts(self):
        routes: dict[str, tuple[int, bytes, dict[str, str]]] = {}
        base_web, base_cdn, client_ctx = self._start_tls_server(routes)
        routes.update(
            {
                "/downgrade": (302, b"", {"Location": "http://github.com/a/b"}),
                "/evil-host": (302, b"", {"Location": "https://evil.example.com/a/b"}),
                "/query-on-github": (302, b"", {"Location": f"{base_web}/path?unexpected=1"}),
                "/oversized": (200, b"x" * 300_000, {}),
                "/broken": (500, b"boom", {}),
            }
        )
        with (
            patch.object(updater, "_GITHUB_RELEASE_HOSTS", frozenset({"127.0.0.1", "localhost"})),
            patch.object(updater, "_GITHUB_CDN_HOSTS", frozenset({"localhost"})),
            patch.object(updater, "_DEFAULT_SSL_CONTEXT", client_ctx),
        ):
            for path in ("/downgrade", "/evil-host", "/query-on-github", "/oversized"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    updater._download_bytes(f"{base_web}{path}", max_bytes=262_144, timeout=2.0)

        # Hanging DNS lookup must obey total deadline.
        dns_release = threading.Event()

        def slow_dns(*_, **__):
            dns_release.wait(timeout=2.0)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]

        try:
            started = time.monotonic()
            with patch.object(socket, "getaddrinfo", side_effect=slow_dns), self.assertRaises(TimeoutError):
                updater._download_bytes("https://github.com/qinjian623/agent-bridge", max_bytes=1024, timeout=0.15)
            self.assertLess(time.monotonic() - started, 0.5)
        finally:
            dns_release.set()

        # Stalled TLS handshake must abort at deadline.
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
        with (
            patch.object(updater, "_GITHUB_RELEASE_HOSTS", frozenset({"127.0.0.1"})),
            self.assertRaises(TimeoutError),
        ):
            updater._download_bytes(
                f"https://127.0.0.1:{tls_srv.getsockname()[1]}/latest.json",
                max_bytes=1024,
                timeout=0.2,
            )
        self.assertLess(time.monotonic() - started, 0.6)

        # Slow TLS headers, slow TLS body, and huge redirect body handling.
        server_ctx, raw_client_ctx = self._tls_contexts()

        class _SlowTlsHandler(BaseHTTPRequestHandler):
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
                            b"HTTP/1.1 302 Found\r\nLocation: /ok\r\nContent-Length: 5000000\r\n\r\n"
                        )
                        for _ in range(10):
                            time.sleep(0.1)
                            self.connection.sendall(b"z" * 1024)
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

        slow_srv = ThreadingHTTPServer(("127.0.0.1", 0), _SlowTlsHandler)
        slow_srv.socket = server_ctx.wrap_socket(slow_srv.socket, server_side=True)
        threading.Thread(target=slow_srv.serve_forever, daemon=True).start()
        self.addCleanup(slow_srv.shutdown)
        self.addCleanup(slow_srv.server_close)
        slow_base = f"https://127.0.0.1:{slow_srv.server_address[1]}"

        with (
            patch.object(updater, "_GITHUB_RELEASE_HOSTS", frozenset({"127.0.0.1"})),
            patch.object(updater, "_DEFAULT_SSL_CONTEXT", raw_client_ctx),
        ):
            for path in ("/slow-body", "/slow-headers"):
                started = time.monotonic()
                with self.subTest(path=path), self.assertRaises(TimeoutError):
                    updater._download_bytes(f"{slow_base}{path}", max_bytes=1024, timeout=0.2)
                self.assertLess(time.monotonic() - started, 0.7)

            started = time.monotonic()
            self.assertEqual(b"final", updater._download_bytes(f"{slow_base}/slow-redirect", max_bytes=1024, timeout=0.5))
            self.assertLess(time.monotonic() - started, 0.5)

    def test_cli_failure_preserves_protected_status_under_lock_races(self):
        for preserved in (
            {"status": "activating", "target_version": "0.2.11"},
            {"status": "uncertain", "target_version": "0.2.11"},
        ):
            updater._write(self.state / "update-status.json", preserved)
            with (
                patch("builtins.print"),
                patch.object(updater, "latest_release", side_effect=RuntimeError("boom")),
                self.assertRaises(SystemExit),
            ):
                updater.main(["--state", str(self.state), "check"])
            saved_status = json.loads((self.state / "update-status.json").read_text())
            self.assertEqual(preserved["status"], saved_status["status"])
            self.assertEqual("0.2.11", saved_status["target_version"])

        (self.state / "update-status.json").unlink()
        hold_lock_fd = os.open(self.root / "update.lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            def concurrent_writer_holds_lock(*_, **__):
                fcntl.flock(hold_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                updater._write(self.state / "update-status.json", {"status": "activating", "target_version": "0.2.12"})
                raise RuntimeError("check failed while lock held")

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

        (self.state / "update-status.json").unlink()

        def concurrent_writer_released_before_record(*_, **__):
            with updater._acquire_update_lock(self.root):
                updater._write(self.state / "update-status.json", {"status": "activating", "target_version": "0.2.13"})
            raise RuntimeError("check failed after activating unlocked")

        with (
            patch("builtins.print"),
            patch.object(updater, "latest_release", side_effect=concurrent_writer_released_before_record),
            self.assertRaises(SystemExit),
        ):
            updater.main(["--state", str(self.state), "check"])
        status_after_release = json.loads((self.state / "update-status.json").read_text())
        self.assertEqual("activating", status_after_release["status"])
        self.assertEqual("0.2.13", status_after_release["target_version"])

        # Corrupt update.json skips un-locked status overwrite.
        (self.state / "update.json").write_text("{corrupt")
        with patch("builtins.print"), self.assertRaises(SystemExit) as corrupt_err:
            updater.main(["--state", str(self.state), "check"])
        self.assertEqual(1, corrupt_err.exception.code)
        status_after_corrupt = json.loads((self.state / "update-status.json").read_text())
        self.assertEqual("activating", status_after_corrupt["status"])
        self.assertEqual("0.2.13", status_after_corrupt["target_version"])

    def test_install_broker_service_generates_managed_systemd_and_launchd_units(self):
        with patch.object(updater.Path, "home", return_value=self.home), patch.object(updater, "_run") as run:
            sys_res = updater.install_broker_service(
                self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service", enable=False
            )
            unit_text = Path(sys_res["service_path"]).read_text()
            self.assertIn(str(self.root / "bin/agent-bridge"), unit_text)
            self.assertIn("serve", unit_text)
            Path(sys_res["service_path"]).write_text("custom unrelated unit")
            with self.assertRaises(ValueError):
                updater.install_broker_service(
                    self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service"
                )
            updater.install_broker_service(
                self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service", force=True
            )

            mac_res = updater.install_broker_service(
                self.state, root=self.root, service_kind="launchd", service_name="com.cli-agent-bridge.broker", enable=False
            )
            plist = plistlib.loads(Path(mac_res["service_path"]).read_bytes())
            self.assertEqual("com.cli-agent-bridge.broker", plist["Label"])
            self.assertEqual([str(self.root / "bin/agent-bridge"), "--state", str(self.state), "serve"], plist["ProgramArguments"])
            self.assertTrue(plist["KeepAlive"])
        run.assert_not_called()

    @unittest.skipUnless(shutil.which("openssl"), "openssl is required")
    def test_github_https_connect_proxy_with_auth_cdn_redirect_and_timeout(self):
        routes: dict[str, tuple[int, bytes, dict[str, str]]] = {}
        base_web, _, client_ctx = self._start_tls_server(routes)
        tls_port = int(base_web.rsplit(":", 1)[1])
        routes.update(
            {
                f"/{updater.REPOSITORY}/releases/download/v0.2.0/latest.json": (
                    302,
                    b"",
                    {"Location": "https://release-assets.githubusercontent.com/assets/123?sp=r&sig=valid"},
                ),
                "/assets/123?sp=r&sig=valid": (200, b'{"proxied":true}', {}),
            }
        )

        connect_targets: list[tuple[str, str]] = []
        proxy_mode = {"stall": False, "auth_required": True}
        expected_auth = "Basic " + base64.b64encode(b"user:secret").decode()

        class _ConnectProxy(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_CONNECT(self):  # noqa: N802
                auth_header = self.headers.get("Proxy-Authorization", "")
                connect_targets.append((self.path, auth_header))
                if proxy_mode["auth_required"] and auth_header != expected_auth:
                    self.send_response(407, "Proxy Authentication Required")
                    self.send_header("Proxy-Authenticate", 'Basic realm="test"')
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if proxy_mode["stall"]:
                    # Hold CONNECT open past deadline without sending status line.
                    time.sleep(0.5)
                    return
                upstream = socket.create_connection(("127.0.0.1", tls_port), timeout=5)
                try:
                    self.send_response(200, "Connection Established")
                    self.end_headers()
                    self.connection.setblocking(False)
                    upstream.setblocking(False)
                    sockets = [self.connection, upstream]
                    while sockets:
                        readable, _, errored = select.select(sockets, [], sockets, 1.0)
                        if errored:
                            break
                        for sock in readable:
                            peer = upstream if sock is self.connection else self.connection
                            try:
                                data = sock.recv(65_536)
                            except OSError:
                                data = b""
                            if not data:
                                return
                            peer.sendall(data)
                finally:
                    upstream.close()

            def log_message(self, format, *args):  # noqa: A002
                return

        proxy = ThreadingHTTPServer(("127.0.0.1", 0), _ConnectProxy)
        proxy.daemon_threads = True
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        self.addCleanup(proxy.shutdown)
        self.addCleanup(proxy.server_close)
        proxy_port = proxy.server_address[1]

        # Test production hosts (`github.com` -> 302 CDN redirect to `release-assets.githubusercontent.com`)
        # via real HTTP CONNECT proxy with Proxy-Authorization without overriding allowed host lists.
        url = f"https://github.com/{updater.REPOSITORY}/releases/download/v0.2.0/latest.json"
        proxy_env = {
            "HTTPS_PROXY": f"http://user:secret@127.0.0.1:{proxy_port}",
            "https_proxy": f"http://user:secret@127.0.0.1:{proxy_port}",
            "NO_PROXY": "",
            "no_proxy": "",
        }
        with (
            patch.dict(os.environ, proxy_env),
            patch.object(updater, "_DEFAULT_SSL_CONTEXT", client_ctx),
        ):
            payload = updater._download_bytes(url, max_bytes=4096, timeout=5.0)
        self.assertEqual(b'{"proxied":true}', payload)
        self.assertEqual(
            [
                ("github.com:443", expected_auth),
                ("release-assets.githubusercontent.com:443", expected_auth),
            ],
            connect_targets,
        )

        # Wrong proxy credentials fail cleanly (407).
        bad_auth_env = {**proxy_env, "HTTPS_PROXY": f"http://user:wrong@127.0.0.1:{proxy_port}", "https_proxy": f"http://user:wrong@127.0.0.1:{proxy_port}"}
        with (
            patch.dict(os.environ, bad_auth_env),
            patch.object(updater, "_DEFAULT_SSL_CONTEXT", client_ctx),
            self.assertRaises((OSError, urllib.error.HTTPError)),
        ):
            updater._download_bytes(url, max_bytes=4096, timeout=5.0)

        # Slow stalling proxy CONNECT enforces deadline TimeoutError.
        proxy_mode["stall"] = True
        with (
            patch.dict(os.environ, proxy_env),
            patch.object(updater, "_DEFAULT_SSL_CONTEXT", client_ctx),
            self.assertRaises(TimeoutError),
        ):
            updater._download_bytes(url, max_bytes=4096, timeout=0.15)

    def test_install_broker_service_migrates_active_systemd_and_checks_busy_and_completion(self):
        unit_dir = self.home / ".config/systemd/user"
        unit_dir.mkdir(parents=True, exist_ok=True)
        unit_path = unit_dir / "cli-agent-bridge.service"
        old_unit = "[Unit]\n[Service]\nExecStart=/old/.venv/bin/agent-bridge serve\n"
        unit_path.write_text(old_unit)
        launcher = str(self.root / "bin/agent-bridge")

        # Put one active task into bridge.sqlite: busy check must abort before mutating unit file.
        store = Store(self.state)
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,desired,created) VALUES('s-busy','codex-test',?,'read-only','open',0)",
                (str(self.home),),
            )
            db.execute(
                "INSERT INTO tasks(id,session_id,instruction,dependencies,reservation,status,created) "
                "VALUES('t-busy','s-busy','run test','[]',0,'running',0)"
            )

        def _show_old(cmd, timeout=10):
            if cmd[:3] == ["systemctl", "--user", "show"]:
                return "ActiveState=active\nMainPID=4242\nExecStart={ path=/old/.venv/bin/agent-bridge ; }\n"
            return ""

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_show_old),
        ):
            with self.assertRaises(ValueError) as busy_err:
                updater.install_broker_service(
                    self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service", enable=True, force=True
                )
            self.assertIn("busy", str(busy_err.exception).lower())
        self.assertEqual(old_unit, unit_path.read_text())

        # Remove busy database so state is idle: migration must acquire maintenance lease,
        # restart active service, verify new PID + launcher, and release maintenance.
        (self.state / "bridge.sqlite").unlink()
        migrated = {"restarted": False}
        commands: list[list[str]] = []
        rpc_calls: list[tuple[str, dict]] = []

        def _run_migration(cmd, timeout=10):
            commands.append(cmd)
            if cmd[:3] == ["systemctl", "--user", "restart"]:
                migrated["restarted"] = True
                return ""
            if cmd[:3] == ["systemctl", "--user", "show"]:
                if migrated["restarted"]:
                    return f"ActiveState=active\nMainPID=9999\nExecStart={{ path={launcher} ; }}\n"
                return "ActiveState=active\nMainPID=4242\nExecStart={ path=/old/.venv/bin/agent-bridge ; }\n"
            return ""

        async def _fake_request(_state, method, args):
            rpc_calls.append((method, dict(args)))
            if method == "agent_update_prepare":
                return {"token": "lease-tok-1", "expires_at": time.time() + 180, "broker_pid": 4242}
            if method == "agent_update_status":
                return {"pid": 9999, "version": "0.2.12"}
            if method == "agent_update_finish":
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_run_migration),
            patch.object(updater, "_expected_managed_version", return_value="0.2.12"),
            patch("agent_bridge.broker.request", side_effect=_fake_request),
        ):
            res = updater.install_broker_service(
                self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service", enable=True, force=True
            )
        self.assertTrue(res["restarted"])
        self.assertEqual(9999, res["pid"])
        self.assertEqual("0.2.12", res["version"])
        self.assertIn(["systemctl", "--user", "restart", "cli-agent-bridge.service"], commands)
        self.assertNotIn(["systemctl", "--user", "enable", "--now", "cli-agent-bridge.service"], commands)
        self.assertEqual(
            [m for m, _ in rpc_calls],
            ["agent_update_prepare", "agent_update_status", "agent_update_finish"],
        )

        # If PID stayed on old 4242 after restart, fail explicitly as incomplete migration (while releasing lease).
        unit_path.write_text(old_unit)
        migrated["restarted"] = False
        rpc_calls.clear()

        async def _stale_pid_request(_state, method, args):
            rpc_calls.append((method, dict(args)))
            if method == "agent_update_prepare":
                return {"token": "lease-tok-2", "expires_at": time.time() + 180, "broker_pid": 4242}
            if method == "agent_update_status":
                return {"pid": 4242, "version": "0.2.12"}
            if method == "agent_update_finish":
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_run_migration),
            patch.object(updater, "_expected_managed_version", return_value="0.2.12"),
            patch.object(updater, "_BROKER_HEALTH_TIMEOUT", 0.05),
            patch("agent_bridge.broker.request", side_effect=_stale_pid_request),
            self.assertRaises(RuntimeError) as inc_err,
        ):
            updater.install_broker_service(
                self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service", enable=True, force=True
            )
        self.assertIn("incomplete", str(inc_err.exception).lower())
        self.assertEqual(rpc_calls[-1], ("agent_update_finish", {"token": "lease-tok-2"}))

    def test_install_broker_service_aborts_toctou_busy_and_missing_lease_without_touching_unit(self):
        unit_dir = self.home / ".config/systemd/user"
        unit_dir.mkdir(parents=True, exist_ok=True)
        unit_path = unit_dir / "cli-agent-bridge.service"
        old_unit = "[Unit]\n[Service]\nExecStart=/old/.venv/bin/agent-bridge serve\n"
        unit_path.write_text(old_unit)
        Store(self.state)

        def _show_old(cmd, timeout=10):
            if cmd[:3] == ["systemctl", "--user", "show"]:
                return "ActiveState=active\nMainPID=4242\nExecStart={ path=/old/.venv/bin/agent-bridge ; }\n"
            raise AssertionError(f"Unexpected command {cmd}")

        # 1. Unreachable broker / missing maintenance lease aborts before rewriting unit.
        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_show_old),
            patch("agent_bridge.broker.request", side_effect=OSError("no broker sock")),
            self.assertRaisesRegex(RuntimeError, "maintenance lease"),
        ):
            updater.install_broker_service(
                self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service", enable=True, force=True
            )
        self.assertEqual(old_unit, unit_path.read_text())

        # 2. Work arrives after validate_broker_service_install() but before lease re-check completes.
        rpc_calls: list[str] = []

        async def _arriving_work_request(_state, method, args):
            rpc_calls.append(method)
            if method == "agent_update_prepare":
                store = Store(self.state)
                with store.connection(write=True) as db:
                    db.execute(
                        "INSERT INTO sessions(id,profile,cwd,access,desired,created) VALUES('s-racy','codex-test',?,'read-only','open',0)",
                        (str(self.home),),
                    )
                    db.execute(
                        "INSERT INTO tasks(id,session_id,instruction,dependencies,reservation,status,created) "
                        "VALUES('t-racy','s-racy','race','[]',0,'running',0)"
                    )
                return {"token": "tok-toctou", "expires_at": time.time() + 180, "broker_pid": 4242}
            if method == "agent_update_finish":
                self.assertEqual(args, {"token": "tok-toctou"})
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_show_old),
            patch("agent_bridge.broker.request", side_effect=_arriving_work_request),
            self.assertRaisesRegex(ValueError, "while busy"),
        ):
            updater.install_broker_service(
                self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service", enable=True, force=True
            )
        self.assertEqual(old_unit, unit_path.read_text())
        self.assertEqual(rpc_calls, ["agent_update_prepare", "agent_update_finish"])

    def test_install_broker_service_launchd_unifies_needs_restart(self):
        with patch.object(updater.Path, "home", return_value=self.home), patch.object(updater, "_run"):
            initial = updater.install_broker_service(
                self.state, root=self.root, service_kind="launchd", service_name="com.cli-agent-bridge.broker", enable=False
            )
        launcher = str(self.root / "bin/agent-bridge")
        plist_path = Path(initial["service_path"])
        commands: list[list[str]] = []

        def _launchctl_unchanged(cmd, timeout=10):
            commands.append(cmd)
            if cmd[:2] == ["launchctl", "print"]:
                return f"\tstate = running\n\tpid = 7777\n\tprogram = {launcher}\n"
            raise AssertionError(f"Unchanged active LaunchAgent must not run {cmd}")

        rpc_calls: list[str] = []

        async def _status_only(_state, method, args):
            rpc_calls.append(method)
            if method == "agent_update_status":
                return {"pid": 7777, "version": "0.2.12"}
            raise AssertionError(f"Unchanged LaunchAgent should not acquire lease ({method})")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_launchctl_unchanged),
            patch.object(updater, "_expected_managed_version", return_value="0.2.12"),
            patch("agent_bridge.broker.request", side_effect=_status_only),
        ):
            res = updater.install_broker_service(
                self.state, root=self.root, service_kind="launchd", service_name="com.cli-agent-bridge.broker", enable=True
            )
        self.assertFalse(res["restarted"])
        self.assertEqual(res["pid"], 7777)
        self.assertEqual(rpc_calls, ["agent_update_status"])
        self.assertTrue(all(cmd[:2] == ["launchctl", "print"] for cmd in commands))

        # Now test unmanaged / modified launchd plist -> restarts cleanly under maintenance lease.
        plist_path.write_text("<plist>old</plist>")
        restarted_state = {"done": False}
        commands.clear()
        rpc_calls.clear()

        def _launchctl_migrate(cmd, timeout=10):
            commands.append(cmd)
            if cmd[:2] == ["launchctl", "print"]:
                if restarted_state["done"]:
                    return f"\tstate = running\n\tpid = 8888\n\tprogram = {launcher}\n"
                return "\tstate = running\n\tpid = 6666\n\tprogram = /old/agent-bridge\n"
            if cmd[:2] == ["launchctl", "bootstrap"]:
                restarted_state["done"] = True
            return ""

        async def _migrate_rpc(_state, method, args):
            rpc_calls.append(method)
            if method == "agent_update_prepare":
                return {"token": "mac-tok", "expires_at": time.time() + 180, "broker_pid": 6666}
            if method == "agent_update_status":
                return {"pid": 8888, "version": "0.2.12"}
            if method == "agent_update_finish":
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_launchctl_migrate),
            patch.object(updater, "_expected_managed_version", return_value="0.2.12"),
            patch("agent_bridge.broker.request", side_effect=_migrate_rpc),
        ):
            res = updater.install_broker_service(
                self.state, root=self.root, service_kind="launchd", service_name="com.cli-agent-bridge.broker", enable=True, force=True
            )
        self.assertTrue(res["restarted"])
        self.assertEqual(res["pid"], 8888)
        self.assertEqual(rpc_calls, ["agent_update_prepare", "agent_update_status", "agent_update_finish"])
        self.assertIn(["launchctl", "bootout", f"gui/{os.getuid()}/com.cli-agent-bridge.broker"], commands)
        self.assertIn(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist_path)], commands)

    def test_verify_migrated_broker_requires_expected_version_and_live_pid(self):
        with (
            patch.object(updater, "_expected_managed_version", return_value=None),
            self.assertRaisesRegex(RuntimeError, "expected managed broker version"),
        ):
            updater._verify_migrated_broker(
                self.state, self.root, "systemd", "cli-agent-bridge.service", "/bin/agent-bridge",
                previous_pid=None, restarted=False,
            )

        with (
            patch.object(updater, "_expected_managed_version", return_value="0.2.12"),
            patch("agent_bridge.broker.request", new=AsyncMock(return_value={"pid": 9999, "version": "0.2.12"})),
            patch.object(updater, "_probe_broker_service_state", return_value=(True, None, True)),
            self.assertRaisesRegex(RuntimeError, "live MainPID"),
        ):
            updater._verify_migrated_broker(
                self.state, self.root, "systemd", "cli-agent-bridge.service", "/bin/agent-bridge",
                previous_pid=None, restarted=False,
            )

    def test_validate_update_install_checks_timer_parent_symlinks_and_permissions(self):
        timer_dir = self.home / ".config/systemd/user"
        timer_dir.parent.mkdir(parents=True)
        (self.home / "elsewhere").mkdir()
        timer_dir.symlink_to(self.home / "elsewhere")
        with patch.object(updater.Path, "home", return_value=self.home):
            with self.assertRaisesRegex(ValueError, "symlinked"):
                updater.validate_update_install(self.state, service_kind="systemd", root=self.root)

        timer_dir.unlink()
        timer_dir.mkdir()
        timer_dir.chmod(0o500)
        try:
            with patch.object(updater.Path, "home", return_value=self.home):
                with self.assertRaisesRegex(ValueError, "not writable"):
                    updater.validate_update_install(self.state, service_kind="systemd", root=self.root)
        finally:
            timer_dir.chmod(0o700)

    def test_install_broker_service_rejects_mismatched_maintenance_lease_pid(self):
        unit_dir = self.home / ".config/systemd/user"
        unit_dir.mkdir(parents=True, exist_ok=True)
        unit_path = unit_dir / "cli-agent-bridge.service"
        old_unit = "[Unit]\n[Service]\nExecStart=/old/.venv/bin/agent-bridge serve\n"
        unit_path.write_text(old_unit)

        def _show_old(cmd, timeout=10):
            if cmd[:3] == ["systemctl", "--user", "show"]:
                return "ActiveState=active\nMainPID=111\nExecStart={ path=/old/.venv/bin/agent-bridge ; }\n"
            raise AssertionError(f"Service manager command {cmd} must not run on lease PID mismatch")

        rpc_calls: list[tuple[str, dict]] = []

        async def _wrong_broker_rpc(_state, method, args):
            rpc_calls.append((method, dict(args)))
            if method == "agent_update_prepare":
                return {"token": "wrong-tok", "expires_at": time.time() + 180, "broker_pid": 222}
            if method == "agent_update_finish":
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_show_old),
            patch("agent_bridge.broker.request", side_effect=_wrong_broker_rpc),
            self.assertRaisesRegex(RuntimeError, "broker PID 222"),
        ):
            updater.install_broker_service(
                self.state, root=self.root, service_kind="systemd", service_name="cli-agent-bridge.service", enable=True, force=True
            )
        self.assertEqual(old_unit, unit_path.read_text())
        self.assertEqual(
            rpc_calls,
            [
                ("agent_update_prepare", {"hot": False}),
                ("agent_update_finish", {"token": "wrong-tok"}),
            ],
        )

    def test_validate_write_target_rejects_higher_ancestor_symlink_without_stop_at(self):
        real_ancestor = self.home / "real_base"
        (real_ancestor / "existing_parent").mkdir(parents=True)
        link_ancestor = self.home / "link_base"
        link_ancestor.symlink_to(real_ancestor)
        linked_state = link_ancestor / "existing_parent" / "state"
        with self.assertRaisesRegex(ValueError, "symlinked directory"):
            updater._validate_write_target(linked_state, label="state directory", expect_dir=True)


class ServiceStateProbeTests(UpdateFixture):
    def test_probe_returns_absent_only_for_confirmed_missing_units_and_jobs(self):
        launcher = str(self.root / "bin/agent-bridge")
        systemd_missing = Mock(returncode=4, stdout="", stderr="Unit example.service could not be found.")
        with patch.object(updater.subprocess, "run", return_value=systemd_missing):
            self.assertEqual(
                (False, None, False),
                updater._probe_broker_service_state("systemd", "example.service", launcher),
            )
        launchd_missing = Mock(returncode=113, stdout="", stderr="Could not find service")
        with patch.object(updater.subprocess, "run", return_value=launchd_missing):
            self.assertEqual(
                (False, None, False),
                updater._probe_broker_service_state("launchd", "com.example.bridge", launcher),
            )

    def test_probe_unknown_failures_do_not_look_absent(self):
        launcher = str(self.root / "bin/agent-bridge")
        for kind in ("systemd", "launchd"):
            for error in (RuntimeError("probe failed"), updater.UpdateUncertain("probe timed out")):
                with (
                    self.subTest(kind=kind, error=type(error).__name__),
                    patch.object(updater, "_run", side_effect=error),
                    self.assertRaises(RuntimeError),
                ):
                    updater._probe_broker_service_state(kind, "example.service", launcher)

    def test_failed_probe_aborts_install_before_writes_or_service_control(self):
        service_path = self.home / ".config/systemd/user/cli-agent-bridge.service"
        for error in (RuntimeError("probe failed"), updater.UpdateUncertain("probe timed out")):
            commands: list[list[str]] = []

            def _run(cmd, timeout=10, error=error, commands=commands):
                commands.append(cmd)
                if cmd[:3] == ["systemctl", "--user", "show"]:
                    raise error
                raise AssertionError(f"Unexpected service command {cmd}")

            with (
                self.subTest(error=type(error).__name__),
                patch.object(updater.Path, "home", return_value=self.home),
                patch.object(updater, "_run", side_effect=_run),
                self.assertRaises(RuntimeError),
            ):
                updater.install_broker_service(
                    self.state,
                    root=self.root,
                    service_kind="systemd",
                    service_name="cli-agent-bridge.service",
                    enable=True,
                    force=True,
                )

            self.assertEqual(1, len(commands))
            self.assertEqual(["systemctl", "--user", "show"], commands[0][:3])
            self.assertFalse(service_path.exists())
            self.assertFalse((self.root / "bin").exists())

    def test_confirmed_missing_systemd_fresh_install_starts_and_verifies(self):
        service_path = self.home / ".config/systemd/user/cli-agent-bridge.service"
        launcher = str(self.root / "bin/agent-bridge")
        started = {"value": False}

        def _subprocess_run(argv, **_kwargs):
            if argv[:3] == ["systemctl", "--user", "show"]:
                if not started["value"]:
                    return Mock(returncode=4, stdout="", stderr="Unit example.service could not be found.")
                return Mock(
                    returncode=0,
                    stdout=(
                        "LoadState=loaded\nActiveState=active\nMainPID=9999\n"
                        f"ExecStart={{ path={launcher} ; }}\n"
                    ),
                    stderr="",
                )
            if argv[:3] == ["systemctl", "--user", "start"]:
                started["value"] = True
            return Mock(returncode=0, stdout="", stderr="")

        async def _request(_state, method, args):
            if method == "agent_update_status":
                return {"pid": 9999, "version": "0.2.12"}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater.subprocess, "run", side_effect=_subprocess_run),
            patch.object(updater, "_expected_managed_version", return_value="0.2.12"),
            patch("agent_bridge.broker.request", side_effect=_request),
        ):
            result = updater.install_broker_service(
                self.state,
                root=self.root,
                service_kind="systemd",
                service_name="cli-agent-bridge.service",
                enable=True,
                force=True,
            )

        self.assertTrue(started["value"])
        self.assertEqual(9999, result["pid"])
        self.assertTrue(service_path.is_file())


class BrokerServiceRollbackTests(UpdateFixture):
    def test_failed_start_restores_systemd_enablement_symlink(self):
        unit = self.home / ".config/systemd/user/cli-agent-bridge.service"
        link = unit.parent / "default.target.wants" / unit.name
        link.parent.mkdir(parents=True)
        for was_enabled in (False, True):
            with self.subTest(was_enabled=was_enabled):
                unit.write_text("[Service]\nExecStart=/old/agent-bridge serve\n")
                link.unlink(missing_ok=True)
                if was_enabled:
                    link.symlink_to("../original.service")

                def run(cmd, timeout=10):
                    if cmd[:3] == ["systemctl", "--user", "show"]:
                        return "LoadState=loaded\nActiveState=inactive\nMainPID=0\n"
                    if cmd[:3] == ["systemctl", "--user", "enable"]:
                        link.unlink(missing_ok=True)
                        link.symlink_to(unit)
                    if cmd[:3] == ["systemctl", "--user", "start"]:
                        raise RuntimeError("start failed")
                    return ""

                with (
                    patch.object(updater.Path, "home", return_value=self.home),
                    patch.object(updater, "_run", side_effect=run),
                    self.assertRaisesRegex(RuntimeError, "start failed"),
                ):
                    updater.install_broker_service(self.state, root=self.root, service_kind="systemd", enable=True, force=True)
                self.assertEqual(was_enabled, link.is_symlink())
                if was_enabled:
                    self.assertEqual("../original.service", os.readlink(link))

    def test_restore_failure_stops_service_commands_and_keeps_maintenance(self):
        unit = self.home / ".config/systemd/user/cli-agent-bridge.service"
        unit.parent.mkdir(parents=True)
        unit.write_text("[Service]\nExecStart=/old/agent-bridge serve\n")
        commands, methods = [], []

        def run(cmd, timeout=10):
            commands.append(cmd)
            if cmd[:3] == ["systemctl", "--user", "show"]:
                return "LoadState=loaded\nActiveState=active\nMainPID=4242\nExecStart={ path=/old/agent-bridge ; }\n"
            if cmd[:3] == ["systemctl", "--user", "restart"]:
                raise RuntimeError("activation failed")
            return ""

        async def request(_state, method, args):
            methods.append(method)
            return {"token": "test-lease", "broker_pid": 4242, "expires_at": time.time() + 180}

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=run),
            patch.object(updater, "_restore_file_snapshot", side_effect=OSError("disk failure")),
            patch("agent_bridge.broker.request", side_effect=request),
            self.assertRaises(updater.UpdateUncertain),
        ):
            updater.install_broker_service(self.state, root=self.root, service_kind="systemd", enable=True, force=True)
        self.assertEqual(["systemctl", "--user", "restart", unit.name], commands[-1])
        self.assertEqual(["agent_update_prepare"], methods)

    def test_updater_enable_probes_before_any_file_changes(self):
        before = {p.relative_to(self.home): p.read_bytes() for p in self.home.rglob("*") if p.is_file()}
        for error in (updater.UpdateUncertain("timeout"), RuntimeError("probe failed")):
            with (
                self.subTest(error=type(error).__name__),
                patch.object(updater.Path, "home", return_value=self.home),
                patch.object(updater, "_run", side_effect=error),
                self.assertRaises(type(error)),
            ):
                updater.install(self.state, root=self.root, service_kind="systemd", enable=True)
            self.assertEqual(before, {p.relative_to(self.home): p.read_bytes() for p in self.home.rglob("*") if p.is_file()})

    def test_launchd_probe_reads_job_fields_above_nested_service_fields(self):
        launcher = str(self.root / "bin/agent-bridge")
        output = (
            "gui/502/local.cli-agent-bridge = {\n"
            "\tstate = running\n"
            f"\tprogram = {launcher}\n"
            "\tpid = 59784\n"
            "\tproperties = {\n"
            "\t\tstate = active\n"
            "\t\tpid = 123\n"
            "\t}\n"
            "}\n"
        )
        with patch.object(updater, "_run", return_value=output):
            self.assertEqual((True, 59784, True), updater._probe_broker_service_state("launchd", "local.cli-agent-bridge", launcher))

    def test_launchd_unknown_or_conflicting_states_are_not_inactive(self):
        for output in ("state = novel-unknown\n", "state = running\nstate = waiting\n", "pid = 42\n"):
            with self.subTest(output=output), patch.object(updater, "_run", return_value=output):
                with self.assertRaises(RuntimeError):
                    updater._probe_broker_service_state("launchd", "com.example.bridge", "/example/bridge")

    def test_active_systemd_migration_failure_restores_unit_mode_and_service_state(self):
        unit_dir = self.home / ".config/systemd/user"
        unit_dir.mkdir(parents=True)
        unit_path = unit_dir / "cli-agent-bridge.service"
        old_unit = "[Unit]\n[Service]\nExecStart=/old/.venv/bin/agent-bridge serve\n"
        unit_path.write_text(old_unit)
        unit_path.chmod(0o640)
        commands: list[list[str]] = []
        restart_count = 0

        def _run(cmd, timeout=10):
            nonlocal restart_count
            commands.append(cmd)
            if cmd[:3] == ["systemctl", "--user", "show"]:
                return "ActiveState=active\nMainPID=4242\nExecStart={ path=/old/.venv/bin/agent-bridge ; }\n"
            if cmd[:3] == ["systemctl", "--user", "restart"]:
                restart_count += 1
                if restart_count == 1:
                    raise RuntimeError("activation failed")
            return ""

        async def _request(_state, method, args):
            if method == "agent_update_prepare":
                return {"token": "tok-rollback", "expires_at": time.time() + 180, "broker_pid": 4242}
            if method == "agent_update_finish":
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_run),
            patch("agent_bridge.broker.request", side_effect=_request),
            self.assertRaisesRegex(RuntimeError, "activation failed"),
        ):
            updater.install_broker_service(
                self.state,
                root=self.root,
                service_kind="systemd",
                service_name="cli-agent-bridge.service",
                enable=True,
                force=True,
            )

        self.assertEqual(old_unit, unit_path.read_text())
        self.assertEqual(0o640, unit_path.stat().st_mode & 0o777)
        self.assertEqual(2, restart_count)
        self.assertEqual(
            [
                ["systemctl", "--user", "restart", "cli-agent-bridge.service"],
                ["systemctl", "--user", "restart", "cli-agent-bridge.service"],
            ],
            [cmd for cmd in commands if cmd[:3] == ["systemctl", "--user", "restart"]],
        )

    def test_inactive_systemd_start_failure_restores_unit_without_restarting_it(self):
        unit_dir = self.home / ".config/systemd/user"
        unit_dir.mkdir(parents=True)
        unit_path = unit_dir / "cli-agent-bridge.service"
        old_unit = "[Unit]\n[Service]\nExecStart=/old/.venv/bin/agent-bridge serve\n"
        unit_path.write_text(old_unit)
        unit_path.chmod(0o644)
        commands: list[list[str]] = []

        def _run(cmd, timeout=10):
            commands.append(cmd)
            if cmd[:3] == ["systemctl", "--user", "show"]:
                return "ActiveState=inactive\nMainPID=0\nExecStart=\n"
            if cmd[:3] == ["systemctl", "--user", "start"]:
                raise RuntimeError("start failed")
            return ""

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_run),
            patch("agent_bridge.broker.request") as request,
            self.assertRaisesRegex(RuntimeError, "start failed"),
        ):
            updater.install_broker_service(
                self.state,
                root=self.root,
                service_kind="systemd",
                service_name="cli-agent-bridge.service",
                enable=True,
                force=True,
            )

        request.assert_not_called()
        self.assertEqual(old_unit, unit_path.read_text())
        self.assertEqual(0o644, unit_path.stat().st_mode & 0o777)
        self.assertFalse(any(cmd[:3] == ["systemctl", "--user", "restart"] for cmd in commands))
        self.assertIn(["systemctl", "--user", "stop", "cli-agent-bridge.service"], commands)

    def test_launchd_migration_failure_restores_previous_plist_and_unrelated_jobs(self):
        launch_dir = self.home / "Library/LaunchAgents"
        launch_dir.mkdir(parents=True)
        plist_path = launch_dir / "com.example.bridge.plist"
        old_plist = b"<plist><dict><key>Label</key><string>com.example.bridge</string></dict></plist>\n"
        plist_path.write_bytes(old_plist)
        plist_path.chmod(0o640)
        unrelated = launch_dir / "com.example.other.plist"
        unrelated_bytes = b"<plist><dict><key>Label</key><string>com.example.other</string></dict></plist>\n"
        unrelated.write_bytes(unrelated_bytes)
        commands: list[list[str]] = []
        bootstrap_count = 0

        def _run(cmd, timeout=10):
            nonlocal bootstrap_count
            commands.append(cmd)
            if cmd[:2] == ["launchctl", "print"]:
                return "\tstate = running\n\tpid = 7777\n\tprogram = /old/agent-bridge\n"
            if cmd[:2] == ["launchctl", "bootstrap"]:
                bootstrap_count += 1
                if bootstrap_count == 1:
                    raise RuntimeError("bootstrap failed")
            return ""

        async def _request(_state, method, args):
            if method == "agent_update_prepare":
                return {"token": "tok-launchd", "expires_at": time.time() + 180, "broker_pid": 7777}
            if method == "agent_update_finish":
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_run),
            patch("agent_bridge.broker.request", side_effect=_request),
            self.assertRaisesRegex(RuntimeError, "bootstrap failed"),
        ):
            updater.install_broker_service(
                self.state,
                root=self.root,
                service_kind="launchd",
                service_name="com.example.bridge.plist",
                enable=True,
                force=True,
            )

        self.assertEqual(old_plist, plist_path.read_bytes())
        self.assertEqual(0o640, plist_path.stat().st_mode & 0o777)
        self.assertEqual(unrelated_bytes, unrelated.read_bytes())
        self.assertEqual(2, bootstrap_count)
        self.assertEqual(
            [["launchctl", "bootout", f"gui/{os.getuid()}/com.example.bridge"]],
            [cmd for cmd in commands if cmd[:2] == ["launchctl", "bootout"]],
        )

    def test_launchd_verification_failure_reloads_previous_job_and_removes_candidate(self):
        launch_dir = self.home / "Library/LaunchAgents"
        launch_dir.mkdir(parents=True)
        plist_path = launch_dir / "com.example.bridge.plist"
        old_plist = b"<plist><dict><key>Label</key><string>com.example.bridge</string></dict></plist>\n"
        plist_path.write_bytes(old_plist)
        plist_path.chmod(0o600)
        commands: list[list[str]] = []

        def _run(cmd, timeout=10):
            commands.append(cmd)
            if cmd[:2] == ["launchctl", "print"]:
                return "\tstate = running\n\tpid = 7777\n\tprogram = /old/agent-bridge\n"
            return ""

        async def _request(_state, method, args):
            if method == "agent_update_prepare":
                return {"token": "tok-verify", "expires_at": time.time() + 180, "broker_pid": 7777}
            if method == "agent_update_finish":
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_run),
            patch.object(updater, "_verify_migrated_broker", side_effect=RuntimeError("verification failed")),
            patch("agent_bridge.broker.request", side_effect=_request),
            self.assertRaisesRegex(RuntimeError, "verification failed"),
        ):
            updater.install_broker_service(
                self.state,
                root=self.root,
                service_kind="launchd",
                service_name="com.example.bridge.plist",
                enable=True,
                force=True,
            )

        self.assertEqual(old_plist, plist_path.read_bytes())
        self.assertEqual(0o600, plist_path.stat().st_mode & 0o777)
        label = f"gui/{os.getuid()}/com.example.bridge"
        self.assertEqual(
            [
                ["launchctl", "bootout", label],
                ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist_path)],
                ["launchctl", "bootout", label],
                ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist_path)],
            ],
            [cmd for cmd in commands if cmd[:2] in (["launchctl", "bootout"], ["launchctl", "bootstrap"])],
        )

    def test_signal_terminated_service_command_is_uncertain(self):
        killed = Mock(returncode=-9, stdout="", stderr="")
        with (
            patch.object(updater.subprocess, "run", return_value=killed),
            self.assertRaises(updater.UpdateUncertain),
        ):
            updater._run(["systemctl", "--user", "restart", "cli-agent-bridge.service"], timeout=10)

    def test_uncertain_rollback_stops_further_commands_and_leaves_maintenance(self):
        self._assert_rollback_stops(updater.UpdateUncertain("rollback reload timed out"))

    def test_failed_rollback_reload_stops_further_commands_and_leaves_maintenance(self):
        self._assert_rollback_stops(RuntimeError("rollback reload failed"))

    def _assert_rollback_stops(self, rollback_error):
        unit_dir = self.home / ".config/systemd/user"
        unit_dir.mkdir(parents=True)
        unit_path = unit_dir / "cli-agent-bridge.service"
        old_unit = "[Unit]\n[Service]\nExecStart=/old/.venv/bin/agent-bridge serve\n"
        unit_path.write_text(old_unit)
        unit_path.chmod(0o600)
        commands: list[list[str]] = []
        activation_failed = {"value": False}
        finish_calls: list[dict] = []

        def _run(cmd, timeout=10):
            commands.append(cmd)
            if cmd[:3] == ["systemctl", "--user", "show"]:
                return "ActiveState=active\nMainPID=4242\nExecStart={ path=/old/.venv/bin/agent-bridge ; }\n"
            if cmd[:3] == ["systemctl", "--user", "daemon-reload"]:
                if activation_failed["value"]:
                    raise rollback_error
                return ""
            if cmd[:3] == ["systemctl", "--user", "enable"]:
                return ""
            if cmd[:3] == ["systemctl", "--user", "restart"]:
                if not activation_failed["value"]:
                    activation_failed["value"] = True
                    raise RuntimeError("activation restart failed")
                return ""
            return ""

        async def _request(_state, method, args):
            if method == "agent_update_prepare":
                return {"token": "tok-uncertain", "expires_at": time.time() + 180, "broker_pid": 4242}
            if method == "agent_update_finish":
                finish_calls.append(dict(args))
                return {"maintenance": False}
            raise AssertionError(f"Unexpected RPC {method}")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_run),
            patch("agent_bridge.broker.request", side_effect=_request),
            self.assertRaises(updater.UpdateUncertain),
        ):
            updater.install_broker_service(
                self.state,
                root=self.root,
                service_kind="systemd",
                service_name="cli-agent-bridge.service",
                enable=True,
                force=True,
            )

        self.assertTrue(activation_failed["value"])
        self.assertEqual(old_unit, unit_path.read_text())
        self.assertEqual(0o600, unit_path.stat().st_mode & 0o777)
        restart_commands = [cmd for cmd in commands if cmd[:3] == ["systemctl", "--user", "restart"]]
        self.assertEqual(1, len(restart_commands))
        self.assertEqual(
            ["systemctl", "--user", "restart", "cli-agent-bridge.service"],
            restart_commands[0],
        )
        self.assertEqual(["systemctl", "--user", "daemon-reload"], commands[-1])
        self.assertEqual([], finish_calls)

    def test_disabled_install_restores_previous_unit_and_removes_new_launchers_on_write_failure(self):
        unit_dir = self.home / ".config/systemd/user"
        unit_dir.mkdir(parents=True)
        unit_path = unit_dir / "cli-agent-bridge.service"
        old_unit = "[Unit]\n[Service]\nExecStart=/old/.venv/bin/agent-bridge serve\n"
        unit_path.write_text(old_unit)
        unit_path.chmod(0o640)

        def _fail_after_write(_item):
            raise RuntimeError("write callback failed")

        with (
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run") as run,
            self.assertRaisesRegex(RuntimeError, "write callback failed"),
        ):
            updater.install_broker_service(
                self.state,
                root=self.root,
                service_kind="systemd",
                service_name="cli-agent-bridge.service",
                enable=False,
                force=True,
                on_write=_fail_after_write,
            )

        run.assert_not_called()
        self.assertEqual(old_unit, unit_path.read_text())
        self.assertEqual(0o640, unit_path.stat().st_mode & 0o777)
        self.assertFalse((self.root / "bin/agent-bridge").exists())
        self.assertFalse((self.root / "bin/agent-bridge-update").exists())


if __name__ == "__main__":
    unittest.main()
