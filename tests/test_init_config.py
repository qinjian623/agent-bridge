import argparse
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from agent_bridge import cli, init_config, updater
from agent_bridge.config import Config
from agent_bridge.errors import BridgeError


class InitConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name).resolve()
        self.state = self.home / "state"
        self.root = self.home / "managed"
        self.project = self.home / "project"
        self.state.mkdir(mode=0o700)
        self.project.mkdir()

    def _run_cli(self, argv: list[str]) -> tuple[int, dict]:
        out = io.StringIO()
        code = 0
        with patch.object(Path, "home", return_value=self.home), redirect_stdout(out):
            try:
                cli.main(["--state", str(self.state), *argv])
            except SystemExit as err:
                code = int(err.code or 0)
        raw = out.getvalue().strip()
        return code, json.loads(raw) if raw else {}

    def test_non_interactive_requires_manual_spec_on_fresh_state(self):
        with self.assertRaises(SystemExit) as err:
            cli.main(["--state", str(self.state), "init", "--non-interactive"])
        self.assertEqual(1, err.exception.code)
        self.assertFalse((self.state / "config.json").exists())

    def test_non_interactive_init_creates_reloads_and_preserves_metadata_idempotently(self):
        code, created = self._run_cli(
            [
                "init",
                "--non-interactive",
                "--profile",
                "name=work-codex,runtime=codex,provider=openai-codex,model=gpt-5,effort=low",
                "--project",
                f"repo={self.project}",
                "--worker-launcher",
                "detached",
                "--parallel-limit",
                "4",
                "--quota-limit",
                "2",
            ]
        )
        self.assertEqual(0, code)
        self.assertTrue(created["config_changed"])
        cfg_path = self.state / "config.json"
        self.assertEqual(0o600, cfg_path.stat().st_mode & 0o777)
        loaded = Config.load(cfg_path)
        self.assertEqual(["work-codex"], list(loaded.profiles))
        self.assertFalse(loaded.profiles["work-codex"].verified)
        self.assertEqual("detached", loaded.worker_launcher)
        self.assertEqual(str(self.project), loaded.projects["repo"])

        # Re-run without changes: completely idempotent (`config_changed == False`).
        code2, replay = self._run_cli(["init", "--non-interactive"])
        self.assertEqual(0, code2)
        self.assertFalse(replay["config_changed"])

        # Simulate verified runtime profile and pinned host identity in existing config.
        raw = json.loads(cfg_path.read_text())
        raw["profiles"]["work-codex"]["verified"] = True
        raw["profiles"]["work-codex"]["verified_at"] = "2026-11-16"
        raw["hosts"] = {
            "worker1": {
                "ssh_target": "worker1-ssh",
                "remote_executable": "/home/user/.local/share/cli-agent-bridge/bin/agent-bridge",
                "pinned": {"node_id": "node-1", "state_generation": "gen-1"},
            }
        }
        cfg_path.write_text(json.dumps(raw, indent=2) + "\n")

        # Updating concurrency or adding another host preserves existing verified profile & pinned host.
        code3, updated = self._run_cli(
            [
                "init",
                "--non-interactive",
                "--parallel-limit",
                "6",
                "--host",
                "id=worker1,ssh_target=worker1-ssh,remote_executable=/opt/bridge/agent-bridge",
                "--host",
                "id=worker2,ssh_target=worker2-ssh,remote_executable=/usr/local/bin/agent-bridge",
            ]
        )
        self.assertEqual(0, code3)
        self.assertTrue(updated["config_changed"])
        after = json.loads(cfg_path.read_text())
        self.assertEqual(6, after["parallel_limit"])
        self.assertTrue(after["profiles"]["work-codex"]["verified"])
        self.assertEqual("2026-11-16", after["profiles"]["work-codex"]["verified_at"])
        self.assertEqual({"node_id": "node-1", "state_generation": "gen-1"}, after["hosts"]["worker1"]["pinned"])

        # Changing a profile's model resets verified, and changing a host's ssh_target clears pinned.
        self._run_cli(
            [
                "init",
                "--non-interactive",
                "--profile",
                "name=work-codex,runtime=codex,provider=openai-codex,model=gpt-5.1",
                "--host",
                "id=worker1,ssh_target=worker1-renamed,remote_executable=/opt/bridge/agent-bridge",
            ]
        )
        reset = json.loads(cfg_path.read_text())
        self.assertFalse(reset["profiles"]["work-codex"]["verified"])
        self.assertNotIn("verified_at", reset["profiles"]["work-codex"])
        self.assertNotIn("pinned", reset["hosts"]["worker1"])

    def test_wizard_does_not_reuse_executable_from_previous_runtime(self):
        for detected in (None, "/usr/bin/claude"):
            with self.subTest(detected=detected):
                config = {"profiles": {"local": {"runtime": "codex", "model": "old", "executable": "/usr/bin/codex"}}}
                answers = iter(["a", "local", "claude", "", "new-model", "", "", "", "", "b"])
                init_config._edit_profiles_menu(
                    config, {"runtimes": {"claude": detected}}, lambda _, answers=answers: next(answers), lambda _: None,
                )
                self.assertEqual(detected, config["profiles"]["local"].get("executable"))

    def test_interactive_init_reloads_edits_aborts_and_configures_services(self):
        fake_env = {
            "platform": "linux",
            "runtimes": {"codex": "/usr/bin/codex", "claude": None, "pi": None, "antigravity": None},
            "tools": {"uv": "/usr/bin/uv", "ssh": "/usr/bin/ssh"},
            "systemd_user": True,
            "launchd_user": False,
        }
        # First interactive run: add profile, project, host (via probed ssh), and install broker+updater units.
        answers_round1 = iter(
            [
                "1",  # Profiles
                "a",
                "my-codex",
                "codex",
                "openai-codex",
                "gpt-5",
                "default",
                "",  # effort
                "",  # executable
                "",  # env_file
                "b",
                "3",  # Projects
                "a",
                "proj",
                str(self.project),
                "b",
                "4",  # Hosts
                "a",
                "node1",
                "node1-alias",
                "y",  # probe SSH
                "",  # accept probed path
                "",  # default remote_state
                "n",  # skip default ssh options
                "b",
                "5",  # Services & OTA
                "systemd",
                "cli-agent-bridge.service",
                str(self.root),
                updater.REPOSITORY,
                "3600",
                "y",  # install broker service
                "n",  # do not run systemctl in test
                "y",  # write update.json/timer
                "n",  # do not enable timer in test
                "s",  # save & exit
            ]
        )
        args = cli.build_parser().parse_args(["--state", str(self.state), "init"])
        lines: list[str] = []
        with (
            patch.object(init_config, "detect_environment", return_value=fake_env),
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(
                init_config,
                "_probe_remote_executable",
                return_value="/home/worker/.local/share/cli-agent-bridge/bin/agent-bridge",
            ),
        ):
            res1 = init_config.run_init(
                self.state,
                args,
                input_fn=lambda prompt: next(answers_round1),
                output_fn=lines.append,
                is_tty=True,
            )
        self.assertEqual("configured", res1["status"])
        self.assertTrue((self.state / "config.json").is_file())
        profile = json.loads((self.state / "config.json").read_text())["profiles"]["my-codex"]
        self.assertEqual("/usr/bin/codex", profile["executable"])
        self.assertTrue((self.state / "update.json").is_file())
        self.assertNotIn("release_url", json.loads((self.state / "update.json").read_text()))
        self.assertIn("broker_service", res1)
        self.assertIn("updater", res1)

        # Quit without saving on bad modification leaves saved config intact.
        before_bytes = (self.state / "config.json").read_bytes()
        answers_round2 = iter(
            [
                "3",
                "a",
                "missing-dir",
                str(self.home / "does-not-exist"),
                "b",
                "s",  # fails validation and stays in menu
                "q",  # quit cleanly without touching disk
            ]
        )
        lines2: list[str] = []
        with (
            patch.object(init_config, "detect_environment", return_value=fake_env),
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
        ):
            res2 = init_config.run_init(
                self.state,
                args,
                input_fn=lambda prompt: next(answers_round2),
                output_fn=lines2.append,
                is_tty=True,
            )
        self.assertEqual("aborted", res2["status"])
        self.assertTrue(any("validation error" in line.lower() for line in lines2))
        self.assertEqual(before_bytes, (self.state / "config.json").read_bytes())

        # Re-open existing config, edit quota_limit, and save while keeping existing profile/host/project intact.
        answers_round3 = iter(
            [
                "2",
                "",  # keep worker_launcher
                "",  # keep parallel_limit
                "5",  # update quota_limit
                "",  # keep strategy
                "",  # keep task_timeout
                "",  # keep interrupt_timeout
                "",  # keep token_budget
                "s",
            ]
        )
        with (
            patch.object(init_config, "detect_environment", return_value=fake_env),
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
        ):
            res3 = init_config.run_init(
                self.state,
                args,
                input_fn=lambda prompt: next(answers_round3),
                output_fn=lambda _msg: None,
                is_tty=True,
            )
        self.assertEqual("configured", res3["status"])
        final_cfg = json.loads((self.state / "config.json").read_text())
        self.assertEqual(5, final_cfg["quota_limit"])
        self.assertEqual("gpt-5", final_cfg["profiles"]["my-codex"]["model"])
        self.assertEqual("node1-alias", final_cfg["hosts"]["node1"]["ssh_target"])
        self.assertEqual(str(self.project), final_cfg["projects"]["proj"])

    def test_corrupt_existing_config_is_not_overwritten(self):
        (self.state / "config.json").write_text("{corrupt-json")
        with self.assertRaises(SystemExit) as err:
            cli.main(["--state", str(self.state), "init", "--non-interactive"])
        self.assertEqual(1, err.exception.code)
        self.assertEqual("{corrupt-json", (self.state / "config.json").read_text())

    def test_non_interactive_unknown_profile_key_is_clear_and_preserves_config(self):
        original = {
            "profiles": {"p1": {"runtime": "codex", "provider": "openai-codex", "model": "gpt-5"}},
            "worker_launcher": "detached",
            "parallel_limit": 3,
            "quota_limit": 2,
            "task_timeout": 3600,
            "interrupt_timeout": 30,
            "default_strategy": "balanced",
            "hosts": {},
            "projects": {},
        }
        config_path = self.state / "config.json"
        config_path.write_text(json.dumps(original, indent=2) + "\n")
        before = config_path.read_bytes()
        secret = "sk-test-secret-value-1234567890"
        profile = json.dumps(
            {
                "name": "p2",
                "runtime": "codex",
                "model": "gpt-5",
                "unknown_option": secret,
            }
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        fake_env = {
            "platform": "linux",
            "runtimes": {},
            "tools": {},
            "systemd_user": False,
            "launchd_user": False,
        }
        with (
            patch.object(Path, "home", return_value=self.home),
            patch.object(init_config, "detect_environment", return_value=fake_env),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            self.assertRaises(SystemExit) as caught,
        ):
            cli.main(
                [
                    "--state",
                    str(self.state),
                    "init",
                    "--non-interactive",
                    "--profile",
                    profile,
                ]
            )

        self.assertEqual(1, caught.exception.code)
        payload = json.loads(stderr.getvalue())
        self.assertEqual("invalid_config", payload["error"]["code"])
        self.assertIn("unknown fields", payload["error"]["message"])
        self.assertNotIn(secret, stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(before, config_path.read_bytes())

    def test_doctor_reports_environment_and_warnings(self):
        self._run_cli(
            [
                "init",
                "--non-interactive",
                "--profile",
                f"name=local,runtime=codex,model=gpt-5,executable={sys.executable}",
                "--worker-launcher",
                "systemd",
                "--host",
                "id=h1,ssh_target=h1,remote_executable=/usr/bin/agent-bridge",
            ]
        )
        fake_env = {
            "platform": "linux",
            "runtimes": {"codex": sys.executable, "claude": None, "pi": None, "antigravity": None},
            "tools": {"uv": None, "ssh": None},
            "systemd_user": False,
            "launchd_user": False,
        }
        with patch.object(cli, "detect_environment", return_value=fake_env):
            code, report = self._run_cli(["doctor"])
        self.assertEqual(0, code)
        self.assertEqual("systemd", report["launcher"])
        self.assertFalse(report["systemd_user"])
        self.assertFalse(report["update_configured"])
        self.assertGreaterEqual(len(report["warnings"]), 3)

    def test_host_pin_clearance_in_host_pins_json_and_profile_price_invalidation(self):
        from agent_bridge.hosts import HOST_PINS_FILENAME, ConfigHostRegistry, NodeRef

        self._run_cli(
            [
                "init",
                "--non-interactive",
                "--profile",
                json.dumps(
                    {
                        "name": "p1",
                        "runtime": "codex",
                        "provider": "openai-codex",
                        "model": "gpt-5",
                        "effort": "high",
                        "executable": sys.executable,
                        "price": {"currency": "USD", "source": "test", "as_of": "2026-11-16", "per_million": {"input_tokens": 1.0, "output_tokens": 2.0}},
                    }
                ),
                "--host",
                "id=w1,ssh_target=w1-a,remote_executable=/usr/bin/agent-bridge",
                "--host",
                "id=w2,ssh_target=w2-a,remote_executable=/usr/bin/agent-bridge",
            ]
        )
        registry = ConfigHostRegistry(state_dir=self.state)
        registry.pin("w1", NodeRef("w1", "node-w1", "gen-w1"))
        registry.pin("w2", NodeRef("w2", "node-w2", "gen-w2"))
        pins_before = json.loads((self.state / HOST_PINS_FILENAME).read_text())["hosts"]
        self.assertEqual({"w1", "w2"}, set(pins_before))

        # 1. Keeping profile identity preserves price; changing w1 ssh_target & removing+re-adding w2 clears both pins from host_pins.json.
        self._run_cli(
            [
                "init",
                "--non-interactive",
                "--profile",
                "name=p1,runtime=codex,provider=openai-codex,model=gpt-5",
                "--host",
                "id=w1,ssh_target=w1-b,remote_executable=/usr/bin/agent-bridge",
                "--remove-host",
                "w2",
                "--host",
                "id=w2,ssh_target=w2-a,remote_executable=/usr/bin/agent-bridge",
            ]
        )
        cfg = json.loads((self.state / "config.json").read_text())
        self.assertIn("price", cfg["profiles"]["p1"])
        pins_after = json.loads((self.state / HOST_PINS_FILENAME).read_text())["hosts"]
        self.assertEqual({}, pins_after)
        reloaded = ConfigHostRegistry(state_dir=self.state)
        self.assertIsNone(reloaded.get("w1").pinned)
        self.assertIsNone(reloaded.get("w2").pinned)

        # 2. Changing model without providing new price clears old price; supplying new price keeps new price.
        self._run_cli(
            [
                "init",
                "--non-interactive",
                "--profile",
                "name=p1,runtime=codex,provider=openai-codex,model=gpt-5.1",
            ]
        )
        cfg_model_changed = json.loads((self.state / "config.json").read_text())
        self.assertNotIn("price", cfg_model_changed["profiles"]["p1"])
        self._run_cli(
            [
                "init",
                "--non-interactive",
                "--profile",
                json.dumps(
                    {
                        "name": "p1",
                        "runtime": "codex",
                        "provider": "openai-codex",
                        "model": "gpt-5.2",
                        "price": {"currency": "USD", "source": "new", "as_of": "2026-11-16", "per_million": {"input_tokens": 3.0, "output_tokens": 6.0}},
                    }
                ),
            ]
        )
        cfg_with_new_price = json.loads((self.state / "config.json").read_text())
        self.assertEqual("new", cfg_with_new_price["profiles"]["p1"]["price"]["source"])

    def test_interactive_dash_clears_optional_fields_and_eof_aborts_everywhere(self):
        fake_env = {
            "platform": "linux",
            "runtimes": {"codex": sys.executable, "claude": None, "pi": None, "antigravity": None},
            "tools": {"uv": "/usr/bin/uv", "ssh": "/usr/bin/ssh"},
            "systemd_user": True,
            "launchd_user": False,
        }
        env_file = self.home / "env.json"
        env_file.write_text('{"FOO":"bar"}\n')
        env_file.chmod(0o600)
        self._run_cli(
            [
                "init",
                "--non-interactive",
                "--profile",
                f"name=p1,runtime=codex,provider=openai-codex,model=gpt-5,effort=high,executable={sys.executable},env_file={env_file}",
                "--host",
                "id=h1,ssh_target=h1-alias,remote_executable=/usr/bin/agent-bridge,remote_state=/tmp/remote-state",
            ]
        )
        answers_clear = iter(
            [
                "1",  # Profiles
                "a",
                "p1",
                "",  # runtime
                "",  # provider
                "",  # model
                "",  # quota_group
                "-",  # clear effort
                "-",  # clear executable
                "-",  # clear env_file
                "b",
                "4",  # Hosts
                "a",
                "h1",
                "",  # ssh_target
                "n",  # probe
                "",  # remote_executable
                "-",  # clear remote_state
                "b",
                "s",
            ]
        )
        args = cli.build_parser().parse_args(["--state", str(self.state), "init"])
        with (
            patch.object(init_config, "detect_environment", return_value=fake_env),
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
        ):
            res = init_config.run_init(
                self.state,
                args,
                input_fn=lambda _p: next(answers_clear),
                output_fn=lambda _m: None,
                is_tty=True,
            )
        self.assertEqual("configured", res["status"])
        cfg = json.loads((self.state / "config.json").read_text())
        self.assertNotIn("effort", cfg["profiles"]["p1"])
        self.assertNotIn("executable", cfg["profiles"]["p1"])
        self.assertNotIn("env_file", cfg["profiles"]["p1"])
        self.assertNotIn("remote_state", cfg["hosts"]["h1"])

        # EOF inside submenu must return aborted cleanly without saving partial state or traceback.
        before = (self.state / "config.json").read_bytes()

        def _eof_in_submenu(prompt: str) -> str:
            if "Select [1-5]" in prompt:
                return "1"
            if "Action" in prompt:
                return "a"
            raise EOFError

        with (
            patch.object(init_config, "detect_environment", return_value=fake_env),
            patch.object(Path, "home", return_value=self.home),
        ):
            aborted = init_config.run_init(
                self.state,
                args,
                input_fn=_eof_in_submenu,
                output_fn=lambda _m: None,
                is_tty=True,
            )
        self.assertEqual("aborted", aborted["status"])
        self.assertEqual(before, (self.state / "config.json").read_bytes())

    def test_preflight_blocks_config_mutation_and_preserves_update_interval_and_symlinks(self):
        # Dangling config.json symlink must be rejected instead of overwritten.
        (self.state / "config.json").symlink_to(self.home / "missing-target.json")
        with self.assertRaises(SystemExit) as sym_err:
            cli.main(
                [
                    "--state",
                    str(self.state),
                    "init",
                    "--non-interactive",
                    "--profile",
                    "name=p1,runtime=codex,model=gpt-5",
                ]
            )
        self.assertEqual(1, sym_err.exception.code)
        self.assertTrue((self.state / "config.json").is_symlink())
        (self.state / "config.json").unlink()

        # Invalid --ota-repo must fail BEFORE creating or mutating config.json.
        with (
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
            self.assertRaises(SystemExit) as bad_ota_err,
        ):
            cli.main(
                [
                    "--state",
                    str(self.state),
                    "init",
                    "--non-interactive",
                    "--profile",
                    "name=p1,runtime=codex,model=gpt-5",
                    "--install-updater",
                    "--ota-repo",
                    "https://evil.com/not-owner-repo",
                ]
            )
        self.assertEqual(1, bad_ota_err.exception.code)
        self.assertFalse((self.state / "config.json").exists())

        # Valid updater install saves interval (900) and subsequent update without --ota-interval preserves 900.
        with (
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
        ):
            code1, _ = self._run_cli(
                [
                    "init",
                    "--non-interactive",
                    "--profile",
                    "name=p1,runtime=codex,model=gpt-5",
                    "--install-updater",
                    "--ota-root",
                    str(self.root),
                    "--ota-interval",
                    "900",
                    "--service-kind",
                    "systemd",
                ]
            )
            self.assertEqual(0, code1)
            self.assertEqual(900, json.loads((self.state / "update.json").read_text())["interval"])
            code2, _ = self._run_cli(
                [
                    "init",
                    "--non-interactive",
                    "--ota-repo",
                    "example/bridge",
                ]
            )
            self.assertEqual(0, code2)
        updated_settings = json.loads((self.state / "update.json").read_text())
        self.assertEqual("example/bridge", updated_settings["repository"])
        self.assertEqual(900, updated_settings["interval"])

    def test_ssh_probe_rejects_option_injection_and_forwards_validated_ssh_options(self):
        with patch.object(init_config.subprocess, "run") as sub_run:
            self.assertIsNone(init_config._probe_remote_executable("-oProxyCommand=id"))
        sub_run.assert_not_called()

        mock_proc = unittest.mock.Mock(returncode=0, stdout="/opt/bridge/bin/agent-bridge\n")
        with patch.object(init_config.subprocess, "run", return_value=mock_proc) as sub_run:
            found = init_config._probe_remote_executable("worker1", ["-i", "/path/key", "-p", "2222"])
        self.assertEqual("/opt/bridge/bin/agent-bridge", found)
        argv = sub_run.call_args.args[0]
        self.assertIn("--", argv)
        dash_idx = argv.index("--")
        self.assertEqual("worker1", argv[dash_idx + 1])
        self.assertIn("-i", argv[:dash_idx])
        self.assertIn("2222", argv[:dash_idx])

    def test_host_pins_preflight_rollback_and_timer_validation(self):
        # Seed config + host_pins.json with initial host target h1 -> old-host.
        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {"p1": {"runtime": "codex", "model": "gpt-5"}},
                    "hosts": {"h1": {"ssh_target": "old-host", "remote_executable": "/bin/ab"}},
                }
            )
        )
        orig_cfg_bytes = (self.state / "config.json").read_bytes()
        pins_path = self.state / "host_pins.json"
        pins_path.write_text(json.dumps({"hosts": {"h1": {"node_id": "n1", "state_generation": "g1"}}}))
        orig_pins_bytes = pins_path.read_bytes()

        # 1. Corrupt or symlinked host_pins.json fails preflight BEFORE touching config.json.
        pins_path.write_text("{corrupt")
        code, _ = self._run_cli(["init", "--non-interactive", "--remove-host", "h1"])
        self.assertEqual(1, code)
        self.assertEqual(orig_cfg_bytes, (self.state / "config.json").read_bytes())

        pins_path.unlink()
        pins_path.symlink_to(self.home / "outside-pins.json")
        code, _ = self._run_cli(["init", "--non-interactive", "--remove-host", "h1"])
        self.assertEqual(1, code)
        self.assertEqual(orig_cfg_bytes, (self.state / "config.json").read_bytes())
        pins_path.unlink()

        # 2. Failure during host_pins.json update rolls back config.json atomically.
        pins_path.write_bytes(orig_pins_bytes)
        with (
            patch.object(
                init_config,
                "_clear_persisted_host_pins",
                side_effect=OSError("disk full updating host_pins"),
            ),
            self.assertRaises(OSError),
        ):
            init_config.run_init(
                self.state,
                argparse.Namespace(
                    non_interactive=True,
                    host=["id=h1,ssh_target=new-host,remote_executable=/bin/ab"],
                ),
            )
        self.assertEqual(orig_cfg_bytes, (self.state / "config.json").read_bytes())
        self.assertEqual(orig_pins_bytes, pins_path.read_bytes())

        # 3. Unwritable updater timer dir blocks config.json mutation in preflight.
        timer_dir = self.home / ".config/systemd/user"
        timer_dir.mkdir(parents=True)
        timer_dir.chmod(0o500)
        try:
            with (
                patch.object(Path, "home", return_value=self.home),
                patch.object(updater.Path, "home", return_value=self.home),
            ):
                code, _ = self._run_cli(
                    [
                        "init",
                        "--non-interactive",
                        "--profile",
                        "name=p2,runtime=codex,model=gpt-5",
                        "--install-updater",
                        "--ota-root",
                        str(self.root),
                        "--service-kind",
                        "systemd",
                    ]
                )
            self.assertEqual(1, code)
            self.assertEqual(orig_cfg_bytes, (self.state / "config.json").read_bytes())
        finally:
            timer_dir.chmod(0o700)

    def test_invalid_existing_mirror_is_not_silently_changed(self):
        self.root.mkdir(parents=True, mode=0o700)
        path = self.state / "update.json"
        original = json.dumps({
            "repository": updater.REPOSITORY, "root": str(self.root), "service_kind": "systemd",
            "service_name": "cli-agent-bridge.service", "bootstrap_python": sys.executable,
            "release_url": "http://public.example.invalid/private-marker",
        })
        path.write_text(original)
        path.chmod(0o600)
        with self.assertRaises(BridgeError) as caught:
            init_config._load_existing_update(self.state)
        self.assertEqual("invalid_config", caught.exception.code)
        self.assertNotIn("private-marker", str(caught.exception))
        self.assertEqual(original, path.read_text())

    def test_interactive_commit_interrupt_and_existing_mirror_source_preserved(self):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        (self.state / "config.json").write_text(
            json.dumps({"profiles": {"p1": {"runtime": "codex", "model": "gpt-5"}}})
        )
        (self.state / "update.json").write_text(
            json.dumps(
                {
                    "repository": updater.REPOSITORY,
                    "root": str(self.root),
                    "service_kind": "systemd",
                    "service_name": "cli-agent-bridge.service",
                    "bootstrap_python": sys.executable,
                    "release_url": "http://127.0.0.1:8088/cli-agent-bridge",
                }
            )
        )
        (self.state / "update.json").chmod(0o600)
        before = (self.state / "update.json").read_bytes()
        with (
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
        ):
            code, out = self._run_cli(["init", "--non-interactive"])
        self.assertEqual(0, code, out)
        self.assertEqual(before, (self.state / "update.json").read_bytes())
        with (
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
        ):
            code, out = self._run_cli(["init", "--non-interactive", "--ota-interval", "7200"])
        self.assertEqual(0, code, out)
        changed = json.loads((self.state / "update.json").read_text())
        self.assertEqual("http://127.0.0.1:8088/cli-agent-bridge", changed["release_url"])
        self.assertEqual(7200, changed["interval"])

        # KeyboardInterrupt during _commit_changes after config.json was saved must NOT claim "Aborted without saving".
        (self.state / "config.json").unlink()
        answers = iter(["1", "a", "p1", "codex", "openai-codex", "gpt-5", "default", "", "", "", "b", "s"])
        outputs: list[str] = []
        with (
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(init_config, "install_broker_service", side_effect=KeyboardInterrupt()),
            self.assertRaises(BridgeError) as int_err,
        ):
            init_config.run_init(
                self.state,
                argparse.Namespace(install_broker_service=True),
                input_fn=lambda _: next(answers),
                output_fn=outputs.append,
                is_tty=True,
            )
        self.assertEqual("init_interrupted", int_err.exception.code)
        self.assertIn("config.json", str(int_err.exception))
        self.assertFalse(any("Aborted without saving" in line for line in outputs))
        self.assertTrue((self.state / "config.json").is_file())

    def test_cli_update_drain_workers_argument_order(self):
        with patch.object(updater, "main", return_value=None) as upd_main:
            cli.main(["--state", str(self.state), "update", "--drain-workers", "run"])
            cli.main(["--state", str(self.state), "update", "run", "--drain-workers"])
        self.assertEqual(
            [c.args[0] for c in upd_main.call_args_list],
            [
                ["--state", str(self.state), "--drain-workers", "run"],
                ["--state", str(self.state), "run", "--drain-workers"],
            ],
        )
        # Verify updater.main parses both flag positions via cli.main end-to-end.
        fake_updater = unittest.mock.Mock()
        fake_updater.run.return_value = {"status": "up_to_date"}
        with patch.object(updater, "Updater", return_value=fake_updater) as updater_cls:
            buf = io.StringIO()
            with redirect_stdout(buf):
                cli.main(["--state", str(self.state), "update", "--drain-workers", "run"])
                cli.main(["--state", str(self.state), "update", "run", "--drain-workers"])
        self.assertEqual(updater_cls.call_args_list, [unittest.mock.call(self.state), unittest.mock.call(self.state)])
        self.assertEqual(fake_updater.run.call_args_list, [unittest.mock.call(drain_workers=True), unittest.mock.call(drain_workers=True)])

        # Unknown flags on non-update subcommands must still fail argparse validation.
        with redirect_stdout(io.StringIO()), patch.object(sys, "stderr", io.StringIO()):
            with self.assertRaises(SystemExit) as bogus_err:
                cli.main(["--state", str(self.state), "serve", "--bogus-flag"])
            self.assertEqual(2, bogus_err.exception.code)

    def test_profile_update_omitting_provider_preserves_existing_provider_and_verified(self):
        cfg = {
            "profiles": {
                "p1": {
                    "runtime": "codex",
                    "provider": "openai-codex",
                    "model": "gpt-5",
                    "effort": "low",
                    "verified": True,
                    "verified_at": "2026-11-01",
                    "price": {
                        "currency": "USD",
                        "source": "test",
                        "as_of": "2026-11-16",
                        "per_million": {"input_tokens": 1.0, "output_tokens": 2.0},
                    },
                }
            }
        }
        (self.state / "config.json").write_text(json.dumps(cfg))
        code, res = self._run_cli(
            ["init", "--non-interactive", "--profile", "name=p1,runtime=codex,model=gpt-5,effort=high"]
        )
        self.assertEqual(0, code, res)
        saved_p1 = json.loads((self.state / "config.json").read_text())["profiles"]["p1"]
        self.assertEqual("openai-codex", saved_p1["provider"])
        self.assertEqual("high", saved_p1["effort"])
        self.assertTrue(saved_p1["verified"])
        self.assertEqual("2026-11-01", saved_p1["verified_at"])
        self.assertEqual(
            {"currency": "USD", "source": "test", "as_of": "2026-11-16", "per_million": {"input_tokens": 1.0, "output_tokens": 2.0}},
            saved_p1["price"],
        )

    def test_concurrent_init_config_modification_raises_config_conflict(self):
        cfg = {"profiles": {"p1": {"runtime": "codex", "provider": "openai-codex", "model": "gpt-5"}}}
        (self.state / "config.json").write_text(json.dumps(cfg))
        concurrent_cfg = {
            "profiles": {
                "p1": {"runtime": "codex", "provider": "openai-codex", "model": "gpt-5"},
                "p_other": {"runtime": "claude", "provider": "anthropic", "model": "claude-sonnet"},
            },
            "quota_limit": 2,
        }
        orig_commit = init_config._commit_changes

        def _racy_commit(*args, **kwargs):
            (self.state / "config.json").write_text(json.dumps(concurrent_cfg))
            return orig_commit(*args, **kwargs)

        with (
            patch.object(init_config, "_commit_changes", side_effect=_racy_commit),
            self.assertRaises(BridgeError) as conflict_err,
        ):
            init_config.run_init(
                self.state,
                argparse.Namespace(non_interactive=True, quota_limit=5),
            )
        self.assertEqual("config_conflict", conflict_err.exception.code)
        reloaded = json.loads((self.state / "config.json").read_text())
        self.assertIn("p_other", reloaded["profiles"])
        self.assertEqual(2, reloaded["quota_limit"])

    def test_host_pins_lock_preserves_concurrent_broker_pin_on_other_host(self):
        from agent_bridge import hosts as hosts_mod
        from agent_bridge.hosts import HOST_PINS_FILENAME, ConfigHostRegistry, NodeRef

        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {"p1": {"runtime": "codex", "model": "gpt-5"}},
                    "hosts": {
                        "h1": {"ssh_target": "host1", "remote_executable": "/bin/ab"},
                        "h2": {"ssh_target": "host2", "remote_executable": "/bin/ab"},
                    },
                }
            )
        )
        registry = ConfigHostRegistry(self.state)
        registry.pin("h1", NodeRef("h1", "node-1", "gen-1"))

        injected = {"done": False}
        orig_clear = hosts_mod.clear_persisted_host_pins

        def _clear_after_broker_pin(state_dir: Path, host_ids: set[str], *, check_only: bool = False):
            if not check_only and not injected["done"]:
                injected["done"] = True
                registry.pin("h2", NodeRef("h2", "node-2", "gen-2"))
            return orig_clear(state_dir, host_ids, check_only=check_only)

        with patch.object(init_config, "_hosts_clear_persisted_pins", side_effect=_clear_after_broker_pin):
            code, _ = self._run_cli(["init", "--non-interactive", "--remove-host", "h1"])
        self.assertEqual(0, code)
        pins = json.loads((self.state / HOST_PINS_FILENAME).read_text())["hosts"]
        self.assertNotIn("h1", pins)
        self.assertIn("h2", pins)
        self.assertEqual("node-2", pins["h2"]["node_id"])

        # Now reverse ordering: init removes h2 while a stale-in-memory registry pins h1!
        stale_registry = ConfigHostRegistry(self.state)
        # Re-add h2 first so stale_registry has both h1 & h2 in memory:
        (self.state / HOST_PINS_FILENAME).write_text(
            json.dumps({"version": 1, "hosts": {"h2": {"host_id": "h2", "node_id": "node-2", "state_generation": "gen-2"}}})
        )
        stale_registry = ConfigHostRegistry(self.state)
        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {"p1": {"runtime": "codex", "model": "gpt-5"}},
                    "hosts": {
                        "h1": {"ssh_target": "host1", "remote_executable": "/bin/ab"},
                        "h2": {"ssh_target": "host2", "remote_executable": "/bin/ab"},
                    },
                }
            )
        )
        stale_registry = ConfigHostRegistry(self.state)
        self.assertEqual(0, self._run_cli(["init", "--non-interactive", "--remove-host", "h2"])[0])
        stale_registry.pin("h1", NodeRef("h1", "node-1-new", "gen-1-new"))
        pins_final = json.loads((self.state / HOST_PINS_FILENAME).read_text())["hosts"]
        self.assertIn("h1", pins_final)
        self.assertNotIn("h2", pins_final)

    def test_service_install_interrupt_with_unchanged_config_reports_saved_service_files(self):
        # Write valid config first so config_changed=False on the second run.
        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {"p1": {"runtime": "codex", "model": "gpt-5"}},
                    "worker_launcher": "systemd" if init_config.detect_environment().get("systemd_user") else "detached",
                    "parallel_limit": 3,
                    "quota_limit": 2,
                    "task_timeout": 3600,
                    "interrupt_timeout": 30,
                    "default_strategy": "balanced",
                    "hosts": {},
                    "projects": {},
                }
            )
        )
        self.root.mkdir(mode=0o700, exist_ok=True)

        def _interrupt_on_systemctl_reload(cmd, timeout=10):
            if cmd[:3] == ["systemctl", "--user", "show"]:
                return "ActiveState=inactive\nMainPID=0\nExecStart=\n"
            raise KeyboardInterrupt()

        with (
            patch.object(Path, "home", return_value=self.home),
            patch.object(updater.Path, "home", return_value=self.home),
            patch.object(updater, "_run", side_effect=_interrupt_on_systemctl_reload),
            self.assertRaises(BridgeError) as int_err,
        ):
            init_config.run_init(
                self.state,
                argparse.Namespace(
                    non_interactive=True,
                    install_broker_service=True,
                    enable_broker_service=True,
                    ota_root=self.root,
                    service_kind="systemd",
                ),
            )
        self.assertEqual("init_interrupted", int_err.exception.code)
        self.assertIn("broker_service", str(int_err.exception))


if __name__ == "__main__":
    unittest.main()
