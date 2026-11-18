import io
import json
import os
import tempfile
import unittest
from contextlib import chdir, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from agent_bridge import cli
from agent_bridge.config import broker_socket_path, resolve_state_directory, state_directory
from agent_bridge.errors import BridgeError


class StateDirectoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()

    def run_cli(self, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        previous_mask = os.umask(0o077)
        try:
            with redirect_stdout(stdout), redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
                cli.main(list(args))
        finally:
            os.umask(previous_mask)
        return caught.exception.code, stdout.getvalue(), stderr.getvalue()

    def test_empty_environment_uses_default_without_touching_project(self):
        project = self.root / "project"
        project.mkdir()
        project.chmod(0o755)
        home = self.root / "home"
        default = home / ".local/state/cli-agent-bridge"
        with chdir(project), patch.object(Path, "home", return_value=home), patch.dict(
            os.environ, {"AGENT_BRIDGE_STATE": "", "AGENT_BRIDGE_LANG": "en"}
        ):
            self.assertEqual(default, resolve_state_directory())
            self.assertFalse(home.exists())
            code, _, _ = self.run_cli("doctor")
            self.assertEqual(1, code)  # No config in the newly created state directory.
            self.assertEqual(0o700, default.stat().st_mode & 0o777)
            self.assertEqual(0o755, project.stat().st_mode & 0o777)
            self.assertEqual([], list(project.iterdir()))

            (default / "config.json").write_text('{"profiles": {}, "language": "zh-CN"}', encoding="utf-8")
            del os.environ["AGENT_BRIDGE_LANG"]
            code, help_text, _ = self.run_cli("--help")
            self.assertEqual(0, code)
            self.assertIn("用法：", help_text)

    def test_explicit_nonprivate_directory_is_not_chmodded(self):
        existing = self.root / "existing"
        existing.mkdir()
        existing.chmod(0o755)
        with self.assertRaises(BridgeError) as caught:
            state_directory(existing)
        self.assertEqual("invalid_state_permissions", caught.exception.code)
        self.assertEqual(0o755, existing.stat().st_mode & 0o777)

    def test_socket_path_limits_are_measured_in_bytes(self):
        suffix_bytes = len(os.fsencode("//broker.sock"))
        for platform, limit in (("linux", 107), ("darwin", 103)):
            with self.subTest(platform=platform), patch("agent_bridge.config.sys.platform", platform):
                state = Path("/") / ("界" + "a" * (limit - suffix_bytes - len(os.fsencode("界"))))
                self.assertEqual(limit, len(os.fsencode(broker_socket_path(state))))
                with self.assertRaises(BridgeError) as caught:
                    broker_socket_path(Path(str(state) + "a"))
                self.assertEqual("socket_path_too_long", caught.exception.code)

    def test_doctor_and_serve_fail_early_with_socket_path_error(self):
        state = self.root / ("界" * 40)
        for command in ("doctor", "serve"):
            with self.subTest(command=command):
                code, _, stderr = self.run_cli("--state", str(state), command)
                self.assertEqual(1, code)
                self.assertEqual("socket_path_too_long", json.loads(stderr)["error"]["code"])
                self.assertEqual([], list(state.iterdir()))
