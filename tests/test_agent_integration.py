import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_bridge import integration, updater
from agent_bridge.errors import BridgeError


class AgentIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name).resolve()
        self.state = self.home / "state"
        self.state.mkdir(mode=0o700)
        self.root = self.home / "managed"
        self.root.mkdir(mode=0o700)
        self.launcher = self.root / "bin/agent-bridge"
        self.launcher.parent.mkdir()
        self.launcher.write_text("#!/bin/sh\n")
        self.launcher.chmod(0o700)
        self.release = self.root / "releases/old"
        bundled = self.release / "skill"
        bundled.mkdir(parents=True)
        (bundled / "SKILL.md").write_text("first release")
        (self.root / "current").symlink_to(self.release)
        self.skill = self.root / "current/skill"
        updater._write(self.state / "update.json", {
            "repository": updater.REPOSITORY,
            "root": str(self.root),
            "service_kind": "systemd",
            "service_name": "cli-agent-bridge.service",
            "bootstrap_python": sys.executable,
        })
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {}, clear=True).start()
        patch.object(Path, "home", return_value=self.home).start()
        patch.object(integration.shutil, "which", side_effect=lambda name: f"/fixture/bin/{name}").start()
        self.args = ["--state", str(self.state), "mcp"]

    def write_server(self, path, command=None, *, agent="pi", name=integration.NAME):
        command = str(self.launcher) if command is None else command
        path.parent.mkdir(parents=True, exist_ok=True)
        if agent == "codex":
            path.write_text(
                f'[mcp_servers.{name}]\ncommand = {json.dumps(command)}\nargs = {json.dumps(self.args)}\n'
            )
        else:
            path.write_text(json.dumps({
                "mcpServers": {name: {"command": command, "args": self.args}},
            }))

    def test_each_client_gets_its_own_registration_and_stable_skill_link(self):
        cases = (
            ("codex", "codex", ".codex/config.toml", ".codex/skills", []),
            ("claude", "claude", ".claude.json", ".claude/skills", ["--scope", "user", "--transport", "stdio"]),
            ("pi", "pi", ".pi/agent/mcp.json", ".pi/agent/skills", []),
            ("antigravity", "agy", ".gemini/config/mcp_config.json", ".gemini/antigravity-cli/skills", ["--type", "stdio"]),
        )
        for agent, cli, config, directory, flags in cases:
            with self.subTest(agent=agent):
                def register(*_args, config=config, agent=agent, **_kwargs):
                    self.write_server(self.home / config, agent=agent)

                with patch.object(integration.subprocess, "run", side_effect=register) as run:
                    result = integration.install_integration(self.state, agent)
                    link = self.home / directory / integration.NAME
                    self.assertEqual(link, Path(result["skill"]))
                    self.assertEqual(self.skill, link.readlink())
                    self.assertEqual(run.call_args.args[0], [
                        f"/fixture/bin/{cli}", "mcp", "add", *flags, integration.NAME, "--", str(self.launcher), *self.args,
                    ])
                    self.assertTrue(run.call_args.kwargs["check"])
                    self.assertEqual(result, integration.install_integration(self.state, agent))
                    run.assert_called_once()
        self.assertFalse((self.home / ".agents").exists())

        new = self.root / "releases/new"
        (new / "skill").mkdir(parents=True)
        (new / "skill/SKILL.md").write_text("updated release")
        (self.root / "current").unlink()
        (self.root / "current").symlink_to(new)
        self.assertEqual("updated release", (self.home / ".codex/skills/cli-agent-bridge/SKILL.md").read_text())

    def test_conflicting_mcp_and_shared_skills_are_preserved(self):
        config = self.home / ".pi/agent/mcp.json"
        self.write_server(config, "/fixture/other", name="cli_agent_bridge")
        original = config.read_bytes()
        with patch.object(integration.subprocess, "run") as run:
            with self.assertRaisesRegex(BridgeError, "conflicting Bridge MCP server name"):
                integration.install_integration(self.state, "pi")
            self.assertEqual(original, config.read_bytes())
            self.assertFalse((self.home / ".pi/agent/skills").exists())
            shared = self.home / ".gemini/config/skills/cli-agent-bridge"
            shared.mkdir(parents=True)
            with self.assertRaisesRegex(BridgeError, "shared skill directory"):
                integration.install_integration(self.state, "agy")
            self.assertTrue(shared.is_dir())
            self.assertFalse((self.home / ".gemini/antigravity-cli").exists())
            run.assert_not_called()

    def test_failed_registration_rolls_back_only_the_new_skill_link(self):
        custom = self.home / "claude-custom"
        with (
            patch.dict(os.environ, {"CLAUDE_CONFIG_DIR": str(custom)}),
            patch.object(integration.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["claude"])),
        ):
            with self.assertRaisesRegex(BridgeError, "could not register"):
                integration.install_integration(self.state, "claude-code")
        self.assertFalse((custom / "skills/cli-agent-bridge").is_symlink())
        self.assertFalse((self.home / ".claude").exists())

    def test_rejects_concurrent_registration_for_the_same_agent(self):
        with (
            integration._install_lock(self.home / ".pi/agent"),
            patch.object(integration.subprocess, "run") as run,
        ):
            with self.assertRaisesRegex(BridgeError, "Another integration"):
                integration.install_integration(self.state, "pi")
            run.assert_not_called()
