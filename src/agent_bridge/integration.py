"""Install the bundled skill and a user-scope stdio MCP registration for one CLI."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import subprocess
import sys
import tomllib
from contextlib import contextmanager
from pathlib import Path

from .errors import BridgeError
from .updater import load_settings

NAME = "cli-agent-bridge"


def _agent_config(agent: str) -> tuple[str, Path, Path, list[str]]:
    home = Path.home()
    match agent:
        case "codex":
            directory = _agent_directory("CODEX_HOME", home / ".codex")
            return "codex", directory / "config.toml", directory / "skills", []
        case "claude":
            directory = _agent_directory("CLAUDE_CONFIG_DIR", home / ".claude")
            config_dir = directory if os.environ.get("CLAUDE_CONFIG_DIR") else home
            config = config_dir / (".claude-custom-oauth.json" if os.environ.get("CLAUDE_CODE_CUSTOM_OAUTH_URL") else ".claude.json")
            return "claude", config, directory / "skills", ["--scope", "user", "--transport", "stdio"]
        case "pi":
            directory = _agent_directory("PI_CODING_AGENT_DIR", home / ".pi/agent")
            return "pi", directory / "mcp.json", directory / "skills", []
        case "antigravity":
            return "agy", home / ".gemini/config/mcp_config.json", home / ".gemini/antigravity-cli/skills", [
                "--type", "stdio",
            ]
    raise ValueError("Unsupported agent")


def _agent_directory(variable: str, default: Path) -> Path:
    directory = Path(os.environ.get(variable) or default).expanduser()
    if not directory.is_absolute():
        raise BridgeError("integration_config_invalid", "The agent configuration directory must be absolute")
    return directory


@contextmanager
def _install_lock(directory: Path):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = os.open(directory / f".{NAME}.install.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise BridgeError("integration_busy", "Another integration is installing for the selected agent") from None
        yield
    finally:
        os.close(lock)


def _bridge_paths(state: Path) -> tuple[Path, Path]:
    python_bin = Path(sys.executable).absolute().parent
    release = python_bin.parent
    settings = state / "update.json"
    if settings.exists() or settings.is_symlink():
        root = Path(load_settings(state)["root"])
    elif release.parent.name == "releases" and (release / "release.json").is_file():
        root = release.parent.parent
    else:
        root = None
    if root:
        launcher, skill = root / "bin/agent-bridge", root / "current/skill"
    else:
        launcher = python_bin / "agent-bridge"
        skill = Path(__file__).resolve().with_name("skill")
        if not skill.is_dir():
            skill = Path(__file__).resolve().parents[2] / "skills" / NAME
    if not launcher.is_file() or not os.access(launcher, os.X_OK) or not (skill / "SKILL.md").is_file():
        raise BridgeError("integration_incomplete", "Bridge launcher or bundled skill is missing")
    return launcher, skill


def _read_server(path: Path, agent: str) -> dict | None:
    try:
        contents = path.read_text()
    except FileNotFoundError:
        if not path.is_symlink():
            return None
        raise BridgeError("integration_config_invalid", "The agent MCP configuration cannot be read") from None
    try:
        is_codex = agent == "codex"
        config = tomllib.loads(contents) if is_codex else json.loads(contents)
        servers = config.get("mcp_servers" if is_codex else "mcpServers", {})
        if not isinstance(servers, dict):
            raise ValueError
        if agent == "pi" and "cli_agent_bridge" in servers:
            raise BridgeError("integration_conflict", "A conflicting Bridge MCP server name is already configured")
        if NAME not in servers:
            return None
        entry = servers[NAME]
        if not isinstance(entry, dict):
            raise ValueError
        return entry
    except (AttributeError, ValueError) as error:
        raise BridgeError("integration_config_invalid", "The agent MCP configuration cannot be read") from error


def _matches_server(entry: dict, launcher: Path, args: list[str]) -> bool:
    command = entry.get("command")
    path = Path(command).expanduser() if isinstance(command, str) else None
    return (
        path is not None
        and path.is_absolute()
        and path.resolve() == launcher.resolve()
        and entry.get("args") == args
        and entry.get("type", "stdio") == "stdio"
        and entry.get("enabled", True) is True
        and not entry.get("disabled")
    )


def install_integration(state: Path, agent: str) -> dict[str, str]:
    agent = {"claude-code": "claude", "agy": "antigravity"}.get(agent, agent)
    executable, config, skills, options = _agent_config(agent)
    cli = shutil.which(executable)
    if not cli:
        raise BridgeError("integration_missing_cli", "The selected agent CLI is not installed")
    launcher, skill = _bridge_paths(state)
    link = skills / NAME
    home = Path.home()
    shared = {
        "codex": [home / ".agents/skills"],
        "pi": [home / ".agents/skills"],
        "antigravity": [home / ".gemini/config/skills", home / ".gemini/skills"],
    }
    for directory in shared.get(agent, []):
        other = directory / NAME
        if directory.resolve() != skills.resolve() and (other.exists() or other.is_symlink()):
            raise BridgeError("integration_conflict", "A Bridge skill already exists in a shared skill directory")
    with _install_lock(skills.parent):
        return _install_client(state, agent, cli, options, config, launcher, skill, link)


def _install_client(
    state: Path, agent: str, cli: str, options: list[str], config: Path, launcher: Path, skill: Path, link: Path,
) -> dict[str, str]:
    linked = link.is_symlink() and link.readlink() == skill
    if not linked and (link.exists() or link.is_symlink()):
        raise BridgeError("integration_conflict", "A different Bridge skill already exists in the selected agent")

    # Pin the selected state even if the agent inherits a different AGENT_BRIDGE_STATE.
    args = ["--state", str(state), "mcp"]
    server = _read_server(config, agent)
    if server is not None and not _matches_server(server, launcher, args):
        raise BridgeError("integration_conflict", "A different Bridge MCP server is already configured")

    if not linked:
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(skill, target_is_directory=True)
    try:
        if server is None:
            try:
                subprocess.run(
                    [cli, "mcp", "add", *options, NAME, "--", str(launcher), *args],
                    cwd=Path.home(), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, timeout=45, check=True,
                )
            except (OSError, subprocess.SubprocessError):
                raise BridgeError("integration_mcp_failed", "The selected agent could not register the Bridge MCP server") from None
            registered = _read_server(config, agent)
            if registered is None or not _matches_server(registered, launcher, args):
                raise BridgeError("integration_mcp_failed", "The Bridge MCP registration could not be verified")
    except BaseException:
        if not linked and link.is_symlink() and link.readlink() == skill:
            link.unlink()
        raise
    return {"agent": agent, "skill": str(link), "mcp": NAME, "reload": "required"}
