"""Check the installed wheel against a temporary broker without a model account."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import tempfile
from importlib.metadata import metadata
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from agent_bridge import __version__
from agent_bridge.updater import REPOSITORY, Updater


async def check() -> None:
    package = metadata("cli-agent-bridge")
    package_version = package["Version"]
    if __version__ != package_version:
        raise RuntimeError(f"Installed wheel version {package_version} differs from runtime version {__version__}")
    if os.environ.get("GITHUB_REF_TYPE") == "tag":
        repository = os.environ["GITHUB_REPOSITORY"]
        urls = set(package.get_all("Project-URL") or ())
        expected = {f"Repository, https://github.com/{repository}", f"Issues, https://github.com/{repository}/issues"}
        if repository.casefold() != REPOSITORY.casefold() or not expected.issubset(urls):
            raise RuntimeError("The wheel metadata and default update source must point to the release repository")
    # macOS's default temporary-directory prefix can exhaust Unix socket path space.
    with tempfile.TemporaryDirectory(prefix="bridge-smoke-", dir="/tmp") as work:
        state = Path(work).resolve()
        (state / "config.json").write_text('{"profiles": {}, "worker_launcher": "detached"}', encoding="utf-8")
        command = [sys.executable, "-I", "-m", "agent_bridge.cli", "--state", str(state)]
        broker = subprocess.Popen([*command, "serve"], cwd=state)
        try:
            await Updater._healthy(state, expected_version=package_version, process=broker)
            params = StdioServerParameters(
                command=command[0], args=[*command[1:], "mcp"], cwd=state,
            )
            async with asyncio.timeout(15):
                async with stdio_client(params) as (reader, writer), ClientSession(reader, writer) as client:
                    await client.initialize()
                    result = await client.call_tool("agent_inspect", {})
                    if result.isError:
                        raise RuntimeError("MCP could not inspect the temporary broker")
        finally:
            if broker.poll() is None:
                broker.terminate()
            try:
                broker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                broker.kill()
                broker.wait(timeout=5)
    print(f"Installed Bridge {package_version} starts and answers an MCP request")


if __name__ == "__main__":
    asyncio.run(check())
