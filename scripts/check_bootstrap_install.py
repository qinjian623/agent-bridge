"""Exercise the Release installer using package artifacts and an isolated home."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


def main(dist: Path) -> None:
    dist = dist.resolve(strict=True)
    manifest = json.loads((dist / "latest.json").read_text())
    tag = manifest["tag_name"]
    with tempfile.TemporaryDirectory(prefix="bridge-install-", dir="/tmp") as directory:
        work = Path(directory).resolve()
        home = work / "home"
        fixtures = work / "bin"
        home.mkdir()
        fixtures.mkdir()
        curl = fixtures / "curl"
        curl.write_text(
            f"#!/bin/sh\nexec {shlex.quote(sys.executable)} -I - \"$@\" <<'PY'\n"
            "import os, sys\n"
            "from pathlib import Path\n"
            "args = sys.argv[1:]\n"
            "base = 'https://github.com/qinjian623/agent-bridge/releases/'\n"
            "url = args[-1]\n"
            "name = url.rsplit('/', 1)[1]\n"
            "tag = os.environ['BRIDGE_FIXTURE_TAG']\n"
            "assert url == base + ('latest/download/latest.json' if name == 'latest.json' else f'download/{tag}/{name}')\n"
            "data = (Path(os.environ['BRIDGE_FIXTURE_DIST']) / name).read_bytes()\n"
            "if name.endswith('.whl') and os.environ.get('BRIDGE_FIXTURE_CORRUPT'):\n"
            "    data = bytes([data[0] ^ 1]) + data[1:]\n"
            "Path(args[args.index('-o') + 1]).write_bytes(data)\n"
            "PY\n",
            encoding="utf-8",
        )
        curl.chmod(0o700)
        env = {
            **os.environ,
            "HOME": str(home),
            "PATH": str(fixtures) + os.pathsep + os.environ["PATH"],
            "AGENT_BRIDGE_STATE": "",
            "BRIDGE_FIXTURE_TAG": tag,
            "BRIDGE_FIXTURE_DIST": str(dist),
            "UV_CACHE_DIR": subprocess.check_output(["uv", "cache", "dir"], text=True).strip(),
            "UV_PYTHON_INSTALL_DIR": subprocess.check_output(["uv", "python", "dir"], text=True).strip(),
        }
        env.pop("PYTHONPATH", None)
        root = home / ".local/share/cli-agent-bridge"
        installer = ["sh", str(dist / "install.sh"), "--no-start"]
        bad = subprocess.run(installer, cwd=work, env={**env, "BRIDGE_FIXTURE_CORRUPT": "1"},
                             capture_output=True, text=True, timeout=90)
        assert bad.returncode != 0 and "SHA-256 verification" in bad.stderr, bad.stderr
        assert not root.exists()
        subprocess.run(installer, cwd=work, env=env, check=True, timeout=150)
        state = home / ".local/state/cli-agent-bridge"
        settings = json.loads((state / "update.json").read_text())
        release = json.loads((root / "current/release.json").read_text())
        assert release["tag"] == tag
        assert settings["repository"] == "qinjian623/agent-bridge" and "release_url" not in settings
        assert Path(settings["bootstrap_python"]).is_file()
        launcher = home / ".local/bin/agent-bridge"
        assert launcher.is_symlink() and launcher.resolve() == root / "bin/agent-bridge"
        assert (root / "current/skill/SKILL.md").is_file()
        if sys.platform == "darwin":
            timer = home / "Library/LaunchAgents/com.cli-agent-bridge.update.plist"
        else:
            timer = home / ".config/systemd/user/cli-agent-bridge-update.timer"
        assert timer.is_file()
        subprocess.run([str(launcher), "doctor"], cwd=work, env=env,
                       stdout=subprocess.DEVNULL, check=True, timeout=25)
        repeated = subprocess.run(installer, cwd=work, env=env, capture_output=True, text=True, timeout=25)
        assert repeated.returncode != 0 and "already exists" in repeated.stderr, repeated.stderr
        assert json.loads((state / "update.json").read_text()) == settings
        verify = """
from json import dumps
from pathlib import Path
from sys import argv
from unittest.mock import patch
from agent_bridge.updater import Updater
u = Updater(Path(argv[1]))
assert u.installed()['tag'] == argv[2]
newer = 'v999.0.0'
assets = [{'name': 'cli_agent_bridge-999.0.0-py3-none-any.whl', 'size': 10},
          {'name': 'SHA256SUMS', 'size': 10}]
with patch('agent_bridge.updater._download_bytes', return_value=dumps({'tag_name': newer, 'assets': assets}).encode()) as download:
    status = u.check()
    assert download.call_args.args[0].startswith('https://github.com/qinjian623/agent-bridge/releases/')
    assert status['update_available'] and status['available']['tag'] == newer
"""
        subprocess.run([str(root / "current/bin/python"), "-I", "-c", verify, str(state), tag],
                       cwd=work, env=env, check=True, timeout=25)
    print(f"Release installer verified for {tag}: SHA-256, isolated install, repeat protection, and OTA discovery.")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
