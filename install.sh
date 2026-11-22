#!/bin/sh
set -eu
umask 077

case ${1-} in
    -h|--help) printf '%s\n' 'Usage: sh install.sh [--no-start]'; exit 0 ;;
    ''|--no-start) [ "$#" -le 1 ] || exit 2 ;;
    *) printf '%s\n' 'Usage: sh install.sh [--no-start]' >&2; exit 2 ;;
esac
case $(uname -s) in
    Linux|Darwin) ;;
    *) printf '%s\n' 'Agent Bridge supports Linux and macOS.' >&2; exit 1 ;;
esac

export PATH="$HOME/.local/bin:$PATH"
command -v curl >/dev/null || { printf '%s\n' 'Install curl first.' >&2; exit 1; }
if ! command -v uv >/dev/null; then
    uv_script=$(mktemp "${TMPDIR:-/tmp}/agent-bridge-uv.XXXXXXXX")
    trap 'rm -- "$uv_script"' EXIT
    curl -qfsSL --proto '=https' --proto-redir '=https' --max-time 60 \
        -o "$uv_script" https://astral.sh/uv/0.13.7/install.sh
    UV_INSTALL_DIR="$HOME/.local/bin" UV_NO_MODIFY_PATH=1 sh "$uv_script"
    rm -- "$uv_script"
    trap - EXIT
fi

exec uv run --no-project --python 3.11 python -I - "$@" <<'PY'
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPOSITORY = "qinjian623/agent-bridge"
RELEASES = f"https://github.com/{REPOSITORY}/releases"
MAX_WHEEL = 100_000_000


def download(url, target, limit):
    subprocess.run([
        "curl", "-qfsSL", "--proto", "=https", "--proto-redir", "=https", "--connect-timeout", "10",
        "--max-time", "120", "--max-filesize", str(limit), "-o", str(target), url,
    ], check=True)
    if not 0 < target.stat().st_size <= limit:
        raise ValueError("Invalid release download size")


def asset_size(manifest, filename, limit):
    assets = manifest.get("assets")
    if not isinstance(assets, list):
        raise ValueError("Invalid GitHub release manifest")
    sizes = [item.get("size") for item in assets if isinstance(item, dict) and item.get("name") == filename]
    if len(sizes) != 1 or type(sizes[0]) is not int or not 0 < sizes[0] <= limit:
        raise ValueError(f"Invalid release asset: {filename}")
    return sizes[0]


def check_fresh(root, state, launcher):
    if root.exists() or root.is_symlink() or launcher.exists() or launcher.is_symlink():
        raise ValueError("An Agent Bridge installation already exists; use its OTA updater to upgrade it")
    if state.is_symlink() or (state.exists() and (not state.is_dir() or any(state.iterdir()))):
        raise ValueError("An Agent Bridge state directory already exists; use agent-bridge init to manage it")


def install():
    home = Path.home()
    root = home / ".local/share/cli-agent-bridge"
    state = Path(os.environ.get("AGENT_BRIDGE_STATE") or home / ".local/state/cli-agent-bridge").expanduser()
    launcher = home / ".local/bin/agent-bridge"
    check_fresh(root, state, launcher)
    with tempfile.TemporaryDirectory(prefix="agent-bridge-install-") as directory:
        files = Path(directory)
        download(f"{RELEASES}/latest/download/latest.json", files / "latest.json", 262_144)
        manifest = json.loads((files / "latest.json").read_bytes())
        tag = manifest.get("tag_name") if isinstance(manifest, dict) else None
        if (not isinstance(tag, str) or not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag)
                or manifest.get("draft") or manifest.get("prerelease")):
            raise ValueError("The installer requires a stable Agent Bridge Release")
        wheel = files / f"cli_agent_bridge-{tag[1:]}-py3-none-any.whl"
        wheel_size = asset_size(manifest, wheel.name, MAX_WHEEL)
        checksums = files / "SHA256SUMS"
        checksums_size = asset_size(manifest, checksums.name, 65_536)
        download(f"{RELEASES}/download/{tag}/{checksums.name}", checksums, checksums_size)
        if checksums.stat().st_size != checksums_size:
            raise ValueError("Release checksum file size does not match latest.json")
        matches = [fields[0] for line in checksums.read_text(encoding="ascii").splitlines()
                   if len(fields := line.split()) == 2 and fields[1] == wheel.name]
        if len(matches) != 1 or not re.fullmatch(r"[0-9a-fA-F]{64}", matches[0]):
            raise ValueError("SHA256SUMS must contain exactly one hash for the Release wheel")
        download(f"{RELEASES}/download/{tag}/{wheel.name}", wheel, wheel_size)
        with wheel.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if wheel.stat().st_size != wheel_size or digest != matches[0].lower():
            raise ValueError("Release wheel failed SHA-256 verification")

        check_fresh(root, state, launcher)
        root.mkdir(parents=True, mode=0o700)
        target = root / "releases" / f"{tag}-initial"
        python = target / "bin/python"
        try:
            subprocess.run(["uv", "venv", "--python", sys.executable, str(target)], check=True)
            subprocess.run(["uv", "pip", "install", "--python", str(python), str(wheel)], check=True)
            probe = (
                "from importlib.metadata import version; from pathlib import Path; import agent_bridge, sys; "
                "assert version('cli-agent-bridge') == agent_bridge.__version__ == sys.argv[1]; "
                "print(Path(agent_bridge.__file__).parent / 'skill')"
            )
            skill = Path(subprocess.check_output([str(python), "-I", "-c", probe, tag[1:]], text=True).strip())
            if not skill.is_dir() or not skill.resolve().is_relative_to(target.resolve()):
                raise ValueError("The Release wheel is missing its packaged skill")
            (target / "skill").symlink_to(skill.relative_to(target), target_is_directory=True)
            (target / "release.json").write_text(json.dumps({
                "tag": tag, "wheel": wheel.name, "release_id": manifest.get("id"), "sha256": digest,
            }) + "\n", encoding="utf-8")
            (root / "current").symlink_to(target, target_is_directory=True)
        except BaseException:
            shutil.rmtree(root)
            raise

    start = "--no-start" not in sys.argv[1:]
    command = [str(python), "-I", "-m", "agent_bridge.cli", "--state", str(state), "init", "--non-interactive",
               "--ota-repo", REPOSITORY, "--install-broker-service", "--install-updater"]
    if start:
        command += ["--enable-broker-service", "--enable-updater"]
    try:
        subprocess.run(command, check=True)
        launcher.parent.mkdir(parents=True, exist_ok=True)
        launcher.symlink_to(root / "bin/agent-bridge")
    except (OSError, subprocess.CalledProcessError):
        print(f"Release files are installed. Use {python} -m agent_bridge.cli init to finish setup.", file=sys.stderr)
        raise
    print(f"Installed Agent Bridge {tag} at {launcher}")
    print("Broker and scheduled OTA enabled." if start else "Service files installed; Broker and OTA timer are stopped.")
    print("Run agent-bridge init to configure your agent profiles.")


try:
    install()
except (OSError, ValueError, subprocess.CalledProcessError) as error:
    sys.exit(f"Agent Bridge install failed: {error}")
PY
