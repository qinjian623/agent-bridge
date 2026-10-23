"""Install local command, skill links and a user unit without changing other services."""

import json
import os
import subprocess
from pathlib import Path


def link(path, target):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() and path.resolve() == target.resolve():
        return
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"Refusing to replace existing installation: {path}")
    path.symlink_to(target)


def main():
    os.umask(0o077)
    root = Path(__file__).resolve().parents[1]
    command = root / ".venv/bin/agent-bridge"
    if not command.is_file():
        raise RuntimeError("Run uv sync --locked first")
    link(Path.home() / ".local/bin/agent-bridge", command)
    link(Path.home() / ".codex/skills/cli-agent-bridge", root / "skills/cli-agent-bridge")
    unit = Path.home() / ".config/systemd/user/cli-agent-bridge.service"
    text = f'''[Unit]
Description=Local Codex Claude Pi task service

[Service]
Type=simple
ExecStart="{command}" serve
Environment=PATH=%h/.local/bin:%h/.npm-global/bin:/usr/local/bin:/usr/bin:/bin
UMask=0077
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
'''
    unit.parent.mkdir(parents=True, exist_ok=True)
    if unit.exists() and unit.read_text() != text:
        raise FileExistsError(f"Refusing to replace existing unit: {unit}")
    unit.write_text(text)
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    print(
        json.dumps(
            {
                "command": str(command),
                "unit": str(unit),
                "skill": str(Path.home() / ".codex/skills/cli-agent-bridge"),
                "started": False,
                "enabled_at_login": False,
            }
        )
    )


if __name__ == "__main__":
    main()
