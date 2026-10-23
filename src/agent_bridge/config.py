from __future__ import annotations

import json
import math
import os
import re
import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .errors import BridgeError
from .usage import validate_price


@dataclass(frozen=True)
class Profile:
    name: str
    runtime: str
    model: str
    provider: str = ""
    endpoint_host: str | None = None
    effort: str | None = None
    service_tier: str | None = None
    executable: str | None = None
    quota_group: str = "default"
    billing_mode: str = "unknown"
    env_file: str | None = None
    input_types: list[str] = field(default_factory=lambda: ["text"])
    context_window: int | None = None
    verified: bool = False
    price: dict | None = None
    tools: list[str] = field(default_factory=lambda: ["read", "search", "edit", "shell"])
    verified_at: str | None = None

    def validate(self):
        if self.runtime not in {"codex", "claude", "pi"}:
            raise BridgeError("invalid_runtime", "runtime must be codex, claude or pi")
        if not self.name or not self.model:
            raise BridgeError("invalid_profile", "profile name and exact model ID are required")
        if self.runtime == "pi" and not self.provider:
            raise BridgeError("invalid_profile", "Pi requires an explicit provider")
        if self.context_window is not None and self.context_window <= 0:
            raise BridgeError("invalid_profile", "context_window must be positive")
        if self.service_tier is not None and (
            self.runtime != "codex" or not isinstance(self.service_tier, str) or not self.service_tier.strip()
        ):
            raise BridgeError("invalid_profile", "service_tier is supported only for Codex and must be a nonempty string")
        validate_price(self.price)
        return self

    def command(self):
        command = self.executable or self.runtime
        resolved = shutil.which(command)
        if not resolved:
            raise BridgeError("cli_missing", f"Executable not found: {command}")
        return resolved

    def environment(self):
        env = dict(os.environ)
        if self.env_file:
            path = Path(self.env_file).expanduser()
            if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077:
                raise BridgeError("private_config_required", "Environment file must be owned by you with mode 600")
            # A data file, never a shell script: no evaluation or command substitution.
            values = json.loads(path.read_text())
            if not isinstance(values, dict) or any(not isinstance(v, str) for v in values.values()):
                raise BridgeError("invalid_profile", "Environment file must contain a JSON string map")
            env.update(values)
        env.pop("CODEX_THREAD_ID", None)
        env.pop("AGENT_BRIDGE_TOKEN", None)
        env["AGENT_BRIDGE_ROLE"] = "child"
        return env

    def public(self):
        value = asdict(self)
        value.pop("env_file", None)
        return value


@dataclass
class Config:
    profiles: dict[str, Profile]
    parallel_limit: int = 3
    quota_limit: int = 2
    worker_launcher: str = "systemd"
    task_timeout: float = 3600
    interrupt_timeout: float = 30
    token_budget: int | None = None
    codex_async_python: str | None = None
    telegram_database: str | None = None
    default_strategy: str = "balanced"

    @classmethod
    def load(cls, path: Path):
        data = json.loads(path.read_text())
        telegram_db = Path.home() / ".local/state/codex-async/telegram.sqlite"
        if "telegram_database" not in data and telegram_db.is_file():
            data["telegram_database"] = str(telegram_db)
        profiles = {name: Profile(name=name, **value).validate() for name, value in data.pop("profiles").items()}
        config = cls(profiles=profiles, **data)
        if config.default_strategy not in {"balanced", "cost", "speed"}:
            raise BridgeError("invalid_config", "default_strategy must be balanced, cost or speed")
        if config.parallel_limit < 1 or config.quota_limit < 1:
            raise BridgeError("invalid_config", "Parallel limits must be positive")
        if config.worker_launcher not in {"systemd", "detached"}:
            raise BridgeError("invalid_config", "worker_launcher must be systemd or detached")
        if any(not math.isfinite(value) or value <= 0 for value in (config.task_timeout, config.interrupt_timeout)):
            raise BridgeError("invalid_config", "Timeouts must be positive finite values")
        if config.token_budget is not None and (not isinstance(config.token_budget, int) or config.token_budget <= 0):
            raise BridgeError("invalid_config", "token_budget must be a positive integer")
        return config


def state_directory(value: str | None = None) -> Path:
    path = (
        Path(value or os.environ.get("AGENT_BRIDGE_STATE", Path.home() / ".local/state/cli-agent-bridge"))
        .expanduser()
        .resolve()
    )
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.stat().st_uid != os.getuid():
        raise BridgeError("invalid_owner", "State directory belongs to a different user")
    path.chmod(0o700)
    return path


def safe_error(value: object) -> str:
    text = str(value)
    text = re.sub(r"(?i)(bearer\s+)[^\s\"']+", r"\1[redacted]", text)
    text = re.sub(r"\bsk-[A-Za-z0-9_-]{12,}", "[redacted]", text)
    return text[:4000]
