from __future__ import annotations

import json
import math
import os
import re
import shutil
import stat as stat_module
import sys
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path

from .errors import BridgeError
from .hosts import HostConfig, parse_hosts
from .i18n import validate_language
from .routing_policy import RoutingPreference, parse_routing
from .usage import validate_price

AGY_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})


def agy_model_effort(model: str) -> str | None:
    base, _, tier = model.rpartition("-")
    return tier if base and tier in AGY_EFFORTS else None


def validate_reasoning_effort(value: object) -> str | None:
    if value is not None and (
        not isinstance(value, str) or not 1 <= len(value) <= 64 or value.startswith("-")
        or any(c.isspace() or ord(c) < 32 or 127 <= ord(c) <= 159 for c in value)
    ):
        raise BridgeError("invalid_request", "Reasoning effort must be a non-empty level without whitespace or control characters")
    return value


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
        if self.runtime not in {"codex", "claude", "pi", "antigravity"}:
            raise BridgeError("invalid_runtime", "runtime must be codex, claude, pi or antigravity")
        if not self.name or not isinstance(self.model, str) or not self.model:
            raise BridgeError("invalid_profile", "profile name and exact model ID are required")
        if self.runtime == "pi" and not self.provider:
            raise BridgeError("invalid_profile", "Pi requires an explicit provider")
        if self.runtime == "antigravity":
            if self.provider != "google-antigravity":
                raise BridgeError("invalid_profile", "Antigravity requires provider=google-antigravity, including Claude models")
            if self.endpoint_host is not None:
                raise BridgeError("invalid_profile", "Antigravity uses the native CLI's authentication and endpoint settings")
            if self.input_types != ["text"]:
                raise BridgeError("invalid_profile", "Antigravity streaming input supports text only")
            if self.effort is not None:
                # Preserve the legacy settings for explicitly configured, unsuffixed IDs.
                tier = agy_model_effort(self.model)
                allowed = {tier} if tier else {"low", "medium", "high"}
                if not isinstance(self.effort, str) or self.effort not in allowed:
                    raise BridgeError("invalid_profile", "Antigravity effort must match the tier in the exact model ID")
        if self.context_window is not None and self.context_window <= 0:
            raise BridgeError("invalid_profile", "context_window must be positive")
        if self.service_tier is not None and (
            self.runtime != "codex" or not isinstance(self.service_tier, str) or not self.service_tier.strip()
        ):
            raise BridgeError("invalid_profile", "service_tier is supported only for Codex and must be a nonempty string")
        validate_price(self.price)
        return self

    def command(self):
        command = self.executable or ("agy" if self.runtime == "antigravity" else self.runtime)
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
    hosts: dict[str, HostConfig] = field(default_factory=dict)
    projects: dict[str, str] = field(default_factory=dict)  # project_id -> resolved local root directory
    language: str | None = None
    routing: tuple[RoutingPreference, ...] = ()

    @classmethod
    def load(cls, path: Path):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, UnicodeError) as error:
            raise BridgeError("invalid_config", "config.json must contain valid UTF-8 JSON") from error
        except OSError as error:
            raise BridgeError("invalid_config", "Could not read config.json") from error
        if not isinstance(data, dict):
            raise BridgeError("invalid_config", "config.json must contain a JSON object")
        profile_fields = {item.name for item in fields(Profile)}
        config_fields = {item.name for item in fields(cls)}
        if "profiles" not in data:
            raise BridgeError("invalid_config", "config.json must contain a profiles object")
        raw_profiles = data.pop("profiles")
        if not isinstance(raw_profiles, dict):
            raise BridgeError("invalid_config", "profiles must be a JSON object")
        profiles = {}
        for name, value in raw_profiles.items():
            if not isinstance(name, str) or not name:
                raise BridgeError("invalid_config", "Profile names must be non-empty strings")
            if not isinstance(value, dict):
                raise BridgeError("invalid_config", "Each profile must be a JSON object")
            unknown = sorted(set(value) - profile_fields)
            if unknown:
                raise BridgeError(
                    "invalid_config",
                    "Profile contains unknown fields; see the profile configuration reference",
                )
            if "name" in value:
                if value["name"] != name:
                    raise BridgeError("invalid_config", "Profile has a mismatched name field")
                value = {key: item for key, item in value.items() if key != "name"}
            for key in ("runtime", "model", "provider", "quota_group", "billing_mode"):
                if key in value and not isinstance(value[key], str):
                    raise BridgeError("invalid_config", f"Profile field {key!r} must be a string")
            for key in ("endpoint_host", "effort", "service_tier", "executable", "env_file", "verified_at"):
                if value.get(key) is not None and not isinstance(value[key], str):
                    raise BridgeError("invalid_config", f"Profile field {key!r} must be a string or null")
            for key in ("input_types", "tools"):
                if key in value and (
                    not isinstance(value[key], list) or any(not isinstance(item, str) for item in value[key])
                ):
                    raise BridgeError("invalid_config", f"Profile field {key!r} must be a list of strings")
            if "verified" in value and type(value["verified"]) is not bool:
                raise BridgeError("invalid_config", "Profile field 'verified' must be a boolean")
            if value.get("context_window") is not None and type(value["context_window"]) is not int:
                raise BridgeError("invalid_config", "Profile field 'context_window' must be an integer or null")
            try:
                profiles[name] = Profile(name=name, **value).validate()
            except BridgeError:
                raise
            except (TypeError, ValueError, OverflowError) as error:
                raise BridgeError("invalid_config", "Profile has invalid field types") from error
        raw_hosts = data.pop("hosts", {})
        unknown_top = sorted(set(data) - config_fields)
        if unknown_top:
            raise BridgeError("invalid_config", "Config contains unknown fields; see the configuration reference")
        try:
            hosts = parse_hosts(raw_hosts)
        except BridgeError as error:
            raise BridgeError("invalid_config", "Invalid hosts configuration") from error
        try:
            routing = parse_routing(data.pop("routing", None), profiles)
            config = cls(profiles=profiles, hosts=hosts, routing=routing, **data)
            if config.language is not None:
                try:
                    config.language = validate_language(config.language)
                except ValueError as error:
                    raise BridgeError("invalid_config", "language must be en, zh-CN or auto") from error
            try:
                config.projects = normalize_projects(config.projects)
            except BridgeError as error:
                raise BridgeError("invalid_config", "Invalid projects configuration") from error
            for key in ("codex_async_python", "telegram_database"):
                if getattr(config, key) is not None and not isinstance(getattr(config, key), str):
                    raise BridgeError("invalid_config", f"Config field {key!r} must be a string or null")
            if config.default_strategy not in {"balanced", "cost", "speed"}:
                raise BridgeError("invalid_config", "default_strategy must be balanced, cost or speed")
            if any(type(value) is not int or value < 1 for value in (config.parallel_limit, config.quota_limit)):
                raise BridgeError("invalid_config", "Parallel limits must be positive integers")
            if config.worker_launcher not in {"systemd", "detached"}:
                raise BridgeError("invalid_config", "worker_launcher must be systemd or detached")
            if any(
                type(value) not in {int, float} or not math.isfinite(value) or value <= 0
                for value in (config.task_timeout, config.interrupt_timeout)
            ):
                raise BridgeError("invalid_config", "Timeouts must be positive finite values")
            if config.token_budget is not None and (type(config.token_budget) is not int or config.token_budget <= 0):
                raise BridgeError("invalid_config", "token_budget must be a positive integer")
        except BridgeError:
            raise
        except (TypeError, ValueError, AttributeError, OverflowError) as error:
            raise BridgeError("invalid_config", "Config contains invalid field types") from error
        return config


def normalize_projects(value: object) -> dict[str, str]:
    """Validate project_id -> directory and return resolved absolute root paths."""
    if not isinstance(value, dict):
        raise BridgeError("invalid_config", "projects must map project ids to directory paths")
    projects: dict[str, str] = {}
    for project_id, root in value.items():
        if not isinstance(project_id, str) or not project_id or not isinstance(root, str) or not root.strip():
            raise BridgeError("invalid_config", "projects must map non-empty project ids to directory paths")
        try:
            resolved = Path(root).expanduser().resolve()
        except (OSError, RuntimeError, ValueError) as error:
            raise BridgeError("invalid_config", "Project root cannot be resolved") from error
        if not resolved.is_dir():
            raise BridgeError("invalid_config", f"Project {project_id!r} root is not a directory")
        projects[project_id] = str(resolved)
    return projects


def resolve_state_directory(value: Path | str | None = None) -> Path:
    """Resolve the state location without creating it or changing permissions."""
    selected = value or os.environ.get("AGENT_BRIDGE_STATE") or Path.home() / ".local/state/cli-agent-bridge"
    return Path(selected).expanduser().resolve()


def state_directory(value: Path | str | None = None) -> Path:
    path = resolve_state_directory(value)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = path.stat()
    if info.st_uid != os.getuid():
        raise BridgeError("invalid_owner", "State directory belongs to a different user")
    if stat_module.S_IMODE(info.st_mode) != 0o700:
        raise BridgeError("invalid_state_permissions", "Use a dedicated state directory with permissions 0700")
    return path


def broker_socket_path(state: Path) -> Path:
    path = state / "broker.sock"
    # sun_path includes the terminating NUL on both supported platforms.
    limit = 103 if sys.platform == "darwin" else 107
    if len(os.fsencode(path)) > limit:
        raise BridgeError("socket_path_too_long", "Broker socket path is too long; choose a shorter state directory")
    return path


def safe_error(value: object) -> str:
    text = str(value)
    text = re.sub(r"(?i)(bearer\s+)[^\s\"']+", r"\1[redacted]", text)
    text = re.sub(r"\bsk-[A-Za-z0-9_-]{12,}", "[redacted]", text)
    return text[:4000]


MAX_TOKEN_BYTES = 8192


def read_admin_token(state: Path | str) -> str:
    """Safely read the shared admin token.

    The file must exist, be a regular file owned by the current UID, and have mode 0600. A missing,
    insecure, empty, oversized or malformed token is rejected instead of being used.
    """
    path = Path(state) / "admin.token"
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError as error:
        raise BridgeError(
            "permission_denied", "Admin token is missing; run init or start the Broker first"
        ) from error
    except OSError as error:
        raise BridgeError("permission_denied", "Admin token cannot be opened safely") from error

    try:
        info = os.fstat(descriptor)
        if not stat_module.S_ISREG(info.st_mode):
            raise BridgeError("permission_denied", "Admin token must be a regular file")
        if info.st_uid != os.getuid():
            raise BridgeError("permission_denied", "Admin token must be owned by the current user")
        if stat_module.S_IMODE(info.st_mode) != 0o600:
            raise BridgeError("permission_denied", "Admin token permissions must be 0600")
        if info.st_size <= 0 or info.st_size > MAX_TOKEN_BYTES:
            raise BridgeError("permission_denied", "Admin token is empty or too large")
        data = os.read(descriptor, MAX_TOKEN_BYTES + 1)
    finally:
        os.close(descriptor)

    try:
        token = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("Admin token is not valid UTF-8") from error
    token = token.strip()
    if not token or any(character.isspace() for character in token):
        raise ValueError("Admin token is malformed")
    return token
