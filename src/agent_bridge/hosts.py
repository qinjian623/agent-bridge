"""Coordinator-side host registry interface (phase A)."""
from __future__ import annotations

import json
import math
import os
import re
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Protocol

from .errors import BridgeError
from .peer_protocol import PROTOCOL_VERSION, HostState, NodeRef, PeerErrorCode

HOST_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
HOST_PINS_FILENAME = "host_pins.json"  # pinned identity triples, outside the administrator's config.json
HOST_PINS_FILE_MODE = 0o600
STICKY_HOST_STATES = frozenset({HostState.IDENTITY_CHANGED, HostState.STATE_CORRUPTED})


@dataclass(frozen=True, slots=True)
class HostConfig:
    """One administrator-registered execution node (config.json -> "hosts")."""

    host_id: str  # stable alias, [a-z0-9][a-z0-9_-]{0,63}
    ssh_target: str  # SSH config alias or user@host; ProxyJump/Tailscale resolved by ssh itself
    remote_executable: str  # absolute path of agent-bridge on the node; never taken from user input
    remote_state: str | None = None  # --state for the remote relay, default state dir if None
    pinned: NodeRef | None = None  # set after first successful peer.hello and admin confirmation
    ssh_options: tuple[str, ...] = field(default_factory=tuple)  # extra -o options, whitelist-checked
    pull_interval: float = 2.0  # peer.pull_requests cadence (design 2.2)
    projects: dict[str, str] = field(default_factory=dict)  # project_id -> remote absolute path (ADR-11)


@dataclass(frozen=True, slots=True)
class HostStatus:
    host_id: str
    state: HostState
    node: NodeRef | None
    last_contact: float | None
    protocol_version: int | None
    detail: str | None = None


class HostRegistry(Protocol):
    """Loads HostConfig and tracks live status. No network I/O in `get`/`list`; status is cached."""

    def get(self, host_id: str) -> HostConfig:
        """Raise BridgeError(peer_host_unknown) if unregistered."""

    def list(self) -> list[HostConfig]: ...

    def status(self, host_id: str | None = None) -> list[HostStatus]:
        """Backs the `agent_hosts` tool."""

    def read_configured_hosts(self) -> dict[str, HostConfig]:
        """Validate administrator configuration without changing the active registry."""

    def replace_hosts(self, hosts: Mapping[str, HostConfig]) -> None:
        """Replace registered hosts in place, keeping unchanged live status."""

    def pin(self, host_id: str, node: NodeRef) -> None:
        """Persist the node triple after admin confirmation. Replaces the pin only via explicit re-registration."""

    def mark(self, host_id: str, state: HostState, detail: str | None = None) -> None:
        """Record state transitions; IDENTITY_CHANGED / STATE_CORRUPTED are sticky until `pin` is called."""


def parse_hosts(value: object) -> dict[str, HostConfig]:
    """Parse the optional `hosts` object of config.json (see docs/multi-machine-interface.md)."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise BridgeError("invalid_config", '"hosts" must be a JSON object')
    hosts: dict[str, HostConfig] = {}
    for host_id, raw in value.items():
        if not isinstance(host_id, str) or not HOST_ID_PATTERN.fullmatch(host_id):
            raise BridgeError("invalid_config", f"Invalid host_id: {host_id!r}")
        if not isinstance(raw, Mapping):
            raise BridgeError("invalid_config", f"Host {host_id!r} must be a JSON object")
        hosts[host_id] = _parse_host(host_id, raw)
    return hosts


def _parse_host(host_id: str, raw: Mapping[str, object]) -> HostConfig:
    allowed = {
        "host_id",
        "ssh_target",
        "remote_executable",
        "remote_state",
        "pinned",
        "ssh_options",
        "pull_interval",
        "projects",
    }
    unknown = set(raw) - allowed
    if unknown:
        raise BridgeError("invalid_config", f"Host {host_id!r} has unknown fields: {', '.join(sorted(unknown))}")
    embedded_host_id = raw.get("host_id")
    if embedded_host_id is not None and embedded_host_id != host_id:
        raise BridgeError("invalid_config", f"Host {host_id!r} declares a different host_id")

    ssh_target = _required_string(raw, host_id, "ssh_target")
    remote_executable = _required_string(raw, host_id, "remote_executable")
    remote_state = raw.get("remote_state")
    if remote_state is not None and (not isinstance(remote_state, str) or not remote_state):
        raise BridgeError("invalid_config", f"Host {host_id!r} remote_state must be a non-empty string")

    pinned = raw.get("pinned")
    pinned_ref = None if pinned is None else _parse_node_ref(host_id, pinned, "invalid_config")

    raw_options = raw.get("ssh_options", ())
    if isinstance(raw_options, (str, bytes)) or not isinstance(raw_options, (list, tuple)):
        raise BridgeError("invalid_config", f"Host {host_id!r} ssh_options must be a list of strings")
    if any(not isinstance(option, str) or not option for option in raw_options):
        raise BridgeError("invalid_config", f"Host {host_id!r} ssh_options must be a list of strings")

    pull_interval = raw.get("pull_interval", 2.0)
    if (
        isinstance(pull_interval, bool)
        or not isinstance(pull_interval, (int, float))
        or not math.isfinite(pull_interval)
        or pull_interval <= 0
    ):
        raise BridgeError("invalid_config", f"Host {host_id!r} pull_interval must be a positive number")

    projects = raw.get("projects", {})
    if not isinstance(projects, Mapping) or any(
        not isinstance(key, str) or not key or not isinstance(path, str) or not path
        for key, path in projects.items()
    ):
        raise BridgeError("invalid_config", f"Host {host_id!r} projects must map project ids to paths")

    return HostConfig(
        host_id=host_id,
        ssh_target=ssh_target,
        remote_executable=remote_executable,
        remote_state=remote_state,
        pinned=pinned_ref,
        ssh_options=tuple(raw_options),
        pull_interval=float(pull_interval),
        projects=dict(projects),
    )


def _required_string(raw: Mapping[str, object], host_id: str, key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise BridgeError("invalid_config", f"Host {host_id!r} requires a non-empty {key}")
    return value


def _parse_node_ref(host_id: str, value: object, code: str) -> NodeRef:
    if not isinstance(value, Mapping):
        raise BridgeError(code, f"Host {host_id!r} pinned identity must be a JSON object")
    unknown = set(value) - {"host_id", "node_id", "state_generation"}
    if unknown:
        raise BridgeError(code, f"Host {host_id!r} pinned identity has unknown fields")
    node_id = value.get("node_id")
    generation = value.get("state_generation")
    if not isinstance(node_id, str) or not node_id:
        raise BridgeError(code, f"Host {host_id!r} pinned identity requires node_id")
    if not isinstance(generation, str) or not generation:
        raise BridgeError(code, f"Host {host_id!r} pinned identity requires state_generation")
    if value.get("host_id", host_id) != host_id:
        raise BridgeError(code, f"Host {host_id!r} pinned identity declares a different host_id")
    return NodeRef(host_id=host_id, node_id=node_id, state_generation=generation)


class ConfigHostRegistry:
    """HostRegistry backed by config.json's `hosts` object and a private pinned-identity file.

    Construct with either a host mapping, a state directory (which loads `<state>/config.json`), or both.
    Pins are persisted to `<state>/host_pins.json` when a state directory is provided; without one they are
    only kept in memory. Live status is cached and never fetched over the network here.
    """

    def __init__(
        self,
        hosts: Mapping[str, HostConfig] | str | Path | None = None,
        state_dir: str | Path | None = None,
    ) -> None:
        if isinstance(hosts, (str, Path)):
            if state_dir is not None:
                raise TypeError("state_dir was provided twice")
            state_dir = hosts
            hosts = None
        self._state_dir = Path(state_dir).expanduser() if state_dir is not None else None
        configured = dict(hosts) if hosts is not None else self._load_configured_hosts()
        self._hosts = self._apply_persisted_pins(configured)
        self._statuses = {
            host_id: HostStatus(
                host_id=host_id,
                state=HostState.OFFLINE,
                node=config.pinned,
                last_contact=None,
                protocol_version=None,
            )
            for host_id, config in self._hosts.items()
        }

    def get(self, host_id: str) -> HostConfig:
        try:
            return self._hosts[host_id]
        except KeyError:
            raise BridgeError(PeerErrorCode.HOST_UNKNOWN, f"Unknown host: {host_id!r}") from None

    def list(self) -> list[HostConfig]:
        return [self._hosts[host_id] for host_id in sorted(self._hosts)]

    def read_configured_hosts(self) -> dict[str, HostConfig]:
        """Validate a fresh disk snapshot without changing the live registry."""
        if self._state_dir is None:
            return dict(self._hosts)
        return self._apply_persisted_pins(self._load_configured_hosts(required=True))

    def replace_hosts(self, hosts: Mapping[str, HostConfig]) -> None:
        configured = dict(hosts)
        statuses = {}
        for host_id, config in configured.items():
            previous = self._hosts.get(host_id)
            current = self._statuses.get(host_id)
            repinned = current is not None and config.pinned is not None and config.pinned != current.node
            if previous == config or (current is not None and current.state in STICKY_HOST_STATES and not repinned):
                statuses[host_id] = current
            else:
                statuses[host_id] = HostStatus(host_id, HostState.OFFLINE, config.pinned, None, None)
        self._hosts.clear()
        self._hosts.update(configured)
        self._statuses.clear()
        self._statuses.update(statuses)

    def status(self, host_id: str | None = None) -> list[HostStatus]:
        if host_id is not None:
            self.get(host_id)
            return [self._statuses[host_id]]
        return [self._statuses[key] for key in sorted(self._statuses)]

    def pin(self, host_id: str, node: NodeRef) -> None:
        if not isinstance(node, NodeRef):
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "pin requires a NodeRef")
        config = self.get(host_id)
        self._hosts[host_id] = replace(config, pinned=node)
        current = self._statuses[host_id]
        # A fresh admin-confirmed pin is the documented way to clear a sticky identity/corruption lock.
        self._statuses[host_id] = HostStatus(
            host_id=host_id,
            state=HostState.OFFLINE,
            node=node,
            last_contact=current.last_contact,
            protocol_version=current.protocol_version,
            detail=None,
        )
        self._save_pins()

    def mark(self, host_id: str, state: HostState, detail: str | None = None) -> None:
        self.get(host_id)
        try:
            state = HostState(state)
        except (TypeError, ValueError):
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"Unknown host state: {state!r}") from None
        current = self._statuses[host_id]
        if current.state in STICKY_HOST_STATES and state not in STICKY_HOST_STATES:
            return
        self._statuses[host_id] = HostStatus(
            host_id=host_id,
            state=state,
            node=current.node,
            last_contact=time.time() if state is HostState.ONLINE else current.last_contact,
            protocol_version=PROTOCOL_VERSION if state is HostState.ONLINE else current.protocol_version,
            detail=detail,
        )

    # ---------------------------------------------------------------- internals

    def _load_configured_hosts(self, *, required: bool = False) -> dict[str, HostConfig]:
        if self._state_dir is None:
            return {}
        path = self._state_dir / "config.json"
        if not required and not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise BridgeError("invalid_config", f"Cannot read {path}: {error}") from error
        if not isinstance(data, dict):
            raise BridgeError("invalid_config", "config.json must contain a JSON object")
        return parse_hosts(data.get("hosts", {}))

    def _apply_persisted_pins(self, hosts: dict[str, HostConfig]) -> dict[str, HostConfig]:
        if self._state_dir is None:
            return hosts
        path = self._state_dir / HOST_PINS_FILENAME
        if not path.exists():
            return hosts
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise BridgeError("invalid_host_pins", f"Cannot read {path}: {error}") from error
        if not isinstance(data, dict) or not isinstance(data.get("hosts"), dict):
            raise BridgeError("invalid_host_pins", f"{HOST_PINS_FILENAME} must contain a hosts object")
        result = dict(hosts)
        for host_id, raw in data["hosts"].items():
            if host_id not in result:
                continue  # a pin for a host removed from config.json is stale and must not resurrect it
            result[host_id] = replace(result[host_id], pinned=_parse_node_ref(host_id, raw, "invalid_host_pins"))
        return result

    def _save_pins(self) -> None:
        if self._state_dir is None:
            return
        self._state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        pinned: dict[str, dict[str, str]] = {}
        for host_id, config in self._hosts.items():
            if config.pinned is None:
                continue
            pinned[host_id] = {
                "host_id": config.pinned.host_id,
                "node_id": config.pinned.node_id,
                "state_generation": config.pinned.state_generation,
            }
        payload = json.dumps({"version": 1, "hosts": pinned}, sort_keys=True) + "\n"
        descriptor, temporary = tempfile.mkstemp(prefix=f".{HOST_PINS_FILENAME}.", dir=self._state_dir)
        try:
            os.fchmod(descriptor, HOST_PINS_FILE_MODE)
            with os.fdopen(descriptor, "w", encoding="utf-8") as file:
                file.write(payload)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self._state_dir / HOST_PINS_FILENAME)
            _fsync_directory(self._state_dir)
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
