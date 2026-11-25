"""Idempotent configuration and service setup workflow for ``agent-bridge init``."""

from __future__ import annotations

import argparse
import contextlib
import copy
import fcntl
import json
import os
import plistlib
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .config import Config
from .errors import BridgeError
from .hosts import clear_persisted_host_pins as _hosts_clear_persisted_pins
from .i18n import translate as _
from .i18n import validate_language
from .updater import (
    REPOSITORY,
    Updater,
    install,
    install_broker_service,
    install_web_service,
    load_settings,
    validate_broker_service_install,
    validate_release_url,
    validate_update_install,
    validate_web_service_install,
)

_RUNTIMES = ("codex", "claude", "pi", "antigravity")
_STRATEGIES = ("balanced", "cost", "speed")
_DEFAULT_SSH_OPTIONS = [
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=10",
    "-o",
    "StrictHostKeyChecking=yes",
    "-o",
    "ForwardAgent=no",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=3",
]


def detect_environment() -> dict[str, Any]:
    """Inspect locally available CLI runtimes, tools, and user service managers."""
    systemd_ok = False
    if sys.platform.startswith("linux") and shutil.which("systemctl"):
        try:
            res = subprocess.run(
                ["systemctl", "--user", "show-environment"],
                capture_output=True,
                text=True,
                timeout=3,
                check=False,
            )
            systemd_ok = res.returncode == 0
        except (OSError, subprocess.SubprocessError):
            systemd_ok = False
    return {
        "platform": sys.platform,
        "runtimes": {
            "codex": shutil.which("codex"),
            "claude": shutil.which("claude"),
            "pi": shutil.which("pi"),
            "antigravity": shutil.which("agy"),
        },
        "tools": {
            "uv": shutil.which("uv"),
            "ssh": shutil.which("ssh"),
        },
        "systemd_user": systemd_ok,
        "launchd_user": sys.platform == "darwin" and shutil.which("launchctl") is not None,
    }


def _default_config_payload(env: dict[str, Any]) -> dict[str, Any]:
    launcher = "systemd" if env.get("systemd_user") else "detached"
    return {
        "profiles": {},
        "worker_launcher": launcher,
        "parallel_limit": 3,
        "quota_limit": 2,
        "task_timeout": 3600,
        "interrupt_timeout": 30,
        "default_strategy": "balanced",
        "hosts": {},
        "projects": {},
    }


def _validate_ssh_target(target: str) -> str:
    if not isinstance(target, str) or not target.strip():
        raise ValueError(_('ssh_target must be a non-empty string'))
    cleaned = target.strip()
    if any(ch.isspace() or ord(ch) < 32 for ch in cleaned):
        raise ValueError(_('ssh_target must not contain whitespace or control characters'))
    if cleaned.startswith("-"):
        raise ValueError(_("ssh_target must not start with '-'"))
    return cleaned


def _validate_config_payload(state: Path, payload: dict[str, Any]) -> None:
    """Validate serialized config.json contents using Config.load before touching disk."""
    hosts = payload.get("hosts", {})
    if isinstance(hosts, dict):
        for hid, hcfg in hosts.items():
            if isinstance(hcfg, dict) and "ssh_target" in hcfg:
                try:
                    _validate_ssh_target(hcfg["ssh_target"])
                except ValueError as error:
                    raise BridgeError("invalid_config", _('Host {hid!r}: {error}', hid=hid, error=_(str(error)))) from error
    temp_dir = str(state) if state.is_dir() else None
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=temp_dir,
        prefix=".config-validate-",
        suffix=".json",
        encoding="utf-8",
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)
        os.chmod(tmp_path, 0o600)
        json.dump(payload, tmp, indent=2, ensure_ascii=False)
        tmp.write("\n")
    try:
        Config.load(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _write_private_json_if_changed(path: Path, payload: dict[str, Any]) -> bool:
    """Atomically write mode-0600 JSON only when its normalized contents differ."""
    if path.is_symlink():
        raise BridgeError("invalid_config", _('Refusing to overwrite symlinked {value0}', value0=path.name))
    if path.exists():
        try:
            if json.loads(path.read_text(encoding="utf-8")) == payload:
                os.chmod(path, 0o600)
                return False
        except (OSError, ValueError):
            pass
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=path.parent,
        prefix=f".{path.name}-",
        encoding="utf-8",
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)
        os.chmod(tmp_path, 0o600)
        json.dump(payload, tmp, indent=2, ensure_ascii=False)
        tmp.write("\n")
        tmp.flush()
        os.fsync(tmp.fileno())
    try:
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return True


def _load_existing_config(state: Path) -> tuple[dict[str, Any], bool]:
    config_path = state / "config.json"
    if config_path.is_symlink():
        raise BridgeError("invalid_config", _('Refusing to edit symlinked config.json'))
    if not config_path.exists():
        return _default_config_payload(detect_environment()), False
    info = config_path.lstat()
    if info.st_uid != os.getuid():
        raise BridgeError("invalid_owner", _('config.json belongs to a different user'))
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
        Config.load(config_path)
    except (OSError, ValueError, TypeError, BridgeError) as error:
        raise BridgeError("invalid_config", _('Existing config.json is invalid: {error}', error=_(str(error)))) from error
    if not isinstance(raw, dict):
        raise BridgeError("invalid_config", _('Existing config.json must be a JSON object'))
    loaded = copy.deepcopy(raw)
    loaded.setdefault("profiles", {})
    loaded.setdefault("hosts", {})
    loaded.setdefault("projects", {})
    return loaded, True


def _detect_existing_update_interval(kind: str | None) -> int | None:
    if kind == "systemd":
        timer_path = Path.home() / ".config/systemd/user/cli-agent-bridge-update.timer"
        if timer_path.is_file() and not timer_path.is_symlink():
            try:
                match = re.search(r"^\s*OnUnitActiveSec=(\d+)s\s*$", timer_path.read_text(encoding="utf-8"), re.M)
                if match and int(match.group(1)) >= 300:
                    return int(match.group(1))
            except OSError:
                pass
    elif kind == "launchd":
        plist_path = Path.home() / "Library/LaunchAgents/com.cli-agent-bridge.update.plist"
        if plist_path.is_file() and not plist_path.is_symlink():
            try:
                plist = plistlib.loads(plist_path.read_bytes())
                interval = plist.get("StartInterval")
                if type(interval) is int and interval >= 300:
                    return interval
            except (OSError, ValueError):
                pass
    return None


def _load_existing_update(state: Path) -> dict[str, Any] | None:
    path = state / "update.json"
    if path.is_symlink():
        raise BridgeError("invalid_config", _('Refusing to edit symlinked update.json'))
    if not path.exists():
        return None
    loaded: dict[str, Any] | None
    try:
        loaded = dict(load_settings(state))
    except (OSError, ValueError) as error:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return None
        if "release_url" in raw:
            raise BridgeError("invalid_config", _('Existing updater settings are invalid; the configured release source was not changed')) from error
        allowed_str = {"repository", "root", "service_kind", "service_name", "bootstrap_python"}
        loaded = {k: v for k, v in raw.items() if k in allowed_str and isinstance(v, str)}
        if type(raw.get("interval")) is int and raw["interval"] >= 300:
            loaded["interval"] = raw["interval"]
    if "interval" not in loaded:
        detected = _detect_existing_update_interval(loaded.get("service_kind"))
        if detected is not None:
            loaded["interval"] = detected
    return loaded


def _parse_kv_or_json(spec: str, *, required_keys: tuple[str, ...], option_name: str) -> dict[str, Any]:
    text = spec.strip()
    if text.startswith("{"):
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError(_('{option_name} JSON must be an object', option_name=option_name))
    else:
        parsed = {}
        for item in text.split(","):
            part = item.strip()
            if not part:
                continue
            if "=" not in part:
                raise ValueError(_('{option_name} entries must use key=value or JSON: {part!r}', option_name=option_name, part=part))
            key, val = part.split("=", 1)
            parsed[key.strip()] = val.strip()
    missing = [key for key in required_keys if not isinstance(parsed.get(key), str) or not str(parsed[key]).strip()]
    if missing:
        raise ValueError(_('{option_name} missing required field(s): {value1}', option_name=option_name, value1=', '.join(missing)))
    return parsed


def _apply_profile_update(
    profiles: dict[str, Any],
    name: str,
    updates: dict[str, Any],
    *,
    clear_fields: set[str] | None = None,
) -> None:
    existing = copy.deepcopy(profiles.get(name, {})) if isinstance(profiles.get(name), dict) else {}
    cleaned = copy.deepcopy(updates)
    to_clear = set(clear_fields or ())
    for optional_key in ("effort", "executable", "env_file"):
        if optional_key in cleaned and cleaned[optional_key] in ("-", None):
            cleaned.pop(optional_key, None)
            to_clear.add(optional_key)
    if "runtime" in cleaned and cleaned["runtime"] != existing.get("runtime") and "executable" not in cleaned:
        to_clear.add("executable")
    identity_changed = bool(existing) and any(
        existing.get(key) != cleaned.get(key) for key in ("runtime", "provider", "model") if key in cleaned
    )
    merged = {**existing, **cleaned}
    merged.pop("name", None)
    for key in to_clear:
        merged.pop(key, None)
    if identity_changed:
        merged["verified"] = False
        merged.pop("verified_at", None)
        if "price" not in cleaned:
            merged.pop("price", None)
    profiles[name] = merged


def _apply_host_update(hosts: dict[str, Any], host_id: str, updates: dict[str, Any]) -> None:
    existing = copy.deepcopy(hosts.get(host_id, {})) if isinstance(hosts.get(host_id), dict) else {}
    cleaned = copy.deepcopy(updates)
    target_changed = bool(existing) and existing.get("ssh_target") != cleaned.get("ssh_target")
    merged = {**existing, **cleaned}
    merged.pop("host_id", None)
    if target_changed:
        merged.pop("pinned", None)
    if not merged.get("remote_state") or merged.get("remote_state") == "-":
        merged.pop("remote_state", None)
    hosts[host_id] = merged


def _prepare_host_pins_update(state: Path, host_ids: set[str]) -> bool:
    """Validate host_pins.json up front under the shared pins lock without writing disk."""
    return _hosts_clear_persisted_pins(state, host_ids, check_only=True)


def _clear_persisted_host_pins(state: Path, host_ids: set[str]) -> bool:
    """Remove persisted host identity pins for deleted or retargeted hosts under cross-process lock."""
    return _hosts_clear_persisted_pins(state, host_ids, check_only=False)


def _prompt(
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
    label: str,
    *,
    default: str | None = None,
    required: bool = False,
) -> str:
    while True:
        label = _(label)
        suffix = f" [{default}]" if default not in (None, "") else ""
        raw = input_fn(_('{label}{suffix}: ', label=label, suffix=suffix)).strip()
        if raw:
            return raw
        if default is not None and (default != "" or not required):
            return default
        if not required:
            return ""
        output_fn(_('  Value is required.'))


def _prompt_yes_no(
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
    label: str,
    *,
    default: bool = False,
) -> bool:
    label = _(label)
    hint = "Y/n" if default else "y/N"
    while True:
        raw = input_fn(_('{label} [{hint}]: ', label=label, hint=hint)).strip().lower()
        if not raw:
            return default
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        output_fn(_('  Please enter y or n.'))


def _edit_profiles_menu(
    config: dict[str, Any],
    env: dict[str, Any],
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
) -> None:
    profiles: dict[str, Any] = config.setdefault("profiles", {})
    while True:
        output_fn(_('\n-- Runtime Profiles --'))
        detected = ", ".join(k for k, v in env["runtimes"].items() if v) or _("none detected")
        output_fn(_('Detected CLIs: {detected}', detected=detected))
        if profiles:
            for pname, pval in profiles.items():
                output_fn(
                    _('  * {pname}: runtime={value1}, provider={value2}, model={value3}, verified={value4}', pname=pname, value1=pval.get('runtime'), value2=pval.get('provider') or '-', value3=pval.get('model'), value4=bool(pval.get('verified', False)))
                )
        else:
            output_fn(_('  (none configured yet)'))
        choice = input_fn(_('Action ([a]dd/edit, [r]emove, [b]ack): ')).strip().lower()
        if choice in {"b", ""}:
            return
        if choice == "r":
            if not profiles:
                continue
            target = _prompt(input_fn, output_fn, _('Profile name to remove'))
            if target in profiles:
                del profiles[target]
                output_fn(_('Removed profile {target!r}.', target=target))
            else:
                output_fn(_('Profile {target!r} not found.', target=target))
            continue
        if choice != "a":
            output_fn(_('  Unknown choice.'))
            continue

        pname = _prompt(input_fn, output_fn, _('Profile name'), required=True)
        cur = profiles.get(pname, {}) if isinstance(profiles.get(pname), dict) else {}
        while True:
            runtime = _prompt(
                input_fn,
                output_fn,
                _('Runtime ({value0})', value0='/'.join(_RUNTIMES)),
                default=cur.get("runtime"),
                required=True,
            )
            if runtime in _RUNTIMES:
                break
            output_fn(_('  Runtime must be one of: {value0}', value0=', '.join(_RUNTIMES)))
        default_provider = cur.get("provider") or ("google-antigravity" if runtime == "antigravity" else "")
        provider = _prompt(
            input_fn,
            output_fn,
            _('Provider (required for pi/antigravity, e.g. openai-codex/deepseek/google-antigravity)'),
            default=default_provider,
            required=runtime in {"pi", "antigravity"},
        )
        model = _prompt(input_fn, output_fn, _('Exact model ID'), default=cur.get("model"), required=True)
        entry: dict[str, Any] = {
            "runtime": runtime,
            "provider": provider,
            "model": model,
            "quota_group": _prompt(
                input_fn, output_fn, _('Quota group'), default=str(cur.get("quota_group") or "default")
            ),
        }
        clear_fields: set[str] = set()
        effort = _prompt(
            input_fn,
            output_fn,
            _("Reasoning effort (provider-specific, '-' to clear)"),
            default=cur.get("effort") or "",
        )
        if effort == "-":
            clear_fields.add("effort")
        elif effort:
            entry["effort"] = effort
        detected_executable = env["runtimes"].get(runtime)
        if not detected_executable or not Path(detected_executable).is_absolute():
            detected_executable = ""
        previous_executable = cur.get("executable") if cur.get("runtime") == runtime else None
        exe = _prompt(
            input_fn,
            output_fn,
            _("Executable path override (optional, '-' to clear)"),
            default=previous_executable or detected_executable,
        )
        if exe == "-":
            clear_fields.add("executable")
        elif exe:
            entry["executable"] = exe
        env_file = _prompt(
            input_fn,
            output_fn,
            _("Private env JSON file path (optional, '-' to clear)"),
            default=cur.get("env_file") or "",
        )
        if env_file == "-":
            clear_fields.add("env_file")
        elif env_file:
            entry["env_file"] = env_file
        _apply_profile_update(profiles, pname, entry, clear_fields=clear_fields)
        output_fn(_('Staged profile {pname!r}.', pname=pname))


def _edit_runtime_defaults_menu(
    config: dict[str, Any],
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
) -> None:
    output_fn(_('\n-- Runtime Defaults --'))
    while True:
        launcher = _prompt(
            input_fn,
            output_fn,
            _('worker_launcher (systemd/detached)'),
            default=str(config.get("worker_launcher", "systemd")),
            required=True,
        )
        if launcher in {"systemd", "detached"}:
            config["worker_launcher"] = launcher
            break
        output_fn(_("  Must be 'systemd' or 'detached'."))
    for key in ("parallel_limit", "quota_limit"):
        while True:
            raw = _prompt(input_fn, output_fn, key, default=str(config.get(key, 2)), required=True)
            try:
                val = int(raw)
                if val >= 1:
                    config[key] = val
                    break
            except ValueError:
                pass
            output_fn(_('  Must be a positive integer.'))
    while True:
        strategy = _prompt(
            input_fn,
            output_fn,
            _('default_strategy ({value0})', value0='/'.join(_STRATEGIES)),
            default=str(config.get("default_strategy", "balanced")),
            required=True,
        )
        if strategy in _STRATEGIES:
            config["default_strategy"] = strategy
            break
        output_fn(_('  Must be one of: {value0}', value0=', '.join(_STRATEGIES)))
    for key, fallback in (("task_timeout", 3600), ("interrupt_timeout", 30)):
        while True:
            raw = _prompt(input_fn, output_fn, _('{key} (seconds)', key=key), default=str(config.get(key, fallback)), required=True)
            try:
                fval = float(raw)
                if fval > 0:
                    config[key] = int(fval) if fval.is_integer() else fval
                    break
            except ValueError:
                pass
            output_fn(_('  Must be a positive number.'))
    while True:
        budget_raw = _prompt(
            input_fn,
            output_fn,
            _("token_budget (positive int, or 'none' to disable)"),
            default="none" if config.get("token_budget") is None else str(config["token_budget"]),
        )
        if budget_raw.lower() in {"", "none", "-"}:
            config["token_budget"] = None
            break
        try:
            budget_val = int(budget_raw)
            if budget_val > 0:
                config["token_budget"] = budget_val
                break
        except ValueError:
            pass
        output_fn(_("  Must be 'none' or a positive integer."))


def _edit_projects_menu(
    config: dict[str, Any],
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
) -> None:
    projects: dict[str, Any] = config.setdefault("projects", {})
    while True:
        output_fn(_('\n-- Local Projects --'))
        if projects:
            for pid, ppath in projects.items():
                output_fn(_('  * {pid}: {ppath}', pid=pid, ppath=ppath))
        else:
            output_fn(_('  (none)'))
        choice = input_fn(_('Action ([a]dd/edit, [r]emove, [b]ack): ')).strip().lower()
        if choice in {"b", ""}:
            return
        if choice == "r":
            pid = _prompt(input_fn, output_fn, _('Project ID to remove'))
            projects.pop(pid, None)
            continue
        if choice != "a":
            continue
        pid = _prompt(input_fn, output_fn, _('Project ID'), required=True)
        path_val = _prompt(input_fn, output_fn, _('Project root directory'), default=projects.get(pid, ""), required=True)
        projects[pid] = str(Path(path_val).expanduser().resolve())


def _probe_remote_executable(ssh_target: str, ssh_options: list[str] | tuple[str, ...] = ()) -> str | None:
    try:
        clean_target = _validate_ssh_target(ssh_target)
    except ValueError:
        return None
    validated_opts: list[str] = []
    for opt in ssh_options:
        if not isinstance(opt, str) or "\x00" in opt or "\n" in opt or "\r" in opt:
            return None
        validated_opts.append(opt)
    try:
        res = subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                *validated_opts,
                "-o",
                "ConnectTimeout=5",
                "--",
                clean_target,
                "command -v agent-bridge || printf '%s' \"$HOME/.local/share/cli-agent-bridge/bin/agent-bridge\"",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if res.returncode == 0:
            candidate = res.stdout.strip().splitlines()[0].strip() if res.stdout.strip() else ""
            if candidate.startswith("/"):
                return candidate
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def _edit_hosts_menu(
    config: dict[str, Any],
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
    *,
    removed_hosts: set[str] | None = None,
) -> None:
    hosts: dict[str, Any] = config.setdefault("hosts", {})
    while True:
        output_fn(_('\n-- Remote Hosts (optional multi-machine dispatch) --'))
        if hosts:
            for hid, hval in hosts.items():
                pinned = _("pinned") if hval.get("pinned") else _("unpinned")
                output_fn(_('  * {hid}: ssh={value1}, exe={value2}, {pinned}', hid=hid, value1=hval.get('ssh_target'), value2=hval.get('remote_executable'), pinned=pinned))
        else:
            output_fn(_('  (none; standalone single-machine mode)'))
        choice = input_fn(_('Action ([a]dd/edit, [r]emove, [b]ack): ')).strip().lower()
        if choice in {"b", ""}:
            return
        if choice == "r":
            hid = _prompt(input_fn, output_fn, _('Host ID to remove'))
            if hid in hosts:
                del hosts[hid]
                if removed_hosts is not None:
                    removed_hosts.add(hid)
            continue
        if choice != "a":
            continue
        hid = _prompt(input_fn, output_fn, _('Host ID (lowercase alias, e.g. worker1)'), required=True)
        cur = hosts.get(hid, {}) if isinstance(hosts.get(hid), dict) else {}
        while True:
            raw_target = _prompt(
                input_fn, output_fn, _('SSH target (host alias or user@host)'), default=cur.get("ssh_target"), required=True
            )
            try:
                ssh_target = _validate_ssh_target(raw_target)
                break
            except ValueError as error:
                output_fn(_('  Invalid SSH target: {error}', error=_(str(error))))
        default_exe = cur.get("remote_executable") or ""
        ssh_opts = [opt for opt in cur.get("ssh_options", []) if isinstance(opt, str)]
        if _prompt_yes_no(input_fn, output_fn, _('Probe remote agent-bridge path over SSH now?'), default=False):
            probed = _probe_remote_executable(ssh_target, ssh_opts)
            if probed:
                output_fn(_('  Detected remote executable: {probed}', probed=probed))
                default_exe = probed
            else:
                output_fn(_('  SSH probe did not succeed; enter remote path manually.'))
        remote_exe = _prompt(
            input_fn, output_fn, _('Remote agent-bridge absolute path'), default=default_exe, required=True
        )
        remote_state = _prompt(
            input_fn,
            output_fn,
            _("Remote state directory (optional, '-' for default)"),
            default=cur.get("remote_state") or "",
        )
        if not cur and _prompt_yes_no(
            input_fn, output_fn, _('Apply recommended strict BatchMode SSH options?'), default=True
        ):
            ssh_opts = list(_DEFAULT_SSH_OPTIONS)
        entry: dict[str, Any] = {
            "ssh_target": ssh_target,
            "remote_executable": remote_exe,
            "ssh_options": ssh_opts,
        }
        if remote_state == "-":
            entry["remote_state"] = "-"
        elif remote_state:
            entry["remote_state"] = remote_state
        _apply_host_update(hosts, hid, entry)
        output_fn(_('Staged host {hid!r}.', hid=hid))


def _edit_service_ota_menu(
    state: Path,
    ota_plan: dict[str, Any],
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
) -> None:
    output_fn(_('\n-- Service Hosting & GitHub Releases OTA --'))
    default_kind = ota_plan.get("service_kind") or ("launchd" if sys.platform == "darwin" else "systemd")
    while True:
        kind = _prompt(input_fn, output_fn, _('service_kind (systemd/launchd)'), default=default_kind, required=True)
        if kind in {"systemd", "launchd"}:
            ota_plan["service_kind"] = kind
            break
        output_fn(_("  Must be 'systemd' or 'launchd'."))
    default_name = ota_plan.get("service_name") or (
        "com.cli-agent-bridge.broker" if ota_plan["service_kind"] == "launchd" else "cli-agent-bridge.service"
    )
    ota_plan["service_name"] = _prompt(input_fn, output_fn, _('Broker service name'), default=default_name, required=True)
    ota_plan["root"] = _prompt(
        input_fn,
        output_fn,
        _('Managed releases root directory'),
        default=ota_plan.get("root") or str(Path.home() / ".local/share/cli-agent-bridge"),
        required=True,
    )
    ota_plan["repository"] = _prompt(
        input_fn,
        output_fn,
        _('GitHub Releases repository (owner/name)'),
        default=ota_plan.get("repository") or REPOSITORY,
        required=True,
    )
    while True:
        raw_interval = _prompt(
            input_fn,
            output_fn,
            _('OTA check interval seconds (>=300)'),
            default=str(ota_plan.get("interval") or 3600),
            required=True,
        )
        try:
            interval = int(raw_interval)
            if interval >= 300:
                ota_plan["interval"] = interval
                break
        except ValueError:
            pass
        output_fn(_('  Must be an integer >= 300.'))
    ota_plan["install_broker_service"] = _prompt_yes_no(
        input_fn,
        output_fn,
        _('Install/update managed broker service unit on save?'),
        default=bool(ota_plan.get("install_broker_service", False)),
    )
    if ota_plan["install_broker_service"]:
        ota_plan["enable_broker_service"] = _prompt_yes_no(
            input_fn,
            output_fn,
            _('Enable & start/restart managed broker service now on save?'),
            default=bool(ota_plan.get("enable_broker_service", False)),
        )
    else:
        ota_plan["enable_broker_service"] = False
    ota_plan["install_updater"] = _prompt_yes_no(
        input_fn,
        output_fn,
        _('Write update.json, managed launchers & OTA timer on save?'),
        default=bool(ota_plan.get("install_updater", (state / "update.json").exists())),
    )
    if ota_plan["install_updater"]:
        ota_plan["enable_updater"] = _prompt_yes_no(
            input_fn,
            output_fn,
            _('Enable periodic GitHub OTA update timer on save?'),
            default=bool(ota_plan.get("enable_updater", False)),
        )
    else:
        ota_plan["enable_updater"] = False


def _print_summary(
    state: Path,
    config: dict[str, Any],
    env: dict[str, Any],
    ota_plan: dict[str, Any],
    output_fn: Callable[[str], None],
) -> None:
    clis = ", ".join(k for k, v in env["runtimes"].items() if v) or _("none")
    uv_status = env["tools"]["uv"] or _("missing")
    output_fn(_('\n=== cli-agent-bridge init ==='))
    output_fn(_('State dir   : {state}', state=state))
    output_fn(_('Detected    : CLIs=[{clis}], uv={uv_status}', clis=clis, uv_status=uv_status))
    output_fn(_('Profiles ({value0}): {value1}', value0=len(config.get('profiles', {})), value1=', '.join(config.get('profiles', {})) or _('(none)')))
    output_fn(
        _('Defaults    : launcher={value0}, parallel={value1}, quota={value2}, strategy={value3}', value0=config.get('worker_launcher'), value1=config.get('parallel_limit'), value2=config.get('quota_limit'), value3=config.get('default_strategy'))
    )
    output_fn(_('Projects ({value0}): {value1}', value0=len(config.get('projects', {})), value1=', '.join(config.get('projects', {})) or _('(none)')))
    output_fn(_('Hosts ({value0})   : {value1}', value0=len(config.get('hosts', {})), value1=', '.join(config.get('hosts', {})) or _('(standalone)')))
    ota_label = _("configured") if (state / "update.json").exists() or ota_plan.get("install_updater") else _("not configured")
    output_fn(_('GitHub OTA  : {ota_label} (repo={value1})', ota_label=ota_label, value1=ota_plan.get('repository')))


def _apply_cli_flags(
    config: dict[str, Any],
    ota_plan: dict[str, Any],
    args: Any,
    *,
    removed_hosts: set[str] | None = None,
) -> tuple[bool, bool]:
    """Apply explicit non-interactive/scripted CLI mutations. Returns (has_config_edits, has_ota_edits)."""
    config_edited = False
    routing_file = getattr(args, "routing_file", None)
    if routing_file is not None:
        try:
            routing = json.loads(routing_file.expanduser().read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise BridgeError("invalid_config", _("Could not read the routing JSON file")) from error
        config["routing"] = routing
        config_edited = True
    elif getattr(args, "clear_routing", False):
        config["routing"] = None
        config_edited = True
    language = getattr(args, "config_language", None)
    if language is not None:
        config["language"] = None if language == "none" else validate_language(language)
        config_edited = True
    for pname in getattr(args, "remove_profile", None) or ():
        config["profiles"].pop(pname, None)
        config_edited = True
    for spec in getattr(args, "profile", None) or ():
        parsed = _parse_kv_or_json(spec, required_keys=("name", "runtime", "model"), option_name="--profile")
        name = str(parsed.pop("name"))
        existing_prof = config["profiles"].get(name) if isinstance(config.get("profiles"), dict) else None
        if "provider" not in parsed:
            if isinstance(existing_prof, dict) and isinstance(existing_prof.get("provider"), str) and existing_prof["provider"]:
                parsed["provider"] = existing_prof["provider"]
            elif parsed["runtime"] in {"pi", "antigravity"}:
                raise ValueError(_('--profile {name!r} with runtime={value1} requires provider=...', name=name, value1=parsed['runtime']))
            else:
                parsed["provider"] = ""
        elif parsed["runtime"] in {"pi", "antigravity"} and not str(parsed.get("provider") or "").strip():
            raise ValueError(_('--profile {name!r} with runtime={value1} requires provider=...', name=name, value1=parsed['runtime']))
        _apply_profile_update(config["profiles"], name, parsed)
        config_edited = True
    for pid in getattr(args, "remove_project", None) or ():
        config["projects"].pop(pid, None)
        config_edited = True
    for spec in getattr(args, "project", None) or ():
        if "=" not in spec:
            raise ValueError(_('--project must be ID=PATH'))
        pid, ppath = spec.split("=", 1)
        pid = pid.strip()
        if not pid or not ppath.strip():
            raise ValueError(_('--project must be non-empty ID=PATH'))
        config["projects"][pid] = str(Path(ppath.strip()).expanduser().resolve())
        config_edited = True
    for hid in getattr(args, "remove_host", None) or ():
        config["hosts"].pop(hid, None)
        if removed_hosts is not None:
            removed_hosts.add(hid)
        config_edited = True
    for spec in getattr(args, "host", None) or ():
        parsed = _parse_kv_or_json(
            spec,
            required_keys=("id", "ssh_target", "remote_executable"),
            option_name="--host",
        )
        parsed["ssh_target"] = _validate_ssh_target(parsed["ssh_target"])
        hid = str(parsed.pop("id"))
        _apply_host_update(config["hosts"], hid, parsed)
        config_edited = True
    for arg_name, cfg_key in (
        ("worker_launcher", "worker_launcher"),
        ("parallel_limit", "parallel_limit"),
        ("quota_limit", "quota_limit"),
        ("default_strategy", "default_strategy"),
        ("task_timeout", "task_timeout"),
        ("interrupt_timeout", "interrupt_timeout"),
    ):
        val = getattr(args, arg_name, None)
        if val is not None:
            config[cfg_key] = val
            config_edited = True
    token_budget = getattr(args, "token_budget", None)
    if token_budget is not None:
        config["token_budget"] = None if str(token_budget).lower() == "none" else int(token_budget)
        config_edited = True
    for arg_name, cfg_key in (
        ("codex_async_python", "codex_async_python"),
        ("telegram_database", "telegram_database"),
    ):
        val = getattr(args, arg_name, None)
        if val is not None:
            config[cfg_key] = val or None
            config_edited = True

    ota_edited = False
    for arg_name, ota_key in (
        ("ota_repo", "repository"),
        ("ota_root", "root"),
        ("ota_release_url", "release_url"),
        ("service_kind", "service_kind"),
        ("service_name", "service_name"),
        ("ota_interval", "interval"),
    ):
        val = getattr(args, arg_name, None)
        if val is not None:
            if arg_name == "ota_root":
                val = str(val)
            elif arg_name == "ota_release_url" and val:
                val = validate_release_url(val)
            ota_plan[ota_key] = val
            if arg_name.startswith("ota_"):
                ota_plan["install_updater"] = True
            ota_edited = True
    if getattr(args, "install_broker_service", False) or getattr(args, "enable_broker_service", False):
        ota_plan["install_broker_service"] = True
        ota_edited = True
    if getattr(args, "enable_broker_service", False):
        ota_plan["enable_broker_service"] = True
        ota_edited = True
    if getattr(args, "force_broker_service", False):
        ota_plan["force_broker_service"] = True
        ota_edited = True
    if getattr(args, "install_updater", False) or getattr(args, "enable_updater", False):
        ota_plan["install_updater"] = True
        ota_edited = True
    if getattr(args, "enable_updater", False):
        ota_plan["enable_updater"] = True
        ota_edited = True
    if ota_edited and ota_plan.get("install_updater"):
        ota_plan["apply_updater"] = True
    enable_web = getattr(args, "enable_web", False)
    if getattr(args, "install_web", False) or enable_web:
        ota_plan["install_web"] = True
        ota_plan["enable_web"] = ota_plan.get("enable_web", False) or enable_web
        ota_edited = True
    return config_edited, ota_edited


def _edit_advanced_menu(
    config: dict[str, Any],
    ota_plan: dict[str, Any],
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
) -> None:
    output_fn(_("Press Enter to keep a setting. Type '-' to clear a text field."))
    language = _prompt(input_fn, output_fn, _("Default language (en/zh-CN/auto)"))
    routing = _prompt(input_fn, output_fn, _("Routing JSON file"))
    async_python = _prompt(input_fn, output_fn, _("codex-async Python path"))
    telegram = _prompt(input_fn, output_fn, _("Telegram database path"))
    mirror = _prompt(input_fn, output_fn, _("OTA release mirror URL"))
    web = _prompt(input_fn, output_fn, _("Managed Web (install/enable; Enter keeps current)"))
    if web not in {"", "install", "enable"}:
        raise ValueError(_("Managed Web must be 'install' or 'enable'"))
    if language and language != "-":
        language = validate_language(language)
    if mirror and mirror != "-":
        mirror = validate_release_url(mirror)
    _apply_cli_flags(config, ota_plan, argparse.Namespace(
        config_language="none" if language == "-" else language or None,
        routing_file=Path(routing) if routing and routing != "-" else None,
        clear_routing=routing == "-",
        codex_async_python="" if async_python == "-" else async_python or None,
        telegram_database="" if telegram == "-" else telegram or None,
        ota_release_url="" if mirror == "-" else mirror or None,
        install_web=web == "install",
        enable_web=web == "enable",
    ))


def _stale_pin_hosts(
    config: dict[str, Any],
    initial_host_targets: dict[str, str | None] | None = None,
    removed_hosts: set[str] | None = None,
    disk_config_hosts: dict[str, Any] | None = None,
) -> set[str]:
    stale: set[str] = set(removed_hosts or ())
    final_hosts = config.get("hosts", {}) if isinstance(config.get("hosts"), dict) else {}
    for baseline in (initial_host_targets or {}, disk_config_hosts or {}):
        for hid, old_entry in baseline.items():
            old_target = old_entry.get("ssh_target") if isinstance(old_entry, dict) else old_entry
            if hid not in final_hosts or not isinstance(final_hosts[hid], dict) or final_hosts[hid].get("ssh_target") != old_target:
                stale.add(hid)
    return stale


@contextlib.contextmanager
def _init_lock(state: Path):
    lock_path = state / ".init-config.lock"
    if lock_path.is_symlink():
        raise BridgeError("invalid_config", _('Refusing to lock symlinked init lock file'))
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise BridgeError("invalid_config", _('Init lock must be a private owned regular file: {lock_path}', lock_path=lock_path))
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _restore_file_snapshot(
    path: Path,
    *,
    existed: bool,
    data: bytes | None,
    mode: int | None,
) -> None:
    if not existed:
        path.unlink(missing_ok=True)
        return
    if data is None:
        return
    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=path.parent,
        prefix=f".{path.name}-rollback-",
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)
        os.chmod(tmp_path, mode if mode is not None else 0o600)
        tmp.write(data)
        tmp.flush()
        os.fsync(tmp.fileno())
    try:
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _validate_state_dir(state: Path) -> None:
    if state.is_symlink():
        raise BridgeError("invalid_config", _('State directory must not be a symlink: {state}', state=state))
    if state.exists():
        info = state.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise BridgeError("invalid_config", _('State directory must be private and owned: {state}', state=state))
        if not os.access(state, os.W_OK | os.X_OK):
            raise BridgeError("invalid_config", _('State directory is not writable: {state}', state=state))
    else:
        cur = state.parent
        while not cur.exists() and not cur.is_symlink() and cur != cur.parent:
            cur = cur.parent
        if cur.is_symlink() or not cur.is_dir() or not os.access(cur, os.W_OK | os.X_OK):
            raise BridgeError("invalid_config", _('Cannot create state directory under {cur}', cur=cur))


def _validate_all_changes(
    state: Path,
    config: dict[str, Any],
    ota_plan: dict[str, Any],
    *,
    stale_pin_hosts: set[str] | None = None,
) -> None:
    """Validate configuration, pin cleanup, and all planned broker/updater service mutations before writing disk."""
    _validate_state_dir(state)
    _validate_config_payload(state, config)
    _prepare_host_pins_update(state, set(stale_pin_hosts or ()))
    root = Path(ota_plan["root"])
    kind = ota_plan["service_kind"]
    name = ota_plan["service_name"]
    will_install_broker = bool(ota_plan.get("install_broker_service"))
    if will_install_broker:
        validate_broker_service_install(
            state,
            root=root,
            service_kind=kind,
            service_name=name,
            enable=bool(ota_plan.get("enable_broker_service")),
            force=bool(ota_plan.get("force_broker_service")),
        )
    will_apply_updater = bool(ota_plan.get("install_updater")) and (
        bool(ota_plan.get("apply_updater")) or not (state / "update.json").exists()
    )
    update_settings = None
    if will_apply_updater:
        _, _, _, update_settings = validate_update_install(
            state,
            repository=ota_plan["repository"],
            service_kind=kind,
            service_name=name,
            root=root,
            interval=int(ota_plan["interval"]),
            update_existing=True,
            release_url=ota_plan.get("release_url"),
        )
        if ota_plan.get("enable_updater") and not will_install_broker and not Updater(state).service_managed():
            raise ValueError(_('Migrate the broker service to its managed launcher before enabling updates'))
    if ota_plan.get("install_web"):
        if update_settings is None:
            if not (state / "update.json").exists():
                raise ValueError(_("Set up the managed updater before installing Web"))
            update_settings = load_settings(state)
        validate_web_service_install(
            state, update_settings, require_launcher=not (will_apply_updater or will_install_broker),
        )


def _commit_changes(
    state: Path,
    config: dict[str, Any],
    ota_plan: dict[str, Any],
    *,
    initial_host_targets: dict[str, str | None] | None = None,
    removed_hosts: set[str] | None = None,
    initial_config_bytes: bytes | None = None,
    check_concurrent_config: bool = False,
) -> dict[str, Any]:
    initial_stale = _stale_pin_hosts(config, initial_host_targets, removed_hosts)
    _validate_all_changes(state, config, ota_plan, stale_pin_hosts=initial_stale)
    state.mkdir(parents=True, mode=0o700, exist_ok=True)
    with _init_lock(state):
        config_path = state / "config.json"
        if config_path.is_symlink():
            raise BridgeError("invalid_config", _('Refusing to overwrite symlinked config.json'))
        prev_existed = config_path.exists()
        prev_bytes = config_path.read_bytes() if prev_existed else None
        if check_concurrent_config and prev_bytes != initial_config_bytes:
            raise BridgeError(
                "config_conflict",
                _('config.json was modified concurrently on disk; reload and re-apply your changes'),
            )
        disk_hosts: dict[str, Any] = {}
        if prev_bytes is not None:
            try:
                loaded_disk = json.loads(prev_bytes.decode("utf-8"))
                if isinstance(loaded_disk, dict) and isinstance(loaded_disk.get("hosts"), dict):
                    disk_hosts = loaded_disk["hosts"]
            except (OSError, ValueError):
                pass
        stale_pin_hosts = _stale_pin_hosts(config, initial_host_targets, removed_hosts, disk_hosts)
        _validate_all_changes(state, config, ota_plan, stale_pin_hosts=stale_pin_hosts)

        prev_mode = stat.S_IMODE(config_path.lstat().st_mode) if prev_existed else None

        applied: list[str] = []

        def _record_applied(item: str) -> None:
            if item not in applied:
                applied.append(item)

        config_changed = False
        try:
            config_changed = _write_private_json_if_changed(config_path, config)
            if config_changed:
                _record_applied("config.json")
            try:
                if _clear_persisted_host_pins(state, stale_pin_hosts):
                    _record_applied("host_pins.json")
            except BaseException:
                if config_changed:
                    _restore_file_snapshot(
                        config_path,
                        existed=prev_existed,
                        data=prev_bytes,
                        mode=prev_mode,
                    )
                    if "config.json" in applied:
                        applied.remove("config.json")
                raise

            result: dict[str, Any] = {
                "status": "configured",
                "config": str(config_path),
                "config_changed": config_changed,
                "profiles": sorted(config.get("profiles", {})),
                "hosts": sorted(config.get("hosts", {})),
                "projects": sorted(config.get("projects", {})),
            }
            if stale_pin_hosts:
                result["cleared_host_pins"] = sorted(stale_pin_hosts)
            root = Path(ota_plan["root"])
            kind = ota_plan["service_kind"]
            name = ota_plan["service_name"]
            if ota_plan.get("install_broker_service"):
                result["broker_service"] = install_broker_service(
                    state,
                    root=root,
                    service_kind=kind,
                    service_name=name,
                    enable=bool(ota_plan.get("enable_broker_service")),
                    force=bool(ota_plan.get("force_broker_service")),
                    on_write=_record_applied,
                )
                _record_applied("broker_service")
            if ota_plan.get("install_updater") and (
                ota_plan.get("apply_updater") or not (state / "update.json").exists()
            ):
                result["updater"] = install(
                    state,
                    repository=ota_plan["repository"],
                    service_kind=kind,
                    service_name=name,
                    root=root,
                    interval=int(ota_plan["interval"]),
                    enable=bool(ota_plan.get("enable_updater")),
                    update_existing=True,
                    on_write=_record_applied,
                    release_url=ota_plan.get("release_url"),
                )
                _record_applied("updater")
            elif (state / "update.json").exists():
                with contextlib.suppress(OSError, ValueError):
                    Updater(state).service_managed()
            if ota_plan.get("install_web"):
                result["web_service"] = install_web_service(
                    state, enable=bool(ota_plan.get("enable_web")), on_write=_record_applied,
                )
        except KeyboardInterrupt as error:
            if applied:
                raise BridgeError(
                    "init_interrupted",
                    _('Interrupted while saving changes (already saved: {value0})', value0=', '.join(applied)),
                ) from error
            raise
        except (BridgeError, OSError, ValueError, RuntimeError) as error:
            if applied:
                raise BridgeError(
                    "init_partial_failure",
                    _('{error} (already saved: {value1})', error=_(str(error)), value1=', '.join(applied)),
                ) from error
            raise
        return result


def add_init_arguments(parser: argparse.ArgumentParser) -> None:
    """Register interactive and explicit flags on the `agent-bridge init` subparser."""
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help=_('Apply passed flags or validate existing configuration without interactive prompts'),
    )
    parser.add_argument(
        "--profile",
        action="append",
        metavar="SPEC",
        help=_('Add/update a runtime profile (name=...,runtime=...,provider=...,model=...[,effort=...] or JSON)'),
    )
    parser.add_argument("--remove-profile", action="append", metavar="NAME", help=_('Remove an existing profile'))
    parser.add_argument("--worker-launcher", choices=["systemd", "detached"])
    parser.add_argument("--parallel-limit", type=int)
    parser.add_argument("--quota-limit", type=int)
    parser.add_argument("--default-strategy", choices=["balanced", "cost", "speed"])
    parser.add_argument("--task-timeout", type=float)
    parser.add_argument("--interrupt-timeout", type=float)
    parser.add_argument("--token-budget", help=_("Positive integer token budget, or 'none' to clear"))
    parser.add_argument(
        "--config-language", choices=["en", "zh-CN", "auto", "none"],
        help=_("Persistent default language ('none' restores the default)"),
    )
    routing = parser.add_mutually_exclusive_group()
    routing.add_argument("--routing-file", type=Path, help=_("Replace routing rules from a JSON file"))
    routing.add_argument("--clear-routing", action="store_true", help=_("Clear configured routing rules"))
    parser.add_argument("--project", action="append", metavar="ID=PATH", help=_('Add/update a local project root'))
    parser.add_argument("--remove-project", action="append", metavar="ID", help=_('Remove a project mapping'))
    parser.add_argument(
        "--host",
        action="append",
        metavar="SPEC",
        help=_('Add/update a remote host (id=...,ssh_target=...,remote_executable=...[,remote_state=...] or JSON)'),
    )
    parser.add_argument("--remove-host", action="append", metavar="ID", help=_('Remove a registered remote host'))
    parser.add_argument("--codex-async-python", help=_("Optional codex-async Python path (pass '' to clear)"))
    parser.add_argument("--telegram-database", help=_("Optional telegram.sqlite path (pass '' to clear)"))
    parser.add_argument("--ota-repo", help=_('GitHub repository owner/name for OTA updates'))
    parser.add_argument("--ota-release-url", help=_("OTA release mirror URL (pass '' to use GitHub)"))
    parser.add_argument("--ota-root", type=Path, help=_('Managed OTA releases directory'))
    parser.add_argument("--ota-interval", type=int, help=_('OTA update check interval in seconds (>=300)'))
    parser.add_argument("--service-kind", choices=["systemd", "launchd"])
    parser.add_argument("--service-name")
    parser.add_argument("--install-broker-service", action="store_true")
    parser.add_argument("--enable-broker-service", action="store_true")
    parser.add_argument("--force-broker-service", action="store_true")
    parser.add_argument("--install-updater", action="store_true")
    parser.add_argument("--enable-updater", action="store_true")
    parser.add_argument("--install-web", action="store_true", help=_("Install the managed loopback Web service"))
    parser.add_argument("--enable-web", action="store_true", help=_("Install and start the managed loopback Web service"))


def run_init(
    state: Path,
    args: Any,
    *,
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], None] = print,
    is_tty: bool | None = None,
) -> dict[str, Any]:
    """Load existing configuration if present, edit idempotently, validate, and save."""
    env = detect_environment()
    config, existed = _load_existing_config(state)
    config_path = state / "config.json"
    initial_config_bytes = config_path.read_bytes() if existed and config_path.exists() else None
    initial_host_targets: dict[str, str | None] = {
        hid: hcfg.get("ssh_target")
        for hid, hcfg in (config.get("hosts", {}) or {}).items()
        if isinstance(hcfg, dict)
    }
    removed_hosts: set[str] = set()
    existing_update = _load_existing_update(state)
    default_kind = (existing_update or {}).get("service_kind") or ("launchd" if sys.platform == "darwin" else "systemd")
    default_name = (existing_update or {}).get("service_name") or (
        "com.cli-agent-bridge.broker" if default_kind == "launchd" else "cli-agent-bridge.service"
    )
    ota_plan: dict[str, Any] = {
        "repository": (existing_update or {}).get("repository", REPOSITORY),
        "root": (existing_update or {}).get("root", str(Path.home() / ".local/share/cli-agent-bridge")),
        "service_kind": default_kind,
        "service_name": default_name,
        "interval": int((existing_update or {}).get("interval", 3600)),
        "install_broker_service": False,
        "enable_broker_service": False,
        "force_broker_service": False,
        "install_updater": existing_update is not None,
        "enable_updater": False,
        "apply_updater": False,
    }
    config_edited, ota_edited = _apply_cli_flags(config, ota_plan, args, removed_hosts=removed_hosts)
    interactive = (not getattr(args, "non_interactive", False)) and (
        sys.stdin.isatty() and sys.stdout.isatty() if is_tty is None else is_tty
    )
    if not interactive:
        if not existed and not config_edited and not ota_edited:
            raise BridgeError(
                "init_requires_manual_config",
                _('Initial configuration requires manual specification: run `agent-bridge init` in an interactive terminal or pass explicit configuration flags such as --profile.'),
            )
        return _commit_changes(
            state,
            config,
            ota_plan,
            initial_host_targets=initial_host_targets,
            removed_hosts=removed_hosts,
            initial_config_bytes=initial_config_bytes,
            check_concurrent_config=True,
        )

    commit_ready = False
    try:
        while True:
            _print_summary(state, config, env, ota_plan, output_fn)
            output_fn(_('1) Profiles  2) Runtime defaults  3) Projects  4) Remote hosts  5) Services & OTA'))
            output_fn(_('6) Advanced settings'))
            choice = input_fn(_('Select [1-6], [s]ave & apply, or [q]uit without saving: ')).strip().lower()
            if choice == "1":
                _edit_profiles_menu(config, env, input_fn, output_fn)
            elif choice == "2":
                _edit_runtime_defaults_menu(config, input_fn, output_fn)
            elif choice == "3":
                _edit_projects_menu(config, input_fn, output_fn)
            elif choice == "4":
                _edit_hosts_menu(config, input_fn, output_fn, removed_hosts=removed_hosts)
            elif choice == "5":
                _edit_service_ota_menu(state, ota_plan, input_fn, output_fn)
                ota_plan["apply_updater"] = bool(ota_plan.get("install_updater"))
            elif choice == "6":
                try:
                    _edit_advanced_menu(config, ota_plan, input_fn, output_fn)
                except (BridgeError, OSError, ValueError) as error:
                    output_fn(_('Configuration validation error: {error}', error=_(str(error))))
            elif choice == "q":
                return {"status": "aborted", "config": str(state / "config.json"), "config_changed": False}
            elif choice == "s":
                stale_pins = _stale_pin_hosts(config, initial_host_targets, removed_hosts)
                try:
                    _validate_all_changes(state, config, ota_plan, stale_pin_hosts=stale_pins)
                except (BridgeError, OSError, ValueError) as error:
                    output_fn(_('Configuration validation error: {error}', error=_(str(error))))
                    continue
                if not config.get("profiles") and not _prompt_yes_no(
                    input_fn, output_fn, _('No profiles configured yet. Save anyway?'), default=False
                ):
                    continue
                commit_ready = True
                break
            else:
                output_fn(_('  Invalid selection.'))
    except (EOFError, KeyboardInterrupt):
        output_fn(_('\nAborted without saving.'))
        return {"status": "aborted", "config": str(state / "config.json"), "config_changed": False}

    assert commit_ready
    return _commit_changes(
        state,
        config,
        ota_plan,
        initial_host_targets=initial_host_targets,
        removed_hosts=removed_hosts,
        initial_config_bytes=initial_config_bytes,
        check_concurrent_config=True,
    )
