"""Install checked GitHub wheels beside existing code; replace only an idle broker."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from xml.sax.saxutils import escape

from .errors import BridgeError

REPOSITORY = "qinjian623/cli-agent-bridge"
TAG = re.compile(r"v(\d+)\.(\d+)\.(\d+)\Z")
NAME = re.compile(r"[A-Za-z0-9_.-]+\Z")


class UpdateUncertain(RuntimeError):
    """A service command may still be running; do not repeat it or undo its files."""


def _private(path: Path, *, directory=False):
    info = path.lstat()
    import stat

    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if not kind(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError(f"Update {'directory' if directory else 'file'} must be private and owned: {path}")


def _write(path: Path, value):
    data = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False, prefix=".update-") as output:
        temporary = Path(output.name)
        os.chmod(temporary, 0o600)
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _run(argv, *, timeout=120):
    # Do not persist stderr or the environment: gh may include credential diagnostics.
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        raise UpdateUncertain("Command timed out; its outcome must be checked before retrying") from error
    if result.returncode:
        raise RuntimeError(f"{Path(argv[0]).name} command failed (exit {result.returncode})")
    return result.stdout


def _children(pid):
    """Return executable names, never prompts or command arguments."""
    if sys.platform == "linux":
        pending, rows = [pid], []
        while pending:
            parent = pending.pop()
            try:
                values = Path(f"/proc/{parent}/task/{parent}/children").read_text().split()
            except FileNotFoundError:
                continue
            for value in values:
                child = int(value)
                try:
                    if Path(f"/proc/{child}/stat").read_text().rsplit(") ", 1)[1].split()[0] == "Z":
                        continue
                    rows.append(Path(f"/proc/{child}/comm").read_text().strip())
                except FileNotFoundError:
                    continue
                pending.append(child)
        return rows
    with subprocess.Popen(
        ["ps", "-ww", "-axo", "pid=,ppid=,stat=,comm="],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    ) as sampler:
        try:
            output, _ = sampler.communicate(timeout=5)
        except subprocess.TimeoutExpired as error:
            sampler.kill()
            sampler.communicate()
            raise UpdateUncertain("Command timed out; its outcome must be checked before retrying") from error
        except BaseException:
            sampler.kill()
            raise
        if sampler.returncode:
            raise RuntimeError(f"ps command failed (exit {sampler.returncode})")
    rows = {}
    for line in output.splitlines():
        child, parent, status, command = line.strip().split(None, 3)
        if int(child) == sampler.pid or status.startswith("Z"):
            continue
        rows.setdefault(int(parent), []).append((int(child), Path(command).name))
    pending, names = [pid], []
    while pending:
        for child, name in rows.get(pending.pop(), []):
            names.append(name)
            pending.append(child)
    return names


def busy_snapshot(state: Path, broker_pid: int | None = None):
    """Read only. Paused historical queues are preserved and do not prevent updates."""
    from .worker import process_identity

    counts, reasons = {}, []
    queries = {
        "open_queued_tasks": "SELECT COUNT(*) FROM tasks t JOIN sessions s ON s.id=t.session_id WHERE t.status='queued' AND s.desired='open'",
        "active_tasks": "SELECT COUNT(*) FROM tasks WHERE status IN ('assigned','running','waiting_input')",
        "api_inflight": "SELECT COUNT(*) FROM api_requests WHERE response IS NULL",
        "notification_sending": "SELECT COUNT(*) FROM deliveries WHERE status='sending'",
        "native_broadcast_sending": "SELECT COUNT(*) FROM native_broadcast_deliveries WHERE status='sending' OR processing_status='running'",
        "native_queue_sending": "SELECT COUNT(*) FROM native_send_queue WHERE status='sending' OR processing_status='running'",
        "peer_sending": "SELECT COUNT(*) FROM peer_outbox WHERE status='sending'",
    }
    try:
        with contextlib.closing(sqlite3.connect(f"file:{state / 'bridge.sqlite'}?mode=ro", uri=True, timeout=2)) as db:
            db.execute("BEGIN")
            counts = {name: db.execute(query).fetchone()[0] for name, query in queries.items()}
            workers = db.execute("SELECT worker_pid,worker_start FROM sessions WHERE worker_pid IS NOT NULL").fetchall()
            counts["live_workers"] = sum(
                bool(identity and (not expected or identity == expected))
                for pid, expected in workers
                if (identity := process_identity(pid))
            )
        reasons.extend(name for name, count in counts.items() if count)
        if broker_pid is not None:
            counts["broker_helpers"] = sum(name != "ssh" for name in _children(broker_pid))
            if counts["broker_helpers"]:
                reasons.append("broker_helpers")
    except (OSError, ValueError, sqlite3.Error, RuntimeError):
        reasons.append("update_state_unknown")
    return {"busy": bool(reasons), "reasons": reasons, "counts": counts}


def load_settings(state: Path):
    path = state / "update.json"
    _private(path)
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or set(value) != {
        "repository",
        "root",
        "service_kind",
        "service_name",
        "bootstrap_python",
    }:
        raise ValueError("Invalid update settings")
    if any(not isinstance(item, str) for item in value.values()):
        raise ValueError("Update settings must be strings")
    repo = value["repository"]
    if not isinstance(repo, str) or len(repo.split("/")) != 2 or not all(NAME.fullmatch(x) for x in repo.split("/")):
        raise ValueError("Invalid GitHub repository")
    if value["service_kind"] not in {"systemd", "launchd"} or not NAME.fullmatch(value["service_name"]):
        raise ValueError("Invalid update service")
    for key in ("root", "bootstrap_python"):
        if (
            not isinstance(value[key], str)
            or not Path(value[key]).is_absolute()
            or "\x00" in value[key]
            or "\n" in value[key]
        ):
            raise ValueError(f"Invalid {key}")
    _private(Path(value["root"]), directory=True)
    return value


def latest_release(repository):
    data = json.loads(
        _run(["gh", "api", "--hostname", "github.com", f"repos/{repository}/releases/latest"], timeout=30)
    )
    if not isinstance(data, dict) or type(data.get("id")) is not int or data["id"] <= 0:
        raise ValueError("Invalid GitHub release response")
    tag = data.get("tag_name")
    if not isinstance(tag, str) or not TAG.fullmatch(tag) or data.get("draft") or data.get("prerelease"):
        raise ValueError("Only stable vX.Y.Z releases can be installed automatically")
    wheel = f"cli_agent_bridge-{tag[1:]}-py3-none-any.whl"
    assets = data.get("assets")
    if not isinstance(assets, list) or any(not isinstance(a, dict) for a in assets):
        raise ValueError("Release assets are missing")
    for name, limit in ((wheel, 100_000_000), ("SHA256SUMS", 65_536)):
        matches = [a for a in assets if a.get("name") == name]
        if len(matches) != 1 or type(matches[0].get("size")) is not int or not 0 < matches[0]["size"] <= limit:
            raise ValueError("Release must contain exactly one bounded wheel and SHA256SUMS")
    return {"tag": tag, "wheel": wheel, "release_id": data["id"]}


def verify_wheel(directory: Path, filename: str):
    wheel, checksums = directory / filename, directory / "SHA256SUMS"
    if (
        wheel.is_symlink()
        or checksums.is_symlink()
        or not 0 < wheel.stat().st_size <= 100_000_000
        or checksums.stat().st_size > 65_536
    ):
        raise ValueError("Invalid release asset")
    matches = []
    for line in checksums.read_text().splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1] == filename:
            matches.append(fields[0])
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    if len(matches) != 1 or not re.fullmatch(r"[0-9a-fA-F]{64}", matches[0]) or digest != matches[0].lower():
        raise ValueError("Wheel SHA256 mismatch or ambiguous checksum")
    return digest


def _version(tag):
    match = TAG.fullmatch(tag)
    if not match:
        raise ValueError("Invalid managed release version")
    return tuple(map(int, match.groups()))


class Updater:
    def __init__(self, state: Path):
        self.state = state
        self.settings = load_settings(state)
        self.root = Path(self.settings["root"])
        self.current = self.root / "current"

    def installed(self):
        if not self.current.is_symlink():
            if self.current.exists():
                raise ValueError("Managed current must be a symlink")
            return None
        target = self.current.resolve(strict=True)
        if target.parent != (self.root / "releases").resolve():
            raise ValueError("Current version is outside the managed releases directory")
        _private(target, directory=True)
        data = json.loads((target / "release.json").read_text())
        _version(data["tag"])
        return data

    def check(self):
        release = latest_release(self.settings["repository"])
        installed = self.installed()
        return {
            "available": release,
            "installed": installed,
            "update_available": not installed or _version(release["tag"]) > _version(installed["tag"]),
        }

    def _record(self, result):
        path = self.state / "update-status.json"
        if result.get("status") == "uncertain" and path.exists():
            previous = json.loads(path.read_text())
            for key in ("target_version", "previous_version", "candidate", "previous"):
                if key in previous:
                    result.setdefault(key, previous[key])
        _write(path, {**result, "checked_at": time.time()})
        return result

    async def _actual(self):
        from .broker import request

        async with asyncio.timeout(2):
            return await request(self.state, "agent_update_status", {})

    def _reconcile(self, previous):
        """Inspect an interrupted switch; never automatically repeat its restart."""
        try:
            actual = asyncio.run(self._actual()) if previous.get("target_version") else {}
        except (OSError, TimeoutError, BridgeError):
            actual = {}
        if (
            actual.get("version") == previous.get("target_version")
            and actual.get("version")
            and not actual.get("maintenance")
        ):
            return self._record({"status": "updated", "version": actual["version"], "recovered": True})
        return self._record({"status": "uncertain", "reason": "previous_service_command_needs_checking"})

    def bootstrap(self):
        """Stage the first version without stopping or reconfiguring an existing service."""
        with self._locked():
            check = self.check()
            if not check["update_available"]:
                return {"status": "up_to_date", "release_dir": str(self.current.resolve())}
            target = self.stage(check["available"])
            return {"status": "staged", "release_dir": str(target), "activated": False}

    @contextlib.contextmanager
    def _locked(self):
        descriptor = os.open(self.root / "update.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            _private(self.root / "update.lock")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(descriptor)

    def stage(self, release):
        releases = self.root / "releases"
        releases.mkdir(mode=0o700, exist_ok=True)
        _private(releases, directory=True)
        target = releases / (release["tag"] + "-" + uuid.uuid4().hex[:12])
        target.mkdir(mode=0o700)
        try:
            with tempfile.TemporaryDirectory(prefix="download-", dir=self.root) as name:
                download = Path(name)
                _run(
                    [
                        "gh",
                        "release",
                        "download",
                        release["tag"],
                        "--repo",
                        "github.com/" + self.settings["repository"],
                        "--dir",
                        str(download),
                        "--pattern",
                        release["wheel"],
                        "--pattern",
                        "SHA256SUMS",
                    ],
                    timeout=120,
                )
                digest = verify_wheel(download, release["wheel"])
                _run(["uv", "venv", "--python", self.settings["bootstrap_python"], str(target)], timeout=120)
                _run(
                    ["uv", "pip", "install", "--python", str(target / "bin/python"), str(download / release["wheel"])],
                    timeout=300,
                )
            self.preflight(target, release["tag"])
            skill = Path(
                _run(
                    [
                        str(target / "bin/python"),
                        "-c",
                        "from pathlib import Path; import agent_bridge; print(Path(agent_bridge.__file__).parent / 'skill')",
                    ],
                    timeout=10,
                ).strip()
            )
            if not skill.is_dir() or not skill.resolve().is_relative_to(target.resolve()):
                raise ValueError("Release wheel is missing its packaged skill")
            (target / "skill").symlink_to(skill.relative_to(target), target_is_directory=True)
            _write(target / "release.json", {**release, "sha256": digest})
            return target
        except BaseException:
            shutil.rmtree(target)
            raise

    def preflight(self, target: Path, tag: str):
        python = str(target / "bin/python")
        code = "import importlib.metadata,sys; assert importlib.metadata.version('cli-agent-bridge')==sys.argv[1]"
        _run([python, "-c", code, tag[1:]], timeout=15)
        _run([python, "-m", "agent_bridge.cli", "--state", str(self.state), "doctor"], timeout=30)
        # No production database copy and no real hosts, credentials, sessions or inference.
        with tempfile.TemporaryDirectory(prefix="selftest-", dir=self.root) as name:
            test = Path(name)
            _write(test / "config.json", {"profiles": {}, "hosts": {}, "telegram_database": None})
            process = subprocess.Popen(
                [python, "-m", "agent_bridge.cli", "--state", str(test), "serve"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                asyncio.run(self._healthy(test, expected_version=tag[1:], process=process))
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()

    @staticmethod
    async def _healthy(state: Path, expected_version=None, process=None):
        from .broker import request

        end = time.monotonic() + 10
        while time.monotonic() < end:
            if process is not None and process.poll() is not None:
                raise RuntimeError("Candidate broker self-test exited")
            try:
                async with asyncio.timeout(1):
                    result = await request(state, "agent_update_status", {})
                if expected_version is not None and result.get("version") != expected_version:
                    raise RuntimeError("Broker started a different release version")
                if process is not None and result.get("pid") != process.pid:
                    raise RuntimeError("Candidate self-test reached a different broker process")
                return
            except (OSError, TimeoutError, BridgeError):
                await asyncio.sleep(0.1)
        raise RuntimeError("Candidate broker did not become healthy")

    def _switch(self, target: Path | None):
        if target is None:
            self.current.unlink(missing_ok=True)
            return
        temporary = self.root / (".current-" + uuid.uuid4().hex)
        temporary.symlink_to(target)
        try:
            os.replace(temporary, self.current)
        finally:
            temporary.unlink(missing_ok=True)

    def _restart(self):
        service = self.settings["service_name"]
        command = (
            ["systemctl", "--user", "restart", service]
            if self.settings["service_kind"] == "systemd"
            else ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{service}"]
        )
        _run(command, timeout=45)

    def service_managed(self):
        """Changing current has no effect until the service uses our stable launcher."""
        launcher = str(self.root / "bin/agent-bridge")
        try:
            if self.settings["service_kind"] == "systemd":
                value = _run(
                    ["systemctl", "--user", "show", self.settings["service_name"], "-p", "ExecStart", "--value"],
                    timeout=10,
                )
                return re.search(r"(?:^|[ ;{])path=" + re.escape(launcher) + r"\s*;", value) is not None
            value = _run(["launchctl", "print", f"gui/{os.getuid()}/{self.settings['service_name']}"], timeout=10)
            return any(line.strip() == "program = " + launcher for line in value.splitlines())
        except (OSError, RuntimeError):
            return False

    async def _activate(self, target: Path, config_digest: str):
        from .broker import request

        try:
            lease = await request(self.state, "agent_update_prepare", {})
        except (OSError, TimeoutError):
            shutil.rmtree(target)
            return {"status": "deferred", "reason": "broker_unavailable"}
        token = lease.get("token")
        if not isinstance(token, str) or not token or lease.get("expires_at", 0) < time.time() + 60:
            raise RuntimeError("Broker returned an invalid or too short maintenance lease")
        previous = self.current.resolve() if self.current.is_symlink() else None
        remove_candidate = True
        try:
            if hashlib.sha256((self.state / "config.json").read_bytes()).hexdigest() != config_digest:
                return {"status": "deferred", "reason": "config_changed"}
            snapshot = busy_snapshot(self.state, broker_pid=lease.get("broker_pid"))
            if snapshot["busy"]:
                return {"status": "deferred", **snapshot}
            target_version = json.loads((target / "release.json").read_text())["tag"][1:]
            previous_version = json.loads((previous / "release.json").read_text())["tag"][1:] if previous else None
            self._record(
                {
                    "status": "activating",
                    "target_version": target_version,
                    "previous_version": previous_version,
                    "candidate": str(target),
                    "previous": str(previous) if previous else None,
                }
            )
            remove_candidate = False
            self._switch(target)
            try:
                self._restart()
                await self._healthy(self.state, expected_version=target_version)
            except UpdateUncertain:
                raise
            except (RuntimeError, OSError, BridgeError):
                self._switch(previous)
                try:
                    self._restart()
                    await self._healthy(self.state, expected_version=previous_version)
                except (RuntimeError, OSError, BridgeError) as error:
                    raise UpdateUncertain("Rollback health could not be confirmed") from error
                remove_candidate = True
                return {"status": "rolled_back", "reason": "new_broker_failed_health_check"}
            return {"status": "updated", "release": target.name}
        finally:
            # A timed-out restart may still be running; leave its lease to expire rather than admitting new work.
            if sys.exc_info()[0] not in {UpdateUncertain, KeyboardInterrupt, SystemExit}:
                try:
                    finished = await request(self.state, "agent_update_finish", {"token": token})
                    if finished.get("maintenance"):
                        raise RuntimeError("Maintenance was not released")
                except (OSError, TimeoutError, BridgeError, RuntimeError) as error:
                    raise UpdateUncertain("Maintenance release could not be confirmed") from error
            if remove_candidate:
                shutil.rmtree(target)

    def run(self):
        try:
            with self._locked():
                status_path = self.state / "update-status.json"
                if status_path.exists():
                    previous_status = json.loads(status_path.read_text())
                    if previous_status.get("status") in {"activating", "uncertain"}:
                        return self._reconcile(previous_status)
                if not self.service_managed():
                    return self._record({"status": "deferred", "reason": "broker_service_not_migrated"})
                check = self.check()
                if not check["update_available"]:
                    try:
                        actual = asyncio.run(self._actual())
                    except (OSError, TimeoutError, BridgeError):
                        return self._record({"status": "deferred", "reason": "broker_unavailable"})
                    if actual.get("version") != check["installed"]["tag"][1:]:
                        return self._record(
                            {
                                "status": "uncertain",
                                "reason": "running_version_differs_from_current",
                                "target_version": check["installed"]["tag"][1:],
                            }
                        )
                    return self._record({"status": "up_to_date", "version": check["installed"]["tag"]})
                snapshot = busy_snapshot(self.state)
                if snapshot["busy"]:
                    return self._record({"status": "deferred", **snapshot})
                digest = hashlib.sha256((self.state / "config.json").read_bytes()).hexdigest()
                target = self.stage(check["available"])
                try:
                    result = asyncio.run(self._activate(target, digest))
                except BridgeError as error:
                    shutil.rmtree(target)
                    if error.code in {"update_busy", "unknown_method", "method_not_found"}:
                        result = {"status": "deferred", "reason": error.code}
                    else:
                        raise
                except UpdateUncertain:
                    result = {"status": "uncertain", "reason": "service_command_timed_out"}
                return self._record(result)
        except BlockingIOError:
            return {"status": "deferred", "reason": "another_updater"}


def _launcher(root: Path, bootstrap: Path, module: str):
    return (
        "#!/bin/sh\nset -eu\n"
        + f"root={shlex.quote(str(root))}\n"
        + 'if [ -x "$root/current/bin/python" ]; then\n'
        + '  release=$(CDPATH= cd -- "$root/current" && pwd -P)\n'
        + f'  exec "$release/bin/python" -m {module} "$@"\nfi\n'
        + f'exec {shlex.quote(str(bootstrap))} -m {module} "$@"\n'
    )


def install(
    state: Path, repository=REPOSITORY, service_kind=None, service_name=None, root=None, interval=3600, enable=False
):
    """Write launchers and timer only; migrating an existing broker service is a separate, deliberate step."""
    if type(interval) is not int or interval < 300:
        raise ValueError("Update interval must be at least 300 seconds")
    root = Path(root) if root else Path.home() / ".local/share/cli-agent-bridge"
    root = root.expanduser().absolute()
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    state.mkdir(parents=True, mode=0o700, exist_ok=True)
    _private(root, directory=True)
    _private(state, directory=True)
    bootstrap = Path(sys.executable).parent.resolve() / Path(sys.executable).name
    kind = service_kind or ("launchd" if sys.platform == "darwin" else "systemd")
    name = service_name or ("com.cli-agent-bridge.broker" if kind == "launchd" else "cli-agent-bridge.service")
    settings = {
        "repository": repository,
        "root": str(root),
        "service_kind": kind,
        "service_name": name,
        "bootstrap_python": str(bootstrap),
    }
    path = state / "update.json"
    if path.exists() and json.loads(path.read_text()) != settings:
        raise ValueError("Existing update settings differ; edit the private config explicitly")
    _write(path, settings)
    load_settings(state)
    binary = root / "bin"
    binary.mkdir(mode=0o700, exist_ok=True)
    for filename, module in (("agent-bridge", "agent_bridge.cli"), ("agent-bridge-update", "agent_bridge.updater")):
        command = binary / filename
        if command.exists() and command.read_text() != _launcher(root, bootstrap, module):
            raise ValueError("Refusing to overwrite an unrelated managed launcher")
        _write(command, _launcher(root, bootstrap, module))
        command.chmod(0o700)
    if enable and not Updater(state).service_managed():
        raise ValueError("Migrate the broker service to its managed launcher before enabling updates")
    args = [str(binary / "agent-bridge-update"), "--state", str(state), "--jitter", "300", "run"]
    if kind == "systemd":
        directory = Path.home() / ".config/systemd/user"
        directory.mkdir(parents=True, exist_ok=True)
        service = directory / "cli-agent-bridge-update.service"
        timer = directory / "cli-agent-bridge-update.timer"
        exec_start = " ".join('"' + a.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%") + '"' for a in args)
        _write(
            service,
            f"[Unit]\nDescription=Check Bridge GitHub releases\n[Service]\nType=oneshot\nExecStart={exec_start}\nUMask=0077\nEnvironment=PATH=%h/.local/bin:%h/.local/share/mise/shims:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin\n",
        )
        _write(
            timer,
            f"[Unit]\nDescription=Check Bridge updates periodically\n[Timer]\nOnBootSec=10m\nOnUnitActiveSec={interval}s\nRandomizedDelaySec=5m\nPersistent=true\n[Install]\nWantedBy=timers.target\n",
        )
        if enable:
            _run(["systemctl", "--user", "daemon-reload"], timeout=15)
            _run(["systemctl", "--user", "enable", "--now", timer.name], timeout=15)
        timer_path = timer
    else:
        directory = Path.home() / "Library/LaunchAgents"
        directory.mkdir(parents=True, exist_ok=True)
        timer_path = directory / "com.cli-agent-bridge.update.plist"
        arguments = "".join(f"<string>{escape(value)}</string>" for value in args)
        path_env = escape(
            str(Path.home() / ".local/bin")
            + ":"
            + str(Path.home() / ".local/share/mise/shims")
            + ":/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
        )
        _write(
            timer_path,
            '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
            + f'<plist version="1.0"><dict><key>Label</key><string>com.cli-agent-bridge.update</string><key>ProgramArguments</key><array>{arguments}</array><key>StartInterval</key><integer>{interval}</integer><key>EnvironmentVariables</key><dict><key>PATH</key><string>{path_env}</string></dict></dict></plist>\n',
        )
        if enable:
            _run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(timer_path)], timeout=15)
    return {
        "settings": str(path),
        "launcher": str(binary / "agent-bridge"),
        "timer": str(timer_path),
        "enabled": enable,
        "broker_service_migrated": False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--repository", default=REPOSITORY)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--service-kind", choices=["systemd", "launchd"])
    parser.add_argument("--service-name")
    parser.add_argument("--interval", type=int, default=3600)
    parser.add_argument("--enable", action="store_true")
    parser.add_argument("--jitter", type=int, default=0)
    parser.add_argument("action", choices=["check", "run", "install", "bootstrap"])
    args = parser.parse_args(argv)
    from .config import state_directory

    try:
        state = state_directory(str(args.state) if args.state else None)
        if not 0 <= args.jitter <= 300:
            raise ValueError("Jitter must be between 0 and 300 seconds")
        if args.jitter:
            time.sleep(secrets.randbelow(args.jitter + 1))
        if args.action == "install":
            result = install(
                state, args.repository, args.service_kind, args.service_name, args.root, args.interval, args.enable
            )
        else:
            updater = Updater(state)
            result = getattr(updater, args.action)()
    except (OSError, ValueError, RuntimeError, BridgeError) as error:
        # Error class is sufficient for logs; never echo gh/credential diagnostics.
        result = {"status": "failed", "error_type": type(error).__name__}
        if "state" in locals():
            with contextlib.suppress(OSError, ValueError):
                _private(state, directory=True)
                _write(state / "update-status.json", {**result, "checked_at": time.time()})
    print(json.dumps(result, ensure_ascii=False))
    if result.get("status") in {"failed", "uncertain", "rolled_back"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
