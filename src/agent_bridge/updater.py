"""Install checked public GitHub Release wheels; drain and hot-swap the broker while compatible workers run."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import http.client
import json
import os
import re
import secrets
import shlex
import shutil
import socket
import sqlite3
import ssl
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

from .errors import BridgeError

REPOSITORY = "qinjian623/cli-agent-bridge"
GITHUB_WEB_ORIGIN = "https://github.com"
GITHUB_API_ORIGIN = "https://api.github.com"
_GITHUB_CDN_HOSTS = frozenset(
    {
        "objects.githubusercontent.com",
        "release-assets.githubusercontent.com",
        "github-releases.githubusercontent.com",
    }
)
_GITHUB_RELEASE_HOSTS = frozenset({"github.com", "api.github.com", *_GITHUB_CDN_HOSTS})
_DEFAULT_SSL_CONTEXT: ssl.SSLContext | None = None
TAG = re.compile(r"v(\d+)\.(\d+)\.(\d+)\Z")
VERSION = re.compile(r"v?(\d+)\.(\d+)\.(\d+)\Z")
NAME = re.compile(r"[A-Za-z0-9_.-]+\Z")
DEFAULT_DRAIN_TIMEOUT = 15.0
LEGACY_WORKER_VERSION = (0, 2, 0)


class UpdateUncertain(RuntimeError):
    """A service command may still be running; do not repeat it or undo its files."""


class _ServiceCommandError(RuntimeError):
    def __init__(self, message: str, *, returncode: int, stderr: str = ""):
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr


def _private(path: Path, *, directory=False):
    info = path.lstat()

    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if not kind(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError(f"Update {'directory' if directory else 'file'} must be private and owned: {path}")


def _write_bytes(path: Path, data: bytes, mode: int = 0o600):
    with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent, delete=False, prefix=".update-") as output:
        temporary = Path(output.name)
        os.chmod(temporary, mode)
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write(path: Path, value):
    data = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    _write_bytes(path, data.encode())


def _run(argv, *, timeout=120):
    # Do not persist stderr or the environment: gh may include credential diagnostics.
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        raise UpdateUncertain("Command timed out; its outcome must be checked before retrying") from error
    if result.returncode < 0:
        raise UpdateUncertain("Command terminated by signal; its outcome must be checked before retrying")
    if result.returncode:
        raise _ServiceCommandError(
            f"{Path(argv[0]).name} command failed (exit {result.returncode})",
            returncode=result.returncode,
            stderr=result.stderr if isinstance(result.stderr, str) else "",
        )
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


def _parse_version(version):
    """Parse a semantic version string (`X.Y.Z` or `vX.Y.Z`) into an integer tuple."""
    if not isinstance(version, str):
        raise ValueError("Invalid release version")
    match = VERSION.fullmatch(version)
    if not match:
        raise ValueError("Invalid release version")
    return tuple(map(int, match.groups()))


def _worker_version_tuple(version):
    """Convert a recorded worker version into a comparable tuple for compatibility checks."""
    if version is None:
        return LEGACY_WORKER_VERSION
    try:
        return _parse_version(version)
    except ValueError:
        return (0, 0, 0)


def release_change_detector(state: Path, *, allow_missing_current: bool = True):
    """Return a zero-arg callable reporting whether the active managed release changed."""
    if not (state / "update.json").exists():
        return lambda: False
    try:
        current = Path(load_settings(state)["root"]) / "current"
    except (OSError, ValueError):
        return lambda: False
    releases_dir = (current.parent / "releases").resolve()
    running = Path(sys.prefix).resolve()
    try:
        initial = current.resolve(strict=True)
    except OSError:
        initial = None if allow_missing_current else current.resolve()
    expected = running if running.parent == releases_dir else initial

    def changed():
        try:
            resolved = current.resolve(strict=True)
        except OSError:
            return expected is not None
        return expected is None or resolved != expected

    return changed


def busy_snapshot(
    state: Path,
    broker_pid: int | None = None,
    *,
    hot: bool = True,
    check_inflight: bool = True,
    min_worker_version: str | None = None,
):
    """Read only. Paused queues stay preserved; hot OTA only blocks on broker-inflight work or incompatible workers."""
    from .worker import process_identity

    counts, reasons = {}, []
    worker_queries = {
        "open_queued_tasks": "SELECT COUNT(*) FROM tasks t JOIN sessions s ON s.id=t.session_id WHERE t.status='queued' AND s.desired='open'",
        "active_tasks": "SELECT COUNT(*) FROM tasks WHERE status IN ('assigned','running','waiting_input')",
    }
    inflight_queries = {
        "api_inflight": "SELECT COUNT(*) FROM api_requests WHERE response IS NULL",
        "notification_sending": "SELECT COUNT(*) FROM deliveries WHERE status='sending'",
        "native_broadcast_sending": "SELECT COUNT(*) FROM native_broadcast_deliveries WHERE status='sending' OR processing_status='running'",
        "native_queue_sending": "SELECT COUNT(*) FROM native_send_queue WHERE status='sending' OR processing_status='running'",
        "peer_sending": "SELECT COUNT(*) FROM peer_outbox WHERE status='sending'",
    }
    try:
        min_tuple = _parse_version(min_worker_version) if min_worker_version is not None else None
        with contextlib.closing(sqlite3.connect(f"file:{state / 'bridge.sqlite'}?mode=ro", uri=True, timeout=2)) as db:
            db.execute("BEGIN")
            for name, query in (*worker_queries.items(), *inflight_queries.items()):
                counts[name] = db.execute(query).fetchone()[0]
            columns = {row[1] for row in db.execute("PRAGMA table_info(sessions)")}
            version_expr = "worker_version" if "worker_version" in columns else "NULL"
            workers = db.execute(
                f"SELECT worker_pid,worker_start,{version_expr} FROM sessions WHERE worker_pid IS NOT NULL"
            ).fetchall()
            live_versions = [
                version
                for pid, expected, version in workers
                if (identity := process_identity(pid)) and (not expected or identity == expected)
            ]
            counts["live_workers"] = len(live_versions)
        if not hot:
            reasons.extend(name for name in ("open_queued_tasks", "active_tasks", "live_workers") if counts[name])
        elif min_tuple is not None and live_versions:
            incompatible = sum(1 for version in live_versions if _worker_version_tuple(version) < min_tuple)
            if incompatible:
                counts["incompatible_workers"] = incompatible
                reasons.append("incompatible_workers")
        if check_inflight:
            reasons.extend(name for name in inflight_queries if counts[name])
            if broker_pid is not None:
                counts["broker_helpers"] = sum(name != "ssh" for name in _children(broker_pid))
                if counts["broker_helpers"]:
                    reasons.append("broker_helpers")
    except (OSError, ValueError, sqlite3.Error, RuntimeError):
        reasons.append("update_state_unknown")
    return {"busy": bool(reasons), "reasons": reasons, "counts": counts, "hot": hot}


def _validate_github_url(
    url: str,
    *,
    allowed_hosts: frozenset[str] | None = None,
    allow_query: bool = False,
) -> str:
    """Validate an HTTPS GitHub Release or official GitHub asset CDN URL."""
    if not isinstance(url, str):
        raise ValueError("Invalid release URL")
    cleaned = url.strip()
    if not cleaned or any(ch.isspace() or ord(ch) < 32 for ch in cleaned):
        raise ValueError("Invalid release URL")
    parsed = urllib.parse.urlsplit(cleaned)
    if parsed.scheme.lower() != "https":
        raise ValueError("Release URLs must use https")
    if parsed.username is not None or parsed.password is not None or parsed.fragment:
        raise ValueError("Release URL must not contain credentials or fragments")
    if parsed.query and not allow_query:
        raise ValueError("Unexpected query string on GitHub release URL")
    host = (parsed.hostname or "").lower().rstrip(".")
    allowed = _GITHUB_RELEASE_HOSTS if allowed_hosts is None else allowed_hosts
    if not host or host not in allowed:
        raise ValueError("Release URL host is not an allowed GitHub release host")
    _ = parsed.port
    if any(seg == ".." for seg in parsed.path.split("/")):
        raise ValueError("Release URL path must not contain '..'")
    return cleaned


class _DownloadDeadline:
    """Enforces an overall wall-clock deadline across DNS, TLS, headers, redirects, and body reads."""

    def __init__(self, timeout: float) -> None:
        total = float(timeout)
        if total <= 0:
            raise TimeoutError("Release download deadline exceeded")
        self.deadline = time.monotonic() + total
        self.expired = threading.Event()
        self._lock = threading.Lock()
        self._socks: set[socket.socket] = set()
        self._timer = threading.Timer(total, self._abort)
        self._timer.daemon = True

    def __enter__(self) -> _DownloadDeadline:
        self._timer.start()
        return self

    def __exit__(self, *args) -> None:
        self._timer.cancel()
        with self._lock:
            self._socks.clear()

    def check(self) -> float:
        remaining = self.deadline - time.monotonic()
        if self.expired.is_set() or remaining <= 0:
            self.expired.set()
            raise TimeoutError("Release download deadline exceeded")
        return remaining

    def track(self, sock: socket.socket) -> None:
        with self._lock:
            if self.expired.is_set() or time.monotonic() >= self.deadline:
                self.expired.set()
                with contextlib.suppress(OSError):
                    sock.close()
                raise TimeoutError("Release download deadline exceeded")
            self._socks.add(sock)

    def untrack(self, sock: socket.socket) -> None:
        with self._lock:
            self._socks.discard(sock)

    def _abort(self) -> None:
        self.expired.set()
        with self._lock:
            socks = list(self._socks)
        for sock in socks:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                sock.close()


class _RejectHTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, req):
        raise ValueError("Plain HTTP release downloads are disabled")


class _GitHubRedirectHandler(urllib.request.HTTPRedirectHandler):
    max_redirections = 5

    def __init__(self, guard: _DownloadDeadline) -> None:
        self._guard = guard

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        host = (urllib.parse.urlsplit(newurl).hostname or "").lower().rstrip(".")
        target = _validate_github_url(newurl, allow_query=(host in _GITHUB_CDN_HOSTS))
        return super().redirect_request(req, fp, code, msg, headers, target)

    def http_error_302(self, req, fp, code, msg, headers):
        try:
            self._guard.check()
            location = headers.get("location") or headers.get("uri")
            if not location:
                return None
            newurl = urllib.parse.urljoin(req.full_url, location.strip())
            new = self.redirect_request(req, fp, code, msg, headers, newurl)
            if new is None:
                return None
            visited = getattr(req, "redirect_dict", {})
            if len(visited) >= self.max_redirections or visited.get(newurl, 0) >= 1:
                raise ValueError("Too many release redirects")
            new.redirect_dict = {**visited, newurl: visited.get(newurl, 0) + 1}
        finally:
            # Close redirect response immediately without draining unbounded or slow bodies.
            with contextlib.suppress(OSError):
                fp.close()
        return self.parent.open(new, timeout=self._guard.check())

    http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302


def _resolve_addrs_with_deadline(host: str, port: int, guard: _DownloadDeadline):
    done = threading.Event()
    box: list[tuple[str, object]] = []

    def _lookup() -> None:
        try:
            box.append(("ok", socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)))
        except BaseException as error:
            box.append(("err", error))
        finally:
            done.set()

    threading.Thread(target=_lookup, daemon=True).start()
    if not done.wait(timeout=guard.check()):
        guard.expired.set()
        raise TimeoutError("Release download DNS resolution exceeded deadline")
    guard.check()
    kind, payload = box[0]
    if kind == "err":
        raise payload  # type: ignore[misc]
    return payload


def _open_guarded_socket(host: str, port: int, guard: _DownloadDeadline) -> socket.socket:
    guard.check()
    addrs = _resolve_addrs_with_deadline(host, port, guard)
    if not addrs:
        raise OSError("Could not resolve release host")
    last_err: OSError | None = None
    for family, socktype, proto, _, sockaddr in addrs:
        sock = None
        try:
            remaining = guard.check()
            sock = socket.socket(family, socktype, proto)
            sock.settimeout(remaining)
            guard.track(sock)
            sock.connect(sockaddr)
            return sock
        except OSError as error:
            last_err = error
            if sock is not None:
                guard.untrack(sock)
                with contextlib.suppress(OSError):
                    sock.close()
    guard.check()
    if last_err is not None:
        raise last_err
    raise OSError("Failed to connect to release host")


class _DeadlineHTTPSConnection(http.client.HTTPSConnection):
    """HTTPSConnection enforcing the shared download deadline across CONNECT proxies, TCP, and TLS."""

    def __init__(self, host: str, *, guard: _DownloadDeadline, **kwargs) -> None:
        super().__init__(host, **kwargs)
        self._guard = guard

    def connect(self):
        raw = _open_guarded_socket(self.host, self.port, self._guard)
        wrapped = None
        try:
            server_hostname = self.host
            if self._tunnel_host:
                server_hostname = self._tunnel_host
                raw.settimeout(self._guard.check())
                self.sock = raw
                try:
                    self._tunnel()
                finally:
                    self.sock = None
            raw.settimeout(self._guard.check())
            wrapped = self._context.wrap_socket(
                raw, server_hostname=server_hostname, do_handshake_on_connect=False
            )
            self._guard.track(wrapped)
            self._guard.untrack(raw)
            wrapped.settimeout(self._guard.check())
            wrapped.do_handshake()
        except Exception:
            if wrapped is not None:
                self._guard.untrack(wrapped)
                with contextlib.suppress(OSError):
                    wrapped.close()
            else:
                self._guard.untrack(raw)
                with contextlib.suppress(OSError):
                    raw.close()
            raise
        self.sock = wrapped


class _BoundedHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, guard: _DownloadDeadline, *, context: ssl.SSLContext) -> None:
        super().__init__(context=context)
        self._guard = guard

    def https_open(self, req):
        req.timeout = self._guard.check()
        return self.do_open(
            lambda host, **kw: _DeadlineHTTPSConnection(host, guard=self._guard, **kw),
            req,
            context=self._context,
        )


def _download_bytes(url: str, *, max_bytes: int, timeout: float = 30.0) -> bytes:
    """Download a bounded HTTPS release payload within a total wall-clock deadline."""
    clean = _validate_github_url(url)
    context = _DEFAULT_SSL_CONTEXT if _DEFAULT_SSL_CONTEXT is not None else ssl.create_default_context()
    with _DownloadDeadline(timeout) as guard:
        handlers: list[urllib.request.BaseHandler] = [
            _GitHubRedirectHandler(guard),
            _RejectHTTPHandler(),
            _BoundedHTTPSHandler(guard, context=context),
        ]
        opener = urllib.request.build_opener(*handlers)
        request = urllib.request.Request(
            clean,
            headers={
                "Accept": "application/vnd.github+json, application/json, application/octet-stream",
                "User-Agent": "cli-agent-bridge-updater",
            },
        )
        chunks = bytearray()
        try:
            with opener.open(request, timeout=guard.check()) as response:
                guard.check()
                while True:
                    remaining = guard.check()
                    raw_stream = getattr(getattr(response, "fp", None), "raw", None)
                    raw_sock = getattr(raw_stream, "_sock", None)
                    if raw_sock is not None:
                        with contextlib.suppress(OSError):
                            raw_sock.settimeout(remaining)
                    read_fn = getattr(response, "read1", response.read)
                    chunk = read_fn(65_536)
                    guard.check()
                    if not chunk:
                        break
                    if len(chunks) + len(chunk) > max_bytes:
                        raise ValueError("Release asset exceeds size limit")
                    chunks.extend(chunk)
        except (OSError, http.client.HTTPException):
            if guard.expired.is_set() or time.monotonic() >= guard.deadline:
                raise TimeoutError("Release download deadline exceeded") from None
            raise
    return bytes(chunks)


def load_settings(state: Path):
    path = state / "update.json"
    _private(path)
    value = json.loads(path.read_text())
    required = {
        "repository",
        "root",
        "service_kind",
        "service_name",
        "bootstrap_python",
    }
    optional = {"interval"}
    if not isinstance(value, dict) or not required.issubset(value) or not set(value).issubset(required | optional):
        raise ValueError("Invalid update settings")
    if any(not isinstance(value[key], str) for key in required):
        raise ValueError("Update settings must be strings")
    if "interval" in value and (type(value["interval"]) is not int or value["interval"] < 300):
        raise ValueError("Update interval must be an integer >= 300")
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


def build_release_manifest(
    tag: str,
    wheel_size: int,
    checksums_size: int,
    *,
    release_id: int | None = None,
) -> dict[str, object]:
    """Build and validate a GitHub release manifest (`latest.json`) matching the release parser."""
    _version(tag)
    wheel = f"cli_agent_bridge-{tag[1:]}-py3-none-any.whl"
    payload: dict[str, object] = {
        "tag_name": tag,
        "draft": False,
        "prerelease": False,
        "assets": [
            {"name": wheel, "size": wheel_size},
            {"name": "SHA256SUMS", "size": checksums_size},
        ],
    }
    if release_id is not None:
        payload["id"] = release_id
    _parse_release_payload(payload, require_positive_id=False)
    return payload


def _parse_release_payload(data, *, require_positive_id: bool = True):
    if not isinstance(data, dict):
        raise ValueError("Invalid release response")
    release_id = data.get("id")
    if require_positive_id:
        if type(release_id) is not int or release_id <= 0:
            raise ValueError("Invalid GitHub release response")
    elif release_id is not None and (type(release_id) is not int or release_id < 0):
        raise ValueError("Invalid release response")
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
    return {"tag": tag, "wheel": wheel, "release_id": release_id}


def latest_release(repository: str):
    if not isinstance(repository, str) or len(repository.split("/")) != 2 or not all(NAME.fullmatch(x) for x in repository.split("/")):
        raise ValueError("Invalid GitHub repository")
    try:
        raw = _download_bytes(
            f"{GITHUB_WEB_ORIGIN}/{repository}/releases/latest/download/latest.json",
            max_bytes=262_144,
            timeout=30,
        )
        require_positive_id = False
    except urllib.error.HTTPError as error:
        with contextlib.closing(error):
            if error.code != 404:
                raise
        raw = _download_bytes(
            f"{GITHUB_API_ORIGIN}/repos/{repository}/releases/latest",
            max_bytes=262_144,
            timeout=30,
        )
        require_positive_id = True
    data = json.loads(raw.decode("utf-8"))
    return _parse_release_payload(data, require_positive_id=require_positive_id)


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


@contextlib.contextmanager
def _acquire_update_lock(root: Path):
    lock_path = root / "update.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        _private(lock_path)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)


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
        with _acquire_update_lock(self.root):
            yield

    def stage(self, release):
        _version(release["tag"])
        expected_wheel = f"cli_agent_bridge-{release['tag'][1:]}-py3-none-any.whl"
        if release.get("wheel") != expected_wheel:
            raise ValueError("Release wheel filename does not match tag version")
        releases = self.root / "releases"
        releases.mkdir(mode=0o700, exist_ok=True)
        _private(releases, directory=True)
        target = releases / (release["tag"] + "-" + uuid.uuid4().hex[:12])
        target.mkdir(mode=0o700)
        try:
            with tempfile.TemporaryDirectory(prefix="download-", dir=self.root) as name:
                download = Path(name)
                repo = self.settings["repository"]
                tag = release["tag"]
                (download / expected_wheel).write_bytes(
                    _download_bytes(
                        f"{GITHUB_WEB_ORIGIN}/{repo}/releases/download/{tag}/{expected_wheel}",
                        max_bytes=100_000_000,
                        timeout=120,
                    )
                )
                (download / "SHA256SUMS").write_bytes(
                    _download_bytes(
                        f"{GITHUB_WEB_ORIGIN}/{repo}/releases/download/{tag}/SHA256SUMS",
                        max_bytes=65_536,
                        timeout=30,
                    )
                )
                digest = verify_wheel(download, expected_wheel)
                _run(["uv", "venv", "--python", self.settings["bootstrap_python"], str(target)], timeout=120)
                _run(
                    ["uv", "pip", "install", "--python", str(target / "bin/python"), str(download / expected_wheel)],
                    timeout=300,
                )
            compatibility = self.preflight(target, release["tag"])
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
            release_meta = {**release, "sha256": digest}
            if isinstance(compatibility, dict):
                if "requires_worker_drain" in compatibility:
                    release_meta["requires_worker_drain"] = bool(compatibility["requires_worker_drain"])
                if isinstance(compatibility.get("min_worker_version"), str):
                    _parse_version(compatibility["min_worker_version"])
                    release_meta["min_worker_version"] = compatibility["min_worker_version"]
            _write(target / "release.json", release_meta)
            return target
        except BaseException:
            shutil.rmtree(target)
            raise

    def preflight(self, target: Path, tag: str):
        python = str(target / "bin/python")
        code = (
            "import importlib.metadata, json, sys, agent_bridge; "
            "assert importlib.metadata.version('cli-agent-bridge') == sys.argv[1]; "
            "print(json.dumps({"
            "'requires_worker_drain': bool(getattr(agent_bridge, 'REQUIRES_WORKER_DRAIN', False)), "
            "'min_worker_version': getattr(agent_bridge, 'MIN_COMPATIBLE_WORKER_VERSION', None)"
            "}))"
        )
        raw = _run([python, "-c", code, tag[1:]], timeout=15).strip()
        compatibility = json.loads(raw) if raw else {}
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
        return compatibility

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
        if self.settings["service_kind"] == "systemd":
            command = ["systemctl", "--user", "restart", service]
        else:
            command = ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{_launchd_label(service)}"]
        _run(command, timeout=45)

    def service_managed(self):
        """Changing current has no effect until the service uses our stable launcher."""
        launcher = str(self.root / "bin/agent-bridge")
        return _probe_broker_service_state(
            self.settings["service_kind"], self.settings["service_name"], launcher,
        )[2]

    async def _activate(
        self,
        target: Path,
        config_digest: str,
        *,
        hot: bool = True,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
    ):
        from .broker import request

        release_path = target / "release.json"
        target_meta = json.loads(release_path.read_text()) if release_path.exists() else {}
        effective_hot = hot and not bool(target_meta.get("requires_worker_drain", False))
        min_worker_version = target_meta.get("min_worker_version")
        prepare_args = {}
        if not effective_hot:
            prepare_args["hot"] = False
        if drain_timeout != DEFAULT_DRAIN_TIMEOUT:
            prepare_args["drain_timeout"] = drain_timeout
        if min_worker_version is not None and _parse_version(min_worker_version) > LEGACY_WORKER_VERSION:
            prepare_args["min_worker_version"] = min_worker_version

        try:
            lease = await request(self.state, "agent_update_prepare", prepare_args)
        except BridgeError as error:
            if error.code == "invalid_request" and prepare_args:
                lease = await request(self.state, "agent_update_prepare", {})
            else:
                raise
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
            snapshot = busy_snapshot(
                self.state,
                broker_pid=lease.get("broker_pid"),
                hot=effective_hot,
                min_worker_version=min_worker_version,
            )
            if snapshot["busy"]:
                return {"status": "deferred", **snapshot}
            target_version = target_meta["tag"][1:]
            previous_version = json.loads((previous / "release.json").read_text())["tag"][1:] if previous else None
            self._record(
                {
                    "status": "activating",
                    "target_version": target_version,
                    "previous_version": previous_version,
                    "candidate": str(target),
                    "previous": str(previous) if previous else None,
                    "hot": effective_hot,
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
                try:
                    self._switch(previous)
                    self._restart()
                    await self._healthy(self.state, expected_version=previous_version)
                except (RuntimeError, OSError, BridgeError) as error:
                    raise UpdateUncertain("Rollback health could not be confirmed") from error
                remove_candidate = True
                return {"status": "rolled_back", "reason": "new_broker_failed_health_check"}
            return {"status": "updated", "release": target.name, "hot": effective_hot}
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

    def run(self, *, drain_workers: bool = False):
        try:
            hot = not drain_workers
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
                snapshot = busy_snapshot(self.state, hot=hot, check_inflight=not hot)
                if snapshot["busy"]:
                    return self._record({"status": "deferred", **snapshot})
                digest = hashlib.sha256((self.state / "config.json").read_bytes()).hexdigest()
                target = self.stage(check["available"])
                try:
                    result = asyncio.run(self._activate(target, digest, hot=hot))
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


_LAUNCHER_MODULES = (
    ("agent-bridge", "agent_bridge.cli"),
    ("agent-bridge-update", "agent_bridge.updater"),
)


def _launcher_prefix(root: Path, module: str):
    return (
        "#!/bin/sh\nset -eu\n"
        + f"root={shlex.quote(str(root))}\n"
        + 'if [ -x "$root/current/bin/python" ]; then\n'
        + '  release=$(CDPATH= cd -- "$root/current" && pwd -P)\n'
        + f'  exec "$release/bin/python" -m {module} "$@"\nfi\nexec '
    )


def _launcher(root: Path, bootstrap: Path, module: str):
    return _launcher_prefix(root, module) + shlex.quote(str(bootstrap)) + f' -m {module} "$@"\n'


def _managed_launcher_bootstrap(content: str, root: Path, module: str) -> Path | None:
    """Return the bootstrap path when content is exactly our launcher template, not arbitrary shell."""
    prefix = _launcher_prefix(root, module)
    suffix = f' -m {module} "$@"\n'
    if not content.startswith(prefix) or not content.endswith(suffix):
        return None
    token = content[len(prefix) : len(content) - len(suffix)]
    if not token:
        return None
    try:
        parts = shlex.split(token)
    except ValueError:
        return None
    if len(parts) != 1 or shlex.quote(parts[0]) != token:
        return None
    raw = parts[0]
    if not raw or "\x00" in raw or "\n" in raw or "\r" in raw:
        return None
    bootstrap = Path(raw)
    return bootstrap if bootstrap.is_absolute() else None


def _systemd_exec_start(args: list[str]) -> str:
    return " ".join('"' + a.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%") + '"' for a in args)


def _launchd_path_env() -> str:
    return escape(
        str(Path.home() / ".local/bin")
        + ":"
        + str(Path.home() / ".local/share/mise/shims")
        + ":/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
    )


def _launchd_label(name: str) -> str:
    return name[:-6] if name.endswith(".plist") else name


def _validate_repository(repository: str) -> None:
    if (
        not isinstance(repository, str)
        or len(repository.split("/")) != 2
        or not all(NAME.fullmatch(x) for x in repository.split("/"))
    ):
        raise ValueError("Invalid GitHub repository")


def _expected_launchers(
    root: Path,
    bootstrap: Path | None = None,
) -> tuple[Path, Path, dict[str, tuple[str, Path]]]:
    bootstrap = bootstrap or Path(sys.executable).parent.resolve() / Path(sys.executable).name
    binary = root / "bin"
    entries = {
        filename: (_launcher(root, bootstrap, module), binary / filename)
        for filename, module in _LAUNCHER_MODULES
    }
    return bootstrap, binary, entries


_DARWIN_SYSTEM_SYMLINKS = frozenset({Path("/tmp"), Path("/var")}) if sys.platform == "darwin" else frozenset()


def _validate_write_target(
    path: Path,
    *,
    label: str,
    stop_at: Path | None = None,
    expect_dir: bool = False,
) -> None:
    if path.is_symlink():
        raise ValueError(f"Refusing to write symlinked {label}: {path}")
    if path.exists():
        if expect_dir:
            if not path.is_dir():
                raise ValueError(f"Expected directory for {label}: {path}")
            if not os.access(path, os.W_OK | os.X_OK):
                raise ValueError(f"Directory is not writable for {label}: {path}")
        elif not path.is_file():
            raise ValueError(f"Expected regular file for {label}: {path}")

    boundary = stop_at if (stop_at is not None and path.is_relative_to(stop_at)) else None
    cur = path.parent
    checked_existing_parent = False
    while True:
        if cur.is_symlink() and cur not in _DARWIN_SYSTEM_SYMLINKS:
            raise ValueError(f"Refusing to write under symlinked directory for {label}: {cur}")
        if cur.exists():
            if not checked_existing_parent:
                if not cur.is_dir():
                    raise ValueError(f"Expected directory for {label}: {cur}")
                if not os.access(cur, os.W_OK | os.X_OK):
                    raise ValueError(f"Directory is not writable for {label}: {cur}")
                checked_existing_parent = True
            elif not cur.is_dir():
                raise ValueError(f"Expected directory for {label}: {cur}")
        if (boundary is not None and cur == boundary) or cur == cur.parent:
            break
        cur = cur.parent


def _check_launcher_conflicts(root: Path) -> None:
    _validate_write_target(root, label="managed root", expect_dir=True)
    if root.exists():
        _private(root, directory=True)
    binary = root / "bin"
    _validate_write_target(binary, label="managed bin directory", stop_at=root, expect_dir=True)
    _, _, entries = _expected_launchers(root)
    for filename, module in _LAUNCHER_MODULES:
        _, command = entries[filename]
        if command.is_symlink():
            raise ValueError("Refusing to overwrite a symlinked managed launcher")
        if command.exists() and (
            not command.is_file() or _managed_launcher_bootstrap(command.read_text(), root, module) is None
        ):
            raise ValueError("Refusing to overwrite an unrelated managed launcher")


def _ensure_launchers(root: Path, bootstrap: Path | None = None) -> tuple[Path, Path]:
    bootstrap, binary, entries = _expected_launchers(root, bootstrap)
    _check_launcher_conflicts(root)
    binary.mkdir(mode=0o700, exist_ok=True)
    for expected, command in entries.values():
        if not command.exists():
            _write(command, expected)
            command.chmod(0o700)
    return bootstrap, binary


def _resolve_root_and_service(
    root: Path | None,
    service_kind: str | None,
    service_name: str | None,
    *,
    create: bool = True,
):
    resolved_root = (Path(root) if root else Path.home() / ".local/share/cli-agent-bridge").expanduser().absolute()
    if resolved_root.is_symlink():
        raise ValueError("Managed root must not be a symlink")
    if create:
        resolved_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        _private(resolved_root, directory=True)
    kind = service_kind or ("launchd" if sys.platform == "darwin" else "systemd")
    if kind not in {"systemd", "launchd"}:
        raise ValueError("Unsupported service_kind")
    name = service_name or ("com.cli-agent-bridge.broker" if kind == "launchd" else "cli-agent-bridge.service")
    if not NAME.fullmatch(name):
        raise ValueError("Invalid update service")
    return resolved_root, kind, name


def validate_update_install(
    state: Path,
    *,
    repository=REPOSITORY,
    service_kind=None,
    service_name=None,
    root=None,
    interval=3600,
    update_existing: bool = False,
) -> tuple[Path, str, str, dict[str, object]]:
    """Validate updater configuration and check file conflicts without mutating disk."""
    if type(interval) is not int or interval < 300:
        raise ValueError("Update interval must be at least 300 seconds")
    _validate_repository(repository)
    _validate_write_target(state, label="state directory", expect_dir=True)
    if state.exists():
        _private(state, directory=True)
    resolved_root, kind, name = _resolve_root_and_service(root, service_kind, service_name, create=False)
    _check_launcher_conflicts(resolved_root)
    path = state / "update.json"
    bootstrap = Path(sys.executable).parent.resolve() / Path(sys.executable).name
    existing = None
    if path.exists() and not path.is_symlink() and path.is_file():
        try:
            existing = json.loads(path.read_text())
        except (OSError, ValueError):
            existing = None
        if isinstance(existing, dict):
            recorded = existing.get("bootstrap_python")
            if (
                isinstance(recorded, str)
                and Path(recorded).is_absolute()
                and "\x00" not in recorded
                and "\n" not in recorded
            ):
                bootstrap = Path(recorded)
    settings: dict[str, object] = {
        "repository": repository,
        "root": str(resolved_root),
        "service_kind": kind,
        "service_name": name,
        "bootstrap_python": str(bootstrap),
        "interval": interval,
    }
    _validate_write_target(path, label="update.json", stop_at=state)
    if kind == "systemd":
        directory = Path.home() / ".config/systemd/user"
        _validate_write_target(directory / "cli-agent-bridge-update.service", label="updater service file")
        _validate_write_target(directory / "cli-agent-bridge-update.timer", label="updater timer file")
    else:
        directory = Path.home() / "Library/LaunchAgents"
        _validate_write_target(directory / "com.cli-agent-bridge.update.plist", label="updater LaunchAgent file")
    if path.exists() and not update_existing:
        if existing is None:
            existing = json.loads(path.read_text())
        if not isinstance(existing, dict):
            raise ValueError("Invalid update settings")
        normalized = {**existing, "interval": existing.get("interval", 3600)}
        if normalized != settings:
            raise ValueError("Existing update settings differ; edit the private config explicitly")
    return resolved_root, kind, name, settings


def install(
    state: Path,
    repository=REPOSITORY,
    service_kind=None,
    service_name=None,
    root=None,
    interval=3600,
    enable=False,
    *,
    update_existing: bool = False,
    on_write: Any = None,
):
    """Write launchers and timer only; migrating an existing broker service is a separate, deliberate step."""
    resolved_root, kind, name, settings = validate_update_install(
        state,
        repository=repository,
        service_kind=service_kind,
        service_name=service_name,
        root=root,
        interval=interval,
        update_existing=update_existing,
    )
    if enable and not _probe_broker_service_state(kind, name, str(resolved_root / "bin/agent-bridge"))[2]:
        raise ValueError("Migrate the broker service to its managed launcher before enabling updates")
    state.mkdir(parents=True, mode=0o700, exist_ok=True)
    _private(state, directory=True)
    resolved_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    _private(resolved_root, directory=True)
    path = state / "update.json"
    _write(path, settings)
    load_settings(state)
    if on_write is not None:
        on_write("update.json")
    _, binary = _ensure_launchers(resolved_root, Path(settings["bootstrap_python"]))
    args = [str(binary / "agent-bridge-update"), "--state", str(state), "--jitter", "300", "run"]
    if kind == "systemd":
        directory = Path.home() / ".config/systemd/user"
        directory.mkdir(parents=True, exist_ok=True)
        service = directory / "cli-agent-bridge-update.service"
        timer = directory / "cli-agent-bridge-update.timer"
        exec_start = _systemd_exec_start(args)
        _write(
            service,
            f"[Unit]\nDescription=Check Bridge GitHub releases\n[Service]\nType=oneshot\nExecStart={exec_start}\nUMask=0077\nEnvironment=PATH=%h/.local/bin:%h/.local/share/mise/shims:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin\n",
        )
        _write(
            timer,
            f"[Unit]\nDescription=Check Bridge updates periodically\n[Timer]\nOnBootSec=10m\nOnUnitActiveSec={interval}s\nRandomizedDelaySec=5m\nPersistent=true\n[Install]\nWantedBy=timers.target\n",
        )
        if on_write is not None:
            on_write("updater")
        if enable:
            _run(["systemctl", "--user", "daemon-reload"], timeout=15)
            _run(["systemctl", "--user", "enable", "--now", timer.name], timeout=15)
        timer_path = timer
    else:
        directory = Path.home() / "Library/LaunchAgents"
        directory.mkdir(parents=True, exist_ok=True)
        timer_path = directory / "com.cli-agent-bridge.update.plist"
        arguments = "".join(f"<string>{escape(value)}</string>" for value in args)
        _write(
            timer_path,
            '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
            + f'<plist version="1.0"><dict><key>Label</key><string>com.cli-agent-bridge.update</string><key>ProgramArguments</key><array>{arguments}</array><key>StartInterval</key><integer>{interval}</integer><key>EnvironmentVariables</key><dict><key>PATH</key><string>{_launchd_path_env()}</string></dict></dict></plist>\n',
        )
        if on_write is not None:
            on_write("updater")
        if enable:
            _run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(timer_path)], timeout=15)
    return {
        "settings": str(path),
        "launcher": str(binary / "agent-bridge"),
        "timer": str(timer_path),
        "enabled": enable,
        "broker_service_migrated": bool(enable),
    }


def _broker_service_spec(
    state: Path,
    *,
    root: Path | None = None,
    service_kind: str | None = None,
    service_name: str | None = None,
) -> tuple[Path, str, str, str, Path, str]:
    resolved_root, kind, name = _resolve_root_and_service(root, service_kind, service_name, create=False)
    launcher = str(resolved_root / "bin/agent-bridge")
    args = [launcher, "--state", str(state), "serve"]
    if kind == "systemd":
        unit_name = name if name.endswith(".service") else f"{name}.service"
        service_path = Path.home() / ".config/systemd/user" / unit_name
        content = (
            "[Unit]\nDescription=CLI Agent Bridge broker service\n\n"
            f"[Service]\nType=simple\nExecStart={_systemd_exec_start(args)}\n"
            "Environment=PATH=%h/.local/bin:%h/.local/share/mise/shims:%h/.npm-global/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin\n"
            "UMask=0077\nRestart=on-failure\nRestartSec=3\n\n"
            "[Install]\nWantedBy=default.target\n"
        )
    else:
        label = _launchd_label(name)
        service_path = Path.home() / "Library/LaunchAgents" / f"{label}.plist"
        arguments = "".join(f"<string>{escape(value)}</string>" for value in args)
        content = (
            '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
            + f'<plist version="1.0"><dict><key>Label</key><string>{escape(label)}</string>'
            + f"<key>ProgramArguments</key><array>{arguments}</array>"
            + "<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>"
            + f"<key>EnvironmentVariables</key><dict><key>PATH</key><string>{_launchd_path_env()}</string></dict></dict></plist>\n"
        )
    return resolved_root, kind, name, launcher, service_path, content


_SERVICE_ACTIVE = "active"
_SERVICE_INACTIVE = "inactive"
_SERVICE_ABSENT = "absent"


class _ServiceProbeResult(tuple):
    def __new__(cls, status: str, pid: int | None, managed: bool):
        value = super().__new__(cls, (status == _SERVICE_ACTIVE, pid, managed))
        value.status = status
        return value


def _confirmed_missing_service(kind: str, error: BaseException) -> bool:
    if not isinstance(error, _ServiceCommandError):
        return False
    stderr = error.stderr.lower() if isinstance(error.stderr, str) else ""
    if kind == "systemd":
        return (
            error.returncode == 4
            or "could not be found" in stderr
            or "not found" in stderr
            or "not-found" in stderr
        )
    return (
        error.returncode == 113
        or "could not find service" in stderr
        or "service not found" in stderr
        or "no such process" in stderr
    )


def _unknown_service_state(kind: str, name: str) -> RuntimeError:
    return RuntimeError(f"Could not determine {kind} service state for {name}; refusing to guess")


def _probe_broker_service_state(kind: str, name: str, launcher: str) -> tuple[bool, int | None, bool]:
    """Return (is_active, main_pid, uses_managed_launcher); raise if the state is not known."""
    if kind == "systemd":
        try:
            output = _run(
                [
                    "systemctl",
                    "--user",
                    "show",
                    name,
                    "-p",
                    "LoadState",
                    "-p",
                    "ActiveState",
                    "-p",
                    "MainPID",
                    "-p",
                    "ExecStart",
                ],
                timeout=10,
            )
        except UpdateUncertain:
            raise
        except (OSError, RuntimeError) as error:
            if _confirmed_missing_service(kind, error):
                return _ServiceProbeResult(_SERVICE_ABSENT, None, False)
            raise _unknown_service_state(kind, name) from error
        fields: dict[str, str] = {}
        for line in output.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                fields[k.strip()] = v.strip()
        load_state = fields.get("LoadState", "")
        active_state = fields.get("ActiveState", "")
        if load_state == "not-found" or active_state == "not-found":
            status = _SERVICE_ABSENT
        elif active_state in {"active", "activating", "reloading"}:
            status = _SERVICE_ACTIVE
        elif active_state in {"inactive", "failed"}:
            status = _SERVICE_INACTIVE
        else:
            raise _unknown_service_state(kind, name) from RuntimeError(
                "service manager returned no recognized unit state"
            )
        raw_pid = fields.get("MainPID", "0")
        pid = int(raw_pid) if raw_pid.isdigit() and int(raw_pid) > 0 else None
        exec_val = fields.get("ExecStart", "")
        managed = re.search(r"(?:^|[ ;{])path=" + re.escape(launcher) + r"\s*;", exec_val) is not None
        return _ServiceProbeResult(status, pid, managed)
    label = _launchd_label(name)
    try:
        output = _run(["launchctl", "print", f"gui/{os.getuid()}/{label}"], timeout=10)
    except UpdateUncertain:
        raise
    except (OSError, RuntimeError) as error:
        if _confirmed_missing_service(kind, error):
            return _ServiceProbeResult(_SERVICE_ABSENT, None, False)
        raise _unknown_service_state(kind, name) from error
    lowered = output.lower()
    if "could not find service" in lowered or "no such process" in lowered:
        return _ServiceProbeResult(_SERVICE_ABSENT, None, False)
    states = [
        line.split("=", 1)[1].strip()
        for line in output.splitlines()
        if line.strip().startswith("state =") and "=" in line
    ]
    if states == ["running"]:
        status = _SERVICE_ACTIVE
    elif len(states) == 1 and states[0] in {"not running", "waiting", "exited"}:
        status = _SERVICE_INACTIVE
    else:
        raise _unknown_service_state(kind, name) from RuntimeError(
            "launchctl returned no recognized job state"
        )
    pid: int | None = None
    for line in output.splitlines():
        m = re.match(r"^\s*pid\s*=\s*(\d+)\s*$", line)
        if m and int(m.group(1)) > 0:
            pid = int(m.group(1))
            break
    managed = any(line.strip() == "program = " + launcher for line in output.splitlines())
    return _ServiceProbeResult(status, pid, managed)


def _expected_managed_version(root: Path) -> str | None:
    current = root / "current"
    if current.is_symlink():
        try:
            target = current.resolve(strict=True)
            data = json.loads((target / "release.json").read_text())
            tag = data.get("tag")
            if isinstance(tag, str) and tag.startswith("v"):
                return tag[1:]
        except (OSError, ValueError):
            return None
    try:
        from . import __version__

        return __version__
    except ImportError:
        return None


_BROKER_HEALTH_TIMEOUT = 10.0


def _verify_migrated_broker(
    state: Path,
    root: Path,
    kind: str,
    name: str,
    launcher: str,
    *,
    previous_pid: int | None,
    restarted: bool,
) -> tuple[int, str]:
    expected_version = _expected_managed_version(root)
    if not isinstance(expected_version, str) or not expected_version:
        raise RuntimeError(
            "Broker service migration incomplete: could not determine expected managed broker version"
        )

    async def _poll_status() -> dict[str, object]:
        from .broker import request

        end = time.monotonic() + _BROKER_HEALTH_TIMEOUT
        while time.monotonic() < end:
            try:
                async with asyncio.timeout(1):
                    status = await request(state, "agent_update_status", {})
                new_pid = status.get("pid")
                if restarted and previous_pid is not None and new_pid == previous_pid:
                    await asyncio.sleep(0.1)
                    continue
                if status.get("version") != expected_version:
                    raise RuntimeError(
                        f"Broker service migration incomplete: expected version {expected_version}, got {status.get('version')}"
                    )
                if type(new_pid) is not int or new_pid <= 0:
                    raise RuntimeError(
                        f"Broker service migration incomplete: invalid broker PID {new_pid!r}"
                    )
                return status
            except (OSError, TimeoutError, BridgeError):
                await asyncio.sleep(0.1)
        if restarted and previous_pid is not None:
            raise RuntimeError(
                f"Broker service migration incomplete: broker did not switch cleanly away from PID {previous_pid}"
            )
        raise RuntimeError("Broker service migration incomplete: candidate broker did not become healthy")

    status = asyncio.run(_poll_status())
    active, live_pid, managed = _probe_broker_service_state(kind, name, launcher)
    if not active or not managed:
        raise RuntimeError("Broker service migration incomplete: service is not running the managed launcher")
    reported_pid = status.get("pid")
    if type(reported_pid) is not int or reported_pid <= 0:
        raise RuntimeError(f"Broker service migration incomplete: invalid reported PID {reported_pid!r}")
    if type(live_pid) is not int or live_pid <= 0:
        raise RuntimeError(
            f"Broker service migration incomplete: service manager did not report a live MainPID ({live_pid!r})"
        )
    if reported_pid != live_pid:
        raise RuntimeError(
            f"Broker service migration incomplete: unit MainPID {live_pid} differs from broker PID {reported_pid}"
        )
    version = status.get("version")
    if version != expected_version:
        raise RuntimeError(
            f"Broker service migration incomplete: expected version {expected_version}, got {version!r}"
        )
    return reported_pid, version


def _acquire_migration_lease(state: Path, expected_pid: int) -> str:
    from .broker import request

    try:
        lease = asyncio.run(request(state, "agent_update_prepare", {"hot": False}))
    except BridgeError as error:
        if error.code == "invalid_request":
            try:
                lease = asyncio.run(request(state, "agent_update_prepare", {}))
            except (OSError, TimeoutError, BridgeError) as inner:
                raise RuntimeError(
                    f"Refusing to restart broker service without a maintenance lease: {inner}"
                ) from inner
        else:
            raise RuntimeError(
                f"Refusing to restart broker service without a maintenance lease: {error}"
            ) from error
    except (OSError, TimeoutError) as error:
        raise RuntimeError(
            f"Refusing to restart broker service without a maintenance lease: {error}"
        ) from error

    if not isinstance(lease, dict):
        raise RuntimeError("Refusing to restart broker service: invalid maintenance lease payload")
    token = lease.get("token")
    expires_at = lease.get("expires_at")
    if (
        not isinstance(token, str)
        or not token
        or isinstance(expires_at, bool)
        or not isinstance(expires_at, (int, float))
        or expires_at < time.time() + 30
    ):
        if isinstance(token, str) and token:
            with contextlib.suppress(OSError, TimeoutError, BridgeError, RuntimeError):
                asyncio.run(request(state, "agent_update_finish", {"token": token}))
        raise RuntimeError("Refusing to restart broker service: invalid or too-short maintenance lease")
    lease_pid = lease.get("broker_pid")
    if type(lease_pid) is not int or lease_pid <= 0:
        try:
            status = asyncio.run(request(state, "agent_update_status", {}))
            lease_pid = status.get("pid") if isinstance(status, dict) else None
        except (OSError, TimeoutError, BridgeError):
            lease_pid = None
    if lease_pid != expected_pid:
        with contextlib.suppress(OSError, TimeoutError, BridgeError, RuntimeError):
            asyncio.run(request(state, "agent_update_finish", {"token": token}))
        raise RuntimeError(
            f"Refusing to restart broker service PID {expected_pid}: "
            f"maintenance lease came from broker PID {lease_pid!r}"
        )
    return token


def validate_broker_service_install(
    state: Path,
    *,
    root: Path | None = None,
    service_kind: str | None = None,
    service_name: str | None = None,
    enable: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    """Validate broker service unit/launcher constraints and live migration readiness without writing disk."""
    _validate_write_target(state, label="state directory", expect_dir=True)
    if state.exists():
        _private(state, directory=True)
    resolved_root, kind, name, launcher, service_path, content = _broker_service_spec(
        state, root=root, service_kind=service_kind, service_name=service_name
    )
    _check_launcher_conflicts(resolved_root)
    _validate_write_target(service_path, label="broker service file")
    existing_content = service_path.read_text() if service_path.exists() else None
    content_changed = existing_content != content
    if existing_content is not None and content_changed and not force:
        raise ValueError(
            "Refusing to replace existing broker service"
            if kind == "systemd"
            else "Refusing to replace existing broker LaunchAgent"
        )
    service_state: str | None = None
    was_active, old_pid, already_managed = (False, None, False)
    if enable:
        probe = _probe_broker_service_state(kind, name, launcher)
        service_state = getattr(probe, "status", _SERVICE_ACTIVE if probe[0] else _SERVICE_INACTIVE)
        was_active, old_pid, already_managed = probe[0], probe[1], probe[2]
        needs_restart = was_active and (content_changed or not already_managed)
        if needs_restart:
            if type(old_pid) is not int or old_pid <= 0:
                raise ValueError("Refusing to migrate running broker service without a known MainPID")
            if (state / "bridge.sqlite").exists() or (state / "broker.sock").exists():
                snapshot = busy_snapshot(state, broker_pid=old_pid, hot=False, check_inflight=True)
                if snapshot["busy"]:
                    raise ValueError(
                        "Refusing to migrate running broker service while busy: "
                        + ", ".join(snapshot["reasons"])
                    )
    return {
        "root": resolved_root,
        "kind": kind,
        "name": name,
        "launcher": launcher,
        "service_path": service_path,
        "content": content,
        "content_changed": content_changed,
        "service_state": service_state,
        "service_present": service_state != _SERVICE_ABSENT if service_state is not None else None,
        "was_active": was_active,
        "old_pid": old_pid,
        "already_managed": already_managed,
    }


def _snapshot_file(path: Path) -> tuple[Path, bool, bytes | None, int | None]:
    if path.is_symlink():
        raise ValueError(f"Refusing to snapshot symlinked file: {path}")
    if not path.exists():
        return path, False, None, None
    if not path.is_file():
        raise ValueError(f"Expected regular file: {path}")
    info = path.lstat()
    return path, True, path.read_bytes(), stat.S_IMODE(info.st_mode)


def _snapshot_broker_files(root: Path, service_path: Path) -> list[tuple[Path, bool, bytes | None, int | None]]:
    _, _, entries = _expected_launchers(root)
    launchers = [command for _, command in entries.values() if not command.exists()]
    return [_snapshot_file(path) for path in (service_path, *launchers)]


def _restore_file_snapshot(snapshot: tuple[Path, bool, bytes | None, int | None]) -> None:
    path, existed, data, mode = snapshot
    if existed:
        _write_bytes(path, data or b"", mode if mode is not None else 0o600)
    else:
        path.unlink(missing_ok=True)


def install_broker_service(
    state: Path,
    *,
    root: Path | None = None,
    service_kind: str | None = None,
    service_name: str | None = None,
    enable: bool = False,
    force: bool = False,
    on_write: Any = None,
) -> dict[str, object]:
    """Create or migrate the user-level broker service pointing to the managed launcher."""
    plan = validate_broker_service_install(
        state,
        root=root,
        service_kind=service_kind,
        service_name=service_name,
        enable=enable,
        force=force,
    )
    resolved_root: Path = plan["root"]
    kind: str = plan["kind"]
    name: str = plan["name"]
    launcher: str = plan["launcher"]
    service_path: Path = plan["service_path"]
    content: str = plan["content"]
    enabled_link = service_path.parent / "default.target.wants" / service_path.name if enable and kind == "systemd" else None
    old_enabled_target = None
    if enabled_link is not None:
        if enabled_link.parent.is_symlink() or (enabled_link.parent.exists() and not enabled_link.parent.is_dir()):
            raise ValueError("Invalid systemd enablement directory")
        if enabled_link.exists() and not enabled_link.is_symlink():
            raise ValueError("Expected a systemd enablement symlink")
        if enabled_link.is_symlink():
            old_enabled_target = os.readlink(enabled_link)

    def _write_service_files() -> None:
        state.mkdir(parents=True, mode=0o700, exist_ok=True)
        _private(state, directory=True)
        resolved_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        _private(resolved_root, directory=True)
        _ensure_launchers(resolved_root)
        service_path.parent.mkdir(parents=True, exist_ok=True)
        _write(service_path, content)
        if on_write is not None:
            on_write("broker_service")

    def _rollback(
        snapshots: list[tuple[Path, bool, bytes | None, int | None]],
        *,
        was_active: bool,
        service_existed: bool,
        reload_attempted: bool,
        restart_attempted: bool,
        start_attempted: bool,
        bootout_succeeded: bool,
        bootstrap_succeeded: bool,
    ) -> None:
        errors: list[BaseException] = []
        for snapshot in snapshots:
            try:
                _restore_file_snapshot(snapshot)
            except (OSError, ValueError) as error:
                raise UpdateUncertain("Broker service files could not be restored") from error
        if enabled_link is not None:
            try:
                if enabled_link.exists() and not enabled_link.is_symlink():
                    raise ValueError("Systemd enablement link changed to a regular file")
                if old_enabled_target is None:
                    enabled_link.unlink(missing_ok=True)
                elif not enabled_link.is_symlink() or os.readlink(enabled_link) != old_enabled_target:
                    temporary = enabled_link.with_name(".restore-" + uuid.uuid4().hex)
                    try:
                        temporary.symlink_to(old_enabled_target)
                        os.replace(temporary, enabled_link)
                    finally:
                        temporary.unlink(missing_ok=True)
            except (OSError, ValueError) as error:
                raise UpdateUncertain("Systemd enablement could not be restored") from error
        if kind == "systemd":
            if reload_attempted or restart_attempted or start_attempted:
                try:
                    _run(["systemctl", "--user", "daemon-reload"], timeout=15)
                except UpdateUncertain:
                    raise
                except (OSError, RuntimeError) as error:
                    raise UpdateUncertain("Rollback service reload could not be confirmed") from error
            try:
                if was_active and restart_attempted and service_existed:
                    _run(["systemctl", "--user", "restart", service_path.name], timeout=45)
                elif not was_active and start_attempted:
                    _run(["systemctl", "--user", "stop", service_path.name], timeout=15)
            except UpdateUncertain:
                raise
            except (OSError, RuntimeError) as error:
                errors.append(error)
        else:
            label = _launchd_label(name)
            try:
                if was_active and bootout_succeeded and service_existed:
                    if bootstrap_succeeded:
                        _run(["launchctl", "bootout", f"gui/{os.getuid()}/{label}"], timeout=15)
                    _run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(service_path)], timeout=15)
                elif not was_active and bootstrap_succeeded:
                    _run(["launchctl", "bootout", f"gui/{os.getuid()}/{label}"], timeout=15)
            except UpdateUncertain:
                raise
            except (OSError, RuntimeError) as error:
                errors.append(error)
        if errors:
            raise UpdateUncertain("Broker service rollback could not be confirmed") from errors[0]
        if reload_attempted or restart_attempted or start_attempted or bootout_succeeded or bootstrap_succeeded:
            try:
                restored = _probe_broker_service_state(kind, name, launcher)
            except (OSError, RuntimeError) as error:
                raise UpdateUncertain("Previous broker service state could not be confirmed") from error
            if restored[0] != was_active:
                raise UpdateUncertain("Previous broker service state was not restored")

    restarted = False
    verified_pid: int | None = None
    verified_version: str | None = None
    if enable:
        was_active: bool = bool(plan["was_active"])
        old_pid: int | None = plan["old_pid"]
        needs_restart = was_active and (bool(plan["content_changed"]) or not bool(plan["already_managed"]))
        if needs_restart:
            if type(old_pid) is not int or old_pid <= 0:
                raise RuntimeError("Refusing to restart broker service without a known MainPID")
            token = _acquire_migration_lease(state, old_pid)
            try:
                if (state / "bridge.sqlite").exists() or (state / "broker.sock").exists():
                    snapshot = busy_snapshot(state, broker_pid=old_pid, hot=False, check_inflight=True)
                    if snapshot["busy"]:
                        raise ValueError(
                            "Refusing to migrate running broker service while busy: "
                            + ", ".join(snapshot["reasons"])
                        )
                snapshots = _snapshot_broker_files(resolved_root, service_path)
                service_existed = snapshots[0][1]
                reload_attempted = restart_attempted = False
                bootout_succeeded = bootstrap_succeeded = False
                try:
                    _write_service_files()
                    if kind == "systemd":
                        unit_name = service_path.name
                        reload_attempted = True
                        _run(["systemctl", "--user", "daemon-reload"], timeout=15)
                        _run(["systemctl", "--user", "enable", unit_name], timeout=15)
                        restart_attempted = True
                        _run(["systemctl", "--user", "restart", unit_name], timeout=45)
                    else:
                        label = _launchd_label(name)
                        _run(["launchctl", "bootout", f"gui/{os.getuid()}/{label}"], timeout=15)
                        bootout_succeeded = True
                        _run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(service_path)], timeout=15)
                        bootstrap_succeeded = True
                    restarted = True
                    verified_pid, verified_version = _verify_migrated_broker(
                        state,
                        resolved_root,
                        kind,
                        name,
                        launcher,
                        previous_pid=old_pid,
                        restarted=True,
                    )
                except BaseException as error:
                    if type(error) not in {UpdateUncertain, KeyboardInterrupt, SystemExit}:
                        try:
                            _rollback(
                                snapshots,
                                was_active=was_active,
                                service_existed=service_existed,
                                reload_attempted=reload_attempted,
                                restart_attempted=restart_attempted,
                                start_attempted=False,
                                bootout_succeeded=bootout_succeeded,
                                bootstrap_succeeded=bootstrap_succeeded,
                            )
                        except UpdateUncertain:
                            raise
                        except RuntimeError as rollback_error:
                            raise RuntimeError(f"{error}; {rollback_error}") from error
                    raise
            finally:
                if sys.exc_info()[0] not in {UpdateUncertain, KeyboardInterrupt, SystemExit}:
                    from .broker import request

                    try:
                        finished = asyncio.run(request(state, "agent_update_finish", {"token": token}))
                        if not isinstance(finished, dict) or finished.get("maintenance"):
                            raise RuntimeError("Maintenance was not released")
                    except (OSError, TimeoutError, BridgeError, RuntimeError) as error:
                        raise UpdateUncertain("Maintenance release could not be confirmed") from error
        else:
            snapshots = _snapshot_broker_files(resolved_root, service_path)
            service_existed = snapshots[0][1]
            reload_attempted = restart_attempted = start_attempted = False
            bootstrap_succeeded = False
            try:
                _write_service_files()
                if kind == "systemd":
                    unit_name = service_path.name
                    reload_attempted = True
                    _run(["systemctl", "--user", "daemon-reload"], timeout=15)
                    _run(["systemctl", "--user", "enable", unit_name], timeout=15)
                    if not was_active:
                        start_attempted = True
                        _run(["systemctl", "--user", "start", unit_name], timeout=15)
                elif not was_active:
                    _run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(service_path)], timeout=15)
                    bootstrap_succeeded = True
                verified_pid, verified_version = _verify_migrated_broker(
                    state,
                    resolved_root,
                    kind,
                    name,
                    launcher,
                    previous_pid=old_pid,
                    restarted=False,
                )
            except BaseException as error:
                if type(error) not in {UpdateUncertain, KeyboardInterrupt, SystemExit}:
                    try:
                        _rollback(
                            snapshots,
                            was_active=was_active,
                            service_existed=service_existed,
                            reload_attempted=reload_attempted,
                            restart_attempted=restart_attempted,
                            start_attempted=start_attempted,
                            bootout_succeeded=False,
                            bootstrap_succeeded=bootstrap_succeeded,
                        )
                    except UpdateUncertain:
                        raise
                    except RuntimeError as rollback_error:
                        raise RuntimeError(f"{error}; {rollback_error}") from error
                raise
    else:
        snapshots = _snapshot_broker_files(resolved_root, service_path)
        try:
            _write_service_files()
        except BaseException as error:
            if type(error) not in {UpdateUncertain, KeyboardInterrupt, SystemExit}:
                try:
                    _rollback(
                        snapshots,
                        was_active=False,
                        service_existed=snapshots[0][1],
                        reload_attempted=False,
                        restart_attempted=False,
                        start_attempted=False,
                        bootout_succeeded=False,
                        bootstrap_succeeded=False,
                    )
                except UpdateUncertain:
                    raise
                except RuntimeError as rollback_error:
                    raise RuntimeError(f"{error}; {rollback_error}") from error
            raise

    result: dict[str, object] = {
        "service_kind": kind,
        "service_name": name,
        "service_path": str(service_path),
        "launcher": launcher,
        "enabled": enable,
        "restarted": restarted,
    }
    if enable:
        result["pid"] = verified_pid
        result["version"] = verified_version
        result["service_managed"] = True
    return result


def _has_protected_update_status(state: Path) -> bool:
    """Return True if update-status.json holds an in-progress or uncertain recovery journal."""
    path = state / "update-status.json"
    if not path.exists():
        return False
    try:
        if path.is_symlink():
            return True
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return True
    return isinstance(data, dict) and data.get("status") in {"activating", "uncertain"}


def _record_cli_failure(state: Path, result: dict[str, object]) -> None:
    try:
        root = Path(load_settings(state)["root"])
        _private(root, directory=True)
        with _acquire_update_lock(root):
            if _has_protected_update_status(state):
                return
            _private(state, directory=True)
            _write(state / "update-status.json", {**result, "checked_at": time.time()})
    except (OSError, ValueError):
        return


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
    parser.add_argument(
        "--drain-workers",
        action="store_true",
        help="Wait until all live workers and active tasks stop before switching versions",
    )
    parser.add_argument("action", choices=["check", "run", "install", "bootstrap"])
    args = parser.parse_args(argv)
    from .config import state_directory

    record_cli_failure = False
    try:
        state = state_directory(str(args.state) if args.state else None)
        if not 0 <= args.jitter <= 300:
            raise ValueError("Jitter must be between 0 and 300 seconds")
        if args.jitter:
            time.sleep(secrets.randbelow(args.jitter + 1))
        if args.action == "install":
            record_cli_failure = True
            result = install(
                state,
                args.repository,
                args.service_kind,
                args.service_name,
                args.root,
                args.interval,
                args.enable,
            )
        elif args.action == "run":
            record_cli_failure = True
            result = Updater(state).run(drain_workers=args.drain_workers)
        else:
            record_cli_failure = True
            updater = Updater(state)
            result = getattr(updater, args.action)()
    except (OSError, ValueError, RuntimeError, BridgeError, http.client.HTTPException) as error:
        # Error class is sufficient for logs; never echo credential or URL diagnostics.
        result = {"status": "failed", "error_type": type(error).__name__}
        if record_cli_failure and "state" in locals() and not isinstance(error, BlockingIOError):
            _record_cli_failure(state, result)
    print(json.dumps(result, ensure_ascii=False))
    if result.get("status") in {"failed", "uncertain", "rolled_back"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
