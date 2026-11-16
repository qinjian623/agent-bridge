"""Install checked GitHub or Tailscale/HTTPS mirror wheels; drain and hot-swap the broker while compatible workers run.

Mirror trust boundary: without independent artifact signatures, SHA256SUMS detects corruption and filename
mismatch, while Tailscale routing/ACLs or HTTPS TLS protect transport and the mirror host is trusted as publisher.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import http.client
import ipaddress
import json
import os
import re
import secrets
import shlex
import shutil
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from xml.sax.saxutils import escape

from .errors import BridgeError

REPOSITORY = "qinjian623/cli-agent-bridge"
TAG = re.compile(r"v(\d+)\.(\d+)\.(\d+)\Z")
VERSION = re.compile(r"v?(\d+)\.(\d+)\.(\d+)\Z")
NAME = re.compile(r"[A-Za-z0-9_.-]+\Z")
DEFAULT_DRAIN_TIMEOUT = 15.0
LEGACY_WORKER_VERSION = (0, 2, 0)


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


_TAILSCALE_IPV4_NET = ipaddress.ip_network("100.64.0.0/10")
_TAILSCALE_IPV6_NET = ipaddress.ip_network("fd7a:115c:a1e0::/48")


def _is_tailscale_or_loopback_ip(raw_ip: str) -> bool:
    host = raw_ip.strip().split("%", 1)[0]
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        addr = addr.ipv4_mapped
    if addr.is_loopback:
        return True
    if isinstance(addr, ipaddress.IPv4Address):
        return addr in _TAILSCALE_IPV4_NET
    return addr in _TAILSCALE_IPV6_NET


def _is_tailscale_or_loopback_host(hostname: str) -> bool:
    host = hostname.strip().lower().rstrip(".")
    if not host:
        return False
    if host == "localhost" or host.endswith(".ts.net"):
        return True
    return _is_tailscale_or_loopback_ip(host)


def validate_release_url(url: str) -> str:
    """Validate and normalize an HTTP(S) OTA release base URL."""
    if not isinstance(url, str):
        raise ValueError("Invalid release_url")
    cleaned = url.strip()
    if not cleaned or any(ch.isspace() or ord(ch) < 32 for ch in cleaned):
        raise ValueError("Invalid release_url")
    parsed = urllib.parse.urlsplit(cleaned)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError("release_url must use http or https")
    if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
        raise ValueError("release_url must not contain credentials, query parameters, or fragments")
    host = parsed.hostname
    if not host:
        raise ValueError("Invalid release_url host")
    _ = parsed.port
    if any(seg == ".." for seg in parsed.path.split("/")):
        raise ValueError("release_url path must not contain '..'")
    if scheme == "http" and not _is_tailscale_or_loopback_host(host):
        raise ValueError("Plain HTTP release URLs are restricted to Tailscale or loopback hosts")
    return cleaned.rstrip("/")


class _SameOriginRedirectHandler(urllib.request.HTTPRedirectHandler):
    max_redirections = 3

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


class _SameOriginRedirectHandler(urllib.request.HTTPRedirectHandler):
    max_redirections = 3

    def __init__(self, expected_scheme: str, expected_host: str, expected_port: int, guard: _DownloadDeadline) -> None:
        self._expected_scheme = expected_scheme
        self._expected_host = expected_host
        self._expected_port = expected_port
        self._guard = guard

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = validate_release_url(newurl)
        parsed = urllib.parse.urlsplit(target)
        scheme = parsed.scheme.lower()
        host = (parsed.hostname or "").lower()
        port = parsed.port or (443 if scheme == "https" else 80)
        if scheme != self._expected_scheme:
            raise ValueError("Refusing cross-scheme release redirect")
        if host != self._expected_host or port != self._expected_port:
            raise ValueError("Refusing cross-origin release redirect")
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


def _open_guarded_socket(
    host: str,
    port: int,
    guard: _DownloadDeadline,
    *,
    tailscale_only: bool,
) -> socket.socket:
    guard.check()
    addrs = _resolve_addrs_with_deadline(host, port, guard)
    if not addrs:
        raise OSError("Could not resolve mirror host")
    if tailscale_only:
        for _, _, _, _, sockaddr in addrs:
            if not _is_tailscale_or_loopback_ip(str(sockaddr[0])):
                raise ValueError("Plain HTTP release host resolved outside Tailscale or loopback ranges")
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
    raise OSError("Failed to connect to mirror host")


class _VerifiedTailscaleHTTPConnection(http.client.HTTPConnection):
    """HTTPConnection pinning DNS results to Tailscale/loopback IPs under the download deadline."""

    def __init__(self, host: str, *, guard: _DownloadDeadline, **kwargs) -> None:
        super().__init__(host, **kwargs)
        self._guard = guard

    def connect(self):
        self.sock = _open_guarded_socket(self.host, self.port, self._guard, tailscale_only=True)


class _DeadlineHTTPSConnection(http.client.HTTPSConnection):
    """HTTPSConnection enforcing the shared download deadline across TCP connect and TLS handshake."""

    def __init__(self, host: str, *, guard: _DownloadDeadline, **kwargs) -> None:
        super().__init__(host, **kwargs)
        self._guard = guard

    def connect(self):
        raw = _open_guarded_socket(self.host, self.port, self._guard, tailscale_only=False)
        wrapped = None
        try:
            raw.settimeout(self._guard.check())
            wrapped = self._context.wrap_socket(raw, server_hostname=self.host, do_handshake_on_connect=False)
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


class _BoundedHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, guard: _DownloadDeadline) -> None:
        super().__init__()
        self._guard = guard

    def http_open(self, req):
        req.timeout = self._guard.check()
        return self.do_open(lambda host, **kw: _VerifiedTailscaleHTTPConnection(host, guard=self._guard, **kw), req)


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
    """Download a bounded release payload within a total wall-clock deadline."""
    clean = validate_release_url(url)
    parsed = urllib.parse.urlsplit(clean)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    port = parsed.port or (443 if scheme == "https" else 80)
    with _DownloadDeadline(timeout) as guard:
        handlers: list[urllib.request.BaseHandler] = [
            _SameOriginRedirectHandler(scheme, host, port, guard),
            _BoundedHTTPHandler(guard),
        ]
        if _is_tailscale_or_loopback_host(host):
            handlers.append(urllib.request.ProxyHandler({}))
        if scheme == "https":
            handlers.append(_BoundedHTTPSHandler(guard, context=ssl.create_default_context()))
        opener = urllib.request.build_opener(*handlers)
        request = urllib.request.Request(clean, headers={"Accept": "application/json, application/octet-stream"})
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
    if not isinstance(value, dict) or not required <= set(value) <= (required | {"release_url"}):
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
    if "release_url" in value:
        validate_release_url(value["release_url"])
    _private(Path(value["root"]), directory=True)
    return value


def build_release_manifest(
    tag: str,
    wheel_size: int,
    checksums_size: int,
    *,
    release_id: int | None = None,
) -> dict[str, object]:
    """Build and validate a release mirror manifest (`latest.json`) sharing the release parser contract."""
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


def latest_release(repository, *, release_url: str | None = None):
    if release_url is not None:
        base = validate_release_url(release_url)
        raw = _download_bytes(f"{base}/latest.json", max_bytes=262_144, timeout=30)
        data = json.loads(raw.decode("utf-8"))
        return _parse_release_payload(data, require_positive_id=False)
    data = json.loads(
        _run(["gh", "api", "--hostname", "github.com", f"repos/{repository}/releases/latest"], timeout=30)
    )
    return _parse_release_payload(data, require_positive_id=True)


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
        release_url = self.settings.get("release_url")
        if release_url is not None:
            release = latest_release(self.settings["repository"], release_url=release_url)
        else:
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
                release_url = self.settings.get("release_url")
                if release_url is not None:
                    base = validate_release_url(release_url)
                    (download / expected_wheel).write_bytes(
                        _download_bytes(f"{base}/releases/{release['tag']}/{expected_wheel}", max_bytes=100_000_000, timeout=120)
                    )
                    (download / "SHA256SUMS").write_bytes(
                        _download_bytes(f"{base}/releases/{release['tag']}/SHA256SUMS", max_bytes=65_536, timeout=30)
                    )
                else:
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
                            expected_wheel,
                            "--pattern",
                            "SHA256SUMS",
                        ],
                        timeout=120,
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
                self._switch(previous)
                try:
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
    state: Path,
    repository=REPOSITORY,
    service_kind=None,
    service_name=None,
    root=None,
    interval=3600,
    enable=False,
    *,
    release_url: str | None = None,
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
    if release_url is not None:
        settings["release_url"] = validate_release_url(release_url)
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


def set_release_url(state: Path, release_url: str | None):
    """Set or clear the HTTP(S) OTA mirror URL in private update settings.

    Upgrade hosts running versions older than 0.2.11 before adding ``release_url``, because older
    ``load_settings()`` parsers reject unrecognized keys in ``update.json``.
    """
    updater = Updater(state)
    normalized = validate_release_url(release_url) if release_url else None
    with updater._locked():
        current = load_settings(state)
        updated = dict(current)
        if normalized is not None:
            updated["release_url"] = normalized
        else:
            updated.pop("release_url", None)
        _write(state / "update.json", updated)
        load_settings(state)
    return {"status": "configured", "release_url": updated.get("release_url")}


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
    parser.add_argument("--release-url", help="Optional Tailscale/HTTPS OTA release mirror base URL")
    parser.add_argument(
        "--drain-workers",
        action="store_true",
        help="Wait until all live workers and active tasks stop before switching versions",
    )
    parser.add_argument("action", choices=["check", "run", "install", "bootstrap", "set-url"])
    args = parser.parse_args(argv)
    from .config import state_directory

    record_cli_failure = False
    try:
        state = state_directory(str(args.state) if args.state else None)
        if args.release_url is not None and args.action not in {"install", "set-url"}:
            raise ValueError("--release-url is only valid with install or set-url")
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
                release_url=args.release_url,
            )
        elif args.action == "set-url":
            if args.release_url is None:
                raise ValueError("--release-url is required for set-url")
            result = set_release_url(state, args.release_url)
        elif args.action == "run":
            record_cli_failure = True
            result = Updater(state).run(drain_workers=args.drain_workers)
        else:
            record_cli_failure = True
            updater = Updater(state)
            result = getattr(updater, args.action)()
    except (OSError, ValueError, RuntimeError, BridgeError, http.client.HTTPException) as error:
        # Error class is sufficient for logs; never echo gh/credential/URL diagnostics.
        result = {"status": "failed", "error_type": type(error).__name__}
        if record_cli_failure and "state" in locals() and not isinstance(error, BlockingIOError):
            _record_cli_failure(state, result)
    print(json.dumps(result, ensure_ascii=False))
    if result.get("status") in {"failed", "uncertain", "rolled_back"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
