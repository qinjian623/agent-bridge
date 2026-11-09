"""Short, process-shared maintenance leases; existing work is never interrupted."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import math
import os
import secrets
import stat
import tempfile
import time
from pathlib import Path

from .errors import BridgeError


@contextlib.contextmanager
def _lock(state: Path, exclusive=False):
    descriptor = os.open(state / "update-gate.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise BridgeError("update_busy", "Update gate is not a private owned file")
        try:
            fcntl.flock(descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise BridgeError("update_busy", "Bridge operations are still active") from error
        yield
    finally:
        os.close(descriptor)


def maintenance(state: Path):
    path = state / "update-maintenance.json"
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise BridgeError("update_busy", "Maintenance lease cannot be read safely") from error
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 2048:
            raise BridgeError("update_busy", "Maintenance lease is not a private owned file")
        value = json.loads(os.read(descriptor, 2049))
        expiry = value["expires_at"]
        digest = value["token_hash"]
        if (
            isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not math.isfinite(expiry)
            or not isinstance(digest, str) or len(digest) != 64
        ):
            raise ValueError("Invalid lease")
        return value if expiry > time.time() else None
    except (KeyError, TypeError, ValueError) as error:
        raise BridgeError("update_busy", "Maintenance lease is invalid; inspect before updating") from error
    finally:
        os.close(descriptor)


@contextlib.contextmanager
def operation(state: Path, *, readonly=False):
    with _lock(state):
        if maintenance(state) is not None and not readonly:
            raise BridgeError("update_busy", "Bridge update is preparing; retry after maintenance")
        yield


def prepare(state: Path, busy_snapshot, *, ttl=180):
    if type(ttl) is not int or not 10 <= ttl <= 300:
        raise BridgeError("invalid_request", "Maintenance TTL must be 10 to 300 seconds")
    with _lock(state, exclusive=True):
        if maintenance(state) is not None:
            raise BridgeError("update_busy", "Another update already holds maintenance")
        status = busy_snapshot(state)
        if status["busy"]:
            raise BridgeError("update_busy", "Bridge is busy: " + "; ".join(status["reasons"]))
        token = secrets.token_urlsafe(32)
        expiry = time.time() + ttl
        value = {"token_hash": hashlib.sha256(token.encode()).hexdigest(), "expires_at": expiry}
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=state, prefix=".update-lease-", delete=False) as output:
                temporary = Path(output.name)
                os.chmod(temporary, 0o600)
                json.dump(value, output)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, state / "update-maintenance.json")
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)
        return {"token": token, "expires_at": expiry}


def finish(state: Path, token):
    if not isinstance(token, str) or not token:
        raise BridgeError("invalid_request", "Maintenance token is required")
    with _lock(state):
        value = maintenance(state)
        if value is None:
            return {"maintenance": False}
        if not secrets.compare_digest(value["token_hash"], hashlib.sha256(token.encode()).hexdigest()):
            raise BridgeError("forbidden", "Maintenance token does not match")
        (state / "update-maintenance.json").unlink()
        return {"maintenance": False}
