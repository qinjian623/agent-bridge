"""Node identity file interface (design 4.2). The file lives outside SQLite and its backups."""
from __future__ import annotations

import json
import os
import secrets
import stat
import tempfile
import threading
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol

from .errors import BridgeError
from .peer_protocol import NodeRef, NodeState, PeerErrorCode

IDENTITY_FILENAME = "node_identity.json"  # mode 0600, next to config.json, excluded from DB backups
IDENTITY_FILE_MODE = 0o600
GENERATION_HEX_BYTES = 16  # state_generation is a random hex string of 2 * this many characters


@dataclass(frozen=True, slots=True)
class NodeIdentity:
    node_id: str  # random UUID, created once by `agent-bridge init`
    state_generation: str  # regenerated on rebuild/restore/re-init
    max_confirmed_seq: int  # highest operation sequence acknowledged to any peer
    state: NodeState = NodeState.READY
    schema_version: int = 1


class NodeIdentityStore(Protocol):
    def path(self) -> Path: ...

    def load_or_create(self) -> NodeIdentity:
        """Create with fresh node_id/generation if absent. Refuse files that are not 0600 or not owned by us."""

    def advance(self, seq: int) -> NodeIdentity:
        """Atomically (write temp, fsync, rename, fsync dir) raise max_confirmed_seq. Never lowers it.
        Called after the SQLite commit and before the ACK is returned to the peer."""

    def verify_database(self, db_max_seq: int) -> NodeIdentity:
        """db_max_seq < max_confirmed_seq means rollback: persist state=state_corrupted and raise
        BridgeError(peer_state_corrupted)."""

    def rotate_generation(self, confirm: str) -> NodeIdentity:
        """Administrator-only recovery from state_corrupted; `confirm` must equal the current node_id."""

    def node_ref(self, host_id: str) -> NodeRef: ...


class FileNodeIdentityStore:
    """File-backed `NodeIdentityStore` (design 4.2).

    Every read re-checks ownership and the 0600 mode so a permission change or a replacement file is
    noticed immediately. Writes go to a temporary file in the same directory, are fsynced, atomically
    renamed over the target, and the containing directory is fsynced.
    """

    def __init__(self, state_dir: str | Path) -> None:
        self._state_dir = Path(state_dir).expanduser()
        self._lock = threading.RLock()

    def path(self) -> Path:
        return self._state_dir / IDENTITY_FILENAME

    def load_or_create(self) -> NodeIdentity:
        with self._lock:
            return self._load_or_create_locked()

    def advance(self, seq: int) -> NodeIdentity:
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "max_confirmed_seq must be a non-negative integer")
        with self._lock:
            current = self._load_or_create_locked()
            if seq <= current.max_confirmed_seq:
                return current
            updated = replace(current, max_confirmed_seq=seq)
            self._write(updated)
            return updated

    def verify_database(self, db_max_seq: int) -> NodeIdentity:
        if isinstance(db_max_seq, bool) or not isinstance(db_max_seq, int) or db_max_seq < 0:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, "db_max_seq must be a non-negative integer")
        with self._lock:
            current = self._load_or_create_locked()
            if db_max_seq >= current.max_confirmed_seq:
                return current
            message = (
                f"Database rollback detected: database max seq {db_max_seq} is below "
                f"confirmed seq {current.max_confirmed_seq}"
            )
            if current.state is not NodeState.STATE_CORRUPTED:
                corrupted = replace(current, state=NodeState.STATE_CORRUPTED)
                self._write(corrupted)
            raise BridgeError(PeerErrorCode.STATE_CORRUPTED, message)

    def rotate_generation(self, confirm: str) -> NodeIdentity:
        with self._lock:
            current = self._load_or_create_locked()
            if confirm != current.node_id:
                raise BridgeError(
                    PeerErrorCode.IDENTITY_MISMATCH,
                    "rotate_generation requires the current node_id as confirmation",
                )
            rotated = replace(
                current,
                state_generation=secrets.token_hex(GENERATION_HEX_BYTES),
                state=NodeState.READY,
            )
            self._write(rotated)
            return rotated

    def node_ref(self, host_id: str) -> NodeRef:
        with self._lock:
            current = self._load_or_create_locked()
            return NodeRef(
                host_id=host_id,
                node_id=current.node_id,
                state_generation=current.state_generation,
            )

    # ---------------------------------------------------------------- internals

    def _load_or_create_locked(self) -> NodeIdentity:
        path = self.path()
        if path.exists():
            identity = self._read(path)
        else:
            identity = NodeIdentity(
                node_id=str(uuid.uuid4()),
                state_generation=secrets.token_hex(GENERATION_HEX_BYTES),
                max_confirmed_seq=0,
            )
            self._write(identity)
        return identity

    def _read(self, path: Path) -> NodeIdentity:
        status = path.stat()
        if status.st_uid != os.getuid():
            raise BridgeError("invalid_identity", f"{IDENTITY_FILENAME} is not owned by the current user")
        if stat.S_IMODE(status.st_mode) != IDENTITY_FILE_MODE:
            raise BridgeError("invalid_identity", f"{IDENTITY_FILENAME} must have mode 0600")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise BridgeError("invalid_identity", f"Cannot read {IDENTITY_FILENAME}: {error}") from error
        return _identity_from_json(data)

    def _write(self, identity: NodeIdentity) -> None:
        directory = self._state_dir
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        payload = json.dumps(
            {
                "schema_version": identity.schema_version,
                "node_id": identity.node_id,
                "state_generation": identity.state_generation,
                "max_confirmed_seq": identity.max_confirmed_seq,
                "state": identity.state.value,
            },
            sort_keys=True,
        ) + "\n"
        descriptor, temporary = tempfile.mkstemp(prefix=f".{IDENTITY_FILENAME}.", dir=directory)
        try:
            os.fchmod(descriptor, IDENTITY_FILE_MODE)
            with os.fdopen(descriptor, "w", encoding="utf-8") as file:
                file.write(payload)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path())
            _fsync_directory(directory)
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise


def _identity_from_json(data: object) -> NodeIdentity:
    if not isinstance(data, dict):
        raise BridgeError("invalid_identity", f"{IDENTITY_FILENAME} must contain a JSON object")
    node_id = data.get("node_id")
    if not isinstance(node_id, str) or not node_id:
        raise BridgeError("invalid_identity", f"{IDENTITY_FILENAME} has an invalid node_id")
    generation = data.get("state_generation")
    if not isinstance(generation, str) or not generation:
        raise BridgeError("invalid_identity", f"{IDENTITY_FILENAME} has an invalid state_generation")
    seq = data.get("max_confirmed_seq", 0)
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
        raise BridgeError("invalid_identity", f"{IDENTITY_FILENAME} has an invalid max_confirmed_seq")
    schema_version = data.get("schema_version", 1)
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version < 1:
        raise BridgeError("invalid_identity", f"{IDENTITY_FILENAME} has an invalid schema_version")
    try:
        state = NodeState(data.get("state", NodeState.READY.value))
    except (TypeError, ValueError) as error:
        raise BridgeError("invalid_identity", f"{IDENTITY_FILENAME} has an invalid state") from error
    return NodeIdentity(
        node_id=node_id,
        state_generation=generation,
        max_confirmed_seq=seq,
        state=state,
        schema_version=schema_version,
    )


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
