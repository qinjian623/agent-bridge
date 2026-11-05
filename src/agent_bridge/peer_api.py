"""Coordinator-side orchestration interfaces and new/extended MCP tool contracts (design 7 and 8)."""
from __future__ import annotations

import hashlib
import json
import socket
import sqlite3
import time
from typing import Any, Literal, NotRequired, Protocol, TypedDict

from . import __version__
from .config import Profile
from .errors import BridgeError
from .node_identity import NodeIdentityStore
from .peer_protocol import (
    CAPABILITIES,
    INLINE_RESULT_BYTES,
    MAX_PAGE_LIMIT,
    PROTOCOL_VERSION,
    ArtifactRef,
    HelloResult,
    NodeRef,
    NodeState,
    PeerDeliveryStatus,
    PeerErrorCode,
    PeerEvent,
    PeerRequest,
    PeerResponse,
    RemoteMember,  # noqa: F401  re-exported so peer_api.RemoteMember is the peer_protocol type
    fail,
    negotiate_version,
    ok,
    peer_error,
    require_method,
)
from .store import encode, validate_session_name

# ---------------------------------------------------------------- MCP tool parameter contracts

class AgentHostsResult(TypedDict):
    hosts: list[dict[str, Any]]  # {host_id,state,node_id,state_generation,last_contact,protocol_version,detail}


class AgentOperationParams(TypedDict):
    operation_id: str
    wait: NotRequired[float]  # seconds, capped by server; 0 = peek


class AgentOperationResult(TypedDict):
    operation_id: str
    host_id: str
    status: str  # OperationStatus
    result: NotRequired[dict[str, Any]]
    artifacts: NotRequired[list[ArtifactRef]]
    error: NotRequired[dict[str, str]]


class AgentDeliveryResolveParams(TypedDict):
    """Admin only. Clears an `uncertain` queue head; never resends."""

    delivery_id: str
    resolution: Literal["received", "not_received"]  # what the evidence shows
    evidence: str  # non-empty
    expected_revision: int  # group revision inspected by the admin
    expected_delivery_revision: int
    expected_remote_revision: int
    request_id: str


class AgentCancelParams(TypedDict):
    """Exactly one of task_id / operation_id / delivery_id. Extends the existing agent_cancel."""

    task_id: NotRequired[str]
    operation_id: NotRequired[str]
    delivery_id: NotRequired[str]
    host_id: NotRequired[str]
    request_id: NotRequired[str]


class RemoteOpenFields(TypedDict):
    """Extra agent_open/agent_dispatch parameters. `cwd` stays a *local* source dir; target is project_id."""

    host_id: NotRequired[str]
    project_id: NotRequired[str]  # required when host_id is set and differs from the local host


CATALOG_HOST_FIELDS = ("host_id", "stale", "fetched_at")  # added to every agent_catalog entry; param: host_id|None


# ---------------------------------------------------------------- coordinator components

class DeliveryScheduler(Protocol):
    """Per-target strictly ordered sender for peer_outbox (design 7.1)."""

    def enqueue(self, *, broadcast_id: str, group_id: str, host_id: str, target_kind: str, target_id: str,
                message: str) -> str:
        """Allocate monotonically increasing target_seq per (host_id, target) inside the broadcast transaction;
        return delivery_id. Group revision snapshot is frozen by the caller."""

    async def pump(self, host_id: str) -> None:
        """Send heads in order. A head in `uncertain` blocks the target (delivery_blocked) until resolved.
        Offline host -> waiting_for_host. Retries reuse delivery_id and target_seq."""

    def cancel(self, delivery_id: str) -> PeerDeliveryStatus:
        """Unsent -> turn into a tombstone that keeps target_seq; later seqs wait for tombstone ACK."""

    def resolve(self, params: AgentDeliveryResolveParams) -> dict[str, Any]:
        """Calls peer.resolve_delivery with expected_delivery_revision; moves resolving -> released/failed."""


class OutboxPuller(Protocol):
    """Pulls node_outbox every HostConfig.pull_interval (design 2.2). Authorisation happens here, not on the node."""

    async def run_once(self, host_id: str) -> int:
        """peer.pull_requests -> verify Binding/group membership/revision -> execute locally ->
        peer.ack_request. Returns processed item count. Must be idempotent per operation_id.
        Advances its cursor only to PullRequestsResult.scan_upper_seq, so skipped rows are not rescanned."""


class EventMirror(Protocol):
    """Maps remote events into coordinator-local `events.seq` (ADR-10, ADR-16)."""

    def mirror(self, node: NodeRef, event: PeerEvent, parent_ref: dict[str, str]) -> tuple[int, bool]:
        """Dedupe on (node, event.seq). Returns (local_seq, duplicate). Never moves cursors backwards."""

    def cursor(self, node: NodeRef) -> int: ...

    def route_parent(self, node: NodeRef, parent_ref: dict[str, str]) -> None:
        """Raises parent_route_conflict when parent_ref is already bound to a different route."""


class OperationTracker(Protocol):
    """Stable operation ids for cross-host requests; backs agent_operation and api_requests references (ADR-06)."""

    def begin(self, *, host_id: str, request_id: str, method: str, params: dict[str, Any]) -> str:
        """Return the existing operation_id for the same (request_id, params); conflict otherwise."""

    async def refresh(self, operation_id: str) -> AgentOperationResult:
        """Resolve unknown outcomes via peer.lookup_delivery before reporting a terminal state."""


# ---------------------------------------------------------------- execution-node service (phase A3/A4)

IMPLEMENTED_PEER_METHODS = (
    "peer.hello",
    "peer.inspect",
    "peer.catalog",
    "peer.deliver",
    "peer.lookup_delivery",
    "peer.cancel",
    "peer.result",
    "peer.events",
)


class DirectPeerService:
    """Execution-node `PeerService` handler behind `agent-bridge relay`.

    Phase A implements the handshake (`peer.hello`) and basic node inspection (`peer.inspect`). Phase B adds
    catalog reads plus the mutating delivery/cancel contract and the result/event read paths. Whitelisted
    methods without a handler are rejected with `peer_not_implemented`; unknown methods never get past
    `require_method`.
    """

    def __init__(
        self,
        identity_store: NodeIdentityStore,
        *,
        host_id: str | None = None,
        store: Any = None,
        config: Any = None,
        broker_version: str = __version__,
    ) -> None:
        self._identity = identity_store
        self._host_id = host_id if host_id is not None else socket.gethostname()
        self._store = store
        self._config = config
        self._broker_version = broker_version

    async def handle(self, request: PeerRequest) -> PeerResponse:
        if not isinstance(request, dict):
            return fail("", peer_error(PeerErrorCode.INVALID_PARAMS, "Peer request must be a JSON object"))
        raw_id = request.get("id")
        request_id = raw_id if isinstance(raw_id, str) else ""
        try:
            envelope_version = request.get("v")
            if envelope_version is not None and envelope_version != PROTOCOL_VERSION:
                raise peer_error(
                    PeerErrorCode.VERSION_MISMATCH,
                    f"Unsupported peer envelope version: {envelope_version!r}",
                )
            spec = require_method(request.get("method"))
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "Peer params must be a JSON object")
            if spec.name == "peer.hello":
                result = self._hello(params)
            elif spec.name == "peer.inspect":
                result = self._inspect(params)
            elif spec.name == "peer.catalog":
                result = self._catalog(params)
            elif spec.name == "peer.deliver":
                result = self._deliver(params)
            elif spec.name == "peer.lookup_delivery":
                result = self._lookup_delivery(params)
            elif spec.name == "peer.cancel":
                result = self._cancel(params)
            elif spec.name == "peer.result":
                result = self._result(params)
            elif spec.name == "peer.events":
                result = self._events(params)
            else:
                raise peer_error(
                    PeerErrorCode.NOT_IMPLEMENTED,
                    f"{spec.name} is not implemented by this node yet",
                )
        except BridgeError as error:
            return fail(request_id, error)
        except (TypeError, ValueError, KeyError) as error:
            return fail(request_id, peer_error(PeerErrorCode.INVALID_PARAMS, str(error)))
        return ok(request_id, result)

    # ---------------------------------------------------------------- handlers

    def _hello(self, params: dict[str, Any]) -> HelloResult:
        versions = params.get("protocol_versions")
        if not isinstance(versions, list) or any(
            isinstance(version, bool) or not isinstance(version, int) for version in versions
        ):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "protocol_versions must be a list of integers")
        protocol_version = negotiate_version(versions)

        identity = self._identity.load_or_create()
        last_acked_seq = params.get("last_acked_seq")
        if last_acked_seq is not None:
            if isinstance(last_acked_seq, bool) or not isinstance(last_acked_seq, int) or last_acked_seq < 0:
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "last_acked_seq must be a non-negative integer")
            # ADR-03: a database/identity sequence below the coordinator's last acknowledged sequence means an
            # inconsistent rollback; verify_database persists state_corrupted and raises before we answer.
            identity = self._identity.verify_database(last_acked_seq)

        node = NodeRef(
            host_id=self._host_id,
            node_id=identity.node_id,
            state_generation=identity.state_generation,
        )
        return {
            "protocol_version": protocol_version,
            "node": node.as_dict(),
            "node_state": identity.state.value,
            "max_confirmed_seq": identity.max_confirmed_seq,
            "capabilities": list(IMPLEMENTED_PEER_METHODS) + list(CAPABILITIES),
            "broker_version": self._broker_version,
        }

    def _inspect(self, params: dict[str, Any]) -> dict[str, Any]:
        kind = params.get("kind")
        if not isinstance(kind, str) or not kind:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "kind must be a non-empty string")
        if kind == "node":
            identity = self._identity.load_or_create()
            node = NodeRef(
                host_id=self._host_id,
                node_id=identity.node_id,
                state_generation=identity.state_generation,
            )
            items: list[dict[str, Any]] = [
                {
                    **node.as_dict(),
                    "node_state": identity.state.value,
                    "max_confirmed_seq": identity.max_confirmed_seq,
                    "protocol_version": PROTOCOL_VERSION,
                    "broker_version": self._broker_version,
                }
            ]
        elif kind == "session" and self._store is not None:
            items = list(self._store.sessions())
        elif kind == "task" and self._store is not None:
            task_id = params.get("id")
            items = [self._store.get_task(task_id)] if isinstance(task_id, str) and task_id else []
        elif kind in {"session", "task", "group", "operation"}:
            items = []
        else:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, f"Unknown inspect kind: {kind!r}")
        return {"items": items}

    # ---------------------------------------------------------------- phase B handlers

    @staticmethod
    def _require_store(store: Any) -> Any:
        if store is None:
            raise peer_error(PeerErrorCode.NOT_IMPLEMENTED, "This node has no local store configured")
        return store

    def _ready_identity(self):
        identity = self._identity.load_or_create()
        if identity.state is NodeState.STATE_CORRUPTED:
            raise peer_error(
                PeerErrorCode.STATE_CORRUPTED,
                "Node state is corrupted; mutating peer calls are refused until an administrator rotates it",
            )
        return identity

    @staticmethod
    def _operation_key(params: dict[str, Any]) -> dict[str, str]:
        operation = params.get("operation")
        if not isinstance(operation, dict):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "operation must be an object")
        values: dict[str, str] = {}
        for key in ("node_id", "state_generation", "operation_id"):
            value = operation.get(key)
            if not isinstance(value, str) or not value:
                raise peer_error(PeerErrorCode.INVALID_PARAMS, f"operation.{key} must be a non-empty string")
            values[key] = value
        return values

    @staticmethod
    def _check_operation_identity(operation: dict[str, str], identity) -> None:
        if (
            operation["node_id"] != identity.node_id
            or operation["state_generation"] != identity.state_generation
        ):
            raise peer_error(
                PeerErrorCode.IDENTITY_MISMATCH,
                "operation identity does not match this node's node_id/state_generation",
            )

    @staticmethod
    def _fingerprint(method: str, params: dict[str, Any]) -> str:
        material = encode({"method": method, "params": params})
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    @staticmethod
    def _operation_outcome(operation: dict[str, Any]) -> dict[str, Any]:
        result = operation.get("result")
        if isinstance(result, dict):
            return result
        if operation.get("status") in {"pending", "running", "uncertain"} or result is None:
            error = operation.get("error")
            if error:
                message = error if isinstance(error, str) else json.dumps(error, ensure_ascii=False)
                raise BridgeError("operation_failed", message)
            raise peer_error(
                PeerErrorCode.OPERATION_PENDING,
                f"Operation {operation.get('operation_id')} has not produced a result yet",
            )
        return {"result": result}

    @staticmethod
    def _required_string(params: dict[str, Any], key: str) -> str:
        value = params.get(key)
        if not isinstance(value, str) or not value:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{key} must be a non-empty string")
        return value

    def _catalog(self, params: dict[str, Any]) -> dict[str, Any]:
        fetched_at = time.time()
        name = params.get("profile")
        if name is not None and (not isinstance(name, str) or not name):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "profile must be a non-empty string")
        entries: list[dict[str, Any]] = []
        profiles = getattr(self._config, "profiles", None) if self._config is not None else None
        if isinstance(profiles, dict):
            for profile in profiles.values():
                if name is not None and profile.name != name:
                    continue
                entries.append(profile.public())
        return {"entries": entries, "stale": False, "fetched_at": fetched_at}

    def _profile_from_open(self, spec: dict[str, Any]) -> Profile:
        raw = spec.get("profile")
        if isinstance(raw, dict):
            data = dict(raw)
            name = data.pop("name", None) or data.pop("profile", None)
            if not isinstance(name, str) or not name:
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "open.profile requires a profile name")
            return Profile(name=name, **data).validate()
        if isinstance(raw, str) and raw:
            profiles = getattr(self._config, "profiles", None) if self._config is not None else None
            if isinstance(profiles, dict) and raw in profiles:
                return profiles[raw]
            raise BridgeError("unknown_profile", raw)
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "A bridge delivery without a session_id requires open.profile")

    def _deliver(self, params: dict[str, Any]) -> dict[str, Any]:
        store = self._require_store(self._store)
        operation = self._operation_key(params)
        identity = self._ready_identity()
        self._check_operation_identity(operation, identity)

        delivery_id = self._required_string(params, "delivery_id")
        target = params.get("target")
        if not isinstance(target, dict):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "target must be an object")
        kind = target.get("kind")
        if kind not in {"bridge", "native"}:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "target.kind must be bridge or native")
        target_id = target.get("id", "")
        if not isinstance(target_id, str):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "target.id must be a string")
        target_seq = params.get("target_seq")
        if isinstance(target_seq, bool) or not isinstance(target_seq, int) or target_seq < 0:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "target_seq must be a non-negative integer")
        seq_kind = params.get("seq_kind", "delivery")
        if seq_kind not in {"message", "tombstone", "delivery"}:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "seq_kind must be message, tombstone or delivery")
        message = params.get("message", "")
        if message is None:
            message = ""
        if not isinstance(message, str):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "message must be a string")
        reservation = params.get("reservation", 0)
        if reservation is None:
            reservation = 0
        if type(reservation) is not int or reservation < 0:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "reservation must be a non-negative integer")

        fingerprint = self._fingerprint("peer.deliver", params)
        accepted_at = time.time()
        with store.connection(write=True) as db:
            row = db.execute(
                "SELECT * FROM peer_operations WHERE operation_id=?", (operation["operation_id"],)
            ).fetchone()
            if row is not None:
                existing = store._peer_operation(row)
                if existing["fingerprint"] != fingerprint:
                    raise peer_error(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "operation_id was already used with different delivery parameters",
                    )
                return self._operation_outcome(existing)
            db.execute(
                "INSERT INTO peer_operations(operation_id,node_id,state_generation,fingerprint,method,params,status,created,updated) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    operation["operation_id"],
                    identity.node_id,
                    identity.state_generation,
                    fingerprint,
                    "peer.deliver",
                    encode(params),
                    "running",
                    accepted_at,
                    accepted_at,
                ),
            )
            inbox = db.execute("SELECT * FROM peer_inbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if inbox is None:
                try:
                    db.execute(
                        "INSERT INTO peer_inbox(delivery_id,host_id,node_id,state_generation,target_seq,seq_kind,operation_id,payload,status,receipt,created) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            delivery_id,
                            self._host_id,
                            identity.node_id,
                            identity.state_generation,
                            target_seq,
                            seq_kind,
                            operation["operation_id"],
                            encode(params),
                            "cancelled" if seq_kind == "tombstone" else "received",
                            encode({"accepted_at": accepted_at}),
                            accepted_at,
                        ),
                    )
                except sqlite3.IntegrityError as error:
                    raise peer_error(
                        PeerErrorCode.SEQUENCE_GAP,
                        f"target_seq {target_seq} is already recorded for this node",
                    ) from error
            elif inbox["operation_id"] != operation["operation_id"]:
                raise peer_error(
                    PeerErrorCode.OPERATION_CONFLICT,
                    "delivery_id was already used for a different operation",
                )

            task_id: str | None = None
            session_id: str | None = None
            session_name: str | None = None
            status = "accepted_remote"
            if seq_kind == "tombstone":
                status = "cancelled"
            elif kind == "bridge":
                if target_id:
                    session = db.execute("SELECT * FROM sessions WHERE id=?", (target_id,)).fetchone()
                    if session is None:
                        raise BridgeError("not_found", f"sessions: {target_id}")
                    if session["desired"] == "closed":
                        raise BridgeError("session_closed", "Reopen this session explicitly before submitting work")
                    session_id = target_id
                else:
                    open_spec = params.get("open")
                    if not isinstance(open_spec, dict):
                        raise peer_error(
                            PeerErrorCode.INVALID_PARAMS,
                            "A bridge delivery without a session_id requires open parameters (ADR-13)",
                        )
                    profile = self._profile_from_open(open_spec)
                    cwd = open_spec.get("cwd")
                    if not isinstance(cwd, str) or not cwd:
                        cwd = "."
                    access = open_spec.get("access") or "read-only"
                    workspace = open_spec.get("workspace")
                    session_name = validate_session_name(open_spec.get("name"))
                    session_id, _ = store.create_session(db, profile, cwd, access, None, workspace, session_name)
                if message.strip():
                    queued = store.enqueue_task(
                        db,
                        session_id,
                        message,
                        [],
                        reservation,
                        {
                            "kind": "peer",
                            "operation_id": operation["operation_id"],
                            "delivery_id": delivery_id,
                        },
                    )
                    task_id = queued["task_id"]
            result: dict[str, Any] = {
                "delivery_id": delivery_id,
                "accepted": True,
                "task_id": task_id,
                "session_id": session_id,
                "target_seq": target_seq,
                "receipt": {"accepted_at": accepted_at},
                "status": status,
            }
            if session_name is not None:
                result["name"] = session_name
            db.execute(
                "UPDATE peer_operations SET status=?,result=?,updated=? WHERE operation_id=?",
                ("cancelled" if status == "cancelled" else "completed", encode(result), time.time(), operation["operation_id"]),
            )
        self._identity.advance(target_seq)
        return result

    def _lookup_delivery(self, params: dict[str, Any]) -> dict[str, Any]:
        store = self._require_store(self._store)
        delivery_id = params.get("delivery_id")
        if delivery_id is not None and (not isinstance(delivery_id, str) or not delivery_id):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "delivery_id must be a non-empty string")
        operation = params.get("operation")
        operation_id = None
        if operation is not None:
            if not isinstance(operation, dict):
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "operation must be an object")
            operation_id = operation.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id:
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "operation.operation_id must be a non-empty string")
        if not delivery_id and not operation_id:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "Provide delivery_id or operation")

        inbox = store.get_inbox_delivery(delivery_id) if delivery_id else None
        if inbox is None and operation_id:
            with store.connection() as db:
                row = db.execute(
                    "SELECT delivery_id FROM peer_inbox WHERE operation_id=? ORDER BY created DESC LIMIT 1",
                    (operation_id,),
                ).fetchone()
            if row is not None:
                inbox = store.get_inbox_delivery(row["delivery_id"])
        stored = store.get_operation(operation_id) if operation_id else None
        if stored is None and inbox is not None:
            stored = store.get_operation(inbox["operation_id"])
        if inbox is None and stored is None:
            return {"found": False}

        delivery: dict[str, Any] | None = None
        if stored is not None and isinstance(stored.get("result"), dict):
            delivery = dict(stored["result"])
        elif inbox is not None:
            delivery = {
                "delivery_id": inbox["delivery_id"],
                "status": "accepted_remote" if inbox["status"] == "received" else inbox["status"],
                "task_id": None,
                "session_id": None,
                "target_seq": inbox["target_seq"],
            }
        if delivery is not None and inbox is not None:
            delivery.setdefault("delivery_id", inbox["delivery_id"])
            delivery.setdefault("target_seq", inbox["target_seq"])
        response: dict[str, Any] = {
            "found": True,
            "operation_status": stored["status"] if stored is not None else inbox["status"],
        }
        if delivery is not None:
            response["delivery"] = delivery
        return response

    def _cancel(self, params: dict[str, Any]) -> dict[str, Any]:
        store = self._require_store(self._store)
        operation = self._operation_key(params)
        identity = self._ready_identity()
        self._check_operation_identity(operation, identity)
        targets = {
            key: params.get(key)
            for key in ("task_id", "operation_id", "delivery_id")
            if isinstance(params.get(key), str) and params.get(key)
        }
        if len(targets) != 1:
            raise peer_error(
                PeerErrorCode.INVALID_PARAMS,
                "Exactly one of task_id, operation_id or delivery_id is required",
            )
        fingerprint = self._fingerprint("peer.cancel", params)
        existing = store.get_operation(operation["operation_id"])
        if existing is not None:
            if existing["fingerprint"] != fingerprint:
                raise peer_error(
                    PeerErrorCode.OPERATION_CONFLICT,
                    "operation_id was already used with different cancel parameters",
                )
            return self._operation_outcome(existing)
        store.record_operation(
            operation["operation_id"],
            identity.node_id,
            identity.state_generation,
            fingerprint,
            "peer.cancel",
            params,
            status="running",
        )

        target, value = next(iter(targets.items()))
        task_id: str | None = None
        target_operation_id: str | None = None
        tombstone_seq: int | None = None
        if target == "task_id":
            task_id = value
            try:
                store.get_task(task_id)
            except BridgeError as error:
                raise peer_error(PeerErrorCode.OPERATION_NOT_FOUND, str(error)) from error
        elif target == "delivery_id":
            inbox = store.get_inbox_delivery(value)
            if inbox is None:
                raise peer_error(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown delivery: {value}")
            tombstone_seq = inbox["target_seq"]
            target_operation_id = inbox["operation_id"]
            delivered = store.get_operation(target_operation_id)
            if delivered is not None and isinstance(delivered.get("result"), dict):
                task_id = delivered["result"].get("task_id")
        else:
            target_operation_id = value
            delivered = store.get_operation(value)
            if delivered is None:
                raise peer_error(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown operation: {value}")
            if isinstance(delivered.get("result"), dict):
                task_id = delivered["result"].get("task_id")

        status = "cancelled"
        if task_id:
            request_id = "peer-cancel:" + hashlib.sha256(operation["operation_id"].encode()).hexdigest()[:32]
            store.cancel(task_id, request_id)
            status = store.get_task(task_id)["status"]
        if target_operation_id and target_operation_id != operation["operation_id"]:
            store.update_operation(target_operation_id, status="cancelled")
        result: dict[str, Any] = {"target": value, "status": status}
        if tombstone_seq is not None:
            result["tombstone_seq"] = tombstone_seq
        store.update_operation(operation["operation_id"], status="completed", result=result)
        return result

    def _artifact_refs(self, task_id: str) -> list[dict[str, Any]]:
        store = self._require_store(self._store)
        with store.connection() as db:
            rows = db.execute(
                "SELECT metadata FROM artifacts WHERE task_id=? ORDER BY created DESC LIMIT 20", (task_id,)
            ).fetchall()
        refs: list[dict[str, Any]] = []
        for row in rows:
            metadata = json.loads(row["metadata"])
            refs.append(
                {
                    "host_id": self._host_id,
                    "artifact_id": metadata["artifact_id"],
                    "sha256": metadata["sha256"],
                    "size": metadata["bytes"],
                }
            )
        return refs

    def _result(self, params: dict[str, Any]) -> dict[str, Any]:
        store = self._require_store(self._store)
        task_id = self._required_string(params, "task_id")
        task = store.get_task(task_id)
        artifacts = self._artifact_refs(task_id)
        artifact_id = params.get("artifact_id")
        if artifact_id is not None:
            if not isinstance(artifact_id, str) or not artifact_id:
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "artifact_id must be a non-empty string")
            offset = params.get("offset", 0)
            limit = params.get("limit", 4000)
            chunk = store.read_artifact(task_id, artifact_id, offset, limit)
            return {
                "task_id": task_id,
                "status": task["status"],
                "artifacts": artifacts,
                "chunk": {
                    "artifact_id": artifact_id,
                    "offset": chunk["offset"],
                    "data": chunk["content"],
                    "eof": chunk["eof"],
                },
            }
        result = task.get("result")
        summary: str | None = None
        large = any(ref["size"] > INLINE_RESULT_BYTES for ref in artifacts)
        if isinstance(result, dict):
            answer = result.get("answer")
            value = result.get("summary")
            if isinstance(answer, str):
                text = answer
            elif isinstance(value, str):
                text = value
            else:
                text = encode(result)
        elif isinstance(result, str):
            text = result
        else:
            text = None
        if text is not None:
            if len(text.encode("utf-8")) > INLINE_RESULT_BYTES:
                large = True
            elif not large:
                summary = text
        response: dict[str, Any] = {"task_id": task_id, "status": task["status"], "artifacts": artifacts}
        if summary is not None:
            response["summary"] = summary
        return response

    def _events(self, params: dict[str, Any]) -> dict[str, Any]:
        store = self._require_store(self._store)
        after = params.get("after", 0)
        limit = params.get("limit", MAX_PAGE_LIMIT)
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "after must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "limit must be a positive integer")
        limit = min(limit, MAX_PAGE_LIMIT)
        rows = store.events(after, limit=limit + 1)
        has_more = len(rows) > limit
        rows = rows[:limit]
        identity = self._identity.load_or_create()
        items: list[PeerEvent] = []
        for row in rows:
            local_id = row.get("task_id") or row.get("session_id") or row.get("attempt_id") or str(row["seq"])
            items.append(
                {
                    "seq": row["seq"],
                    "type": row["kind"],
                    "ref": {
                        "host_id": self._host_id,
                        "node_id": identity.node_id,
                        "state_generation": identity.state_generation,
                        "local_id": str(local_id),
                    },
                    "payload": row["data"],
                    "created": row["created"],
                }
            )
        cursor = items[-1]["seq"] if items else after
        return {"items": items, "cursor": cursor, "has_more": has_more}
