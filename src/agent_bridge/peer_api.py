"""Coordinator-side orchestration interfaces and new/extended MCP tool contracts (design 7 and 8)."""
from __future__ import annotations

import hashlib
import json
import socket
import sqlite3
import time
from typing import Any, Literal, NotRequired, Protocol, TypedDict

from . import __version__, groups
from .config import Profile
from .errors import BridgeError
from .node_identity import NodeIdentityStore
from .peer_protocol import (
    CAPABILITIES,
    INLINE_RESULT_BYTES,
    MAX_PAGE_LIMIT,
    PROTOCOL_VERSION,
    AckRequestResult,
    ArtifactRef,
    DeliverySource,
    HelloResult,
    NodeOutboxStatus,
    NodeRef,
    NodeState,
    NotifyParentResult,
    PeerDeliveryStatus,
    PeerErrorCode,
    PeerEvent,
    PeerRequest,
    PeerResponse,
    PullRequestsResult,
    RemoteMember,  # noqa: F401  re-exported so peer_api.RemoteMember is the peer_protocol type
    canonical_operation_id,
    fail,
    negotiate_version,
    normalize_ack_request_params,
    normalize_notify_parent_params,
    normalize_outbox_item,
    ok,
    peer_error,
    require_method,
    validate_object_ref,
)
from .store import Store, encode, identifier, validate_session_name

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


def _decode_json(value: Any) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


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
    "peer.pull_requests",
    "peer.ack_request",
    "peer.deliver",
    "peer.lookup_delivery",
    "peer.cancel",
    "peer.result",
    "peer.events",
    "peer.notify_parent",
    "peer.resolve_delivery",
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
            elif spec.name == "peer.pull_requests":
                result = self._pull_requests(params)
            elif spec.name == "peer.ack_request":
                result = self._ack_request(params)
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
            elif spec.name == "peer.notify_parent":
                result = self._notify_parent(params)
            elif spec.name == "peer.resolve_delivery":
                result = self._resolve_delivery(params)
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

    def _pull_requests(self, params: dict[str, Any]) -> PullRequestsResult:
        store = self._require_store(self._store)
        after = params.get("after", 0)
        limit = params.get("limit", MAX_PAGE_LIMIT)
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "after must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "limit must be a positive integer")
        limit = min(limit, MAX_PAGE_LIMIT)
        rows = store.pull_outbox(after, limit)
        with store.connection() as db:
            scan_upper_seq = db.execute("SELECT COALESCE(MAX(seq),0) FROM node_outbox").fetchone()[0]
        items = [normalize_outbox_item(row) for row in rows]
        return {
            "items": items,
            "cursor": items[-1]["seq"] if items else after,
            "has_more": len(items) == limit,
            "scan_upper_seq": scan_upper_seq,
        }

    def _ack_request(self, params: dict[str, Any]) -> AckRequestResult:
        store = self._require_store(self._store)
        normalized = normalize_ack_request_params(params)
        operation = normalized.get("operation")
        if operation is not None:
            identity = self._ready_identity()
            self._check_operation_identity(operation, identity)
        row = store.ack_outbox(
            normalized["operation_id"],
            result=normalized["result"],
            outcome=normalized["outcome"],
            status=str(NodeOutboxStatus.ACKED),
        )
        ack_seq = store.record_commit(
            f"peer.ack_request:{normalized['operation_id']}", "peer.ack_request"
        )
        self._identity.advance(ack_seq)
        return {"operation_id": row["operation_id"], "status": row["status"]}

    @staticmethod
    def _notification_source(event_data: Any) -> tuple[dict[str, Any], int] | None:
        if not isinstance(event_data, dict):
            return None
        source = event_data.get("source")
        remote_seq = event_data.get("remote_seq")
        if isinstance(source, dict) and isinstance(remote_seq, int) and not isinstance(remote_seq, bool):
            return source, remote_seq
        return None

    def _notify_parent(self, params: dict[str, Any]) -> NotifyParentResult:
        store = self._require_store(self._store)
        normalized = normalize_notify_parent_params(params)
        operation = normalized["operation"]
        identity = self._ready_identity()
        self._check_operation_identity(operation, identity)
        parent_ref = normalized["parent_ref"]
        event = normalized["event"]
        local_id = parent_ref["local_id"]
        encoded_parent = encode(parent_ref)
        source_ref = event["ref"]
        fingerprint = hashlib.sha256(encode(event).encode("utf-8")).hexdigest()
        with store.connection(write=True) as db:
            parent = db.execute("SELECT * FROM parents WHERE id=?", (local_id,)).fetchone()
            session = None
            if parent is None:
                session = db.execute("SELECT * FROM sessions WHERE id=?", (local_id,)).fetchone()
            if parent is None and session is None:
                raise peer_error(
                    PeerErrorCode.OPERATION_NOT_FOUND,
                    f"Parent {local_id!r} is not registered on this host",
                )

            query = (
                "SELECT m.*, e.data AS event_data FROM mirrored_parent_notifications m "
                "JOIN events e ON e.seq=m.local_seq "
            )
            existing = db.execute(query + "WHERE m.operation_id=?", (operation["operation_id"],)).fetchone()
            if existing is not None:
                source = self._notification_source(Store._decode_json(existing["event_data"]))
                if existing["parent_ref"] != encoded_parent or source != (event["ref"], event["seq"]):
                    raise peer_error(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "operation_id was already used for a different parent notification",
                    )
                return {"mirror_seq": existing["local_seq"], "duplicate": True}

            for row in db.execute(query + "WHERE m.parent_ref=?", (encoded_parent,)).fetchall():
                if self._notification_source(Store._decode_json(row["event_data"])) == (event["ref"], event["seq"]):
                    return {"mirror_seq": row["local_seq"], "duplicate": True}

            mirrored = db.execute(
                "SELECT * FROM peer_event_mirrors WHERE host_id=? AND node_id=? AND state_generation=? "
                "AND remote_seq=?",
                (source_ref["host_id"], source_ref["node_id"], source_ref["state_generation"], event["seq"]),
            ).fetchone()
            if mirrored is not None:
                if mirrored["fingerprint"] != fingerprint:
                    raise peer_error(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "remote event was mirrored with different content",
                    )
                mirror_seq = mirrored["local_seq"]
            else:
                data: dict[str, Any] = {
                    "source": source_ref,
                    "remote_seq": event["seq"],
                    "type": event["type"],
                    "payload": event["payload"],
                    "created": event["created"],
                }
                if "result" in normalized:
                    data["result"] = normalized["result"]
                mirror_seq = store.event(
                    db,
                    event["type"],
                    data,
                    session=None if parent is not None else local_id,
                )
                store.mirror_peer_event(
                    source_ref["host_id"],
                    source_ref["node_id"],
                    source_ref["state_generation"],
                    event["seq"],
                    fingerprint,
                    mirror_seq,
                    db=db,
                )
            occupied = db.execute(
                "SELECT parent_ref FROM mirrored_parent_notifications WHERE local_seq=?", (mirror_seq,)
            ).fetchone()
            if occupied is not None and occupied["parent_ref"] != encoded_parent:
                raise peer_error(
                    PeerErrorCode.PARENT_ROUTE_CONFLICT,
                    "this event is already routed to a different parent",
                )
            receipt: dict[str, Any] = {
                "event": {"ref": event["ref"], "seq": event["seq"], "type": event["type"]},
                "status": "received",
            }
            if "result" in normalized:
                receipt["result"] = normalized["result"]
            db.execute(
                "INSERT INTO mirrored_parent_notifications(local_seq,parent_ref,operation_id,status,receipt,updated) "
                "VALUES(?,?,?,?,?,?)",
                (mirror_seq, encoded_parent, operation["operation_id"], "received", encode(receipt), time.time()),
            )
            if parent is not None and parent["notify"]:
                db.execute("INSERT INTO deliveries(event_seq,parent_id) VALUES(?,?)", (mirror_seq, local_id))
            ack_seq = store.record_commit(
                f"peer.notify_parent:{operation['operation_id']}", "peer.notify_parent", db=db
            )
        self._identity.advance(ack_seq)
        return {"mirror_seq": mirror_seq, "duplicate": False}

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
        ordering_mode = params.get("ordering_mode", "legacy")
        if ordering_mode not in {"legacy", "bootstrap", "ordered"}:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "ordering_mode must be legacy, bootstrap or ordered")
        stream_id = params.get("stream_id")
        authority_ref = params.get("authority_ref")
        parent_ref = params.get("parent_ref")
        if parent_ref is not None:
            # The execution node retains the coordinator-side parent reference for event attribution; it is
            # never written to this node's sessions.parent.
            validate_object_ref(parent_ref, "parent_ref")
        target_runtime = target.get("runtime", "")
        if kind == "bridge":
            if target_runtime not in ("", None):
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "bridge target runtime must be empty")
            target_runtime = ""
        elif target_runtime not in ("codex", "antigravity"):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "native target runtime must be codex or antigravity")
        if ordering_mode == "ordered":
            if not isinstance(stream_id, str) or not stream_id:
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "ordered delivery requires stream_id")
            if target_seq < 1:
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "ordered delivery requires target_seq >= 1")
            if not isinstance(authority_ref, dict):
                raise peer_error(PeerErrorCode.INVALID_PARAMS, "ordered delivery requires authority_ref")

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
                if ordering_mode == "ordered":
                    incoming = db.execute(
                        "SELECT admitted_seq FROM incoming_streams WHERE stream_id=?", (stream_id,)
                    ).fetchone()
                    if incoming is None:
                        if target_seq != 1:
                            raise peer_error(
                                PeerErrorCode.SEQUENCE_GAP,
                                f"target_seq {target_seq} arrived before sequence 1",
                            )
                        local_target = {
                            "host_id": self._host_id, "node_id": identity.node_id,
                            "state_generation": identity.state_generation, "kind": kind,
                            "runtime": target_runtime, "target_id": target_id,
                        }
                        db.execute(
                            "INSERT INTO incoming_streams(stream_id,authority_ref,target_ref,admitted_seq) "
                            "VALUES(?,?,?,?)",
                            (stream_id, encode(authority_ref), encode(local_target), target_seq),
                        )
                    else:
                        if target_seq != incoming["admitted_seq"] + 1:
                            raise peer_error(
                                PeerErrorCode.SEQUENCE_GAP,
                                f"target_seq {target_seq} does not follow {incoming['admitted_seq']}",
                            )
                        db.execute(
                            "UPDATE incoming_streams SET admitted_seq=? WHERE stream_id=?",
                            (target_seq, stream_id),
                        )
                try:
                    db.execute(
                        "INSERT INTO peer_inbox(delivery_id,host_id,node_id,state_generation,target_seq,seq_kind,operation_id,payload,status,receipt,created,ordering_mode,stream_id,authority_ref,target_kind,runtime,target_id,revision) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                            ordering_mode,
                            stream_id,
                            encode(authority_ref) if authority_ref is not None else None,
                            kind,
                            target_runtime,
                            target_id,
                            0,
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
            send_status: str | None = None
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
            elif kind == "native":
                if not target_id:
                    raise peer_error(PeerErrorCode.INVALID_PARAMS, "native delivery requires target.id")
                if db.execute("SELECT 1 FROM sessions WHERE native_id=?", (target_id,)).fetchone():
                    raise BridgeError(
                        "native_owned_by_bridge", "Use the existing bridge session for this native session"
                    )
                queue = db.execute(
                    "SELECT * FROM native_send_queue WHERE delivery_source=? AND delivery_id=?",
                    (DeliverySource.PEER_DELIVER, delivery_id),
                ).fetchone()
                if seq_kind == "tombstone":
                    if queue is not None:
                        if queue["status"] in {"pending", "sending", "uncertain"}:
                            queue = store.update_native_send(
                                queue["id"], "cancelled",
                                receipt=encode({"cancelled_at": accepted_at, "resolution": "not_received"}),
                                db=db,
                            )
                        elif queue["status"] != "cancelled":
                            raise peer_error(
                                PeerErrorCode.DELIVERY_BLOCKED,
                                f"native delivery is already {queue['status']}; cannot cancel it",
                            )
                        send_status = queue["status"]
                    status = "cancelled"
                else:
                    if queue is None:
                        queue = store.enqueue_native_send(
                            target_runtime, target_id, DeliverySource.PEER_DELIVER,
                            delivery_id, message, db=db,
                        )
                    send_status = queue["status"]
                    status = "accepted_remote"
            result: dict[str, Any] = {
                "delivery_id": delivery_id,
                "accepted": True,
                "task_id": task_id,
                "session_id": session_id,
                "target_seq": target_seq,
                "receipt": {"accepted_at": accepted_at},
                "status": status,
            }
            if send_status is not None:
                result["send_status"] = send_status
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

    def _resolve_delivery(self, params: dict[str, Any]) -> dict[str, Any]:
        store = self._require_store(self._store)
        operation = self._operation_key(params)
        identity = self._ready_identity()
        self._check_operation_identity(operation, identity)
        delivery_id = self._required_string(params, "delivery_id")
        resolution = params.get("resolution")
        if resolution not in {"received", "not_received"}:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "resolution must be received or not_received")
        evidence = params.get("evidence")
        if not isinstance(evidence, str):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "evidence must be a string")
        evidence = evidence.strip()
        if not evidence:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "evidence must not be empty")
        if len(evidence.encode("utf-8")) > 16 * 1024:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "evidence must be at most 16 KiB")
        expected_revision = params.get("expected_delivery_revision")
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "expected_delivery_revision must be a non-negative integer")
        fingerprint = self._fingerprint("peer.resolve_delivery", params)
        resolved_at = time.time()
        with store.connection(write=True) as db:
            existing = db.execute(
                "SELECT * FROM peer_operations WHERE operation_id=?", (operation["operation_id"],)
            ).fetchone()
            if existing is not None:
                current = store._peer_operation(existing)
                if current["fingerprint"] != fingerprint:
                    raise peer_error(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "operation_id was already used with different resolution parameters",
                    )
                return self._operation_outcome(current)
            db.execute(
                "INSERT INTO peer_operations(operation_id,node_id,state_generation,fingerprint,method,params,status,created,updated) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    operation["operation_id"], identity.node_id, identity.state_generation, fingerprint,
                    "peer.resolve_delivery", encode(params), "running", resolved_at, resolved_at,
                ),
            )
            inbox = db.execute("SELECT * FROM peer_inbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if inbox is None:
                if resolution == "received":
                    raise peer_error(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown delivery: {delivery_id}")
                if expected_revision != 0:
                    raise peer_error(
                        PeerErrorCode.REVISION_CONFLICT,
                        f"Delivery revision {expected_revision} does not match the absent inbox row",
                    )
                result: dict[str, Any] = {
                    "delivery_id": delivery_id,
                    "status": "cancelled",
                    "delivery_revision": 0,
                    "receipt": encode({"cancelled_at": resolved_at, "resolution": "not_received"}),
                }
            else:
                if inbox["revision"] != expected_revision:
                    raise peer_error(
                        PeerErrorCode.REVISION_CONFLICT,
                        f"Delivery revision changed: expected {expected_revision}, found {inbox['revision']}",
                    )
                queue = db.execute(
                    "SELECT * FROM native_send_queue WHERE delivery_source=? AND delivery_id=?",
                    (DeliverySource.PEER_DELIVER, delivery_id),
                ).fetchone()
                if resolution == "received":
                    if inbox["seq_kind"] == "tombstone" or inbox["status"] == "cancelled":
                        raise peer_error(
                            PeerErrorCode.OPERATION_CONFLICT,
                            "The execution node already recorded this delivery as cancelled",
                        )
                    if queue is not None:
                        if queue["status"] in {"cancelled", "failed"}:
                            raise peer_error(
                                PeerErrorCode.OPERATION_CONFLICT,
                                f"Native queue already recorded {queue['status']} for this delivery",
                            )
                        if queue["status"] not in {"received", "enqueued"}:
                            queue = store.update_native_send(
                                queue["id"], "received",
                                receipt=encode({"received_at": resolved_at, "resolution": "received"}),
                                processing_status=queue["processing_status"], db=db,
                            )
                    receipt = encode({"received_at": resolved_at, "resolution": "received"})
                    final_status = "received"
                else:
                    if queue is not None and queue["status"] in {"enqueued", "received"}:
                        raise peer_error(
                            PeerErrorCode.OPERATION_CONFLICT,
                            f"Native queue already recorded {queue['status']} for this delivery",
                        )
                    if inbox["seq_kind"] != "tombstone" and inbox["status"] == "received":
                        delivered = db.execute(
                            "SELECT result FROM peer_operations WHERE operation_id=?",
                            (inbox["operation_id"],),
                        ).fetchone()
                        delivered_result = _decode_json(delivered["result"]) if delivered is not None else None
                        task_id = delivered_result.get("task_id") if isinstance(delivered_result, dict) else None
                        if isinstance(task_id, str) and db.execute(
                            "SELECT 1 FROM tasks WHERE id=?", (task_id,)
                        ).fetchone():
                            raise peer_error(
                                PeerErrorCode.OPERATION_CONFLICT,
                                "The execution node already has a queued task for this delivery",
                            )
                    if queue is not None and queue["status"] in {"pending", "sending", "uncertain"}:
                        store.update_native_send(
                            queue["id"], "cancelled",
                            receipt=encode({"cancelled_at": resolved_at, "resolution": "not_received"}),
                            db=db,
                        )
                    receipt = encode({"cancelled_at": resolved_at, "resolution": "not_received"})
                    final_status = "cancelled"
                revision = inbox["revision"] + 1
                db.execute(
                    "UPDATE peer_inbox SET status=?,receipt=?,revision=? WHERE delivery_id=?",
                    (final_status, receipt, revision, delivery_id),
                )
                result = {
                    "delivery_id": delivery_id,
                    "status": final_status,
                    "delivery_revision": revision,
                    "receipt": receipt,
                }
            cursor = db.execute(
                "INSERT INTO peer_commit_log(mutation_key,method,created) VALUES(?,?,?)",
                (f"peer.resolve_delivery:{operation['operation_id']}", "peer.resolve_delivery", resolved_at),
            )
            ack_seq = cursor.lastrowid
            db.execute(
                "UPDATE peer_operations SET status=?,result=?,updated=? WHERE operation_id=?",
                ("completed", encode(result), time.time(), operation["operation_id"]),
            )
        self._identity.advance(ack_seq)
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


# ---------------------------------------------------------------- coordinator manual resolution (design 3.8)

_OFFLINE_RESOLUTION_CODES = {
    str(PeerErrorCode.HOST_OFFLINE),
    str(PeerErrorCode.HOST_UNKNOWN),
    str(PeerErrorCode.IDENTITY_MISMATCH),
    str(PeerErrorCode.STATE_CORRUPTED),
    str(PeerErrorCode.OPERATION_PENDING),
    str(PeerErrorCode.DELIVERY_UNCERTAIN),
}


def _resolution_response(db, resolution_row, outbox_row=None):
    if outbox_row is None:
        outbox_row = db.execute(
            "SELECT status,revision FROM peer_outbox WHERE delivery_id=?", (resolution_row["delivery_id"],)
        ).fetchone()
    remote = _decode_json(resolution_row["remote_result"])
    receipt = remote.get("receipt") if isinstance(remote, dict) else None
    remote_revision = remote.get("delivery_revision") if isinstance(remote, dict) else None
    if isinstance(remote_revision, bool) or not isinstance(remote_revision, int):
        remote_revision = resolution_row["remote_revision"]
    status = outbox_row["status"] if outbox_row is not None else resolution_row["resolution"]
    response = {
        "resolution_id": resolution_row["id"],
        "status": status,
        "delivery_id": resolution_row["delivery_id"],
        "revision": outbox_row["revision"] if outbox_row is not None else resolution_row["delivery_revision"],
        "remote_revision": remote_revision,
    }
    if receipt is not None:
        response["receipt"] = _decode_json(receipt)
    if status in {
        str(PeerDeliveryStatus.UNCERTAIN), str(PeerDeliveryStatus.RESOLVING),
        str(PeerDeliveryStatus.SENDING), str(PeerDeliveryStatus.WAITING_FOR_HOST),
        str(PeerDeliveryStatus.PENDING), str(PeerDeliveryStatus.TOMBSTONE_PENDING),
        str(PeerDeliveryStatus.ACCEPTED_REMOTE),
    }:
        response["blocked_by"] = status
    return response


def _resolution_client(peer_factory, host_id):
    if peer_factory is None:
        raise BridgeError(PeerErrorCode.HOST_UNKNOWN, "No peer client factory is configured")
    if hasattr(peer_factory, "client"):
        candidate = peer_factory.client(host_id)
    elif callable(peer_factory):
        candidate = peer_factory(host_id)
    else:
        candidate = peer_factory
    if not hasattr(candidate, "resolve_delivery"):
        raise BridgeError(PeerErrorCode.NOT_IMPLEMENTED, "The peer client cannot resolve deliveries")
    return candidate


async def resolve_delivery(
    store,
    peer_factory=None,
    params: dict[str, Any] | None = None,
    *,
    delivery_id: str | None = None,
    resolution: str | None = None,
    evidence: str | None = None,
    expected_revision: int | None = None,
    expected_delivery_revision: int | None = None,
    expected_remote_revision: int | None = None,
    request_id: str | None = None,
    actor: str = "admin",
    admin: bool = True,
    client=None,
    **kwargs,
):
    """Record and deliver an evidence-based decision for an uncertain broadcast head.

    The pending decision and the CAS to `resolving` commit before the network call. Only a positive remote
    acknowledgement moves the coordinator delivery to `released`/`cancelled`; an offline or timed-out peer
    leaves it `resolving` so the head continues to block later messages.
    """
    values = dict(params or {})
    for key, value in (
        ("delivery_id", delivery_id), ("resolution", resolution), ("evidence", evidence),
        ("expected_revision", expected_revision),
        ("expected_delivery_revision", expected_delivery_revision),
        ("expected_remote_revision", expected_remote_revision), ("request_id", request_id),
    ):
        if value is not None:
            values[key] = value
    if client is not None and peer_factory is None:
        peer_factory = client
    if "is_admin" in kwargs:
        admin = kwargs.pop("is_admin")
    if kwargs:
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "Unknown resolve_delivery parameters")
    if not admin:
        raise BridgeError(PeerErrorCode.PERMISSION_DENIED, "Only an administrator can resolve a delivery")
    if not isinstance(actor, str) or not actor.strip():
        raise BridgeError(PeerErrorCode.PERMISSION_DENIED, "A non-empty administrator identity is required")
    delivery_id = values.get("delivery_id")
    if not isinstance(delivery_id, str) or not delivery_id:
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "delivery_id must be a non-empty string")
    resolution = values.get("resolution")
    if resolution not in {"received", "not_received"}:
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "resolution must be received or not_received")
    evidence = values.get("evidence")
    if not isinstance(evidence, str):
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "evidence must be a string")
    evidence = evidence.strip()
    if not evidence:
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "evidence must not be empty")
    if len(evidence.encode("utf-8")) > 16 * 1024:
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "evidence must be at most 16 KiB")
    for key in ("expected_revision", "expected_delivery_revision", "expected_remote_revision"):
        value = values.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise BridgeError(PeerErrorCode.INVALID_PARAMS, f"{key} must be a non-negative integer")
    request_id = values.get("request_id")
    if not isinstance(request_id, str) or not request_id or len(request_id) > 200:
        raise BridgeError("invalid_request_id", "A request_id of at most 200 characters is required")
    expected_revision = values["expected_revision"]
    expected_delivery_revision = values["expected_delivery_revision"]
    expected_remote_revision = values["expected_remote_revision"]
    normalized = {
        "delivery_id": delivery_id, "resolution": resolution, "evidence": evidence,
        "expected_revision": expected_revision,
        "expected_delivery_revision": expected_delivery_revision,
        "expected_remote_revision": expected_remote_revision,
        "request_id": request_id, "actor": actor,
    }
    fingerprint = hashlib.sha256(encode(normalized).encode("utf-8")).hexdigest()

    with store.connection(write=True) as db:
        existing = db.execute(
            "SELECT * FROM delivery_resolutions WHERE request_id=?", (request_id,)
        ).fetchone()
        if existing is not None:
            if existing["fingerprint"] != fingerprint:
                raise BridgeError(
                    PeerErrorCode.OPERATION_CONFLICT,
                    "request_id already represents a different resolution decision",
                )
            outbox = db.execute(
                "SELECT * FROM peer_outbox WHERE delivery_id=?", (existing["delivery_id"],)
            ).fetchone()
            if outbox is None:
                raise BridgeError(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown delivery: {existing['delivery_id']}")
            if outbox["status"] != str(PeerDeliveryStatus.RESOLVING) or existing["status"] not in {
                "pending", "resolving",
            }:
                return _resolution_response(db, existing, outbox)
            resolution_row = existing
        else:
            outbox = db.execute(
                "SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if outbox is None:
                raise BridgeError(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown delivery: {delivery_id}")
            if outbox["group_id"] is None:
                raise BridgeError(
                    PeerErrorCode.DELIVERY_BLOCKED,
                    "agent_delivery_resolve only handles group broadcast deliveries",
                )
            group = db.execute("SELECT * FROM session_groups WHERE id=?", (outbox["group_id"],)).fetchone()
            if group is None or group["deleted"]:
                raise BridgeError("group_deleted", "This group was deleted")
            if group["revision"] != expected_revision:
                raise BridgeError(
                    PeerErrorCode.REVISION_CONFLICT,
                    "Group revision changed; inspect the group before resolving",
                )
            member = db.execute(
                "SELECT 1 FROM remote_group_members WHERE group_id=? AND host_id=? AND node_id=? "
                "AND state_generation=? AND target_kind=? AND runtime=? AND target_id=?",
                (outbox["group_id"], outbox["host_id"], outbox["node_id"], outbox["state_generation"],
                 outbox["target_kind"], outbox["runtime"], outbox["target_id"]),
            ).fetchone()
            if member is None:
                raise BridgeError(
                    PeerErrorCode.IDENTITY_MISMATCH,
                    "The remote member identity changed after this broadcast",
                )
            if outbox["status"] != str(PeerDeliveryStatus.UNCERTAIN):
                raise BridgeError(
                    PeerErrorCode.DELIVERY_BLOCKED,
                    f"Delivery is {outbox['status']}; only an uncertain head can be resolved",
                )
            if outbox["revision"] != expected_delivery_revision:
                raise BridgeError(
                    PeerErrorCode.REVISION_CONFLICT,
                    "Delivery revision changed; inspect the delivery before resolving",
                )
            head = db.execute(
                "SELECT MIN(target_seq) FROM peer_outbox WHERE stream_id=? "
                "AND status NOT IN ('released','cancelled','failed')",
                (outbox["stream_id"],),
            ).fetchone()[0]
            if head != outbox["target_seq"]:
                raise BridgeError(
                    PeerErrorCode.DELIVERY_BLOCKED,
                    "This delivery is not the blocking head of its target stream",
                )
            changed = db.execute(
                "UPDATE peer_outbox SET status='resolving',revision=revision+1,updated=? "
                "WHERE delivery_id=? AND status='uncertain' AND revision=?",
                (time.time(), delivery_id, expected_delivery_revision),
            ).rowcount
            if not changed:
                raise BridgeError(
                    PeerErrorCode.REVISION_CONFLICT,
                    "Delivery changed while its resolution was being recorded",
                )
            resolution_id = identifier()
            now = time.time()
            db.execute(
                "INSERT INTO delivery_resolutions(id,delivery_id,request_id,fingerprint,group_revision,"
                "delivery_revision,remote_revision,request_json,resolution,evidence,actor,status,created,updated) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (resolution_id, delivery_id, request_id, fingerprint, expected_revision,
                 expected_delivery_revision, expected_remote_revision, encode(normalized), resolution,
                 evidence, actor, "pending", now, now),
            )
            resolution_row = db.execute(
                "SELECT * FROM delivery_resolutions WHERE id=?", (resolution_id,)
            ).fetchone()
            outbox = db.execute(
                "SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)
            ).fetchone()

    payload = _decode_json(outbox["payload"])
    authority = payload.get("authority_ref") if isinstance(payload, dict) else None
    if not isinstance(authority, dict):
        from .node_identity import FileNodeIdentityStore

        authority = FileNodeIdentityStore(store.state).node_ref("local").as_dict()
    operation_id = canonical_operation_id(
        authority, outbox["state_generation"], "peer.resolve_delivery", resolution_row["id"],
    )
    remote_params = {
        "operation": {
            "node_id": outbox["node_id"],
            "state_generation": outbox["state_generation"],
            "operation_id": operation_id,
        },
        "delivery_id": delivery_id,
        "resolution": resolution,
        "evidence": evidence,
        "expected_delivery_revision": expected_remote_revision,
    }

    def mark_pending():
        with store.connection(write=True) as db:
            db.execute(
                "UPDATE delivery_resolutions SET status='resolving',updated=? WHERE id=?",
                (time.time(), resolution_row["id"]),
            )
            refreshed = db.execute(
                "SELECT * FROM delivery_resolutions WHERE id=?", (resolution_row["id"],)
            ).fetchone()
            current = db.execute(
                "SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            return _resolution_response(db, refreshed, current)

    def restore_uncertain(error):
        with store.connection(write=True) as db:
            db.execute(
                "UPDATE peer_outbox SET status='uncertain',revision=revision+1,error=?,updated=? "
                "WHERE delivery_id=? AND status='resolving'",
                (error, time.time(), delivery_id),
            )
            db.execute(
                "UPDATE delivery_resolutions SET status='failed',remote_result=?,updated=? WHERE id=?",
                (encode({"error": error}), time.time(), resolution_row["id"]),
            )

    try:
        remote_result = await _resolution_client(peer_factory, outbox["host_id"]).resolve_delivery(remote_params)
    except BridgeError as error:
        if str(error.code) in _OFFLINE_RESOLUTION_CODES:
            return mark_pending()
        restore_uncertain(str(error))
        raise
    except (TimeoutError, ConnectionError, OSError):
        return mark_pending()
    if not isinstance(remote_result, dict):
        restore_uncertain("The execution node returned an invalid resolution response")
        raise BridgeError(PeerErrorCode.INVALID_PARAMS, "The execution node returned an invalid resolution response")

    remote_status = remote_result.get("status")
    if resolution == "received":
        if remote_status not in {"received", "accepted_remote"}:
            restore_uncertain(f"Remote reported {remote_status!r} instead of a receipt")
            raise BridgeError(
                PeerErrorCode.OPERATION_CONFLICT,
                "The execution node has a contradictory delivery fact; inspect both sides",
            )
        final_status = str(PeerDeliveryStatus.RELEASED)
    else:
        if remote_status != "cancelled":
            restore_uncertain(f"Remote reported {remote_status!r} instead of cancellation")
            raise BridgeError(
                PeerErrorCode.OPERATION_CONFLICT,
                "The execution node has a contradictory delivery fact; inspect both sides",
            )
        final_status = str(PeerDeliveryStatus.CANCELLED)

    receipt = remote_result.get("receipt")
    encoded_receipt = receipt if isinstance(receipt, str) else (encode(receipt) if receipt is not None else None)
    remote_revision = remote_result.get("delivery_revision", expected_remote_revision)
    send_status = remote_result.get("send_status")
    processing_status = remote_result.get("processing_status")
    with store.connection(write=True) as db:
        changed = db.execute(
            "UPDATE peer_outbox SET status=?,receipt=COALESCE(?,receipt),send_status=?,processing_status=?,"
            "error=NULL,revision=revision+1,updated=? WHERE delivery_id=? AND status='resolving'",
            (final_status, encoded_receipt, send_status, processing_status, time.time(), delivery_id),
        ).rowcount
        if not changed:
            raise BridgeError(
                PeerErrorCode.REVISION_CONFLICT,
                "Delivery changed while its resolution was in flight",
            )
        db.execute(
            "UPDATE target_streams SET released_seq=MAX(released_seq,?) WHERE stream_id=?",
            (outbox["target_seq"], outbox["stream_id"]),
        )
        groups.update_broadcast_recipient(
            db, delivery_id, final_status,
            send_status=send_status, processing_status=processing_status, error=None,
        )
        groups.settle_broadcast(db, outbox["broadcast_id"])
        db.execute(
            "UPDATE delivery_resolutions SET status='completed',remote_result=?,updated=? WHERE id=?",
            (encode(remote_result), time.time(), resolution_row["id"]),
        )
        refreshed = db.execute(
            "SELECT * FROM delivery_resolutions WHERE id=?", (resolution_row["id"],)
        ).fetchone()
        current = db.execute(
            "SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)
        ).fetchone()
        response = _resolution_response(db, refreshed, current)
    response["remote_revision"] = remote_revision
    return response
