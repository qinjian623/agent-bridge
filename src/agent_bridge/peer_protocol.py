"""Wire contract for multi-machine peering (see docs/multi-machine-interface.md).

Pure definitions: enums, identifiers, envelopes, per-method params/results and the method whitelist.
No I/O and no behaviour beyond validation helpers, so every other peer module can import it safely.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, NotRequired, TypedDict

from .errors import BridgeError

PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 1 << 20  # one JSONL frame; larger payloads must be Artifact references (ADR-15)
MAX_PAGE_LIMIT = 100
INLINE_RESULT_BYTES = 16 * 1024  # results above this are returned as ArtifactRef only
CAPABILITIES = (
    "groups_v1",
    "ordered_delivery_v1",
    "member_outbox_v1",
    "event_mirror_v1",
    "workspace_v1",
    "session_name_v1",
    "prune_guard_v1",
)


# ---------------------------------------------------------------- identity (design 4.1)

@dataclass(frozen=True, slots=True)
class NodeRef:
    """Identity of one node incarnation. Two refs are the same node only if all fields match."""

    host_id: str
    node_id: str
    state_generation: str

    def as_dict(self) -> dict[str, str]:
        return {"host_id": self.host_id, "node_id": self.node_id, "state_generation": self.state_generation}


@dataclass(frozen=True, slots=True)
class ObjectRef:
    """Globally unique reference: host_id + node_id + state_generation + local resource id."""

    node: NodeRef
    local_id: str

    def as_dict(self) -> dict[str, str]:
        return {**self.node.as_dict(), "local_id": self.local_id}


DeliverOrderingMode = Literal["legacy", "bootstrap", "ordered"]


@dataclass(frozen=True, slots=True)
class GroupRef:
    """Group identity: authority (coordinator) node incarnation plus group_id."""

    authority: NodeRef
    group_id: str

    def as_dict(self) -> dict[str, Any]:
        return {"authority": self.authority.as_dict(), "group_id": self.group_id}


@dataclass(frozen=True, slots=True)
class TargetRef:
    """Complete destination: node incarnation plus kind/runtime/target_id."""

    node: NodeRef
    kind: str
    target_id: str
    runtime: str = ""

    def __post_init__(self) -> None:
        if self.kind not in ("bridge", "native"):
            raise ValueError(f"invalid target kind: {self.kind!r}")
        if self.kind == "bridge" and self.runtime != "":
            raise ValueError("bridge target must have empty runtime")
        if self.kind == "native" and self.runtime not in ("codex", "antigravity"):
            raise ValueError(f"invalid native runtime: {self.runtime!r}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "host_id": self.node.host_id,
            "node_id": self.node.node_id,
            "state_generation": self.node.state_generation,
            "kind": self.kind,
            "runtime": self.runtime,
            "target_id": self.target_id,
        }


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _node_dict(node: NodeRef | dict[str, Any]) -> dict[str, Any]:
    if isinstance(node, NodeRef):
        return node.as_dict()
    return {k: node[k] for k in ("host_id", "node_id", "state_generation")}


def compute_stream_id(authority: NodeRef | dict[str, Any], target: TargetRef | dict[str, Any]) -> str:
    """stream_id = SHA256(canonical_json(authority NodeRef, TargetRef))."""
    target_dict = target.as_dict() if isinstance(target, TargetRef) else {
        k: target[k] for k in ("host_id", "node_id", "state_generation", "kind", "runtime", "target_id")
    }
    return hashlib.sha256(_canonical_bytes({"authority": _node_dict(authority), "target": target_dict})).hexdigest()


def canonical_operation_id(
    source_node: NodeRef | dict[str, Any], dest_generation: str, method: str, request_id: str
) -> str:
    """Deterministic operation id from full source identity, receiver generation, method and request id."""
    source = _node_dict(source_node)
    payload = {
        "source_node_id": source["node_id"],
        "source_state_generation": source["state_generation"],
        "dest_generation": dest_generation,
        "method": method,
        "request_id": request_id,
    }
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


# ---------------------------------------------------------------- state enums

class HostState(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    IDENTITY_CHANGED = "identity_changed"  # host_id/node_id/generation/SSH host key mismatch; new requests blocked
    STATE_CORRUPTED = "state_corrupted"  # remote reported database rollback; needs administrator re-registration


class NodeState(StrEnum):
    """Execution-node self state persisted in node_identity.json."""

    READY = "ready"
    STATE_CORRUPTED = "state_corrupted"


class OperationStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNCERTAIN = "uncertain"

    @property
    def terminal(self) -> bool:
        return self in {self.COMPLETED, self.FAILED, self.CANCELLED}


class NodeOutboxStatus(StrEnum):  # node_outbox.status
    PENDING = "pending"
    PULLED = "pulled"
    ACKED = "acked"


class PeerDeliveryStatus(StrEnum):  # coordinator peer_outbox.status
    PENDING = "pending"
    WAITING_FOR_HOST = "waiting_for_host"
    ACCEPTED_REMOTE = "accepted_remote"
    CANCELLED = "cancelled"
    UNCERTAIN = "uncertain"
    SENDING = "sending"
    TOMBSTONE_PENDING = "tombstone_pending"
    RELEASED = "released"
    RESOLVING = "resolving"
    FAILED = "failed"


class SendQueueStatus(StrEnum):  # node native_send_queue.status
    PENDING = "pending"
    SENDING = "sending"
    ENQUEUED = "enqueued"
    RECEIVED = "received"
    UNCERTAIN = "uncertain"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DeliverySource(StrEnum):  # native_send_queue.delivery_source
    LOCAL_BROADCAST = "local_broadcast"
    PEER_DELIVER = "peer_deliver"
    PARENT_NOTIFICATION = "parent_notification"
    MIGRATION = "migration"


class NativeSendQueueItem(TypedDict):
    id: str
    runtime: str
    native_id: str
    local_seq: int
    delivery_source: str
    delivery_id: str
    message: str
    status: str
    receipt: str | None
    native_turn_id: str | None
    processing_status: str | None
    error: str | None
    revision: int
    created: float
    last_attempt: float


def is_head_blocking(status: str) -> bool:
    return status in (SendQueueStatus.PENDING, SendQueueStatus.SENDING, SendQueueStatus.UNCERTAIN)


def can_advance_native_queue(status: str) -> bool:
    return status in (
        SendQueueStatus.RECEIVED,
        SendQueueStatus.ENQUEUED,
        SendQueueStatus.CANCELLED,
        SendQueueStatus.FAILED,
    )


class ProcessingStatus(StrEnum):  # native processing, independent of receipt
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    UNCERTAIN = "uncertain"


class SeqKind(StrEnum):
    MESSAGE = "message"
    TOMBSTONE = "tombstone"  # ADR-09: cancelled message that still occupies its target_seq


# ---------------------------------------------------------------- error codes

class PeerErrorCode(StrEnum):
    METHOD_NOT_ALLOWED = "peer_method_not_allowed"
    VERSION_MISMATCH = "peer_version_mismatch"
    INVALID_PARAMS = "peer_invalid_params"
    IDENTITY_MISMATCH = "peer_identity_mismatch"
    # Alias used when a pinned host's node_id/generation changed; the wire code stays peer_identity_mismatch.
    IDENTITY_CHANGED = "peer_identity_mismatch"
    STATE_CORRUPTED = "peer_state_corrupted"
    HOST_OFFLINE = "peer_host_offline"
    HOST_UNKNOWN = "peer_host_unknown"
    OPERATION_PENDING = "operation_pending"
    OPERATION_CONFLICT = "operation_conflict"  # same operation_id, different parameters
    OPERATION_NOT_FOUND = "operation_not_found"
    DELIVERY_UNCERTAIN = "delivery_uncertain"
    DELIVERY_BLOCKED = "delivery_blocked"  # head of target queue is uncertain; resolve with evidence
    SEQUENCE_GAP = "peer_sequence_gap"
    REVISION_CONFLICT = "revision_conflict"
    PERMISSION_DENIED = "peer_permission_denied"
    BUDGET_SCOPE = "peer_budget_scope"  # token budget enabled but group has remote native members (ADR-14)
    FRAME_TOO_LARGE = "peer_frame_too_large"
    NOT_IMPLEMENTED = "peer_not_implemented"
    WORKSPACE_CONFLICT = "workspace_conflict"
    WORKSPACE_ALREADY_ATTACHED = "workspace_already_attached"
    WORKSPACE_LIMIT_EXCEEDED = "workspace_limit_exceeded"
    WORKSPACE_PROJECT_CHANGED = "workspace_project_changed"
    GROUP_PRUNE_BLOCKED = "group_prune_blocked"
    NATIVE_STOP_UNVERIFIABLE = "native_stop_unverifiable"
    PARENT_ROUTE_CONFLICT = "parent_route_conflict"


def peer_error(code: PeerErrorCode, message: str) -> BridgeError:
    return BridgeError(str(code), message)


# ---------------------------------------------------------------- envelopes

class PeerRequest(TypedDict):
    v: int
    id: str  # frame id, only correlates one response; NOT an idempotency key
    method: str
    params: dict[str, Any]


class PeerErrorBody(TypedDict):
    code: str
    message: str


class PeerResponse(TypedDict):
    v: int
    id: str
    ok: bool
    result: NotRequired[dict[str, Any]]
    error: NotRequired[PeerErrorBody]


class OperationKey(TypedDict):
    """Idempotency namespace (ADR-04). Mutating methods carry it; replays return the stored outcome."""

    node_id: str
    state_generation: str
    operation_id: str


class Page(TypedDict):
    cursor: int  # opaque-to-caller monotonically increasing; pass back as `after`
    has_more: bool


class ArtifactRef(TypedDict):
    """ADR-15: large content is referenced, never inlined."""

    host_id: str
    artifact_id: str
    sha256: str
    size: int


# ---------------------------------------------------------------- per-method params / results

class HelloParams(TypedDict):
    protocol_versions: list[int]
    coordinator_id: str
    coordinator_generation: str
    expected: NotRequired[dict[str, str]]  # {host_id,node_id,state_generation} pinned by coordinator, if known
    last_acked_seq: NotRequired[int]  # coordinator's view of node max_confirmed_seq


class HelloResult(TypedDict):
    protocol_version: int
    node: dict[str, str]  # NodeRef.as_dict()
    node_state: str  # NodeState
    max_confirmed_seq: int
    capabilities: list[str]
    broker_version: str


class CatalogParams(TypedDict):
    refresh: NotRequired[bool]
    profile: NotRequired[str]


class CatalogResult(TypedDict):
    profiles: list[dict[str, Any]]  # same shape as local agent_catalog, sanitized (no env/secret fields)
    stale: bool
    fetched_at: float


class InspectParams(TypedDict):
    kind: str  # session | task | group | operation | node | workspace
    id: NotRequired[str]
    section: NotRequired[Literal["base", "missing", "status"]]  # kind=workspace
    project_id: NotRequired[str | None]
    base_commit: NotRequired[str | None]
    after: NotRequired[int | None]  # missing-chunk pagination
    limit: NotRequired[int | None]


class InspectResult(TypedDict):
    items: list[dict[str, Any]]


class PullRequestsParams(TypedDict):
    after: int
    limit: NotRequired[int]


class OutboxItem(TypedDict):
    operation_id: str
    binding_id: str | None
    request_id: str | None
    method: str  # agent_broadcast | agent_group_inbox ... (member-role MCP methods only)
    payload: dict[str, Any]
    created: float
    seq: int | None  # node_outbox sequence
    sender_ref: dict[str, str] | None  # ObjectRef.as_dict() of the sending member
    authority_ref: dict[str, str] | None  # ObjectRef.as_dict() of the authorising object
    authorization_revision: int | None
    fingerprint: str | None
    group_id: str | None
    native_id: str | None


class PullRequestsResult(Page):
    items: list[OutboxItem]
    scan_upper_seq: NotRequired[int]  # highest node_outbox seq scanned, including skipped rows


class AckRequestParams(TypedDict):
    operation: NotRequired[OperationKey]
    operation_id: str
    outcome: str  # accepted | rejected | failed
    result: dict[str, Any]


class AckRequestResult(TypedDict):
    operation_id: str
    status: str  # NodeOutboxStatus


class DeliverTarget(TypedDict):
    kind: str  # bridge | native
    id: str  # session_id for bridge, native_id for native
    runtime: NotRequired[str]


class DeliverParams(TypedDict):
    operation: OperationKey
    delivery_id: str
    group_id: str
    broadcast_id: str
    target: DeliverTarget
    target_seq: int
    seq_kind: str  # SeqKind
    message: NotRequired[str]  # omitted for tombstones
    expected_group_revision: int
    # Composite open+dispatch (ADR-13): present when target.kind == "bridge" and no session exists yet.
    open: NotRequired[dict[str, Any]]  # {profile, project_id, access, ...}; never a local cwd
    ordering_mode: NotRequired[str]
    stream_id: NotRequired[str]
    authority_ref: NotRequired[dict[str, str]]
    parent_ref: NotRequired[dict[str, str]]


class DeliverResult(TypedDict):
    delivery_id: str
    status: str  # PeerDeliveryStatus (accepted_remote | uncertain | cancelled)
    session_id: NotRequired[str]
    task_id: NotRequired[str]
    send_status: NotRequired[str]  # SendQueueStatus
    processing_status: NotRequired[str]


class LookupDeliveryParams(TypedDict):
    delivery_id: NotRequired[str]
    operation: NotRequired[OperationKey]


class LookupDeliveryResult(TypedDict):
    found: bool
    delivery: NotRequired[DeliverResult]
    operation_status: NotRequired[str]  # OperationStatus


class ControlParams(TypedDict):
    operation: OperationKey
    session_id: str
    action: str  # pause | resume | close
    expected_revision: NotRequired[int]  # session revision CAS check
    continue_task_id: NotRequired[str]
    reason: NotRequired[str]


class ControlResult(TypedDict):
    session_id: str
    desired: str
    observed: str


class CancelParams(TypedDict):
    operation: OperationKey
    task_id: NotRequired[str]
    operation_id: NotRequired[str]
    delivery_id: NotRequired[str]  # exactly one of the three


class CancelResult(TypedDict):
    target: str
    status: str  # OperationStatus | PeerDeliveryStatus
    tombstone_seq: NotRequired[int]


class ReviewParams(TypedDict):
    operation: OperationKey
    task_id: str
    attempt_id: str
    verdict: NotRequired[str]
    decision: NotRequired[str]  # accepted | rejected (matches store.review)
    evidence: str
    expected_revision: NotRequired[int]  # session revision CAS check


class ReviewResult(TypedDict):
    task_id: str
    status: str


class EventsParams(TypedDict):
    after: int
    limit: NotRequired[int]


class PeerEvent(TypedDict):
    seq: int  # node-local; meaningful only with its NodeRef (ADR-10)
    type: str
    ref: dict[str, str]  # ObjectRef.as_dict()
    payload: dict[str, Any]
    created: float


class EventsResult(Page):
    items: list[PeerEvent]


class ResultParams(TypedDict):
    task_id: str
    artifact_id: NotRequired[str]
    offset: NotRequired[int]
    limit: NotRequired[int]


class ResultResult(TypedDict):
    task_id: str
    status: str
    summary: NotRequired[str]
    artifacts: list[ArtifactRef]
    chunk: NotRequired[dict[str, Any]]  # {artifact_id, offset, data, eof}


class InboxParams(TypedDict):
    group_id: str
    runtime: str
    native_id: str
    after: NotRequired[int]
    limit: NotRequired[int]


class InboxResult(Page):
    messages: list[dict[str, Any]]


class NotifyParentParams(TypedDict):
    operation: OperationKey
    parent_ref: dict[str, str]  # ObjectRef.as_dict() of the *coordinator-side* parent session
    event: PeerEvent
    result: NotRequired[ArtifactRef]


class NotifyParentResult(TypedDict):
    mirror_seq: int  # coordinator-local events.seq assigned after dedupe by (NodeRef, event.seq)
    duplicate: bool


_OBJECT_REF_KEYS = ("host_id", "node_id", "state_generation", "local_id")


def validate_object_ref(value: Any, name: str = "ref") -> dict[str, str]:
    """Return a clean ObjectRef dict (host_id,node_id,state_generation,local_id) or raise INVALID_PARAMS."""
    if not isinstance(value, dict):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name} must be an object")
    for key in _OBJECT_REF_KEYS:
        item = value.get(key)
        if not isinstance(item, str) or not item:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name}.{key} must be a non-empty string")
    return {key: value[key] for key in _OBJECT_REF_KEYS}


def _text(data: dict[str, Any], key: str, required: bool = True) -> str | None:
    value = data.get(key)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{key} must be a non-empty string")
    return value


def _int(value: Any, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name} must be an integer >= {minimum}")
    return value


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name} must be a number")
    return float(value)


def _object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name} must be an object")
    return value


def _operation_key(value: Any) -> OperationKey:
    value = _object(value, "operation")
    return {
        "node_id": _text(value, "node_id"),  # type: ignore[typeddict-item]
        "state_generation": _text(value, "state_generation"),  # type: ignore[typeddict-item]
        "operation_id": _text(value, "operation_id"),  # type: ignore[typeddict-item]
    }


def normalize_outbox_item(data: dict[str, Any]) -> OutboxItem:
    data = _object(data, "outbox item")
    revision = data.get("authorization_revision")
    seq = data.get("seq")
    return {
        "operation_id": _text(data, "operation_id"),  # type: ignore[typeddict-item]
        "binding_id": _text(data, "binding_id", False),
        "request_id": _text(data, "request_id", False),
        "method": _text(data, "method"),  # type: ignore[typeddict-item]
        "payload": _object(data.get("payload"), "payload"),
        "created": _number(data.get("created"), "created"),
        "seq": None if seq is None else _int(seq, "seq", 1),
        "sender_ref": None if data.get("sender_ref") is None else validate_object_ref(data["sender_ref"], "sender_ref"),
        "authority_ref": (
            None if data.get("authority_ref") is None else validate_object_ref(data["authority_ref"], "authority_ref")
        ),
        "authorization_revision": None if revision is None else _int(revision, "authorization_revision"),
        "fingerprint": _text(data, "fingerprint", False),
        "group_id": _text(data, "group_id", False),
        "native_id": _text(data, "native_id", False),
    }


def normalize_ack_request_params(data: dict[str, Any]) -> AckRequestParams:
    data = _object(data, "params")
    params: AckRequestParams = {
        "operation_id": _text(data, "operation_id"),  # type: ignore[typeddict-item]
        "outcome": _text(data, "outcome"),  # type: ignore[typeddict-item]
        "result": _object(data.get("result"), "result"),
    }
    if data.get("operation") is not None:
        params["operation"] = _operation_key(data["operation"])
    return params


def _normalize_peer_event(value: Any) -> PeerEvent:
    value = _object(value, "event")
    return {
        "seq": _int(value.get("seq"), "event.seq"),
        "type": _text(value, "type"),  # type: ignore[typeddict-item]
        "ref": validate_object_ref(value.get("ref"), "event.ref"),
        "payload": _object(value.get("payload"), "event.payload"),
        "created": _number(value.get("created"), "event.created"),
    }


def normalize_notify_parent_params(data: dict[str, Any]) -> NotifyParentParams:
    data = _object(data, "params")
    params: NotifyParentParams = {
        "operation": _operation_key(data.get("operation")),
        "parent_ref": validate_object_ref(data.get("parent_ref"), "parent_ref"),
        "event": _normalize_peer_event(data.get("event")),
    }
    if data.get("result") is not None:
        artifact = _object(data["result"], "result")
        params["result"] = {
            "host_id": _text(artifact, "host_id"),  # type: ignore[typeddict-item]
            "artifact_id": _text(artifact, "artifact_id"),  # type: ignore[typeddict-item]
            "sha256": _text(artifact, "sha256"),  # type: ignore[typeddict-item]
            "size": _int(artifact.get("size"), "result.size"),
        }
    return params


WorkspaceAction = Literal["prepare", "chunk", "seal", "abort"]
WORKSPACE_ACTIONS = ("prepare", "chunk", "seal", "abort")
WORKSPACE_DELIVERY_MODES = ("patch", "snapshot")
WORKSPACE_MODES = ("git",)
MAX_WORKSPACE_CHUNK_BYTES = 256 * 1024
MANIFEST_SCHEMA = 1
INSPECT_KINDS = ("session", "task", "group", "operation", "node", "workspace")
WORKSPACE_SECTIONS = ("base", "missing", "status")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


class WorkspacePrepareParams(TypedDict):
    operation: OperationKey
    action: Literal["prepare"]
    workspace_id: str
    project_id: str
    manifest_sha256: str
    manifest_size: int
    delivery_mode: Literal["patch", "snapshot"]
    workspace_mode: Literal["git"]
    source_base_commit: str | None
    estimated_total_bytes: int
    authority_ref: dict[str, str]


class WorkspaceChunkParams(TypedDict):
    operation: OperationKey
    action: Literal["chunk"]
    workspace_id: str
    blob_sha256: str
    offset: int
    size: int
    chunk_sha256: str
    data: str  # base64 chunk payload, decoded length == size


class WorkspaceSealParams(TypedDict):
    operation: OperationKey
    action: Literal["seal"]
    workspace_id: str
    manifest_sha256: str
    expected_revision: int


class WorkspaceAbortParams(TypedDict):
    operation: OperationKey
    action: Literal["abort"]
    workspace_id: str
    reason: str
    expected_revision: int


class WorkspaceParams(TypedDict):
    """Union of every action's fields; `normalize_workspace_params` enforces the per-action subset."""

    operation: OperationKey
    action: WorkspaceAction
    workspace_id: str
    # prepare
    project_id: NotRequired[str]
    manifest_sha256: NotRequired[str]
    manifest_size: NotRequired[int]
    delivery_mode: NotRequired[Literal["patch", "snapshot"]]
    workspace_mode: NotRequired[Literal["git"]]
    source_base_commit: NotRequired[str | None]
    estimated_total_bytes: NotRequired[int]
    authority_ref: NotRequired[dict[str, str]]
    # chunk
    blob_sha256: NotRequired[str]
    offset: NotRequired[int]
    size: NotRequired[int]
    chunk_sha256: NotRequired[str]
    data: NotRequired[str]
    # seal / abort
    expected_revision: NotRequired[int]
    reason: NotRequired[str]


class WorkspaceResult(TypedDict):
    action: str
    workspace_id: NotRequired[str]
    status: NotRequired[str]
    revision: NotRequired[int]
    received: NotRequired[int]  # bytes durably stored so far
    missing_blobs: NotRequired[list[dict[str, Any]]]  # manifest blobs still needed (prepare)
    limits: NotRequired[dict[str, int]]
    input_digest: NotRequired[str]  # seal -> ready
    receipt: NotRequired[str]


def normalize_operation_key(value: Any) -> OperationKey:
    return _operation_key(value)


def _sha256(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{key} must be a 64-character lowercase hex sha256")
    return value


def _choice(data: dict[str, Any], key: str, choices: tuple[str, ...], default: str | None = None) -> str:
    value = data.get(key, default)
    if not isinstance(value, str) or value not in choices:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{key} must be one of {', '.join(choices)}")
    return value


def normalize_workspace_params(params: dict[str, Any]) -> dict[str, Any]:
    params = _object(params, "params")
    action = _choice(params, "action", WORKSPACE_ACTIONS)
    out: dict[str, Any] = {"operation": normalize_operation_key(params.get("operation")), "action": action}
    out["workspace_id"] = _text(params, "workspace_id")
    if action == "prepare":
        out["project_id"] = _text(params, "project_id")
        out["manifest_sha256"] = _sha256(params, "manifest_sha256")
        out["manifest_size"] = _int(params.get("manifest_size"), "manifest_size", 1)
        out["delivery_mode"] = _choice(params, "delivery_mode", WORKSPACE_DELIVERY_MODES)
        out["workspace_mode"] = _choice(params, "workspace_mode", WORKSPACE_MODES, "git")
        out["source_base_commit"] = _text(params, "source_base_commit", False)
        out["estimated_total_bytes"] = _int(params.get("estimated_total_bytes"), "estimated_total_bytes")
        out["authority_ref"] = validate_object_ref(params.get("authority_ref"), "authority_ref")
    elif action == "chunk":
        out["blob_sha256"] = _sha256(params, "blob_sha256")
        out["offset"] = _int(params.get("offset"), "offset")
        size = _int(params.get("size"), "size", 1)
        if size > MAX_WORKSPACE_CHUNK_BYTES:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, f"size must be <= {MAX_WORKSPACE_CHUNK_BYTES}")
        out["size"] = size
        out["chunk_sha256"] = _sha256(params, "chunk_sha256")
        data = params.get("data")
        if not isinstance(data, str) or len(data) > 4 * ((size + 2) // 3):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "data must be a base64 string matching size")
        try:
            decoded = base64.b64decode(data, validate=True)
        except (binascii.Error, ValueError):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "data must be valid base64") from None
        if len(decoded) != size:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "data length does not match size")
        out["data"] = data
    elif action == "seal":
        out["manifest_sha256"] = _sha256(params, "manifest_sha256")
        out["expected_revision"] = _int(params.get("expected_revision"), "expected_revision")
    else:
        out["reason"] = _text(params, "reason")
        out["expected_revision"] = _int(params.get("expected_revision"), "expected_revision")
    return out


def normalize_inspect_params(params: dict[str, Any]) -> dict[str, Any]:
    params = _object(params, "params")
    kind = _choice(params, "kind", INSPECT_KINDS)
    out: dict[str, Any] = {"kind": kind}
    if params.get("id") is not None:
        out["id"] = _text(params, "id")
    if params.get("after") is not None:
        out["after"] = _int(params["after"], "after")
    if params.get("limit") is not None:
        out["limit"] = _int(params["limit"], "limit", 1)
        if out["limit"] > MAX_PAGE_LIMIT:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, f"limit must be <= {MAX_PAGE_LIMIT}")
    if kind != "workspace":
        for key in ("section", "project_id", "base_commit"):
            if params.get(key) is not None:
                raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{key} is only valid for kind=workspace")
        return out
    section = _choice(params, "section", WORKSPACE_SECTIONS)
    out["section"] = section
    out["project_id"] = _text(params, "project_id", False)
    out["base_commit"] = _text(params, "base_commit", False)
    if section == "base":
        _text(params, "project_id")
    else:
        _text(params, "id")
    return out


# ---------------------------------------------------------------- workspace manifest (design 4.3)

class ManifestFile(TypedDict):
    path: str
    op: Literal["put", "delete"]
    size: int
    before_sha256: str | None
    sha256: str | None
    mode: int


class WorkspaceManifest(TypedDict):
    schema: int
    workspace_id: str
    project_id: str
    workspace_mode: str
    source_base_commit: str | None
    delivery_mode: Literal["patch", "snapshot"]
    selected_files: list[str]
    input_digest: str
    files: list[ManifestFile]
    patch: dict[str, Any] | None
    total_bytes: int
    file_count: int


def validate_manifest_path(value: Any, name: str = "path") -> str:
    """Canonical UTF-8 relative path: no NUL, '..', '.', empty segments, leading '/' or '.git' component."""
    if not isinstance(value, str) or not value:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name} must be a non-empty string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name} must be valid UTF-8") from None
    if "\0" in value or value.startswith("/"):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name} must be a relative path without NUL")
    for part in value.split("/"):
        if part in ("", ".", "..", ".git"):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, f"{name} is not a canonical safe relative path: {value!r}")
    return value


def _normalize_manifest_file(item: Any) -> ManifestFile:
    item = _object(item, "file")
    path = validate_manifest_path(item.get("path"))
    op = _choice(item, "op", ("put", "delete"))
    before = item.get("before_sha256")
    if before is not None:
        before = _sha256(item, "before_sha256")
    if op == "delete":
        if item.get("sha256") is not None:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "delete file must not carry sha256")
        return {"path": path, "op": "delete", "size": 0, "before_sha256": before, "sha256": None, "mode": 0}
    mode = item.get("mode")
    if isinstance(mode, bool) or not isinstance(mode, int) or not 0 <= mode <= 0o777:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "mode must be an integer permission in 0..0o777")
    return {
        "path": path, "op": "put", "size": _int(item.get("size"), "size"),
        "before_sha256": before, "sha256": _sha256(item, "sha256"), "mode": mode,
    }


def normalize_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    manifest = _object(manifest, "manifest")
    if isinstance(manifest.get("schema"), bool) or manifest.get("schema") != MANIFEST_SCHEMA:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"manifest schema must be {MANIFEST_SCHEMA}")
    delivery_mode = _choice(manifest, "delivery_mode", WORKSPACE_DELIVERY_MODES)
    selected = manifest.get("selected_files")
    if not isinstance(selected, list):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "selected_files must be a list")
    selected = [validate_manifest_path(path, "selected_files[]") for path in selected]
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "files must be a list")
    files = [_normalize_manifest_file(item) for item in raw_files]
    paths = [file["path"] for file in files]
    if len(set(paths)) != len(paths):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "duplicate manifest path")
    path_set = set(paths)
    for path in paths:
        parts = path.split("/")
        if any("/".join(parts[:i]) in path_set for i in range(1, len(parts))):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, f"manifest path conflicts with a parent file: {path!r}")
    if delivery_mode == "patch" and not path_set <= set(selected):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "patch files must be a subset of selected_files")
    patch = manifest.get("patch")
    if patch is not None:
        patch = _object(patch, "patch")
        patch = {"sha256": _sha256(patch, "sha256"), "size": _int(patch.get("size"), "patch.size")}
    file_count = _int(manifest.get("file_count"), "file_count")
    if file_count != len(files):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "file_count does not match files")
    return {
        "schema": MANIFEST_SCHEMA,
        "workspace_id": _text(manifest, "workspace_id"),
        "project_id": _text(manifest, "project_id"),
        "workspace_mode": _choice(manifest, "workspace_mode", WORKSPACE_MODES, "git"),
        "source_base_commit": _text(manifest, "source_base_commit", False),
        "delivery_mode": delivery_mode,
        "selected_files": selected,
        "input_digest": _text(manifest, "input_digest"),
        "files": files,
        "patch": patch,
        "total_bytes": _int(manifest.get("total_bytes"), "total_bytes"),
        "file_count": file_count,
    }


def compute_manifest_digest(manifest: dict[str, Any]) -> str:
    """SHA-256 over the canonical manifest JSON without its own `input_digest` field."""
    return hashlib.sha256(_canonical_bytes({k: v for k, v in manifest.items() if k != "input_digest"})).hexdigest()


def compute_input_digest(files: list[dict[str, Any]]) -> str:
    """SHA-256 over {mode,path,sha256,size} of non-deleted files sorted by UTF-8 path bytes."""
    entries = sorted((f for f in files if f.get("op") != "delete"), key=lambda f: f["path"].encode("utf-8"))
    payload = [{"mode": f["mode"], "path": f["path"], "sha256": f["sha256"], "size": f["size"]} for f in entries]
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


class ResolveDeliveryParams(TypedDict):
    operation: OperationKey
    delivery_id: str
    resolution: Literal["received", "not_received"]
    evidence: str  # non-empty
    expected_delivery_revision: int


class ResolveDeliveryResult(TypedDict):
    delivery_id: str
    status: str  # PeerDeliveryStatus
    delivery_revision: NotRequired[int]
    receipt: NotRequired[str | None]


# ---------------------------------------------------------------- remote members (design 3.1)

class RemoteMember(TypedDict):
    """agent_group.remote_members item. kind=native registers an existing native session (never starts it)."""

    host_id: str
    target_kind: Literal["bridge", "native"]
    session_id: NotRequired[str]  # target_kind == "bridge"
    runtime: NotRequired[str]  # target_kind == "native": codex | antigravity
    native_id: NotRequired[str]  # target_kind == "native"
    cwd: NotRequired[str]  # path on the remote host, informational
    label: NotRequired[str]
    node_id: NotRequired[str]  # required for remove
    state_generation: NotRequired[str]  # required for remove


def _member_text(member: dict[str, Any], key: str) -> str:
    value = member.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, f"remote member {key} must be a string")
    return value


def normalize_remote_member(member: dict[str, Any], action: str = "add") -> dict[str, Any]:
    """Validate a remote member and return canonical keys (target_id is session_id or native_id)."""
    if not isinstance(member, dict):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "remote member must be an object")
    kind = member.get("target_kind")
    alias = member.get("kind")
    if kind is None:
        kind = alias
    elif alias is not None and alias != kind:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "remote member kind conflicts with target_kind")
    if kind not in ("bridge", "native"):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "remote member target_kind must be bridge or native")
    host_id = _member_text(member, "host_id")
    if not host_id:
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "remote member host_id is required")
    session_id = _member_text(member, "session_id")
    native_id = _member_text(member, "native_id")
    runtime = _member_text(member, "runtime")
    cwd = _member_text(member, "cwd")
    if kind == "bridge":
        if not session_id:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "bridge remote member requires session_id")
        if runtime or native_id:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "bridge remote member must not set runtime or native_id")
        target_id = session_id
        cwd = ""
    else:
        if session_id:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "native remote member must not set session_id")
        if runtime not in ("codex", "antigravity"):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "native remote member runtime must be codex or antigravity")
        if not native_id:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "native remote member requires native_id")
        target_id = native_id
    node_id = _member_text(member, "node_id")
    state_generation = _member_text(member, "state_generation")
    if action == "remove" and not (node_id and state_generation):
        raise peer_error(PeerErrorCode.INVALID_PARAMS, "remove requires node_id and state_generation")
    return {
        "host_id": host_id,
        "node_id": node_id,
        "state_generation": state_generation,
        "target_kind": kind,
        "runtime": runtime,
        "target_id": target_id,
        "cwd": cwd,
        "label": _member_text(member, "label"),
    }


# ---------------------------------------------------------------- coordinator peer_outbox (design 3.2, 3.5, 3.6)

class PeerOutboxItem(TypedDict):
    delivery_id: str
    broadcast_id: str | None
    group_id: str | None
    group_revision: int | None
    stream_id: str
    target_seq: int
    host_id: str
    node_id: str
    state_generation: str
    target_kind: str
    runtime: str
    target_id: str
    operation_id: str
    payload: str
    fingerprint: str
    seq_kind: str  # SeqKind
    status: str  # PeerDeliveryStatus
    send_status: str | None
    processing_status: str | None
    receipt: str | None
    error: str | None
    revision: int
    cancel_requested: int
    blocked_by: str | None
    created: float
    updated: float
    last_attempt: float


def is_peer_outbox_head_blocking(status: str) -> bool:
    return status in (
        PeerDeliveryStatus.PENDING,
        PeerDeliveryStatus.WAITING_FOR_HOST,
        PeerDeliveryStatus.SENDING,
        PeerDeliveryStatus.ACCEPTED_REMOTE,
        PeerDeliveryStatus.UNCERTAIN,
        PeerDeliveryStatus.TOMBSTONE_PENDING,
        PeerDeliveryStatus.RESOLVING,
    )


def can_advance_peer_stream(status: str) -> bool:
    return status in (PeerDeliveryStatus.RELEASED, PeerDeliveryStatus.CANCELLED, PeerDeliveryStatus.FAILED)


class GroupBroadcast(TypedDict):
    id: str
    group_id: str
    group_revision: int
    sender_ref: str | None
    message: str
    recipient_snapshot: str
    request_key: str
    created: float
    settled_at: float | None


class BroadcastRecipient(TypedDict):
    broadcast_id: str
    recipient_key: str
    target_ref: str
    delivery_id: str | None
    task_id: str | None
    status: str
    send_status: str | None
    processing_status: str | None
    error: str | None
    updated: float


class DeliveryResolution(TypedDict):
    id: str
    delivery_id: str
    request_id: str
    fingerprint: str
    group_revision: int
    delivery_revision: int
    remote_revision: int
    request_json: str
    resolution: Literal["received", "not_received"]
    evidence: str
    actor: str
    status: str
    remote_result: str | None
    created: float
    updated: float


# ---------------------------------------------------------------- method registry / whitelist (ADR-12)

@dataclass(frozen=True, slots=True)
class MethodSpec:
    name: str
    params: type
    result: type
    mutating: bool  # mutating => params must carry `operation` (or an equivalent natural key) and be replay-safe
    description: str
    admin_only: bool = False


PEER_METHODS: dict[str, MethodSpec] = {spec.name: spec for spec in (
    MethodSpec("peer.hello", HelloParams, HelloResult, False, "Handshake, identity pinning, version negotiation"),
    MethodSpec("peer.catalog", CatalogParams, CatalogResult, False, "Sanitized model catalog"),
    MethodSpec("peer.inspect", InspectParams, InspectResult, False, "Read sessions/tasks/operations"),
    MethodSpec("peer.pull_requests", PullRequestsParams, PullRequestsResult, False, "Page node_outbox"),
    MethodSpec("peer.ack_request", AckRequestParams, AckRequestResult, True, "Persist outcome of an outbox item"),
    MethodSpec("peer.deliver", DeliverParams, DeliverResult, True, "Deliver one task/broadcast/tombstone"),
    MethodSpec("peer.lookup_delivery", LookupDeliveryParams, LookupDeliveryResult, False,
               "Resolve a delivery/operation after lost ACK"),
    MethodSpec("peer.control", ControlParams, ControlResult, True, "pause/resume/close", admin_only=True),
    MethodSpec("peer.cancel", CancelParams, CancelResult, True, "Cancel task/operation/delivery"),
    MethodSpec("peer.review", ReviewParams, ReviewResult, True, "Submit review evidence", admin_only=True),
    MethodSpec("peer.events", EventsParams, EventsResult, False, "Cursor-paged event stream"),
    MethodSpec("peer.result", ResultParams, ResultResult, False, "Result summary and paged artifact data"),
    MethodSpec("peer.inbox", InboxParams, InboxResult, False, "Native member inbox history"),
    MethodSpec("peer.notify_parent", NotifyParentParams, NotifyParentResult, True,
               "Deliver completion event to the coordinator-side parent mirror"),
    MethodSpec("peer.workspace", WorkspaceParams, WorkspaceResult, True,
               "Prepare/upload/seal/abort a project workspace", admin_only=True),
    MethodSpec("peer.resolve_delivery", ResolveDeliveryParams, ResolveDeliveryResult, True,
               "Resolve an uncertain delivery with evidence", admin_only=True),
)}


def require_method(name: Any) -> MethodSpec:
    """Whitelist gate. Relay and node dispatcher must call this before touching the local broker."""
    spec = PEER_METHODS.get(name) if isinstance(name, str) else None
    if spec is None:
        raise peer_error(PeerErrorCode.METHOD_NOT_ALLOWED, f"Method is not in the peer whitelist: {name!r}")
    return spec


def negotiate_version(offered: list[int]) -> int:
    common = [version for version in offered if version == PROTOCOL_VERSION]
    if not common:
        raise peer_error(PeerErrorCode.VERSION_MISMATCH, f"Supported protocol versions: [{PROTOCOL_VERSION}]")
    return PROTOCOL_VERSION


def ok(request_id: str, result: dict[str, Any]) -> PeerResponse:
    return {"v": PROTOCOL_VERSION, "id": request_id, "ok": True, "result": result}


def fail(request_id: str, error: BridgeError) -> PeerResponse:
    return {"v": PROTOCOL_VERSION, "id": request_id, "ok": False, "error": error.as_dict()}  # type: ignore[typeddict-item]



# ---------------------------------------------------------------- group lifecycle (C5)

GroupActivityState = Literal["active", "idle", "unknown", "deleted"]

_DURATION_UNITS = {"s": 1, "h": 3600, "d": 86400, "w": 604800}
_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?|\.\d+)([shdw]?)")


class GroupPruneRun(TypedDict):
    id: str
    request_id: str
    fingerprint: str
    cutoff: float
    status: str
    created: float
    finished: float | None


class GroupPruneItem(TypedDict):
    run_id: str
    group_id: str
    expected_revision: int
    member_snapshot: str
    check_result: str
    status: str
    reason: str | None


class GroupPruneParams(TypedDict):
    older_than: str | int | float
    dry_run: NotRequired[bool]
    request_id: str


class GroupPruneResult(TypedDict):
    run_id: str
    cutoff: float
    dry_run: bool
    items: list[dict[str, Any]]
    summary: dict[str, int]


def parse_older_than(older_than: str | int | float) -> float:
    """Return a positive duration in seconds from a number or a string like '7d', '12h', '2w', '3600s'."""
    if isinstance(older_than, bool):
        raise BridgeError("invalid_request", "older_than must be a positive number or duration string")
    if isinstance(older_than, (int, float)):
        value = float(older_than)
    elif isinstance(older_than, str):
        match = _DURATION_RE.fullmatch(older_than.strip().lower())
        if match is None:
            raise BridgeError("invalid_request", f"invalid older_than duration: {older_than!r}")
        value = float(match.group(1)) * _DURATION_UNITS[match.group(2) or "s"]
    else:
        raise BridgeError("invalid_request", "older_than must be a positive number or duration string")
    if not value > 0 or value == float("inf"):
        raise BridgeError("invalid_request", "older_than must be a positive finite duration")
    return value


def normalize_group_prune_params(params: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(params, dict):
        raise BridgeError("invalid_request", "params must be an object")
    if "older_than" not in params:
        raise BridgeError("invalid_request", "older_than is required")
    older_than = parse_older_than(params["older_than"])
    dry_run = params.get("dry_run", True)
    if not isinstance(dry_run, bool):
        raise BridgeError("invalid_request", "dry_run must be a boolean")
    request_id = params.get("request_id")
    if not isinstance(request_id, str) or not request_id:
        raise BridgeError("invalid_request", "request_id must be a non-empty string")
    return {"older_than": older_than, "dry_run": dry_run, "request_id": request_id}
