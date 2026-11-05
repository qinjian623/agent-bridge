"""Wire contract for multi-machine peering (see docs/multi-machine-interface.md).

Pure definitions: enums, identifiers, envelopes, per-method params/results and the method whitelist.
No I/O and no behaviour beyond validation helpers, so every other peer module can import it safely.
"""
from __future__ import annotations

import hashlib
import json
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
    kind: str  # session | task | group | operation | node
    id: NotRequired[str]


class InspectResult(TypedDict):
    items: list[dict[str, Any]]


class PullRequestsParams(TypedDict):
    after: int
    limit: NotRequired[int]


class OutboxItem(TypedDict):
    operation_id: str
    binding_id: str
    request_id: str
    method: str  # agent_broadcast | agent_group_inbox ... (member-role MCP methods only)
    payload: dict[str, Any]
    created: float
    seq: NotRequired[int]  # node_outbox sequence
    sender_ref: NotRequired[dict[str, str]]  # ObjectRef.as_dict() of the sending member
    authority_ref: NotRequired[dict[str, str]]  # ObjectRef.as_dict() of the authorising object
    authorization_revision: NotRequired[int]
    fingerprint: NotRequired[str]


class PullRequestsResult(Page):
    items: list[OutboxItem]
    scan_upper_seq: NotRequired[int]  # highest node_outbox seq scanned, including skipped rows


class AckRequestParams(TypedDict):
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


class WorkspaceParams(TypedDict):
    operation: OperationKey
    action: Literal["prepare", "chunk", "seal", "abort"]
    # prepare
    project_id: NotRequired[str]
    workspace_id: NotRequired[str]
    manifest: NotRequired[dict[str, Any]]
    total_size: NotRequired[int]
    # chunk
    upload_id: NotRequired[str]
    offset: NotRequired[int]
    data: NotRequired[str]  # base64 chunk payload
    sha256: NotRequired[str]
    # seal / abort
    expected_sha256: NotRequired[str]
    reason: NotRequired[str]


class WorkspaceResult(TypedDict):
    action: str
    workspace_id: NotRequired[str]
    upload_id: NotRequired[str]
    received: NotRequired[int]  # bytes durably stored so far
    status: NotRequired[str]


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
