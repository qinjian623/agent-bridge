"""Wire contract for multi-machine peering (see docs/multi-machine-interface.md).

Pure definitions: enums, identifiers, envelopes, per-method params/results and the method whitelist.
No I/O and no behaviour beyond validation helpers, so every other peer module can import it safely.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, NotRequired, TypedDict

from .errors import BridgeError

PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 1 << 20  # one JSONL frame; larger payloads must be Artifact references (ADR-15)
MAX_PAGE_LIMIT = 100
INLINE_RESULT_BYTES = 16 * 1024  # results above this are returned as ArtifactRef only


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


class SendQueueStatus(StrEnum):  # node native_send_queue.status
    PENDING = "pending"
    SENDING = "sending"
    RECEIVED = "received"
    UNCERTAIN = "uncertain"
    FAILED = "failed"


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


class PullRequestsResult(Page):
    items: list[OutboxItem]


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
