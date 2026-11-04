"""Coordinator-side orchestration interfaces and new/extended MCP tool contracts (design 7 and 8)."""
from __future__ import annotations

import socket
from typing import Any, NotRequired, Protocol, TypedDict

from . import __version__
from .errors import BridgeError
from .node_identity import NodeIdentityStore
from .peer_protocol import (
    PROTOCOL_VERSION,
    ArtifactRef,
    HelloResult,
    NodeRef,
    PeerDeliveryStatus,
    PeerErrorCode,
    PeerEvent,
    PeerRequest,
    PeerResponse,
    fail,
    negotiate_version,
    ok,
    peer_error,
    require_method,
)

# ---------------------------------------------------------------- MCP tool parameter contracts

class RemoteMember(TypedDict):
    """agent_group.remote_members item. kind=native registers an existing native session (never starts it)."""

    host_id: str
    kind: str  # native (bridge members on a remote host are added via session refs in a later phase)
    runtime: str  # codex | antigravity
    native_id: str
    cwd: str  # path on the remote host, informational
    label: str


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
    resolution: str  # received | not_received  (what the evidence shows)
    evidence: str  # non-empty
    expected_revision: int  # group revision inspected by the admin
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

    def resolve(self, params: AgentDeliveryResolveParams) -> dict[str, Any]: ...


class OutboxPuller(Protocol):
    """Pulls node_outbox every HostConfig.pull_interval (design 2.2). Authorisation happens here, not on the node."""

    async def run_once(self, host_id: str) -> int:
        """peer.pull_requests -> verify Binding/group membership/revision -> execute locally ->
        peer.ack_request. Returns processed item count. Must be idempotent per operation_id."""


class EventMirror(Protocol):
    """Maps remote events into coordinator-local `events.seq` (ADR-10, ADR-16)."""

    def mirror(self, node: NodeRef, event: PeerEvent, parent_ref: dict[str, str]) -> tuple[int, bool]:
        """Dedupe on (node, event.seq). Returns (local_seq, duplicate). Never moves cursors backwards."""

    def cursor(self, node: NodeRef) -> int: ...


class OperationTracker(Protocol):
    """Stable operation ids for cross-host requests; backs agent_operation and api_requests references (ADR-06)."""

    def begin(self, *, host_id: str, request_id: str, method: str, params: dict[str, Any]) -> str:
        """Return the existing operation_id for the same (request_id, params); conflict otherwise."""

    async def refresh(self, operation_id: str) -> AgentOperationResult:
        """Resolve unknown outcomes via peer.lookup_delivery before reporting a terminal state."""


# ---------------------------------------------------------------- execution-node service (phase A3/A4)

IMPLEMENTED_PEER_METHODS = ("peer.hello", "peer.inspect")


class DirectPeerService:
    """Execution-node `PeerService` handler behind `agent-bridge relay`.

    Phase A3 implements the handshake (`peer.hello`) and the basic node inspection (`peer.inspect`). Every
    other whitelisted method is rejected with `peer_not_implemented` until its phase lands; unknown methods
    never get past `require_method`.
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
            "capabilities": list(IMPLEMENTED_PEER_METHODS),
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
