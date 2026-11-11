"""Transport and endpoint interfaces for peer RPC. Implementations: SSH JSONL client, relay, node dispatcher."""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import math
import socket
import uuid
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Protocol, cast

from .errors import BridgeError
from .hosts import HostConfig, HostRegistry
from .peer_protocol import (
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    AckRequestParams,
    AckRequestResult,
    CancelParams,
    CancelResult,
    CatalogParams,
    CatalogResult,
    ControlParams,
    ControlResult,
    DeliverParams,
    DeliverResult,
    EventsParams,
    EventsResult,
    HelloParams,
    HelloResult,
    HostState,
    InboxParams,
    InboxResult,
    InspectParams,
    InspectResult,
    LookupDeliveryParams,
    LookupDeliveryResult,
    NodeState,
    NotifyParentParams,
    NotifyParentResult,
    PeerErrorCode,
    PeerRequest,
    PeerResponse,
    PullRequestsParams,
    PullRequestsResult,
    ResolveDeliveryParams,
    ResolveDeliveryResult,
    ResultParams,
    ResultResult,
    ReviewParams,
    ReviewResult,
    WorkspaceParams,
    WorkspaceResult,
    fail,
    peer_error,
    require_method,
)

DEFAULT_PEER_TIMEOUT = 65.0
SSH_EXECUTABLE = "ssh"
TERMINATE_TIMEOUT = 5.0


class PeerTransport(Protocol):
    """One framed JSONL channel (UTF-8, one JSON object per line, <= MAX_FRAME_BYTES).

    Coordinator implementation reuses `ssh <target> <remote_executable> relay [--state ...]` per host;
    it never opens a TCP port. Connection loss raises BridgeError(peer_host_offline) and must
    never be converted into a failed operation: the outcome is unknown until peer.lookup_delivery says otherwise.
    """

    async def call(self, request: PeerRequest, timeout: float) -> PeerResponse: ...

    async def close(self) -> None: ...


class RelayServer(Protocol):
    """`agent-bridge relay`: bridges stdin/stdout JSONL to the local broker.sock with a fresh short connection per
    frame. Must call `require_method` first, reject anything else, and never expose raw broker methods."""

    async def serve(self, stdin: Any, stdout: Any) -> None: ...


class PeerClient(Protocol):
    """Typed coordinator-side facade over PeerTransport. Every method maps 1:1 to a whitelisted peer.* name."""

    async def hello(self, params: HelloParams) -> HelloResult: ...
    async def catalog(self, params: CatalogParams) -> CatalogResult: ...
    async def inspect(self, params: InspectParams) -> InspectResult: ...
    async def pull_requests(self, params: PullRequestsParams) -> PullRequestsResult: ...
    async def ack_request(self, params: AckRequestParams) -> AckRequestResult: ...
    async def deliver(self, params: DeliverParams) -> DeliverResult: ...
    async def lookup_delivery(self, params: LookupDeliveryParams) -> LookupDeliveryResult: ...
    async def control(self, params: ControlParams) -> ControlResult: ...
    async def cancel(self, params: CancelParams) -> CancelResult: ...
    async def review(self, params: ReviewParams) -> ReviewResult: ...
    async def events(self, params: EventsParams) -> EventsResult: ...
    async def result(self, params: ResultParams) -> ResultResult: ...
    async def inbox(self, params: InboxParams) -> InboxResult: ...
    async def notify_parent(self, params: NotifyParentParams) -> NotifyParentResult: ...
    async def workspace(self, params: WorkspaceParams) -> WorkspaceResult: ...
    async def resolve_delivery(self, params: ResolveDeliveryParams) -> ResolveDeliveryResult: ...


class PeerClientFactory(Protocol):
    def client(self, host_id: str) -> PeerClient:
        """Returns a client that enforces pinned identity: hello mismatch -> mark IDENTITY_CHANGED and raise."""

    async def retire(self, host_ids: Iterable[str]) -> None:
        """Refuse new requests, finish existing requests and close only the selected clients."""

    def release_retired(self, host_ids: Iterable[str]) -> None:
        """Allow new clients after the active registry has been replaced."""


class PeerService(Protocol):
    """Execution-node handler behind the relay. Same methods as PeerClient, but synchronous to the local store.

    Contract for every mutating method:
    1. Validate `operation.node_id/state_generation` equals this node, else peer_identity_mismatch.
    2. Look up operation_id: same params -> return stored outcome (or operation_pending); different -> operation_conflict.
    3. Commit Intent to SQLite, then fsync node_identity.json (max_confirmed_seq), then reply (design 4.2).
    4. If node_state == state_corrupted reject every mutating call with peer_state_corrupted.
    """

    async def handle(self, request: PeerRequest) -> PeerResponse:
        """Whitelist check, params validation, dispatch to the typed handler, error mapping."""


# ---------------------------------------------------------------- SSH transport (phase A3, ADR-01/ADR-05)

class SshPeerTransport:
    """One persistent SSH JSONL channel, started lazily and serialized frame by frame.

    The SSH command is always executed with an argument list (never through a shell). A lost connection,
    closed pipe, exited process or timeout raises `BridgeError(peer_host_offline)`; callers must treat that as an
    unknown outcome rather than a failed operation (ADR-01/ADR-05). After a transport error the child is
    terminated so a stale frame can never be paired with a later response.
    """

    def __init__(
        self,
        config: HostConfig | str | None = None,
        ssh_target: str | None = None,
        remote_executable: str | None = None,
        remote_state: str | None = None,
        ssh_options: Sequence[str] = (),
        *,
        host_id: str | None = None,
        ssh_command: str = SSH_EXECUTABLE,
    ) -> None:
        if isinstance(config, HostConfig):
            if ssh_target is not None or remote_executable is not None or remote_state is not None or ssh_options:
                raise TypeError("Pass either a HostConfig or individual transport fields, not both")
            host_id = config.host_id
            ssh_target = config.ssh_target
            remote_executable = config.remote_executable
            remote_state = config.remote_state
            ssh_options = config.ssh_options
        elif isinstance(config, str):
            if host_id is not None and host_id != config:
                raise TypeError("host_id was provided twice")
            host_id = config
        elif config is not None:
            raise TypeError("config must be a HostConfig, a host_id string or None")

        if not isinstance(ssh_target, str) or not ssh_target:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "ssh_target is required")
        if not isinstance(remote_executable, str) or not remote_executable:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "remote_executable is required")
        if remote_state is not None and (not isinstance(remote_state, str) or not remote_state):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "remote_state must be a non-empty string")
        if isinstance(ssh_options, (str, bytes)):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "ssh_options must be a sequence of strings")
        options = tuple(ssh_options)
        if any(not isinstance(option, str) or not option for option in options):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "ssh_options must be a sequence of non-empty strings")

        self.host_id = host_id or ssh_target
        self.ssh_target = ssh_target
        self.remote_executable = remote_executable
        self.remote_state = remote_state
        self.ssh_options = options
        self.ssh_command = ssh_command
        argv = [ssh_command, *options, ssh_target, remote_executable, "relay"]
        if remote_state is not None:
            argv.extend(("--state", remote_state))
        self._argv = tuple(argv)
        self._process: asyncio.subprocess.Process | None = None
        self.connection_generation = 0
        self._lock = asyncio.Lock()

    @property
    def argv(self) -> tuple[str, ...]:
        """Exact argument vector used to start SSH (no shell, no string interpolation)."""
        return self._argv

    async def call(
        self, request: PeerRequest, timeout: float, *, expected_generation: int | None = None,
    ) -> PeerResponse:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "timeout must be a positive finite number")
        try:
            frame = json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        except (TypeError, ValueError) as error:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, f"Peer request is not JSON serializable: {error}") from error
        if len(frame) > MAX_FRAME_BYTES:
            raise peer_error(
                PeerErrorCode.FRAME_TOO_LARGE,
                f"Peer request frame is {len(frame)} bytes; the limit is {MAX_FRAME_BYTES}",
            )

        async with self._lock:
            if expected_generation is not None and (
                expected_generation != self.connection_generation
                or self._process is None
                or self._process.returncode is not None
            ):
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.HOST_OFFLINE,
                    f"SSH transport for {self.host_id} changed after identity verification; request was not sent",
                )
            process = await self._start()
            try:
                process.stdin.write(frame)
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, OSError) as error:
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.HOST_OFFLINE, f"SSH transport for {self.host_id} closed while sending: {error}"
                ) from error

            try:
                line = await asyncio.wait_for(process.stdout.readline(), timeout)
            except TimeoutError as error:
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.HOST_OFFLINE,
                    f"SSH transport for {self.host_id} timed out after {timeout:g}s waiting for a reply",
                ) from error
            except ValueError as error:  # StreamReader line limit exceeded
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.FRAME_TOO_LARGE,
                    f"SSH transport for {self.host_id} received a frame above {MAX_FRAME_BYTES} bytes",
                ) from error
            except (BrokenPipeError, ConnectionResetError, OSError) as error:
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.HOST_OFFLINE, f"SSH transport for {self.host_id} closed while reading: {error}"
                ) from error

            if not line:
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.HOST_OFFLINE,
                    f"SSH transport for {self.host_id} closed before replying; the operation outcome is unknown",
                )
            if len(line) > MAX_FRAME_BYTES:
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.FRAME_TOO_LARGE,
                    f"SSH transport for {self.host_id} received a frame above {MAX_FRAME_BYTES} bytes",
                )
            try:
                response = json.loads(line)
            except ValueError as error:
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.INVALID_PARAMS, f"Peer {self.host_id} sent malformed JSON: {error}"
                ) from error
            if not isinstance(response, dict):
                await self._stop(force=True)
                raise peer_error(PeerErrorCode.INVALID_PARAMS, f"Peer {self.host_id} sent a non-object frame")
            response_id = response.get("id")
            if response_id is not None and response_id != request.get("id"):
                await self._stop(force=True)
                raise peer_error(
                    PeerErrorCode.INVALID_PARAMS,
                    f"Peer {self.host_id} replied with id {response_id!r} instead of {request.get('id')!r}",
                )
            return cast(PeerResponse, response)

    async def close(self) -> None:
        async with self._lock:
            await self._stop()

    # ---------------------------------------------------------------- internals

    async def _start(self) -> asyncio.subprocess.Process:
        if self._process is not None and self._process.returncode is None:
            return self._process
        await self._stop()
        try:
            # Every argument is passed verbatim; shell=False is implicit in create_subprocess_exec.
            process = await asyncio.create_subprocess_exec(
                *self._argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=MAX_FRAME_BYTES + 2,
            )
        except OSError as error:
            raise peer_error(
                PeerErrorCode.HOST_OFFLINE, f"Cannot start SSH transport for {self.host_id}: {error}"
            ) from error
        self._process = process
        self.connection_generation += 1
        return process

    async def _stop(self, *, force: bool = False) -> None:
        process, self._process = self._process, None
        if process is None:
            return
        if process.stdin is not None:
            with contextlib.suppress(Exception):
                process.stdin.close()
        if process.returncode is not None:
            with contextlib.suppress(Exception):
                await process.wait()
            return
        with contextlib.suppress(ProcessLookupError):
            if force:
                process.kill()
            else:
                process.terminate()
        try:
            await asyncio.wait_for(process.wait(), TERMINATE_TIMEOUT)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            with contextlib.suppress(Exception):
                await process.wait()


# ---------------------------------------------------------------- typed client facade

class DirectPeerClient:
    """Typed `peer.*` facade over a `PeerTransport`; each call uses a fresh frame id.

    A registry-bound client verifies hello on the same connection before every business request. Calls without
    a registry remain a direct facade, for in-process services and protocol tests.
    """

    def __init__(
        self,
        transport: PeerTransport,
        *,
        timeout: float = DEFAULT_PEER_TIMEOUT,
        registry: HostRegistry | None = None,
        host_id: str | None = None,
    ) -> None:
        if registry is not None and not host_id:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "host_id is required when identity checking is enabled")
        self._transport = transport
        self._timeout = timeout
        self._registry = registry
        self._host_id = host_id
        self._lock = asyncio.Lock()
        self._retired = False
        self._hello_params: HelloParams = {
            "protocol_versions": [PROTOCOL_VERSION],
            "coordinator_id": socket.gethostname(),
            "coordinator_generation": "local",
        }

    async def hello(self, params: HelloParams) -> HelloResult:
        return cast(HelloResult, await self._call("hello", params))

    async def close(self) -> None:
        async with self._lock:
            await self._transport.close()

    def retire(self) -> None:
        self._retired = True

    async def catalog(self, params: CatalogParams) -> CatalogResult:
        return cast(CatalogResult, await self._call("catalog", params))

    async def inspect(self, params: InspectParams) -> InspectResult:
        return cast(InspectResult, await self._call("inspect", params))

    async def pull_requests(self, params: PullRequestsParams) -> PullRequestsResult:
        return cast(PullRequestsResult, await self._call("pull_requests", params))

    async def ack_request(self, params: AckRequestParams) -> AckRequestResult:
        return cast(AckRequestResult, await self._call("ack_request", params))

    async def deliver(self, params: DeliverParams) -> DeliverResult:
        return cast(DeliverResult, await self._call("deliver", params))

    async def lookup_delivery(self, params: LookupDeliveryParams) -> LookupDeliveryResult:
        return cast(LookupDeliveryResult, await self._call("lookup_delivery", params))

    async def control(self, params: ControlParams) -> ControlResult:
        return cast(ControlResult, await self._call("control", params))

    async def cancel(self, params: CancelParams) -> CancelResult:
        return cast(CancelResult, await self._call("cancel", params))

    async def review(self, params: ReviewParams) -> ReviewResult:
        return cast(ReviewResult, await self._call("review", params))

    async def events(self, params: EventsParams) -> EventsResult:
        return cast(EventsResult, await self._call("events", params))

    async def result(self, params: ResultParams) -> ResultResult:
        return cast(ResultResult, await self._call("result", params))

    async def inbox(self, params: InboxParams) -> InboxResult:
        return cast(InboxResult, await self._call("inbox", params))

    async def notify_parent(self, params: NotifyParentParams) -> NotifyParentResult:
        return cast(NotifyParentResult, await self._call("notify_parent", params))

    async def workspace(self, params: WorkspaceParams) -> WorkspaceResult:
        return cast(WorkspaceResult, await self._call("workspace", params))

    async def resolve_delivery(self, params: ResolveDeliveryParams) -> ResolveDeliveryResult:
        return cast(ResolveDeliveryResult, await self._call("resolve_delivery", params))

    # ---------------------------------------------------------------- internals

    async def _call(self, name: str, params: dict[str, Any]) -> dict[str, Any]:
        if self._retired:
            raise BridgeError("host_busy", "Host connection has been retired; obtain its current client")
        async with self._lock:
            if self._retired:
                raise BridgeError("host_busy", "Host connection has been retired; obtain its current client")
            if self._registry is None:
                return await self._request(name, params)
            verifying = True
            try:
                hello = await self._request("hello", params if name == "hello" else self._hello_params)
                self._verify_hello(hello)
                if name == "hello":
                    self._hello_params = cast(HelloParams, dict(params))
                    return hello
                verifying = False
                generation = (
                    self._transport.connection_generation if isinstance(self._transport, SshPeerTransport) else None
                )
                return await self._request(name, params, expected_generation=generation)
            except asyncio.CancelledError:
                await self._disconnect("Peer request was cancelled")
                raise
            except BridgeError as error:
                if error.code == PeerErrorCode.IDENTITY_MISMATCH:
                    await self._disconnect(str(error), HostState.IDENTITY_CHANGED)
                elif error.code == PeerErrorCode.STATE_CORRUPTED:
                    await self._disconnect(str(error), HostState.STATE_CORRUPTED)
                elif verifying or error.code == PeerErrorCode.HOST_OFFLINE:
                    await self._disconnect(str(error))
                raise

    async def _disconnect(self, detail: str, state: HostState = HostState.OFFLINE) -> None:
        if self._registry is not None and self._host_id is not None:
            self._registry.mark(self._host_id, state, detail)
        with contextlib.suppress(Exception):
            await self._transport.close()

    async def _request(
        self, name: str, params: dict[str, Any], *, expected_generation: int | None = None,
    ) -> dict[str, Any]:
        request: PeerRequest = {
            "v": PROTOCOL_VERSION,
            "id": uuid.uuid4().hex,
            "method": f"peer.{name}",
            "params": params,
        }
        try:
            if expected_generation is not None and isinstance(self._transport, SshPeerTransport):
                response = await self._transport.call(request, self._timeout, expected_generation=expected_generation)
            else:
                response = await self._transport.call(request, self._timeout)
        except (BridgeError, OSError) as error:
            await self._disconnect(str(error))
            raise
        if self._registry is not None and (
            not isinstance(response, dict)
            or isinstance(response.get("v"), bool)
            or response.get("v") != PROTOCOL_VERSION
            or response.get("id") != request["id"]
            or not isinstance(response.get("ok"), bool)
            or (response["ok"] and not isinstance(response.get("result"), dict))
            or (not response["ok"] and not isinstance(response.get("error"), dict))
        ):
            error = peer_error(PeerErrorCode.INVALID_PARAMS, "Peer returned an invalid response envelope")
            await self._disconnect(str(error))
            raise error
        if not response.get("ok"):
            error = response.get("error")
            if isinstance(error, dict):
                code = str(error.get("code") or "peer_error")
                message = str(error.get("message") or "Peer request failed")
            else:
                code, message = "peer_error", "Peer request failed"
            raise BridgeError(code, message)
        result = response.get("result")
        return cast(dict[str, Any], result if isinstance(result, dict) else {})

    def _verify_hello(self, result: dict[str, Any]) -> None:
        registry, host_id = self._registry, self._host_id
        if registry is None or host_id is None:
            return
        status = registry.status(host_id)[0]
        if status.state == HostState.IDENTITY_CHANGED:
            raise peer_error(PeerErrorCode.IDENTITY_MISMATCH, status.detail or "Host identity requires re-registration")
        if status.state == HostState.STATE_CORRUPTED:
            raise peer_error(PeerErrorCode.STATE_CORRUPTED, status.detail or "Host state requires re-registration")
        version = result.get("protocol_version")
        if isinstance(version, bool) or version != PROTOCOL_VERSION:
            raise peer_error(PeerErrorCode.VERSION_MISMATCH, "peer.hello returned an unsupported protocol version")
        node = result.get("node")
        if not isinstance(node, dict) or any(
            not isinstance(node.get(key), str) or not node[key] for key in ("node_id", "state_generation")
        ):
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "peer.hello returned no node identity")
        pinned = registry.get(host_id).pinned
        if pinned is not None:
            node_id = node.get("node_id")
            generation = node.get("state_generation")
            if node_id != pinned.node_id or generation != pinned.state_generation:
                message = (
                    f"Host {host_id} identity changed: pinned node_id={pinned.node_id!r} "
                    f"state_generation={pinned.state_generation!r}, got node_id={node_id!r} "
                    f"state_generation={generation!r}"
                )
                registry.mark(host_id, HostState.IDENTITY_CHANGED, message)
                raise peer_error(PeerErrorCode.IDENTITY_MISMATCH, message)
        if result.get("node_state") == NodeState.STATE_CORRUPTED.value:
            message = f"Host {host_id} reports a corrupted state; new requests are blocked"
            registry.mark(host_id, HostState.STATE_CORRUPTED, message)
            raise peer_error(PeerErrorCode.STATE_CORRUPTED, message)
        if result.get("node_state") != NodeState.READY.value:
            raise peer_error(PeerErrorCode.INVALID_PARAMS, "peer.hello returned an invalid node state")
        registry.mark(host_id, HostState.ONLINE)


class DefaultPeerClientFactory:
    """Creates identity-checking clients for the hosts registered in a `HostRegistry`."""

    def __init__(
        self,
        registry: HostRegistry,
        transport_factory: Callable[[HostConfig], PeerTransport] | None = None,
        *,
        timeout: float = DEFAULT_PEER_TIMEOUT,
    ) -> None:
        self._registry = registry
        self._transport_factory = transport_factory or SshPeerTransport
        self._timeout = timeout
        self._clients: dict[str, DirectPeerClient] = {}
        self._retiring: set[str] = set()

    def client(self, host_id: str) -> PeerClient:
        if host_id in self._retiring:
            raise BridgeError("host_busy", f"Host {host_id} configuration is being replaced")
        config = self._registry.get(host_id)
        if host_id not in self._clients:
            self._clients[host_id] = DirectPeerClient(
                self._transport_factory(config),
                timeout=self._timeout,
                registry=self._registry,
                host_id=host_id,
            )
        return self._clients[host_id]

    async def retire(self, host_ids: Iterable[str]) -> None:
        """Stop new calls, then drain existing calls before closing only these connections."""
        host_ids = set(host_ids)
        self._retiring.update(host_ids)
        clients = {host_id: self._clients[host_id] for host_id in host_ids if host_id in self._clients}
        for client in clients.values():
            client.retire()
        await asyncio.gather(*(client.close() for client in clients.values()))
        for host_id, client in clients.items():
            if self._clients.get(host_id) is client:
                del self._clients[host_id]

    def release_retired(self, host_ids: Iterable[str]) -> None:
        """Call after the registry has been replaced, before serving its new clients."""
        self._retiring.difference_update(host_ids)

    async def close(self) -> None:
        clients, self._clients = self._clients, {}
        await asyncio.gather(*(client.close() for client in clients.values()))


# ---------------------------------------------------------------- relay server

class DirectRelayServer:
    """Relay stdin/stdout JSONL frames to a local `PeerService`.

    The whitelist gate runs before the service is touched, so an unknown method never reaches local broker
    functionality. Input and output may be asyncio streams or plain binary file objects.
    """

    def __init__(self, service: PeerService, *, retire=None) -> None:
        self._service = service
        self._retire = retire or (lambda: False)

    async def serve(self, stdin: Any, stdout: Any) -> None:
        while True:
            try:
                line = await _readline(stdin)
            except (EOFError, asyncio.IncompleteReadError, OSError):
                break
            except ValueError as error:  # StreamReader line limit exceeded
                await _write(
                    stdout,
                    json.dumps(
                        fail("", peer_error(PeerErrorCode.FRAME_TOO_LARGE, str(error))),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\n",
                )
                continue
            if not line:
                break
            if not line.strip():
                continue  # keepalive/blank lines are not frames
            if self._retire():
                try:
                    request = json.loads(line)
                    request_id = request.get("id", "") if isinstance(request, dict) else ""
                except (ValueError, UnicodeDecodeError):
                    request_id = ""
                response = fail(request_id, peer_error(
                    PeerErrorCode.HOST_OFFLINE,
                    "Execution node release changed; reconnect before retrying; no request was executed",
                ))
            else:
                response = await self._handle_line(line)
            payload = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
            if len(payload) > MAX_FRAME_BYTES:
                payload = (
                    json.dumps(
                        fail(
                            str(response.get("id") or ""),
                            peer_error(
                                PeerErrorCode.FRAME_TOO_LARGE,
                                f"Relay response exceeds {MAX_FRAME_BYTES} bytes",
                            ),
                        ),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\n"
                )
            await _write(stdout, payload)
            if self._retire():
                break

    async def _handle_line(self, line: Any) -> PeerResponse:
        if isinstance(line, str):
            text = line
        else:
            if len(line) > MAX_FRAME_BYTES:
                return fail("", peer_error(PeerErrorCode.FRAME_TOO_LARGE, "Relay request frame is too large"))
            try:
                text = line.decode("utf-8")
            except UnicodeDecodeError as error:
                return fail("", peer_error(PeerErrorCode.INVALID_PARAMS, f"Relay frame is not UTF-8: {error}"))
        if not text.strip():
            return fail("", peer_error(PeerErrorCode.INVALID_PARAMS, "Empty relay frame"))
        try:
            request = json.loads(text)
        except ValueError as error:
            return fail("", peer_error(PeerErrorCode.INVALID_PARAMS, f"Relay frame is not valid JSON: {error}"))
        if not isinstance(request, dict):
            return fail("", peer_error(PeerErrorCode.INVALID_PARAMS, "Relay frame must be a JSON object"))
        raw_id = request.get("id")
        request_id = raw_id if isinstance(raw_id, str) else ""
        try:
            require_method(request.get("method"))
        except BridgeError as error:
            return fail(request_id, error)
        try:
            response = await self._service.handle(cast(PeerRequest, request))
        except BridgeError as error:
            return fail(request_id, error)
        if not isinstance(response, dict):
            return fail(request_id, peer_error(PeerErrorCode.INVALID_PARAMS, "Peer service returned no response"))
        return cast(PeerResponse, response)


async def _readline(stream: Any) -> Any:
    value = stream.readline()
    if inspect.isawaitable(value):
        return await value
    return value


async def _write(stream: Any, payload: bytes) -> None:
    result = stream.write(payload)
    if inspect.isawaitable(result):
        await result
    drain = getattr(stream, "drain", None)
    if drain is not None:
        drained = drain()
        if inspect.isawaitable(drained):
            await drained
        return
    flush = getattr(stream, "flush", None)
    if flush is not None:
        flushed = flush()
        if inspect.isawaitable(flushed):
            await flushed
