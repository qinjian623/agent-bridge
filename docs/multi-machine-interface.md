> Language: English | [中文](zh/multi-machine-interface.md)

# Multi-Machine Interface Definition (Implementation Contract)

Derived from [multi-machine-design.md](multi-machine-design.md). Together with the referenced Python modules, this defines module boundaries and shared contracts. Update this contract first before altering signatures.

## 1. File Ownership and Components

| File | Scope | Phase |
|---|---|---|
| `src/agent_bridge/peer_protocol.py` | Protocol version, identity refs, status enums, error codes, envelopes, parameter/result TypedDicts across all 16 `peer.*` methods, `PEER_METHODS` / `require_method` allowlist | All |
| `src/agent_bridge/node_identity.py` | `NodeIdentity`, `NodeIdentityStore` (`node_identity.json`) | A |
| `src/agent_bridge/hosts.py` | `HostConfig`, `HostStatus`, `HostRegistry` | A |
| `src/agent_bridge/peer_transport.py` | `PeerTransport`, `RelayServer`, `PeerClient`, `PeerClientFactory`, `PeerService` | A/B |
| `src/agent_bridge/peer_api.py` | MCP parameter types and `DirectPeerService` covering catalog, delivery, receipts, workspace, and results | B/C |
| `src/agent_bridge/broker.py` | Integrates `DeliveryScheduler`, `OutboxPuller`, `EventMirror`, and live host registry watcher | B/C |
| `tests/test_peer_protocol.py` | Contract tests (allowlist parity, client parity, mutating methods require `operation`) | — |

All components above are wired into live code. Each host owns an independent SQLite WAL database; `relay` runs over SSH calling `DirectPeerService` against its local database while the local broker spawns workers for admitted tasks. Interface `Protocol` declarations serve structural typing only and do not indicate unimplemented components.

## 2. Wire Protocol

The broker watches `config.json.hosts` and `host_pins.json` periodically and supports immediate zero-arg refresh via `agent_hosts_reload`, returning `{added, updated, removed}` (while `agent_hosts` preserves `hosts` and exposes `config_error`). Reload adds no `peer.*` methods and never weakens member restrictions.

`HostRegistry.read_configured_hosts()` validates candidate host configs without side effects; `replace_hosts()` applies in place. `PeerClientFactory.retire(host_ids)` blocks new traffic on stale handles while waiting for in-flight RPCs/pollers/ACKs; `release_retired(host_ids)` closes retired connections cleanly once replaced. Invalid batches or referenced active hosts reject changes atomically without affecting unchanged hosts. See [Dynamic host configuration](dynamic-hosts.md).

- Transport: `ssh <target> <remote_executable> relay`, one UTF-8 JSON line per frame, max `MAX_FRAME_BYTES` (1 MiB).
- Request: `{"v":1,"id":"<frame id>","method":"peer.x","params":{...}}`; response: `{"v":1,"id":...,"ok":true,"result":{...}}` or `"ok":false,"error":{code,message}`.
- Frame `id` correlates messages only—it is **not** an idempotency key; idempotency uses `OperationKey = (node_id, state_generation, operation_id)`.
- Relay and node handlers validate `require_method` first and return `peer_method_not_allowed` for unknown methods.
- Broken transports return `peer_host_offline` and **never** mark remote tasks `failed`; inspect true state via `peer.lookup_delivery`.
- `relay` revalidates on-disk config each frame and refuses requests if missing or corrupt. `peer.catalog` returns sanitized `entries`, `profiles`, and `discovery` (`refresh=true` queries provider catalogs; failures report `stale`/`error`).
- After three empty outbox or event polls, that poller waits up to two extra seconds between checks (2 → 4 seconds with the default `pull_interval`). Activity or a polling error restores the configured interval. The preliminary `peer.hello` can send `identity_only: true` to skip dynamic capability checks while still verifying node identity and the database's confirmed sequence.
- When OTA switches managed `current`, old relays finish in-flight frame responses before exiting (or reject newly arriving idle frames explicitly so coordinators spawn a fresh relay via the managed launcher without blindly retrying uncertain mutations).
- `peer.pull_requests` paginates by both item count and full UTF-8 byte limits. Single items exceeding a frame are rejected before writing `node_outbox`; oversized historical items report their `operation_id` rather than skipping or forging ACKs (coordinators may shrink page sizes down to 1 item).

## 3. Method Reference

| Method | Mutating | Admin | Purpose |
|---|---|---|---|
| `peer.hello` | No | No | Handshake: protocol version, identity pin check, `max_confirmed_seq` comparison |
| `peer.catalog` | No | No | Sanitized model catalog with `stale` flags |
| `peer.inspect` | No | No | Session / task / operation / node inspection |
| `peer.pull_requests` | No | No | Paginated `node_outbox` polling (`after` + `limit`) |
| `peer.ack_request` | Yes | No | Coordinator persists outbox acceptance outcome |
| `peer.deliver` | Yes | No | Deliver task, broadcast, or cancel tombstone (supports atomic `open` per ADR-13) |
| `peer.lookup_delivery` | No | No | Reconcile lost ACKs by `delivery_id` or `operation` |
| `peer.control` | Yes | Yes | **Not yet implemented**: pause/resume/close (`peer_not_implemented`) |
| `peer.cancel` | Yes | No | Cancel by mutually exclusive `task_id` / `operation_id` / `delivery_id` |
| `peer.review` | Yes | Yes | **Not yet implemented**: remote review submission (`peer_not_implemented`) |
| `peer.events` | No | No | Cursor-paginated event stream |
| `peer.result` | No | No | Task result summary and paginated artifact bytes |
| `peer.inbox` | No | No | **Not yet implemented**: native inbox history (`peer_not_implemented`) |
| `peer.notify_parent` | Yes | No | Deliver mirrored completion event to parent host returning `mirror_seq` |
| `peer.workspace` | Yes | Yes | Workspace transfer actions: `prepare` / `chunk` / `seal` / `abort` (all with `operation`) |
| `peer.resolve_delivery` | Yes | Yes | Resolve head-of-line `uncertain` delivery with non-empty `evidence` and `expected_delivery_revision` (`received` / `not_received`) |

> Spec reconciliation note: this contract restores `peer.lookup_delivery` (omitted in Section 5 of the early design draft) and brings the full allowlist to 16 methods with Phase C additions (`peer.workspace`, `peer.resolve_delivery`).

## 4. Implementation Rules

Mutating handlers (`PeerService`):
1. Validate `operation.node_id` / `state_generation`; mismatches return `peer_identity_mismatch`.
2. Identical `operation_id` + params -> return cached result or `operation_pending`; differing params -> `operation_conflict`.
3. Persist state mutation + `peer_commit_log.ack_seq` atomically in SQLite, call `NodeIdentityStore.advance(ack_seq)` with `fsync`, then ACK caller (`target_seq` scopes per target only, never per node).
4. Refuse all mutating RPCs when `node_state == state_corrupted`.

Deliveries (`DeliveryScheduler`):
- Strictly monotonic `target_seq` per target stream; head-of-line `uncertain` returns `delivery_blocked` until cleared via `agent_delivery_resolve` (non-empty `evidence` + exact revision).
- Cancelling un-sent messages transmits `seq_kind=tombstone` preserving `target_seq`; followers wait until tombstone confirmation.
- Retries reuse identical `delivery_id` and `target_seq` and never blindly resend `uncertain`.
- Native targets settle to `accepted_remote` upon local enqueue; periodic lookups match node, target, sequence, receipt, and turn before releasing `target_seq`.
- `peer.lookup_delivery` checks live `native_send_queue` read-only without launching or resuming Codex/AGY sessions; non-empty `cwd` persists cleanly for native wake checks.
- Remote Antigravity rejects `read-only` prior to session creation identically to local hosts.

Identity: Verify DB commit sequence never falls below `node_identity.json` (plus optional peer `last_acked_seq`). Rollbacks trigger `state_corrupted` requiring manual re-generation; never fabricate missing commit records.

Events & payloads: `EventMirror.mirror` deduplicates via `(NodeRef, event.seq)` advancing monotonic local mirrors; `parent_ref` addresses coordinator parent records. Payloads above `INLINE_RESULT_BYTES` return `ArtifactRef{host_id, artifact_id, sha256, size}` fetched via `peer.result`.

## 5. MCP Contract Extensions (`peer_api.py`)

| Tool | Change | Parameters |
|---|---|---|
| `agent_hosts` | New (read-only) | None; returns `AgentHostsResult` |
| `agent_operation` | New (read-only) | `operation_id`, `wait?` |
| `agent_delivery_resolve` | New (admin) | `delivery_id`, `resolution` (`received`/`not_received`), `evidence`, `expected_revision`, `expected_delivery_revision`, `expected_remote_revision`, `request_id` |
| `agent_catalog` | Extended | `host_id?`; returns per-entry `host_id`, `stale`, `fetched_at` |
| `agent_open` / `agent_dispatch` | Extended | `host_id?`, `project_id?`; `cwd` stays local caller path |
| `agent_group` | Extended | `remote_members: list[RemoteMember]` |
| `agent_cancel` | Extended | Mutually exclusive `task_id` / `operation_id` / `delivery_id` |

Budgets stay scoped per host (`budget_scope=host`); metered broadcasts containing remote native members fail cleanly with `peer_budget_scope`.

## 6. Error Codes

`PeerErrorCode`: `peer_method_not_allowed`, `peer_version_mismatch`, `peer_invalid_params`, `peer_identity_mismatch`, `peer_state_corrupted`, `peer_host_offline`, `peer_host_unknown`, `operation_pending`, `operation_conflict`, `operation_not_found`, `delivery_uncertain`, `delivery_blocked`, `peer_sequence_gap`, `revision_conflict`, `peer_permission_denied`, `peer_budget_scope`, `peer_frame_too_large`, `peer_not_implemented`.

Phase C error codes: `workspace_conflict`, `workspace_already_attached`, `workspace_limit_exceeded`, `workspace_project_changed`, `group_prune_blocked`, `native_stop_unverifiable`, `parent_route_conflict`. Extended `PeerDeliveryStatus`: `sending`, `tombstone_pending`, `released`, `resolving`, `failed`.

`peer_protocol.CAPABILITIES`: `groups_v1`, `ordered_delivery_v1`, `member_outbox_v1`, `event_mirror_v1`, `workspace_v1`, `session_name_v1`, `prune_guard_v1`, `task_timeout_v1`. Of the 16 allowlisted RPCs, 13 have active server handlers (`peer.workspace` and `peer.resolve_delivery` included; `peer.control`, `peer.review`, `peer.inbox` marked unimplemented above).

## 7. Work Package Mapping

| Package | Depends On | Primary Modules |
|---|---|---|
| A1 `NodeIdentityStore` + init | protocol | `node_identity.py`, `cli.py` |
| A2 `HostRegistry` + config `hosts` | protocol | `hosts.py`, `config.py` |
| A3 SSH `PeerTransport` + `PeerClient` + `relay` | A2 | `peer_transport.py`, `cli.py` |
| A4 `PeerService` (hello/catalog/inspect) + `agent_hosts` | A1, A3 | `peer_api.py`, `mcp_server.py` |
| B1 `node_outbox`, `peer_inbox`, operations schema | protocol | `store.py` |
| B2 `peer.deliver/cancel/lookup/result/events` + `OperationTracker` | A4, B1 | `broker.py` |
| C1 `native_send_queue` extraction | B1 | `native_broadcast.py`, `codex_queue.py`, `agy_wake.py` |
| C2 `DeliveryScheduler`, `OutboxPuller`, `EventMirror`, `remote_members` | B2, C1 | `groups.py`, `peer_api.py` |
