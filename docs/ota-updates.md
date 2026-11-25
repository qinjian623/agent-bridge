> Language: English | [中文](zh/ota-updates.md)

# OTA updates

## Update behavior

The updater checks compatibility, drains short requests, switches the broker version, and retires compatible workers after their current turn.

After an idle worker retires, an open session with no queued tasks stays stopped until new work arrives. The broker limits concurrent worker startups through native CLI readiness and initializes the database before launching workers. Slow PID registration alone does not mark a session as needing recovery.

### Compatibility Contract (`__init__.py`, `store.py`, `updater.py`)

| Mechanism | Location | Description |
|---|---|---|
| Package compatibility constants | `src/agent_bridge/__init__.py` | `MIN_COMPATIBLE_WORKER_VERSION = "0.2.0"`, `REQUIRES_WORKER_DRAIN = False`; inspected by `Updater.preflight()` and written to `release.json` during `stage()`. |
| Additive schema column | `src/agent_bridge/store.py` | Adds nullable `worker_version TEXT` to `sessions`; written on `worker_starting()`/`worker_ready()` and cleared on `worker_stopped()`. Compatible with existing worker processes. |
| Dual-mode snapshot & floor | `src/agent_bridge/updater.py` | `busy_snapshot(..., hot=True, check_inflight=True, min_worker_version=None)`: `hot=True` skips `open_queued_tasks` / `active_tasks` / `live_workers`, checking only short in-flight RPCs and incompatible older workers (`LEGACY_WORKER_VERSION=(0,2,0)` when NULL). Automatically falls back to `hot=False` if `requires_worker_drain=True` or `--drain-workers` is passed. |

### Broker Drain & Maintenance Gate (`broker.py`, `peer_api.py`, `store.py`)

1. **`Broker.prepare_update(hot=True, drain_timeout=15.0, min_worker_version=None)`** (`src/agent_bridge/broker.py`):
   - Validates args/version format and acquires `self._update_prepare_lock`.
   - Sets `self.update_draining`, yields via `await asyncio.sleep(0)`, pauses background loops (`DeliveryScheduler.run`, `OutboxPuller._host_loop`, `EventMirror._host_loop` sharing `_wait_host_interval()`, `mirror_backfill`, `notifications`, `native_notifications` with tracked `self._native_tasks`, `scheduler`, `watch_hosts`).
   - Wakes long polls early: `agent_wait` and `_operation(..., wait=...)` use `_sleep_step()` to return safely on `update_draining` instead of pinning locks for 20-60s.
   - Read/write gate: `Broker.api_call()` uses `_is_readonly_api_call()` to check the method and parameters: mutating RPCs immediately return `update_busy` while strictly read-only queries (`agent_hosts`, `agent_monitor`, `agent_capabilities`, `agent_groups`, `agent_group_inbox`, `agent_result`, `agent_inspect` without `parent_id`, `agent_operation` with zero `wait`, `agent_wait` with zero `timeout`, non-refresh `agent_catalog` / `agent_broadcast_status`, and `agent_update_status`) succeed without calling mutating helpers (`refresh_parent_usage()`). `DirectPeerService.handle()` participates via `update_guard.operation(..., readonly=...)`.
   - Runs `update_guard.prepare(...)` off the event loop via `asyncio.to_thread(...)`.
   - Fails fast without waiting `drain_timeout` on non-drainable states (`update_state_unknown`, `incompatible_workers`, or non-hot live work when `hot=False`).
   - Always clears `self.update_draining` in `finally` on abort/timeout, or clears on `agent_update_finish`.
2. **Short-transaction SQLite backfill (`src/agent_bridge/store.py`, `src/agent_bridge/worker.py`)**:
   - `Store.__init__()` adds nullable `origin_*` columns on `peer_event_mirrors`, then `_finish_peer_event_mirrors_migration()` builds indexes and backfills at most `PEER_MIRROR_MIGRATION_BATCH` (`500`) rows in `<= 50`-row short transactions to avoid blocking online workers.
   - `Worker.event()` retries transient `sqlite3.OperationalError` up to 3 times with warnings.

### Client Retry Across Broker Restarts & Legacy Fallback (`broker.py`, `updater.py`)

- `broker.request(state, method, params, *, retry_timeout=10.0)` splits `_single_request()` and `_update_in_progress()`:
  - Non-`agent_update_*` methods retry `FileNotFoundError` / `ConnectionRefusedError` (with `min(0.15, remaining)` backoff) **only** when `update_guard.maintenance(state)` exists or `update-status.json` indicates an active transition (`status == "activating"` within `-30 <= now - checked_at <= 180`). Normal offline failures stay zero-delay.
  - Idempotency safeguard: if disconnect occurs after write (`ConnectionResetError` / `BrokenPipeError` / `EOFError` / `json.JSONDecodeError`), retries **only** `_READONLY_RPC_METHODS` or writes carrying an explicit `request_id`; non-idempotent writes raise without retrying.
  - Brief `BridgeError("update_busy")` also retries across active maintenance windows.
- **Legacy fallback (`src/agent_bridge/updater.py`)**: If an older broker returns `invalid_request` for new `agent_update_prepare` options in non-hot mode, `_activate()` falls back to empty args `{}`.

### Worker / Relay Boundary Rolling Retire (`updater.py`, `cli.py`, `worker.py`, `broker.py`)

1. **Shared detector**: `updater.release_change_detector(state)` compares `current` symlink target against `sys.prefix` (defensively returning `False` on corrupt `update.json`); shared by `cli._relay_release_changed()` and `Worker.__init__`.
2. **Dual exit condition (`_should_retire_for_release`)**: Live tasks never interrupt mid-turn; idle/post-turn workers retire only when `release_changed()` is `True` **and** `update_guard.maintenance(state) is None` (preventing premature exit before health checks settle/rollback).
3. **Lossless requeue & lock-first exit**: Unstarted claimed tasks return to `queued` in `worker_stopped()` (`Continue the interrupted task...` triggers only when `attempt.started is not None`). `Worker.run()` unlocks `{session_id}.lock` before setting `observed='stopped'` / `worker_version=NULL`; `Broker.launch()` waits up to 2.0s via `_wait_worker_released()` (`O_RDWR | O_NOFOLLOW` + unit cleanup) and defers with `worker_busy` rather than misreporting `recovery_needed`.

---
