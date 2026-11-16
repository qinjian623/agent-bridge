> Language: English | [中文](zh/ota-hot-update-20261116.md)

# OTA Hot Update and Legacy Smooth Bootstrap Cutover Design (2026-11-16)

## 1. Problem Statement

Prior to hot OTA updates, `updater.busy_snapshot` strictly vetoed upgrades whenever any of the following existed, keeping long-running hosts with persistent `open` sessions or active compilation jobs permanently `deferred` (for example, a primary Linux host running ~9-10 persistent `open` workers and multi-hour jobs never observed a completely idle window):

- `open_queued_tasks`, `active_tasks`, `live_workers` (even when workers are healthy and self-contained, or merely `idle/open`)
- Short in-flight operations: `api_inflight`, `notification_sending`, `native_broadcast_sending`, `native_queue_sending`, `peer_sending`, `broker_helpers` (non-`ssh`)

Two related gaps also existed:
1. Client `broker.request()` raised `FileNotFoundError` / `ConnectionRefusedError` / `update_busy` across the brief socket gap while the broker service restarted.
2. After a broker restart, existing idle or turn-boundary workers did not retire cleanly onto the new launcher; if claimed for a task right before exit (prior to `begin_attempt`), naive attempt-count checks risked injecting `"Continue the interrupted task..."`.

---

## 2. Hot OTA Four-Layer Architecture

Following information-hiding and unidirectional scheduler/updater boundaries without killing live tasks:

### 2.1 Compatibility Contract (`__init__.py`, `store.py`, `updater.py`)

| Mechanism | Location | Description |
|---|---|---|
| Package compatibility constants | `src/agent_bridge/__init__.py` | `MIN_COMPATIBLE_WORKER_VERSION = "0.2.0"`, `REQUIRES_WORKER_DRAIN = False`; inspected by `Updater.preflight()` and written to `release.json` during `stage()`. |
| Additive schema column | `src/agent_bridge/store.py` | Adds nullable `worker_version TEXT` to `sessions`; written on `worker_starting()`/`worker_ready()` and cleared on `worker_stopped()`. Compatible with existing worker processes. |
| Dual-mode snapshot & floor | `src/agent_bridge/updater.py` | `busy_snapshot(..., hot=True, check_inflight=True, min_worker_version=None)`: `hot=True` skips `open_queued_tasks` / `active_tasks` / `live_workers`, checking only short in-flight RPCs and incompatible older workers (`LEGACY_WORKER_VERSION=(0,2,0)` when NULL). Automatically falls back to `hot=False` if `requires_worker_drain=True` or `--drain-workers` is passed. |

### 2.2 Broker Drain & Maintenance Gate (`broker.py`, `peer_api.py`, `store.py`)

1. **`Broker.prepare_update(hot=True, drain_timeout=15.0, min_worker_version=None)`** (`src/agent_bridge/broker.py`):
   - Validates args/version format and acquires `self._update_prepare_lock`.
   - Sets `self.update_draining`, yields via `await asyncio.sleep(0)`, pauses background loops (`DeliveryScheduler.run`, `OutboxPuller._host_loop`, `EventMirror._host_loop` sharing `_wait_host_interval()`, `mirror_backfill`, `notifications`, `native_notifications` with tracked `self._native_tasks`, `scheduler`, `watch_hosts`).
   - Wakes long polls early: `agent_wait` and `_operation(..., wait=...)` use `_sleep_step()` to return safely on `update_draining` instead of pinning locks for 20-60s.
   - Read/write gate: `Broker.api_call()` shares exact semantics with client `_READONLY_RPC_METHODS`: mutating RPCs immediately return `update_busy` while strictly read-only queries (`agent_hosts`, `agent_monitor`, `agent_capabilities`, `agent_groups`, `agent_group_inbox`, `agent_result`, `agent_inspect` without `parent_id`, `agent_operation` with zero `wait`, `agent_wait` with zero `timeout`, non-refresh `agent_catalog` / `agent_broadcast_status`, and `agent_update_status`) succeed without calling mutating helpers (`refresh_parent_usage()`). `DirectPeerService.handle()` participates via `update_guard.operation(..., readonly=...)`.
   - Runs `update_guard.prepare(...)` off the event loop via `asyncio.to_thread(...)`.
   - Fails fast without waiting `drain_timeout` on un-drainable states (`update_state_unknown`, `incompatible_workers`, or non-hot live work when `hot=False`).
   - Always clears `self.update_draining` in `finally` on abort/timeout, or clears on `agent_update_finish`.
2. **Short-transaction SQLite backfill (`src/agent_bridge/store.py`, `src/agent_bridge/worker.py`)**:
   - `Store.__init__()` adds nullable `origin_*` columns on `peer_event_mirrors`, then `_finish_peer_event_mirrors_migration()` builds indexes and backfills at most `PEER_MIRROR_MIGRATION_BATCH` (`500`) rows in `<= 50`-row short transactions to avoid blocking online workers.
   - `Worker.event()` retries transient `sqlite3.OperationalError` up to 3 times with warnings.

### 2.3 Client Retry Across Broker Restarts & Legacy Fallback (`broker.py`, `updater.py`)

- `broker.request(state, method, params, *, retry_timeout=10.0)` splits `_single_request()` and `_update_in_progress()`:
  - Non-`agent_update_*` methods retry `FileNotFoundError` / `ConnectionRefusedError` (with `min(0.15, remaining)` backoff) **only** when `update_guard.maintenance(state)` exists or `update-status.json` indicates an active transition (`status == "activating"` within `-30 <= now - checked_at <= 180`). Normal offline failures stay zero-delay.
  - Idempotency safeguard: if disconnect occurs after write (`ConnectionResetError` / `BrokenPipeError` / `EOFError` / `json.JSONDecodeError`), retries **only** `_READONLY_RPC_METHODS` or writes carrying an explicit `request_id`; non-idempotent writes raise cleanly.
  - Brief `BridgeError("update_busy")` also retries across active maintenance windows.
- **Legacy fallback (`src/agent_bridge/updater.py`)**: If an older broker returns `invalid_request` for new `agent_update_prepare` options in non-hot mode, `_activate()` falls back to empty args `{}`.

### 2.4 Worker / Relay Boundary Rolling Retire (`updater.py`, `cli.py`, `worker.py`, `broker.py`)

1. **Shared detector**: `updater.release_change_detector(state)` compares `current` symlink target against `sys.prefix` (defensively returning `False` on corrupt `update.json`); shared cleanly between `cli._relay_release_changed()` and `Worker.__init__`.
2. **Dual exit condition (`_should_retire_for_release`)**: Live tasks never interrupt mid-turn; idle/post-turn workers retire only when `release_changed()` is `True` **and** `update_guard.maintenance(state) is None` (preventing premature exit before health checks settle/rollback).
3. **Lossless requeue & lock-first exit**: Unstarted claimed tasks return cleanly to `queued` in `worker_stopped()` (`Continue the interrupted task...` triggers only when `attempt.started is not None`). `Worker.run()` unlocks `{session_id}.lock` before setting `observed='stopped'` / `worker_version=NULL`; `Broker.launch()` waits up to 2.0s via `_wait_worker_released()` (`O_RDWR | O_NOFOLLOW` + unit cleanup) and defers safely with `worker_busy` rather than misreporting `recovery_needed`.

---

## 3. Smooth Bootstrap Cutover from Pre-Hot-OTA Releases (`v0.2.4` / `v0.2.8` -> `v0.2.9`)

### 3.1 Context

- Secondary macOS host (`macos-node`) at `v0.2.8` with zero active workers upgraded directly via `agent-bridge update run`.
- Primary Linux host running `v0.2.4` held live multi-hour workers (`1` running + `8` persistent `idle/open` `v0.2.3`/`v0.2.4` workers + active SSH relays) where legacy `agent_update_prepare` lacked `hot=True`.

### 3.2 Bootstrap Invariants

1. **Process isolation**: Under systemd user units, `cli-agent-bridge.service` and per-worker transient `agent-bridge-*.service` units reside in sibling scopes under `app.slice`, so restarting the broker leaves running worker processes untouched.
2. **Forward/backward SQLite compatibility**: Legacy workers read/write explicit column lists and tolerate nullable `worker_version` and `origin_*` columns. Pre-migrating large historical `peer_event_mirrors` tables (~847k rows) via `Store(state)` before cutover prevents `10s` startup timeouts.
3. **No forced worker kills or online VACUUM**: Legacy workers lacking `release_change_detector()` stay alive cleanly until naturally idle/closed; large SQLite `mirrors prune --vacuum` runs strictly offline in dedicated maintenance windows.

### 3.3 Cutover Sequence Executed (`v0.2.9`)

1. **Stage**: `Updater.bootstrap()` / `stage()` verified the wheel, installed dependencies, and passed isolated `preflight()` checks without modifying `current` or production `bridge.sqlite`.
2. **Pre-migrate DB**: Staged `v0.2.9` Python instantiated `Store(state)` cleanly ahead of service switchover.
3. **Short maintenance window**: Acquired `update_guard.prepare(state, lambda s: busy_snapshot(s, broker_pid=old_pid, hot=True))` waiting only for zero in-flight RPCs/sends/helpers.
4. **Atomic pointer switch & broker restart**: Recorded rollback state, atomically swapped `current -> v0.2.9`, updated `update-status.json`, and restarted `cli-agent-bridge.service` with automatic rollback on health-check failure.
5. **Post-check**: Released maintenance lease (`agent_update_status` returned `version == "0.2.9"`, `maintenance == false`), confirmed live worker PIDs stayed undisturbed, and enabled seamless hot OTA for `v0.2.10+`.
