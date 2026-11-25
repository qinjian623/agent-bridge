> 语言：[English](../ota-updates.md) | 中文

# OTA 更新

## 更新过程

更新程序检查版本兼容性，等待短请求结束，切换 Broker 版本；兼容 worker 在当前轮次结束后退出。

空闲 worker 退出后，没有排队任务的开放会话保持停止，等新任务到来才启动。Broker 限制同时启动的 worker 数量，直到原生 CLI 就绪，并在启动它们前完成数据库初始化。登记 PID 较慢时，会话继续等待。

### 兼容契约层（`__init__.py`、`store.py`、`updater.py`）

| 机制 | 位置 | 说明 |
|---|---|---|
| 包导出兼容契约 | `src/agent_bridge/__init__.py` | `MIN_COMPATIBLE_WORKER_VERSION = "0.2.0"`、`REQUIRES_WORKER_DRAIN = False`；由 `Updater.preflight()` 从目标虚拟环境读取，并由 `stage()` 持久化写入候选版本的 `release.json`。 |
| 增量列迁移 | `src/agent_bridge/store.py` | 为 `sessions` 表增加可空列 `worker_version TEXT`；`worker_starting()` / `worker_ready()` 写入版本，`worker_stopped()` 清空为 `NULL`。不做破坏性改表，也不依赖按列位置解包，旧版 worker 可继续共用同一 SQLite 文件。 |
| 双模式与版本门槛 | `src/agent_bridge/updater.py` | `busy_snapshot(..., hot=True, check_inflight=True, min_worker_version=None)`：默认 `hot=True` 忽略 `open_queued_tasks` / `active_tasks` / `live_workers`，仅检查在途操作与不兼容版本 worker（`incompatible_workers`，未知/NULL 视作 `LEGACY_WORKER_VERSION=(0,2,0)`）。当候选版 `requires_worker_drain=True` 或传入 CLI `--drain-workers` 时自动回退为 `hot=False` 全量排空。 |

### 等待短请求结束并进入维护（`broker.py`、`peer_api.py`、`store.py`）

1. **`Broker.prepare_update(hot=True, drain_timeout=15.0, min_worker_version=None)`**（`src/agent_bridge/broker.py`）：
   - 先校验参数与版本格式，再串行获取 `self._update_prepare_lock`。
   - 设置 `self.update_draining` 后先 `await asyncio.sleep(0)` 让出当前事件循环微任务，让后台循环不再开始新一轮工作，并等待已开始的操作结束：
     - 停止新派发：`DeliveryScheduler.run`、`OutboxPuller._host_loop`、`EventMirror._host_loop`（共享 `_wait_host_interval()` 等待 helper）、`mirror_backfill`、`notifications`、`native_notifications`（额外跟踪 `self._native_tasks` 并等待排空）、`scheduler`、`watch_hosts`。
     - 提前唤醒长轮询：`agent_wait` 与 `_operation(..., wait=...)` 使用 `_sleep_step()`，检测到 `update_draining` 后提前安全返回，避免持续占用等待锁 20–60 秒。
     - 请求检查：`Broker.api_call()` 使用 `_is_readonly_api_call()` 检查方法和参数：在 `update_draining` 生效时立即拒绝新的非只读 RPC（`update_busy`），只放行真正只读请求（如 `agent_hosts`、`agent_monitor`、`agent_capabilities`、`agent_groups`、`agent_group_inbox`、`agent_result`、无 `parent_id` 的 `agent_inspect`、`wait in (None, 0, 0.0)` 的 `agent_operation`、`timeout in (0, 0.0)` 的 `agent_wait`、未带 `refresh=True` 的 `agent_catalog` / `agent_broadcast_status` 与 `agent_update_status`），并在 draining 或 maintenance 期间跳过 `agent_inspect` / `agent_result` 中的 `refresh_parent_usage()` 写库；同时 `DirectPeerService.handle()`（`src/agent_bridge/peer_api.py`）在 `update_guard.operation(..., readonly=...)` 保护下，也会一并参与排空并在维护期间拒绝写入。
   - 在途排空结束后，通过 `await asyncio.to_thread(update_guard.prepare, ...)` 在工作线程获取维护租约与 `busy_snapshot`，避免同步扫描子进程或数据库阻塞 asyncio 事件循环。
   - 遇到无法靠等待排空的条件时**快速失败（fail-fast）**，无需等到 `drain_timeout`。例如 `update_state_unknown`、`incompatible_workers`，以及 `hot=False` 时的 `open_queued_tasks`、`active_tasks` 或 `live_workers`。
   - 若超时或失败退出，`finally` 会清除 `self.update_draining` 以恢复调度与写接口；成功拿到维护租约后，则在 `agent_update_finish` 时清零 `self.update_draining`。
2. **渐进式、短事务 SQLite 迁移（`src/agent_bridge/store.py`、`src/agent_bridge/worker.py`）**：
   - `Store.__init__()` 中的建表事务仅负责添加 `peer_event_mirrors` 可空 `origin_*` 列，随后由 `_finish_peer_event_mirrors_migration()` 将索引创建（`peer_event_mirrors_origin`、`peer_event_mirrors_missing_origin`）及至多 `PEER_MIRROR_MIGRATION_BATCH`（`500`）行、每批 `<= 50` 行的历史回填拆分为独立短事务，避免 `Store` 启动时长时间持有 SQLite 写锁阻塞在线 worker。
   - `Worker.event()` 对 `store.record(...)` 增加最多 3 次瞬态 `sqlite3.OperationalError` 重试与告警日志，防止数据库短暂锁竞争直接打断事件保存。

### 客户端无感跨重启重试与兼容回退（`broker.py`、`updater.py`）

- `broker.request(state, method, params, *, retry_timeout=10.0)` 拆分为 `_single_request()` 与 `_update_in_progress()`：
  - **严格限定触发条件与新鲜度窗口**：仅当 `method` 不是 `agent_update_*` 时参与重试；对 `FileNotFoundError` / `ConnectionRefusedError` 仅当检测到 `update_guard.maintenance(state)` 存在或 `update-status.json` 的 `status == "activating"` 且 `checked_at` 处于有效窗口（`-30 <= now - checked_at <= 180`）时退避重试（`min(0.15, remaining)`）；日常 Broker 未运行或残留过期草稿均保持 0 延迟失败。
  - **幂等安全约束**：若请求已写出但在 Broker 关闭瞬间遭遇 `ConnectionResetError` / `BrokenPipeError` / `EOFError` / `json.JSONDecodeError`，**仅限只读 RPC（`_READONLY_RPC_METHODS`）或携带显式幂等 `request_id` 的写请求**自动重试；未带 `request_id` 的非幂等写请求直接抛出异常，由调用方按结果不明处理，避免重复产生副作用。
  - 若旧 Broker 返回 `BridgeError("update_busy")`，同样在维护窗口期内短暂退避重试，等待新 Broker 接管后释放租约。
- **旧版 Broker 参数回退（`src/agent_bridge/updater.py`）**：`Updater._activate()` 调用 `agent_update_prepare` 时，若旧版 Broker 因不识别新参数返回 `BridgeError("invalid_request")` 且本地已退化为 non-hot 模式，自动回退为空参数 `agent_update_prepare({})` 完成握手。

### Worker / Relay 回合边界滚动升级与无损交接（`updater.py`、`cli.py`、`worker.py`、`broker.py`）

1. **共享版本切换检测器**：`updater.release_change_detector(state)` 比较受管 `current` symlink 的实际物理路径与当前运行进程 `sys.prefix`（或启动期解析路径）；若 `update.json` 异常或损坏，安全回退为 `lambda: False`。`cli._relay_release_changed()` 与 `Worker.__init__(..., release_changed=...)` 共用同一函数，消除重复代码。
2. **双条件安全退场（`_should_retire_for_release`）**：
   - 运行中的任务**不会中断**，必须先执行完当前回合。
   - 在回合边界以及每 `0.15s` 空闲轮询开始时检查 `self._should_retire_for_release()`：**必须同时满足 `release_changed()` 为 `True` 且 `update_guard.maintenance(self.state) is None`**。这样即使新版本切换后健康检查失败触发回滚，在 `maintenance` 解除前也不会误退场。
3. **无损重排队与先解锁后标记退出**：
   - 如果 `Broker.claim()` 恰好在 worker 退出前的短暂竞态窗口给它分配了下一条未启动任务，worker 会在调用 `begin_attempt()` 之前进入 `worker_stopped()`，取消这条未启动的 attempt，让任务回到 `queued`。同时，`Worker.run()` 检查历史 attempts 时，只在存在 `attempt.get("started") is not None` 时才注入 `"Continue the interrupted task..."`，保证从未真正执行过的任务保留原始指令。
   - `Worker.run()` 先退出 `{session_id}.lock` 上下文释放 `flock`，再在外层 `finally` 中调用 `worker_stopped()`，将数据库状态设为 `observed='stopped'` 与 `worker_version=NULL`；新 worker 在 `_acquire_session_lock()` 中于 `worker_pid is None` 期间最多等待 `2.0s`。
   - `Broker.launch()` 先调用 `_wait_worker_released()`（对已有锁文件使用 `O_RDWR | O_NOFOLLOW`）等待锁与对应 `systemd` 瞬态 unit 完全退出（`failed` 时自动 `reset-failed`）；若超时返回 `False`，`Broker.launch()` 抛出 `BridgeError("worker_busy")`，由 `ensure_worker()` 安全推迟到下一调度周期而不误判为 `recovery_needed`。

---
