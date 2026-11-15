# OTA 热更新与旧版平滑引导切换设计（2026-11-16）

## 1. 背景与问题陈述

在引入 OTA 热更新前，`updater.busy_snapshot` 对以下所有指标一律一票否决，导致节点只要存在持久 `open` 会话或长时间运行任务就持续返回 `deferred`（例如生产主机 `linux-node` 常驻约 9–10 个 `open` worker 与长周期任务，无法等来全站绝对空闲窗口）：

- `open_queued_tasks`、`active_tasks`、`live_workers`（即使 worker 正在独自稳定执行，或仅仅处于 `idle/open` 状态）
- 短周期在途操作：`api_inflight`、`notification_sending`、`native_broadcast_sending`、`native_queue_sending`、`peer_sending`、`broker_helpers`（非 `ssh`）

同时还存在两个关联缺口：
1. 客户端 `broker.request()` 在 Broker 重启摘下 `broker.sock` 的短窗口内直接抛出 `FileNotFoundError` / `ConnectionRefusedError` / `update_busy`，无法对维护窗口无感过渡。
2. 新版 Broker 重启后，老版本已启动的空闲/刚完成回合的 worker 不会自动退场升级；若刚被 `claim` 分配了尚未 `begin_attempt` 的任务即退场，旧的续跑判断仅检查历史 attempt 数量，可能误注入 `"Continue the interrupted task..."`。

---

## 2. OTA 热更新架构与《代码大全》设计约束

按“将易变决策隐藏在清晰接口后、保持核心调度与更新状态单向依赖、不跨边界强杀任务”原则，分为四层协同完成升级：

### 2.1 兼容契约层（`__init__.py`、`store.py`、`updater.py`）

| 机制 | 位置 | 说明 |
|---|---|---|
| 包导出兼容契约 | `src/agent_bridge/__init__.py` | `MIN_COMPATIBLE_WORKER_VERSION = "0.2.0"`、`REQUIRES_WORKER_DRAIN = False`；由 `Updater.preflight()` 读取目标虚拟环境，并写入候选版本 `release.json`（`stage()` 持久化）。 |
| 加法式列迁移 | `src/agent_bridge/store.py` | 为 `sessions` 表增加可空列 `worker_version TEXT`；`worker_starting()` / `worker_ready()` 写入版本，`worker_stopped()` 清空为 `NULL`。无任何破坏性改表或位置解包，旧版 worker 可共用同一 SQLite 文件。 |
| 双模式与版本门槛 | `src/agent_bridge/updater.py` | `busy_snapshot(..., hot=True, check_inflight=True, min_worker_version=None)`：默认 `hot=True` 忽略 `open_queued_tasks` / `active_tasks` / `live_workers`，仅检查在途操作与不兼容版本 worker（`incompatible_workers`，未知/NULL 视作 `LEGACY_WORKER_VERSION=(0,2,0)`）。当候选版 `requires_worker_drain=True` 或传入 CLI `--drain-workers` 时自动回退 `hot=False` 全量排空。 |

### 2.2 Broker 主动排空与维护闸门（`broker.py`、`peer_api.py`、`store.py`）

1. **`Broker.prepare_update(hot=True, drain_timeout=15.0, min_worker_version=None)`**（`src/agent_bridge/broker.py`）：
   - 先做参数与版本格式校验，串行获取 `self._update_prepare_lock`。
   - 置位 `self.update_draining` 后先 `await asyncio.sleep(0)` 让出当前事件循环微任务，令后台循环暂停开启新一轮工作并等待在途任务收口：
     - 停止新派发：`DeliveryScheduler.run`、`OutboxPuller._host_loop`、`EventMirror._host_loop`（共享 `_wait_host_interval()` 等待 helper）、`mirror_backfill`、`notifications`、`native_notifications`（额外跟踪 `self._native_tasks` 并等待排空）、`scheduler`、`watch_hosts`。
     - 提前唤醒长轮询：`agent_wait` 与 `_operation(..., wait=...)` 使用 `_sleep_step()`，检测到 `update_draining` 后提前安全返回而非占死 20–60s 等待锁。
     - 写请求闸门：`Broker.api_call()` 通过 `_is_readonly_api_call()` 与客户端 `_READONLY_RPC_METHODS` 保持精确对齐：在 `update_draining` 置位时即刻拒绝新非只读 RPC（`update_busy`），只放行真正只读请求（如 `agent_hosts`、`agent_monitor`、`agent_capabilities`、`agent_groups`、`agent_group_inbox`、`agent_result`、无 `parent_id` 的 `agent_inspect`、`wait in (None, 0, 0.0)` 的 `agent_operation`、`timeout in (0, 0.0)` 的 `agent_wait`、未带 `refresh=True` 的 `agent_catalog` / `agent_broadcast_status` 与 `agent_update_status`），并在 draining 或 maintenance 期间跳过 `agent_inspect` / `agent_result` 中的 `refresh_parent_usage()` 写库；同时 `DirectPeerService.handle()`（`src/agent_bridge/peer_api.py`）在 `update_guard.operation(..., readonly=...)` 保护下天然参与排空与维护拒写。
   - 在途排空结束后，通过 `await asyncio.to_thread(update_guard.prepare, ...)` 在工作线程获取维护租约与 `busy_snapshot`，避免同步扫描子进程或数据库阻塞 asyncio 事件循环。
   - 对非热不可排空原因（如 `update_state_unknown`、`incompatible_workers`，以及 `hot=False` 时的 `open_queued_tasks`/`active_tasks`/`live_workers`）**快速失败（fail-fast）**，不空耗 `drain_timeout`。
   - 若超时或失败退出，`finally` 必清 `self.update_draining` 恢复调度与写接口；成功获取维护租约后在 `agent_update_finish` 时清零 `self.update_draining`。
2. **渐进式、短事务 SQLite 迁移（`src/agent_bridge/store.py`、`src/agent_bridge/worker.py`）**：
   - `Store.__init__()` 中的建表事务仅负责添加 `peer_event_mirrors` 可空 `origin_*` 列，随后由 `_finish_peer_event_mirrors_migration()` 将索引创建（`peer_event_mirrors_origin`、`peer_event_mirrors_missing_origin`）及至多 `PEER_MIRROR_MIGRATION_BATCH`（`500`）行、每批 `<= 50` 行的历史回填拆分为独立短事务，避免 `Store` 启动时长时间持有 SQLite 写锁阻塞在线 worker。
   - `Worker.event()` 对 `store.record(...)` 增加最多 3 次瞬态 `sqlite3.OperationalError` 重试与告警日志，防止数据库短暂锁竞争直接打断事件落盘。

### 2.3 客户端无感跨重启重试与兼容回退（`broker.py`、`updater.py`）

- `broker.request(state, method, params, *, retry_timeout=10.0)` 拆分为 `_single_request()` 与 `_update_in_progress()`：
  - **严格限定触发条件与新鲜度窗口**：仅当 `method` 不是 `agent_update_*` 时参与重试；对 `FileNotFoundError` / `ConnectionRefusedError` 仅当检测到 `update_guard.maintenance(state)` 存在或 `update-status.json` 的 `status == "activating"` 且 `checked_at` 处于有效窗口（`-30 <= now - checked_at <= 180`）时退避重试（`min(0.15, remaining)`）；日常 Broker 未运行或残留过期草稿均保持 0 延迟失败。
  - **幂等安全约束**：若请求已写出但在 Broker 关闭瞬间遭遇 `ConnectionResetError` / `BrokenPipeError` / `EOFError` / `json.JSONDecodeError`，**仅限只读 RPC（`_READONLY_RPC_METHODS`）或携带显式幂等 `request_id` 的写请求**自动重试，未带 `request_id` 的非幂等写请求直接抛出异常由调用方按不确定态处理，杜绝重复副作用。
  - 若旧 Broker 返回 `BridgeError("update_busy")`，同样在维护窗口期内短暂退避重试，等待新 Broker 接管后释放租约。
- **旧版 Broker 参数回退（`src/agent_bridge/updater.py`）**：`Updater._activate()` 调用 `agent_update_prepare` 时，若旧版 Broker 因不识别新参数返回 `BridgeError("invalid_request")` 且本地已退化为 non-hot 模式，自动回退为空参数 `agent_update_prepare({})` 完成握手。

### 2.4 Worker / Relay 回合边界滚动升级与无损交接（`updater.py`、`cli.py`、`worker.py`、`broker.py`）

1. **共享版本切换检测器**：`updater.release_change_detector(state)` 比较受管 `current` symlink 的实际物理路径与当前运行进程 `sys.prefix`（或启动期解析路径），并对异常或损坏的 `update.json` 防御性降级为 `lambda: False`；`cli._relay_release_changed()` 与 `Worker.__init__(..., release_changed=...)` 共用同一函数，消除重复代码。
2. **双条件安全退场（`_should_retire_for_release`）**：
   - 运行中的任务**绝不中断**，必先完整执行完当前回合。
   - 在回合边界与空闲 `0.15s` 轮询顶部检查 `self._should_retire_for_release()`：**必须同时满足 `release_changed()` 为 `True` 且 `update_guard.maintenance(self.state) is None`**。若新版本切过去又发生健康检查失败回滚，`maintenance` 尚未解除期间不会误退场。
3. **无损重排队与先解锁后标记退出**：
   - 若 `Broker.claim()` 恰好在 worker 退出前的微小竞态窗口给它分配了下一条未启动任务，worker 未调用 `begin_attempt()` 即进入 `worker_stopped()` 取消该未启动 attempt 使任务回到 `queued`；`Worker.run()` 检查历史 attempts 时改为仅当存在 `attempt.get("started") is not None` 才注入 `"Continue the interrupted task..."`，保证未真正执行的任务保持原始指令。
   - `Worker.run()` 先退出 `{session_id}.lock` 上下文释放 `flock` 后，再于外层 `finally` 调用 `worker_stopped()` 将数据库置为 `observed='stopped'` 与 `worker_version=NULL`；新 worker 在 `_acquire_session_lock()` 中于 `worker_pid is None` 期间最多等待 `2.0s`。
   - `Broker.launch()` 先调用 `_wait_worker_released()`（对已有锁文件使用 `O_RDWR | O_NOFOLLOW`）等待锁与对应 `systemd` 瞬态 unit 完全退出（`failed` 时自动 `reset-failed`）；若超时返回 `False`，`Broker.launch()` 抛出 `BridgeError("worker_busy")`，由 `ensure_worker()` 安全推迟到下一调度周期而不误判为 `recovery_needed`。

---

## 3. 旧版本 Broker（`v0.2.4` / `v0.2.8`）向 `v0.2.9` 的平滑引导切换设计

### 3.1 为什么最后这一跳无法依赖旧 Broker 自动完成

- 当前 Mac（`macos-node`）运行 `v0.2.8` 且无活跃任务/worker，走常规 `agent-bridge update run` 即可直接升到 `v0.2.9`。
- **`linux-node` 当前在线运行的是 `v0.2.4`**，且长期存在运行中与空闲持久化 worker（检查现场为 1 个运行中任务 `e01e60e0` / session `a79a063e` `PID <process-06-pid>`，以及 8 个 `idle/open` `v0.2.3`/`v0.2.4` worker 与在途 SSH relay）。旧版 `v0.2.4` 的 `agent_update_prepare` 尚未支持 `hot=True`，继续调用普通 `update run` 只会持续返回 `deferred`。

### 3.2 不断任务的平滑引导升级（Bootstrap Cutover）关键不变量

1. **进程隔离保障（已核实）**：在 `linux-node` 上，主服务 `cli-agent-bridge.service` 与每个 worker 的瞬态 unit（`agent-bridge-<prefix>-<sid>.service`）同在 `user@<uid>.service/app.slice` 下平级托管；`systemctl --user restart cli-agent-bridge.service` **仅重启 Broker 进程，不会向独立 systemd scope 中的 worker 发送信号**。
2. **数据库与协议前后兼容（已核实）**：全仓库 `src/agent_bridge/` 与 `v0.2.3`/`v0.2.4` 对 `sessions` 及 `peer_event_mirrors` 表均使用具名字段访问或显式列 `INSERT`，`v0.2.9` 新增的 `sessions.worker_version` 与 `peer_event_mirrors.origin_*` 均为可空列；针对 `linux-node` 上约 `84.7 万` 行的历史 `peer_event_mirrors` 表，切换前先用 `v0.2.9` 环境的 `Store(state)` 单独完成 `origin_*` 建列、建索引及首轮 `500` 行微批回填，避免新 Broker 首次启动建索引耗时过长拖垮 `10s` 健康检查或长时间持锁影响在线 Worker。
3. **不强行杀死存量进程、不在线做 VACUUM**：
   - 未内置 `release_change_detector()` 的 `v0.2.3`/`v0.2.4` 旧 worker 不主动杀停，让其在原 unit 内继续安全完成运行中与新派发任务直至自然空闲退出或关闭；之后所有新建或重开会话自动使用 `v0.2.9` worker。
   - `linux-node` 上历史遗留的约 `5.2 GiB` `bridge.sqlite` 去重压缩（`mirrors prune --vacuum`）严禁与在线进程并发运行，留待离线窗口单独处理，本次切换不触碰数据库镜像清理。

### 3.3 平滑引导切换步骤（`v0.2.9` Release 发布后在 `linux-node` 执行）

1. **预热（零副作用）**：调用已安装 `v0.2.9` 源码或包内 `Updater.bootstrap()` / `stage()` 在隔离版本目录下完成 `v0.2.9` wheel 校验、依赖安装与 `preflight()` 自检，**不改动 `current` 与 `bridge.sqlite`**。
2. **数据库安全预迁移（零断流）**：使用新 staged `v0.2.9` 虚拟环境中的 `python` 调用 `Store(state)`，将 `sessions.worker_version`、`peer_event_mirrors.origin_*` 列、两张新索引及首轮 `500` 行微批（每次至多 `50` 行）回填提前落地，不触碰在途任务或重启任何服务。
3. **短窗口准入（不断 worker）**：记录旧 Broker PID 与全部运行中 worker PID（尤其是运行中 `<process-06-pid>`），在 `v0.2.9` 运行时下通过 `update_guard.prepare(state, lambda s: busy_snapshot(s, broker_pid=old_pid, hot=True))` 获取维护租约——仅当 `api_inflight` / `notification_sending` / `native_*_sending` / `peer_sending` / 非 `ssh` `broker_helpers` 全部为 0 时放行，拒收切换瞬间的外部新写请求。
4. **原子切换并仅重启 Broker 服务**：记录回滚指针后更新 `current -> v0.2.9` 与 `update-status.json`（`activating` + `checked_at`），仅执行 `systemctl --user restart cli-agent-bridge.service`；若健康检查失败，立即回指 `current` 并再次重启恢复（绝不回滚数据库）。
5. **验收与后续**：
   - 释放维护租约并将 `update-status.json` 写为 `updated`，确认 `agent_update_status` 返回 `version == "0.2.9"`、`maintenance == false`；
   - 原有运行中 `v0.2.4` worker PID（如 `<process-06-pid>`）及启动标识保持不变、任务不被打断；因 `v0.2.4` 存量 worker 代码中尚未包含 `_should_retire_for_release()`，它们会继续平稳服务直至自然结束/关闭，新启动的 worker 与中继会话则立即运行 `v0.2.9`；
   - 后续 `v0.2.9 -> v0.2.10+` 即可直接使用内置 OTA 热更新全自动完成。
