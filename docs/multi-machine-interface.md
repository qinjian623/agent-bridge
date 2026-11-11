# 多机器接口定义（实现契约）

依据 [multi-machine-design.md](multi-machine-design.md)。本文与以下代码定义是各模块实现的共同边界；实现者只填充实现，不改签名，需改动时先改这里并说明。

## 1. 文件与归属

| 文件 | 内容 | 对应阶段 |
|---|---|---|
| `src/agent_bridge/peer_protocol.py` | 协议版本、身份引用、状态枚举、错误码、信封、16 个 `peer.*` 方法的参数/结果类型、白名单 `PEER_METHODS` / `require_method` | 全部 |
| `src/agent_bridge/node_identity.py` | `NodeIdentity`、`NodeIdentityStore`（`node_identity.json`） | A |
| `src/agent_bridge/hosts.py` | `HostConfig`、`HostStatus`、`HostRegistry` | A |
| `src/agent_bridge/peer_transport.py` | `PeerTransport`、`RelayServer`、`PeerClient`、`PeerClientFactory`、`PeerService` | A/B |
| `src/agent_bridge/peer_api.py` | MCP 参数类型与 `DirectPeerService`，包含目录、投递、回执、工作区和结果处理 | B/C |
| `src/agent_bridge/broker.py` | 已接入 `DeliveryScheduler`、`OutboxPuller`、`EventMirror` 与动态主机清单 | B/C |
| `tests/test_peer_protocol.py` | 契约测试（白名单、方法与客户端一一对应、可变方法带 `operation`） | — |

上述组件已接入运行程序。每台机器有独立的 SQLite WAL 数据库；`relay` 在 SSH 进程中直接调用 `DirectPeerService` 访问本机数据库，本机 Broker 再启动 Worker 执行已入库任务。代码中保留的 `Protocol` 是类型声明，不能据此判断功能未实现。

## 2. 线路协议

管理员主机清单支持运行中更新：Broker 定期读取 `config.json.hosts` 与 `host_pins.json`，或通过无参数管理工具 `agent_hosts_reload` 立即更新。返回 `{added,updated,removed}`；`agent_hosts` 保留 `hosts`，新增 `config_error`。不会为刷新调用新增 `peer.*` 方法或放宽成员权限。

`HostRegistry.read_configured_hosts()` 只验证候选配置；`replace_hosts()` 原地应用。`PeerClientFactory.retire(host_ids)` 禁止旧句柄新发请求并等待已发请求完成；替换清单后调用 `release_retired(host_ids)`。完整 API 与轮询 ACK 先结束，未变化连接不关闭。无效输入或尚有未完成引用时整批不应用；历史路由须实际核实。详见 [动态节点检查](dynamic-hosts-20261109.md)。

- 传输：`ssh <target> <remote_executable> relay`，每行一个 UTF-8 JSON，单帧不超过 `MAX_FRAME_BYTES`（1 MiB）。
- 请求 `{"v":1,"id":"<frame id>","method":"peer.x","params":{…}}`；响应 `{"v":1,"id":…,"ok":true,"result":{…}}` 或 `"ok":false,"error":{code,message}`。
- `id` 只用于帧对应，**不是幂等键**；幂等键是 `OperationKey = (node_id, state_generation, operation_id)`。
- Relay 与节点分发器必须先调用 `require_method`，白名单之外的方法一律返回 `peer_method_not_allowed`。
- 连接中断只能得到 `peer_host_offline`，**不得**转成任务失败；结果以 `peer.lookup_delivery` 查证为准。
- `relay` 每帧重新检查配置；配置损坏或已加载文件被删除时拒绝执行请求。`peer.catalog` 保留 `entries`，生产入口同时返回 `profiles` 和 `discovery`，`refresh=true` 调用已有模型目录查询，失败通过 `stale/error` 表示。
- 自动更新切换 managed `current` 后，旧 relay 完整返回在途帧的响应再退出；静默等待输入的旧 relay 在下一帧到达时返回明确未执行错误后退出。后续请求通过固定入口启动新版，不自动重放结果不明的业务请求。
- `peer.pull_requests` 按条数和完整 UTF-8 响应字节数共同分页。新单条请求大到无法容纳一帧时，在写入 outbox 前拒绝；历史超大记录报出对应 operation_id，不跳过或伪造 ACK。连接旧 relay 时协调端可减小读取页，最小为 1 条。

## 3. 方法一览

| 方法 | 可变 | 管理员 | 说明 |
|---|---|---|---|
| `peer.hello` | 否 | 否 | 握手：版本协商、身份钉扎、`max_confirmed_seq` 对比 |
| `peer.catalog` | 否 | 否 | 脱敏模型目录，带 `stale` |
| `peer.inspect` | 否 | 否 | 会话/任务/操作/节点查询 |
| `peer.pull_requests` | 否 | 否 | 分页读取 `node_outbox`（`after` + `limit`） |
| `peer.ack_request` | 是 | 否 | 协调端回传出站请求处理结论 |
| `peer.deliver` | 是 | 否 | 投递任务/广播/取消墓碑；无 session 时携带 `open` 作复合操作（ADR-13） |
| `peer.lookup_delivery` | 否 | 否 | 丢 ACK 后按 `delivery_id` 或 `operation` 查证 |
| `peer.control` | 是 | 是 | **尚未实现**：pause/resume/close，返回 `peer_not_implemented` |
| `peer.cancel` | 是 | 否 | `task_id` / `operation_id` / `delivery_id` 三选一 |
| `peer.review` | 是 | 是 | **尚未实现**：提交审查证据，返回 `peer_not_implemented` |
| `peer.events` | 否 | 否 | 游标分页事件流 |
| `peer.result` | 否 | 否 | 结果摘要与 Artifact 分页数据 |
| `peer.inbox` | 否 | 否 | **尚未实现**：原生成员收件箱历史，返回 `peer_not_implemented` |
| `peer.notify_parent` | 是 | 否 | 完成事件进入父会话镜像，返回本地 `mirror_seq` |
| `peer.workspace` | 是 | 是 | 工作区上传：`action` = `prepare` / `chunk` / `seal` / `abort`，均带 `operation` |
| `peer.resolve_delivery` | 是 | 是 | 以非空 `evidence` 和 `expected_delivery_revision` 解决 `uncertain` 投递（`received` / `not_received`），不重发 |

> 设计文档的修订点：5.节方法清单原缺少第 9 节恢复矩阵使用的 `peer.lookup_delivery`，本契约已补入；“5 组白名单”按清单实为 14 个方法，Phase C 增加 `peer.workspace`、`peer.resolve_delivery` 后共 16 个，以本表为准。

## 4. 实现必须遵守的规则

可变方法（`PeerService` 文档）：
1. 校验 `operation.node_id / state_generation`，不符返回 `peer_identity_mismatch`。
2. 同 `operation_id` 同参数 → 返回已存结果或 `operation_pending`；参数不同 → `operation_conflict`。
3. 在事务中提交修改与 `peer_commit_log.ack_seq`，再 `NodeIdentityStore.advance(ack_seq)`（fsync），最后返回 ACK；`target_seq` 只表示目标内顺序，不能用作节点提交序号。
4. `node_state == state_corrupted` 时拒绝全部可变调用。

投递（`DeliveryScheduler`）：
- 每个目标 `target_seq` 严格递增；队头 `uncertain` 时后续返回 `delivery_blocked`，只能由 `agent_delivery_resolve`（非空证据 + 正确 revision）解除。
- 取消未发送消息 = 发送 `seq_kind=tombstone` 的 `peer.deliver`，保留原序号，墓碑确认后后续消息才放行。
- 重试复用同一 `delivery_id` 与 `target_seq`，绝不自动重发 `uncertain`。
- 原生目标入队后为 `accepted_remote`；定期查询同一投递，匹配节点、目标、序号、原生回执和 turn 后才释放下一条。`pending/enqueued/sending` 继续等待，缺失或矛盾回执转 `uncertain`；收到与处理完成分开记录。
- `peer.lookup_delivery` 读取实时 `native_send_queue`；Codex 只检查既有队列与对应用户消息，不重新发送、启动或恢复会话。远端原生成员的非空 `cwd` 随广播保存，AGY 缺少有效原目录时不启动。
- 远端 AGY 和本机一样，创建会话前拒绝 `read-only`，不先创建无法运行的会话。

身份：校验实际数据库提交序号不小于已确认值；握手提供 `last_acked_seq` 时，还检查节点不小于协调端已确认值。发现回滚即写入 `state_corrupted`；仅管理员可恢复。旧版哈希序号只在数据库中存在对应 legacy 投递时保留为迁移起点，不凭身份文件伪造数据库记录。当前 Broker 未发送 `last_acked_seq`，本机数据库/身份文件检查不能替代整机快照一起回滚的协调端检测。

事件：`EventMirror.mirror` 以 `(NodeRef, event.seq)` 去重生成本地 `events.seq`，游标只前进；`peer.notify_parent` 的 `parent_ref` 是协调端对象引用。

大内容：超过 `INLINE_RESULT_BYTES` 只返回 `ArtifactRef{host_id, artifact_id, sha256, size}`，用 `peer.result` 分页读取。

## 5. MCP 工具契约（`peer_api.py`）

| 工具 | 类型 | 参数 |
|---|---|---|
| `agent_hosts` | 新增，只读 | 无；返回 `AgentHostsResult` |
| `agent_operation` | 新增，只读 | `operation_id`, `wait?` |
| `agent_delivery_resolve` | 新增，管理员 | `delivery_id`, `resolution`(`received`/`not_received`), `evidence`, `expected_revision`, `expected_delivery_revision`, `expected_remote_revision`, `request_id` |
| `agent_catalog` | 增强 | 增加 `host_id?`；每项增加 `host_id`, `stale`, `fetched_at` |
| `agent_open` / `agent_dispatch` | 增强 | 增加 `host_id?`, `project_id?`；`cwd` 仍是本地源目录 |
| `agent_group` | 增强 | 增加 `remote_members: list[RemoteMember]` |
| `agent_cancel` | 增强 | `task_id` / `operation_id` / `delivery_id` 互斥 |

预算：`budget_scope=host`；含远端原生成员且启用预算时返回 `peer_budget_scope`。

## 6. 错误码

见 `PeerErrorCode`：`peer_method_not_allowed`、`peer_version_mismatch`、`peer_invalid_params`、`peer_identity_mismatch`、`peer_state_corrupted`、`peer_host_offline`、`peer_host_unknown`、`operation_pending`、`operation_conflict`、`operation_not_found`、`delivery_uncertain`、`delivery_blocked`、`peer_sequence_gap`、`revision_conflict`、`peer_permission_denied`、`peer_budget_scope`、`peer_frame_too_large`、`peer_not_implemented`。

Phase C 新增错误码：`workspace_conflict`、`workspace_already_attached`、`workspace_limit_exceeded`、`workspace_project_changed`、`group_prune_blocked`、`native_stop_unverifiable`、`parent_route_conflict`。`PeerDeliveryStatus` 新增 `sending`、`tombstone_pending`、`released`、`resolving`、`failed`。

`peer_protocol.CAPABILITIES`：`groups_v1`、`ordered_delivery_v1`、`member_outbox_v1`、`event_mirror_v1`、`workspace_v1`、`session_name_v1`、`prune_guard_v1`。

Phase C 接口：`RemoteMember` 使用 `target_kind`（`bridge` 带 `session_id`；`native` 带 `runtime` + `native_id`）；`peer.deliver` 支持 `ordering_mode`、`stream_id`、`authority_ref`、`parent_ref`；`peer.pull_requests` 返回 `scan_upper_seq`；`OutboxItem` 带 `seq`、`sender_ref`、`authority_ref`、`authorization_revision`、`fingerprint`。`peer.workspace` 与 `peer.resolve_delivery` 已实现；16 个白名单方法中，13 个有运行处理代码，未实现项已在上表标出。

## 7. 实现分工建议

| 工作包 | 依赖 | 主要文件 |
|---|---|---|
| A1 `NodeIdentityStore` + `init` 生成 | protocol | `node_identity.py`, `cli.py` |
| A2 `HostRegistry` + config `hosts` | protocol | `hosts.py`, `config.py` |
| A3 SSH `PeerTransport` + `PeerClient` + `relay` 命令 | A2 | `peer_transport.py`, `cli.py` |
| A4 `PeerService`（hello/catalog/inspect）+ `agent_hosts` | A1, A3 | 新 `peer_service.py`, `mcp_server.py` |
| B1 表结构 `node_outbox`、`peer_inbox`、操作表 | protocol | `store.py` |
| B2 `peer.deliver/cancel/lookup/result/events` + `OperationTracker` + `agent_operation` | A4, B1 | `broker.py` |
| C1 `native_send_queue` 抽取 | B1 | `native_broadcast.py`, `codex_queue.py`, `agy_wake.py` |
| C2 `DeliveryScheduler`、`OutboxPuller`、`EventMirror`、`remote_members` | B2, C1 | `groups.py`, `peer_api.py` |
