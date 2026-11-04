# 多机器接口定义（实现契约）

依据 [multi-machine-design.md](multi-machine-design.md)。本文与以下代码定义是各模块实现的共同边界；实现者只填充实现，不改签名，需改动时先改这里并说明。

## 1. 文件与归属

| 文件 | 内容 | 对应阶段 |
|---|---|---|
| `src/agent_bridge/peer_protocol.py` | 协议版本、身份引用、状态枚举、错误码、信封、14 个 `peer.*` 方法的参数/结果类型、白名单 `PEER_METHODS` / `require_method` | 全部 |
| `src/agent_bridge/node_identity.py` | `NodeIdentity`、`NodeIdentityStore`（`node_identity.json`） | A |
| `src/agent_bridge/hosts.py` | `HostConfig`、`HostStatus`、`HostRegistry` | A |
| `src/agent_bridge/peer_transport.py` | `PeerTransport`、`RelayServer`、`PeerClient`、`PeerClientFactory`、`PeerService` | A/B |
| `src/agent_bridge/peer_api.py` | 新增/扩展 MCP 工具参数类型；`DeliveryScheduler`、`OutboxPuller`、`EventMirror`、`OperationTracker` | B/C |
| `tests/test_peer_protocol.py` | 契约测试（白名单、方法与客户端一一对应、可变方法带 `operation`） | — |

现有单机代码未改动。接口均为 `Protocol` / `TypedDict`，没有任何运行时接线。

## 2. 线路协议

- 传输：`ssh <target> <remote_executable> relay`，每行一个 UTF-8 JSON，单帧不超过 `MAX_FRAME_BYTES`（1 MiB）。
- 请求 `{"v":1,"id":"<frame id>","method":"peer.x","params":{…}}`；响应 `{"v":1,"id":…,"ok":true,"result":{…}}` 或 `"ok":false,"error":{code,message}`。
- `id` 只用于帧对应，**不是幂等键**；幂等键是 `OperationKey = (node_id, state_generation, operation_id)`。
- Relay 与节点分发器必须先调用 `require_method`，白名单之外的方法一律返回 `peer_method_not_allowed`。
- 连接中断只能得到 `peer_host_offline`，**不得**转成任务失败；结果以 `peer.lookup_delivery` 查证为准。

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
| `peer.control` | 是 | 是 | pause/resume/close |
| `peer.cancel` | 是 | 否 | `task_id` / `operation_id` / `delivery_id` 三选一 |
| `peer.review` | 是 | 是 | 提交审查证据 |
| `peer.events` | 否 | 否 | 游标分页事件流 |
| `peer.result` | 否 | 否 | 结果摘要与 Artifact 分页数据 |
| `peer.inbox` | 否 | 否 | 原生成员收件箱历史 |
| `peer.notify_parent` | 是 | 否 | 完成事件进入父会话镜像，返回本地 `mirror_seq` |

> 设计文档的修订点：5.节方法清单原缺少第 9 节恢复矩阵使用的 `peer.lookup_delivery`，本契约已补入；“5 组白名单”按清单实为 14 个方法，以本表为准。

## 4. 实现必须遵守的规则

可变方法（`PeerService` 文档）：
1. 校验 `operation.node_id / state_generation`，不符返回 `peer_identity_mismatch`。
2. 同 `operation_id` 同参数 → 返回已存结果或 `operation_pending`；参数不同 → `operation_conflict`。
3. 先提交 SQLite Intent，再 `NodeIdentityStore.advance(seq)`（fsync），最后返回 ACK。
4. `node_state == state_corrupted` 时拒绝全部可变调用。

投递（`DeliveryScheduler`）：
- 每个目标 `target_seq` 严格递增；队头 `uncertain` 时后续返回 `delivery_blocked`，只能由 `agent_delivery_resolve`（非空证据 + 正确 revision）解除。
- 取消未发送消息 = 发送 `seq_kind=tombstone` 的 `peer.deliver`，保留原序号，墓碑确认后后续消息才放行。
- 重试复用同一 `delivery_id` 与 `target_seq`，绝不自动重发 `uncertain`。

身份：`NodeIdentityStore.verify_database(db_max_seq)` 发现回滚即写入 `state_corrupted`；仅 `rotate_generation` 可恢复。

事件：`EventMirror.mirror` 以 `(NodeRef, event.seq)` 去重生成本地 `events.seq`，游标只前进；`peer.notify_parent` 的 `parent_ref` 是协调端对象引用。

大内容：超过 `INLINE_RESULT_BYTES` 只返回 `ArtifactRef{host_id, artifact_id, sha256, size}`，用 `peer.result` 分页读取。

## 5. MCP 工具契约（`peer_api.py`）

| 工具 | 类型 | 参数 |
|---|---|---|
| `agent_hosts` | 新增，只读 | 无；返回 `AgentHostsResult` |
| `agent_operation` | 新增，只读 | `operation_id`, `wait?` |
| `agent_delivery_resolve` | 新增，管理员 | `delivery_id`, `resolution`(`received`/`not_received`), `evidence`, `expected_revision`, `request_id` |
| `agent_catalog` | 增强 | 增加 `host_id?`；每项增加 `host_id`, `stale`, `fetched_at` |
| `agent_open` / `agent_dispatch` | 增强 | 增加 `host_id?`, `project_id?`；`cwd` 仍是本地源目录 |
| `agent_group` | 增强 | 增加 `remote_members: list[RemoteMember]` |
| `agent_cancel` | 增强 | `task_id` / `operation_id` / `delivery_id` 互斥 |

预算：`budget_scope=host`；含远端原生成员且启用预算时返回 `peer_budget_scope`。

## 6. 错误码

见 `PeerErrorCode`：`peer_method_not_allowed`、`peer_version_mismatch`、`peer_invalid_params`、`peer_identity_mismatch`、`peer_state_corrupted`、`peer_host_offline`、`peer_host_unknown`、`operation_pending`、`operation_conflict`、`operation_not_found`、`delivery_uncertain`、`delivery_blocked`、`peer_sequence_gap`、`revision_conflict`、`peer_permission_denied`、`peer_budget_scope`、`peer_frame_too_large`、`peer_not_implemented`。

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
