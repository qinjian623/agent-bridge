> 语言：[English](../multi-machine-review.md) | 中文

# 多机器设计审查记录（历史归档）

> **归档说明**：本审查记录中的全部 16 项审查意见（A1–A8 与 D1–D8）已全量集成至统一的正式设计规范：  
> 👉 **[多机器接入设计与架构规范（正式版）](multi-machine-design.md)**（见第 3 节“架构决策记录”）。  
> 本文档保留作为静态对抗性审查的历史审计底稿。

---

## 1. 审查背景与对象

- **审查日期**：2026-11-03
- **审查对象**：`docs/multi-machine-design.md` 初稿（SHA-256：`ddef0b4617a8297962f0480be377d4742fb2fd45b167b3b2f6dc64f5b6c47f1a`）及单机核心源码。
- **审查方式**：通过本机 `cli-agent-bridge` 分别执行两项独立审查。使用临时状态目录与文件副本，未改动主服务配置与真实训练会话。

| 审查方 | 程序与供应商 | 请求及实际报告的模型 | 审查结论 |
|---|---|---|---|
| **AGY** | Antigravity / google-antigravity | `gemini-4-argon-high` | completed，提出 8 项意见（5 项 P1、3 项 P2） |
| **DeepSeek** | Pi / deepseek | `deepseek-flash` | completed，提出 8 项意见（6 项 P1、2 项 P2） |

---

## 2. 审查意见与设计修订对照表

两份报告的问题经梳理合并为 16 项关键缺陷，并在正式版设计中完成了闭环解决方案：

| 编号 | 问题来源 | 核心发现 | 在正式版中的落地与解决方案 |
|---|---|---|---|
| 1 | A1、D5、D6 | 反向推送与现有 Unix socket 短连接不兼容 | 改为协调机器主动 RPC，通过 `peer.pull_requests` 定时拉取节点 outbox；Relay 对本机 Broker 保持短连接。 |
| 2 | A2、D1 | 自报 native ID 无法证明发送者真实身份 | 增加受控 Member MCP，由 Broker 签发短期不透明 Binding，校对真实 PID、`starttime` 与写锁，拒绝伪造。 |
| 3 | A6、D2 | 数据库回滚时 Generation 同步回滚，旧数据丢失无法发现 | 独立于 SQLite 备份保存 `node_identity.json`，核验 Generation 与最大确认序号，回滚立即报警并拒绝新请求。 |
| 4 | D6 | 两个节点的同名 `request_id` 发生冲突，重连无法排空 | 引入全局唯一的 `(node_id, state_generation, operation_id)` 命名空间，重连直接比对持久化 Operation。 |
| 5 | D3 | 原生外部进程写入与本地数据库无法原子提交 | 实行两阶段提交：先提交 Intent，再标记 `sending`，调用 CLI 并保存 Receipt；未确认记为 `uncertain`，禁止重发。 |
| 6 | A3 | `operation_pending` 被原单机缓存误判为冲突死锁 | `api_requests` 针对跨机操作保存 Operation 引用；断网恢复优先核验关联操作，直至终端状态明确。 |
| 7 | A4、D8 | 远端成员记录违反本地组外键约束，UUID 发生冲突 | 新增独立的 `remote_group_members`、`peer_inbox` 与去外键化的 `native_send_queue`，唯一键强制带机器三元组。 |
| 8 | A5 | 等待中的原生消息被后续新消息插队超车 | 建立严格的队头顺序与本地递增序号，遇到前序状态不明时阻断后续发送，必须通过人工 Evidence 解除。 |
| 9 | D7 | 取消未转交消息制造永久序号空洞 | 引入携带原序号的 Cancel Tombstone（取消墓碑）；后续消息必须等待取消墓碑在执行端确认后才能继续递增。 |
| 10 | A7、D4 | 远端 `event_seq` 与父机器外键冲突导致通知丢失 | 引入跨机 `parent_ref`；协调机去重后生成父机器本地镜像 `events.seq`，再接入既有的 `deliveries` / `parent_ack` 状态流。 |
| 11 | A8 | 跨机源目录与目标目录混淆、取消目标缺失 | 严格分离本地源目录 `cwd` 与远端托管工程 `project_id`；`agent_cancel` 扩展支持 `operation_id` / `delivery_id`。 |
| 12 | D5、D8 | Relay 缺乏方法白名单，远程透传越权 | Relay 严格限定 5 组 `peer.*` 方法白名单；禁用任意命令透传与远程 Quota 任意修改。 |
| 13 | A3 | 跨机复合 Open/Dispatch 步骤断网造成孤儿子会话 | 远端无 `session_id` 的派发作为原子复合操作处理，执行端在同一 Operation 下创建并找回 Session，禁止拆成两次独立 RPC。 |
| 14 | 主 agent 补充 | 共享账号跨机预算失控 | 明确定义 Token 预算仅限单机作用域（`budget_scope=host`），包含远端原生成员的广播在启用预算时严格拒绝。 |
| 15 | 主 agent 补充 | 跨机大结果与日志将通知撑爆 | 结果只回传元数据引用（`host_id + artifact_id + sha256`），大内容强制在原机按页读取。 |
| 16 | 主 agent 补充 | 断网重连事件与旧通知重放导致震荡 | 机器重启后不改变原有事件游标，已确认的收件和处理回执不随连接刷新而倒退。 |

---

## 3. 原始审查记录凭据
原始审查结果、模型返回字段、任务/attempt ID、文档版本摘要和关闭检查保存在本机私有目录：  
`<private-report-04>/`
