> 语言：[English](../swarm-protocol-plan-20261115.md) | 中文

# Bridge 多 Agent 协作协议改进设计与实施计划

创建日期：2026-11-15  
参考来源：《How to build an AI agent swarm that works without you》、[项目说明](README.md)、[任务派发与记录规范](agent-tiering.md)、[Bridge 路由规范](skills/cli-agent-bridge/resources/router-prompt.md)、[cli-agent-bridge Skill](skills/cli-agent-bridge/SKILL.md)

## 1. 背景与目标

对照多智能体协作实战方案与 `cli-agent-bridge` 当前实现可以确认：Bridge 在运行隔离、事务状态、完成判定与预算限流等底层设施上已具备完备机制；但在**任务切分拓扑、派单边界消歧、交接事实透明度、独立视角验收与上下文瘦身**方面的工程约束可以进一步固化到协作协议层。

本设计计划仅针对协作协议、派单/验收模板与 Skill 文档做增量改进，明确**不做以下改动**：
- 不修改现有 Broker、Worker、SQLite 存储（[store.py](../../src/agent_bridge/store.py)）、独立 Git worktree/副本隔离（[workspace.py](../../src/agent_bridge/workspace.py)）、事件流及 MCP/CLI 接口契约；
- 不改变已有模型的配额冷却、均衡打分与 `agent_review` 接口参数；
- 不涉及重启运行中的 `cli-agent-bridge.service` 或修改生产数据库。

## 2. 现状对照：Bridge 已有能力 vs 待补短板

| 维度 | 文章的手工方案 | `cli-agent-bridge` 现有能力 | 评估与处置 |
|---|---|---|---|
| **写冲突隔离** | 靠提示词约定每个 Worker 只写 `outputs/T[n].md` | [workspace.py](../../src/agent_bridge/workspace.py) 为 `workspace-write` 会话创建独立 Git worktree 或 `snapshot_files` 副本，输出二进制安全的 diff | **Bridge 已覆盖** |
| **状态与幂等** | 主控手工刷新 `BOARD.md` 任务表 | [store.py](../../src/agent_bridge/store.py) 基于 SQLite 事务、`request_id` 幂等、`expected_revision` 乐观锁及 `operation_id` 防重跑 | **Bridge 已覆盖** |
| **完成与截断判定** | 文件末行手写 `END` 标记防截断 | [runtimes.py](../../src/agent_bridge/runtimes.py) 与 [artifacts.py](../../src/agent_bridge/artifacts.py) 检查原生终态、`owned_processes` cgroup 进程退出，并按 UTF-8 字节分页读取大输出 | **Bridge 已覆盖** |
| **预算与模型分级** | 口头限制轮次和总费用，主控用强模型 | [routing.py](../../src/agent_bridge/routing.py) 与 [router-prompt.md](skills/cli-agent-bridge/resources/router-prompt.md) 已实现 `token_budget` 预留、429 冷却、低峰 Flash 路由、并发上限与最多重试 1 次 | **Bridge 已覆盖** |
| **任务切分拓扑** | 单模块端到端闭环 | 收敛自由转交，默认单模块端到端闭环 | **改进（3.1）** |
| **派单边界与停机红线** | 两两消歧与停机前提 | 补充 `shared_decisions`、`allowed_files`/`out_of_scope`、`stop_if` | **改进（3.2）** |
| **交接透明度** | 强制单列偏离、假设、已核验与未核验 | 强制分离 `deviations`、`assumptions`、`unverified` | **改进（3.3）** |
| **独立验收与定点返工** | 零历史、异构模型 Checker、防注入、跨产出冲突检查与定点 Fix | 固化 Checker 与定点 Fix 标准模板 | **改进（3.4）** |
| **Skill 上下文控制** | 控制经验条数并分层瘦身 | 分层精简 `SKILL.md` 并建立经验晋升规则 | **改进（3.5）** |

## 3. 详细设计方案

### 3.1 任务切分与拓扑约束（端到端模块闭环，收敛串行工序交接）
1. **单模块端到端闭环优先**：独立功能、修复或调研条目由同一执行会话在其 `worktree` 内完成“实现修改 + 配套单测/自测”闭环，仅在职责本质不同且必须独立上下文的节点（`lead` 分派 -> 并行执行 -> 独立只读 `evaluator`/`editor` 验收 -> `lead` 合并）做跨角色交接。
2. **正交并行拆分子集**：仅当子任务互相正交且无需相互等待时并发派发。
3. **收敛自由转交**：子任务负责人唯一，禁止私自转包或拉起未授权子代理；边界分歧与阻塞统一报 `lead` 裁决。

### 3.2 派单协议（Request）与 3.3 完工回报（Handoff）扩展
- Request 增加 `shared_decisions`、`allowed_files`、`out_of_scope`、`stop_if`（对齐 [agent-tiering.md](agent-tiering.md)）。
- Response 强制包含 `deviations`、`assumptions`、`unverified`、`unresolved`，严禁把未经核实的假设写入 `evidence`。

### 3.4 独立审查（Checker / `evaluator` + `agent_review`）规范
1. 代码修改、并行合并或高风险任务在调用 `agent_review(accepted)` 前必须开启全新零历史只读会话（`access="read-only"`）；优先选择已验证且支持 `read-only` 的异构模型（排除不支持 `read-only` 的 Antigravity），只接收原始要求、实际工作区差异、独立测试输出与结构化手递字段。
2. 审查提示词固定声明防间接提示词注入。
3. 逐条核对 `PASS` / `FAIL` / `UNVERIFIED`，并在多任务合并前显式核查跨产物 `conflicts`。
4. 首次明确 `FAIL` 最多允许 1 次定点修复：新开工作区必须按 Git（原 worktree + 相对 `snapshot_files` 核对 `snapshot_tree`）或 copy（原副本 + 现存输入不含已删文件并核对 `workspace.manifest`）准确移交待修版本，修复后全量复审；仍失败则升级人工处理。

### 3.5 Skill 上下文分层瘦身与经验晋升机制
`SKILL.md` 保留高频红线并链接 `README.md` 细节；日常经验遵循单条一行带日期、总数不超过 30 条，成熟规则固化进代码或模板后移除旧条目。

## 4. 分阶段实施计划与验证标准

按阶段先后修订 `router-prompt.md`、`SKILL.md` 与 `docs/agent-tiering.md`，保持 YAML 字段与链接一致、Ruff 与完整 `unittest` 通过且不干扰正在运行的服务进程。
