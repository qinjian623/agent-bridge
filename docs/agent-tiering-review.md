# Agent 分级设计审查

审查对象：`docs/agent-tiering.md`，以 worktree 的 HEAD `164bdbd8a29ef7fb8a07ddd8098a770af7ce7738` 为代码依据。本文是静态审查，没有调用真实模型、读取私有运行数据库或验证线上服务。主目录仅只读执行了 `git diff --stat -- src`，本次没有输出；不据此推断主目录与本 worktree 所有文件相同，也不把主目录修改当作已包含的实现。

遵守第 9 节已定决定：外部 JSONL、不改 bridge；Reviewer 费用单列；trailer 与 blame 只产生候选，确认后归因；统一参考阈值先记录、不自动淘汰，样本及评分充分后再决定分级。以下是对设计稿的修改建议，没有修改原稿。

## 1. 问题清单（按严重度）

### 高：H1 重试指令与 bridge 的约束冲突

设计第 5 节要求“带上拒绝原因重试”，但 `retry_task_id` 必须保持 instruction 原文及 dependencies，改变指令会得到 `invalid_retry`。代码：`src/agent_bridge/store.py:39`、`:251`、`:470`。重试沿用原 task ID，清空 active_attempt，排队后再生成新 attempt；不是创建新任务。已有 session 不可换模型（`src/agent_bridge/broker.py:246`、`:256`）。

建议：区分原指令重试和修改规格。原指令重试可读取事先约定的、按版本保存的外部反馈文件，记录 feedback hash；反馈不得更改目标或验收条件。需要修改指令时创建新 bridge task，以外部 logical_task_id / supersedes 关联，继续计入同一业务任务的尝试序列。换模型创建新 session；新 worktree 的前次补丁如何带入、以哪个输入树重跑必须明确。不能假定换 session 会继承修改。

### 高：H2 现有 quality 不是逐 attempt 的 FPA，也不是 Reviewer 评分

第 7.3 节“每个 attempt 的质量记到各自模型上”过于宽泛。`task_samples` 按任务分组，再为某 profile 的配置身份生成一个样本；同配置多次尝试不是每次一个质量样本。单配置任务用任务当前 validation；混用配置时取对应配置最后一个已开始 attempt 的 review/status。匹配项包括 runtime、provider、model、effort、service_tier、env_file，不只是 profile 名称和 model。代码：`src/agent_bridge/routing.py:11`–`:71`。

建议：外部按每次 attempt 事件重算 FPA，不能直接取 bridge 的 quality。保存 session 配置的非敏感身份摘要与版本；同名 profile 改 effort、服务档位、提示词或运行程序后分开统计。bridge 的 kind 默认是 general，不包含 difficulty（`src/agent_bridge/broker.py:270`）。

### 高：H3 balanced 的样本与成本定义不能直接沿用

第 7.2 节的 0.5/0.5 权重描述成立，但遗漏前提：`default_strategy` 默认 balanced，可被任务 strategy 覆盖；质量先筛选，再去掉被其他候选同时在质量、费用和时间上压过的候选，费用/时间按候选最小值归一化，最后加权。全部候选必须有可比数据才评分；USD 不完整时统一改用 token，token/耗时也不完整则返回 Unmeasured。代码：`src/agent_bridge/config.py:104`、`src/agent_bridge/routing.py:94`、`:136`–`:198`；README“均衡选择与费用”。

其费用/耗时样本包含所有 attempt 加已登记父会话开销，要求父计数停止、完整且只关联一个任务；混模型或共享父会话不能形成完整费用样本（`routing.py:50`–`:68`）。它既不是 Executor-only 成本，也不是“总失败花费 / 成功数”。

建议：外部独立计算 Executor-only 指标，Reviewer 单列，父会话另列；可以借用归一化公式，但不称为直接沿用现有评分。记录 cost_scope、完整比例、币种/单位、价格日期；任一 attempt 价格未知则该任务 USD 未知，不当零，不把目录估价叫作实际账单（`src/agent_bridge/usage.py:32`）。

### 高：H4 外部任务名、bridge task 与 attempt 缺少可靠关联

设计示例 `feat-xxx-01` 是人定义任务名；bridge task 和 attempt 是 UUID，dispatch 返回 queued 时没有 attempt_id，领取时才创建（`src/agent_bridge/store.py:23`、`:219`、`:448`）。retry 会改变任务所属 session；从任务当前 session 推断历史模型会误记。

建议：增加 logical_task_id、bridge_instance_id、bridge_task_id、session_id、attempt_id、dispatch_request_id、bridge_event_seq、role、spec_version。每个 attempt 绑定它自身的 session。先记派发意图，再用相同 request_id 重取结果补关联，领取事件到达后绑定 attempt。bridge 与 JSONL 无跨库事务，必须允许补录和重复事件去重。不能只在 review 时记录：启动失败无 attempt，failed / recovery_needed 也未必有 review。

### 高：H5 JSONL 追加仍有并发、半行和修正冲突

第 7.6 节“只追加不需要处理并发更新或表结构迁移”不成立。并行主会话写入、进程中断、磁盘满仍会丢记录或留下半行。文中四类事件也未包含承诺的 correction；task_id 不足以定位应修正的那次 review。

建议：单机锁保护去重检查与整行追加，刷新并 fsync 后确认；schema_version、event_id、幂等键必填。correction 指向 target_event_id，限定字段、记录原因，按文件顺序生效，拒绝环及修改 ID。半行停止追加，先报告并由显式修复命令保留残片、截到最后完整换行，再恢复。跨机写入不支持，先汇集到单机记录器。字段演进仍需兼容规则。

## 2. 中、低严重度问题

| 编号 | 严重度 | 问题及具体修改建议 |
|---|---|---|
| M1 | 中 | `agent_review` 仅接收 task_id、attempt_id、decision、evidence、expected_revision、request_id（`src/agent_bridge/mcp_server.py:108`）；不接收 reject_reason/checks/review_score，也不会自动审查代码。将原因枚举写入外部记录，evidence 引用验证报告 hash。仅当前 completed attempt 可评，revision 必须最新且不可重复评（`store.py:687`）。failed/recovery_needed 记外部 outcome，不能强行调用 review。 |
| M2 | 中 | 第 7.2 节立即按阈值剔除与第 9 节暂不淘汰冲突。生成 advisory 路由表，默认不执行阈值过滤。显式指定 profile 会绕过 bridge 的 verified、质量及自动 quota 候选过滤（`routing.py:109`、`:138`、`:166`），外部自动选择前须自行检查资格，bridge 队列仍会检查 quota。 |
| M3 | 中 | verified 是配置声明的推理验证标记，目录可见不是验证；Unmeasured 是选择原因字符串，不是 profile 生命周期状态（`broker.py:145`、`routing.py:198`）。新模型清空 verified/price（`discovery.py:294`）。校准成功不自动改 bridge 配置；外部可记录验证结果，但不可冒充 bridge verified。 |
| M4 | 中 | L0 的职责写得超出现有服务：bridge 处理排队、依赖 accepted 条件、并发及预算预留，但不自动执行测试、DAG 拆分、升降级、分级预算或 max_attempts。依赖不满足时任务可以先排队，只是不会开始（`store.py:398`）。reservation 是 token 预留，无法保证硬上限（`mcp_server.py:54`，README 预算说明）。将这些动作交给外部运行器。 |
| M5 | 中 | 测试失败不能自动判“虚报”。缺依赖、测试不稳定、验证树不同都可能失败。只将明确自述的同一命令/同一修改与独立验证冲突记为 suspected，由确认者决定 confirmed；无法确认时 unknown。research 的不可测项用人工判断，不能默认 tests_pass=false。 |
| M6 | 中 | diff 必须相对输入 snapshot_tree，不能一律对 HEAD，否则用户带入的未提交文件会算作越界。还须检查未跟踪文件、重命名两端、删除、权限、二进制、符号链接和验证期间文件变化。见 `src/agent_bridge/workspace.py:110`、`:161`。测试可执行仓库脚本，必须使用受控执行环境；仅 worktree 无法隔离同 UID 读写。 |
| M7 | 中 | trailer 是 commit 级属性；同一 commit 的人工修改不会“自然算人工”。一个任务含多个模型尝试时只记最终 attempt 也不代表此前贡献。要求单任务 commit，混合人工贡献明确标 mixed 或放弃自动按行归因；合入后确认 trailer 和最终 SHA。squash/rebase 后重新检查关联；缺陷须去重、记录观察窗口与确认时间。 |
| M8 | 中 | 未定义指标分母和排除规则，容易因跳过难任务、只看成功任务、频繁改规格而美化结果。FPA 以业务任务首次 attempt 判定，换 task ID 不重置首次；原始通过率和剔除 spec/env 后通过率一起列，排除必须有理由。未结束任务单列，失败成本也纳入费用；未通过数为零时单位成功成本为 null。 |
| M9 | 中 | Beta 平滑与时间衰减不能仍只用历史 N≥10 判断可信。同时报告原始 N、加权成功/失败及有效样本量 `(sum w)^2/sum(w^2)`；旧样本不能无限保持资格。探索保留抽样概率、候选集、随机种子，受权限/风险/费用限制；10% 是长期目标，不能为满足比例强行派发。 |
| M10 | 中 | Reviewer 可能偏好某程序风格，评分可被 Executor 文本诱导。审查规格和固定 diff，弱化模型身份；分开记录评分与最终 accepted。定期双评少量样本、人工裁决分歧；防止任务作者自行改难度/测试/结果。零缺陷不等于已观察足够久。 |
| M11 | 中 | JSONL、trailer、Reviewer 输入可能泄露仓库名、绝对路径、账号、提示词或源代码。外部目录 700、文件 600，字段白名单，仅保存短结论、内容 hash 和受控引用；不复制原始聊天/凭据/数据库。公开提交 trailer 不写账号及认证路径，标识可用不透明 ID。定义保留期限和脱敏导出。 |
| L1 | 低 | “平均返工轮数”目前写成 attempt 数；应定义为 max(attempt_count−1,0)，另列平均尝试数。输出 token / diff 行数在空 diff、文档、删除和重构时不可比；分任务类型且零行时 null，不用于自动路由。 |
| L2 | 低 | 6.4“定级准确率”实际列的是升级率及低难度用 L3 的比例，缺少正确标签。分别命名；只在有人工参考等级时计算准确率。合理求助率也需人工确认“真实歧义”的分母。 |

## 3. 可取数据和取法

以下命令仅展示接口，本文没有执行。替换 `$STATE`、ID 和 request_id；`--state` 是全局参数。

```bash
agent-bridge --state "$STATE" call agent_catalog --json '{}'
agent-bridge --state "$STATE" call agent_inspect --json '{"task_id":"<bridge task UUID>"}'
agent-bridge --state "$STATE" call agent_wait --json '{"after":0,"timeout":20,"task_id":"<bridge task UUID>"}'
agent-bridge --state "$STATE" call agent_result --json '{"task_id":"<bridge task UUID>","attempt_id":"<attempt UUID>"}'
```

| 所需数据 | 取法、字段及限制 |
|---|---|
| 配置、模型、资格、价格信息 | catalog.profiles/discovery；objective、quota_cooldowns。inspect(session_id).profile 取实际保存配置；不要用今天的配置代替历史配置。目录允许输入类型不代表 bridge 已传递图片，当前任务是文本。 |
| task 与 attempt、状态及用量 | inspect(task_id) / result：id、session_id、status、validation、active_attempt、attempt_count、attempts[].{id,session_id,status,started,finished,usage,result}。实现见 `store.py:274`、`:877`，`broker.py:343`。usage.total_tokens、complete、catalog_estimate.{usd,complete,source,price_date}；字段可能缺失。 |
| attempt 关联、开始/结束及 review | wait.events 的 seq、kind、task_id、attempt_id、session_id、data、created（`store.py:160`、`:1121`）；保存 cursor，重放去重。task_assigned 先于 started，task_finished 不代表 accepted；task_reviewed 才有 decision。 |
| 结果、测试自述、补丁 | result.result、artifacts；长文本用 artifact_id 和 next_offset 分页读取（`src/agent_bridge/artifacts.py:32`）。摘要及默认最近 20 次不能代替完整历史；已知旧 attempt_id 可指定读取。patch 提取失败也保留工作目录，不能只统计成功补丁。 |
| review 与历史超出接口摘要的数据 | 首选持续收集 wait 事件和指定 attempt 的 result。必要时用只读 SQLite 连接读取 `$STATE/bridge.sqlite` 的 tasks、attempts、sessions、reviews、events；事务内一致读取、按 schema 版本检查。不能实例化 Store 当作只读工具：初始化会迁移并 chmod（`store.py:107`）。数据库是内部格式，不是稳定公共 API。 |
| 时间与等待 | tasks.created、attempts.started/finished、task_reviewed.created 可分离排队、运行、等审时间。业务首次派发到最终 accepted 的墙钟由外部定义，不能直接用单 attempt 时长替代。 |
| 父会话成本、预算 | inspect() 的 budget 包含 child_reported_tokens、parent_reported_tokens、complete、scope、catalog_estimate；inspect(parent_id) 与 parent_register/meter_stop 管理计量。见 `store.py:729`。共享父会话不能准确按任务分摊，未登记的主会话开销拿不到。 |
| 问题和恢复 | inspect(session_id).questions、任务 waiting_reason、recovery_needed 及事件；resolve 需 evidence 和 revision。提出了问题不等于“合理求助”。 |

bridge 本身拿不到：业务任务类型/难度、外部规格版本、独立验证是否通过、结构化评分、真正的缺陷因果、用户最终接受、真实账单和未计量的人工时间。能从源码/diff/自述推测的字段也不能冒充直接观测。Reviewer 只有通过 bridge 运行时才自然产生 attempt 用量；外部 Reviewer 需要另报或标未知。JSONL 不复制 usage 的决定可保留，但源状态丢失时必须报告 missing_source；报表保存查询时间、数据版本摘要和覆盖率，不能用未知成本排名。

## 4. 建议改稿次序

先补 H1/H4 的 ID 与重试说明、H5 的写入规则，再澄清 H2/H3 的统计定义和 M1/M2 的工具与路由行为。随后补验证、费用、缺陷的 unknown / mixed / 人工确认规则。无需修改 bridge 核心；模块设计见 `agent-tiering-modules.md`。
