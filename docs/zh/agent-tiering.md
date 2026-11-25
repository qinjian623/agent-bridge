> 语言：[English](../agent-tiering.md) | 中文

# Agent 任务派发、检查与记录（第一版）

## 1. 第一版做什么

主 agent 派发一个任务，执行 agent 在独立 worktree 中修改，脚本检查测试和文件修改，主 agent 决定通过或拒绝，再保存记录、查看通过率和耗时。

第一版只连接一个 bridge，每次任务只执行一轮。主 agent 直接选择一个已有配置，不自动重试、不自动换模型，也不按统计结果自动选模型。

[cli-agent-bridge](README.md) 已提供会话、任务、worktree、等待、结果查询和 `agent_review`。仓库中的独立脚本 [scripts/tiering.py](../../scripts/tiering.py) 已提供检查、task/review JSONL 记录，以及数量、通过率和耗时报表；任务派发和 Bridge 审核仍由主 agent 执行。

本版记录只保留 `task`、`review` 两种。

## 2. 谁负责什么

| 由谁执行 | 要做什么 |
|---|---|
| 主 agent | 写清要求、选择配置、派发任务；阅读检查结果和 diff，决定通过或拒绝，保存记录。 |
| 执行 agent | 在指定 worktree 内修改允许的文件，自测并报告结果；要求不清楚时提问。 |
| 检查脚本 | 独立重跑约定测试，列出全部文件修改，检查是否超出允许范围，返回命令、退出码和具体问题。 |

主 agent 不能把执行 agent 的“已完成”“测试通过”直接当作检查结果。脚本只检查事实，不能代替主 agent 判断功能是否符合要求。

## 3. 任务怎么写

任务说明要短，但目标、允许修改的文件、测试命令和验收要求必须齐全。例如：

```yaml
goal: 修复解析器收到空输入时报错的问题
allowed_files:
  - src/parser.py
  - tests/test_parser.py
out_of_scope:
  - src/tokenizer.py
test_command: PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_parser.py
acceptance:
  - 空输入返回空列表，正常输入的结果保持不变
  - 新增空输入测试，全部测试通过
  - 只修改上述文件，不提交代码
stop_if:
  - 需改动 allowed_files 之外的文件或发现未说明的公共接口变更
```

这是任务说明示例，文件名按实际仓库填写；不是 bridge 的参数对象。主 agent 将说明原样传给 `agent_dispatch` 的 `instruction`。多任务并行派发时，应先锁定 `shared_decisions` 并对各任务的 `out_of_scope` 做两两消歧，避免交叉改动（与 [router-prompt.md](skills/cli-agent-bridge/resources/router-prompt.md) 保持一致）。

派发前选定 `profile`，从会话查询结果读取实际模型和推理强度；不要从配置名猜。程序、供应商或运行设置变化时另用配置名，避免混在一起统计。

用 `agent_open` 创建可写的 Git worktree，保存返回的 `workspace.snapshot_tree`。需要带入原目录的未提交文件时，用 `snapshot_files` 明确列出，并确认它们已包含在输入快照中。

## 4. 一次任务怎么做完

1. **派发。** 主 agent 写好任务说明，调用 `agent_open`、`agent_dispatch`。拿到 bridge 返回的 `task_id` 后，追加一行 `task`，保存要求、配置和输入快照。
2. **执行。** 执行 agent 修改和自测。主 agent 用 `agent_wait` 等待，再用 `agent_result`、`agent_inspect` 核对状态和本次 `attempt_id`，追加一行 `task` 执行结果。
3. **独立检查。** 状态为 `completed` 后，确认执行 agent 已停止写文件，再由脚本重跑测试并检查修改范围。检查期间文件变了，就重新检查。对于涉及代码修改（`workspace-write`）、多子任务并行合并或高风险结论的任务，除确定性检查脚本外，在调用 `agent_review(accepted)` 前**必须**按 [router-prompt.md](skills/cli-agent-bridge/resources/router-prompt.md) 的 `Checker` / `evaluator` 规范开启一个**零历史只读独立会话（`access="read-only"`）**。如果 `agent_catalog` 中存在已验证且支持 `read-only` 的异构模型，必须选用与执行者不同的模型，且优先不同运行时（仅更换运行时但使用同一底层模型不算异构；Antigravity 不支持 `read-only`，故排除）；只有没有可用的异构只读模型时，才允许用同模型新建只读会话。审查时应对照任务要求、`diff`、独立测试输出、执行回报中的 `deviations`/`assumptions`/`unverified`/`unresolved`（回报字段只作待核实线索，不作为完成证明）以及并行任务合并前的 `conflicts` 做交叉核查；简单单步只读查询可由主会话直接核对。
4. **审查判定。** 主 agent 对照任务要求阅读实际 diff 和检查结果（以及独立 `Checker` 结论），决定 `accepted` 或 `rejected`，调用 `agent_review`。测试失败、改了不允许的文件或要求未完成，都不能通过。
5. **记录。** 确认 bridge 已保存审查结论后，追加一行 `review`，写清检查结果和原因，再按任务统计数量、通过率和耗时。上述扩展字段与独立审查结论仅用于主 agent 判断与可选本地审查摘要，**不改变**下文 `agent_review` 接口参数以及第 6 节的 `task`、`review` JSONL 记录 schema。

`completed` 只表示执行结束，仍须独立检查和审查。测试全部通过，也不能省略阅读 diff。

执行失败、中断或取消时，将状态和原因写入 `task` 后停止，不调用 `agent_review`，也不写 `review`。`recovery_needed` 等结果不明的状态先保留，交给人核对。

第一版基准台账固定记录单轮执行，该模式下失败或拒绝后不自动重派，由人决定是否另派新任务（使用新的 `task_id`，保留旧结果；要求变化也重新派发）。在常规多 Agent 协作任务中，主 agent 可按 [router-prompt.md](skills/cli-agent-bridge/resources/router-prompt.md) 对首次 `FAIL` 的子任务执行最多 1 次定点修复。如果新开可写会话：Git 模式须将新会话 `cwd` 设为原执行 worktree 根目录，通过相对路径 `snapshot_files` 移交已修改、未跟踪和已删除文件，并核对 `workspace.snapshot_tree` 与实际内容；`workspace_mode="copy"` 模式须将 `cwd` 设为原副本目录，在 `snapshot_files` 中列出全部现存必要输入及已修改或新增文件（不传已删除文件），核对 `workspace.manifest` 与实际内容后再修改。修复完成后都必须重新做全量只读独立复审；如果同步记入第一版台账，该次修复按新 `task_id` 记录，并计入该子任务的唯一一次修正额度。

`agent_review` 只接收下面这些参数，详见 [接口定义](../../src/agent_bridge/mcp_server.py) 和 [审查条件](../../src/agent_bridge/store.py)：

- `task_id`、`attempt_id`：当前任务及其当前 `completed` 尝试。
- `decision`：`accepted` 或 `rejected`。
- `evidence`：非空的具体检查摘要，例如“独立重跑 12 项测试通过；只修改两个允许的文件；空输入返回空列表”。
- `expected_revision`：该会话最新的 revision，通过 `agent_inspect` 查询。
- `request_id`：本次调用的请求编号；核对同一次请求时保留原编号和原参数。

revision 过期时重新查询并确认任务没有变化。同一次尝试只能审查一次；调用结果不明时先核对，不换请求编号盲目再提交。

## 5. 脚本独立检查什么

检查必须针对执行 agent 留下的实际文件进行，不能只读它的总结或它提供的补丁。

- 在该 worktree 的允许执行环境中，独立重跑派发前写明的测试命令，保存命令和退出码。
- 相对派发时保存的 `snapshot_tree` 检查全部修改，不直接拿 `HEAD` 作比较。快照里已有的未提交内容不算执行 agent 新改的内容。
- 检查已跟踪、未跟踪和被 Git 忽略的文件；包括新增、删除、重命名、权限、二进制文件和符号链接变化。
- 重命名时检查旧文件名和新文件名，两者都必须在允许范围内。未检查的项目不能写成“没有问题”。
- 比较检查前后的文件清单、内容和权限。发现变化时，本次结果不能使用，停止写入后从头检查。

在 worktree 根目录，下面两个命令可分别列出已跟踪文件的修改和未跟踪文件：

```bash
git diff --name-status --no-renames "$snapshot_tree" --
git ls-files --others -z
```

这里的 `snapshot_tree` 使用派发时保存的值。第一条禁用重命名合并，让旧名和新名分别列出；第二条不加忽略规则，避免漏掉被忽略的文件。

这两个命令只用于列文件；脚本还要比较内容和权限，主 agent 仍要阅读 diff。不能仅凭输出行数判断是否通过。

测试缓存和报告尽量写到 worktree 外。确需写入仓库的生成文件，要在派发前写明允许位置；不能检查后才临时放宽范围。

必需测试未运行、范围检查未完成或检查结果已过期时，不能记为通过。事先约定没有自动测试的文档任务，要写清人工检查方式和结果。

worktree 只分开工作文件，不是操作系统沙箱；测试仍须在任务允许的环境中运行。

## 6. 记录什么

记录使用 UTF-8 JSONL，每行是一个完整 JSON 对象，行末有换行。每个主会话使用独立文件，例如 `~/.local/state/agent-tiering/main-01.jsonl`。

只保留两种记录，不复制 bridge 的全部事件，也不读取内部数据库：

| `event` | 什么时候写 | 写什么 |
|---|---|---|
| `task` | 派发成功后；取得执行结果后再追加一行 | 首行保存任务要求和配置，后续行保存执行状态、尝试编号和原因。 |
| `review` | 独立检查与审查完成，bridge 确认保存结论后 | 通过或拒绝、具体原因、测试退出码和超出范围的文件。 |

共同字段是 `event`、`time`、`task_id`；`time` 使用带时区的时间。ID 直接使用 bridge 返回值，不额外生成任务编号。

`task` 首行包含 `profile`、`model`、`effort`、`goal`、`allowed_files`、`test_command`、`acceptance`、`snapshot_tree`、`status` 和 `attempt_id`。
取得结果后，新的 `task` 行只需写共同字段及 `status`、`attempt_id`、`reason`；派发时的要求保持原样，不在后续行改写。

`review` 另含 `attempt_id`、`decision`、`reason`、`test_exit_code`、`out_of_scope_files`。这些字段写进 JSONL，不作为额外参数传给 bridge。

下面是一项通过任务的完整记录，每个对象各占一行。示例 ID 和快照值是占位说明，实际写 bridge 返回值。

```jsonl
{"event":"task","time":"2026-10-28T12:00:00+08:00","task_id":"task-001","profile":"codex-sol","model":"gpt-6-sol","effort":"high","goal":"修复解析器收到空输入时报错的问题","allowed_files":["src/parser.py","tests/test_parser.py"],"test_command":"PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_parser.py","acceptance":["空输入返回空列表，正常输入结果不变","新增空输入测试，全部测试通过","只修改允许的文件，不提交代码"],"snapshot_tree":"<agent_open 返回的 snapshot_tree>","status":"queued","attempt_id":null}
{"event":"task","time":"2026-10-28T12:03:00+08:00","task_id":"task-001","status":"completed","attempt_id":"attempt-001","reason":"执行结束，等待独立检查"}
{"event":"review","time":"2026-10-28T12:05:00+08:00","task_id":"task-001","attempt_id":"attempt-001","decision":"accepted","reason":"独立测试通过；只改了允许的两个文件；空输入返回空列表，正常输入结果不变","test_exit_code":0,"out_of_scope_files":[]}
```

另一种情况：同样执行结束，但独立测试失败，结论行如下。实际只能写通过或拒绝中的一条，不能将两个示例同时追加给同一次尝试。

```jsonl
{"event":"review","time":"2026-10-28T12:05:00+08:00","task_id":"task-001","attempt_id":"attempt-001","decision":"rejected","reason":"独立重跑时空输入测试仍报 IndexError","test_exit_code":1,"out_of_scope_files":[]}
```

启动失败、尚未产生尝试时，只记 `task`，不编造审查：

```jsonl
{"event":"task","time":"2026-10-28T12:20:00+08:00","task_id":"task-002","profile":"codex-sol","model":"gpt-6-sol","effort":"high","goal":"修复解析器收到空输入时报错的问题","allowed_files":["src/parser.py","tests/test_parser.py"],"test_command":"PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_parser.py","acceptance":["空输入返回空列表，正常输入结果不变","新增空输入测试，全部测试通过","只修改允许的文件，不提交代码"],"snapshot_tree":"<agent_open 返回的 snapshot_tree>","status":"queued","attempt_id":null}
{"event":"task","time":"2026-10-28T12:20:05+08:00","task_id":"task-002","status":"failed","attempt_id":null,"reason":"执行程序启动失败"}
```

`status` 照实使用 bridge 返回值；尚无尝试时 `attempt_id` 为 `null`。`completed` 没有 `review` 是待审，结果不明是待核对，都不能当作通过。

`test_exit_code=0` 才表示独立测试通过；未跑或事先约定没有自动测试时填 `null`，在原因中说明。`out_of_scope_files=[]` 表示已检查且没有超出范围，没查填 `null`。

本机只允许一个进程顺序写一个记录文件。每次追加用同一固定锁文件的排他 `flock`，完整写一行并确认写入成功后再释放；不支持跨机器共写。

相同记录重复出现时只统计一次；同一任务从排队到执行结束是状态变化，不是两个任务。同一任务出现矛盾的执行结果或审查结论时，保留原文件，报错并暂停汇总。

写入失败或读到坏行时停止，报告文件名和行号，不跳过、不自动截断。bridge 保存成功而 JSONL 写入失败时，也停止后续派发，先人工核对已有任务和请求信息。

这版不自动补齐进程崩溃期间缺失的记录；调用结果不明、记录不全时先报告缺项，不把报表说成完整结果。

## 7. 怎么看结果

按派发时间选择一批任务，写明统计截止时间，按配置、实际模型和推理强度分别展示。按 `task_id` 计数，同一任务的多行记录只算一个任务。

| 显示项 | 怎么计算 |
|---|---|
| 原始数量 | 派发总数、执行状态为 `completed` 的数量、通过数、拒绝数、执行失败数、中断或取消数，以及仍在等待、待审、待核对的数量。 |
| 任务通过率 | 通过数 ÷ 已确定结果总数。总数包括通过、拒绝、执行失败、中断或取消；等待、待审和待核对另列。 |
| 平均耗时 | 对全部已确定结果的任务，计算从派发到确定结论的时长，再取算术平均，包括通过和未通过任务。 |

执行状态采用最后一行 `task`；审查结论采用对应的 `review`。只有派发行时，通过结果接口查询当前状态；不能确认时列为待核对，不猜它已经完成或失败。

通过或拒绝的结束时间用 `review.time`；失败、中断或取消用对应 `task.time`，开始时间均用派发行的 `time`。耗时包括排队、执行、测试和等待审查，不称为模型推理速度。

通过率分母为零时显示“暂无已确定结果”；平均耗时没有可用任务时也留空。尚未确定结果的任务不参与平均，数量必须同时显示。

例如：通过 6 项、拒绝 2 项、执行失败 1 项、取消 1 项，另有 2 项待审，则通过率是 `6 / 10 = 60%`，同时显示“待审 2 项”。

少于 10 条已确定结果时，只展示数量和耗时，不轻易判断哪个配置更好。达到 10 条也要看任务差异，不能只凭通过率排名。

只算原始通过率和算术平均耗时，不加权，不按时间减少旧记录的作用。bridge 自带的 `quality` 有自己的计算规则，不能代替这里的任务通过率。

## 8. 实施与后续规划

**P0：跑通一次完整任务。**

- 固定一个已有配置，写好任务说明，完成派发、执行、独立检查、主 agent 审查和两种 JSONL 记录。
- 用简单脚本完成测试和全部文件修改检查；输出原始数量、通过率与平均耗时。
- 用通过、测试失败和启动失败三种情况核对记录：检查失败不能记通过，执行失败不能漏掉，未审任务不能算成功。
- 记录目录权限设为 700，记录及锁文件权限设为 600。只存任务要求、必要 ID、配置、时间、状态和短检查摘要，不存密钥、认证状态、原始聊天或数据库。

**P1：减少重复操作。**

- 将派发、等待、检查和记录的重复步骤整理成脚本，仍由主 agent 阅读 diff 并决定通过或拒绝。
- 增加按日期、配置筛选，列出待审任务和导出表格；保持上述统计定义和记录种类。

记录保留多久、何时删除由使用者手工管理；第一版不做自动清理、自动修复或更换存储。

有实际需求后，再单独考虑人工重试管理、固定任务比较、模型自动分级和费用统计。这些都不作为 P0、P1 的实施条件。
