---
name: cli-agent-bridge
description: 通过本机服务让 Codex、Claude Code、Pi、Antigravity 分别执行子任务，管理独立模型会话、暂停恢复和检查结果。适用于跨程序分工或同程序多模型任务；单纯定时提醒仍使用 codex-async。
---

命令为 `agent-bridge`，实际入口用 `command -v agent-bridge` 核对；源码通常在 `~/gits/cli-agent-bridge`。自动更新安装的固定入口为 `~/.local/share/cli-agent-bridge/bin/agent-bridge`。优先使用已注册的 MCP 工具；工具尚未加载时用 `agent-bridge call <工具名>`，JSON 从标准输入传入。Linux 服务状态用 `systemctl --user is-active cli-agent-bridge`，macOS 用对应 LaunchAgent，配置检查用 `agent-bridge doctor`。

自动更新用 `agent-bridge update check/run`，最近结果在状态目录 `update-status.json`。维护期间的 `update_busy` 表示暂缓，不能换 request_id 重跑；`uncertain` 先核对实际进程与版本。更新不重启原生终端、不升级其他 CLI，也不清除旧版本或还原数据库。MCP/relay 和 skill 必须使用固定入口或 `current/skill`，不要继续引用旧 checkout。私有 GitHub 授权由该机器的 `gh` 管理，不复制其他机器的 token。

- 先读 `agent_catalog`。程序、供应商和模型分别核对；本机 Claude Code 当前连接 DeepSeek 兼容接口。指定的模型不得自动替换。
- Antigravity 需要 `runtime=antigravity`、`provider=google-antigravity` 和完整 `agy models` ID，即使选 Claude 也不能填 anthropic。仅支持文本与 workspace-write，read-only 明确拒绝。headless 无逐次审批，原生工具被拒绝不能当作执行成功；不要加跳过权限参数。消费套餐目前不支持封装进第三方编码代理，使用前查项目 README 的账号限制。原生累计 token 在进程恢复后缺少可靠基数时只报观察到的步骤量并标不完整；不能当实际账单。新增接口的离线测试和真实 CLI 握手已验证，真实推理/工具/中断仍需单独验证。
- `agent_catalog` 会自主查询已配置供应商的模型，缓存 5 分钟；需要最新结果时传 `refresh=true`，也可用 `profile` 只查一个配置。读 `discovery` 中的准确 `model`、来源、时间和错误；目录可见不代表推理成功。查询失败或 `stale=true` 时，不能选择其中的新模型。
- 主 agent 可按任务选择目录中的模型，通过 `agent_open/agent_dispatch(profile=已有配置名, model=准确ID)` 执行，无需先改配置。原认证、供应商、权限、并发组和推理强度继续生效；已有 session 不能换模型。新模型不会继承旧模型的价格或验证标记。省略目标时，服务的均衡打分仍只使用已验证配置；不会自动付费测试整个目录。
- 简单任务、前后强依赖的串行任务在主会话直接完成；单个模块由同一执行会话端到端完成“修改 + 自测”闭环，仅对互不依赖的正交子任务并行派单（由 `lead` 统一协调，子会话不私自转包或复制整段聊天）。跨角色分工（lead/developer/worker/writer/editor/operator/evaluator/visualizer/architect）、模型路由与派单/审查协议详见 [resources/router-prompt.md](resources/router-prompt.md)。
  * 派单协议：提供 `goal`, `inputs`, `allowed_files`, `out_of_scope`, `deliverable`, `acceptance`, `stop_if`（并行任务先锁定 `shared_decisions` 并两两消歧；长任务/副作用操作必填 `operation_id`，超时凭 ID 先查原作业防重复执行）。完工回报须区分 `evidence` 与 `deviations`、`assumptions`、`unverified`，触发 `stop_if` 立即报 `blocked`。
  * 预算与返工红线：每个子任务最多定点修正 1 次（仅修审查列出的 `FIX_ITEMS`，需新证据，换会话或换模型不重置次数），专家会诊（Opus/Sonnet/Gemini）全流程限 1 次；本地 Qwen 离线时自动按路由表降级（小改动转 Luna/DS Flash，重活转 Sol）。
- 分组用 `agent_group(action=create/add/remove/delete)`，`session_ids` 填已有 bridge ID；`native_members` 可登记原生会话 `{runtime,native_id,cwd,label}`，不创建替代会话或 worker，移除时只需 runtime/native_id。用 `agent_groups(group_id=...)` 读成员和 revision，省略 group_id 只列出分组。分组不改变原有 parent 通知。
- `agent_broadcast(group_id, message, expected_revision, request_id, reservation)` 只向指定组的成员各派发一条任务，可能产生推理费用；reservation 按每个成员计算。不要把全部会话加入临时组来代替明确选择目标。暂停、关闭、正在排空和等待恢复的成员会跳过，检查每个 recipient 的任务 ID 或原因。
- 广播重试保留相同 request_id 和参数，返回原收件人和任务 ID；组成员后来改变不会补发。入队不等于执行成功，按 task_id 查看结果。移除成员或删除组不撤回已排队任务，需用 agent_cancel；删除的组 ID 不复用。
- 原生成员广播与收件：用 `agent_broadcast_status(broadcast_id, refresh=true)` 查各目标状态（`pending`/`enqueued`/`received` 及 `processing_status`；收到不代表执行完成，`uncertain`/`unconfirmed` 先读原生记录核实，不自动重发）。Codex 仅入原会话队列，不唤醒已关闭会话；AGY 仅在确认原实例关闭后自动恢复同一原生 ID（沿用原配置，不写活动终端、不传跳过权限参数、不补发历史 `skipped`）。原生会话不计入 bridge 预算（启用 `token_budget` 时拒绝含原生成员的广播），无 `task_id` 故 `agent_cancel` 不适用。
- 原生成员可主动用 `agent_group_inbox(group_id, runtime, native_id, after, limit)` 分页只读收件（保存 `cursor` 作下次 `after`，每页最多 10 条；协调者代查不等于目标已收到）。本机有管理权限的原生会话可发广播，受限子会话不可；只通知用户指定成员，禁止成员收到后自动广播确认。唤醒锁与进程检查细节见 [README 分组与广播](../../README.md#分组与广播)。
- `agent_inspect {}` 总览直接列出每个会话的 `runtime`、`provider`、`model`、`profile`、`effort` 和 `service_tier`；查询实际会话详情时传 `session_id`。
- 修改代码用 `agent_open(access=workspace-write)`；服务创建独立 Git worktree，默认从 HEAD 开始。需要当前未提交内容时列出 `snapshot_files`；非 Git 目录用 `workspace_mode=copy` 并列出文件。不能偷偷 commit、stash 或清理主目录。
- `request_id` 在重试时保持不变，改变内容时使用新 ID。已存在的 session 固定程序和供应商；换模型用新 session，重试旧任务可提供 `retry_task_id`。
- 向已有 session 派发时省略 `access` 会沿用权限，明确传入不同权限会报错。重试可用 `reservation` 更新下一次执行的预留；启用预算后必须为正数，旧 attempt 的预留不会随之减少。
- `agent_wait` 等待超时仅表示暂时没有事件。任务 `completed` 后核对实际文件、`diff`、测试及回报中的 `deviations`/`assumptions`/`unverified`/`unresolved`：凡涉及代码修改（`workspace-write`）、多子任务并行合并或高风险结论的任务，在调用 `agent_review(accepted)` 前**必须**使用零历史只读独立会话（`access="read-only"`）按 [resources/router-prompt.md](resources/router-prompt.md) Checker 规范审查（`agent_catalog` 存在已验证且支持 `read-only` 的异构模型时必须选用与执行者不同的模型，优先不同运行时，仅换运行时但同一底层模型不算异构；Antigravity 不支持 `read-only` 故不作只读审查；仅无可用异构只读模型时允许同模型新只读会话），防范待审文本注入并在合并前排查跨任务 `conflicts`；简单单步只读查询可由主会话直接核对；依赖任务仅在 accepted 后运行。若开启新会话执行定点修复，须按模式移交待修版本：Git 模式将新会话 `cwd` 指向上一轮 worktree 根目录并在 `snapshot_files` 列出相对该目录的已修改/未跟踪/已删除文件，创建后核对 `workspace.snapshot_tree` 与实际内容；copy 模式传 `workspace_mode="copy"`、以原副本目录为 `cwd` 并在 `snapshot_files` 列出全部现存必要输入与已改/新增文件（不传已删除文件），创建后核对 `workspace.manifest` 与实际内容后再修改，修复后必须重新全量独立复审。
- 排队时读 `waiting_reason`；回答被截短时按 `artifact_id` 和 `next_offset` 分页读取，不把摘要当完整日志。指定 `attempt_id` 可查历史执行。
- 写入任务提取补丁失败会标为 `failed`，回答、用量与工作目录中的修改仍保留，先检查 `result.files.error` 和实际文件。
- Pi 已确认结束但查询统计失败时仍保留回答和文件变化，检查 `result.statistics_error`；用量只算部分统计。
- 控制前读取最新 revision。pause 中断当前轮次；pause_drain 等它结束；resume 默认不重试中断任务，明确继续时才传 `continue_task_id`。close 保留文件和历史，reopen 必须明确调用。
- `recovery_needed` 表示不能确认外部执行结果。检查原生记录、worktree 和可能仍运行的外部命令后，使用 resolve 写入检查记录，再决定是否恢复；不要换 request_id 盲目重跑。
- 会话因启动或中断请求失败而需要恢复时，尚未执行的任务仍是 `queued`；检查会话错误后明确 resume，这些排队任务不需要 resolve。
- 需要输入或批准时从 `agent_inspect` 读取 question_id 和 expires，使用 respond 回答仍有效的请求；不自动扩大用户授权。
- 普通 Flash 任务先运行本 skill 的 `scripts/provider_windows.py`，按已核对的低峰窗口优先选 `claude-ollama`（Ollama Cloud）；官方 `pi-deepseek` / `claude-deepseek` 使用 DeepSeek 官方低峰窗口。只影响新任务，须先满足模型验证、工具及质量要求；供应商认证与配额分别保留，已有 session 不迁移。详细时段及节假日用法见 [分工表](resources/router-prompt.md#flash-按供应商低峰时间分配)。
- 默认使用均衡策略。单个任务可传 `requirements.strategy=cost/speed/balanced`；指定模型不被自动替换。遇到配额等待先读 `quota_cooldowns`，确认恢复后再用 `quota_resume`；失败任务不会自动重跑。
- 用量与计费：子任务统计含每次 attempt；需汇总父会话时在 `parent_register` 传原生 Codex 的 `usage_path`（可选 `usage_since`，`notify=false` 仅统计，`parent_meter_stop` 停止计数）。缺少执行前基数或未登记父用量时只报不完整/部分统计，不把历史总量当本轮新增；换模型重试的质量归各自 attempt、费用计全部执行；未知价格不按零算，CLI 估算不作实际账单。详见 [README 均衡选择与费用](../../README.md#均衡选择与费用)。
- 父会话通知与 Telegram（可选，详见 [README 父会话与 Telegram](../../README.md#父会话与-telegram)）：不默认登记无关主会话；Telegram 任务须配置可读的 `telegram_database` 并保留默认 `external_child_filter` 子会话登记（不支持时更新现有 bot，严禁设为 `null` 绕过）。父会话若用临时 MCP 启动，登记时须传私有 `resume_config_file`（不改父模型或审批策略）。收到通知后仅 `completed` 可调 `agent_review`，结果不明先核查再 `resolve`，处理后用 `parent_ack` 确认（不重发结果不明的通知，也不重启共享 Codex daemon）。

任务结束后关闭不再使用的子会话，并检查 `owned_processes`；顶层回答结束不证明后台进程结束。cgroup 之外的 tmux、远程作业等需另查。保留未合入的 worktree、副本和补丁供主 agent 检查；不要自动删除。

经验沉淀遵循“单条一行带日期、总数不超过 30 条”；已由代码强制校验或已固化进 [resources/router-prompt.md](resources/router-prompt.md) / `SKILL.md` 的规则及时从日常经验记录中移除，避免上下文膨胀。
