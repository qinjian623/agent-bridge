---
name: cli-agent-bridge
description: 通过本机服务让 Codex、Claude Code、Pi、Antigravity 分别执行子任务，管理独立模型会话、暂停恢复和检查结果。适用于跨程序分工或同程序多模型任务；单纯定时提醒仍使用 codex-async。
---

本机源码在 `/home/user/gits/cli-agent-bridge`，命令为 `agent-bridge`。优先使用已注册的 MCP 工具；工具尚未加载时用 `agent-bridge call <工具名>`，JSON 从标准输入传入。服务状态用 `systemctl --user is-active cli-agent-bridge`，配置检查用 `agent-bridge doctor`。

- 先读 `agent_catalog`。程序、供应商和模型分别核对；本机 Claude Code 当前连接 DeepSeek 兼容接口。指定的模型不得自动替换。
- Antigravity 需要 `runtime=antigravity`、`provider=google-antigravity` 和完整 `agy models` ID，即使选 Claude 也不能填 anthropic。仅支持文本与 workspace-write，read-only 明确拒绝。headless 无逐次审批，原生工具被拒绝不能当作执行成功；不要加跳过权限参数。消费套餐目前不支持封装进第三方编码代理，使用前查项目 README 的账号限制。原生累计 token 在进程恢复后缺少可靠基数时只报观察到的步骤量并标不完整；不能当实际账单。新增接口的离线测试和真实 CLI 握手已验证，真实推理/工具/中断仍需单独验证。
- `agent_catalog` 会自主查询已配置供应商的模型，缓存 5 分钟；需要最新结果时传 `refresh=true`，也可用 `profile` 只查一个配置。读 `discovery` 中的准确 `model`、来源、时间和错误；目录可见不代表推理成功。查询失败或 `stale=true` 时，不能选择其中的新模型。
- 主 agent 可按任务选择目录中的模型，通过 `agent_open/agent_dispatch(profile=已有配置名, model=准确ID)` 执行，无需先改配置。原认证、供应商、权限、并发组和推理强度继续生效；已有 session 不能换模型。新模型不会继承旧模型的价格或验证标记。省略目标时，服务的均衡打分仍只使用已验证配置；不会自动付费测试整个目录。
- 简单工作在主会话直接完成。分配任务时给出具体目标、完成条件、相关文件和测试，避免复制整段聊天。跨角色协作分工（lead/developer/worker/writer/editor/operator/evaluator/visualizer/architect）、模型路由与派单协议详见 [resources/router-prompt.md](resources/router-prompt.md)。
  * 派单协议：按规范提供 `goal`, `inputs`, `deliverable`, `acceptance`；长任务/副作用操作必填 `operation_id`（超时凭 ID 先查原作业，防重复执行）。
  * 预算红线：子任务最多修正 1 次（需新证据），专家会诊（Opus/Sonnet/Gemini）全流程限 1 次；本地 Qwen 离线时自动按路由表降级（小改动转 Luna/DS Flash，重活转 Sol）。
- 分组用 `agent_group(action=create/add/remove/delete)`，`session_ids` 填已有 bridge ID；`native_members` 可登记原生会话 `{runtime,native_id,cwd,label}`，不创建替代会话或 worker，移除时只需 runtime/native_id。用 `agent_groups(group_id=...)` 读成员和 revision，省略 group_id 只列出分组。分组不改变原有 parent 通知。
- `agent_broadcast(group_id, message, expected_revision, request_id, reservation)` 只向指定组的成员各派发一条任务，可能产生推理费用；reservation 按每个成员计算。不要把全部会话加入临时组来代替明确选择目标。暂停、关闭、正在排空和等待恢复的成员会跳过，检查每个 recipient 的任务 ID 或原因。
- 广播重试保留相同 request_id 和参数，返回原收件人和任务 ID；组成员后来改变不会补发。入队不等于执行成功，按 task_id 查看结果。移除成员或删除组不撤回已排队任务，需用 agent_cancel；删除的组 ID 不复用。
- 原生成员广播用 `agent_broadcast_status(broadcast_id,refresh=true)` 查每个目标。Codex 只向原会话持久队列写入消息，不启动/恢复/抢占/中断会话。pending 是 bridge 本地待发送，enqueued 才是 Codex 原生队列回执，received 需匹配原生用户消息 ID；队列消失不能当作收到。回执丢失记 uncertain，不自动重发。原生会话不计入 bridge 预算，启用 token_budget 时拒绝包含原生成员的广播；原生消息无 task_id，agent_cancel 不适用。
- Antigravity 新组消息会在确认原实例完全关闭后自动恢复同一原生 ID。与 Telegram 共用恢复锁，检查原生 presence 和 --conversation/-c 进程；活动、多实例或所属不明时保持 pending，不写终端。启动后及写入前再检查；原生记录不存在时拒绝启动，不创建替代会话。收到必须是新 user_input/DONE，过滤旧步骤；processing_status 单独记录推理结果，收到不代表执行完成。恢复沿用原生模型、认证、MCP 和权限，不套用 child 环境或跳过审批。历史 skipped 消息不补发，uncertain 不盲目重试。
- 具有本机 bridge 管理权限的原生会话可以调用组广播；分组不扩大受限子会话的工具权限。只通知用户指定的成员，准确说明哪些目标能接收。不要让成员自动广播确认，避免重复触发。
- 原生成员可主动调用 `agent_group_inbox(group_id,runtime,native_id,after,limit)` 读取自己在该组的消息，包括旧 AGY skipped 记录。保存 cursor，下次作为 after，每页最多 10 条；读取不会修改发送回执，协调者代查不能当作 AGY 收到。AGY 的 `mcp list` 是配置清单；恢复后会自动载入，真实工具调用才说明可用。不要向活动终端写入输入或为活动会话启动第二份。
- `agent_inspect {}` 总览直接列出每个会话的 `runtime`、`provider`、`model`、`profile`、`effort` 和 `service_tier`；查询实际会话详情时传 `session_id`。
- 修改代码用 `agent_open(access=workspace-write)`；服务创建独立 Git worktree，默认从 HEAD 开始。需要当前未提交内容时列出 `snapshot_files`；非 Git 目录用 `workspace_mode=copy` 并列出文件。不能偷偷 commit、stash 或清理主目录。
- `request_id` 在重试时保持不变，改变内容时使用新 ID。已存在的 session 固定程序和供应商；换模型用新 session，重试旧任务可提供 `retry_task_id`。
- 向已有 session 派发时省略 `access` 会沿用权限，明确传入不同权限会报错。重试可用 `reservation` 更新下一次执行的预留；启用预算后必须为正数，旧 attempt 的预留不会随之减少。
- `agent_wait` 等待超时仅表示暂时没有事件。任务 `completed` 后检查实际文件和测试，再 `agent_review(accepted/rejected)`；依赖任务仅在 accepted 后运行。
- 排队时读 `waiting_reason`；回答被截短时按 `artifact_id` 和 `next_offset` 分页读取，不把摘要当完整日志。指定 `attempt_id` 可查历史执行。
- 写入任务提取补丁失败会标为 `failed`，回答、用量与工作目录中的修改仍保留，先检查 `result.files.error` 和实际文件。
- Pi 已确认结束但查询统计失败时仍保留回答和文件变化，检查 `result.statistics_error`；用量只算部分统计。
- 控制前读取最新 revision。pause 中断当前轮次；pause_drain 等它结束；resume 默认不重试中断任务，明确继续时才传 `continue_task_id`。close 保留文件和历史，reopen 必须明确调用。
- `recovery_needed` 表示不能确认外部执行结果。检查原生记录、worktree 和可能仍运行的外部命令后，使用 resolve 写入检查记录，再决定是否恢复；不要换 request_id 盲目重跑。
- 会话因启动或中断请求失败而需要恢复时，尚未执行的任务仍是 `queued`；检查会话错误后明确 resume，这些排队任务不需要 resolve。
- 需要输入或批准时从 `agent_inspect` 读取 question_id 和 expires，使用 respond 回答仍有效的请求；不自动扩大用户授权。
- 默认使用均衡策略。单个任务可传 `requirements.strategy=cost/speed/balanced`；指定模型不被自动替换。遇到配额等待先读 `quota_cooldowns`，确认恢复后再用 `quota_resume`；失败任务不会自动重跑。
- 子任务统计包含每次 attempt。需要总量时在 parent_register 中传入原生 Codex 的 `usage_path`，从登记时的累计值开始计数；已有任务可用 `usage_since` 指定起点。`notify=false` 只统计、不发通知，`parent_meter_stop` 停止计数。未登记父用量时只报告部分统计；未知价格不能当零，CLI 估算不能当实际账单。
- 恢复后缺少执行前累计计数时，用量会标为不完整，不把历史总量当本轮新增。换模型重试的质量分别归入对应 attempt，费用仍计算所有执行。

父会话通知是可选功能，使用前读 [项目说明](/home/user/gits/cli-agent-bridge/README.md) 的“父会话与 Telegram”。Telegram 任务必须配置 `telegram_database` 并检查其可读性，服务才会尊重原话题的暂停和删除状态；真实消息联调尚待授权。不默认登记无关主会话。定时任务、原 bot、已有 Pi 终端扩展继续使用现有 skill，不重启共享 Codex daemon。

已验证 Codex 父进程退出后的原会话继续。父会话若启动时临时配置 MCP，登记时须用 `resume_config_file` 引用原配置，否则原生恢复可能连接全局默认服务。只引用私有文件，不把凭据内容放入工具参数；不能通过恢复改变父模型或批准策略。收到通知后按事件状态检查结果：只有 completed 可以 agent_review；结果不明先检查再 resolve。处理后用 parent_ack 确认，即使发送状态仍是 sending 或 uncertain 也可以确认；这不允许重发结果不明的通知。

即使没有登记父会话，真实 CLI 子会话也可能被旧 bot 自动转发。必须保留默认的 `telegram_database` 检测与子会话登记；遇到 `external_child_filter` 不支持时更新现有 bot 并检查生效，不能将该配置设为 null 绕过。

任务结束后关闭不再使用的子会话，并检查 `owned_processes`；顶层回答结束不证明后台进程结束。cgroup 之外的 tmux、远程作业等需另查。保留未合入的 worktree、副本和补丁供主 agent 检查；不要自动删除。
