> 语言：[English](../verification.md) | 中文

# 检查记录

## 2026-10-27 Antigravity 接入检查

新增 Antigravity 运行时、模型发现、conversation ID 续接、SIGINT 中断、原生权限拒绝处理、用量与限额解析。每个任务一个原生进程，等待终态结果、进程退出和 stderr 读完后才返回。

- 126 项自动测试通过，Ruff 检查通过；其中新增 12 项 Antigravity 测试。
- 离线 CLI fixture 覆盖多轮与跨进程记忆、取消后重开、进程退出但无终态、错误 JSON、错误会话 ID、额外终态、非零退出覆盖 SUCCESS、延迟且超过 stderr 缓冲长度的权限拒绝、累计计数重置、额度耗尽。
- 真实 broker/worker 的离线组合检查通过：隔离副本、任务存储、连续追问、文件变化提取、220 token 累计均有断言；只读请求在创建会话前拒绝。
- 本机真实 `agy 1.4.3` 查询返回 13 个模型，启动 NDJSON 会话收到 init，conversation ID 与请求的模型字段正常。没有发送推理提示，原生进程已关闭。

未验证：真实供应商推理、真实工具权限/执行、真实 SIGINT 后续接、真实额度耗尽。未增加默认 profile，未改变私有配置或重启共享服务。仅支持文本及 workspace-write；消费套餐的使用限制见 README。

## 2026-10-22 开发检查

这是可运行开发版本的检查记录。已完成本机跨程序执行与原父会话冷恢复；真实 Telegram 消息检查仍待授权。

自动检查 57 项通过，包含实际 MCP 客户端、Unix socket、独立进程、服务重启、worker 被杀、并发事务、预算预留、原生结束判断、通知去重、Telegram 暂停和删除标记、首轮前的子会话登记，以及继承环境不得改变声明的供应商。父会话计数、Claude 累计差值、价格字段、限流等待、三种选择策略和固定任务断言也已检查。worker 崩溃保留已报告的部分用量，在途用量超过预留时不继续派发。

本轮新增未提交文件快照、非 Git 副本、相对输入快照的补丁、UTF-8 分页、历史 attempt、父配置恢复、排队原因、问题截止时间、首个输出计时、服务档位和进程清单检查。使用模拟 CLI 的部分不等同于供应商真实执行。现有 `codex-async` 的 107 项检查在此前修改过滤功能时通过；其测试有既存的 SQLite ResourceWarning，本轮未再修改该项目。

```text
uv run ruff check src tests scripts     All checks passed
uv run python -m unittest discover -s tests -v
Ran 57 tests; OK
skill-creator/scripts/quick_validate.py skills/cli-agent-bridge
Skill is valid
```

最初一轮真实模型测试有 17 个 attempt：14 个正常结束、3 个确认中断；该轮无失败或结果不明的 attempt。结束时 3 个会话暂停、5 个关闭，测试 worker 全部退出。以下数量均注明所属测试，不把不同阶段统计混成项目实际账单。原始回答和原生 ID 仅保存在私有状态目录，未放入 Git。

| 检查 | Codex | Claude Code | Pi |
| --- | --- | --- | --- |
| 实际供应商/模型 | OpenAI Codex / gpt-6.1-sol | DeepSeek Anthropic 兼容接口 / deepseek-v4-flash | DeepSeek / deepseek-flash |
| 读取文件并算 17+25 | 42 | 42 | 42 |
| 原会话追问 | 正确记住 lantern | 正确记住 lantern | 正确记住 lantern |
| 暂停时提交任务 | 保持 queued | 保持 queued | 保持 queued |
| 运行中原生中断 | interrupted | interrupted | interrupted |
| close、reopen 后上下文 | 原 ID、原记忆保留 | 原 ID、原记忆保留 | 原文件、原记忆保留 |
| 只读任务尝试写文件 | 原生只读文件系统拒绝 | 无写入/编辑/shell 工具 | 无写入/编辑/shell 工具 |

真实写入测试中，Codex 修复 `add`，Pi 修复 `slug`，分别使用独立 worktree。主程序另起 Python 进程执行断言，两项退出码均为 0；源目录中的未提交和未跟踪文件保持原样。修改、测试结果和 patch 路径保留在任务结果中。

私有报告：

- `<private-validation-state>/report.json`
- `<private-validation-state>/report.json`
- `<private-validation-state>/report.json`
- `<private-validation-state>/report.json`

报告由仓库 `scripts/*_smoke.py` 生成。它们会产生真实推理费用，普通单元测试不会调用这些脚本。

费用记录中，17 个 attempt 已报告 261,403 个 token，其中包含大量缓存输入；这是部分子会话统计，不是项目总量。Claude 主循环以外的用量、部分中断时的用量和主会话开销不完整。数据库中的 `charged_tokens` 是预算核算值，包含保留的预留量，不是实际账单或实际 token 总量。

## 与设计检查的对应

| 编号 | 状态 | 证据或未完成部分 |
| --- | --- | --- |
| R1 | 通过 | 三个程序真实读取和追问 |
| R2 | 通过真实完整用例 | Astra 退出后恢复同一原生会话，检查 Sol 和 Pi，再派发 Claude 复查，接受全部结果并确认通知 |
| R3 | 部分通过 | 工具、输入、供应商、上下文条件和未知质量信息已检查；没有足够分类质量样本 |
| R4 | 通过基本用例 | 开发主会话原生记录确认为 gpt-6-astra，前述 Sol 与 Pi 任务由它同时提交；三个程序保留实际配置。固定比较另有真实 Astra 分工与审查记录 |
| R5 | 通过已列操作 | 三个程序真实 open/pause/resume/close/reopen；另检查 Pi 创建的后台进程随所属 worker 关闭。外部 tmux/远程作业不在本机 cgroup 范围内 |
| R6 | 通过小型任务集 | [固定比较](benchmark.md) 包含三种方式、三个校准样本和两个独立验证任务；计入主会话和各 attempt，无一般收益结论 |
| E1 | 通过 | 并发重复请求；自动选择之后配置改变仍返回原 task |
| E2 | 通过基本用例 | revision、暂停不派发、drain 不中断；持续压力测试未执行 |
| E3 | 通过 | 关闭保留结果与目录，原 ID 重开；关闭父记录不发通知 |
| E4 | 自动与 Pi 实测通过 | Pi 原生命令运行期间重启独立 broker，worker PID、进程身份和原生会话均未改变，只有一次执行；模拟 CLI 另覆盖杀 worker 后不重发 |
| E5 | 通过已列用例 | 两个真实修改任务独立 worktree、三个真实只读任务、真实非 Git 副本；快照和补丁用自动断言检查，不承诺恶意同 UID 隔离 |
| E6 | 通过 | 原生真实任务 + 指定 turn / agent_settled / 终止状态测试 |
| E7 | 通过自动检查 | 共享并发、不替换指定模型、共享限流等待和明确解除配额等待；没有故意耗尽真实账户配额 |
| E8 | 通过已列检查 | 父子计数增量、重复事件、恢复、未知价格和部分用量保留；Claude 三轮真实累计差值正确。金额仅为有来源的估算，实际账单未知 |
| E9 | 离线及 Claude 实测通过 | 问题与 attempt 关联、过期拒绝、没有回答不批准；固定比较中一次真实 Claude 编辑审批只批准指定测试文件。Codex/Pi 的真实交互请求尚待补充 |
| E10 | 本机通过，Telegram 待授权 | 父暂停/关闭、去重、现有 Telegram 状态读取；真实冷恢复完成三程序任务。独立 Telegram 话题的真实消息尚未测试 |
| E11 | 通过基本用例 | 三个程序中断后重新打开并追问；明确继续创建新 attempt 的自动检查通过 |
| E12 | 通过 | 接受正确 attempt 后释放依赖，旧 worker/旧 attempt 无法覆盖新结果 |

## 本机安装

命令：`~/.local/bin/agent-bridge`。服务：`cli-agent-bridge.service`，已启动，未设置登录后自动启动。MCP：`cli-agent-bridge` 已注册，stdio 运行本项目命令。Skill：链接到仓库内的 skill。

运行数据与配置：`~/.local/state/cli-agent-bridge`，与上述开发测试状态分开。没有登记任何真实父会话，因此不会主动向主会话或 Telegram 发完成消息。

开发中发现旧 Telegram 服务将真实 CLI 子会话当成普通会话，自动发出 11 条测试回复。这些自动转发不代表完成了计划中的 Telegram 联调。已增加 `external_children` 登记及文字、附件过滤；新 worker 在不支持过滤时禁止执行模型。确认无运行、排队任务和待发送通知后，重载了一次 Telegram 服务，检查过滤版本已生效。已有消息没有删除。

修复后又分别运行一次 Codex 和 Claude 小任务，均正常返回 `OK_LOCAL`；确认登记成功，并等待 bot 扫描后检查，两条原生会话的 outbox 记录均为 0。补充报告位于 `<private-validation-state>/report.json`。连同前面的 17 个 attempt，共真实执行 19 次：16 次正常结束、3 次中断。

定时任务服务和共享 Codex daemon 未重启。

## 后续工作

首版剩余的真实检查是独立 Telegram 话题中的队列、冷恢复与回复，需要此前已询问的发送测试消息授权。

待执行方案：只使用现有私人聊天，新建“agent bridge 测试”话题；运行已通过的本机 Astra/Sol/Pi/Claude 小任务，关闭父进程后继续原会话。父会话最多产生四条短测试回复，子会话仍保持禁止单独转发。核对原生 turn 结束、桥接事件确认、bot outbox 及 Telegram 消息 ID，检查有无重复；异常时停止这组测试，不自动重复发消息。结束后关闭测试会话，保留话题供用户检查，不删除已有消息。这个方案尚未执行。

当前限制：固定比较只有两例独立验证任务，不推广为普遍收益；未做持续压力测试或实际账单核对。输入仍为文本和明确的本机文件，原生图片参数暂不支持。未知价格保持未知，后台远程或 tmux 作业须单独检查，暂停不能撤回已发生的外部操作。

## 费用与选择补充检查

均衡已写入本机默认配置，费用、耗时权重各 0.5；单任务可选费用优先或速度优先。父用量从原生累计计数的指定起点读取，不复制整段聊天；暂停通知和仅统计可分别选择。固定比较的 9 个案例均得到完整原生计数，三种方式各通过 2 个验证任务，详见 [报告](benchmark.md)。

另运行 Claude 三轮小任务：前两轮在同一个运行中的会话，第三轮关闭后恢复，分别报告 2,058、2,978、2,100 个 token，每轮均等于相邻累计值的差，三次均正常结束。报告：`<private-validation-state>/report.json`。新实现不追改旧测试中的部分用量。

## 冷恢复、文件与进程补充检查

`scripts/cold_resume_smoke.py` 在独立服务中创建 Astra 父会话及暂停的 Sol、Pi 子任务。初始目录查询成功后退出父进程，原生状态确认为 `notLoaded`，再释放子任务。恢复后的 Astra 自行检查代码和两项断言、接受 Sol/Pi 结果、创建 Claude 复查，最终接受三个 attempt，并确认处理三条完成事件。原目录中的错误函数保持原样，修改仅在子 worktree 中。

成功报告：`<private-validation-state>/report.json`。原生记录中的四个 turn context 全部为 `gpt-6-astra`、`low`、`on-request`，没有更换父模型或取消批准要求。父记录和三个子会话均已关闭。

此前两次未通过，分别保存失败报告，没有覆盖或当作成功：

- `cold-example-run-report.json`：测试客户端的回调拒绝了 MCP 调用，但初始检查只读到了模型说 READY。改为检查原生 MCP 调用确实成功；给只读工具正确标记，并仅在独立测试会话中预先允许已授权测试工具。
- `cold-example-run-report.json`：恢复后原生接口丢失临时 MCP 配置，连到了默认服务，找不到测试任务。新增私有 `resume_config_file` 引用，恢复相同的 MCP 配置，不覆盖父模型、沙箱或批准策略。

失败测试的父队列和活动轮次仅按本次测试的 ID 清理，并确认停止。bot 已扫描这三次测试涉及的七个 Codex/Claude 原生会话，均登记为外部子会话，outbox 记录全部为 0。Pi 后台会话不走该扫描。没有创建 Telegram 话题或发送测试消息。

`scripts/restart_smoke.py` 使用非 Git 文件副本运行真实 Pi 任务。在脚本开始、尚未结束时重启独立 broker；确认 worker PID、启动身份、原生 session 保持一致，输入脚本只运行一次。顶层任务完成时，`sleep` 后台进程仍在该 worker 的 cgroup 中；关闭子会话后，确认后台 PID 已退出、整个 cgroup 没有运行进程。源目录没有产生测试输出。报告：`<private-validation-state>/report.json`。

先前用 Codex 做该检查时，重启与单次执行断言通过，但把沙箱内 PID 3 当成宿主 PID，后台检查未通过，报告保留为 `restart-example-run-report.json`。没有据此声称后台已停止；改用 Pi 验证真实宿主进程，接口文档也说明 cgroup 清单使用宿主 PID。

快照自动检查覆盖已暂存和未暂存修改、删除、二进制、未跟踪文件、符号链接拒绝、请求重试与补丁实际应用。大输出检查重组完整中文结果、核对文件权限和跨任务访问拒绝。服务档位检查覆盖 Codex 启动/恢复/执行和其他程序拒绝不支持字段；未测试付费档位的费用或速度收益。

原生元数据检查确认 `fast` 返回 `priority`，`priority` 和 `default` 原样确认；前者按官方说明作为同一档位处理。该检查没有发起推理，报告为 `<private-validation-state>/report.json`。没有因此更改三个已安装 profile 的档位设置。

## 自主发现模型（2026-10-23）

66 项自动检查通过，包含真实 MCP 参数、Codex 目录分页和查询进程退出、并发缓存、刷新失败保留旧数据、Pi 供应商筛选、Claude 准确 ID、DeepSeek 重定向限制、模型设置检查、会话固定模型及重复请求。`ruff check src tests scripts` 通过。

三个真实目录均查询成功：Codex/OpenAI 7 个、Pi/DeepSeek 2 个、Claude Code/DeepSeek 2 个。后两者列出相同的 2 个 DeepSeek 模型，不能相加当成不同模型。原始目录报告保存在 `<private-validation-state>/report.json`。这些查询没有发送推理请求。

独立服务中的真实执行检查：

| 目标 | 结果 |
| --- | --- |
| `claude-deepseek` + `deepseek-flash`（原配置为 `deepseek-v4-flash`） | 通过：目录发现、创建会话、完成一次请求、返回准确模型 ID、检查结果、关闭 worker |
| `codex-sol` + `gpt-6-luna`（原配置为 `gpt-6.1-sol`） | 未通过调用检查：90 秒内没有输出，测试取消任务并关闭 worker；不能据目录可见声称推理成功 |

两次测试最终均无存活 worker。报告分别为 `<private-validation-state>/report.json` 和 `discovery-example-run.json`，失败任务和取消结果保留在该目录的 SQLite 中。测试子会话由现有 `external_children` 过滤，不登记父会话，也没有执行真实 Telegram 消息测试。没有逐一调用目录内全部模型，未验证 Anthropic 官方账户的真实调用。

可用 `scripts/discovery_smoke.py --state 独立状态目录 --profile 配置名 --model 目录中的新模型ID` 重做指定模型的检查；该脚本会产生一次真实推理请求。它要求模型与配置中的默认模型不同，结果只用于检查接口，不用于费用与速度排名。

完成后重载 `cli-agent-bridge.service`，实际查询确认三个配置和 `objective=balanced` 已生效；服务仍未设置登录自启。独立冷恢复测试服务已停止，其九个子会话均关闭、worker 全部退出，失败报告和工作目录保留。

## 三方评审后的修复（2026-10-23）

Codex CLI、Claude Code、Pi 的独立评审中，确认并修复了以下 13 项问题。原始评审、重现结果保存在本机 `<private-validation-state>/report`。

| 编号 | 修复 | 对应检查 |
| --- | --- | --- |
| F1 | 原生程序退出后，后台进程占住输出管道不再让关闭一直等待；worker 清理也有时限 | 真实继承管道测试；独立 systemd worker 约 10.18 秒关闭，脱离进程组的后台进程和整个 cgroup 均停止 |
| F2 | 换模型重试时，后一个模型成功不会算成前一个模型成功 | A 失败、B 成功的自动选择测试；总用量仍保留两次执行 |
| F3 | 父会话暂停、关闭、确认通知不再等待一整批慢请求 | 发送首条通知期间立即暂停，剩余通知保留；同时收到的确认不会被发送结果覆盖 |
| F4 | 派发参数先检查，再创建会话和工作目录；检查后发生状态变化时关闭新建会话 | 无效指令、依赖、预留、重试 ID、权限和 requirements；派发时状态变化测试 |
| F5 | 没有正在执行任务的 `pause_drain` 立即暂停 | 空会话、有排队任务和正在执行任务三种情况 |
| F6 | 已有会话明确指定不同权限时拒绝；省略权限时沿用原值 | CLI 请求和实际 MCP 默认参数检查 |
| F7 | worker 崩溃时保存任务事件，向已登记父会话安排恢复通知 | 任务保持 `recovery_needed`、通知只创建一次、不自动重放 |
| F8 | 通知辅助进程尚未启动、入队前初始化失败时可以重试 | 缺失解释器、客户端初始化失败；响应丢失仍保留 `uncertain` |
| F9 | 重试可以修改下一次执行的 token 预留；旧 attempt 独立保存原预留 | 启用预算后从零预留重试、数据库升级、旧未知用量仍计入预算 |
| F10 | 提取补丁失败返回 `failed` 和具体错误，同时保留回答、用量、文件 | 真实 Git 提取失败的 worker 测试 |
| F11 | 恢复时缺少执行前计数不再把历史累计量算成新增用量 | 缺失计数、崩溃后不采用旧计数；真实 Codex 关闭重开后新增量等于累计值之差 |
| F12 | 无效 requirements 返回稳定参数错误；未知异常保存明确的结果不明状态 | 重复请求得到同一错误，未知异常不会重复执行 |
| F13 | Pi 保留 `tool_execution_start` 事件 | 实际事件过滤测试；真实只读任务各有一条工具开始和结束事件 |

自动检查由 66 项增至 89 项，全部通过；Ruff 通过。服务更新前用 SQLite backup 保存数据库，真实短请求均返回准确答案与指定模型。

## 2026-10-23 第二轮修复

第二轮 review 确认的 12 项问题均已修复。修改和新增检查如下：

| 编号 | 修改 | 检查 |
| --- | --- | --- |
| R1 | 中断请求和计时对应具体 attempt；下一任务等旧请求结束后才能开始。请求报错或超时时停止 worker，未执行任务保持排队 | 旧中断迟到返回、报错和超时，后续任务不被误取消；未确认的请求不允许下一任务开始 |
| R2 | 通知按上次尝试时间轮换，顺序保存在 SQLite | 最早 10 条因父会话忙碌、等待唤醒或 Telegram 暂停无法继续时，第 11 条仍能发送；数据库升级保留原记录 |
| R3 | 启动、连接及执行前的 worker 故障为排队任务保存事件并通知父会话 | 连接失败、空错误消息的超时、启动器失败、已分配但未开始的任务；同次故障不重复通知，也不自动重跑 |
| R4 | 检查原生通知队列时跟随 `nextCursor` | 目标在第二页、队列中已没有目标、重复游标三种情况 |
| R5 | 已收到通知可在 `sending` 和 `uncertain` 时确认 | 首次发送尚未返回、返回丢失、后续不重发；`pending` 和其他父会话仍不能确认 |
| R6 | 通知按事件保存的状态说明后续操作 | 成功才要求 review；结果不明要求检查后 resolve；失败、中断、取消和启动失败分别给出可用操作；重试不改变旧事件说明 |
| R7 | Claude 缺失或无效累计计数保留为未知值，不当成零 | 空计数、恢复时缺计数、字段不全、计数重置和模型暂时缺失；重新取得有效区间后才报告完整用量 |
| R8 | Pi 已确认结束后，最终统计失败仍保存终态、回答和文件变化 | 实际 Worker 和 PiRuntime 配合模拟超时，回答与修改保留，已报告 token 保留为不完整统计，费用仍未知 |
| R9 | 新模型重试尚未开始时，使用原 attempt 的失败结果 | 重试排队、已分配和取消三种状态均保留原模型失败样本，不把未完成任务作为完整费用样本 |
| R10 | 判断父会话是否处理过多个任务时同时查询当前任务和历史 attempt | 任务迁出后，剩余任务仍不能独占旧父会话的共享开销 |
| R11 | copy 模式比较并记录文件权限 | 仅 chmod、内容及权限同时修改、新文件、未修改文件；原目录权限保持不变 |
| R12 | 用户取消、关闭优先于超时和预算中断 | 同时满足取消或关闭、超时、预算条件时记录为取消 |

自动检查由 89 项增至 114 项，全部通过；Ruff 和 skill 格式检查通过。

## 2026-11-02 分组与指定组广播

新增 `agent_groups`、`agent_group`、`agent_broadcast`，保留原有九个接口及父会话通知。成员使用现有 bridge 会话 ID；广播需要明确的 group_id 和组 revision，各接收方进入原任务队列。使用方法见 [README](README.md#分组与广播)。

新增 14 项检查，涵盖分组增删、成员增删及重复成员、组外不接收、多个组共享成员、并发重复请求、改变请求内容被拒绝、组成员改变后的原请求重试、服务重启后取回原任务 ID、空组及未知组、暂停与关闭状态、预算限制、写入失败时全部撤销，以及旧数据库升级后保留任务和父通知。

独立进程测试启动三个隔离的模拟 CLI 会话，只将两个加入目标组；全套结果 `226 passed, 42 subtests passed in 26.39s`，Ruff 与 skill 校验通过。

## 2026-11-02 原生会话分组与非打断队列

`agent_group` 新增可选 `native_members`，直接登记现有 Codex / Antigravity 原生 ID、目录和名称。Codex 通过已运行共享服务的 `thread/queue/add` 入队，`agent_broadcast_status(refresh=true)` 读取队列与历史回执；全套 242 passed。授权创建 `example-agents` 组登记 4 个会话。

## 2026-11-02 AGY 配置与主动读取组消息

核对重启后的 AGY 进程（启动目录 `~`）并通过现有分组接口修正目录；使用 `agy mcp add` 注册并验证独立 MCP 客户端握手；新增只读分页工具 `agent_group_inbox`，全套 244 passed。

## 2026-11-03 AGY 关闭后按组消息自动恢复

原生 AGY 广播保存为 `pending`，确认原实例完全关闭后以原 native ID 恢复一次 stream-json 会话；`agy_presence.py` 与 `agy_wake.py` 协同检测 native presence flock 与进程所属。新增 35 项测试后全套 `279 passed, 66 subtests passed in 15.64s`，Ruff 通过。

## 2026-11-13：区分任务总时限与外部暂停

确认三条 Pi 任务在约 3600 秒时因 `task_timeout` 结束，而非约 74 秒后邻近会话的暂停请求所致。修复在非成功终态结果中区分保留 `termination`（`cause`、`stage`、`elapsed_seconds`、`task_timeout_seconds`）与 `native_error`；840 项完整 unittest 及 21 项 worker 检查通过。

## 2026-11-13：非 UTF-8 输出路径与单任务时限

修复 `git ls-files -z` 遇合法 `0xff` 字节引发 `UnicodeDecodeError` 覆盖终态的问题，改为保存原始 bytes、原始 Git patch 和可选 raw hex 列表；同时新增并校验 `requirements.task_timeout`（本地与远端前置检查，拒绝无 `task_timeout_v1` 能力的旧节点/worker 静默忽略）。
