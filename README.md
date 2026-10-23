# cli-agent-bridge

在本机通过 MCP 或命令行管理 Codex、Claude Code 和 Pi 的独立会话。会话可以多次追问、暂停、中断、关闭和重新打开；任务和结果保存在 SQLite，运行任务的进程独立于 MCP 客户端和任务服务。

当前为可运行的开发版本。三个程序的真实执行、中断、重开，以及 Astra 父进程退出后继续处理 Sol、Pi、Claude 结果均已通过检查。包含主会话开销的小型比较见 [报告](docs/benchmark.md)。真实 Telegram 消息测试仍待授权，详见 [检查记录](docs/verification.md)。

## 安装与调用

需要 Linux 用户级 systemd、Python 3.11+、uv，以及已登录的本机 CLI。项目固定使用 MCP Python SDK 1.30.0、Claude Agent SDK 0.2.172。实际检查的 CLI 为 Codex 0.163.0、Claude Code 2.1.307、Pi 1.3.0。

```bash
uv sync --locked
uv run agent-bridge init
uv run agent-bridge doctor
uv run agent-bridge serve
```

默认状态目录为 `~/.local/state/cli-agent-bridge`。在 `config.json` 中配置程序、供应商、默认模型和认证引用；同一供应商的其他模型可实时发现，无需逐个填写。初始配置不假定已完成推理验证，自动打分只采用 `verified: true` 的配置；主 agent 可以明确选择发现的新模型。Claude 必须填写实际供应商和模型，本机当前使用的是 DeepSeek 兼容接口。

本机安装脚本会创建命令链接、用户服务和 skill 链接，不覆盖已有的其他安装。服务启动不等于自动运行模型。

```bash
uv run python scripts/install_local.py
systemctl --user start cli-agent-bridge
```

需要登录后自动启动时再执行 `systemctl --user enable cli-agent-bridge`。停止服务不会关闭独立的 worker；结束子任务应先用 `agent_control(action=close)`。

Codex MCP 配置可通过本机 CLI 注册：

```bash
codex mcp add cli-agent-bridge -- /absolute/path/cli-agent-bridge/.venv/bin/agent-bridge mcp
```

命令行与 MCP 使用相同的九个接口。例如：

```bash
agent-bridge call agent_catalog --json '{}'
agent-bridge call agent_inspect --json '{}'
agent-bridge call agent_open --json '{"profile":"codex-sol","cwd":"/absolute/project","request_id":"open-parser-1","access":"workspace-write"}'
agent-bridge call agent_dispatch --json '{"session_id":"返回的 ID","instruction":"修复解析错误并运行指定测试","request_id":"fix-parser-1","reservation":20000}'
```

写入会话从明确提交创建独立 Git worktree，默认 `HEAD`。需要未提交文件时传 `snapshot_files=["src/parser.py","tests/test_parser.py"]`，服务保存这些文件的当前内容，再放入 worktree；支持已跟踪文件的删除、二进制文件和明确选择的未跟踪文件。目录、符号链接和越界路径会被拒绝，不会对源目录 commit、stash 或清理。返回补丁以这份输入快照为起点，只包含子任务后续修改。

非 Git 目录可用 `workspace_mode=copy`，必须同时列出 `snapshot_files`；只复制这些文件。结果保留独立目录及文件变化清单，包含内容和权限变化；仅修改可执行位也会记录。同一 `request_id` 重试使用原快照，不会覆盖子任务已做的修改。worktree 和副本都不是操作系统沙箱。

## 九个工具

| 工具 | 行为 |
| --- | --- |
| `agent_catalog` | 查询配置对应的模型目录、验证标记、并发限制；`refresh=true` 强制刷新 |
| `agent_open` | 创建独立会话；写入会话创建 worktree |
| `agent_dispatch` | 排队、明确指定配置或均衡选择；`retry_task_id` 可保留旧 attempt 后重试 |
| `agent_control` | pause、pause_drain、resume、close、reopen、respond、resolve，以及 parent_* 操作 |
| `agent_cancel` | 取消一个任务，不关闭整个会话 |
| `agent_inspect` | 总览列出会话的软件、供应商、模型和配置名；可指定任务、会话或父会话查询详情 |
| `agent_wait` | 按事件序号等待，单次最多 50 秒 |
| `agent_result` | 读取回答摘要、补丁、attempt、usage，或按字节分页读取完整原始结果 |
| `agent_review` | 接受或拒绝当前 attempt，附实际检查记录 |

写操作都需要稳定的 `request_id`。重试相同请求返回原结果，复用 ID 改变内容会报错。控制和检查结果操作需要 `expected_revision`，从 `agent_inspect` 或 `agent_result.session_revision` 取得。

暂停会中断当前轮次并停止后续派发；`pause_drain` 等当前轮次结束，没有正在执行的任务时立即暂停。`resume` 默认只允许后续排队任务运行；仅在明确提供 `continue_task_id` 时重试那条中断任务。关闭会取消未执行任务，保留历史和 worktree，只有 `reopen` 能重新打开。

下一任务会等待上一任务的原生中断请求返回，中断计时只影响原来的 attempt。中断请求报错或超时时停止该 worker，未执行任务保持排队，等待检查后明确恢复。用户取消或关闭与超时、预算同时发生时优先记录为取消。

向已有会话派发任务时，省略 `access` 会沿用会话权限；明确传入不同权限会报错，需要新建会话。派发前检查指令、依赖、重试条件和预算，无效参数不会新建工作目录或会话。

任务 `completed` 仅表示原生程序结束了回答。主 agent 检查文件和测试后调用 `agent_review(decision=accepted)`，依赖任务才可以开始。失败或换程序时使用 `retry_task_id`，旧程序、原生轮次和费用保留在旧 attempt 中。

写入任务提取补丁失败时标记为 `failed`，返回具体错误和工作目录，同时保留回答、用量与已修改文件。

Pi 已确认执行结束后，最终统计查询失败不改变任务终态：保留回答和文件变化，在 `result.statistics_error` 中记录错误，用量标为不完整。

排队任务的 `waiting_reason` 说明是在等前序检查、同会话任务、worker、并发额度、供应商限流还是 token 预算。它与实际派发使用同一组条件。输入和批准请求包含 `expires`，到任务时限后拒绝迟到的回答；未回答不会自动同意。

大输出保存在权限 600 的私有文件。默认回答最多 4,000 个字符，带 `answer_truncated` 和 `artifact`；用 `agent_result(task_id=..., artifact_id=..., offset=0, limit=4000)` 读取，按返回的 `next_offset` 继续。偏移量和上限按 UTF-8 字节计算，分页不会截断字符；上限为 65,536 字节。`attempt_id` 可取指定历史执行记录，默认只列最近 20 次。

`agent_inspect(session_id=...)` 还列出 systemd worker 所属 cgroup 中进程的宿主机 PID，包括脱离父进程会话的后台命令。Codex 沙箱内报告的 PID 可能不同。关闭后应同时确认 `observed=stopped` 和 `owned_processes.status=stopped`。借助已有 tmux、远程服务或其他进程启动的工作可能在该 cgroup 之外，`external_jobs=unknown`；主 agent 仍需检查这些具体作业。

断线后无法确认执行结果时进入 `recovery_needed`，保存任务事件，并通知已登记且允许通知的父会话，不会自动重发。查看原生历史、文件和独立进程后，用 `agent_control(action=resolve, task_id=..., resolution=failed/cancelled/interrupted, evidence=...)` 记录处理结果，再恢复会话。

worker 启动或连接失败也会通知已登记父会话。尚未执行的任务保持 `queued`，检查会话错误、修复原因后明确 `resume`；不把这些任务记成执行结果不明，也不自动重试启动。

## 自主发现与选择模型

`agent_catalog()` 自动查询已配置供应商的目录，缓存 5 分钟；`profile=配置名` 只查一个，`refresh=true` 立即重查。`profiles` 保留原配置，`discovery` 按配置返回准确模型 ID、目录来源、查询时间、已知输入类型和上下文长度。目录可见不证明账户能成功调用；`verified` 只沿用相同模型的原配置标记，其他模型不会继承它。

| 程序与供应商 | 查询方式 |
| --- | --- |
| Codex / OpenAI | App Server `model/list`，读取全部分页 |
| Pi / 已配置供应商 | RPC `get_available_models`，按配置中的供应商筛选 |
| Claude Code / Anthropic | SDK `get_server_info().models` 的 `resolvedModel`，不用界面别名 |
| Claude Code / DeepSeek | DeepSeek `GET /models`，只列声明支持 Anthropic 消息接口的模型 |

主 agent 查询后，可以用 `profile + model` 明确选模型并分配任务。例如，先在目录中确认下例 ID 仍然存在：

```bash
agent-bridge call agent_catalog --json '{"profile":"codex-sol","refresh":true}'
agent-bridge call agent_dispatch --json '{"profile":"codex-sol","model":"gpt-6-luna","cwd":"/absolute/project","instruction":"检查指定文件并给出结果","request_id":"check-discovered-model-1"}'
```

`agent_open` 同样接受 `model`。选中新模型时保留认证、供应商、工具限制、推理强度、服务档位和并发组；若目录明确不支持原来的推理强度或档位，拒绝执行。新模型的价格、上下文长度、验证时间不会照搬旧模型。会话保存最终配置，重复请求返回原结果，已有会话不能换模型。

查询失败按配置返回 `error`；旧目录保留并标记 `stale`，不能用它创建新的模型配置。原先配置的模型仍可使用，不因目录中断或移除旧别名而失效。模型信息只来自现有配置允许的供应商，不自动添加账户或供应商。当前不支持未知 Claude 兼容服务和非 OpenAI 的 Codex 目录；原有明确配置仍能执行。

发现过程不发送推理请求，也不自动逐个测试模型。新发现模型由主 agent 明确选择；省略 `profile/model` 时，服务的均衡打分仍使用已经验证的配置。当前任务只接受文本，即使模型目录列出图片输入，也不会因此启用图片传递。目录缺失价格时保留未知，不能按零计算。

接口依据：[Codex App Server](https://learn.chatgpt.com/docs/app-server)、[Pi RPC](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md)、[Claude Python SDK](https://code.claude.com/docs/en/agent-sdk/python)、[DeepSeek 模型目录](https://api-docs.deepseek.com/api/list-models/)。

## 均衡选择与费用

明确指定的配置从不自动替换。自动选择检查输入类型、工具、上下文大小、供应商许可、账户等待状态、推理验证状态和已有质量记录。默认配置 `default_strategy=balanced`：质量达标后，费用和耗时各赋 0.5 权重。单任务可传 `requirements.strategy=cost/speed/balanced`。没有可比价格时统一采用 token；数据不足时按已验证配置顺序选择并返回 `Unmeasured`，不编造分数。

选择依据是任务的全部 attempt 加主会话开销，不使用只有子会话用量的部分样本。目前只有一个父记录对应一个任务、其计数已停止且结果已检查时，才能作为完整费用/耗时样本；并行任务共享父会话时不猜测如何分摊。`requirements.learn_from_result=false` 可以保留测试结果而不用于后续选择，固定比较中的验证任务采用此设置。

换模型重试尚未开始或被取消时，原模型的失败样本仍保留。任务换到另一父会话后，旧父会话处理过它的记录仍参与判断，不能把共享开销全部计入剩余的单个任务。

任务换过模型或程序时，质量记录分别采用对应 attempt 的结果，后一个模型成功不会改写前一个模型的失败记录。这类任务不用于比较单个模型的费用和耗时；总预算仍计算所有 attempt。

目前任务输入为文本。任务可使用读取工具访问明确给出的本机文件；图片和其他二进制输入的原生传递尚未实现。

默认同时运行最多 3 个外部任务，每个 `quota_group` 最多 2 个。一个 session 同时只运行一个任务，后续任务按提交顺序等待。设置 `token_budget` 后需要每个任务提供 token 预留。预留在 SQLite 事务中分配；已知失败计入总量，未知 usage 保留原预留。限制为尽力控制，可能多付一个在途请求；要求绝不超额会拒绝启动。

重试时可用 `reservation` 更新下一次执行的预留量，省略则沿用任务原值；启用预算后必须为正数。每次 attempt 分别保存预留，调整重试预留不会减少此前未知用量的预算占用。

Codex profile 可设置 `service_tier`，启动、恢复和执行时均传给原生接口，并要求启动响应确认。`fast` 对应原生确认值 `priority`，按 [Codex 配置说明](https://learn.chatgpt.com/docs/config-file/config-reference) 处理该别名；其他不一致会报错。结果分别记录请求与确认的档位。Claude、Pi 当前不支持此字段，配置后会报错。档位变化后的历史样本不会混用。

attempt 保存起止时间和 `first_output`。Codex 使用首个非空文本增量；Pi、Claude 分别使用首个非空的原生消息和 SDK 文本消息，精度不同，不能把它们都称为首 token 延迟。原始工具结果保留命令、退出码和原生报告的耗时；测试耗时只依据实际测试记录，不从任意 shell 命令猜测。

Codex、Pi 和 Claude 均使用原生累计用量的差值。Claude 改用 `model_usage`，缺失或计数重置时退回部分统计；不把累计费用反复相加。其字段含义见 [Claude 官方用量说明](https://code.claude.com/docs/en/agent-sdk/cost-tracking)。Pi 和 Claude 的内置价格估算附来源；价格日期未知时保留未知值。它们都不能当作账户实际账单。

Claude 的空计数、缺失模型或无效字段会保存为未知值。重新收到有效计数后，只有具备前后两个有效计数的区间才能报告完整用量；不会把缺失计数当成零。

Codex 恢复时缺少本轮执行前的累计值，会将本轮用量标为不完整；不会把会话历史总量算成本轮新增。发生崩溃后也不会拿更早的成功记录补作执行前计数。

可在 profile 中配置 `price={"currency":"USD","source":"价格来源","as_of":"2026-10-22","per_million":{"input_tokens":2,"output_tokens":10,"cache_read_tokens":0.5}}`，上述数字仅为格式示例。缓存缺少价格时不按零处理。结果分别返回 `catalog_estimate` 和始终待账单确认的 `actual_billed_usd`；预算中的 `charged_tokens` 包含未知用量保留的预留，不能当作实际用量。

供应商返回 429 时，同一个 `quota_group` 的后续任务共同等待，优先遵守返回的等待秒数；缺失时使用 60 秒起、最多 15 分钟的递增等待。配额耗尽且没有恢复时间时等待明确处理。查看 `agent_catalog.quota_cooldowns`，确认账户恢复后用 `agent_control(action=quota_resume, session_id=配额组名, expected_revision=..., evidence=..., request_id=...)` 解除等待。失败任务始终需要明确重试，不会因等待结束而自动再执行。

## 父会话与 Telegram

`codex_async_python` 指向已有 `codex-async` 安装的 Python，例如 `/home/user/gits/codex-async/.venv/bin/python`。本项目直接调用其 `CodexAppServer`，保留队列、共享 daemon 和会话占用检查，不复制 Telegram 接收、发送和凭据代码。

通过 `agent_control(action=parent_register, session_id=父记录名, native_session_id=原生 Codex ID, cwd=主目录, expected_revision=0, request_id=...)` 登记主会话，`agent_open(parent=父记录名)` 将子会话关联到它。父会话必须已经配置本项目工具或能使用命令行，收到通知后才能检查结果。

登记时提供 `usage_path=该 Codex 会话 JSONL 的绝对路径`，服务核对文件首行中的原生 ID，只保存计数和读取位置。默认从登记时的累计值开始；`usage_since=Unix时间戳` 可选择已有记录的起点，精度为原生 usage 事件。重复事件和服务重启不重复计量，文件替换或计数重置标为不完整。`usage_profile=已配置的Codex配置名` 可提供同模型价格；模型变化后不套用旧模型价格。`notify=false` 只登记统计，不发送完成通知、不唤醒父会话。用 `parent_meter_stop` 固定结束计数，再读取父记录中的 `usage`。

总统计合计所有子任务 attempt 和已登记父会话；未登记的主会话、已登记但未启用统计的父记录、丢失的原生计数都会保留统计限制。不能用不完整总量声称节省费用。外部 CLI 上报有延迟，共享预算仍只能尽力控制。

完成事件持久保存后入原生队列。原客户端仍占用会话时等待；退出后向原生接口发送 `thread/resume`，不覆盖父会话的模型、推理强度和批准策略。需要批准或输入时返回原生等待标记。只恢复已登记的父会话，不重启共享 Codex 服务。`parent_pause` 和 `parent_close` 不等待正在发送的整批通知，会停止后续通知和唤醒；已经开始发送的单条通知仍可能完成，已经交给原客户端的消息不能撤回。`parent_reopen` 必须由主 agent 明确调用。

Codex 0.163.0 的恢复接口不会保留启动时临时传入的 MCP 配置。若父会话使用独立服务或临时工具配置，登记时用 `resume_config_file` 引用一份由当前用户拥有、权限 600 的 JSON 文件，内容与原会话启动时一致。仅允许 `mcp_servers`、`plugins`、`apps` 和 `features.multi_agent`，不能借此更改模型或沙箱权限；服务只保存文件位置。普通全局配置无需此参数。MCP 只读查询已标记为只读，写操作仍遵循父会话自身的批准设置；[Codex 工具批准配置说明](https://learn.chatgpt.com/docs/extend/mcp)。

通知区分队列已接收与主 agent 已处理，后者通过 `parent_ack(event_seq=...)` 确认。通知辅助进程未能启动、或入队前初始化失败时保留等待状态，可以重试；队列响应丢失时标记 `uncertain`，不自动重发。发送结果不会覆盖同时到达的 `parent_ack`。

主会话确实收到并处理通知后，`sending`、`uncertain`、`wake_pending`、`accepted` 均可确认；尚未发送的 `pending` 不能确认。等待中的通知按上次尝试时间轮换，某个父会话持续忙碌或 Telegram 暂停不会让其他会话一直等不到。恢复前会分页查完原生队列，直到找到通知或确认队列中已经没有它。

通知按事件发生时的状态说明后续操作：成功才要求 `agent_review`；结果不明要求检查后 `resolve`；失败、取消或中断要求检查现有修改后再决定是否重试；启动失败要求检查会话错误并明确恢复。

Telegram 仍使用原来的话题和发送记录。配置 `telegram_database` 为现有 bot 的 `telegram.sqlite` 后，通知和恢复前只读检查暂停、结果不明和删除标记；`/stop` 后等待，`/continue` 后允许继续，删除话题后关闭父记录。数据库不可读时停止发送。

子会话在首轮执行前登记到 bot 的 `external_children` 表，只保存原生 ID 和所属程序标识。已有 bot 增加了文字和附件过滤；不支持过滤时 worker 禁止执行模型。配置文件省略 `telegram_database` 且检测到本机已有 bot 数据库时会自动启用该登记。只有明确设置 `null` 才跳过登记，限没有真实 bot 的隔离测试环境使用。

开发中发现旧 bot 曾把测试子会话当普通会话，自动发出 11 条回复；已补过滤并保留原消息，没有删除。此次冷恢复测试的父、子会话也先登记过滤，确认 bot 扫描后没有新增 outbox 记录。主动创建测试话题和发送真实 Telegram 消息仍等待授权。

## 检查

```bash
uv run ruff check src tests scripts
uv run python -m unittest discover -s tests -v
```

自动检查使用真实 MCP/Unix socket/进程及模拟 CLI，不产生推理费用。`scripts/*_smoke.py` 和 `benchmark.py` 会真实调用模型；`cold_resume_smoke.py` 检查父进程退出后继续工作，`restart_smoke.py` 在独立服务中检查重启与后台命令关闭。命令和报告见 [检查记录](docs/verification.md)。

凭据只引用已有登录或权限 600 的私有 JSON 环境文件。数据目录、socket 和 token 文件仅当前用户可访问。子进程默认不注册其他 agent 的管理工具；Claude/Pi 只读模式仅提供读取工具，Codex 使用原生只读沙箱并禁用继承的 MCP/插件。它们共享本机用户身份，本项目不承诺防御恶意同 UID 程序。
