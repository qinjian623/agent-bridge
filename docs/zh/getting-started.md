> [English](../getting-started.md) | 中文

# 安装与使用

初次使用见[简短步骤](README.md)。本文介绍配置、结果、后台服务与运行细节。移除程序见[卸载](uninstall.md)。

## 环境与安装

使用 Linux 或 macOS，需要 Python 3.11+ 和 `uv`。Windows 用户应在 WSL2 内运行 broker、模型 CLI、项目和 MCP 客户端；不支持原生 Windows。单独安装并登录至少一个目标 CLI（`codex`、`claude`、`pi` 或 `agy`）。Bridge 不安装这些程序，也不代为登录。使用同一用户账号，只连接受信任的远端主机；不要把 broker 或监控页面作为多人服务公开。

独立安装：

```bash
uv tool install 'git+https://github.com/qinjian623/cli-agent-bridge.git'
agent-bridge --help
agent-bridge init
agent-bridge doctor
```

`uv tool update-shell` 将 uv 工具目录加入以后打开的 shell。这里不依赖特定发布 tag 或 PyPI 包。固定版本时，在 Git URL 后添加 `@<commit-or-existing-tag>`，把占位符替换为真实提交或已存在的 tag。

需要开发或保留源码目录时：

```bash
git clone https://github.com/qinjian623/cli-agent-bridge.git
cd cli-agent-bridge
uv sync --locked
uv run agent-bridge init
uv run agent-bridge doctor
uv run agent-bridge serve
```

本文后续的 `agent-bridge`，源码用户应替换为 `uv run agent-bridge`，或仓库内 `.venv/bin/agent-bridge` 的绝对路径。依赖固定在 `uv.lock`；当前运行依赖 MCP Python SDK 1.30.0、Claude Agent SDK 0.2.172。

## 配置

`agent-bridge init` 会加载已有设置，保留没有修改的内容。选择菜单项修改，用 `b` 返回，`s` 保存；`q` 放弃本次修改。它会探测已安装的程序，但完整模型 ID 需要你填写；不会执行模型测试，也不会将配置标记为已经通过推理检查。

| 菜单 | 填写内容 |
| --- | --- |
| 1) Profiles | 如 `local` 的名字、程序、供应商、完整模型 ID、共享额度组，以及可选的 effort、可执行文件、私有环境文件 |
| 2) Runtime defaults | worker 启动方式、并发数、选择策略、任务及中断时限、可选 token 预算 |
| 3) Projects | 本地项目 ID 和已存在的绝对目录；远端派单会使用 |
| 4) Remote hosts | 可选 SSH 目标、远端程序与状态目录、SSH 参数；单机使用留空 |
| 5) Services & OTA | 可选 broker 服务、固定程序入口、发布更新定时器；只有明确选择后才启动 |

首次只读任务使用名为 `local` 的 Codex、Claude Code 或 Pi 配置，填写已登录 CLI 支持的完整模型 ID。未经推理检查的配置可以明确指定；自动选择仅考虑 `verified: true` 的配置。Antigravity 只支持写入权限，见下文。

也可以非交互配置。把 `<exact-model-id>` 替换为该 CLI 支持的真实 ID：

```bash
agent-bridge init --non-interactive \
  --profile 'name=local,runtime=codex,provider=openai,model=<exact-model-id>'
```

容器或 macOS 可以使用独立后台进程启动 worker：

```bash
agent-bridge init --non-interactive --worker-launcher detached
```

默认最多同时运行 3 个外部任务，每个 `quota_group` 最多 2 个，每个会话同时执行一轮；任务总时限为 3600 秒，中断时限为 30 秒。Linux 检测到可用用户服务管理器时使用 `systemd` worker；macOS 与无 systemd 环境使用 `detached`。Linux 无人值守运行时，用户服务管理器需要在退出登录后继续运行；按主机的管理规则配置 linger。

Linux systemd broker 启动的 detached worker 仍属于该服务的进程组，直接重启服务可能终止它们。更新器会在这些 worker 存在时延期；更新前关闭不用的 Bridge 会话。

配置和 SQLite 历史默认保存在 `~/.local/state/cli-agent-bridge`。使用其他状态目录时，在子命令**之前**传入 `--state`，或设置 `AGENT_BRIDGE_STATE`。broker、CLI 和 MCP 必须使用同一目录：

```bash
agent-bridge --state "$HOME/.local/state/bridge-test" init
agent-bridge --state "$HOME/.local/state/bridge-test" serve
```

配置与环境文件为私有文件。`env_file` 是权限 `0600`、内容为字符串环境变量的 JSON 对象；不要在任务、提交、截图或共享日志中填写凭据。改变配置的 runtime/provider/model 会清除旧推理检查标记与价格信息；移除主机或更改 SSH 目标会同时清理配置与 `host_pins.json` 中的旧身份记录。

`doctor` 检查配置、可执行文件、服务可用情况与 socket 是否存在，不证明登录、模型推理或远端任务已经可用。成功调用 `agent_inspect` 才证明当前客户端能与运行中的 broker 通信。

<a id="mcp-setup"></a>
## MCP 设置

先运行 broker（`agent-bridge serve`）。Codex 注册已安装的程序：

```bash
codex mcp add cli-agent-bridge -- "$(command -v agent-bridge)" mcp
codex mcp list
```

源码安装时，将 `$(command -v agent-bridge)` 替换为仓库 `.venv/bin/agent-bridge` 的绝对路径。自定义状态目录：

```bash
codex mcp add cli-agent-bridge -- "$(command -v agent-bridge)" --state "$HOME/.local/state/bridge-test" mcp
```

其他 stdio MCP 客户端使用相同程序和参数。例如，把 `/absolute/path/to/agent-bridge` 替换为 `command -v agent-bridge` 的输出：

```json
{
  "mcpServers": {
    "cli-agent-bridge": {
      "command": "/absolute/path/to/agent-bridge",
      "args": ["mcp"]
    }
  }
}
```

更改设置后，重新加载 MCP 客户端或新建会话。登记成功只表示配置存在；当前客户端成功调用工具，才证明已经加载。MCP 启动的是 stdio 转接程序，不会启动 broker。

启用托管更新后，MCP 的绝对可执行文件应使用 `$HOME/.local/share/cli-agent-bridge/bin/agent-bridge`，或你所配置 root 下的 `bin/agent-bridge`，让以后启动的客户端加载选中的版本。

<a id="results-and-review"></a>
## 结果与检查

[README](README.md#派发第一个任务)演示了一次简单的只读任务。保存返回的 ID，不要照抄别处安装的 ID。`request_id` 对相同请求去重；同一 ID 改变参数会报冲突。换新 ID 重做任务可能再次计费。

`agent_result(task_id=...)` 返回回答、attempt 历史、文件修改和用量。`queued`、`assigned`、`running` 尚未结束；`waiting_input` 需要在过期前答复；`failed`、`cancelled`、`interrupted` 需要检查后决定是否明确重试。`recovery_needed` 要先核实执行结果，再用 `agent_control(action="resolve", ...)` 处理，不要直接重发。

`completed` 表示原生 CLI 结束了回答。检查答案和实际文件、运行相应检查后再接受。使用结果中 `attempts` 的当前 attempt ID，以及 `session_revision`。下面需替换所有占位符，将 `0` 改为该当前 revision，并填写真实检查内容：

```bash
agent-bridge call agent_result --json '{"task_id":"<task_id>"}'
agent-bridge call agent_review --json '{"task_id":"<task_id>","attempt_id":"<attempt_id>","decision":"accepted","evidence":"<what you inspected and the actual check results>","expected_revision":0,"request_id":"review-task-1"}'
```

依赖任务等待前项的当前已完成 attempt 被接受后才运行。被拒绝的 attempt 需要明确重试或新任务。`agent_result` 默认只显示简短回答；出现 `answer_truncated` 或结果文件时，用返回的 `artifact_id`、`offset`、`next_offset` 分页读取。每页按 UTF-8 字节计数，最多 65,536 字节，不能按字符计算。出现 `files.error`、`statistics_error` 时分别检查；有回答不证明文件记录和用量查询都成功。

交互查看用 `agent-bridge monitor`；一次快照用 `agent-bridge monitor --once --format json --no-native`。登记父会话后可以异步接收通知；`agent_wait` 只是可选的有限等待，最多 50 秒，不应让协调者反复轮询。

## 修改文件与停止任务

需要改文件时使用 `access="workspace-write"`。Git 项目会从 `base`（默认 `HEAD`）创建独立 worktree。源目录的未提交内容默认不包含在内；需要时明确选择 `snapshot_files=["src/parser.py","tests/test_parser.py"]`。支持已跟踪删除、二进制文件和选中的未跟踪文件；目录、符号链接和项目外路径会被拒绝。Bridge 不会对源目录 commit、stash 或清理，返回补丁以初始快照为起点。

非 Git 目录使用 `workspace_mode="copy"` 并列出 `snapshot_files`，只复制这些文件。结果包含内容与权限变化，包括只改可执行位。同一 `request_id` 重试保留原快照及进行中的修改。worktree 和副本分开文件修改，不提供操作系统安全隔离；所有 worker 都使用你的用户身份。检查后再把修改应用到主目录。

`agent_cancel` 取消一个任务；`agent_control(action="close")` 取消未开始的工作，并请求中断活动任务，同时保留历史与目录。控制操作前查询当前会话 `revision`。收到取消回执后仍需再次检查任务终态及 worker 退出情况。`pause`、`pause_drain`、`resume`、`reopen` 的具体行为见下文。停止 broker 或移除 MCP 不会停止独立 worker，也不会停止在管理的进程范围以外启动的任务。

<a id="managed-services"></a>
## 托管服务

首次使用只需要前台 broker。需要用户级后台服务时，完成配置、停止前台 broker 后运行：

```bash
agent-bridge init --non-interactive --install-broker-service --enable-broker-service
```

这会写入并启动 Linux `systemd --user` unit 或 macOS `launchd` LaunchAgent，通过固定程序入口启动。尚未选中托管发布版时，会用安装时的 Python 启动；请保留该 uv 工具环境或源码 `.venv`。遇到不同内容的已有服务文件会拒绝覆盖；不要在没有查看文件及活动任务的情况下添加 `--force-broker-service`。

Linux 默认命令：

```bash
systemctl --user status cli-agent-bridge.service
journalctl --user -u cli-agent-bridge.service -n 50
```

macOS 默认命令：

```bash
launchctl print "gui/$(id -u)/com.cli-agent-bridge.broker"
```

自定义过服务名时使用实际名称。profiles 在 broker 启动时读取，普通配置修改需要重启；主机清单会自动检查变化，见下文。停止所有工作时，还要单独关闭并检查 worker。

旧的 `uv run python scripts/install_local.py` 辅助脚本**仅支持 Linux/systemd**。它把源码 `.venv` 命令和配套 skill 链接到用户目录，再写入用户 unit；不会启动服务，已有文件冲突时拒绝覆盖。新的跨平台安装使用 `init`。两种方式的移除步骤见[卸载](uninstall.md)。

## 常见问题

| 情况 | 检查或处理 |
| --- | --- |
| 找不到命令 | 运行 `uv tool update-shell` 并打开新终端；源码目录内使用 `uv run` |
| `init` 要求人工配置 | 在交互终端执行，或在首次非交互运行时明确传入 `--profile` |
| 无法连接 broker | 启动 `serve`，检查状态目录是否一致，以及服务状态和日志 |
| 找不到 CLI 或登录失败 | 运行 `doctor`，检查目标 CLI 登录和可执行文件绝对路径；服务 PATH 可能与终端不同 |
| 无法自动选择配置 | 首个任务明确指定未经推理检查的配置；发现模型不等于已验证推理 |
| 任务一直排队 | 读取 `waiting_reason`：前序检查、暂停、并发、供应商额度、预算、worker 启动都可能需要处理 |
| revision 冲突 | 重新查询，确认原操作仍适用后再创建新请求 |
| `recovery_needed` 或发送结果不明 | 查看已有记录、原生记录及文件，核实后 resolve，不要直接重发 |
| 登记 MCP 后没有工具 | 重启或重载客户端，尝试一次只读调用；核对可执行文件与状态目录 |
| 想移除安装 | 按[卸载](uninstall.md)操作；检查内容前保留状态和工作目录 |

## 远端主机

多机器接入方案见 [多机器架构规范](multi-machine-design.md)（已整合 [AGY / DeepSeek 审查结论](multi-machine-review.md)）。代码已提供 `agent-bridge relay`、SSH JSONL 传输、节点握手、主机查询、远端任务与分组接口。远端机器需要独立运行 Broker；连接按主机复用，每次请求前核对节点身份，断线不自动重发原请求。macOS 节点的首次服务启动与只读通信检查见 [部署记录](macos-node-service-verification-20261108.md)，该检查未执行远端模型任务或广播。

当前运行结构、协议格式、远端原生广播接收检查、relay 配置刷新和升级时的连接处理见 [接口说明](multi-machine-interface.md)。`peer.control`、`peer.review`、`peer.inbox` 尚未实现，不能仅凭协议方法已声明就当作可用。

本版 Broker 会自动检查 `config.json` 中的 `hosts` 和 `host_pins.json`，运行中增删节点无需重启服务。也可调用 `agent-bridge call agent_hosts_reload --json '{}'` 立即应用配置；`agent_hosts.config_error` 显示配置错误或拒绝变更的原因。未变化节点保留原 SSH，正在处理的请求和 ACK 先完成，再替换相关连接。配置无效时保留上一份有效清单，不重发任务。

仍在分组中、存在待处理消息或结果不明的节点拒绝删除/改址；历史父通知路由需只读核实会话关闭、任务结束且完成审查后才允许。节点移除不停止远端程序，也不清除历史。使用方式、限制及首次部署说明见 [动态节点检查记录](dynamic-hosts-20261109.md)。

参与的节点都应由同一用户信任。SSH 凭据和主机密钥保存在仓库之外。远端主机需要自行安装 Bridge、配置 profiles/projects 并运行 broker；添加本地主机条目不会自动部署远端。派发远端任务前阅读所链接的多机说明。`codex-async` 和 Telegram 都是可选集成。

<a id="tool-interface"></a>
## 工具接口

| 工具 | 行为 |
| --- | --- |
| `agent_catalog` | 查询配置对应的模型目录、验证标记、并发限制；`refresh=true` 强制刷新 |
| `agent_open` | 创建独立会话；写入会话创建 worktree；可用 `name` 为新建会话命名 |
| `agent_dispatch` | 排队、明确指定配置或均衡选择；`retry_task_id` 可保留旧 attempt 后重试；新建会话时可用 `name` 命名 |
| `agent_control` | pause、pause_drain、resume、close、reopen、respond、resolve，以及 parent_* 操作 |
| `agent_cancel` | 取消一个任务，不关闭整个会话 |
| `agent_inspect` | 总览列出会话的软件、供应商、模型和配置名；可指定任务、会话或父会话查询详情 |
| `agent_wait` | 按事件序号等待，单次最多 50 秒 |
| `agent_result` | 读取回答摘要、补丁、attempt、usage，或按字节分页读取完整原始结果 |
| `agent_review` | 接受或拒绝当前 attempt，附实际检查记录 |
| `agent_groups` | 列出分组，或查询某组的 revision、成员和会话状态 |
| `agent_group` | 创建、删除分组，添加或移除成员 |
| `agent_broadcast` | 向明确指定组的成员各派发一条任务，返回每个成员的任务 ID 或跳过原因 |

写操作都需要稳定的 `request_id`。重试相同请求返回原结果，复用 ID 改变内容会报错。会话控制和结果检查操作需要 `expected_revision`，从 `agent_inspect` 或 `agent_result.session_revision` 取得。

`name` 只在本次调用创建新会话时生效：去除首尾空白后需为 1～128 个 Unicode 字符，且不能包含 NUL、换行或控制字符；允许中文和重复名称。对已有 `session_id` 传 `name` 会返回 `invalid_request`，改名请使用独立的 `agent-bridge name` 命令。`name` 进入请求 fingerprint，同一 `request_id` 改 name 会冲突。

### 分组与广播

组成员可以是 bridge 管理的 `session_id`，也可以是登记的原生会话，同一会话可以加入多个组。分组不按目录、模型、父会话或 `quota_group` 自动添加成员，不改变现有父会话通知。原有接口继续使用。

以下示例先查询成员，再使用返回的组 revision 广播。将示例会话 ID 换成要接收消息的会话；创建分组需要 revision 0。

```bash
agent-bridge call agent_group --json '{"group_id":"backend","action":"create","session_ids":["session-a","session-b"],"expected_revision":0,"request_id":"backend-create-1"}'
agent-bridge call agent_groups --json '{}'
agent-bridge call agent_groups --json '{"group_id":"backend"}'
agent-bridge call agent_broadcast --json '{"group_id":"backend","message":"检查各自任务的接口变更并返回结果","expected_revision":0,"reservation":8000,"request_id":"backend-update-1"}'
```

`agent_group` 的 `add`、`remove` 使用非空 `session_ids` 列表；`delete` 不传成员列表。修改时使用最新组 revision；添加已有成员、移除已不在组内的成员不会重复记录成员或增加 revision。未知会话会使整次成员修改失败。删除后保留组 ID 以防旧请求误发给同名新组；需要重建时使用新 ID。

广播必须提供一个存在且非空的 `group_id` 和查询得到的 `expected_revision`，不支持省略目标或用通配符发送给所有会话。成员有变化时拒绝旧 revision。组的成员快照、各接收方的任务和重试记录在同一次 SQLite 事务中保存：只会给该快照里的成员创建任务，组外会话不会收到广播。加入、移出成员或删除组只影响之后的新广播；已排队的任务需要用现有 `agent_cancel` 单独取消。

广播沿用任务队列，会让成员执行消息内容并可能产生推理费用。一个成员繁忙时，新任务会排队；暂停、正在排空、已关闭或等待恢复的成员会返回 `skipped` 和原因，不会自动恢复。返回的 `queued_count` 只表示已入队，`recipients` 中每条已入队记录含 `task_id`；用 `agent_result` 检查实际完成或失败，用 `agent_wait` 查看执行事件。`reservation` 是每个成员的预留量，预算、并发和配额限制仍由原队列执行。

响应丢失或重复调用时保留相同的 `request_id` 和参数，将返回原收件人及原任务 ID，即使之后组成员或会话状态改变也不会补发。更换请求 ID 表示一次新广播，可能再次执行同一指令。某个成员执行失败不会自动重发，也不影响其他成员；按原有检查、恢复和重试接口处理。广播生成的任务带有 `details.group_id`、`group_revision` 和 `broadcast_id`，不用于自动模型选择的质量统计；它们完成后的父会话通知仍发送到各自原有的 `parent`。

已有的原生 Codex / Antigravity 会话可以通过 `native_members` 加入组，无需创建替代会话或 bridge worker。每个成员提供 `runtime`、原生会话 ID `native_id`、原会话绝对目录 `cwd` 和可选 `label`；移除时只需 `runtime`、`native_id`。同一个原生会话重复添加不会重复发送，成员信息不一致时拒绝修改。已由 bridge 管理的原生会话仍使用其 bridge `session_id`。

```bash
agent-bridge call agent_group --json '{"group_id":"native-team","action":"create","native_members":[{"runtime":"codex","native_id":"<native-session-uuid>","cwd":"/absolute/project","label":"负责人"}],"expected_revision":0,"request_id":"native-team-create-1"}'
agent-bridge call agent_broadcast --json '{"group_id":"native-team","message":"组内通知","expected_revision":0,"request_id":"native-team-notice-1"}'
agent-bridge call agent_broadcast_status --json '{"broadcast_id":"上一步返回的ID","refresh":true}'
```

原生 Codex 消息使用已经运行的共享服务的 `thread/queue/add`，不调用 `thread/resume`、`turn/start`、`turn/steer` 或中断接口，也不启动 daemon。正在运行的原生终端保持原有写锁，队列由原生程序处理；关闭的 Codex 会话不会被本功能唤醒。发送前检查原生 ID 和目录，并继续遵守 Telegram 暂停、删除状态。

原生 Antigravity 支持关闭后按组消息自动恢复。新广播先保存为 `pending`；bridge 与 Telegram 共用 `antigravity-locks/<native_id>.lock`，并检查原生 presence 锁、进程参数的两种 `--conversation` 写法及 `-c/--continue`。同一会话仍活动、存在重复实例或无法确认进程所属时，消息等待，不写活动终端。确认关闭后，核对原生记录存在，再用原 ID 启动一次 stream-json 进程；启动后及写入前再次检查其他实例。原会话的模型、认证、MCP 和权限设置沿用原生配置，不传跳过权限参数。历史 `skipped` 通知保持原样，不自动补发旧通知。

`agent_broadcast_status` 列出每个目标的当前结果：`pending` 只表示 bridge 本地待发送；`enqueued` 表示已取得 Codex 原生队列回执。Codex 的 `received` 表示原生历史中出现了匹配消息 ID 的用户消息；AGY 的 `received` 要求原 ID 返回新的 `user_input/DONE` 事件，过滤恢复时重放的历史步骤。`native_turn_id` 对 AGY 是这条原生用户输入的步骤编号。收到后立即保存，后续推理通过 `processing_status=running/completed/failed` 单独记录，其他会话可以继续发送。原生审批拒绝会记录失败，不放宽权限重试。

`refresh=true` 对 Codex 只读取队列和最近 100 轮历史，消息离开队列但找不到对应记录时为 `unconfirmed`；AGY 显示保存的真实回执，不通过查询再次启动进程。发送回执丢失或服务在发送时退出会记为 `uncertain`，不会自动重发；只能通过读取原生记录核实。收到后服务退出，收件状态仍保留，未确认的执行结果单独记为 `processing_status=uncertain`。连接失败、活动实例等待等明确发生在发送前的情况可以重试。每次恢复完成后清理本次启动的进程及其 MCP 子进程。

原生会话的费用和执行次数不由 bridge worker 统计，启用了 bridge token 预算时拒绝包含原生成员的广播。移除成员或删除组不撤回已保存的广播；原生消息没有 bridge task_id，不能用 `agent_cancel` 撤销。分组也不改变工具权限：本机具有 bridge 管理权限的原生会话可以调用组广播；受限的 bridge 子会话仍不能管理其他会话。通知时只把实际支持的收发方式告诉组员，收到广播后不要自动再次广播确认，避免互相重复触发。

原生会话也可以用 `agent_group_inbox(group_id,runtime,native_id,after,limit)` 主动读取发给自己的组消息，Antigravity 推送被跳过的消息也保留在这里。必须指定组和当前原生成员；结果只含该成员在该组的收件记录。保存返回的 `cursor`，下次作为 `after` 传入，避免重复处理；每页最多 10 条。这个只读工具不启动、恢复或中断任何会话，不修改发送回执，协调者代查不能当作目标会话已收到。

Antigravity 的 MCP 可用 `agy mcp add cli-agent-bridge /absolute/path/to/agent-bridge mcp` 登记，之后恢复时由原生程序自动加载。`agy mcp list` 只证明配置存在，独立 MCP 握手只证明服务可用，都不能证明正在运行的 AGY 已加载工具。现有窗口可用 `/mcp` 手动重载；实际工具调用与原生收件记录分别检查。MCP 本身不提供向活动终端推送消息的入口，因此自动恢复只在原实例关闭后执行。参见 [官方 MCP 说明](https://www.antigravity.google/docs/cli/mcp/)。

暂停会中断当前轮次并停止后续派发；`pause_drain` 等当前轮次结束，没有正在执行的任务时立即暂停。`resume` 默认只允许后续排队任务运行；仅在明确提供 `continue_task_id` 时重试那条中断任务。关闭会取消未执行任务，保留历史和 worktree，只有 `reopen` 能重新打开。

下一任务会等待上一任务的原生中断请求返回，中断计时只影响原来的 attempt。中断请求报错或超时时停止该 worker，未执行任务保持排队，等待检查后明确恢复。用户取消或关闭与超时、预算同时发生时优先记录为取消。

`task_timeout` 是一次任务执行的总时限，默认 3600 秒，持续有输出不会延长它。长编译任务可在 `agent_dispatch` 中指定 `requirements={"task_timeout":21600}`，将这一条任务的时限设为 6 小时；省略参数时仍用节点默认值，其他任务不变。值必须是有限正数，单位秒。时限在 attempt 开始时固定，审批等待也使用该时限。本地重试省略参数沿用原任务值，明确传值仅影响下一次执行；不能修改运行中任务的时限。远端 `retry_task_id` 暂不支持，会明确拒绝。

节点和 worker 都必须加载支持 `task_timeout_v1` 的版本。旧节点或仍在运行的旧 worker 会明确拒绝覆盖时限，不会把入队当作参数已经生效；升级不要求中断它们，后续使用支持该功能的新 worker。已知超时、暂停或取消原因保存在结果的 `termination` 中，原生模糊错误保留为 `native_error`；执行结果不明仍需检查后恢复。

Linux 文件名可以包含非 UTF-8 字节。结果中的 `untracked_files` 和 `changed_files` 用安全转义显示；发生转义时，平行的 `untracked_files_raw_hex` / `changed_files_raw_hex` 保存原始路径字节。不要把显示文字直接用作文件路径。Git patch 按原始 bytes 保存；copy 的 JSON 清单保留可逆转义，符号链接目标也不丢字节。采集失败单独记录于 `files.error`，不会盖掉已经确认的原生回答、用量或中断原因。

向已有会话派发任务时，省略 `access` 会沿用会话权限；明确传入不同权限会报错，需要新建会话。派发前检查指令、依赖、重试条件和预算，无效参数不会新建工作目录或会话。

任务 `completed` 仅表示原生程序结束了回答。主 agent 检查文件和测试后调用 `agent_review(decision=accepted)`，依赖任务才可以开始。失败或换程序时使用 `retry_task_id`，旧程序、原生轮次和费用保留在旧 attempt 中。

写入任务提取补丁失败时标记为 `failed`，回答、用量与工作目录中的修改仍保留，先检查 `result.files.error` 和实际文件。

Pi 已确认执行结束后，最终统计查询失败不改变任务终态：保留回答和文件变化，在 `result.statistics_error` 中记录错误，用量标为不完整。

排队任务的 `waiting_reason` 说明是在等前序检查、同会话任务、worker、并发额度、供应商限流还是 token 预算。它与实际派发使用同一组条件。输入和批准请求包含 `expires`，到任务时限后拒绝迟到的回答；未回答不会自动同意。

大输出保存在权限 600 的私有文件。默认回答最多 4,000 个字符，带 `answer_truncated` 和 `artifact`；用 `agent_result(task_id=..., artifact_id=..., offset=0, limit=4000)` 读取，按返回的 `next_offset` 继续。偏移量和上限按 UTF-8 字节计算，分页不会截断字符；上限为 65,536 字节。`attempt_id` 可取指定历史执行记录，默认只列最近 20 次。

`agent_inspect(session_id=...)` 还列出 systemd worker 所属 cgroup 中进程的宿主机 PID，包括脱离父进程会话的后台命令。Codex 沙箱内报告的 PID 可能不同。关闭后应同时确认 `observed=stopped` 和 `owned_processes.status=stopped`。借助已有 tmux、远程服务或其他进程启动的工作可能在该 cgroup 之外，`external_jobs=unknown`；主 agent 仍需检查这些具体作业。

断线后无法确认执行结果时进入 `recovery_needed`，保存任务事件，并通知已登记且允许通知的父会话，不会自动重发。查看原生历史、文件和独立进程后，用 `agent_control(action=resolve, task_id=..., resolution=failed/cancelled/interrupted, evidence=...)` 记录处理结果，再恢复会话。

worker 启动或连接失败也会通知已登记父会话。尚未执行的任务保持 `queued`，检查会话错误、修复原因后明确 `resume`；不把这些任务记成执行结果不明，也不自动重试启动。

<a id="autonomous-model-discovery-and-selection"></a>
## 自主发现与选择模型

`agent_catalog()` 自动查询已配置供应商的目录，缓存 5 分钟；`profile=配置名` 只查一个，`refresh=true` 立即重查。`profiles` 保留原配置，`discovery` 按配置返回准确模型 ID、目录来源、查询时间、已知输入类型和上下文长度。目录可见不证明账户能成功调用；`verified` 只沿用相同模型的原配置标记，其他模型不会继承它。

| 程序与供应商 | 查询方式 |
| --- | --- |
| Codex / OpenAI | App Server `model/list`，读取全部分页 |
| Pi / 已配置供应商 | RPC `get_available_models`，按配置中的供应商筛选 |
| Claude Code / Anthropic | SDK `get_server_info().models` 的 `resolvedModel`，不用界面别名 |
| Claude Code / DeepSeek | DeepSeek `GET /models`，只列声明支持 Anthropic 消息接口的模型 |
| Claude Code / Ollama Cloud | Ollama `GET /api/tags`，保留目录返回的准确模型 ID |

主 agent 查询后，可以用 `profile + model` 明确选模型并分配任务。例如，先在目录中确认下例 ID 仍然存在：

```bash
agent-bridge call agent_catalog --json '{"profile":"codex-sol","refresh":true}'
agent-bridge call agent_dispatch --json '{"profile":"codex-sol","model":"gpt-6-luna","cwd":"/absolute/project","instruction":"检查指定文件并给出结果","request_id":"check-discovered-model-1"}'
```

`agent_open` 同样接受 `model`。选中新模型时保留认证、供应商、工具限制、推理强度、服务档位和并发组；若目录明确不支持原来的推理强度或档位，拒绝执行。新模型的价格、上下文长度、验证时间不会照搬旧模型。会话保存最终配置，重复请求返回原结果，已有会话不能换模型。

查询失败按配置返回 `error`；旧目录保留并标记 `stale`，不能用它创建新的模型配置。原先配置的模型仍可使用，不因目录中断或移除旧别名而失效。模型信息只来自现有配置允许的供应商，不自动添加账户或供应商。当前不支持未知 Claude 兼容服务和非 OpenAI 的 Codex 目录；原有明确配置仍能执行。

发现过程不发送推理请求，也不自动逐个测试模型。新发现模型由主 agent 明确选择；省略 `profile/model` 时，服务的均衡打分仍使用已经验证的配置。当前任务只接受文本，即使模型目录列出图片输入，也不会因此启用图片传递。目录缺失价格时保留未知，不能按零计算。

接口依据：[Codex App Server](https://learn.chatgpt.com/docs/app-server)、[Pi RPC](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md)、[Claude Python SDK](https://code.claude.com/docs/en/agent-sdk/python)、[DeepSeek 模型目录](https://api-docs.deepseek.com/api/list-models/)、[Ollama Cloud](https://github.com/ollama/ollama/blob/main/docs/cloud.mdx)。

### Claude Code 使用 Ollama Cloud

在 `config.json` 的 `profiles` 中加入独立配置，保留其他已有配置；下面是要合并到 `profiles` 的条目：

```json
{
  "claude-ollama": {
    "runtime": "claude",
    "provider": "ollama-anthropic",
    "endpoint_host": "ollama.com",
    "model": "deepseek-v4.1-flash",
    "env_file": "/absolute/private/ollama-env.json",
    "quota_group": "ollama-account",
    "billing_mode": "api",
    "verified": false
  }
}
```

`env_file` 是权限为 `0600` 的 JSON 字符串表，至少设置 `ANTHROPIC_BASE_URL=https://ollama.com`；可在其中单独设置 `HTTPS_PROXY`（代理变量可选，保留继承值和明确的空值，Bridge 不指定本机代理端口）。认证使用 `OLLAMA_API_KEY`，未设置时读取当前用户已有的 `~/.config/claude-ollama/api-key`（属主必须为当前用户，普通文件、权限 `0600`），不借用官方 DeepSeek 的凭据。

新配置和供应商代码需要 broker 重新启动后读取。目录查询成功仍保持 `verified: false`，真实调用核对返回模型后再标为已验证。Flash 低峰优先规则由 [分工 skill](skills/cli-agent-bridge/resources/router-prompt.md#flash-按供应商低峰时间分配) 执行，时间脚本不修改 broker 的均衡打分，也不迁移已有会话。

<a id="balanced-selection-and-costs"></a>
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

<a id="parent-sessions-and-telegram"></a>
## 父会话与 Telegram

`codex_async_python`（可选，默认 `null`）可指向已有 `codex-async` 安装的 Python，例如 `/path/to/codex-async/.venv/bin/python`；普通用户无需安装 `codex-async` 即可使用单机/多机核心功能。配置该项时，本项目直接调用其 `CodexAppServer`，保留队列、共享 daemon 和会话占用检查，不复制 Telegram 接收、发送和凭据代码。

通过 `agent_control(action=parent_register, session_id=父记录名, native_session_id=原生 Codex ID, cwd=主目录, expected_revision=0, request_id=...)` 登记主会话，`agent_open(parent=父记录名)` 将子会话关联到它。父会话必须已经配置本项目工具或能使用命令行，收到通知后才能检查结果。

登记时提供 `usage_path=该 Codex 会话 JSONL 的绝对路径`，服务核对文件首行中的原生 ID，只保存计数和读取位置。默认从登记时的累计值开始；`usage_since=Unix时间戳` 可选择已有记录的起点，精度为原生 usage 事件。重复事件和服务重启不重复计量，文件替换或计数重置标为不完整。`usage_profile=已配置的Codex配置名` 可提供同模型价格；模型变化后不套用旧模型价格。`notify=false` 只登记统计，不发送完成通知、不唤醒父会话。用 `parent_meter_stop` 固定结束计数，再读取父记录中的 `usage`。

总统计合计所有子任务 attempt 和已登记父会话；未登记的主会话、已登记但未启用统计的父记录、丢失的原生计数都会保留统计限制。不能用不完整总量声称节省费用。外部 CLI 上报有延迟，共享预算仍只能尽力控制。

完成事件持久保存后入原生队列。原客户端仍占用会话时等待；退出后向原生接口发送 `thread/resume`，不覆盖父会话的模型、推理强度和批准策略。需要批准或输入时返回原生等待标记。只恢复已登记的父会话，不重启共享 Codex 服务。`parent_pause` 和 `parent_close` 不等待正在发送的整批通知，会停止后续通知和唤醒；已经开始发送的单条通知仍可能完成。`parent_reopen` 必须由主 agent 明确调用。

Codex 0.163.0 的恢复接口不会保留启动时临时传入的 MCP 配置。若父会话使用独立服务或临时工具配置，登记时用 `resume_config_file` 引用一份由当前用户拥有、权限 600 的 JSON 文件，内容与原会话启动时一致。仅允许 `mcp_servers`、`plugins`、`apps` 和 `features.multi_agent`，不能借此更改模型或沙箱权限；服务只保存文件位置。普通全局配置无需此参数。MCP 只读查询已标记为只读，写操作仍遵循父会话自身的批准设置；[Codex 工具批准配置说明](https://learn.chatgpt.com/docs/extend/mcp)。

通知区分原生入队、已进入原生轮次与父 agent 已处理。`native_status=enqueued` 只表示入队；匹配到原生 `userMessage.clientId` 才记为 `received` 并保存 `native_turn_id`，这仍不证明模型已读。`parent_ack(event_seq=...)` 把处理状态记为 `processed`，可以来自父 agent 对 `agent_result` 的独立处理，不推断通知已进入原生轮次。旧版 `accepted` 不作为已读证明，会重新只读核查。

ACK 后仍在原生队列的同一事件会被精确撤回，结果记录在 `queue_cleanup=removed/already_started/uncertain`。同时核对目标、事件 clientId 与已有回执；发送期间到达 ACK 也保留迟到回执。清理不受父记录暂停或关闭影响，只操作这一条队列记录，不恢复或中断会话。已经进入原生轮次的通知不能撤回；找不到消息、回执冲突或删除失败都不当作已读，不重新发送。删除结果未知后，后续自动检查仅查询队列和原生轮次，不再次删除；尚未尝试删除就连接失败时允许安全重试。

通知辅助进程未能启动、或入队前初始化失败时保留等待状态，可以重试；队列响应丢失时标记 `uncertain`，不自动重发。发送结果不会覆盖同时到达的 `parent_ack`。通知正文包含事件时间，并明确不授权恢复已暂停的开发或重跑任务。

主会话确实收到并处理通知后，`sending`、`uncertain`、`wake_pending`、`accepted` 均可确认；尚未发送的 `pending` 不能确认。等待中的通知按上次尝试时间轮换，某个父会话持续忙碌或 Telegram 暂停不会让其他会话一直等不到。恢复前会分页查完原生队列，直到找到通知或确认队列中已经没有它。

通知按事件发生时的状态说明后续操作：成功才要求 `agent_review`；结果不明要求检查后 `resolve`；失败、取消或中断要求检查现有修改后再决定是否重试；启动失败要求检查会话错误并明确恢复。

Telegram 仍使用原来的话题和发送记录。配置 `telegram_database` 为现有 bot 的 `telegram.sqlite` 后，通知和恢复前只读检查暂停、结果不明和删除标记；`/stop` 后等待，`/continue` 后允许继续，删除话题后关闭父记录。数据库不可读时停止发送。

子会话在首轮执行前会检查 `telegram_database`（默认 `null`，即不启用）：配置可读的 bot `telegram.sqlite` 路径后，会登记到 `external_children` 表并对文字和附件过滤，不支持过滤时 worker 禁止执行模型；设为 `null` 或省略该项时跳过此检查。

历史检查不代表已经验证真实 Telegram 收发。首次连接现有 bot 前核实子会话过滤；真实发送会影响现有话题。

## 监控与接入接口

TUI（`agent-bridge monitor`）、Web（`agent-bridge monitor --web`）和 MCP `agent_monitor` 统一覆盖 `pi`、`claude`、`antigravity`（`agy`）、`codex` 四个运行时，分别统计 Bridge 托管会话与本机原生 CLI / worker 进程（包括 `presence/*.lock`、`codex resume`、独立 `pi` 与 `claude` 终端）。Web 与 Bridge 会话列表优先展示活跃 / 运行中 / 待审批会话，避免被大量历史已停止记录淹没。

- CLI 过滤：`agent-bridge monitor --once --format json --runtime agy --source all --active-only --session-limit 50`（也支持 `--no-native`；`--web` 不接受终端筛选参数）。
- MCP 工具：`agent_monitor` 支持 `runtime`（含 `agy` 别名）、`source`（`all`/`bridge`/`native`）、`status`（`all`/`active`/`running`/`idle`/`waiting`/`waiting_input`/`stopped`）、`session_limit`、`include_native`、`after_seq`。
- HTTP / SSE 接口（携带 `Authorization: Bearer <admin.token>` 或 `bridge_session` Web 登录 Cookie）：
  - `GET /api/v1/health`：节点身份、总指标与四运行时汇总；
  - `GET /api/v1/runtimes`：`pi` / `claude` / `antigravity`（`agy`）/ `codex` 状态与总数；
  - `GET /api/v1/sessions`：统一返回 Bridge 与原生进程会话（支持 `runtime`、`source`、`status`、`host_id`、`limit`）；
  - `GET /api/v1/events`：按 `after_seq` 增量拉取事件；
  - `GET /api/v1/stream`：SSE 实时流（支持 `runtime`、`source`、`status`、`after_seq`、`Last-Event-ID`、`interval`、`once=1`、`max_events`、`max_seconds`）；
  - `GET /api/v1/schema`：接口与字段自描述清单；
  - `GET /api/snapshot`：保留完整快照格式，支持上述筛选 query 参数。

## 检查

跨程序执行、中断、重开及父会话退出后的恢复记录见[检查记录](verification.md)；包含父会话开销的小型比较见[报告](benchmark.md)。Antigravity 有离线协议测试，以及模型目录、零推理初始化的历史检查；真实推理、工具执行及实时中断需要在各环境中确认。历史检查不证明你的账号或本次安装已经可用。

```bash
uv run ruff check src tests scripts
uv run python -m unittest discover -s tests -v
```

自动检查使用真实 MCP/Unix socket/进程及模拟 CLI，不产生推理费用。`scripts/*_smoke.py` 和 `benchmark.py` 会真实调用模型；`cold_resume_smoke.py` 检查父进程退出后继续工作，`restart_smoke.py` 在独立服务中检查重启与后台命令关闭。命令和报告见 [检查记录](verification.md)。

凭据只引用已有登录或权限 600 的私有 JSON 环境文件。数据目录、socket 和 token 文件仅当前用户可访问。子进程默认不注册其他 agent 的管理工具；Claude/Pi 只读模式仅提供读取工具，Codex 使用原生只读沙箱并禁用继承的 MCP/插件。它们共享本机用户身份，本项目不承诺防御恶意同 UID 程序。

## Antigravity

在私有 `config.json` 的 `profiles` 中加入配置，已有的其他配置继续保留：

```json
{
  "agy-sonnet": {
    "runtime": "antigravity",
    "provider": "google-antigravity",
    "model": "claude-sonnet-5-5-medium",
    "quota_group": "antigravity-account",
    "billing_mode": "subscription",
    "verified": false
  }
}
```

默认可执行文件为 `agy`，可用 `executable` 覆盖。即使选择 Claude 模型，供应商仍为 `google-antigravity`，不能填写 `anthropic`。模型使用 `agy models` 返回的完整 ID；若设置 `effort`，必须与 ID 的 low/medium/high 档一致。未验证配置不参与自动选择，但可以显式指定。

broker 在启动时读取 profiles。修改后停止并重新启动前台 broker；托管服务用户检查活动任务后重启对应服务，见[托管服务](#managed-services)。已有会话保留原来的程序与模型。

```bash
agent-bridge call agent_catalog --json '{"profile":"agy-sonnet","refresh":true}'
agent-bridge call agent_open --json '{"profile":"agy-sonnet","cwd":"/absolute/project","access":"workspace-write","request_id":"agy-open-1"}'
# session_id 用于 agent_dispatch/agent_control；取消任务用 task_id 调用 agent_cancel。
```

- 仅支持文本和 `workspace-write`。只读请求在创建会话前拒绝；`--mode plan` 不被当作只读文件访问保证。写入会话继续使用独立 worktree/副本。
- 原生 headless 模式不支持 bridge 的逐次工具审批，也不接受 Claude SDK 的控制消息。调用使用原生权限策略和 `--sandbox`，不使用 `--dangerously-skip-permissions`。需要审批且被拒绝的工具会使 bridge 将任务记为失败，即使 CLI 的最终状态为 SUCCESS。`tools` 描述任务选择所需工具，不限制 Antigravity 实际可用的工具。
- 每个任务使用独立的 `--input-format stream-json --output-format stream-json` 进程，发送一条提示后关闭输入；后续任务用同一 conversation ID 恢复。只有确认当前 conversation 的终态 `result`、进程退出及错误输出读完后才结束任务。进程退出、文本输出和 RUNNING/WAITING 状态均不证明执行完成。
- SIGINT 请求中断。只有原生 INTERRUPTED/CANCELED 结果确认后才记录中断；进程消失而没有结果进入 `recovery_needed`。确认中断后关闭该原生进程，下一轮通过 `--conversation` 恢复。
- 首个新会话任务使用原生累计计数，思考不重复计入输出。每轮重开的进程缺少可靠累计基数或计数重置时，不把历史累计量计入新任务；改报本轮观察到的步骤用量，`complete=false`。缓存写入及计费换算未知，不自动生成美元估价；它不是订阅额度总量。
- `RESOURCE_EXHAUSTED` 等终态错误进入既有额度等待处理，不自动换模型或重试。

账号限制：使用前核实你的账号条款。此前引用的 Google [官方 FAQ](https://www.antigravity.google/docs/faq#can-i-use-antigravity-cli-headless-mode-in-third-party-coding-agents-or-automate-account-switching) 说明，个人套餐不支持把 `agy` 封装到第三方编码代理或应用。实现这些接口不表示个人订阅支持该用途；请按具体账号授权使用。本项目不会自动加入 Antigravity 配置或切换账号。协议参考 [Headless mode](https://www.antigravity.google/docs/cli/headless)。

## GitHub 发布与定时更新

GitHub 在 push/PR 后于 Linux 运行离线测试和 Ruff，再于 Linux/macOS 运行打包安装检查。稳定 `vX.Y.Z` tag 全部通过后自动发布 wheel、源码包、`SHA256SUMS` 与 `latest.json`；见 [CI 与发布](ci-release.md)。

已安装版本可使用：

```bash
agent-bridge update install
agent-bridge update check
agent-bridge update run
agent-bridge update --drain-workers run
```

更新配置为 `~/.local/state/cli-agent-bridge/update.json`（0600），可配置 GitHub 仓库、独立版本目录、systemd/launchd 服务名和检查间隔。OTA 更新**仅通过公开 GitHub Releases HTTPS 直连**（若配置了 `HTTPS_PROXY` 则走代理 `CONNECT` 隧道）拉取资产并校验 `SHA256SUMS`，无需配置 `gh` CLI 或登录凭据。安装时默认不启动定时器，也不修改已有 Broker 服务；通过 `agent-bridge init`（第 5 项服务与 OTA 向导）或完成服务迁移后再启用定时器。Linux 每小时检查并随机延迟最多 5 分钟，macOS 使用 LaunchAgent 定时检查。

更新下载经过 SHA256 检查的 wheel，在新版本专用虚拟环境中安装，先运行配置与隔离服务检查。默认启用 OTA 热更新：不中断运行中的任务和兼容 worker，进入维护前仅排空短在途请求、通知与非 SSH helper；兼容 Worker 与 SSH relay 在当前轮次结束或空闲后自行退场由新版拉起（仅当目标版声明不兼容、或指定 `--drain-workers` 时才等待全部任务与 Worker 清空）；暂停会话里的历史排队任务原样保留。切换版本前，Broker 的短期维护记录禁止新派单和发送，其他节点也不能在此时投递新工作；CLI 在维护和重启短暂窗口内对只读与带 `request_id` 的幂等请求自动重试。服务启动后核对实际版本和 PID，明确失败时恢复上一程序版本，不还原数据库、不重放任务；重启结果不明时保持 `uncertain`，停止自动重试。详情参见 [OTA 热更新设计](ota-hot-update-20261116.md)。

`~/.local/share/cli-agent-bridge/bin/agent-bridge` 是固定入口，Broker、CLI、MCP、远端 relay 和 skill 应使用该入口或 `current/skill`，不能只改 Broker 而让其它入口仍指向旧 checkout。版本目录保留，源仓库及其未提交修改不被更新程序覆盖。正在运行的原生终端不被重启，MCP 配置修改在下次加载时生效。定时器只更新 Bridge，不更新 Codex、Claude、Pi 或 AGY 本身。

最近结果保存在状态目录的 `update-status.json`；下载或校验失败会记录失败类型。首版迁移及各节点验证记录另列，不能将定时器已安装当作升级已成功。


## detached worker 导致的更新延期

detached worker 仍是 broker 的子进程，即使没有正在执行的任务，更新程序也可能报告 `broker_helpers` 并延期。重试前关闭不再使用的 Bridge 会话；程序会保留工作，不会直接重启仍拥有活动子进程的 broker。这不要求关闭无关的原生终端。
