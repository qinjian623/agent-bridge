# macos-node 的 AGY 与 Bridge 检查（2026-11-09）

检查日期：2026-11-09，首次连接为 14:02 UTC，原生推理为 14:10 UTC。macos-node 实际返回 `macos-node.local`、Darwin、用户 `user`；SSH 普通命令和 Bridge 远程接口均成功。

| 对象 | 实际结果 |
| --- | --- |
| Bridge 服务 | `gui/502/local.cli-agent-bridge` 正在运行，PID `<process-15-pid>`，本次未重启 |
| Bridge 安装 | `/Users/user/gits/cli-agent-bridge/.venv/bin/agent-bridge`；仓库 HEAD `1eb588246e340dfce5308be3f1ab07438b9a667d`，工作区干净 |
| 远程查询 | 协调机 `agent_catalog(host_id=macos-node, profile=agy)` 成功返回远端配置 |
| AGY 配置 | AGY `1.5.3`；`runtime=antigravity`、`provider=google-antigravity`、模型 `claude-sonnet-5-5-medium`；Bridge profile 名为 `agy`，仍未标为已验证 |
| AGY 的 MCP | 原先没有 `cli-agent-bridge`，现已通过 `agy mcp add` 登记；其余配置保留 |
| 独立 MCP 客户端 | initialize、列出 19 个工具、调用 `agent_hosts` 均成功 |
| 新原生 AGY 会话 | 实际调用 Bridge 的 `agent_hosts` 成功；细节见下文 |

## 原生调用的确认方法

使用全新空目录，以默认权限启动一次 `agy --print`。没有使用 `--continue`、`--conversation`、跳过审批参数；只要求调用一次 `cli-agent-bridge` 的 `agent_hosts`，未要求文件操作、广播或其他消息发送。

- 新会话 ID：`00000000-0000-7000-8000-000000000016`。启动前持锁的原生 ID 是 `00000000-0000-7000-8000-000000000012`、`00000000-0000-7000-8000-000000000017`；测试没有恢复这两个 ID。
- Native init 确认模型 `claude-sonnet-5-5-medium`；最终状态 `SUCCESS`，1 轮，退出码 0。
- 原生流中步骤 2 的 `call_mcp_tool` 有 `ACTIVE` 和 `DONE`，没有工具错误。由于该工具统一显示为 `call_mcp_tool`，额外只读核对了这个新测试会话数据库的同一步骤：请求参数明确为 `ServerName=cli-agent-bridge`、`ToolName=agent_hosts`，输出为 `hosts` 列表，`error_details` 为空。没有保存工具正文或聊天原文。
- 测试进程 PID `<process-10-pid>` 及其进程组已退出，测试 presence 锁已释放。原有终端 PID `<process-03-pid>`、`31342` 保持运行，本次没有重启、输入或接管它们。

macOS 的 `ps -axo pid=,ppid=,comm=,args=` 本次将两个真实 AGY 的 `comm` 都截成 `/opt/homebrew/bi`，按该列识别进程会漏报。启动前用 `ps -axo pid=,comm=` 得到完整名称并发现了活动实例；需要完整参数时应使用宽输出并检查实际字段，不把截断名称当作进程不存在。

这确认了 macos-node 上新原生 AGY 可以主动调用本机 Bridge。两个既有活动 AGY 实例没有重载 MCP，不能据此声称它们也已加载新配置。没有验证真实会话广播，也没有执行 Bridge 派发到 AGY 的模型任务；远端 `agy` profile 的验证标记本次未修改。

原生检查的紧凑元数据在 macos-node：`<private-validation-state>/report.json`，权限 `0600`。该文件不含凭据、工具正文或聊天原文。

## 自动更新前需要处理的事项

- LaunchAgent 启用 `RunAtLoad`、`KeepAlive` 和 10 秒重启间隔，目前直接运行上述仓库 `.venv/bin/agent-bridge --state /Users/user/.local/state/cli-agent-bridge serve`；工作目录为该仓库。PATH 中目前没有独立 `agent-bridge` 命令。
- `uv`、`python3`、`tmux`、`rsync` 的绝对路径均为 `/opt/homebrew/bin/<命令名>`。LaunchAgent 环境变量仅有 `HOME`、`PATH`；PATH 为 `/Users/user/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin`。
- 新登记的 AGY MCP 同样引用该 `.venv` 入口。使用独立版本目录后，服务、MCP 命令、协调机 `hosts.macos-node` 的 relay 命令都应改用稳定启动器，避免其中一个继续使用旧版本。
- 未在已检查的 AGY/Gemini settings JSON 中找到服务器配置条目；`~/.gemini/antigravity-cli/mcp/cli-agent-bridge/` 仅有工具 schema 缓存。更新命令应使用已验证的 `agy mcp add cli-agent-bridge <稳定启动器> --state /Users/user/.local/state/cli-agent-bridge mcp`，由原生 CLI 保留其他配置，不修改缓存文件。
- `gh` 和 `uv` 已安装。`gh auth status` 返回无效凭据，非网络错误；SSH 环境、LaunchAgent 环境均没有 `GH_TOKEN` / `GITHUB_TOKEN`。配置文件为私有文件，本次未读取或复制凭据值。
- GitHub SSH 仓库只读访问为 `Permission denied`；现有 HTTPS origin 的只读访问也因认证失败而拒绝。因此目前不能声称 macos-node 已能从私有 GitHub release 下载更新。需要用户在 macos-node 更新原有 GitHub 登录；本次没有发起登录或设备授权。

14:18 UTC 的迁移前只读检查中，原有 18 个 `paused/stopped` 会话和其 7 条 queued 任务保持原样；另外有一个今日 09:36 CST 创建的旧 AGY 检查会话 `00000000-0000-7000-8000-000000000011`，状态为 `open/recovery_needed`，其任务 `00000000-0000-7000-8000-000000000015` 仍 queued。该会话没有 native ID 或活 worker；旧 worker PID `<process-16-pid>` 已为僵尸。所有发送中记录、原生处理中记录和未返回 API 均为 0。这些状态不等于整个队列为空，本次没有取消、恢复或修改旧任务。自动升级与首次迁移应保留这些记录，并单独处理恢复状态，不能盲目执行旧队列。

没有升级底层 AGY、Codex 或其他 CLI，没有操作训练、原有队列或工作进程。
