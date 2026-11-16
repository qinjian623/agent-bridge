> 语言：[English](../macos-node-agy-check-20261109.md) | 中文

# 远端 macOS 节点（`macos-node`）的 AGY 与 Bridge 检查（2026-11-09）

检查日期：2026-11-09，首次连接为 14:02 UTC，原生推理为 14:10 UTC。`macos-node`（匿名化名称）为 macOS 测试从机的 SSH 别名，返回 Darwin；SSH 普通命令和 Bridge 远程接口均成功。

| 对象 | 实际结果 |
| --- | --- |
| Bridge 服务 | `gui/<uid>/local.cli-agent-bridge` 正在运行，本次未重启 |
| Bridge 安装 | 仓库 `.venv/bin/agent-bridge`，工作区干净 |
| 远程查询 | 协调机 `agent_catalog(host_id=macos-node, profile=agy)` 成功返回远端配置 |
| AGY 配置 | AGY `1.5.3`；`runtime=antigravity`、`provider=google-antigravity`、模型 `claude-sonnet-5-5-medium`；Bridge profile 名为 `agy`，仍未标为已验证 |
| AGY 的 MCP | 原先没有 `cli-agent-bridge`，现已通过 `agy mcp add` 登记；其余配置保留 |
| 独立 MCP 客户端 | initialize、列出 19 个工具、调用 `agent_hosts` 均成功 |
| 新原生 AGY 会话 | 实际调用 Bridge 的 `agent_hosts` 成功；细节见下文 |

## 原生调用的确认方法

使用全新空目录，以默认权限启动一次 `agy --print`。没有使用 `--continue`、`--conversation`、跳过审批参数；只要求调用一次 `cli-agent-bridge` 的 `agent_hosts`，未要求文件操作、广播或其他消息发送。

- 新会话 ID：`<test-session-id>`。启动前持锁的原生 ID（`<locked-id-1>`、`<locked-id-2>`）未被恢复。
- Native init 确认模型 `claude-sonnet-5-5-medium`；最终状态 `SUCCESS`，1 轮，退出码 0。
- 原生流中步骤 2 的 `call_mcp_tool` 有 `ACTIVE` 和 `DONE`，没有工具错误。由于该工具统一显示为 `call_mcp_tool`，额外只读核对了这个新测试会话数据库的同一步骤：请求参数明确为 `ServerName=cli-agent-bridge`、`ToolName=agent_hosts`，输出为 `hosts` 列表，`error_details` 为空。没有保存工具正文或聊天原文。
- 测试进程及其进程组已退出，测试 presence 锁已释放。原有终端保持运行，本次没有重启、输入或接管它们。

macOS 的 `ps -axo pid=,ppid=,comm=,args=` 本次将真实 AGY 的 `comm` 截断，启动前改用 `ps -axo pid=,comm=` 得到完整名称并发现了活动实例；需要完整参数时应检查实际字段，不把截断名称当作进程不存在。

这确认了 `macos-node` 上新原生 AGY 可以主动调用本机 Bridge。既有活动 AGY 实例没有重载 MCP，不能据此声称它们也已加载新配置。没有验证真实会话广播，也没有执行 Bridge 派发到 AGY 的模型任务；远端 `agy` profile 的验证标记本次未修改。

原生检查的紧凑元数据在 `macos-node`：`<private-validation-state>/report.json`，权限 `0600`。该文件不含凭据、工具正文或聊天原文。

## 自动更新迁移前说明（历史记录）

- LaunchAgent 原先启用 `RunAtLoad`、`KeepAlive` 和 10 秒重启间隔，运行仓库 `.venv/bin/agent-bridge --state ~/.local/state/cli-agent-bridge serve`。改用自动更新后，服务、MCP 命令以及协调机 `hosts.macos-node` 的 relay 命令应统一改用固定启动器 `~/.local/share/cli-agent-bridge/bin/agent-bridge`。
- AGY MCP 登记使用已验证命令 `agy mcp add cli-agent-bridge ~/.local/share/cli-agent-bridge/bin/agent-bridge --state ~/.local/state/cli-agent-bridge mcp`，保留其他配置。
- 公开 GitHub Releases OTA 直接通过无认证 HTTPS 拉取经过校验的 wheel 与 `SHA256SUMS`，无需配置本地 GitHub CLI 凭据。
- 14:18 UTC 的迁移前只读检查中，原有 18 个 `paused/stopped` 会话和其 7 条 queued 任务保持原样，另有 1 个 `open/recovery_needed` 的旧 AGY 检查会话及其 queued 任务；所有在途发送记录为 0。首次迁移保留这些记录，不盲目执行旧队列。

没有升级底层 AGY、Codex 或其他 CLI，没有操作原有队列或工作进程。
