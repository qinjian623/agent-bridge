> [English](../../README.md) | 中文

# cli-agent-bridge

通过 MCP 或命令行运行 Codex、Claude Code、Pi 和 Antigravity 的独立会话。可以派发任务、继续追问、查看结果和停止会话；任务历史保存在 SQLite，MCP 客户端断开后 worker 仍可继续运行。

适合单个用户在自己的电脑或受信任的 SSH 主机上使用。支持 Linux 和 macOS，Windows 上请使用 WSL2。模型调用使用你登录的 CLI 账号，可能产生费用。worktree 将文件修改分开，仍使用同一个系统账号的权限。

## 安装

在支持 `systemd --user` 的 Linux 上，或登录 macOS 桌面后，运行以下命令安装最新稳定版并启用自动更新。

```bash
curl -fsSL https://github.com/qinjian623/agent-bridge/releases/latest/download/install.sh | sh
```

脚本会校验 wheel，按需安装 uv 和 Python，并用当前账号启动 broker 和更新定时器。如果找不到 `agent-bridge`，把 `$HOME/.local/bin` 加入 `PATH`，或直接运行 `~/.local/bin/agent-bridge`。源码安装、暂不启动服务和迁移已有安装的步骤见[安装选项](getting-started.md#环境与安装)。

## 配置与检查

请单独安装并登录模型 CLI。运行 `agent-bridge init`，选择 **1) Profiles**，再选 **a** 添加名为 `local` 的配置。填写已登录的 Codex、Claude Code 或 Pi CLI 及其完整模型 ID，用 **b** 返回，再用 **s** 保存。新配置尚未通过推理检查，首次派发时可以明确指定 `local`。Antigravity 的权限要求见[配置说明](getting-started.md#antigravity)。

保存 profile 后，重启 broker 加载配置，再检查连接。Linux 和 macOS 的命令见[托管服务](getting-started.md#managed-services)。独立源码安装用户可在另一个终端运行 `agent-bridge serve`。

```bash
agent-bridge call agent_inspect --json '{}'
```

CLI 帮助、配置向导、终端监控、Web 页面和父任务通知支持英文与简体中文。运行 `agent-bridge --lang zh-CN --help` 查看中文帮助；Web 仪表盘和登录页可单独选择语言，见[语言设置](getting-started.md#language)。

需要查看 Web 仪表盘时，在另一个终端运行 `agent-bridge monitor --web`，打开[本机登录页](http://127.0.0.1:8080/login)，使用 Bridge 状态目录下 `admin.token` 中的令牌登录。见 [Web 设置](getting-started.md#监控与接入接口)。

## 接入 MCP

Codex 用户执行：

```bash
codex mcp add cli-agent-bridge -- "$(command -v agent-bridge)" mcp
codex mcp list
```

新建一个 Codex 会话以加载工具。其他 MCP 客户端将命令设为 `agent-bridge` 的绝对路径，参数设为 `["mcp"]`。broker 需要保持运行。自定义状态目录和托管入口见 [MCP 设置](getting-started.md#mcp-setup)。

## 派发第一个任务

命令行与 MCP 使用相同工具。把 `/absolute/path/to/project` 替换为真实项目目录；配置名不叫 `local` 时也要修改。下面会实际调用模型，可能产生费用：

```bash
agent-bridge call agent_dispatch --json '{"profile":"local","cwd":"/absolute/path/to/project","access":"read-only","instruction":"Read the project README and summarize how to run it. Do not modify files.","request_id":"quickstart-task-1"}'
```

保存返回的 `task_id` 和 `session_id`。相同参数与 `request_id` 重试会返回同一个任务；每个新任务使用新的 ID。通过 MCP 使用时，可以让助手先调用 `agent_catalog`，再用 `profile="local"` 和真实项目目录派发同一条只读任务。

本机新任务自动选择配置时，可使用[低峰时段规则](getting-started.md#off-peak-routing)。

## 查看结果

把尖括号占位符替换为返回的 ID：

```bash
agent-bridge call agent_result --json '{"task_id":"<task_id>"}'
agent-bridge monitor
```

`queued`、`running` 表示任务尚未结束；`waiting_input` 需要答复；`recovery_needed` 需要检查实际情况。`completed` 表示模型已结束回答。检查回答、文件与测试结果后，才能用 `agent_review` 接受任务或开始依赖它的工作。[详细说明](getting-started.md#results-and-review)介绍了检查步骤和完整结果读取。

新安装可用 `agent_result` 或监控页面查看进展。自动发送完成消息需要配置[父会话通知辅助程序](getting-started.md#parent-sessions-and-telegram)。

## 停止任务或会话

只取消一个任务：

```bash
agent-bridge call agent_cancel --json '{"task_id":"<task_id>","request_id":"quickstart-cancel-1"}'
```

关闭会话并取消尚未执行的任务时，先查询会话，再把下面的 `0` 替换为返回的**当前 `revision`**：

```bash
agent-bridge call agent_inspect --json '{"session_id":"<session_id>"}'
agent-bridge call agent_control --json '{"session_id":"<session_id>","action":"close","expected_revision":0,"request_id":"quickstart-close-1"}'
```

再次查询，确认该会话的 worker 已退出。在 broker 终端按 Ctrl+C 可以停止 broker；独立 worker 需要单独取消任务或关闭会话。关闭会话会保留历史与工作目录。

[配置与故障排查](getting-started.md) · [远程节点与事件记录](dynamic-hosts.md) · [卸载](uninstall.md) · [设计](design.md) · [参与贡献](CONTRIBUTING.md) · [安全](SECURITY.md) · [检查与限制](production-readiness.md) · [隐私](privacy.md) · [变更](CHANGELOG.md)

采用 [Apache-2.0](../../LICENSE) 许可证。
