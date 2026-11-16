> [English](../../README.md) | 中文

# cli-agent-bridge

通过 MCP 或命令行运行 Codex、Claude Code、Pi 和 Antigravity 的独立会话。可以派发任务、继续追问、查看结果和停止会话；任务历史保存在 SQLite，MCP 客户端断开后 worker 仍可继续运行。

适合单个用户在自己的电脑或受信任的 SSH 主机上使用。需要 Python 3.11+、[uv](https://docs.astral.sh/uv/getting-started/installation/) 和至少一个已安装、已登录的模型 CLI。支持 Linux、macOS；Windows 请使用 WSL2。模型调用使用你的 CLI 账号，可能产生费用。worktree 只能分开文件修改，不提供操作系统隔离。

## 安装

直接从仓库安装，无需 PyPI 包：

```bash
uv tool install 'git+https://github.com/qinjian623/cli-agent-bridge.git'
agent-bridge init
agent-bridge doctor
```

找不到 `agent-bridge` 时，运行 `uv tool update-shell` 并打开新终端。在 `init` 中选择 **1) Profiles**，再选 **a**，添加一个名为 `local` 的配置，填写已登录的 Codex、Claude Code 或 Pi CLI 及其完整模型 ID；用 **b** 返回，再用 **s** 保存。首次使用保留远端主机和服务设置为空。新配置尚未经过实际推理检查，但可以通过明确指定 `local` 派发首个任务。Antigravity 需要额外设置及写入权限，见[详细说明](getting-started.md#antigravity)。

## 启动

在一个终端中保持运行：

```bash
agent-bridge serve
```

在另一个终端中检查服务：

```bash
agent-bridge call agent_inspect --json '{}'
```

需要后台启动时，见[托管服务](getting-started.md#managed-services)。

## 接入 MCP

Codex 用户在第二个终端执行：

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

## 查看结果

把尖括号占位符替换为返回的 ID：

```bash
agent-bridge call agent_result --json '{"task_id":"<task_id>"}'
agent-bridge monitor
```

`queued`、`running` 表示任务尚未结束；`waiting_input` 需要答复；`recovery_needed` 需要检查实际情况。`completed` 表示模型已结束回答。检查回答、文件与测试结果后，才能用 `agent_review` 接受任务或开始依赖它的工作。[详细说明](getting-started.md#results-and-review)介绍了检查步骤和完整结果读取。

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

[配置与故障排查](getting-started.md) · [卸载](uninstall.md) · [设计](design.md) · [参与贡献](CONTRIBUTING.md) · [安全](SECURITY.md) · [检查与限制](production-readiness.md) · [隐私](privacy.md) · [变更](CHANGELOG.md)

采用 [Apache-2.0](../../LICENSE) 许可证。
