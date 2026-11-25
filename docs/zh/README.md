> [English](../../README.md) | 中文

# cli-agent-bridge

通过 MCP 或命令行把任务交给 Codex、Claude Code、Pi 或 Antigravity。每个 worker 在独立会话里工作，客户端断开后也能继续。任务历史保存在 SQLite。

Bridge 适合单个用户在自己的电脑或受信任的 SSH 主机上使用。支持 Linux 和 macOS，Windows 上请使用 WSL2。worker 使用当前系统账号的权限，通过你已登录的 CLI 调用模型，可能产生费用。Git worktree 会分开保存文件修改。

## 一键安装

Linux 需要 `systemd --user`，macOS 需要登录桌面。

```bash
curl -fsSL https://github.com/qinjian623/agent-bridge/releases/latest/download/install.sh | sh
```

安装后会启动 Bridge 并启用自动更新。如果找不到 `agent-bridge`，把 `$HOME/.local/bin` 加入 `PATH`。

## 快速 init

```bash
agent-bridge init
```

选择 **1) Profiles（运行配置）**，按 **a** 给已登录的模型 CLI 添加一份配置。要跑下面的示例，选 Pi，再填供应商和完整模型 ID。按 **b** 返回，按 **s** 保存。[重启 Broker 加载新配置](getting-started.md#managed-services)。

## 配置 agent

以 Codex 为例，用下面的命令安装 Bridge Skill 和 MCP：

```bash
agent-bridge integrate codex
```

Claude Code、Pi 或 Antigravity 分别用 `claude`、`pi` 或 `agy`。运行 `agent-bridge integrate list` 可以查看支持的选项。接入后打开一个新的 agent 对话。

## 在 agent 对话里派发任务

在已加载 Bridge MCP 和 skill 的 agent 里打开项目。配好 Pi 后，直接说：

> 用 Bridge 让 Pi 看下这次改动，有没有问题。

想看结果时，在同一个对话里问“Pi 看完了么？”配置步骤和 agent 会做的事见[完整对话示例](agent-demo.md)。

[命令行指南](cli.md) · [init 参数说明](cli.md#用-init-配置-bridge)

[配置与故障排查](getting-started.md) · [远程节点与事件记录](dynamic-hosts.md) · [卸载](uninstall.md) · [设计](design.md) · [参与贡献](CONTRIBUTING.md) · [安全](SECURITY.md) · [检查与限制](production-readiness.md) · [隐私](privacy.md) · [变更](CHANGELOG.md)

采用 [Apache-2.0](../../LICENSE) 许可证。
