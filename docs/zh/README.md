> [English](../../README.md) | 中文

# cli-agent-bridge

通过 MCP 或命令行运行 Codex、Claude Code、Pi 和 Antigravity 的独立会话。可以派发任务、继续追问、查看结果和停止会话；任务历史保存在 SQLite，MCP 客户端断开后 worker 仍可继续运行。

适合单个用户在自己的电脑或受信任的 SSH 主机上使用。支持 Linux 和 macOS，Windows 上请使用 WSL2。模型调用使用你登录的 CLI 账号，可能产生费用。worktree 将文件修改分开，仍使用同一个系统账号的权限。

## 命令行指南

安装 Bridge、接入 MCP 和 skill，或在终端派发和管理任务，请看[命令行指南](cli.md)。

配置菜单和脚本参数见[用 init 配置 Bridge](cli.md#用-init-配置-bridge)。

## 在 agent 对话里派发任务

在已加载 Bridge MCP 和 skill 的 agent 里打开项目。配好 Pi 后，直接说：

> 用 Bridge 让 Pi 看下这次改动，有没有问题。

想看结果时，在同一个对话里问“Pi 看完了么？”配置步骤和 agent 会做的事见[完整对话示例](agent-demo.md)。

[配置与故障排查](getting-started.md) · [远程节点与事件记录](dynamic-hosts.md) · [卸载](uninstall.md) · [设计](design.md) · [参与贡献](CONTRIBUTING.md) · [安全](SECURITY.md) · [检查与限制](production-readiness.md) · [隐私](privacy.md) · [变更](CHANGELOG.md)

采用 [Apache-2.0](../../LICENSE) 许可证。
