> [English](../agent-demo.md) | 中文

# 在 Codex 对话里把任务交给 Pi

Pi 在独立的 Bridge 会话里检查你改过的代码。稍后回到同一个 Codex 对话就能问结果。派发任务会调用模型，可能产生费用。

## 准备 Codex 和 Pi

1. 用已登录的 Pi CLI 和可用的模型配置 Bridge，然后启动 broker。见[配置与检查](cli.md#配置与检查)。
2. 给 Codex 安装 Bridge MCP 和 skill，再在有本地改动的 Git 仓库里打开一个新的 Codex 对话。见 [MCP 接入](cli.md#接入-mcp)。

## 派发任务

把这段话发给 Codex：

> 用 Bridge 让 Pi 看下这次改动，有没有问题。

Codex 用 `agent_catalog` 找到 Pi 配置，再用 `agent_dispatch` 派一条只读检查任务。项目目录和请求 ID 由 Codex 填写，任务 ID 和会话 ID 也由它记住。任务入队后就可以继续聊天。

## 查看结果

想看结果时，在同一个 Codex 对话里发送：

> Pi 看完了么？

Codex 用保存的任务 ID 调用 `agent_result`。还在跑就告诉你进度，完成后会对照代码改动核对 Pi 的发现，再给你总结。如果还有 Bridge 任务依赖这次检查，Codex 会用 `agent_review` 记录核对结果。见 [结果与检查](getting-started.md#results-and-review)。

全新安装默认不会自动通知主会话。配置并验证[父会话通知辅助程序](getting-started.md#parent-sessions-and-telegram)后，Codex 可以在收到完成事件时读取结果。示例结束后，可以让 Codex 按保存的会话 ID 关闭这条 Bridge 会话。
