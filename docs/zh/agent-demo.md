> [English](../agent-demo.md) | 中文

# 在 Codex 对话里把任务交给 Pi

在这个示例中，你让 Codex 派一条只读任务给 Pi，稍后回到同一个对话看结果。Pi 在独立的 Bridge 会话中运行。派发任务会调用模型，可能产生费用。

## 准备 Codex 和 Pi

1. 用已登录的 Pi CLI 和可用的模型配置一个名为 `pi-demo` 的 Bridge profile，再用这份配置启动 broker。已有配置时，把示例中的配置名替换成自己的即可。见 [配置](getting-started.md#配置)。
2. 给 Codex 注册 Bridge MCP，然后在一个带 README 的项目中打开新的 Codex 对话。见 [MCP 设置](getting-started.md#mcp-setup)。

## 派发任务

把这段话发给 Codex：

> 用 Agent Bridge 的 MCP 工具。先用 `agent_catalog` 确认 `pi-demo` 配置，再在当前项目里派一条只读任务给它。请 Pi 读 README，总结安装和运行测试的命令，并注明引用了 README 的哪些章节。不要修改项目文件。任务入队后就把任务 ID 和会话 ID 告诉我，我们可以继续用这个对话。

Codex 调用 `agent_catalog(profile="pi-demo")` 查找 Pi 配置，再调用 `agent_dispatch`，传入配置名、项目绝对路径、`access="read-only"`、任务说明和唯一的 `request_id`。Pi 完成前，派发调用就会返回。如果要重试同一次提交，Codex 会沿用原来的参数和 `request_id`。

## 查看结果

想看结果时，在同一个 Codex 对话里发送：

> 查一下刚才派给 Pi 的任务。如果完成了，请读取回答，对照 README 核对命令，然后告诉我核对结果。如果还在排队或运行，告诉我当前状态，保留这条任务。

Codex 会用保存的任务 ID 调用 `agent_result(task_id=...)`。如果任务需要输入或恢复，它可以检查详情并告诉你需要处理什么。后续 Bridge 任务依赖这份答案时，Codex 需要先核对已完成的结果，再调用 `agent_review` 记录核对情况。见 [结果与检查](getting-started.md#results-and-review)。

全新安装默认不会自动通知主会话。配置并验证[父会话通知辅助程序](getting-started.md#parent-sessions-and-telegram)后，Codex 可以在收到完成事件时读取结果。示例结束后，可以让 Codex 按保存的会话 ID 关闭这条 Bridge 会话。
