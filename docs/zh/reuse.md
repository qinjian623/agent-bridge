> 语言：[English](../reuse.md) | 中文

# 现有 skill 和代码的处理

2026-10-22 检查了本机安装，保留现有服务和凭据。

| 现有项目或 skill | 本次处理 |
| --- | --- |
| `codex-async` | `codex_parent.py` 按需使用已安装环境的 `CodexAppServer`，包括原生队列 ID、共享 daemon、占用检查及保留配置的恢复；未另写定时任务服务 |
| `codex-telegram` | 保留话题、接收队列、最终回复和附件发送；补外部子会话登记过滤，防止测试子会话单独发消息。不复制 bot 密钥，不另建接收循环 |
| 现有 Claude 调用代码 | 保留准确 UUID、继续原会话和原生认证的做法；新项目需要流式中断及审批回调，因此采用官方 SDK，并固定本机 CLI 路径 |
| 现有 Pi RPC/extension | 保留原生 session 文件和 `agent_settled` 结束判断；新建后台子会话禁用 extension，已打开终端仍由原 extension 管理 |
| `task-learning` | 检索相关经验；有新检查结果时保存短记录，不复制原始聊天和凭据 |
| `skill-creator` | 新建小型 `cli-agent-bridge` skill，说明本机入口、恢复要求和检查结果，不重复已有 Telegram 安装步骤 |
| CLI-Subagents、PAL clink、codex-agy-bridge | 采用已评审的真实 CLI 会话、简单 MCP 工具、独立后台进程设计；本次没有复制第三方源码或安装三套服务 |

现有配套代码整合检查在隔离环境中完成，独立于外部仓库和共享 Codex daemon。

新增 `telegram_guard.py` 读取暂停、结果不明和话题删除标记，并登记子会话 ID。配置 `telegram_database` 时，旧 bot 在入队和发送前检查登记表；已同步适配到对应 `codex-async` / `codex-telegram` skill 的 runtime 副本，当时既有 runtime 的 107 项检查通过。确认没有运行或排队任务后，重载了一次 Telegram 服务；未重启共享 Codex daemon。

冷恢复实测后，父会话调用改为原生 `request("thread/resume", ...)`：现有 `thread(..., preserve_config=True)` 辅助函数仍会设置 `approvalPolicy=never`，不适用于保留父权限。临时 MCP 设置通过受限的私有配置文件引用恢复；模型、推理强度和批准策略不覆盖。已在同一 Astra 原生会话中完成 Sol、Pi、Claude 三个结果的检查和通知确认。
