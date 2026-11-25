> [English](../cli.md) | 中文

# 命令行指南

在终端安装 Bridge、接入 agent，以及派发和管理任务。

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

### 用 init 配置 Bridge

在终端运行 `agent-bridge init` 打开配置菜单。选 **6) 高级设置** 可设置默认语言、路由、通知路径、OTA 镜像和托管 Web。运行 `agent-bridge init --help` 查看可用于脚本的参数。非交互模式只修改传入的设置。

| 设置 | 参数 |
| --- | --- |
| 运行配置，包括模型、思考强度、配额、工具和价格 | `--profile SPEC`、`--remove-profile NAME`。可用 JSON 对象填写数组和其他结构化字段。 |
| 并发、超时、分配策略和 token 预算 | `--worker-launcher`、`--parallel-limit`、`--quota-limit`、`--task-timeout`、`--interrupt-timeout`、`--default-strategy`、`--token-budget` |
| 项目和远端节点 | `--project ID=PATH`、`--remove-project ID`、`--host SPEC`、`--remove-host ID`。节点的 SSH 参数和事件镜像等字段可用 JSON。 |
| 默认语言与路由规则 | `--config-language en\|zh-CN\|auto\|none`、`--routing-file PATH`、`--clear-routing` |
| 父会话通知 | `--codex-async-python PATH`、`--telegram-database PATH`。用空字符串清除对应路径。 |
| Broker 服务和更新 | `--service-kind`、`--service-name`、`--install-broker-service`、`--enable-broker-service`、`--force-broker-service`、`--ota-repo`、`--ota-root`、`--ota-interval`、`--ota-release-url`、`--install-updater`、`--enable-updater` |
| 托管 Web | `--install-web` 写入服务文件，`--enable-web` 同时启动。依赖托管更新设置，可在同一条命令里一并配置。 |

例如，把默认语言设为中文，并从文件加载[路由规则](getting-started.md#off-peak-routing)。

```bash
agent-bridge init --non-interactive --config-language zh-CN --routing-file routing.json
```

文件内容为包含 `calendars`、`windows` 和 `preferences` 的路由对象。导入会整体替换路由规则，`--clear-routing` 用于清除。用 `--ota-release-url URL` 设置镜像，用 `--ota-release-url ''` 切回 GitHub。OTA 参数只准备更新程序所需文件；要启动服务或启用定时器，请加上对应的 `--enable-*` 参数或在菜单里开启。

CLI 帮助、配置向导、终端监控、Web 页面和父任务通知支持英文与简体中文。运行 `agent-bridge --lang zh-CN --help` 查看中文帮助；Web 仪表盘和登录页可单独选择语言，见[语言设置](getting-started.md#language)。

想用 Web 仪表盘，可以在安装时传入 `--with-web`，把 Web 装成用户服务并随 OTA 一起重启。也可在另一个终端运行 `agent-bridge monitor --web`，打开[本机登录页](http://127.0.0.1:8080/login)，使用 Bridge 状态目录下 `admin.token` 中的令牌登录。见 [Web 设置](getting-started.md#监控与接入接口)。

## 接入 MCP

运行 `agent-bridge integrate list` 查看支持的客户端和安装命令。

执行对应客户端的那一行：

```bash
agent-bridge integrate codex
agent-bridge integrate claude
agent-bridge integrate pi
agent-bridge integrate agy
```

新建客户端会话以加载 MCP 工具和 skill。其他 MCP 客户端将命令设为 `agent-bridge` 的绝对路径，参数设为 `["mcp"]`。broker 需要保持运行。Pi 安装、自定义状态目录和托管入口见 [MCP 设置](getting-started.md#mcp-setup)。

## 派发第一个任务

命令行与 MCP 使用相同工具。把 `/absolute/path/to/project` 替换为真实项目目录；配置名不叫 `local` 时也要修改。下面会实际调用模型，可能产生费用：

```bash
agent-bridge call agent_dispatch --json '{"profile":"local","cwd":"/absolute/path/to/project","access":"read-only","instruction":"Read the project README and summarize how to run it. Do not modify files.","request_id":"quickstart-task-1"}'
```

保存返回的 `task_id` 和 `session_id`。相同参数与 `request_id` 重试会返回同一个任务；每个新任务使用新的 ID。

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

## 其他命令行操作

运行 `agent-bridge --help` 查看命令列表。把 `--help` 加在子命令后面可以查看它的参数。

| 操作 | 命令 | 详细说明 |
| --- | --- | --- |
| 查配置和模型 | `agent-bridge call agent_catalog --json '{}'` | [模型发现](getting-started.md#autonomous-model-discovery-and-selection) |
| 查看分组 | `agent-bridge call agent_groups --json '{}'` | [分组与广播](getting-started.md#分组与广播) |
| 查看已配置节点 | `agent-bridge call agent_hosts --json '{}'` | [远端主机](getting-started.md#远端主机) |
| 检查或维护镜像事件 | `agent-bridge mirrors --help` | [远端事件记录](dynamic-hosts.md) |
| 检查新版 | `agent-bridge update check` | [发布与定时更新](getting-started.md#github-发布与定时更新) |

`agent-bridge call` 可用的方法见[工具接口](getting-started.md#tool-interface)。移除 Bridge 见[卸载指南](uninstall.md)。
