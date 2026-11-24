> [English](../uninstall.md) | 中文

# 卸载

服务登记和程序安装需要分别移除。检查后再处理任务历史和工作目录：写入会话中可能还有未应用到源项目的修改。项目没有 `agent-bridge uninstall` 命令。

下面使用默认状态目录、托管目录和服务名。使用过 `--state` 或 `AGENT_BRIDGE_STATE` 时，所有 Bridge 命令都应指定实际目录。安装过托管服务时，从 `<state>/update.json` 查看 `root`、`service_kind`、`service_name`，替换下面对应值。源码用户可以在仓库目录用 `uv run agent-bridge` 完成这些步骤。

## 1. 停止定时更新，结束现有工作

安装过更新定时器时，先停止它；这不会停止已经开始的更新：

Linux：

```bash
systemctl --user disable --now cli-agent-bridge-update.timer
systemctl --user status cli-agent-bridge-update.service
```

macOS：

```bash
launchctl print "gui/$(id -u)/com.cli-agent-bridge.update"
```

macOS 先检查更新 job 是否运行（例如输出中是否有 PID），等待结束后再卸载：

```bash
launchctl bootout "gui/$(id -u)/com.cli-agent-bridge.update"
```

Linux 等待一次性的更新服务结束。没有安装的服务跳过对应命令；不存在或未运行的服务可能返回非零状态。更新结果为 `uncertain` 时，先检查实际情况再继续。

关闭会话期间保持 broker 运行：

```bash
agent-bridge call agent_inspect --json '{}'
```

对每个需要停止的会话，先查询，再使用当前 `revision` 关闭。替换占位符和示例中的 `0`：

```bash
agent-bridge call agent_inspect --json '{"session_id":"<session_id>"}'
agent-bridge call agent_control --json '{"session_id":"<session_id>","action":"close","expected_revision":0,"request_id":"uninstall-close-1"}'
agent-bridge call agent_inspect --json '{"session_id":"<session_id>"}'
```

每个会话使用不同的 `request_id`。等待活动任务达到终态、各 worker 退出（`worker_alive=false`）。遇到 `recovery_needed` 要检查，不能当作已经停止。close 会取消排队任务并请求中断活动任务，保留历史与文件；它不会停止在 worker 管理的进程范围以外启动的工作，也不会停止远端 broker。需要时在远端所属节点关闭会话；移除主机配置不会卸载远端程序。

## 2. 停止 broker，移除用户服务文件

前台 broker 在运行终端按 Ctrl+C。服务安装按对应系统操作；先查看服务文件，确认属于本次安装。自定义服务名需修改命令。

Linux `systemd --user`（也适用于旧 `install_local.py`）：

```bash
systemctl --user cat cli-agent-bridge.service
# 如已安装 Web 用户服务：
systemctl --user disable --now cli-agent-bridge-web.service
rm -f "$HOME/.config/systemd/user/cli-agent-bridge-web.service"
systemctl --user disable --now cli-agent-bridge.service
rm -f "$HOME/.config/systemd/user/cli-agent-bridge.service"
rm -f "$HOME/.config/systemd/user/cli-agent-bridge-update.timer"
rm -f "$HOME/.config/systemd/user/cli-agent-bridge-update.service"
systemctl --user daemon-reload
```

没安装更新定时器时跳过对应文件，不要停止或删除无关用户服务。worker unit 独立运行，必须在关闭 broker 前完成上面的会话检查。

macOS `launchd`：

```bash
launchctl print "gui/$(id -u)/com.cli-agent-bridge.broker"
# 如已安装 Web 用户服务：
launchctl bootout "gui/$(id -u)/com.cli-agent-bridge.web"
rm -f "$HOME/Library/LaunchAgents/com.cli-agent-bridge.web.plist"
launchctl bootout "gui/$(id -u)/com.cli-agent-bridge.broker"
rm -f "$HOME/Library/LaunchAgents/com.cli-agent-bridge.broker.plist"
rm -f "$HOME/Library/LaunchAgents/com.cli-agent-bridge.update.plist"
```

更新 LaunchAgent 应已在第 1 步卸载。默认 broker 日志和任务记录仍保留在状态目录。

## 3. 移除 MCP 和配套 skill 登记

按快速入门登记的 Codex MCP：

```bash
codex mcp remove cli-agent-bridge
codex mcp list
```

用过 `agent-bridge integrate` 时，确认该命令输出的 skill 链接指向 Bridge 随包技能，再移除该链接。在其他配置过的 MCP 客户端中移除对应条目，再重载或退出客户端，让其 stdio MCP 进程退出。这不会删除模型 CLI 的登录凭据。

使用过 `scripts/install_local.py` 时，检查它创建的链接：

```bash
ls -ld "$HOME/.local/bin/agent-bridge" "$HOME/.codex/skills/cli-agent-bridge"
```

只有确认它们是指向本次源码目录的符号链接后，才移除：

```bash
unlink "$HOME/.local/bin/agent-bridge"
unlink "$HOME/.codex/skills/cli-agent-bridge"
```

实际文件或目录、属于其他安装的条目不要动。自行创建的其他 skill 链接也要先检查目标；托管安装可能指向 `<managed-root>/current/skill`。

## 4. 移除程序安装

通过 `uv tool install` 安装的程序（包括 Git 来源）：

```bash
uv tool list
uv tool uninstall cli-agent-bridge
```

通过源码目录 `uv sync` 安装时，没有全局包需要卸载。移除服务、MCP 登记和辅助脚本链接后，可以保留仓库和 `.venv`，或用平时的文件工具移除。先检查 `git status`，保存自己的修改。若曾通过 `uv tool install` 安装该源码目录，还需执行上面的 `uv tool uninstall`。

用一键脚本安装时，确认 `~/.local/bin/agent-bridge` 是指向 `~/.local/share/cli-agent-bridge/bin/agent-bridge` 的符号链接，再移除该链接。

托管服务和 OTA 默认另建 `~/.local/share/cli-agent-bridge`，含 `bin`、`current`、`releases`，与 uv 工具环境分开。确认所有服务、worker、更新进程、MCP 进程都已停止后，移除自己加入 shell 的托管 `bin` PATH 设置，再用文件工具将此目录移走或删除。自定义过 `root` 时使用实际目录。托管入口仍依赖安装时 Python 的情况下，不要先删除该 Python 环境。

## 5. 单独决定是否删除本地数据

| 位置 | 内容与默认处理 |
| --- | --- |
| `~/.local/state/cli-agent-bridge` 或自定义状态目录 | 配置、SQLite 历史、主机身份、私有结果、日志和会话工作目录：保留 |
| `~/.local/share/cli-agent-bridge` 或自定义托管目录 | 托管入口与发布版本：所有进程停止后可选移除 |
| 源码目录及 `.venv` | 源码修改与依赖：检查后再处理 |
| 引用的 `env_file`、模型 CLI 登录文件 | 独立凭据：保留；卸载 Bridge 不会撤销账号 |
| 远端安装 | 继续保留；确实要卸载时，在每台主机重复对应步骤 |

以后要删除历史时，停止所有使用该状态目录的进程，检查工作目录和结果文件，私下备份需要的内容。状态目录可能含提示词和凭据文件引用，不要作为 issue 附件上传。本文不提供直接删除运行数据的命令。

打开新 shell，检查 `command -v agent-bridge` 和 `uv tool list`。仍有命令时查看其位置，可能还有其他 uv 安装、源码辅助链接或托管入口。确认移除的服务已停止。只剩历史目录不代表仍有进程在运行。
