> 语言：[English](../install-verification.md) | 中文

# 隔离安装与首个任务检查

2026-11-17（UTC 2026-11-16 18:07–18:08），在 Linux x86-64、Python 3.13.15、uv 0.13.8 上，将当时的候选 `0.3.0` wheel 安装到全新的 HOME、XDG、uv 工具、状态和项目目录，再按新用户步骤使用。模型执行部分采用 `tests/fixtures/fake_codex.py` 的未修改副本，没有使用真实模型、CLI 账号、共享 Codex daemon、已有 Bridge 状态或已安装的用户服务。

17 项检查全部通过，其中包含 12 项无效输入。此前候选包曾因未知 profile 字段输出 Python traceback；本次重新安装后，确认返回清楚且不回显输入名称的错误，原配置保持不变。

## 隔离方式与包内容

所有临时文件和详细报告都留在源码目录以外的私有维护目录。`TMPDIR`、HOME、XDG 目录、`UV_TOOL_DIR`、`UV_TOOL_BIN_DIR`、`AGENT_BRIDGE_STATE` 均指向该处。通过硬链接复用已有 uv 包缓存，没有复制认证数据。构建与安装均使用 `--offline`，并禁止下载 Python。

临时 `codex` 包装命令只通过已安装工具的 Python 启动仓库 RPC fixture；profile 也明确指定该包装命令。临时 `systemctl` 命令返回不可用，保存的 worker 启动方式为 `detached`。没有向真实客户端注册 MCP；独立 stdio MCP 客户端完成初始化、工具列表和 fixture 模型目录查询。

wheel 与源码分发包都包含 Apache-2.0 元数据、LICENSE、NOTICE 和配套 skill。wheel 共 46 个条目，其中三个为 skill 文件。归档内每个文本文件均通过仓库检查脚本，未命中所查私人目录、凭据、邮箱或会话标识格式，也未发现禁止公开的运行文件。格式匹配不能代替完整隐私检查。新 uv 工具安装中的 CLI 帮助可以正常运行。

实测 wheel 的 SHA256 为 `df3f7cfcf364f91b4e815987aceb9d91627dc1d7e024234d84eb7aee959aa0d4`。前几轮安装结果另行保留；本文描述最后一轮代码快照。之后重新构建源码分发包可以加入文档更新，无需改变本次测试的 wheel 代码。

## 命令与首个任务

以下使用说明变量，不公开本次私有目录或随机生成的真实 ID。`SMOKE_ROOT` 代表源码目录以外、全新且较短的私有目录；`PYTHON` 代表已安装的 Python 解释器。从仓库根目录运行，并使用上文说明的隔离环境。`local` profile 必须指向 fixture 包装命令，不能指向已登录的真实模型 CLI。

```sh
uv build --offline --python "$PYTHON" --out-dir "$SMOKE_ROOT/dist" --no-create-gitignore
uv tool install --offline --python "$PYTHON" "$SMOKE_ROOT/dist/cli_agent_bridge-0.3.0-py3-none-any.whl"
agent-bridge --help
agent-bridge init
agent-bridge init --non-interactive
agent-bridge init --non-interactive
agent-bridge doctor
agent-bridge serve
```

交互 `init` 在真实伪终端中执行：Profiles → add → 名称 `local` → 程序 `codex` → 供应商 `openai-codex` → 模型 `fixed-model` → 默认额度组 → 不设 effort → 临时 fixture 可执行文件 → 不设环境文件 → 返回 → 保存。远程主机与服务均未设置。之后两次初始化均未改变配置文件字节或 profile 的未验证标记。

broker 就绪后，独立 MCP 客户端发现 20 个工具，包括原有九个任务和会话工具。`agent_catalog(profile="local", refresh=true)` 返回含 `fixed-model` 的 fixture 目录。

CLI 向临时项目提交了以下模拟首任务：

```sh
agent-bridge call agent_dispatch --json '{"profile":"local","cwd":"/example/project","access":"read-only","instruction":"Read the project README and summarize how to run it. Do not modify files.","request_id":"quickstart-task-1"}'
agent-bridge call agent_result --json '{"task_id":"<task_id>"}'
```

完全相同的派单请求重复提交后，两个响应仍指向同一任务和会话。Python 内的限时等待读取结果直到完成；数据库中只有一个任务、一个 attempt，fixture 只执行一次。检查到预期的 `fixture complete` 结果。临时 README 未变；fixture 自己生成的 `invocations.txt` 记录了一次模拟轮次。

随后使用实际返回的当前 revision 审查结果并关闭会话：

```sh
agent-bridge call agent_review --json '{"task_id":"<task_id>","attempt_id":"<attempt_id>","decision":"accepted","evidence":"Checked the expected offline fixture result and its single invocation; real model inference was not tested.","expected_revision":0,"request_id":"quickstart-review-1"}'
agent-bridge call agent_inspect --json '{"session_id":"<session_id>"}'
agent-bridge call agent_control --json '{"session_id":"<session_id>","action":"close","expected_revision":0,"request_id":"quickstart-close-1"}'
```

示例中的零是占位值；本次使用结果和会话实际返回的当前 revision。确认 `desired=closed`、`worker_alive=false` 后，仅停止并重新启动隔离的前台 broker。原任务仍为 completed、accepted，会话仍关闭，fixture 没有再次执行。

## 错误、权限、卸载与进程退出

| 检查 | 结果 |
| --- | --- |
| 首次无配置的非交互初始化 | 非零退出，并提示运行交互 init 或提供 profile 参数 |
| `--parallel-limit 0` | 清楚的非零错误；原配置未变 |
| `call` 的 JSON 无效 | 清楚的非零错误；未增加任务 |
| 派单指定不存在的 profile | 清楚的非零错误；未增加任务或会话 |
| profile JSON 含未知字段 | 清楚的非零错误；不回显输入的 profile 名称或字段名；原配置未变 |
| profile 字段类型无效 | 拒绝字符串 `verified`、列表 `executable`、字符串 `tools`；无 traceback，不回显输入名称，原配置未变 |
| 已保存配置无效 | 拒绝损坏 JSON、小数并发上限、布尔超时、未知顶层字段及错误 profile 标记；`doctor` 未改写待检查文件 |
| profile 环境文件 | 拒绝 0644；接受当前用户的 0600 JSON 字符串字典；拒绝非字符串值；shell 文本未执行；子进程环境移除了父会话标识和管理员令牌 |
| 状态目录权限 | 目录 0700；配置、管理员令牌和 SQLite 数据库均属当前用户、权限 0600 |
| 服务安装 | 未安装用户服务或 LaunchAgent |
| `uv tool uninstall cli-agent-bridge` | 工具命令和环境删除；配置与数据库保留，文件哈希未变 |
| 进程退出 | 记录的 44 个进程全部退出，包括两个 broker、一个 worker、两个 fixture 进程及 MCP 服务端和客户端；无存活进程，无需强制清理 |

此前失败的输入现已返回 `invalid_config` 及清楚的说明：

```sh
agent-bridge init --non-interactive --profile '{"name":"bad-example","runtime":"codex","model":"fixed-model","unknown_option":"example"}'
```

12 项无效输入命令均非零退出，无 Python traceback。另以模拟敏感内容作为 profile 名称和未知字段名，确认输出没有回显；原配置字节保持不变，没有增加任务或会话。完整复现日志、包哈希、命令、进程标识及模拟状态均保留在私有目录。本次对打包源码与构建输入计算了哈希，检查期间未发生变化。

本次安装检查未测试：macOS、Windows/WSL、Python 3.11/3.14、GitHub 安装或下载、真实模型推理或认证、原生文件系统只读限制、托管服务、远程主机或实际升级。本次明确指定了较短的状态目录，没有测试默认状态位置。fixture 完成证明安装后的 CLI、MCP、队列和状态保存可以工作，不能证明供应商或模型表现。
