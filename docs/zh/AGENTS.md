> 语言：[English](../../AGENTS.md) | 中文

# Repository Guidelines

## 项目结构

- `src/agent_bridge/`：Python 源码。`cli.py` 和 `mcp_server.py` 提供入口，`broker.py` 管理请求，`worker.py` 执行任务，`init_config.py` 提供交互与非交互配置，`updater.py` 负责 GitHub Releases OTA 更新，`store.py` 保存 SQLite 数据。
- `tests/`：自动测试；`tests/fixtures/` 提供模拟 Codex、Antigravity CLI。
- `scripts/`：本机安装、smoke 检查、基准测试和任务记录工具。
- `docs/`：英文设计说明与检查记录；`docs/zh/`：中文说明与对应文档；`skills/cli-agent-bridge/`：配套 skill 和提示词资源。

## 安装、运行与检查

需要 Python 3.11+ 和 `uv`；运行本机托管服务另需 Linux 用户级 `systemd`（或 macOS `launchd`），调用真实模型前应先登录对应 CLI。

```bash
uv sync --locked                         # 按 uv.lock 安装依赖
uv run agent-bridge init                 # 交互式初始化或修改本机配置
uv run agent-bridge doctor               # 检查配置与运行环境
uv run agent-bridge serve                # 前台运行 broker
uv run python -m unittest discover -s tests -v
uv run ruff check src tests scripts
```

后两条命令分别运行完整自动测试和静态检查。从仓库根目录执行命令；修改依赖时同步更新 `pyproject.toml` 与 `uv.lock`。

## 代码风格与命名

使用四空格缩进。模块、函数和变量采用 `snake_case`，类采用 `PascalCase`；保持现有类型标注和错误处理写法。Ruff 配置位于 `pyproject.toml`，目标版本为 Python 3.11，行长参考值为 120，未启用行长错误检查。沿用已有模块中的函数，不要为一次性用途额外加包装函数。

## 测试要求

使用 `unittest.TestCase`；异步测试使用 `unittest.IsolatedAsyncioTestCase`。文件命名为 `tests/test_<module>.py`，方法命名为 `test_<behavior>`。单独检查示例：

```bash
uv run python -m unittest discover -s tests -p 'test_store.py' -v
```

修复缺陷时补充能重现问题的测试。涉及请求重试、并发或进程退出时，检查幂等性和状态保存行为。使用临时状态目录、模拟 CLI 与 mock；项目未设置覆盖率百分比要求。真实 CLI smoke 可能产生费用或影响真实会话，运行前核实脚本行为和目标。

## 提交与 Pull Request

现有提交使用简短的英文祈使句，例如 `Add session groups and broadcast to bridge and native sessions`；沿用 `Add …`、`Fix …` 等写法。一次提交只包含相关修改。

PR 说明具体问题、修改后的行为、执行过的检查及结果；有相关 issue 时附链接。接口、配置或命令变化应更新 README 和对应文档。区分模拟测试结果与真实 CLI 检查结果。

## 配置与私有数据

默认状态目录为 `~/.local/state/cli-agent-bridge`；隔离检查可使用 `agent-bridge --state /tmp/bridge-check …`。不要提交密钥、认证文件、运行数据库、原始聊天记录或屏幕录制。配置引用的 `env_file` 应仅属主可读写（`0600`）。安装脚本会创建命令链接和用户服务；修改代码后不要顺手重启现有服务或向真实会话发送消息。
