> 语言：[English](../../CONTRIBUTING.md) | 中文

# 参与贡献

欢迎按 [Apache-2.0](../../LICENSE) 许可贡献修改。贡献内容应为你自己的作品，或已取得兼容的使用许可；保留第三方要求的声明。

## 开发

安装 Python 3.11+ 和 uv，然后执行：

```sh
uv sync --locked
uv run ruff check src tests scripts skills/cli-agent-bridge/scripts
uv run python -m unittest discover -s tests -v
uv build
```

测试使用临时状态目录和模拟 agent CLI。普通测试不得运行真实模型 smoke 脚本、安装服务、向原生会话广播，或使用他人的运行数据库。此类检查需要明确选择目标并取得授权，也可能产生供应商费用。

每次修改只处理相关问题。修复已确认缺陷时，补充能重现问题的测试；涉及相关行为时，检查幂等重试、取消、状态保存和失败处理。面向用户的变化应同时更新英文文档及对应的 `docs/zh/` 中文文档。示例使用虚构的主机名、路径、标识和数据。不要提交凭据、数据库、原始聊天、私人电脑路径或录屏。

PR 应说明具体问题、修改后的行为、实际执行的检查和未验证项。UI 修改的截图应使用虚构数据。漏洞请按 [安全说明](SECURITY.md) 私密报告。
