> [English](../production-readiness.md) | 中文

# 检查及使用限制

Bridge 面向单个操作系统用户及明确受信任的 SSH 节点，不提供多租户访问控制，也不能隔离同一用户身份下的恶意程序。见[安全说明](SECURITY.md)。

## 自动检查

[CI](../../.github/workflows/ci.yml)在推送、Pull Request 和手动触发时运行：

| 检查 | 内容 |
| --- | --- |
| Linux / Python 3.11、3.13、3.14 | 锁定的依赖、Ruff、公开内容模式检查和完整离线测试。 |
| Linux 和 macOS 打包 | 构建 wheel 和源码包，在新环境安装，检查命令入口与初始化，启动临时 broker 验证版本并通过 Unix socket 完成一次 MCP 请求。 |

测试使用临时数据、模拟 CLI、真实 stdio/Unix socket 和独立进程，覆盖请求重试、取消、保存与恢复、权限、文件传输、节点消息、监控及更新失败处理，不调用模型供应商。

本机可运行相同的源码检查：

```sh
uv sync --locked
uv run ruff check src tests scripts skills/cli-agent-bridge/scripts
uv run python scripts/check_public_content.py
uv run python -m unittest discover -s tests -v
```

[隔离安装记录](install-verification.md)说明了一次使用模拟 CLI 的候选版本检查，不证明当前原生账号、生产服务或主机迁移已通过。

## 普通 CI 未覆盖的检查

- 原生登录、供应商可用性、推理、工具执行和实时中断，需要使用指定 CLI 账号及模型验证。模型发现不代表这些检查通过。
- macOS CI 检查软件包安装，不安装或重启真实 launchd 服务。实际服务回滚、SSH 断开重连和断电恢复需要在目标环境单独检查。
- HTTP 和 Web 测试不能证明浏览器排版与交互正常，需要在真实浏览器查看页面。
- 生产 SQLite 数据迁移和跨版本恢复，需要为该次升级准备备份并检查结果。软件包安装成功不表示数据迁移成功。
- 手动运行 `scripts/*_smoke.py` 和 `scripts/benchmark.py` 会调用真实模型。任何真实服务、会话或通知操作，都需要指定目标并获得授权。

模型回复完成只说明该轮已经结束。模拟任务和 CI 的结果只适用于各自执行的检查。工业认证和其他环境需要另行验证。见[安装与使用](getting-started.md)、[CI 与发布](ci-release.md)和[隐私检查](privacy.md)。
