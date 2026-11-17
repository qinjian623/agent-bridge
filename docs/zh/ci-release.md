> 语言：[English](../ci-release.md) | 中文

# GitHub 测试与发布

`.github/workflows/ci.yml` 在 push、pull request 和手动触发时运行：

- Ubuntu / Python 3.11、3.13、3.14：按 `uv.lock` 安装，运行 Ruff 和完整离线测试。测试使用模拟 CLI，不需要模型账号或密钥。
- 上述检查通过后，Ubuntu 和 macOS 分别构建 wheel、源码包，写入 `SHA256SUMS` 与 `latest.json`，并在新虚拟环境中安装 wheel、检查命令以及幂等 `agent-bridge init` / `doctor`。构建文件保留为 14 天的 Actions artifact。

macOS 检查安装和打包；完整测试含 Linux `/proc` 检查，目前仅在 Ubuntu 运行。它不代表真实远程 AGY、模型工具或服务升级已验证。

发布稳定版本时，先让 `pyproject.toml`、包版本以及锁文件保持一致，再推送同版本的 `vX.Y.Z` tag。tag 版本不匹配时禁止发布。所有测试和两台构建任务通过后，发布任务取 Ubuntu 的构建结果并重新核对 SHA256，先创建 draft release、上传完整附件，再公布为 latest：

```text
cli_agent_bridge-X.Y.Z-py3-none-any.whl
cli_agent_bridge-X.Y.Z.tar.gz
SHA256SUMS
latest.json
```

Actions 的测试、构建任务只有仓库读取权限；仅 tag 发布任务具有写 Release 的权限。第三方 Action 固定到已经核对的 commit。流程不推送 PyPI、不更新终端、不重启 Bridge，也不自动覆盖已有 Release；节点自动更新程序直接通过公开 GitHub Releases HTTPS 读取 `latest.json`、`SHA256SUMS` 与 wheel，无需配置 `gh` 登录或令牌。不要把 GitHub token 写进仓库、workflow 或包文件。
