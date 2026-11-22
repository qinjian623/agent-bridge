> 语言：[English](../ci-release.md) | 中文

# GitHub 测试与发布

`.github/workflows/ci.yml` 在 push、pull request 和手动触发时运行：

- Ubuntu / Python 3.11、3.13、3.14：按 `uv.lock` 安装，运行 Ruff 和完整离线测试。测试使用模拟 CLI，不需要模型账号或密钥。
- 上述检查通过后，Ubuntu 和 macOS 分别构建 wheel 和源码包，准备 `install.sh`，生成 `SHA256SUMS` 和 `latest.json`。打包任务在新环境中安装 wheel，检查命令帮助、重复初始化和 `doctor`，并在临时目录启动前台 broker，确认运行版本与 wheel 一致，通过 MCP 调用 `agent_inspect`。安装脚本检查用本次打包文件和独立用户目录，运行时不启动用户服务。构建文件保留 14 天。

macOS 检查安装和打包；完整测试含 Linux `/proc` 检查，目前仅在 Ubuntu 运行。它不代表真实远程 AGY、模型工具或服务升级已验证。

发布稳定版本时，先让 `pyproject.toml`、包版本以及锁文件保持一致，再推送同版本的 `vX.Y.Z` tag。tag 版本不匹配时禁止发布。所有测试和两台构建任务通过后，发布任务取 Ubuntu 的构建结果并重新核对 SHA-256，将以下文件上传到发布草稿，再公布为最新版本。

```text
cli_agent_bridge-X.Y.Z-py3-none-any.whl
cli_agent_bridge-X.Y.Z.tar.gz
install.sh
SHA256SUMS
latest.json
```

Actions 的测试、构建任务只有仓库读取权限，tag 发布任务具有写 Release 的权限。第三方 Action 固定到已经核对的 commit。流程不推送 PyPI、不更新终端、不重启 Bridge，也不自动覆盖已有 Release。发布页提供 `install.sh`，`SHA256SUMS` 包含该脚本、wheel 和源码包的校验值。新安装和已启用定时更新的节点都通过公开 GitHub Releases HTTPS 获取清单、wheel 和校验文件，无需配置 `gh` 登录或令牌。不要把 GitHub token 写进仓库、workflow 或包文件。

## 新公开仓库的首个版本

用已检查的源码或 Git 历史建立公开仓库。未纳入发布的 Git 引用、旧 Release 附件和 Actions 记录留在私库。推送发布 tag 前，将中英文文档里的安装和克隆地址、包的 Repository 和 Issues 链接、随包 skill 的链接以及默认更新源设为公开地址。tag 构建会核对包的链接和默认更新源是否指向当前发布仓库。

首发版本应高于存量节点安装的版本，并保留 Python 包名及 wheel 命名规则。存量节点继续使用现有更新源，直到管理员明确切换。先用未登录的客户端验证能从公开 Release 安装、连接 MCP 并读取 OTA 清单，再让存量节点切换。见[隐私检查](privacy.md)和[切换更新源](getting-started.md#switching-update-sources)。
