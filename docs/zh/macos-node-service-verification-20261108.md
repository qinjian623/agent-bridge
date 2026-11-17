> 语言：[English](../macos-node-service-verification-20261108.md) | 中文

# macOS 从机（`macos-node`）服务检查（2026-11-08）

从机（匿名化 SSH 别名 `macos-node`）仓库位于 `~/gits/cli-agent-bridge`。主协调机通过 SSH 执行远端 `agent-bridge relay`，不需要 `macos-node` 再通过反向 SSH 登录协调机。

## 服务与配置

- `macos-node`：安装并加载用户服务 `gui/<uid>/local.cli-agent-bridge`，配置文件位于 `~/Library/LaunchAgents/local.cli-agent-bridge.plist`。启用 `RunAtLoad`、`KeepAlive` 和 10 秒重启间隔。
- 两端使用各自的 `~/.local/state/cli-agent-bridge`，没有复制认证或共享 SQLite。
- 本机 `hosts.macos-node` 指向远端绝对命令和状态目录。SSH 使用非交互认证、已知主机密钥检查、连接超时及保活，关闭 agent forwarding；沿用已登记的节点身份。
- 本机仅重启 Broker 加载主机配置。已有 Pi worker 属于独立 systemd unit，没有停止它们。

`macos-node` 原数据库有 18 个 `open/stopped`、没有原生 ID 和 worker 的旧会话，以及 7 条排队任务。启动 Broker 会自动拉起这些会话，因此启动前使用 `pause_drain` 暂停它们，保留队列和历史。没有取消或执行这些任务；需要继续时，应明确选择会话后再 `resume`。

## 本次修复

持续运行检查发现，后台轮询每次创建 SSH 连接却不关闭，约 70 秒积累了 42 条连接并导致从机拒绝新 SSH。现改为每个主机复用同一客户端，Broker 退出时关闭连接。

每次远端请求先在同一 SSH 连接上握手并核对节点身份；握手后连接发生变化时拒绝发送本次请求，下次新请求重新握手。身份异常保持禁止发送，直到管理员重新登记。断线不重发结果不明的原请求。

监控原先每次重新创建主机清单，导致已连接的主机仍显示 `offline`。Broker 内部监控改为使用运行中的主机清单；独立终端和 Web 监控通过本机 Broker 读取快照，请求失败时仍可查看本地数据。

## 实际检查

- `macos-node` 本机 `agent_inspect` 调用成功，`doctor` 显示 `broker_socket=true`。
- SSH `peer.hello` 与 `peer.inspect(kind=node)` 返回同一节点，并匹配已有身份登记；协议版本为 1，节点状态为 `ready`。
- 本机 `agent_hosts` 返回 `macos-node=online`，最近通信时间持续更新；`agent_catalog(host_id=macos-node)` 读到远端 3 个配置；MCP 和独立终端监控均显示在线。
- 连续观察 30.5 秒，四次取样始终只有同一个 SSH 子进程，主机和监控始终在线；此前的连接增长没有再发生。
- 向 `macos-node` 的 Broker 发送正常 `SIGTERM` 后，launchd 自动拉起新实例，原生接口恢复，18 个会话仍暂停，7 条任务仍排队，没有模型 worker。
- 610 项自动测试通过，Ruff 与 `git diff --check` 通过。测试覆盖连接复用、主机隔离、关闭、并发、身份不符、握手后断线、重连、原请求不重发及监控读取。

未执行远端模型推理、训练或真实会话广播。远端模型配置仍标记为未验证。本次 launchd 检查发生在用户已登录的情况下，未验证登出或整机重启后的恢复。

详细检查结果保存在本机私有目录 `<private-report-03>/`，远端服务检查保存在 `<private-report-09>/`；不在仓库保存数据库、令牌或原始会话内容。
