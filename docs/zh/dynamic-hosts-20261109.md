> 语言：[English](../dynamic-hosts-20261109.md) | 中文

# 动态节点与远端 Linux 从机接入（2026-11-09）

## 运行中的配置变更

加载本版代码后，Broker 每 0.5 秒检查当前状态目录的 `config.json.hosts` 和 `host_pins.json`。在 `hosts` 中新增、删除或修改一个条目即可生效，不需要重启 Broker、Web 或 worker；只更新主机配置，不自动更改程序、模型、权限或配额。使用临时文件加原子替换可以避免编辑中间状态被读到。

需要立即检查时调用管理接口：

```bash
agent-bridge call agent_hosts_reload --json '{}'
agent-bridge call agent_hosts --json '{}'
```

刷新返回 `added`、`updated`、`removed` 三个列表。重复刷新没有新变化时返回三个空列表。该接口无参数，仅接受同 UID 和管理令牌，不加入受限成员或远端 peer 方法。自动检查无需 MCP 客户端重新发现这个新工具。

保留原配置格式，例如向现有 `hosts` 对象合并（下文将测试机 SSH 别名脱敏记为 `linux-node-b`）：

```json
{
  "linux-node-b": {
    "ssh_target": "linux-node-b",
    "remote_executable": "~/.local/share/cli-agent-bridge/bin/agent-bridge",
    "remote_state": "~/.local/state/cli-agent-bridge",
    "projects": {"bridge": "~/gits/cli-agent-bridge"},
    "pinned": {"node_id": "核实过的节点ID", "state_generation": "核实过的代次"}
  }
}
```

先通过已知 SSH 主机核对 `peer.hello` 和 `peer.inspect(kind=node)`。不要把任意第一次响应直接当作已固定身份；`host_pins.json` 中同名条目优先于配置中的 pin。不要覆盖已有 `macos-node` 条目或其他配置，也不复制两台机器的认证或数据库。

## 变更时保留的工作

- 未变化节点的客户端和在线状态保留。变更节点先拒绝新请求，再等待完整 API 操作、轮询、ACK 和游标写入完成，最后关闭它自己的旧 SSH。已经取到旧客户端的调用不能重开连接发送。
- 非法 JSON、缺失配置或不合法的主机字段不会被当成空清单。整批不应用，`agent_hosts.config_error` 保留失败原因；文件修复后自动重试。
- 活动分组成员、非终态 `peer_outbox` 和未解决跨机操作阻止删除/修改。`completed` 的提交操作不等于模型任务完成，不据此直接放行。
- 历史父通知路由只读查询远端：会话必须 `closed/stopped`；任务必须完成且已 accepted/rejected，或为 failed/cancelled/interrupted 终态。离线、缺失记录、活动会话、未审查结果或身份不同均拒绝。历史路由和游标保留。
- 不取消远端任务，不关闭原生会话，不清空队列，不把 uncertain 改回 pending，也不自动迁移任务。后台旧节点循环自行退出，重新加入时清除它的临时身份/分页缓存。
- EventMirror 因事件指纹冲突暂停时，仅改同身份 SSH 配置不解除暂停。明确换 pin 或移除节点时清除临时暂停；原有指纹和持久游标仍保留，不能跳过冲突检查。

已知限制：旧身份的历史父通知路由没有持久退休证明。将同一 `host_id` 复用于另一个 `node_id` 后，这些旧路由仍可能阻止再次移除；这种情况使用新的 `host_id`。本版不提供强制删除或伪造任务结束的操作。

## 远端 Linux 节点（`linux-node-b`）现场状态与验证

- 源码与 Git 状态已通过同步脚本校对，远端目录位于 `~/gits/cli-agent-bridge`；没有传认证、状态数据库或本机虚拟环境。
- 远端用户服务 `cli-agent-bridge.service` 已 enabled/active，`broker.sock` 存在，直接对远端 Broker 的 `agent_inspect` 成功；并非仅凭 SSH Relay 回应判断服务可用。
- `peer.hello`、节点 inspect 和静态配置目录一致，协议为 1、状态 ready。主机配置已在首次升级完成后合并进正式清单，并由自动监视器加入；MCP 与原有 Web 均显示该节点在线。
- 远端原有两个 Codex app-server 未重启，模型配置保留未验证状态，不触发实际推理调用。
- 独立 Broker 与 Web 使用同一个进程 PID，在真实 SSH 上完成新增 `linux-node-b`、删除、重新加入；Web 无重启即显示变化。另一个测试别名使用同一个空节点，SSH PID 全程不变。
- 写坏测试配置后，原有效节点和两条 SSH 均保留；修复后自动恢复。测试没有创建模型任务或会话，临时状态已移除。

完整 687 项自动测试通过，耗时 24.342 秒，Ruff 与差异检查通过。真实检查结果保存在本机私有目录 `<private-validation-state>/report`。

## 首次启用记录

旧 Broker PID <process-02-pid> 没有配置监视器或新接口，修改源文件不能让已载入内存的 Python 代码自动变化，经核实空闲窗口后执行了单次 Broker 升级：PID <process-02-pid> 变为 2166491，新即时刷新接口返回正常，Web PID <process-01-pid> 与共享 Codex daemon PID <process-14-pid> 启动标识均保持不变。随后合并 `linux-node-b` 配置后协调端 PID 不变，远端服务 PID <process-07-pid> 运行正常。复查时 `macos-node` 因私网链路暂时离线（`<vpn-ip>:22` 超时），配置与注册保持原样。
