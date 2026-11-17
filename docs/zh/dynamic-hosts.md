> 语言：[English](../dynamic-hosts.md) | 中文

# 动态节点配置

## 运行中的配置变更

Broker 每 0.5 秒检查当前状态目录的 `config.json.hosts` 和 `host_pins.json`。在 `hosts` 中新增、删除或修改一个条目即可生效，不需要重启 Broker、Web 或 worker；只更新主机配置，不自动更改程序、模型、权限或配额。使用临时文件加原子替换可以避免编辑中间状态被读到。

需要立即检查时调用管理接口：

```bash
agent-bridge call agent_hosts_reload --json '{}'
agent-bridge call agent_hosts --json '{}'
```

刷新返回 `added`、`updated`、`removed` 三个列表。重复刷新没有新变化时返回三个空列表。该接口无参数，仅接受同 UID 和管理令牌，不加入受限成员或远端 peer 方法。自动刷新配置不要求重新注册 MCP 工具。

保留原配置格式，向现有 `hosts` 对象合并。以下使用虚构 SSH 别名 `linux-node-b` 举例：

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

先通过已知 SSH 主机核对 `peer.hello` 和 `peer.inspect(kind=node)`。不要把任意第一次响应直接当作已固定身份；`host_pins.json` 中同名条目优先于配置中的 pin。保留已有主机条目和其他配置，不复制凭据或 SQLite 数据库。

## 变更时保留的工作

- 未变化节点的客户端和在线状态保留。变更节点先拒绝新请求，再等待完整 API 操作、轮询、ACK 和游标写入完成，最后关闭它自己的旧 SSH。已经取到旧客户端的调用不能重开连接发送。
- 非法 JSON、缺失配置或不合法的主机字段不会被当成空清单。整批不应用，`agent_hosts.config_error` 保留失败原因；文件修复后自动重试。
- 活动分组成员、非终态 `peer_outbox` 和未解决跨机操作阻止删除/修改。`completed` 的提交操作不等于模型任务完成，不据此直接放行。
- 历史父通知路由只读查询远端：会话必须 `closed/stopped`；任务必须完成且已 accepted/rejected，或为 failed/cancelled/interrupted 终态。离线、缺失记录、活动会话、未审查结果或身份不同均拒绝。历史路由和游标保留。
- 不取消远端任务，不关闭原生会话，不清空队列，不把 uncertain 改回 pending，也不自动迁移任务。后台旧节点循环自行退出，重新加入时清除它的临时身份/分页缓存。
- EventMirror 因事件指纹冲突暂停时，仅改同身份 SSH 配置不解除暂停。明确换 pin 或移除节点时清除临时暂停；原有指纹和持久游标仍保留，不能跳过冲突检查。

已知限制：旧身份的历史父通知路由没有持久退休证明。将同一 `host_id` 复用于另一个 `node_id` 后，这些旧路由仍可能阻止再次移除；这种情况使用新的 `host_id`。本版不提供强制删除或伪造任务结束的操作。
