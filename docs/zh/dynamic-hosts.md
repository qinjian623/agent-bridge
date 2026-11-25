> 语言：[English](../dynamic-hosts.md) | 中文

# 动态节点配置

## 运行中的配置变更

Broker 每 0.5 秒检查当前状态目录的 `config.json.hosts` 和 `host_pins.json`。在 `hosts` 中新增、删除或修改一个条目即可生效，不需要重启 Broker、Web 或 worker；只更新主机配置，不自动更改程序、模型、权限或配额。先写临时文件再原子替换，可以避免 Broker 读到写了一半的配置。

需要立即检查时调用管理接口：

```bash
agent-bridge call agent_hosts_reload --json '{}'
agent-bridge call agent_hosts --json '{}'
```

刷新返回 `added`、`updated`、`removed` 三个列表。重复刷新没有新变化时返回三个空列表。该接口无参数，仅接受同 UID 和管理令牌，不开放给受限成员或远端 peer 方法。自动刷新配置不要求重新注册 MCP 工具。

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

- 未变化节点的客户端和在线状态会保留。变更节点会先拒绝新请求，等正在进行的 API 操作、轮询、ACK 和游标写入全部完成后，再关闭旧的 SSH 连接。已经拿到旧客户端句柄的调用不能重开连接继续发送。
- 非法 JSON、缺失配置或不合法的主机字段不会被当成空清单。整批不应用，`agent_hosts.config_error` 保留失败原因；文件修复后自动重试。
- 如果节点仍有活动分组成员、未到终态的 `peer_outbox` 或未结束的跨机操作，则禁止删除或修改。状态为 `completed` 的提交操作只代表提交完成，不代表模型任务已经结束，不会据此直接放行。
- 存在历史父通知路由时，Broker 会向远端做只读核查：对应会话必须处于 `closed/stopped`，任务必须已完成并经 accepted/rejected，或处于 failed/cancelled/interrupted 终态。只要遇到节点离线、记录缺失、会话仍活动、结果未审查或身份不一致，都会拒绝变更。历史路由和游标会保留。
- 不取消远端任务，不关闭原生会话，不清空队列，不把 uncertain 改回 pending，也不自动迁移任务。旧节点的后台循环会自行退出；节点重新加入时，会清除它之前的临时身份和分页缓存。
- EventMirror 因事件指纹冲突暂停时，仅改同身份 SSH 配置不解除暂停。明确换 pin 或移除节点时清除临时暂停；原有指纹和持久游标仍保留，不能跳过冲突检查。

已知限制：旧代际（`state_generation`）的历史父通知路由缺少明确的停用证明。将同一 `host_id` 复用于另一个 `node_id` 后，这些旧路由仍可能阻止再次移除；这种情况应使用新的 `host_id`。本版不提供强制删除或伪造任务结束的操作。

## 远程事件记录

后台轮询同步任务和会话事件，默认跳过每次工具调用的开始和结束。需要同步某台机器后续的工具明细时，在对应的 `hosts` 项中设置 `mirror_tool_events: true`。即使已有任务或分组引用该节点，也可更改。轮询从保存的游标继续。更早的明细可在来源节点用 `agent_wait` 按任务 ID 和单独的游标查询。

在保存副本的机器上先预览，再明确执行旧工具明细清理。

```bash
agent-bridge mirrors expire --days 30 --limit 500
agent-bridge mirrors expire --days 30 --limit 500 --apply
```

每次处理一页。返回 `has_more: true` 时，把 `next_after` 作为下一次的 `--after`。仅清理已被同步游标覆盖的旧明细，保留本机序号、来源、时间和校验摘要。仍被通知、投递或原生发送引用的数据继续保留。执行清理后，应使用已升级的新版本来处理通知，这样晚到的有效通知才能恢复对应明细。来源节点的原始事件保留，SQLite 可复用释放的空间。
