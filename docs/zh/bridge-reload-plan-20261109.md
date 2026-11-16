> 语言：[English](../bridge-reload-plan-20261109.md) | 中文

# Bridge 重载与实际验证计划

本文保留 2026-11-09 的计划与检查结果，不是再次重启服务的指令。私人项目、进程和通知标识以及报告位置已改为说明性占位符；报告仍在私下保留，不随仓库发布。

状态更新（11-09 12:03）：本机于 11:43:49 在本轮操作之外重新开机，Bridge 于 11:44:11 自动启动为 PID <process-01-pid>，已加载通知修复。本轮没有执行 restart。下文保留开机前的准备步骤；不应在服务已经更新后照单再重启一次。

11:53 的检查无活跃任务、worker、在途发送或未返回 API 请求。生产环境通知记录已精确撤回：10:15 曾确认同一回执仍在原生队列，11:53:38 清理后记录为 `processed/removed/removed`，11:57 只读复查队列及最近 100 轮均无该项。代码只有取得原生 `deleted=true` 才写入此状态；未另存删除响应原文或完成时间。

另补一个旧版本兼容情况：旧 Broker 可以在新 worker 添加列以后 ACK，留下 `processed + queue_cleanup=NULL`。初始化现在会将这种记录安排为 pending，保留原生接收状态。代码修改后、正常新任务启动期间，历史记录已自动补查；常驻程序于 12:02:19 核对到真实 turn（`<turn-id>`），现为 `received/already_started`。准备手动初始化前发现已有新任务，检查停止，未额外写生产数据库。645 项测试、Ruff 和 diff 检查通过。

截至 12:03，71 条旧通知仍为 `unknown/uncertain`，创建于 11-03 至 11-04，不能解释为 71 项待执行任务，也不补发。`macos-node` 在线；本机 8080 未监听，不能报告网页已恢复。

独立 Native 验证未通过，已在任何队列写入前停止。空 `CODEX_HOME` 的独立 stdio 实例可以创建测试线程，但 `thread/turns/list` 返回 -32600：首条用户消息前线程尚未 materialized；`thread/read(includeTurns=true)` 又返回 -32601：`list_turns is not supported yet`。因此无法按计划确认空历史，不制造首个模型轮次来绕过限制。所有测试进程已退出、临时目录与数据库已删除；没有复制认证、使用共享 daemon、发送或删除队列消息。可重跑的脚本为 `scripts/check_native_notification_retract.py`，私有报告在 `<private-report-06>/`。生产 <observed-notification-id> 的实际撤回与此次未通过的隔离测试分别记录，不合并为“全项实测通过”。

脚本增加 7 项方法、目标与报告保护测试后，最终完整 652 项测试通过，耗时 21.659 秒；Ruff 与差异检查通过。最终结果保存在 `<private-report-07>/validation.json` 和 `unittest-final.log`。Bridge 仍为 PID <process-01-pid>，NRestarts=0。本轮未提交或推送代码。

13:29 后按用户要求恢复 Web：新增本机用户服务 `~/.config/systemd/user/cli-agent-bridge-web.service`，启用 `default.target` 自动启动及失败重启，仍只监听 `127.0.0.1:8080`，不在 unit 中存凭据。unit 验证通过，登录页和认证后快照返回 200，未认证快照返回 401；连续三次快照 `macos-node` 均在线。Bridge、共享 Codex daemon 与原有三个 worker 的 PID/启动标识均未变化。用户 linger 已启用；未为验证再次重启整机。检查元数据在 `<private-report-08>/validation.json`。

本次只重载 `cli-agent-bridge.service`。不重启共享 Codex daemon、`macos-node` 服务、Web 监控，不改变模型、权限或已有会话的原生 ID，不重跑任何编译任务。

## 开机前准备快照与会受影响的部分

- 本机 Broker：PID <process-02-pid>，11-08 15:41 启动，尚未加载昨晚的常驻通知修复。
- 当前有一个 Flash 任务运行，worker PID <process-03-pid>。三个旧 Pi worker PID <process-04-pid>、<process-05-pid>、<process-06-pid> 空闲。四者分别位于独立 `agent-bridge-...service`，不在 Broker 的 cgroup。
- 本次只读快照中，通知、原生发送队列及 peer_outbox 的 sending=0，原生 processing running=0，未返回 API 请求=0；这些数字必须在执行时重新检查。
- `processed + queue_cleanup=pending` 共 146 条，全部属于 `example-coordinator`。这是待核查记录数，不能当作原生队列中的实际消息数。

重载会让新的通知、ACK、原生接收及撤回逻辑生效。服务启动后会自动核查上述已 ACK 记录：只撤回仍在同一原生线程队列中、clientId 与已有回执均匹配的那一条；已经进入原生轮次的记录不删除、不打断。结果未知后只读核查，不自动再次删除或重新发送。未 ACK 的通知不在这批清理范围内。

因此重载并非完全不操作原生队列。若本批确认仍要求队列保持不变，就不能启动这版常驻通知循环；先停在准备阶段，不通过改数据库或禁用 Telegram 检查绕开限制。

## 对现有 worker 的影响

Broker 通过独立 `systemd-run --unit=agent-bridge-...` 启动 worker；其 `KillMode=control-group` 只覆盖自己的 cgroup。重载前必须确认 Broker 子进程只有它自己的 SSH Relay 客户端，没有发送 helper、AGY 恢复进程或其他工作。

正常情况下，worker 的 PID、进程启动标识、native ID、模型和工作目录保持不变。Broker socket 在重启期间会短暂不可用；父会话在收到完成报告前不要派新任务或提交新的状态修改。

**现有 worker 不会因 Broker 重启而重载 Python 模块。** 本轮重载生效的是常驻通知处理；worker 的写入/响应超时、接收确认前超时修复，仅对已经加载新文件的 worker 或今后新启动的 worker 生效。老 worker 若需更新，应在其自身空闲后单独安排，不能把本轮 Broker 重载说成所有 worker 都已更新。本轮不关闭、重开或迁移这些会话。

## 确认空闲后，执行前检查

1. 固定当前代码文件摘要，核对仍是已通过 644 项测试的修复内容。如果有人继续修改相关代码，先检查改动并运行必要测试，不能沿用旧测试结论。
2. 再查本批任务：queued、assigned、running、waiting_input 均为 0；任何 recovery_needed 或外部执行结果未知的工作先交负责人检查，不能当作正常空闲。
3. 检查所有通知发送状态：`deliveries`、`native_send_queue`、`native_broadcast_deliveries`、`peer_outbox` 均无 sending；原生 processing 无 running；`api_requests.response IS NULL` 为 0。重复短间隔采样，发现新任务、投递或未返回调用就停止。
4. 记录 Broker、所有 worker、共享 Codex daemon、`macos-node` Broker 和 Web 监控的 PID/启动标识及监听位置。确认 worker 与 Broker cgroup 分离；存在未知子进程就停止。
5. 只读列出本次会被自动检查的 ACK 记录范围和当前原生队列匹配元数据，不保存正文或完整数据库。若影响范围超出获准父会话，停止并报告。

## 实际验证先用独立测试实例

不能只用临时 Bridge `--state` 就声称原生会话已隔离。生产 `codex_parent.main()` 固定使用 `daemon=True, start_daemon=False`，仍会连接默认共享 daemon；后者只禁止启动 daemon，不禁止队列处理。

测试程序使用独立的空 `CODEX_HOME` 和空工作目录，持有 `CodexAppServer(daemon=False)` 的独立 stdio 子进程。不要复制认证文件，不加载编译仓库，不改全局环境。若该版本在无认证环境下不能完成这些纯队列操作，停止并报告，不退回共享 daemon 偷跑测试。

1. 原始 RPC `thread/start` 只创建一个测试线程，不使用 SDK `.thread()`，因为该便利方法会附加权限和 developer instructions。不要设置 Goal；用 `thread/turns/list` 确认没有轮次。
2. 临时 Bridge 只使用 Store 和单次函数，不启动 Broker、scheduler 或 worker。合成一个明确标为测试的完成事件，调用实际 helper 核心函数，将测试通知及两个唯一 marker 入队到这个测试线程。
3. ACK 前只调用 `receipt_status` 和原生 queue/list：通知必须为 enqueued，三条 ID/clientId 都能找到。不要再次调用已有 receipt 的普通 `advance()`，它在 notLoaded 时可能恢复会话。
4. 对临时 Store 执行 ACK，调用一次实际撤回核心函数。要求 Native 返回 `deleted=true`，目标 receipt 消失，两个 marker 的 ID/clientId 保留，Bridge 状态为 processed/removed。
5. 用相同 request_id 重复 ACK，确认没有新入队或删除操作。原生轮次列表仍必须为空。
6. 失败分支可在独立测试线程内，由测试程序在 helper 查到目标后提前精确删除该测试项，让 helper 收到真实 `deleted=false`；随后 check_only=true 只允许 list/turns 查询，不再 delete/add。报告标记为“人为删除竞态，Native 真实响应”，不能称为真实网络断线测试或原生自动取出测试。
7. 结束后只清理自有 marker 和独立子进程；保留测试 ID、RPC 方法、回执及状态元数据，移除临时状态中的测试数据库，不保留凭据或原始聊天。

全过程禁止 `turn/start`、`thread/queue/start`、`thread/resume`、steer 和 interrupt。每阶段都确认没有原生轮次；出现自动执行、目标标识冲突或异常共享连接就停止。

这验证的是**同一核心 helper 与真实 Native 协议**，不是生产共享-daemon subprocess 入口的完整实测。合成完成事件也不是真实模型子任务，不能拿它证明模型已读、实际编译完成或主 Codex 长请求已修复。

## 只重载 Broker

隔离验证通过且执行前条件仍满足后，只执行一次：

```bash
systemctl --user restart cli-agent-bridge.service
```

不修改配置、不重启其他 unit。命令结果不明或停止超时，先查这一次 systemd 作业与实际进程，不能再提交第二次 restart 或强杀；记录未知结果，不重放任务。不回滚数据库、清空队列或覆盖别人的未提交改动。

## 重载后的生产检查

- 新 Broker PID 改变并 active；共享 Codex daemon、`macos-node`、Web 和原 worker 的 PID/启动标识保持不变。查询原任务、attempt 和审查状态，没有新重复 task/attempt，也没有被改成 completed 的未知任务。
- `agent_hosts`、`agent_monitor` 与 HTTP `/api/snapshot` 对应；`macos-node` 在正常网络下恢复握手并保持同一条复用 SSH。网页进程和认证令牌不需要重置。
- 通知区分 enqueued、匹配到 userMessage/turn、父 ACK。对获准 ACK 范围核对 cleanup 结果；找不到或超过历史查询范围的仍保留未知，不宣称全部清理完成。
- 只读验证生产 helper 的 inspect 路径可连接现有 daemon，不发送测试消息到编译会话。只有确实观察到生产中已 ACK 的同一队列项被精确撤回，才报告生产撤回实测通过；没有可验证的对象就记为未验证。
- 收到故障、范围不符或结果未知时停止后续验证并报告，不改模型、不恢复/重跑编译，也不为了测试中断共享 daemon。

最终分别报告：代码测试通过、Broker 已加载、独立 Native 验证结果、生产只读检查、实际撤回结果和未验证项。**主 Codex 长模型请求的原因仍未知，不能因为本次重载或 644 项测试就宣布该问题已修复。**
