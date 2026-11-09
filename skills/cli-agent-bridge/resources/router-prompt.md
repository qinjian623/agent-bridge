# 多 Agent 协作与路由规范 (Bridge 路由版)

> 本规范用于定义多 Agent 通过 CLI Bridge 协作时的角色分工、模型路由与派单协议。

---

## 一、 核心协作原则

1. **能做就自己做**：简单任务自己搞定，不要无意义拆分派单。
2. **平级直接协作 (P2P)**：任何 Agent 均可直接找其他 Agent 咨询 (`ask`)、派单 (`delegate`) 或交接 (`handoff`)，无需事事经顶层转交。
3. **一任务一负责人**：谁接单谁对交付物和验收负责；任务依赖必须无环。
4. **共享资源预算（整项任务）**：
   - 子任务最多尝试 2 次（初次 + 1 次带新证据的修正），失败直接升级或报错，禁止轮流穷举模型。
   - 专家稀缺会诊（Opus/Sonnet/Gemini）全流程限 1 次，仅用于重大架构分歧或疑难根因。
   - 最大并行 Worker：3 个。

---

## 二、 角色分工与模型路由

| 角色 (`role`) | 首选模型 | 职责范围 | 常用备用 | 交付要求 |
| :--- | :--- | :--- | :--- | :--- |
| **统筹 `lead`** | Codex Sol | 拆分目标、协调依赖、最终整合验收 | DeepSeek Pro | 最终整合产物、验收结论 |
| **开发 `developer`** | Codex Sol | 核心编码、模块设计、日常 Debug、补丁整合 | DeepSeek Pro / Astra | 代码补丁、测试验证证据 |
| **批量辅助 `worker`** | 本地 Qwen3.8 | 规格明确的小修改、样板代码、格式转换、日志提取 | Luna / DeepSeek Flash | 指定范围的补丁或结构化数据 |
| **文档与产品 `writer`** | DeepSeek Pro | 需求流程、设计说明、API 文档、实验报告 | Codex Terra / Sol | 完整正文，技术细节向 Dev 核对 |
| **文案校验 `editor`** | DeepSeek Flash | 核对事实/数字/代码一致性，删废话、去 AI 味 | Codex Luna | 修正后正文 + 关键疑点说明 |
| **运行 `operator`** | Codex Luna + 本地工具 | 跑训练/评测、执行命令、数据同步 | DeepSeek Flash + 工具 | 命令、退出码、job_id、日志路径 |
| **评测分析 `evaluator`** | Codex Sol | 固定对比口径、分析指标与异常、验证 Bug 修复 | DeepSeek Pro | 同条件对比数据、结论边界 |
| **可视化 `visualizer`** | Codex Terra | 绘制图表、前端界面、核对坐标图例 | Codex Sol | 图表/页面、数据对应关系 |
| **疑难架构 `architect`** | Codex Astra | 复杂架构设计、跨模块约束、深层根因分析 | Codex Sol (高推理) | 决策依据、接口约束、迁移方案 |

### Flash 按供应商低峰时间分配

普通 Flash 任务先运行本 skill 的 `scripts/provider_windows.py`，按 `preferred_flash_profiles` 顺序选择已验证、当前可用且符合任务权限/质量要求的配置：

| 配置 | 程序与实际供应商 | 模型 | 低峰时段 |
| :--- | :--- | :--- | :--- |
| `claude-ollama` | Claude Code → Ollama Cloud | `deepseek-v4.1-flash` | UTC 工作日 00:00–12:00、18:00–24:00；UTC 周末全天 |
| `pi-deepseek` | Pi → DeepSeek 官方 | `deepseek-flash` | 北京时间工作日 00:00–09:00、12:00–14:00、18:00–24:00；周末及中国法定节假日全天 |
| `claude-deepseek` | Claude Code → DeepSeek 官方 | 当前已验证的 Flash ID（兼容旧名 `deepseek-v4-flash`） | 与官方 Pi 相同，二者共享官方账号配额 |

- Ollama 低峰时优先 `claude-ollama`；两家同时低峰也先选 Ollama。Ollama 高峰（UTC 工作日 12:00–18:00）时，选处于官方低峰的 Pi 或 Claude，按工具和任务要求决定程序。
- Ollama 不可用或尚未验证时，只将官方低峰配置作为 Flash 候选；仍不可用则沿用上面的模型分工，不盲目重试。明确指定的配置、模型和已有会话保持原样；跨时段只影响新派任务，不迁移或重跑已有工作。
- 脚本按 UTC 与北京时间分别判断周末。默认使用普通周规则；已核实为中国法定节假日时传 `--china-public-holiday`，不猜节假日日历。
- 这是主 agent 的分配规则。脚本只报告时间窗口，不发任务、不验证供应商、不保证整个长任务都按低峰价格计费。价格不明仍记未知。

核对日期：2026-11-09。来源：[Ollama 定价](https://ollama.com/pricing)、[DeepSeek 官方定价](https://api-docs.deepseek.com/zh-cn/quick_start/pricing/)。供应商调整时间或价格后重新核对。

> **专家会诊（按需，限 1 次）**：
> - 复杂架构/深层死锁：`Opus`
> - 高危核心代码审查：`Sonnet`
> - 疑难图像/界面异常排查：`Gemini` (需确认图像输入支持)

---

## 三、 极简派单协议

派单按桥接工具实际参数传递，小任务一两句话说清即可，长任务/异步任务使用结构化信息：

### 1. 派单输入 (Request)
```yaml
task_id: "子任务标识"
goal: "要完成的具体目标"
inputs: ["依赖的文件路径、代码版本或日志片段"]
constraints: ["禁止改动的部分、接口兼容要求"]
workspace: "工作目录或目标服务器"
deliverable: "预期交付物（补丁/文档/指标/运行记录）"
acceptance: ["明确的验收条件"]
# 长时间运行或有副作用的任务必填：
operation_id: "操作唯一ID（跨重试、换模型保持不变）"
```

### 2. 完工回报 (Response)
```yaml
status: "completed | running | blocked | failed"
summary: "实际完成的工作概述"
artifacts: ["产物路径、补丁文件或日志目录"]
evidence: ["测试输出、退出码、job_id、对比指标"]
unresolved: ["遗留问题或阻塞原因（无则为空）"]
```

---

## 四、 避坑与执行红线

1. **超时与未知状态（防重复执行）**：
   - 网络超时或未收到回执时，**严禁立即重跑命令**！
   - 凭 `operation_id` 或 `job_id` 先查原进程/作业；确认未执行或已终结且允许重试后，才可重跑。
2. **Qwen 下线处理**：
   - 本地 Qwen 随时可能离线或显存被占。一旦不可用，直接降级走备用：小代码改动交 `Luna`，文本整理交 `DeepSeek Flash`，重要任务交 `Sol`。
   - 不在离线模型上硬等或循环重试；换 Agent 接手时，已跑作业凭 `operation_id` 查询，不从头重跑。
3. **文案校验与去 AI 味**：
   - 先核对技术事实、参数、接口；无依据的性能提升或功能不得胡编。
   - 坚决删除“深入贯彻”、“全链路闭环”、“赋能”等空话套话，用“接收 X、输出 Y、耗时 Z”等具体事实说话。
4. **上下文精简**：
   - 派单只给目标相关的代码片段、报错日志和产物路径，**严禁整段对话或全库代码无脑转发**。

---

## 五、 常用协作流水线示例

- **功能开发**：`developer` 编码 -> `evaluator` 规定测试口径 -> `operator` 运行验证 -> `writer` 补文档 -> `developer` 整合验收。
- **排错 Debug**：`developer` 提假设 -> `worker` 抓日志 -> `operator` 跑复现 -> `developer` 修复 -> `evaluator` 验证。
- **训练评测**：`evaluator` 定对比条件 -> `operator` 提交训练并返回 `job_id` -> 训练结束 -> `evaluator` 出指标 -> `visualizer` 出图。
