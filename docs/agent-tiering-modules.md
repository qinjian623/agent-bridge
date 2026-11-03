# Agent 分级功能模块开发设计

本文件把设计稿第 8 节 P0–P5 拆成外部 Python 包 `agent-tiering` 的开发任务。命令均为拟开发接口，现在还不能运行。第一版单机、单用户、本地文件，不建服务，不修改 bridge 核心、配置或数据库。沿用第 9 节决定：JSONL 放在 `~/.local/state/agent-tiering/events.jsonl`；Reviewer 成本单列；缺陷经确认才归因；参考阈值仅展示，不自动淘汰。

## 1. 模块与顺序

工作量按一名熟悉 Python 和本仓库的开发者估算，包含离线测试与文档，不含真实模型排队、费用审批及多轮校准时间。

| 顺序 | 模块 | 阶段 | 依赖 | 工作量 |
|---|---|---|---|---|
| 1 | M0 任务规格与 bridge 调用包装 | P0 | 无 | 2–3 人日 |
| 2 | M1 JSONL 记录器与关联补录 | P0 | M0 的数据定义；可一起开发 | 2–3 人日 |
| 3 | M2 L0 验证器 | P1 | M0、M1 | 3–4 人日 |
| 4 | M3 Reviewer 评分与最终 review | P2 | M2、M1 | 2–3 人日 |
| 5 | M4 commit trailer 与缺陷关联辅助 | P2 后、P3 前 | M1、M2、M3 | 1–2 人日 |
| 6 | M5 指标汇总报表 | P3 | M1–M4；缺陷数据可为空 | 3–4 人日 |
| 7 | M6 路由表生成与查询 | P4 | M5、M0 | 2–3 人日 |
| 8 | M7 校准任务集运行器 | P5 | M0–M3、M5、M6 | 2–3 人日，加任务编写 |

约 17–25 人日。先完成一个“派发→领取关联→独立验证→人工决定→记录”的最小例子，再加评分、报表和路由。先用少量校准夹具测试运行器；正式 20–40 个任务逐步补齐，不能把任务编写成本藏在运行器估算里。所有模块都是外部包的子命令；没有必须改 bridge 的模块。

## 2. 共同数据格式

### 2.1 标识与事件

logical_task_id 是外部分配的不透明 UUID；task_key 是可读名称；bridge_task_id 是 dispatch 返回 UUID，不能混用。retry 保留 bridge_task_id；修改目标后创建新 bridge task，但保留 logical_task_id 并增加 spec_version、supersedes。attempt 的唯一引用为 `(bridge_instance_id, attempt_id)`，同时验证所属 bridge task 和 session。bridge_instance_id 由外部登记，绑定明确的状态库来源；复制库不可自动视为新运行，重建数据库须重新登记，不以可变目录路径当唯一身份。

每行是完整 UTF-8 JSON 对象，不包含注释。所有事件必填：schema_version=1、event_id(UUID)、event、ts(UTC RFC3339)、logical_task_id、project_id、producer、idempotency_key。与执行有关的事件按已知程度填写 bridge_instance_id、bridge_task_id、session_id、attempt_id；未生成的 ID 用 null，不能猜测。示例：

```json
{"schema_version":1,"event_id":"<uuid>","event":"attempt","ts":"2026-10-28T04:00:00Z","logical_task_id":"<uuid>","project_id":"repo-01","producer":"tiering/1","idempotency_key":"bridge-A:seq:42","bridge_instance_id":"bridge-A","bridge_task_id":"<uuid>","session_id":"<uuid>","attempt_id":"<uuid>","bridge_event_seq":42,"role":"executor","spec_version":1,"profile":"codex-main","model":"<exact-id>","identity":{"runtime":"codex","provider":"openai","effort":"high","service_tier":"priority","config_digest":"<non-secret-sha256>"},"outcome":"assigned"}
```

事件类型及专用必填字段：

| event | 专用字段 |
|---|---|
| task | task_key、type、difficulty(1–5)、tier(L1–L3)、dataset(online/calibration)、spec_version、spec_hash、policy_version |
| dispatch | phase(intent/bound)、dispatch_request_id、instruction_hash、role、supersedes；bound 时 bridge_task_id/session_id 必填，intent 时可 null |
| attempt | role(executor/reviewer)、bridge_event_seq、outcome、实际配置身份；绑定 ID 完整 |
| validation | attempt 引用、report_ref、report_sha256、input_tree、output_tree_hash、status |
| review | 被评 attempt 引用、role、decision(accepted/rejected/unreviewed)、reject_reason、checks_ref、score_ref、reviewer_attempt_ref、bridge_review_event_seq；unreviewed 必填 outcome，accepted/rejected 必须与 bridge 核实一致 |
| merge | bridge/attempt 引用、repo_id、完整 commit SHA、tree SHA、trailer_verified、contribution(single/mixed) |
| defect | defect_id、fix_logical_task_id、caused_by(原 logical_task_id)、candidate_commits、status(candidate/confirmed/dismissed)、confirmed_by、confirmed_at、reason_ref；未确认不得进入逃逸率分子 |
| route | table_hash、candidate_ids、chosen_id、mode、reason、seed、selection_probability、policy_version |
| correction | target_event_id、patch、reason、actor；只允许改分类、原因、人工确认等白名单字段，不可改 event_id/类型/桥接 ID/usage |

相比原稿四类事件，新增 dispatch、attempt、validation、route 和 correction 是为了覆盖无 review 的失败、补录及可重复统计；数量少时也沿用同一格式，不引入第二个数据库。correction 只能指向此前非 correction 事件；多次修正按文件顺序合并，保留原值与修正记录。schema_version 未知时拒绝计分，报告不支持的记录数。

reject_reason 使用原稿八项枚举：spec_unclear、wrong_logic、incomplete、tests_fail、false_claim、out_of_scope、style、env_failure。accepted 时 null；排除执行者失败须另填 attribution、confirmed_by、reason_ref，不仅靠枚举自动免计。取消/未知结果另记 outcome，不硬凑 rejection。

usage 和耗时仍从 bridge 取，不复制到事件中。报表输出聚合值和来源摘要、as_of、缺失比例；来源丢失显示 missing_source。证据文件放外部私有 reports/，JSONL 只保存相对引用、hash 和短结论。引用禁止越过私有目录；不持久化原始聊天、凭据、登录信息或私有数据库副本。

### 2.2 L0 验证结果 JSON

```json
{
  "schema_version":1,"attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<uuid>"},
  "spec_hash":"<sha256>","input_tree":"<snapshot-tree>","output_tree_hash":"<sha256>",
  "status":"fail","tests":[{"id":"unit-a","argv":["python","-m","unittest","tests.test_a"],"exit_code":1,"status":"fail","duration_ms":300,"evidence_ref":"reports/test-a.txt","evidence_sha256":"<sha256>"}],
  "diff":{"status":"pass","out_of_scope_files":[],"changed_files":["src/a.py"],"complete":true},
  "claims":[{"id":"claim-1","kind":"tests_pass","test_id":"unit-a","source_ref":"<artifact-id>","verdict":"suspected","reason":"同一输出树独立执行失败"}],
  "done_when":[{"id":"criterion-1","status":"fail","evidence_ref":"reports/test-a.txt"}],
  "false_claim":"suspected","errors":[]
}
```

status 为 pass/fail/unknown；tests 还可为 timeout/skipped/error，done_when 可 manual_pending。false_claim 为 none/suspected/confirmed/unknown；confirmed 必须带人工或主 agent 确认引用。退出码为 null 时不能写 pass。未经执行或证据不完整为 unknown。status=pass 要求所有强制条件通过，人工项已有确认；同一验证器输出不能自动代替用户接受。

### 2.3 Reviewer 评分 JSON

```json
{
  "schema_version":1,"subject_attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<executor-uuid>"},
  "reviewer_attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<reviewer-uuid>"},
  "spec_hash":"<sha256>","diff_hash":"<sha256>","rubric_version":"1",
  "scores":{"correctness":4,"completeness":4,"code_quality":3},
  "recommendation":"accept","reject_reason":null,"confidence":"medium",
  "findings":[],"limitations":["未验证外部服务"],"evidence_refs":["reports/l0.json"]
}
```

评分为整数 1–5：1 核心目标失败，2 有重要遗漏，3 达到明确要求但有可修问题，4 完整且证据充分，5 同样充分且处理了规格相关异常；各项附固定说明，不能为了给 5 分要求扩大范围。研究/文档的 code_quality 为 null，另写不适用理由，不冒充满分。findings 每项含 severity、file、line、trigger、consequence、evidence_ref。recommendation 为 accept/reject/needs_human，不直接写 bridge decision。结构化输出不合法最多修复请求一次，额外 attempt 成本仍计 Reviewer，不能悄悄丢掉差评分。

### 2.4 路由表 JSON

```json
{
  "schema_version":1,"generated_at":"2026-10-28T04:00:00Z","expires_at":"2026-11-04T04:00:00Z",
  "mode":"advisory","source_digest":"<sha256>","policy_version":"1",
  "reference_thresholds":{"fpa":0.7,"false_claim":0.05,"min_n":10},
  "buckets":[{"type":"implement","difficulty":3,"candidates":[{
    "profile":"codex-main","model":"<exact-id>","identity_digest":"<sha256>",
    "bridge_verified":true,"measurement":"Unmeasured","n":4,"effective_n":3.8,
    "fpa_smoothed":0.625,"false_claim_rate":null,
    "executor_cost_per_accept":{"unit":"USD","value":null,"complete":false},
    "seconds_per_accept":null,"score":null,"eligible":true,"reasons":["样本不足，使用初始等级"]
  }],"fallback_tier":"L2"}]
}
```

候选可以有描述性统计，但不足样本时 score=null；eligible 仅表示权限、输入类型、验证等条件满足，不由暂定阈值自动剔除。未知价格不能填 0。外部表里的 Unmeasured 是本包的字段，不假定 bridge 返回同名状态字段。

## 3. M0：规格与 bridge 调用包装

职责：验证任务规格、固定指令及身份、调用现有接口、保存 request_id 和 cursor，区分业务任务与 bridge task。外部模块不代替 bridge 的排队和进程管理，也不自动改 profile verified。

输入为版本化 JSON 规格：logical_task_id、type、difficulty、allowed_files、read_only、snapshot_files、done_when（稳定 id 与自动/人工判断方式）、commands（argv 与超时）、base_commit、max_attempts、reservation_tokens、risk、required_access、instruction。输出规格 hash、dispatch 关联及观察到的事件。type 传入 bridge `requirements.kind`，difficulty 只存外部。

接口：`tiering task validate --spec spec.json`；`tiering dispatch --spec spec.json --profile NAME --model ID`；`tiering collect --task UUID --until settled`；`tiering retry --logical-task UUID --feedback feedback.json`。collect 在程序内 wait，不让模型逐次轮询。正常重试固定原 instruction/dependencies，反馈只走预先约定文件；目标变化创建新 bridge task 并保持业务任务尝试计数。反馈、输入树和版本都要记录。调用结果丢失时用相同 request_id 重试；参数冲突停止，不换 ID 猜测。recovery_needed 只检查并交人工/主 agent 决定 resolve。

离线测试：模拟 queued→assigned→running→completed→review、重复请求、过期 revision、改指令 retry、跨 session 升级、预算预留和启动失败。真实模型测试：一项文本任务连续追问、一次拒绝后原文重试、换模型新 session 后确认历史身份。验收：每次实际尝试可回到原规格和实际配置，无重复执行、无漏计旧 attempt；max_attempts 由外部在派发前检查。

## 4. M1：记录器与补录

职责：验证 schema、锁定写入、去重、修正、补齐 bridge 关联。只在外部目录写，源 bridge 只读。输入为 M0 的事件及 bridge 返回，输出 event_id、持久化确认和缺失关联报告。

接口：`append_event(event) -> event_id`、`reconcile(instance_id, after_seq) -> summary`；CLI `tiering events append --file event.json`、`tiering reconcile --instance bridge-A`、`tiering events check`、`tiering events repair --preserve-tail`。

用外部 lock 文件的 flock 串行保护幂等键检查和追加，文件 600、目录 700；单行序列化后写全、flush、fsync 才确认。小规模可在锁内扫描幂等键，先不加索引。拒绝符号链接和不可信引用。半行先停止，repair 显式保留残片再截尾，不能读时默默跳过并继续写。reconcile 从 intent/request_id 找已创建任务，从事件补 attempt，确认 review 已生效后再记录 accepted。两个系统之间不承诺原子事务。

离线测试：多进程并发写、重复幂等键同内容/异内容、写到一半崩溃、磁盘满、修正冲突、源库丢失、事件重放、instance 混淆。真实模型无需；与真实 broker 的模拟 CLI 集成即可。验收：所有完整行能解析；重复消费不重复计数；任意中断后能列出并补齐缺失项；usage 未被复制进 JSONL。

## 5. M2：L0 验证器

职责：独立执行规格命令，检查修改范围与自述，输出 2.2 格式。外部模块；bridge 只提供工作目录、输入树、结果与补丁。不执行由模型回答临时提供的 shell 字符串。

接口：`tiering validate --spec spec.json --attempt ID --output report.json`；`validate(spec, workspace, claims) -> ValidationReport`。测试使用 argv、受控 cwd、超时和进程组清理；命令白名单由规格作者维护。未配置足够隔离时要求用户确认可信仓库脚本，不能宣称 worktree 是安全沙箱。真实资源调用需单独授权。

比较 snapshot_tree 与最终输出，完整枚举 tracked/index/worktree/untracked；重命名两端和模式变化均检查，禁止符号链接越界。冻结输入输出或执行前后核对树 hash；测试生成文件在临时目录保存，避免误算 Executor 修改。测试若改了受评文件，本次验证为 unknown，不能继续拿旧 diff 评分。人工项不自动通过。

自述取 result artifact，只有明确测试命令、结果和对应输出树才可比较；缺上下文记 unknown。失败仅标 suspected，确认后才写 false_claim 枚举原因。报告保留清理后的短日志/hash，不存完整聊天。

离线测试：越界新增/删除/重命名、带入快照修改、未跟踪/二进制/符号链接、超时、依赖缺失、不稳定测试、虚报与普通失败区分、测试中修改文件。真实模型测试：一个正常实现和一个明确虚报夹具，主 agent 检查自述是否被正确提取。验收：违规都能指出具体文件或条件；unknown 不被算 pass/confirmed false_claim；完全离线任务无推理调用。

## 6. M3：评分协议与 review 辅助

职责：生成固定审查包，校验 2.3 JSON，关联 Reviewer attempt，辅助主 agent 调用 agent_review。Reviewer 不修改代码；推荐与最终决定分开记录。

接口：`tiering review prepare --attempt ID`、`tiering review score --reviewer-profile NAME --model ID`、`tiering review finalize --attempt ID --decision accepted --evidence report.json`。prepare 输出规格、固定 diff、L0 报告 hash；score 以异模型只读 session 执行，不能只比较 profile 名称来判断异模型。Antigravity 无受保证的只读模式，第一版不作为只读 Reviewer；见 `src/agent_bridge/routing.py:107`。

finalize 重新 inspect，确认 current completed、报告对应当前输出树、必审项完成，取得 session_revision 后 agent_review。stale_revision 重新检查；已评不可反向覆盖，外部 correction 不能撤销 bridge accepted。审核完成后 JSONL 写入失败由 M1 补录。Reviewer 自身结果也要由主 agent 检查后评，不再递归请第二个 Reviewer；其 review 以 role=reviewer 记录。

离线测试：评分越界、缺字段、同模型审查、报告树不符、needs_human、stale revision、已失败 attempt、Reviewer 未完成及非法 JSON。真实模型：固定样本异模型审查，少量双评检验一致性，人工确认模型未仅复述 Executor。验收：评分有证据引用，Reviewer 全部尝试费用只进 review_cost；只有主 agent 决定后才 accepted；failed/recovery_needed 不调用 agent_review。

## 7. M4：trailer 与缺陷辅助

职责：准备提交消息、检查 trailer 和最终 commit，查 blame 候选并记录确认。默认外部只生成消息文件和检查清单，不自动 apply/stage/commit；真实合入仍须已有授权，按文件范围操作。

接口：`tiering merge prepare --logical-task ID --attempt ID --out message.txt`、`tiering merge record --commit SHA`、`tiering defect candidates --repo PATH --file REL --lines A,B --at SHA`、`tiering defect confirm --defect ID --caused-by ID --actor NAME`。

输入为 accepted attempt、冻结 diff、目标仓库/基准；输出 Agent-Task（外部 ID）、Agent-Attempt、Agent-Model、Agent-Reviewer 和附加 Agent-Bridge trailer，避免跨实例混淆。人工修改或多个任务混合时 contribution=mixed，拒绝宣称单一模型归因。record 检查实际提交 trailer、完整 SHA 与预期文件/内容，再写 merge；commit 已成功但记录失败时只补录，不再提交。

blame 使用指定版本及行范围，结合 trailer 提候选；确认表记录原因和修复任务，重复 defect_id 不重复计数。格式化、移动、删除行或遗漏代码造成的缺陷不能保证 blame 找到，应允许手工候选。squash/rebase 后通过重新 record/correction 记录新 SHA，不伪造旧 commit。

离线测试：临时仓库 trailer 重复/缺失、合并多个任务、squash/rebase、移动行、人工混改、重复缺陷及错误 repo。无需真实模型；归因准确性用人工标注样本检查。验收：候选不会直接进入缺陷率；真实提交与任务/attempt 一致；缺失 trailer 不能自动归成人工贡献。

## 8. M5：指标汇总

职责：重放 JSONL，读取 bridge 数据，在明确统计窗口内输出 JSON 和 Markdown 报表；不使用 bridge routing.task_samples 代替外部逐尝试统计。输入 events、只读 bridge 来源、截止时间、policy，输出 report.json/report.md、缺失明细、来源 hash。

接口：`tiering report --as-of TIME --dataset online --out DIR`；`aggregate(events, bridge_reader, policy) -> Report`。优先工具读取及已知 attempt_id；超过摘要的历史通过事件补齐，必要时可选只读 SQL reader，使用 mode=ro 和一致读事务，不初始化 Store、不复制数据库。内部 schema 不兼容就停止并报错。

计算规则：

- 按实际配置身份 × type × difficulty × dataset × policy/spec/rubric 版本分桶。一个业务任务只算一次首次；第一次 executor attempt accepted 才是首次通过，重建 bridge task 不重置。另报每模型尝试成功率，不与任务 FPA 混称。
- 原始 FPA、剔除经确认 spec/env 问题后的 FPA 并列；分母、排除数、取消/未结束数均展示。合理求助和用户接受用人工记录，没有则 null。
- 不升级最终通过率只针对配置未切换的任务；返工轮数为 attempts−1。混模型任务的逐 attempt 质量保留，费用不用于单模型比较。
- Executor 单个成功成本 = 窗口内同配置且已结束任务的所有 executor attempts 成本（含最终失败）/ accepted 任务数。无成功为 null；有任一未知费用则完整指标为 null，已知部分仅作参考并显示覆盖率。Reviewer 所有 attempts 单列 review_cost，主会话总费用另列，不能猜测共享父开销分摊。USD 不全时，全桶使用完整 token 指标；不可逐任务混单位。
- 派发至 accepted 的墙钟包含等待和返工，另列运行时间；失败任务耗时单列。并发度按运行区间积分，不从派发数量猜。父会话未计量时编排开销占比为 null。
- 缺陷逃逸按“至少一个已确认缺陷的 accepted 任务数 / 已经过相同观察期的 accepted 任务数”计算，同任务多个 bug 不重复计数。成熟窗口默认 30 天并可配置，未到期单列。
- 展示原始 N、Beta(2,2) 平滑 `(加权成功+2)/(加权总数+4)`、半衰期 30 天及有效样本量。N≥10 只是最低参考，评分覆盖及有效样本量不足仍 Unmeasured。不以差异很小的均值声称模型更好。

离线测试：手算小数据、重试/升级/改规格、未知价格、共享父成本、重复事件、跨窗口任务、时间衰减、零分母、迟到缺陷和 correction。真实模型不是计算正确性的必要条件；采集的字段完整性需少量真实任务核对。验收：示例可手工复算；每个指标能列出入选/排除的任务 ID；未知不变零，校准与线上不混合，Reviewer 成本不进执行者单位成功成本。

## 9. M6：路由生成与查询

职责：从 M5 生成 2.4 路由表、解释推荐；查询时合并实时 catalog 与任务权限要求。外部执行，不改 default_strategy，不向 requirements 塞入 difficulty 等未知字段。

接口：`tiering route build --report report.json --mode advisory --out routes.json`、`tiering route query --spec spec.json --table routes.json`。输出候选、推荐、原因、表版本、是否仅供参考。M0 最终显式 profile/model 派发；因此 M6 必须自己检查 verified、准确模型身份、只读支持、输入类型/上下文、供应商、quota 和表有效期，不能依赖显式派发自动补做所有筛选。目录不新鲜/新模型未验证则需要人工明确选择或校准，不继承旧 verified。

默认 advisory：统一阈值只显示警告，不剔除，不宣称已正式分级。初始等级由主 agent 选择。未来样本与评分足够且另行启用 active 策略后，才按批准的规则筛选；有完整可比费用与耗时才用 0.5×费用/min费用 + 0.5×耗时/min耗时，全部候选统一单位，未知则 score=null，采用已验证候选固定顺序并说明 Unmeasured。不能把外部分数当 bridge 原生 quality。

探索必须明确启用，只在允许风险和预算的候选里按约 10% 抽样，记录概率和种子；高风险任务不为补探索比例降级。表过期、候选全失效、文件 hash 不符或无安全候选时返回 requires_manual，不自动换供应商。旧表可留供查看，但不能默默执行旧推荐。

离线测试：未知价格、新模型同名配置、候选失效、过期表、quota 等待、权限不符、固定随机种子、全候选无评分。真实模型：一次完整推荐派发后核对实际 profile/model；路由收益需足量配对任务，单次不作性能结论。验收：解释能回到样本和策略；默认不按阈值淘汰；显式选择仍满足权限；没有把未知费用排成最便宜。

## 10. M7：校准任务运行器

职责：在固定仓库版本和隔离目录运行版本化任务集，逐任务调用 M0–M3、M5；仅收集数据，不修改桥接配置 verified。输入 dataset manifest：dataset_version、case_id、repo/base、spec、输入/预期结果 hash、judge、timeout、risk、seed。输出 dataset=calibration 事件、每例验证与评分、独立校准报告。

接口：`tiering calibrate plan --suite suite.json --profiles profiles.json` 只列计划；`tiering calibrate run --plan plan.json --max-attempts 1 --max-concurrency 2`；`tiering calibrate resume --run ID`。真实运行须已有模型调用和费用授权；未来周期执行可由外部定时工具调用，第一版不创建 timer。

每个模型/任务用干净工作目录，固定规格/测试/样本顺序并记录实际模型 ID；独立判定器和隐藏保留集不交 Executor 修改。research 可指定人工 judge，不强迫测试自动判断研究质量。设置最大任务数/attempt 数/时间、token 预留和停止条件；这些是停止后续派发的限制，不是原生硬费用保证。恢复先核对已存在 request_id/终态，recovery_needed 停下来检查，不重跑未确认任务。

离线测试：fake CLI 完整运行、部分完成恢复、重复启动、预算停止、模型版本变化、判定器被修改、保留集泄露、人工判断待定。真实模型：先 2–3 例核对协议及费用完整性，再跑 20–40 例；同版本多模型对比须保留失败，不挑结果。验收：离线任务可重复得到同一判断；线上每个模型调用都有 attempt 关联；真实得分与离线流程验证分开报告，不能把模拟 CLI 通过写成模型已验证。

## 11. 与 bridge 九个工具的调用关系

工具签名以 `src/agent_bridge/mcp_server.py:17`–`:112` 为准；外部脚本通过 `agent-bridge --state STATE call TOOL --json JSON` 或对应 MCP 调用，不新增 bridge 工具。

| 工具 | 外部调用者 | 用法及约束 |
|---|---|---|
| agent_catalog | M0、M6、M7 | 读 profiles/discovery、verified、objective、quota；refresh 按需，不发生推理。 |
| agent_open | M0、M3、M7 | 准确 profile/model、access、base/snapshot_files 建 session；换模型开新 session，返回 workspace/profile 记关联。 |
| agent_dispatch | M0、M3、M7 | instruction 原文、稳定 request_id、requirements.kind、reservation；dependencies 用 bridge task UUID，等待 accepted 后才运行；retry_task_id 保留原指令。 |
| agent_control | M0、M7 | inspect 后带 revision 执行 respond/pause/resume/close；resolve 需已有检查及证据，不猜结果；父计量登记/停止可选。 |
| agent_cancel | M0、M7 | 达停止条件取消指定任务；随后检查终态与遗留进程，不把请求成功当作执行已停。 |
| agent_inspect | M0–M7 中需要源数据者 | 查任务/session/profile/revision/waiting_reason/questions/budget；M2/M4 通过 M0 reader 取，不重复实现。 |
| agent_wait | M0、M1、M7 | 程序内持久 cursor 收集事件，最长单次 50 秒，超时只表示无新事件；恢复重放去重。 |
| agent_result | M0、M2、M3、M5 | 指定 attempt 取历史，长结果按 artifact next_offset 读；默认最近 20 次不能当完整历史。 |
| agent_review | M3，M0 负责传输 | 主 agent 已核查后提交 accepted/rejected、非空 evidence、当前 revision 和稳定 request_id；原因及评分由 M1 写外部事件。 |

## 12. 整体交付检查

先用临时仓库和 fake CLI 完成一条含拒绝重试、Reviewer、trailer、缺陷候选及报表的离线流程，核对 JSONL 关联、费用未知处理和幂等恢复。再在获准的真实模型上验证一次完整任务及一次恢复过程，单独报告模型、实际 attempt、源状态覆盖和费用完整性。第一版通过条件是记录和判断可信、失败能解释、没有越过授权范围；是否节省费用必须等校准及线上可比数据，不能由实现完成直接推出。
