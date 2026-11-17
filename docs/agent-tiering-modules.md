> Language: English | [中文](zh/agent-tiering-modules.md)

# Agent Tiering Module Development Design

This document expands the draft's P0–P5 stages into development tasks for an external Python package, `agent-tiering`. **All commands below are proposed interfaces and cannot yet be run.** This is the broader module proposal; the later [basic-scope review](agent-tiering-basic-review.md) recommends a smaller first version. It is not a claim that these modules are implemented or required by the current Bridge release.

The proposed first version is single-machine, single-user, and file-based. It adds no service and changes neither Bridge core, configuration, nor database. It retains the draft's decisions: JSONL at `~/.local/state/agent-tiering/events.jsonl`, separate Reviewer cost, defect attribution only after confirmation, and display-only reference thresholds without automatic elimination.

## 1. Modules and order

Estimates assume one developer familiar with Python and this repository. They include offline tests and documentation, but exclude model queues, spending approval, and repeated calibration runs.

| Order | Module | Phase | Dependencies | Effort |
| --- | --- | --- | --- | --- |
| 1 | M0 Task specification and Bridge calls | P0 | None | 2–3 person-days |
| 2 | M1 JSONL recording and missing associations | P0 | M0 data definitions; can be developed together | 2–3 person-days |
| 3 | M2 Deterministic L0 validator | P1 | M0, M1 | 3–4 person-days |
| 4 | M3 Reviewer scores and final review | P2 | M2, M1 | 2–3 person-days |
| 5 | M4 Commit trailers and defect association | After P2, before P3 | M1, M2, M3 | 1–2 person-days |
| 6 | M5 Metrics reports | P3 | M1–M4; defect data may be empty | 3–4 person-days |
| 7 | M6 Routing-table generation and queries | P4 | M5, M0 | 2–3 person-days |
| 8 | M7 Calibration-suite runner | P5 | M0–M3, M5, M6 | 2–3 person-days plus task authoring |

Total estimate: 17–25 person-days. First complete a minimal dispatch -> bind execution -> independently validate -> human decision -> record example, then add scores, reports, and routing. Start the runner with a few calibration fixtures and gradually author the full 20–40 tasks. Do not hide suite-authoring cost inside the runner estimate. All modules are subcommands of the external package; none requires modifying Bridge.

## 2. Shared data formats

### 2.1 Identifiers and events

`logical_task_id` is an externally assigned opaque UUID; `task_key` is a readable name; `bridge_task_id` is the UUID returned by dispatch. Do not interchange them. Retry retains `bridge_task_id`. Changing the goal creates a new Bridge task while retaining `logical_task_id` and advancing `spec_version` and `supersedes`.

An attempt's unique reference is `(bridge_instance_id, attempt_id)`, checked against its Bridge task and session. Register `bridge_instance_id` externally against a specific state-database source. A copied database is not automatically a new run; rebuilding the database requires new registration. A mutable directory path is not a unique instance identity.

Each line is a complete UTF-8 JSON object without comments. Every event requires `schema_version=1`, `event_id` (UUID), `event`, `ts` (UTC RFC3339), `logical_task_id`, `project_id`, `producer`, and `idempotency_key`. Execution events include `bridge_instance_id`, `bridge_task_id`, `session_id`, and `attempt_id` as known. An ID not yet generated is `null`, never guessed. Example:

```json
{"schema_version":1,"event_id":"<uuid>","event":"attempt","ts":"2026-10-28T04:00:00Z","logical_task_id":"<uuid>","project_id":"repo-01","producer":"tiering/1","idempotency_key":"bridge-A:seq:42","bridge_instance_id":"bridge-A","bridge_task_id":"<uuid>","session_id":"<uuid>","attempt_id":"<uuid>","bridge_event_seq":42,"role":"executor","spec_version":1,"profile":"codex-main","model":"<exact-id>","identity":{"runtime":"codex","provider":"openai","effort":"high","service_tier":"priority","config_digest":"<non-secret-sha256>"},"outcome":"assigned"}
```

| `event` | Event-specific required fields |
| --- | --- |
| `task` | `task_key`, `type`, `difficulty` (1–5), `tier` (L1–L3), `dataset` (online/calibration), `spec_version`, `spec_hash`, `policy_version` |
| `dispatch` | `phase` (intent/bound), `dispatch_request_id`, `instruction_hash`, `role`, `supersedes`; bound requires `bridge_task_id`/`session_id`, intent may use null |
| `attempt` | `role` (executor/reviewer), `bridge_event_seq`, `outcome`, actual configuration identity, complete associated IDs |
| `validation` | Attempt reference, `report_ref`, `report_sha256`, `input_tree`, `output_tree_hash`, `status` |
| `review` | Subject attempt reference, `role`, `decision` (accepted/rejected/unreviewed), `reject_reason`, `checks_ref`, `score_ref`, `reviewer_attempt_ref`, `bridge_review_event_seq`; unreviewed requires `outcome`; accepted/rejected must match Bridge |
| `merge` | Bridge/attempt references, `repo_id`, full commit SHA, tree SHA, `trailer_verified`, `contribution` (single/mixed) |
| `defect` | `defect_id`, `fix_logical_task_id`, `caused_by` (original logical task), `candidate_commits`, `status` (candidate/confirmed/dismissed), `confirmed_by`, `confirmed_at`, `reason_ref`; unconfirmed defects never enter the escape-rate numerator |
| `route` | `table_hash`, `candidate_ids`, `chosen_id`, `mode`, `reason`, `seed`, `selection_probability`, `policy_version` |
| `correction` | `target_event_id`, `patch`, `reason`, `actor`; allow only classification/reason/manual-confirmation fields, never event ID/type, Bridge IDs, or usage |

Compared with the earlier four-event draft, `dispatch`, `attempt`, `validation`, `route`, and `correction` cover failures without review, missing associations, and repeatable statistics. Use one format even for small datasets rather than adding a second database. A correction may target only a preceding non-correction event. Apply multiple corrections in file order while retaining the original and all corrections. Unknown `schema_version` prevents scoring and reports the unsupported-record count.

`reject_reason` uses eight values: `spec_unclear`, `wrong_logic`, `incomplete`, `tests_fail`, `false_claim`, `out_of_scope`, `style`, `env_failure`; use null for acceptance. Excluding an executor failure also requires `attribution`, `confirmed_by`, and `reason_ref`, rather than automatic exemption from an enum. Cancellation and uncertain results have their own `outcome`, not an invented rejection.

Read usage and duration from Bridge rather than copying them into events. Reports include aggregate values, source summary, `as_of`, and missing-data fraction; missing sources show `missing_source`. Keep evidence files in external private `reports/`; JSONL stores relative references, hashes, and short conclusions. References cannot escape the private directory. Do not persist raw chats, credentials, login data, or private database copies.

### 2.2 L0 validation-result JSON

```json
{
  "schema_version":1,"attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<uuid>"},
  "spec_hash":"<sha256>","input_tree":"<snapshot-tree>","output_tree_hash":"<sha256>",
  "status":"fail","tests":[{"id":"unit-a","argv":["python","-m","unittest","tests.test_a"],"exit_code":1,"status":"fail","duration_ms":300,"evidence_ref":"reports/test-a.txt","evidence_sha256":"<sha256>"}],
  "diff":{"status":"pass","out_of_scope_files":[],"changed_files":["src/a.py"],"complete":true},
  "claims":[{"id":"claim-1","kind":"tests_pass","test_id":"unit-a","source_ref":"<artifact-id>","verdict":"suspected","reason":"Independent execution failed on the same output tree"}],
  "done_when":[{"id":"criterion-1","status":"fail","evidence_ref":"reports/test-a.txt"}],
  "false_claim":"suspected","errors":[]
}
```

Overall `status` is pass/fail/unknown. Tests may also be timeout/skipped/error; `done_when` may be manual_pending. `false_claim` is none/suspected/confirmed/unknown; confirmed requires a human or primary-agent confirmation reference. A null exit code cannot pass. Unexecuted checks or incomplete evidence are unknown. Overall pass requires all mandatory conditions, including confirmed manual checks. Validator output does not automatically replace user acceptance.

### 2.3 Reviewer-score JSON

```json
{
  "schema_version":1,"subject_attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<executor-uuid>"},
  "reviewer_attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<reviewer-uuid>"},
  "spec_hash":"<sha256>","diff_hash":"<sha256>","rubric_version":"1",
  "scores":{"correctness":4,"completeness":4,"code_quality":3},
  "recommendation":"accept","reject_reason":null,"confidence":"medium",
  "findings":[],"limitations":["External services not verified"],"evidence_refs":["reports/l0.json"]
}
```

Use integers 1–5 with fixed descriptions: 1 fails the core goal; 2 has important omissions; 3 meets explicit requirements with fixable issues; 4 is complete with sufficient evidence; 5 is equally supported and handles relevant exceptional cases. Do not expand task scope to justify a five. For research/documents, `code_quality` is null with an explanation, not an automatic maximum score.

Each finding includes `severity`, `file`, `line`, `trigger`, `consequence`, and `evidence_ref`. `recommendation` is accept/reject/needs_human and is not a Bridge decision. Invalid structured output gets at most one repair request; the extra attempt remains Reviewer cost and cannot silently remove a poor score.

### 2.4 Routing-table JSON

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
    "seconds_per_accept":null,"score":null,"eligible":true,"reasons":["Too few samples; use the initial tier"]
  }],"fallback_tier":"L2"}]
}
```

Candidates may have descriptive statistics, but insufficient samples require `score=null`. `eligible` means permission/input/verification requirements are satisfied; provisional thresholds cannot automatically exclude a candidate. Unknown price cannot be zero. This package's `Unmeasured` field does not imply Bridge returns a same-named state field.

## 3. M0: Specifications and Bridge calls

Validate the specification, preserve instruction and identities, call existing tools, retain `request_id`/cursor, and distinguish logical tasks from Bridge tasks. This module does not replace Bridge queues/process management or automatically change profile `verified`.

Input is versioned JSON containing `logical_task_id`, `type`, `difficulty`, `allowed_files`, `read_only`, `snapshot_files`, `done_when` (stable IDs and automatic/manual judges), `commands` (argv/timeouts), `base_commit`, `max_attempts`, `reservation_tokens`, `risk`, `required_access`, and `instruction`. Output is the specification hash, dispatch association, and observed events. Send type as Bridge `requirements.kind`; keep difficulty external.

Proposed commands: `tiering task validate --spec spec.json`; `tiering dispatch --spec spec.json --profile NAME --model ID`; `tiering collect --task UUID --until settled`; `tiering retry --logical-task UUID --feedback feedback.json`.

Collect waits inside the program, without repeated model polling. Ordinary retry fixes the original instruction/dependencies and passes feedback only through the agreed file. Changed goals create a new Bridge task without resetting logical-task attempt counts. Record feedback, input tree, and version. Retry a lost response with the same `request_id`; stop on a parameter conflict instead of guessing with a new ID. Inspect `recovery_needed` and leave `resolve` to the human/primary agent.

Offline tests cover queued -> assigned -> running -> completed -> review, identical requests, stale revision, retry with changed instructions, cross-session escalation, budget reservation, and startup failure. Authorized live tests cover a text follow-up, rejection followed by unchanged-instruction retry, and a new session after model change with retained historical identity. Completion requires every actual attempt to resolve to its original spec/configuration, no duplicate execution or lost old attempt, and an external `max_attempts` check before dispatch.

## 4. M1: Recording and reconciliation

Validate schema, lock writes, deduplicate, apply corrections, and fill missing Bridge associations. Write only to the external directory; read the source Bridge without changes. Input is M0 events and Bridge replies; output is `event_id`, durable-write acknowledgment, and missing-association reports.

Interfaces: `append_event(event) -> event_id`, `reconcile(instance_id, after_seq) -> summary`; proposed CLI: `tiering events append --file event.json`, `tiering reconcile --instance bridge-A`, `tiering events check`, `tiering events repair --preserve-tail`.

An external lock file with `flock` serializes the idempotency check and append. Files use 0600 and directories 0700. Serialize/write the complete line, flush, and fsync before acknowledgment. At small scale scan keys under the lock without an index. Reject symlinks and untrusted references. Stop on an incomplete line; explicit repair preserves its fragment before truncating, rather than silently skipping and writing more. Reconcile finds created tasks by intent/request ID, adds attempts from events, and records acceptance only after confirming Bridge review. No atomic transaction across the two stores is promised.

Offline tests: multiprocess writes, identical/different payloads under the same key, mid-write crash, disk full, correction conflicts, missing source DB, replay, and instance confusion. No real model is needed; integration with a real broker and fake CLI is sufficient. Completion requires parseable complete lines, no duplicate counts on replay, discoverable/reconcilable missing entries after interruption, and no copied usage in JSONL.

## 5. M2: Deterministic L0 validator

Independently run specification commands, check changed-file scope and claims, and output section 2.2. Bridge supplies directory, input tree, result, and patch. Never run shell strings newly supplied by a model answer.

Interfaces: `tiering validate --spec spec.json --attempt ID --output report.json`; `validate(spec, workspace, claims) -> ValidationReport`. Tests use argv, controlled cwd, timeouts, and process-group cleanup. The spec author maintains the allowed commands. Without sufficient isolation, ask the user to confirm trusted repository scripts; do not claim a worktree is a security sandbox. Real resource calls need separate authorization.

Compare `snapshot_tree` with final output and enumerate tracked/index/worktree/untracked changes completely. Check both sides of renames and permission changes; prohibit symlink escape. Freeze input/output or compare tree hashes before/after. Put generated test files in temporary directories so they are not mistaken for execution-agent changes. If a test changes evaluated files, mark this validation unknown rather than scoring the old diff. Manual conditions never pass automatically.

Extract claims from result artifacts. Compare only when the test command, result, and matching output tree are explicit; missing context is unknown. Failure alone is suspected; a confirmed false-claim reason requires confirmation. Keep sanitized short logs/hashes, not full chats.

Offline tests cover out-of-scope additions/deletions/renames, preexisting snapshot edits, untracked/binary/symlink files, timeouts, missing dependencies, flaky tests, false claims versus ordinary failures, and file changes during tests. Authorized live checks use a normal implementation and a clearly labeled false-claim fixture; the primary agent checks extraction. Completion requires a concrete file/condition per violation, unknown never becoming pass/confirmed false claim, and no inference in fully offline tasks.

## 6. M3: Scoring and review assistance

Build a fixed review package, validate section 2.3 JSON, link the Reviewer attempt, and help the primary agent call `agent_review`. The Reviewer does not edit code. Store recommendation separately from final decision.

Commands: `tiering review prepare --attempt ID`, `tiering review score --reviewer-profile NAME --model ID`, `tiering review finalize --attempt ID --decision accepted --evidence report.json`. Prepare outputs the specification, fixed diff, and L0-report hash. Score uses a different model in a read-only session; different profile names alone do not establish different models. Antigravity has no guaranteed read-only mode and is excluded from first-version read-only review; see [routing.py](../src/agent_bridge/routing.py).

Finalize reinspects the current completed attempt, matches the report to the current output tree, confirms required reviews, obtains `session_revision`, and calls `agent_review`. A `stale_revision` requires rechecking. An existing review cannot be reversed; external correction cannot undo Bridge acceptance. M1 reconciles a failed JSONL write after saved review. The primary agent also checks the Reviewer's own result without recursively creating another Reviewer; record that review as `role=reviewer`.

Offline tests cover score range, missing fields, same-model review, mismatched trees, needs_human, stale revision, failed attempts, incomplete Reviewer work, and invalid JSON. Authorized live tests use fixed samples and a different model, a small dual-review sample, and human confirmation that the Reviewer did more than repeat the executor. Completion requires evidence references, all Reviewer attempts counted only under `review_cost`, acceptance only after the primary agent decides, and no `agent_review` for failed/recovery-needed work.

## 7. M4: Commit trailers and defects

Prepare commit messages, check trailers/final commits, find blame candidates, and record confirmations. By default only generate message files/checklists; never automatically apply, stage, or commit. Real merging still needs existing authorization and scoped file operations.

Commands: `tiering merge prepare --logical-task ID --attempt ID --out message.txt`, `tiering merge record --commit SHA`, `tiering defect candidates --repo PATH --file REL --lines A,B --at SHA`, `tiering defect confirm --defect ID --caused-by ID --actor NAME`.

Input is an accepted attempt, frozen diff, target repository/base. Output includes `Agent-Task` (external ID), `Agent-Attempt`, `Agent-Model`, `Agent-Reviewer`, and additional `Agent-Bridge` trailers to distinguish instances. Human edits or combined tasks require `contribution=mixed`; do not attribute the commit to one model. Record checks actual trailers, full SHA, and expected files/content before writing merge. If the commit succeeded but recording failed, reconcile only; do not commit again.

Use blame at the specified revision/line range and trailers to suggest candidates. Confirmation records the cause and fix task; repeated `defect_id` counts once. Formatting, moved/deleted lines, and omitted code can defeat blame, so allow manual candidates. After squash/rebase, record the new SHA through record/correction without inventing old commits.

Offline tests use temporary repositories for duplicate/missing trailers, combined tasks, squash/rebase, moved lines, human edits, duplicate defects, and wrong repositories. No real model is needed; check attribution against manually labeled samples. Candidates must not enter defect rate directly, real commits must match task/attempt, and missing trailers cannot automatically mean human contribution.

## 8. M5: Metrics reports

Replay JSONL and read Bridge data in an explicit reporting window to produce JSON/Markdown. Do not substitute Bridge `routing.task_samples` for external attempt-level statistics. Inputs are events, read-only Bridge sources, cutoff, and policy; outputs are report.json/report.md, missing-data details, and source hashes.

Interfaces: `tiering report --as-of TIME --dataset online --out DIR`; `aggregate(events, bridge_reader, policy) -> Report`. Prefer tools and known `attempt_id` values. Complete history beyond summaries from events. If necessary, an optional SQL reader uses `mode=ro` and a consistent read transaction, never initializing Store or copying the database. Stop with an error on incompatible internal schema.

Calculation rules:

- Group by actual configuration × type × difficulty × dataset × policy/spec/rubric versions. One logical task counts once for first-pass acceptance (FPA): its first executor attempt must be accepted, and rebuilding a Bridge task does not reset that. Report per-model attempt success separately.
- Show raw FPA beside FPA excluding confirmed spec/environment problems, with denominators, exclusions, cancellations, and unfinished counts. Justified help and user acceptance require manual records; otherwise null.
- Final acceptance without escalation includes only tasks whose configuration did not change. Rework is `attempts - 1`. Preserve attempt quality in mixed-model tasks, but exclude their costs from single-model comparisons.
- Executor cost per accepted task is all executor-attempt cost, including ultimate failures, for ended tasks of that configuration in the window, divided by accepted task count. No acceptance yields null. Any unknown cost makes the complete metric null; display known partial cost only as reference with coverage. Count all Reviewer attempts separately under `review_cost` and primary total cost separately. Never guess allocation of shared-parent overhead. With incomplete USD, use complete token metrics for the entire group, without mixing units per task.
- Dispatch-to-acceptance elapsed time includes waiting/rework. Also report runtime and failed-task duration separately. Integrate running intervals to measure concurrency; do not infer it from dispatch counts. Primary overhead fraction is null without parent metering.
- Defect escape rate is accepted tasks with at least one confirmed defect divided by accepted tasks that have completed an equal observation period. Multiple bugs in one task count once. The default maturity window is configurable at 30 days; show immature tasks separately.
- Show raw N, `Beta(2,2)` smoothing `(weighted_successes + 2) / (weighted_total + 4)`, 30-day half-life, and effective sample size. `N >= 10` is only a minimum reference; inadequate score coverage/effective samples still yield `Unmeasured`. Tiny differences in means do not establish a better model.

Offline tests use hand-calculated datasets, retry/escalation/spec changes, unknown prices, shared-parent costs, duplicate events, cross-window tasks, decay, zero denominators, late defects, and corrections. Real models are unnecessary to prove arithmetic, but a few real tasks must check field completeness. Completion requires hand-reproducible examples, included/excluded task IDs per metric, unknown values remaining unknown rather than zero, separate calibration/online data, and no Reviewer cost in executor cost per acceptance.

## 9. M6: Routing-table generation and queries

Generate section 2.4 tables from M5 and explain recommendations; combine current catalog data with task permissions at query time. This is external execution: do not change `default_strategy` or put unknown fields such as difficulty into `requirements`.

Commands: `tiering route build --report report.json --mode advisory --out routes.json`, `tiering route query --spec spec.json --table routes.json`. Return candidates, recommendation, reasons, table version, and whether advisory. M0 dispatches an explicit profile/model, so M6 must independently check verified status, exact model identity, read-only support, input/context, provider, quota, and table lifetime. Explicit dispatch does not perform every automatic-selection filter for it. Stale catalogs/unverified new models need explicit human choice or calibration; never inherit an old verified flag.

Default advisory thresholds show warnings without exclusions or claims of formal model tiers. The primary agent chooses the initial tier. Only a separately enabled future active policy with adequate samples/scores may filter according to approved rules. With comparable complete costs/times, use `0.5 * cost / min_cost + 0.5 * duration / min_duration`, one unit across candidates. Unknown values require `score=null`, a fixed verified-candidate order, and an explanation of `Unmeasured`. External scores are not Bridge-native quality.

Exploration requires explicit enablement and samples about 10% only among allowed-risk/budget candidates, recording probability and seed. Do not downgrade high-risk tasks to fill a quota. Expired tables, all-invalid candidates, hash mismatch, or no safe candidate return `requires_manual`, without automatically changing provider. Old tables may remain readable but cannot silently drive execution.

Offline tests cover unknown prices, new models under the same profile name, invalid candidates, expired tables, quota waits, permission mismatch, fixed seeds, and no scored candidates. A live authorized dispatch checks actual profile/model. Routing benefits require sufficient paired tasks; one run is not a performance conclusion. Explanations must trace to samples/policy; default thresholds do not eliminate; explicit choice still meets permissions; unknown cost is not cheapest.

## 10. M7: Calibration-suite runner

Run a versioned task suite at fixed repository revisions in isolated directories through M0–M3 and M5. Collect data only; do not change Bridge profile `verified`. The dataset manifest contains `dataset_version`, `case_id`, repo/base, spec, input/expected-result hashes, judge, timeout, risk, and seed. Outputs are `dataset=calibration` events, per-case validation/scores, and a separate calibration report.

Commands: `tiering calibrate plan --suite suite.json --profiles profiles.json` lists the plan only; `tiering calibrate run --plan plan.json --max-attempts 1 --max-concurrency 2`; `tiering calibrate resume --run ID`. Real runs require existing model-call/spending authorization. An external timer may call it in future; the first version creates no timer.

Each model/task gets a clean directory, fixed spec/tests/sample order, and recorded actual model ID. Do not let the executor edit independent judges or hidden holdouts. Research may use a human judge instead of pretending tests determine research quality. Limit tasks, attempts, time, token reservations, and stopping conditions; these stop subsequent dispatches and are not native hard-spending guarantees. Resume first checks existing request IDs/terminal states; stop to inspect `recovery_needed` rather than rerunning uncertain work.

Offline tests cover a complete fake-CLI run, partial-run resume, duplicate starts, budget stop, model-version changes, modified judges, holdout leakage, and pending human decisions. Start authorized live checks with 2–3 cases to verify protocol/cost completeness, then 20–40. Retain failures in same-version multi-model comparisons without selecting favorable results. Completion requires repeatable offline judgments, an attempt reference for every live model call, and separate reporting of real scores versus fake-CLI process checks. A passing fake CLI does not verify a model.

## 11. Calls to the nine original Bridge tools

Use the current signatures in [mcp_server.py](../src/agent_bridge/mcp_server.py), not stale line-number ranges. External scripts invoke `agent-bridge --state STATE call TOOL --json JSON` or the matching MCP tool; no new Bridge tool is proposed here.

| Tool | External caller | Use and restrictions |
| --- | --- | --- |
| `agent_catalog` | M0, M6, M7 | Read profiles/discovery, verified, objective, quota; refresh as needed without inference |
| `agent_open` | M0, M3, M7 | Exact profile/model, access, base/snapshot files; new session for model change; record returned workspace/profile association |
| `agent_dispatch` | M0, M3, M7 | Original instruction, stable request ID, `requirements.kind`, reservation; dependencies are Bridge task UUIDs and wait for acceptance; `retry_task_id` retains instruction |
| `agent_control` | M0, M7 | Inspect then revision-bound respond/pause/resume/close; resolve only with checks/evidence; optional parent-meter registration/stop |
| `agent_cancel` | M0, M7 | Cancel the selected task at a stop condition, then check terminal state and remaining processes; request success is not confirmed stop |
| `agent_inspect` | M0–M7 as needed | Task/session/profile/revision/waiting reason/questions/budget; M2/M4 use the M0 reader instead of another implementation |
| `agent_wait` | M0, M1, M7 | Persist cursor inside the program, wait at most 50 seconds per call; timeout means no new event; deduplicate replay on resume |
| `agent_result` | M0, M2, M3, M5 | Select historical attempts; paginate artifacts with `next_offset`; the default recent 20 is not complete history |
| `agent_review` | M3 through M0 transport | Primary-agent checked accepted/rejected, nonempty evidence, current revision, stable request ID; M1 records reasons/scores externally |

## 12. Overall delivery checks

First use a temporary repository and fake CLI for a complete offline case including rejection/retry, Reviewer, trailer, defect candidates, and reports. Check JSONL associations, unknown-cost handling, and idempotent recovery. Then use authorized real models for one complete task and one recovery case, reporting model, actual attempts, source-data coverage, and cost completeness separately. The first-version criterion is trustworthy records/judgments, explained failures, and actions within authorization. Cost savings require calibration and comparable live data; implementation completion alone cannot establish them.
