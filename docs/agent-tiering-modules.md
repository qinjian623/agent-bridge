> Language: English | [中文](zh/agent-tiering-modules.md)

# Agent Tiering Module Development Design

Breaks down Section 8 (`P0-P5`) of the design draft into subcommands of an external `agent-tiering` Python package. Commands below describe the planned external interface (not yet implemented). v1 runs single-host, single-user on local files with no daemon changes to Bridge core, configuration, or SQLite schema. Retains settled decisions from Section 9: JSONL stored at `~/.local/state/agent-tiering/events.jsonl`; Reviewer costs accounted separately; defect attribution requires explicit bug confirmation; reference thresholds are advisory only.

## 1. Modules and Implementation Sequence

Effort estimates assume one developer familiar with Python and this repository, including offline tests and documentation (excluding real model queue wait times, spend approvals, or multi-round calibration runs):

| Order | Module | Phase | Dependencies | Estimate |
|---|---|---|---|---|
| 1 | M0 Spec & Bridge RPC wrapper | P0 | None | 2-3 person-days |
| 2 | M1 JSONL logger & deferred reconciliation | P0 | M0 schema (developable together) | 2-3 person-days |
| 3 | M2 Deterministic L0 validator | P1 | M0, M1 | 3-4 person-days |
| 4 | M3 Reviewer scoring & final review | P2 | M2, M1 | 2-3 person-days |
| 5 | M4 Commit trailer & defect candidate helper | Between P2/P3 | M1, M2, M3 | 1-2 person-days |
| 6 | M5 Metrics aggregation report | P3 | M1-M4 (defect data optional) | 3-4 person-days |
| 7 | M6 Routing table builder & query | P4 | M5, M0 | 2-3 person-days |
| 8 | M7 Calibration suite runner | P5 | M0-M3, M5, M6 | 2-3 person-days (+ suite authoring) |

Total ~17-25 person-days. Implement a minimal end-to-end "dispatch -> bind -> validate -> decide -> log" slice first before adding scoring, reports, and routing. None of these modules require changes to `cli-agent-bridge` core.

## 2. Shared Data Schemas

### 2.1 Identifiers and Events

`logical_task_id` is an externally generated opaque UUID; `task_key` is a human-readable alias; `bridge_task_id` is the UUID returned by `agent_dispatch`. Retries preserve `bridge_task_id`; modifying requirements creates a new `bridge_task_id` preserving `logical_task_id` while advancing `spec_version` and `supersedes`. Unique attempt identity is `(bridge_instance_id, attempt_id)` cross-checked against its task and session (`bridge_instance_id` binds a specific state DB identity without assuming mutable directory paths).

Every line is a standalone UTF-8 JSON object requiring: `schema_version=1`, `event_id` (UUID), `event`, `ts` (UTC RFC3339), `logical_task_id`, `project_id`, `producer`, `idempotency_key`, plus execution IDs (`null` until materialized, never guessed):

```json
{"schema_version":1,"event_id":"<uuid>","event":"attempt","ts":"2026-10-28T04:00:00Z","logical_task_id":"<uuid>","project_id":"repo-01","producer":"tiering/1","idempotency_key":"bridge-A:seq:42","bridge_instance_id":"bridge-A","bridge_task_id":"<uuid>","session_id":"<uuid>","attempt_id":"<uuid>","bridge_event_seq":42,"role":"executor","spec_version":1,"profile":"codex-main","model":"<exact-id>","identity":{"runtime":"codex","provider":"openai","effort":"high","service_tier":"priority","config_digest":"<non-secret-sha256>"},"outcome":"assigned"}
```

Event types and specific required fields:
- `task`: `task_key`, `type`, `difficulty` (1-5), `tier` (`L1`-`L3`), `dataset` (`online`/`calibration`), `spec_version`, `spec_hash`, `policy_version`
- `dispatch`: `phase` (`intent`/`bound`), `dispatch_request_id`, `instruction_hash`, `role`, `supersedes` (`bridge_task_id`/`session_id` required once `bound`)
- `attempt`: `role` (`executor`/`reviewer`), `bridge_event_seq`, `outcome`, resolved config identity
- `validation`: attempt ref, `report_ref`, `report_sha256`, `input_tree`, `output_tree_hash`, `status`
- `review`: subject attempt ref, `role`, `decision` (`accepted`/`rejected`/`unreviewed`), `reject_reason`, `checks_ref`, `score_ref`, `reviewer_attempt_ref`, `bridge_review_event_seq`
- `merge`: attempt ref, `repo_id`, full commit SHA, tree SHA, `trailer_verified`, `contribution` (`single`/`mixed`)
- `defect`: `defect_id`, `fix_logical_task_id`, `caused_by`, `candidate_commits`, `status` (`candidate`/`confirmed`/`dismissed`), `confirmed_by`, `confirmed_at`, `reason_ref`
- `route`: `table_hash`, `candidate_ids`, `chosen_id`, `mode`, `reason`, `seed`, `selection_probability`, `policy_version`
- `correction`: `target_event_id`, allowlisted `patch`, `reason`, `actor` (never mutates `event_id`, `event`, Bridge IDs, or usage)

`reject_reason` enum: `spec_unclear`, `wrong_logic`, `incomplete`, `tests_fail`, `false_claim`, `out_of_scope`, `style`, `env_failure` (`null` on `accepted`; external exemptions require explicit attribution). Usage/duration stay sourced from Bridge cleanly rather than duplicated into JSONL. Report evidence stays in mode-`0700` local `reports/` (`0600` files) referenced by relative paths and SHA-256 digests without storing raw transcripts or credentials.

### 2.2 L0 Validation JSON

```json
{
  "schema_version":1,"attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<uuid>"},
  "spec_hash":"<sha256>","input_tree":"<snapshot-tree>","output_tree_hash":"<sha256>",
  "status":"fail","tests":[{"id":"unit-a","argv":["python","-m","unittest","tests.test_a"],"exit_code":1,"status":"fail","duration_ms":300,"evidence_ref":"reports/test-a.txt","evidence_sha256":"<sha256>"}],
  "diff":{"status":"pass","out_of_scope_files":[],"changed_files":["src/a.py"],"complete":true},
  "claims":[{"id":"claim-1","kind":"tests_pass","test_id":"unit-a","source_ref":"<artifact-id>","verdict":"suspected","reason":"Independent rerun failed on identical tree"}],
  "done_when":[{"id":"criterion-1","status":"fail","evidence_ref":"reports/test-a.txt"}],
  "false_claim":"suspected","errors":[]
}
```

### 2.3 Reviewer Score JSON

```json
{
  "schema_version":1,"subject_attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<executor-uuid>"},
  "reviewer_attempt_ref":{"bridge_instance_id":"bridge-A","attempt_id":"<reviewer-uuid>"},
  "spec_hash":"<sha256>","diff_hash":"<sha256>","rubric_version":"1",
  "scores":{"correctness":4,"completeness":4,"code_quality":3},
  "recommendation":"accept","reject_reason":null,"confidence":"medium",
  "findings":[],"limitations":["External services unverified"],"evidence_refs":["reports/l0.json"]
}
```

Integer rubric 1-5 (`null` `code_quality` for non-code research/docs). Recommendations (`accept`/`reject`/`needs_human`) remain distinct from Bridge `agent_review` decisions, and all reviewer attempts count under separate `review_cost`.

### 2.4 Routing Table JSON

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
    "seconds_per_accept":null,"score":null,"eligible":true,"reasons":["Insufficient samples; using baseline tier"]
  }],"fallback_tier":"L2"}]
}
```

## 3. Module Breakdown (M0-M7)

- **M0 (Spec & Bridge Wrapper)**: Validates specs (`tiering task validate`, `dispatch`, `collect`, `retry`), fixes instruction + feedback hashes on retries vs creating new bridge tasks on spec revisions, and tracks idempotency via identical `request_id`.
- **M1 (Logger & Reconciliation)**: Serializes single-host JSONL writes under `flock` + `fsync` (`tiering events append/check/repair`, `tiering reconcile`), halting cleanly on half-lines (`--preserve-tail`) without cross-store transactional assumptions.
- **M2 (L0 Validator)**: Runs allowlisted test `argv` commands cleanly (`tiering validate`) without evaluating arbitrary shell strings from worker outputs, checking full diff relative to `snapshot_tree` (tracked/untracked/renames/modes/binaries/symlinks) and preserving `unknown` if tests mutate evaluated files.
- **M3 (Reviewer Scoring & Review Assistant)**: Builds immutable review bundles (`tiering review prepare/score/finalize`) ensuring heterogeneous read-only models where available (`src/agent_bridge/routing.py:107`; Antigravity excluded from read-only review), keeping recommendations separate from final primary-agent `agent_review` decisions.
- **M4 (Commit Trailers & Defect Helper)**: Prepares commit trailers (`Agent-Task`, `Agent-Attempt`, `Agent-Model`, `Agent-Reviewer`, `Agent-Bridge`) and tracks candidate defects via `git blame` (`tiering merge prepare/record`, `tiering defect candidates/confirm`), flagging `mixed` contributions honestly.
- **M5 (Metrics Aggregation)**: Replays JSONL + read-only Bridge sources (`tiering report`), computing raw/adjusted FPA, rework counts (`attempts - 1`), executor vs reviewer vs parent costs (`null` when price data missing), defect escape rates over configurable maturation windows (default 30d), and Beta(2,2) smoothed metrics alongside effective sample counts.
- **M6 (Routing Table Builder & Query)**: Generates advisory route tables (`tiering route build/query`), enforcing fresh `agent_catalog` validation (`verified`, input types, permissions, cooldowns) before explicit `profile/model` dispatch without mutating Bridge `default_strategy`.
- **M7 (Calibration Runner)**: Executes versioned isolated benchmark suites (`tiering calibrate plan/run/resume`) while keeping offline fake-CLI checks strictly separate from live model validation.

## 4. Mapping to Core Bridge MCP Tools

External helpers strictly invoke the documented public Bridge MCP/CLI contract (`src/agent_bridge/mcp_server.py:17`-`:112`) via `agent-bridge --state STATE call <tool> --json <payload>`: `agent_catalog` (M0/M6/M7), `agent_open` (M0/M3/M7), `agent_dispatch` (M0/M3/M7), `agent_control` (M0/M7), `agent_cancel` (M0/M7), `agent_inspect` (M0-M7), `agent_wait` (M0/M1/M7), `agent_result` (M0/M2/M3/M5), and `agent_review` (M3 via M0).

## 5. End-to-End Acceptance Criteria

Verify full offline end-to-end execution on a temp repository and fake CLIs (including reject -> retry, Reviewer scoring, commit trailers, defect candidates, and aggregation with missing price edge cases), followed by controlled real-model verification reporting model identities, observed attempts, coverage, and cost completeness separately.
