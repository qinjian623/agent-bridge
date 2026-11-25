> Language: English | [中文](zh/agent-tiering.md)

# Agent Task Dispatch, Inspection, and Logging (v1)

## 1. Scope of v1

The primary agent dispatches a task, and the execution agent edits files inside an isolated worktree. A validation script then checks tests and modified files, the primary agent accepts or rejects the attempt, and execution metadata is logged to track pass rates and turnaround times.

v1 connects to a single bridge instance executing one round per task. The primary agent explicitly picks an existing profile without automatic retries, automatic model switching, or metrics-driven auto-selection.

[cli-agent-bridge](../README.md) already implements sessions, tasks, isolated worktrees, `agent_wait`, `agent_result`, and `agent_review`. The external helper [scripts/tiering.py](../scripts/tiering.py) provides independent checks, task/review JSONL recording, and counts, pass-rate, and duration reports; dispatch and Bridge review remain the primary agent's responsibility.

The v1 ledger persists only `task` and `review` event records.

## 2. Responsibilities

| Actor | Responsibilities |
|---|---|
| Primary agent | Write clear specs, choose profile, dispatch work, review independent test output and diff, accept or reject, and append log records. |
| Execution agent | Edit allowed files inside assigned worktree, run self-tests, report outcomes; ask clarifying questions when specifications are unclear. |
| Check script | Independently re-run agreed test commands, enumerate all file changes, verify allowed scope boundaries, and return commands, exit codes, and discrepancies. |

Never accept an execution agent's self-reported "done / tests passing" as verification. Scripts check objective facts and complement rather than replace primary-agent judgment.

## 3. Writing Task Specifications

Keep specifications short, concrete, and complete with goal, allowed files, test commands, and acceptance checks:

```yaml
goal: Fix crash when parser receives empty input
allowed_files:
  - src/parser.py
  - tests/test_parser.py
out_of_scope:
  - src/tokenizer.py
test_command: PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_parser.py
acceptance:
  - Empty input returns an empty list; existing non-empty inputs stay unchanged
  - New regression test covers empty input and full suite passes
  - Changes stay within allowed files with no commits created
stop_if:
  - Changes required outside allowed_files or undocumented public interface changes discovered
```

Pass the specification directly inside `agent_dispatch.instruction`. Before dispatching parallel tasks, lock `shared_decisions`, check each pair of tasks for overlap, and record what is `out_of_scope` for each task. See [router-prompt.md](../skills/cli-agent-bridge/resources/router-prompt.md).

Select `profile` upfront and inspect the resolved session model and effort rather than guessing from profile names. Give runtime, provider, or option changes separate profile names so metrics stay comparable. Open writable Git worktrees via `agent_open`, persist `workspace.snapshot_tree`, and pass dirty or untracked dependencies explicitly in `snapshot_files`.

## 4. End-to-End Task Lifecycle

1. **Dispatch**: Author the spec, call `agent_open` and `agent_dispatch`, then append an initial `task` JSONL record containing `task_id`, requirements, profile, and `snapshot_tree`.
2. **Execute**: The worker edits files and runs self-tests; wait via `agent_wait`, read the state and `attempt_id` via `agent_result`/`agent_inspect`, and append an updated `task` line.
3. **Independent Verification**: Once the task is `completed` and writes have stopped, re-run tests and scope checks (restart if files change mid-check). Before calling `agent_review(accepted)` on any `workspace-write` change, parallel merge, or high-risk output, the coordinator **must** open a **zero-history isolated read-only session (`access="read-only"`)** following the Checker/evaluator rules in [router-prompt.md](../skills/cli-agent-bridge/resources/router-prompt.md). Pick a verified heterogeneous model—preferring a different runtime and excluding Antigravity, which lacks `read-only`—and fall back to a fresh same-model read-only session only if no heterogeneous candidate exists. Check the original spec, workspace diff, independent test output, worker handoff fields (`deviations`/`assumptions`/`unverified`/`unresolved`, treated as leads only), and cross-task `conflicts`. Trivial read-only queries may be verified directly by the coordinator.
4. **Review Verdict**: Inspect the actual diff and independent evidence, then submit `accepted` or `rejected` via `agent_review` (reject on test failures, out-of-scope edits, or missed acceptance items).
5. **Log**: Append a `review` JSONL record after Bridge confirms persistence, then compute counts, pass rate, and durations without altering `agent_review` RPC signatures or the JSONL schema below.

`completed` only means the runtime turn finished, not that the work is accepted; never skip reading the actual diff even when tests pass.

On execution `failed`, `interrupted`, or `cancelled`, append the final `task` status and stop without calling `agent_review` or appending `review`. Preserve `recovery_needed` for manual inspection.

Single-round v1 ledgers record one task per run (`task_id`); retries or spec edits create a new `task_id` by operator decision while keeping failed or rejected predecessors. In standard multi-agent workflows, the primary agent may dispatch at most 1 targeted fix pass per [router-prompt.md](../skills/cli-agent-bridge/resources/router-prompt.md). Hand over the unmerged state explicitly—using the prior worktree root, relative `snapshot_files`, and `workspace.snapshot_tree` in Git mode, or the prior copy directory, surviving files (excluding deleted paths), and `workspace.manifest` in `copy` mode—then run a full read-only re-review.

`agent_review` parameters ([MCP interface](../src/agent_bridge/mcp_server.py), [review rules](../src/agent_bridge/store.py)):
- `task_id`, `attempt_id`: active completed attempt.
- `decision`: `accepted` or `rejected`.
- `evidence`: non-empty concrete verification summary.
- `expected_revision`: latest session revision from `agent_inspect`.
- `request_id`: stable idempotency key for identical submissions.

## 5. What Automated Check Scripts Verify

Check files directly in the workspace rather than trusting worker summaries:
- Run the specified tests independently in the permitted environment, capturing commands and exit codes.
- Diff against `snapshot_tree` rather than `HEAD` so caller-provided dirty snapshots are excluded.
- Cover tracked, untracked, and Git-ignored paths across adds, deletes, renames (checking both old and new paths), permissions, binaries, and symlinks.
- Snapshot directory state before and after test commands; invalidate results if files change mid-check.

Inside the worktree root:

```bash
git diff --name-status --no-renames "$snapshot_tree" --
git ls-files --others -z
```

Disabling rename folding prevents hiding old paths, and `-z` preserves non-ASCII or special filenames. Still inspect raw byte and mode diffs, and avoid writing caches or artifacts inside the worktree unless explicitly allowed before dispatch. Worktrees isolate paths rather than providing an OS security sandbox.

## 6. JSONL Record Schema

UTF-8 single-line JSON objects stored privately per coordinator (e.g. `~/.local/state/agent-tiering/main-01.jsonl`). Only two event types exist:

| `event` | When Appended | Contents |
|---|---|---|
| `task` | Right after dispatch succeeds, then again when terminal outcome resolves | First line holds task spec/profile/snapshot; follow-up line records `status`, `attempt_id`, and `reason`. |
| `review` | After independent checks finish and `agent_review` persists | `decision`, concrete `reason`, `test_exit_code`, and `out_of_scope_files`. |

Shared fields: `event`, `time` (timezone-aware timestamp), and `task_id` (verbatim Bridge UUID). Never synthesize substitute IDs.

Accepted task example (placeholders shortened for readability):

```jsonl
{"event":"task","time":"2026-10-28T12:00:00+08:00","task_id":"task-001","profile":"codex-sol","model":"gpt-6-sol","effort":"high","goal":"Fix crash when parser receives empty input","allowed_files":["src/parser.py","tests/test_parser.py"],"test_command":"PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_parser.py","acceptance":["Empty input returns empty list; non-empty unchanged","Regression test added and suite passes","Only allowed files edited"],"snapshot_tree":"<agent_open snapshot_tree>","status":"queued","attempt_id":null}
{"event":"task","time":"2026-10-28T12:03:00+08:00","task_id":"task-001","status":"completed","attempt_id":"attempt-001","reason":"Execution finished; awaiting independent verification"}
{"event":"review","time":"2026-10-28T12:05:00+08:00","task_id":"task-001","attempt_id":"attempt-001","decision":"accepted","reason":"Independent test rerun passed; only two allowed files modified; empty input handled correctly","test_exit_code":0,"out_of_scope_files":[]}
```

Rejected example:

```jsonl
{"event":"review","time":"2026-10-28T12:05:00+08:00","task_id":"task-001","attempt_id":"attempt-001","decision":"rejected","reason":"Independent test rerun failed on empty input with IndexError","test_exit_code":1,"out_of_scope_files":[]}
```

Startup failure (no `attempt_id`, no `review`):

```jsonl
{"event":"task","time":"2026-10-28T12:20:00+08:00","task_id":"task-002","profile":"codex-sol","model":"gpt-6-sol","effort":"high","goal":"Fix crash when parser receives empty input","allowed_files":["src/parser.py","tests/test_parser.py"],"test_command":"PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_parser.py","acceptance":["Empty input returns empty list; non-empty unchanged","Regression test added and suite passes","Only allowed files edited"],"snapshot_tree":"<agent_open snapshot_tree>","status":"queued","attempt_id":null}
{"event":"task","time":"2026-10-28T12:20:05+08:00","task_id":"task-002","status":"failed","attempt_id":null,"reason":"Worker startup failed"}
```

Rules:
- Guard single-host sequential appends with an exclusive `flock`; cross-host shared JSONL appends are unsupported.
- `test_exit_code=0` means tests passed; `null` when intentionally N/A (never claim pass for missing required tests). `out_of_scope_files=[]` confirms checked and clean (`null` if unchecked).
- Deduplicate exact repeated lines; abort and surface contradictions or malformed lines with file and line numbers rather than silently truncating or guessing. On partial failures (e.g. Bridge committed while JSONL failed), halt new dispatches until reconciled manually.

## 7. Reporting Metrics

Filter by dispatch timestamp range, specify cutoff time, and break down by profile, resolved model, and effort—counting unique `task_id` entries:

| Metric | Definition |
|---|---|
| Raw counts | Dispatched total, runtime `completed`, accepted, rejected, failed, interrupted/cancelled, queued/running, pending review, and needs inspection. |
| Pass rate | `accepted / decided_total` where `decided_total = accepted + rejected + failed + interrupted/cancelled` (display waiting/pending/unverified counts separately). |
| Mean duration | Arithmetic mean from `task.time` at dispatch to resolved `review.time` (accepted/rejected) or terminal `task.time` (failed/cancelled/interrupted), covering queue + run + check + review wait across all decided tasks. |

For example: 6 accepted, 2 rejected, 1 failed, 1 cancelled, plus 2 pending review -> `6 / 10 = 60%` alongside `2 pending review`. Leave ratios empty when `decided_total == 0`, avoid comparisons under `n < 10`, do not apply synthetic Bayesian weights or decay in v1, and never conflate Bridge's internal `quality` with task pass rates.

## 8. Implementation Milestones & Guardrails

**P0 (Complete end-to-end single-task path)**:
- Dispatch, verify independently, review, log `task`/`review` JSONL rows, and summarize raw counts, pass rate, and duration.
- Validate accepted, test-failed, and startup-failed paths without dropping failures or passing unreviewed work.
- Set directory permissions to `0700` and log/lock files to `0600`; exclude secrets, credentials, raw chats, and database paths.

**P1 (Ergonomics)**:
- Wrap dispatch/wait/check/append steps in scripts (keeping primary-agent diff review required) and add date/profile filtering, pending-review listings, and CSV/Markdown exports using identical definitions.

Log retention and manual deletion remain user-managed; retry management, fixed-task calibration suites, auto-tiering, and cost modeling stay deferred outside P0/P1.
