> Language: English | [中文](zh/agent-tiering-review.md)

# Agent Tiering Architecture Design Review

Reviewed target: `docs/agent-tiering.md` against baseline HEAD `164bdbd8a29ef7fb8a07ddd8098a770af7ce7738`. Static architectural audit without live model calls, production SQLite access, or live service runs (no unverified assumptions beyond read-only source inspection).

Respects settled constraints in Section 9: external JSONL without modifying Bridge core; separate Reviewer accounting; commit trailers and `git blame` yield candidates only requiring confirmation; advisory thresholds only without auto-pruning prior to statistical sufficiency. Recommended revisions follow.

## 1. Findings by Severity

### High: H1 Retry Instruction vs Bridge Retry Contract

Draft Section 5 specifies "retrying with rejection reason attached", whereas `retry_task_id` enforces immutable original `instruction` and `dependencies` (`invalid_retry` otherwise; `src/agent_bridge/store.py:39`, `:251`, `:470`). Retries reuse the same `task_id`, clear `active_attempt`, and queue another attempt inside existing sessions, while existing sessions lock their model (`src/agent_bridge/broker.py:246`, `:256`).

Recommendation: Distinguish identical-spec retry (reading versioned external feedback files recorded via feedback hash without altering task goal/acceptance) from specification revision (spawning a new bridge task linked externally via `logical_task_id` / `supersedes` while counting toward total logical task attempts). Switching models requires opening a new session with explicit handling of how unmerged diffs/snapshot trees carry over.

### High: H2 Existing `quality` Is Neither Attempt-Level FPA Nor Reviewer Rubrics

`task_samples` groups per task and generates one sample per normalized configuration identity (`runtime`, `provider`, `model`, `effort`, `service_tier`, `env_file`), rather than per attempt (`src/agent_bridge/routing.py:11`-`:71`). Multi-config tasks take the latest started attempt per identity; Bridge `kind` defaults to `general` without `difficulty` (`src/agent_bridge/broker.py:270`).

Recommendation: Compute FPA strictly from external per-attempt events; persist non-secret config identity digests and re-bucket whenever `effort`, `service_tier`, prompt template, or runtime changes.

### High: H3 Balanced Scoring Samples Cannot Be Reused Directly

Bridge's `0.5 / 0.5` `balanced` weighting filters quality-eligible candidates, prunes dominated entries across quality/cost/speed, normalizes against candidate minimums, and requires comparable metrics across **all** candidates—falling back from USD to tokens, or returning `Unmeasured` (`src/agent_bridge/config.py:104`, `src/agent_bridge/routing.py:94`, `:136`-`:198`). Samples merge all attempts plus single-task stopped parent metering (`routing.py:50`-`:68`), not executor-only accept cost.

Recommendation: Compute executor-only, reviewer, and parent metrics separately in the external tiering engine, recording `cost_scope`, completeness coverage, currency/unit, and price date cleanly; missing attempt prices make USD totals unknown (`null`), never `$0` (`src/agent_bridge/usage.py:32`).

### High: H4 Missing Link Between External Tasks, Bridge Tasks, and Attempts

User-defined keys (`feat-xxx-01`) vs async UUIDs: `agent_dispatch` returns `queued` before any `attempt_id` exists (`src/agent_bridge/store.py:23`, `:219`, `:448`), and retries can shift sessions.

Recommendation: Track `logical_task_id`, `bridge_instance_id`, `bridge_task_id`, `session_id`, `attempt_id`, `dispatch_request_id`, `bridge_event_seq`, `role`, and `spec_version`. Record pre-dispatch intent, reconcile crashed calls by re-issuing identical `request_id`, bind attempts from event streams, and record failed/recovery tasks without waiting for `agent_review`.

### High: H5 JSONL Concurrency, Partial Writes, and Correction Conflicts

"Append-only needs no locking or migrations" breaks under concurrent coordinators, mid-write kills, or full disks leaving truncated lines; correction patches keyed only by `task_id` cannot disambiguate attempts.

Recommendation: Guard duplicate checks and single-line appends under an exclusive file lock (`flock`), `flush` + `fsync` before ACKs, enforce `schema_version` / `event_id` / `idempotency_key`, target `correction` entries explicitly via `target_event_id` with acyclic allowlisted patches, and halt cleanly on half-lines for explicit repair tooling (`--preserve-tail`). Cross-host shared JSONL appends are unsupported.

## 2. Medium and Low Findings

| ID | Severity | Issue & Recommendation |
|---|---|---|
| M1 | Medium | `agent_review` accepts only `task_id`, `attempt_id`, `decision`, `evidence`, `expected_revision`, `request_id` (`mcp_server.py:108`); keep `reject_reason`, rubrics, and report hashes in external events, only reviewing `completed` attempts at latest revision (`store.py:687`). |
| M2 | Medium | Section 7.2 auto-filtering conflicts with deferred elimination in Section 9: emit advisory routing tables first, checking eligibility explicitly in external dispatch rather than relying on Bridge's auto-selector (`routing.py:109`, `:138`, `:166`). |
| M3 | Medium | `verified` marks inference verification; catalog presence does not equal verified, and `Unmeasured` is a selection string rather than lifecycle state (`broker.py:145`, `discovery.py:294`). External calibration records must never spoof Bridge's `verified`. |
| M4 | Medium | Orchestration/DAG splitting/hard tier budgets/max_attempts live outside Bridge core; Bridge handles per-session queueing, `depends_on` acceptance gating, concurrency limits, and token reservations (`store.py:398`). |
| M5 | Medium | Failed tests alone indicate `suspected` discrepancies (env/flaky/different tree); promote to `confirmed` false claims only after human/coordinator verification, preserving `unknown` for un-testable research tasks. |
| M6 | Medium | Diff scopes against `snapshot_tree`, not `HEAD`, covering untracked, both sides of renames, deletes, modes, binaries, and symlinks (`workspace.py:110`, `:161`); test runners still execute within host UID boundaries. |
| M7 | Medium | Track commit-level attribution explicitly (`single` vs `mixed` when humans edit same commit), re-link on squash/rebase, and deduplicate defect observation windows. |
| M8 | Medium | Report both raw and spec/env-adjusted FPA per first logical attempt alongside explicit exclusion reasons and un-settled task counts; never divide by zero when zero tasks pass. |
| M9 | Medium | With Beta smoothing + decay, display raw N, weighted counts, effective sample size `(sum w)^2 / sum(w^2)`, and explicit exploration seeds/probabilities without lowering high-risk tasks artificially. |
| M10 | Medium | Mitigate reviewer style bias and prompt injection using normalized diffs, anonymized model identity during scoring, and periodic audited spot dual-reviews. |
| M11 | Medium | Protect external directories (`0700`) / JSONL (`0600`), restrict fields to allowlisted opaque refs/hashes, redact repos/paths/usernames in git commit trailers, and never store raw chats or secrets. |
| L1 | Low | Rework rounds equal `max(attempt_count - 1, 0)`; scope `output_tokens / diff_lines` by task type (`null` on empty diffs) and omit from auto-routing. |
| L2 | Low | Separate routing escalation rates from ground-truth tier classification accuracy; require labeled ambiguity denominators for help-seeking rates. |

## 3. Public Read Interfaces Available Without Modifying Bridge

```bash
agent-bridge --state "$STATE" call agent_catalog --json '{}'
agent-bridge --state "$STATE" call agent_inspect --json '{"task_id":"<bridge task UUID>"}'
agent-bridge --state "$STATE" call agent_wait --json '{"after":0,"timeout":20,"task_id":"<bridge task UUID>"}'
agent-bridge --state "$STATE" call agent_result --json '{"task_id":"<bridge task UUID>","attempt_id":"<attempt UUID>"}'
```

| Required Data | Interface Source & Limits |
|---|---|
| Profiles, models, eligibility, prices | `agent_catalog` (`profiles`/`discovery`, `quota_cooldowns`), `agent_inspect(session_id).profile`; prompt inputs remain text-only. |
| Task/attempt states & usages | `agent_inspect(task_id)` / `agent_result` (`store.py:274`, `:877`; `broker.py:343`): `usage`, `catalog_estimate`. |
| Lifecycle binding & reviews | `agent_wait` events (`seq`, `kind`, `task_id`, `attempt_id`, `session_id`, `created`; `store.py:160`, `:1121`) with monotonic deduplicated cursors. |
| Answers, patches, artifacts | `agent_result` + UTF-8 pagination via `artifact_id`/`next_offset` (`artifacts.py:32`); inspect failed-patch workspaces too. |
| Extended history beyond 20 attempts | Event stream + explicit `attempt_id`, or `mode=ro` SQLite read-only transactions checked by schema version (never instantiate mutable `Store()` for reads). |
| Timings & budgets | `tasks.created`, `attempts.started`/`finished`, `task_reviewed.created`; parent/child totals via `agent_inspect()` (`store.py:729`). |

Bridge cannot natively supply business `task_type`/`difficulty`, external `spec_version`, independent L0 verdict, rubric scores, causal defect links, user end-satisfaction, actual billing invoices, or un-metered manual hours.

## 4. Revision Sequence

Clarify `H1`/`H4` IDs and retry contracts plus `H5` locking first, then `H2`/`H3` metrics and `M1`/`M2` contracts, followed by explicit `unknown`/`mixed`/manual confirmation rules. Architecture stays strictly modular in `agent-tiering-modules.md` without modifying Bridge core.
