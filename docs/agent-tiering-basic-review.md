> Language: English | [中文](zh/agent-tiering-basic-review.md)

# Agent Tiering Scope Review: MVP Simplification & Terminology Cleanup

Reviewed draft: [agent-tiering.md](agent-tiering.md) (485-line initial draft prior to simplification). Recommends stripping speculative metrics and premature complexity down to a ~150-line Phase 1 specification without altering Bridge core APIs.

Phase 1 only needs: author crisp task specs, dispatch work, run independent tests and diff scope checks relative to `snapshot_tree`, let the human/coordinator judge accept/reject, log minimal records, and view raw counts, pass rates, and durations. Deferred features (auto-calibration suites, Beta(2,2) priors, decay weights, exploration quotas, commit trailers, 12 event schemas, multi-writer reconciliation, auto-repair tail truncation) should not dictate Phase 1 record schemas.

## Part 1: Plain-Language Replacements

Replaces opaque jargon across three groups while leaving standard technical concepts (`UUID`, Git trees, JSONL, `flock`, `fsync`, Beta priors) and existing Bridge field names intact:

- **Dispatch & Roles**: Eliminate confusing hierarchy labels (`L0-L3`, `Carrier`, `Orchestrator`, `Triage/Router`, `Executor`, `Summarizer`, `Task DAG`, `business UUID`, `task_key`, `spec_version`, `cumulative diff`, `impact surface`, `configuration identity tuple`). Use plain concepts: primary agent, execution agent, validation script, task ID (`task_id`), `attempt_id`, profile name, resolved model/effort, and "changes relative to input `snapshot_tree`".
- **Metrics & Selection**: Remove `FPA`, `adjusted FPA`, `defect escape rate`, `false claim honesty rate`, `suspected/confirmed/unknown`, `spec compliance score`, `justified help rate`, `token / effective diff line ratio`, `advisory` mode thresholds, `Beta(2,2)` smoothing, `30d half-life`, `effective sample size`, and `10% random exploration`. Report raw decided counts, plain pass rate (`accepted / decided_total`), mean turnaround time, and concrete factual rejection notes.
- **Logging & File Handling**: Consolidate 12 premature event types (`task_spec`, `dispatch_intent`, `dispatch_link`, `bridge_event`, `attempt_link`, `outcome`, `review`, `review_result`, `merge`, `defect`, `correction`, `repair`), complex pre-edit effective hashes, event mirroring/replay cursors, and half-line auto-truncation repairs into sequential single-process JSONL appends that halt with file and line numbers on corruption/contradiction without silently dropping rows.

## Part 2: Scope Trim Summary

### 2.1 Removed from Phase 1
- Multi-tier L0-L3 tables, 1-5 task scoring rubrics, auto escalation/de-escalation, multi-round patch relays, 11-field config identity hashes, 12 event schemas -> compressed to `dispatch` / `result` / `review` (or v1's merged `task` + `review`).
- External Bridge SQLite mirroring/cursors, generic field `correction` overlays, auto-truncation repair events, Bayesian smoothing, random exploration tracking, heterogeneous Reviewer rubrics, 20-40 benchmark suites, Git commit trailers/`git blame` defect pipelines, and mandatory `retention_days` auto-pruning.

### 2.2 Kept in P0 / P1
- **P0**: Complete end-to-end flow for one task (explicit spec + `allowed_files` + test command -> dispatch -> independent diff/test verification -> human/primary decision -> append records -> summary metrics), keeping accepted, test-failed, and startup-failed paths cleanly tested; single-process append under `flock`, mode `0700` dir / `0600` files, zero secrets/chats stored.
- **P1**: Wrap repeatable commands in scripts and add date/profile filters, pending-review queries, and CSV/Markdown exports using identical definitions.

## Part 3: Minimal v1 Specification Outline

1. **Five core steps**: Author & dispatch (`agent_open` + `agent_dispatch`, save `snapshot_tree`) -> await & record execution (`task_id`, `attempt_id`, `status`) -> run independent checks (re-run test command, inspect `git diff --no-renames "$snapshot_tree"` + `git ls-files --others -z`, confirm no mid-check edits) -> coordinator review (`agent_review` with concrete `evidence`) -> append terminal verdict & summarize.
2. **Event records**: `dispatch`, `result`, terminal `review` (one JSON object per line using timezone-aware timestamps, verbatim Bridge UUIDs, explicit `null` when required checks did not run; never duplicate Bridge transcripts).
3. **Two computed metrics alongside raw counts**: Task pass rate `accepted / (accepted + rejected + failed + interrupted_cancelled)` and arithmetic mean duration across decided tasks (`review.time` or terminal `result.time` minus `dispatch.time`), omitting incomplete/pending counts from averages while displaying them explicitly alongside decided totals.

## Part 4: Target Structural Verification

Ensures the streamlined ~150-180 line `agent-tiering.md` spec keeps essential safety checks intact (no blind trust in worker claims, mandatory independent diff and test reruns relative to `snapshot_tree`, preserved failures, halt on write/parse corruption, and explicit working links into [MCP interface](../src/agent_bridge/mcp_server.py) and [review checks](../src/agent_bridge/store.py)).
