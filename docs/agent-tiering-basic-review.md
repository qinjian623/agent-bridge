> Language: English | [中文](zh/agent-tiering-basic-review.md)

# Agent Tiering Design Review: Basic Functionality and Wording

Reviewed document: [agent-tiering.md](agent-tiering.md), originally 485 lines. Line references below refer to that original draft, not the current shortened file. This report recommends changes; it does not implement a logger, validation scripts, or automatic model selection.

Phase 1 needs to describe a task, dispatch it, independently check tests and changed files, let a human or primary agent accept/reject it, save records, and report pass rate and duration. The draft also develops model evaluation, automatic selection, cost accounting, defect tracing, and log recovery. Although some are deferred, their fields and rules still burden the Phase 1 logger.

## Part 1: Plain wording and unnecessary terminology

The following tables follow the original draft, combining equivalent expressions. They include vague wording, unexplained English labels, and long field names. Technical terms are not inherently wrong; the problem is requiring readers to learn them before seeing the basic procedure.

`UUID`, Git trees, JSONL, `flock`, `fsync`, and Beta distributions are established concepts. For each, ask whether Phase 1 needs it, whether it belongs in the main text, and whether a short explanation would help. Keep existing Bridge parameter names. Proposed changes to external-design fields below do not change Bridge APIs.

### 1.1 Roles and task descriptions

| Original expression and draft location | Problem | Plain replacement or action |
| --- | --- | --- |
| External runner (4, 11, throughout) | Does not identify a program, script, or primary-agent duty | A script that calls Bridge, runs checks, and writes records; the primary agent may follow the steps manually in Phase 1 |
| Carrier (22) | The heading does not identify software or a person | Who executes it |
| L0 deterministic handling / L0 validation (24, 75, 145) | Puts scripts and models into one rank table, then uses ranks for actions | Run tests and check changed-file scope with scripts; remove L0 as a rank |
| Lower-tier / lightweight / execution / higher-tier model (14, 25–27) | Does not identify a configuration and can imply quality from a model name | Name the profile; compare speed/pass counts only with actual data |
| Complete configuration identity / configuration identity (30, 191–198) | Turns ordinary runtime settings into a new identity concept | Program, model, and settings used for this run |
| External tiering / external calibration (30) | Does not identify who is external or which tasks calibration runs | The primary agent's initial assignment; trials using fixed tasks, deferred |
| Catalog visibility is not verified inference (30) | Catalog can be confused with a filesystem directory | A listed model name does not show that a call has succeeded |
| Orchestrator (36, throughout) | Duplicates the existing primary-agent role | Primary agent throughout |
| Triage / Router (37) | Adds a separate role and two names for deciding whether a task is simple | The primary agent decides whether to delegate; remove the role |
| Executor (38, throughout) | Mixed terminology forces repeated translation | Execution agent |
| Reviewer / different-model Reviewer (39, 59, 146) | Makes independent checking sound like a mandatory extra model call | Reviewer; use a person or primary agent in Phase 1 |
| Summarizer (40) | Creates a role without a demonstrated log-summary need | Summarize logs when needed; remove it from Phase 1 roles |
| Split into a task DAG (50, 73) | A dependency graph is unnecessary before dispatching one task | Run dependent tasks in order; describe one task in the basic procedure |
| Business UUID / business task (82, 88) | Introduces two task levels before they are needed | Task; use the `task_id` returned by Bridge |
| `task_key` / readable external alias (83) | Adds another required name that cannot be used for association | An optional displayed title, without a mandatory alias |
| Dispatch intent / externally allocated `bridge_instance_id` (88, 385) | Adds intent and instance concepts to one call | Record the task before calling; one Bridge instance in Phase 1, without these extra fields |
| `spec_version` / `supersedes` (95–96, 174) | Adds replacement/version relationships to the basic example | Dispatch a new task when requirements change and keep the old result; remove replacement fields |
| Fix as `instruction` (90) | Fix could mean write, freeze, or transform | Pass the task description unchanged as `instruction` |
| Feedback version fixed before dispatch / version at a fixed entry point (107, 172) | Makes a rejection reason into a file-publication protocol | Give the execution agent the rejection reason before the next run; defer versioned feedback |
| Fixed output tree / refix input (123) | Repeats freeze terminology without the action | Stop writes and record the edits being checked; repeat checks if files change |
| Cumulative diff / cumulative patch hash (125, 182) | Mainly needed for several models taking turns | All changes since the initial input; defer relay execution |
| Ambiguity / context volume / verifiability / novelty (135–139) | Turns judgment into subjective scores | Are requirements clear, how many files need reading, what checks exist, is there similar code; use these as writing reminders only |
| Impact surface (136) | Does not name the affected objects | Files, interfaces, and data formats that will change |
| Downgrade-probe suggestion / exploration task (164, 322) | Implies a policy where the action is trying a cheaper model | Manually try another model on a low-risk task; defer |
| Original-instruction retry / carry previous changes (166–185) | Expands retry into versions, sessions, and patch transfer | Whether to retain prior edits on a retry; stop after Phase 1 failure and let a person decide |

### 1.2 Evaluation and model choice

| Original expression and draft location | Problem | Plain replacement or action |
| --- | --- | --- |
| Configuration identity × `task_type` × `difficulty` buckets (191) | Hides three classifications in one phrase | Count by configuration; defer type/difficulty subdivisions |
| Complete tuple and version (320; fields 194–195) | Eleven configuration fields have no basic-use justification | Settings used for this run; no complete versioned configuration table in Phase 1 |
| Non-sensitive opaque identifier and version (198) | Does not tell users what to enter | A generated configuration number without accounts/keys; no authentication-configuration ID required in Phase 1 |
| Rebucket (198) | Readers must reconstruct the grouping rule | Start a new statistics group after changing settings |
| Data coverage (200, 243) | Does not say what is missing | Total records and how many have results/costs; show counts directly |
| First-pass acceptance / FPA (206) | Abbreviation and cross-task deduplication complicate Phase 1 | Define task pass rate only; do not imply that it measures first-attempt success |
| Final pass rate (207) | Defined as no escalation, unlike the everyday meaning of final success | If retained later, pass rate without changing models; remove from Phase 1 |
| Raw FPA / adjusted FPA / exclusion rules (200, 212, 321) | Requires extra judgments about excluded failures and favors the nicer number | Pass rate including all failures versus one excluding named causes; Phase 1 uses the first and keeps factual reasons |
| Defect escape rate / equal observation period (209) | Requires post-merge observation and confirmation | Accepted tasks later found defective; defer |
| Reliability/honesty / false completion rate (218–228) | A failing test alone does not prove dishonesty | Record claimed tests passed but independent rerun failed; use the rejection note |
| `suspected / confirmed / unknown` (222, 228) | Supports a new classification process before needed | Pending confirmation / confirmed / unclear; no new Phase 1 classification system |
| Spec compliance score (224) | A ratio hides which requirements are missing | Name unfinished requirements in the rejection reason |
| Justified help rate / genuine ambiguity (226) | Denominator still needs manual judgment and offers little immediate guidance | Ask promptly when requirements are unclear; retain the behavior, remove the metric |
| Executor-only / `executor_cost_per_accept` (234, 241) | Adds naming and metering requirements to a simple calculation | Execution-agent cost per accepted task; do not calculate in Phase 1 |
| Parent overhead / `parent_overhead` (236) | Parent may be unfamiliar to readers | Primary-agent usage; defer cost allocation |
| Wall-clock time (237) | Accurate but unnecessarily technical here | Time from dispatch until acceptance/rejection is decided |
| Token efficiency / effective diff lines (238) | Effective lines are undefined and more edits do not establish quality | Display tokens and changed-line counts if useful; remove the ratio |
| `cost_scope` / `price_date` / `catalog_estimate` (243) | Introduces cost scope, pricing date, and estimates absent from the basic report | Whose cost, which date's prices, whether estimated; defer the section |
| Multi-model relay (245) | Hides the cost/input work of several sequential models | Several models editing the same task in turn; no automatic continuation in Phase 1 |
| Spec defect rate / tier-assignment accuracy (251, 254) | Evaluates the primary agent before stable task samples exist | Note unclear requirements and unsuitable assignments first |
| User final pass rate (257) | Easily confused with agent-review acceptance | User-confirmed requests; no new Phase 1 acceptance-statistics process |
| Controlled report reference (267, 434) | Does not identify readers or location | A report file readable only by the local owner; basic records keep short check summaries |
| Data-version digest / `missing_source` (292) | Adds a state just to query old timing data again | Original record missing; save basic timestamps directly |
| `mode=advisory` / reference thresholds / hard filter / automatic elimination (298) | Turns deferred model rules into a whole mode | Display results and let a person choose; remove thresholds/mode fields |
| Candidate eligibility / verification eligibility (302, 305) | Does not say what is checked | Whether the configuration can run and its permissions meet the task |
| `measurement_status=insufficient` (303) | Replaces an explanation with a field | Only three records, too few to compare; no new state |
| Normalization formula / `balanced` score (304, 313) | Phase 1 need not reproduce automatic selection internals | Equal cost/time weighting; remove the algorithm from the main text |
| `Unmeasured` / profile lifecycle state (312) | Explains an internal value readers do not yet need | Use the default order without sufficient measurements; consult Bridge docs when needed |
| Small samples and decay / `Beta(2,2)` prior / smoothed value (319) | Valid terminology without a demonstrated Phase 1 problem | Draw no conclusion from too few samples; remove the formula |
| 30-day half-life / effective sample size `N_eff` (319) | Requires weighting, decay, and eligibility rules before raw counting works | Show actual record counts; remove weighting/decay |
| Attribution / assign to first configuration (206, 320, 324) | Requires a research-like separation of tasks, attempts, models, and defects | State who ran the attempt; defer identifying which change caused a later defect |
| Exploration / candidate set, sampling probability, random seed (322) | Describes a model-choice experiment rather than basic dispatch | Manually try another model occasionally; no 10% target or sampling records |
| Reviewer bias / small-sample dual review (323, 471) | Adds a second experiment to score reviewers | Ask a second person when needed; expand only after actual disagreements |
| Calibration suite / detect regression (326–332) | Requires 20–40 maintained tasks before regular evaluation is justified | Compare models on fixed tasks, or check increased failures; defer |
| Defect attribution: commit trailer / `mixed` contribution (334–356) | Mixes Git concepts with contribution accounting far beyond dispatch/review | Task number in a commit message; both humans and agents edited it; not required in Phase 1 |
| Opaque identifier / opaque ID (338, 356) | Makes an ordinary privacy-preserving number seem special | Random number without account/project names; do not expand before public export is needed |

### 1.3 Records and file handling

| Original expression and draft location | Problem | Plain replacement or action |
| --- | --- | --- |
| Metadata storage (358) | Does not say what is stored | Task records: one JSON object per line |
| Field evolution / `schema_version` (360, 397) | Defines upgrades before the format is in use | Explain old-record reading when fields actually change; no Phase 1 version protocol |
| Idempotency key / `idempotency_key` (364, 396) | Adds a second key beside Bridge `request_id` | Do not count a record twice; distinguish by event, task, and attempt |
| `task_spec` (368) | Makes the task description a separate event | Include requirements in dispatch |
| `dispatch_intent / dispatch_link` (369–370) | Splits one dispatch into before/after events | Dispatch record; no automatic recovery of interrupted calls in Phase 1 |
| `bridge_event / attempt_link` (371–372) | Mirrors events and associations already held by Bridge | Save the current attempt ID with the result; do not copy the event stream |
| `outcome` (373) | Mixes completion, startup failure, and recovery | Execution result; record uncertainty and stop automatic handling |
| `review / review_result` (374–375) | Requires a two-stage review model | Append the review only after Bridge confirms it saved |
| `correction` / allowlisted `changes` (378, 401) | Designs editable-field rules for a few manual labels | Report mistakes/conflicts and let a person check them in Phase 1 |
| `expected_effective_hash` (378, 401–403, 449) | Requires hashing a record after applying historical corrections | Check that another writer has not changed it; remove the field/rules in Phase 1 |
| Dispatch, claim, asynchronous backfill (383) | Hides ordering, association, and recovery complexity | Add the attempt ID when the execution result arrives; no asynchronous backfill in Phase 1 |
| Derived association / derived record / persistent write (387–388) | Does not identify the generated record/file | Complete the task record from Bridge's reply; write JSONL |
| Cross-database transaction (388) | Uses a valid concern to add complexity after choosing JSONL | Bridge and the record file may not both save; stop and request inspection |
| Advance cursor / replay and deduplicate (290, 388) | Basic statistics need not manage an event reader | Remember the last read point and skip duplicates; remove from Phase 1 implementation |
| Lost acknowledgment / non-business fields such as generated time (396) | Record retry rules exceed the task's complexity | Write succeeded but reply was lost; report uncertainty without automatic write retry |
| Pre-correction effective-record hash / corrections apply in file order (401–403, 449) | Readers must understand layered history before reading one review | Current review contents; no repeated field overlays in Phase 1 |
| Corrupt tail / explicit repair tool / controlled backup reference (399–405) | Turns an incomplete final line into fragments, truncation, backups, and repair events | Stop reads/writes, report its line, and let a person back up and repair it |
| Fragment hash / original byte offset (405) | File-recovery details dominate basic design | Bad-line contents and position; remove automatic truncation/repair events |
| Allowed fields, short verdict, content hash, controlled reference (459) | Still does not identify what to store | List permitted fields and prohibit chats, keys, and authentication files |
| `retention_days` / operate on whole task association groups (459) | Adds mandatory retention/deletion rules that can prevent startup | State who deletes records and when; manual Phase 1 retention, no startup dependency |
| Advisory reference table / automatic threshold elimination (472–485) | Deferred features still control implementation order | Pass-rate and duration table; no Phase 1 automatic model selection |

## Part 2: Remove work outside the basic version

### 2.1 Remove from Phase 1

| Original design and location | Why it exceeds Phase 1 | Concrete change |
| --- | --- | --- |
| Four tiers, five roles, routing, summaries (18–76) | One task needs primary/execution agents; independent checks need not add a model | Keep those two; a person or primary agent reviews; add other roles only for a demonstrated need |
| Five subjective 1–5 scores (129–148) | Determines execution/review before any task records exist | Select a configured executor directly; do simple tasks locally |
| Escalation/de-escalation, counts, feedback versions, model-change patch transfer (152–185) | Requires retries, changed specs, workspaces, and contribution history | Stop after failure/rejection; manually dispatch a new task if chosen and retain failure |
| Multiple task IDs, spec replacement, eleven-part configuration identity (78–117, 191–198) | Mainly serves multiple instances, retries, and historical model comparisons | Use Bridge `task_id` and result `attempt_id`; record profile, actual model, and effort; fix configuration during a reporting period and use a new name after other changes |
| Twelve events (366–379) | Each adds writing, reading, association, and failure code | Keep `dispatch`, `result`, `review`; requirements belong to dispatch and results include attempt IDs |
| Spec/input/feedback/report/parameter hashes and pre-correction comparison (117, 123, 172–185, 267, 401–403) | Digests do not replace actual inspection | Keep Bridge `snapshot_tree` for Git diff; record commands, results, and reasons without hashing every external file |
| Bridge event mirror, cursor, out-of-order backfill, restart replay, direct SQLite reads (275–292, 383–390) | Rebuilds an event-state system for a small record file | Use existing wait/result/inspect tools; do not mirror events or read internal DB; stop on uncertainty |
| Generic `correction`, editable fields, `expected_effective_hash`, layered corrections (399–403) | No demonstrated concurrent review-editing need | Append normal records only; preserve files, report mistakes/conflicts, and suspend official summaries for manual checking |
| Tail backup, digest, truncation, `repair` event (405) | Detecting bad lines matters; automatic repair is unnecessary | Stop and report any corrupt line, including middle lines; never drop it and continue counting |
| General concurrent writes, extra idempotency keys, version-compatibility protocol (392–397) | Prepares for multiple writers and long-lived upgrades | One local process writes complete lines sequentially; deduplicate repeats; no multi-process/multi-host writers or live format upgrade |
| `Beta(2,2)`, 30-day half-life, weighted successes, effective N (319) | Basic completeness/counting is not established; parameters lack project evidence | Show raw success/total/pass counts and small samples without ranking |
| About 10% exploration, candidates, probabilities, seeds (322) | Already a selection experiment | No exploration target or sampling records; manually try alternatives |
| Combined quality/cost/time score, thresholds, elimination preparation (296–315, 485) | Even advisory mode requires grouping, completeness checks, scores, tables | Show pass rate and mean duration only; people choose; do not change Bridge's built-in selection |
| Honesty/help/assignment scores, rework, allocated cost, token/diff ratios (202–257) | Collects many judgments with manually labeled denominators | Keep rejection reasons, task pass rate, mean duration; state test/scope failures directly |
| Different-model review, three 1–5 scores, dual review (39, 210, 323, 471) | Adds cost, queues, and disagreement handling | Human or primary review by default; request another reviewer only when needed |
| 20–40 calibration tasks, monthly reruns (326–332) | Adds long-term suite maintenance before basic use | Future work after real records show a comparison need |
| Commit trailers, blame, observation periods, mixed contribution, squash associations (334–356) | Post-merge defect investigation is not required by dispatch/check/record | Remove from the basic design and design separately if needed |
| Automatic retention, associated deletion, required `retention_days`, DB migration at 10,000 lines (459–461) | Adds deletion/storage migration before data exists; line count alone does not establish JSONL failure | Keep privacy and permissions; manage retention manually and defer cleanup/migration |

The draft actually lists **12 events**, not nine: `task_spec`, `dispatch_intent`, `dispatch_link`, `bridge_event`, `attempt_link`, `outcome`, `review`, `review_result`, `merge`, `defect`, `correction`, `repair`. Keep Git `snapshot_tree`, which identifies the actual input.

### 2.2 Keep in P0 and P1

| Phase | Keep | Observable completion |
| --- | --- | --- |
| P0: one complete task | Goal, allowed files, check command, fixed profile, dispatch, independent checks, human/primary acceptance or rejection, three record types, counts/pass rate/mean duration | One real task has dispatch/result/review records; failed checks cannot become acceptance and execution failure is retained |
| P0: correct records | Separate execution ending from review acceptance; retain failures and pending work; deduplicate; stop on failed writes or corrupt lines | Every report count can be traced to records, including failed tasks |
| P0: necessary file rules | Inspect changes relative to input `snapshot_tree`, including untracked files; one writer; directory 0700, files 0600; no keys, auth state, raw chats | Scope checks do not count existing dirty input as new execution-agent changes; no sensitive records |
| P1: convenience | Turn existing checks into scripts; date/fixed-profile filters, table export, pending-review list | Less repeated manual work without changing definitions or adding event types |

## Part 3: Minimal first-version specification

### 3.1 Scope and five steps

Use one Bridge instance and prefer clearly specified Git-worktree tasks with known checks. The five steps are: describe and dispatch -> wait and save execution result -> independently check changes against `snapshot_tree` and rerun agreed tests -> review and label -> append the decision and summarize. See [MCP interface definitions](../src/agent_bridge/mcp_server.py) and [review checks](../src/agent_bridge/store.py).

Call `agent_open` and `agent_dispatch`, retaining `snapshot_tree`. Save returned `task_id`, `attempt_id`, and status. Inspect `git diff --no-renames "$snapshot_tree"` and `git ls-files --others -z`, and check that files do not change during validation. Submit `agent_review` with concrete `evidence` only after independent inspection; then append the terminal verdict.

### 3.2 Three JSONL record types

Keep `dispatch`, `result`, and `review`; each has `event`, `time`, and `task_id`. Use one JSON object per line, timezone-aware timestamps, IDs returned by Bridge, and explicit `null` when a required check did not run. Do not duplicate Bridge transcripts.

### 3.3 Two computed metrics

Show raw counts as well as task pass rate and mean duration for decided tasks. Pass rate is `accepted / (accepted + rejected + failed + interrupted_cancelled)`. Duration is `review.time` or terminal `result.time` minus `dispatch.time`, averaged over decided tasks. Show pending-review/unfinished counts separately; omit them from completed-task averages rather than hiding them.

## Part 4: Rewrite outline and checks

Aim for a 150–180-line main specification with a title identifying the first-version scope. A reader should be able to follow an example through dispatch, independent tests/scope checks, review, and recording. Keep necessary safety and failure handling: no blind trust in worker claims, checks relative to `snapshot_tree`, retained failures, stopping on failed writes/corrupt records, and working links to MCP/review implementation. The length target must not remove those requirements.
