> Language: English | [中文](zh/swarm-protocol-plan-20261115.md)

# Bridge Multi-Agent Collaboration Protocol Enhancement Plan (2026-11-15)

Synthesizes workflow patterns from *"How to build an AI agent swarm that works without you"* alongside [README](../README.md), [Agent Tiering](agent-tiering.md), [Router Prompt](../skills/cli-agent-bridge/resources/router-prompt.md), and [SKILL.md](../skills/cli-agent-bridge/SKILL.md).

## 1. Scope and Non-Goals

Refines orchestration prompts, dispatch/handoff schemas, and Skill docs **without** altering Broker/Worker/SQLite (`store.py`), worktree/copy isolation (`workspace.py`), quotas/scoring, `agent_review` tool schemas, or restarting running services.

Goals: minimize merge conflicts and serial handoff context bloat while raising deliverable auditability and independent verification rigor.

## 2. Current Bridge Strengths vs Remaining Gaps

| Area | Manual File-Board Approach | Existing `cli-agent-bridge` Primitive | Action |
|---|---|---|---|
| **Write isolation** | Prompt-only output paths (`outputs/T[n].md`) | [workspace.py](../src/agent_bridge/workspace.py) isolated Git worktrees / file copies & binary-safe diffs | **Already handled**: retain isolation |
| **State & idempotency** | Manual `BOARD.md` updates | [store.py](../src/agent_bridge/store.py) SQLite transactions, `request_id`, `expected_revision` CAS, `operation_id` | **Already handled**: avoid parallel markdown board |
| **Completion & pagination** | Text `END` sentinel lines | [runtimes.py](../src/agent_bridge/runtimes.py) / [artifacts.py](../src/agent_bridge/artifacts.py) native terminals, cgroups, UTF-8 byte offsets | **Already handled**: verify via native state |
| **Budgets & routing** | Manual turn caps | [routing.py](../src/agent_bridge/routing.py) `token_budget`, 429 cooldowns, off-peak Flash helper, 1-fix cap | **Already handled**: align prompt caps |
| **Task topology** | End-to-end orthogonal modules instead of code->test->review relays | `router-prompt.md` still sketched multi-hop serial relays | **Enhance (3.1)**: single-module closure |
| **Boundary guards & halts** | Upfront pairwise `OUT OF SCOPE`, `DECISIONS`, `STOP AND ASK IF` | Request template lacked pairwise `shared_decisions` / `stop_if` | **Enhance (3.2)**: add fields |
| **Handoff transparency** | Separate `deviations`/`assumptions`/`verified`/`unverified` | Response lacked dedicated separation of guesses vs measured checks | **Enhance (3.3)**: enforce separation |
| **Independent Checker & Fix** | Zero-history heterogeneous reviewer + anti-injection + conflict check | Existing `agent_review` lacked explicit isolation/conflict protocol | **Enhance (3.4)**: add templates |
| **Skill context footprint** | Keep lessons <= 30 lines, fold mature rules into prompts | Dense 47-line `SKILL.md` mixed critical rules with internal internals | **Enhance (3.5)**: link details cleanly |

## 3. Detailed Design

### 3.1 Single-Module End-to-End Closure & Orthogonal Parallelism
- Keep trivial/dependent chains in the primary session; assign each standalone module's "code + unit self-test" closure to **one** worker inside its own worktree rather than splitting coding and unit testing across bouncing sessions.
- Run parallel workers strictly for orthogonal sub-tasks (`out_of_scope` pairwise disambiguated); converge boundary changes cleanly through `lead` without unapproved sub-delegation.

### 3.2 Dispatch (`Request`) & 3.3 Handoff (`Response`) Extensions
- Added `shared_decisions`, `allowed_files` / `out_of_scope` (aligned with [agent-tiering.md](agent-tiering.md)), and `stop_if` (`status: blocked` immediately rather than guessing silently).
- Added required `deviations`, `assumptions`, and `unverified` lists to handoff outputs so unverified guesses can never hide inside `summary` or `evidence`.

### 3.4 Independent Checker (`access="read-only"`), Prompt-Injection Guard & 1-Fix Cap
- Any `workspace-write` change, multi-task merge, or high-risk output must run a **fresh zero-history read-only Checker session** using a verified **heterogeneous model** when available (different runtime & distinct model; excluding Antigravity since it rejects `read-only`) inspecting original spec, actual diff/manifest, independent test exit codes, handoff claims, and cross-task `conflicts` before `agent_review(accepted)`.
- Fix paragraph 1 against indirect prompt injection inside diffs/logs.
- Cap targeted rework at **1 Fix pass** cleanly transferring unmerged workspace state (`cwd = prior worktree` + relative `snapshot_files` verifying `workspace.snapshot_tree` in Git mode; `workspace_mode="copy"` + surviving files without deleted paths verifying `workspace.manifest` in copy mode) followed by a full independent re-audit.

### 3.5 Skill Context Hygiene & Lesson Pruning
- Keep `SKILL.md` focused on critical pre-flight guards, linking implementation specifics to `README.md`, and keep learned lessons one-line dated entries capped at 30 lines by promoting enforced checks directly into code/templates.

## 4. Phased Rollout & Verification

1. **Stage 1**: Update `skills/cli-agent-bridge/resources/router-prompt.md` (principles, Request/Response schemas, Checker & Fix templates, pipelines).
2. **Stage 2**: Trim and align `skills/cli-agent-bridge/SKILL.md` links without modifying Python behavior.
3. **Stage 3**: Align `docs/agent-tiering.md` sections 2-5 while preserving v1 JSONL compatibility and `agent_review` contracts.
4. **Checklist**: Verify strict diff scope (markdown only), consistent YAML keys across all 3 docs, clean `ruff` + `unittest`, zero disruption to running broker daemons, and optional read-only dry run.
