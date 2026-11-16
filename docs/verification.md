> Language: English | [中文](zh/verification.md)

# Verification Records

## 2026-10-27 Antigravity Integration Check

Added the Antigravity runtime, model discovery, `conversation` ID resume, `SIGINT` cancellation, native permission-denial failure tracking, and metered/cooldown parsing. One native CLI process runs per turn, completing only after terminal result events, process exit, and stderr completion.

- 126 automated tests passed and Ruff clean (including 12 new Antigravity tests).
- Offline CLI fixture coverage verified multi-turn/cross-process memory, cancel + reopen persistence, exit without terminal result, malformed JSON, wrong session ID, extra terminal results, non-zero exit overriding `SUCCESS`, delayed permission denial beyond buffer size, usage baseline resets, and quota exhaustion.
- Isolated broker/worker integration passed (file copy isolation, task store, follow-ups, diff extraction, 220 cumulative tokens; pre-creation rejection on `read-only`).
- Real `agy 1.4.3` query returned 13 models and launched an NDJSON session confirming `init`, conversation ID, and model without sending paid inference prompts.

Unverified: real provider inference, real tool permissions/execution, live `SIGINT` resume, and live quota exhaustion. Consumer subscription terms remain governed by the notice in README.

## 2026-10-22 Development Verification

Automated suite: 57 tests passing covering live MCP stdio clients, Unix sockets, isolated worker processes, broker restarts, worker kill recovery, SQLite concurrency transactions, budget reservations, native terminal detection, notification deduplication, Telegram pause/deletion checks, child registration prior to first turns, inherited env isolation, parent usage accumulation, Claude cumulative deltas, pricing fields, cooldown backoff, all three selection strategies, and fixed benchmark assertions (`ruff` clean, `skill-creator` validator clean; legacy 107 `codex-async` tests also passed on filter integration).

Initial real-model sweep executed 17 attempts (14 completed, 3 interrupted; 3 paused, 5 closed, zero live workers left). Raw transcripts and session IDs stayed in private local directories without committing to Git.

| Check | Codex | Claude Code | Pi |
| --- | --- | --- | --- |
| Provider / Model | OpenAI Codex / `gpt-6.1-sol` | DeepSeek Anthropic API / `deepseek-v4-flash` | DeepSeek / `deepseek-flash` |
| File read + `17 + 25` | `42` | `42` | `42` |
| Follow-up memory ("lantern") | Retained | Retained | Retained |
| Queued while paused | Stayed `queued` | Stayed `queued` | Stayed `queued` |
| Native interrupt mid-turn | `interrupted` | `interrupted` | `interrupted` |
| Close -> Reopen context | Preserved ID + memory | Preserved ID + memory | Preserved file + memory |
| Read-only write attempt | Native RO sandbox blocked | No write/edit/shell tools | No write/edit/shell tools |

Writable tasks (`add` in Codex, `slug` in Pi) ran in isolated worktrees and passed independent verification with zero edits to dirty/untracked caller checkouts (`scripts/*_smoke.py` reports kept in `<private-validation-state>/report`). Usage totaled 261,403 tokens across those 17 attempts (including prompt caching; child-only partial count rather than full project billing).

### Mapping Against Design Criteria (`R1`-`R6`, `E1`-`E12`)

- **R1-R6**: Verified live multi-runtime reads/writes/follow-ups, cold Astra parent recovery processing Sol/Pi results plus Claude cross-check (`benchmark.md` for controlled 2-task comparisons), explicit model constraints, multi-model per-runtime isolation, and lifecycle controls (`R3` partial on sample volume).
- **E1-E12**: Verified concurrent idempotency (`E1`), revision/pause/drain guards (`E2`), close/reopen isolation (`E3`), mid-task Pi broker restart preserving worker PID (`E4`), isolated worktrees/copies (`E5`), native terminal markers (`E6`), shared quota cooldowns (`E7`), non-zero missing usage handling (`E8`), expiring interactive approvals in Claude (`E9`), local parent notifications + Telegram filter blocking 11 unintended replies and verifying 0 bot outbox entries after fix (`E10`), interrupt continuation (`E11`), and attempt-locked `agent_review` gating (`E12`).

## Cold Resume, Files, and Process Inspection Follow-Up

`scripts/cold_resume_smoke.py` created an Astra coordinator session plus Sol and Pi child tasks inside an isolated service, exited Astra (`notLoaded`), let children complete, then resumed Astra cleanly under its original `gpt-6-astra / low / on-request` policy to inspect diffs, pass assertions, cross-check via Claude, accept all 3 attempts, and ACK notifications without touching source repos (`<private-validation-state>/report.json`). Earlier two failures cleanly diagnosed MCP callback permissions and ephemeral `--mcp` loss fixed via owner-only `resume_config_file`.

`scripts/restart_smoke.py` validated non-Git `copy` execution with Pi across a mid-run broker restart, confirming preserved worker PID, host PIDs in cgroup (`owned_processes`), and clean exit of background `sleep` subprocesses upon session close (`restart-example-run-report.json`). Earlier Codex sandbox PID mismatch (`PID 3` inside sandbox vs host PID) stayed explicitly documented rather than misclaimed. `service_tier` alias `fast -> priority` was confirmed without paid inference (`service-tier-alias-verified.json`).

## Autonomous Model Discovery (2026-10-23)

66 automated tests + Ruff passed covering catalog caching, pagination, stale error retention, Pi provider filtering, Claude resolved model IDs, and DeepSeek restrictions. Zero-inference catalog queries returned 7 Codex/OpenAI models, 2 Pi/DeepSeek models, and 2 Claude/DeepSeek models. Isolated live model probe (`discovery_smoke.py`): `claude-deepseek` + `deepseek-flash` succeeded (`OK`); `codex-sol` + `gpt-6-luna` timed out after 90s with zero output and cancelled cleanly, confirming catalog visibility alone does not guarantee inference availability.

## Third-Party Review Fixes — Round 1 & Round 2 (2026-10-23)

- **Round 1 (`F1`-`F13`, 89 tests passing + Ruff + 5 short real follow-up requests)**: Bounded pipe cleanup & worker shutdown timeout (`F1`), model-change quality attribution isolation (`F2`), non-blocking coordinator pause/close/ACK (`F3`), pre-session dispatch validation (`F4`), immediate idle `pause_drain` (`F5`), explicit access-mode mismatch rejection (`F6`), worker-crash `recovery_needed` parent notification (`F7`), retryable helper initialization failures (`F8`), per-retry reservation updates (`F9`), failed patch preservation (`F10`), missing pre-resume usage baseline markers (`F11`), stable error on bad requirements (`F12`), and Pi `tool_execution_start` events (`F13`).
- **Round 2 (`R1`-`R12`, 114 tests passing + Ruff + 5 real short requests)**: Attempt-scoped interrupts & stop on error (`R1`), FIFO starvation-free delivery rotation via `deliveries.last_attempt` (`R2`), startup failure parent alerts with `queued` preservation (`R3`), paginated `nextCursor` native queue checks (`R4`), ACK while `sending`/`uncertain` (`R5`), state-aware notification instructions (`R6`), null unknown Claude usage intervals (`R7`), Pi preserved answers on stats timeout (`R8`), unstarted retry failure sample retention (`R9`), full-history multi-task parent checks (`R10`), `copy` mode permission diffs (`R11`), and cancel/close precedence over timeout/budget (`R12`).

## Session Groups & Targeted Broadcasts (2026-11-02)

Added `agent_groups`, `agent_group`, `agent_broadcast`, then `native_members`, `agent_group_inbox`, and closed Antigravity wake support (`agy_presence.py`, `agy_wake.py`):
- 226 tests (groups) -> 242 tests (native Codex queue attachment without waking closed terminals + `clientId` receipt proofs) -> 244 tests (`agent_group_inbox` pagination with readonly cursor) -> 279 tests / 66 subtests (35 new fake-CLI tests for offline AGY auto-resume, presence/flock locking, duplicate process guards, post-baseline `user_input/DONE`, child MCP reaping, and systemd `comm`/`cmdline` matching).
- Live group `example-agents` attached 4 sessions (3 Codex enqueued via `thread/queue/add`; active dual-instance AGY left unpushed safely, later directory corrected from `~` and MCP registered cleanly without injecting into live terminals).

## 2026-11-13 Total Task Deadline, Non-UTF-8 Paths, and Per-Task Timeouts

- Diagnosed three Pi jobs hitting the default 3600s `task_timeout` (independent of a later pause on another session ~74s afterward); separated structured `termination` (`cause`, `stage`, `elapsed_seconds`, `task_timeout_seconds`) and raw `native_error` cleanly (840 tests passing + 21 worker tests using event waits).
- Fixed `git ls-files -z` non-UTF-8 byte decoding crash (`0xff`) using escaped display arrays plus parallel raw hex arrays (`untracked_files_raw_hex` / `changed_files_raw_hex`) and raw byte patches; added validated `requirements.task_timeout` checked upfront across local and remote paths with explicit rejection on legacy workers lacking `task_timeout_v1`.
