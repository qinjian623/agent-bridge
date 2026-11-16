> Language: English | [中文](zh/verification.md)

# Verification Records

These are dated historical checks, not a statement that every check was rerun for the current release. Private report locations, project/group names, and execution identifiers are omitted. Paths in angle brackets and example filenames below are illustrative replacements; raw conversations and native session IDs are not published.

## 2026-10-27 Antigravity integration

Added the Antigravity runtime, model discovery, conversation-ID continuation, SIGINT interruption, native permission-denial handling, and usage/quota parsing. Each task uses one native process and returns only after the terminal result, process exit, and stderr drain.

- 126 automated tests passed, including 12 new Antigravity tests; Ruff passed.
- Offline CLI fixtures covered multi-turn and cross-process memory, cancel/reopen, process exit without a terminal result, malformed JSON, wrong session ID, extra terminal results, nonzero exit overriding `SUCCESS`, delayed permission denial beyond the stderr buffer, cumulative-counter resets, and quota exhaustion.
- An isolated real broker/worker combination with fake CLI execution passed assertions for directory copies, task persistence, follow-ups, file-change extraction, and 220 cumulative tokens. Read-only requests were rejected before session creation.
- A real local `agy 1.4.3` query returned 13 models. An NDJSON session received `init` with the expected conversation ID and requested-model field. No inference prompt was sent, and the native process was closed.

Unverified: real provider inference, actual tool permission/execution, continuation after a real SIGINT, and real quota exhaustion. No default profile was added, private configuration changed, or shared service restarted. Support was limited to text and `workspace-write`; consumer-plan restrictions are explained in the README.

## 2026-10-22 development checks

This was a runnable development release. Local cross-program execution and restoration of the original parent after exit were checked; real Telegram-message testing still required authorization.

57 automated tests passed. They included a real MCP client, Unix sockets, independent processes, service restart, killed workers, concurrent transactions, budget reservations, native completion, notification deduplication, Telegram pause/deletion markers, child registration before the first turn, and rejection of inherited environment settings that change the declared provider. Parent counters, Claude cumulative deltas, prices, rate-limit waits, three selection strategies, and fixed-comparison assertions were checked. Worker crashes preserved reported partial usage; usage exceeding an in-flight reservation stopped further dispatch.

This round also checked dirty/untracked input snapshots, non-Git copies, patches relative to input snapshots, UTF-8 pagination, historical attempts, parent-configuration restoration, queue reasons, question expiration, first-output timing, service tiers, and process lists. Fake-CLI checks do not establish real provider execution. An earlier filtering change passed 107 `codex-async` tests; those tests had an existing SQLite `ResourceWarning`, and that project was not modified again in this round.

```text
uv run ruff check src tests scripts
All checks passed
uv run python -m unittest discover -s tests -v
Ran 57 tests; OK
skill-creator/scripts/quick_validate.py skills/cli-agent-bridge
Skill is valid
```

The first real-model sweep had 17 attempts: 14 completed normally and three were confirmed interrupted, with no failed or uncertain attempts in that sweep. Three sessions ended paused, five closed, and all test workers exited. Counts below belong to their named checks, not a combined project invoice. Original answers and native IDs remained in private state outside Git.

| Check | Codex | Claude Code | Pi |
| --- | --- | --- | --- |
| Actual provider/model | OpenAI Codex / `gpt-6.1-sol` | DeepSeek Anthropic-compatible API / `deepseek-v4-flash` | DeepSeek / `deepseek-flash` |
| Read a file and calculate `17 + 25` | `42` | `42` | `42` |
| Original-session follow-up | Test marker retained; original text omitted | Test marker retained; original text omitted | Test marker retained; original text omitted |
| Submit while paused | Remained `queued` | Remained `queued` | Remained `queued` |
| Native interruption during execution | `interrupted` | `interrupted` | `interrupted` |
| Context after close/reopen | Original ID and memory retained | Original ID and memory retained | Original file and memory retained |
| Read-only task tries to write | Native read-only filesystem denied it | No write/edit/shell tools | No write/edit/shell tools |

For real write checks, Codex fixed an `add` function and Pi fixed a `slug` function in separate worktrees. A separate Python process ran assertions for each and exited zero. Dirty and untracked source files were unchanged. Task results retained changes, test results, and patch references.

Private smoke, lifecycle, code, and read-only reports were generated by `scripts/*_smoke.py`; illustrative locations are `<private-dev-state>/smoke-report.json`, `lifecycle-report.json`, `code-report.json`, and `readonly-report.json`. These scripts incur real inference costs; ordinary unit tests do not invoke them.

The 17 attempts reported 261,403 tokens, including substantial cached input. This was a partial child-session count, not a project total. Usage outside Claude's main loop, some interrupted usage, and primary-session overhead were incomplete. Database `charged_tokens` includes retained reservations for budget accounting and is neither an invoice nor an actual token total.

## Mapping to the design checks

| ID | Status | Observed result or remaining work |
| --- | --- | --- |
| R1 | Passed | All three programs performed real reads and follow-ups |
| R2 | Passed a complete live case | After Astra exited, the same native parent was restored, checked Sol/Pi, dispatched Claude review, accepted all results, and acknowledged notices |
| R3 | Partial | Tools, inputs, provider/context conditions, and missing-quality information checked; too few categorized quality samples |
| R4 | Basic case passed | Native primary records confirmed `gpt-6-astra`; it submitted Sol/Pi concurrently and all three programs retained their actual configurations; the fixed comparison separately recorded real Astra delegation/review |
| R5 | Listed operations passed | Real open/pause/resume/close/reopen for three programs; a Pi background process stopped with its worker; external tmux/remote jobs were outside the local cgroup |
| R6 | Small task set passed | [Fixed comparison](benchmark.md) covered three methods, three calibration samples, and two independent validation tasks, including parent/all-attempt cost; no general benefit claim |
| E1 | Passed | Concurrent identical requests; changing configuration after automatic selection still returned the original task |
| E2 | Basic cases passed | Revisions, no dispatch while paused, and drain without interruption; no sustained stress test |
| E3 | Passed | Close retained results/directories and reopening retained IDs; closed parent records received no notices |
| E4 | Automated checks and live Pi case passed | Restarted an isolated broker during a native Pi command; worker PID, process identity, and native session stayed unchanged with one execution; fake CLIs also covered no resend after killing a worker |
| E5 | Listed cases passed | Two real edits in separate worktrees, three real read-only tasks, and a real non-Git copy; automated snapshot/patch assertions do not promise hostile same-UID isolation |
| E6 | Passed | Real native tasks plus matching-turn, `agent_settled`, and terminal-state tests |
| E7 | Automated checks passed | Shared concurrency, no explicit-model substitution, shared rate-limit waits, and explicit quota resume; real account quota was not deliberately exhausted |
| E8 | Listed checks passed | Parent/child increments, duplicate events, restoration, unknown prices, and retained partial usage; three real Claude cumulative deltas matched; prices were sourced estimates, actual invoices unknown |
| E9 | Offline and live Claude cases passed | Questions linked to attempts, expired requests rejected, no approval without an answer; one real Claude edit approval permitted only the specified test file; live Codex/Pi interaction remained untested |
| E10 | Local checks passed; Telegram authorization pending | Parent pause/close, deduplication, existing Telegram-state reads, and cold recovery through three programs checked; no real-message test in a separate Telegram topic |
| E11 | Basic cases passed | Three programs reopened after interruption and answered follow-ups; automated explicit continuation created a new attempt |
| E12 | Passed | Correct-attempt acceptance released dependencies; old workers/attempts could not overwrite new results |

## Local installation at that date

The command was installed at the standard user location `~/.local/bin/agent-bridge`. `cli-agent-bridge.service` was running without login autostart. The `cli-agent-bridge` MCP registration ran the project command over stdio, and the skill linked to its repository copy.

Runtime configuration/state used the standard `~/.local/state/cli-agent-bridge`, separate from development test state. No real parent was registered there, so it would not proactively send completion notices to a primary session or Telegram.

During development an older Telegram service treated real CLI children as ordinary sessions and automatically forwarded 11 test replies. That did not complete the planned Telegram integration check. The fix added `external_children` registration plus text/attachment filtering; new workers refused model execution without supported filtering. After confirming no running/queued tasks or pending notices, one Telegram-service reload activated the filtering version. Existing messages were not deleted.

After the fix, one Codex and one Claude small task each returned the expected local test marker. Registration succeeded and, after bot scanning, both native-session outboxes contained zero records. An illustrative report reference is `<private-state>/no-forward-report.json`. Combined with the initial 17, this was 19 real attempts: 16 normal completions and three interruptions.

The scheduled-task service and shared Codex daemon were not restarted.

## Remaining work from that check

The remaining initial-version live check was queue handling, cold recovery, and replies in an independent Telegram topic. It required the previously requested authorization to send test messages.

The unexecuted plan was to use an existing private chat, create a topic with a generic test name, and run the already checked local Astra/Sol/Pi/Claude task while continuing the same parent after its process exited. The parent would produce at most four short replies; children would remain excluded from individual forwarding. Check native turn termination, Bridge acknowledgments, bot outbox, Telegram message IDs, and duplicates. On failure stop that test group without automatic resends. Close test sessions afterward, retain the topic for user inspection, and delete no existing messages. This plan had not run.

Limits: only two independent validation tasks in the fixed comparison, no general savings conclusion, no sustained stress test, and no invoice reconciliation. Inputs were text and explicit local files; native image parameters were unsupported. Unknown prices stayed unknown. Remote/tmux jobs needed separate checks; pause could not undo external operations.

## Additional cost and selection checks

The local default used balanced selection with cost/time weights of 0.5 each; individual tasks could select cost or speed. Parent usage was read from a specified starting point in native cumulative counters without copying full chats. Notification pause and metering-only operation were separate choices. All nine fixed-comparison cases obtained complete native counts; each of the three methods passed two validation tasks. See the [comparison report](benchmark.md).

A separate three-turn Claude check used one running session for two turns, then closed and restored it for the third. Reported usage was 2,058, 2,978, and 2,100 tokens, each equal to the difference between adjacent cumulative totals; all three ended normally. An illustrative report is `<private-state>/usage-report.json`. The new implementation did not rewrite old partial-usage results.

## Cold recovery, files, and processes

`scripts/cold_resume_smoke.py` created an Astra parent and paused Sol/Pi children in an isolated service. After a successful catalog query, it exited the parent, confirmed `notLoaded`, and released the children. The restored Astra inspected code and two assertions, accepted Sol/Pi, created a Claude cross-check, accepted all three attempts, and acknowledged three completion events. The source's faulty functions remained unchanged; edits stayed in child worktrees.

The successful report remained private; an illustrative reference is `<private-cold-state>/cold-success-report.json`. All four native turn contexts retained `gpt-6-astra`, `low`, and `on-request`. The parent model and approval requirement were unchanged. The parent record and all three children were closed afterward.

Two earlier failures retained separate reports rather than being overwritten or called successful:

- `<private-cold-state>/cold-mcp-permission-failure-report.json`: the test client's callback rejected MCP calls, while the initial check read only the model's readiness statement. The fix checked actual native MCP-call success, marked read-only tools correctly, and preapproved only authorized test tools in the isolated session.
- `<private-cold-state>/cold-resume-config-failure-report.json`: restoration lost temporary MCP configuration and connected to the default service, where test tasks did not exist. A private `resume_config_file` restored the same MCP configuration without replacing parent model, sandbox, or approval policy.

Cleanup targeted only those failed tests' own parent queues and active turns and confirmed they stopped. The bot scanned seven Codex/Claude native sessions across the three checks; all were registered external children with zero outbox records. Pi background sessions were outside that scanner. No Telegram topic or test message was created.

`scripts/restart_smoke.py` ran a real Pi task against a non-Git file copy. It restarted the isolated broker after the input script started and before it finished. Worker PID, start identity, and native session stayed unchanged; the script executed once. A background `sleep` still existed in the worker cgroup after the top-level task completed. Closing the child then removed that PID and left no running cgroup processes. No test output appeared in the source directory. An illustrative report is `<private-cold-state>/restart-pi-success-report.json`.

An earlier Codex run passed restart/single-execution checks but mistook sandbox PID 3 for a host PID, so background-process verification failed. That separate failure report was retained; it was not used to claim background cleanup. Pi then verified actual host processes, and interface documentation clarified that cgroup lists use host PIDs.

Snapshot tests covered staged/unstaged changes, deletions, binaries, untracked files, symlink rejection, request retries, and actual patch application. Large-output checks reconstructed complete Chinese text, checked file permissions, and rejected cross-task access. Service-tier tests covered Codex start/resume/execute and rejection of unsupported fields in other programs; they did not measure paid-tier cost or speed gains.

Native metadata confirmed that `fast` reports `priority`, while `priority` and `default` are confirmed unchanged. The alias was treated as the same tier according to official documentation. This sent no inference request; an illustrative private report is `<private-cold-state>/service-tier-alias-report.json`. Installed profiles' tiers were not changed by that check.

## Autonomous model discovery — 2026-10-23

66 automated tests passed, including real MCP parameters, Codex pagination and query-process exit, concurrent cache access, retained old data after failed refresh, Pi provider filtering, exact Claude IDs, DeepSeek redirect restrictions, model settings, fixed session models, and request retries. `ruff check src tests scripts` passed.

All three real catalogs succeeded: seven Codex/OpenAI, two Pi/DeepSeek, and two Claude Code/DeepSeek entries. The latter two listed the same two models and must not be added as distinct models. The raw catalog report stayed private, represented here as `<private-state>/discovery-report.json`. No catalog query sent inference.

| Isolated live target | Result |
| --- | --- |
| `claude-deepseek` + `deepseek-flash`, replacing its default `deepseek-v4-flash` | Passed discovery, session creation, one real request, exact reported model, result checking, and worker close |
| `codex-sol` + `gpt-6-luna`, replacing its default `gpt-6.1-sol` | No output within 90 seconds; task cancelled and worker closed. Catalog visibility did not establish inference success |

Neither check left a live worker. Separate success/failure reports and the failure/cancellation records remained in private discovery test state. Existing `external_children` filtering covered the children; no parent was registered and no real Telegram-message test ran. The checks did not invoke every catalog model or verify a real official Anthropic account.

To repeat one authorized check, use `scripts/discovery_smoke.py --state /tmp/bridge-discovery-example --profile example-profile --model example-new-model-id`. This sends one real inference request and requires a model different from the profile default. It checks the interface, not model cost/speed ranking.

Afterward `cli-agent-bridge.service` was reloaded and queried to confirm three profiles and `objective=balanced`. Login autostart remained disabled. The isolated cold-recovery service stopped, its nine children closed, and all workers exited; failure reports/workspaces were retained.

## Fixes after three independent reviews — 2026-10-23

Codex CLI, Claude Code, and Pi reviews identified 13 confirmed issues that were fixed. Original reviews and reproductions remained private, represented here by `<private-state>/review-reports/`.

| ID | Fix | Check |
| --- | --- | --- |
| F1 | Background processes holding output pipes after native exit no longer block shutdown indefinitely; worker cleanup is bounded | Real inherited-pipe test; isolated systemd worker closed in about 10.18 seconds, including a detached background process and its cgroup |
| F2 | A later model's successful retry is not credited to the first failed model | Automatic-selection test with A failing and B succeeding; both attempts' usage retained |
| F3 | Parent pause, close, and notice ACK do not wait for a whole slow batch | Pause during the first send retained later notices; concurrent ACK was not overwritten by send completion |
| F4 | Validate dispatch before creating sessions/directories; close newly created sessions if state changes afterward | Invalid instruction, dependencies, reservation, retry ID, access, requirements, and dispatch-time state changes |
| F5 | `pause_drain` pauses immediately when no task is executing | Empty session, queued work, and active work |
| F6 | Reject explicit access mismatch for an existing session; inherit access when omitted | CLI request and actual MCP defaults |
| F7 | Worker crashes save a task event and schedule a recovery notice for registered parents | Task remains `recovery_needed`; one notice only; no replay |
| F8 | Retry when notification helper has not started or initialization fails before enqueue | Missing interpreter/client initialization; lost response remains `uncertain` |
| F9 | Retry may change the next token reservation while old attempts retain their original reservation | Retry from zero reservation with a budget, DB upgrade, and old unknown usage still counted |
| F10 | Patch extraction failure returns `failed` with a specific error while preserving answer, usage, and files | Worker test causing a real Git extraction failure |
| F11 | Missing pre-execution counters on restore no longer turn historical totals into new usage | Missing/crashed baselines; real reopened Codex increments matched cumulative differences |
| F12 | Invalid requirements return stable parameter errors; unknown exceptions save uncertain outcomes | Repeated requests return the same error; unknown exceptions do not repeat execution |
| F13 | Retain Pi `tool_execution_start` | Event-filter test; real read-only task recorded one tool-start and one tool-end event |

Automated checks increased from 66 to 89, all passed, and Ruff passed. Before updating the service, SQLite backup saved its database. Real short requests returned the expected answers and specified models.

## Second fix round — 2026-10-23

All 12 confirmed issues from the second review were fixed.

| ID | Change | Check |
| --- | --- | --- |
| R1 | Bind interrupt request/timing to its attempt; wait for the old request before the next task; stop the worker on interrupt error/timeout and leave unstarted work queued | Late/error/timeout interrupts cannot cancel later work; unconfirmed requests prevent the next task from starting |
| R2 | Rotate notices by last-attempt time stored in SQLite | An eleventh notice still sends when the first ten wait on busy parents, wakeup, or Telegram pause; DB upgrade retains records |
| R3 | Worker startup/connection/pre-execution failures save events and notify parents | Connect failure, timeout with empty message, launcher failure, and assigned-but-unstarted work; no duplicate notice or automatic rerun |
| R4 | Follow `nextCursor` in native notification-queue inspection | Target on page two, target absent, repeated cursor |
| R5 | Allow ACK of received notices while `sending` or `uncertain` | ACK before initial send returns, lost response, no later resend; still reject `pending` and wrong-parent ACK |
| R6 | Derive notice instructions from the saved event state | Review only after success; inspect/resolve uncertainty; distinct operations for failure/interruption/cancellation/startup failure; retry does not rewrite old instructions |
| R7 | Preserve missing/invalid Claude cumulative counters as unknown | Empty/restored/partial/reset counters and temporary missing models; complete usage only after a valid interval |
| R8 | Preserve Pi terminal state, answer, and files when final-stat collection fails after confirmed completion | Actual Worker/PiRuntime with simulated timeout; reported tokens retained as incomplete, cost unknown |
| R9 | Keep the original attempt's failure sample while a different-model retry has not started | Queued, assigned, and cancelled retries retain original-model failure; unfinished work is not a complete cost sample |
| R10 | Check current tasks and historical attempts before assigning shared parent overhead to a single task | Moving one task does not let the remaining task claim the old parent's entire shared cost |
| R11 | Compare/save permissions in copy mode | Chmod-only, content-plus-mode, new, and unchanged files; original directory permissions unchanged |
| R12 | User cancellation/close takes precedence over timeout or budget interruption | Simultaneous cancellation/close, timeout, and budget conditions record cancellation |

Automated checks increased from 89 to 114, all passed; Ruff and skill-format checks passed.

## Groups and explicit-group broadcasts — 2026-11-02

Added `agent_groups`, `agent_group`, and `agent_broadcast` without replacing the original nine interfaces or parent notices. Members are existing Bridge session IDs. Broadcasts require an explicit `group_id` and revision; each recipient uses its existing task queue. See [Groups and Broadcasts](getting-started.md#groups-and-broadcasts).

Fourteen new checks covered creating/deleting groups, adding/removing/duplicating members, excluding outsiders, shared members across groups, concurrent identical requests, changed-payload rejection, retry after membership changes, original task IDs after restart, empty/unknown groups, paused/closed states, budgets, complete rollback on write failure, and existing tasks/parent notices after DB upgrade.

Independent-process tests started three isolated fake-CLI sessions and added only two to the target group. Full results were `226 passed, 42 subtests passed in 26.39s`; Ruff and skill checks passed.

## Native groups and non-interrupting queues — 2026-11-02

`agent_group` gained optional `native_members` to register existing Codex/Antigravity IDs, directories, and names. Codex enqueue uses the already-running shared service's `thread/queue/add`; `agent_broadcast_status(refresh=true)` reads queue/history receipts. The full suite reached 242 passing tests. An authorized live group registered four sessions; its private name and IDs are omitted. Three Codex sessions received enqueues; active dual-instance AGY was left unpushed.

## AGY configuration and reading group messages — 2026-11-02

The restarted AGY process initially used the home directory; its directory was corrected through the group interface. `agy mcp add` registered MCP and an independent client completed the handshake. The read-only paginated `agent_group_inbox` was added; the full suite reached 244 passing tests.

## Restoring closed AGY from group messages — 2026-11-03

Native AGY broadcasts are saved as `pending`. After confirming the original instance has fully closed, one stream-json session resumes with its original native ID. `agy_presence.py` and `agy_wake.py` check presence flocks and process ownership. Thirty-five new tests brought the suite to `279 passed, 66 subtests passed in 15.64s`; Ruff passed. The tests covered offline wakeup, presence/duplicate-process protection, post-baseline `user_input/DONE`, child-MCP cleanup, and systemd `comm`/`cmdline` matching.

## Total task deadline versus external pause — 2026-11-13

Three Pi tasks ended at about 3600 seconds due to `task_timeout`, rather than a neighboring session's pause request about 74 seconds later. Non-success results now separately preserve `termination` (`cause`, `stage`, `elapsed_seconds`, `task_timeout_seconds`) and `native_error`. The complete 840-test unittest suite and 21 worker checks passed.

## Non-UTF-8 paths and per-task timeouts — 2026-11-13

Fixed a `UnicodeDecodeError` from a valid `0xff` byte in `git ls-files -z` that had overwritten terminal task state. Retain raw bytes, raw Git patches, escaped display values, and optional `untracked_files_raw_hex` / `changed_files_raw_hex` lists. Added validated `requirements.task_timeout` with local/remote prechecks and explicit rejection for older nodes/workers without `task_timeout_v1`, rather than silently ignoring the request.
