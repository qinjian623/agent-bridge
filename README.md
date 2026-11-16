> Language: English | [中文](docs/zh/README.md)

# cli-agent-bridge

Manage independent sessions for Codex, Claude Code, Pi, and Antigravity locally or across hosts via MCP or the CLI. Sessions support multi-turn follow-ups, pause, interrupt, close, and reopen; tasks and results are persisted in SQLite, while worker processes run independently of the MCP client and broker service.

This is a working development release. Real cross-runtime execution, interrupts, session reopening, and parent-session cold recovery after exiting an Astra coordinator while still processing Sol, Pi, and Claude results have been verified. For a small benchmark including parent-session overhead, see [Benchmark](docs/benchmark.md). Live interactive Telegram message delivery remains unexecuted without explicit test authorization; see [Verification Log](docs/verification.md).

For multi-machine operation, see [Multi-Machine Design Specification](docs/multi-machine-design.md) (integrating [Adversarial Review Findings](docs/multi-machine-review.md)). The codebase implements `agent-bridge relay`, SSH JSONL framing, node handshakes, host inspection, remote task dispatch, and cross-host session groups. Remote hosts run their own independent broker; connections are multiplexed per host, peer identity is verified before requests, and dropped connections never blindly resend unconfirmed requests. Initial service startup and read-only communication checks against a secondary macOS node are documented in [Host Service Verification](docs/macos-node-service-verification-20261108.md) (without remote model inference or broadcasts).

Wire architecture, protocol envelopes, remote native inbox polling, dynamic relay refresh, and upgrade handling are specified in [Multi-Machine Interface Contract](docs/multi-machine-interface.md). `peer.control`, `peer.review`, and `peer.inbox` are not yet implemented; do not assume declared protocol types imply live implementation.

The broker automatically watches `hosts` in `config.json` and `host_pins.json` so hosts can be added or removed at runtime without restarting the broker. You can also trigger immediate reconciliation with `agent-bridge call agent_hosts_reload --json '{}'`; `agent_hosts.config_error` reports validation errors or rejected changes. Unchanged hosts keep their existing SSH sessions, while changed hosts drain in-flight requests and ACKs before closing old connections. Invalid configs retain the last working registry without resending work.

Hosts with active group membership, pending deliveries, or uncertain outcomes refuse deletion or target modification; historical parent notification routes require read-only confirmation that sessions are closed and tasks have finished and been reviewed. Removing a host does not stop remote processes or purge history. See [Dynamic Hosts Check](docs/dynamic-hosts-20261109.md) for usage and constraints.

Antigravity is an integrated runtime: offline CLI protocol fixtures and broker/worker tests cover multi-turn follow-ups, cancellation, resume, permission denials, and token accounting; a live `agy 1.4.3` check verified model catalog discovery and zero-inference initialization. Real model inference, tool execution, and live interrupt integration require per-environment verification.

## Antigravity

Add a profile entry to private `config.json` alongside existing profiles:

```json
"agy-sonnet": {
  "runtime": "antigravity",
  "provider": "google-antigravity",
  "model": "claude-sonnet-5-5-medium",
  "quota_group": "antigravity-account",
  "billing_mode": "subscription",
  "verified": false
}
```

The default executable is `agy` (overridable via `executable`). Even when selecting a Claude model, `provider` must remain `google-antigravity` rather than `anthropic`. Use the exact full model ID returned by `agy models`; if setting `effort`, it must match the ID suffix tier (`low`/`medium`/`high`). Unverified profiles are excluded from automatic balanced selection unless targeted explicitly.

The broker loads `config.json` on startup. Restart the broker service to query newly added profiles (existing standalone workers keep running untouched until idle/reopened):

```bash
systemctl --user restart cli-agent-bridge
```

```bash
agent-bridge call agent_catalog --json '{"profile":"agy-sonnet","refresh":true}'
agent-bridge call agent_open --json '{"profile":"agy-sonnet","cwd":"/absolute/project","access":"workspace-write","request_id":"agy-open-1"}'
# Pass the returned session_id to agent_dispatch, agent_cancel, and agent_control.
```

- Supports text prompts and `workspace-write` only. `read-only` requests are rejected before session creation; `--mode plan` is not treated as a read-only filesystem boundary. Writable sessions use isolated worktrees or directory copies.
- Native headless mode does not support bridge per-tool approval callbacks or Claude SDK control messages. Runs use native permission policies with `--sandbox` and never pass `--dangerously-skip-permissions`. Any denied native tool marks the bridge task `failed` even if the CLI exits with status `SUCCESS`. `tools` describes routing metadata rather than an Antigravity tool whitelist.
- Each task spawns an isolated `--input-format stream-json --output-format stream-json` process, sends one prompt, closes stdin, and resumes subsequent turns with the same conversation ID. Tasks complete only after the terminal `result` for that conversation, process exit, and stderr draining finish. Process exit or partial text output alone never proves completion.
- `SIGINT` requests cancellation. Interrupted state is recorded only after a native `INTERRUPTED`/`CANCELED` terminal event; unexpected exit enters `recovery_needed`. Confirmed interruption shuts down that process cleanly and resumes next turn via `--conversation`.
- The first turn in a new session records cumulative usage without double-counting thinking tokens. Whenever a fresh process after resume lacks a verified baseline or counters reset, historical totals are never treated as newly consumed—only observed turn increments are reported with `complete=false`. Unknown cache pricing yields null cost estimates rather than $0; CLI estimates are never actual invoices.
- Terminal `RESOURCE_EXHAUSTED` errors trigger per-quota cooldown backoff without silent model switching or automatic retries.

Account terms notice: Google's [official FAQ](https://www.antigravity.google/docs/faq#can-i-use-antigravity-cli-headless-mode-in-third-party-coding-agents-or-automate-account-switching) notes that consumer tiers do not authorize wrapping `agy` inside third-party coding agents or apps. Implementing runtime support does not grant license to violate account terms—use only under appropriate authorization. This project does not auto-provision Antigravity accounts. Protocol reference: [Headless mode](https://www.antigravity.google/docs/cli/headless).

## GitHub Releases and Scheduled OTA Updates

GitHub Actions runs offline tests, Ruff linting, and clean packaging/installation smoke checks on Linux and macOS for pushes and pull requests. Pushing a matching `vX.Y.Z` tag builds and publishes the wheel, source tarball, `SHA256SUMS`, and `latest.json`; see [CI and Releases](docs/ci-release.md).

Manage scheduled self-updates from the CLI:

```bash
agent-bridge update install
agent-bridge update check
agent-bridge update run
agent-bridge update --drain-workers run
```

Settings live in `~/.local/state/cli-agent-bridge/update.json` (mode `0600`) specifying the GitHub repository, managed releases directory, `systemd`/`launchd` service name, and check interval. OTA updates pull release assets **directly from public GitHub Releases over HTTPS** (honoring `HTTPS_PROXY` via `CONNECT` if configured) and verify `SHA256SUMS`, requiring no `gh` CLI login or API credentials. Installation does not auto-enable the timer or disturb an unmanaged broker service; configure and enable managed services via `agent-bridge init` (item 5: Services & OTA) or migrate explicitly before enabling periodic checks. Linux checks hourly with up to 5 minutes of jitter; macOS uses a `LaunchAgent` timer.

Updates download the SHA-256-verified wheel, install into a dedicated versioned virtual environment, and run configuration + isolated smoke broker health checks before switching. Hot OTA updates are enabled by default: live tasks and compatible workers keep running untouched while the broker drains short in-flight API requests, notifications, and non-SSH helpers before briefly entering maintenance; compatible workers and SSH relays finish their current turn cleanly and retire on idle so new sessions spawn on the updated launcher (full worker/task drain happens only when `REQUIRES_WORKER_DRAIN` is set or `--drain-workers` is passed); paused queues stay preserved. Before switching symlinks, the short maintenance lease blocks new dispatches/broadcasts and coordinator polls; the CLI transparently retries read-only requests and idempotent `request_id` writes across the brief restart gap. After restarting, the updater checks actual version and PID, rolling back the symlink automatically on confirmed startup failure (without rolling back SQLite or replaying tasks); ambiguous restarts enter `uncertain` and halt further auto-retries. See [OTA Hot Update Design](docs/ota-hot-update-20261116.md).

`~/.local/share/cli-agent-bridge/bin/agent-bridge` is the stable managed launcher path. Broker services, CLI invocations, MCP registrations, remote host `relay` paths, and skills should point to that launcher (or `current/skill`) rather than stale source checkouts. Versioned directories preserve uncommitted edits in source repos, never restart live interactive terminals, and never upgrade external CLIs (`codex`, `claude`, `pi`, `agy`).

The latest updater outcome is recorded in `<state>/update-status.json` along with failure classifications. Never equate installing an update timer with a completed upgrade.

## Local Installation and Setup

Supports **Linux** (recommended user-level `systemd` with linger enabled for headless use; set `worker_launcher` to `"detached"` in container/non-systemd environments) and **macOS** (`launchd` + `"detached"` workers). Native Windows is unsupported; run inside WSL2 on Windows. Requires Python 3.11+, `uv`, and authenticated target CLIs. Dependencies pin MCP Python SDK 1.30.0 and Claude Agent SDK 0.2.172.

```bash
uv sync --locked
uv run agent-bridge init
uv run agent-bridge doctor
uv run agent-bridge serve
```

`agent-bridge init` inspects locally installed CLIs (`codex`/`claude`/`pi`/`agy`), `uv`, and user service managers, then walks you interactively through explicit setup of **runtime profiles, defaults, local projects, optional remote hosts, managed services (`systemd`/`launchd`), and GitHub Releases OTA**. When `config.json` (or `update.json`) already exists, `agent-bridge init` reloads it cleanly and lets you update subsets idempotently while preserving untouched settings, profile verification flags, and pinned host identities (invalidating stale pins in both `config.json` and `host_pins.json` only when removing a host or changing its `ssh_target`, and clearing stale verification/price metadata when changing a profile's runtime, provider, or model). Leaving `hosts` empty runs cleanly in standalone single-machine mode. The default state directory is `~/.local/state/cli-agent-bridge`.

For cross-platform managed service installation (**Linux `systemd --user`** or **macOS `launchd`**), use `agent-bridge init` (item 5: Services & OTA) or pass explicit flags:

```bash
uv run agent-bridge init --non-interactive --install-broker-service --enable-broker-service
```

On **Linux only**, `scripts/install_local.py` is a convenience script that symlinks the local `.venv` CLI/skill and writes a `systemd --user` unit without overwriting existing files (do **not** run it on macOS, as it invokes `systemctl --user`):

```bash
# Linux (systemd --user) only:
uv run python scripts/install_local.py
systemctl --user start cli-agent-bridge
systemctl --user enable cli-agent-bridge
```

Stopping the broker service does not kill detached worker units; close child sessions explicitly with `agent_control(action=close)`.

Register MCP in Codex via the CLI:

```bash
codex mcp add cli-agent-bridge -- /absolute/path/to/agent-bridge mcp
```

CLI and MCP share identical tool contracts:

```bash
agent-bridge call agent_catalog --json '{}'
agent-bridge call agent_inspect --json '{}'
agent-bridge call agent_open --json '{"profile":"codex-sol","cwd":"/absolute/project","request_id":"open-parser-1","access":"workspace-write"}'
agent-bridge call agent_dispatch --json '{"session_id":"<session_id>","instruction":"Fix parser bug and run targeted tests","request_id":"fix-parser-1","reservation":20000}'
```

Writable Git sessions create isolated worktrees from a target commit (`HEAD` by default). Include dirty or untracked files explicitly via `snapshot_files=["src/parser.py","tests/test_parser.py"]`; directories, symlinks, and path-traversal entries are rejected, and the source checkout is never committed, stashed, or cleaned. Returned patches diff strictly from that initial input snapshot.

Non-Git directories can set `workspace_mode=copy` alongside `snapshot_files`; only listed files are copied and tracked with content/permission manifests (including chmod-only changes). Retries with identical `request_id` preserve the initial snapshot without overwriting in-progress edits. Note that worktrees and file copies isolate edits rather than acting as OS security sandboxes.

## Tool Interface

| Tool | Behavior |
| --- | --- |
| `agent_catalog` | List configured profiles, discovered provider models, verification status, and limits (`refresh=true` bypasses cache) |
| `agent_open` | Open an independent session (creating a worktree for writable access; optional display `name`) |
| `agent_dispatch` | Enqueue work on an open session, target a profile/model, or use balanced auto-selection (`retry_task_id` retries failed work) |
| `agent_control` | `pause`, `pause_drain`, `resume`, `close`, `reopen`, `respond`, `resolve`, plus `parent_*` operations |
| `agent_cancel` | Cancel a single queued or running task without closing its session |
| `agent_inspect` | Overview runtime/provider/model/profile across sessions, or inspect detailed session/task/parent state |
| `agent_wait` | Block up to 50s waiting for sequence events |
| `agent_result` | Fetch concise answers, patches, attempts, usage, or UTF-8 byte-paginated artifacts |
| `agent_review` | Accept or reject the current completed attempt with concrete inspection evidence |
| `agent_groups` | List session groups or inspect revision, members, and status |
| `agent_group` | Create/delete groups and add/remove bridge or native members |
| `agent_broadcast` | Dispatch a task to each member in an explicit group, returning per-recipient task IDs or skip reasons |

Mutating operations require a stable `request_id`. Exact retries return cached results; reusing a `request_id` with differing arguments raises a conflict. Control and review operations require `expected_revision` from `agent_inspect` or `agent_result.session_revision`.

`name` applies only when creating a new session: trimmed length 1-128 Unicode chars without NUL, newlines, or control characters (non-ASCII names and duplicates allowed). Passing `name` with an existing `session_id` raises `invalid_request`; rename existing sessions via `agent-bridge name`. `name` is hashed into the request fingerprint.

### Groups and Broadcasts

Group members may be bridge-managed `session_id`s or registered native sessions (`native_members`), and a session may join multiple groups. Grouping never auto-adds sessions or alters parent notifications.

```bash
agent-bridge call agent_group --json '{"group_id":"backend","action":"create","session_ids":["session-a","session-b"],"expected_revision":0,"request_id":"backend-create-1"}'
agent-bridge call agent_groups --json '{}'
agent-bridge call agent_groups --json '{"group_id":"backend"}'
agent-bridge call agent_broadcast --json '{"group_id":"backend","message":"Review API interface changes and report results","expected_revision":0,"reservation":8000,"request_id":"backend-update-1"}'
```

`add` and `remove` require non-empty member lists; `delete` omits member lists. Revisions advance via CAS; idempotent repeats do not double-add or advance revisions. Unknown sessions reject the entire mutation. Deleted group IDs are retired permanently.

Broadcasts require an existing non-empty `group_id` and exact `expected_revision`; wildcard targeting is unsupported. Snapshots, tasks, and delivery records commit atomically in SQLite; subsequent group changes affect future broadcasts only. Busy recipients queue normally, while paused, draining, closed, or recovery-pending members report `skipped`. Exact `request_id` retries return original task IDs; failed recipients require explicit retry rather than automatic resends.

Existing native Codex or Antigravity sessions join via `native_members` (`runtime`, `native_id`, absolute `cwd`, optional `label`; removal requires only `runtime` and `native_id`):

```bash
agent-bridge call agent_group --json '{"group_id":"native-team","action":"create","native_members":[{"runtime":"codex","native_id":"<native-session-uuid>","cwd":"/absolute/project","label":"lead"}],"expected_revision":0,"request_id":"native-team-create-1"}'
agent-bridge call agent_broadcast --json '{"group_id":"native-team","message":"Team status update","expected_revision":0,"request_id":"native-team-notice-1"}'
agent-bridge call agent_broadcast_status --json '{"broadcast_id":"<broadcast_id>","refresh":true}'
```

Codex native messaging appends via `thread/queue/add` on an active shared daemon (`start_daemon=False`) without sending `thread/resume`, `turn/start`, or interrupts, preserving terminal locks and skipping closed sessions. Antigravity saves new group messages as `pending`, coordinates via shared locks (`antigravity-locks/<native_id>.lock` plus native presence/process checks), waits while active or ambiguous, and resumes offline instances under original native settings without permission-bypass flags or replaying historical `skipped` items.

`agent_broadcast_status` distinguishes `pending`, `enqueued`, `received` (matched native `clientId` / post-baseline Antigravity `user_input/DONE` with `native_turn_id`), and `processing_status=running/completed/failed`. `refresh=true` inspects Codex queues/recent 100 turns read-only without launching processes on Antigravity. Unconfirmed deliveries become `uncertain` or `unconfirmed` and must be checked rather than blindly resent.

Native sessions sit outside bridge worker metering; broadcasts containing native members are rejected when `token_budget` is enabled, and native deliveries cannot be canceled via `agent_cancel`. Native sessions can poll their own inbox read-only via `agent_group_inbox(group_id, runtime, native_id, after, limit)` (max 10 per page with monotonic `cursor`). Register Antigravity MCP via `agy mcp add cli-agent-bridge /absolute/path/to/agent-bridge mcp` (`/mcp` reloads open UI windows; see [Antigravity MCP docs](https://www.antigravity.google/docs/cli/mcp/)).

Control semantics: `pause` interrupts the active turn and stops dispatches; `pause_drain` waits for an active turn (or pauses immediately when idle); `resume` advances subsequent queue items unless `continue_task_id` explicitly re-runs the interrupted task; `close` cancels unstarted items and preserves history/worktrees until explicit `reopen`.

`task_timeout` defaults to 3600s total per attempt (streaming output does not extend the wall-clock deadline). Override per task with `requirements={"task_timeout": 21600}` (rejected cleanly by older nodes/workers lacking `task_timeout_v1`). Non-UTF-8 Linux filenames remain losslessly safe via escaped display fields alongside raw hex arrays (`untracked_files_raw_hex` / `changed_files_raw_hex`) and raw Git patches.

Dispatching to an existing session inherits its access mode unless overridden (mismatches error cleanly). Large outputs save privately (`0600`) and paginate in UTF-8 chunks up to 65,536 bytes (`agent_result` with `artifact_id` and `next_offset`). `agent_inspect` includes host PIDs from systemd worker cgroups (`owned_processes`; external tmux/remote jobs report `external_jobs=unknown`). Unexpected disconnects enter `recovery_needed` and notify registered parents rather than resending blindly; resolve with `agent_control(action=resolve, ...)` after manual verification. Startup/connect failures keep unstarted tasks `queued` for explicit `resume`.

## Autonomous Model Discovery and Selection

`agent_catalog()` queries configured providers (cached 5 minutes; `profile=...` scopes cleanly, `refresh=true` bypasses cache) and returns exact IDs, sources, timestamps, input modalities, and context limits under `discovery`. Catalog presence never certifies account permission or `verified=true`.

| Runtime / Provider | Discovery Method |
| --- | --- |
| Codex / OpenAI | App Server paginated `model/list` |
| Pi / configured provider | RPC `get_available_models` filtered by provider |
| Claude Code / Anthropic | SDK `get_server_info().models` (`resolvedModel`) |
| Claude Code / DeepSeek | DeepSeek `GET /models` supporting Anthropic Messages |
| Claude Code / Ollama Cloud | Ollama `GET /api/tags` |

Coordinators can target cataloged models on demand via `profile` + `model` (`agent_open` / `agent_dispatch`) while inheriting credentials, provider, permissions, `quota_group`, and effort (validated against discovered model capabilities). Discovered models never inherit old prices or verification flags; omit `profile/model` to keep balanced selection scoped to verified profiles.

References: [Codex App Server](https://learn.chatgpt.com/docs/app-server), [Pi RPC](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md), [Claude Python SDK](https://code.claude.com/docs/en/agent-sdk/python), [DeepSeek Models](https://api-docs.deepseek.com/api/list-models/), [Ollama Cloud](https://github.com/ollama/ollama/blob/main/docs/cloud.mdx).

### Claude Code with Ollama Cloud

Add a separate profile in `config.json`:

```json
"claude-ollama": {
  "runtime": "claude",
  "provider": "ollama-anthropic",
  "endpoint_host": "ollama.com",
  "model": "deepseek-v4.1-flash",
  "env_file": "/absolute/private/ollama-env.json",
  "quota_group": "ollama-account",
  "billing_mode": "api",
  "verified": false
}
```

`env_file` is an owner-only `0600` JSON string map setting `ANTHROPIC_BASE_URL=https://ollama.com` (and optional proxy variables; when unset, the Claude Ollama wrapper defaults `HTTPS_PROXY` to `http://127.0.0.1:1080`, so set `HTTPS_PROXY` explicitly for your environment). Auth uses `OLLAMA_API_KEY` or falls back to owner-only `0600` `~/.config/claude-ollama/api-key` without sharing DeepSeek keys.

## Balanced Selection and Costs

Explicit profiles are never silently replaced. Auto-selection (`default_strategy=balanced`) filters by input type, tools, context size, provider policy, cooldowns, verification status, and historical quality before weighting normalized cost and duration 0.5/0.5 (override per task with `requirements.strategy=cost|speed|balanced`; insufficient data returns `Unmeasured` in profile order). Samples include all task attempts plus metered single-task parent overhead (`requirements.learn_from_result=false` excludes fixed comparisons from selection updates). Cross-model retry keeps attempt quality isolated to each model while counting total attempt budget/cost.

By default at most 3 external tasks run concurrently, capped at 2 per `quota_group`, and one active turn per session. When `token_budget` is configured, every dispatch reserves estimated tokens inside an SQLite transaction (`retry` updates future reservations only); external CLI metering has reporting delay, so budgets are best-effort rather than hard provider billing caps. Codex supports `service_tier` (`fast` maps to `priority` per [Codex Config Reference](https://learn.chatgpt.com/docs/config-file/config-reference)); Claude and Pi reject `service_tier`.

Attempt metrics record start/end timestamps, `first_output`, native deltas (`model_usage` on Claude per [Claude Cost Tracking](https://code.claude.com/docs/en/agent-sdk/cost-tracking)), optional `price={"currency":"USD","source":"...","as_of":"YYYY-MM-DD","per_million":{...}}`, `catalog_estimate`, and pending invoice markers (`actual_billed_usd`). Unknown pricing stays null—never zero. On HTTP 429, all profiles in that `quota_group` observe vendor `Retry-After` or exponential backoff (60s up to 15m); lift explicitly via `agent_control(action=quota_resume, ...)` once recovered.

## Parent Sessions and Telegram

Optional `codex_async_python` (default `null`) may point to an existing `codex-async` virtualenv Python (e.g. `/path/to/codex-async/.venv/bin/python`) to reuse its `CodexAppServer` queue and shared daemon lock checks; standard standalone and multi-machine deployments require neither `codex-async` nor Telegram.

Register a coordinator record with `agent_control(action=parent_register, session_id=..., native_session_id=..., cwd=..., expected_revision=0, request_id=...)`, optionally passing `usage_path` (plus `usage_since`, `usage_profile`, `notify=false`, and later `parent_meter_stop`) to meter parent session usage accurately without copying raw transcripts. Child sessions link via `agent_open(parent=...)`. When a coordinator uses temporary MCP settings, pass an owner-only `0600` `resume_config_file` (restricted to `mcp_servers`, `plugins`, `apps`, `features.multi_agent`—see [Codex MCP docs](https://learn.chatgpt.com/docs/extend/mcp)) so cold `thread/resume` restores tools without altering model, reasoning effort, or approval policy.

Deliveries distinguish queue acceptance (`enqueued`), native turn admission (`received` via `userMessage.clientId`), and coordinator acknowledgment (`parent_ack` -> `processed`). `parent_ack` triggers exact item-only queue retraction (`queue_cleanup=removed|already_started|uncertain`) without disturbing unrelated messages or paused state. Optional `telegram_database` (default `null`) reads existing bot `telegram.sqlite` topic pause/deletion markers read-only and registers child sessions in `external_children` so workers never emit unintended Telegram bot replies.

## Monitoring and HTTP / SSE API

Terminal UI (`agent-bridge monitor`), web UI (`agent-bridge monitor --web`), and MCP `agent_monitor` unify `pi`, `claude`, `antigravity` (`agy`), and `codex` across both bridge-managed sessions and local native processes, prioritizing active, running, and approval-waiting sessions.

- CLI filters: `agent-bridge monitor --once --format json --runtime agy --source all --active-only --session-limit 50` (supports `--no-native`).
- MCP tool: `agent_monitor` supports `runtime` (including `agy`), `source` (`all`/`bridge`/`native`), `status` (`all`/`active`/`running`/`idle`/`waiting`/`waiting_input`/`stopped`), `session_limit`, `include_native`, and `after_seq`.
- Authenticated HTTP / SSE endpoints (`Authorization: Bearer <admin.token>` or `bridge_session` cookie with CSRF for mutations):
  - `GET /api/v1/health`
  - `GET /api/v1/runtimes`
  - `GET /api/v1/sessions`
  - `GET /api/v1/events`
  - `GET /api/v1/stream` (SSE with `Last-Event-ID`, `interval`, `once=1`, `max_events`, `max_seconds`)
  - `GET /api/v1/schema`
  - `GET /api/snapshot`

## Verification

```bash
uv run ruff check src tests scripts
uv run python -m unittest discover -s tests -v
```

Automated tests use real MCP stdio, Unix sockets, isolated processes, and mock CLIs without incurring model API charges. `scripts/*_smoke.py` and `scripts/benchmark.py` invoke real models when run manually (`scripts/cold_resume_smoke.py` tests cold parent recovery; `scripts/restart_smoke.py` tests live daemon restarts and cgroup cleanup). See [Verification Log](docs/verification.md).

Credentials reference existing CLI logins or mode-`0600` JSON environment files only; state dirs, Unix sockets, and tokens remain owner-only. Child sub-sessions exclude coordinator management tools by default, and read-only modes enforce filesystem/tool restrictions per backend without claiming OS-level protection against malicious same-UID code.
