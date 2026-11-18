> English | [中文](zh/getting-started.md)

# Installation and use

Start with the [short walkthrough](../README.md). This guide covers configuration, results, background services, and operating details. For removal, see [uninstall](uninstall.md).

## Requirements and installation

Use Linux or macOS with Python 3.11+ and `uv`. On Windows, run the broker, model CLIs, projects, and MCP client inside WSL2; native Windows is unsupported. Install and log in to at least one target CLI (`codex`, `claude`, `pi`, or `agy`) separately. Bridge does not install or authenticate those programs. Use a single user account and only trusted remote hosts; do not publish the broker or monitor as a multi-user service.

For a standalone installation, use a repository you can access:

```bash
uv tool install 'git+https://github.com/qinjian623/cli-agent-bridge.git'
agent-bridge --help
agent-bridge init
agent-bridge doctor
```

`uv tool update-shell` adds the uv tool directory to future shells. No release tag or PyPI availability is assumed here. To install a particular revision, append `@<commit-or-existing-tag>` to the Git URL, replacing the placeholder with a real revision.

For development or a checkout you want to keep:

```bash
git clone https://github.com/qinjian623/cli-agent-bridge.git
cd cli-agent-bridge
uv sync --locked
uv run agent-bridge init
uv run agent-bridge doctor
uv run agent-bridge serve
```

For the rest of this guide, source installations should use `uv run agent-bridge` in place of `agent-bridge`, or the checkout's absolute `.venv/bin/agent-bridge`. Dependencies are pinned in `uv.lock`; the runtime currently uses MCP Python SDK 1.30.0 and Claude Agent SDK 0.2.172.

## Configuration

`agent-bridge init` loads existing settings and lets you edit them without resetting untouched values. Choose a menu item, make changes, return with `b`, and save with `s`; `q` discards staged changes. It detects installed programs but asks you to supply exact model IDs. It does not run a model test or mark a profile verified.

| Menu | What to enter |
| --- | --- |
| 1) Profiles | A name such as `local`, runtime, provider, exact model ID, shared quota group, optional effort/executable/private environment file |
| 2) Runtime defaults | Worker launcher, concurrent task limits, strategy, task/interrupt timeouts, optional token budget |
| 3) Projects | Local project identifiers and existing absolute directories; useful for remote dispatch |
| 4) Remote hosts | Optional SSH target, remote executable/state directory, and SSH options; leave empty for local use |
| 5) Services & OTA | Optional broker service, managed launcher, and release update timer; nothing starts unless selected |

For the first read-only task, configure a Codex, Claude Code, or Pi profile named `local`. Use the model ID supported by that logged-in CLI. An unverified profile can be targeted explicitly; automatic selection only considers verified profiles. Antigravity only supports writable access; see its section below.

Non-interactive configuration is also supported. Replace `<exact-model-id>` with a real ID supported by your CLI:

```bash
agent-bridge init --non-interactive \
  --profile 'name=local,runtime=codex,provider=openai,model=<exact-model-id>'
```

To change container/macOS workers to detached processes:

```bash
agent-bridge init --non-interactive --worker-launcher detached
```

Defaults: 3 concurrent external tasks, at most 2 per `quota_group`, one active turn per session, a 3600-second task timeout, and 30-second interrupt timeout. Linux uses `systemd` workers when a working user manager is detected; macOS and non-systemd environments use `detached`. For unattended Linux operation, your user manager must remain available after logout; configure linger according to your host policy if needed.

With a Linux systemd broker, detached workers remain in the broker's service group. Directly restarting that service can terminate them. Use the updater's busy checks; it defers while these workers exist. Close unused Bridge sessions before updating.

Settings and SQLite history live in `~/.local/state/cli-agent-bridge` by default. To use another state directory, pass `--state` **before** the subcommand or set `AGENT_BRIDGE_STATE`. Use the same directory for the broker, CLI, and MCP:

```bash
agent-bridge --state "$HOME/.local/state/bridge-test" init
agent-bridge --state "$HOME/.local/state/bridge-test" serve
```

Use a dedicated directory owned by your account with mode `0700` and a short absolute path. An empty `AGENT_BRIDGE_STATE` uses the default. The broker opens `broker.sock` inside it, and Unix sockets have a platform-specific path length limit in bytes. If `doctor` or `serve` reports a long socket path, choose a shorter state directory and keep it consistent across processes.

Config and environment files are private. An `env_file` is a JSON object of string environment variables with mode `0600`; never put credentials into tasks, commits, screenshots, or shared logs. Changing a profile's runtime/provider/model clears old verification and price information. Removing a host or changing its SSH target clears stale identity pins in both config and `host_pins.json`.

`doctor` checks configuration, executable paths, service availability, and socket presence. It does not prove that login, model inference, or a remote task works. A successful `agent_inspect` call confirms communication with the running broker.

### Backups and disk usage

For a consistent full backup, stop writes from the broker and its workers first. Back up the entire state directory privately. The SQLite database records task history and references to files. Keep `artifacts/` with it so full answers and tool outputs remain readable. If restoring to another state directory, those JSON artifacts can be read from the restored directory. Existing workspace locations, native session paths, and configured executable or environment file paths may still need the original locations.

SQLite maintenance affects database files. Stored outputs in `artifacts/` and retained worktrees or copies take separate space and can grow even if the database shrinks. Check directory sizes separately. Keep work that has yet to be applied or reviewed. See [state retained after uninstall](uninstall.md#5-keep-or-remove-local-data-deliberately).

## Language

English is the CLI default. Use `--lang` (or `--language`) before or after a subcommand, or set `AGENT_BRIDGE_LANG`:

```bash
agent-bridge --lang zh-CN --help
agent-bridge monitor --lang zh-CN --once
AGENT_BRIDGE_LANG=zh-CN agent-bridge init
```

Choices are `en`, `zh-CN`, and `auto`. `auto` reads `LC_ALL`, then `LC_MESSAGES`, then `LANG`; unsupported locales use English. For a persistent default, merge `"language": "zh-CN"` into the top-level private `config.json`, preserving its other settings. A missing or `null` value keeps the English default. The order is CLI option, `AGENT_BRIDGE_LANG`, config, then English. The configuration wizard preserves this setting; selecting a display language does not rewrite config. Parent task notices use the broker's language; notices already queued keep their original body.

The dashboard and login page offer Auto, English, and 简体中文. The choice is stored in that browser; Auto follows browser language preferences and falls back to English. Switching language only changes the display and does not send, retry, or cancel tasks.

Help, setup prompts, monitor labels, fixed human-facing errors, and parent notices are translated, including CLI error descriptions printed to stderr. Tool names, IDs, JSON field names, error/status codes, raw tool payloads, task text, model replies, and provider diagnostics remain unchanged. Messages without a translation fall back to English.

<a id="mcp-setup"></a>
## MCP setup

Run the broker first (`agent-bridge serve`). For Codex, register the installed executable:

```bash
codex mcp add cli-agent-bridge -- "$(command -v agent-bridge)" mcp
codex mcp list
```

For a source installation, replace `$(command -v agent-bridge)` with the absolute `.venv/bin/agent-bridge` in your checkout. For another state directory:

```bash
codex mcp add cli-agent-bridge -- "$(command -v agent-bridge)" --state "$HOME/.local/state/bridge-test" mcp
```

Other stdio MCP clients use the same executable and arguments. For example, replace `/absolute/path/to/agent-bridge` with the result of `command -v agent-bridge`:

```json
{
  "mcpServers": {
    "cli-agent-bridge": {
      "command": "/absolute/path/to/agent-bridge",
      "args": ["mcp"]
    }
  }
}
```

Reload your MCP client or start a new session after changing its settings. An MCP registration only proves that the settings exist. The first successful tool call proves that the current client has loaded them. MCP starts a stdio adapter; it does not start the broker.

After enabling managed updates, use `$HOME/.local/share/cli-agent-bridge/bin/agent-bridge` as the absolute MCP executable (or your configured managed root's `bin/agent-bridge`) so future clients load the selected release.

<a id="results-and-review"></a>
## Results and review

The [README walkthrough](../README.md#send-a-first-task) sends a small read-only task. Preserve returned IDs instead of copying IDs from another installation. `request_id` deduplicates exact requests; changing parameters with the same ID causes a conflict. A repeated task with a new ID may incur a new charge.

Use `agent_result(task_id=...)` to read the answer, attempt history, file changes, and usage. `queued`, `assigned`, and `running` are unfinished. `waiting_input` requires a response before its expiry. `failed`, `cancelled`, and `interrupted` need inspection before any explicit retry. `recovery_needed` means the outcome must be verified before `agent_control(action="resolve", ...)`; do not blindly resend.

`completed` means the native CLI finished its reply. Inspect the answer and actual files, and run the appropriate checks before review. Supply the current attempt ID from `attempts` and `session_revision` from the result. In the example, replace every placeholder and replace `0` with that current revision; write your actual inspection evidence:

```bash
agent-bridge call agent_result --json '{"task_id":"<task_id>"}'
agent-bridge call agent_review --json '{"task_id":"<task_id>","attempt_id":"<attempt_id>","decision":"accepted","evidence":"<what you inspected and the actual check results>","expected_revision":0,"request_id":"review-task-1"}'
```

Dependent tasks do not run until the prerequisite's current completed attempt has an accepted review. A rejected attempt needs an explicit retry or new work. `agent_result` defaults to a short answer; when `answer_truncated` or an artifact is returned, pass its `artifact_id`, `offset`, and `next_offset` to retrieve all pages. Artifact limits are UTF-8 byte limits (up to 65,536), not character counts. Read `files.error` and `statistics_error` if present; an answer alone does not prove that file capture or usage collection succeeded.

Use `agent-bridge monitor` for interactive status or `agent-bridge monitor --once --format json --no-native` for one snapshot. Automatic parent notifications are off in a fresh installation. Keep the task ID and use `agent_result` to fetch its status and answer. To wait for an event, call `agent_wait(task_id=..., timeout=20)` once and check the result afterward. The wait is limited to 50 seconds and a timeout does not mean the task failed.

## Editing files and stopping work

For changes, use `access="workspace-write"`. In Git projects, Bridge creates a separate worktree from `base` (default `HEAD`). Unsaved changes in the source checkout are absent unless explicitly selected with `snapshot_files=["src/parser.py","tests/test_parser.py"]`. Tracked deletions, binary files, and selected untracked files are supported; directories, symlinks, and paths outside the project are rejected. Bridge does not commit, stash, or clean the source checkout. Returned patches start from that initial snapshot.

For a non-Git directory, use `workspace_mode="copy"` with an explicit `snapshot_files` list; only those files are copied. Results include content and permission changes, including executable-bit-only edits. Exact `request_id` retries preserve the original snapshot and in-progress edits. Worktrees and copies separate edits; they are not OS security sandboxes. All workers use your user identity. Review changes before applying them to your main checkout.

`agent_cancel` cancels one task. `agent_control(action="close")` cancels unstarted work and requests cancellation of an active turn while preserving history and workspace. Inspect the current session `revision` before control calls. A cancellation response is a request, so inspect the task/session again for its final state and worker exit. `pause`, `pause_drain`, `resume`, and `reopen` have the additional semantics described below. Stopping the broker or removing MCP does not stop independent workers or external jobs launched outside their managed process scope.

<a id="managed-services"></a>

Detached workers remain descendants of the broker. An updater can report `broker_helpers` and defer even when such a worker is idle; close unused Bridge sessions before retrying. This preserves work instead of restarting a broker that still owns live children. It does not require closing unrelated native terminals.

## Managed services

A foreground broker is enough for initial use. For user-level background services, finish configuration first, stop your foreground broker, then run:

```bash
agent-bridge init --non-interactive --install-broker-service --enable-broker-service
```

This writes and starts a Linux `systemd --user` unit or macOS `launchd` LaunchAgent using a stable managed launcher. It keeps the installation's Python as a bootstrap fallback until a managed release is selected; keep that uv tool environment or source `.venv` available. Existing conflicting service files are refused; do not add `--force-broker-service` without checking their contents and active work.

Background services may have a different `PATH` than your shell. The wizard offers the detected absolute path for the profile's `executable` field. If the executable is a script that needs an interpreter such as Node.js from a custom directory, include that directory in the `PATH` value of the profile's private `env_file`. The scheduled updater includes the directory of the `uv` found during setup. If you move `uv`, refresh the updater configuration from a shell that can find it. A successful `doctor` in your interactive shell only checks that shell's environment.

Default Linux commands:

```bash
systemctl --user status cli-agent-bridge.service
journalctl --user -u cli-agent-bridge.service -n 50
```

Default macOS command:

```bash
launchctl print "gui/$(id -u)/com.cli-agent-bridge.broker"
```

Use your configured service name if changed. Runtime profiles are loaded at broker startup; ordinary profile changes need a broker restart. Host registry changes are watched automatically, as described below. Close/inspect workers separately when stopping all work.

The older `uv run python scripts/install_local.py` helper is **Linux/systemd-only**. It links the source `.venv` command and companion skill into your home directory and writes a user unit; it does not start it and refuses conflicting files. Use `init` for new cross-platform setups. See [uninstall](uninstall.md) for removal of either installation style.

## Troubleshooting

| Symptom | Check or action |
| --- | --- |
| Command not found | Run `uv tool update-shell`, open a new shell, or use `uv run` in a source checkout |
| `init` requires manual configuration | Run in an interactive terminal, or pass an explicit `--profile` for a fresh non-interactive setup |
| Broker connection fails | Start `serve`; compare state directories and inspect the service's status/logs |
| Unix socket path is too long | Use a shorter absolute state path for the broker, CLI and MCP |
| Executable missing or authentication failure | Run `doctor`, then check the target CLI's own login and absolute executable path; service PATH may differ from your terminal |
| No profile selected | Explicitly name an unverified profile for its first task; catalog discovery does not verify inference |
| Task remains queued | Read `waiting_reason`: dependencies, paused session, concurrency, quota cooldown, budget, or worker startup may require action |
| Revision conflict | Inspect again and decide whether the original action still applies before making a fresh request |
| `recovery_needed` or uncertain delivery | Inspect saved/native records and files before resolve; do not resend blindly |
| No tools after MCP registration | Restart/reload the client and make one read-only tool call; check executable and state directory |
| Want to remove the installation | Follow [uninstall](uninstall.md); retain state/workspaces until their contents have been checked |

## Remote hosts

For multi-machine operation, see the [architecture decisions](multi-machine-design.md) and [current peer interface](multi-machine-interface.md). Bridge implements `agent-bridge relay`, SSH JSONL framing, node handshakes, host inspection, remote task dispatch, and cross-host session groups. Each remote host runs its own broker; connections are reused per host, node identity is checked before requests, and dropped connections do not blindly resend unconfirmed work.

Wire architecture, protocol envelopes, remote native inbox polling, dynamic relay refresh, and upgrade handling are specified in [Multi-Machine Interface Contract](multi-machine-interface.md). `peer.control`, `peer.review`, and `peer.inbox` are not yet implemented; do not assume declared protocol types imply live implementation.

The broker automatically watches `hosts` in `config.json` and `host_pins.json` so hosts can be added or removed at runtime without restarting the broker. You can also trigger immediate reconciliation with `agent-bridge call agent_hosts_reload --json '{}'`; `agent_hosts.config_error` reports validation errors or rejected changes. Unchanged hosts keep their existing SSH sessions, while changed hosts drain in-flight requests and ACKs before closing old connections. Invalid configs retain the last working registry without resending work.

Hosts with active group membership, pending deliveries, or uncertain outcomes refuse deletion or target modification; historical parent notification routes require read-only confirmation that sessions are closed and tasks have finished and been reviewed. Removing a host does not stop remote processes or purge history. See [dynamic host configuration](dynamic-hosts.md) for examples and restrictions.

All participating nodes must be trusted by the same user. SSH credentials and host keys remain outside this repository. A remote host needs its own installed Bridge, configured profiles/projects, and running broker; adding a local host entry does not deploy it. See the linked multi-machine guide before remote dispatch. `codex-async` and Telegram are optional integrations, not prerequisites.

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

Broadcasts execute the message as a task and can incur inference costs. `queued_count` means acceptance, not completion; use each recipient's `task_id` with `agent_result`. `reservation` applies per member. A new request ID creates a new broadcast. Generated tasks record `details.group_id`, `group_revision`, and `broadcast_id`; they do not train automatic quality selection, and their notifications keep each session's original parent. Cancel already queued tasks separately; membership removal does not retract them.

Broadcasts require an existing non-empty `group_id` and exact `expected_revision`; wildcard targeting is unsupported. Snapshots, tasks, and delivery records commit atomically in SQLite; subsequent group changes affect future broadcasts only. Busy recipients queue normally, while paused, draining, closed, or recovery-pending members report `skipped`. Exact `request_id` retries return original task IDs; failed recipients require explicit retry rather than automatic resends.

Existing native Codex or Antigravity sessions join via `native_members` (`runtime`, `native_id`, absolute `cwd`, optional `label`; removal requires only `runtime` and `native_id`):

```bash
agent-bridge call agent_group --json '{"group_id":"native-team","action":"create","native_members":[{"runtime":"codex","native_id":"<native-session-uuid>","cwd":"/absolute/project","label":"lead"}],"expected_revision":0,"request_id":"native-team-create-1"}'
agent-bridge call agent_broadcast --json '{"group_id":"native-team","message":"Team status update","expected_revision":0,"request_id":"native-team-notice-1"}'
agent-bridge call agent_broadcast_status --json '{"broadcast_id":"<broadcast_id>","refresh":true}'
```

Codex native messaging appends via `thread/queue/add` on an active shared daemon (`start_daemon=False`) without sending `thread/resume`, `turn/start`, or interrupts, preserving terminal locks and skipping closed sessions. Antigravity saves new group messages as `pending`, coordinates via shared locks (`antigravity-locks/<native_id>.lock` plus native presence/process checks), waits while active or ambiguous, and resumes offline instances under original native settings without permission-bypass flags or replaying historical `skipped` items.

Native members are deduplicated by runtime and native ID; conflicting metadata is rejected. Use the bridge `session_id` for a native session already managed by Bridge. Codex checks the native ID and directory and honors Telegram pause/deletion state; it never sends `turn/steer`. Antigravity checks shared presence locks, process arguments (`--conversation` and `-c/--continue`), and duplicate ownership before startup and before writing input. Unknown ownership waits; a missing native record is not recreated. The original model, auth, MCP, and permissions are retained. Native tool denial records failure rather than retrying with broader permissions.

`agent_broadcast_status` distinguishes `pending`, `enqueued`, `received` (matched native `clientId` / post-baseline Antigravity `user_input/DONE` with `native_turn_id`), and `processing_status=running/completed/failed`. `refresh=true` inspects Codex queues/recent 100 turns read-only without launching processes on Antigravity. Unconfirmed deliveries become `uncertain` or `unconfirmed` and must be checked rather than blindly resent.

A missing Codex queue entry without a matching native turn is `unconfirmed`. If a service exits during sending, receipt becomes `uncertain` and is not automatically resent. A saved receipt survives restart, while an unconfirmed execution outcome can have `processing_status=uncertain`. Confirmed pre-send failures can be retried. Antigravity recovery cleans up only the processes and MCP children started for that delivery.

Native sessions sit outside bridge worker metering; broadcasts containing native members are rejected when `token_budget` is enabled, and native deliveries cannot be canceled via `agent_cancel`. Native sessions can poll their own inbox read-only via `agent_group_inbox(group_id, runtime, native_id, after, limit)` (max 10 per page with monotonic `cursor`). Register Antigravity MCP via `agy mcp add cli-agent-bridge /absolute/path/to/agent-bridge mcp` (`/mcp` reloads open UI windows; see [Antigravity MCP docs](https://www.antigravity.google/docs/cli/mcp/)).

Removing a member or deleting its group does not retract saved broadcasts. Native deliveries have no bridge task ID. Group membership grants no new tool permissions; restricted worker sessions still cannot manage other sessions. Do not automatically acknowledge a broadcast by broadcasting again. Inbox reads require an explicit group and current native member, return only that member's records, and never launch, resume, or interrupt a process or mark it received; a coordinator's read is not proof of native receipt. Save the returned cursor as the next `after` to avoid reprocessing.

`agy mcp list` proves registration, a standalone MCP handshake proves reachability, and a tool call proves the current client loaded its tools. Check native receipt separately. MCP provides no message push into an active terminal; automatic Antigravity recovery waits until the original instance closes.

Control semantics: `pause` interrupts the active turn and stops dispatches; `pause_drain` waits for an active turn (or pauses immediately when idle); `resume` advances subsequent queue items unless `continue_task_id` explicitly re-runs the interrupted task; `close` cancels unstarted items and preserves history/worktrees until explicit `reopen`.

`task_timeout` defaults to 3600s total per attempt (streaming output does not extend the wall-clock deadline). Override per task with `requirements={"task_timeout": 21600}` (rejected cleanly by older nodes/workers lacking `task_timeout_v1`). Non-UTF-8 Linux filenames remain losslessly safe via escaped display fields alongside raw hex arrays (`untracked_files_raw_hex` / `changed_files_raw_hex`) and raw Git patches.

The next task waits for the previous native interrupt request to finish. Interrupt failures/timeouts stop that worker, preserving unstarted tasks for explicit recovery. User cancellation/close takes priority over timeout or budget termination. A task timeout must be finite and positive; it covers approval waiting as well as inference, is fixed when an attempt starts, and cannot change a running attempt. Local retries inherit the previous timeout unless overridden; remote `retry_task_id` is unsupported. Both node and worker must support `task_timeout_v1`; older workers refuse overrides instead of silently ignoring them. Known stop reasons appear in `termination`, ambiguous native errors in `native_error`.

Do not use escaped `untracked_files` or `changed_files` display strings as filesystem paths. Their parallel `*_raw_hex` fields preserve the original bytes; copy manifests use reversible JSON escapes, and Git patches retain raw bytes. Capture failure appears in `files.error` without hiding confirmed native output or usage.

Dispatching to an existing session inherits its access mode unless overridden (mismatches error cleanly). Large outputs save privately (`0600`) and paginate in UTF-8 chunks up to 65,536 bytes (`agent_result` with `artifact_id` and `next_offset`). `agent_inspect` includes host PIDs from systemd worker cgroups (`owned_processes`; external tmux/remote jobs report `external_jobs=unknown`). Unexpected disconnects enter `recovery_needed` and notify registered parents rather than resending blindly; resolve with `agent_control(action=resolve, ...)` after manual verification. Startup/connect failures keep unstarted tasks `queued` for explicit `resume`.

If writable-task patch extraction fails, the task becomes `failed`; the answer, usage, and working directory remain available. Inspect `result.files.error` and actual files. Pi's final statistics query can fail after a confirmed successful result without changing its terminal state; `result.statistics_error` reports that failure and usage becomes incomplete. Queued tasks include `waiting_reason`; input/approval questions expire with the task and never auto-approve. `attempt_id` selects a historical attempt; the default result includes the latest 20 attempts.

For systemd workers, `owned_processes` lists host PIDs in the worker cgroup, including detached child commands; sandbox-visible PIDs may differ. After close, verify `observed=stopped` and `owned_processes.status=stopped` as well as worker exit. External tmux or remote jobs can remain outside that cgroup and require their own checks. To resolve an unknown result after checking native history, files, and independent processes, use `agent_control(action=resolve, task_id=..., resolution=failed/cancelled/interrupted, evidence=...)`, then explicitly resume. Worker startup/connect failures notify registered parents but leave unstarted tasks queued rather than declaring their execution unknown.

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

For example, after verifying the requested ID is present in the discovered catalog:

```bash
agent-bridge call agent_catalog --json '{"profile":"codex-sol","refresh":true}'
agent-bridge call agent_dispatch --json '{"profile":"codex-sol","model":"gpt-6-luna","cwd":"/absolute/project","instruction":"Check the specified file and report the result","request_id":"check-discovered-model-1"}'
```

The selected model inherits service tier as well as effort; unsupported declared settings are rejected. Its context limit and verification timestamp are not copied from the old model. Sessions save the resolved configuration and cannot change models later. Discovery errors are reported per profile; previous catalogs remain marked `stale` and cannot authorize newly selected models. A configured model remains usable if a catalog fails or removes its alias. Discovery adds no new accounts/providers; unknown Claude-compatible providers and non-OpenAI Codex catalogs are unsupported, though existing explicit profiles can still run.

Discovery performs no inference or automatic model tests. Task input currently supports text only; a catalog listing image support does not enable binary/image delivery. Missing prices remain unknown.

References: [Codex App Server](https://learn.chatgpt.com/docs/app-server), [Pi RPC](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md), [Claude Python SDK](https://code.claude.com/docs/en/agent-sdk/python), [DeepSeek Models](https://api-docs.deepseek.com/api/list-models/), [Ollama Cloud](https://github.com/ollama/ollama/blob/main/docs/cloud.mdx).

### Claude Code with Ollama Cloud

Add a separate profile under `profiles` in `config.json` (merge the entry; keep existing profiles):

```json
{
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
}
```

`env_file` is an owner-only `0600` JSON string map setting `ANTHROPIC_BASE_URL=https://ollama.com` (and optional proxy variables; inherited and explicitly empty proxy settings are preserved, and Bridge does not choose a local proxy port). Auth uses `OLLAMA_API_KEY` or falls back to owner-only `0600` `~/.config/claude-ollama/api-key` without sharing DeepSeek keys.

Restart the broker after changing profile or provider code. Catalog success does not change `verified: false`; verify actual inference and the reported model first. Optional Flash off-peak preferences live in the [routing skill](../skills/cli-agent-bridge/resources/router-prompt.md); time rules do not alter broker scoring or migrate existing sessions.

## Balanced Selection and Costs

Explicit profiles are never silently replaced. Auto-selection (`default_strategy=balanced`) filters by input type, tools, context size, provider policy, cooldowns, verification status, and historical quality before weighting normalized cost and duration 0.5/0.5 (override per task with `requirements.strategy=cost|speed|balanced`; insufficient data returns `Unmeasured` in profile order). Samples include all task attempts plus metered single-task parent overhead (`requirements.learn_from_result=false` excludes fixed comparisons from selection updates). Cross-model retry keeps attempt quality isolated to each model while counting total attempt budget/cost.

When comparable monetary prices are unavailable, selection consistently uses tokens. A complete comparison sample requires one parent record for one task, stopped parent metering, and a reviewed result; concurrent tasks sharing a parent do not get guessed allocations. A retried/cancelled model's failure record remains, even when a later model succeeds. Tasks spanning models do not become single-model cost/time comparisons; all attempts still count toward total budget. Changing parent sessions retains the old parent's involvement instead of assigning shared overhead to the remaining task. Current tasks accept text; tools may read explicitly supplied local files, but native binary/image input is not implemented.

By default at most 3 external tasks run concurrently, capped at 2 per `quota_group`, and one active turn per session. When `token_budget` is configured, every dispatch reserves estimated tokens inside an SQLite transaction (`retry` updates future reservations only); external CLI metering has reporting delay, so budgets are best-effort rather than hard provider billing caps. Codex supports `service_tier` (`fast` maps to `priority` per [Codex Config Reference](https://learn.chatgpt.com/docs/config-file/config-reference)); Claude and Pi reject `service_tier`.

With a token budget, each task requires a positive reservation. SQLite allocates reservations transactionally; known failures count toward use, and unknown usage retains its reservation. A retry's explicit `reservation` changes only the next attempt; it never releases older unknown usage. Requests demanding a verified hard budget are rejected, because CLI reporting can lag by an in-flight request. `charged_tokens` can include retained reservations and is not actual observed usage.

Codex passes `service_tier` on startup, resume, and execution and requires native confirmation; requested and confirmed values are recorded separately, and historical samples with different tiers are not mixed.

Attempt metrics record start/end timestamps, `first_output`, native deltas (`model_usage` on Claude per [Claude Cost Tracking](https://code.claude.com/docs/en/agent-sdk/cost-tracking)), optional `price={"currency":"USD","source":"...","as_of":"YYYY-MM-DD","per_million":{...}}`, `catalog_estimate`, and pending invoice markers (`actual_billed_usd`). Unknown pricing stays null—never zero. On HTTP 429, all profiles in that `quota_group` observe vendor `Retry-After` or exponential backoff (60s up to 15m); lift explicitly via `agent_control(action=quota_resume, ...)` once recovered.

`first_output` is the first nonempty text delta for Codex and the first nonempty native/SDK text message for Pi/Claude; their precision differs and should not all be called first-token latency. Native tool results preserve commands, exit codes, and reported durations; only actual test output supports a test-duration claim.

Codex, Pi, and Claude report differences between native cumulative usage counters. Missing/reset Claude `model_usage` falls back to partial reporting without adding cumulative costs again. Empty counts, missing models, and invalid fields remain unknown; complete intervals need valid counters at both ends. A resumed Codex attempt without a verified pre-turn baseline reports incomplete usage rather than charging its full history. An older successful attempt cannot supply a missing crash baseline. CLI price estimates retain their source and unknown date; they are not account invoices. Cache tokens without a price do not cost zero.

A profile price can use `price={"currency":"USD","source":"pricing reference","as_of":"2026-10-22","per_million":{"input_tokens":2,"output_tokens":10,"cache_read_tokens":0.5}}`; these numbers only illustrate the format. `catalog_estimate` remains separate from `actual_billed_usd`, which awaits invoice confirmation. For quota cooldowns, inspect `agent_catalog.quota_cooldowns` and use `agent_control(action=quota_resume, session_id=<quota-group>, expected_revision=..., evidence=..., request_id=...)` only after the account recovers. Exhaustion without a retry time waits for explicit action; failed tasks always require an explicit retry.

## Parent Sessions and Telegram

Optional `codex_async_python` (default `null`) may point to an existing `codex-async` virtualenv Python (e.g. `/path/to/codex-async/.venv/bin/python`) to reuse its `CodexAppServer` queue and shared daemon lock checks; standard standalone and multi-machine deployments require neither `codex-async` nor Telegram.

Without this helper the broker does not deliver automatic parent notices, even if a parent is registered. Verify that a test notice reaches your own registered Codex session before relying on delivery. Until then, use [task results and bounded waits](#results-and-review).

Register a coordinator record with `agent_control(action=parent_register, session_id=..., native_session_id=..., cwd=..., expected_revision=0, request_id=...)`, optionally passing `usage_path` (plus `usage_since`, `usage_profile`, `notify=false`, and later `parent_meter_stop`) to meter parent session usage accurately without copying raw transcripts. Child sessions link via `agent_open(parent=...)`. When a coordinator uses temporary MCP settings, pass an owner-only `0600` `resume_config_file` (restricted to `mcp_servers`, `plugins`, `apps`, `features.multi_agent`—see [Codex MCP docs](https://learn.chatgpt.com/docs/extend/mcp)) so cold `thread/resume` restores tools without altering model, reasoning effort, or approval policy.

Deliveries distinguish queue acceptance (`enqueued`), native turn admission (`received` via `userMessage.clientId`), and coordinator acknowledgment (`parent_ack` -> `processed`). `parent_ack` triggers exact item-only queue retraction (`queue_cleanup=removed|already_started|uncertain`) without disturbing unrelated messages or paused state. Optional `telegram_database` (default `null`) reads existing bot `telegram.sqlite` topic pause/deletion markers read-only and registers child sessions in `external_children` so workers never emit unintended Telegram bot replies.

The parent must already have Bridge tools or CLI access. Parent usage verifies the native ID in the JSONL header and stores only counters and offsets, beginning at registration unless `usage_since` selects an earlier native usage event. Duplicate events/restarts do not double-count. File replacement or counter resets make usage incomplete. `usage_profile` can supply prices only for the same model; `notify=false` meters without waking the parent. `parent_meter_stop` fixes the endpoint. Totals include child attempts and registered metered parents, with omissions identified; incomplete totals cannot establish cost savings.

Notifications persist before enqueueing. While the original client owns the parent, delivery waits; after it exits, only that registered parent can be resumed with its original model, effort, and approval policy. The shared Codex daemon is not restarted. `parent_pause`/`parent_close` stop future notifications without waiting for a whole batch, though an already sending item can finish. Only explicit `parent_reopen` reopens it. The Codex 0.163.0 recovery interface drops temporary MCP startup settings, so such sessions need the owner-only `resume_config_file`; ordinary global settings do not. Read-only tools are annotated, while writes still follow the parent's approval settings.

`received` does not prove that the model read the message. `parent_ack(event_seq=...)` may acknowledge independent processing of `agent_result`; it sets `processed` without inventing native receipt. Legacy `accepted` is rechecked. Retraction checks the destination, client ID, and saved receipt and handles an ACK arriving during send. It operates only on the exact queued event, even if the parent is paused/closed; it never resumes or interrupts the session. Started turns cannot be retracted. Missing messages, conflicting receipts, or deletion failures are not proof of reading and do not trigger resends. Once deletion is uncertain, later checks only inspect queues/turns instead of deleting again; a confirmed pre-delete connection failure can retry.

Notification helpers failing before enqueue retain pending status and can retry. A lost queue reply becomes `uncertain`; late replies never overwrite a simultaneous ACK. Notifications include event time and do not authorize resuming paused development or rerunning work. A parent that actually received and processed an event can acknowledge `sending`, `uncertain`, `wake_pending`, or legacy `accepted`; an unsent `pending` item cannot be acknowledged. Retry order rotates by last attempt time so a busy parent cannot indefinitely delay others. Before recovery, the full native queue is paged until the event is found or confirmed absent.

Successful events require review; unknown outcomes require inspection and resolve; failed/cancelled/interrupted work requires checking existing edits before retry; startup failures need investigation and explicit resume. Configured Telegram integration reads existing topic state before send/recovery: `/stop` waits, `/continue` permits continuation, deletion closes the parent, and an unreadable database prevents sends. Before a child's first inference, a configured database must support registration in `external_children` and text/attachment filtering, otherwise the worker refuses inference. Null/omitted `telegram_database` disables this integration. Historical tests do not verify live Telegram delivery; check filtering before connecting an existing bot, because live sends affect its topics.

## Monitoring and HTTP / SSE API

Terminal UI (`agent-bridge monitor`), web UI (`agent-bridge monitor --web`), and MCP `agent_monitor` unify `pi`, `claude`, `antigravity` (`agy`), and `codex` across both bridge-managed sessions and local native processes, prioritizing active, running, and approval-waiting sessions.

After starting the broker, run `agent-bridge monitor --web` in another terminal. If you supplied a custom `--state`, use that same option before `monitor`. Open [http://127.0.0.1:8080/login](http://127.0.0.1:8080/login) and enter the token from `admin.token` in that state directory. The default location is `~/.local/state/cli-agent-bridge/admin.token`. Keep the listener on loopback and the token private. The macOS native process list does not report the process working directory.

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

See [checks and support limits](production-readiness.md) for CI coverage and checks that require a real account or host. Model discovery, fixture execution, and historical installation checks do not establish that your current account can run inference, execute tools, or interrupt and resume a native session.

```bash
uv run ruff check src tests scripts
uv run python -m unittest discover -s tests -v
```

Automated tests use real MCP stdio, Unix sockets, isolated processes, and mock CLIs without incurring model API charges. `scripts/*_smoke.py` and `scripts/benchmark.py` invoke real models when run manually (`scripts/cold_resume_smoke.py` tests cold parent recovery; `scripts/restart_smoke.py` tests live daemon restarts and cgroup cleanup). Select isolated targets before running these scripts; ordinary CI does not invoke real models.

Credentials reference existing CLI logins or mode-`0600` JSON environment files only; state dirs, Unix sockets, and tokens remain owner-only. Child sub-sessions exclude coordinator management tools by default, and read-only modes enforce filesystem/tool restrictions per backend without claiming OS-level protection against malicious same-UID code.

## Antigravity

Add a profile entry to private `config.json` alongside existing profiles:

```json
{
  "agy-sonnet": {
    "runtime": "antigravity",
    "provider": "google-antigravity",
    "model": "claude-sonnet-5-5-medium",
    "quota_group": "antigravity-account",
    "billing_mode": "subscription",
    "verified": false
  }
}
```

The default executable is `agy` (overridable via `executable`). Even when selecting a Claude model, `provider` must remain `google-antigravity` rather than `anthropic`. Use the exact full model ID returned by `agy models`; if setting `effort`, it must match the ID suffix tier (`low`/`medium`/`high`). Unverified profiles are excluded from automatic balanced selection unless targeted explicitly.

The broker loads profiles from `config.json` on startup. After editing profiles, stop and start the foreground broker, or restart your managed service after checking active work (see [managed services](#managed-services)). Existing sessions keep their original runtime and model.

```bash
agent-bridge call agent_catalog --json '{"profile":"agy-sonnet","refresh":true}'
agent-bridge call agent_open --json '{"profile":"agy-sonnet","cwd":"/absolute/project","access":"workspace-write","request_id":"agy-open-1"}'
# Use session_id with agent_dispatch/agent_control; use task_id with agent_cancel.
```

- Supports text prompts and `workspace-write` only. `read-only` requests are rejected before session creation; `--mode plan` is not treated as a read-only filesystem boundary. Writable sessions use isolated worktrees or directory copies.
- Native headless mode does not support bridge per-tool approval callbacks or Claude SDK control messages. Runs use native permission policies with `--sandbox` and never pass `--dangerously-skip-permissions`. Any denied native tool marks the bridge task `failed` even if the CLI exits with status `SUCCESS`. `tools` describes routing metadata rather than an Antigravity tool whitelist.
- Each task spawns an isolated `--input-format stream-json --output-format stream-json` process, sends one prompt, closes stdin, and resumes subsequent turns with the same conversation ID. Tasks complete only after the terminal `result` for that conversation, process exit, and stderr draining finish. Process exit or partial text output alone never proves completion.
- `SIGINT` requests cancellation. Interrupted state is recorded only after a native `INTERRUPTED`/`CANCELED` terminal event; unexpected exit enters `recovery_needed`. Confirmed interruption shuts down that process cleanly and resumes next turn via `--conversation`.
- The first turn in a new session records cumulative usage without double-counting thinking tokens. Whenever a fresh process after resume lacks a verified baseline or counters reset, historical totals are never treated as newly consumed—only observed turn increments are reported with `complete=false`. Unknown cache pricing yields null cost estimates rather than $0; CLI estimates are never actual invoices.
- Terminal `RESOURCE_EXHAUSTED` errors trigger per-quota cooldown backoff without silent model switching or automatic retries.

Account terms: check the current terms for your account before using this runtime. The previously referenced [official Google FAQ](https://www.antigravity.google/docs/faq#can-i-use-antigravity-cli-headless-mode-in-third-party-coding-agents-or-automate-account-switching) notes that consumer tiers do not authorize wrapping `agy` inside third-party coding agents or apps. Implementing runtime support does not grant license to violate account terms—use only under appropriate authorization. This project does not auto-provision Antigravity accounts. Protocol reference: [Headless mode](https://www.antigravity.google/docs/cli/headless).

## GitHub Releases and Scheduled OTA Updates

For pushes and pull requests, GitHub Actions runs offline tests and Ruff on Linux, then clean packaging/installation smoke checks on Linux and macOS. Pushing a matching `vX.Y.Z` tag builds and publishes the wheel, source tarball, `SHA256SUMS`, and `latest.json`; see [CI and Releases](ci-release.md).

Manage scheduled self-updates from the CLI:

```bash
agent-bridge update install
agent-bridge update check
agent-bridge update run
agent-bridge update --drain-workers run
```

Settings live in `~/.local/state/cli-agent-bridge/update.json` (mode `0600`) specifying the GitHub repository, managed releases directory, `systemd`/`launchd` service name, and check interval. OTA updates pull release assets **from public GitHub Releases over HTTPS by default** (honoring `HTTPS_PROXY` via `CONNECT` if configured) and verify `SHA256SUMS`, requiring no `gh` CLI login or API credentials. Installation does not auto-enable the timer or disturb an unmanaged broker service; configure and enable managed services via `agent-bridge init` (item 5: Services & OTA) or migrate explicitly before enabling periodic checks. Linux checks hourly with up to 5 minutes of jitter; macOS uses a `LaunchAgent` timer.

Existing private `release_url` settings remain in use: manifests come from `<base>/latest.json` and assets from `<base>/releases/<tag>/`. Re-running `init` or changing its interval preserves the source. Mirrors use HTTPS, or HTTP restricted to loopback/Tailscale with DNS and same-origin redirect checks. A mirror failure does not fall back to GitHub.

Updates download the SHA-256-verified wheel, install into a dedicated versioned virtual environment, and run configuration + isolated smoke broker health checks before switching. Hot OTA updates are enabled by default: live tasks and compatible workers keep running untouched while the broker drains short in-flight API requests, notifications, and non-SSH helpers before briefly entering maintenance; compatible workers and SSH relays finish their current turn cleanly and retire on idle so new sessions spawn on the updated launcher (full worker/task drain happens only when `REQUIRES_WORKER_DRAIN` is set or `--drain-workers` is passed); paused queues stay preserved. Before switching symlinks, the short maintenance lease blocks new dispatches/broadcasts and coordinator polls; the CLI transparently retries read-only requests and idempotent `request_id` writes across the brief restart gap. After restarting, the updater checks actual version and PID, rolling back the symlink automatically on confirmed startup failure (without rolling back SQLite or replaying tasks); ambiguous restarts enter `uncertain` and halt further auto-retries. See [OTA Hot Update Design](ota-updates.md).

`~/.local/share/cli-agent-bridge/bin/agent-bridge` is the stable managed launcher path. Broker services, CLI invocations, MCP registrations, remote host `relay` paths, and skills should point to that launcher (or `current/skill`) rather than stale source checkouts. Versioned directories preserve uncommitted edits in source repos, never restart live interactive terminals, and never upgrade external CLIs (`codex`, `claude`, `pi`, `agy`).

The latest updater outcome is recorded in `<state>/update-status.json` along with failure classifications. Never equate installing an update timer with a completed upgrade.

### Switching update sources

For an existing managed installation, replace `OWNER/REPO` with the new GitHub release repository:

```bash
agent-bridge update source --repository OWNER/REPO
agent-bridge update check
```

Use the same `--state` before `update` on both commands when using a custom state directory. The source command locks and edits `update.json` while keeping the managed installation directory, service and timer settings. Add `--clear-mirror` only when deliberately leaving a configured `release_url` mirror. Changing the source does not run an update or restart the service. `update check` queries the source and reports whether it offers a version newer than the installed release.

If the installed launcher does not recognize `update source`, first download and verify a newer release wheel against its published `SHA256SUMS`. Run the command from that wheel, replacing both absolute paths and the repository:

```bash
uv tool run --from /absolute/path/to/cli_agent_bridge-0.3.1-py3-none-any.whl \
  agent-bridge --state /absolute/path/to/state update source --repository OWNER/REPO
```

When preparing a new release repository, update the README and guide installation URLs, `pyproject.toml` project URLs, the companion skill's documentation links, and the default `REPOSITORY` in `src/agent_bridge/updater.py`. Existing installations retain their configured source until explicitly switched.
