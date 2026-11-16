> Language: English | [中文](zh/macos-node-agy-check-20261109.md)

# Remote macOS Node (`macos-node`) Antigravity & Bridge Check (2026-11-09)

Inspection date: 2026-11-09 (initial connect 14:02 UTC, native turn 14:10 UTC). The secondary macOS node (anonymized SSH alias `macos-node`) resolved to Darwin; plain SSH commands and remote Bridge RPCs succeeded.

| Target | Observed Result |
| --- | --- |
| Bridge service | `gui/<uid>/local.cli-agent-bridge` active, without restart during test |
| Bridge installation | Repository `.venv/bin/agent-bridge`, clean working tree |
| Remote catalog | Coordinator `agent_catalog(host_id=macos-node, profile=agy)` succeeded |
| AGY profile | AGY `1.5.3`, `runtime=antigravity`, `provider=google-antigravity`, `claude-sonnet-5-5-medium`, `verified=false` |
| AGY MCP | Registered `cli-agent-bridge` via `agy mcp add` while keeping existing settings |
| Standalone MCP client | Initialize, listing 19 tools, and calling `agent_hosts` all succeeded |
| Fresh native AGY turn | Invoked Bridge `agent_hosts` successfully (details below) |

## Native Call Verification Method

Launched one isolated `agy --print` invocation inside an empty temp directory with default permissions (no `--continue`, `--conversation`, or permission-bypass flags), requesting only a single call to `cli-agent-bridge`'s `agent_hosts`.

- Created new session ID `<test-session-id>` without resuming pre-existing locked sessions (`<locked-id-1>`, `<locked-id-2>`).
- Native init reported `claude-sonnet-5-5-medium`; final status `SUCCESS`, 1 turn, exit code 0.
- Native stream showed `call_mcp_tool` transitioning `ACTIVE -> DONE` with no error; read-only inspection of the test session record confirmed `ServerName=cli-agent-bridge`, `ToolName=agent_hosts`, valid `hosts` response, and empty `error_details`. No raw prompts or transcripts stored.
- Test process and lock exited cleanly; pre-existing interactive terminal processes remained undisturbed.

Note on macOS `ps`: `ps -axo pid=,ppid=,comm=,args=` truncated executable names in `comm`, whereas `ps -axo pid=,comm=` exposed full process paths and detected live instances—use non-truncated checks rather than assuming truncated `comm` strings indicate absence.

This confirms fresh native Antigravity sessions on macOS can invoke the local Bridge MCP server. Pre-existing open instances did not reload MCP config, and neither live group broadcasts nor bridge-dispatched AGY inference turns were tested here.

Metadata report persisted on remote private state: `<private-validation-state>/report.json` (mode `0600`).

## Pre-OTA Migration Notes (Historical)

- The original `LaunchAgent` used `RunAtLoad`, `KeepAlive`, and a direct checkout `.venv/bin/agent-bridge --state ~/.local/state/cli-agent-bridge serve` command. Both the `LaunchAgent` and coordinator `hosts.macos-node` relay command must switch to the managed launcher (`~/.local/share/cli-agent-bridge/bin/agent-bridge`) before enabling auto-updates.
- AGY MCP registration uses `agy mcp add cli-agent-bridge ~/.local/share/cli-agent-bridge/bin/agent-bridge --state ~/.local/state/cli-agent-bridge mcp`.
- Public GitHub Releases OTA downloads verified wheels and `SHA256SUMS` over unauthenticated HTTPS without requiring local GitHub CLI credentials.
- Pre-migration DB check (14:18 UTC) preserved 18 historical `paused/stopped` sessions with 7 queued tasks and 1 `open/recovery_needed` legacy AGY session (`<legacy-session-id>`, dead zombie PID) without cancelling or auto-running queued items.
