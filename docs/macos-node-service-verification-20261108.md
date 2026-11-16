> Language: English | [中文](zh/macos-node-service-verification-20261108.md)

# Secondary Node (`macos-node`) Service Verification (2026-11-08)

The remote macOS host (anonymized SSH alias `macos-node`) holds checkout under `~/gits/cli-agent-bridge`. The Linux coordinator executes `agent-bridge relay` over SSH; the remote node never requires reverse SSH access back to the coordinator.

## Services and Configuration

- Remote macOS node (`macos-node`): installed and loaded user `LaunchAgent` `gui/<uid>/local.cli-agent-bridge` (`~/Library/LaunchAgents/local.cli-agent-bridge.plist`) with `RunAtLoad`, `KeepAlive`, and a 10-second throttle interval.
- Both hosts maintain independent `~/.local/state/cli-agent-bridge` directories without copying credentials or sharing SQLite files.
- Coordinator `hosts.macos-node` points to the remote binary and state directory. SSH enforces non-interactive authentication, known-host key checks, connect timeouts, keepalives, disabled agent forwarding, and pinned node identity.
- Only the coordinator broker was restarted to reload host configuration; existing independent Pi systemd workers remained running.

The remote database already held 18 historical `open/stopped` sessions (with no native IDs or workers) and 7 queued tasks. Before bringing up the remote broker, those sessions were switched via `pause_drain` so history and queued items stayed preserved without auto-executing stale tasks; resume explicitly only when selected.

## Fixes Applied During Bring-Up

Continuous runtime polling exposed an issue where background host pollers opened fresh SSH processes without closing them (~42 accumulated connections over ~70s, exhausting `macos-node` SSH limits). Updated `PeerClient` to reuse one multiplexed SSH subprocess per host cleanly until broker exit or host reconfiguration.

Each remote RPC verifies peer handshake identity on the shared SSH connection; connection replacement between handshake and request rejects the call, while identity mismatches lock out sends until manual administrator re-pinning. Unexpected disconnects never blindly resend uncertain requests.

Monitor snapshots previously rebuilt a fresh host registry and showed connected nodes as `offline`. Internal broker monitoring now queries the active runtime registry directly; CLI and web monitors fetch live broker snapshots and retain read-only local data on failure.

## Observed Verification

- `agent_inspect` on `macos-node` succeeded; `doctor` reported `broker_socket=true`.
- SSH `peer.hello` and `peer.inspect(kind=node)` matched the pinned node identity (`protocol_version=1`, `state=ready`).
- Coordinator `agent_hosts` reported `macos-node=online` with updating contact timestamps, `agent_catalog(host_id=macos-node)` returned 3 remote profiles, and MCP/TUI monitors displayed `online`.
- Across 4 samples over 30.5 seconds, only a single SSH child process remained active without connection leaks.
- Sending `SIGTERM` to the remote broker caused `launchd` to restart it automatically while preserving the 18 paused sessions and 7 queued tasks with zero unexpected workers.
- Automated suite at check time: 610 tests passed, Ruff and `git diff --check` clean (covering connection reuse, host isolation, cleanup, concurrency, identity mismatch, disconnect/reconnect, no blind retries, and monitoring reads).

Remote model inference, training jobs, and live broadcasts were not executed in this verification pass (remote profiles stayed `verified=false`), and `launchd` was verified only within an active logged-in session rather than pre-login/post-reboot.

Detailed artifacts remain in local private state (`<private-validation-state>/report` and remote `deployments/macos-node-service-20261108/`); no databases, tokens, or raw chats were committed.
