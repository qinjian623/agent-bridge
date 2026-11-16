> Language: English | [中文](zh/bridge-reload-plan-20261109.md)

# Bridge Reload and Live Validation Plan (2026-11-09)

Status update (11-09 12:03): the local host rebooted outside this procedure at 11:43:49, automatically bringing Bridge up as `PID <process-02-pid>` at 11:44:11 with the notification fixes loaded. No manual restart was performed afterward. The pre-reboot prep checklist below stays preserved for audit history.

At 11:53 checks found zero active tasks, workers, in-flight sends, or pending API requests. A production notification was retracted accurately (`processed/removed/removed` at 11:53:38, confirmed absent via read-only scan of the queue and latest 100 turns at 11:57). Legacy compatibility backfilling for `processed + queue_cleanup=NULL` also verified a historical record cleanly onto its target turn (`received/already_started`).

As of 12:03, 71 historical notifications from 11-03..11-04 remained `unknown/uncertain` (not pending tasks and never resent). Secondary macOS node `macos-node` remained online; localhost `:8080` was initially unbound until web service restoration at 13:29.

Isolated live-stdio verification (`scripts/check_native_notification_retract.py`) halted safely before enqueueing messages because an empty `CODEX_HOME` unmaterialized thread returned `-32600` on `thread/turns/list` and `-32601` (`list_turns is not supported yet`) on `thread/read(includeTurns=true)`. All temp processes and state were cleaned up without touching shared daemons.

After adding 7 protection tests covering `check_native_notification_retract.py`, 652 tests passed in 21.659s alongside Ruff and `git diff --check`. Reports were stored privately under `<private-validation-state>/report`.

After 13:29, a user systemd unit `~/.config/systemd/user/cli-agent-bridge-web.service` was configured binding `127.0.0.1:8080` only with no embedded credentials (`401` unauthenticated, `200` on login + `/api/snapshot`), leaving Bridge, shared Codex daemon, and worker PIDs untouched (`<private-validation-state>/report.json`).

---

## Pre-Reboot Prep Procedure (Preserved Reference)

### 1. Affected State & Preconditions

- Only `cli-agent-bridge.service` reloads; shared Codex daemon, remote brokers, Web UI, running model choices, approvals, and existing workers (running in independent `agent-bridge-*.service` cgroups) must not be killed or restarted.
- Note that **existing live worker processes do not reload Python code when the Broker restarts**—stdin/response timeout fixes take effect only for newly spawned workers.
- Pending `processed + queue_cleanup=pending` items (`146` rows in the pre-reboot check) trigger exact queue retraction matching `(thread_id, clientId, receipt)` on startup, leaving unrelated items and active turns intact.

### 2. Pre-Reload Idle Safety Checklist

1. Pin current code digests matching tested builds.
2. Verify `queued`, `assigned`, `running`, and `waiting_input` task counts are zero for affected batches and check `recovery_needed` items explicitly.
3. Sample `deliveries`, `native_send_queue`, `native_broadcast_deliveries`, `peer_outbox`, native `processing`, and `api_requests.response IS NULL` repeatedly until all equal zero.
4. Record PIDs and start times for Broker, workers, shared Codex daemon, remote peer, and Web monitor; confirm zero stray helpers in Broker's cgroup.
5. Inspect candidate ACK cleanup scope read-only without persisting chat content.

### 3. Isolated Instance Pre-Validation Steps

1. Run isolated `CodexAppServer(daemon=False)` in an empty `CODEX_HOME` (never falling back to the shared daemon).
2. Create an empty test thread via raw `thread/start` and verify zero turns.
3. Run helper enqueue with two unique markers plus synthetic test completion event (without starting a broker or model turns).
4. Before ACK, inspect `receipt_status` + queue listing (`enqueued` across all three IDs).
5. Apply single ACK and run retraction once: verify `deleted=true`, target removed, peer markers preserved, state `processed/removed`.
6. Retry ACK with identical `request_id`: verify idempotency and zero native turns created.
7. Verify delete-race fallback (`deleted=false` -> subsequent `check_only=true` list-only without re-delete/re-send).
8. Clean up test markers, temporary state, and isolated subprocess cleanly.

### 4. Reload & Post-Reload Verification

- Reload strictly via a single `systemctl --user restart cli-agent-bridge.service` without blind double-restarts or DB resets.
- Confirm new Broker PID, active status, preserved worker/daemon PIDs, matching `agent_hosts`/`agent_monitor`/`/api/snapshot` views, exact queue retraction accounting, and keep open client request stalls clearly distinguished from Bridge fixes.
