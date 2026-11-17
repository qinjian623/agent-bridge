> Language: English | [中文](zh/bridge-reload-plan-20261109.md)

# Bridge Reload and Live Validation Plan

This is a historical plan and record from 2026-11-09. The local reboot described below already loaded the broker changes; the preserved preparation steps are not instructions to restart it again. Private project names, process/notification/session identifiers, and report locations are replaced with descriptive placeholders. Report references below are illustrative, not published copies of private state.

Status update at 12:03: the machine rebooted outside this procedure at 11:43:49. Bridge started automatically at 11:44:11 as `<broker-after-reboot-pid>` with the notification fixes loaded. No restart was issued in this procedure. The pre-reboot preparation remains below for reference.

At 11:53 there were no active tasks, workers, in-flight sends, or unanswered API requests. One production notice was precisely retracted: its existing receipt had been confirmed in the native queue at 10:15; cleanup at 11:53:38 recorded `processed/removed/removed`; read-only inspection at 11:57 found it absent from both queue and latest 100 turns. Code records that state only after native `deleted=true`. The original deletion reply and a separate completion timestamp were not saved.

An old-version compatibility case was also checked. An old Broker can ACK after a new worker adds columns, leaving `processed + queue_cleanup=NULL`. Initialization now schedules such records as pending while retaining native receipt state. After the fix, normal task startup triggered historical backfill, and at 12:02:19 the persistent process matched the notice to a real native turn, recording `received/already_started`. A new task appeared before planned manual initialization, so inspection stopped without another production-DB write. At that point 645 tests, Ruff, and diff checks passed.

As of 12:03, 71 old notices created on 11-03..11-04 remained `unknown/uncertain`. They were not 71 pending tasks and were never resent. The secondary macOS node, called `example-macos-node` here, was online. Local port 8080 was not listening, so Web restoration could not yet be claimed.

The isolated Native test did not pass and stopped before any queue write. With an empty `CODEX_HOME`, the independent stdio instance could create a thread, but `thread/turns/list` returned -32600 because the thread was not materialized before its first user message. `thread/read(includeTurns=true)` returned -32601, `list_turns is not supported yet`. The test did not start a model turn to bypass this limitation. All test processes exited and temporary directories/databases were removed. It copied no authentication, used no shared daemon, and added/deleted no queue messages. The repeatable script is `scripts/check_native_notification_retract.py`; reports stayed private, illustrated as `<private-validation-state>/native-check.json`. The observed production retraction and failed isolated check are separate results, not a claim that every live test passed.

After seven method/target/report-protection tests were added for the script, the final complete suite passed 652 tests in 21.659 seconds; Ruff and diff checks passed. Illustrative private references are `<private-validation-state>/report.json` and `unittest-final.log`. The broker retained `<broker-after-reboot-pid>` with `NRestarts=0`. No commit or push was made in that procedure.

After 13:29, a separately authorized Web restoration added the standard user unit `~/.config/systemd/user/cli-agent-bridge-web.service`, enabled startup through `default.target` and restart on failure, and bound only `127.0.0.1:8080`. No credential was stored in the unit. Unit validation passed; the login page and authenticated snapshot returned 200, while unauthenticated snapshot returned 401. Three successive snapshots showed `example-macos-node` online. The broker, shared Codex daemon, and three existing workers retained their PIDs/start identities. User linger was enabled; the machine was not rebooted again for validation. Illustrative metadata reference: `<private-web-check-state>/result.json`.

The preserved reload procedure below affects only `cli-agent-bridge.service`. It does not restart the shared Codex daemon, remote broker, or Web monitor, change models/permissions/native session IDs, or rerun compilation tasks.

## Pre-reboot snapshot and affected work

- The local broker, `<broker-before-reboot-pid>`, had started at 15:41 on 11-08 and had not loaded the previous evening's persistent-notification fixes.
- One Flash task was running in `<active-worker-pid>`. Three old Pi workers were idle. All four used separate `agent-bridge-...service` units outside the broker cgroup.
- The read-only snapshot showed zero sending notices/native-queue/peer-outbox entries, zero native processing entries running, and zero unanswered API calls. These figures must be checked again at execution time.
- There were 146 `processed + queue_cleanup=pending` rows, all within one authorized parent scope whose private name is omitted. This is a count of records awaiting inspection, not native queued messages.

Reload activates the new notice, ACK, native receipt, and retraction behavior. On startup the service automatically checks those acknowledged records. It retracts only the same item still in the same native thread queue with matching `clientId` and existing receipt. Items already admitted to a turn are neither deleted nor interrupted. Unknown outcomes lead to read-only checks without another automatic delete or send. Unacknowledged notices are outside this cleanup batch.

Reload therefore can change native queues. If authorization for that batch still requires queues to remain unchanged, do not start this persistent loop. Stop at preparation; do not edit the database or disable Telegram checks to bypass the restriction.

## Effect on existing workers

The broker launches workers with independent `systemd-run --unit=agent-bridge-...` units. Its `KillMode=control-group` affects only its own cgroup. Before reload, confirm its only children are its SSH relay clients, without send helpers, AGY restore processes, or other work.

Normally worker PID, start identity, native ID, model, and working directory remain unchanged. The broker socket is briefly unavailable during restart; the parent should not dispatch new tasks or submit state changes before receiving the completion report.

**Existing workers do not reload Python modules when the broker restarts.** This procedure updates persistent notification handling. Worker write/response timeouts and timeout-before-receipt fixes apply only to workers that already loaded the changed files or workers started later. Update old workers separately after they are idle; do not call this broker reload an update of every worker. This procedure does not close, reopen, or migrate their sessions.

## Checks after idle is confirmed and before execution

1. Record code-file digests and confirm they match the previously tested fix, which had 644 tests at the preparation stage. If related code changes again, inspect the change and run necessary tests instead of reusing that result.
2. Recheck the authorized task batch: queued, assigned, running, and waiting_input must all be zero. Refer `recovery_needed` or unknown external execution to its owner; do not count it as normal idle.
3. Check `deliveries`, `native_send_queue`, `native_broadcast_deliveries`, and `peer_outbox` for zero sending; native processing for zero running; and `api_requests.response IS NULL` for zero. Repeat short-interval sampling and stop if new tasks, sends, or unanswered calls appear.
4. Record broker, workers, shared Codex daemon, remote broker, and Web monitor PIDs/start identities/listening addresses. Confirm worker/broker cgroup separation; stop on an unknown child process.
5. Read only the ACK-record scope to be checked and matching native-queue metadata, without saving message bodies or full databases. Stop and report if scope exceeds the authorized parent.

## Validate first in an independent Native test instance

A temporary Bridge `--state` alone does not isolate native sessions. Production `codex_parent.main()` fixes `daemon=True, start_daemon=False`, which still connects to the default shared daemon; the second flag prevents starting it, not queue handling.

Use an empty independent `CODEX_HOME`, empty working directory, and `CodexAppServer(daemon=False)` stdio child. Do not copy authentication, load a compilation repository, or change global environment settings. If unauthenticated pure queue operations are unavailable in that version, stop and report; do not fall back to the shared daemon.

1. Use raw `thread/start` only to create a test thread, not SDK `.thread()`, which adds permission settings and developer instructions. Set no Goal. Confirm zero turns using `thread/turns/list`.
2. Temporary Bridge state uses only Store and single function calls, without broker, scheduler, or worker. Create a clearly labeled synthetic completion event, then use the real helper core to enqueue its test notice and two unique synthetic markers into that thread.
3. Before ACK, call only `receipt_status` and native queue/list. The notice must be enqueued and all three IDs/clientIds present. Do not call ordinary `advance()` again for an existing receipt: it may restore a notLoaded session.
4. ACK in the temporary Store and invoke the actual retraction core once. Require native `deleted=true`, target receipt absent, both marker IDs/clientIds retained, and Bridge `processed/removed`.
5. Repeat ACK with the same `request_id`; confirm no new enqueue/delete and a still-empty native turn list.
6. For the failure case, after the helper locates the target, the test may precisely delete that same test item first so the helper receives real `deleted=false`. Subsequent `check_only=true` may only query list/turns, never delete/add. Label this an induced deletion race with a real native reply, not a real disconnect or automatic native-consumption test.
7. Clean up only owned markers and the independent child. Retain test IDs, RPC methods, receipts, and state metadata privately; remove the temporary test database. Keep no credentials or raw chats.

Throughout, prohibit `turn/start`, `thread/queue/start`, `thread/resume`, steer, and interrupt. Confirm no native turns at each stage. Stop on automatic execution, target-ID conflicts, or an unexpected shared connection.

This checks **the same helper core against the real Native protocol**, not the complete production shared-daemon subprocess entry point. A synthetic completion is not a real model task and does not establish that a model read it, compilation finished, or the primary Codex long-request problem was fixed.

## Reload only the broker

Only after the isolated check passes and pre-execution conditions still hold, run once:

```bash
systemctl --user restart cli-agent-bridge.service
```

Do not change configuration or restart other units. If the result is unclear or stop times out, inspect that one systemd job and actual processes before doing anything else. Do not submit another restart or force-kill. Record the unknown outcome without replaying tasks, rolling back the database, clearing queues, or overwriting others' uncommitted work.

## Production checks after reload

- Broker PID changes and service is active. Shared Codex daemon, remote broker, Web monitor, and original workers retain PIDs/start identities. Original task/attempt/review records show no duplicate task/attempt and no uncertain task relabeled completed.
- `agent_hosts`, `agent_monitor`, and HTTP `/api/snapshot` agree. Under normal networking the remote node handshakes again over the same multiplexed SSH connection. Web process and authentication token need no reset.
- Distinguish notice enqueue, matching userMessage/turn receipt, and parent ACK. Check cleanup only within the authorized ACK scope. Missing items or history beyond the query window remain unknown; do not claim every record is cleaned.
- Read-only inspection of the production helper may connect to the existing daemon but sends no test message to a compilation session. Claim observed production retraction only for the same acknowledged queue item actually seen removed. Without a checkable item, mark it unverified.
- Stop further checks and report any failure, scope mismatch, or uncertain outcome. Do not change models, restore/rerun compilation, or interrupt the shared daemon for testing.

Report code tests, broker-loaded version, isolated Native result, production read-only checks, observed retraction, and unverified items separately. **The cause of the primary Codex long model request remained unknown; neither this reload nor the preparation-stage 644 tests established that it was fixed.**
