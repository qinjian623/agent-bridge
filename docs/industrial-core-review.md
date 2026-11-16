> Historical pre-fix review: this report records the implementation before the candidate fixes. See [current candidate checks](production-readiness.md) for completed checks and pending work; the original findings and results below are preserved.

What this repo does: It queues work for independent coding-agent sessions and stores tasks, attempts, results, and message receipts in SQLite. Each host owns its workers; trusted SSH peers exchange requests, group messages, and parent notices. This review assumes one owner, a few trusted hosts, concurrent workers, and ordinary disconnects or process crashes. It does not assume hostile tenants or isolation from another process running as the same user.

Review date: 2026-11-17, Asia/Shanghai. This is a read-only review of the core execution, persistence, and messaging code. Only this report was added. Findings describe the reviewed implementation, before any separate fixes are accepted.

**Must fix**

1. **Mirror pruning deletes different event content** (`src/agent_bridge/mirror_prune.py:231-247`).
   - **What this is:** A maintenance command removes repeated copies of peer events. An origin is identified by its node, state generation, sequence number, and content fingerprint, which is a hash of the event.
   - **Problem:** The deletion groups omit `origin_fingerprint`, so two events with the same origin number but different payloads are treated as duplicates. An offline check inserted a direct event with `{"answer":"first"}` and a wrapped event with `{"answer":"conflicting"}`, both from `node-a/gen-a/17`; applying the default origin repair with self-origin pruning disabled deleted one of the two events.
   - **Fix:** Include the content fingerprint when grouping duplicates and keep conflicting copies. Add a test with the same origin number and different content, alongside the existing test that removes identical wrapped copies.
   - **If we skip it:** Maintenance removes the record needed to inspect a peer-content conflict. The normal event reader already treats different content as a conflict (`broker.py:941-975`); cleanup must preserve that distinction.

2. **Self-origin pruning can delete the only surviving event** (`src/agent_bridge/mirror_prune.py:203,241-244`).
   - **What this is:** A peer can return an event that originally came from this host. The command assumes the local original makes that returned copy unnecessary.
   - **Problem:** `is_self` compares only the node ID; deletion checks neither the state generation nor whether the original event exists with matching content. Offline checks covered an earlier generation and the current generation with no original: each started with one mirror and ended with zero events after ordinary `prune_duplicate_mirrors(dry_run=False)`.
   - **Fix:** Delete a self-origin copy only after proving a matching original still exists in the current generation; retain unknown, mismatched, or earlier-generation copies. Add tests for both absent-original cases, an altered original, and a matching original that may safely replace a mirror.
   - **If we skip it:** Running cleanup after restoring or partially retaining state can erase the only remaining copy. Matching a node ID alone is insufficient proof that another copy exists.

3. **Cancelling a remote native message reports success but still sends it** (`src/agent_bridge/peer_api.py:1933-1971`; caller: `broker.py:2184-2244`).
   - **What this is:** `agent_cancel(host_id=..., delivery_id=...)` calls the execution host's `peer.cancel`. A native target is an existing CLI session whose messages are held in `native_send_queue` before sending.
   - **Problem:** Native deliveries have no `task_id`, so `_cancel` skips actual cancellation, marks the operation cancelled, and returns success while its queue row stays `pending`. A subsequent ordinary delivery pass sends that row; the offline reproduction below observed exactly one mocked send after the successful cancel.
   - **Fix:** Find the native queue row through the saved inbox target and delivery ID. Cancel a still-pending row in the same transaction as its saved cancellation result; refuse a sending or uncertain row until its outcome is resolved, and test both cancellation targets (`delivery_id` and `operation_id`) plus a concurrent send claim.
   - **If we skip it:** A user's stop request appears successful while the message can still start work. A saved operation status does not stop the sender.

   Minimal offline input, using a fresh temporary store and the same node identity in both operations:

   ```python
   await service.handle({"v": 1, "id": "d", "method": "peer.deliver", "params": {
       "operation": operation("send-op"), "delivery_id": "delivery-1",
       "group_id": "group-1", "broadcast_id": "broadcast-1",
       "target": {"kind": "native", "runtime": "codex", "id": "native-1"},
       "target_seq": 1, "seq_kind": "message", "message": "do work",
   }})
   await service.handle({"v": 1, "id": "c", "method": "peer.cancel", "params": {
       "operation": operation("cancel-op"), "delivery_id": "delivery-1",
   }})
   await native_broadcast.deliver(store, config, sender=mock_sender)
   ```

   Actual result: cancel returns `{"target":"delivery-1","status":"cancelled","tombstone_seq":1}`; the queue row immediately after cancellation is `pending`; `mock_sender` is called once. Expected: the pending row becomes cancelled and the sender is never called. No real native session was used.

4. **The public cancel API cannot cancel a pending group delivery** (`src/agent_bridge/broker.py:2249-2267`; producer: `groups.py:698-732`).
   - **What this is:** A remote group broadcast returns a `delivery_id` before the network sender sends its message. `DeliveryScheduler.cancel` already knows how to replace a pending message with a cancellation marker at the same ordered position (`broker.py:346-385`).
   - **Problem:** The public handler searches `peer_operations`, while group broadcasts write these rows to `peer_outbox`; no production caller connects it to `DeliveryScheduler.cancel`. A real temporary `Broker.api_call` returned `operation_not_found` for a just-created group delivery, and a subsequent scheduler pass still submitted it to the mocked peer.
   - **Fix:** Check `peer_outbox` for `delivery_id` before the older operation lookup, and call the existing scheduler cancellation method for a pending row. Test through `Broker.api_call`, including replay of the same request and refusal to cancel a sending or uncertain row; direct scheduler tests do not cover the public handler.
   - **If we skip it:** A user cannot stop a group message that is still waiting locally, although the queue already contains the implementation needed to do so.

   Minimal offline input, using a temporary broker and a fake peer client:

   ```python
   member = {"host_id": "gpu-a", "node_id": "node-a", "state_generation": "gen-a",
             "target_kind": "bridge", "session_id": "remote-a"}
   groups.control_group(store, "team", "create", 0, "create", remote_members=[member])
   sent = groups.broadcast(store, "team", "queued message", 0, "broadcast")
   delivery_id = sent["recipients"][0]["delivery_id"]
   result = await broker.api_call("agent_cancel", {
       "delivery_id": delivery_id, "request_id": "cancel",
   })
   await broker.delivery_scheduler.step()
   ```

   Actual result: `result.error.code == "operation_not_found"`, followed by one mocked send of the original message. Expected: the saved row becomes `tombstone_pending`, and the scheduler sends only the cancellation marker. Supplying `host_id` before the remote node has received the message cannot cancel the coordinator's still-pending row.

5. **Remote failure notices lose the task name and outcome** (`src/agent_bridge/notifications.py:120,141-151`; producer: `peer_api.py:694-707`).
   - **What this is:** A remote event is saved with its source reference and original payload. The ordinary parent-notification sender then turns that saved event into a message for the coordinator.
   - **Problem:** The sender reads the local `events.task_id` and a top-level `status`; mirrored events instead carry the remote task in `data.source.local_id` and status in `data.payload.status`. A remote `task_finished` event for `remote-task` with `payload={"status":"failed","validation":"pending"}` produced the text `Child task None ended with status None`, without the remote task ID, host, or failed outcome.
   - **Fix:** Read the saved source reference and original payload when formatting a mirrored event, and include its host and task ID so the coordinator can request the remote result. Extend `test_event_mirror.py` through `notifications.deliver` to assert the full meaning of failed, completed, recovery-needed, and startup-failure notices, while retaining the existing deduplication checks.
   - **If we skip it:** Notices arrive but do not identify the task or tell the coordinator which handling is required. In particular, a remote uncertain result loses the instruction to inspect it before resolving it.

   Minimal event sent through `peer.notify_parent`, with a registered local parent and a valid local operation:

   ```json
   {"seq":18,"type":"task_finished",
    "ref":{"host_id":"remote","node_id":"remote-node",
           "state_generation":"remote-gen","local_id":"remote-task"},
    "payload":{"status":"failed","validation":"pending"},"created":100.0}
   ```

   Run `notifications.deliver` with a recording sender. Actual body includes `Child task None ended with status None`; expected body identifies `remote/remote-task` and `failed`, and gives usable remote result instructions. Existing `test_local_parent_route_mirrors_event_and_delivers_notification` asserts the event number reaches the sender but does not inspect the message body.

**Should fix**

6. **Codex tool starts never reach the saved event stream** (`src/agent_bridge/runtimes.py:173-185,288-293`).
   - **What this is:** Workers record tool events so the owner can inspect an active command and investigate an interrupted attempt. Pi and Claude already emit `tool_started` through this shared recording method.
   - **Problem:** Codex's filter excludes `item/started`, and its turn reader only converts completed commands and file changes to tool events. Passing an `item/started` event with a `commandExecution` item to the real filter returned `False`; a command interrupted before completion therefore leaves no start record in bridge history.
   - **Fix:** Accept `item/started` and record matching command/file tool starts for the active thread and turn, using the same artifact storage as completion. Add a test through the actual filter and reader, including an interrupted command and a wrong-turn start.
   - **If we skip it:** Codex command progress is less useful than the other runtimes, and bridge history cannot show which tool started before a disconnect. Native history may still contain it; this finding does not claim the native CLI loses its own history.

**Checks and limits**

The source review traced broker API calls to worker claiming, task start/finish, cancellation, questions, result storage, peer admission, ordered delivery queues, parent routing, and mirror deletion. SQLite writes use `BEGIN IMMEDIATE`; task claims, worker-generation checks, and persisted request IDs have useful protection against ordinary competing callers. Worker disappearance moves running work to `recovery_needed`, rather than replaying it; native and peer sends become uncertain after a broker restart. These paths have tests, and no separate duplicate-execution defect was confirmed in them during this review.

The following 374 existing offline tests passed in 13.811 seconds under Python 3.13.15. The reviewed checkout's source and tests were selected through `PYTHONPATH`, using an existing installed dependency environment; this was not a clean installation check.

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:tests python -m unittest \
  test_store test_worker test_runtimes test_parent test_notifications \
  test_peer_dispatch test_delivery_scheduler test_delivery_resolve \
  test_event_mirror test_store_phase_c test_native_send_queue test_groups \
  test_remote_groups test_mirror_prune test_group_prune test_task_timeout_worker \
  test_task_timeout_dispatch test_peer_transport test_node_identity -q
```

Additional temporary-store assertions reproduced all six findings. They called only mock senders/peers; no real model, user service, SSH node, production database, or broadcast target was used. No implementation or test file was changed. Resource warnings from existing test connections were observed; they did not fail the run and were not promoted to an unproven production leak finding.

Known limitations: `Store.question` emits `input_required` and sets `waiting_input` without creating a local parent delivery (`store.py:1709-1725`). A temporary parent/child check observed zero deliveries and zero sends after asking a question; the router prompt now describes this limit. Ordinary asynchronous interactive use still needs an input-request notice and suitable message text; a completion notice cannot unblock a question before its deadline. The reviewed cleanup covers repeated mirrors and group metadata; this review did not establish a bounded retention policy for all tasks, artifacts, questions, receipts, or ordinary events.

Verdict: Fix mirror data loss and both public cancellation paths before calling cleanup and stop behavior production-ready. Repair remote notice contents before relying on remote asynchronous coordination.

Not checked: real CLI versions, paid inference, live SSH disconnects, hardware or filesystem power-loss durability, macOS process behavior, long-running storage growth, release installation, UI, and the full repository test suite. Separate pending fixes, including changes in another checkout, have not been accepted or validated by this report. No speculative lean changes were added.
