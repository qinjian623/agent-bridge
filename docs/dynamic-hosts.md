> Language: English | [中文](zh/dynamic-hosts.md)

# Dynamic host configuration

## Live Runtime Configuration Reloads

The broker polls `config.json.hosts` and `host_pins.json` in the state directory every 0.5 seconds. Adding, editing, or removing entries under `hosts` takes effect without restarting the broker, Web monitor, or running workers. Only host registry entries are reloaded; runtimes, models, permissions, and quotas are unchanged. Write updates atomically by renaming a temporary file so the broker never reads a partial file.

To force an immediate reload via MCP/CLI:

```bash
agent-bridge call agent_hosts_reload --json '{}'
agent-bridge call agent_hosts --json '{}'
```

`agent_hosts_reload` returns `{added, updated, removed}` (empty lists when unchanged). It takes no parameters, requires local same-UID coordinator admin token authentication, and is excluded from restricted member and `peer.*` paths. Existing MCP clients do not need re-discovery unless listing newly registered tools.

Example `hosts` entry using a synthetic SSH alias:

```json
{
  "linux-node-b": {
    "ssh_target": "linux-node-b",
    "remote_executable": "~/.local/share/cli-agent-bridge/bin/agent-bridge",
    "remote_state": "~/.local/state/cli-agent-bridge",
    "projects": {"bridge": "~/gits/cli-agent-bridge"},
    "pinned": {"node_id": "<verified-node-id>", "state_generation": "<verified-generation>"}
  }
}
```

Inspect `peer.hello` and `peer.inspect(kind=node)` over an explicit SSH target before pinning rather than trusting first-contact responses automatically. Matching entries in `host_pins.json` override inline `config.json` pins. Keep host entries isolated and never copy credentials or SQLite databases across nodes.

## Continuity Guarantees on Registry Edits

- Unchanged hosts retain their existing SSH connections and online status. Modified hosts stop accepting new requests, drain active API operations, poller loops, ACKs, and cursor writes, and retire old SSH clients so callers cannot reopen them.
- Malformed JSON, missing files, or invalid host schemas are never treated as an empty registry: the entire candidate batch is ignored, `agent_hosts.config_error` records the reason, and valid fixes reload automatically.
- You cannot remove or retarget a host referenced by active group members, unfinished `peer_outbox` deliveries, or unresolved operations—even if a separate submit operation completed while downstream tasks remain active.
- The broker checks historical parent notification routes with read-only requests to the remote host. A session must have `desired=closed` and `observed=stopped`. A task must be `completed` with an `accepted` or `rejected` review, or have status `failed`, `cancelled`, or `interrupted`. The broker blocks edits if the host is offline, a record is missing, work is still active or unreviewed, or the node identity differs. Existing routes and cursors are retained.
- Registry removal never cancels remote tasks, closes remote native sessions, flushes queues, mutates `uncertain` records, or migrates work. Stale poll loops exit, and temporary identity or cursor caches clear upon re-registration.
- Changing only SSH connection fields for the same identity does not bypass an `EventMirror` fingerprint-conflict pause; explicit re-pinning or removal clears transient pause flags while keeping historical fingerprints and durable cursors intact.

Known limitation: historical parent routes from a retired `state_generation` have no explicit retirement proof, so reusing the same `host_id` for a different `node_id` may block later removal. Assign a fresh `host_id` to replacement nodes rather than bypassing safety checks.

## Remote event history

Background polling copies task and session events and skips individual tool starts and completions. To mirror future tool events from a specific node, set `mirror_tool_events: true` under its `hosts` entry. This setting can change on a registered host with active references. Polling resumes at the saved cursor. For earlier details, use `agent_wait` on the source node with the task ID and an independent cursor.

Preview expiration of old mirrored tool payloads, then apply it explicitly on the machine storing the copies:

```bash
agent-bridge mirrors expire --days 30 --limit 500
agent-bridge mirrors expire --days 30 --limit 500 --apply
```

Each call checks one page. When `has_more` is true, pass `next_after` as `--after` to continue that pass. Expiration requires the saved peer cursor to cover the event. It keeps the local sequence, source identity, timestamps and fingerprints, and preserves data referenced by notifications, deliveries or native sends. A valid late notification can restore its exact payload. Use the upgraded release to handle notifications after applying expiration. Source events are unaffected. SQLite can reuse freed space without an automatic VACUUM.
