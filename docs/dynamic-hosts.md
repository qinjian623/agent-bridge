> Language: English | [中文](zh/dynamic-hosts.md)

# Dynamic host configuration

## Live Runtime Configuration Reloads

The broker polls `config.json.hosts` and `host_pins.json` in the state directory every 0.5 seconds. Adding, editing, or removing entries under `hosts` takes effect immediately without restarting the broker, Web monitor, or running workers (applying host registry changes only without modifying runtimes, models, permissions, or quotas). Write changes atomically via temporary file rename to prevent partial reads.

To force an immediate reload via MCP/CLI:

```bash
agent-bridge call agent_hosts_reload --json '{}'
agent-bridge call agent_hosts --json '{}'
```

`agent_hosts_reload` returns `{added, updated, removed}` (empty lists when unchanged). It takes no parameters, requires local same-UID coordinator admin token auth, and is excluded from restricted member and `peer.*` paths. Existing MCP clients do not need re-discovery unless listing newly registered tools.

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

Inspect `peer.hello` and `peer.inspect(kind=node)` over an explicit SSH target before pinning rather than auto-trusting first-contact responses; matching entries in `host_pins.json` override inline `config.json` pins. Keep existing host entries isolated without copying credentials or SQLite DBs across nodes.

## Continuity Guarantees on Registry Edits

- Unchanged hosts retain existing SSH connections and online status. Modified hosts stop admitting new requests, drain active API operations, poller loops, ACKs, and cursor writes, and retire old SSH clients cleanly without re-opening retired clients in callers.
- Malformed JSON, missing files, or invalid host schemas are never treated as empty registries: the entire candidate batch is ignored, `agent_hosts.config_error` records the reason, and valid fixes reload automatically.
- Nodes referenced by active group members, non-terminal `peer_outbox` deliveries, or unresolved operations reject removal or target changes (even if a separate submit operation completed while downstream tasks remain active).
- Historical parent notification routes undergo read-only remote verification: sessions must be `closed/stopped`, tasks must be finished and reviewed (`accepted`/`rejected`) or in terminal failure/cancel/interrupt states. Offline/missing/active/unreviewed/mismatched-identity states block edits while retaining historical routes and cursors.
- Registry removal never cancels remote tasks, closes remote native sessions, flushes queues, mutates `uncertain` records, or migrates work. Stale poll loops exit cleanly; temporary identity/cursor caches clear upon re-registration.
- Changing only SSH connection fields for the same identity does not bypass an `EventMirror` fingerprint-conflict pause; explicit re-pinning or removal clears transient pause flags while keeping historical fingerprints and durable cursors intact.

Known limitation: historical parent routes from a retired `state_generation` lack explicit retirement proofs and may block later removal if reusing the exact same `host_id` for a different `node_id`—assign a fresh `host_id` when provisioning replacement nodes rather than force-deleting safety checks.
