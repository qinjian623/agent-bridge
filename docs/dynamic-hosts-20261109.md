> Language: English | [中文](zh/dynamic-hosts-20261109.md)

# Dynamic Host Registry and Secondary Linux Node Verification (2026-11-09)

## Live Runtime Configuration Reloads

The broker polls `config.json.hosts` and `host_pins.json` in the state directory every 0.5 seconds. Adding, editing, or removing entries under `hosts` takes effect immediately without restarting the broker, Web monitor, or running workers (applying host registry changes only without modifying runtimes, models, permissions, or quotas). Write changes atomically via temporary file rename to prevent partial reads.

To force an immediate reload via MCP/CLI:

```bash
agent-bridge call agent_hosts_reload --json '{}'
agent-bridge call agent_hosts --json '{}'
```

`agent_hosts_reload` returns `{added, updated, removed}` (empty lists when unchanged). It takes no parameters, requires local same-UID coordinator admin token auth, and is excluded from restricted member and `peer.*` paths. Existing MCP clients do not need re-discovery unless listing newly registered tools.

Example `hosts` entry (note SSH host aliases in this check are redacted to `linux-node-b`):

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

## Live Node Bring-Up & Verification Summary (`linux-node-b`)

- Synced repository and Git metadata cleanly via sync script to `~/gits/cli-agent-bridge` without copying credentials, SQLite state, or local virtualenvs.
- Verified `cli-agent-bridge.service` user unit enabled/active, `broker.sock` present, and direct `agent_inspect` succeeded remotely (not relying solely on SSH relay).
- Confirmed `peer.hello`, node `inspect`, and remote catalog (`protocol_version=1`, `ready`); adding the node surfaced it online across MCP and the running Web monitor without restart.
- Two pre-existing Codex app-server daemons on `linux-node-b` stayed undisturbed (`verified=false` intentionally kept with zero inference triggered).
- Verified dynamic add -> remove -> re-add of `linux-node-b` over real SSH under an isolated broker/Web PID without restarts, plus rejection + automatic recovery when intentionally writing corrupt test config.
- Full test suite at check time: 687 tests passed in 24.342s, Ruff and `git diff --check` clean; isolated deployment verification details remain saved privately under `<private-validation-state>/report`.

## Initial Coordinator Cutover Record

Because the legacy running broker (`PID <process-02-pid>`) pre-dated live config-watch support, a single authorized hot-idle broker restart upgraded PID `<process-02-pid> -> 2166491` once all in-flight sends, API calls, socket connections, and non-SSH helpers reached zero (`Web PID <process-01-pid>`, shared Codex daemon `PID <process-14-pid>`, and exited historical workers stayed undisturbed). Merging `linux-node-b` afterward left coordinator PIDs untouched and registered remote daemon `PID <process-07-pid>` cleanly. Meanwhile `macos-node` remained unreachable over private VPN (`<vpn-ip>:22` timed out, `Online=false` in mesh view) with its config preserved untouched.
