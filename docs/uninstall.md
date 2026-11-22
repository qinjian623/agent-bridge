> English | [中文](zh/uninstall.md)

# Uninstall

Remove service registrations and the installed program separately. Keep task history and working directories until you have checked them: writable sessions can contain changes that were never applied to the source project. There is no `agent-bridge uninstall` command.

The commands below use the default state directory, managed root, and service names. If you used `--state` or `AGENT_BRIDGE_STATE`, use that directory for every Bridge command. If you configured managed services, read `root`, `service_kind`, and `service_name` from `<state>/update.json` and substitute them below. Source installations can run `uv run agent-bridge` from their checkout while completing these steps.

## 1. Stop scheduled updates and finish existing work

Stop the update timer first if it was installed. This does not stop an updater already in progress:

Linux:

```bash
systemctl --user disable --now cli-agent-bridge-update.timer
systemctl --user status cli-agent-bridge-update.service
```

macOS:

```bash
launchctl print "gui/$(id -u)/com.cli-agent-bridge.update"
```

On macOS, check whether the update job is running (for example, a PID in the output) and wait for it to finish before unloading it:

```bash
launchctl bootout "gui/$(id -u)/com.cli-agent-bridge.update"
```

On Linux, wait for the oneshot update service to finish. If a service was never installed, skip its commands. A missing/inactive service can return a nonzero status. An `uncertain` update result needs inspection before continuing.

Keep the broker running while closing sessions:

```bash
agent-bridge call agent_inspect --json '{}'
```

For each session you want to stop, inspect it and close it using its current `revision`. Replace the placeholder and the example `0`:

```bash
agent-bridge call agent_inspect --json '{"session_id":"<session_id>"}'
agent-bridge call agent_control --json '{"session_id":"<session_id>","action":"close","expected_revision":0,"request_id":"uninstall-close-1"}'
agent-bridge call agent_inspect --json '{"session_id":"<session_id>"}'
```

Use a different `request_id` for each session. Wait until active tasks reach a final state and each worker has exited (`worker_alive=false`). Investigate `recovery_needed` rather than treating it as stopped. Close cancels queued work and requests interruption of an active turn; it preserves history and files. It does not stop jobs launched outside the worker's process scope, or remote brokers. Close remote sessions on their owning nodes as needed; removing a host entry never uninstalls that host.

## 2. Stop the broker and remove user service files

For a foreground broker, press Ctrl+C in its terminal. For services, use the matching section. Inspect the service files first to confirm they belong to this installation; custom names require corresponding changes.

Linux `systemd --user` (including the legacy `install_local.py` helper):

```bash
systemctl --user cat cli-agent-bridge.service
systemctl --user disable --now cli-agent-bridge.service
rm -f "$HOME/.config/systemd/user/cli-agent-bridge.service"
rm -f "$HOME/.config/systemd/user/cli-agent-bridge-update.timer"
rm -f "$HOME/.config/systemd/user/cli-agent-bridge-update.service"
systemctl --user daemon-reload
```

Skip updater files if the timer was not installed. Do not stop or delete unrelated user services. Worker units are separate; the session checks above must happen before shutting down the broker.

macOS `launchd`:

```bash
launchctl print "gui/$(id -u)/com.cli-agent-bridge.broker"
launchctl bootout "gui/$(id -u)/com.cli-agent-bridge.broker"
rm -f "$HOME/Library/LaunchAgents/com.cli-agent-bridge.broker.plist"
rm -f "$HOME/Library/LaunchAgents/com.cli-agent-bridge.update.plist"
```

The updater LaunchAgent should already have been unloaded in step 1. Default broker logs and task records remain in the state directory.

## 3. Remove MCP and companion skill registrations

For a Codex registration created by the quick start:

```bash
codex mcp remove cli-agent-bridge
codex mcp list
```

Remove the matching entry from any other MCP clients you configured, then reload or exit those clients so their stdio MCP processes stop. This does not remove model CLI login credentials.

If you used `scripts/install_local.py`, inspect its links:

```bash
ls -ld "$HOME/.local/bin/agent-bridge" "$HOME/.codex/skills/cli-agent-bridge"
```

Only when those entries are symlinks to this checkout, unlink them:

```bash
unlink "$HOME/.local/bin/agent-bridge"
unlink "$HOME/.codex/skills/cli-agent-bridge"
```

If a path is a real file/directory or belongs to another installation, leave it alone. Remove other manually created skill links only after checking their targets. Managed installations can have links pointing to `<managed-root>/current/skill`.

## 4. Remove the installed program

For `uv tool install`, including installs from Git:

```bash
uv tool list
uv tool uninstall cli-agent-bridge
```

For `uv sync` in a source checkout, there is no global package to uninstall. Once services, MCP registrations, and source helper links have been removed, the checkout and its `.venv` can be retained or removed using your normal file tools. Check `git status` and preserve your edits first. If you installed the checkout through `uv tool install`, use `uv tool uninstall` as above as well.

For a one-command release install, remove `~/.local/bin/agent-bridge` after confirming it is a symlink to `~/.local/share/cli-agent-bridge/bin/agent-bridge`.

Managed services/OTA also create `~/.local/share/cli-agent-bridge` by default, containing `bin`, `current`, and `releases`. They are separate from a uv tool environment. After all services, workers, update jobs, and MCP processes have stopped, remove any shell PATH entries you added for its `bin`, then move this directory aside or remove it with your file tools. Use the configured `root` if different. Do not remove the bootstrap Python environment while a managed launcher still depends on it.

## 5. Keep or remove local data deliberately

| Location | Contents and default action |
| --- | --- |
| `~/.local/state/cli-agent-bridge` (or your custom state) | Config, SQLite history, identity pins, private artifacts, logs, and session workspaces: keep |
| `~/.local/share/cli-agent-bridge` (or configured managed root) | Managed launchers and releases: optional removal after all processes stop |
| Source checkout and `.venv` | Source edits and source-installed dependencies: inspect before removal |
| Referenced `env_file`, model CLI login files | Separate credentials: keep; Bridge uninstall does not revoke the account |
| Remote installations | Remain installed; repeat these steps on each host only when intended |

To remove history later, stop every process using that state directory, inspect its workspaces and artifacts, and keep a private backup of anything needed. The state directory can contain prompts and credentials references; do not upload it as an issue attachment. Deletion commands for runtime data are intentionally omitted.

Open a new shell and check `command -v agent-bridge` and `uv tool list`. If a command remains, inspect its location: another uv installation, source helper link, or managed launcher may still exist. Check that the removed service is no longer active. A remaining history directory alone does not mean a process is still running.
