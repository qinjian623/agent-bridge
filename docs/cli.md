> English | [中文](zh/cli.md)

# Command-line guide

Install Bridge, connect an agent, and manage tasks from a terminal.

## Install

On Linux with `systemd --user` or macOS in a logged-in desktop session, this command installs the latest stable release and enables automatic Bridge updates.

```bash
curl -fsSL https://github.com/qinjian623/agent-bridge/releases/latest/download/install.sh | sh
```

The installer verifies the wheel, sets up uv and Python if needed, and starts the broker and update timer under your account. If your shell cannot find `agent-bridge`, add `$HOME/.local/bin` to `PATH` or run `~/.local/bin/agent-bridge` directly. For source installation, installing without starting services, or migrating an existing installation, see [installation options](getting-started.md#requirements-and-installation).

## Configure and check

Install and log in to a model CLI separately. Run `agent-bridge init`, choose **1) Profiles**, then **a** to add a profile named `local` using your logged-in Codex, Claude Code, or Pi CLI and its exact model ID. Return with **b** and save with **s**. Profiles start unverified. You can still specify `local` to dispatch your first task. See [Antigravity setup](getting-started.md#antigravity) for its access requirements.

After saving a profile, restart the broker so it loads the changes, then check that it responds. See [managed service commands](getting-started.md#managed-services) for Linux and macOS. With a standalone source installation, start `agent-bridge serve` in another terminal.

```bash
agent-bridge call agent_inspect --json '{}'
```

### Configure Bridge with init

Run `agent-bridge init` in a terminal for the wizard. Choose **6) Advanced settings** for the saved language, routing rules, notification paths, OTA mirror, and managed Web. Use `agent-bridge init --help` to see the scriptable options. Only the settings supplied on the command line are changed in non-interactive mode.

| Settings | Flags |
| --- | --- |
| Runtime profiles and their model, effort, quota, tools, and prices | `--profile SPEC` or `--remove-profile NAME`. Use a JSON object for structured fields. |
| Worker limits, timeouts, selection strategy, and token budget | `--worker-launcher`, `--parallel-limit`, `--quota-limit`, `--task-timeout`, `--interrupt-timeout`, `--default-strategy`, `--token-budget` |
| Projects and remote hosts | `--project ID=PATH`, `--remove-project ID`, `--host SPEC`, `--remove-host ID`. Host specs accept JSON for fields such as SSH options and event mirroring. |
| Saved interface language and routing rules | `--config-language en\|zh-CN\|auto\|none`, `--routing-file PATH`, `--clear-routing` |
| Parent notifications | `--codex-async-python PATH`, `--telegram-database PATH`. An empty string clears either path. |
| Broker services and updates | `--service-kind`, `--service-name`, `--install-broker-service`, `--enable-broker-service`, `--force-broker-service`, `--ota-repo`, `--ota-root`, `--ota-interval`, `--ota-release-url`, `--install-updater`, `--enable-updater` |
| Managed Web | `--install-web` writes the service file. `--enable-web` also starts it. Requires managed updater settings, which can be set up in the same command. |

For example, save the default language and load a [routing rules](getting-started.md#off-peak-routing) file:

```bash
agent-bridge init --non-interactive --config-language zh-CN --routing-file routing.json
```

The file contains the routing object with `calendars`, `windows`, and `preferences`. The command replaces the routing rules as a unit. Pass `--clear-routing` to clear them. Use `--ota-release-url URL` to set a mirror and `--ota-release-url ''` to return to GitHub. OTA settings prepare the updater files. Starting services or enabling the timer requires the corresponding `--enable-*` flag or wizard choice.

CLI help, setup prompts, the terminal monitor, Web pages, and parent task notices support English and Simplified Chinese. Try `agent-bridge --lang zh-CN --help`; the Web dashboard and login page have their own language selector. See [language settings](getting-started.md#language).

To install Web as a background service that restarts with OTA, pass `--with-web` to the installer. For a foreground dashboard, run `agent-bridge monitor --web`. Visit [the local login page](http://127.0.0.1:8080/login). Sign in with the token in your Bridge state's `admin.token`. See [Web setup](getting-started.md#monitoring-and-http--sse-api).

## Connect MCP

Run `agent-bridge integrate list` to see supported clients and their install commands.

Run the line for your client:

```bash
agent-bridge integrate codex
agent-bridge integrate claude
agent-bridge integrate pi
agent-bridge integrate agy
```

Start a new client session to load the MCP tools and skill. For another MCP client, use the absolute `agent-bridge` executable as the command and `["mcp"]` as its arguments. The broker must be running. See [MCP setup](getting-started.md#mcp-setup) for Pi installation, custom state directories, and managed launchers.

## Send a first task

The CLI uses the same tools as MCP. Replace `/absolute/path/to/project` with a real project directory, and `local` if you chose a different profile name. This command starts a real model task and may incur charges:

```bash
agent-bridge call agent_dispatch --json '{"profile":"local","cwd":"/absolute/path/to/project","access":"read-only","instruction":"Read the project README and summarize how to run it. Do not modify files.","request_id":"quickstart-task-1"}'
```

Save the returned `task_id` and `session_id`. Repeating the exact request with the same `request_id` returns the same task; give each new task a new ID.

For automatic profile selection on new local tasks, see [off-peak routing](getting-started.md#off-peak-routing).

## Read the result

Replace the angle-bracket placeholders with the returned IDs:

```bash
agent-bridge call agent_result --json '{"task_id":"<task_id>"}'
agent-bridge monitor
```

`queued` or `running` means work remains. `waiting_input` needs a response; `recovery_needed` needs investigation. `completed` means the model finished its reply. Check its answer, files, and any reported tests before accepting it with `agent_review` or starting dependent work. The [guide](getting-started.md#results-and-review) explains review and full result retrieval.

New installations use `agent_result` or the monitor to see progress. Automatic completion notices require a configured [parent notification helper](getting-started.md#parent-sessions-and-telegram).

## Stop a task or session

Cancel just one task:

```bash
agent-bridge call agent_cancel --json '{"task_id":"<task_id>","request_id":"quickstart-cancel-1"}'
```

To close its session and cancel unstarted work, inspect it, then replace `0` below with the returned **current `revision`**:

```bash
agent-bridge call agent_inspect --json '{"session_id":"<session_id>"}'
agent-bridge call agent_control --json '{"session_id":"<session_id>","action":"close","expected_revision":0,"request_id":"quickstart-close-1"}'
```

Check the session again until its worker has exited. Pressing Ctrl+C in the broker terminal stops the broker; independent workers require their own cancellation or close. Closing a session preserves its history and working directory.

## More command-line operations

Use `agent-bridge --help` for the command list, or append `--help` to a subcommand for its options.

| Operation | Command | Details |
| --- | --- | --- |
| Find profiles and models | `agent-bridge call agent_catalog --json '{}'` | [Model discovery](getting-started.md#autonomous-model-discovery-and-selection) |
| List groups | `agent-bridge call agent_groups --json '{}'` | [Groups and broadcasts](getting-started.md#groups-and-broadcasts) |
| List configured hosts | `agent-bridge call agent_hosts --json '{}'` | [Remote hosts](getting-started.md#remote-hosts) |
| Inspect or maintain mirrored events | `agent-bridge mirrors --help` | [Remote event history](dynamic-hosts.md) |
| Check for a new release | `agent-bridge update check` | [Updates and OTA](getting-started.md#github-releases-and-scheduled-ota-updates) |

See the [tool reference](getting-started.md#tool-interface) for methods available through `agent-bridge call`, and the [uninstall guide](uninstall.md) to remove Bridge.
