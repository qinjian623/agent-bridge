> English | [中文](docs/zh/README.md)

# cli-agent-bridge

Run independent Codex, Claude Code, Pi, and Antigravity sessions through MCP or a command line. Send tasks, follow up, inspect results, and stop sessions; the broker keeps task history in SQLite, and workers can continue when the MCP client disconnects.

For one user on their own computer or trusted SSH hosts. Requires Python 3.11+, [uv](https://docs.astral.sh/uv/getting-started/installation/), and at least one installed, authenticated model CLI. Linux and macOS are supported; Windows requires WSL2. Model calls use your CLI account and may cost money. Worktrees separate file edits; they do not provide OS isolation.

## Install

Install directly from this GitHub repository with uv.

```bash
uv tool install 'git+https://github.com/qinjian623/agent-bridge.git'
agent-bridge init
agent-bridge doctor
```

If `agent-bridge` is not found, run `uv tool update-shell` and open a new terminal. In `init`, choose **1) Profiles**, then **a** to add a profile named `local` using your logged-in Codex, Claude Code, or Pi CLI and its exact model ID; return with **b**, then **s** to save. Keep remote hosts and services unset for this first run. Profiles start unverified; specifying `local` explicitly still allows a first task. Antigravity needs additional setup and writable access; see the [guide](docs/getting-started.md#antigravity).

## Start

Keep this running in a terminal:

```bash
agent-bridge serve
```

In a second terminal, check the broker:

```bash
agent-bridge call agent_inspect --json '{}'
```

For background startup, see [managed services](docs/getting-started.md#managed-services).

CLI help, setup prompts, the terminal monitor, Web pages, and parent task notices support English and Simplified Chinese. Try `agent-bridge --lang zh-CN --help`; the Web dashboard and login page have their own language selector. See [language settings](docs/getting-started.md#language).

To open the dashboard, run `agent-bridge monitor --web` in another terminal and visit [the local login page](http://127.0.0.1:8080/login). Sign in with the token in your Bridge state's `admin.token`. See [Web setup](docs/getting-started.md#monitoring-and-http--sse-api).

## Connect MCP

For Codex, run in the second terminal:

```bash
codex mcp add cli-agent-bridge -- "$(command -v agent-bridge)" mcp
codex mcp list
```

Start a new Codex session to load the tools. For other MCP clients, use the absolute `agent-bridge` executable as the command and `["mcp"]` as its arguments. The broker must be running. See [MCP setup](docs/getting-started.md#mcp-setup) for custom state directories and managed launchers.

## Send a first task

The CLI uses the same tools as MCP. Replace `/absolute/path/to/project` with a real project directory, and `local` if you chose a different profile name. This command starts a real model task and may incur charges:

```bash
agent-bridge call agent_dispatch --json '{"profile":"local","cwd":"/absolute/path/to/project","access":"read-only","instruction":"Read the project README and summarize how to run it. Do not modify files.","request_id":"quickstart-task-1"}'
```

Save the returned `task_id` and `session_id`. Repeating the exact request with the same `request_id` returns the same task; give each new task a new ID. Through MCP, ask your assistant to call `agent_catalog`, then dispatch the same read-only task with `profile="local"` and your project directory.

For automatic profile selection on new local tasks, see [off-peak routing](docs/getting-started.md#off-peak-routing).

## Read the result

Replace the angle-bracket placeholders with the returned IDs:

```bash
agent-bridge call agent_result --json '{"task_id":"<task_id>"}'
agent-bridge monitor
```

`queued` or `running` means work remains. `waiting_input` needs a response; `recovery_needed` needs investigation. `completed` means the model finished its reply. Check its answer, files, and any reported tests before accepting it with `agent_review` or starting dependent work. The [guide](docs/getting-started.md#results-and-review) explains review and full result retrieval.

New installations use `agent_result` or the monitor to see progress. Automatic completion notices require a configured [parent notification helper](docs/getting-started.md#parent-sessions-and-telegram).

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

[Configuration and troubleshooting](docs/getting-started.md) · [Uninstall](docs/uninstall.md) · [Design](docs/design.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Checks and limits](docs/production-readiness.md) · [Privacy](docs/privacy.md) · [Changes](CHANGELOG.md)

Licensed under [Apache-2.0](LICENSE).
