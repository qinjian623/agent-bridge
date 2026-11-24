> English | [中文](docs/zh/README.md)

# cli-agent-bridge

Run independent Codex, Claude Code, Pi, and Antigravity sessions through MCP or a command line. Send tasks, follow up, inspect results, and stop sessions; the broker keeps task history in SQLite, and workers can continue when the MCP client disconnects.

For one user on their own computer or trusted SSH hosts. Supports Linux and macOS. On Windows, use WSL2. Model calls use your own authenticated CLI account and may cost money. Worktrees keep file edits separate and share your user account's OS permissions.

## Install

On Linux with `systemd --user` or macOS in a logged-in desktop session, this command installs the latest stable release and enables automatic Bridge updates.

```bash
curl -fsSL https://github.com/qinjian623/agent-bridge/releases/latest/download/install.sh | sh
```

The installer verifies the wheel, sets up uv and Python if needed, and starts the broker and update timer under your account. If your shell cannot find `agent-bridge`, add `$HOME/.local/bin` to `PATH` or run `~/.local/bin/agent-bridge` directly. For source installation, installing without starting services, or migrating an existing installation, see [installation options](docs/getting-started.md#requirements-and-installation).

## Configure and check

Install and log in to a model CLI separately. Run `agent-bridge init`, choose **1) Profiles**, then **a** to add a profile named `local` using your logged-in Codex, Claude Code, or Pi CLI and its exact model ID. Return with **b** and save with **s**. Profiles start unverified. You can still specify `local` to dispatch your first task. See [Antigravity setup](docs/getting-started.md#antigravity) for its access requirements.

After saving a profile, restart the broker so it loads the changes, then check that it responds. See [managed service commands](docs/getting-started.md#managed-services) for Linux and macOS. With a standalone source installation, start `agent-bridge serve` in another terminal.

```bash
agent-bridge call agent_inspect --json '{}'
```

CLI help, setup prompts, the terminal monitor, Web pages, and parent task notices support English and Simplified Chinese. Try `agent-bridge --lang zh-CN --help`; the Web dashboard and login page have their own language selector. See [language settings](docs/getting-started.md#language).

To install Web as a background service that restarts with OTA, pass `--with-web` to the installer. For a foreground dashboard, run `agent-bridge monitor --web`. Visit [the local login page](http://127.0.0.1:8080/login). Sign in with the token in your Bridge state's `admin.token`. See [Web setup](docs/getting-started.md#monitoring-and-http--sse-api).

## Connect MCP

For Codex, run:

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

[Configuration and troubleshooting](docs/getting-started.md) · [Remote hosts and event history](docs/dynamic-hosts.md) · [Uninstall](docs/uninstall.md) · [Design](docs/design.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Checks and limits](docs/production-readiness.md) · [Privacy](docs/privacy.md) · [Changes](CHANGELOG.md)

Licensed under [Apache-2.0](LICENSE).
