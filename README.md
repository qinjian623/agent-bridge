> English | [中文](docs/zh/README.md)

# cli-agent-bridge

Use MCP or the command line to send work to Codex, Claude Code, Pi, or Antigravity. Each worker runs in its own session and can keep working after the client disconnects. The broker stores task history in SQLite.

Bridge is for one user on a personal computer or trusted SSH hosts. It runs on Linux, macOS, and Windows through WSL2. Workers have your OS permissions and use your signed-in CLI accounts. Model calls may cost money. Git worktrees keep file edits separate.

## Install in one command

On Linux with `systemd --user` or macOS in a logged-in desktop session, run:

```bash
curl -fsSL https://github.com/qinjian623/agent-bridge/releases/latest/download/install.sh | sh
```

The installer starts Bridge and enables automatic updates. If your shell cannot find `agent-bridge`, add `$HOME/.local/bin` to `PATH`.

## Quick init

```bash
agent-bridge init
```

Choose **1) Profiles**, then **a** to add a profile for a model CLI you've signed into. For the demo below, select Pi and enter its provider and exact model ID. Press **b** to return and **s** to save. [Restart the broker to load the new profile](docs/getting-started.md#managed-services).

## Configure your agent

To install the Bridge Skill and MCP server for Codex, run:

```bash
agent-bridge integrate codex
```

For Claude Code, Pi, or Antigravity, use `claude`, `pi`, or `agy` in place of `codex`. Run `agent-bridge integrate list` to see the available choices. Open a new chat in your chosen client.

## Delegate from an agent chat

Open your project in an agent with Bridge MCP and the skill loaded. If you have a Pi profile, ask:

> Have Pi review my changes with Bridge.

Later in the same chat, ask “What did Pi find?” See the [agent chat demo](docs/agent-demo.md) for setup details and the steps the agent runs.

[Command-line guide](docs/cli.md) · [All init options](docs/cli.md#configure-bridge-with-init)

[Configuration and troubleshooting](docs/getting-started.md) · [Remote hosts and event history](docs/dynamic-hosts.md) · [Uninstall](docs/uninstall.md) · [Design](docs/design.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Checks and limits](docs/production-readiness.md) · [Privacy](docs/privacy.md) · [Changes](CHANGELOG.md)

Licensed under [Apache-2.0](LICENSE).
