> English | [中文](docs/zh/README.md)

# cli-agent-bridge

Run independent Codex, Claude Code, Pi, and Antigravity sessions through MCP or a command line. Send tasks, follow up, inspect results, and stop sessions; the broker keeps task history in SQLite, and workers can continue when the MCP client disconnects.

For one user on their own computer or trusted SSH hosts. Supports Linux and macOS. On Windows, use WSL2. Model calls use your own authenticated CLI account and may cost money. Worktrees keep file edits separate and share your user account's OS permissions.

## Command-line guide

See the [command-line guide](docs/cli.md) for installation, MCP and skill setup, and managing tasks from a terminal.

For the setup wizard and scripting options, see [Configure Bridge with init](docs/cli.md#configure-bridge-with-init).

## Delegate from an agent chat

Open your project in an agent with Bridge MCP and the skill loaded. If you have a Pi profile, ask:

> Have Pi review my changes with Bridge.

Later in the same chat, ask “What did Pi find?” See the [agent chat demo](docs/agent-demo.md) for setup and what the agent does for you.

[Configuration and troubleshooting](docs/getting-started.md) · [Remote hosts and event history](docs/dynamic-hosts.md) · [Uninstall](docs/uninstall.md) · [Design](docs/design.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Checks and limits](docs/production-readiness.md) · [Privacy](docs/privacy.md) · [Changes](CHANGELOG.md)

Licensed under [Apache-2.0](LICENSE).
