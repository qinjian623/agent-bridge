> English | [中文](zh/agent-demo.md)

# Delegate from a Codex chat to Pi

In this example, Pi reviews your changes in a separate Bridge session. You ask for the findings later in the same Codex chat. Dispatch may incur model charges.

## Set up the agent and worker

1. Configure a Bridge profile for a signed-in Pi CLI with a model you can use, then start the broker with that configuration. See [configure and check](cli.md#configure-and-check).
2. Install Bridge MCP and the skill for Codex, then open a new Codex chat in a Git repository with local changes. See [connect MCP](cli.md#connect-mcp).

## Send a task

Paste this into the Codex chat:

> Have Pi review my changes with Bridge.

Codex finds a Pi profile with `agent_catalog` and sends a read-only review of the local changes with `agent_dispatch`. It supplies the project directory and request ID, keeps the task and session IDs, and returns after the task is queued.

## Read the answer

When you want the result, send a follow-up in the same Codex chat:

> What did Pi find?

Codex uses `agent_result` with the saved task ID. If the review is still running, it reports the current status. Once the review finishes, Codex checks Pi's findings against the diff before summarizing them. If another Bridge task depends on the review, Codex records the verification with `agent_review`. See [results and review](getting-started.md#results-and-review).

A fresh Bridge installation does not send automatic completion notices. With a configured and verified [parent notification helper](getting-started.md#parent-sessions-and-telegram), Codex can handle the completion event and fetch the same result automatically. When you are done, ask Codex to close the demo Bridge session using the saved session ID.
