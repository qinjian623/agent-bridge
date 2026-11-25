> English | [中文](zh/agent-demo.md)

# Delegate from a Codex chat to Pi

In this example, you ask Codex to delegate a read-only task to Pi and retrieve the answer later in the same chat. Pi runs in its own Bridge session. Dispatch may incur model charges.

## Set up the agent and worker

1. Configure a Bridge profile named `pi-demo` for a signed-in Pi CLI with a model you can use. Start the broker with that configuration. Substitute your own profile name in the prompts if you already have one. See [configuration](getting-started.md#configuration).
2. Register Bridge MCP for Codex, then open a new Codex chat in a repository with a README. See [MCP setup](getting-started.md#mcp-setup).

## Send a task

Paste this into the Codex chat:

> Use the Agent Bridge MCP tools. Check the `pi-demo` profile with `agent_catalog`, then send one read-only task to it in the current project. Ask Pi to read the README and summarize the install and test commands. Ask it to cite the README sections it used and leave project files untouched. Give me the task ID and session ID once the task is queued so we can keep using this chat.

Codex calls `agent_catalog(profile="pi-demo")` to find the configured Pi profile, then calls `agent_dispatch` with that profile, the absolute project directory, `access="read-only"`, the instruction, and a unique `request_id`. Dispatch returns before Pi finishes. If Codex needs to retry the same submission, it keeps the same arguments and `request_id`.

## Read the answer

When you want the result, send a follow-up in the same Codex chat:

> Check the result of the Pi task we just queued. If it finished, read its answer, compare the commands with the README, and summarize what you verified. If it is still queued or running, tell me its current status and keep the existing task.

Codex calls `agent_result(task_id=...)` with the saved task ID. If the task needs input or recovery, it can inspect the task and report what is needed. Before starting another Bridge task that depends on this answer, Codex records its verification with `agent_review` on the completed attempt. See [results and review](getting-started.md#results-and-review).

A fresh Bridge installation does not send automatic completion notices. With a configured and verified [parent notification helper](getting-started.md#parent-sessions-and-telegram), Codex can handle the completion event and fetch the same result automatically. When you are done, ask Codex to close the demo Bridge session using the saved session ID.
