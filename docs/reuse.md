> Language: English | [中文](zh/reuse.md)

# Handling of Existing Skills and Code (2026-10-22)

Local installations were inspected on 2026-10-22 while keeping all existing services and credentials intact.

| Existing Project or Skill | Treatment |
| --- | --- |
| `codex-async` | `codex_parent.py` optionally uses an existing installation's `CodexAppServer` (including native queue IDs, shared daemon reuse, busy checks, and config-preserving resume); no separate scheduler service was reimplemented. |
| `codex-telegram` | Preserves existing topics, receive queues, final replies, and attachment delivery; added optional `external_children` filtering so bridge sub-sessions do not trigger unintended replies. Bot secrets and receive loops are not duplicated. |
| Existing Claude caller code | Kept exact UUID resolution, session resume, and native authentication; `cli-agent-bridge` uses the official `claude-agent-sdk` for streaming interrupt and approval callbacks, pointing at the local CLI path. |
| Existing Pi RPC/extension | Preserved native session files and `agent_settled` completion detection; new background sub-sessions disable extensions so interactive terminal sessions remain under existing extension control. |
| `task-learning` | Consulted related learnings and stored concise inspection notes without copying raw transcripts or credentials. |
| `skill-creator` | Created a compact `cli-agent-bridge` skill documenting CLI entrypoints, resume rules, and verification boundaries without duplicating Telegram setup steps. |
| CLI-Subagents, PAL clink, codex-agy-bridge | Adopted the reviewed architecture patterns (real CLI sessions, simple MCP tools, detached background workers); third-party source was not copied directly. |

Integration against `codex-async` and companion skill runtime copies was verified in place without modifying the shared Codex daemon.

Added `telegram_guard.py` to read pause, uncertain, and topic-deletion markers and register child session IDs. When configured via `telegram_database`, the legacy bot filters registered external children (verified with 107 existing bot tests passing at integration time before reloading the Telegram service cleanly while leaving the shared Codex daemon untouched).

After cold-resume testing, parent session resume switched to raw `request("thread/resume", ...)` because convenience helpers with `preserve_config=True` still overrode `approvalPolicy=never`. Temporary MCP settings restore via an owner-only `0600` private config reference without altering model, reasoning effort, or approval policy. Completed verification across Sol, Pi, and Claude results within a single native Astra parent session.
