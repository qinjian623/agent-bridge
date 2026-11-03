from __future__ import annotations

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from .broker import request


def server(state):
    mcp = FastMCP(
        "cli-agent-bridge",
        instructions="Control independent native agent sessions. Completed results require agent_review before dependent tasks run. Never claim unknown costs are zero.",
    )
    read_only = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False)

    @mcp.tool(annotations=read_only)
    async def agent_groups(group_id: str | None = None) -> dict:
        """List active groups, or inspect one group's revision and member session states. Does not send messages."""
        return await request(state, "agent_groups", locals())

    @mcp.tool()
    async def agent_group(
        group_id: str, action: str, expected_revision: int, request_id: str, session_ids: list[str] | None = None,
        native_members: list[dict] | None = None,
    ) -> dict:
        """Create/add/remove/delete a group. session_ids are existing bridge IDs. native_members register existing native sessions as {runtime, native_id, cwd, label}; remove only needs runtime/native_id. Registration never starts a session. Codex uses its native queue; Antigravity restores the original session when completely closed and waits while any original or unidentified instance is active. Create requires revision 0; other changes use the inspected revision. Delete retires the ID but never cancels queued tasks/messages."""
        params = locals()
        if native_members is None:
            params.pop("native_members")
        return await request(state, "agent_group", params)

    @mcp.tool()
    async def agent_broadcast(
        group_id: str, message: str, expected_revision: int, request_id: str, reservation: int = 0,
    ) -> dict:
        """Queue one message per member of the explicit group at its inspected revision. May incur inference costs. Bridge members get tasks; native Codex uses its existing queue without takeover. A closed original Antigravity session is restored for the message; active/uncertain owners wait, with no terminal input or duplicate instance. Old skipped broadcasts are never replayed. Retry the same request_id and parameters to avoid duplicates. Read task_id with agent_result or broadcast_id with agent_broadcast_status. Pending is local; received requires native user_input confirmation and is separate from processing_status. Native delivery requires the bridge token budget disabled; parent notifications are unchanged."""
        return await request(state, "agent_broadcast", locals())

    @mcp.tool(annotations=read_only)
    async def agent_broadcast_status(broadcast_id: str, refresh: bool = False) -> dict:
        """Read each recipient's current state. refresh reads native queues/history only; never sends or wakes a session. Queue disappearance alone is unconfirmed, not received. Uncertain deliveries are never automatically resent."""
        return await request(state, "agent_broadcast_status", locals())

    @mcp.tool(annotations=read_only)
    async def agent_group_inbox(group_id: str, runtime: str, native_id: str, after: int = 0, limit: int = 10) -> dict:
        """Read saved broadcasts addressed to one current native member of an explicit group. Antigravity can pull messages here even when push delivery was skipped. Does not start/resume/interrupt any session or mark messages received. A coordinator reading on behalf of a target is not native receipt. Remember cursor and pass it as after to avoid reading old messages again; has_more indicates another page. Requires normal bridge management access."""
        return await request(state, "agent_group_inbox", locals())

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=True))
    async def agent_catalog(refresh: bool = False, profile: str | None = None) -> dict:
        """Discover models from configured providers without inference. Cache 5 minutes; refresh forces a query. Returns per-profile errors/stale data. Select an exact discovered ID with profile + model in open/dispatch; listed does not mean inference-verified."""
        return await request(state, "agent_catalog", locals())

    @mcp.tool()
    async def agent_open(
        profile: str,
        cwd: str,
        request_id: str,
        access: str = "read-only",
        parent: str | None = None,
        base: str = "HEAD",
        snapshot_files: list[str] | None = None,
        workspace_mode: str = "git",
        model: str | None = None,
    ) -> dict:
        """Create a persistent native session. Optional model selects an exact discovered ID under this profile's provider/auth/quota. Writes use a Git worktree; snapshot_files selects current file contents. Non-Git workspace_mode=copy requires snapshot_files."""
        return await request(state, "agent_open", locals())

    @mcp.tool()
    async def agent_dispatch(
        instruction: str,
        request_id: str,
        session_id: str | None = None,
        profile: str | None = None,
        cwd: str | None = None,
        access: str | None = None,
        dependencies: list[str] | None = None,
        reservation: int | None = None,
        requirements: dict | None = None,
        parent: str | None = None,
        retry_task_id: str | None = None,
        base: str = "HEAD",
        snapshot_files: list[str] | None = None,
        workspace_mode: str = "git",
        model: str | None = None,
    ) -> dict:
        """Queue one task. Omitted access inherits an existing session or defaults to read-only for a new one; explicit access must match. Omitted reservation preserves retry's prior value; an explicit value changes only the next attempt. Optional profile + model selects an exact discovered model; existing sessions cannot change models. Dependencies require accepted reviews. requirements: kind, tools, input_types, input_tokens, providers, minimum_quality, execute_locally, strategy (balanced/cost/speed), learn_from_result. No verified hard_budget support."""
        return await request(state, "agent_dispatch", locals())

    @mcp.tool()
    async def agent_control(
        session_id: str,
        action: str,
        expected_revision: int,
        request_id: str,
        continue_task_id: str | None = None,
        question_id: str | None = None,
        answer: dict | None = None,
        native_session_id: str | None = None,
        cwd: str | None = None,
        event_seq: int | None = None,
        task_id: str | None = None,
        resolution: str | None = None,
        evidence: str | None = None,
        usage_path: str | None = None,
        usage_since: float | None = None,
        usage_profile: str | None = None,
        notify: bool = True,
        resume_config_file: str | None = None,
    ) -> dict:
        """Pause, pause_drain, resume, close, reopen, respond, resolve; or parent_register/pause/resume/close/reopen/ack/meter_stop and quota_resume. usage_path must match native_session_id; usage_since is a Unix timestamp. notify=false only meters. resume_config_file references private original MCP settings, never model/permissions. Resume repeats nothing unless continue_task_id is given. Inspect revision first."""
        return await request(state, "agent_control", locals())

    @mcp.tool()
    async def agent_cancel(task_id: str, request_id: str) -> dict:
        """Cancel this task, keeping other tasks and the session intact."""
        return await request(state, "agent_cancel", locals())

    @mcp.tool(annotations=read_only)
    async def agent_inspect(
        session_id: str | None = None, task_id: str | None = None, parent_id: str | None = None
    ) -> dict:
        """List sessions with profile, runtime, provider, model, effort and service_tier; or inspect one session/task's revision, waiting_reason, questions, owned worker processes and usage. Processes outside the worker cgroup remain unknown."""
        return await request(state, "agent_inspect", locals())

    @mcp.tool(annotations=read_only)
    async def agent_wait(
        after: int = 0, timeout: float = 20, session_id: str | None = None, task_id: str | None = None
    ) -> dict:
        """Wait up to 50 seconds for durable events after a cursor; timeout is not failure."""
        return await request(state, "agent_wait", locals())

    @mcp.tool(annotations=read_only)
    async def agent_result(
        task_id: str, artifact_id: str | None = None, offset: int = 0, limit: int = 4000, attempt_id: str | None = None
    ) -> dict:
        """Read a compact result and recent attempts. Read a result/tool artifact by ID and byte offset (limit 4..65536); use returned next_offset for UTF-8 text. An attempt_id selects one historical attempt."""
        return await request(state, "agent_result", locals())

    @mcp.tool()
    async def agent_review(
        task_id: str, attempt_id: str, decision: str, evidence: str, expected_revision: int, request_id: str
    ) -> dict:
        """Accept or reject the current completed attempt with actual verification evidence; decision is accepted or rejected."""
        return await request(state, "agent_review", locals())

    return mcp
