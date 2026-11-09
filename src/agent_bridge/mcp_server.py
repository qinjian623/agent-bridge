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
    async def agent_hosts() -> dict:
        """List administrator-registered execution hosts with their cached state, pinned identity and last contact. Never opens a network connection; refresh happens through peer traffic."""
        return await request(state, "agent_hosts", {})

    @mcp.tool()
    async def agent_hosts_reload() -> dict:
        """Administrator-only immediate refresh of config.json hosts and host_pins.json; the Broker also watches them automatically. Adds unused nodes without restart. Removing or changing a node with group members, parent routes or unsettled work is refused, keeping the previous registry. Does not cancel tasks, restart agents or replay deliveries. Invalid configuration preserves the last working list; agent_hosts.config_error explains the failure."""
        return await request(state, "agent_hosts_reload", {})

    @mcp.tool(annotations=read_only)
    async def agent_monitor(limit: int = 15, include_events: bool = True) -> dict:
        """Overall multi-agent monitor snapshot: cluster hosts, active sessions, running tasks, pending questions/approvals, broadcasts, and recent communications."""
        return await request(state, "agent_monitor", locals())

    @mcp.tool(annotations=read_only)
    async def agent_groups(group_id: str | None = None, active_only: bool = False) -> dict:
        """List groups with derived activity state, or inspect one group including deleted details and member states. active_only keeps active and unknown groups; unknown is deliberately included because unverified members must not disappear. Does not send messages."""
        return await request(state, "agent_groups", locals())

    @mcp.tool()
    async def agent_group_prune(
        older_than: str | int | float, dry_run: bool = True, request_id: str | None = None,
    ) -> dict:
        """Administrator-only historical group cleanup. Soft-deletes groups whose last activity is strictly older than older_than (for example 7d, 12h, 2w, 3600s) only after read-only verification that every member is stopped and all work is settled; unreachable, running, or unknown members skip the group. dry_run defaults to true and only reports eligible/blocked groups. Reuse request_id to return the same run; a new request_id is required after reviewing a dry run to actually prune."""
        return await request(state, "agent_group_prune", locals())

    @mcp.tool()
    async def agent_group(
        group_id: str, action: str, expected_revision: int, request_id: str, session_ids: list[str] | None = None,
        native_members: list[dict] | None = None, remote_members: list[dict] | None = None,
    ) -> dict:
        """Create/add/remove/delete a group. session_ids are existing bridge IDs. native_members register existing native sessions as {runtime, native_id, cwd, label}; remove only needs runtime/native_id. remote_members register execution-node targets as {host_id, target_kind, session_id|runtime+native_id, cwd, label}; node identity is resolved from the registered host and must match when supplied. Registration never starts a session. Codex uses its native queue; Antigravity restores the original session when completely closed and waits while any original or unidentified instance is active. Create requires revision 0; other changes use the inspected revision. Delete retires the ID but never cancels queued tasks/messages."""
        params = locals()
        if native_members is None:
            params.pop("native_members")
        if remote_members is None:
            params.pop("remote_members")
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

    @mcp.tool()
    async def agent_delivery_resolve(
        delivery_id: str, resolution: str, evidence: str, expected_revision: int,
        expected_delivery_revision: int, expected_remote_revision: int, request_id: str,
    ) -> dict:
        """Administrator-only repair for an uncertain head of a remote broadcast stream. resolution=received records that the execution node already has the delivery; resolution=not_received cancels that original sequence and never resends it. evidence must be non-empty (at most 16 KiB) and name what was inspected plus the conclusion. All three revisions are required: the group revision, this delivery's peer_outbox revision, and the execution node's inspection revision. Offline peers return resolving and keep later messages blocked."""
        return await request(state, "agent_delivery_resolve", locals())

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=True))
    async def agent_catalog(refresh: bool = False, profile: str | None = None, host_id: str | None = None) -> dict:
        """Discover models from configured providers without inference, locally or on a registered remote host. Cache 5 minutes; refresh forces a query. Returns per-profile errors/stale data. Select an exact discovered ID with profile + model in open/dispatch; listed does not mean inference-verified."""
        return await request(state, "agent_catalog", locals())

    @mcp.tool(annotations=read_only)
    async def agent_operation(operation_id: str, wait: float | None = None) -> dict:
        """Read the state and stored result of a cross-host operation by its stable operation_id. Optional wait (seconds, capped) polls until the operation settles or the timeout expires; a timeout is not failure."""
        return await request(state, "agent_operation", locals())

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
        host_id: str | None = None,
        project_id: str | None = None,
        name: str | None = None,
    ) -> dict:
        """Create a persistent native session locally or on a registered remote host. Optional model selects an exact discovered ID under this profile's provider/auth/quota. Optional name labels a newly created session (1-128 trimmed characters, no control characters); it is rejected for an existing session. Writes use a Git worktree; snapshot_files selects current file contents. Non-Git workspace_mode=copy requires snapshot_files. For remote hosts, cwd stays the local source directory and project_id names the managed remote project; open+dispatch is one atomic operation there (ADR-13)."""
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
        host_id: str | None = None,
        project_id: str | None = None,
        name: str | None = None,
    ) -> dict:
        """Queue one task locally or on a registered remote host. Omitted access inherits an existing session or defaults to read-only for a new one; explicit access must match. Omitted reservation preserves retry's prior value; an explicit value changes only the next attempt. Optional profile + model selects an exact discovered model; existing sessions cannot change models. Optional name labels a session created by this dispatch (1-128 trimmed characters, no control characters) and is rejected when session_id is given. Dependencies require accepted reviews. requirements: kind, tools, input_types, input_tokens, providers, minimum_quality, execute_locally, strategy (balanced/cost/speed), learn_from_result. No verified hard_budget support. Remote dispatch without session_id uses one atomic open+dispatch operation; cwd is the local source dir and project_id names the remote project."""
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
    async def agent_cancel(
        task_id: str | None = None,
        request_id: str = "",
        operation_id: str | None = None,
        delivery_id: str | None = None,
        host_id: str | None = None,
    ) -> dict:
        """Cancel by exactly one of task_id, operation_id or delivery_id, keeping other tasks and the session intact. host_id routes the cancel to a registered remote node; operation_id/delivery_id also work before a remote task_id exists (ADR-11)."""
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
