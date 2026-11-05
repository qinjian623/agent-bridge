"""Antigravity's NDJSON protocol is not the Claude SDK control protocol."""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import signal

from .config import safe_error
from .errors import BridgeError
from .quota import provider_limit
from .runtimes import JsonProcess

FIELDS = {
    "input_tokens": "input_tokens", "output_tokens": "output_tokens",
    "thinking_tokens": "reasoning_tokens", "cache_read_tokens": "cache_read_tokens",
    "total_tokens": "total_tokens",
}


class AntigravityProcess(JsonProcess):
    denied = False

    async def read_stderr(self):
        while line := await self.process.stderr.readline():
            notice = safe_error(line.decode(errors="replace").strip())
            self.denied |= bool(re.search(r"soft[ -]denied|permission denied|requires? approval", notice, re.I))
            self.stderr.append(notice)


def counters(value):
    if not isinstance(value, dict) or any(type(value.get(k)) is not int or value[k] < 0 for k in FIELDS):
        return None
    return {k: value[k] for k in FIELDS}


def turn_usage(before, after, steps):
    valid = before is not None and after is not None and all(
        after[k] - before.get(k, 0) >= steps.get(k, 0) for k in FIELDS
    )
    counts = {k: after[k] - before.get(k, 0) for k in FIELDS} if valid else steps
    return {
        **{dest: counts.get(src) for src, dest in FIELDS.items()},
        "scope": "attempt_cli_reported" if valid else "attempt_observed_steps",
        "complete": valid,
        "cache_write_tokens": None,
        "input_includes_cache": None,
        "pricing_supported": False,
        "reason": None if valid else "Missing/reset process counters; only this attempt's observed steps are counted",
    }


class AntigravityRuntime:
    def __init__(self, profile, session, state, on_question):
        self.profile, self.session = profile, session
        self.native_id = session.get("native_id")
        self.client = None
        self.running = False
        self.total_usage = None
        self.turns = None
        self.last_step = -1

    async def connect(self):
        self.profile.validate()
        if self.session["access"] != "workspace-write":
            raise BridgeError("unsupported_access", "Antigravity cannot enforce read-only tools; workspace-write is required")
        argv = [self.profile.command(), "--input-format", "stream-json", "--output-format", "stream-json",
                "--model", self.profile.model, "--sandbox", "--mode", "accept-edits"]
        if self.profile.effort:
            argv += ["--effort", self.profile.effort]
        resumed = bool(self.native_id)
        if resumed:
            argv += ["--conversation", self.native_id]
        self.client = AntigravityProcess(argv, self.session["cwd"], self.profile.environment(),
                                         lambda e: e.get("event") in {"init", "step_update", "result"}, self.reject_question)
        try:
            await self.client.start()
            event = await asyncio.wait_for(self.client.event(), 60)
            native = event.get("conversation_id")
            if event.get("event") != "init" or not isinstance(native, str) or not native:
                raise BridgeError("invalid_protocol", "Antigravity did not return an initial conversation ID")
            if self.native_id and native != self.native_id:
                raise BridgeError("session_mismatch", "Antigravity resumed a different conversation")
            if event.get("init", {}).get("model") != self.profile.model:
                raise BridgeError("model_mismatch", "Antigravity did not confirm the exact requested model")
            self.native_id = native
            # Across process restarts the CLI's cumulative-counter baseline is not established.
            self.total_usage = None if resumed else {}
            self.turns, self.last_step = None, -1
        except BaseException:
            await self.close()
            raise

    async def reject_question(self, _):
        raise BridgeError("unsupported_approval", "Antigravity streaming input has no external approval protocol")

    async def run(self, instruction, on_started, on_event):
        if not self.client or self.client.process.returncode is not None:
            await self.close()
            await self.connect()
        before, previous_step = self.total_usage, self.last_step
        steps, tool_states = {}, {}
        first_output = accepted = denied = False
        self.client.stderr.clear()
        self.running = True
        try:
            await self.client.write({"event": "user", "message": {"content": instruction}})
            # EOF ends the process after this turn, establishing a stdout/stderr completion barrier.
            self.client.process.stdin.close()
            while True:
                event = await self.client.event() if accepted else await asyncio.wait_for(self.client.event(), 60)
                kind = event.get("event")
                if kind == "step_update":
                    step = event.get("step_update", {})
                    if step.get("conversation_id") != self.native_id:
                        raise BridgeError("session_mismatch", "Antigravity step belongs to a different conversation")
                    index = step.get("step_index")
                    if type(index) is not int or index < 0:
                        raise BridgeError("invalid_protocol", "Antigravity returned an invalid step index")
                    if index <= previous_step:
                        continue
                    self.last_step = max(self.last_step, index)
                    if not accepted:
                        await on_started(None)
                        accepted = True
                    if step.get("text_delta") and not first_output:
                        first_output = True
                        await on_event("first_output", {"source": "step_update"})
                    if step.get("step_type") == "tool":
                        phase = step.get("state")
                        identity = (index, phase)
                        info = step.get("tool_info") or {}
                        if phase in {"ACTIVE", "DONE"} and tool_states.get(identity) != info:
                            tool_states[identity] = info
                            await on_event("tool_started" if phase == "ACTIVE" else "tool_completed", {
                                "id": index, "tool": step.get("tool_name"), **info,
                                "is_error": bool(info.get("error")),
                            })
                            if info.get("error") and re.search(r"permission|approval|denied|not.allowed", str(info["error"]), re.I):
                                denied = True
                    if step.get("state") == "DONE" and (counts := counters(step.get("usage"))) is not None:
                        steps[index] = counts
                        await on_event("usage_updated", turn_usage(None, None, self.sum_steps(steps)))
                elif kind == "result":
                    result = event.get("result", {})
                    if not isinstance(result, dict) or not isinstance(result.get("response", ""), str):
                        raise BridgeError("invalid_protocol", "Antigravity returned an invalid result envelope")
                    if result.get("conversation_id") != self.native_id:
                        raise BridgeError("session_mismatch", "Antigravity result belongs to a different conversation")
                    native_status = result.get("status")
                    if native_status not in {"SUCCESS", "ERROR", "INTERRUPTED", "CANCELED"}:
                        raise BridgeError("unconfirmed_result", "Antigravity did not confirm a terminal state")
                    turns = result.get("num_turns")
                    if type(turns) is not int or turns < 0 or turns == 0 and native_status != "ERROR":
                        raise BridgeError("invalid_protocol", "Antigravity result has no confirmed user turn")
                    if self.turns is not None and turns <= self.turns:
                        continue
                    if self.turns is not None and turns != self.turns + 1:
                        raise BridgeError("invalid_protocol", "Antigravity skipped a user turn")
                    if not accepted:
                        await on_started(None)
                    after = counters(result.get("usage"))
                    usage = turn_usage(before, after, self.sum_steps(steps))
                    self.total_usage, self.turns = after, turns
                    answer = result.get("response") or ""
                    status = "interrupted" if native_status in {"INTERRUPTED", "CANCELED"} else (
                        "completed" if native_status == "SUCCESS" and answer else "failed")
                    async with asyncio.timeout(10):
                        await self.client.process.wait()
                        await asyncio.gather(self.client.reader, self.client.err_reader)
                    # A single submitted prompt must have exactly one terminal result.
                    while not self.client.events.empty():
                        trailing = self.client.events.get_nowait()
                        if trailing is not None:
                            raise BridgeError("invalid_protocol", "Unexpected events after Antigravity's terminal result")
                    denied |= self.client.denied
                    error = result.get("error")
                    if denied:
                        status, error = "failed", "Antigravity denied a tool under its native permission policy"
                    elif self.client.process.returncode != 0 and status == "completed":
                        status, error = "failed", "Antigravity exited nonzero after its terminal result"
                    output = dict(answer=answer, native_session_id=self.native_id, native_turn_id=str(turns),
                                  requested_model=self.profile.model, reported_model=self.profile.model,
                                  model_identity="cli_init_selection", terminal_reason=native_status,
                                  native_usage_total=after, permission_denied=denied)
                    if status == "failed":
                        output.update(error=safe_error(error or "Antigravity ended without a successful answer"),
                                      provider_limit=provider_limit(error or result))
                    return status, output, usage
        finally:
            self.running = False
            await self.close()

    @staticmethod
    def sum_steps(steps):
        return {k: sum(v[k] for v in steps.values()) for k in FIELDS} if steps else {}

    async def interrupt(self):
        if self.running and self.client and self.client.process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.client.process.pid, signal.SIGINT)

    async def close(self):
        if self.client:
            await self.client.close()
            self.client = None
