from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
from collections import deque
from pathlib import Path
from urllib.parse import urlsplit

from .config import Profile, safe_error
from .errors import BridgeError
from .quota import provider_limit
from .usage import claude_counters, claude_usage


class JsonProcess:
    """One stdout reader owns replies, events and host requests."""

    def __init__(self, argv, cwd, env, event_filter, on_question):
        self.argv, self.cwd, self.env = argv, cwd, env
        self.event_filter, self.on_question = event_filter, on_question
        self.pending, self.handlers = {}, set()
        self.events = asyncio.Queue()
        self.stderr = deque(maxlen=20)
        self.sequence = 0
        self.process = None

    async def start(self):
        self.process = await asyncio.create_subprocess_exec(
            *self.argv,
            cwd=self.cwd,
            env=self.env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
            limit=16 * 1024 * 1024,
        )
        self.reader = asyncio.create_task(self.read())
        self.err_reader = asyncio.create_task(self.read_stderr())

    async def write(self, value):
        self.process.stdin.write((json.dumps(value, ensure_ascii=False) + "\n").encode())
        await self.process.stdin.drain()

    async def request(self, value, timeout=30):
        self.sequence += 1
        key = str(self.sequence)
        future = asyncio.get_running_loop().create_future()
        self.pending[key] = future
        try:
            await self.write({**value, "id": key})
            response = await asyncio.wait_for(future, timeout)
        finally:
            self.pending.pop(key, None)
        if "error" in response or response.get("success") is False:
            error = response.get("error", "Native request failed")
            raise BridgeError("native_error", safe_error(error), provider_limit(error))
        return response.get("result", response.get("data", {}))

    async def host_request(self, event):
        response = await self.on_question(event)
        await self.write({"id": event["id"], "result": response})

    async def read(self):
        try:
            while line := await self.process.stdout.readline():
                event = json.loads(line)
                if not isinstance(event, dict):
                    continue
                key = str(event.get("id"))
                if key in self.pending and ("result" in event or "error" in event or event.get("type") == "response"):
                    future = self.pending[key]
                    if not future.done():
                        future.set_result(event)
                elif "id" in event and "method" in event:
                    task = asyncio.create_task(self.host_request(event))
                    self.handlers.add(task)
                    task.add_done_callback(self.handlers.discard)
                elif self.event_filter(event):
                    await self.events.put(event)
        except (ValueError, OSError, asyncio.LimitOverrunError) as error:
            self.stderr.append(safe_error(error))
        finally:
            error = BridgeError("connection_lost", "Agent connection ended; execution outcome may be unknown")
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(error)
            await self.events.put(None)

    async def read_stderr(self):
        while line := await self.process.stderr.readline():
            self.stderr.append(safe_error(line.decode(errors="replace").strip()))

    async def event(self):
        event = await self.events.get()
        if event is None:
            raise BridgeError(
                "connection_lost", "Agent exited without a confirmed final result: " + " ".join(self.stderr)[-1500:]
            )
        return event

    async def close(self, timeout=10):
        if not self.process:
            return
        tasks = (*self.handlers, self.reader, self.err_reader)
        try:
            async with asyncio.timeout(timeout):
                for task in tuple(self.handlers):
                    task.cancel()
                await asyncio.gather(*self.handlers, return_exceptions=True)
                self.process.stdin.close()
                try:
                    await asyncio.wait_for(self.process.wait(), timeout / 2)
                except TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(self.process.pid, signal.SIGTERM)
                    await self.process.wait()
                await asyncio.gather(self.reader, self.err_reader, return_exceptions=True)
        except TimeoutError:
            pass
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGKILL)
            # asyncio exposes no public Process.close(); descendants may still hold the pipes.
            self.process._transport.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def usage_difference(before, after):
    fields = {
        "inputTokens": "input_tokens",
        "outputTokens": "output_tokens",
        "cachedInputTokens": "cache_read_tokens",
        "cacheWriteInputTokens": "cache_write_tokens",
        "totalTokens": "total_tokens",
        "reasoningOutputTokens": "reasoning_tokens",
    }
    if before is None or not after or "totalTokens" not in after or any(after.get(src, 0) < before.get(src, 0) for src in fields):
        return {
            "scope": "unknown",
            "total_tokens": None,
            "complete": False,
            "reason": "Missing or reset native usage counter",
        }
    result = {dest: after.get(src, 0) - before.get(src, 0) for src, dest in fields.items()}
    result["scope"] = "attempt"
    result["complete"] = True
    result["input_includes_cache"] = True
    return result


class CodexRuntime:
    def __init__(self, profile: Profile, session, state: Path, on_question):
        self.profile, self.session = profile, session
        keep = {
            "item/completed",
            "item/agentMessage/delta",
            "turn/completed",
            "thread/tokenUsage/updated",
            "model/rerouted",
        }
        self.client = JsonProcess(
            [profile.command(), "app-server", "--stdio"],
            session["cwd"],
            profile.environment(),
            lambda e: e.get("method") in keep,
            on_question,
        )
        self.native_id = session.get("native_id")
        self.turn_id = None
        self.total_usage = session.get("native_usage_total") or (None if session.get("has_native_history") else {})
        self.reported_service_tier = None

    async def connect(self):
        await self.client.start()
        await self.client.request(
            {
                "method": "initialize",
                "params": {
                    "clientInfo": {"name": "cli_agent_bridge", "version": "0.1.0"},
                    "capabilities": {"experimentalApi": True},
                },
            }
        )
        await self.client.write({"method": "initialized"})
        loaded = await self.client.request({"method": "config/read", "params": {"includeLayers": False}})
        config = loaded.get("config", {})
        params = {
            "cwd": self.session["cwd"],
            "model": self.profile.model,
            "sandbox": self.session["access"],
            "approvalPolicy": "on-request",
            "config": {
                "mcp_servers": {key: {"enabled": False} for key in (config.get("mcp_servers") or {})},
                "plugins": {key: {"enabled": False} for key in (config.get("plugins") or {})},
                "features": {"multi_agent": False},
                "apps": {"_default": {"enabled": False}},
            },
        }
        if self.native_id:
            params["threadId"] = self.native_id
        if self.profile.service_tier is not None:
            params["serviceTier"] = self.profile.service_tier
        provider = "openai" if self.profile.provider == "openai-codex" else self.profile.provider
        if provider:
            params["modelProvider"] = provider
        reply = await self.client.request(
            {"method": "thread/resume" if self.native_id else "thread/start", "params": params}
        )
        actual = reply["thread"]["id"]
        self.rollout_path = reply["thread"].get("path")
        if self.native_id and actual != self.native_id:
            raise BridgeError("session_mismatch", "Codex resumed a different session")
        self.native_id = actual
        self.reported_service_tier = reply.get("serviceTier")
        expected_tier = "priority" if self.profile.service_tier == "fast" else self.profile.service_tier
        if expected_tier is not None and self.reported_service_tier != expected_tier:
            raise BridgeError("service_tier_mismatch", "Codex did not confirm the explicitly requested service tier")
        actual_provider = reply.get("modelProvider") or reply["thread"].get("modelProvider")
        if provider and actual_provider and actual_provider != provider:
            raise BridgeError("provider_mismatch", "Codex selected a different configured provider")

    async def run(self, instruction, on_started, on_event):
        # Drain metadata from connection/resume, never reuse an earlier turn's final answer.
        while not self.client.events.empty():
            event = self.client.events.get_nowait()
            if event and event.get("method") == "thread/tokenUsage/updated" and event.get("params", {}).get("threadId") == self.native_id:
                self.total_usage = event["params"].get("tokenUsage", {}).get("total", {})
        before = dict(self.total_usage) if self.total_usage is not None else None
        params = {
            "threadId": self.native_id,
            "input": [{"type": "text", "text": instruction}],
            "model": self.profile.model,
        }
        if self.profile.effort:
            params["effort"] = self.profile.effort
        if self.profile.service_tier is not None:
            params["serviceTier"] = self.profile.service_tier
        reply = await self.client.request({"method": "turn/start", "params": params})
        self.turn_id = reply["turn"]["id"]
        await on_started(self.turn_id)
        answers, actual_model = {}, self.profile.model
        first_output = False
        while True:
            event = await self.client.event()
            data, method = event.get("params", {}), event.get("method")
            if data.get("threadId") != self.native_id:
                continue
            if (
                not first_output
                and data.get("turnId") == self.turn_id
                and (
                    method == "item/agentMessage/delta"
                    and data.get("delta")
                    or method == "item/completed"
                    and data.get("item", {}).get("type") == "agentMessage"
                    and data["item"].get("text")
                )
            ):
                first_output = True
                await on_event("first_output", {"source": method})
            if method == "thread/tokenUsage/updated":
                self.total_usage = data.get("tokenUsage", {}).get("total", self.total_usage)
                await on_event("usage_updated", usage_difference(before, self.total_usage))
            elif method == "model/rerouted":
                actual_model = data.get("toModel")
                await on_event("model_changed", {"requested": self.profile.model, "reported": actual_model})
                await self.interrupt()
            elif method == "item/completed" and data.get("turnId") == self.turn_id:
                item = data.get("item", {})
                if item.get("type") == "agentMessage" and item.get("phase") in {None, "final_answer"}:
                    answers[item["id"]] = item.get("text", "")
                elif item.get("type") in {"commandExecution", "fileChange"}:
                    await on_event("tool_completed", item)
            elif method == "turn/completed" and data.get("turn", {}).get("id") == self.turn_id:
                turn = data["turn"]
                status = turn["status"]
                answer = "\n\n".join(answers.values()).strip()
                usage = (
                    usage_difference(before, self.total_usage)
                    if self.total_usage
                    else {"scope": "unknown", "total_tokens": None, "complete": False}
                )
                result = {
                    "answer": answer,
                    "requested_model": self.profile.model,
                    "reported_model": actual_model,
                    "native_session_id": self.native_id,
                    "native_turn_id": self.turn_id,
                    "native_usage_total": self.total_usage,
                    "requested_service_tier": self.profile.service_tier,
                    "reported_service_tier": self.reported_service_tier,
                }
                if actual_model != self.profile.model:
                    status, result["error"] = "failed", "Provider changed the explicitly requested model"
                elif status == "failed":
                    result["error"] = safe_error(turn.get("error"))
                    result["provider_limit"] = provider_limit(turn.get("error"))
                elif status == "completed" and not answer:
                    status, result["error"] = "failed", "Codex ended without a final answer"
                self.turn_id = None
                return status, result, usage

    async def interrupt(self):
        if self.turn_id:
            await self.client.request(
                {"method": "turn/interrupt", "params": {"threadId": self.native_id, "turnId": self.turn_id}}
            )

    async def close(self):
        await self.client.close()


class PiRuntime:
    def __init__(self, profile: Profile, session, state: Path, on_question):
        self.profile, self.session = profile, session
        self.native_id = session.get("native_id") or str(state / "pi-sessions" / (session["id"] + ".jsonl"))
        Path(self.native_id).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        argv = [
            profile.command(),
            "--mode",
            "rpc",
            "--session",
            self.native_id,
            "--provider",
            profile.provider,
            "--model",
            profile.model,
            "--no-extensions",
            "--no-mcp",
        ]
        if session["access"] == "read-only":
            argv += ["--tools", "read,grep,find,ls"]
        if profile.effort:
            argv += ["--thinking", profile.effort]
        keep = {"agent_settled", "message_end", "tool_execution_start", "tool_execution_end", "extension_ui_request"}
        self.client = JsonProcess(
            argv, session["cwd"], profile.environment(), lambda e: e.get("type") in keep, on_question
        )

    async def connect(self):
        await self.client.start()
        state = await self.client.request({"type": "get_state"})
        model = state.get("model") or {}
        if model.get("id") != self.profile.model or model.get("provider") != self.profile.provider:
            raise BridgeError("model_mismatch", "Pi did not select the requested provider and model")

    async def run(self, instruction, on_started, on_event):
        while not self.client.events.empty():
            self.client.events.get_nowait()
        before = await self.client.request({"type": "get_session_stats"})
        reply = await self.client.request({"type": "prompt", "message": instruction})
        disposition = reply.get("disposition")
        if disposition not in {None, "started"}:
            raise BridgeError("unexpected_disposition", f"Pi prompt disposition: {disposition}")
        await on_started(None)
        answer, error, aborted, model = "", None, False, self.profile.model
        consumed = 0
        first_output = False
        while True:
            event = await self.client.event()
            kind = event.get("type")
            if kind == "message_end":
                message = event.get("message", {})
                if message.get("role") == "assistant":
                    consumed += message.get("usage", {}).get("totalTokens", 0)
                    await on_event("usage_updated", {"total_tokens": consumed, "scope": "attempt"})
                    text = "\n".join(
                        item.get("text", "") for item in message.get("content", []) if item.get("type") == "text"
                    )
                    if text:
                        answer = text
                        if not first_output:
                            first_output = True
                            await on_event("first_output", {"source": "message_end"})
                    model = message.get("model", model)
                    error = message.get("errorMessage")
                    aborted = message.get("stopReason") == "aborted"
                    if message.get("stopReason") == "error" and not error:
                        error = "Pi reported an assistant error"
            elif kind in {"tool_execution_start", "tool_execution_end"}:
                await on_event(
                    "tool_started" if kind == "tool_execution_start" else "tool_completed",
                    {
                        **event,
                        "id": event.get("toolCallId"),
                        "tool": event.get("toolName"),
                        "is_error": event.get("isError"),
                    },
                )
            elif kind == "extension_ui_request":
                response = await self.client.on_question(event)
                await self.client.write({"type": "extension_ui_response", "id": event["id"], **response})
            elif kind == "agent_settled":
                statistics_error = None
                try:
                    after = await self.client.request({"type": "get_session_stats"})
                except (BridgeError, OSError, TimeoutError) as failure:
                    after = {}
                    statistics_error = safe_error(failure) or type(failure).__name__
                fields = [
                    ("input", "input_tokens"),
                    ("output", "output_tokens"),
                    ("cacheRead", "cache_read_tokens"),
                    ("cacheWrite", "cache_write_tokens"),
                    ("total", "total_tokens"),
                ]
                valid = "total" in after.get("tokens", {}) and all(
                    after.get("tokens", {}).get(src, 0) >= before.get("tokens", {}).get(src, 0) for src, _ in fields
                )
                usage = {
                    key: after.get("tokens", {}).get(src, 0) - before.get("tokens", {}).get(src, 0) if valid else None
                    for src, key in fields
                }
                cost = after["cost"] - before.get("cost", 0) if "cost" in after else None
                usage.update(
                    scope="attempt",
                    complete=valid,
                    input_includes_cache=False,
                    catalog_estimate={
                        "usd": cost if cost is not None and cost >= 0 else None,
                        "source": "pi_local_model_catalog",
                        "price_date": None,
                        "complete": valid and cost is not None and cost >= 0,
                    },
                )
                status = (
                    "interrupted"
                    if aborted or event.get("aborted")
                    else "failed"
                    if error or not answer
                    else "completed"
                )
                result = {
                    "answer": answer,
                    "error": safe_error(error) if error else None,
                    "native_session_id": self.native_id,
                    "requested_model": self.profile.model,
                    "reported_model": model,
                }
                if statistics_error:
                    result["statistics_error"] = statistics_error
                    usage.update(total_tokens=consumed or None, scope="partial_attempt", reason=statistics_error)
                if model != self.profile.model:
                    status, result["error"] = "failed", "Pi used a different model"
                if status == "failed":
                    result["provider_limit"] = provider_limit(error)
                return status, result, usage

    async def interrupt(self):
        await self.client.request({"type": "abort"})

    async def close(self):
        await self.client.close()


def claude_environment(profile):
    environment = profile.environment()
    endpoint = urlsplit(environment.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")).hostname
    expected = profile.endpoint_host or {
        "anthropic": "api.anthropic.com",
        "deepseek-anthropic": "api.deepseek.com",
    }.get(profile.provider)
    if not expected or endpoint != expected:
        raise BridgeError(
            "provider_mismatch",
            "Claude provider must declare the actual endpoint host; inherited credentials cannot silently change providers",
        )
    return environment


class ClaudeRuntime:
    def __init__(self, profile: Profile, session, state: Path, on_question):
        self.profile, self.session, self.on_question = profile, session, on_question
        self.native_id = session["native_id"]
        self.started_before = bool(session.get("has_native_history"))
        self.total_usage = claude_counters(session.get("native_usage_total")) if self.started_before else {}
        self.client = None

    async def permission(self, tool_name, input_data, context):
        from claude_agent_sdk import PermissionResultAllow, PermissionResultDeny

        answer = await self.on_question(
            {
                "type": "permission",
                "tool": tool_name,
                "input": input_data,
                "request_id": getattr(context, "tool_use_id", None),
            }
        )
        if answer.get("decision") == "allow":
            return PermissionResultAllow(updated_input=input_data)
        return PermissionResultDeny(message="Not approved", interrupt=True)

    async def connect(self):
        from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

        environment = claude_environment(self.profile)
        options = {
            "cwd": self.session["cwd"],
            "model": self.profile.model,
            "cli_path": self.profile.command(),
            "env": environment,
            "can_use_tool": self.permission,
            "permission_mode": "default",
            "setting_sources": [],
            "strict_mcp_config": True,
            "disallowed_tools": ["Agent", "Task"],
        }
        if self.session["access"] == "read-only":
            options["tools"] = ["Read", "Glob", "Grep"]
            options["mcp_servers"] = {}
        if self.profile.effort:
            options["effort"] = self.profile.effort
        if self.started_before:
            options["resume"] = self.native_id
        else:
            options["session_id"] = self.native_id
        self.client = ClaudeSDKClient(options=ClaudeAgentOptions(**options))
        await self.client.connect()

    async def run(self, instruction, on_started, on_event):
        from claude_agent_sdk import (
            AssistantMessage,
            ResultMessage,
            SystemMessage,
            TextBlock,
            ToolResultBlock,
            ToolUseBlock,
            UserMessage,
        )

        await self.client.query(instruction)
        await on_started(None)
        answer, reported_model = "", None
        first_output = False
        async for message in self.client.receive_messages():
            if isinstance(message, SystemMessage) and message.subtype == "init":
                actual = message.data.get("session_id")
                if actual and actual != self.native_id:
                    raise BridgeError("session_mismatch", "Claude started a different native session")
            elif isinstance(message, AssistantMessage) and message.parent_tool_use_id is None:
                reported_model = message.model
                for block in message.content:
                    if isinstance(block, ToolUseBlock):
                        await on_event(
                            "tool_started",
                            {
                                "id": block.id,
                                "tool": block.name,
                                "input": block.input,
                                "command": block.input.get("command"),
                            },
                        )
                text = "\n".join(block.text for block in message.content if isinstance(block, TextBlock))
                if text:
                    answer = text
                    if not first_output:
                        first_output = True
                        await on_event("first_output", {"source": "AssistantMessage"})
            elif isinstance(message, UserMessage) and isinstance(message.content, list):
                for block in message.content:
                    if isinstance(block, ToolResultBlock):
                        await on_event(
                            "tool_completed",
                            {
                                "id": block.tool_use_id,
                                "content": block.content,
                                "is_error": block.is_error,
                                "native_result": message.tool_use_result,
                            },
                        )
            elif isinstance(message, ResultMessage):
                if message.origin and message.origin.get("kind") not in {"human", "sdk"}:
                    await on_event("background_result", {"origin": message.origin, "is_error": message.is_error})
                    continue
                if message.session_id != self.native_id:
                    raise BridgeError("session_mismatch", "Claude returned a different session")
                reason = getattr(message, "terminal_reason", None)
                status = (
                    "interrupted"
                    if reason in {"aborted_streaming", "aborted_tools"}
                    else "failed"
                    if message.is_error or message.subtype != "success"
                    else "completed"
                )
                answer = message.result or answer
                normalized = claude_usage(
                    self.total_usage, message.model_usage, message.usage or {}, message.total_cost_usd
                )
                self.total_usage = claude_counters(message.model_usage, self.total_usage)
                result = {
                    "answer": answer,
                    "native_session_id": self.native_id,
                    "requested_model": self.profile.model,
                    "reported_model": reported_model,
                    "model_identity": "reported_unverified",
                    "terminal_reason": reason,
                    "native_usage_total": self.total_usage,
                }
                if status == "failed":
                    result["error"] = safe_error(message.errors or answer or reason or message.subtype)
                    result["provider_limit"] = provider_limit(
                        {"status": message.api_error_status, "error": message.errors or answer}
                    )
                elif status == "completed" and not answer:
                    status, result["error"] = "failed", "Claude ended without a final answer"
                elif reported_model and reported_model != self.profile.model:
                    status, result["error"] = (
                        "failed",
                        "Claude reported a different model; configure an exact model rather than an alias",
                    )
                self.started_before = True
                return status, result, normalized
        raise BridgeError("connection_lost", "Claude stream ended without a result")

    async def interrupt(self):
        await self.client.interrupt()

    async def close(self):
        if self.client:
            await self.client.disconnect()


def runtime(profile, session, state, on_question):
    return {"codex": CodexRuntime, "pi": PiRuntime, "claude": ClaudeRuntime}[profile.runtime](
        profile, session, state, on_question
    )
