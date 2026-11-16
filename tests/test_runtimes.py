import asyncio
import contextlib
import json
import os
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from agent_bridge.config import Profile
from agent_bridge.errors import BridgeError
from agent_bridge.runtimes import (
    ClaudeRuntime,
    CodexRuntime,
    JsonProcess,
    PiRuntime,
    claude_environment,
    usage_difference,
)


class OllamaEnvironmentTests(unittest.TestCase):
    def profile(self, **changes):
        return Profile("ollama", "claude", "model", provider="ollama-anthropic", **changes)

    def test_proxy_settings_are_inherited_or_left_unset(self):
        with patch.dict(os.environ, {"OLLAMA_API_KEY": "key"}, clear=True):
            environment = claude_environment(self.profile())
        for name in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
            self.assertNotIn(name, environment)
        self.assertEqual("key", environment["OLLAMA_API_KEY"])
        self.assertEqual("key", environment["ANTHROPIC_AUTH_TOKEN"])
        self.assertEqual("", environment["ANTHROPIC_API_KEY"])
        self.assertEqual("https://ollama.com", environment["ANTHROPIC_BASE_URL"])

    def test_empty_and_env_file_proxy_settings_are_not_replaced(self):
        with patch.dict(
            os.environ,
            {"OLLAMA_API_KEY": "key", "HTTPS_PROXY": "", "HTTP_PROXY": "", "https_proxy": "http://lower:9999"},
            clear=True,
        ):
            environment = claude_environment(self.profile())
        self.assertEqual("", environment["HTTPS_PROXY"])
        self.assertEqual("", environment["HTTP_PROXY"])
        self.assertEqual("http://lower:9999", environment["https_proxy"])
        self.assertNotIn("http_proxy", environment)

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "environment.json"
            path.write_text(json.dumps({"HTTPS_PROXY": "", "HTTP_PROXY": "http://from-file:8080"}))
            path.chmod(0o600)
            with patch.dict(
                os.environ,
                {"OLLAMA_API_KEY": "key", "HTTPS_PROXY": "http://inherited:8080", "HTTP_PROXY": ""},
                clear=True,
            ):
                environment = claude_environment(self.profile(env_file=str(path)))
        self.assertEqual("", environment["HTTPS_PROXY"])
        self.assertEqual("http://from-file:8080", environment["HTTP_PROXY"])

    def test_endpoint_restriction_and_credential_isolation_remain_intact(self):
        with patch.dict(
            os.environ,
            {"OLLAMA_API_KEY": "key", "ANTHROPIC_BASE_URL": "https://api.anthropic.com"},
            clear=True,
        ), self.assertRaisesRegex(BridgeError, "HTTPS ollama.com"):
            claude_environment(self.profile())

        with patch.dict(
            os.environ,
            {"OLLAMA_API_KEY": "key", "ANTHROPIC_AUTH_TOKEN": "official", "ANTHROPIC_API_KEY": "official"},
            clear=True,
        ):
            environment = claude_environment(self.profile())
        self.assertEqual("key", environment["ANTHROPIC_AUTH_TOKEN"])
        self.assertEqual("", environment["ANTHROPIC_API_KEY"])


class Protocol:
    def __init__(self, replies):
        self.replies = replies
        self.events = asyncio.Queue()
        self.calls = []

    async def request(self, value):
        self.calls.append(value)
        return self.replies[value.get("method", value.get("type"))]

    async def event(self):
        return await self.events.get()


class RuntimeProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def no_op(self, *args):
        pass

    async def test_rpc_timeout_bounds_write_and_never_retries(self):
        client = JsonProcess([], "/tmp", {}, lambda _: False, self.no_op)
        written = []

        async def blocked_write(value):
            written.append(value)
            await asyncio.Event().wait()

        client.write = blocked_write
        with self.assertRaisesRegex(TimeoutError, "turn/start.*during write"):
            await asyncio.wait_for(client.request({"method": "turn/start"}, timeout=.02), 1)
        self.assertEqual(1, len(written))
        self.assertEqual({}, client.pending)

    async def test_rpc_response_timeout_preserves_method_and_clears_pending(self):
        client = JsonProcess([], "/tmp", {}, lambda _: False, self.no_op)
        written = []

        async def write(value):
            written.append(value)

        client.write = write
        with self.assertRaisesRegex(TimeoutError, "prompt.*during response.*outcome is unknown"):
            await client.request({"type": "prompt"}, timeout=.02)
        self.assertEqual(1, len(written))
        self.assertEqual({}, client.pending)

    async def test_close_is_bounded_when_detached_child_keeps_output_pipes_open(self):
        with tempfile.TemporaryDirectory() as folder:
            script = Path(folder) / "native.py"
            script.write_text("import subprocess,sys\nfrom pathlib import Path\np=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],start_new_session=True)\nPath('child.pid').write_text(str(p.pid))\nsys.stdin.read()\n")
            client = JsonProcess([sys.executable, str(script)], folder, dict(os.environ), lambda _: False, self.no_op)
            await client.start()
            child = None
            try:
                for _ in range(100):
                    if (Path(folder) / "child.pid").exists():
                        break
                    await asyncio.sleep(.01)
                child = int((Path(folder) / "child.pid").read_text())
                await asyncio.wait_for(client.close(timeout=.2), 1)
                self.assertEqual(0, client.process.returncode)
                self.assertTrue(client.reader.done())
                self.assertTrue(client.err_reader.done())
            finally:
                if child:
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(child, signal.SIGKILL)
                await client.close(timeout=.2)

    async def test_recovered_codex_without_baseline_never_counts_whole_history_as_new(self):
        for resume_metadata in (False, True):
            with self.subTest(resume_metadata=resume_metadata):
                runtime = CodexRuntime(Profile("test", "codex", "fixed", executable=sys.executable),
                    {"cwd": "/tmp", "access": "read-only", "native_id": "native", "has_native_history": True}, Path("/tmp"), self.no_op)
                runtime.client = Protocol({"turn/start": {"turn": {"id": "turn"}}})
                if resume_metadata:
                    await runtime.client.events.put({"method": "thread/tokenUsage/updated", "params": {"threadId": "native", "tokenUsage": {"total": {"totalTokens": 120}}}})
                for total in (150, 180):
                    running = asyncio.create_task(runtime.run("task", self.no_op, self.no_op))
                    await asyncio.sleep(0)
                    for event in (
                        {"method": "thread/tokenUsage/updated", "params": {"threadId": "native", "tokenUsage": {"total": {"totalTokens": total}}}},
                        {"method": "item/completed", "params": {"threadId": "native", "turnId": "turn", "item": {"id": "answer", "type": "agentMessage", "text": "done"}}},
                        {"method": "turn/completed", "params": {"threadId": "native", "turn": {"id": "turn", "status": "completed"}}},
                    ):
                        await runtime.client.events.put(event)
                    status, result, usage = await running
                    self.assertEqual("completed", status)
                    if resume_metadata or total == 180:
                        self.assertEqual(30, usage["total_tokens"])
                        self.assertTrue(usage["complete"])
                    else:
                        self.assertIsNone(usage["total_tokens"])
                        self.assertFalse(usage["complete"])
                    self.assertEqual(total, result["native_usage_total"]["totalTokens"])

    async def test_pi_tool_start_and_end_pass_through_the_actual_filter(self):
        with tempfile.TemporaryDirectory() as folder:
            runtime = PiRuntime(Profile("test", "pi", "fixed", provider="provider", executable=sys.executable),
                {"id": "session", "cwd": folder, "access": "read-only"}, Path(folder), self.no_op)
            original_filter = runtime.client.event_filter
            runtime.client = Protocol({"get_session_stats": {}, "prompt": {"disposition": "started"}})
            events = []

            async def record(kind, data):
                events.append(kind)

            running = asyncio.create_task(runtime.run("task", self.no_op, record))
            await asyncio.sleep(0)
            for kind in ("tool_execution_start", "tool_execution_end", "agent_settled"):
                event = {"type": kind, "toolCallId": "tool", "toolName": "read"}
                if original_filter(event):
                    await runtime.client.events.put(event)
            await asyncio.wait_for(running, 1)
            self.assertEqual(["tool_started", "tool_completed"], events)

    async def test_codex_service_tier_is_confirmed_before_inference_and_retained(self):
        for native_id in (None, "native"):
            runtime = CodexRuntime(
                Profile("test", "codex", "fixed", service_tier="fast", executable=sys.executable),
                {"cwd": "/tmp", "access": "read-only", "native_id": native_id}, Path("/tmp"), self.no_op,
            )
            method = "thread/resume" if native_id else "thread/start"
            runtime.client = Protocol({
                "initialize": {}, "config/read": {"config": {}},
                method: {"thread": {"id": "native"}, "serviceTier": "priority"},
                "turn/start": {"turn": {"id": "turn"}},
            })
            runtime.client.start = runtime.client.write = self.no_op
            await runtime.connect()
            self.assertEqual("fast", runtime.client.calls[-1]["params"]["serviceTier"])
            running = asyncio.create_task(runtime.run("test", self.no_op, self.no_op))
            await asyncio.sleep(0)
            await runtime.client.events.put({"method": "item/completed", "params": {
                "threadId": "native", "turnId": "turn", "item": {"id": "message", "type": "agentMessage", "text": "ok"},
            }})
            await runtime.client.events.put({"method": "turn/completed", "params": {
                "threadId": "native", "turn": {"id": "turn", "status": "completed"},
            }})
            _, result, _ = await running
            self.assertEqual("fast", runtime.client.calls[-1]["params"]["serviceTier"])
            self.assertEqual("priority", result["reported_service_tier"])
            runtime.client.replies["thread/resume"] = {"thread": {"id": "native"}, "serviceTier": None}
            with self.assertRaisesRegex(BridgeError, "service tier"):
                await runtime.connect()

    def test_other_runtimes_do_not_silently_ignore_service_tier(self):
        for kind in ("claude", "pi"):
            with self.assertRaisesRegex(BridgeError, "only for Codex"):
                Profile("test", kind, "fixed", provider="provider", service_tier="fast").validate()

    async def test_claude_inherited_endpoint_cannot_change_named_provider(self):
        runtime = ClaudeRuntime(
            Profile("test", "claude", "model", provider="anthropic"),
            {"cwd": "/tmp", "native_id": "session", "access": "read-only"},
            Path("/tmp"),
            self.no_op,
        )
        with (
            patch.dict("os.environ", {"ANTHROPIC_BASE_URL": "https://api.deepseek.com/anthropic"}),
            self.assertRaisesRegex(BridgeError, "actual endpoint"),
        ):
            await runtime.connect()

    async def test_claude_missing_counters_stay_unknown_until_a_valid_interval_exists(self):
        from claude_agent_sdk import ResultMessage

        def counters(total):
            return {"inputTokens": total, "outputTokens": 0, "cacheReadInputTokens": 0, "cacheCreationInputTokens": 0}

        cases = [
            ({"fixed": counters(100)}, [{}, {"fixed": counters(150)}, {"fixed": counters(180)}], [None, None, 30]),
            ({}, [{"fixed": counters(150)}, {"fixed": counters(180)}], [None, 30]),
            ({"fixed": {"inputTokens": 100}}, [{"fixed": counters(150)}, {"fixed": counters(180)}], [None, 30]),
            ({"fixed": counters(100)}, [{"fixed": counters(5)}, {"fixed": counters(15)}], [None, 10]),
            ({"fixed": counters(100), "aux": counters(200)}, [
                {"fixed": counters(110)}, {"fixed": counters(120)},
                {"fixed": counters(150), "aux": counters(250)}, {"fixed": counters(180), "aux": counters(260)},
            ], [None, None, None, 40]),
        ]
        for baseline, sequence, expected in cases:
            with self.subTest(baseline=baseline):
                for totals, tokens in zip(sequence, expected, strict=True):
                    runtime = ClaudeRuntime(Profile("test", "claude", "fixed", provider="anthropic"),
                        {"native_id": "native", "has_native_history": True, "native_usage_total": baseline}, Path("/tmp"), self.no_op)

                    async def receive(totals=totals):
                        yield ResultMessage(subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
                            num_turns=1, session_id="native", result="done", model_usage=totals, usage={})

                    runtime.client = SimpleNamespace(query=self.no_op, receive_messages=receive)
                    status, result, usage = await runtime.run("task", self.no_op, self.no_op)
                    self.assertEqual("completed", status)
                    self.assertEqual(tokens is not None, usage["complete"])
                    self.assertEqual(tokens, usage["total_tokens"])
                    # Also exercise the persisted baseline when a worker reconnects.
                    baseline = result["native_usage_total"]

    async def test_codex_wrong_turn_cannot_finish_current_task(self):
        runtime = CodexRuntime(
            Profile("test", "codex", "fixed", executable=sys.executable),
            {"cwd": "/tmp", "access": "read-only", "native_id": "native"},
            Path("/tmp"),
            self.no_op,
        )
        runtime.client = Protocol({"turn/start": {"turn": {"id": "current"}}})
        running = asyncio.create_task(runtime.run("test", self.no_op, self.no_op))
        await asyncio.sleep(0)
        await runtime.client.events.put(
            {"method": "turn/completed", "params": {"threadId": "native", "turn": {"id": "old", "status": "completed"}}}
        )
        await asyncio.sleep(0)
        self.assertFalse(running.done())
        await runtime.client.events.put(
            {
                "method": "item/completed",
                "params": {
                    "threadId": "native",
                    "turnId": "current",
                    "item": {"type": "agentMessage", "phase": "final_answer", "id": "message", "text": "result"},
                },
            }
        )
        await runtime.client.events.put(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "native",
                    "turn": {"id": "current", "status": "failed", "error": "backend failed"},
                },
            }
        )
        status, result, _ = await running
        self.assertEqual("failed", status)
        self.assertEqual("result", result["answer"])

    async def test_codex_tool_starts_and_completions_only_use_the_current_thread_and_turn(self):
        runtime = CodexRuntime(
            Profile("test", "codex", "fixed", executable=sys.executable),
            {"cwd": "/tmp", "access": "read-only", "native_id": "native"},
            Path("/tmp"),
            self.no_op,
        )
        original_filter = runtime.client.event_filter
        self.assertTrue(original_filter({"method": "item/started"}))
        runtime.client = Protocol({"turn/start": {"turn": {"id": "current"}}})
        events = []

        async def record(kind, data):
            events.append((kind, data))

        running = asyncio.create_task(runtime.run("test", self.no_op, record))
        await asyncio.sleep(0)
        command = {"id": "command", "type": "commandExecution", "command": "pwd", "status": "inProgress"}
        change = {"id": "change", "type": "fileChange", "status": "inProgress", "changes": []}
        for event in (
            {"method": "item/started", "params": {"threadId": "other", "turnId": "current", "item": command}},
            {"method": "item/started", "params": {"threadId": "native", "turnId": "old", "item": change}},
            {
                "method": "item/started",
                "params": {
                    "threadId": "native",
                    "turnId": "current",
                    "item": {"id": "other", "type": "reasoning", "text": "ignored"},
                },
            },
            {"method": "item/started", "params": {"threadId": "native", "turnId": "current", "item": command}},
            {"method": "item/started", "params": {"threadId": "native", "turnId": "current", "item": change}},
        ):
            await runtime.client.events.put(event)
        completed = {**command, "status": "completed", "exitCode": 0, "aggregatedOutput": "ok"}
        await runtime.client.events.put(
            {"method": "item/completed", "params": {"threadId": "native", "turnId": "current", "item": completed}}
        )
        await runtime.client.events.put(
            {
                "method": "item/completed",
                "params": {
                    "threadId": "native",
                    "turnId": "current",
                    "item": {"id": "answer", "type": "agentMessage", "phase": "final_answer", "text": "done"},
                },
            }
        )
        await runtime.client.events.put(
            {
                "method": "turn/completed",
                "params": {"threadId": "native", "turn": {"id": "current", "status": "completed"}},
            }
        )
        status, result, _ = await asyncio.wait_for(running, 1)
        self.assertEqual("completed", status)
        self.assertEqual("done", result["answer"])
        self.assertEqual(
            ["tool_started", "tool_started", "tool_completed"],
            [kind for kind, _ in events if kind.startswith("tool_")],
        )
        self.assertEqual(["command", "change"], [data["id"] for kind, data in events if kind == "tool_started"])
        completed_event = [data for kind, data in events if kind == "tool_completed"][-1]
        self.assertEqual("command", completed_event["id"])
        self.assertEqual("ok", completed_event["aggregatedOutput"])

    async def test_codex_interruption_closes_only_open_started_tools(self):
        runtime = CodexRuntime(
            Profile("test", "codex", "fixed", executable=sys.executable),
            {"cwd": "/tmp", "access": "read-only", "native_id": "native"},
            Path("/tmp"),
            self.no_op,
        )
        runtime.client = Protocol({"turn/start": {"turn": {"id": "current"}}})
        events = []

        async def record(kind, data):
            events.append((kind, data))

        running = asyncio.create_task(runtime.run("test", self.no_op, record))
        await asyncio.sleep(0)
        command = {"id": "command", "type": "commandExecution", "command": "pwd", "status": "inProgress"}
        change = {"id": "change", "type": "fileChange", "status": "inProgress", "changes": []}
        for item in (command, change):
            await runtime.client.events.put(
                {"method": "item/started", "params": {"threadId": "native", "turnId": "current", "item": item}}
            )
        await runtime.client.events.put(
            {
                "method": "item/completed",
                "params": {
                    "threadId": "native",
                    "turnId": "current",
                    "item": {**command, "status": "completed"},
                },
            }
        )
        await runtime.client.events.put(
            {
                "method": "turn/completed",
                "params": {"threadId": "native", "turn": {"id": "current", "status": "interrupted"}},
            }
        )
        status, _, _ = await asyncio.wait_for(running, 1)
        self.assertEqual("interrupted", status)
        completed = [data for kind, data in events if kind == "tool_completed"]
        self.assertEqual(["command", "change"], [data["id"] for data in completed])
        self.assertEqual(["completed", "interrupted"], [data["status"] for data in completed])

    async def test_pi_prompt_ack_and_message_end_are_not_completion(self):
        with tempfile.TemporaryDirectory() as path:
            runtime = PiRuntime(
                Profile("test", "pi", "fixed", provider="provider", executable=sys.executable),
                {"id": "session", "cwd": path, "access": "read-only"},
                Path(path),
                self.no_op,
            )
            runtime.client = Protocol({"get_session_stats": {}, "prompt": {"disposition": "started"}})
            running = asyncio.create_task(runtime.run("test", self.no_op, self.no_op))
            await asyncio.sleep(0)
            await runtime.client.events.put(
                {
                    "type": "message_end",
                    "message": {
                        "role": "assistant",
                        "model": "fixed",
                        "content": [{"type": "text", "text": "partial"}],
                    },
                }
            )
            await asyncio.sleep(0)
            self.assertFalse(running.done())
            await runtime.client.events.put(
                {
                    "type": "message_end",
                    "message": {
                        "role": "assistant",
                        "model": "fixed",
                        "stopReason": "error",
                        "errorMessage": "provider rejected request",
                        "content": [],
                    },
                }
            )
            await runtime.client.events.put({"type": "agent_settled"})
            status, result, _ = await running
            self.assertEqual("failed", status)
            self.assertIn("rejected", result["error"])

    def test_usage_is_difference_and_reasoning_not_double_counted(self):
        result = usage_difference(
            {"inputTokens": 50, "outputTokens": 10, "totalTokens": 60},
            {"inputTokens": 70, "outputTokens": 30, "totalTokens": 100, "reasoningOutputTokens": 12},
        )
        self.assertEqual(40, result["total_tokens"])
        self.assertEqual(12, result["reasoning_tokens"])
