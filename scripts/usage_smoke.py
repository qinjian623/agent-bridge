"""Billable Claude cumulative-counter check, including close and native resume."""

import argparse
import asyncio
import json
import time
from pathlib import Path

from native_smoke import wait_task

from agent_bridge.broker import request


async def main(args):
    state = Path(args.state).resolve()
    tag = "usage-" + str(time.time_ns())
    directory = state / tag
    directory.mkdir(mode=0o700)
    opened = await request(
        state, "agent_open", {"profile": "claude-deepseek", "cwd": str(directory), "request_id": tag + ":open"}
    )
    sid = opened["session_id"]
    records, previous = [], 0
    try:
        for index in range(3):
            if index == 2:
                session = await request(state, "agent_inspect", {"session_id": sid})
                await request(
                    state,
                    "agent_control",
                    {
                        "session_id": sid,
                        "action": "close",
                        "expected_revision": session["revision"],
                        "request_id": tag + ":close",
                    },
                )
                for _ in range(100):
                    session = await request(state, "agent_inspect", {"session_id": sid})
                    if session["observed"] == "stopped":
                        break
                    await asyncio.sleep(0.2)
                assert session["observed"] == "stopped", session["observed"]
                await request(
                    state,
                    "agent_control",
                    {
                        "session_id": sid,
                        "action": "reopen",
                        "expected_revision": session["revision"],
                        "request_id": tag + ":reopen",
                    },
                )
            dispatched = await request(
                state,
                "agent_dispatch",
                {
                    "session_id": sid,
                    "instruction": f"Reply only METER_{index}. Do not use tools.",
                    "request_id": tag + f":task-{index}",
                    "reservation": 20000,
                },
            )
            task = await wait_task(state, dispatched["task_id"])
            attempt = task["attempts"][-1]
            totals = (attempt["result"] or {}).get("native_usage_total") or {}
            cumulative = sum(
                sum(
                    model.get(key, 0)
                    for key in ("inputTokens", "outputTokens", "cacheReadInputTokens", "cacheCreationInputTokens")
                )
                for model in totals.values()
            )
            passed = (
                task["status"] == "completed"
                and attempt["usage"].get("complete") is True
                and cumulative - previous == attempt["usage"].get("total_tokens")
            )
            records.append(
                {
                    "session_id": sid,
                    "index": index,
                    "task_id": task["id"],
                    "passed": passed,
                    "usage": attempt["usage"],
                    "cumulative_tokens": cumulative,
                }
            )
            previous = cumulative
            assert passed, records[-1]
    finally:
        session = await request(state, "agent_inspect", {"session_id": sid})
        await request(
            state,
            "agent_control",
            {
                "session_id": sid,
                "action": "close",
                "expected_revision": session["revision"],
                "request_id": tag + ":final-close",
            },
        )
        path = state / (tag + ".json")
        path.write_text(json.dumps(records, ensure_ascii=False, indent=2))
        print(
            json.dumps(
                {
                    "report": str(path),
                    "checks": [
                        {"index": r["index"], "passed": r["passed"], "tokens": r["usage"]["total_tokens"]}
                        for r in records
                    ],
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    asyncio.run(main(parser.parse_args()))
