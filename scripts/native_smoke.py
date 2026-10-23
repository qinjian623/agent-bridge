"""Explicit, billable smoke tests. Never imported by the offline test suite."""

import argparse
import asyncio
import json
import time
from pathlib import Path

from agent_bridge.broker import request


async def wait_task(state, task_id):
    for _ in range(240):
        task = await request(state, "agent_result", {"task_id": task_id})
        if task["status"] in {"completed", "failed", "interrupted", "cancelled", "recovery_needed"}:
            return task
        if task["status"] == "waiting_input":
            raise RuntimeError(
                "Smoke task unexpectedly requests approval; inspect it rather than automatically approving"
            )
        await asyncio.sleep(1)
    raise TimeoutError(task_id)


async def one(state, profile, run):
    opened = await request(
        state,
        "agent_open",
        {"profile": profile, "cwd": str(state / "smoke-workspace"), "request_id": run + profile + ":open"},
    )
    sid = opened["session_id"]
    first = await request(
        state,
        "agent_dispatch",
        {
            "session_id": sid,
            "instruction": "Read fixture.txt. Reply only with alpha+beta as a decimal integer. Remember the word lantern for a follow-up. Do not modify files, access network or delegate.",
            "request_id": run + profile + ":first",
            "reservation": 16000,
        },
    )
    task = await wait_task(state, first["task_id"])
    result = {
        "profile": profile,
        "session_id": sid,
        "first_task": first["task_id"],
        "first_status": task["status"],
        "first_result": task["result"],
        "first_usage": task["attempts"][-1]["usage"] if task["attempts"] else None,
    }
    print(json.dumps(result, ensure_ascii=False), flush=True)
    if task["status"] == "completed":
        follow = await request(
            state,
            "agent_dispatch",
            {
                "session_id": sid,
                "instruction": "What word did I ask you to remember? Reply with that word only. Do not use tools.",
                "request_id": run + profile + ":follow",
                "reservation": 16000,
            },
        )
        second = await wait_task(state, follow["task_id"])
        result.update(
            follow_task=follow["task_id"],
            follow_status=second["status"],
            follow_result=second["result"],
            follow_usage=second["attempts"][-1]["usage"],
        )
        result["passed"] = (
            task["result"].get("answer", "").strip() == "42"
            and second["status"] == "completed"
            and "lantern" in second["result"].get("answer", "").lower()
            and task["result"].get("native_session_id") == second["result"].get("native_session_id")
        )
    else:
        result["passed"] = False
    return result


async def main(args):
    state = Path(args.state).resolve()
    run = "smoke-" + str(time.time_ns())
    results = await asyncio.gather(*(one(state, profile, run) for profile in args.profiles))
    path = state / (run + ".json")
    path.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print(
        json.dumps(
            {
                "report": str(path),
                "results": [
                    {"profile": r["profile"], "passed": r["passed"], "session_id": r["session_id"]} for r in results
                ],
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("profiles", nargs="+")
    asyncio.run(main(parser.parse_args()))
