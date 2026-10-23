"""Billable native lifecycle checks using sessions from native_smoke.py."""

import argparse
import asyncio
import json
import time
from pathlib import Path

from agent_bridge.broker import request


async def poll(state, method, args, predicate, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = await request(state, method, args)
        if predicate(value):
            return value
        await asyncio.sleep(0.2)
    raise TimeoutError(str(value))


async def check(state, record, tag):
    sid, profile = record["session_id"], record["profile"]

    async def control(action, **extras):
        session = await request(state, "agent_inspect", {"session_id": sid})
        return await request(
            state,
            "agent_control",
            {
                "session_id": sid,
                "action": action,
                "expected_revision": session["revision"],
                "request_id": tag + profile + action,
                **extras,
            },
        )

    await control("pause")
    await poll(state, "agent_inspect", {"session_id": sid}, lambda s: s["observed"] == "stopped")
    queued = await request(
        state,
        "agent_dispatch",
        {
            "session_id": sid,
            "instruction": "List the decimal integers from 1 to 10000, one integer per line. Do not use tools.",
            "request_id": tag + profile + "long",
        },
    )
    await asyncio.sleep(0.5)
    assert (await request(state, "agent_result", {"task_id": queued["task_id"]}))["status"] == "queued"
    await control("resume")
    await poll(
        state,
        "agent_wait",
        {"task_id": queued["task_id"], "timeout": 0},
        lambda r: any(e["kind"] == "native_started" for e in r["events"]),
    )
    # Distinct request ID for the second pause, after the new revision is visible.
    session = await request(state, "agent_inspect", {"session_id": sid})
    await request(
        state,
        "agent_control",
        {
            "session_id": sid,
            "action": "pause",
            "expected_revision": session["revision"],
            "request_id": tag + profile + "interrupt",
        },
    )
    stopped = await poll(
        state, "agent_inspect", {"session_id": sid}, lambda s: s["observed"] in {"stopped", "recovery_needed"}
    )
    interrupted = await request(state, "agent_result", {"task_id": queued["task_id"]})
    if interrupted["status"] != "interrupted":
        raise AssertionError(
            {
                "profile": profile,
                "state": stopped,
                "task_status": interrupted["status"],
                "result": interrupted["result"],
            }
        )
    await control("close")
    await control("reopen")
    follow = await request(
        state,
        "agent_dispatch",
        {
            "session_id": sid,
            "instruction": "What word did I ask you to remember before the interrupted listing? Reply with that word only. Do not use tools.",
            "request_id": tag + profile + "continuity",
        },
    )
    result = await poll(
        state,
        "agent_result",
        {"task_id": follow["task_id"]},
        lambda r: r["status"] in {"completed", "failed", "recovery_needed"},
        180,
    )
    passed = (
        result["status"] == "completed"
        and "lantern" in result["result"].get("answer", "").lower()
        and result["result"].get("native_session_id") == record["first_result"].get("native_session_id")
    )
    await control("pause_drain")
    final = await poll(state, "agent_inspect", {"session_id": sid}, lambda s: s["observed"] == "stopped")
    return {
        "profile": profile,
        "passed": passed,
        "interrupted_task": queued["task_id"],
        "continuation_task": follow["task_id"],
        "native_session_id": final["native_id"],
        "final_desired": final["desired"],
        "answer": result["result"].get("answer"),
        "usage": result["attempts"][-1]["usage"],
    }


async def main(args):
    state = Path(args.state).resolve()
    records = json.loads(Path(args.report).read_text())
    tag = "lifecycle-" + str(time.time_ns())
    values = await asyncio.gather(
        *(check(state, record, tag) for record in records if record["passed"]), return_exceptions=True
    )
    results = [str(v) if isinstance(v, Exception) else v for v in values]
    path = state / (tag + ".json")
    path.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print(json.dumps({"report": str(path), "results": results}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--report", required=True)
    asyncio.run(main(parser.parse_args()))
