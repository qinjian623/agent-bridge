"""Billable check: discover an unconfigured model, execute it, then close its worker."""

import argparse
import asyncio
import json
import time
from pathlib import Path

from lifecycle_smoke import poll

from agent_bridge.broker import request


async def main(args):
    state = Path(args.state).resolve()
    if state == Path.home() / ".local/state/cli-agent-bridge":
        raise ValueError("Use a separate bridge service for this check")
    tag = "discovery-" + str(time.time_ns())
    catalog = await request(state, "agent_catalog", {"profile": args.profile, "refresh": True})
    directory = catalog["discovery"][0]
    assert not directory["error"] and not directory["stale"], directory["error"]
    assert args.model in {m["model"] for m in directory["models"]}
    assert catalog["profiles"][0]["model"] != args.model, "Choose a model absent from the configured profile"
    opened = await request(
        state,
        "agent_open",
        {
            "profile": args.profile,
            "model": args.model,
            "cwd": str(state),
            "request_id": tag + ":open",
        },
    )
    sid = opened["session_id"]
    report = {"profile": args.profile, "model": args.model, "source": directory["source"], "passed": False}
    try:
        task = await request(
            state,
            "agent_dispatch",
            {
                "session_id": sid,
                "model": args.model,
                "request_id": tag + ":run",
                "instruction": "Reply with exactly DISCOVERY_OK. Do not use tools or delegate.",
                "reservation": 4000,
                "requirements": {"learn_from_result": False},
            },
        )
        result = await poll(
            state,
            "agent_result",
            {"task_id": task["task_id"]},
            lambda t: t["status"] in {"completed", "failed", "waiting_input", "recovery_needed"},
        )
        report["result"] = result
        assert result["status"] == "completed", result["status"]
        assert result["result"]["answer"].strip() == "DISCOVERY_OK", result["result"]["answer"]
        assert result["result"]["requested_model"] == args.model
        assert result["result"]["reported_model"] == args.model
        assert len(result["attempts"]) == 1
        await request(
            state,
            "agent_review",
            {
                "task_id": task["task_id"],
                "attempt_id": result["attempts"][-1]["id"],
                "decision": "accepted",
                "evidence": "Exact answer and requested/reported model IDs matched; one attempt",
                "expected_revision": result["session_revision"],
                "request_id": tag + ":review",
            },
        )
        report["passed"] = True
    except Exception as error:
        report["error_type"] = type(error).__name__
        if "task" in locals():
            report["result"] = await request(state, "agent_result", {"task_id": task["task_id"]})
        raise
    finally:
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
        report["stopped"] = await poll(
            state,
            "agent_inspect",
            {"session_id": sid},
            lambda s: s["observed"] == "stopped" and s["owned_processes"]["status"] == "stopped",
        )
        path = state / (tag + ".json")
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        path.chmod(0o600)
        print(
            json.dumps({"profile": args.profile, "model": args.model, "passed": report["passed"], "report": str(path)})
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--model", required=True)
    asyncio.run(main(parser.parse_args()))
