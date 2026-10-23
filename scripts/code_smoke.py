"""Billable isolated code edit, dependency review, and read-only checks."""

import argparse
import asyncio
import json
import subprocess
import time
from pathlib import Path

from agent_bridge.broker import request


async def wait(state, task_id):
    for _ in range(360):
        task = await request(state, "agent_result", {"task_id": task_id})
        if task["status"] in {"completed", "failed", "interrupted", "cancelled", "recovery_needed"}:
            return task
        if task["status"] == "waiting_input":
            session = await request(state, "agent_inspect", {"session_id": task["session_id"]})
            # Test code runs only in a fresh disposable Git worktree. No approvals for shell or external writes.
            for question in session["questions"]:
                print(json.dumps({"waiting_question": question["id"], "session_id": session["id"]}), flush=True)
            raise RuntimeError("An approval requires inspection; no automatic approval in this test")
        await asyncio.sleep(0.5)
    raise TimeoutError(task_id)


async def main(args):
    state = Path(args.state).resolve()
    tag = "code-" + str(time.time_ns())
    source = state / tag
    source.mkdir()
    (source / "calc.py").write_text("def add(a, b):\n    return a - b\n\ndef slug(text):\n    return text\n")
    (source / "notes.txt").write_text("committed note\n")
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "smoke fixture",
        ],
        check=True,
    )
    (source / "notes.txt").write_text("private dirty note\n")
    (source / "untracked.txt").write_text("preserve me\n")
    results = []
    for profile, function, test in [
        ("codex-sol", "add(a,b) must return a+b", "from calc import add; assert add(17,25)==42; assert add(-2,3)==1"),
        (
            "pi-deepseek",
            "slug(text) must lowercase and replace ASCII spaces with hyphens",
            "from calc import slug; assert slug('Hello World')=='hello-world'; assert slug('ABC')=='abc'",
        ),
    ]:
        opened = await request(
            state,
            "agent_open",
            {"profile": profile, "cwd": str(source), "access": "workspace-write", "request_id": tag + profile},
        )
        task = await request(
            state,
            "agent_dispatch",
            {
                "session_id": opened["session_id"],
                "instruction": f"In calc.py fix only this requirement: {function}. Do not delegate, access network, commit, or change other files. Run python3 -c {test!r}. Return the change and test result briefly.",
                "request_id": tag + profile + "task",
                "reservation": 20000,
            },
        )
        results.append({"profile": profile, "session": opened, "task_id": task["task_id"], "test": test})
    for result in results:
        task = await wait(state, result["task_id"])
        verification = subprocess.run(
            ["python3", "-c", result["test"]], cwd=result["session"]["cwd"], capture_output=True, text=True
        )
        result.update(
            status=task["status"],
            exit_code=verification.returncode,
            answer=(task["result"] or {}).get("answer"),
            usage=task["attempts"][-1]["usage"] if task["attempts"] else None,
        )
        result["passed"] = task["status"] == "completed" and verification.returncode == 0
        if result["passed"]:
            await request(
                state,
                "agent_review",
                {
                    "task_id": result["task_id"],
                    "attempt_id": task["active_attempt"],
                    "decision": "accepted",
                    "evidence": f"Independent subprocess ran {result['test']!r}; exit code 0",
                    "expected_revision": task["session_revision"],
                    "request_id": tag + result["profile"] + "review",
                },
            )
        session = await request(state, "agent_inspect", {"session_id": result["session"]["session_id"]})
        await request(
            state,
            "agent_control",
            {
                "session_id": session["id"],
                "action": "close",
                "expected_revision": session["revision"],
                "request_id": tag + result["profile"] + "close",
            },
        )
        print(json.dumps(result, ensure_ascii=False), flush=True)
    assert (source / "calc.py").read_text().startswith("def add(a, b):\n    return a - b")
    assert (source / "notes.txt").read_text() == "private dirty note\n"
    assert (source / "untracked.txt").read_text() == "preserve me\n"
    path = state / (tag + ".json")
    path.write_text(json.dumps({"results": results, "source_preserved": True}, ensure_ascii=False, indent=2))
    print(json.dumps({"report": str(path), "passed": all(r["passed"] for r in results)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    asyncio.run(main(parser.parse_args()))
