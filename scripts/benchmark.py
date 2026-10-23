"""Billable fixed-task comparison. Registers every native session before inference; sends no notifications."""

import argparse
import asyncio
import json
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path
from statistics import median

from agent_bridge.broker import request
from agent_bridge.config import Config
from agent_bridge.runtimes import CodexRuntime
from agent_bridge.store import Store
from agent_bridge.telegram_guard import register_child
from agent_bridge.worker import denial
from agent_bridge.workspace import capture_patch, worktree

CASES = {
    "label": {
        "requirement": "normalize_label(text) strips leading/trailing whitespace, lowercases Unicode letters, and replaces every run of whitespace with one hyphen. Empty/whitespace-only input gives ''.",
        "test": "from calc import normalize_label as f; assert f('  Hello \\t WORLD  ')=='hello-world'; assert f('')==''; assert f(' \\n ') == ''; assert f('Straße')=='straße'; assert f('A\\u00a0B')=='a-b'; assert f(f(' Ab C '))=='ab-c'",
    },
    "intervals": {
        "requirement": "merge_intervals(intervals) takes a list of integer (start,end) pairs, returns a sorted list of tuple pairs merging all overlapping or touching intervals, including duplicates and zero-length pairs. Reject any start>end with ValueError. Empty input gives []. Never mutate the input.",
        "test": "from calc import merge_intervals as f; a=[(5,7),(1,3),(3,5),(10,10),(1,3)]; b=a.copy(); assert f(a)==[(1,7),(10,10)]; assert a==b; assert f([])==[]; assert f([(0,0),(0,1)])==[(0,1)]; assert f([(-4,-2),(-1,0)])==[(-4,-2),(-1,0)]\ntry: f([(4,1)])\nexcept ValueError: pass\nelse: raise AssertionError('reversed interval accepted')",
    },
    "chunks": {
        "requirement": "chunk_items(items,size) consumes any iterable once and returns a list of lists of at most size elements, preserving order and a final short chunk. size must be a positive int: bool or non-int raises TypeError; size<=0 raises ValueError. Empty iterable gives [].",
        "test": "from calc import chunk_items as f; assert f(iter(range(5)),2)==[[0,1],[2,3],[4]]; assert f([],3)==[]; assert f('abcd',3)==[['a','b','c'],['d']]; assert f([1],1)==[[1]]\nfor size,error in [(0,ValueError),(-1,ValueError),(True,TypeError),(2.0,TypeError)]:\n try: f([],size)\n except error: pass\n else: raise AssertionError('invalid size accepted')",
    },
}


def fixture(path):
    path.mkdir()
    (path / "calc.py").write_text(
        "def normalize_label(text):\n    return text\n\ndef merge_intervals(intervals):\n    return intervals\n\ndef chunk_items(items, size):\n    return list(items)\n"
    )
    (path / ".gitignore").write_text("__pycache__/\n")
    for args in (
        ["init", "-q"],
        ["add", "."],
        [
            "-c",
            "user.name=benchmark",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "Fixed benchmark fixture",
        ],
    ):
        subprocess.run(["git", "-C", str(path), *args], check=True)
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()


async def no_event(*args):
    pass


async def deny(data):
    return denial(data)


async def native_turn(agent, text):
    return await asyncio.wait_for(agent.run(text, no_event, no_event), 180)


async def wait_task(state, task_id, cwd):
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        task = await request(state, "agent_result", {"task_id": task_id})
        if task["status"] in {"completed", "failed", "interrupted", "cancelled", "recovery_needed"}:
            return task
        if task["status"] == "waiting_input":
            session = await request(state, "agent_inspect", {"session_id": task["session_id"]})
            for question in session["questions"]:
                data = question["data"]
                target = Path(data.get("input", {}).get("file_path", ""))
                allowed = (
                    data.get("type") == "permission"
                    and data.get("tool") in {"Edit", "Write"}
                    and target.is_absolute()
                    and target.resolve() == Path(cwd) / "calc.py"
                )
                await request(
                    state,
                    "agent_control",
                    {
                        "session_id": session["id"],
                        "action": "respond",
                        "expected_revision": session["revision"],
                        "request_id": question["id"] + ":fixture-permission",
                        "question_id": question["id"],
                        "answer": {"decision": "allow" if allowed else "deny"},
                    },
                )
                break
        await asyncio.sleep(0.3)
    raise TimeoutError("Benchmark task did not settle")


async def close_session(state, sid):
    session = await request(state, "agent_inspect", {"session_id": sid})
    await request(
        state,
        "agent_control",
        {
            "session_id": sid,
            "action": "close",
            "expected_revision": session["revision"],
            "request_id": sid + ":benchmark-close",
        },
    )


async def run_case(state, source, config, case, mode, run, profile=None):
    tag = f"{run}-{mode}-{case}-{profile or 'auto'}"
    started = time.time()
    directory, _ = worktree(state, source, tag)
    main_profile = replace(config.profiles["codex-sol"], name="benchmark-main", model="gpt-6-astra", effort="low")
    main = CodexRuntime(
        main_profile, {"cwd": directory, "access": "workspace-write" if mode == "direct" else "read-only"}, state, deny
    )
    instruction = (
        "Fix only calc.py to satisfy this requirement: "
        + CASES[case]["requirement"]
        + " Do not delegate, access network, run shell commands, commit, or change other functions/files. Return a brief explanation. Independent tests will be run by the caller."
    )
    native_turns, child, parent_registered = [], None, False
    record = {"case": case, "mode": mode, "profile": profile, "started_at": started, "parent_id": tag}
    try:
        await main.connect()
        register_child(config.telegram_database, "codex", main.native_id)
        record["parent_native_id"] = main.native_id
        prompt = (
            instruction
            if mode == "direct"
            else "Produce a concise instruction for a child coding agent to perform the following request. Do not perform the task or use tools. Preserve every requirement:\n"
            + instruction
        )
        status, result, usage = await native_turn(main, prompt)
        native_turns.append({"status": status, "result": result, "usage": usage})
        if status != "completed":
            raise RuntimeError("Main initial turn failed")
        rollout = (
            Path(main.rollout_path)
            if main.rollout_path
            else next((Path.home() / ".codex/sessions").rglob(f"*{main.native_id}.jsonl"))
        )
        await request(
            state,
            "agent_control",
            {
                "session_id": tag,
                "action": "parent_register",
                "expected_revision": 0,
                "request_id": tag + ":parent",
                "native_session_id": main.native_id,
                "cwd": directory,
                "usage_path": str(rollout),
                "usage_since": started,
                "notify": False,
            },
        )
        parent_registered = True
        target = directory
        if mode != "direct":
            dispatched = await request(
                state,
                "agent_dispatch",
                {
                    "cwd": str(source),
                    "access": "workspace-write",
                    "parent": tag,
                    "profile": profile,
                    "instruction": result["answer"] + "\nOriginal acceptance requirements:\n" + instruction,
                    "request_id": tag + ":child",
                    "reservation": 40000,
                    "requirements": {
                        "kind": "benchmark_python",
                        "tools": ["edit"],
                        "strategy": "balanced",
                        "learn_from_result": mode == "calibration",
                    },
                },
            )
            record["task_id"], record["session_id"] = dispatched["task_id"], dispatched["session_id"]
            session = await request(state, "agent_inspect", {"session_id": record["session_id"]})
            target = session["cwd"]
            record["profile"] = session["profile"]["name"]
            child = await wait_task(state, record["task_id"], target)
        checked = subprocess.run(
            [sys.executable, "-c", CASES[case]["test"]], cwd=target, text=True, capture_output=True, timeout=10
        )
        files = capture_patch(target, state / (tag + ".patch"))
        patch_text = (state / (tag + ".patch")).read_text()
        passed = checked.returncode == 0 and (child is None or child["status"] == "completed")
        record.update(test_exit=checked.returncode, test_error=checked.stderr[-2000:], files=files)
        if mode != "direct":
            status, result, usage = await native_turn(
                main,
                "Review this change against the request. Do not use tools. Reply ACCEPT if correct, otherwise REJECT and one reason.\nRequest: "
                + instruction
                + f"\nIndependent test exit: {checked.returncode}\n"
                + checked.stderr[-1000:]
                + "\nPatch:\n"
                + patch_text,
            )
            native_turns.append({"status": status, "result": result, "usage": usage})
            passed = passed and status == "completed" and result["answer"].strip().upper().startswith("ACCEPT")
            if child["status"] == "completed":
                await request(
                    state,
                    "agent_review",
                    {
                        "task_id": child["id"],
                        "attempt_id": child["active_attempt"],
                        "decision": "accepted" if passed else "rejected",
                        "evidence": f"Fixed independent assertions exit {checked.returncode}; main review: {result['answer'][:500]}",
                        "expected_revision": child["session_revision"],
                        "request_id": tag + ":review",
                    },
                )
        record["passed"] = passed
    except Exception as error:
        record.update(passed=False, error=str(error))
    finally:
        await main.close()
        if record.get("session_id"):
            await close_session(state, record["session_id"])
        if parent_registered:
            await request(
                state,
                "agent_control",
                {
                    "session_id": tag,
                    "action": "parent_meter_stop",
                    "expected_revision": 0,
                    "request_id": tag + ":meter-stop",
                },
            )
            parent = await request(state, "agent_inspect", {"parent_id": tag})
            record["parent_usage"] = parent["usage"]
        record["main_turns"] = native_turns
        record["child_attempts"] = child["attempts"] if child else []
        usages = [record.get("parent_usage", {})] + [
            a["usage"] or {} for a in record["child_attempts"] if a["started"] is not None
        ]
        record["tokens_complete"] = all(u.get("complete") is True for u in usages)
        record["total_tokens"] = sum(u.get("total_tokens") or 0 for u in usages) if record["tokens_complete"] else None
        record["seconds"] = time.time() - started
        (state / (tag + ".json")).write_text(json.dumps(record, ensure_ascii=False, indent=2))
        print(
            json.dumps(
                {k: record.get(k) for k in ("case", "mode", "profile", "passed", "total_tokens", "seconds", "error")}
            ),
            flush=True,
        )
    return record


def summarize(records):
    result = {}
    for mode in ("direct", "explicit", "balanced"):
        rows = [r for r in records if r["mode"] == mode]
        result[mode] = {
            "cases": len(rows),
            "passed": sum(r["passed"] for r in rows),
            "median_seconds": median(r["seconds"] for r in rows),
            "max_seconds": max(r["seconds"] for r in rows),
            "total_tokens": sum(r["total_tokens"] for r in rows)
            if all(r["total_tokens"] is not None for r in rows)
            else None,
        }
    return result


async def main(args):
    state = Path(args.state).resolve()
    config = Config.load(state / "config.json")
    run = "benchmark-" + str(time.time_ns())
    source = state / (run + "-source")
    commit = fixture(source)
    records = []
    # Calibration data are distinct from the two held-out cases. No tuning after inspecting held-out results.
    for profile in ("codex-sol", "pi-deepseek", "claude-deepseek"):
        records.append(await run_case(state, source, config, "label", "calibration", run, profile))
    for case, explicit, order in (
        ("intervals", "codex-sol", ("direct", "explicit", "balanced")),
        ("chunks", "pi-deepseek", ("balanced", "direct", "explicit")),
    ):
        for mode in order:
            records.append(
                await run_case(state, source, config, case, mode, run, explicit if mode == "explicit" else None)
            )
    report = {
        "fixture_commit": commit,
        "cases": records,
        "summary": summarize(records),
        "cache_conditions": "Fresh native sessions; provider caches were neither cleared nor controlled; cached token counters retained.",
        "limits": "Two held-out examples, one run each; no claim of general savings. Tokens are not monetary cost. Native counters are not a provider bill.",
    }
    path = state / (run + "-report.json")
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps({"report": str(path), "summary": report["summary"]}), flush=True)
    assert all(
        parent["notify"] == 0 and not parent["deliveries"]
        for parent in (Store(state).get_parent(r["parent_id"]) for r in records if r.get("parent_usage"))
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    asyncio.run(main(parser.parse_args()))
