"""Billable native restart and owned background-process check in an isolated service."""

import argparse
import asyncio
import json
import subprocess
import time
from pathlib import Path

from lifecycle_smoke import poll

from agent_bridge.broker import request
from agent_bridge.store import Store
from agent_bridge.worker import process_identity


async def main(args):
    state = Path(args.state).resolve()
    command = subprocess.run(
        ["systemctl", "--user", "show", args.unit, "--property=ExecStart", "--value"],
        check=True, capture_output=True, text=True,
    ).stdout
    if str(state) not in command or state == Path.home() / ".local/state/cli-agent-bridge":
        raise ValueError("Use a separate bridge service whose ExecStart names this test state")
    store = Store(state)
    if any(s["worker_pid"] for s in store.sessions()):
        raise RuntimeError("The isolated service already has live workers; inspect them before this test")
    tag = "restart-" + str(time.time_ns())
    source = state / tag
    source.mkdir(mode=0o700)
    (source / "input.txt").write_text("42\n")
    (source / "job.py").write_text(
        "import json,subprocess,time\nfrom pathlib import Path\n"
        "with open('events.txt','a') as f: f.write('started\\n')\n"
        "time.sleep(12)\n"
        "p=subprocess.Popen(['sleep','120'],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)\n"
        "Path('background.json').write_text(json.dumps({'pid':p.pid}))\n"
        "with open('events.txt','a') as f: f.write('finished\\n')\n"
        "assert Path('input.txt').read_text().strip()=='42'\nprint('JOB_OK')\n"
    )
    opened = await request(state, "agent_open", {
        "profile": "pi-deepseek", "cwd": str(source), "access": "workspace-write",
        "workspace_mode": "copy", "snapshot_files": ["job.py", "input.txt"], "request_id": tag + ":open",
    })
    sid, directory = opened["session_id"], Path(opened["cwd"])
    report = {"session_id": sid, "passed": False}
    checked = False
    try:
        task = await request(state, "agent_dispatch", {
            "session_id": sid, "request_id": tag + ":run",
            "instruction": "Authorized local service test: run python3 job.py exactly once in this directory, wait for that command to finish, then reply exactly JOB_OK. The script intentionally leaves a sleep process; the host will verify and close it. Do not run the script twice, edit files, use network, or delegate.",
        })
        report["task_id"] = task["task_id"]
        for _ in range(300):
            if (directory / "events.txt").exists():
                break
            current = await request(state, "agent_result", {"task_id": task["task_id"]})
            if current["status"] in {"waiting_input", "failed", "recovery_needed"}:
                raise RuntimeError("Inspect native test status: " + current["status"])
            await asyncio.sleep(0.2)
        else:
            raise TimeoutError("Native command never started")
        before = await request(state, "agent_inspect", {"session_id": sid})
        report["before"] = before
        subprocess.run(["systemctl", "--user", "restart", args.unit], check=True, timeout=30)
        for _ in range(100):
            try:
                after = await request(state, "agent_inspect", {"session_id": sid})
                break
            except (ConnectionError, OSError):
                await asyncio.sleep(0.1)
        else:
            raise TimeoutError("Isolated broker did not restart")
        report["after"] = after
        assert after["worker_pid"] == before["worker_pid"] and after["worker_start"] == before["worker_start"]
        assert after["native_id"] == before["native_id"]
        result = await poll(state, "agent_result", {"task_id": task["task_id"]},
                            lambda t: t["status"] in {"completed", "failed", "recovery_needed", "waiting_input"}, 120)
        assert result["status"] == "completed", result["status"]
        assert result["result"]["answer"].strip() == "JOB_OK", result["result"]["answer"]
        assert len(result["attempts"]) == 1
        assert result["attempts"][0]["first_output"] is not None
        assert (directory / "events.txt").read_text().splitlines() == ["started", "finished"]
        background = json.loads((directory / "background.json").read_text())["pid"]
        identity = process_identity(background)
        running = await request(state, "agent_inspect", {"session_id": sid})
        report.update(result=result, background_pid=background, background_identity=identity, running=running)
        assert identity and any(p["pid"] == background for p in running["owned_processes"]["processes"])
        assert not (source / "events.txt").exists() and not (source / "background.json").exists()
        checked = True
    finally:
        session = await request(state, "agent_inspect", {"session_id": sid})
        await request(state, "agent_control", {
            "session_id": sid, "action": "close", "expected_revision": session["revision"], "request_id": tag + ":close",
        })
        stopped = await poll(state, "agent_inspect", {"session_id": sid},
                             lambda s: s["observed"] == "stopped" and s["owned_processes"]["status"] == "stopped")
        if checked:
            assert process_identity(report["background_pid"]) != report["background_identity"]
            report.update(passed=True, stopped=stopped)
        path = state / (tag + "-report.json")
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps({"report": str(path), "passed": report["passed"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--unit", required=True)
    asyncio.run(main(parser.parse_args()))
