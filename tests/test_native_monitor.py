"""Tests for multi-runtime (pi, claude, agy, codex) native discovery, monitor filtering, and REST/SSE APIs."""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from typing import Any

from agent_bridge.hosts import ConfigHostRegistry
from agent_bridge.monitor import MonitorCollector, render_tui
from agent_bridge.native_monitor import (
    collect_native_sessions,
    display_runtime,
    normalize_runtime,
    summarize_runtimes,
    unified_sessions,
)
from agent_bridge.store import Store
from agent_bridge.web_monitor import WebMonitorHandler, WebSessionManager

TOKEN = "test-monitor-token-0123456789-abcdefghijklmnopqrstuvwxyz"


class _FakeSocket:
    def __init__(self, request_bytes: bytes) -> None:
        self.rfile = BytesIO(request_bytes)
        self.wfile = BytesIO()

    def makefile(self, mode: str, *args: Any) -> BytesIO:
        return self.rfile if "r" in mode else self.wfile

    def sendall(self, data: bytes) -> None:
        self.wfile.write(data)


def _parse_http(raw: bytes) -> tuple[int, dict[str, list[str]], str]:
    text = raw.decode("utf-8", "replace")
    head, _, body = text.partition("\r\n\r\n")
    lines = head.split("\r\n")
    status = int(lines[0].split(" ")[1])
    headers: dict[str, list[str]] = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        headers.setdefault(name.strip().lower(), []).append(value.strip())
    return status, headers, body


def _write_proc(
    proc_root: Path,
    pid: int,
    *,
    ppid: int = 1,
    comm: str,
    args: list[str],
    exe: str = "",
    cwd: str = "/tmp/work",
) -> None:
    pdir = proc_root / str(pid)
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / "stat").write_text(f"{pid} ({comm}) S {ppid} {pid} {pid} 0 -1 0 0 0 0 0 0 0 20 0 1 0 100\n")
    (pdir / "cmdline").write_bytes(b"\0".join(a.encode("utf-8") for a in args) + b"\0")
    (pdir / "comm").write_text(comm + "\n")
    if exe:
        os.symlink(exe, pdir / "exe")
    os.symlink(cwd, pdir / "cwd")


def _build_native_fixture(root: Path) -> tuple[Path, Path]:
    proc_root = root / "proc"
    home = root / "home"
    agy_home = home / ".gemini" / "antigravity-cli"
    codex_home = home / ".codex"
    (agy_home / "presence").mkdir(parents=True)
    (agy_home / "annotations").mkdir(parents=True)
    codex_home.mkdir(parents=True)

    conv_agy = "00000000-0000-7000-8000-000000000014"
    lock_file = agy_home / "presence" / f"{conv_agy}.lock"
    lock_file.write_text("")
    lock_ino = lock_file.stat().st_ino
    (agy_home / "annotations" / f"{conv_agy}.pbtxt").write_text('title: "AGY Live Review"\n')

    conv_codex = "00000000-0000-7000-8000-000000000001"
    (codex_home / "session_index.jsonl").write_text(
        json.dumps({"id": conv_codex, "thread_name": "Codex Native Task"}) + "\n"
    )

    # /proc/locks references lock_file inode owned by pid 2001
    (proc_root).mkdir(parents=True, exist_ok=True)
    (proc_root / "locks").write_text(
        f"1: FLOCK ADVISORY WRITE 2001 fe:01:{lock_ino} 0 EOF\n"
    )

    # 1. AGY interactive process (plus wrapper bash child that should be deduped)
    _write_proc(
        proc_root,
        2001,
        ppid=1,
        comm="agy",
        exe="/opt/agy/agy-real.old",
        cwd="/repo/agy",
        args=["agy", "--model", "claude-sonnet-5-5-medium", "--input-format", "stream-json"],
    )
    _write_proc(
        proc_root,
        2002,
        ppid=2001,
        comm="bash",
        exe="/usr/bin/bash",
        cwd="/repo/agy",
        args=["/bin/bash", "/home/user/.local/bin/agy"],
    )

    # 2. AGY headless worker
    _write_proc(
        proc_root,
        2003,
        ppid=1,
        comm="agy",
        exe="/opt/agy/agy-real",
        cwd="/repo/agy-worker",
        args=["agy", "-p", "verify module", "--conversation", "conv-agy-worker-1", "--model", "gemini-4-hydrogen-high"],
    )

    # 3. Pi standalone + headless worker
    _write_proc(
        proc_root,
        2004,
        ppid=1,
        comm="pi",
        exe="/usr/bin/node",
        cwd="/repo/pi",
        args=["node", "/usr/lib/node_modules/pi-coding-agent/dist/cli.js", "--session", "/tmp/pi-1234abcd.jsonl"],
    )
    _write_proc(
        proc_root,
        2005,
        ppid=1,
        comm="pi-ollama",
        exe="/usr/bin/node",
        cwd="/repo/pi-worker",
        args=["node", "/usr/lib/node_modules/pi-coding-agent/dist/cli.js", "--mode", "rpc", "--model", "deepseek-v4.1"],
    )

    # 4. Claude Code CLI
    _write_proc(
        proc_root,
        2006,
        ppid=1,
        comm="claude",
        exe=str(home / ".local/share/claude/versions/2.1.322"),
        cwd="/repo/claude",
        args=[
            "claude",
            "--resume",
            "claude-sess-9988",
            "--settings",
            json.dumps({"env": {"ANTHROPIC_MODEL": "claude-sonnet-5"}}),
        ],
    )

    # 5. Codex CLI + app-server daemon
    _write_proc(
        proc_root,
        2007,
        ppid=1,
        comm="codex",
        exe="/usr/bin/codex",
        cwd="/repo/codex",
        args=["codex", "resume", conv_codex],
    )
    _write_proc(
        proc_root,
        2008,
        ppid=1,
        comm="codex",
        exe="/usr/bin/codex",
        cwd="/",
        args=["codex", "app-server", "--listen", "stdio://"],
    )

    # 6. Bridge worker + child subtree that MUST be excluded as bridge-managed
    _write_proc(
        proc_root,
        3000,
        ppid=1,
        comm="python",
        exe="/usr/bin/python3",
        cwd="/repo/bridge",
        args=["python", "-m", "agent_bridge.worker"],
    )
    _write_proc(
        proc_root,
        3001,
        ppid=3000,
        comm="agy",
        exe="/opt/agy/agy-real",
        cwd="/repo/bridge",
        args=["agy", "-p", "managed task", "--conversation", "bridge-conv-1"],
    )

    return proc_root, home


class NativeAndApiMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.state = self.root / "state"
        self.state.mkdir()
        (self.state / "admin.token").write_text(TOKEN)
        (self.state / "admin.token").chmod(0o600)
        self.proc_root, self.home = _build_native_fixture(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _seed_store(self) -> None:
        store = Store(self.state)
        with store.connection(write=True) as db:
            pi_prof = json.dumps({"name": "pi-fast", "runtime": "pi", "model": "deepseek-v4.1-flash"})
            claude_prof = json.dumps({"name": "claude-sonnet", "runtime": "claude", "model": "claude-sonnet-5"})
            agy_prof = json.dumps(
                {"name": "agy-sonnet", "runtime": "antigravity", "model": "claude-sonnet-5-5-medium"}
            )
            codex_prof = json.dumps({"name": "codex-sol", "runtime": "codex", "model": "gpt-6.1-sol"})

            # Seed 22 older stopped pi sessions to ensure active/other runtimes aren't buried.
            for idx in range(22):
                sid = f"pi-old-{idx:02d}"
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (sid, pi_prof, "/repo/pi", "workspace-write", "closed", "stopped", 1, 1000.0 + idx),
                )

            # Active AGY, Claude, and Codex bridge sessions with older creation timestamps
            for sid, prof_json, obs, nid in (
                ("agy-live-1", agy_prof, "running", "bridge-conv-1"),
                ("claude-wait-1", claude_prof, "waiting_input", "claude-native-1"),
                ("codex-idle-1", codex_prof, "idle", "codex-native-1"),
            ):
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, native_id, created) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (sid, prof_json, f"/repo/{sid}", "workspace-write", "open", obs, 1, nid, 500.0),
                )

            db.execute(
                "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                ("agy-live-1", "t1", "a1", "task_finished", json.dumps({"status": "running"}), 1700000001.0),
            )
            db.execute(
                "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                ("claude-wait-1", "t2", "a2", "input_required", json.dumps({"tool": "bash"}), 1700000002.0),
            )

    def test_runtime_normalization_aliases(self) -> None:
        self.assertEqual("antigravity", normalize_runtime("agy"))
        self.assertEqual("antigravity", normalize_runtime("agy-sonnet"))
        self.assertEqual("agy", display_runtime("antigravity"))
        self.assertEqual("claude", display_runtime("claude-sonnet"))

    def test_native_discovery_covers_all_four_runtimes_and_excludes_bridge_children(self) -> None:
        items = collect_native_sessions(
            proc_root=self.proc_root,
            home=self.home,
            exclude_pids={3000},
            exclude_native_ids={("claude", "already-managed-id")},
        )
        pids = {item["pid"]: item for item in items}

        # Managed worker child 3001 and wrapper bash 2002 excluded
        self.assertNotIn(3000, pids)
        self.assertNotIn(3001, pids)
        self.assertNotIn(2002, pids)

        # AGY interactive via /proc/locks + pbtxt title
        self.assertIn(2001, pids)
        self.assertEqual("antigravity", pids[2001]["runtime"])
        self.assertEqual("agy", pids[2001]["display_runtime"])
        self.assertEqual("AGY Live Review", pids[2001]["name"])
        self.assertEqual("00000000-0000-7000-8000-000000000014", pids[2001]["native_id"])

        # AGY headless worker
        self.assertEqual("worker", pids[2003]["kind"])
        self.assertEqual("headless", pids[2003]["mode"])

        # Pi standalone & rpc worker
        self.assertEqual("pi", pids[2004]["runtime"])
        self.assertEqual("worker", pids[2005]["kind"])
        self.assertEqual("deepseek-v4.1", pids[2005]["model"])

        # Claude Code with settings model
        self.assertEqual("claude", pids[2006]["runtime"])
        self.assertEqual("claude-sess-9988", pids[2006]["native_id"])
        self.assertEqual("claude-sonnet-5", pids[2006]["model"])

        # Codex interactive + app-server daemon
        self.assertEqual("Codex Native Task", pids[2007]["name"])
        self.assertEqual("daemon", pids[2008]["kind"])

    def test_collector_priority_filtering_and_tui_rendering(self) -> None:
        self._seed_store()
        collector = MonitorCollector(
            self.state,
            host_registry=ConfigHostRegistry({}),
            proc_root=self.proc_root,
            native_home=self.home,
        )
        snap = collector.collect(limit=25)

        # Older active AGY, Claude, and Codex bridge sessions rank BEFORE 22 stopped pi sessions
        top_three_ids = [s["id"] for s in snap["sessions"][:3]]
        self.assertEqual(["claude-wait-1", "agy-live-1", "codex-idle-1"], top_three_ids)

        # Runtime breakdown includes all 4 runtimes
        self.assertEqual({"pi", "claude", "antigravity", "codex"}, set(snap["runtimes"].keys()))
        self.assertEqual(2, snap["runtimes"]["antigravity"]["native_active"])
        self.assertEqual(1, snap["runtimes"]["codex"]["native_daemons"])

        # Filtering by runtime='agy'
        agy_only = collector.collect(limit=25, runtime="agy")
        self.assertEqual(["agy-live-1"], [s["id"] for s in agy_only["sessions"]])
        self.assertTrue(all(n["runtime"] == "antigravity" for n in agy_only["native_sessions"]))
        self.assertEqual(1, len(agy_only["events"]))
        self.assertEqual("antigravity", agy_only["events"][0]["runtime"])

        # TUI renders Runtime Coverage and Native sections cleanly
        tui = render_tui(snap)
        self.assertIn("Runtimes (Pi · Claude · AGY · Codex)", tui)
        self.assertIn("Native CLI Processes & Workers", tui)
        self.assertIn("AGY Live Review", tui)
        self.assertIn("Codex Native Task", tui)

    def _dispatch(self, collector: MonitorCollector, method: str, target: str, token: str | None = TOKEN) -> tuple[int, dict[str, list[str]], str]:
        raw_req = f"{method} {target} HTTP/1.1\r\nHost: localhost:8080\r\n"
        if token is not None:
            raw_req += f"Authorization: Bearer {token}\r\n"
        raw_req += "\r\n"
        sock = _FakeSocket(raw_req.encode("utf-8"))

        class _Server:
            server_address = ("127.0.0.1", 8080)

        cls = type(
            "BoundTestHandler",
            (WebMonitorHandler,),
            {
                "collector": collector,
                "state": self.state,
                "allowed_hosts": {"localhost:8080"},
                "public_origin": None,
                "sessions": WebSessionManager(),
            },
        )
        cls(sock, ("127.0.0.1", 50000), _Server())
        return _parse_http(sock.wfile.getvalue())

    def test_web_v1_rest_and_sse_endpoints(self) -> None:
        self._seed_store()
        collector = MonitorCollector(
            self.state,
            host_registry=ConfigHostRegistry({}),
            proc_root=self.proc_root,
            native_home=self.home,
        )

        # Unauthenticated rejected
        status, _, _ = self._dispatch(collector, "GET", "/api/v1/health", token=None)
        self.assertEqual(401, status)

        # POST rejected with 405
        status, _, _ = self._dispatch(collector, "POST", "/api/v1/sessions")
        self.assertEqual(405, status)

        # Health + Runtimes + Schema
        status, _, body = self._dispatch(collector, "GET", "/api/v1/health")
        self.assertEqual(200, status)
        health = json.loads(body)
        self.assertTrue(health["ok"])
        self.assertIn("antigravity", health["runtimes"])

        status, _, body = self._dispatch(collector, "GET", "/api/v1/runtimes")
        self.assertEqual(200, status)
        rt_payload = json.loads(body)
        self.assertEqual(["pi", "claude", "antigravity", "codex"], rt_payload["monitored_runtimes"])
        self.assertEqual("antigravity", rt_payload["aliases"]["agy"])

        status, _, body = self._dispatch(collector, "GET", "/api/v1/schema")
        self.assertEqual(200, status)
        schema = json.loads(body)
        self.assertIn("GET /api/v1/stream", schema["endpoints"])

        # Unified sessions filtered by runtime=agy & status=active
        status, _, body = self._dispatch(collector, "GET", "/api/v1/sessions?runtime=agy&source=all&status=active")
        self.assertEqual(200, status)
        sess_payload = json.loads(body)
        self.assertEqual("antigravity", sess_payload["filters"]["runtime"])
        sources = {item["source"] for item in sess_payload["sessions"]}
        self.assertEqual({"bridge", "native"}, sources)
        self.assertEqual(3, sess_payload["count"])

        # Incremental events after_seq=1
        status, _, body = self._dispatch(collector, "GET", "/api/v1/events?after_seq=1")
        self.assertEqual(200, status)
        ev_payload = json.loads(body)
        self.assertEqual(1, ev_payload["count"])
        self.assertEqual(2, ev_payload["next_seq"])

        # SSE stream with once=1
        status, headers, sse_body = self._dispatch(collector, "GET", "/api/v1/stream?once=1&runtime=agy")
        self.assertEqual(200, status)
        self.assertIn("text/event-stream", headers["content-type"][0])
        self.assertIn("event: snapshot\n", sse_body)
        self.assertIn("event: event\n", sse_body)

    def test_summarize_and_unified_sessions_helpers(self) -> None:
        bridge = [
            {"id": "b1", "runtime": "claude", "observed": "running", "alive": True, "attempt": {"cost_usd": 0.42}},
            {"id": "b2", "runtime": "antigravity", "observed": "stopped", "alive": False},
        ]
        native = [
            {"id": "n1", "runtime": "antigravity", "kind": "worker", "observed": "running", "pid": 11},
            {"id": "n2", "runtime": "codex", "kind": "daemon", "observed": "running", "pid": 12},
        ]
        summary = summarize_runtimes(bridge, native)
        self.assertEqual(1, summary["claude"]["bridge_running"])
        self.assertEqual(0.42, summary["claude"]["total_cost_usd"])
        self.assertEqual(1, summary["antigravity"]["native_workers"])
        self.assertEqual(1, summary["codex"]["native_daemons"])

        rows = unified_sessions({"sessions": bridge, "native_sessions": native}, runtime="agy", status="active")
        self.assertEqual(["n1"], [r["id"] for r in rows])


if __name__ == "__main__":
    unittest.main()
