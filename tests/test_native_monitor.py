"""Tests for multi-runtime (pi, claude, agy, codex) native discovery, monitor filtering, and REST/SSE APIs."""
from __future__ import annotations

import asyncio
import json
import os
import re
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unicodedata
import unittest
from http.server import ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

from agent_bridge.broker import Broker
from agent_bridge.cli import main as cli_main
from agent_bridge.config import Config
from agent_bridge.hosts import ConfigHostRegistry, HostConfig
from agent_bridge.mcp_server import server as build_mcp_server
from agent_bridge.monitor import MonitorCollector, _clip_ansi_to_width, render_tui
from agent_bridge.native_monitor import (
    collect_native_sessions,
    display_runtime,
    filter_snapshot_dict,
    normalize_runtime,
    summarize_runtimes,
    unified_sessions,
)
from agent_bridge.store import Store
from agent_bridge.usage import priced_usage
from agent_bridge.web_monitor import SESSION_COOKIE, WebMonitorHandler, WebSessionManager

TOKEN = "test-monitor-token-0123456789-abcdefghijklmnopqrstuvwxyz"


class _FakeSocket:
    def __init__(self, request_bytes: bytes) -> None:
        self.rfile = BytesIO(request_bytes)
        self.wfile = BytesIO()
        self.timeout: float | None = None

    def settimeout(self, value: float | None) -> None:
        self.timeout = value

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
    lock_st = lock_file.stat()
    lock_maj = os.major(lock_st.st_dev)
    lock_min = os.minor(lock_st.st_dev)
    (agy_home / "annotations" / f"{conv_agy}.pbtxt").write_text('title: "AGY Live Review"\n')

    conv_codex = "00000000-0000-7000-8000-000000000001"
    (codex_home / "session_index.jsonl").write_text(
        json.dumps({"id": conv_codex, "thread_name": "Codex Native Task"}) + "\n"
    )

    # /proc/locks references lock_file dev+inode owned by pid 2001
    (proc_root).mkdir(parents=True, exist_ok=True)
    (proc_root / "locks").write_text(
        f"1: FLOCK ADVISORY WRITE 2001 {lock_maj:02x}:{lock_min:02x}:{lock_st.st_ino} 0 EOF\n"
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

    def _dispatch(
        self,
        collector: Any,
        method: str,
        target: str,
        token: str | None = TOKEN,
        extra_headers: dict[str, str] | None = None,
        state: Path | None = None,
    ) -> tuple[int, dict[str, list[str]], str]:
        raw_req = f"{method} {target} HTTP/1.1\r\nHost: localhost:8080\r\n"
        if token is not None:
            raw_req += f"Authorization: Bearer {token}\r\n"
        for k, v in (extra_headers or {}).items():
            raw_req += f"{k}: {v}\r\n"
        raw_req += "\r\n"
        sock = _FakeSocket(raw_req.encode("utf-8"))

        class _Server:
            server_address = ("127.0.0.1", 8080)

        cls = type(
            "BoundTestHandler",
            (WebMonitorHandler,),
            {
                "collector": collector,
                "state": state if state is not None else self.state,
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

        # Invalid runtime filter returns 400 (including on SSE BEFORE 200 headers!)
        for bad_path in (
            "/api/snapshot?runtime=bogus",
            "/api/v1/sessions?runtime=bogus",
            "/api/v1/events?runtime=bogus",
            "/api/v1/stream?runtime=bogus",
            "/api/v1/stream?once=1&after_seq=bad",
        ):
            status, _, _ = self._dispatch(collector, "GET", bad_path)
            self.assertEqual(400, status, bad_path)
        status, _, _ = self._dispatch(
            collector, "GET", "/api/v1/stream?once=1", extra_headers={"Last-Event-ID": "not-an-int"}
        )
        self.assertEqual(400, status)

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
        self.assertIn("Cookie: bridge_session=<id>", schema["auth"]["schemes"])
        self.assertEqual(["POST /api/respond", "POST /api/host/ping"], schema["auth"]["csrf_required_for"])

        # Unified sessions filtered by runtime=agy & status=active, plus limit=600 works and status=waiting works
        status, _, body = self._dispatch(collector, "GET", "/api/v1/sessions?runtime=agy&source=all&status=active")
        self.assertEqual(200, status)
        sess_payload = json.loads(body)
        self.assertEqual("antigravity", sess_payload["filters"]["runtime"])
        sources = {item["source"] for item in sess_payload["sessions"]}
        self.assertEqual({"bridge", "native"}, sources)
        self.assertEqual(3, sess_payload["count"])

        status, _, body = self._dispatch(collector, "GET", "/api/v1/sessions?status=waiting&limit=600")
        self.assertEqual(200, status)
        wait_payload = json.loads(body)
        self.assertEqual(600, wait_payload["filters"]["limit"])
        self.assertEqual("waiting_input", wait_payload["filters"]["status"])
        self.assertIn("claude-wait-1", [s["id"] for s in wait_payload["sessions"]])

        # Incremental events after_seq=1
        status, _, body = self._dispatch(collector, "GET", "/api/v1/events?after_seq=1")
        self.assertEqual(200, status)
        ev_payload = json.loads(body)
        self.assertEqual(1, ev_payload["count"])
        self.assertEqual(2, ev_payload["next_seq"])

        # Initial SSE stream without cursor MUST NOT pre-ack un-sent matching events,
        # emits matching event frame(s), and advances cursor past trailing non-matching events.
        status, headers, init_sse_body = self._dispatch(collector, "GET", "/api/v1/stream?once=1&runtime=agy")
        self.assertEqual(200, status)
        self.assertIn("text/event-stream", headers["content-type"][0])
        self.assertIn("id: 0\nevent: snapshot\n", init_sse_body)
        self.assertIn("id: 1\nevent: event\n", init_sse_body)
        self.assertIn("id: 2\nevent: cursor\n", init_sse_body)
        init_snap_frame = next(f for f in init_sse_body.split("\n\n") if "event: snapshot\n" in f)
        init_snap_data = json.loads(
            next(line[6:] for line in init_snap_frame.splitlines() if line.startswith("data: "))
        )
        self.assertEqual([], init_snap_data["events"])

        # If client disconnects right after snapshot (max_events=1), reconnect MUST still receive seq=1!
        status, _, cut_sse_body = self._dispatch(collector, "GET", "/api/v1/stream?runtime=agy&max_events=1")
        self.assertEqual(200, status)
        self.assertIn("id: 0\nevent: snapshot\n", cut_sse_body)
        self.assertNotIn("event: event\n", cut_sse_body)

        # SSE stream with once=1 & Last-Event-ID
        status, headers, sse_body = self._dispatch(
            collector, "GET", "/api/v1/stream?once=1&runtime=agy", extra_headers={"Last-Event-ID": "0"}
        )
        self.assertEqual(200, status)
        self.assertIn("text/event-stream", headers["content-type"][0])
        self.assertIn("id: 0\nevent: snapshot\n", sse_body)
        self.assertIn("id: 1\nevent: event\n", sse_body)
        self.assertIn("id: 2\nevent: cursor\n", sse_body)

    def test_summarize_and_unified_sessions_helpers(self) -> None:
        bridge = [
            {
                "id": "b1",
                "runtime": "claude",
                "observed": "running",
                "alive": True,
                "cost_usd": 0.42,
                "cost_source": "reported_session_cost",
            },
            {"id": "b2", "runtime": "antigravity", "observed": "stopped", "alive": False},
            {"id": "b3", "runtime": "codex", "observed": "stopped", "alive": False, "attempt": {"id": "a3", "cost_usd": None}},
        ]
        native = [
            {"id": "n1", "runtime": "antigravity", "kind": "worker", "observed": "running", "pid": 11},
            {"id": "n2", "runtime": "codex", "kind": "daemon", "observed": "running", "pid": 12},
        ]
        summary = summarize_runtimes(bridge, native)
        self.assertEqual(1, summary["claude"]["bridge_running"])
        self.assertEqual(0.42, summary["claude"]["total_cost_usd"])
        self.assertTrue(summary["claude"]["cost_complete"])
        self.assertTrue(summary["antigravity"]["cost_complete"])
        self.assertFalse(summary["codex"]["cost_complete"])
        self.assertEqual(1, summary["antigravity"]["native_workers"])
        self.assertEqual(1, summary["codex"]["native_daemons"])

        # Fair merge: 250 stopped bridge rows must NOT hide active native rows under limit=200
        many_stopped = [
            {"id": f"stop-{i}", "runtime": "pi", "observed": "stopped", "alive": False, "created": 9000.0 + i}
            for i in range(250)
        ]
        rows = unified_sessions({"sessions": many_stopped, "native_sessions": native}, limit=200)
        self.assertEqual(200, len(rows))
        self.assertEqual(["n1", "n2"], [r["id"] for r in rows[:2]])

        agy_active = unified_sessions({"sessions": bridge, "native_sessions": native}, runtime="agy", status="active")
        self.assertEqual(["n1"], [r["id"] for r in agy_active])

    def test_security_redaction_tui_sanitization_and_process_edge_cases(self) -> None:
        # 1. No raw "command" leaked by collect_native_sessions
        items = collect_native_sessions(proc_root=self.proc_root, home=self.home, exclude_pids={3000})
        self.assertTrue(all("command" not in item for item in items))

        # 2. AGY bash wrapper as PARENT of agy-real, Codex flag before subcommand,
        #    non-dict JSONL index line, and prompt mentioning "agent_bridge/proxy.mjs"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc_root = root / "proc"
            home = root / "home"
            (home / ".gemini" / "antigravity-cli" / "presence").mkdir(parents=True)
            (home / ".codex").mkdir(parents=True)

            lock_f = home / ".gemini" / "antigravity-cli" / "presence" / "conv-multi-1.lock"
            lock_f2 = home / ".gemini" / "antigravity-cli" / "presence" / "conv-multi-2.lock"
            lock_f.write_text("")
            lock_f2.write_text("")
            other_fs_file = root / "other_file"
            other_fs_file.write_text("")

            # Cross-FS collision test: lock_f inode in /proc/locks with wrong dev, plus fd symlink pointing elsewhere
            _write_proc(
                proc_root,
                4001,
                ppid=1,
                comm="bash",
                exe="/usr/bin/bash",
                args=["/bin/bash", "/home/user/.local/bin/agy"],
            )
            _write_proc(
                proc_root,
                4002,
                ppid=4001,
                comm="agy-real",
                exe="/opt/agy/agy-real",
                args=["agy-real", "--model", "gemini-4"],
            )
            fd_dir = proc_root / "4002" / "fd"
            fd_dir.mkdir(parents=True)
            os.symlink(str(lock_f), fd_dir / "10")
            os.symlink(str(lock_f2), fd_dir / "11")
            (proc_root / "locks").write_text(
                f"1: FLOCK ADVISORY WRITE 4002 fe:99:{lock_f.stat().st_ino} 0 EOF\n"
                f"2: FLOCK ADVISORY WRITE 4002 fe:99:{lock_f2.stat().st_ino} 0 EOF\n"
            )

            # Codex with --model flag BEFORE exec subcommand
            _write_proc(
                proc_root,
                4003,
                ppid=1,
                comm="codex",
                exe="/usr/bin/codex",
                args=["codex", "--model", "gpt-6.1", "exec", "check tests"],
            )
            # Claude user session whose prompt mentions agent_bridge proxy.mjs MUST NOT be filtered out
            _write_proc(
                proc_root,
                4004,
                ppid=1,
                comm="claude",
                exe="/usr/local/bin/claude",
                args=["claude", "-p", "review agent_bridge/proxy.mjs and server"],
            )
            # Non-dict line inside codex session_index.jsonl must not raise
            (home / ".codex" / "session_index.jsonl").write_text('["not-a-dict"]\n{"id":"x","thread_name":"ok"}\n')

            found = {p["pid"]: p for p in collect_native_sessions(proc_root=proc_root, home=home)}
            self.assertNotIn(4001, found)
            self.assertIn(4002, found)
            self.assertEqual(["conv-multi-1", "conv-multi-2"], found[4002]["native_ids"])
            self.assertEqual("worker", found[4003]["kind"])
            self.assertIn(4004, found)

        # 3. Cross-FS inode collision rejected when fd points to a file with DIFFERENT st_dev/st_ino
        with tempfile.TemporaryDirectory() as tmp2:
            root2 = Path(tmp2)
            proc2 = root2 / "proc"
            home2 = root2 / "home"
            (home2 / ".gemini" / "antigravity-cli" / "presence").mkdir(parents=True)
            lfile = home2 / ".gemini" / "antigravity-cli" / "presence" / "conv-real.lock"
            lfile.write_text("")
            other = root2 / "unrelated_same_ino_elsewhere"
            other.write_text("")
            _write_proc(proc2, 5001, ppid=1, comm="agy-real", exe="/opt/agy/agy-real", args=["agy-real"])
            fd_d = proc2 / "5001" / "fd"
            fd_d.mkdir(parents=True)
            os.symlink(str(other), fd_d / "5")
            st_l = lfile.stat()
            # Claim in /proc/locks matches lfile's inode + wrong major:minor
            (proc2 / "locks").write_text(f"1: FLOCK ADVISORY WRITE 5001 ff:ee:{st_l.st_ino} 0 EOF\n")
            res = collect_native_sessions(proc_root=proc2, home=home2)
            self.assertEqual(1, len(res))
            self.assertIsNone(res[0]["native_id"])

        # 4. TUI strips ANSI/C0 control sequences
        evil_snap = {
            "timestamp": 1700000000.0,
            "coordinator": {"state": "online", "broker_pid": 1},
            "metrics": {"active_sessions": 0, "total_sessions": 0, "native_sessions": 1, "total_cost_usd": 0.0},
            "runtimes": summarize_runtimes([], [{"id": "n1", "runtime": "claude", "kind": "interactive"}]),
            "hosts": [],
            "sessions": [],
            "native_sessions": [
                {
                    "id": "n1",
                    "pid": 99,
                    "runtime": "claude",
                    "display_runtime": "claude",
                    "kind": "interactive",
                    "observed": "active",
                    "name": "ok\x1b[31mEVIL\x07title",
                    "model": "sonnet\r\nhacked",
                }
            ],
            "events": [{"seq": 1, "created_rel": "1s ago", "kind": "note\x1b[2J", "session_name": "s\x1b]0;X\x07", "summary": "a\nb\x1bc"}],
            "broadcasts": [{"broadcast_id": "b1", "group_id": "g1", "received": 1, "total": 1, "message_preview": "hi\x1b[31m!"}],
            "pending_questions": [],
        }
        rendered = render_tui(evil_snap)
        self.assertNotIn("\x1b[31mEVIL", rendered)
        self.assertNotIn("\x07", rendered)
        self.assertNotIn("\x1b]0;X", rendered)

    def test_events_pagination_legacy_broker_and_sse_auth_revocation(self) -> None:
        self._seed_store()
        store = Store(self.state)
        with store.connection(write=True) as db:
            # Add two extra claude events then one trailing codex event (seq 3=claude, 4=claude, 5=codex)
            db.execute(
                "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                ("claude-wait-1", "t3", "a3", "tool_completed", "{}", 1700000003.0),
            )
            db.execute(
                "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                ("claude-wait-1", "t4", "a4", "task_finished", json.dumps({"status": "idle"}), 1700000004.0),
            )
            db.execute(
                "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                ("codex-idle-1", "t5", "a5", "tool_completed", "{}", 1700000005.0),
            )

        collector = MonitorCollector(
            self.state,
            host_registry=ConfigHostRegistry({}),
            proc_root=self.proc_root,
            native_home=self.home,
        )

        # Page 1: after_seq=1, runtime=claude, limit=1 -> MUST stop at seq=2 (not skip 3 & 4!)
        _, _, body1 = self._dispatch(collector, "GET", "/api/v1/events?after_seq=1&runtime=claude&limit=1")
        p1 = json.loads(body1)
        self.assertEqual([2], [e["seq"] for e in p1["events"]])
        self.assertEqual(2, p1["next_seq"])

        # Page 2: after_seq=2, runtime=claude, limit=5 -> returns [3, 4] and skips trailing non-matching 5 -> next_seq=5!
        _, _, body2 = self._dispatch(collector, "GET", "/api/v1/events?after_seq=2&runtime=claude&limit=5")
        p2 = json.loads(body2)
        self.assertEqual([3, 4], [e["seq"] for e in p2["events"]])
        self.assertEqual(5, p2["next_seq"])

        # Legacy broker fallback (returns snapshot WITHOUT runtimes/native_sessions) must still filter cleanly
        class _LegacyCollector:
            proc_root = self.proc_root
            native_home = self.home

            def collect(self, **kwargs: Any) -> dict[str, Any]:
                return {
                    "coordinator": {"state": "online"},
                    "metrics": {"active_sessions": 2, "latest_event_seq": 5},
                    "hosts": [],
                    "sessions": [
                        {"id": "c1", "runtime": "claude", "observed": "running", "alive": True},
                        {"id": "x1", "runtime": "codex", "observed": "running", "alive": True},
                    ],
                    "events": [
                        {"seq": 2, "runtime": "claude", "kind": "note"},
                        {"seq": 5, "runtime": "codex", "kind": "note"},
                    ],
                    "pending_questions": [],
                    "groups": [],
                    "broadcasts": [],
                }

        status, _, snap_body = self._dispatch(_LegacyCollector(), "GET", "/api/snapshot?runtime=claude")
        self.assertEqual(200, status)
        upgraded = json.loads(snap_body)
        self.assertTrue(upgraded.get("legacy_broker"))
        self.assertEqual(["c1"], [s["id"] for s in upgraded["sessions"]])
        self.assertTrue(all(n["runtime"] == "claude" for n in upgraded["native_sessions"]))
        self.assertEqual([2], [e["seq"] for e in upgraded["events"]])

        # Legacy broker MUST reject incremental after_seq AND continuous streaming without once=1
        for inc_path in ("/api/v1/events?after_seq=1", "/api/v1/stream?after_seq=1&once=1", "/api/v1/stream"):
            status, _, _ = self._dispatch(_LegacyCollector(), "GET", inc_path)
            self.assertEqual(501, status, inc_path)

        # SSE auth revocation during initial collect -> rejected 401 before sending 200 or any snapshot!
        token_file = self.state / "admin.token"
        poll_count = 0

        class _RotatingCollector:
            proc_root = self.proc_root
            native_home = self.home

            def collect(self, **kwargs: Any) -> dict[str, Any]:
                nonlocal poll_count
                poll_count += 1
                if poll_count == 1:
                    token_file.write_text("rotated-new-token-" + TOKEN)
                return collector.collect(**kwargs)

        status, _, body = self._dispatch(
            _RotatingCollector(), "GET", "/api/v1/stream?interval=0.2&max_seconds=5&max_events=10"
        )
        self.assertEqual(401, status)
        self.assertNotIn("event: snapshot", body)

    def test_round2_sse_reconnect_live_revocation_costs_multilock_and_tui_width(self) -> None:
        self._seed_store()
        store = Store(self.state)
        with store.connection(write=True) as db:
            # 1. Seed sparse claude event (seq=1) buried behind 620 codex events, plus catalog_estimate & cumulative attempts
            db.execute(
                "INSERT INTO tasks (id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-cost-1', 'claude-wait-1', 'first', '[]', 'completed', 'approved', 0, 100.0)"
            )
            db.execute(
                "INSERT INTO attempts (id, task_id, session_id, status, usage, started, finished) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    "att-1",
                    "t-cost-1",
                    "claude-wait-1",
                    "completed",
                    json.dumps({"complete": True, "catalog_estimate": {"usd": 0.15, "complete": True}}),
                    101.0,
                    105.0,
                ),
            )
            db.execute(
                "INSERT INTO tasks (id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-cost-2', 'claude-wait-1', 'second', '[]', 'running', 'pending', 0, 200.0)"
            )
            for i in range(620):
                db.execute(
                    "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                    ("codex-idle-1", "tx", "ax", "tool_completed", "{}", 1700000100.0 + i),
                )

        collector = MonitorCollector(
            self.state,
            host_registry=ConfigHostRegistry({}),
            proc_root=self.proc_root,
            native_home=self.home,
        )

        # Cumulative catalog_estimate cost present AND marked incomplete due to inflight 2nd task;
        # plus sparse claude event STILL found despite 620 newer codex events!
        claude_snap = collector.collect(runtime="claude", limit=10)
        c_sess = claude_snap["sessions"][0]
        self.assertEqual(0.15, c_sess["cost_usd"])
        self.assertFalse(c_sess["cost_complete"])
        self.assertTrue(len(claude_snap["events"]) >= 1)
        self.assertEqual("claude", claude_snap["events"][0]["runtime"])

        # Reconnecting browser EventSource with ?after_seq=0 and Last-Event-ID=10 must NOT replay seq 1..10
        status, _, sse_replay = self._dispatch(
            collector,
            "GET",
            "/api/v1/stream?once=1&after_seq=0&runtime=claude",
            extra_headers={"Last-Event-ID": "10"},
        )
        self.assertEqual(200, status)
        self.assertIn("event: snapshot\n", sse_replay)
        self.assertNotIn("event: event\n", sse_replay)

        # Pagination metadata on /api/v1/sessions (including session_limit & host_id before slicing)
        status, _, p_body = self._dispatch(collector, "GET", "/api/v1/sessions?offset=1&limit=2")
        self.assertEqual(200, status)
        pag = json.loads(p_body)
        self.assertEqual(1, pag["offset"])
        self.assertTrue(pag["truncated"])
        self.assertGreater(pag["total"], 3)

        status, _, sl_body = self._dispatch(collector, "GET", "/api/v1/sessions?session_limit=1&offset=1")
        self.assertEqual(200, status)
        sl_pag = json.loads(sl_body)
        self.assertEqual(1, sl_pag["count"])
        self.assertGreater(sl_pag["total"], 3)
        self.assertTrue(sl_pag["truncated"])
        self.assertEqual(pag["sessions"][0]["id"], sl_pag["sessions"][0]["id"])

        status, _, hl_body = self._dispatch(collector, "GET", "/api/v1/sessions?host_id=local&session_limit=1")
        self.assertEqual(200, status)
        hl_pag = json.loads(hl_body)
        self.assertEqual(1, hl_pag["count"])
        self.assertEqual("local", hl_pag["sessions"][0]["host_id"])
        self.assertEqual(pag["total"], hl_pag["total"])

        # Real socket + ThreadingHTTPServer: revoke session/token WHILE poll #2 is blocked inside collect()
        poll2_entered = threading.Event()
        poll2_release = threading.Event()
        poll_n = 0

        class _BlockingCollector:
            proc_root = self.proc_root
            native_home = self.home

            def collect(self, **kwargs: Any) -> dict[str, Any]:
                nonlocal poll_n
                poll_n += 1
                cur_n = poll_n
                if cur_n == 2:
                    poll2_entered.set()
                    poll2_release.wait(timeout=3.0)
                return collector.collect(**kwargs)

        handler_cls = type(
            "LiveSSETestHandler",
            (WebMonitorHandler,),
            {
                "collector": _BlockingCollector(),
                "state": self.state,
                "allowed_hosts": {"127.0.0.1"},
                "public_origin": None,
                "sessions": WebSessionManager(),
            },
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        t.start()
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=5.0) as sock:
                sock.sendall(
                    (
                        f"GET /api/v1/stream?interval=0.2&max_seconds=4 HTTP/1.1\r\n"
                        f"Host: 127.0.0.1\r\n"
                        f"Authorization: Bearer {TOKEN}\r\n\r\n"
                    ).encode()
                )
                self.assertTrue(poll2_entered.wait(timeout=3.0))
                # Rotate token while handler is paused inside poll #2 collect()
                (self.state / "admin.token").write_text("revoked-" + TOKEN)
                poll2_release.set()
                chunks: list[bytes] = []
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
            wire = b"".join(chunks).decode("utf-8", "replace")
            # Only the 1 initial snapshot was sent; poll #2's snapshot MUST be suppressed!
            self.assertEqual(1, wire.count("event: snapshot\n"))
        finally:
            poll2_release.set()
            server.shutdown()
            server.server_close()

        # SSE auth revoked during digest computation (after post-collect check, right before write_sse('snapshot'))
        (self.state / "admin.token").write_text(TOKEN)

        class _RevokeBeforeSnapshotSessions(WebSessionManager):
            def __init__(self) -> None:
                super().__init__()
                self.calls = 0

            def validate_session(self, session_id: str, current_token: str) -> dict[str, Any] | None:
                self.calls += 1
                if self.calls >= 5:
                    return None
                return super().validate_session(session_id, current_token)

        revoking_sessions = _RevokeBeforeSnapshotSessions()
        cookie_sid, _ = revoking_sessions.create_session(TOKEN)
        snap_revoke_handler = type(
            "SnapRevokeSSEHandler",
            (WebMonitorHandler,),
            {
                "collector": collector,
                "state": self.state,
                "allowed_hosts": {"127.0.0.1"},
                "public_origin": None,
                "sessions": revoking_sessions,
            },
        )
        snap_srv = ThreadingHTTPServer(("127.0.0.1", 0), snap_revoke_handler)
        snap_port = snap_srv.server_address[1]
        snap_thread = threading.Thread(target=snap_srv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        snap_thread.start()
        try:
            with socket.create_connection(("127.0.0.1", snap_port), timeout=5.0) as sock:
                sock.sendall(
                    (
                        "GET /api/v1/stream?interval=0.2&max_seconds=2 HTTP/1.1\r\n"
                        "Host: 127.0.0.1\r\n"
                        f"Cookie: {SESSION_COOKIE}={cookie_sid}\r\n\r\n"
                    ).encode()
                )
                chunks2: list[bytes] = []
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    chunks2.append(chunk)
            wire2 = b"".join(chunks2).decode("utf-8", "replace")
            self.assertEqual(5, revoking_sessions.calls)
            self.assertEqual(0, wire2.count("event: snapshot\n"))
        finally:
            snap_srv.shutdown()
            snap_srv.server_close()

        # Mixed multi-lock WITH exclude_pids={7700, 7701}: must STILL keep unmanaged native locks
        # across excluded pid, excluded parent's child, suppressed AGY wrapper, and non-AGY (claude) CLI lock holders.
        with tempfile.TemporaryDirectory() as tmp_mix:
            r_m = Path(tmp_mix)
            p_m = r_m / "proc"
            h_m = r_m / "home"
            (h_m / ".gemini" / "antigravity-cli" / "presence").mkdir(parents=True)
            l_bridge = h_m / ".gemini" / "antigravity-cli" / "presence" / "bridge-managed-1.lock"
            l_native = h_m / ".gemini" / "antigravity-cli" / "presence" / "native-free-2.lock"
            l_wrap = h_m / ".gemini" / "antigravity-cli" / "presence" / "native-wrap-3.lock"
            l_claude = h_m / ".gemini" / "antigravity-cli" / "presence" / "native-claude-lock-4.lock"
            for lf in (l_bridge, l_native, l_wrap, l_claude):
                lf.write_text("")
            # 7700 (excluded parent) -> 7701 (agy-real) holding bridge-managed-1 + native-free-2
            _write_proc(p_m, 7700, ppid=1, comm="python3", exe="/usr/bin/python3", args=["python3", "-m", "agent_bridge", "worker"])
            _write_proc(p_m, 7701, ppid=7700, comm="agy-real", exe="/opt/agy/agy-real", args=["agy-real"])
            # 7702 suppressed wrapper owning native-free-2 (via inherited fd, must NOT duplicate 7701)
            # AND 7703 suppressed wrapper owning native-wrap-3 with NO classified AGY child (must be surfaced via fallback!)
            # AND 7705 classified as claude while ALSO holding unmanaged AGY lock native-claude-lock-4!
            _write_proc(p_m, 7702, ppid=7701, comm="bash", exe="/bin/bash", args=["bash", "/usr/bin/agy"])
            _write_proc(p_m, 7703, ppid=1, comm="bash", exe="/bin/bash", args=["bash", "/usr/bin/agy"])
            _write_proc(p_m, 7704, ppid=7703, comm="agy-real", exe="/opt/agy/agy-real", args=["agy-real"])
            _write_proc(p_m, 7705, ppid=1, comm="claude", exe="/usr/local/bin/claude", args=["claude"])
            for proc_pid, targets in (
                (7701, [l_bridge, l_native]),
                (7702, [l_native]),
                (7703, [l_wrap]),
                (7705, [l_claude]),
            ):
                fd_d = p_m / str(proc_pid) / "fd"
                fd_d.mkdir(parents=True)
                for idx, tgt in enumerate(targets, start=3):
                    os.symlink(str(tgt), fd_d / str(idx))
            st_b, st_n, st_w, st_c = l_bridge.stat(), l_native.stat(), l_wrap.stat(), l_claude.stat()
            (p_m / "locks").write_text(
                f"1: FLOCK ADVISORY WRITE 7701 {os.major(st_b.st_dev):02x}:{os.minor(st_b.st_dev):02x}:{st_b.st_ino} 0 EOF\n"
                f"2: FLOCK ADVISORY WRITE 7701 {os.major(st_n.st_dev):02x}:{os.minor(st_n.st_dev):02x}:{st_n.st_ino} 0 EOF\n"
                f"3: FLOCK ADVISORY WRITE 7702 {os.major(st_n.st_dev):02x}:{os.minor(st_n.st_dev):02x}:{st_n.st_ino} 0 EOF\n"
                f"4: FLOCK ADVISORY WRITE 7703 {os.major(st_w.st_dev):02x}:{os.minor(st_w.st_dev):02x}:{st_w.st_ino} 0 EOF\n"
                f"5: FLOCK ADVISORY WRITE 7705 {os.major(st_c.st_dev):02x}:{os.minor(st_c.st_dev):02x}:{st_c.st_ino} 0 EOF\n"
            )
            mixed = collect_native_sessions(
                proc_root=p_m,
                home=h_m,
                exclude_pids={7700, 7701},
                exclude_native_ids={("antigravity", "bridge-managed-1")},
            )
            found_nids = sorted(cid for item in mixed for cid in item.get("native_ids", []))
            self.assertEqual(["native-claude-lock-4", "native-free-2", "native-wrap-3"], found_nids)
            self.assertIn("native:claude:7705", {item["id"] for item in mixed})
            self.assertIn("native:antigravity:7705", {item["id"] for item in mixed})

        # Zero-event SSE disconnect + multi-page reconnect MUST NOT skip events (> limit) or replay prior history
        with tempfile.TemporaryDirectory() as sse_tmp:
            sse_state = Path(sse_tmp)
            (sse_state / "admin.token").write_text(TOKEN)
            (sse_state / "admin.token").chmod(0o600)
            sse_store = Store(sse_state)
            with sse_store.connection(write=True) as db:
                prof_s = json.dumps({"name": "claude-main", "runtime": "claude", "model": "claude-sonnet-5"})
                prof_x = json.dumps({"name": "codex-main", "runtime": "codex", "model": "gpt-5"})
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) VALUES "
                    "('sc', ?, '/repo', 'read-only', 'open', 'idle', 1, 1.0), "
                    "('sx', ?, '/repo', 'read-only', 'open', 'idle', 1, 2.0)",
                    (prof_s, prof_x),
                )
            sse_coll = MonitorCollector(sse_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home)
            # Case A: empty DB initial connect -> snapshot gets id: 0
            st_a, _, wire_a = self._dispatch(sse_coll, "GET", "/api/v1/stream?once=1&limit=5", state=sse_state)
            self.assertEqual(200, st_a)
            self.assertIn("id: 0\nevent: snapshot\n", wire_a)
            # While disconnected, 12 events (> limit=5) occur; reconnect with Last-Event-ID: 0 MUST yield ALL 1..12
            with sse_store.connection(write=True) as db:
                for i in range(1, 13):
                    db.execute(
                        "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                        ("sx", f"t{i}", f"a{i}", "tool_completed", "{}", 100.0 + i),
                    )
            st_a2, _, wire_a2 = self._dispatch(
                sse_coll,
                "GET",
                "/api/v1/stream?limit=5&max_seconds=2&interval=0.05&max_events=20",
                extra_headers={"Last-Event-ID": "0"},
                state=sse_state,
            )
            self.assertEqual(200, st_a2)
            recv_seqs = [
                int(json.loads(line[6:])["seq"])
                for frame in wire_a2.split("\n\n")
                if "event: event\n" in frame
                for line in frame.splitlines()
                if line.startswith("data: ")
            ]
            self.assertEqual(list(range(1, 13)), recv_seqs)
            # Case B: DB now at seq=12 (all codex), runtime=claude zero-event connect -> snapshot gets id: 12!
            st_b, _, wire_b = self._dispatch(sse_coll, "GET", "/api/v1/stream?once=1&runtime=claude&limit=5", state=sse_state)
            self.assertEqual(200, st_b)
            self.assertIn("id: 12\nevent: snapshot\n", wire_b)
            with sse_store.connection(write=True) as db:
                for i in range(13, 25):
                    db.execute(
                        "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                        ("sc", f"tc{i}", f"ac{i}", "tool_completed", "{}", 200.0 + i),
                    )
            st_b2, _, wire_b2 = self._dispatch(
                sse_coll,
                "GET",
                "/api/v1/stream?runtime=claude&limit=5&max_seconds=2&interval=0.05&max_events=20",
                extra_headers={"Last-Event-ID": "12"},
                state=sse_state,
            )
            self.assertEqual(200, st_b2)
            recv_b = [
                int(json.loads(line[6:])["seq"])
                for frame in wire_b2.split("\n\n")
                if "event: event\n" in frame
                for line in frame.splitlines()
                if line.startswith("data: ")
            ]
            self.assertEqual(list(range(13, 25)), recv_b)
            # Case C: include_events=false rejected cleanly on /api/v1/stream so cursor never advances blindly
            st_inc, _, _ = self._dispatch(
                sse_coll, "GET", "/api/v1/stream?include_events=false&once=1", state=sse_state
            )
            self.assertEqual(400, st_inc)
            # Case C2: initial un-cursored stream returns snapshot + latest tail `limit` window,
            # while explicit after_seq=0 replays ascending from seq 1 without skipping history.
            st_tail, _, wire_tail = self._dispatch(
                sse_coll, "GET", "/api/v1/stream?once=1&limit=5", state=sse_state
            )
            self.assertEqual(200, st_tail)
            self.assertIn("id: 19\nevent: snapshot\n", wire_tail)
            tail_seqs = [
                int(json.loads(line[6:])["seq"])
                for frame in wire_tail.split("\n\n")
                if "event: event\n" in frame
                for line in frame.splitlines()
                if line.startswith("data: ")
            ]
            self.assertEqual([20, 21, 22, 23, 24], tail_seqs)
            st_replay, _, wire_replay = self._dispatch(
                sse_coll, "GET", "/api/v1/stream?once=1&after_seq=0&limit=5", state=sse_state
            )
            self.assertEqual(200, st_replay)
            self.assertIn("id: 0\nevent: snapshot\n", wire_replay)
            replay_seqs = [
                int(json.loads(line[6:])["seq"])
                for frame in wire_replay.split("\n\n")
                if "event: event\n" in frame
                for line in frame.splitlines()
                if line.startswith("data: ")
            ]
            self.assertEqual([1, 2, 3, 4, 5], replay_seqs)
            # Case D: with existing cursor (Last-Event-ID: 12), if only non-matching (codex) events arrive
            # while digest is unchanged across Poll 1 -> Poll 2, server MUST push updated cursor ID to client.
            with sse_store.connection(write=True) as db:
                for i in range(25, 28):
                    db.execute(
                        "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                        ("sx", f"t{i}", f"a{i}", "tool_completed", "{}", 250.0 + i),
                    )
            poll_c = 0

            class _NonMatchingCursorCollector:
                proc_root = self.proc_root
                native_home = self.home

                def collect(self, **kwargs: Any) -> dict[str, Any]:
                    nonlocal poll_c
                    poll_c += 1
                    if poll_c == 2:
                        with sse_store.connection(write=True) as db:
                            for i in range(28, 31):
                                db.execute(
                                    "INSERT INTO events (session_id, task_id, attempt_id, kind, data, created) VALUES (?, ?, ?, ?, ?, ?)",
                                    ("sx", f"t{i}", f"a{i}", "tool_completed", "{}", 300.0 + i),
                                )
                    res = sse_coll.collect(**kwargs)
                    res["timestamp"] = 1700000999.0
                    for sess_row in res.get("sessions", []):
                        sess_row["created_rel"] = f"{poll_c}s ago"
                    return res

            st_d, _, wire_d = self._dispatch(
                _NonMatchingCursorCollector(),
                "GET",
                "/api/v1/stream?runtime=claude&limit=5&interval=0.2&max_seconds=0.5&max_events=3",
                extra_headers={"Last-Event-ID": "24"},
                state=sse_state,
            )
            self.assertEqual(200, st_d)
            self.assertIn("id: 27\nevent: snapshot\n", wire_d)
            self.assertIn("id: 30\nevent: cursor\n", wire_d)
            self.assertEqual(1, wire_d.count("event: snapshot\n"))

            # Case E: broadcast/hosts/groups/peer_queues state changes MUST trigger new snapshot in SSE digest
            b_poll = 0

            class _BroadcastChangeCollector:
                proc_root = self.proc_root
                native_home = self.home

                def collect(self, **kwargs: Any) -> dict[str, Any]:
                    nonlocal b_poll
                    b_poll += 1
                    res = sse_coll.collect(**kwargs)
                    res["broadcasts"] = [
                        {
                            "broadcast_id": "bc-1",
                            "group_id": "grp-1",
                            "message_preview": "ping",
                            "total": 2,
                            "pending": 1 if b_poll == 1 else 0,
                            "sending": 0,
                            "received": 1 if b_poll == 1 else 2,
                            "uncertain": 0,
                            "failed": 0,
                            "created": 100.0,
                            "created_rel": f"{b_poll}s ago",
                        }
                    ]
                    return res

            st_e, _, wire_e = self._dispatch(
                _BroadcastChangeCollector(),
                "GET",
                "/api/v1/stream?runtime=claude&limit=5&interval=0.2&max_seconds=0.5&max_events=3",
                extra_headers={"Last-Event-ID": "30"},
                state=sse_state,
            )
            self.assertEqual(200, st_e)
            self.assertEqual(2, wire_e.count("event: snapshot\n"))

        # TUI 80-col width check with REAL 36-char UUIDs, variation selector, and independent width check
        ansi_sgr = re.compile(r"\x1b\[[0-9;]*m")

        def terminal_cells(text: str) -> int:
            # Independent column counter: VS16 (0xFE0F) promotes preceding narrow char to 2 cols (e.g. "❤️" == 2 cols).
            plain = ansi_sgr.sub("", text)
            cols = 0
            prev_base = 0
            for ch in plain:
                cp = ord(ch)
                if cp == 0xFE0F:
                    if prev_base == 1:
                        cols += 1
                        prev_base = 2
                    continue
                if 0xFE00 <= cp <= 0xFE0E or 0xE0100 <= cp <= 0xE01EF or cp == 0x200D or unicodedata.combining(ch):
                    continue
                w = 2 if (0x4E00 <= cp <= 0x9FFF or 0x3000 <= cp <= 0x303F or 0xFF00 <= cp <= 0xFFEF or 0x1F300 <= cp <= 0x1FAFF) else 1
                cols += w
                prev_base = w
            return cols

        real_sid = "00000000-0000-7000-8000-000000000005"
        real_qid = "00000000-0000-7000-8000-000000000013"
        tui_snap = {
            "timestamp": 1700000000.0,
            "coordinator": {"host_id": "主控节点非常长名字", "node_id": "abcdef1234567890", "db_size": 10240},
            "metrics": {"active_sessions": 1, "total_sessions": 1, "running_tasks": 0, "waiting_questions": 1, "total_cost_usd": 0.15, "cost_complete": False},
            "runtimes": collector.collect(limit=1)["runtimes"],
            "hosts": [
                {
                    "host_id": "remote-worker-node-very-long",
                    "state": "online",
                    "ssh_target": "user@very-long-hostname.example.com",
                    "node_id": "fedcba9876543210",
                    "last_contact_rel": "5s ago",
                }
            ],
            "sessions": [
                {
                    "id": real_sid,
                    "name": "中文代理测试名称超长截取测试",
                    "host_id": "remote-worker",
                    "runtime": "claude",
                    "display_runtime": "claude",
                    "model": "claude-sonnet-5",
                    "observed": "waiting_input",
                    "revision": 3,
                    "waiting_question": True,
                    "cost_usd": 0.15,
                    "cost_complete": False,
                    "has_attempts": True,
                }
            ],
            "native_sessions": [
                {
                    "id": "native-cjk-1",
                    "pid": 99999,
                    "runtime": "antigravity",
                    "display_runtime": "antigravity (agy)",
                    "kind": "interactive",
                    "mode": "interactive",
                    "observed": "active",
                    "name": "❤️" * 20 + "测试原生会话名字超长截断",
                    "model": "gpt-6.1-terra",
                    "cwd": "/工作区/非常长的中文路径/测试项目目录",
                }
            ],
            "pending_questions": [
                {
                    "id": real_qid,
                    "session_id": real_sid,
                    "session_name": "evil\x1b[2Jname",
                    "tool": "bash\x07",
                    "command": "echo hi this is a very long command line exceeding eighty terminal columns\x1b]0;PWN\x07",
                    "created_rel": "1s ago",
                }
            ],
            "broadcasts": [
                {
                    "broadcast_id": "broadcast-id-very-long-12345",
                    "group_id": "group-name-long",
                    "received": 2,
                    "total": 3,
                    "uncertain": 1,
                    "message_preview": "非常长的中文广播消息预览超过八十列测试内容",
                }
            ],
            "groups": [{"id": "group-name-long"}],
            "events": [
                {
                    "seq": 999,
                    "created_rel": "2s ago",
                    "kind": "tool_started",
                    "session_name": "中文会话测试长名字",
                    "summary": "❤️" * 30 + "非常长的中文事件摘要内容测试八十列截断",
                }
            ],
        }
        tui80 = render_tui(tui_snap, width=80)
        self.assertNotIn("\x07", tui80)
        self.assertNotIn("\x1b[2J", tui80)
        self.assertIn("$0.1500*", tui80)
        self.assertIn("❤️", tui80)
        self.assertIn(f"session_id={real_sid}", tui80)
        self.assertIn(f"question_id={real_qid}", tui80)
        self.assertIn("expected_revision=3", tui80)
        self.assertIn('answer={"decision":"allow"}', tui80)
        for line in tui80.splitlines():
            plain_line = ansi_sgr.sub("", line)
            self.assertLessEqual(terminal_cells(line), 80, plain_line)
            # Clipped emoji VS16 sequences must never leave a bare '❤' separated from '\ufe0f'
            self.assertEqual(plain_line.count("\u2764"), plain_line.count("\u2764\ufe0f"), plain_line)
        # Even when an ANSI SGR sequence sits between '❤' and '\ufe0f', cluster integrity & width hold
        split_clipped = ansi_sgr.sub("", _clip_ansi_to_width("\u2764\x1b[31m\ufe0f\u2764\ufe0f", 3))
        self.assertEqual("\u2764\ufe0f", split_clipped)
        self.assertLessEqual(terminal_cells(split_clipped), 3)

        # MCP schema for agent_monitor includes after_seq
        mcp = build_mcp_server(self.state)
        tool_props = mcp._tool_manager._tools["agent_monitor"].parameters["properties"]
        self.assertIn("after_seq", tool_props)

    def test_round3_cost_semantics_and_large_history_performance(self) -> None:
        # 1. Reported cumulative session cost MUST NOT double-count (1.0 then 2.0 -> 2.0, NOT 3.0)
        #    AND summarize_runtimes prioritizes session cumulative cost over single latest attempt cost!
        store = Store(self.state)
        with store.connection(write=True) as db:
            prof_r = json.dumps({"name": "claude-rep", "runtime": "claude", "model": "claude-sonnet-5"})
            prof_c = json.dumps({"name": "claude-cat", "runtime": "claude", "model": "claude-sonnet-5"})
            db.execute(
                "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                "VALUES ('s-reported', ?, '/repo/a', 'workspace-write', 'open', 'idle', 1, 100.0)",
                (prof_r,),
            )
            db.execute(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-r1', 's-reported', 'one', '[]', 'completed', 'approved', 0, 101.0)"
            )
            db.execute(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES ('a-r1', 't-r1', 's-reported', 'completed', ?, 102.0, 103.0)",
                (json.dumps({"complete": True, "reported_session_cost": 1.0}),),
            )
            db.execute(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-r2', 's-reported', 'two', '[]', 'completed', 'approved', 0, 104.0)"
            )
            db.execute(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES ('a-r2', 't-r2', 's-reported', 'completed', ?, 105.0, 106.0)",
                (json.dumps({"complete": True, "reported_session_cost": 2.0}),),
            )

            # Second session has catalog_estimate per attempt (0.15 + 0.25 = 0.40 cumulative, latest attempt 0.25)
            db.execute(
                "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                "VALUES ('s-catalog', ?, '/repo/b', 'workspace-write', 'open', 'idle', 1, 200.0)",
                (prof_c,),
            )
            db.execute(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-c1', 's-catalog', 'one', '[]', 'completed', 'approved', 0, 201.0)"
            )
            db.execute(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES ('a-c1', 't-c1', 's-catalog', 'completed', ?, 202.0, 203.0)",
                (json.dumps({"complete": True, "catalog_estimate": {"usd": 0.15, "complete": True}, "reported_session_cost": 0.15}),),
            )
            db.execute(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-c2', 's-catalog', 'two', '[]', 'completed', 'approved', 0, 204.0)"
            )
            db.execute(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES ('a-c2', 't-c2', 's-catalog', 'completed', ?, 205.0, 206.0)",
                (json.dumps({"complete": True, "catalog_estimate": {"usd": 0.25, "complete": True}, "reported_session_cost": 0.40}),),
            )
            # Third session mixes cumulative reported_session_cost (1.0) followed by delta catalog_estimate (0.25) -> 1.25 complete
            db.execute(
                "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                "VALUES ('s-mixed', ?, '/repo/m', 'workspace-write', 'open', 'idle', 1, 300.0)",
                (prof_r,),
            )
            db.execute(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-m1', 's-mixed', 'one', '[]', 'completed', 'approved', 0, 301.0), "
                "('t-m2', 's-mixed', 'two', '[]', 'completed', 'approved', 0, 302.0)"
            )
            db.execute(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES ('a-m1', 't-m1', 's-mixed', 'completed', ?, 301.5, 301.8), "
                "('a-m2', 't-m2', 's-mixed', 'completed', ?, 302.5, 302.8)",
                (
                    json.dumps({"complete": True, "reported_session_cost": 1.0}),
                    json.dumps({"complete": True, "catalog_estimate": {"usd": 0.25, "complete": True}}),
                ),
            )
            # Fourth session has trailing missing cost -> stays partial (cost_complete=False)
            db.execute(
                "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                "VALUES ('s-gap', ?, '/repo/g', 'workspace-write', 'open', 'idle', 1, 400.0)",
                (prof_r,),
            )
            db.execute(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-g1', 's-gap', 'one', '[]', 'completed', 'approved', 0, 401.0), "
                "('t-g2', 's-gap', 'two', '[]', 'completed', 'approved', 0, 402.0)"
            )
            db.execute(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES ('a-g1', 't-g1', 's-gap', 'completed', ?, 401.5, 401.8), "
                "('a-g2', 't-g2', 's-gap', 'completed', ?, 402.5, 402.8)",
                (json.dumps({"complete": True, "reported_session_cost": 1.0}), json.dumps({})),
            )
            # Fifth session: attempt 1 has {}, attempt 2 has BOTH reported_session_cost=1.0 AND delta=0.25 -> 1.0 (not 0.25!)
            # plus chronological check: earlier unfinished (started=501, reported=1.0) followed by later finished (started=502, reported=2.0) -> 2.0!
            db.execute(
                "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                "VALUES ('s-both', ?, '/repo/b2', 'workspace-write', 'open', 'idle', 1, 450.0), "
                "('s-chrono', ?, '/repo/ch', 'workspace-write', 'open', 'idle', 1, 500.0)",
                (prof_r, prof_r),
            )
            db.execute(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-b1', 's-both', 'one', '[]', 'completed', 'approved', 0, 451.0), "
                "('t-b2', 's-both', 'two', '[]', 'completed', 'approved', 0, 452.0), "
                "('t-ch1', 's-chrono', 'one', '[]', 'running', 'pending', 0, 501.0), "
                "('t-ch2', 's-chrono', 'two', '[]', 'completed', 'approved', 0, 502.0)"
            )
            db.execute(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES ('a-b1', 't-b1', 's-both', 'completed', ?, 451.5, 451.8), "
                "('a-b2', 't-b2', 's-both', 'completed', ?, 452.5, 452.8), "
                "('a-ch1', 't-ch1', 's-chrono', 'running', ?, 501.5, NULL), "
                "('a-ch2', 't-ch2', 's-chrono', 'completed', ?, 502.5, 502.8)",
                (
                    json.dumps({}),
                    json.dumps({"complete": True, "reported_session_cost": 1.0, "catalog_estimate": {"usd": 0.25, "complete": True}}),
                    json.dumps({"complete": False, "reported_session_cost": 1.0}),
                    json.dumps({"complete": True, "reported_session_cost": 2.0}),
                ),
            )

        c = MonitorCollector(
            self.state,
            host_registry=ConfigHostRegistry({}),
            proc_root=self.proc_root,
            native_home=self.home,
        )
        snap = c.collect(limit=25, include_native=False)
        by_id = {s["id"]: s for s in snap["sessions"]}
        self.assertEqual(2.0, by_id["s-reported"]["cost_usd"])
        self.assertEqual(0.4, by_id["s-catalog"]["cost_usd"])
        self.assertEqual(0.25, by_id["s-catalog"]["attempt"]["cost_usd"])
        self.assertEqual(1.25, by_id["s-mixed"]["cost_usd"])
        self.assertTrue(by_id["s-mixed"]["cost_complete"])
        self.assertEqual(1.0, by_id["s-gap"]["cost_usd"])
        self.assertFalse(by_id["s-gap"]["cost_complete"])
        self.assertEqual(1.0, by_id["s-both"]["cost_usd"])
        self.assertEqual(2.0, by_id["s-chrono"]["cost_usd"])
        # Total across sessions: 2.0 + 0.4 + 1.25 + 1.0 + 1.0 + 2.0 = 7.65
        self.assertEqual(7.65, snap["metrics"]["total_cost_usd"])
        self.assertEqual(7.65, snap["runtimes"]["claude"]["total_cost_usd"])
        self.assertFalse(snap["runtimes"]["claude"]["cost_complete"])

        # Cross-session retry: source session s-from loses its ONLY task to s-target -> s-from keeps $0.50 AND s-target.attempt is None!
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                "VALUES ('s-from', ?, '/repo/f', 'workspace-write', 'open', 'idle', 1, 550.0)",
                (prof_r,),
            )
            db.execute(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES ('t-trans', 's-from', 'retryable task', '[]', 'running', 'pending', 0, 551.0)"
            )
            db.execute(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES ('a-trans', 't-trans', 's-from', 'failed', ?, 552.0, 553.0)",
                (json.dumps({"complete": True, "catalog_estimate": {"usd": 0.50, "complete": True}}),),
            )
        warm_run = c.collect(limit=25, include_native=False)
        self.assertEqual("running", next(s for s in warm_run["sessions"] if s["id"] == "s-from")["task"]["status"])
        with store.connection(write=True) as db:
            db.execute("UPDATE tasks SET status='failed' WHERE id='t-trans'")
        warm_fail = c.collect(limit=25, include_native=False)
        self.assertEqual("failed", next(s for s in warm_fail["sessions"] if s["id"] == "s-from")["task"]["status"])
        store.retry("t-trans", "s-catalog", "retryable task", "req-retry-1")
        warm_retry = c.collect(limit=25, include_native=False)
        r_by = {s["id"]: s for s in warm_retry["sessions"]}
        # t-trans moved to s-catalog, s-from now has NO tasks, yet s-from retains $0.50 and s-catalog does NOT display a-trans!
        self.assertIsNone(r_by["s-from"]["task"])
        self.assertEqual(0.5, r_by["s-from"]["cost_usd"])
        self.assertEqual("queued", r_by["s-catalog"]["task"]["status"])
        self.assertIsNone(r_by["s-catalog"]["attempt"])
        self.assertEqual(0.4, r_by["s-catalog"]["cost_usd"])

        # Empty tasks table MUST still retain attempt costs on cold collect
        with tempfile.TemporaryDirectory() as no_task_tmp:
            nt_state = Path(no_task_tmp)
            nt_store = Store(nt_state)
            with nt_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES ('s-notask', ?, '/repo/nt', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r,),
                )
                db.execute("INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) VALUES ('t-del', 's-notask', 'x', '[]', 'completed', 'approved', 0, 2.0)")
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a-nt', 't-del', 's-notask', 'completed', ?, 3.0, 4.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 0.77}),),
                )
                db.commit()
                db.execute("PRAGMA foreign_keys = OFF")
                db.execute("DELETE FROM tasks")
            nt_snap = MonitorCollector(
                nt_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
            ).collect(limit=25, include_native=False)
            self.assertEqual(0.77, nt_snap["sessions"][0]["cost_usd"])

            # Cross-poll out-of-order started timestamps: first poll caches started=200 (2.0),
            # second poll receives new row with started=100 (1.0) -> MUST rebuild chronologically and keep 2.0!
            nt_coll = MonitorCollector(
                nt_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
            )
            with nt_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t-late', 's-notask', 'late', '[]', 'completed', 'approved', 0, 10.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a-late-2', 't-late', 's-notask', 'completed', ?, 200.0, 205.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 2.0}),),
                )
            p1_snap = nt_coll.collect(limit=25, include_native=False)
            self.assertEqual(2.0, p1_snap["sessions"][0]["cost_usd"])
            with nt_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a-late-1', 't-late', 's-notask', 'completed', ?, 100.0, 105.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 1.0}),),
                )
            p2_snap = nt_coll.collect(limit=25, include_native=False)
            p2_cold = MonitorCollector(
                nt_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
            ).collect(limit=25, include_native=False)
            self.assertEqual(2.0, p2_snap["sessions"][0]["cost_usd"])
            self.assertEqual(p2_cold["sessions"][0]["cost_usd"], p2_snap["sessions"][0]["cost_usd"])

            # Cached finished attempt with started=NULL (e.g. cancelled before start, reported=2.0)
            # followed in Poll 2 by finite started=100 (reported=1.0) -> hot MUST rebuild and match cold (2.0)!
            with tempfile.TemporaryDirectory() as null_tmp:
                n_state = Path(null_tmp)
                n_store = Store(n_state)
                with n_store.connection(write=True) as db:
                    db.execute(
                        "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                        "VALUES ('s-null', ?, '/repo/nl', 'workspace-write', 'open', 'idle', 1, 1.0)",
                        (prof_r,),
                    )
                    db.execute(
                        "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                        "VALUES ('t-nl', 's-null', 'null-start', '[]', 'cancelled', 'pending', 0, 2.0)"
                    )
                    db.execute(
                        "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                        "VALUES ('a-nl-2', 't-nl', 's-null', 'cancelled', ?, NULL, 20.0)",
                        (json.dumps({"complete": True, "reported_session_cost": 2.0}),),
                    )
                nl_coll = MonitorCollector(
                    n_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                )
                self.assertEqual(2.0, nl_coll.collect(limit=25, include_native=False)["sessions"][0]["cost_usd"])
                with n_store.connection(write=True) as db:
                    db.execute(
                        "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                        "VALUES ('a-nl-1', 't-nl', 's-null', 'completed', ?, 10.0, 15.0)",
                        (json.dumps({"complete": True, "reported_session_cost": 1.0}),),
                    )
                hot_nl = nl_coll.collect(limit=25, include_native=False)
                cold_nl = MonitorCollector(
                    n_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)
                self.assertEqual(2.0, hot_nl["sessions"][0]["cost_usd"])
                self.assertEqual(cold_nl["sessions"][0]["cost_usd"], hot_nl["sessions"][0]["cost_usd"])

            # Multi-task simultaneous cross-session retry in one warm batch (including newer task moving
            # to target + untouched older task fallback + secondary move into vacated source): hot == cold.
            with tempfile.TemporaryDirectory() as mig_tmp:
                m_state = Path(mig_tmp)
                m_store = Store(m_state)
                with m_store.connection(write=True) as db:
                    for sid in ("sA", "sB", "sC"):
                        db.execute(
                            "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                            "VALUES (?, ?, '/repo', 'read-only', 'open', 'idle', 1, 1.0)",
                            (sid, json.dumps({"name": sid, "runtime": "codex", "model": "fixed"})),
                        )
                    db.execute(
                        "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                        "VALUES "
                        "('t-a0', 'sA', 'a0', '[]', 'completed', 'approved', 0, 150.0), "
                        "('t-a1', 'sA', 'a1', '[]', 'completed', 'approved', 0, 200.0), "
                        "('t-b1', 'sB', 'b1', '[]', 'completed', 'approved', 0, 100.0), "
                        "('t-c0', 'sC', 'c0', '[]', 'completed', 'approved', 0, 50.0)"
                    )
                m_coll = MonitorCollector(
                    m_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                )
                m_coll.collect(limit=25, include_native=False)
                with m_store.connection(write=True) as db:
                    # Move BOTH t-a1 (newer) -> sC and t-b1 (older) -> sA (where untouched t-a0@150 is newer),
                    # plus another batch where two tasks move to sC together.
                    db.execute("UPDATE tasks SET session_id='sC', status='pending' WHERE id='t-a1'")
                    db.execute("UPDATE tasks SET session_id='sA', status='pending' WHERE id='t-b1'")
                    db.execute(
                        "INSERT INTO events(session_id, task_id, attempt_id, kind, data, created) VALUES "
                        "('sA', 't-a1', NULL, 'retry_requested', '{}', 300.0), "
                        "('sB', 't-b1', NULL, 'retry_requested', '{}', 301.0)"
                    )
                hot_m1 = {s["id"]: s["task"]["id"] if s["task"] else None for s in m_coll.collect(limit=25, include_native=False)["sessions"]}
                cold_m1 = {
                    s["id"]: s["task"]["id"] if s["task"] else None
                    for s in MonitorCollector(
                        m_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                    ).collect(limit=25, include_native=False)["sessions"]
                }
                self.assertEqual(cold_m1, hot_m1)
                self.assertEqual({"sA": "t-a0", "sB": None, "sC": "t-a1"}, hot_m1)

                with m_store.connection(write=True) as db:
                    db.execute("UPDATE tasks SET session_id='sA', status='completed' WHERE id='t-a1'")
                    db.execute("UPDATE tasks SET session_id='sB', status='completed' WHERE id='t-b1'")
                    db.execute(
                        "INSERT INTO events(session_id, task_id, attempt_id, kind, data, created) VALUES "
                        "('sC', 't-a1', NULL, 'retry_requested', '{}', 310.0), "
                        "('sA', 't-b1', NULL, 'retry_requested', '{}', 311.0)"
                    )
                m_coll.collect(limit=25, include_native=False)
                with m_store.connection(write=True) as db:
                    # Move BOTH t-a1 and t-b1 into sC simultaneously while touching t-c0->sB
                    db.execute("UPDATE tasks SET session_id='sC' WHERE id IN ('t-a1', 't-b1')")
                    db.execute("UPDATE tasks SET session_id='sB' WHERE id='t-c0'")
                    db.execute(
                        "INSERT INTO events(session_id, task_id, attempt_id, kind, data, created) VALUES "
                        "('sA', 't-a1', NULL, 'retry_requested', '{}', 320.0), "
                        "('sB', 't-b1', NULL, 'retry_requested', '{}', 321.0), "
                        "('sC', 't-c0', NULL, 'retry_requested', '{}', 322.0)"
                    )
                hot_m2 = {s["id"]: s["task"]["id"] if s["task"] else None for s in m_coll.collect(limit=25, include_native=False)["sessions"]}
                cold_m2 = {
                    s["id"]: s["task"]["id"] if s["task"] else None
                    for s in MonitorCollector(
                        m_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                    ).collect(limit=25, include_native=False)["sessions"]
                }
                self.assertEqual(cold_m2, hot_m2)
                self.assertEqual({"sA": "t-a0", "sB": "t-c0", "sC": "t-a1"}, hot_m2)

        # Cancelled unstarted assigned attempts (started=NULL, finished!=NULL, usage=NULL) MUST NOT mark cost incomplete
        with tempfile.TemporaryDirectory() as can_tmp:
            c_state = Path(can_tmp)
            c_store = Store(c_state)
            with c_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES ('s-can', ?, '/repo/can', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r,),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t-can', 's-can', 'paused-before-start', '[]', 'cancelled', 'pending', 0, 2.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a-can-0', 't-can', 's-can', 'cancelled', NULL, NULL, 100.0)"
                )
            can_coll = MonitorCollector(
                c_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
            )
            can_snap1 = can_coll.collect(limit=25, include_native=False)
            self.assertEqual(0.0, can_snap1["sessions"][0]["cost_usd"])
            self.assertTrue(can_snap1["sessions"][0]["cost_complete"])
            self.assertTrue(can_snap1["metrics"]["cost_complete"])

            # Re-run same task -> real attempt must win over earlier unstarted cancelled attempt & keep cost_complete=True
            with c_store.connection(write=True) as db:
                db.execute("UPDATE tasks SET status='completed', validation='approved' WHERE id='t-can'")
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a-can-1', 't-can', 's-can', 'completed', ?, 200.0, 205.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 0.5}),),
                )
            can_snap2 = can_coll.collect(limit=25, include_native=False)
            self.assertEqual(0.5, can_snap2["sessions"][0]["cost_usd"])
            self.assertTrue(can_snap2["sessions"][0]["cost_complete"])
            self.assertEqual("a-can-1", can_snap2["sessions"][0]["attempt"]["id"])
            self.assertTrue(can_snap2["metrics"]["cost_complete"])

            # Concurrent collect() serialization: older reader paused inside SQLite tx cannot clobber newer cache
            t1_in_tx = threading.Event()
            release_t1 = threading.Event()
            t1_tid = [None]

            class _SlowFirstTxCollector(MonitorCollector):
                def _has_table(self, db: Any, table_name: str) -> bool:
                    if table_name == "tasks" and t1_tid[0] == threading.get_ident():
                        t1_in_tx.set()
                        release_t1.wait(timeout=3.0)
                        t1_tid[0] = None
                    return super()._has_table(db, table_name)

            race_coll = _SlowFirstTxCollector(
                c_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
            )
            res_holder: dict[str, dict[str, Any]] = {}

            def _run_t1() -> None:
                t1_tid[0] = threading.get_ident()
                res_holder["t1"] = race_coll.collect(limit=25, include_native=False)

            th1 = threading.Thread(target=_run_t1)
            th1.start()
            self.assertTrue(t1_in_tx.wait(timeout=3.0))
            with c_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t-new-concurrent', 's-can', 'concurrent', '[]', 'completed', 'approved', 0, 300.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a-new-concurrent', 't-new-concurrent', 's-can', 'completed', ?, 301.0, 305.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 1.25}),),
                )
            th2 = threading.Thread(
                target=lambda: res_holder.__setitem__("t2", race_coll.collect(limit=25, include_native=False))
            )
            th2.start()
            time.sleep(0.05)
            # Even with th2 queued on _db_lock, releasing th1 finishes th1 cleanly BEFORE th2 snapshots DB!
            release_t1.set()
            th1.join(timeout=5.0)
            th2.join(timeout=5.0)
            res_holder["t3"] = race_coll.collect(limit=25, include_native=False)
            self.assertEqual("t-new-concurrent", res_holder["t2"]["sessions"][0]["task"]["id"])
            self.assertEqual("t-new-concurrent", res_holder["t3"]["sessions"][0]["task"]["id"])
            self.assertEqual(1.25, res_holder["t3"]["sessions"][0]["cost_usd"])

        # Claude worker restart cost isolation: never underestimate pre-restart spend or falsely mark complete
        from agent_bridge.config import Profile
        from agent_bridge.runtimes import ClaudeRuntime

        rt_p = Profile(name="claude-test", runtime="claude", model="claude-sonnet-5", executable="claude")
        rt_w1 = ClaudeRuntime(rt_p, {"native_id": "nc-1", "cwd": "/repo", "access": "read-only"}, self.state, lambda q: None)
        rt_w2 = ClaudeRuntime(rt_p, {"native_id": "nc-1", "cwd": "/repo", "access": "read-only", "has_native_history": True}, self.state, lambda q: None)
        self.assertNotEqual(rt_w1._client_cost_scope, rt_w2._client_cost_scope)

        with tempfile.TemporaryDirectory() as rst_tmp:
            r_state = Path(rst_tmp)
            r_store = Store(r_state)
            with r_store.connection(write=True) as db:
                for sid in ("s-rst-bare", "s-rst-delta", "s-rst-legacy"):
                    db.execute(
                        "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                        "VALUES (?, ?, '/repo/rst', 'workspace-write', 'open', 'idle', 1, 1.0)",
                        (sid, prof_r),
                    )
                    db.execute(
                        "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                        "VALUES (?, ?, 'turn1', '[]', 'completed', 'approved', 0, 10.0)",
                        (f"t1-{sid}", sid),
                    )
                    db.execute(
                        "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                        "VALUES (?, ?, ?, 'completed', ?, 11.0, 15.0)",
                        (
                            f"a1-{sid}",
                            f"t1-{sid}",
                            sid,
                            json.dumps(
                                {
                                    "complete": True,
                                    "reported_session_cost": 1.0,
                                    **({"reported_cost_scope": rt_w1._client_cost_scope} if sid != "s-rst-legacy" else {}),
                                }
                            ),
                        ),
                    )
            r_coll = MonitorCollector(
                r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
            )
            r_coll.collect(limit=25, include_native=False)

            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO events(session_id, task_id, attempt_id, kind, data, created) "
                    "VALUES ('s-rst-legacy', NULL, NULL, 'worker_stopped', '{}', 20.0)"
                )
                for sid in ("s-rst-bare", "s-rst-delta", "s-rst-legacy"):
                    db.execute(
                        "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                        "VALUES (?, ?, 'turn2', '[]', 'completed', 'approved', 0, 30.0)",
                        (f"t2-{sid}", sid),
                    )
                # s-rst-bare: worker restarted with reported_session_cost=0.25 only -> $1.25 INCOMPLETE (not $0.25 complete!)
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a2-s-rst-bare', 't2-s-rst-bare', 's-rst-bare', 'completed', ?, 31.0, 35.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 0.25, "reported_cost_scope": rt_w2._client_cost_scope}),),
                )
                # s-rst-delta: worker restarted with verified catalog_estimate=0.25 + turn2 delta in same new scope -> $1.25 complete
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a2-s-rst-delta', 't2-s-rst-delta', 's-rst-delta', 'completed', ?, 31.0, 35.0)",
                    (
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 0.25,
                                "reported_cost_scope": rt_w2._client_cost_scope,
                                "catalog_estimate": {"usd": 0.25, "complete": True},
                            }
                        ),
                    ),
                )
                # s-rst-legacy: worker_stopped occurred, post-restart attempt has larger reported=1.50 without delta -> $2.50 INCOMPLETE
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a2-s-rst-legacy', 't2-s-rst-legacy', 's-rst-legacy', 'completed', ?, 31.0, 35.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 1.5}),),
                )

            r_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            r_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }
            for label, by_r in (("hot", r_hot), ("cold", r_cold)):
                self.assertEqual(1.25, by_r["s-rst-bare"]["cost_usd"], label)
                self.assertFalse(by_r["s-rst-bare"]["cost_complete"], label)
                self.assertEqual(1.25, by_r["s-rst-delta"]["cost_usd"], label)
                self.assertTrue(by_r["s-rst-delta"]["cost_complete"], label)
                self.assertEqual(2.5, by_r["s-rst-legacy"]["cost_usd"], label)
                self.assertFalse(by_r["s-rst-legacy"]["cost_complete"], label)

            # Intermediate delta-only turn (catalog_estimate=0.20 without reported_session_cost -> $1.45),
            # followed by reported_session_cost=0.60 MUST yield $1.60 (NOT $1.80!) with cost_complete=True.
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t3-s-rst-delta', 's-rst-delta', 'turn3', '[]', 'completed', 'approved', 0, 40.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a3-s-rst-delta', 't3-s-rst-delta', 's-rst-delta', 'completed', ?, 41.0, 45.0)",
                    (
                        json.dumps(
                            {
                                "complete": True,
                                "reported_cost_scope": rt_w2._client_cost_scope,
                                "catalog_estimate": {"usd": 0.20, "complete": True},
                            }
                        ),
                    ),
                )
            d2_mid = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            self.assertEqual(1.45, d2_mid["s-rst-delta"]["cost_usd"])
            self.assertTrue(d2_mid["s-rst-delta"]["cost_complete"])

            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t4-s-rst-delta', 's-rst-delta', 'turn4', '[]', 'completed', 'approved', 0, 50.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a4-s-rst-delta', 't4-s-rst-delta', 's-rst-delta', 'completed', ?, 51.0, 55.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 0.60, "reported_cost_scope": rt_w2._client_cost_scope}),),
                )
                # Legacy unscoped ($1.00) -> upgraded scoped ($1.50) WITHOUT worker_stopped event:
                # MUST detect scope switch, preserve pre-upgrade spend ($2.50), and mark incomplete.
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES ('s-rst-upgrade', ?, '/repo/upg', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r,),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t1-upg', 's-rst-upgrade', 'legacy', '[]', 'completed', 'approved', 0, 60.0), "
                    "('t2-upg', 's-rst-upgrade', 'upgraded', '[]', 'completed', 'approved', 0, 70.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a1-upg', 't1-upg', 's-rst-upgrade', 'completed', ?, 61.0, 65.0), "
                    "('a2-upg', 't2-upg', 's-rst-upgrade', 'completed', ?, 71.0, 75.0)",
                    (
                        json.dumps({"complete": True, "reported_session_cost": 1.0}),
                        json.dumps({"complete": True, "reported_session_cost": 1.5, "reported_cost_scope": rt_w2._client_cost_scope}),
                    ),
                )
            d2_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            d2_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }
            for label, by_d in (("hot", d2_hot), ("cold", d2_cold)):
                self.assertEqual(1.6, by_d["s-rst-delta"]["cost_usd"], label)
                self.assertTrue(by_d["s-rst-delta"]["cost_complete"], label)
                self.assertEqual(2.5, by_d["s-rst-upgrade"]["cost_usd"], label)
                self.assertFalse(by_d["s-rst-upgrade"]["cost_complete"], label)

            # Non-restarted intermediate delta ($0.25 -> +$0.20 = $0.45) followed by stale
            # reported_session_cost=0.25 MUST NOT drop below $0.45 or falsely stay complete.
            # Missing-usage attempt inside same client scope ($1.00 -> usage=None -> $1.50)
            # MUST NOT split the scope and double-count to $2.50.
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES "
                    "('s-stale-delta', ?, '/repo/stale', 'workspace-write', 'open', 'idle', 1, 1.0), "
                    "('s-gap-same-scope', ?, '/repo/gap', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r, prof_r),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES "
                    "('t1-st', 's-stale-delta', 'r1', '[]', 'completed', 'approved', 0, 100.0), "
                    "('t2-st', 's-stale-delta', 'd2', '[]', 'completed', 'approved', 0, 110.0), "
                    "('t3-st', 's-stale-delta', 'r3', '[]', 'completed', 'approved', 0, 120.0), "
                    "('t1-gs', 's-gap-same-scope', 'g1', '[]', 'completed', 'approved', 0, 100.0), "
                    "('t2-gs', 's-gap-same-scope', 'g2', '[]', 'failed', 'none', 0, 110.0), "
                    "('t3-gs', 's-gap-same-scope', 'g3', '[]', 'completed', 'approved', 0, 120.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES "
                    "('a1-st', 't1-st', 's-stale-delta', 'completed', ?, 101.0, 105.0), "
                    "('a2-st', 't2-st', 's-stale-delta', 'completed', ?, 111.0, 115.0), "
                    "('a3-st', 't3-st', 's-stale-delta', 'completed', ?, 121.0, 125.0), "
                    "('a1-gs', 't1-gs', 's-gap-same-scope', 'completed', ?, 101.0, 105.0), "
                    "('a2-gs', 't2-gs', 's-gap-same-scope', 'failed', NULL, 111.0, 115.0), "
                    "('a3-gs', 't3-gs', 's-gap-same-scope', 'completed', ?, 121.0, 125.0)",
                    (
                        json.dumps({"complete": True, "reported_session_cost": 0.25, "reported_cost_scope": "scope-stale"}),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_cost_scope": "scope-stale",
                                "catalog_estimate": {"usd": 0.20, "complete": True},
                            }
                        ),
                        json.dumps({"complete": True, "reported_session_cost": 0.25, "reported_cost_scope": "scope-stale"}),
                        json.dumps({"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "scope-gap"}),
                        json.dumps({"complete": True, "reported_session_cost": 1.50, "reported_cost_scope": "scope-gap"}),
                    ),
                )
            d3_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            d3_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }
            for label, by_d in (("hot", d3_hot), ("cold", d3_cold)):
                self.assertEqual(0.45, by_d["s-stale-delta"]["cost_usd"], label)
                self.assertFalse(by_d["s-stale-delta"]["cost_complete"], label)
                self.assertEqual(1.5, by_d["s-gap-same-scope"]["cost_usd"], label)
                self.assertFalse(by_d["s-gap-same-scope"]["cost_complete"], label)
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t4-st', 's-stale-delta', 'r4', '[]', 'completed', 'approved', 0, 130.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a4-st', 't4-st', 's-stale-delta', 'completed', ?, 131.0, 135.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 0.60, "reported_cost_scope": "scope-stale"}),),
                )
            d4_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            d4_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }
            for label, by_d in (("hot", d4_hot), ("cold", d4_cold)):
                self.assertEqual(0.60, by_d["s-stale-delta"]["cost_usd"], label)

            # Round 16 cost-accounting edge cases:
            # 1. Same-scope attempt carrying BOTH stale reported_session_cost AND current delta MUST NOT drop delta.
            # 2. Post-restart attempt carrying BOTH stale reported_session_cost AND current delta MUST preserve prior + current unabsorbed delta.
            # 3. Missing-telemetry attempt with worker_stopped either BEFORE or AFTER it yields $1.00 + $1.50 = $2.50 incomplete.
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES "
                    "('s-same-cur-stale', ?, '/repo/scs', 'workspace-write', 'open', 'idle', 1, 1.0), "
                    "('s-same-prior-cur', ?, '/repo/spc', 'workspace-write', 'open', 'idle', 1, 1.0), "
                    "('s-rst-cur-stale', ?, '/repo/rcs', 'workspace-write', 'open', 'idle', 1, 1.0), "
                    "('s-stop-before-gap', ?, '/repo/sbg', 'workspace-write', 'open', 'idle', 1, 1.0), "
                    "('s-stop-after-gap', ?, '/repo/sag', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r, prof_r, prof_r, prof_r, prof_r),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES "
                    "('t1-scs', 's-same-cur-stale', '1', '[]', 'completed', 'approved', 0, 200.0), "
                    "('t2-scs', 's-same-cur-stale', '2', '[]', 'completed', 'approved', 0, 210.0), "
                    "('t1-spc', 's-same-prior-cur', '1', '[]', 'completed', 'approved', 0, 200.0), "
                    "('t2-spc', 's-same-prior-cur', '2', '[]', 'completed', 'approved', 0, 210.0), "
                    "('t3-spc', 's-same-prior-cur', '3', '[]', 'completed', 'approved', 0, 220.0), "
                    "('t1-rcs', 's-rst-cur-stale', '1', '[]', 'completed', 'approved', 0, 200.0), "
                    "('t2-rcs', 's-rst-cur-stale', '2', '[]', 'completed', 'approved', 0, 210.0), "
                    "('t3-rcs', 's-rst-cur-stale', '3', '[]', 'completed', 'approved', 0, 220.0), "
                    "('t4-rcs', 's-rst-cur-stale', '4', '[]', 'completed', 'approved', 0, 230.0), "
                    "('t5-rcs', 's-rst-cur-stale', '5', '[]', 'completed', 'approved', 0, 240.0), "
                    "('t1-sbg', 's-stop-before-gap', '1', '[]', 'completed', 'approved', 0, 200.0), "
                    "('t2-sbg', 's-stop-before-gap', '2', '[]', 'failed', 'none', 0, 210.0), "
                    "('t3-sbg', 's-stop-before-gap', '3', '[]', 'completed', 'approved', 0, 220.0), "
                    "('t1-sag', 's-stop-after-gap', '1', '[]', 'completed', 'approved', 0, 200.0), "
                    "('t2-sag', 's-stop-after-gap', '2', '[]', 'failed', 'none', 0, 210.0), "
                    "('t3-sag', 's-stop-after-gap', '3', '[]', 'completed', 'approved', 0, 220.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES "
                    "('a1-scs', 't1-scs', 's-same-cur-stale', 'completed', ?, 201.0, 205.0), "
                    "('a2-scs', 't2-scs', 's-same-cur-stale', 'completed', ?, 211.0, 215.0), "
                    "('a1-spc', 't1-spc', 's-same-prior-cur', 'completed', ?, 201.0, 205.0), "
                    "('a2-spc', 't2-spc', 's-same-prior-cur', 'completed', ?, 211.0, 215.0), "
                    "('a3-spc', 't3-spc', 's-same-prior-cur', 'completed', ?, 221.0, 225.0), "
                    "('a1-rcs', 't1-rcs', 's-rst-cur-stale', 'completed', ?, 201.0, 205.0), "
                    "('a2-rcs', 't2-rcs', 's-rst-cur-stale', 'completed', ?, 211.0, 215.0), "
                    "('a3-rcs', 't3-rcs', 's-rst-cur-stale', 'completed', ?, 221.0, 225.0), "
                    "('a4-rcs', 't4-rcs', 's-rst-cur-stale', 'completed', ?, 231.0, 235.0), "
                    "('a5-rcs', 't5-rcs', 's-rst-cur-stale', 'completed', ?, 241.0, 245.0), "
                    "('a1-sbg', 't1-sbg', 's-stop-before-gap', 'completed', ?, 201.0, 205.0), "
                    "('a2-sbg', 't2-sbg', 's-stop-before-gap', 'failed', NULL, 211.0, 215.0), "
                    "('a3-sbg', 't3-sbg', 's-stop-before-gap', 'completed', ?, 221.0, 225.0), "
                    "('a1-sag', 't1-sag', 's-stop-after-gap', 'completed', ?, 201.0, 205.0), "
                    "('a2-sag', 't2-sag', 's-stop-after-gap', 'failed', NULL, 211.0, 215.0), "
                    "('a3-sag', 't3-sag', 's-stop-after-gap', 'completed', ?, 221.0, 225.0)",
                    (
                        json.dumps({"complete": True, "reported_session_cost": 1.0, "reported_cost_scope": "sc-scs"}),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 1.0,
                                "reported_cost_scope": "sc-scs",
                                "catalog_estimate": {"usd": 0.25, "complete": True},
                            }
                        ),
                        json.dumps({"complete": True, "reported_session_cost": 0.25, "reported_cost_scope": "sc-spc"}),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_cost_scope": "sc-spc",
                                "catalog_estimate": {"usd": 0.20, "complete": True},
                            }
                        ),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 0.45,
                                "reported_cost_scope": "sc-spc",
                                "catalog_estimate": {"usd": 0.10, "complete": True},
                            }
                        ),
                        json.dumps({"complete": True, "reported_session_cost": 1.0, "reported_cost_scope": "sc-rcs-1"}),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 0.25,
                                "reported_cost_scope": "sc-rcs-2",
                                "catalog_estimate": {"usd": 0.25, "complete": True},
                            }
                        ),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_cost_scope": "sc-rcs-2",
                                "catalog_estimate": {"usd": 0.20, "complete": True},
                            }
                        ),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 0.25,
                                "reported_cost_scope": "sc-rcs-2",
                                "catalog_estimate": {"usd": 0.10, "complete": True},
                            }
                        ),
                        json.dumps({"complete": True, "reported_session_cost": 0.60, "reported_cost_scope": "sc-rcs-2"}),
                        json.dumps({"complete": True, "reported_session_cost": 1.0, "reported_cost_scope": "sc-sbg-1"}),
                        json.dumps({"complete": True, "reported_session_cost": 1.5, "reported_cost_scope": "sc-sbg-2"}),
                        json.dumps({"complete": True, "reported_session_cost": 1.0, "reported_cost_scope": "sc-sag-1"}),
                        json.dumps({"complete": True, "reported_session_cost": 1.5, "reported_cost_scope": "sc-sag-2"}),
                    ),
                )
                db.execute(
                    "INSERT INTO events(session_id, kind, data, created) "
                    "VALUES "
                    "('s-rst-cur-stale', 'worker_stopped', '{}', 208.0), "
                    "('s-stop-before-gap', 'worker_stopped', '{}', 208.0), "
                    "('s-stop-after-gap', 'worker_stopped', '{}', 218.0)"
                )
            d5_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            d5_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }
            for label, by_d in (("hot", d5_hot), ("cold", d5_cold)):
                self.assertEqual(1.25, by_d["s-same-cur-stale"]["cost_usd"], label)
                self.assertFalse(by_d["s-same-cur-stale"]["cost_complete"], label)
                self.assertEqual(0.55, by_d["s-same-prior-cur"]["cost_usd"], label)
                self.assertFalse(by_d["s-same-prior-cur"]["cost_complete"], label)
                self.assertEqual(1.60, by_d["s-rst-cur-stale"]["cost_usd"], label)
                self.assertFalse(by_d["s-rst-cur-stale"]["cost_complete"], label)
                self.assertEqual(2.50, by_d["s-stop-before-gap"]["cost_usd"], label)
                self.assertFalse(by_d["s-stop-before-gap"]["cost_complete"], label)
                self.assertEqual(2.50, by_d["s-stop-after-gap"]["cost_usd"], label)
                self.assertFalse(by_d["s-stop-after-gap"]["cost_complete"], label)

            # Round 17/18 partial growth & post-restart excess cases across incremental hot cache collects:
            # normal scope: 1.00 -> (collect) -> (reported=1.10, delta=0.25) -> 1.25 incomplete
            # post-restart scope: pre=1.00 -> (collect) -> stop -> (reported=0.25, delta=0.25) -> (collect) -> (reported=0.35, delta=0.25) -> 1.50 incomplete
            # post-restart excess: pre=1.00 -> (collect) -> stop -> (reported=0.50, delta=0.25) -> 1.50 incomplete
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES "
                    "('s-same-partial', ?, '/repo/sp', 'workspace-write', 'open', 'idle', 1, 1.0), "
                    "('s-rst-partial', ?, '/repo/rp', 'workspace-write', 'open', 'idle', 1, 1.0), "
                    "('s-rst-excess', ?, '/repo/re', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r, prof_r, prof_r),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES "
                    "('t1-sp', 's-same-partial', '1', '[]', 'completed', 'approved', 0, 300.0), "
                    "('t1-rp', 's-rst-partial', '1', '[]', 'completed', 'approved', 0, 300.0), "
                    "('t1-re', 's-rst-excess', '1', '[]', 'completed', 'approved', 0, 300.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES "
                    "('a1-sp', 't1-sp', 's-same-partial', 'completed', ?, 301.0, 305.0), "
                    "('a1-rp', 't1-rp', 's-rst-partial', 'completed', ?, 301.0, 305.0), "
                    "('a1-re', 't1-re', 's-rst-excess', 'completed', ?, 301.0, 305.0)",
                    (
                        json.dumps({"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-sp"}),
                        json.dumps({"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-rp-1"}),
                        json.dumps({"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-re-1"}),
                    ),
                )
            d6_step1 = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            self.assertEqual(1.00, d6_step1["s-same-partial"]["cost_usd"])
            self.assertEqual(1.00, d6_step1["s-rst-partial"]["cost_usd"])
            self.assertEqual(1.00, d6_step1["s-rst-excess"]["cost_usd"])

            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO events(session_id, kind, data, created) "
                    "VALUES "
                    "('s-rst-partial', 'worker_stopped', '{}', 308.0), "
                    "('s-rst-excess', 'worker_stopped', '{}', 308.0)"
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES "
                    "('t2-sp', 's-same-partial', '2', '[]', 'completed', 'approved', 0, 310.0), "
                    "('t2-rp', 's-rst-partial', '2', '[]', 'completed', 'approved', 0, 310.0), "
                    "('t2-re', 's-rst-excess', '2', '[]', 'completed', 'approved', 0, 310.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES "
                    "('a2-sp', 't2-sp', 's-same-partial', 'completed', ?, 311.0, 315.0), "
                    "('a2-rp', 't2-rp', 's-rst-partial', 'completed', ?, 311.0, 315.0), "
                    "('a2-re', 't2-re', 's-rst-excess', 'completed', ?, 311.0, 315.0)",
                    (
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 1.10,
                                "reported_cost_scope": "sc-sp",
                                "catalog_estimate": {"usd": 0.25, "complete": True},
                            }
                        ),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 0.25,
                                "reported_cost_scope": "sc-rp-2",
                                "catalog_estimate": {"usd": 0.25, "complete": True},
                            }
                        ),
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 0.50,
                                "reported_cost_scope": "sc-re-2",
                                "catalog_estimate": {"usd": 0.25, "complete": True},
                            }
                        ),
                    ),
                )
            d6_step2 = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            self.assertEqual(1.25, d6_step2["s-same-partial"]["cost_usd"])
            self.assertFalse(d6_step2["s-same-partial"]["cost_complete"])
            self.assertEqual(1.25, d6_step2["s-rst-partial"]["cost_usd"])
            self.assertTrue(d6_step2["s-rst-partial"]["cost_complete"])
            self.assertEqual(1.50, d6_step2["s-rst-excess"]["cost_usd"])
            self.assertFalse(d6_step2["s-rst-excess"]["cost_complete"])

            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t3-rp', 's-rst-partial', '3', '[]', 'completed', 'approved', 0, 320.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a3-rp', 't3-rp', 's-rst-partial', 'completed', ?, 321.0, 325.0)",
                    (
                        json.dumps(
                            {
                                "complete": True,
                                "reported_session_cost": 0.35,
                                "reported_cost_scope": "sc-rp-2",
                                "catalog_estimate": {"usd": 0.25, "complete": True},
                            }
                        ),
                    ),
                )
            d6_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}
            d6_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }
            for label, by_d in (("hot", d6_hot), ("cold", d6_cold)):
                self.assertEqual(1.25, by_d["s-same-partial"]["cost_usd"], label)
                self.assertFalse(by_d["s-same-partial"]["cost_complete"], label)
                self.assertEqual(1.50, by_d["s-rst-partial"]["cost_usd"], label)
                self.assertFalse(by_d["s-rst-partial"]["cost_complete"], label)
                self.assertEqual(1.50, by_d["s-rst-excess"]["cost_usd"], label)
                self.assertFalse(by_d["s-rst-excess"]["cost_complete"], label)

            # Round 19 post-restart without cumulative baseline:
            # $1.00 -> stop -> delta=0.25 ($1.25) -> reported-only 0.10 (stays $1.25, keeps 0.15 unabsorbed)
            # -> reported-only 0.25 (MUST stay $1.25, NOT $1.40!) -> reported-only 0.40 ($1.40).
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES ('s-rst-nobase-stale', ?, '/repo/rnbs', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r,),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t1-rnbs', 's-rst-nobase-stale', '1', '[]', 'completed', 'approved', 0, 400.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a1-rnbs', 't1-rnbs', 's-rst-nobase-stale', 'completed', ?, 401.0, 405.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-rnbs-1"}),),
                )
            self.assertEqual(
                1.00,
                {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-rst-nobase-stale"]["cost_usd"],
            )
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO events(session_id, kind, data, created) "
                    "VALUES ('s-rst-nobase-stale', 'worker_stopped', '{}', 408.0)"
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t2-rnbs', 's-rst-nobase-stale', '2', '[]', 'completed', 'approved', 0, 410.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a2-rnbs', 't2-rnbs', 's-rst-nobase-stale', 'completed', ?, 411.0, 415.0)",
                    (
                        json.dumps(
                            {
                                "complete": True,
                                "reported_cost_scope": "sc-rnbs-2",
                                "catalog_estimate": {"usd": 0.25, "complete": True},
                            }
                        ),
                    ),
                )
            d7_s2 = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-rst-nobase-stale"]
            self.assertEqual(1.25, d7_s2["cost_usd"])
            self.assertTrue(d7_s2["cost_complete"])
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t3-rnbs', 's-rst-nobase-stale', '3', '[]', 'completed', 'approved', 0, 420.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a3-rnbs', 't3-rnbs', 's-rst-nobase-stale', 'completed', ?, 421.0, 425.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 0.10, "reported_cost_scope": "sc-rnbs-2"}),),
                )
            d7_s3 = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-rst-nobase-stale"]
            self.assertEqual(1.25, d7_s3["cost_usd"])
            self.assertFalse(d7_s3["cost_complete"])
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t4-rnbs', 's-rst-nobase-stale', '4', '[]', 'completed', 'approved', 0, 430.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a4-rnbs', 't4-rnbs', 's-rst-nobase-stale', 'completed', ?, 431.0, 435.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 0.25, "reported_cost_scope": "sc-rnbs-2"}),),
                )
            d7_s4_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-rst-nobase-stale"]
            d7_s4_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-rst-nobase-stale"]
            for label, item in (("hot", d7_s4_hot), ("cold", d7_s4_cold)):
                self.assertEqual(1.25, item["cost_usd"], label)
                self.assertFalse(item["cost_complete"], label)
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t5-rnbs', 's-rst-nobase-stale', '5', '[]', 'completed', 'approved', 0, 440.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a5-rnbs', 't5-rnbs', 's-rst-nobase-stale', 'completed', ?, 441.0, 445.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 0.40, "reported_cost_scope": "sc-rnbs-2"}),),
                )
            d7_s5_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-rst-nobase-stale"]
            d7_s5_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-rst-nobase-stale"]
            for label, item in (("hot", d7_s5_hot), ("cold", d7_s5_cold)):
                self.assertEqual(1.40, item["cost_usd"], label)
                self.assertFalse(item["cost_complete"], label)

            # Equal-started tie-breaker (started, rowid): an unfinished attempt with equal started
            # and LOWER rowid than a cached attempt MUST trigger chronological rebuild so hot matches cold.
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES ('s-tie-key', ?, '/repo/tk', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r,),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES "
                    "('t-tie-hi', 's-tie-key', 'hi', '[]', 'completed', 'approved', 0, 500.0), "
                    "('t-tie-lo', 's-tie-key', 'lo', '[]', 'running', 'pending', 0, 500.0)"
                )
                db.execute(
                    "INSERT INTO attempts(rowid, id, task_id, session_id, status, usage, started, finished) "
                    "VALUES (5000, 'a-tie-hi', 't-tie-hi', 's-tie-key', 'completed', ?, 500.0, 505.0)",
                    (json.dumps({"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-tk"}),),
                )
            d8_warm = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-tie-key"]
            self.assertEqual(1.00, d8_warm["cost_usd"])
            # Now insert a live attempt with identical started=500.0 but lower rowid=4999; in SQL chronological
            # order a-tie-lo ($0.25 delta) runs BEFORE a-tie-hi (cumulative $1.00), yielding $1.00 (not $1.25!).
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO attempts(rowid, id, task_id, session_id, status, usage, started, finished) "
                    "VALUES (4999, 'a-tie-lo', 't-tie-lo', 's-tie-key', 'running', ?, 500.0, NULL)",
                    (json.dumps({"complete": True, "reported_cost_scope": "sc-tk", "catalog_estimate": {"usd": 0.25, "complete": True}}),),
                )
            d8_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-tie-key"]
            d8_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-tie-key"]
            for label, item in (("hot", d8_hot), ("cold", d8_cold)):
                self.assertEqual(1.00, item["cost_usd"], label)
                self.assertFalse(item["cost_complete"], label)

            # Complete a-tie-lo and warm cache to rowid=5010, then insert a COMPLETED backfill rowid=5009 (< watermark):
            # the very next collect() MUST immediately see the backfill via finished-count check before query.
            with r_store.connection(write=True) as db:
                db.execute("UPDATE attempts SET status='completed', finished=505.0 WHERE id='a-tie-lo'")
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES ('s-bf-done', ?, '/repo/bfd', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r,),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES "
                    "('t-bf-hi', 's-bf-done', 'hi', '[]', 'completed', 'approved', 0, 510.0), "
                    "('t-bf-lo', 's-bf-done', 'lo', '[]', 'completed', 'approved', 0, 509.0)"
                )
                db.execute(
                    "INSERT INTO attempts(rowid, id, task_id, session_id, status, usage, started, finished) "
                    "VALUES (5010, 'a-bf-hi', 't-bf-hi', 's-bf-done', 'completed', ?, 510.0, 515.0)",
                    (json.dumps({"complete": True, "catalog_estimate": {"usd": 0.40, "complete": True}}),),
                )
            d9_warm = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-bf-done"]
            self.assertEqual(0.40, d9_warm["cost_usd"])
            with r_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO attempts(rowid, id, task_id, session_id, status, usage, started, finished) "
                    "VALUES (5009, 'a-bf-lo', 't-bf-lo', 's-bf-done', 'completed', ?, 509.0, 510.0)",
                    (json.dumps({"complete": True, "catalog_estimate": {"usd": 0.25, "complete": True}}),),
                )
            d9_hot = {s["id"]: s for s in r_coll.collect(limit=25, include_native=False)["sessions"]}["s-bf-done"]
            d9_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    r_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-bf-done"]
            for label, item in (("hot", d9_hot), ("cold", d9_cold)):
                self.assertEqual(0.65, item["cost_usd"], label)
                self.assertTrue(item["cost_complete"], label)

        # VACUUM rowid renumbering: TEXT PRIMARY KEY rowids compact during VACUUM so newly inserted rows
        # reuse rowids <= old watermark; watermark-id verification detects this immediately.
        with tempfile.TemporaryDirectory() as vac_tmp:
            v_state = Path(vac_tmp)
            v_store = Store(v_state)
            with v_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES ('s-vac', ?, '/repo/vac', 'workspace-write', 'open', 'idle', 1, 1.0)",
                    (prof_r,),
                )
                db.execute(
                    "INSERT INTO tasks(rowid, id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES "
                    "(1, 't1-vac', 's-vac', 'one', '[]', 'completed', 'approved', 0, 10.0), "
                    "(2, 't2-vac', 's-vac', 'two', '[]', 'completed', 'approved', 0, 20.0), "
                    "(3, 't3-vac', 's-vac', 'three', '[]', 'completed', 'approved', 0, 30.0)"
                )
                db.execute(
                    "INSERT INTO attempts(rowid, id, task_id, session_id, status, usage, started, finished) "
                    "VALUES "
                    "(1, 'a1-vac', 't1-vac', 's-vac', 'completed', ?, 11.0, 12.0), "
                    "(2, 'a2-vac', 't2-vac', 's-vac', 'completed', ?, 21.0, 22.0), "
                    "(3, 'a3-vac', 't3-vac', 's-vac', 'completed', ?, 31.0, 32.0)",
                    (
                        json.dumps({"complete": True, "catalog_estimate": {"usd": 0.10, "complete": True}}),
                        json.dumps({"complete": True, "catalog_estimate": {"usd": 0.20, "complete": True}}),
                        json.dumps({"complete": True, "catalog_estimate": {"usd": 0.30, "complete": True}}),
                    ),
                )
            v_coll = MonitorCollector(
                v_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
            )
            v_warm = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            self.assertEqual(0.60, v_warm["cost_usd"])
            # Delete rowid 1, compact remaining rowids (2,3 -> 1,2), VACUUM, then insert new row getting auto rowid=3.
            # Notice completed count at rowid <= 3 remains 3, yet rowid 3's id changed from a3-vac to a4-vac!
            with v_store.connection(write=True) as db:
                db.execute("DELETE FROM attempts WHERE rowid = 1")
                db.execute("DELETE FROM tasks WHERE rowid = 1")
                db.execute("UPDATE attempts SET rowid = rowid - 1 WHERE rowid > 1")
                db.execute("UPDATE tasks SET rowid = rowid - 1 WHERE rowid > 1")
            v_raw = sqlite3.connect(str(v_coll.db_path), isolation_level=None)
            try:
                v_raw.execute("VACUUM")
            finally:
                v_raw.close()
            with v_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES ('t4-vac', 's-vac', 'four', '[]', 'completed', 'approved', 0, 40.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES ('a4-vac', 't4-vac', 's-vac', 'completed', ?, 41.0, 42.0)",
                    (json.dumps({"complete": True, "catalog_estimate": {"usd": 0.40, "complete": True}}),),
                )
            v_hot = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            v_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    v_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-vac"]
            for label, item in (("hot", v_hot), ("cold", v_cold)):
                self.assertEqual("t4-vac", item["task"]["id"], label)
                self.assertEqual(0.90, item["cost_usd"], label)
                self.assertTrue(item["cost_complete"], label)
            # In-place row replacement BELOW unchanged watermark rowid=3 (delete rowid=1 'a2-vac'/$0.20 and reuse rowid=1
            # so row count=3 and watermark rowid=3/id='a4-vac' stay identical): BOTH tasks and attempts caches MUST rebuild.
            with v_store.connection(write=True) as db:
                db.execute("DELETE FROM attempts WHERE rowid = 1")
                db.execute("DELETE FROM tasks WHERE rowid = 1")
                db.execute(
                    "INSERT INTO tasks(rowid, id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES (1, 'abXcd', 's-vac', 'replaced', '[]', 'completed', 'approved', 0, 50.0)"
                )
                db.execute(
                    "INSERT INTO attempts(rowid, id, task_id, session_id, status, usage, started, finished) "
                    "VALUES (1, 'uvXwx', 'abXcd', 's-vac', 'completed', ?, 51.0, 52.0)",
                    ('{"complete": true, "catalog_estimate": {"usd": 0.75, "complete": true}}',),
                )
            v_rep_hot = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            v_rep_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    v_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-vac"]
            for label, item in (("hot", v_rep_hot), ("cold", v_rep_cold)):
                self.assertEqual("abXcd", item["task"]["id"], label)
                self.assertEqual(1.45, item["cost_usd"], label)
                self.assertTrue(item["cost_complete"], label)
            # Isolate single-field mutations so EACH field is independently proven to trigger rebuild:
            # 1) Equal-length usage-only mutation ($0.75 -> $0.20, identical id/task_id/timestamps)
            with v_store.connection(write=True) as db:
                db.execute(
                    "UPDATE attempts SET usage = ? WHERE rowid = 1",
                    ('{"complete": true, "catalog_estimate": {"usd": 0.20, "complete": true}}',),
                )
            v_u_hot = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            v_u_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    v_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-vac"]
            for label, item in (("hot", v_u_hot), ("cold", v_u_cold)):
                self.assertEqual(0.90, item["cost_usd"], label)
                self.assertTrue(item["cost_complete"], label)
            # 2) Task ID-only mutation on attempt rowid=1 (same attempt id/usage/timestamps; changes cached attempt owner key)
            with v_store.connection(write=True) as db:
                db.execute("UPDATE attempts SET task_id = 't4-vac' WHERE rowid = 1")
            v_tid_hot = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            v_tid_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    v_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-vac"]
            self.assertIsNone(v_tid_hot["attempt"])
            self.assertIsNone(v_tid_cold["attempt"])
            with v_store.connection(write=True) as db:
                db.execute("UPDATE attempts SET task_id = 'abXcd' WHERE rowid = 1")
            v_tid2_hot = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            v_tid2_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    v_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-vac"]
            self.assertEqual(v_tid2_cold["attempt"], v_tid2_hot["attempt"])
            self.assertEqual("uvXwx", v_tid2_hot["attempt"]["id"])
            # 3) Constant-sum timestamp swap only (51.0/52.0 -> 51.2/51.8; same id/task_id/usage)
            with v_store.connection(write=True) as db:
                db.execute("UPDATE attempts SET started = 51.2, finished = 51.8 WHERE rowid = 1")
            v_ts_hot = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            v_ts_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    v_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-vac"]
            self.assertEqual(v_ts_cold["attempt"], v_ts_hot["attempt"])
            self.assertEqual(51.2, v_ts_hot["attempt"]["started"])
            self.assertEqual(51.8, v_ts_hot["attempt"]["finished"])
            # 4) Middle-char-only task ID replacement (t5Xvac -> t5Yvac) with identical rowid & created and zero attempt changes:
            with v_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO tasks(rowid, id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES (4, 't5Xvac', 's-vac', 'replaced', '[]', 'completed', 'approved', 0, 60.0)"
                )
            v_mid_warm = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            self.assertEqual("t5Xvac", v_mid_warm["task"]["id"])
            with v_store.connection(write=True) as db:
                db.execute("UPDATE tasks SET id = 't5Yvac' WHERE rowid = 4")
            v_mid_hot = {s["id"]: s for s in v_coll.collect(limit=25, include_native=False)["sessions"]}["s-vac"]
            v_mid_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    v_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-vac"]
            for label, item in (("hot", v_mid_hot), ("cold", v_mid_cold)):
                self.assertEqual("t5Yvac", item["task"]["id"], label)
                self.assertEqual(0.90, item["cost_usd"], label)
                self.assertTrue(item["cost_complete"], label)

        # Store.control('resume', continue_task_id=...) and control('close') across collect() calls:
        with tempfile.TemporaryDirectory() as ctl_tmp:
            c_state = Path(ctl_tmp)
            c_store = Store(c_state)
            c_profile = Profile("codex", "codex", "fixed", provider="fixture", executable=sys.executable)
            s_ctl = c_store.open(c_profile, str(c_state), "read-only", "open-ctl")["session_id"]
            c_store.worker_starting(s_ctl, "g1", 1, "start1")
            c_store.worker_ready(s_ctl, "g1", 1, "start1", "native-ctl")
            t_ctl = c_store.dispatch(s_ctl, "continue-me", "disp-ctl")["task_id"]
            c_cfg = Config({"codex": c_profile}, worker_launcher="detached")
            c_store.claim(c_cfg)
            attempt_ctl = c_store.begin_attempt(s_ctl, "g1")["active_attempt"]
            c_store.control(s_ctl, "pause", c_store.get_session(s_ctl)["revision"], "pause-ctl")
            c_store.finish(s_ctl, "g1", t_ctl, attempt_ctl, "interrupted", {"answer": "partial"}, {"total_tokens": 10})
            c_coll = MonitorCollector(
                c_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
            )
            c_snap1 = {s["id"]: s for s in c_coll.collect(limit=25, include_native=False)["sessions"]}[s_ctl]
            self.assertEqual("interrupted", c_snap1["task"]["status"])
            c_store.control(s_ctl, "resume", c_store.get_session(s_ctl)["revision"], "resume-ctl", continue_task_id=t_ctl)
            ctl_events = [e for e in c_store.events(session_id=s_ctl) if e["kind"] == "session_control_requested"]
            self.assertEqual(t_ctl, ctl_events[-1]["task_id"])
            c_hot2 = {s["id"]: s for s in c_coll.collect(limit=25, include_native=False)["sessions"]}[s_ctl]
            c_cold2 = {
                s["id"]: s
                for s in MonitorCollector(
                    c_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }[s_ctl]
            self.assertEqual("queued", c_hot2["task"]["status"])
            self.assertEqual("queued", c_cold2["task"]["status"])
            c_store.control(s_ctl, "close", c_store.get_session(s_ctl)["revision"], "close-ctl")
            c_hot3 = {s["id"]: s for s in c_coll.collect(limit=25, include_native=False)["sessions"]}[s_ctl]
            c_cold3 = {
                s["id"]: s
                for s in MonitorCollector(
                    c_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }[s_ctl]
            self.assertEqual("cancelled", c_hot3["task"]["status"])
            self.assertEqual("cancelled", c_cold3["task"]["status"])

            # Backfilling a historical worker_stopped event between two already-cached completed attempts
            # MUST invalidate the attempt cost cache so cumulative post-restart spend matches cold collection.
            with c_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id, profile, cwd, access, desired, observed, revision, created) "
                    "VALUES ('s-stop-bf', ?, '/repo/stopbf', 'read-only', 'open', 'idle', 1, 1.0)",
                    (prof_r,),
                )
                db.execute(
                    "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                    "VALUES "
                    "('t-sb1', 's-stop-bf', 'sb1', '[]', 'completed', 'approved', 0, 10.0), "
                    "('t-sb2', 's-stop-bf', 'sb2', '[]', 'completed', 'approved', 0, 20.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                    "VALUES "
                    "('a-sb1', 't-sb1', 's-stop-bf', 'completed', ?, 10.0, 12.0), "
                    "('a-sb2', 't-sb2', 's-stop-bf', 'completed', ?, 20.0, 22.0)",
                    (
                        json.dumps({"complete": True, "reported_session_cost": 1.00}),
                        json.dumps({"complete": True, "reported_session_cost": 1.50}),
                    ),
                )
            sb_warm = {s["id"]: s for s in c_coll.collect(limit=25, include_native=False)["sessions"]}["s-stop-bf"]
            self.assertEqual(1.50, sb_warm["cost_usd"])
            with c_store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO events(session_id, task_id, attempt_id, kind, data, created) "
                    "VALUES ('s-stop-bf', NULL, NULL, 'worker_stopped', '{\"observed\":\"stopped\"}', 15.0)"
                )
            sb_hot = {s["id"]: s for s in c_coll.collect(limit=25, include_native=False)["sessions"]}["s-stop-bf"]
            sb_cold = {
                s["id"]: s
                for s in MonitorCollector(
                    c_state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home
                ).collect(limit=25, include_native=False)["sessions"]
            }["s-stop-bf"]
            self.assertEqual(2.50, sb_hot["cost_usd"])
            self.assertEqual(2.50, sb_cold["cost_usd"])

        # Seed 3,000 completed tasks + attempts -> Broker._call reuses MonitorCollector hot cache!
        with store.connection(write=True) as db:
            db.execute("UPDATE attempts SET finished=502.0 WHERE id='a-ch1'")
            db.executemany(
                "INSERT INTO tasks(id, session_id, instruction, dependencies, status, validation, reservation, created) "
                "VALUES (?, 's-catalog', 'hist', '[]', 'completed', 'approved', 0, ?)",
                [(f"hist-t-{i}", 600.0 + i * 0.001) for i in range(3000)],
            )
            db.executemany(
                "INSERT INTO attempts(id, task_id, session_id, status, usage, started, finished) "
                "VALUES (?, ?, 's-catalog', 'completed', ?, ?, ?)",
                [
                    (
                        f"hist-a-{i}",
                        f"hist-t-{i}",
                        json.dumps({"complete": True, "catalog_estimate": {"usd": 0.001, "complete": True}}),
                        600.0 + i * 0.001,
                        600.5 + i * 0.001,
                    )
                    for i in range(3000)
                ],
            )
        (self.state / "config.json").write_text(
            json.dumps(
                {
                    "profiles": {"fixture": {"runtime": "codex", "model": "fixed", "executable": sys.executable}},
                    "worker_launcher": "detached",
                    "telegram_database": None,
                }
            )
        )
        broker = Broker(self.state)

        async def _probe_broker() -> tuple[dict[str, Any], float, bool]:
            await broker._call("agent_monitor", {"limit": 25, "include_native": False})
            first_coll = broker._monitor_collector
            t0 = time.monotonic()
            res = await broker._call("agent_monitor", {"limit": 25, "include_native": False})
            elapsed = (time.monotonic() - t0) * 1000.0
            return res, elapsed, broker._monitor_collector is first_coll

        warm, elapsed_ms, reused = asyncio.run(_probe_broker())
        self.assertTrue(reused)
        self.assertLess(elapsed_ms, 150.0, f"Broker hot collect took {elapsed_ms:.1f}ms")
        warm_cat = next(s for s in warm["sessions"] if s["id"] == "s-catalog")
        self.assertAlmostEqual(3.4, warm_cat["cost_usd"], places=4)

        # Cancelled broker._call('agent_monitor') MUST serialize its in-flight thread before the next call
        # touches shared collector.host_registry or enters collector.collect().
        first_entered = threading.Event()
        release_first = threading.Event()
        concurrency_lock = threading.Lock()
        active_collects = 0
        max_active_collects = 0
        call_seq = 0
        orig_collect = broker._monitor_collector.collect

        def _guarded_collect(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal active_collects, max_active_collects, call_seq
            with concurrency_lock:
                call_seq += 1
                my_seq = call_seq
                active_collects += 1
                max_active_collects = max(max_active_collects, active_collects)
            try:
                if my_seq == 1:
                    first_entered.set()
                    release_first.wait(timeout=3.0)
                return orig_collect(*args, **kwargs)
            finally:
                with concurrency_lock:
                    active_collects -= 1

        async def _probe_cancel_serialization() -> tuple[dict[str, Any], int]:
            with patch.object(broker._monitor_collector, "collect", side_effect=_guarded_collect):
                t1 = asyncio.create_task(broker._call("agent_monitor", {"limit": 10, "include_native": False}))
                while not first_entered.is_set():
                    await asyncio.sleep(0.01)
                t1.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await t1
                sentinel_registry = ConfigHostRegistry({})
                broker.host_registry = sentinel_registry
                t2 = asyncio.create_task(broker._call("agent_monitor", {"limit": 10, "include_native": False}))
                await asyncio.sleep(0.05)
                # While call 1 thread is still running, call 2 MUST NOT overwrite host_registry or enter collect().
                self.assertEqual(1, call_seq)
                self.assertIsNot(sentinel_registry, broker._monitor_collector.host_registry)
                release_first.set()
                res2 = await t2
                self.assertIs(sentinel_registry, broker._monitor_collector.host_registry)
                return res2, max_active_collects

        res_after_cancel, max_conc = asyncio.run(_probe_cancel_serialization())
        self.assertEqual(1, max_conc)
        self.assertGreater(res_after_cancel["sessions"][0]["created"], 0.0)

    def test_priced_usage_custom_catalog_not_compared_as_missing_reported_cost(self) -> None:
        custom_price = {
            "currency": "USD",
            "source": "configured",
            "as_of": "2026-10-01",
            "per_million": {"input_tokens": 120.0, "output_tokens": 0.0},
        }
        u_norm = priced_usage(
            {
                "scope": "turn",
                "input_tokens": 10_000,
                "output_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "complete": True,
                "reported_session_cost": 1.00,
                "reported_cost_scope": "sc-norm",
            },
            custom_price,
        )
        u_pre = priced_usage(
            {
                "scope": "turn",
                "input_tokens": 20_000,
                "output_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "complete": True,
                "reported_session_cost": 2.00,
                "reported_cost_scope": "sc-pre",
            },
            custom_price,
        )
        u_rst1 = priced_usage(
            {
                "scope": "turn",
                "input_tokens": 10_000,
                "output_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "complete": True,
                "reported_session_cost": 1.00,
                "reported_cost_scope": "sc-post",
            },
            custom_price,
        )
        u_rst2 = priced_usage(
            {
                "scope": "turn",
                "input_tokens": 5_000,
                "output_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "complete": True,
                "reported_session_cost": 1.50,
                "reported_cost_scope": "sc-post",
            },
            custom_price,
        )
        # Verify priced_usage produced $1.20 catalog_estimate > $1.00 reported_session_cost
        self.assertAlmostEqual(1.20, u_norm["catalog_estimate"]["usd"], places=6)

        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "state"
            store = Store(state)
            (state / "admin.token").write_text(TOKEN, "utf-8")
            u_gap_custom_only = priced_usage(
                {
                    "scope": "turn",
                    "input_tokens": 10_000,
                    "output_tokens": 0,
                    "cache_read_tokens": 0,
                    "cache_write_tokens": 0,
                    "complete": True,
                    "reported_cost_scope": "sc-gap",
                },
                custom_price,
            )
            with store.connection(write=True) as db:
                for sid in ("s-cust-norm", "s-cust-rst", "s-gap-norm", "s-gap-rst"):
                    db.execute(
                        "INSERT INTO sessions(id,profile,cwd,access,desired,observed,created) VALUES(?,?,?,'read','closed','stopped',?)",
                        (sid, "claude-opus", "/tmp", 100.0),
                    )
                db.execute(
                    "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                    "VALUES('t-n1','s-cust-norm','hi','[]','completed','passed','a-n1',1,101.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-n1", "t-n1", "s-cust-norm", "completed", json.dumps(u_norm), 102.0, 103.0),
                )

                db.execute(
                    "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                    "VALUES('t-r1','s-cust-rst','p1','[]','completed','passed','a-r1',1,101.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-r1", "t-r1", "s-cust-rst", "completed", json.dumps(u_pre), 102.0, 103.0),
                )
                db.execute(
                    "INSERT INTO events(session_id,task_id,attempt_id,kind,data,created) VALUES(?,?,?,?,?,?)",
                    ("s-cust-rst", None, None, "worker_stopped", "{}", 104.0),
                )
                db.execute(
                    "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                    "VALUES('t-r2','s-cust-rst','p2','[]','completed','passed','a-r2',1,105.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-r2", "t-r2", "s-cust-rst", "completed", json.dumps(u_rst1), 106.0, 107.0),
                )
                db.execute(
                    "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                    "VALUES('t-r3','s-cust-rst','p3','[]','completed','passed','a-r3',1,108.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-r3", "t-r3", "s-cust-rst", "completed", json.dumps(u_rst2), 109.0, 110.0),
                )

                # Gap case 1 (normal scope): $1.00 reported -> $1.20 custom-only (no reported) -> $2.00 reported -> $2.10 reported
                for tid, aid, ts, usage_obj in (
                    ("t-gn1", "a-gn1", 101.0, {"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-gap"}),
                    ("t-gn2", "a-gn2", 104.0, u_gap_custom_only),
                    ("t-gn3", "a-gn3", 107.0, {"complete": True, "reported_session_cost": 2.00, "reported_cost_scope": "sc-gap"}),
                    ("t-gn4", "a-gn4", 110.0, {"complete": True, "reported_session_cost": 2.10, "reported_cost_scope": "sc-gap"}),
                ):
                    db.execute(
                        "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                        "VALUES(?,?,'g','[]','completed','passed',?,1,?)",
                        (tid, "s-gap-norm", aid, ts),
                    )
                    db.execute(
                        "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                        (aid, tid, "s-gap-norm", "completed", json.dumps(usage_obj), ts + 1.0, ts + 2.0),
                    )

                # Gap case 2 (post-restart scope): $3.00 pre-restart -> $1.00 post -> $1.20 custom-only -> $2.00 reported -> $2.10 reported
                for tid, aid, ts, usage_obj in (
                    ("t-gr0", "a-gr0", 101.0, {"complete": True, "reported_session_cost": 3.00, "reported_cost_scope": "sc-gap-pre"}),
                    ("t-gr1", "a-gr1", 104.0, {"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-gap"}),
                    ("t-gr2", "a-gr2", 107.0, u_gap_custom_only),
                    ("t-gr3", "a-gr3", 110.0, {"complete": True, "reported_session_cost": 2.00, "reported_cost_scope": "sc-gap"}),
                    ("t-gr4", "a-gr4", 113.0, {"complete": True, "reported_session_cost": 2.10, "reported_cost_scope": "sc-gap"}),
                ):
                    db.execute(
                        "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                        "VALUES(?,?,'gr','[]','completed','passed',?,1,?)",
                        (tid, "s-gap-rst", aid, ts),
                    )
                    db.execute(
                        "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                        (aid, tid, "s-gap-rst", "completed", json.dumps(usage_obj), ts + 1.0, ts + 2.0),
                    )

            coll = MonitorCollector(state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home)
            for label, snap in (("cold", coll.collect(include_native=False)), ("warm", coll.collect(include_native=False))):
                by_id = {s["id"]: s for s in snap["sessions"]}
                norm_s = by_id["s-cust-norm"]
                self.assertAlmostEqual(1.00, norm_s["cost_usd"], places=6, msg=label)
                self.assertTrue(norm_s["cost_complete"], msg=label)
                rst_s = by_id["s-cust-rst"]
                # Pre-restart $2.00 + post-restart first delta $1.20 + Turn 3 native reported delta ($1.50 - $1.00 = $0.50) = $3.70
                self.assertAlmostEqual(3.70, rst_s["cost_usd"], places=6, msg=label)
                self.assertTrue(rst_s["cost_complete"], msg=label)

                gap_norm = by_id["s-gap-norm"]
                # $2.20 after Turn 3 + ($2.10 - $2.00 = +$0.10) on Turn 4 = $2.30 incomplete (MUST NOT drop back to $2.10)
                self.assertAlmostEqual(2.30, gap_norm["cost_usd"], places=6, msg=label)
                self.assertFalse(gap_norm["cost_complete"], msg=label)

                gap_rst = by_id["s-gap-rst"]
                # Pre-restart $3.00 + $1.00 post + $1.20 custom-only + ($2.10 - $2.00) = $5.30 (no double-count), marked incomplete
                self.assertAlmostEqual(5.30, gap_rst["cost_usd"], places=6, msg=label)
                self.assertFalse(gap_rst["cost_complete"], msg=label)

            # Stepwise mixed comparable + custom-catalog deltas in normal branch:
            # 1) s-mix-low: $1.00 -> $0.50(comp) -> $0.20(custom) -> $1.10 -> $1.20 -> $2.00
            #    must yield (1.00, True) -> (1.50, True) -> (1.70, True) -> (1.70, False) -> (1.70, False) -> (2.20, False)
            # 2) s-mix-mid: $1.00 -> $0.50(comp) -> $0.20(custom) -> $1.60 -> $1.65
            #    must yield (1.00, True) -> (1.50, True) -> (1.70, True) -> (1.70, False) -> (1.75, False)
            u_comp_050 = {
                "scope": "turn",
                "input_tokens": 10_000,
                "output_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "complete": True,
                "reported_cost_scope": "sc-mix",
                "catalog_estimate": {
                    "usd": 0.50,
                    "complete": True,
                    "source": "claude_cli_model_usage",
                },
            }
            u_cust_020 = priced_usage(
                {
                    "scope": "turn",
                    "input_tokens": 10_000,
                    "output_tokens": 0,
                    "cache_read_tokens": 0,
                    "cache_write_tokens": 0,
                    "complete": True,
                    "reported_cost_scope": "sc-mix",
                },
                {
                    "currency": "USD",
                    "source": "configured",
                    "as_of": "2026-10-01",
                    "per_million": {"input_tokens": 20.0, "output_tokens": 0.0},
                },
            )
            with store.connection(write=True) as db:
                for sid in ("s-mix-low", "s-mix-mid"):
                    db.execute(
                        "INSERT INTO sessions(id,profile,cwd,access,desired,observed,created) VALUES(?,?,?,'read','closed','stopped',?)",
                        (sid, "claude-opus", "/tmp", 100.0),
                    )
            mix_specs = {
                "s-mix-low": [
                    ({"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-mix"}, 1.00, True),
                    (u_comp_050, 1.50, True),
                    (u_cust_020, 1.70, True),
                    ({"complete": True, "reported_session_cost": 1.10, "reported_cost_scope": "sc-mix"}, 1.70, False),
                    ({"complete": True, "reported_session_cost": 1.20, "reported_cost_scope": "sc-mix"}, 1.70, False),
                    ({"complete": True, "reported_session_cost": 2.00, "reported_cost_scope": "sc-mix"}, 2.20, False),
                ],
                "s-mix-mid": [
                    ({"complete": True, "reported_session_cost": 1.00, "reported_cost_scope": "sc-mix"}, 1.00, True),
                    (u_comp_050, 1.50, True),
                    (u_cust_020, 1.70, True),
                    ({"complete": True, "reported_session_cost": 1.60, "reported_cost_scope": "sc-mix"}, 1.70, False),
                    ({"complete": True, "reported_session_cost": 1.65, "reported_cost_scope": "sc-mix"}, 1.75, False),
                ],
            }
            for step_idx in range(6):
                with store.connection(write=True) as db:
                    for sid, steps in mix_specs.items():
                        if step_idx >= len(steps):
                            continue
                        usage_obj, _, _ = steps[step_idx]
                        tid = f"t-{sid}-{step_idx}"
                        aid = f"a-{sid}-{step_idx}"
                        ts = 200.0 + step_idx * 5.0
                        db.execute(
                            "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                            "VALUES(?,?,'mix','[]','completed','passed',?,1,?)",
                            (tid, sid, aid, ts),
                        )
                        db.execute(
                            "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                            (aid, tid, sid, "completed", json.dumps(usage_obj), ts + 1.0, ts + 2.0),
                        )
                inc_by_id = {s["id"]: s for s in coll.collect(include_native=False)["sessions"]}
                cold_coll = MonitorCollector(state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home)
                cold_by_id = {s["id"]: s for s in cold_coll.collect(include_native=False)["sessions"]}
                for sid, steps in mix_specs.items():
                    if step_idx >= len(steps):
                        continue
                    _, exp_usd, exp_complete = steps[step_idx]
                    for tag, res_map in (("inc", inc_by_id), ("cold", cold_by_id)):
                        sess_res = res_map[sid]
                        self.assertAlmostEqual(exp_usd, sess_res["cost_usd"], places=6, msg=f"{tag}:{sid}:{step_idx}")
                        self.assertEqual(exp_complete, sess_res["cost_complete"], msg=f"{tag}:{sid}:{step_idx}")

    def test_attempt_display_immune_to_clock_rollback_and_honors_active_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "state"
            store = Store(state)
            (state / "admin.token").write_text(TOKEN, "utf-8")
            with store.connection(write=True) as db:
                for sid in ("s-roll-null", "s-roll-active", "s-explicit"):
                    db.execute(
                        "INSERT INTO sessions(id,profile,cwd,access,desired,observed,created) VALUES(?,?,?,'read','closed','stopped',?)",
                        (sid, "claude-opus", "/tmp", 100.0),
                    )
                # Case 1: active_attempt cleared (NULL), unstarted attempt A cancelled at t=100, retry attempt B ran at t=90..95 after clock rollback
                db.execute(
                    "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                    "VALUES('t-null','s-roll-null','retry null','[]','completed','passed',NULL,1,101.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-null-1", "t-null", "s-roll-null", "cancelled", None, None, 100.0),
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-null-2", "t-null", "s-roll-null", "completed", json.dumps({"complete": True, "reported_session_cost": 0.35}), 90.0, 95.0),
                )

                # Case 2: active_attempt='a-act-2', attempt 1 cancelled at t=100, attempt 2 live inflight with clock rollback t=90
                db.execute(
                    "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                    "VALUES('t-act','s-roll-active','retry live','[]','running','pending','a-act-2',1,101.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-act-1", "t-act", "s-roll-active", "cancelled", None, None, 100.0),
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-act-2", "t-act", "s-roll-active", "running", None, 90.0, None),
                )

                # Case 3: explicit tasks.active_attempt points to a-exp-1 even though a-exp-2 has higher rowid
                db.execute(
                    "INSERT INTO tasks(id,session_id,instruction,dependencies,status,validation,active_attempt,reservation,created) "
                    "VALUES('t-exp','s-explicit','explicit active','[]','completed','passed','a-exp-1',1,101.0)"
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-exp-1", "t-exp", "s-explicit", "completed", json.dumps({"complete": True, "reported_session_cost": 0.42}), 90.0, 92.0),
                )
                db.execute(
                    "INSERT INTO attempts(id,task_id,session_id,status,usage,started,finished) VALUES(?,?,?,?,?,?,?)",
                    ("a-exp-2", "t-exp", "s-explicit", "cancelled", None, None, 105.0),
                )

            coll = MonitorCollector(state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home)
            for label, snap in (("cold", coll.collect(include_native=False)), ("warm", coll.collect(include_native=False))):
                by_id = {s["id"]: s for s in snap["sessions"]}
                self.assertEqual("a-null-2", by_id["s-roll-null"]["attempt"]["id"], msg=label)
                self.assertEqual("completed", by_id["s-roll-null"]["attempt"]["status"], msg=label)
                self.assertEqual("a-act-2", by_id["s-roll-active"]["attempt"]["id"], msg=label)
                self.assertEqual("running", by_id["s-roll-active"]["attempt"]["status"], msg=label)
                self.assertEqual("a-exp-1", by_id["s-explicit"]["attempt"]["id"], msg=label)
                self.assertEqual("completed", by_id["s-explicit"]["attempt"]["status"], msg=label)
                self.assertNotIn("_rowid", by_id["s-roll-null"]["attempt"], msg=label)

    def test_sse_and_snapshot_source_and_status_filter_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "state"
            store = Store(state)
            (state / "admin.token").write_text(TOKEN, "utf-8")
            with store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id,profile,cwd,access,desired,observed,worker_pid,created) VALUES(?,?,?,'read','closed','stopped',NULL,?)",
                    ("s-stop", "claude-opus", "/tmp", 100.0),
                )
                db.execute(
                    "INSERT INTO sessions(id,profile,cwd,access,desired,observed,worker_pid,created) VALUES(?,?,?,'read','open','running',?,?)",
                    ("s-run", "claude-opus", "/tmp", os.getpid(), 101.0),
                )
                db.execute(
                    "INSERT INTO events(session_id,task_id,attempt_id,kind,data,created) VALUES(?,?,?,?,?,?)",
                    ("s-stop", "t1", "a1", "task_finished", json.dumps({"status": "completed"}), 101.0),
                )
                db.execute(
                    "INSERT INTO events(session_id,task_id,attempt_id,kind,data,created) VALUES(?,?,?,?,?,?)",
                    ("s-run", "t2", "a2", "tool_started", json.dumps({"tool": "bash", "command": "ls"}), 102.0),
                )
                db.execute(
                    "INSERT INTO events(session_id,task_id,attempt_id,kind,data,created) VALUES(?,?,?,?,?,?)",
                    ("s-stop", "t3", "a3", "tool_completed", "{}", 103.0),
                )
                db.execute(
                    "INSERT INTO events(session_id,task_id,attempt_id,kind,data,created) VALUES(?,?,?,?,?,?)",
                    ("s-stop", "t4", "a4", "task_finished", json.dumps({"status": "completed"}), 104.0),
                )

            coll = MonitorCollector(state, host_registry=ConfigHostRegistry({}), proc_root=self.proc_root, native_home=self.home)

            # Direct collector source=native: 0 Bridge events, cursor still reflects latest DB seq
            native_snap = coll.collect(source="native", limit=10)
            self.assertEqual([], native_snap["events"])
            self.assertEqual(4, native_snap["metrics"]["latest_event_seq"])

            # Direct collector status=running with limit=1: running event seq=2 is NOT starved by stopped seq=3..4
            run_snap = coll.collect(status="running", limit=1, include_native=False)
            self.assertEqual([2], [e["seq"] for e in run_snap["events"]])

            # filter_snapshot_dict alone also filters events for source=native and status=running before pagination
            full_snap = coll.collect(limit=10, include_native=True)
            self.assertEqual(4, len(full_snap["events"]))
            self.assertEqual([], filter_snapshot_dict(full_snap, source="native")["events"])
            filtered_run = filter_snapshot_dict(full_snap, status="running", event_limit=1, session_limit=0)
            self.assertEqual([2], [e["seq"] for e in filtered_run["events"]])

            # SSE /api/v1/stream?source=native: never emits Bridge events even if cursor starts at 0, advances snapshot id to 4
            st_n, _, wire_n = self._dispatch(
                coll, "GET", "/api/v1/stream?once=1&source=native&after_seq=0&limit=1", state=state
            )
            self.assertEqual(200, st_n)
            self.assertNotIn("event: event\n", wire_n)
            self.assertIn("id: 4\nevent: snapshot\n", wire_n)

            # Defense-in-depth: even if a custom/mocked collector ignores source=native and returns Bridge events,
            # /api/v1/stream?source=native strips events and still finishes cleanly without busy-looping.
            class _LeakCollector:
                proc_root = self.proc_root
                native_home = self.home

                def collect(self, **kwargs: Any) -> dict[str, Any]:
                    return coll.collect(limit=kwargs.get("limit", 10), include_native=True)

            st_leak, _, wire_leak = self._dispatch(
                _LeakCollector(), "GET", "/api/v1/stream?once=1&source=native&after_seq=0&limit=1", state=state
            )
            self.assertEqual(200, st_leak)
            self.assertNotIn("event: event\n", wire_leak)
            self.assertIn("id: 4\nevent: snapshot\n", wire_leak)

            # SSE /api/v1/stream?status=running&limit=1&after_seq=0: emits only seq=2, no stopped events;
            # page boundary (len==limit=1) keeps cursor at 2 until follow-up poll, then advances past stopped seq=3..4 to 4.
            # Test BOTH real MonitorCollector AND a compat collector that ignores status and after_seq (DESC LIMIT limit).
            for test_coll in (coll, _LeakCollector()):
                st_r, _, wire_r = self._dispatch(
                    test_coll, "GET", "/api/v1/stream?once=1&status=running&after_seq=0&limit=1", state=state
                )
                self.assertEqual(200, st_r)
                self.assertIn("id: 2\nevent: event\n", wire_r)
                self.assertNotIn("id: 1\nevent: event\n", wire_r)
                self.assertNotIn("id: 3\nevent: event\n", wire_r)
                self.assertNotIn("id: 4\nevent: event\n", wire_r)
                self.assertNotIn("id: 4\nevent: cursor\n", wire_r)

                st_r2, _, wire_r2 = self._dispatch(
                    test_coll,
                    "GET",
                    "/api/v1/stream?once=1&status=running&limit=1",
                    extra_headers={"Last-Event-ID": "2"},
                    state=state,
                )
                self.assertEqual(200, st_r2)
                self.assertNotIn("event: event\n", wire_r2)
                self.assertIn("id: 4\nevent: snapshot\n", wire_r2)

            # Even if an un-filtering collector overflows the 500-event widening cap with non-matching events
            # (or even with a newer matching event inside the truncated 500-event tail while older matching events are cut off),
            # SSE MUST reject cursor replay / continuous streaming with 501, and on snapshot-only (once=1, no after_seq)
            # MUST NOT emit partial truncated event frames or advance cursor past latest_event_seq.
            class _OverflowLeakCollector:
                proc_root = self.proc_root
                native_home = self.home

                def __init__(self, *, include_matching_seq: int | None = None) -> None:
                    self.include_matching_seq = include_matching_seq

                def collect(self, **kwargs: Any) -> dict[str, Any]:
                    lim = int(kwargs.get("limit") or 50)
                    base = coll.collect(limit=10, include_native=False)
                    events = []
                    for idx in range(lim):
                        seq = 600 - idx
                        is_match = self.include_matching_seq == seq
                        events.append(
                            {
                                "seq": seq,
                                "session_id": "s-run" if is_match else "s-stop",
                                "session_name": f"claude #{'s-run' if is_match else 's-stop'}",
                                "runtime": "claude",
                                "task_id": "t-ov",
                                "kind": "tool_completed",
                                "summary": "tool done (ok)",
                                "created": 100.0,
                                "created_rel": "1s ago",
                            }
                        )
                    base["events"] = events
                    base["metrics"]["latest_event_seq"] = 600
                    return base

            for ov_coll in (_OverflowLeakCollector(), _OverflowLeakCollector(include_matching_seq=550)):
                st_ov_501, _, body_ov_501 = self._dispatch(
                    ov_coll,
                    "GET",
                    "/api/v1/stream?once=1&status=running&after_seq=0&limit=1",
                    state=state,
                )
                self.assertEqual(501, st_ov_501)
                ov_err = json.loads(body_ov_501)
                self.assertFalse(ov_err["ok"])
                self.assertIn("truncated", ov_err["error"])

                st_ov_200, _, wire_ov_200 = self._dispatch(
                    ov_coll,
                    "GET",
                    "/api/v1/stream?once=1&status=running&limit=1",
                    state=state,
                )
                self.assertEqual(200, st_ov_200)
                self.assertNotIn("event: event\n", wire_ov_200)
                self.assertIn("id: 0\nevent: snapshot\n", wire_ov_200)
                self.assertNotIn("id: 550\n", wire_ov_200)
                self.assertNotIn("id: 600\n", wire_ov_200)

            # Compat collector (< v3) returning newest matching event at head (limit=1 -> [seq=4(run)])
            # while ignoring after_seq/status: MUST still widen so older matching seq=1 is not skipped.
            class _MatchingHeadCompatCollector:
                proc_root = self.proc_root
                native_home = self.home

                def collect(self, **kwargs: Any) -> dict[str, Any]:
                    lim = int(kwargs.get("limit") or 50)
                    base = coll.collect(limit=10, include_native=False)
                    base["collector_version"] = 2
                    all_evs = [
                        {
                            "seq": 4,
                            "session_id": "s-run",
                            "session_name": "claude #s-run",
                            "runtime": "claude",
                            "task_id": "t-m4",
                            "kind": "tool_completed",
                            "summary": "run 4",
                            "created": 104.0,
                            "created_rel": "1s ago",
                        },
                        {
                            "seq": 3,
                            "session_id": "s-stop",
                            "session_name": "claude #s-stop",
                            "runtime": "claude",
                            "task_id": "t-m3",
                            "kind": "tool_completed",
                            "summary": "stop 3",
                            "created": 103.0,
                            "created_rel": "2s ago",
                        },
                        {
                            "seq": 2,
                            "session_id": "s-stop",
                            "session_name": "claude #s-stop",
                            "runtime": "claude",
                            "task_id": "t-m2",
                            "kind": "tool_completed",
                            "summary": "stop 2",
                            "created": 102.0,
                            "created_rel": "3s ago",
                        },
                        {
                            "seq": 1,
                            "session_id": "s-run",
                            "session_name": "claude #s-run",
                            "runtime": "claude",
                            "task_id": "t-m1",
                            "kind": "tool_started",
                            "summary": "run 1",
                            "created": 101.0,
                            "created_rel": "4s ago",
                        },
                    ]
                    base["events"] = all_evs[:lim]
                    base["metrics"]["latest_event_seq"] = 4
                    return base

            mh_coll = _MatchingHeadCompatCollector()
            st_mh1, _, wire_mh1 = self._dispatch(
                mh_coll, "GET", "/api/v1/stream?once=1&status=running&after_seq=0&limit=1", state=state
            )
            self.assertEqual(200, st_mh1)
            self.assertIn("id: 1\nevent: event\n", wire_mh1)
            self.assertNotIn("id: 4\nevent: event\n", wire_mh1)
            st_mh2, _, wire_mh2 = self._dispatch(
                mh_coll,
                "GET",
                "/api/v1/stream?once=1&status=running&limit=1",
                extra_headers={"Last-Event-ID": "1"},
                state=state,
            )
            self.assertEqual(200, st_mh2)
            self.assertIn("id: 4\nevent: event\n", wire_mh2)
            self.assertNotIn("id: 1\nevent: event\n", wire_mh2)

            # Unfiltered pre-v3/legacy collectors with 600 events (honoring OR ignoring limit):
            # - once=1&limit=50 without after_seq MUST NOT emit partial 551..600 events or advance cursor to 600,
            #   and even legacy snapshots MUST clear inline snapshot.events and set events_truncated=True.
            # - continuous stream and after_seq=0 MUST return 501.
            class _UnfilteredCompatCollector:
                proc_root = self.proc_root
                native_home = self.home

                def __init__(self, *, ignore_limit: bool, legacy: bool) -> None:
                    self.ignore_limit = ignore_limit
                    self.legacy = legacy

                def collect(self, **kwargs: Any) -> dict[str, Any]:
                    lim = 50 if self.ignore_limit else int(kwargs.get("limit") or 50)
                    base = coll.collect(limit=10, include_native=False)
                    if self.legacy:
                        base = {
                            "coordinator": base["coordinator"],
                            "hosts": base["hosts"],
                            "sessions": base["sessions"],
                            "metrics": {"active_sessions": base["metrics"]["active_sessions"], "latest_event_seq": 600},
                            "pending_questions": [],
                            "groups": [],
                            "broadcasts": [],
                        }
                    else:
                        base["collector_version"] = 2
                        base["metrics"]["latest_event_seq"] = 600
                    base["events"] = [
                        {
                            "seq": 600 - idx,
                            "session_id": "s-run",
                            "session_name": "claude #s-run",
                            "runtime": "claude",
                            "task_id": "t-u",
                            "kind": "tool_completed",
                            "summary": f"ev {600 - idx}",
                            "created": 100.0,
                            "created_rel": "1s ago",
                        }
                        for idx in range(min(lim, 600))
                    ]
                    return base

            for compat_c, is_leg in (
                (_UnfilteredCompatCollector(ignore_limit=False, legacy=False), False),
                (_UnfilteredCompatCollector(ignore_limit=True, legacy=False), False),
                (_UnfilteredCompatCollector(ignore_limit=True, legacy=True), True),
            ):
                st_u_once, _, wire_u_once = self._dispatch(compat_c, "GET", "/api/v1/stream?once=1&limit=50", state=state)
                self.assertEqual(200, st_u_once)
                self.assertNotIn("event: event\n", wire_u_once)
                self.assertNotIn("id: 600\n", wire_u_once)
                u_frame = next(f for f in wire_u_once.split("\n\n") if "event: snapshot\n" in f)
                u_data = json.loads(next(ln[6:] for ln in u_frame.splitlines() if ln.startswith("data: ")))
                self.assertEqual([], u_data["events"])
                self.assertTrue(u_data.get("events_truncated"))
                if not is_leg:
                    self.assertIn("id: 0\nevent: snapshot\n", wire_u_once)
                st_u_cur, _, _ = self._dispatch(compat_c, "GET", "/api/v1/stream?once=1&limit=50&after_seq=0", state=state)
                self.assertEqual(501, st_u_cur)
                st_u_cont, _, _ = self._dispatch(compat_c, "GET", "/api/v1/stream?limit=50", state=state)
                self.assertEqual(501, st_u_cont)
                st_ev_cur, _, _ = self._dispatch(compat_c, "GET", "/api/v1/events?after_seq=0&limit=50", state=state)
                self.assertEqual(501, st_ev_cur)
                st_ev_nc, _, body_ev_nc = self._dispatch(compat_c, "GET", "/api/v1/events?limit=50", state=state)
                self.assertEqual(200, st_ev_nc)
                ev_nc_data = json.loads(body_ev_nc)
                self.assertEqual([], ev_nc_data["events"])
                self.assertEqual(0, ev_nc_data["next_seq"])
                self.assertTrue(ev_nc_data.get("events_truncated"))

            # Real MonitorCollector proxying a v2 broker over agent_monitor IPC MUST preserve
            # pre-filter truncation evidence across filter_snapshot_dict (e.g. after_seq=600 -> 0 or status=running -> 2 < limit=5).
            proxy_sock = state / "broker.sock"
            proxy_sock.touch()
            try:
                proxy_coll = MonitorCollector(state, proc_root=self.proc_root, native_home=self.home)

                async def _fake_v2_trunc_broker(_state: Any, method: str, rpc_args: dict[str, Any]) -> dict[str, Any]:
                    self.assertEqual("agent_monitor", method)
                    base = coll.collect(limit=10, include_native=False)
                    base["collector_version"] = 2
                    base["metrics"]["latest_event_seq"] = 600
                    base["events"] = [
                        {
                            "seq": 600 - idx,
                            "session_id": "s-run" if idx in (1, 2) else "s-stop",
                            "session_name": f"claude #{'s-run' if idx in (1, 2) else 's-stop'}",
                            "runtime": "claude",
                            "task_id": "t-p",
                            "kind": "tool_completed",
                            "summary": f"proxied {600 - idx}",
                            "created": 100.0,
                            "created_rel": "1s ago",
                        }
                        for idx in range(50)
                    ]
                    return base

                with patch("agent_bridge.broker.request", side_effect=_fake_v2_trunc_broker):
                    st_p_501, _, _ = self._dispatch(
                        proxy_coll, "GET", "/api/v1/stream?once=1&after_seq=600&limit=50", state=state
                    )
                    self.assertEqual(501, st_p_501)
                    st_p_once, _, wire_p_once = self._dispatch(
                        proxy_coll, "GET", "/api/v1/stream?once=1&status=running&limit=5", state=state
                    )
                    self.assertEqual(200, st_p_once)
                    self.assertNotIn("event: event\n", wire_p_once)
                    p_frame = next(f for f in wire_p_once.split("\n\n") if "event: snapshot\n" in f)
                    p_data = json.loads(next(ln[6:] for ln in p_frame.splitlines() if ln.startswith("data: ")))
                    self.assertEqual([], p_data["events"])
                    self.assertTrue(p_data.get("events_truncated"))
                    st_pe_501, _, _ = self._dispatch(
                        proxy_coll, "GET", "/api/v1/events?after_seq=0&limit=50", state=state
                    )
                    self.assertEqual(501, st_pe_501)
                    st_pe_200, _, body_pe_200 = self._dispatch(
                        proxy_coll, "GET", "/api/v1/events?limit=50", state=state
                    )
                    self.assertEqual(200, st_pe_200)
                    pe_data = json.loads(body_pe_200)
                    self.assertEqual([], pe_data["events"])
                    self.assertEqual(0, pe_data["next_seq"])
                    self.assertTrue(pe_data.get("events_truncated"))

                # Non-truncated v2 broker whose running events sit in the middle ([4(stop), 3(run), 2(run), 1(stop)])
                # MUST NOT be falsely flagged truncated by compat_ignored_limit after MonitorCollector filters status=running.
                async def _fake_v2_mid_broker(_state: Any, method: str, rpc_args: dict[str, Any]) -> dict[str, Any]:
                    self.assertEqual("agent_monitor", method)
                    lim = int(rpc_args.get("limit") or 50)
                    base = coll.collect(limit=10, include_native=False)
                    base["collector_version"] = 2
                    base["metrics"]["latest_event_seq"] = 4
                    evs = [
                        {"seq": 4, "session_id": "s-stop", "session_name": "claude #s-stop", "runtime": "claude", "task_id": "t4", "kind": "note", "summary": "4", "created": 104.0, "created_rel": "1s ago"},
                        {"seq": 3, "session_id": "s-run", "session_name": "claude #s-run", "runtime": "claude", "task_id": "t3", "kind": "note", "summary": "3", "created": 103.0, "created_rel": "2s ago"},
                        {"seq": 2, "session_id": "s-run", "session_name": "claude #s-run", "runtime": "claude", "task_id": "t2", "kind": "note", "summary": "2", "created": 102.0, "created_rel": "3s ago"},
                        {"seq": 1, "session_id": "s-stop", "session_name": "claude #s-stop", "runtime": "claude", "task_id": "t1", "kind": "note", "summary": "1", "created": 101.0, "created_rel": "4s ago"},
                    ]
                    base["events"] = evs[:lim]
                    return base

                with patch("agent_bridge.broker.request", side_effect=_fake_v2_mid_broker):
                    st_pm, _, wire_pm = self._dispatch(
                        proxy_coll, "GET", "/api/v1/stream?once=1&status=running&limit=1", state=state
                    )
                    self.assertEqual(200, st_pm)
                    self.assertIn("id: 3\nevent: event\n", wire_pm)
                    st_pme, _, body_pme = self._dispatch(
                        proxy_coll, "GET", "/api/v1/events?after_seq=0&limit=1", state=state
                    )
                    self.assertEqual(200, st_pme)
                    pme = json.loads(body_pme)
                    self.assertEqual([1], [e["seq"] for e in pme["events"]])
                    self.assertEqual(1, pme["next_seq"])
            finally:
                proxy_sock.unlink(missing_ok=True)

            st_r_all, _, wire_r_all = self._dispatch(
                coll, "GET", "/api/v1/stream?once=1&status=running&after_seq=0&limit=10", state=state
            )
            self.assertEqual(200, st_r_all)
            self.assertIn("id: 2\nevent: event\n", wire_r_all)
            self.assertIn("id: 4\nevent: cursor\n", wire_r_all)
            self.assertNotIn("id: 1\nevent: event\n", wire_r_all)
            self.assertNotIn("id: 3\nevent: event\n", wire_r_all)
            self.assertNotIn("id: 4\nevent: event\n", wire_r_all)

            # Combined status=running & session_limit=1 with 2 running sessions + stopped events:
            # MUST still deliver events from BOTH running sessions, exclude stopped events, and cap snapshot.sessions at 1
            # even when a collector ignores status and session_limit.
            with store.connection(write=True) as db:
                db.execute(
                    "INSERT INTO sessions(id,profile,cwd,access,desired,observed,worker_pid,created) VALUES(?,?,?,'read','open','running',?,?)",
                    ("s-run2", "claude-opus", "/tmp", os.getpid(), 105.0),
                )
                db.execute(
                    "INSERT INTO events(session_id,task_id,attempt_id,kind,data,created) VALUES(?,?,?,?,?,?)",
                    ("s-run2", "t5", "a5", "tool_started", json.dumps({"tool": "bash", "command": "pwd"}), 105.0),
                )

            for test_coll in (coll, _LeakCollector()):
                st_sl, _, wire_sl = self._dispatch(
                    test_coll,
                    "GET",
                    "/api/v1/stream?once=1&status=running&session_limit=1&after_seq=0&limit=10",
                    state=state,
                )
                self.assertEqual(200, st_sl)
                snap_frame = next(f for f in wire_sl.split("\n\n") if "event: snapshot\n" in f)
                snap_data = json.loads(next(ln[6:] for ln in snap_frame.splitlines() if ln.startswith("data: ")))
                self.assertEqual(1, len(snap_data["sessions"]))
                self.assertIn("id: 2\nevent: event\n", wire_sl)
                self.assertIn("id: 5\nevent: event\n", wire_sl)
                self.assertNotIn("id: 1\nevent: event\n", wire_sl)
                self.assertNotIn("id: 3\nevent: event\n", wire_sl)
                self.assertNotIn("id: 4\nevent: event\n", wire_sl)

                # Same status=running & session_limit=1 check on /api/snapshot directly!
                st_snap_sl, _, body_snap_sl = self._dispatch(
                    test_coll,
                    "GET",
                    "/api/snapshot?status=running&session_limit=1&after_seq=0&limit=10",
                    state=state,
                )
                self.assertEqual(200, st_snap_sl)
                snap_sl_data = json.loads(body_snap_sl)
                self.assertEqual(1, len(snap_sl_data["sessions"]))
                self.assertEqual([2, 5], [e["seq"] for e in snap_sl_data["events"]])

            # v2 collector returning FEWER events than requested limit (fixed 20 of 40 or 20 of 600)
            # MUST still be detected as truncated on direct AND proxied /api/v1/events, /api/v1/stream, and /api/snapshot;
            # and /api/snapshot MUST surface events_truncated=True on direct truncated collectors.
            class _FixedSmallV2Collector:
                proc_root = self.proc_root
                native_home = self.home

                def __init__(self, total_latest: int) -> None:
                    self.total_latest = total_latest

                def collect(self, **kwargs: Any) -> dict[str, Any]:
                    base = coll.collect(limit=10, include_native=False)
                    base["collector_version"] = 2
                    base["metrics"]["latest_event_seq"] = self.total_latest
                    base["events"] = [
                        {
                            "seq": self.total_latest - idx,
                            "session_id": "s-run",
                            "session_name": "claude #s-run",
                            "runtime": "claude",
                            "task_id": "t-fs",
                            "kind": "note",
                            "summary": f"fs {self.total_latest - idx}",
                            "created": 100.0,
                            "created_rel": "1s ago",
                        }
                        for idx in range(20)
                    ]
                    return base

            class _GappedSmallV2Collector:
                proc_root = self.proc_root
                native_home = self.home

                def collect(self, **kwargs: Any) -> dict[str, Any]:
                    base = coll.collect(limit=10, include_native=False)
                    base["collector_version"] = 2
                    base["metrics"]["latest_event_seq"] = 600
                    seqs = [600] + list(range(19, 0, -1))
                    base["events"] = [
                        {
                            "seq": seq,
                            "session_id": "s-run",
                            "session_name": "claude #s-run",
                            "runtime": "claude",
                            "task_id": "t-gap",
                            "kind": "note",
                            "summary": f"gap {seq}",
                            "created": 100.0,
                            "created_rel": "1s ago",
                        }
                        for seq in seqs
                    ]
                    return base

            def _assert_truncated_v2_behavior(target_coll: Any, max_seq: int) -> None:
                st_fse, _, _ = self._dispatch(target_coll, "GET", "/api/v1/events?after_seq=0&limit=50", state=state)
                self.assertEqual(501, st_fse)
                st_fe_nc, _, body_fe_nc = self._dispatch(target_coll, "GET", "/api/v1/events?limit=50", state=state)
                self.assertEqual(200, st_fe_nc)
                fe_nc = json.loads(body_fe_nc)
                self.assertEqual([], fe_nc["events"])
                self.assertEqual(0, fe_nc["next_seq"])
                self.assertTrue(fe_nc.get("events_truncated"))
                st_fss, _, _ = self._dispatch(target_coll, "GET", "/api/v1/stream?once=1&after_seq=0&limit=50", state=state)
                self.assertEqual(501, st_fss)
                st_fs_nc, _, wire_fs_nc = self._dispatch(target_coll, "GET", "/api/v1/stream?once=1&limit=50", state=state)
                self.assertEqual(200, st_fs_nc)
                self.assertNotIn("event: event\n", wire_fs_nc)
                self.assertNotIn(f"id: {max_seq}\n", wire_fs_nc)
                fs_frame = next(f for f in wire_fs_nc.split("\n\n") if "event: snapshot\n" in f)
                fs_data = json.loads(next(ln[6:] for ln in fs_frame.splitlines() if ln.startswith("data: ")))
                self.assertEqual([], fs_data["events"])
                self.assertTrue(fs_data.get("events_truncated"))
                st_fs_snap, _, body_fs_snap = self._dispatch(target_coll, "GET", "/api/snapshot?limit=50", state=state)
                self.assertEqual(200, st_fs_snap)
                self.assertTrue(json.loads(body_fs_snap).get("events_truncated"))

            for direct_v2, max_seq in (
                (_FixedSmallV2Collector(40), 40),
                (_FixedSmallV2Collector(600), 600),
                (_GappedSmallV2Collector(), 600),
            ):
                _assert_truncated_v2_behavior(direct_v2, max_seq)

            # /api/snapshot on direct overflow collector MUST surface events_truncated=True.
            st_ov_snap, _, body_ov_snap = self._dispatch(
                _OverflowLeakCollector(), "GET", "/api/snapshot?status=running&limit=1", state=state
            )
            self.assertEqual(200, st_ov_snap)
            self.assertTrue(json.loads(body_ov_snap).get("events_truncated"))

            # Real MonitorCollector proxying a v3 broker with status=running & session_limit=1
            # MUST NOT drop second running session's events during proxy re-filtering,
            # AND proxied fixed-20 / gapped v2 brokers MUST preserve truncation across all REST/SSE endpoints.
            proxy_sock2 = state / "broker.sock"
            proxy_sock2.touch()
            try:
                proxy_coll2 = MonitorCollector(state, proc_root=self.proc_root, native_home=self.home)

                async def _fake_v3_broker(_state: Any, method: str, rpc_args: dict[str, Any]) -> dict[str, Any]:
                    self.assertEqual("agent_monitor", method)
                    return coll.collect(
                        limit=int(rpc_args.get("limit") or 25),
                        include_events=bool(rpc_args.get("include_events", True)),
                        runtime=rpc_args.get("runtime"),
                        source=rpc_args.get("source"),
                        status=rpc_args.get("status"),
                        session_limit=rpc_args.get("session_limit"),
                        include_native=bool(rpc_args.get("include_native", True)),
                        after_seq=rpc_args.get("after_seq"),
                    )

                with patch("agent_bridge.broker.request", side_effect=_fake_v3_broker):
                    direct_px = proxy_coll2.collect(status="running", session_limit=1, after_seq=0, limit=10)
                    self.assertEqual(1, len(direct_px["sessions"]))
                    self.assertEqual([2, 5], [e["seq"] for e in direct_px["events"]])

                    st_px_snap, _, body_px_snap = self._dispatch(
                        proxy_coll2, "GET", "/api/snapshot?status=running&session_limit=1&after_seq=0&limit=10", state=state
                    )
                    self.assertEqual(200, st_px_snap)
                    px_snap = json.loads(body_px_snap)
                    self.assertEqual(1, len(px_snap["sessions"]))
                    self.assertEqual([2, 5], [e["seq"] for e in px_snap["events"]])

                for total_n, inner in ((40, _FixedSmallV2Collector(40)), (600, _FixedSmallV2Collector(600)), (600, _GappedSmallV2Collector())):
                    async def _fake_v2_proxy(_state: Any, method: str, rpc_args: dict[str, Any], _c: Any = inner) -> dict[str, Any]:
                        return _c.collect(**rpc_args)

                    with patch("agent_bridge.broker.request", side_effect=_fake_v2_proxy):
                        _assert_truncated_v2_behavior(proxy_coll2, total_n)

                # If the second widening IPC call times out/fails or returns a malformed payload
                # (e.g. partial {"sessions": []}, wrong types, or invalid inner fields),
                # MonitorCollector MUST keep the online hosts, sessions, and metrics from the
                # first successful reply and mark events_truncated=True without raising.
                async def _make_first_v2_reply() -> dict[str, Any]:
                    base = coll.collect(limit=10, include_native=False)
                    base["collector_version"] = 2
                    base["hosts"] = [
                        {
                            "host_id": "remote-1",
                            "state": "online",
                            "ssh_target": "remote-1",
                            "remote_executable": "/bin/bridge",
                            "projects": {},
                            "node_id": "n1",
                            "state_generation": "g1",
                            "last_contact": 100.0,
                            "last_contact_rel": "1s ago",
                            "protocol_version": 1,
                            "detail": None,
                        }
                    ]
                    base["metrics"]["latest_event_seq"] = 600
                    base["events"] = [
                        {
                            "seq": 600 - idx,
                            "session_id": "s-run",
                            "session_name": "claude #s-run",
                            "runtime": "claude",
                            "task_id": "t-wt",
                            "kind": "note",
                            "summary": f"wt {600 - idx}",
                            "created": 100.0,
                            "created_rel": "1s ago",
                        }
                        for idx in range(50)
                    ]
                    return base

                bad_wide_replies: list[Any] = [
                    TimeoutError("wide IPC timed out"),
                    {"sessions": []},
                    {"coordinator": {}, "hosts": [], "sessions": "not-a-list", "events": [], "metrics": {}},
                    {
                        "coordinator": {},
                        "hosts": [{"host_id": "remote-1", "state": []}],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [{"id": 123}],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [{"seq": "not-an-int"}],
                        "events_truncated": True,
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600, "online_hosts": "many"},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                        "runtimes": {"claude": {"bridge_running": []}},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                        "broadcasts": [{}],
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [{"seq": 1}],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                        "runtimes": {"claude": {"bridge_running": None}},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                        "broadcasts": None,
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                        "native_sessions": None,
                    },
                    {
                        "timestamp": 10**400,
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [{"id": "s1", "cost_usd": 10**400}],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {"db_size": 10**400},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [{"id": "s1", "created": 10**400}],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                        "groups": [{"group_id": "g1", "extra_nested": {"huge": 10**400}}],
                    },
                    {
                        "coordinator": {},
                        "hosts": [{"host_id": "remote-1", "state": "online", "last_contact": -1.7e308}],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [{"id": "s1", "created": -1.7e308}],
                        "events": [],
                        "metrics": {"latest_event_seq": 600},
                    },
                    {
                        "coordinator": {},
                        "hosts": [],
                        "sessions": [],
                        "events": [],
                        "metrics": {"latest_event_seq": 600, "total_cost_usd": 1.7e308},
                    },
                ]
                for bad_wide in bad_wide_replies:
                    async def _fake_v2_wide_bad(
                        _state: Any,
                        method: str,
                        rpc_args: dict[str, Any],
                        _bad: Any = bad_wide,
                    ) -> dict[str, Any]:
                        if int(rpc_args.get("limit") or 0) > 50:
                            if isinstance(_bad, BaseException):
                                raise _bad
                            return _bad
                        return await _make_first_v2_reply()

                    with patch("agent_bridge.broker.request", side_effect=_fake_v2_wide_bad):
                        wt_snap = proxy_coll2.collect(limit=50, include_events=True, include_native=False)
                        self.assertTrue(wt_snap.get("events_truncated"))
                        self.assertEqual(
                            [{"host_id": "remote-1", "state": "online"}],
                            [{"host_id": h["host_id"], "state": h["state"]} for h in wt_snap["hosts"]],
                        )
                        self.assertEqual(600, wt_snap["metrics"]["latest_event_seq"])
                        self.assertGreater(len(wt_snap["sessions"]), 0)
                        self.assertIn("remote-1", render_tui(wt_snap, width=100))
                        self.assertGreater(len(unified_sessions(wt_snap)), 0)
            finally:
                proxy_sock2.unlink(missing_ok=True)

            # Concurrent ConfigHostRegistry.replace_hosts + MonitorCollector.collect MUST NOT race or drop hosts,
            # even when replace_hosts completes strictly mid-flight inside an active collect() call.
            hosts_a = {
                "h1": HostConfig(host_id="h1", ssh_target="h1", remote_executable="/bin/bridge"),
                "h2": HostConfig(host_id="h2", ssh_target="h2", remote_executable="/bin/bridge"),
            }
            hosts_b = {
                "h1": HostConfig(host_id="h1", ssh_target="h1b", remote_executable="/bin/bridge"),
                "h3": HostConfig(host_id="h3", ssh_target="h3", remote_executable="/bin/bridge"),
            }
            stop_race = threading.Event()
            mid_collect_entered = threading.Event()
            mid_collect_reloaded = threading.Event()
            race_errors: list[BaseException] = []
            reload_count = 0

            class _SyncTestRegistry(ConfigHostRegistry):
                def __init__(self, initial: dict[str, HostConfig]) -> None:
                    super().__init__(initial)
                    self._snap_calls = 0

                def snapshot(self) -> list[tuple[HostConfig, Any]]:
                    snap = super().snapshot()
                    if self._snap_calls == 0:
                        self._snap_calls += 1
                        mid_collect_entered.set()
                        if not mid_collect_reloaded.wait(timeout=2.0):
                            raise AssertionError("reloader did not execute mid-collect swap in time")
                    else:
                        self._snap_calls += 1
                    return snap

            reg = _SyncTestRegistry(hosts_a)
            race_coll = MonitorCollector(state, host_registry=reg, proc_root=self.proc_root, native_home=self.home)

            def _reload_loop() -> None:
                nonlocal reload_count
                try:
                    if not mid_collect_entered.wait(timeout=2.0):
                        return
                    reg.replace_hosts(hosts_b)
                    reload_count = 1
                    mid_collect_reloaded.set()
                    idx = 2
                    while not stop_race.is_set():
                        reg.replace_hosts(hosts_a if idx % 2 == 0 else hosts_b)
                        idx += 1
                        reload_count = idx
                        time.sleep(0.0002)
                except BaseException as exc:  # pragma: no cover - asserted empty below
                    race_errors.append(exc)
                    mid_collect_reloaded.set()

            reloader = threading.Thread(target=_reload_loop)
            reloader.start()
            reloads_during = 0
            try:
                first_snap = race_coll.collect(limit=1, include_events=False, include_native=False)
                self.assertTrue(mid_collect_reloaded.is_set())
                self.assertEqual(
                    {("h1", "h1"), ("h2", "h2")},
                    {(h["host_id"], h["ssh_target"]) for h in first_snap["hosts"]},
                )
                reloads_before = reload_count
                collect_iters = 0
                deadline = time.monotonic() + 5.0
                while collect_iters < 80 or ((reload_count - reloads_before) < 10 and time.monotonic() < deadline):
                    r_snap = race_coll.collect(limit=1, include_events=False, include_native=False)
                    self.assertEqual(3, r_snap["metrics"]["total_hosts"])
                    got_pairs = {(h["host_id"], h["ssh_target"]) for h in r_snap["hosts"]}
                    self.assertIn(got_pairs, ({("h1", "h1"), ("h2", "h2")}, {("h1", "h1b"), ("h3", "h3")}))
                    collect_iters += 1
                reloads_during = reload_count - reloads_before
            finally:
                stop_race.set()
                reloader.join(timeout=5.0)
            self.assertTrue(mid_collect_reloaded.is_set())
            self.assertGreaterEqual(reloads_during, 10)
            self.assertEqual([], race_errors)

    def test_cli_web_rejects_terminal_filters(self) -> None:
        with patch("sys.stderr"):
            with self.assertRaises(SystemExit) as ctx:
                cli_main(["monitor", "--web", "--runtime", "agy"])
        self.assertEqual(2, ctx.exception.code)

    def test_sse_filtered_full_page_advances_cursor_and_stagnant_compat_sleeps(self) -> None:
        self._seed_store()
        sse_state = self.root / "sse-spin-state"
        sse_state.mkdir()
        (sse_state / "admin.token").write_text(TOKEN)
        (sse_state / "admin.token").chmod(0o600)
        store = Store(sse_state)
        now = time.time()
        with store.connection(write=True) as db:
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,desired,observed,revision,created) VALUES(?,?,?,?,?,?,?,?)",
                ("s-codex", json.dumps({"runtime": "codex", "model": "gpt-6.1"}), "/repo", "read-only", "open", "running", 1, now),
            )
            db.execute(
                "INSERT INTO sessions(id,profile,cwd,access,desired,observed,revision,created) VALUES(?,?,?,?,?,?,?,?)",
                ("s-agy", json.dumps({"runtime": "agy", "model": "claude-sonnet-5"}), "/repo", "read-only", "open", "running", 1, now),
            )

        real_coll = MonitorCollector(sse_state, proc_root=self.proc_root, native_home=self.home)
        template = real_coll.collect(limit=10, include_native=False)
        calls: list[int | None] = []
        base_sessions = [s for s in template.get("sessions", []) if s["id"] == "s-agy"]

        class _PagedCollector:
            proc_root = self.proc_root
            native_home = self.home

            def collect(self, **kwargs: Any) -> dict[str, Any]:
                after = kwargs.get("after_seq")
                calls.append(after)
                base = {
                    **template,
                    "collector_version": 3,
                    "events_truncated": False,
                    "sessions": list(base_sessions),
                    "metrics": {**template["metrics"], "latest_event_seq": 10},
                }
                if (after or 0) == 0:
                    # Normalized "agy" passes filter_snapshot_dict ("antigravity") into raw_events
                    # then fails line-188 raw equality check, exercising full-page empty-events cursor advance.
                    base["events"] = [
                        {
                            "seq": 1,
                            "session_id": "s-codex",
                            "session_name": "agy #s-codex",
                            "runtime": "agy",
                            "task_id": "t1",
                            "kind": "tool_completed",
                            "summary": "e1",
                            "created": now,
                            "created_rel": "1s ago",
                        },
                        {
                            "seq": 2,
                            "session_id": "s-codex",
                            "session_name": "agy #s-codex",
                            "runtime": "agy",
                            "task_id": "t2",
                            "kind": "tool_completed",
                            "summary": "e2",
                            "created": now + 1,
                            "created_rel": "1s ago",
                        },
                    ]
                elif after == 2:
                    base["events"] = [
                        {
                            "seq": 3,
                            "session_id": "s-agy",
                            "session_name": "agy #s-agy",
                            "runtime": "antigravity",
                            "task_id": "t3",
                            "kind": "tool_completed",
                            "summary": "e3",
                            "created": now + 2,
                            "created_rel": "1s ago",
                        }
                    ]
                else:
                    base["events"] = []
                return base

        status, _, wire = self._dispatch(
            _PagedCollector(),
            "GET",
            "/api/v1/stream?runtime=agy&limit=2&interval=0.1&max_seconds=0.35&max_events=5",
            extra_headers={"Last-Event-ID": "0"},
            state=sse_state,
        )
        self.assertEqual(200, status)
        # First poll hits full filtered-out page (seq 1..2), advances cursor immediately to seq 2 (not latest=10), and emits seq 3.
        self.assertGreaterEqual(len(calls), 2)
        self.assertEqual(0, calls[0])
        self.assertEqual(2, calls[1])
        self.assertIn("id: 3\nevent: event\n", wire)

        # Compat collector whose page ignores after_seq and never advances must sleep rather than busy-spin.
        stagnant_calls = 0

        class _StagnantCompatCollector:
            proc_root = self.proc_root
            native_home = self.home

            def collect(self, **kwargs: Any) -> dict[str, Any]:
                nonlocal stagnant_calls
                stagnant_calls += 1
                base = real_coll.collect(limit=10, include_native=False)
                base["collector_version"] = 3
                base["events_truncated"] = False
                base["sessions"] = list(base_sessions)
                base["metrics"]["latest_event_seq"] = 10
                base["events"] = [
                    {
                        "seq": 1,
                        "session_id": "s-codex",
                        "session_name": "codex #s-codex",
                        "runtime": "codex",
                        "task_id": "t1",
                        "kind": "tool_completed",
                        "summary": "ignored",
                        "created": now,
                        "created_rel": "1s ago",
                    },
                    {
                        "seq": 2,
                        "session_id": "s-codex",
                        "session_name": "codex #s-codex",
                        "runtime": "codex",
                        "task_id": "t2",
                        "kind": "tool_completed",
                        "summary": "ignored",
                        "created": now,
                        "created_rel": "1s ago",
                    },
                ]
                return base

        st2, _, _ = self._dispatch(
            _StagnantCompatCollector(),
            "GET",
            "/api/v1/stream?runtime=claude&limit=2&interval=0.1&max_seconds=0.3&max_events=5",
            extra_headers={"Last-Event-ID": "0"},
            state=sse_state,
        )
        self.assertEqual(200, st2)
        self.assertLessEqual(stagnant_calls, 5)


if __name__ == "__main__":
    unittest.main()
