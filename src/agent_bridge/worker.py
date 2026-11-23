from __future__ import annotations

import asyncio
import contextlib
import ctypes
import fcntl
import json
import locale
import os
import signal
import sqlite3
import subprocess
import sys
import time
from functools import cache
from pathlib import Path

from . import __version__, update_guard
from .config import Config, Profile, safe_error
from .errors import BridgeError
from .runtimes import runtime
from .store import Store, identifier, validate_task_timeout
from .updater import release_change_detector
from .usage import priced_usage
from .workspace import capture_patch

CONTROL_INTERVAL = 0.15
IDLE_INTERVAL = 1.0
IDLE_POLL_THRESHOLD = 3


class _DarwinProcessInfo(ctypes.Structure):
    # struct proc_bsdinfo from sys/proc_info.h, with unused fields grouped by type.
    _fields_ = [
        ("flags_and_status", ctypes.c_uint32 * 3),
        ("pid", ctypes.c_uint32),
        ("parent_user_group_and_reserved", ctypes.c_uint32 * 8),
        ("command_and_name", ctypes.c_char * (16 + 32)),
        ("file_group_tty_and_priority", ctypes.c_uint32 * 6),
        ("started_seconds", ctypes.c_uint64),
        ("started_microseconds", ctypes.c_uint64),
    ]


class _DarwinLocalTime(ctypes.Structure):
    # struct tm from time.h, including the Darwin UTC offset and zone fields.
    _fields_ = [
        ("components", ctypes.c_int * 9),
        ("utc_offset", ctypes.c_long),
        ("zone", ctypes.c_char_p),
    ]


@cache
def _darwin_process_functions():
    try:
        proc_pidinfo = ctypes.CDLL("/usr/lib/libproc.dylib").proc_pidinfo
        proc_pidinfo.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int)
        proc_pidinfo.restype = ctypes.c_int
        libc = ctypes.CDLL(None)
        localtime = libc.localtime_r
        localtime.argtypes = (ctypes.POINTER(ctypes.c_long), ctypes.POINTER(_DarwinLocalTime))
        localtime.restype = ctypes.POINTER(_DarwinLocalTime)
        newlocale = libc.newlocale
        newlocale.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_void_p)
        newlocale.restype = ctypes.c_void_p
        format_time = libc.strftime_l
        format_time.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.POINTER(_DarwinLocalTime), ctypes.c_void_p)
        format_time.restype = ctypes.c_size_t
        freelocale = libc.freelocale
        freelocale.argtypes = (ctypes.c_void_p,)
        freelocale.restype = ctypes.c_int
        return proc_pidinfo, localtime, newlocale, format_time, freelocale
    except (AttributeError, OSError):
        return None


def _darwin_process_start(pid):
    functions = _darwin_process_functions()
    if functions is None:
        return None
    proc_pidinfo, localtime, newlocale, format_time, freelocale = functions
    info = _DarwinProcessInfo()
    size = ctypes.sizeof(info)
    proc_pidtbsdinfo = 3  # PROC_PIDTBSDINFO in sys/proc_info.h
    if proc_pidinfo(pid, proc_pidtbsdinfo, 0, ctypes.byref(info), size) != size or info.pid != pid or not info.started_seconds:
        return None
    started = _DarwinLocalTime()
    seconds = ctypes.c_long(info.started_seconds)
    if not localtime(ctypes.byref(seconds), ctypes.byref(started)):
        return None
    # Match ps -o lstart= and keep the identity stored by older bridge releases.
    lc_all_mask = 63  # LC_ALL_MASK in xlocale.h
    environment_locale = newlocale(lc_all_mask, b"", None)
    if not environment_locale:
        return None
    try:
        output = ctypes.create_string_buffer(80)
        if format_time(output, len(output), b"%c", ctypes.byref(started), environment_locale):
            encoding = "utf-8" if sys.flags.utf8_mode else locale.getencoding()
            return output.value.decode(encoding).strip() or None
    except UnicodeError:
        return None
    finally:
        freelocale(environment_locale)
    return None


def process_identity(pid):
    if sys.platform != "darwin":
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
            if stat[0] == "Z":
                return None
            return Path("/proc/sys/kernel/random/boot_id").read_text().strip() + ":" + stat[19]
        except (OSError, IndexError):
            pass
    try:
        os.kill(pid, 0)
        output = _darwin_process_start(pid) if sys.platform == "darwin" else None
        if output is None:
            output = subprocess.check_output(
                ["ps", "-p", str(pid), "-o", "lstart="],
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
        return f"darwin:{pid}:{output}" if output else None
    except (OSError, subprocess.SubprocessError):
        return None


def alive(session):
    return bool(session["worker_pid"] and process_identity(session["worker_pid"]) == session["worker_start"])


def process_scope(pid):
    for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines():
        if line.startswith("0::"):
            return str(Path("/sys/fs/cgroup") / line[3:].lstrip("/"))
    raise RuntimeError("A systemd worker requires a readable cgroup v2 process list")


def owned_processes(scope):
    if not scope:
        return {"status": "unknown", "processes": [], "external_jobs": "unknown"}
    root = Path(scope)
    processes = []
    try:
        sources = [root / "cgroup.procs", *(p for p in root.rglob("cgroup.procs") if p != root / "cgroup.procs")]
        for source in sources:
            try:
                pids = source.read_text().split()
            except FileNotFoundError:
                if source == root / "cgroup.procs" and root.exists():
                    return {"status": "unknown", "error": "Worker cgroup process list is unavailable", "external_jobs": "unknown"}
                continue
            for value in pids:
                pid = int(value)
                identity = process_identity(pid)
                if identity:
                    try:
                        name = Path(f"/proc/{pid}/comm").read_text().strip()
                    except FileNotFoundError:
                        continue
                    processes.append({"pid": pid, "identity": identity, "name": name})
    except (OSError, ValueError) as error:
        return {"status": "unknown", "error": safe_error(error), "external_jobs": "unknown"}
    return {"status": "running" if processes else "stopped", "processes": processes, "external_jobs": "unknown"}


def denial(data):
    method = data.get("method", "")
    if method == "item/tool/requestUserInput":
        return {"answers": {}}
    if method.endswith("requestApproval"):
        return {"decision": "decline"}
    if method == "item/tool/call":
        return {"success": False, "contentItems": [{"type": "inputText", "text": "No approval or tool handler"}]}
    if data.get("type") == "extension_ui_request":
        return {"cancelled": True}
    return {"decision": "deny"}


class Worker:
    def __init__(self, state: Path, config: Config, session_id: str, *, release_changed=None):
        self.state, self.config, self.session_id = state, config, session_id
        self.store = Store(state)
        self.generation = identifier()
        self.task = None
        self.agent = None
        self.shutdown = False
        self.stop_reason = None
        self.termination = None
        self.termination_attempt = None
        self.started = 0
        self.task_timeout = config.task_timeout
        self.last_error = None
        self.native_accepted = asyncio.Event()
        self._task_active = asyncio.Event()
        self.interrupt_done = asyncio.Event()
        self.interrupt_done.set()
        self.read_only = False
        self.release_changed = release_changed if release_changed is not None else release_change_detector(state)

    def _should_retire_for_release(self) -> bool:
        """Retire only between turns once the release changed and broker maintenance cleared."""
        if not self.release_changed():
            return False
        try:
            return update_guard.maintenance(self.state) is None
        except BridgeError:
            return False

    async def _acquire_session_lock(self, lock) -> bool:
        """Acquire the per-session lock, waiting briefly for a stopping worker handoff."""
        deadline = time.monotonic() + 2.0
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                try:
                    session = self.store.get_session(self.session_id)
                except BridgeError:
                    return False
                if session["worker_pid"] is not None or time.monotonic() >= deadline:
                    return False
                await asyncio.sleep(0.05)

    async def question(self, data):
        if not self.task:
            return denial(data)
        if self.read_only and (data.get("method", "").endswith("requestApproval") or data.get("type") == "permission"):
            return denial(data)
        # Large native inputs stay private in SQLite; they are never copied to logs.
        try:
            expires = time.time() + max(0, self.task_timeout - (time.monotonic() - self.started))
            key = self.store.question(self.session_id, self.task["id"], self.task["active_attempt"], data, expires)
        except BridgeError:
            return denial(data)
        while True:
            question = self.store.get_question(key)
            if question["status"] == "answered":
                return question["answer"]
            if question["status"] != "pending" or self.shutdown or self.stop_reason:
                return denial(data)
            await asyncio.sleep(0.2)

    async def started_native(self, native_turn_id):
        self.store.native_started(self.session_id, self.task["id"], self.task["active_attempt"], native_turn_id)
        self.native_accepted.set()

    async def event(self, kind, data):
        for attempt in range(3):
            try:
                if kind == "usage_updated":
                    self.store.progress_usage(
                        self.session_id, self.generation, self.task["id"], self.task["active_attempt"], data
                    )
                    return
                self.store.record(kind, data, self.session_id, self.task["id"], self.task["active_attempt"])
                return
            except sqlite3.OperationalError as error:
                if attempt < 2:
                    await asyncio.sleep(0.25)
                    continue
                print(
                    json.dumps({"session_id": self.session_id, "warning": f"Transient event write skipped: {safe_error(error)}"}),
                    file=sys.stderr,
                    flush=True,
                )

    async def watch_control(self, owner):
        interrupted_at = None
        interrupted_attempt = None
        while True:
            if self.task is None:
                await self._task_active.wait()
            await asyncio.sleep(CONTROL_INTERVAL)
            current = self.task
            if current is None:
                # The worker loop handles session control while waiting for work.
                continue
            session = self.store.get_session(self.session_id)
            self.read_only = session["access"] == "read-only"
            task = self.store.get_task(current["id"])
            attempt = current["active_attempt"]
            if attempt != interrupted_attempt:
                interrupted_at = None
            reason = None
            cause = None
            if self.shutdown or session["desired"] in {"paused", "closed"}:
                reason = "cancelled" if session["desired"] == "closed" else "interrupted"
                cause = "session_control"
            if task and task["cancel_requested"]:
                reason = "cancelled"
                cause = "task_cancel"
            if task and not reason and time.monotonic() - self.started > self.task_timeout:
                reason = "interrupted"
                cause = "task_timeout"
            if task and not reason and self.config.token_budget is not None:
                usage = self.store.budget()
                if usage["charged_tokens"] + usage["inflight_tokens"] >= self.config.token_budget:
                    reason = "interrupted"
                    cause = "token_budget"
            if task and reason and not self.stop_reason:
                stage = "turn" if self.native_accepted.is_set() else "native_acceptance"
                self.termination = {
                    "stage": stage, "cause": cause,
                    "elapsed_seconds": time.monotonic() - self.started,
                    "task_timeout_seconds": self.task_timeout,
                }
                self.termination_attempt = attempt
                self.store.record("native_wait_ended", self.termination, self.session_id, task["id"], attempt)
                if not self.native_accepted.is_set():
                    self.stop_reason, self.shutdown = reason, True
                    self.last_error = (
                        f"Native task wait ended during {stage} ({cause}); execution outcome is unknown"
                    )
                    owner.cancel()
                    return
                self.stop_reason, interrupted_at = reason, time.monotonic()
                interrupted_attempt = attempt
                self.interrupt_done.clear()
                try:
                    # wait_for can swallow concurrent cancellation on Python 3.11.
                    async with asyncio.timeout(self.config.interrupt_timeout):
                        await self.agent.interrupt()
                except Exception as error:
                    self.last_error = "Native wait failed during interrupt: " + (safe_error(error) or type(error).__name__)
                    self.store.record("interrupt_failed", {"error": self.last_error}, self.session_id, task["id"], attempt)
                    self.shutdown = True
                    if self.task and self.task["active_attempt"] == attempt:
                        owner.cancel()
                    return
                finally:
                    self.interrupt_done.set()
            if (
                interrupted_at and self.task and self.task["active_attempt"] == interrupted_attempt
                and time.monotonic() - interrupted_at > self.config.interrupt_timeout
            ):
                self.last_error = "Native interrupt did not confirm a terminal result before timeout"
                owner.cancel()
                return

    @staticmethod
    def _is_interrupt_related_failure(result):
        if result.get("provider_limit"):
            return False
        requested_model = result.get("requested_model")
        reported_model = result.get("reported_model")
        if requested_model and reported_model and requested_model != reported_model:
            return False
        reason = str(result.get("terminal_reason") or "").lower()
        if reason in {"aborted_streaming", "aborted_tools"}:
            return True
        error = str(result.get("error") or "").strip().lower()
        if not error:
            return False
        return any(token in error for token in ("abort", "interrupt", "cancel"))

    def termination_result(self, status, result):
        if not self.termination or not self.task or self.termination_attempt != self.task["active_attempt"]:
            return status, result
        result = {**result, "termination": dict(self.termination)}
        # A native terminal success wins a race with the interrupt request.
        if status == "completed":
            return status, result
        if self.termination["stage"] == "native_acceptance" or self.shutdown and self.last_error:
            status = "recovery_needed"
        elif status == "failed" and not self._is_interrupt_related_failure(result):
            return status, result
        elif self.stop_reason and status in {"failed", "interrupted", "cancelled"}:
            status = self.stop_reason
        if result.get("error"):
            result["native_error"] = result["error"]
        cause = self.termination["cause"]
        reason = {
            "task_timeout": "total task deadline exceeded",
            "session_control": "session pause/close or worker shutdown requested",
            "task_cancel": "task cancellation requested",
            "token_budget": "configured token budget reached",
        }[cause]
        result["error"] = (
            f"Bridge {cause}: {reason}; stage={self.termination['stage']}; "
            f"elapsed={self.termination['elapsed_seconds']:.3f}s; "
            f"task_timeout={self.termination['task_timeout_seconds']:g}s"
        )
        if status == "recovery_needed":
            result["error"] += "; execution outcome is unknown"
        return status, result

    async def run(self):
        lock_path = self.state / (self.session_id + ".lock")
        registered = False
        try:
            with lock_path.open("a") as lock, contextlib.ExitStack() as resources:
                if not await self._acquire_session_lock(lock):
                    return
                pid, start = os.getpid(), process_identity(os.getpid())
                scope = process_scope(pid) if self.config.worker_launcher == "systemd" else None
                if not self.store.worker_starting(
                    self.session_id, self.generation, pid, start, scope, worker_version=__version__,
                ):
                    return
                registered = True
                session = self.store.get_session(self.session_id)
                with self.store.connection() as db:
                    session["has_native_history"] = bool(
                        db.execute(
                            "SELECT 1 FROM attempts WHERE session_id=? AND started IS NOT NULL", (self.session_id,)
                        ).fetchone()
                    )
                    last = db.execute(
                        "SELECT result FROM attempts WHERE session_id=? AND started IS NOT NULL ORDER BY started DESC,rowid DESC LIMIT 1",
                        (self.session_id,),
                    ).fetchone()
                    if last:
                        session["native_usage_total"] = json.loads(last[0] or "{}").get("native_usage_total")
                self.read_only = session["access"] == "read-only"
                profile = Profile(**session["profile"])
                self.agent = runtime(profile, session, self.state, self.question)
                loop = asyncio.get_running_loop()
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(sig, self.request_shutdown)
                monitor = None
                try:
                    # The SDK's connect, receive and disconnect share one AnyIO task.
                    startup_deadline = asyncio.timeout(self.config.task_timeout)
                    try:
                        async with startup_deadline:
                            await self.agent.connect()
                    except TimeoutError as error:
                        if not startup_deadline.expired():
                            raise TimeoutError(
                                "Native startup failed during connect: " + (safe_error(error) or type(error).__name__)
                            ) from error
                        self.store.record("native_wait_ended", {
                            "stage": "connect", "cause": "task_timeout",
                            "task_timeout_seconds": self.config.task_timeout,
                        }, self.session_id)
                        raise TimeoutError(
                            f"Native startup timed out during connect after {self.config.task_timeout:g}s; no task was dispatched"
                        ) from error
                    self.store.worker_ready(
                        self.session_id,
                        self.generation,
                        pid,
                        start,
                        self.agent.native_id,
                        task_timeout_supported=True,
                        worker_version=__version__,
                    )
                    poll_db = resources.enter_context(self.store.connection())
                    monitor = asyncio.create_task(self.watch_control(asyncio.current_task()))
                    idle_polls = 0
                    while not self.shutdown:
                        # Pi's abort is untargeted; it must settle before another prompt starts.
                        await self.interrupt_done.wait()
                        session = poll_db.execute(
                            "SELECT desired,access,cwd,workspace FROM sessions WHERE id=?", (self.session_id,),
                        ).fetchone()
                        if session is None:
                            raise BridgeError("not_found", f"sessions: {self.session_id}")
                        if self.shutdown or session["desired"] != "open" or self._should_retire_for_release():
                            break
                        self.task = self.store.begin_attempt(self.session_id, self.generation, reuse=poll_db)
                        if not self.task:
                            idle_polls += 1
                            await asyncio.sleep(IDLE_INTERVAL if idle_polls >= IDLE_POLL_THRESHOLD else CONTROL_INTERVAL)
                            continue
                        idle_polls = 0
                        self.read_only = session["access"] == "read-only"
                        self.stop_reason, self.started = None, time.monotonic()
                        self.termination, self.termination_attempt = None, None
                        details = self.task["details"]
                        details = json.loads(details) if isinstance(details, str) else details
                        override = validate_task_timeout(details.get("task_timeout"))
                        self.task_timeout = self.config.task_timeout if override is None else override
                        self.native_accepted.clear()
                        instruction = self.task["instruction"]
                        previous = self.store.get_task(self.task["id"])["attempts"]
                        if any(
                            attempt["id"] != self.task["active_attempt"] and attempt.get("started") is not None
                            for attempt in previous
                        ):
                            instruction = (
                                "Continue the interrupted task. Inspect existing files and native history before acting; do not repeat completed side effects.\n\n"
                                + instruction
                            )
                        self._task_active.set()
                        try:
                            status, result, usage = await self.agent.run(instruction, self.started_native, self.event)
                            status, result = self.termination_result(status, result)
                            if session["access"] == "workspace-write":
                                try:
                                    result["files"] = capture_patch(
                                        session["cwd"], self.state / (self.task["active_attempt"] + ".patch"),
                                        json.loads(session["workspace"]),
                                    )
                                except Exception as error:
                                    result["files"] = {"workspace": session["cwd"], "error": safe_error(error)}
                                    if status == "completed":
                                        status, result["error"] = "failed", "Patch capture failed: " + safe_error(error)
                            self.store.finish(
                                self.session_id,
                                self.generation,
                                self.task["id"],
                                self.task["active_attempt"],
                                status,
                                result,
                                priced_usage(usage, {**profile.price, "model": profile.model} if profile.price else None),
                            )
                        except (BridgeError, OSError, TimeoutError) as error:
                            # A transport failure after send cannot establish whether work ran.
                            self.last_error = safe_error(error) or type(error).__name__
                            if isinstance(error, TimeoutError):
                                stage = "turn" if self.native_accepted.is_set() else "native_acceptance"
                                self.last_error = f"Native wait timed out during {stage}: " + self.last_error
                            status, result = self.termination_result(
                                "recovery_needed",
                                {"error": self.last_error, "provider_limit": getattr(error, "provider_limit", None)},
                            )
                            self.last_error = result["error"]
                            self.store.finish(
                                self.session_id,
                                self.generation,
                                self.task["id"],
                                self.task["active_attempt"],
                                status,
                                result,
                            )
                            break
                        self.task = None
                        self._task_active.clear()
                except (asyncio.CancelledError, Exception) as error:
                    if isinstance(error, asyncio.CancelledError):
                        self.last_error = self.last_error or "Worker was interrupted before a terminal result was confirmed"
                    else:
                        self.last_error = safe_error(error) or type(error).__name__
                    if self.task and (self.termination or isinstance(error, asyncio.CancelledError)):
                        status, result = self.termination_result("recovery_needed", {"error": self.last_error})
                        self.last_error = result["error"]
                        self.store.finish(
                            self.session_id, self.generation, self.task["id"], self.task["active_attempt"], status, result,
                        )
                finally:
                    if monitor:
                        monitor.cancel()
                        await asyncio.gather(monitor, return_exceptions=True)
                    try:
                        async with asyncio.timeout(self.config.interrupt_timeout):
                            await self.agent.close()
                    except Exception as error:
                        self.last_error = self.last_error or "Native cleanup failed: " + (safe_error(error) or type(error).__name__)
        finally:
            if registered:
                # Publish `observed='stopped'` only after `lock` closes and releases `flock`.
                self.store.worker_stopped(self.session_id, self.generation, self.last_error)
                if self.last_error:
                    print(json.dumps({"session_id": self.session_id, "error": self.last_error}), flush=True)

    def request_shutdown(self):
        self.shutdown = True
