from __future__ import annotations

import asyncio
import fcntl
import json
import os
import signal
import subprocess
import time
from pathlib import Path

from .config import Config, Profile, safe_error
from .errors import BridgeError
from .runtimes import runtime
from .store import Store, identifier, validate_task_timeout
from .telegram_guard import register_child
from .usage import priced_usage
from .workspace import capture_patch


def process_identity(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        if stat[0] == "Z":
            return None
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip() + ":" + stat[19]
    except (OSError, IndexError):
        try:
            os.kill(pid, 0)
            output = subprocess.check_output(
                ["ps", "-p", str(pid), "-o", "lstart="],
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
            if not output:
                return None
            return f"darwin:{pid}:{output}"
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
    def __init__(self, state: Path, config: Config, session_id: str):
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
        self.interrupt_done = asyncio.Event()
        self.interrupt_done.set()
        self.read_only = False

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
        if kind == "usage_updated":
            self.store.progress_usage(
                self.session_id, self.generation, self.task["id"], self.task["active_attempt"], data
            )
            return
        self.store.record(kind, data, self.session_id, self.task["id"], self.task["active_attempt"])

    async def watch_control(self, owner):
        interrupted_at = None
        interrupted_attempt = None
        while True:
            await asyncio.sleep(0.15)
            session = self.store.get_session(self.session_id)
            self.read_only = session["access"] == "read-only"
            task = self.store.get_task(self.task["id"]) if self.task else None
            attempt = self.task["active_attempt"] if self.task else None
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
                    await asyncio.wait_for(self.agent.interrupt(), self.config.interrupt_timeout)
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
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            pid, start = os.getpid(), process_identity(os.getpid())
            scope = process_scope(pid) if self.config.worker_launcher == "systemd" else None
            if not self.store.worker_starting(self.session_id, self.generation, pid, start, scope):
                return
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
                register_child(self.config.telegram_database, profile.runtime, self.agent.native_id)
                self.store.worker_ready(
                    self.session_id, self.generation, pid, start, self.agent.native_id, task_timeout_supported=True,
                )
                monitor = asyncio.create_task(self.watch_control(asyncio.current_task()))
                while not self.shutdown:
                    # Pi's abort is untargeted; it must settle before another prompt starts.
                    await self.interrupt_done.wait()
                    session = self.store.get_session(self.session_id)
                    if self.shutdown or session["desired"] != "open":
                        break
                    self.task = self.store.begin_attempt(self.session_id, self.generation)
                    if not self.task:
                        await asyncio.sleep(0.15)
                        continue
                    self.stop_reason, self.started = None, time.monotonic()
                    self.termination, self.termination_attempt = None, None
                    details = self.task["details"]
                    details = json.loads(details) if isinstance(details, str) else details
                    override = validate_task_timeout(details.get("task_timeout"))
                    self.task_timeout = self.config.task_timeout if override is None else override
                    self.native_accepted.clear()
                    instruction = self.task["instruction"]
                    previous = self.store.get_task(self.task["id"])["attempts"]
                    if len(previous) > 1:
                        instruction = (
                            "Continue the interrupted task. Inspect existing files and native history before acting; do not repeat completed side effects.\n\n"
                            + instruction
                        )
                    try:
                        status, result, usage = await self.agent.run(instruction, self.started_native, self.event)
                        status, result = self.termination_result(status, result)
                        if session["access"] == "workspace-write":
                            try:
                                result["files"] = capture_patch(
                                    session["cwd"], self.state / (self.task["active_attempt"] + ".patch"), session["workspace"],
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
                self.store.worker_stopped(self.session_id, self.generation, self.last_error)
                if self.last_error:
                    print(json.dumps({"session_id": self.session_id, "error": self.last_error}), flush=True)

    def request_shutdown(self):
        self.shutdown = True
