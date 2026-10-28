from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import secrets
import signal
import socket
import struct
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

from .config import Config, Profile, safe_error
from .discovery import ModelDirectory
from .errors import BridgeError
from .routing import choose
from .store import Store, encode
from .worker import alive, owned_processes
from .workspace import directory_copy, worktree


def unit_name(state, session_id):
    return "agent-bridge-" + hashlib.sha256(str(state).encode()).hexdigest()[:8] + "-" + session_id + ".service"


class Broker:
    def __init__(self, state: Path):
        self.state = state
        self.config = Config.load(state / "config.json")
        self.store = Store(state)
        self.models = ModelDirectory(state)
        token_path = state / "admin.token"
        if not token_path.exists():
            with token_path.open("x") as token_file:
                token_file.write(secrets.token_urlsafe(32))
            token_path.chmod(0o600)
        self.token = token_path.read_text().strip()
        self.launching = set()
        self.stopping = asyncio.Event()
        self.request_locks = {}
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE deliveries SET status='uncertain',error='Broker restarted before queue acknowledgement' WHERE status='sending'"
            )

    async def notifications(self):
        from .notifications import deliver

        while not self.stopping.is_set():
            await asyncio.to_thread(self.store.refresh_parent_usage)
            await deliver(self.store, self.config)
            try:
                await asyncio.wait_for(self.stopping.wait(), 2)
            except TimeoutError:
                pass

    async def launch(self, session_id):
        args = [sys.executable, "-m", "agent_bridge.cli", "--state", str(self.state), "worker", session_id]
        if self.config.worker_launcher == "systemd":
            proc = await asyncio.create_subprocess_exec(
                "systemd-run",
                "--user",
                "--quiet",
                "--collect",
                "--unit=" + unit_name(self.state, session_id),
                "--property=KillMode=control-group",
                "--property=TimeoutStopSec=45",
                "--property=UMask=0077",
                "--setenv=PATH=" + os.environ.get("PATH", ""),
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode:
                check = await asyncio.create_subprocess_exec(
                    "systemctl",
                    "--user",
                    "is-active",
                    unit_name(self.state, session_id),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await check.communicate()
                if check.returncode:
                    raise BridgeError("worker_start_failed", safe_error(stderr.decode()))
        else:
            # shortcut: detached mode is for tests; systemd is required for crash descendant cleanup.
            log = (self.state / (session_id + ".log")).open("ab")
            try:
                subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
            finally:
                log.close()
        for _ in range(100):
            session = self.store.get_session(session_id)
            if session["worker_pid"] or session["observed"] == "recovery_needed":
                return
            await asyncio.sleep(0.1)
        raise BridgeError("worker_start_failed", "Worker did not register within 10 seconds")

    async def ensure_worker(self, session_id):
        try:
            await self.launch(session_id)
        except Exception as error:
            self.store.worker_start_failed(session_id, safe_error(error) or type(error).__name__)
        finally:
            self.launching.discard(session_id)

    async def scheduler(self):
        pending = set()
        try:
            while not self.stopping.is_set():
                for session in self.store.sessions():
                    if session["worker_pid"] and not alive(session):
                        self.store.worker_stopped(session["id"], session["generation"], "Worker process disappeared")
                    elif (
                        session["desired"] == "open"
                        and session["observed"] == "stopped"
                        and session["id"] not in self.launching
                    ):
                        self.launching.add(session["id"])
                        task = asyncio.create_task(self.ensure_worker(session["id"]))
                        pending.add(task)
                        task.add_done_callback(pending.discard)
                while self.store.claim(self.config):
                    pass
                await asyncio.sleep(0.15)
        finally:
            await asyncio.gather(*pending, return_exceptions=True)

    async def call(self, method, args):
        if method == "agent_catalog":
            if not isinstance(args.get("refresh", False), bool):
                raise BridgeError("invalid_request", "refresh must be a boolean")
            name = args.get("profile")
            if name is not None and name not in self.config.profiles:
                raise BridgeError("unknown_profile", str(name))
            profiles = [p for p in self.config.profiles.values() if name is None or p.name == name]
            discoveries = await asyncio.gather(*(self.models.get(p, args.get("refresh", False)) for p in profiles))
            return {
                "profiles": [p.public() for p in profiles],
                "discovery": discoveries,
                "objective": self.config.default_strategy,
                "strategies": ["balanced", "cost", "speed"],
                "parallel_limit": self.config.parallel_limit,
                "quota_limit": self.config.quota_limit,
                "quota_cooldowns": self.store.quotas(),
                "price_notice": "Catalog estimates are not confirmed bills",
                "model_directory": "Discovered IDs can be selected with profile + model; directory visibility is not inference verification. Cached for 300 seconds; refresh=true queries again. Automatic scoring still uses verified configured profiles.",
            }
        if method == "agent_open":
            name = args["profile"]
            if name not in self.config.profiles:
                raise BridgeError("unknown_profile", name)
            profile = await self.models.resolve(self.config.profiles[name], args.get("model"))
            profile.command()
            cwd = args["cwd"]
            metadata = None
            access = args.get("access", "read-only")
            if access not in {"read-only", "workspace-write"}:
                raise BridgeError("invalid_access", "access must be read-only or workspace-write")
            if profile.runtime == "antigravity" and access != "workspace-write":
                raise BridgeError("unsupported_access", "Antigravity requires workspace-write; plan mode is not read-only enforcement")
            mode = args.get("workspace_mode", "git")
            if mode not in {"git", "copy"} or access == "read-only" and (mode != "git" or args.get("snapshot_files")):
                raise BridgeError(
                    "invalid_workspace", "Snapshots/copies require workspace-write; workspace_mode is git or copy"
                )
            if access == "workspace-write":
                if mode == "copy":
                    if args.get("base", "HEAD") != "HEAD" or args.get("snapshot_files") is None:
                        raise BridgeError(
                            "invalid_workspace", "Copy mode needs an explicit snapshot_files list and no Git base"
                        )
                    cwd, metadata = await asyncio.to_thread(
                        directory_copy, self.state, cwd, args["request_id"], args["snapshot_files"]
                    )
                else:
                    cwd, metadata = await asyncio.to_thread(
                        worktree,
                        self.state,
                        cwd,
                        args["request_id"],
                        args.get("base", "HEAD"),
                        args.get("snapshot_files"),
                    )
            result = self.store.open(profile, cwd, access, args["request_id"], args.get("parent"), metadata)
            return {**result, "cwd": cwd, "workspace": metadata, "profile": profile.public()}
        if method == "agent_dispatch":
            requirements = args.get("requirements")
            if requirements is None:
                requirements = {}
            if not isinstance(requirements, dict):
                raise BridgeError("invalid_request", "requirements must be an object")
            if requirements.get("hard_budget"):
                raise BridgeError("hard_budget_unsupported", "No verified hard cap is available")
            reservation = self.store.validate_dispatch(
                args["instruction"], args.get("reservation"), args.get("dependencies"), args.get("retry_task_id")
            )
            if self.config.token_budget is not None and reservation <= 0:
                raise BridgeError("reservation_required", "A positive token reservation is required when a shared budget is configured")
            access = args.get("access")
            if access is not None and access not in {"read-only", "workspace-write"}:
                raise BridgeError("invalid_access", "access must be read-only or workspace-write")
            selection = None
            opened = None
            session_id = args.get("session_id")
            if not session_id:
                config = self.config
                if args.get("model") is not None:
                    name = args.get("profile")
                    if name not in self.config.profiles:
                        raise BridgeError(
                            "unknown_profile", "Select a configured profile together with an exact model ID"
                        )
                    selected = await self.models.resolve(self.config.profiles[name], args["model"])
                    config = replace(config, profiles={name: selected})
                selection = choose(
                    config, self.store, requirements, args.get("profile"), access or "read-only"
                )
                if selection.get("execute_locally"):
                    return selection
                opened = await self.call(
                    "agent_open",
                    {
                        "profile": selection["profile"],
                        "model": args.get("model"),
                        "cwd": args["cwd"],
                        "access": access or "read-only",
                        "request_id": "session:" + hashlib.sha256(args["request_id"].encode()).hexdigest(),
                        "parent": args.get("parent"),
                        "base": args.get("base", "HEAD"),
                        "snapshot_files": args.get("snapshot_files"),
                        "workspace_mode": args.get("workspace_mode", "git"),
                    },
                )
                session_id = opened["session_id"]
                selection.update(
                    {k: opened["profile"][k] for k in ("runtime", "provider", "model", "effort", "service_tier")}
                )
            elif args.get("profile") and self.store.get_session(session_id)["profile"]["name"] != args["profile"]:
                raise BridgeError("profile_mismatch", "An existing session's model and provider cannot be replaced")
            elif (
                args.get("snapshot_files") is not None
                or args.get("workspace_mode", "git") != "git"
                or args.get("base", "HEAD") != "HEAD"
            ):
                raise BridgeError("workspace_mismatch", "Open a new session to change its input snapshot")
            if access is not None and self.store.get_session(session_id)["access"] != access:
                raise BridgeError("access_mismatch", "An existing session's access cannot change; open a new session")
            if (
                args.get("model") is not None
                and self.store.get_session(session_id)["profile"]["model"] != args["model"]
            ):
                raise BridgeError("model_mismatch", "Open a new session to change its model")
            if session_id and requirements:
                existing = self.store.get_session(session_id)
                profile = Profile(**existing["profile"])
                choose(Config({profile.name: profile}), self.store, requirements, profile.name, existing["access"])
            try:
                if args.get("retry_task_id"):
                    return self.store.retry(
                        args["retry_task_id"], session_id, args["instruction"], args["request_id"], reservation
                    )
                return self.store.dispatch(
                    session_id, args["instruction"], args["request_id"], args.get("dependencies"), reservation,
                    {"kind": requirements.get("kind", "general"), "selection": selection,
                     "learn_from_result": requirements.get("learn_from_result", True)},
                )
            except BridgeError:
                if opened:
                    session = self.store.get_session(session_id)
                    self.store.control(session_id, "close", session["revision"], "abort:" + session_id)
                raise
        if method == "agent_control":
            if args["action"] == "quota_resume":
                return self.store.quota_resume(
                    args["session_id"], args["expected_revision"], args["request_id"], args.get("evidence")
                )
            if args["action"] == "resolve":
                return self.store.resolve(
                    args["session_id"],
                    args["task_id"],
                    args["expected_revision"],
                    args["request_id"],
                    args["resolution"],
                    args["evidence"],
                )
            if args["action"].startswith("parent_"):
                price = None
                if args.get("usage_profile"):
                    profile = self.config.profiles.get(args["usage_profile"])
                    if not profile or profile.runtime != "codex":
                        raise BridgeError("invalid_meter", "usage_profile must name a configured Codex profile")
                    price = {**profile.price, "model": profile.model} if profile.price else None
                return self.store.parent_control(
                    args["session_id"], args["action"], args["expected_revision"], args["request_id"],
                    args.get("native_session_id"), args.get("cwd"), args.get("event_seq"),
                    args.get("usage_path"), args.get("usage_since"), args.get("notify", True), price,
                    args.get("resume_config_file"),
                )
            if args["action"] == "respond":
                return self.store.respond(
                    args["session_id"],
                    args["question_id"],
                    args["answer"],
                    args["expected_revision"],
                    args["request_id"],
                )
            return self.store.control(
                args["session_id"],
                args["action"],
                args["expected_revision"],
                args["request_id"],
                args.get("continue_task_id"),
            )
        if method == "agent_cancel":
            return self.store.cancel(args["task_id"], args["request_id"])
        if method in {"agent_inspect", "agent_result"}:
            if not args.get("task_id") and not args.get("session_id"):
                await asyncio.to_thread(self.store.refresh_parent_usage)
            if args.get("parent_id"):
                from .notifications import send
                from .telegram_guard import delivery_status

                parent = self.store.get_parent(args["parent_id"])
                parent["telegram_state"] = delivery_status(self.config.telegram_database, parent["native_id"])
                try:
                    parent["native"] = await send(self.config, parent, "", 0, inspect=True)
                except Exception as error:
                    parent["native"] = {"error": safe_error(error)}
                return parent
            if args.get("task_id"):
                if args.get("artifact_id"):
                    return self.store.read_artifact(
                        args["task_id"], args["artifact_id"], args.get("offset", 0), args.get("limit", 4000)
                    )
                task = self.store.public_task(args["task_id"], args.get("attempt_id"))
                task["waiting_reason"] = self.store.waiting_reason(args["task_id"], self.config)
                task["session_revision"] = self.store.get_session(task["session_id"])["revision"]
                return task
            if args.get("session_id"):
                session = self.store.get_session(args["session_id"])
                session["profile"].pop("env_file", None)
                session["worker_alive"] = alive(session)
                session["owned_processes"] = owned_processes(session["process_scope"])
                with self.store.connection() as db:
                    session["questions"] = [
                        {**dict(row), "data": json.loads(row["data"])}
                        for row in db.execute(
                            "SELECT id,data,status,expires FROM questions WHERE session_id=? AND status='pending' AND (expires IS NULL OR expires>strftime('%s','now'))",
                            (args["session_id"],),
                        )
                    ]
                return session
            return {
                "sessions": self.store.sessions(),
                "budget": self.store.budget(),
                "token_budget": self.config.token_budget,
                "quota_cooldowns": self.store.quotas(),
            }
        if method == "agent_review":
            return self.store.review(
                args["task_id"],
                args["attempt_id"],
                args["decision"],
                args["evidence"],
                args["request_id"],
                args["expected_revision"],
            )
        if method == "agent_wait":
            deadline = asyncio.get_running_loop().time() + min(max(float(args.get("timeout", 20)), 0), 50)
            while True:
                events = self.store.events(args.get("after", 0), args.get("session_id"), args.get("task_id"))
                if events or asyncio.get_running_loop().time() >= deadline:
                    return {
                        "events": events,
                        "cursor": events[-1]["seq"] if events else args.get("after", 0),
                        "timed_out": not bool(events),
                    }
                await asyncio.sleep(0.15)
        raise BridgeError("unknown_method", method)

    async def api_call(self, method, args):
        if method not in {"agent_open", "agent_dispatch", "agent_control", "agent_cancel", "agent_review"}:
            return {"result": await self.call(method, args)}
        key = args.get("request_id")
        if not isinstance(key, str) or not key or len(key) > 200:
            raise BridgeError("invalid_request_id", "A request_id of at most 200 characters is required")
        fingerprint = hashlib.sha256(encode({"method": method, "params": args}).encode()).hexdigest()
        lock = self.request_locks.setdefault(key, asyncio.Lock())
        async with lock:
            with self.store.connection(write=True) as db:
                old = db.execute("SELECT * FROM api_requests WHERE id=?", (key,)).fetchone()
                if old:
                    if old["fingerprint"] != fingerprint:
                        raise BridgeError("request_conflict", "request_id already represents different API parameters")
                    if old["response"]:
                        return json.loads(old["response"])
                    inner = db.execute("SELECT result FROM requests WHERE id=?", (key,)).fetchone()
                    if inner:
                        recovered = {"result": json.loads(inner["result"])}
                        db.execute("UPDATE api_requests SET response=? WHERE id=?", (encode(recovered), key))
                        return recovered
                    raise BridgeError(
                        "request_uncertain",
                        "A previous broker stopped during this operation; inspect its session/task before submitting new work",
                    )
                db.execute("INSERT INTO api_requests(id,fingerprint) VALUES(?,?)", (key, fingerprint))
            try:
                response = {"result": await self.call(method, args)}
            except BridgeError as error:
                response = {"error": error.as_dict()}
            except (ValueError, TypeError, KeyError, OSError) as error:
                response = {"error": {"code": "invalid_request", "message": safe_error(error)}}
            except Exception as error:
                response = {"error": {"code": "request_uncertain", "message": "Result not confirmed; inspect before retrying: " + safe_error(error)}}
            with self.store.connection(write=True) as db:
                db.execute("UPDATE api_requests SET response=? WHERE id=?", (encode(response), key))
            return response

    async def connection(self, reader, writer):
        try:
            _, uid, _ = struct.unpack(
                "3i", writer.get_extra_info("socket").getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            )
            if uid != os.getuid():
                raise BridgeError("forbidden", "Connection UID differs from service UID")
            request = json.loads(await asyncio.wait_for(reader.readline(), 5))
            if request.get("version") != 1 or not secrets.compare_digest(str(request.get("token", "")), self.token):
                raise BridgeError("forbidden", "Protocol version or management token is invalid")
            response = await self.api_call(request["method"], request.get("params", {}))
        except BridgeError as error:
            response = {"error": error.as_dict()}
        except (ValueError, TypeError, KeyError, TimeoutError) as error:
            response = {"error": {"code": "invalid_request", "message": safe_error(error)}}
        except Exception as error:
            response = {"error": {"code": "internal_error", "message": safe_error(error)}}
        with contextlib.suppress(ConnectionError):
            writer.write((json.dumps(response, ensure_ascii=False) + "\n").encode())
            await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def serve(self):
        lock = (self.state / "broker.lock").open("a")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise BridgeError("already_running", "A broker already owns this state directory") from error
        path = self.state / "broker.sock"
        path.unlink(missing_ok=True)
        server = await asyncio.start_unix_server(self.connection, str(path), limit=2 * 1024 * 1024)
        path.chmod(0o600)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stopping.set)
        scheduler = asyncio.create_task(self.scheduler())
        notifications = asyncio.create_task(self.notifications())
        try:
            async with server:
                await self.stopping.wait()
        finally:
            await scheduler
            await notifications
            path.unlink(missing_ok=True)
            lock.close()


async def request(state: Path, method, params):
    if os.environ.get("AGENT_BRIDGE_ROLE") == "child":
        raise BridgeError("forbidden", "Child runtimes may not administer parent or sibling sessions")
    reader, writer = await asyncio.open_unix_connection(str(state / "broker.sock"), limit=16 * 1024 * 1024)
    try:
        token = os.environ.get("AGENT_BRIDGE_TOKEN") or (state / "admin.token").read_text().strip()
        params = {key: value for key, value in params.items() if key != "state"}
        writer.write((json.dumps({"version": 1, "token": token, "method": method, "params": params}) + "\n").encode())
        await writer.drain()
        reply = json.loads(await asyncio.wait_for(reader.readline(), 65))
        if "error" in reply:
            raise BridgeError(reply["error"]["code"], reply["error"]["message"])
        return reply["result"]
    finally:
        writer.close()
        await writer.wait_closed()
