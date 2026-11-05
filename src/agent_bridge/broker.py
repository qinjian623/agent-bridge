from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import json
import math
import os
import secrets
import signal
import socket
import struct
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

from . import groups, native_broadcast, peer_api
from .config import Config, Profile, safe_error
from .discovery import ModelDirectory
from .errors import BridgeError
from .hosts import ConfigHostRegistry
from .peer_protocol import PROTOCOL_VERSION, NodeRef, PeerDeliveryStatus, PeerErrorCode, canonical_operation_id
from .peer_transport import DefaultPeerClientFactory
from .routing import choose
from .store import Store, encode, task_input, validate_session_name
from .worker import alive, owned_processes
from .workspace import directory_copy, worktree


def unit_name(state, session_id):
    return "agent-bridge-" + hashlib.sha256(str(state).encode()).hexdigest()[:8] + "-" + session_id + ".service"


_TERMINAL_PEER_STATUSES = (
    str(PeerDeliveryStatus.RELEASED), str(PeerDeliveryStatus.CANCELLED), str(PeerDeliveryStatus.FAILED),
)
_HOST_BLOCKING_CODES = {
    str(PeerErrorCode.HOST_OFFLINE), str(PeerErrorCode.HOST_UNKNOWN),
    str(PeerErrorCode.IDENTITY_MISMATCH), str(PeerErrorCode.STATE_CORRUPTED),
}


class DeliveryScheduler:
    """Two-layer ordered sender for coordinator `peer_outbox` rows (design 3.5).

    One scheduler pass examines only the lowest non-terminal `target_seq` of each target stream. A head that is
    already in flight (`sending`, `uncertain`, `waiting_for_host`, `tombstone_pending`, `resolving`) blocks every
    later sequence for that target and is never skipped. Network I/O happens outside the CAS transaction.
    """

    def __init__(self, store, peer_factory=None, *, authority_ref=None, config=None):
        self.store = store
        self.peer_factory = peer_factory
        self.authority_ref = authority_ref
        self.config = config
        self._stopping = asyncio.Event()

    def _decode(self, value):
        if value is None:
            return {}
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return {}

    def _authority(self, payload):
        value = payload.get("authority_ref") if isinstance(payload, dict) else None
        if isinstance(value, dict):
            return value
        return groups.resolve_authority_ref(self.store, self.authority_ref)

    def _client(self, host_id):
        if self.peer_factory is None:
            raise BridgeError(PeerErrorCode.HOST_UNKNOWN, f"Host is not registered: {host_id}")
        if hasattr(self.peer_factory, "client"):
            return self.peer_factory.client(host_id)
        if callable(self.peer_factory):
            return self.peer_factory(host_id)
        return self.peer_factory

    def heads(self, host_id=None):
        """Return the blocking head row of every target stream (optionally one host)."""
        placeholders = ",".join("?" for _ in _TERMINAL_PEER_STATUSES)
        sql = (
            "SELECT o.* FROM peer_outbox o JOIN ("
            "SELECT stream_id,MIN(target_seq) AS target_seq FROM peer_outbox "
            f"WHERE status NOT IN ({placeholders}) GROUP BY stream_id"
            ") h ON h.stream_id=o.stream_id AND h.target_seq=o.target_seq "
            f"WHERE o.status NOT IN ({placeholders})"
        )
        values = [*_TERMINAL_PEER_STATUSES, *_TERMINAL_PEER_STATUSES]
        if host_id is not None:
            sql += " AND o.host_id=?"
            values.append(host_id)
        sql += " ORDER BY o.host_id,o.stream_id,o.target_seq"
        with self.store.connection() as db:
            return [dict(row) for row in db.execute(sql, values)]

    def _claim(self, delivery_id):
        with self.store.connection(write=True) as db:
            row = db.execute("SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row is None or row["status"] not in {
                str(PeerDeliveryStatus.PENDING), str(PeerDeliveryStatus.TOMBSTONE_PENDING),
            }:
                return None
            changed = db.execute(
                "UPDATE peer_outbox SET status='sending',last_attempt=?,revision=revision+1,updated=? "
                "WHERE delivery_id=? AND status=?",
                (time.time(), time.time(), delivery_id, row["status"]),
            ).rowcount
            if not changed:
                return None
            return dict(db.execute("SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)).fetchone())

    def _deliver_params(self, outbox, payload):
        target = {"kind": outbox["target_kind"], "id": outbox["target_id"]}
        if outbox["target_kind"] == "native":
            target["runtime"] = outbox["runtime"]
        params = {
            "operation": {
                "node_id": outbox["node_id"],
                "state_generation": outbox["state_generation"],
                "operation_id": outbox["operation_id"],
            },
            "delivery_id": outbox["delivery_id"],
            "group_id": outbox["group_id"] or "",
            "broadcast_id": outbox["broadcast_id"] or "",
            "target": target,
            "target_seq": outbox["target_seq"],
            "seq_kind": outbox["seq_kind"],
            "expected_group_revision": outbox["group_revision"] or 0,
            "ordering_mode": "ordered",
            "stream_id": outbox["stream_id"],
            "authority_ref": self._authority(payload),
        }
        if outbox["seq_kind"] == "message":
            params["message"] = payload.get("message", "")
            params["reservation"] = int(payload.get("reservation") or 0)
        return params

    def _finish(self, outbox, status, *, result=None, error=None, release=False):
        receipt = None
        send_status = None
        processing_status = None
        if isinstance(result, dict):
            value = result.get("receipt")
            if value is not None:
                receipt = value if isinstance(value, str) else encode(value)
            send_status = result.get("send_status")
            processing_status = result.get("processing_status")
        encoded_error = None
        if error is not None:
            encoded_error = error if isinstance(error, str) else encode(error)
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE peer_outbox SET status=?,receipt=COALESCE(?,receipt),send_status=?,"
                "processing_status=?,error=?,revision=revision+1,updated=? WHERE delivery_id=?",
                (status, receipt, send_status, processing_status, encoded_error, time.time(),
                 outbox["delivery_id"]),
            )
            if release:
                db.execute(
                    "UPDATE target_streams SET released_seq=MAX(released_seq,?) WHERE stream_id=?",
                    (outbox["target_seq"], outbox["stream_id"]),
                )
            groups.update_broadcast_recipient(
                db, outbox["delivery_id"], status,
                send_status=send_status, processing_status=processing_status, error=encoded_error,
            )
            if outbox["broadcast_id"]:
                groups.settle_broadcast(db, outbox["broadcast_id"])
            return dict(db.execute(
                "SELECT * FROM peer_outbox WHERE delivery_id=?", (outbox["delivery_id"],)
            ).fetchone())

    async def _dispatch(self, outbox):
        payload = self._decode(outbox["payload"])
        params = self._deliver_params(outbox, payload)
        try:
            result = await self._client(outbox["host_id"]).deliver(params)
        except BridgeError as error:
            if str(error.code) in _HOST_BLOCKING_CODES:
                return self._finish(outbox, str(PeerDeliveryStatus.WAITING_FOR_HOST), error=error.as_dict())
            return self._finish(outbox, str(PeerDeliveryStatus.FAILED), error=error.as_dict())
        except (TimeoutError, ConnectionError, OSError) as error:
            return self._finish(outbox, str(PeerDeliveryStatus.UNCERTAIN), error=safe_error(error))
        except Exception as error:
            return self._finish(outbox, str(PeerDeliveryStatus.UNCERTAIN), error=safe_error(error))
        if not isinstance(result, dict):
            return self._finish(
                outbox, str(PeerDeliveryStatus.UNCERTAIN),
                error="The execution node returned an invalid delivery response",
            )
        remote_status = result.get("status")
        if outbox["seq_kind"] == "tombstone" or remote_status == "cancelled":
            return self._finish(
                outbox, str(PeerDeliveryStatus.CANCELLED), result=result, release=True,
            )
        if remote_status == "uncertain" or remote_status == "sending":
            return self._finish(outbox, str(PeerDeliveryStatus.UNCERTAIN), result=result)
        if remote_status not in {"accepted_remote", "released", "received"}:
            return self._finish(
                outbox, str(PeerDeliveryStatus.FAILED),
                result=result, error=f"Unexpected remote delivery status: {remote_status!r}",
            )
        if outbox["target_kind"] == "bridge":
            # A bridge ACK means the task was committed to the remote durable queue: network order is safe.
            return self._finish(
                outbox, str(PeerDeliveryStatus.RELEASED), result=result, release=True,
            )
        # Native ACK only commits the send-queue row; the stream stays blocked until native receipt.
        return self._finish(outbox, str(PeerDeliveryStatus.ACCEPTED_REMOTE), result=result)

    async def step(self, host_id=None):
        """Launch at most one pending head per target stream; blocked heads are left untouched."""
        processed = []
        for head in self.heads(host_id):
            if head["status"] not in {
                str(PeerDeliveryStatus.PENDING), str(PeerDeliveryStatus.TOMBSTONE_PENDING),
            }:
                continue
            claimed = self._claim(head["delivery_id"])
            if claimed is None:
                continue
            processed.append(await self._dispatch(claimed))
        return processed

    async def pump(self, host_id=None):
        """Drain every runnable stream for one host (or all hosts) without crossing blocked heads."""
        processed = []
        while True:
            rows = await self.step(host_id)
            if not rows:
                return processed
            processed.extend(rows)

    def cancel(self, delivery_id):
        """Turn a still-pending logical message into a tombstone at its original target_seq."""
        if not isinstance(delivery_id, str) or not delivery_id:
            raise BridgeError("invalid_request", "delivery_id must be a non-empty string")
        with self.store.connection(write=True) as db:
            row = db.execute("SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row is None:
                raise BridgeError("not_found", f"peer_outbox: {delivery_id}")
            if row["status"] in _TERMINAL_PEER_STATUSES:
                return dict(row)
            if row["status"] == str(PeerDeliveryStatus.TOMBSTONE_PENDING) or row["seq_kind"] == "tombstone":
                return dict(row)
            if row["status"] != str(PeerDeliveryStatus.PENDING):
                raise BridgeError(
                    PeerErrorCode.DELIVERY_BLOCKED,
                    f"Delivery is {row['status']}; prove its outcome with lookup/resolve before cancelling",
                )
            payload = self._decode(row["payload"])
            payload["seq_kind"] = "tombstone"
            payload.pop("message", None)
            payload.pop("reservation", None)
            authority = self._authority(payload)
            operation_id = canonical_operation_id(
                authority, row["state_generation"], "peer.deliver", f"{delivery_id}:tombstone",
            )
            payload["operation_id"] = operation_id
            fingerprint = hashlib.sha256(encode(payload).encode("utf-8")).hexdigest()
            changed = db.execute(
                "UPDATE peer_outbox SET status='tombstone_pending',seq_kind='tombstone',operation_id=?,"
                "payload=?,fingerprint=?,cancel_requested=1,revision=revision+1,updated=? "
                "WHERE delivery_id=? AND status='pending'",
                (operation_id, encode(payload), fingerprint, time.time(), delivery_id),
            ).rowcount
            if not changed:
                current = db.execute(
                    "SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)
                ).fetchone()
                return dict(current)
            groups.update_broadcast_recipient(db, delivery_id, str(PeerDeliveryStatus.TOMBSTONE_PENDING))
            return dict(db.execute("SELECT * FROM peer_outbox WHERE delivery_id=?", (delivery_id,)).fetchone())

    def stop(self):
        self._stopping.set()

    async def resolve(self, params=None, **kwargs):
        return await peer_api.resolve_delivery(self.store, self.peer_factory, params, **kwargs)

    async def run(self):
        while not self._stopping.is_set():
            try:
                await self.step()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._stopping.wait(), 0.5)
            except TimeoutError:
                pass


class Broker:
    def __init__(self, state: Path, peer_factory=None):
        self.state = state
        self.config = Config.load(state / "config.json")
        self.store = Store(state)
        self.host_registry = ConfigHostRegistry(self.config.hosts, state)
        # Phase B routing: one identity-checking client factory over the registered hosts. Injecting one keeps
        # tests (and future transports) independent of SSH while production uses the default SSH transport.
        self.peer_factory = peer_factory if peer_factory is not None else DefaultPeerClientFactory(self.host_registry)
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
        self.delivery_scheduler = DeliveryScheduler(self.store, self.peer_factory, config=self.config)
        with self.store.connection(write=True) as db:
            db.execute(
                "UPDATE deliveries SET status='uncertain',error='Broker restarted before queue acknowledgement' WHERE status='sending'"
            )
            db.execute(
                "UPDATE native_broadcast_deliveries SET status='uncertain',error='Broker restarted before queue acknowledgement' WHERE status='sending'"
            )
            db.execute(
                "UPDATE native_broadcast_deliveries SET processing_status='uncertain',error='Broker stopped after native receipt; execution outcome is unknown' WHERE status='received' AND processing_status='running'"
            )
            # ADR-05: an in-flight native write is never retried automatically. Its outcome is unknown until a
            # native receipt is found by inspect/lookup, so the ordered queue head stays blocked as uncertain.
            db.execute(
                "UPDATE native_send_queue SET status='uncertain',error='Broker restarted before queue acknowledgement' WHERE status='sending'"
            )
            db.execute(
                "UPDATE native_send_queue SET processing_status='uncertain',error='Broker stopped after native receipt; execution outcome is unknown' WHERE status='received' AND processing_status='running'"
            )
            db.execute(
                "UPDATE native_broadcast_deliveries SET status='uncertain',error='Broker restarted before queue acknowledgement' "
                "WHERE status IN ('pending','sending') AND id IN ("
                "SELECT delivery_id FROM native_send_queue WHERE delivery_source IN ('local_broadcast','native_broadcast') "
                "AND status='uncertain' AND last_attempt>0)"
            )
            # ADR-05/3.5: an in-flight ordered network write is never retried until its outcome is proven.
            db.execute(
                "UPDATE peer_outbox SET status='uncertain',error='Broker restarted before peer acknowledgement' "
                "WHERE status='sending'"
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

    async def native_notifications(self):
        running = set()
        try:
            while not self.stopping.is_set():
                await native_broadcast.deliver(self.store, self.config, tasks=running)
                try:
                    await asyncio.wait_for(self.stopping.wait(), 2)
                except TimeoutError:
                    pass
        finally:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)

    async def launch(self, session_id):
        args = [sys.executable, "-m", "agent_bridge.cli", "--state", str(self.state), "worker", session_id]
        if self.config.worker_launcher == "systemd":
            cmd = [
                "systemd-run",
                "--user",
                "--quiet",
                "--collect",
                "--unit=" + unit_name(self.state, session_id),
                "--property=KillMode=control-group",
                "--property=TimeoutStopSec=45",
                "--property=UMask=0077",
                "--setenv=PATH=" + os.environ.get("PATH", ""),
            ]
            for var in (
                "http_proxy",
                "https_proxy",
                "all_proxy",
                "no_proxy",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "NO_PROXY",
            ):
                if var in os.environ:
                    cmd.append(f"--setenv={var}={os.environ[var]}")
            cmd.extend(args)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
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

    # ---------------------------------------------------------------- phase B remote routing

    @staticmethod
    def _remote_host(args):
        host_id = args.get("host_id")
        if host_id is None or host_id == "local":
            return None
        if not isinstance(host_id, str) or not host_id:
            raise BridgeError("invalid_request", "host_id must be a non-empty string")
        return host_id

    async def _peer_node(self, host_id, client=None):
        """Resolve the pinned execution-node identity, handshaking once when no pin exists yet."""
        config = self.host_registry.get(host_id)
        if config.pinned is not None:
            return config.pinned
        client = client or self.peer_factory.client(host_id)
        hello = await client.hello(
            {
                "protocol_versions": [PROTOCOL_VERSION],
                "coordinator_id": socket.gethostname(),
                "coordinator_generation": "local",
            }
        )
        node = hello.get("node") if isinstance(hello, dict) else None
        if not isinstance(node, dict):
            raise BridgeError("invalid_request", "peer.hello returned no node identity")
        node_id = node.get("node_id")
        generation = node.get("state_generation")
        if not isinstance(node_id, str) or not node_id or not isinstance(generation, str) or not generation:
            raise BridgeError("invalid_request", "peer.hello returned an invalid node identity")
        return NodeRef(host_id, node_id, generation)

    async def _resolve_remote_members(self, members):
        """Fill omitted node_id/state_generation from the pinned or handshaken host identity.

        Explicit values must match exactly; a mismatch is an identity change, not a member edit.
        """
        if not isinstance(members, list):
            return members
        resolved = []
        cache = {}
        for member in members:
            if not isinstance(member, dict):
                resolved.append(member)
                continue
            host_id = member.get("host_id")
            if not isinstance(host_id, str) or not host_id:
                resolved.append(member)
                continue
            node = cache.get(host_id)
            if node is None:
                node = await self._peer_node(host_id)
                cache[host_id] = node
            for field, actual in (("node_id", node.node_id), ("state_generation", node.state_generation)):
                supplied = member.get(field)
                if isinstance(supplied, str) and supplied and supplied != actual:
                    raise BridgeError(
                        PeerErrorCode.IDENTITY_MISMATCH,
                        f"Remote member {field} does not match the registered identity of host {host_id}",
                    )
            resolved.append({**member, "node_id": node.node_id, "state_generation": node.state_generation})
        return resolved

    @staticmethod
    def _stable_operation_id(host_id, method, request_id, params):
        material = encode(
            {"host_id": host_id, "method": method, "request_id": request_id, "params": params}
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    @staticmethod
    def _remote_open_spec(args):
        spec = {}
        for key in ("profile", "cwd", "access", "project_id", "model", "workspace_mode", "base", "snapshot_files", "name"):
            value = args.get(key)
            if value is not None:
                spec[key] = value
        return spec

    @staticmethod
    def _target_seq(operation_id):
        # Deterministic from the stable operation id: retries reuse the same target_seq (ADR-05/ADR-09).
        return int(operation_id[:16], 16) % (1 << 62)

    def _operation_response(self, operation):
        result = operation.get("result")
        params = operation.get("params") if isinstance(operation.get("params"), dict) else {}
        payload = dict(result) if isinstance(result, dict) else {}
        payload.setdefault("operation_id", operation["operation_id"])
        payload.setdefault("host_id", params.get("host_id") or "local")
        if isinstance(result, dict):
            return payload
        if operation.get("error"):
            error = operation["error"]
            raise BridgeError(
                "operation_failed", error if isinstance(error, str) else json.dumps(error, ensure_ascii=False)
            )
        raise BridgeError(PeerErrorCode.OPERATION_PENDING, "The cross-host operation has no result yet")

    async def _remote_deliver(self, host_id, method, args, *, instruction=None, open_only=False):
        # Omitted and explicit name=None must produce the same stable operation id (pre-upgrade retries).
        if args.get("name") is None:
            args = {key: value for key, value in args.items() if key != "name"}
        client = self.peer_factory.client(host_id)
        node = await self._peer_node(host_id, client)
        operation_id = self._stable_operation_id(
            host_id, method, str(args.get("request_id") or ""), args
        )
        target_seq = self._target_seq(operation_id)
        target_id = args.get("session_id") or ""
        parameters = {
            "operation": {
                "node_id": node.node_id,
                "state_generation": node.state_generation,
                "operation_id": operation_id,
            },
            "delivery_id": "delivery-" + operation_id[:40],
            "group_id": str(args.get("group_id") or ""),
            "broadcast_id": str(args.get("broadcast_id") or ""),
            "target": {"kind": "bridge", "id": target_id},
            "target_seq": target_seq,
            "seq_kind": "message",
            "expected_group_revision": int(args.get("expected_group_revision") or 0),
            "reservation": int(args.get("reservation") or 0),
            "host_id": host_id,
        }
        if open_only or not target_id:
            parameters["open"] = self._remote_open_spec(args)
        if not open_only:
            parameters["message"] = instruction or ""
        fingerprint = hashlib.sha256(encode(parameters).encode("utf-8")).hexdigest()
        existing = self.store.get_operation(operation_id)
        if existing is not None:
            if existing["fingerprint"] != fingerprint:
                raise BridgeError(
                    PeerErrorCode.OPERATION_CONFLICT,
                    "operation_id was already used with different remote parameters",
                )
            return self._operation_response(existing)
        self.store.record_operation(
            operation_id,
            node.node_id,
            node.state_generation,
            fingerprint,
            method,
            parameters,
            status="pending",
        )
        try:
            result = await client.deliver(parameters)  # type: ignore[arg-type]
        except BridgeError as error:
            self.store.update_operation(operation_id, status="uncertain", error=error.as_dict())
            raise
        self.store.update_operation(operation_id, status="completed", result=result)
        return {"operation_id": operation_id, "host_id": host_id, **result}

    async def _remote_open(self, host_id, args):
        if not args.get("profile"):
            raise BridgeError("invalid_request", "profile is required for a remote open")
        return await self._remote_deliver(host_id, "agent_open", args, open_only=True)

    async def _remote_dispatch(self, host_id, args):
        requirements = args.get("requirements")
        if requirements is None:
            requirements = {}
        if not isinstance(requirements, dict):
            raise BridgeError("invalid_request", "requirements must be an object")
        if requirements.get("hard_budget"):
            raise BridgeError("hard_budget_unsupported", "No verified hard cap is available")
        instruction = args.get("instruction")
        reservation = args.get("reservation")
        if reservation is None:
            reservation = 0
        task_input(instruction, reservation, args.get("dependencies"))
        if not args.get("session_id") and not args.get("profile"):
            raise BridgeError("invalid_request", "profile is required when a remote dispatch opens a session")
        return await self._remote_deliver(host_id, "agent_dispatch", args, instruction=instruction)

    async def _cancel(self, args):
        supplied = [
            key
            for key in ("task_id", "operation_id", "delivery_id")
            if isinstance(args.get(key), str) and args.get(key)
        ]
        if len(supplied) != 1:
            raise BridgeError(
                "invalid_request", "Exactly one of task_id, operation_id or delivery_id is required"
            )
        target = supplied[0]
        value = args[target]
        request_id = args.get("request_id")
        if request_id is not None and (not isinstance(request_id, str) or len(request_id) > 200):
            raise BridgeError("invalid_request_id", "request_id must be at most 200 characters")
        host_id = self._remote_host(args)

        if host_id is not None:
            client = self.peer_factory.client(host_id)
            node = await self._peer_node(host_id, client)
            operation_id = self._stable_operation_id(host_id, "agent_cancel", str(request_id or ""), args)
            parameters = {
                "operation": {
                    "node_id": node.node_id,
                    "state_generation": node.state_generation,
                    "operation_id": operation_id,
                },
                target: value,
                "host_id": host_id,
            }
            fingerprint = hashlib.sha256(encode(parameters).encode("utf-8")).hexdigest()
            existing = self.store.get_operation(operation_id)
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise BridgeError(
                        PeerErrorCode.OPERATION_CONFLICT,
                        "operation_id was already used with different cancel parameters",
                    )
                return self._operation_response(existing)
            self.store.record_operation(
                operation_id,
                node.node_id,
                node.state_generation,
                fingerprint,
                "agent_cancel",
                parameters,
                status="pending",
            )
            try:
                result = await client.cancel(parameters)  # type: ignore[arg-type]
            except BridgeError as error:
                self.store.update_operation(operation_id, status="uncertain", error=error.as_dict())
                raise
            self.store.update_operation(operation_id, status="completed", result=result)
            return {"operation_id": operation_id, "host_id": host_id, **result}

        if target == "task_id":
            return self.store.cancel(value, request_id or ("cancel:" + value))

        if target == "operation_id":
            operation = self.store.get_operation(value)
        else:
            operation = None
            with self.store.connection() as db:
                rows = db.execute(
                    "SELECT * FROM peer_operations WHERE params LIKE ? ORDER BY created DESC LIMIT 20",
                    (f"%{value}%",),
                ).fetchall()
            for row in rows:
                params = Store._decode_json(row["params"])
                if not isinstance(params, dict):
                    continue
                parameters = params.get("delivery") if isinstance(params.get("delivery"), dict) else params
                if parameters.get("delivery_id") == value:
                    operation = Store._peer_operation(row)
                    break
        if operation is None:
            raise BridgeError(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown operation: {value}")
        parameters = operation.get("params") if isinstance(operation.get("params"), dict) else {}
        operation_host = parameters.get("host_id")
        if operation_host and operation_host != "local" and target != "task_id":
            return await self._cancel({**args, "host_id": operation_host})
        result = operation.get("result") if isinstance(operation.get("result"), dict) else {}
        task_id = result.get("task_id")
        if task_id:
            self.store.cancel(task_id, request_id or ("cancel:" + task_id))
        self.store.update_operation(value, status="cancelled")
        return {"target": value, "status": "cancelled"}

    def _operation_view(self, operation):
        params = operation.get("params") if isinstance(operation.get("params"), dict) else {}
        host_id = params.get("host_id")
        if not isinstance(host_id, str) or not host_id:
            host_id = "local"
        view = {"operation_id": operation["operation_id"], "host_id": host_id, "status": operation["status"]}
        result = operation.get("result")
        if isinstance(result, dict):
            view["result"] = result
            artifacts = result.get("artifacts")
            if isinstance(artifacts, list):
                view["artifacts"] = artifacts
        error = operation.get("error")
        if error is not None:
            view["error"] = (
                error if isinstance(error, dict) else {"code": "operation_failed", "message": str(error)}
            )
        return view

    async def _refresh_operation(self, operation, host_id):
        lookup = await self.peer_factory.client(host_id).lookup_delivery(
            {
                "operation": {
                    "node_id": operation["node_id"],
                    "state_generation": operation["state_generation"],
                    "operation_id": operation["operation_id"],
                }
            }
        )
        if not isinstance(lookup, dict) or not lookup.get("found"):
            return
        status = lookup.get("operation_status")
        delivery = lookup.get("delivery")
        if not isinstance(status, str) and isinstance(delivery, dict):
            status = delivery.get("status")
        mapping = {
            "accepted_remote": "completed",
            "received": "running",
            "queued": "running",
            "pending": "pending",
            "running": "running",
            "completed": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
            "uncertain": "uncertain",
        }
        refreshed = mapping.get(status) if isinstance(status, str) else None
        updates = {}
        if refreshed is not None and refreshed != operation["status"]:
            updates["status"] = refreshed
        if isinstance(delivery, dict) and delivery.get("task_id"):
            base = operation.get("result") if isinstance(operation.get("result"), dict) else {}
            updates["result"] = {**base, **delivery}
        if updates:
            self.store.update_operation(operation["operation_id"], **updates)

    async def _operation(self, args):
        operation_id = args.get("operation_id")
        if not isinstance(operation_id, str) or not operation_id:
            raise BridgeError("invalid_request", "operation_id must be a non-empty string")
        wait = args.get("wait")
        if wait is None:
            wait = 0
        if isinstance(wait, bool) or not isinstance(wait, (int, float)) or not math.isfinite(wait) or wait < 0:
            raise BridgeError("invalid_request", "wait must be a non-negative finite number")
        deadline = asyncio.get_running_loop().time() + min(float(wait), 50)
        while True:
            operation = self.store.get_operation(operation_id)
            if operation is None:
                raise BridgeError(PeerErrorCode.OPERATION_NOT_FOUND, f"Unknown operation: {operation_id}")
            params = operation.get("params") if isinstance(operation.get("params"), dict) else {}
            host_id = params.get("host_id")
            if operation["status"] in {"pending", "running", "uncertain"} and isinstance(host_id, str) and host_id != "local":
                await self._refresh_operation(operation, host_id)
                operation = self.store.get_operation(operation_id)
            if operation["status"] not in {"pending", "running"}:
                return self._operation_view(operation)
            if asyncio.get_running_loop().time() >= deadline:
                return self._operation_view(operation)
            await asyncio.sleep(0.15)

    async def call(self, method, args):
        if method == "agent_monitor":
            from .monitor import MonitorCollector

            collector = MonitorCollector(self.state)
            limit = args.get("limit", 15)
            include_events = args.get("include_events", True)
            return collector.collect(limit=limit, include_events=include_events)
        if method == "agent_hosts":
            host_id = args.get("host_id")
            if host_id is not None and not isinstance(host_id, str):
                raise BridgeError("invalid_request", "host_id must be a string")
            return {
                "hosts": [
                    {
                        "host_id": status.host_id,
                        "state": status.state.value,
                        "node_id": status.node.node_id if status.node else None,
                        "state_generation": status.node.state_generation if status.node else None,
                        "last_contact": status.last_contact,
                        "protocol_version": status.protocol_version,
                        "detail": status.detail,
                    }
                    for status in self.host_registry.status(host_id)
                ]
            }
        if method == "agent_groups":
            return groups.inspect_groups(self.store, args.get("group_id"))
        if method == "agent_group":
            remote_members = args.get("remote_members")
            if remote_members is not None and args.get("action") in {"create", "add"}:
                remote_members = await self._resolve_remote_members(remote_members)
            return groups.control_group(
                self.store, args["group_id"], args["action"], args["expected_revision"],
                args["request_id"], args.get("session_ids"), args.get("native_members"), remote_members,
            )
        if method == "agent_broadcast":
            return groups.broadcast(
                self.store, args["group_id"], args["message"], args["expected_revision"],
                args["request_id"], args.get("reservation", 0), token_budget=self.config.token_budget,
            )
        if method == "agent_broadcast_status":
            broadcast_id = args["broadcast_id"]
            with self.store.connection() as db:
                unified = db.execute("SELECT 1 FROM group_broadcasts WHERE id=?", (broadcast_id,)).fetchone()
            if unified is None:
                return await native_broadcast.inspect(
                    self.store, self.config, broadcast_id, refresh=args.get("refresh", False),
                )
            if args.get("refresh", False):
                # Refresh only legacy native receipts; remote queue state belongs to the scheduler.
                await native_broadcast.inspect(self.store, self.config, broadcast_id, refresh=True)
            return groups.broadcast_status(self.store, broadcast_id)
        if method == "agent_delivery_resolve":
            return await peer_api.resolve_delivery(self.store, self.peer_factory, args)
        if method == "agent_group_inbox":
            return native_broadcast.inbox(
                self.store, args["group_id"], args["runtime"], args["native_id"],
                args.get("after", 0), args.get("limit", 10),
            )
        if method == "agent_catalog":
            if not isinstance(args.get("refresh", False), bool):
                raise BridgeError("invalid_request", "refresh must be a boolean")
            host_id = self._remote_host(args)
            if host_id is not None:
                catalog_params = {"refresh": args.get("refresh", False)}
                if args.get("profile") is not None:
                    catalog_params["profile"] = args["profile"]
                result = await self.peer_factory.client(host_id).catalog(catalog_params)
                return {**result, "host_id": host_id}
            name = args.get("profile")
            if name is not None and name not in self.config.profiles:
                raise BridgeError("unknown_profile", str(name))
            profiles = [p for p in self.config.profiles.values() if name is None or p.name == name]
            discoveries = await asyncio.gather(*(self.models.get(p, args.get("refresh", False)) for p in profiles))
            fetched_at = time.time()
            entries = [
                {**p.public(), "host_id": "local", "stale": False, "fetched_at": fetched_at} for p in profiles
            ]
            return {
                "profiles": entries,
                "host_id": "local",
                "stale": False,
                "fetched_at": fetched_at,
                "discovery": discoveries,
                "objective": self.config.default_strategy,
                "strategies": ["balanced", "cost", "speed"],
                "parallel_limit": self.config.parallel_limit,
                "quota_limit": self.config.quota_limit,
                "quota_cooldowns": self.store.quotas(),
                "price_notice": "Catalog estimates are not confirmed bills",
                "model_directory": "Discovered IDs can be selected with profile + model; directory visibility is not inference verification. Cached for 300 seconds; refresh=true queries again. Automatic scoring still uses verified configured profiles.",
            }
        if method == "agent_operation":
            return await self._operation(args)
        if method == "agent_open":
            session_name = validate_session_name(args.get("name"))
            if session_name is not None and args.get("session_id") is not None:
                raise BridgeError("invalid_request", "Cannot set name on an existing session")
            if session_name is None:
                args = {key: value for key, value in args.items() if key != "name"}
            else:
                args = {**args, "name": session_name}
            host_id = self._remote_host(args)
            if host_id is not None:
                return await self._remote_open(host_id, args)
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
            result = self.store.open(profile, cwd, access, args["request_id"], args.get("parent"), metadata, session_name)
            return {**result, "cwd": cwd, "workspace": metadata, "profile": profile.public()}
        if method == "agent_dispatch":
            requirements = args.get("requirements")
            if requirements is None:
                requirements = {}
            if not isinstance(requirements, dict):
                raise BridgeError("invalid_request", "requirements must be an object")
            if requirements.get("hard_budget"):
                raise BridgeError("hard_budget_unsupported", "No verified hard cap is available")
            session_name = validate_session_name(args.get("name"))
            if session_name is not None and args.get("session_id"):
                raise BridgeError("invalid_request", "Cannot set name on an existing session")
            if session_name is None:
                args = {key: value for key, value in args.items() if key != "name"}
            else:
                args = {**args, "name": session_name}
            host_id = self._remote_host(args)
            if host_id is not None:
                return await self._remote_dispatch(host_id, args)
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
                open_request = {
                    "profile": selection["profile"],
                    "model": args.get("model"),
                    "cwd": args["cwd"],
                    "access": access or "read-only",
                    "request_id": "session:" + hashlib.sha256(args["request_id"].encode()).hexdigest(),
                    "parent": args.get("parent"),
                    "base": args.get("base", "HEAD"),
                    "snapshot_files": args.get("snapshot_files"),
                    "workspace_mode": args.get("workspace_mode", "git"),
                }
                if session_name is not None:
                    open_request["name"] = session_name
                opened = await self.call("agent_open", open_request)
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
            return await self._cancel(args)
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
        if method not in {"agent_open", "agent_dispatch", "agent_control", "agent_cancel", "agent_review", "agent_group", "agent_broadcast", "agent_delivery_resolve"}:
            return {"result": await self.call(method, args)}
        if method in {"agent_open", "agent_dispatch"} and "name" in args:
            name = validate_session_name(args.get("name"))
            args = {field: value for field, value in args.items() if field != "name"}
            if name is not None:
                args["name"] = name
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
        native_notifications = asyncio.create_task(self.native_notifications())
        delivery_scheduler = asyncio.create_task(self.delivery_scheduler.run())
        try:
            async with server:
                await self.stopping.wait()
        finally:
            await scheduler
            await notifications
            await native_notifications
            delivery_scheduler.cancel()
            await asyncio.gather(delivery_scheduler, return_exceptions=True)
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
