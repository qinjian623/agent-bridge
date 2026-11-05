from __future__ import annotations

import asyncio
import base64
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
from .config import Config, Profile, read_admin_token, safe_error
from .discovery import ModelDirectory
from .errors import BridgeError
from .hosts import ConfigHostRegistry
from .peer_protocol import (
    MAX_WORKSPACE_CHUNK_BYTES,
    PROTOCOL_VERSION,
    NodeRef,
    PeerDeliveryStatus,
    PeerErrorCode,
    canonical_operation_id,
    compute_manifest_digest,
)
from .peer_transport import DefaultPeerClientFactory
from .routing import choose
from .store import Store, encode, task_input, validate_session_name
from .worker import alive, owned_processes
from .workspace import (
    MAX_WORKSPACE_FILE_BYTES,
    MAX_WORKSPACE_FILES,
    MAX_WORKSPACE_TOTAL_BYTES,
    create_workspace_manifest,
    directory_copy,
    selected_path,
    worktree,
)
from .workspace import (
    git as workspace_git,
)


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
        if isinstance(payload.get("parent_ref"), dict):
            params["parent_ref"] = payload["parent_ref"]
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


class OutboxPuller:
    """Coordinator-side puller for one execution node's `node_outbox` (design 3.7).

    Authorisation happens here, on the coordinator: the execution node only stores what a local member asked
    for. Every full scan starts at `after=0`; an in-memory cursor advances through the page range and returns
    to zero once the scan's fixed `scan_upper_seq` has been consumed. A permanent rejection is persisted in
    `pulled_request_receipts` and acknowledged so it never blocks later requests.
    """

    def __init__(self, store, peer_factory, host_registry=None, config=None):
        self.store = store
        self.peer_factory = peer_factory
        self.host_registry = host_registry
        self.config = config
        self._temp_after: dict[str, int] = {}
        self._scan_upper: dict[str, int] = {}
        self._nodes: dict[str, NodeRef] = {}
        self._stopping = asyncio.Event()

    def _client(self, host_id):
        if hasattr(self.peer_factory, "client"):
            return self.peer_factory.client(host_id)
        if callable(self.peer_factory):
            return self.peer_factory(host_id)
        return self.peer_factory

    def _host_config(self, host_id):
        getter = getattr(self.host_registry, "get", None)
        if getter is None:
            return None
        try:
            return getter(host_id)
        except BridgeError:
            return None

    def _hosts(self):
        lister = getattr(self.host_registry, "list", None)
        return list(lister()) if lister is not None else []

    async def _node(self, host_id, client, items=()):
        cached = self._nodes.get(host_id)
        if cached is not None:
            return cached
        config = self._host_config(host_id)
        if config is not None and config.pinned is not None:
            self._nodes[host_id] = config.pinned
            return config.pinned
        hello = getattr(client, "hello", None)
        if hello is not None:
            reply = await hello(
                {
                    "protocol_versions": [PROTOCOL_VERSION],
                    "coordinator_id": socket.gethostname(),
                    "coordinator_generation": "local",
                }
            )
            node = reply.get("node") if isinstance(reply, dict) else None
            if isinstance(node, dict):
                node_id = node.get("node_id")
                generation = node.get("state_generation")
                if isinstance(node_id, str) and node_id and isinstance(generation, str) and generation:
                    resolved = NodeRef(host_id, node_id, generation)
                    self._nodes[host_id] = resolved
                    return resolved
        # A minimal transport can omit hello when the first outbox item carries the authenticated sender.
        for item in items:
            sender = self._json_object(item.get("sender_ref")) if isinstance(item, dict) else None
            if sender is None:
                continue
            node_id = sender.get("node_id")
            generation = sender.get("state_generation")
            if isinstance(node_id, str) and node_id and isinstance(generation, str) and generation:
                resolved = NodeRef(host_id, node_id, generation)
                self._nodes[host_id] = resolved
                return resolved
        raise BridgeError("invalid_request", f"Could not resolve the node identity of host {host_id!r}")

    @staticmethod
    def _json_object(value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError):
                return None
        return value if isinstance(value, dict) else None

    @classmethod
    def _payload(cls, item):
        return cls._json_object(item.get("payload"))

    @staticmethod
    def _fingerprint(item, payload):
        fingerprint = item.get("fingerprint")
        if isinstance(fingerprint, str) and fingerprint:
            return fingerprint
        return hashlib.sha256(encode(payload).encode("utf-8")).hexdigest()

    def _verify(self, host_id, node, item, payload):
        if item.get("method") != "agent_broadcast":
            return {"code": "unsupported_method", "message": "Only agent_broadcast may use the member outbox"}
        group_id = item.get("group_id") or payload.get("group_id")
        if not isinstance(group_id, str) or not group_id:
            return {"code": "invalid_group", "message": "group_id is required"}
        revision = payload.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            return {"code": "invalid_revision", "message": "payload.revision must be a non-negative integer"}
        sender = self._json_object(item.get("sender_ref"))
        if sender is None:
            return {"code": "unverified_sender", "message": "The request has no verified sender reference"}
        if (
            sender.get("host_id") != host_id
            or sender.get("node_id") != node.node_id
            or sender.get("state_generation") != node.state_generation
        ):
            return {"code": "identity_mismatch", "message": "The sender does not belong to this execution node"}
        sender_id = sender.get("local_id")
        if not isinstance(sender_id, str) or not sender_id:
            return {"code": "unverified_sender", "message": "The sender reference has no local_id"}
        with self.store.connection() as db:
            group = db.execute("SELECT deleted,revision FROM session_groups WHERE id=?", (group_id,)).fetchone()
            if group is None:
                return {"code": "group_not_found", "message": f"Unknown group: {group_id}"}
            if group["deleted"]:
                return {"code": "group_deleted", "message": "The group was deleted"}
            if group["revision"] != revision:
                return {
                    "code": "stale_revision",
                    "message": f"Group revision is {group['revision']}, request expected {revision}",
                }
            member = db.execute(
                "SELECT target_id FROM remote_group_members WHERE group_id=? AND host_id=? AND node_id=? "
                "AND state_generation=? AND target_id=?",
                (group_id, host_id, node.node_id, node.state_generation, sender_id),
            ).fetchone()
            if member is None:
                return {"code": "not_a_member", "message": "The sender is not a remote member of this group"}
        return None

    async def _ack(self, client, node, operation_id, outcome, result):
        await client.ack_request(
            {
                "operation": {
                    "node_id": node.node_id,
                    "state_generation": node.state_generation,
                    "operation_id": operation_id,
                },
                "operation_id": operation_id,
                "outcome": outcome,
                "result": result,
            }
        )

    async def _process(self, host_id, node, item, client):
        operation_id = item.get("operation_id")
        if not isinstance(operation_id, str) or not operation_id:
            return
        receipt = self.store.get_pulled_request_receipt(host_id, node.node_id, node.state_generation, operation_id)
        if receipt is not None:
            # A previous attempt persisted the receipt but did not receive a durable ACK: never execute twice.
            await self._ack(client, node, operation_id, receipt["outcome"], receipt["result"] or {})
            self.store.update_pulled_request_receipt_acked(host_id, node.node_id, node.state_generation, operation_id)
            return

        payload = self._payload(item)
        fingerprint = self._fingerprint(item, payload) if payload is not None else operation_id
        if payload is None:
            error = {"code": "invalid_payload", "message": "The outbox payload must be an object"}
        else:
            error = self._verify(host_id, node, item, payload)

        if error is not None:
            self.store.record_pulled_request_receipt(
                host_id, node.node_id, node.state_generation, operation_id,
                int(item.get("seq") or 0), fingerprint, "rejected", {"error": error},
            )
            await self._ack(client, node, operation_id, "rejected", {"error": error})
            self.store.update_pulled_request_receipt_acked(host_id, node.node_id, node.state_generation, operation_id)
            return

        group_id = str(item.get("group_id") or payload.get("group_id"))
        request_id = item.get("request_id") if isinstance(item.get("request_id"), str) else None
        request_id = request_id or operation_id
        authority_ref = item.get("authority_ref")
        sender_ref = self._json_object(item.get("sender_ref"))
        with self.store.connection(write=True) as db:
            result = groups.broadcast(
                self.store,
                group_id,
                str(payload.get("message", "")),
                int(payload["revision"]),
                request_id,
                int(payload.get("reservation") or 0),
                token_budget=getattr(self.config, "token_budget", None),
                authority_ref=authority_ref,
                sender_ref=sender_ref,
                db=db,
            )
            self.store.record_pulled_request_receipt(
                host_id, node.node_id, node.state_generation, operation_id,
                int(item.get("seq") or 0), fingerprint, "accepted", result, db=db,
            )
        await self._ack(client, node, operation_id, "accepted", result)
        self.store.update_pulled_request_receipt_acked(host_id, node.node_id, node.state_generation, operation_id)

    async def run_once(self, host_id: str) -> int:
        client = self._client(host_id)
        after = self._temp_after.get(host_id, 0)
        response = await client.pull_requests({"after": after, "limit": 100})
        items = response.get("items") if isinstance(response, dict) else None
        if not isinstance(items, list):
            items = []
        node = await self._node(host_id, client, items)
        processed = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                await self._process(host_id, node, item, client)
            except asyncio.CancelledError:
                raise
            except Exception:
                # A transient failure leaves the row unacknowledged; the next full scan retries it.
                pass
            processed += 1

        seqs = [
            item["seq"] for item in items
            if isinstance(item, dict) and isinstance(item.get("seq"), int) and not isinstance(item.get("seq"), bool)
        ]
        upper = response.get("scan_upper_seq") if isinstance(response, dict) else None
        if isinstance(upper, int) and not isinstance(upper, bool):
            scan_upper = self._scan_upper.setdefault(host_id, upper)
            if seqs:
                self._temp_after[host_id] = max(seqs)
            if not seqs or self._temp_after.get(host_id, 0) >= scan_upper:
                self._temp_after[host_id] = 0
                self._scan_upper.pop(host_id, None)
        elif seqs:
            self._temp_after[host_id] = max(seqs)
        else:
            self._temp_after[host_id] = 0
        return processed

    async def _host_loop(self, config):
        while not self._stopping.is_set():
            try:
                await self.run_once(config.host_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._stopping.wait(), config.pull_interval)
            except TimeoutError:
                pass

    async def run(self):
        tasks = {}
        try:
            while not self._stopping.is_set():
                for config in self._hosts():
                    task = tasks.get(config.host_id)
                    if task is None or task.done():
                        tasks[config.host_id] = asyncio.create_task(self._host_loop(config))
                try:
                    await asyncio.wait_for(self._stopping.wait(), 0.5)
                except TimeoutError:
                    pass
        finally:
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    def stop(self):
        self._stopping.set()


class EventMirror:
    """Mirrors remote events into local `events.seq` and wakes authorised parent routes (design 3.9).

    Dedupe is `(source node, remote seq)`. A different fingerprint for an already mirrored sequence pauses the
    source instead of overwriting data. The durable cursor advances only after every event on the page has been
    mapped and its parent notification attempted; a transient notification failure is retried on the next poll.
    """

    def __init__(self, store, peer_factory, host_registry=None, config=None, *, host_id="local", interval=2.0):
        self.store = store
        self.peer_factory = peer_factory
        self.host_registry = host_registry
        self.config = config
        self.host_id = host_id
        self.interval = interval
        self._nodes: dict[str, NodeRef] = {}
        self._paused: set[str] = set()
        self._stopping = asyncio.Event()

    def _client(self, host_id):
        if hasattr(self.peer_factory, "client"):
            return self.peer_factory.client(host_id)
        if callable(self.peer_factory):
            return self.peer_factory(host_id)
        return self.peer_factory

    def _host_config(self, host_id):
        getter = getattr(self.host_registry, "get", None)
        if getter is None:
            return None
        try:
            return getter(host_id)
        except BridgeError:
            return None

    def _hosts(self):
        lister = getattr(self.host_registry, "list", None)
        return list(lister()) if lister is not None else []

    async def _node(self, host_id, client):
        cached = self._nodes.get(host_id)
        if cached is not None:
            return cached
        config = self._host_config(host_id)
        if config is not None and config.pinned is not None:
            self._nodes[host_id] = config.pinned
            return config.pinned
        hello = getattr(client, "hello", None)
        if hello is None:
            raise BridgeError("invalid_request", f"Could not resolve the node identity of host {host_id!r}")
        reply = await hello(
            {
                "protocol_versions": [PROTOCOL_VERSION],
                "coordinator_id": socket.gethostname(),
                "coordinator_generation": "local",
            }
        )
        node = reply.get("node") if isinstance(reply, dict) else None
        if not isinstance(node, dict) or not isinstance(node.get("node_id"), str) or not isinstance(
            node.get("state_generation"), str
        ):
            raise BridgeError("invalid_request", f"peer.hello returned no identity for host {host_id!r}")
        resolved = NodeRef(host_id, node["node_id"], node["state_generation"])
        self._nodes[host_id] = resolved
        return resolved

    def _parent_is_local(self, parent_ref):
        local_id = parent_ref.get("local_id")
        if not isinstance(local_id, str) or not local_id:
            return False
        with self.store.connection() as db:
            return bool(
                db.execute("SELECT 1 FROM parents WHERE id=?", (local_id,)).fetchone()
                or db.execute("SELECT 1 FROM sessions WHERE id=?", (local_id,)).fetchone()
            )

    def _notify_local(self, parent_ref, event):
        from .node_identity import FileNodeIdentityStore

        identity = FileNodeIdentityStore(self.store.state)
        operation_id = hashlib.sha256(
            encode({"method": "peer.notify_parent", "parent_ref": parent_ref, "event": event}).encode("utf-8")
        ).hexdigest()
        service = peer_api.DirectPeerService(
            identity, host_id=self.host_id, store=self.store, config=self.config,
        )
        return service._notify_parent(
            {
                "operation": {
                    "node_id": identity.load_or_create().node_id,
                    "state_generation": identity.load_or_create().state_generation,
                    "operation_id": operation_id,
                },
                "parent_ref": parent_ref,
                "event": event,
            }
        )

    async def _notify_remote(self, event, parent_ref, local_seq, fingerprint):
        parent_host = parent_ref.get("host_id")
        if not isinstance(parent_host, str) or not parent_host:
            raise BridgeError("invalid_request", "parent_ref.host_id is required")
        client = self._client(parent_host)
        node = await self._node(parent_host, client)
        operation_id = hashlib.sha256(
            encode({"method": "peer.notify_parent", "parent_ref": parent_ref, "event": event}).encode("utf-8")
        ).hexdigest()
        with self.store.connection() as db:
            existing = db.execute(
                "SELECT 1 FROM mirrored_parent_notifications WHERE operation_id=?", (operation_id,)
            ).fetchone()
        if existing is None:
            result = await client.notify_parent(
                {
                    "operation": {
                        "node_id": node.node_id,
                        "state_generation": node.state_generation,
                        "operation_id": operation_id,
                    },
                    "parent_ref": parent_ref,
                    "event": event,
                }
            )
            with self.store.connection(write=True) as db:
                db.execute(
                    "INSERT OR IGNORE INTO mirrored_parent_notifications"
                    "(local_seq,parent_ref,operation_id,status,receipt,updated) VALUES(?,?,?,?,?,?)",
                    (local_seq, encode(parent_ref), operation_id, "sent", encode(result), time.time()),
                )
        return None

    @staticmethod
    def _canonical_event(value):
        """Return the exact PeerEvent shape used by peer_protocol's normalizer, or None."""
        if not isinstance(value, dict):
            return None
        ref = value.get("ref")
        payload = value.get("payload")
        event_type = value.get("type")
        created = value.get("created")
        seq = value.get("seq")
        if (
            isinstance(seq, bool) or not isinstance(seq, int) or seq < 0
            or not isinstance(event_type, str) or not event_type
            or not isinstance(ref, dict)
            or not isinstance(payload, dict)
            or isinstance(created, bool) or not isinstance(created, (int, float))
        ):
            return None
        clean = {
            "seq": seq,
            "type": event_type,
            "ref": {
                key: ref.get(key) for key in ("host_id", "node_id", "state_generation", "local_id")
            },
            "payload": payload,
            "created": float(created),
        }
        if not all(isinstance(clean["ref"][key], str) and clean["ref"][key] for key in clean["ref"]):
            return None
        return clean

    def _record_conflict(self, event, existing, fingerprint):
        with self.store.connection(write=True) as db:
            self.store.event(
                db,
                "peer_event_conflict",
                {
                    "source": event.get("ref"),
                    "remote_seq": event.get("seq"),
                    "stored_fingerprint": existing.get("fingerprint"),
                    "received_fingerprint": fingerprint,
                },
            )

    async def run_once(self, host_id: str) -> int:
        if host_id in self._paused:
            return 0
        client = self._client(host_id)
        node = await self._node(host_id, client)
        cursor = self.store.get_peer_event_cursor(host_id, node.node_id, node.state_generation)
        response = await client.events({"after": cursor, "limit": 100})
        items = response.get("items") if isinstance(response, dict) else None
        if not isinstance(items, list):
            items = []
        processed = 0
        last_seq = None
        for raw_event in items:
            event = self._canonical_event(raw_event)
            if event is None:
                break
            source = event["ref"]
            remote_seq = event["seq"]
            fingerprint = hashlib.sha256(encode(event).encode("utf-8")).hexdigest()
            existing = self.store.get_mirrored_event(host_id, node.node_id, node.state_generation, remote_seq)
            if existing is not None and existing["fingerprint"] != fingerprint:
                self._record_conflict(event, existing, fingerprint)
                self._paused.add(host_id)
                break
            if existing is not None:
                local_seq = existing["local_seq"]
            else:
                data = {
                    "source": source,
                    "remote_seq": remote_seq,
                    "type": event.get("type"),
                    "payload": event.get("payload"),
                    "created": event.get("created"),
                }
                with self.store.connection(write=True) as db:
                    local_seq = self.store.event(db, str(event.get("type", "")), data)
                    self.store.mirror_peer_event(
                        host_id, node.node_id, node.state_generation, remote_seq, fingerprint, local_seq, db=db,
                    )
            route = self.store.lookup_parent_route(
                host_id, node.node_id, node.state_generation, source["local_id"],
            )
            if route is not None:
                parent_ref = route["parent_ref"]
                try:
                    if self._parent_is_local(parent_ref):
                        self._notify_local(parent_ref, event)
                    else:
                        await self._notify_remote(event, parent_ref, local_seq, fingerprint)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    break
            last_seq = remote_seq
            processed += 1
        if last_seq is not None:
            self.store.advance_peer_event_cursor(host_id, node.node_id, node.state_generation, last_seq)
        return processed

    async def _host_loop(self, config):
        while not self._stopping.is_set():
            try:
                await self.run_once(config.host_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._stopping.wait(), config.pull_interval or self.interval)
            except TimeoutError:
                pass

    async def run(self):
        tasks = {}
        try:
            while not self._stopping.is_set():
                for config in self._hosts():
                    task = tasks.get(config.host_id)
                    if task is None or task.done():
                        tasks[config.host_id] = asyncio.create_task(self._host_loop(config))
                try:
                    await asyncio.wait_for(self._stopping.wait(), 0.5)
                except TimeoutError:
                    pass
        finally:
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    def stop(self):
        self._stopping.set()


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
        self.launching = set()
        self.stopping = asyncio.Event()
        self.request_locks = {}
        self.delivery_scheduler = DeliveryScheduler(self.store, self.peer_factory, config=self.config)
        self.outbox_puller = OutboxPuller(self.store, self.peer_factory, self.host_registry, self.config)
        self.event_mirror = EventMirror(self.store, self.peer_factory, self.host_registry, self.config)
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
    def _member_binding_args(args):
        """Return (binding_id, binding_token) from the member-facing broadcast arguments, if present."""
        binding_id = args.get("binding_id")
        binding_token = args.get("binding_token")
        binding = args.get("binding")
        if isinstance(binding, dict):
            binding_id = binding_id or binding.get("id") or binding.get("binding_id")
            binding_token = binding_token or binding.get("token")
        elif isinstance(binding, str):
            binding_token = binding_token or binding
        member = args.get("member")
        if isinstance(member, dict):
            binding_id = binding_id or member.get("binding_id")
            binding_token = binding_token or member.get("binding_token") or member.get("binding")
        if not isinstance(binding_id, str) or not binding_id:
            binding_id = None
        if not isinstance(binding_token, str) or not binding_token:
            binding_token = None
        return binding_id, binding_token

    def _member_broadcast(self, args, binding_id, binding_token):
        """Persist a member's broadcast request in this node's `node_outbox` (design 3.7)."""
        request_id = args.get("request_id")
        if not isinstance(request_id, str) or not request_id or len(request_id) > 200:
            raise BridgeError("invalid_request_id", "A nonempty request_id of at most 200 characters is required")
        group_id = args.get("group_id")
        if not isinstance(group_id, str) or not group_id:
            raise BridgeError("invalid_request", "group_id must be a non-empty string")
        message = args.get("message")
        if not isinstance(message, str):
            raise BridgeError("invalid_request", "message must be a string")
        revision = args.get("expected_revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            raise BridgeError("invalid_revision", "Provide the group's nonnegative expected_revision")
        reservation = args.get("reservation", 0)
        if isinstance(reservation, bool) or not isinstance(reservation, int) or reservation < 0:
            raise BridgeError("invalid_request", "reservation must be a non-negative integer")
        payload = {
            "op": "broadcast",
            "group_id": group_id,
            "message": message,
            "revision": revision,
            "reservation": reservation,
        }
        binding = None
        if binding_id is not None or binding_token is not None:
            with self.store.connection() as db:
                if binding_id is not None:
                    binding = db.execute("SELECT * FROM member_bindings WHERE id=?", (binding_id,)).fetchone()
                if binding is None and binding_token is not None:
                    digest = hashlib.sha256(binding_token.encode("utf-8")).hexdigest()
                    binding = db.execute(
                        "SELECT * FROM member_bindings WHERE token_digest=?", (digest,)
                    ).fetchone()
                if binding is None and binding_token is not None:
                    binding = db.execute(
                        "SELECT * FROM member_bindings WHERE id=?", (binding_token,)
                    ).fetchone()
        if binding is not None:
            allowed = binding["allowed_methods"]
            try:
                allowed = json.loads(allowed) if isinstance(allowed, str) else allowed
            except ValueError:
                allowed = []
            if (
                binding["revoked_at"] is not None
                or binding["expires_at"] <= time.time()
                or binding["group_id"] != group_id
                or "agent_broadcast" not in allowed
            ):
                raise BridgeError("forbidden", "The member binding does not authorise this broadcast")
            binding_id = binding["id"]
            sender_ref = json.loads(binding["sender_ref"])
            authority_ref = json.loads(binding["authority_ref"])
            authorization_revision = binding["authorization_revision"]
        else:
            sender_ref = args.get("sender_ref")
            authority_ref = args.get("authority_ref")
            authorization_revision = args.get("authorization_revision")
            if not isinstance(sender_ref, dict) or not isinstance(authority_ref, dict):
                raise BridgeError("forbidden", "A valid member binding or verified sender reference is required")
        operation_id = hashlib.sha256(
            encode(
                {
                    "binding_id": binding_id,
                    "request_id": request_id,
                    "group_id": group_id,
                    "payload": payload,
                }
            ).encode("utf-8")
        ).hexdigest()
        fingerprint = hashlib.sha256(encode(payload).encode("utf-8")).hexdigest()
        self.store.add_outbox(
            operation_id,
            "agent_broadcast",
            payload,
            group_id,
            args.get("native_id"),
            binding_id=binding_id,
            request_id=request_id,
            sender_ref=sender_ref,
            authority_ref=authority_ref,
            authorization_revision=authorization_revision,
            fingerprint=fingerprint,
        )
        return {"operation_id": operation_id, "status": "pending"}

    @staticmethod
    def _remote_open_spec(args):
        spec = {}
        for key in (
            "profile", "cwd", "access", "project_id", "model", "workspace_mode", "base",
            "snapshot_files", "name", "workspace_ref",
        ):
            value = args.get(key)
            if value is not None:
                spec[key] = value
        return spec

    @staticmethod
    def _target_seq(operation_id):
        # Deterministic from the stable operation id: retries reuse the same target_seq (ADR-05/ADR-09).
        return int(operation_id[:16], 16) % (1 << 62)

    @staticmethod
    def _remote_workspace_requested(method, args, target_id):
        return (
            method in {"agent_open", "agent_dispatch"}
            and not target_id
            and args.get("access") == "workspace-write"
            and args.get("workspace_mode", "git") == "git"
        )

    @staticmethod
    def _workspace_operation(host_id, node, request_id, payload):
        operation_id = Broker._stable_operation_id(host_id, "peer.workspace", request_id, payload)
        return {
            "node_id": node.node_id,
            "state_generation": node.state_generation,
            "operation_id": operation_id,
        }

    @staticmethod
    def _source_repository(cwd):
        if not isinstance(cwd, str) or not cwd:
            raise BridgeError(
                "invalid_request", "cwd (the local source checkout) is required for a remote workspace-write open"
            )
        root = Path(cwd).expanduser().resolve()
        if not root.is_dir():
            raise BridgeError("invalid_directory", "The local workspace source directory must exist")
        try:
            top = Path(workspace_git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve()
        except BridgeError as error:
            raise BridgeError("invalid_workspace", "Remote workspace-write requires a Git source checkout") from error
        if top != root:
            raise BridgeError("invalid_workspace", "Writing sessions require the repository root")
        return root

    @staticmethod
    def _resolve_source_commit(root, value):
        if not isinstance(value, str) or not value or value.startswith("-"):
            raise BridgeError("invalid_base", "Invalid Git revision")
        try:
            return workspace_git(root, "rev-parse", "--verify", value + "^{commit}").decode().strip()
        except BridgeError as error:
            raise BridgeError("invalid_base", f"Invalid Git revision: {value}") from error

    @staticmethod
    def _source_entry_bytes(root, base_commit, entry, selected):
        path_name = entry["path"]
        if path_name in selected:
            path = selected_path(root, path_name)
            if not path.is_file() or path.is_symlink():
                raise BridgeError("snapshot_changed", f"Source file changed during workspace capture: {path_name}")
            data = path.read_bytes()
        else:
            try:
                listing = workspace_git(root, "ls-tree", "-z", base_commit, "--", path_name)
            except BridgeError as error:
                raise BridgeError("snapshot_changed", f"Source base blob is unavailable: {path_name}") from error
            record = listing.split(b"\0", 1)[0]
            parts = record.split(b"\t", 1)[0].split(b" ")
            if len(parts) != 3 or parts[1] != b"blob":
                raise BridgeError("snapshot_changed", f"Source base blob is unavailable: {path_name}")
            data = workspace_git(root, "cat-file", "blob", parts[2].decode())
        if len(data) != entry["size"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise BridgeError("snapshot_changed", f"Source file changed during workspace capture: {path_name}")
        return data

    async def _workspace_missing(self, client, workspace_id):
        items = []
        after = 0
        while True:
            result = await client.inspect(
                {
                    "kind": "workspace",
                    "id": workspace_id,
                    "section": "missing",
                    "after": after,
                    "limit": 100,
                }
            )
            page = result.get("items") if isinstance(result, dict) else None
            if not isinstance(page, list):
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "peer.inspect returned no workspace items")
            items.extend(page)
            if not result.get("has_more"):
                return items
            cursor = result.get("cursor")
            if not isinstance(cursor, int) or isinstance(cursor, bool) or cursor <= after:
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "peer.inspect returned an invalid cursor")
            after = cursor

    async def _send_workspace_chunk(self, client, node, workspace_id, blob_sha256, chunk, offset):
        operation_id = hashlib.sha256(
            encode(
                {
                    "action": "chunk",
                    "workspace_id": workspace_id,
                    "blob_sha256": blob_sha256,
                    "offset": offset,
                }
            ).encode("utf-8")
        ).hexdigest()
        await client.workspace(
            {
                "operation": {
                    "node_id": node.node_id,
                    "state_generation": node.state_generation,
                    "operation_id": operation_id,
                },
                "action": "chunk",
                "workspace_id": workspace_id,
                "blob_sha256": blob_sha256,
                "offset": offset,
                "size": len(chunk),
                "chunk_sha256": hashlib.sha256(chunk).hexdigest(),
                "data": base64.b64encode(chunk).decode("ascii"),
            }
        )

    async def _upload_workspace_blob(self, client, node, workspace_id, blob_sha256, data):
        for offset in range(0, len(data), MAX_WORKSPACE_CHUNK_BYTES):
            await self._send_workspace_chunk(
                client,
                node,
                workspace_id,
                blob_sha256,
                data[offset:offset + MAX_WORKSPACE_CHUNK_BYTES],
                offset,
            )

    async def _prepare_remote_workspace(self, host_id, client, node, args):
        request_id = str(args.get("request_id") or "")
        project_id = args.get("project_id")
        if not isinstance(project_id, str) or not project_id:
            raise BridgeError("invalid_request", "project_id is required for a remote workspace-write open")
        root = self._source_repository(args.get("cwd"))
        base_commit = self._resolve_source_commit(root, args.get("base") or "HEAD")
        selection = args.get("snapshot_files")
        if selection is None:
            selection = []
        if not isinstance(selection, list):
            raise BridgeError("invalid_snapshot", "snapshot_files must be a list")
        selected = set(selection)

        inspected = await client.inspect(
            {
                "kind": "workspace",
                "section": "base",
                "project_id": project_id,
                "base_commit": base_commit,
            }
        )
        base_items = inspected.get("items") if isinstance(inspected, dict) else None
        remote_has_base = bool(
            isinstance(base_items, list) and base_items and isinstance(base_items[0], dict)
            and base_items[0].get("exists")
        )
        delivery_mode = "patch" if remote_has_base else "snapshot"
        identity_material = encode(
            {
                "host_id": host_id,
                "project_id": project_id,
                "request_id": request_id,
                "base_commit": base_commit,
                "selected_files": selection,
                "delivery_mode": delivery_mode,
            }
        )
        workspace_id = "ws-" + hashlib.sha256(identity_material.encode("utf-8")).hexdigest()[:32]
        authority_ref = {
            **groups.resolve_authority_ref(self.store, None),
            "local_id": request_id or workspace_id,
        }
        manifest = create_workspace_manifest(
            project_id,
            workspace_id,
            root,
            delivery_mode,
            base_commit=base_commit,
            selected_files=selection,
            authority_ref=None,
        )
        manifest_sha256 = compute_manifest_digest(manifest)
        manifest_data = encode(manifest).encode("utf-8")
        if len(manifest_data) > 32 * 1024 * 1024:
            raise BridgeError(PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED, "The workspace manifest exceeds 32 MiB")
        unique_sizes: dict[str, int] = {}
        for entry in manifest["files"]:
            if entry["op"] == "put":
                unique_sizes.setdefault(entry["sha256"], entry["size"])
        if len(unique_sizes) > MAX_WORKSPACE_FILES:
            raise BridgeError(PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED, "The workspace exceeds 100000 files")
        if any(size > MAX_WORKSPACE_FILE_BYTES for size in unique_sizes.values()):
            raise BridgeError(PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED, "A workspace file exceeds 64 MiB")
        estimated_total = len(manifest_data) + sum(unique_sizes.values())
        if estimated_total > MAX_WORKSPACE_TOTAL_BYTES:
            raise BridgeError(PeerErrorCode.WORKSPACE_LIMIT_EXCEEDED, "The workspace exceeds 1 GiB")

        prepare_payload = {
            "action": "prepare",
            "workspace_id": workspace_id,
            "project_id": project_id,
            "manifest_sha256": manifest_sha256,
            "manifest_size": len(manifest_data),
            "delivery_mode": delivery_mode,
            "workspace_mode": "git",
            "source_base_commit": manifest["source_base_commit"],
            "estimated_total_bytes": estimated_total,
            "authority_ref": authority_ref,
        }
        await client.workspace(
            {
                "operation": self._workspace_operation(host_id, node, request_id, prepare_payload),
                **prepare_payload,
            }
        )

        await self._upload_workspace_blob(client, node, workspace_id, manifest_sha256, manifest_data)
        missing = await self._workspace_missing(client, workspace_id)
        for item in missing:
            if item.get("blob_sha256") != manifest_sha256:
                continue
            offset = item.get("offset")
            size = item.get("size")
            if not isinstance(offset, int) or not isinstance(size, int):
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "peer.inspect returned an invalid manifest chunk")
            chunk = manifest_data[offset:offset + size]
            if len(chunk) != size:
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "peer.inspect returned an out-of-range manifest chunk")
            await self._send_workspace_chunk(client, node, workspace_id, manifest_sha256, chunk, offset)
        if any(item.get("blob_sha256") == manifest_sha256 for item in missing):
            missing = await self._workspace_missing(client, workspace_id)

        entries_by_sha: dict[str, dict] = {}
        for entry in manifest["files"]:
            if entry["op"] == "put":
                entries_by_sha.setdefault(entry["sha256"], entry)
        for item in missing:
            digest = item.get("sha256")
            if digest is None:
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "The workspace manifest was not verified")
            entry = entries_by_sha.get(digest)
            if entry is None:
                raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "The remote requested an unknown workspace blob")
            data = self._source_entry_bytes(root, manifest["source_base_commit"], entry, selected)
            await self._upload_workspace_blob(client, node, workspace_id, digest, data)

        status = await client.inspect({"kind": "workspace", "id": workspace_id, "section": "status"})
        status_items = status.get("items") if isinstance(status, dict) else None
        if not isinstance(status_items, list) or not status_items or not isinstance(status_items[0], dict):
            raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "peer.inspect returned no workspace status")
        revision = status_items[0].get("revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
            raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "peer.inspect returned an invalid workspace revision")
        seal_payload = {
            "action": "seal",
            "workspace_id": workspace_id,
            "manifest_sha256": manifest_sha256,
            "expected_revision": revision,
        }
        sealed = await client.workspace(
            {
                "operation": self._workspace_operation(host_id, node, request_id, seal_payload),
                **seal_payload,
            }
        )
        if not isinstance(sealed, dict) or sealed.get("status") != "ready":
            raise BridgeError(PeerErrorCode.WORKSPACE_CONFLICT, "The remote workspace did not become ready")
        return {
            **args,
            "access": "workspace-write",
            "workspace_mode": "git",
            "workspace_ref": {
                "workspace_id": workspace_id,
                "manifest_sha256": manifest_sha256,
                "input_digest": manifest["input_digest"],
            },
        }

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
        target_id = args.get("session_id") or ""
        if self._remote_workspace_requested(method, args, target_id):
            args = await self._prepare_remote_workspace(host_id, client, node, args)
        operation_id = self._stable_operation_id(
            host_id, method, str(args.get("request_id") or ""), args
        )
        target_seq = self._target_seq(operation_id)
        parent_ref = args.get("parent_ref")
        if parent_ref is None and args.get("parent"):
            parent_id = args.get("parent")
            if not isinstance(parent_id, str) or not parent_id:
                raise BridgeError("invalid_request", "parent must be a non-empty string")
            if self.store.get_parent(parent_id) is None:
                raise BridgeError("not_found", f"parents: {parent_id}")
            from .node_identity import FileNodeIdentityStore

            local = FileNodeIdentityStore(self.state).node_ref("local").as_dict()
            parent_ref = {**local, "local_id": parent_id}
        if parent_ref is not None and not isinstance(parent_ref, dict):
            raise BridgeError("invalid_request", "parent_ref must be an object")
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
        if parent_ref is not None:
            parameters["parent_ref"] = parent_ref
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
            stored_params = existing.get("params") if isinstance(existing.get("params"), dict) else {}
            stored_parent = stored_params.get("parent_ref")
            if isinstance(stored_parent, dict):
                if target_id:
                    self.store.register_parent_route(
                        host_id, node.node_id, node.state_generation, target_id, stored_parent,
                    )
                stored_result = existing.get("result")
                if isinstance(stored_result, dict):
                    for field in ("session_id", "task_id"):
                        child_id = stored_result.get(field)
                        if isinstance(child_id, str) and child_id:
                            self.store.register_parent_route(
                                host_id, node.node_id, node.state_generation, child_id, stored_parent,
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
        if parent_ref is not None and target_id:
            self.store.register_parent_route(host_id, node.node_id, node.state_generation, target_id, parent_ref)
        try:
            result = await client.deliver(parameters)  # type: ignore[arg-type]
        except BridgeError as error:
            self.store.update_operation(operation_id, status="uncertain", error=error.as_dict())
            raise
        self.store.update_operation(operation_id, status="completed", result=result)
        if parent_ref is not None:
            for field in ("session_id", "task_id"):
                child_id = result.get(field) if isinstance(result, dict) else None
                if isinstance(child_id, str) and child_id:
                    self.store.register_parent_route(
                        host_id, node.node_id, node.state_generation, child_id, parent_ref,
                    )
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
                outbox = self.store.get_node_outbox(operation_id)
                if outbox is not None:
                    view = {
                        "operation_id": operation_id,
                        "host_id": "local",
                        "status": outbox["status"],
                        "outbox": {
                            "group_id": outbox.get("group_id"),
                            "native_id": outbox.get("native_id"),
                            "method": outbox.get("method"),
                            "created": outbox.get("created"),
                            "pulled_at": outbox.get("pulled_at"),
                            "acked_at": outbox.get("acked_at"),
                        },
                    }
                    if outbox.get("outcome") is not None:
                        view["outcome"] = outbox["outcome"]
                    if outbox.get("result") is not None:
                        view["result"] = outbox["result"]
                    if outbox["status"] in {"pending", "pulled"} and asyncio.get_running_loop().time() < deadline:
                        await asyncio.sleep(0.15)
                        continue
                    return view
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
            return groups.inspect_groups(
                self.store, args.get("group_id"), args.get("active_only", False),
            )
        if method == "agent_group_prune":
            return await groups.prune_groups(
                self.store, self, args["older_than"], args.get("dry_run", True), args["request_id"],
            )
        if method == "agent_group":
            remote_members = args.get("remote_members")
            if remote_members is not None and args.get("action") in {"create", "add"}:
                remote_members = await self._resolve_remote_members(remote_members)
            return groups.control_group(
                self.store, args["group_id"], args["action"], args["expected_revision"],
                args["request_id"], args.get("session_ids"), args.get("native_members"), remote_members,
            )
        if method == "agent_broadcast":
            binding_id, binding_token = self._member_binding_args(args)
            if (
                binding_id is not None
                or binding_token is not None
                or isinstance(args.get("sender_ref"), dict)
            ):
                return self._member_broadcast(args, binding_id, binding_token)
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

    @property
    def token(self) -> str:
        """Current admin token, re-read from disk on every access so rotation applies immediately."""
        try:
            return read_admin_token(self.state)
        except (BridgeError, ValueError) as error:
            raise BridgeError("forbidden", "Management token is unavailable or insecure: " + safe_error(error)) from error

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
        with contextlib.suppress(ConnectionError):
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
        outbox_puller = asyncio.create_task(self.outbox_puller.run())
        event_mirror = asyncio.create_task(self.event_mirror.run())
        try:
            async with server:
                await self.stopping.wait()
        finally:
            await scheduler
            await notifications
            await native_notifications
            for task in (delivery_scheduler, outbox_puller, event_mirror):
                task.cancel()
            await asyncio.gather(delivery_scheduler, outbox_puller, event_mirror, return_exceptions=True)
            path.unlink(missing_ok=True)
            lock.close()


async def request(state: Path, method, params):
    if os.environ.get("AGENT_BRIDGE_ROLE") == "child":
        raise BridgeError("forbidden", "Child runtimes may not administer parent or sibling sessions")
    reader, writer = await asyncio.open_unix_connection(str(state / "broker.sock"), limit=16 * 1024 * 1024)
    try:
        token = os.environ.get("AGENT_BRIDGE_TOKEN") or read_admin_token(state)
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
