from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import uuid
from pathlib import Path

from .config import Config, state_directory
from .errors import BridgeError


class _AsyncStdinReader:
    """Async line reader over a blocking binary stream such as sys.stdin.buffer."""

    def __init__(self, stream):
        self._stream = stream

    async def readline(self):
        return await asyncio.to_thread(self._stream.readline)


def _relay_release_changed(state):
    """Retire a managed relay between frames when its stable launcher changes release."""
    if not (state / "update.json").exists():
        return lambda: False
    from .updater import load_settings

    current = Path(load_settings(state)["root"]) / "current"
    initial = current.resolve()
    running = Path(sys.prefix).resolve()
    expected = running if running.parent == current.parent / "releases" else initial

    def changed():
        try:
            return current.resolve(strict=True) != expected
        except OSError:
            return True

    return changed


class _HostListAction(argparse.Action):
    """Collect repeated or comma-separated --allowed-hosts values into one flat list."""

    def __call__(self, parser, namespace, values, option_string=None):
        entries = list(getattr(namespace, self.dest, None) or [])
        for value in str(values).split(","):
            value = value.strip()
            if value:
                entries.append(value)
        setattr(namespace, self.dest, entries)


def build_parser():
    parser = argparse.ArgumentParser(description="Persistent local Codex / Claude Code / Pi / Antigravity sessions")
    parser.add_argument("--state")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Write a private example config; never overwrite existing config")
    commands.add_parser("serve", help="Run the persistent Unix socket broker")
    commands.add_parser("mcp", help="Run the MCP tools over stdio")
    commands.add_parser("doctor", help="Check local configuration and installed CLI paths")
    update = commands.add_parser("update", help="Check or install verified GitHub releases")
    update.add_argument("update_args", nargs=argparse.REMAINDER)
    relay = commands.add_parser("relay", help="Bridge stdin/stdout peer JSONL to the local node service")
    relay.add_argument("--state", default=argparse.SUPPRESS)
    worker = commands.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("session_id")
    monitor = commands.add_parser("monitor", help="Monitor active agents, tasks, and communications")
    monitor.add_argument("--web", action="store_true", help="Launch local web-based monitor dashboard")
    monitor.add_argument("--port", type=int, default=8080, help="Web monitor port (default: 8080)")
    monitor.add_argument("--host", default="127.0.0.1", help="Web monitor host (default: 127.0.0.1)")
    monitor.add_argument(
        "--allowed-hosts",
        action=_HostListAction,
        default=None,
        metavar="HOST[:PORT]",
        help="Browser-visible Host values for the web monitor; repeat or comma-separate (default: loopback only)",
    )
    monitor.add_argument(
        "--public-origin",
        default=None,
        metavar="ORIGIN",
        help="Explicit public origin for an HTTPS reverse proxy, for example https://monitor.example.com",
    )
    monitor.add_argument("--interval", type=float, default=2.0, help="Terminal refresh interval in seconds (default: 2.0)")
    monitor.add_argument("--once", action="store_true", help="Print a single terminal dashboard snapshot and exit")
    monitor.add_argument("--format", choices=["text", "json"], default="text", help="Output format for --once (default: text)")
    monitor.add_argument(
        "--runtime",
        choices=["all", "pi", "claude", "agy", "antigravity", "codex"],
        default="all",
        help="Filter sessions and events to a specific runtime",
    )
    monitor.add_argument(
        "--source",
        choices=["all", "bridge", "native"],
        default="all",
        help="Filter sessions by source (bridge, native, or all)",
    )
    monitor.add_argument("--active-only", action="store_true", help="Show only active/running/waiting sessions")
    monitor.add_argument("--session-limit", type=int, default=None, help="Maximum sessions per source to return")
    monitor.add_argument("--no-native", action="store_true", help="Skip native local CLI process discovery")
    group = commands.add_parser("group", help="Inspect and prune explicit groups")
    group_commands = group.add_subparsers(dest="group_command", required=True)
    group_prune = group_commands.add_parser(
        "prune", help="Soft-delete groups with no activity older than the threshold",
    )
    group_prune.add_argument(
        "--older-than", required=True, metavar="DURATION",
        help="Positive duration such as 7d, 12h, 2w or 3600s; activity at or after the cutoff is kept",
    )
    group_prune.add_argument(
        "--dry-run", action="store_true", help="Only report eligible and blocked groups; do not delete",
    )
    name_cmd = commands.add_parser("name", help="Set human-friendly alias name for a session")
    name_cmd.add_argument("session_id", help="Session ID (prefix or full UUID)")
    name_cmd.add_argument("name", help="Human-readable alias name")
    call = commands.add_parser("call", help="Call a tool using JSON from --json or stdin")
    call.add_argument("method")
    call.add_argument("--json", default=None)
    return parser


def main():
    os.umask(0o077)
    args = build_parser().parse_args()
    try:
        state = state_directory(args.state)
        if args.command == "init":
            template = {
                "profiles": {
                    "codex-sol": {
                        "runtime": "codex",
                        "provider": "openai-codex",
                        "model": "gpt-6.1-sol",
                        "effort": "low",
                        "quota_group": "codex-account",
                    },
                    "pi-deepseek": {
                        "runtime": "pi",
                        "provider": "deepseek",
                        "model": "deepseek-flash",
                        "quota_group": "deepseek-account",
                    },
                },
                "worker_launcher": "detached" if sys.platform == "darwin" else "systemd",
                "parallel_limit": 3,
                "quota_limit": 2,
                "default_strategy": "balanced",
            }
            with (state / "config.json").open("x") as file:
                json.dump(template, file, indent=2)
            print(
                json.dumps(
                    {
                        "config": str(state / "config.json"),
                        "note": "Profiles remain unverified until a real task passes; add Claude with the actual provider/model.",
                    }
                )
            )
        elif args.command == "serve":
            from .broker import Broker

            asyncio.run(Broker(state).serve())
        elif args.command == "update":
            from .updater import main as update_main

            update_main(["--state", str(state), *args.update_args])
        elif args.command == "worker":
            from .worker import Worker

            asyncio.run(Worker(state, Config.load(state / "config.json"), args.session_id).run())
        elif args.command == "relay":
            from .discovery import ModelDirectory
            from .node_identity import FileNodeIdentityStore
            from .peer_api import DirectPeerService
            from .peer_transport import DirectRelayServer
            from .store import Store

            config_path = state / "config.json"
            config = Config.load(config_path) if config_path.exists() else None
            service = DirectPeerService(
                FileNodeIdentityStore(state),
                host_id=socket.gethostname(),
                store=Store(state),
                config=config,
                config_path=config_path,
                model_directory=ModelDirectory(state),
            )
            asyncio.run(DirectRelayServer(service, retire=_relay_release_changed(state)).serve(
                _AsyncStdinReader(sys.stdin.buffer), sys.stdout.buffer,
            ))
        elif args.command == "mcp":
            from .mcp_server import server

            server(state).run(transport="stdio")
        elif args.command == "doctor":
            config = Config.load(state / "config.json")
            profiles = []
            for profile in config.profiles.values():
                try:
                    profiles.append(
                        {"name": profile.name, "executable": profile.command(), "verified": profile.verified}
                    )
                except BridgeError as error:
                    profiles.append({"name": profile.name, "error": error.as_dict()})
            print(
                json.dumps(
                    {
                        "profiles": profiles,
                        "launcher": config.worker_launcher,
                        "broker_socket": (state / "broker.sock").exists(),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        elif args.command == "monitor":
            if args.web:
                from .web_monitor import run_web_monitor

                run_web_monitor(
                    state,
                    host=args.host,
                    port=args.port,
                    allowed_hosts=args.allowed_hosts,
                    public_origin=args.public_origin,
                )
            else:
                from .monitor import run_tui_monitor

                run_tui_monitor(
                    state,
                    interval=args.interval,
                    once=args.once,
                    output_format=args.format,
                    runtime=args.runtime,
                    source=args.source,
                    active_only=args.active_only,
                    session_limit=args.session_limit,
                    include_native=not args.no_native,
                )
        elif args.command == "name":
            from .monitor import set_session_name

            set_session_name(state, args.session_id, args.name)
            print(f"Session {args.session_id} named: {args.name}")
        elif args.command == "group":
            from .broker import request

            result = asyncio.run(request(state, "agent_group_prune", {
                "older_than": args.older_than,
                "dry_run": bool(args.dry_run),
                "request_id": uuid.uuid4().hex,
            }))
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            from .broker import request

            params = json.loads(args.json if args.json is not None else sys.stdin.read() or "{}")
            print(json.dumps(asyncio.run(request(state, args.method, params)), ensure_ascii=False, indent=2))
    except (BridgeError, OSError, ValueError) as error:
        print(
            json.dumps(
                {"error": error.as_dict() if isinstance(error, BridgeError) else str(error)}, ensure_ascii=False
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
