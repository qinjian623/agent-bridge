from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import uuid

from .config import Config, state_directory
from .errors import BridgeError
from .init_config import add_init_arguments, detect_environment, run_init


class _AsyncStdinReader:
    """Async line reader over a blocking binary stream such as sys.stdin.buffer."""

    def __init__(self, stream):
        self._stream = stream

    async def readline(self):
        return await asyncio.to_thread(self._stream.readline)


def _relay_release_changed(state):
    """Retire a managed relay between frames when its stable launcher changes release."""
    from .updater import release_change_detector

    return release_change_detector(state)


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
    init_parser = commands.add_parser(
        "init",
        help="Interactively configure or idempotently update config.json, services, and GitHub OTA",
    )
    add_init_arguments(init_parser)
    commands.add_parser("serve", help="Run the persistent Unix socket broker")
    commands.add_parser("mcp", help="Run the MCP tools over stdio")
    commands.add_parser("doctor", help="Check local configuration and installed CLI paths")
    commands.add_parser(
        "update",
        add_help=False,
        help="Check or install verified GitHub releases",
    )
    relay = commands.add_parser("relay", help="Bridge stdin/stdout peer JSONL to the local node service")
    relay.add_argument("--state", default=argparse.SUPPRESS)
    worker = commands.add_parser("worker", help="Internal worker command (managed automatically)")
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
    mirrors = commands.add_parser("mirrors", help="Inspect and prune duplicated peer event mirror history")
    mirrors_commands = mirrors.add_subparsers(dest="mirrors_command", required=True)
    mirrors_prune = mirrors_commands.add_parser(
        "prune", help="Deduplicate mirrored peer events while keeping deliveries, notifications, and cursors intact",
    )
    mirrors_prune.add_argument(
        "--dry-run", action="store_true", help="Report mirror duplication and reclaimable space without modifying the database",
    )
    mirrors_prune.add_argument(
        "--vacuum", action="store_true", help="Run SQLite VACUUM after pruning (requires the broker and workers to be stopped)",
    )
    mirrors_prune.add_argument(
        "--keep-self-origins", action="store_true",
        help="Keep one canonical copy of self-origin mirrored echoes instead of removing unreferenced echoes",
    )
    mirrors_prune.add_argument(
        "--no-repair-origins", action="store_true",
        help="Skip re-unwrapping stored origin metadata on existing mirror rows",
    )
    mirrors_prune.add_argument(
        "--batch-size", type=int, default=500, help="Maximum rows processed per read/delete batch (default: 500)",
    )
    name_cmd = commands.add_parser("name", help="Set human-friendly alias name for a session")
    name_cmd.add_argument("session_id", help="Session ID (prefix or full UUID)")
    name_cmd.add_argument("name", help="Human-readable alias name")
    call = commands.add_parser("call", help="Call a tool using JSON from --json or stdin")
    call.add_argument("method")
    call.add_argument("--json", default=None)
    return parser


def main(argv: list[str] | None = None):
    os.umask(0o077)
    parser = build_parser()
    args, update_args = parser.parse_known_args(argv)
    if args.command != "update" and update_args:
        parser.parse_args(argv)
    try:
        state = state_directory(args.state)
        if args.command == "mirrors":
            if args.mirrors_command == "prune":
                from .store import Store

                result = Store(state).prune_duplicate_mirrors(
                    dry_run=args.dry_run,
                    batch_size=args.batch_size,
                    prune_self_origins=not args.keep_self_origins,
                    repair_origins=not args.no_repair_origins,
                    vacuum=args.vacuum,
                )
                print(json.dumps(result, indent=2, ensure_ascii=False))
        elif args.command == "init":
            print(json.dumps(run_init(state, args), ensure_ascii=False, indent=2))
        elif args.command == "serve":
            from .broker import Broker

            asyncio.run(Broker(state).serve())
        elif args.command == "update":
            from .updater import main as update_main

            update_main(["--state", str(state), *update_args])
        elif args.command == "worker":
            from .worker import Worker

            asyncio.run(Worker(state, Config.load(state / "config.json"), args.session_id).run())
        elif args.command == "relay":
            from .broker import request
            from .discovery import ModelDirectory
            from .node_identity import FileNodeIdentityStore
            from .peer_api import DirectPeerService
            from .peer_transport import DirectRelayServer
            from .store import Store

            config_path = state / "config.json"
            config = Config.load(config_path) if config_path.exists() else None

            async def capability_probe():
                return await request(state, "agent_capabilities", {})

            service = DirectPeerService(
                FileNodeIdentityStore(state),
                host_id=socket.gethostname(),
                store=Store(state),
                config=config,
                config_path=config_path,
                model_directory=ModelDirectory(state),
                capability_probe=capability_probe,
            )
            asyncio.run(DirectRelayServer(service, retire=_relay_release_changed(state)).serve(
                _AsyncStdinReader(sys.stdin.buffer), sys.stdout.buffer,
            ))
        elif args.command == "mcp":
            from .mcp_server import server

            server(state).run(transport="stdio")
        elif args.command == "doctor":
            config = Config.load(state / "config.json")
            env = detect_environment()
            profiles = []
            for profile in config.profiles.values():
                try:
                    profiles.append(
                        {"name": profile.name, "executable": profile.command(), "verified": profile.verified}
                    )
                except BridgeError as error:
                    profiles.append({"name": profile.name, "error": error.as_dict()})
            managed = False
            update_configured = (state / "update.json").exists()
            if update_configured:
                from .updater import Updater

                try:
                    managed = Updater(state).service_managed()
                except (OSError, ValueError, RuntimeError):
                    managed = None
            warnings: list[str] = []
            if managed is None:
                warnings.append("Could not determine whether the broker service uses the managed launcher")
            if config.worker_launcher == "systemd" and not env["systemd_user"]:
                warnings.append("worker_launcher is 'systemd' but systemd --user is unavailable; consider 'detached'")
            if not env["tools"]["uv"]:
                warnings.append("'uv' executable not found on PATH; OTA self-update requires uv")
            if config.hosts and not env["tools"]["ssh"]:
                warnings.append("remote hosts are configured but 'ssh' executable was not found on PATH")
            print(
                json.dumps(
                    {
                        "profiles": profiles,
                        "launcher": config.worker_launcher,
                        "broker_socket": (state / "broker.sock").exists(),
                        "platform": env["platform"],
                        "uv": env["tools"]["uv"],
                        "ssh": env["tools"]["ssh"],
                        "systemd_user": env["systemd_user"],
                        "update_configured": update_configured,
                        "broker_service_managed": managed,
                        "warnings": warnings,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        elif args.command == "monitor":
            if args.web:
                if (
                    args.runtime != "all"
                    or args.source != "all"
                    or args.active_only
                    or args.session_limit is not None
                    or args.no_native
                ):
                    parser.error(
                        "--runtime, --source, --active-only, --session-limit, and --no-native "
                        "only apply to terminal monitor mode (use the Web UI filters or /api/v1/* query params)."
                    )
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
    except (BridgeError, OSError, ValueError, RuntimeError) as error:
        print(
            json.dumps(
                {"error": error.as_dict() if isinstance(error, BridgeError) else str(error)}, ensure_ascii=False
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
