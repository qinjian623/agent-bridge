from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import socket
import sys
import unicodedata
import uuid
from contextlib import nullcontext

from .config import Config, broker_socket_path, resolve_state_directory, state_directory
from .errors import BridgeError
from .i18n import LANGUAGE_ENV, resolve_language, use_language, validate_language
from .i18n import translate as _
from .init_config import add_init_arguments, detect_environment, run_init


class _HelpFormatter(argparse.HelpFormatter):
    def start_section(self, heading):
        super().start_section(_(heading))

    def add_usage(self, usage, actions, groups, prefix=None):
        super().add_usage(usage, actions, groups, prefix=_("usage: ") if prefix is None else prefix)

    def _split_lines(self, text, width):
        if resolve_language() != "zh-CN":
            return super()._split_lines(text, width)
        text = self._whitespace_matcher.sub(" ", text).strip()
        lines, line, columns = [], "", 0
        for char in text:
            size = 0 if unicodedata.category(char) in {"Mn", "Me", "Cf"} else (
                2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1
            )
            if line and columns + size > width:
                lines.append(line.rstrip())
                line, columns = "", 0
            if line or not char.isspace():
                line += char
                columns += size
        if line:
            lines.append(line)
        return lines

    def _fill_text(self, text, width, indent):
        if resolve_language() != "zh-CN":
            return super()._fill_text(text, width, indent)
        return "\n".join(indent + line for line in self._split_lines(text, max(1, width - len(indent))))


def _argument_error(message: str) -> str:
    """Translate argparse's own templates while leaving argument values untouched."""
    patterns = (
        (r"argument (.+?): (.*)", "argument {argument}: {detail}", ("argument", "detail")),
        (r"unrecognized arguments: (.*)", "unrecognized arguments: {arguments}", ("arguments",)),
        (r"the following arguments are required: (.*)", "the following arguments are required: {arguments}", ("arguments",)),
        (r"invalid choice: (.*?) \(choose from (.*)\)", "invalid choice: {value} (choose from {choices})", ("value", "choices")),
        (r"invalid (.+?) value: (.*)", "invalid {type} value: {value}", ("type", "value")),
        (r"ambiguous option: (.*?) could match (.*)", "ambiguous option: {option} could match {matches}", ("option", "matches")),
    )
    for pattern, template, fields in patterns:
        if match := re.fullmatch(pattern, message, re.DOTALL):
            values = dict(zip(fields, match.groups(), strict=True))
            if "detail" in values:
                values["detail"] = _argument_error(values["detail"])
            return _(template, **values)
    return _(message)


class _ArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("formatter_class", _HelpFormatter)
        super().__init__(*args, **kwargs)

    def add_argument(self, *args, **kwargs):
        if kwargs.get("help"):
            kwargs["help"] = _(kwargs["help"])
        return super().add_argument(*args, **kwargs)

    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(2, _("{program}: error: {detail}\n", program=self.prog, detail=_argument_error(message)))


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
    parser = _ArgumentParser(description=_("Persistent local Codex / Claude Code / Pi / Antigravity sessions"))
    parser.add_argument("--state")
    parser.add_argument("--lang", "--language", type=validate_language, help=_("Display language: en, zh-CN or auto (default: English)"))
    commands = parser.add_subparsers(dest="command", required=True)
    init_parser = commands.add_parser(
        "init",
        help=_('Interactively configure or idempotently update config.json, services, and GitHub OTA'),
    )
    add_init_arguments(init_parser)
    commands.add_parser("serve", help=_('Run the persistent Unix socket broker'))
    commands.add_parser("mcp", help=_('Run the MCP tools over stdio'))
    integration = commands.add_parser(
        "integrate", help=_('Install the Bridge MCP server and skill for one agent'),
    )
    integration.add_argument("agent", choices=("codex", "claude", "claude-code", "pi", "antigravity", "agy"))
    commands.add_parser("doctor", help=_('Check local configuration and installed CLI paths'))
    commands.add_parser(
        "update",
        add_help=False,
        help=_('Check or install verified GitHub releases'),
    )
    relay = commands.add_parser("relay", help=_('Bridge stdin/stdout peer JSONL to the local node service'))
    relay.add_argument("--state", default=argparse.SUPPRESS)
    worker = commands.add_parser("worker", help=_('Internal worker command (managed automatically)'))
    worker.add_argument("session_id")
    monitor = commands.add_parser("monitor", help=_('Monitor active agents, tasks, and communications'))
    monitor.add_argument("--web", action="store_true", help=_('Launch local web-based monitor dashboard'))
    monitor.add_argument("--port", type=int, default=8080, help=_('Web monitor port (default: 8080)'))
    monitor.add_argument("--host", default="127.0.0.1", help=_('Web monitor host (default: 127.0.0.1)'))
    monitor.add_argument(
        "--allowed-hosts",
        action=_HostListAction,
        default=None,
        metavar="HOST[:PORT]",
        help=_('Browser-visible Host values for the web monitor; repeat or comma-separate (default: loopback only)'),
    )
    monitor.add_argument(
        "--public-origin",
        default=None,
        metavar="ORIGIN",
        help=_('Explicit public origin for an HTTPS reverse proxy, for example https://monitor.example.com'),
    )
    monitor.add_argument("--interval", type=float, default=2.0, help=_('Terminal refresh interval in seconds (default: 2.0)'))
    monitor.add_argument("--once", action="store_true", help=_('Print a single terminal dashboard snapshot and exit'))
    monitor.add_argument("--format", choices=["text", "json"], default="text", help=_('Output format for --once (default: text)'))
    monitor.add_argument(
        "--runtime",
        choices=["all", "pi", "claude", "agy", "antigravity", "codex"],
        default="all",
        help=_('Filter sessions and events to a specific runtime'),
    )
    monitor.add_argument(
        "--source",
        choices=["all", "bridge", "native"],
        default="all",
        help=_('Filter sessions by source (bridge, native, or all)'),
    )
    monitor.add_argument("--active-only", action="store_true", help=_('Show only active/running/waiting sessions'))
    monitor.add_argument("--session-limit", type=int, default=None, help=_('Maximum sessions per source to return'))
    monitor.add_argument("--no-native", action="store_true", help=_('Skip native local CLI process discovery'))
    group = commands.add_parser("group", help=_('Inspect and prune explicit groups'))
    group_commands = group.add_subparsers(dest="group_command", required=True)
    group_prune = group_commands.add_parser(
        "prune", help=_('Soft-delete groups with no activity older than the threshold'),
    )
    group_prune.add_argument(
        "--older-than", required=True, metavar="DURATION",
        help=_('Positive duration such as 7d, 12h, 2w or 3600s; activity at or after the cutoff is kept'),
    )
    group_prune.add_argument(
        "--dry-run", action="store_true", help=_('Only report eligible and blocked groups; do not delete'),
    )
    mirrors = commands.add_parser("mirrors", help=_('Inspect and maintain peer event mirrors'))
    mirrors_commands = mirrors.add_subparsers(dest="mirrors_command", required=True)
    mirrors_prune = mirrors_commands.add_parser(
        "prune", help=_('Deduplicate mirrored peer events while keeping deliveries, notifications, and cursors intact'),
    )
    mirrors_prune.add_argument(
        "--dry-run", action="store_true", help=_('Report mirror duplication and reclaimable space without modifying the database'),
    )
    mirrors_prune.add_argument(
        "--vacuum", action="store_true", help=_('Run SQLite VACUUM after pruning (requires the broker and workers to be stopped)'),
    )
    mirrors_prune.add_argument(
        "--keep-self-origins", action="store_true",
        help=_('Keep one canonical copy of self-origin mirrored echoes instead of removing unreferenced echoes'),
    )
    mirrors_prune.add_argument(
        "--no-repair-origins", action="store_true",
        help=_('Skip re-unwrapping stored origin metadata on existing mirror rows'),
    )
    mirrors_prune.add_argument(
        "--batch-size", type=int, default=500, help=_('Maximum rows processed per read/delete batch (default: 500)'),
    )
    mirrors_expire = mirrors_commands.add_parser(
        "expire", help=_('Expire old mirrored tool details while keeping event identities and references'),
    )
    mirrors_expire.add_argument("--days", type=int, default=30, help=_('Age in days before details can expire (default: 30)'))
    mirrors_expire.add_argument("--limit", type=int, default=500, help=_('Maximum mirrors scanned in one batch (default: 500)'))
    mirrors_expire.add_argument("--after", type=int, default=0, help=_('Start after this local event sequence (default: 0)'))
    mirrors_expire.add_argument("--apply", action="store_true", help=_('Expire eligible details (default: preview only)'))
    name_cmd = commands.add_parser("name", help=_('Set human-friendly alias name for a session'))
    name_cmd.add_argument("session_id", help=_('Session ID (prefix or full UUID)'))
    name_cmd.add_argument("name", help=_('Human-readable alias name'))
    call = commands.add_parser("call", help=_('Call a tool using JSON from --json or stdin'))
    call.add_argument("method")
    call.add_argument("--json", default=None)
    # Subcommands accept the global language option in the position users usually type it.
    def add_language_options(action):
        for command_parser in action.choices.values():
            command_parser.add_argument("--lang", "--language", type=validate_language, default=argparse.SUPPRESS,
                                        help=_("Display language: en, zh-CN or auto (default: English)"))
            for child in command_parser._actions:
                if isinstance(child, argparse._SubParsersAction):
                    add_language_options(child)
    add_language_options(commands)
    return parser


def main(argv: list[str] | None = None):
    argv = list(sys.argv[1:] if argv is None else argv)
    with use_language(None):
        preliminary = _ArgumentParser(add_help=False)
        preliminary.add_argument("--lang", "--language")
        known, _unknown = preliminary.parse_known_args(argv)
        language = None
        if known.lang is not None:
            try:
                language = validate_language(known.lang)
            except ValueError as error:
                preliminary.error(str(error))
        elif LANGUAGE_ENV in os.environ:
            language = resolve_language(os.environ[LANGUAGE_ENV])
        with use_language(language or "en"):
            preliminary.add_argument("--state")
            known, _unknown = preliminary.parse_known_args(argv)
            if language is None:
                # Help must remain available even when the rest of config.json needs repair.
                try:
                    state_path = resolve_state_directory(known.state)
                    payload = json.loads((state_path / "config.json").read_text(encoding="utf-8"))
                    language = payload.get("language") if isinstance(payload, dict) else None
                    if language is not None:
                        language = validate_language(language)
                except (OSError, RuntimeError, ValueError):
                    language = None
            with use_language(language or "en"):
                _main(argv)


def _main(argv: list[str]):
    os.umask(0o077)
    parser = build_parser()
    args, update_args = parser.parse_known_args(argv)
    if args.command != "update" and update_args:
        parser.parse_args(argv)
    try:
        state = state_directory(args.state)
        if args.command in {"doctor", "serve"}:
            broker_socket_path(state)
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
            else:
                import sqlite3

                from . import update_guard
                from .mirror_prune import expire_mirror_details

                try:
                    with update_guard.operation(state) if args.apply else nullcontext():
                        result = expire_mirror_details(
                            state / "bridge.sqlite", days=args.days, limit=args.limit, after=args.after, apply=args.apply,
                        )
                except sqlite3.Error as error:
                    raise BridgeError("mirror_expiration_failed", str(error)) from error
            print(json.dumps(result, indent=2, ensure_ascii=False))
        elif args.command == "init":
            print(json.dumps(run_init(state, args), ensure_ascii=False, indent=2))
        elif args.command == "serve":
            from .broker import Broker

            asyncio.run(Broker(state).serve())
        elif args.command == "integrate":
            from .integration import install_integration

            print(json.dumps(install_integration(state, args.agent), ensure_ascii=False, indent=2))
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
                warnings.append(_('Could not determine whether the broker service uses the managed launcher'))
            if config.worker_launcher == "systemd" and not env["systemd_user"]:
                warnings.append(_("worker_launcher is 'systemd' but systemd --user is unavailable; consider 'detached'"))
            if not env["tools"]["uv"]:
                warnings.append(_("'uv' executable not found on PATH; OTA self-update requires uv"))
            if config.hosts and not env["tools"]["ssh"]:
                warnings.append(_("remote hosts are configured but 'ssh' executable was not found on PATH"))
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
                        _('--runtime, --source, --active-only, --session-limit, and --no-native only apply to terminal monitor mode (use the Web UI filters or /api/v1/* query params).')
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
            print(_('Session {value0} named: {value1}', value0=args.session_id, value1=args.name))
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
                {"error": {**error.as_dict(), "message": _(str(error))} if isinstance(error, BridgeError) else _(str(error))}, ensure_ascii=False
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
