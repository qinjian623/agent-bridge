from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys

from .config import Config, state_directory
from .errors import BridgeError


class _AsyncStdinReader:
    """Async line reader over a blocking binary stream such as sys.stdin.buffer."""

    def __init__(self, stream):
        self._stream = stream

    async def readline(self):
        return await asyncio.to_thread(self._stream.readline)


def build_parser():
    parser = argparse.ArgumentParser(description="Persistent local Codex / Claude Code / Pi / Antigravity sessions")
    parser.add_argument("--state")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Write a private example config; never overwrite existing config")
    commands.add_parser("serve", help="Run the persistent Unix socket broker")
    commands.add_parser("mcp", help="Run the MCP tools over stdio")
    commands.add_parser("doctor", help="Check local configuration and installed CLI paths")
    relay = commands.add_parser("relay", help="Bridge stdin/stdout peer JSONL to the local node service")
    relay.add_argument("--state", default=argparse.SUPPRESS)
    worker = commands.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("session_id")
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
                "worker_launcher": "systemd",
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
        elif args.command == "worker":
            from .worker import Worker

            asyncio.run(Worker(state, Config.load(state / "config.json"), args.session_id).run())
        elif args.command == "relay":
            from .node_identity import FileNodeIdentityStore
            from .peer_api import DirectPeerService
            from .peer_transport import DirectRelayServer

            service = DirectPeerService(FileNodeIdentityStore(state), host_id=socket.gethostname())
            asyncio.run(DirectRelayServer(service).serve(_AsyncStdinReader(sys.stdin.buffer), sys.stdout.buffer))
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
