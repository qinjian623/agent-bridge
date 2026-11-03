from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from .config import Config, state_directory
from .errors import BridgeError


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Persistent local Codex / Claude Code / Pi / Antigravity sessions")
    parser.add_argument("--state")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Write a private example config; never overwrite existing config")
    commands.add_parser("serve", help="Run the persistent Unix socket broker")
    commands.add_parser("mcp", help="Run the MCP tools over stdio")
    commands.add_parser("doctor", help="Check local configuration and installed CLI paths")
    worker = commands.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("session_id")
    call = commands.add_parser("call", help="Call a tool using JSON from --json or stdin")
    call.add_argument("method")
    call.add_argument("--json", default=None)
    args = parser.parse_args()
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
