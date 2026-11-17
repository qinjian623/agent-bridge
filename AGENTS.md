> Language: English | [中文](docs/zh/AGENTS.md)

# Repository Guidelines

## Project Structure

- `src/agent_bridge/`: Python source code. `cli.py` and `mcp_server.py` provide entrypoints, `broker.py` manages requests, `worker.py` runs tasks, `init_config.py` drives interactive/non-interactive configuration, `updater.py` handles verified GitHub Releases OTA updates, and `store.py` persists SQLite state.
- `tests/`: Automated tests; `tests/fixtures/` provides mock Codex and Antigravity CLIs.
- `scripts/`: Local install, smoke check, benchmark, and task ledger helpers.
- `docs/`: English design specs and verification records; `docs/zh/`: Chinese translations and mirrored documentation; `skills/cli-agent-bridge/`: companion skill and prompt resources.

## Installation, Running, and Checks

Requires Python 3.11+ and `uv`; managed user service features require Linux user-level `systemd` (or macOS `launchd`), and calling real models requires logging in via the target CLI beforehand.

```bash
uv sync --locked                         # Install dependencies pinned in uv.lock
uv run agent-bridge init                 # Initialize or edit local configuration interactively
uv run agent-bridge doctor               # Check configuration and runtime environment
uv run agent-bridge serve                # Run the broker in the foreground
uv run python -m unittest discover -s tests -v
uv run ruff check src tests scripts
```

The final two commands run the full automated test suite and static lint checks. Run commands from the repository root; when changing dependencies, update both `pyproject.toml` and `uv.lock`.

## Code Style and Naming

Use 4-space indentation. Modules, functions, and variables use `snake_case`; classes use `PascalCase`; preserve existing type annotations and error-handling conventions. Ruff configuration lives in `pyproject.toml` (target Python 3.11, reference line length 120 without `E501` enforcement). Reuse existing helper functions rather than adding single-use wrappers.

## Testing Requirements

Use `unittest.TestCase`, and `unittest.IsolatedAsyncioTestCase` for async tests. Name files `tests/test_<module>.py` and methods `test_<behavior>`. Example targeted run:

```bash
uv run python -m unittest discover -s tests -p 'test_store.py' -v
```

Add regression tests reproducing bugs when fixing defects. For request retry, concurrency, or process exit changes, verify idempotency and state persistence. Use temporary state directories, mock CLIs, and mocks; no percentage coverage threshold is enforced. Real CLI smoke scripts may incur model API charges or affect real sessions—verify script arguments and targets before running them.

## Commits and Pull Requests

Use short imperative English commit subjects following existing history (for example, `Add session groups and broadcast to bridge and native sessions`, `Fix ...`). Keep each commit focused on related changes.

Pull requests should describe the problem, updated behavior, checks executed, and observed results; link related issues when applicable. Update `README.md` (and `docs/zh/` translations where appropriate) whenever interfaces, configuration, or commands change. Clearly separate simulated test results from real-CLI verification results.

## Configuration and Private Data

The default state directory is `~/.local/state/cli-agent-bridge`; use `agent-bridge --state /tmp/bridge-check ...` for isolated checks. Never commit API keys, credential files, runtime SQLite databases, raw chat transcripts, or screen recordings. Referenced `env_file` files must be owner-only readable/writable (`0600`). Installation helpers create command links and user services; do not casually restart already-running user services or message real sessions while editing code.
