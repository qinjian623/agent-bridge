> English | [中文](zh/production-readiness.md)

# Checks and support limits

Bridge is intended for one operating-system user and explicitly trusted SSH peers. It does not provide multi-tenant access control or isolation from hostile processes running under the same user. See [security](../SECURITY.md).

## Automated checks

The [CI workflow](../.github/workflows/ci.yml) runs on pushes, pull requests, and manual dispatch:

| Job | What it checks |
| --- | --- |
| Linux / Python 3.11, 3.13, 3.14 | Locked dependencies, Ruff, publishable-content patterns, and the complete offline test suite. |
| Linux and macOS packaging | Wheel and source archive builds, package installation in a fresh environment, CLI entrypoints, idempotent initialization, and `doctor`. |

Tests use temporary state, mock CLIs, real stdio/Unix sockets, and isolated processes. They cover request retries, cancellation, persistence, permissions, workspace transfers, peer delivery, monitoring, and updater failure paths without calling a model provider.

Run the same source checks locally:

```sh
uv sync --locked
uv run ruff check src tests scripts skills/cli-agent-bridge/scripts
uv run python scripts/check_public_content.py
uv run python -m unittest discover -s tests -v
```

The [isolated installation record](install-verification.md) describes one dated candidate check with a fixture CLI. It does not establish that a current native account, production service, or host migration works.

## Checks outside ordinary CI

- Native authentication, provider availability, inference, tool execution, and live interruption require checks with the chosen CLI account and model. Discovery alone does not verify them.
- macOS CI checks package installation; it does not install or restart a real launchd service. Real service rollback, SSH reconnect behavior, and recovery after power loss need separate target-specific checks.
- HTTP and Web tests do not establish browser layout or interaction quality; inspect the dashboard in an actual browser.
- Existing production SQLite migrations and cross-version recovery require suitable backups and checks for that upgrade. Package installation is not a migration result.
- `scripts/*_smoke.py` and `scripts/benchmark.py` call real models when run manually. Select targets and obtain authorization for any real service, session, or notification changes.

A model reporting completion, a passing fixture, or a successful CI job proves only the checks it performed. None is an industrial certification or a guarantee about an untested environment. See [installation and use](getting-started.md), [CI and releases](ci-release.md), and [privacy checks](privacy.md).
