> Language: English | [中文](docs/zh/CONTRIBUTING.md)

# Contributing

Changes are welcome under [Apache-2.0](LICENSE). Contributions must be your own work or have compatible permission; retain any required third-party notices.

## Development

Install Python 3.11+ and uv, then run:

```sh
uv sync --locked
uv run ruff check src tests scripts skills/cli-agent-bridge/scripts
uv run python -m unittest discover -s tests -v
uv build
```

Tests use temporary state and mocked agent CLIs. Never run real-model smoke scripts, install services, broadcast to native sessions, or use another person's runtime database as part of a routine test. Such checks need explicitly selected targets and authorization and may incur provider costs.

Keep a change focused. Add a regression test for a confirmed defect and test idempotent retries, cancellation, persistence, and failure paths when relevant. Update the English documentation and its `docs/zh/` counterpart for user-facing changes. Examples must use synthetic hostnames, paths, identities, and data. Never commit credentials, databases, raw conversations, private computer paths, or recordings.

A pull request should describe the concrete problem, changed behavior, checks actually run, and anything unverified. Screenshots for UI changes should contain synthetic data. Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
