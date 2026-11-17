> Language: English | [中文](zh/ci-release.md)

# GitHub Testing and Releases

`.github/workflows/ci.yml` runs on push, pull request, and manual dispatch:

- Ubuntu / Python 3.11, 3.13, 3.14: installs dependencies from `uv.lock`, then runs Ruff and the full offline test suite. Tests use mock CLIs and require no model accounts or API keys.
- After tests pass, both Ubuntu and macOS build a wheel and source distribution (`sdist`), generate `SHA256SUMS` and `latest.json`, install the wheel inside a fresh virtual environment, verify CLI entrypoints, test idempotent `agent-bridge init` configuration, and run `agent-bridge doctor`. Build artifacts are retained as private Actions artifacts for 14 days.

macOS jobs validate packaging, clean installation, and environment initialization; the full unit test suite (which includes Linux `/proc` inspection tests) currently runs on Ubuntu. CI passing does not claim real remote Antigravity/model CLI inference or live service upgrades were executed.

When publishing a stable release, keep `pyproject.toml`, package metadata (`__version__`), and `uv.lock` aligned, then push the matching `vX.Y.Z` tag. Tag mismatches abort publishing. Once all test and packaging jobs pass, the release job downloads the Ubuntu build artifacts, re-verifies SHA-256 digests, creates a draft release, uploads all release assets, and publishes the release:

```text
cli_agent_bridge-X.Y.Z-py3-none-any.whl
cli_agent_bridge-X.Y.Z.tar.gz
SHA256SUMS
latest.json
```

Test and packaging jobs hold read-only repository permissions; only the tag-triggered release job has write access to Releases. Third-party Actions are pinned to verified commit SHAs. The release workflow never publishes to PyPI, updates interactive terminals, restarts running Bridge daemons, or overwrites existing releases. Nodes with periodic updates enabled fetch `latest.json`, the wheel, and `SHA256SUMS` directly from public GitHub Releases over unauthenticated HTTPS without requiring `gh` CLI authentication; never commit GitHub tokens into repositories, workflows, or built packages.
