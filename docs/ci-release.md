> Language: English | [中文](zh/ci-release.md)

# GitHub Testing and Releases

`.github/workflows/ci.yml` runs on push, pull request, and manual dispatch:

- Ubuntu / Python 3.11, 3.13, 3.14: installs dependencies from `uv.lock`, then runs Ruff and the full offline test suite. Tests use mock CLIs and require no model accounts or API keys.
- After tests pass, Ubuntu and macOS build a wheel and source distribution (`sdist`), generate `SHA256SUMS` and `latest.json`, and install the wheel in a fresh virtual environment. The jobs check CLI help, idempotent initialization, and `doctor`. They also start a temporary foreground broker, compare its running version with the wheel metadata, and call `agent_inspect` over MCP. Build artifacts are retained for 14 days.

macOS jobs validate packaging, clean installation, and environment initialization; the full unit test suite (which includes Linux `/proc` inspection tests) currently runs on Ubuntu. CI passing does not claim real remote Antigravity/model CLI inference or live service upgrades were executed.

When publishing a stable release, keep `pyproject.toml`, package metadata (`__version__`), and `uv.lock` aligned, then push the matching `vX.Y.Z` tag. Tag mismatches abort publishing. Once all test and packaging jobs pass, the release job downloads the Ubuntu build artifacts, re-verifies SHA-256 digests, creates a draft release, uploads all release assets, and publishes the release:

```text
cli_agent_bridge-X.Y.Z-py3-none-any.whl
cli_agent_bridge-X.Y.Z.tar.gz
SHA256SUMS
latest.json
```

Test and packaging jobs hold read-only repository permissions; only the tag-triggered release job has write access to Releases. Third-party Actions are pinned to verified commit SHAs. The release workflow never publishes to PyPI, updates interactive terminals, restarts running Bridge daemons, or overwrites existing releases. Nodes with periodic updates enabled fetch `latest.json`, the wheel, and `SHA256SUMS` directly from public GitHub Releases over unauthenticated HTTPS without requiring `gh` CLI authentication; never commit GitHub tokens into repositories, workflows, or built packages.

## First release in a new public repository

Start the public repository from the reviewed source tree. Keep the private repository, its history, and its Release and Actions artifacts separate. Before pushing a release tag, change the install and clone URLs in both languages, the package's Repository and Issues links, the packaged skill's links, and the updater's default repository to the confirmed public address. Tag builds compare the installed wheel's links and default update source against the repository running CI.

Use a new version higher than the version existing nodes have installed. Keep the Python distribution name and wheel naming convention so older managed installations can recognize the new package. Existing nodes retain their configured update source until their operator switches it. Verify that an anonymous client can install from the public release, load MCP, and read the OTA manifest before directing nodes to it. See [privacy checks](privacy.md) and [switching update sources](getting-started.md#switching-update-sources).
