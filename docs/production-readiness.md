> English | [中文](zh/production-readiness.md)

# Candidate checks before production use

This records work on the unreleased `0.3.0` candidate. Code changes address the 16 initial findings and follow-up review findings. The frozen source passed 1,081 tests on each of Python 3.11, 3.13, and 3.14 without a source hash change. A fresh independent read-only reviewer accepted the named checks on the frozen source without further findings. This document does not claim industrial certification or verification beyond the checks listed here.

The supported use is one operating-system user and explicitly trusted SSH peers. Workers, worktrees, and copies use that user's identity. The project does not provide multi-tenant access control or protection from hostile processes running under the same user. See [security scope](../SECURITY.md).

## Changes made

The three reviews below describe the implementation **before these fixes**. Their reproductions and original test results are retained as historical records.

| Initial review | Findings addressed in the candidate |
| --- | --- |
| [Core execution and messages](industrial-core-review.md), 6 findings | Keep conflicting mirror fingerprints; require a matching local original before deleting a self-origin mirror; cancel pending remote native messages; connect public group cancellation to its saved queue; identify the remote task and outcome in parent notices; record Codex tool starts. |
| [Installation, updates, and Web security](industrial-install-security-review.md), 6 findings | Bound slow HTTP requests and handler concurrency; restore earlier service files/state after confirmed migration failure; preserve bootstrap launcher settings after OTA; remove the machine-specific proxy default; normalize macOS LaunchAgent names; allow the selected Web port with default loopback Host settings. |
| [Workspaces and result files](industrial-workspace-review.md), 4 findings | Accept incomplete multi-chunk input until its declared size arrives; retain accepted chunk bytes on conflicting retries; retain copy-mode output per attempt; allow new nested files in patch transfers while checking existing components. |

Follow-up fixes include:

- Reject malformed configuration, wrong field types, and unknown fields with clear errors; redact supplied sensitive names and preserve saved settings on failure.
- Retain copy-result bytes safely using pinned directory descriptors; reject symlink ancestors and FIFO replacements. Reject overlapping upload chunks inside the write transaction.
- Restore systemd enablement links on a failed install.
- Distinguish an absent service from an unknown probe result before writing files; stop after uncertain service/rollback outcomes and preserve recovery information. Failed restoration stops further service commands.
- Recheck mirror content and protected references when deleting, including changes made after the cleanup plan was prepared.
- Notify registered parents when local tasks require input. Keep execution, review, input, process presence, and unknown states distinct in monitoring; HTTP freshness and broker connectivity are separate.

These descriptions summarize changes in code. The frozen source passed independent static acceptance of the reviewed cases. See [changes](../CHANGELOG.md) for the candidate's other user-visible changes.

The tables below describe the original candidate snapshot. On 2026-11-17, the subsequent OTA source-compatibility fixes passed 1,091 offline tests on Python 3.13; that result does not replace the earlier multi-version checks or the unverified checks listed below.

## Checks performed locally

| Check | Observed result and limit |
| --- | --- |
| Complete frozen Python matrix | Python 3.11.18: 1,081 passed; Python 3.13.15: 1,081 passed; Python 3.14.8: 1,081 passed. The source hash remained unchanged throughout. |
| Latest targeted suites | Updater 80, configuration 10, initialization 17, workspace plus Store 96, monitor 81: passed. These targeted results do not replace the complete suite. |
| New-HOME Linux installation | Candidate `0.3.0`: 17 checks passed, including 12 invalid-input cases; CLI first task, exact-request retry, result/review, close, persistence after broker restart, and uninstall were checked with the repository Codex fixture. |
| Independent stdio MCP client | Initialized, listed 20 tools, and queried the fixture catalog. No real client registration or account authentication was tested. |
| Process cleanup | All 44 recorded installation-check processes exited; zero survivors and no forced cleanup. |
| Wheel and source archive | License/NOTICE, Apache-2.0 metadata, and packaged skill present; the archive pattern checks found zero private-content matches or prohibited runtime files. Pattern checks alone are not an exhaustive privacy audit. |
| Independent read-only review | A fresh DeepSeek checker inspected 19 source/test files and accepted the named updater, configuration, workspace, upload, and monitor checks with no further findings or conflicts. That checker had no execution tool; test execution was performed separately by the lead. |
| Browser visual inspection | **Not performed yet.** Dashboard changes and HTTP tests do not establish browser layout or interaction quality. |

The installation checks used Linux, Python 3.13, a new HOME/state/project, detached workers, and a mock model CLI. They did not install a real user service or call an external model. Details and their limits are in the [installation check](install-verification.md).

## CI and release handling

The [workflow](../.github/workflows/ci.yml) is configured to run the complete offline suite on Linux with Python 3.11, 3.13, and 3.14, then build and check clean packaging on Linux and macOS. It creates a wheel, source archive, `SHA256SUMS`, and `latest.json`. A matching stable version tag triggers automatic GitHub Release publication only after the required jobs pass. See [CI and releases](ci-release.md).

The preparation branch was pushed and its initial CI passed. On 2026-11-17, the 21 inspected release attachments containing private information and the old CI records and package artifacts were removed; old build caches were invalidated. Git history rewriting and the new main CI are checked separately. GitHub remains private, and no 0.3.0 tag, release or production upgrade was created. Do not republish the private backups or merge old clone history. See [privacy preparation](privacy.md).

## Work still unverified

- Browser visual and interaction checks remain pending.
- Real macOS service installation/restart/rollback, fresh SSH disconnect/reconnect behavior, and power-loss recovery were not repeated in this round. Older reports are not proof that this candidate passed them.
- Real external-model authentication, inference, tool execution, and interaction among multiple model CLIs were not repeated. Fixture completion proves the tested program flow, not provider availability or model quality.
- Live GitHub publication and download of this candidate, existing-installation migration, and cross-version recovery of production databases were not tested here.

Use the [installation guide](getting-started.md), [uninstall instructions](uninstall.md), and [privacy notes](privacy.md) when evaluating the candidate. A successful test or package build does not remove the remaining checks above.
