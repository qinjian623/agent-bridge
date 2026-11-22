> English | [中文](docs/zh/CHANGELOG.md)

# Changes

## 0.3.3 — 2026-11-22

- Use a short temporary socket for OTA self-tests so a long managed release path does not prevent installation.

## 0.3.2 — 2026-11-22

- Publish the first stable Release in the public repository. The curl installer checks the release wheel, installs it into the managed version directory, and enables scheduled OTA updates.
- Discover provider-specific reasoning levels and let new sessions choose a model and reasoning level explicitly.
- Configure off-peak profile preferences for new local tasks and record each selection with the task.
- Ignore nested launchd state fields when inspecting the broker service on macOS.

- Prepare version 0.3.1. Check installed wheel and broker versions and exercise MCP on both package runners. Reconfigure an existing installation's GitHub update source explicitly, with confirmation before clearing a configured mirror.
- Treat an empty state directory setting as unset. Reject non-private state directories without changing their permissions, and report Unix socket paths that are too long.
- Read managed task artifacts by ID after restoring a complete state directory to a new path. Document the remaining absolute paths and storage retention.
- Preserve detected CLI paths during interactive setup and include the detected uv directory in scheduled updater environments. Clarify how to check task results before enabling parent notifications.

- Add English and Simplified Chinese presentation for CLI help, setup, terminal and Web monitors, login, fixed human-facing errors, and parent notices. Add `--lang` / `--language`, `AGENT_BRIDGE_LANG`, the `language` config field, and a browser language selector. Keep protocol data and queued notice bodies unchanged.

## 0.3.0 — 2026-11-18

This release publishes the reviewed open-source candidate. It is not an industrial certification.

- Add Apache-2.0 licensing, English documentation with Chinese translations, a short installation guide, and safe uninstall instructions.
- Add interactive configuration, explicit first-run profiles, environment checks, and clear errors for invalid configuration. Profiles are not automatically marked inference-verified.
- Preserve accepted chunks on conflicting retries; support input files and manifests spanning multiple chunks, and new nested files in patch transfers.
- Retain copy-mode task output files per attempt so later tasks cannot replace earlier results. Result change records use `path` for the retained file and `source_path` for the active workspace file.
- Cancel unsent local and remote group messages without false success; refuse uncertain outcomes rather than resending or pretending cancellation worked.
- Identify remote tasks in parent notices and notify registered parents when local tasks need input. Notification transport requirements still apply; a notice does not complete or accept a task.
- Preserve differing mirror-event contents and recheck retained copies and protected references during pruning.
- Preserve explicitly configured release mirrors, including restricted loopback/Tailscale HTTP, across loading and initialization; failures do not silently switch to GitHub.
- Preserve valid bootstrap settings across OTA and stop service operations after uncertain outcomes. Keep earlier service files and known state on confirmed migration failures.
- Bound Web request concurrency and slow headers/bodies; keep authentication, Origin/CSRF checks, SSE checks, and explicit Host restrictions.
- Refresh the dashboard layout and distinguish task execution, review, input, process presence, and unknown states. HTTP freshness and broker connectivity are separate.
- Preserve inherited or explicitly empty proxy settings instead of choosing a local proxy port. Quote remote SSH command arguments without expanding shell syntax.

Configuration, task history, and working directories remain private state outside the repository. Back up state before upgrading. Worktrees and retained results are not OS-level protection against another process running as the same user.

Release preparation also checks historical refs and old downloadable assets. Publishing reviewed files does not remove sensitive content from earlier release attachments or other clones; see [privacy checks](docs/privacy.md).
