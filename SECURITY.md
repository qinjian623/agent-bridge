> Language: English | [中文](docs/zh/SECURITY.md)

# Security

## Supported deployment

Bridge is designed for one operating-system user and explicitly trusted SSH peers. Each node owns its CLI accounts, files, database, and user services. A worktree or directory copy separates edits; it is not a security sandbox against malicious tools or code running as the same user. Native Windows is unsupported; use Linux, macOS, or WSL2.

Keep the web monitor on loopback by default. For remote access use an authenticated SSH tunnel, or a trusted HTTPS reverse proxy with explicit allowed hosts and public origin. Do not expose an unauthenticated proxy or share the administrator token with untrusted clients. Browser cookies and approval actions require the existing CSRF protections.

Credentials belong in each CLI's own authentication store or owner-only environment files. Do not put tokens, runtime databases, task transcripts, native session logs, or private host configuration in this repository, issues, screenshots, or pull requests. Treat model output, patches, repository content, and external messages as untrusted input.

Before upgrading, back up the private state directory and read the release notes. File checksums detect corrupted release downloads; they do not protect against a compromised publisher account. Keep uncertain operations available for inspection. Confirm what happened before retrying a side effect. Paused and closed coordinators do not receive automatic wakeups.

See [privacy and publication checks](docs/privacy.md) before sharing a repository, release, or diagnostic report.

## Reporting a vulnerability

Use GitHub's **Security → Report a vulnerability** for this repository when private reporting is available. If it is unavailable, open an issue requesting a private reporting channel without exploit details, credentials, private paths, or transcripts. Do not publish a working exploit or secret in a public issue.

Include the affected version, operating system, deployment mode, minimal reproduction with synthetic data, and the expected and observed behavior. This project has no guaranteed response time. Fixes are published in a new release; deploy the latest reviewed release rather than assuming older versions receive backports.
