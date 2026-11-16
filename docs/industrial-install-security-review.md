> Historical pre-fix review: this report records the implementation before the candidate fixes. See [current candidate checks](production-readiness.md) for completed checks and pending work; the original findings and results below are preserved.

# Installation, updates, and Web security review

What this repo does: It runs local coding-agent sessions and connects optional remote hosts over SSH. It stores session state in SQLite and can install a user service, update itself from GitHub Releases, and serve an authenticated Web monitor. This review assumes one owner per installation, concurrent workers, and a small number of browser clients; access from other machines is optional.

Reviewed on 2026-11-17 against the working tree based on `257065e`, including its uncommitted preparation changes. This was a read-only source review with isolated tests. Only this report was written.

## Must fix

1. **Unauthenticated requests can hold unlimited Web threads** (`src/agent_bridge/web_monitor.py:1750-1754`, `2688-2694`, `2920`).
   - **What this is:** The Web monitor starts a `ThreadingHTTPServer`. Login reads the declared request body before it can check the supplied management token.
   - **Problem:** Send `POST /auth/login`, an allowed Host, and `Content-Length: 1`, then keep the connection open without sending the body. Each connection keeps a thread waiting without a socket timeout; the SSE connection limit does not cover these requests. Four isolated connections produced four waiting handlers, each with `connection.gettimeout() == None`.
   - **Fix:** Give accepted connections a short read deadline and cap ordinary HTTP handler concurrency before creating threads. Keep the separate SSE limits, and add one partial-request test that confirms a handler is released.
   - **If we skip it:** Anyone who can reach the monitor can exhaust its threads or file descriptors without knowing the management token. The monitor and browser approvals become unavailable.

2. **A failed service migration overwrites the working service without saving a recovery copy** (`src/agent_bridge/updater.py:1601-1630`).
   - **What this is:** `init --force-broker-service --enable-broker-service` can replace an existing running broker service with the managed launcher.
   - **Problem:** The old service file is replaced before restart or, on macOS, before `bootout` followed by `bootstrap`. A confirmed start or health-check failure reaches only maintenance cleanup; the old file is not restored. An isolated simulated restart failure left the replacement file in place and no backup of the old file.
   - **Fix:** Save the old service bytes, permissions, and active state before replacement. Restore and verify the old service after a confirmed failed migration; retain the saved recovery information when a timed-out service command has an unknown outcome.
   - **If we skip it:** A failed migration can leave an existing installation stopped and discard the configuration needed to start it again. The user must reconstruct the old service by hand.

3. **Reconfiguring managed services fails after an OTA update** (`src/agent_bridge/updater.py:1054-1061`, `1105-1125`).
   - **What this is:** Managed launchers keep a bootstrap Python path for use before the first release is activated. After activation, the launcher executes the Python inside the selected release directory.
   - **Problem:** `_expected_launchers()` always uses the current `sys.executable`, and `_check_launcher_conflicts()` requires an exact text match with that newly generated launcher. After OTA changes the running Python from the bootstrap environment to a release environment, editing Services & OTA or reinstalling the broker service fails with `Refusing to overwrite an unrelated managed launcher`. The isolated test reproduced this by installing the launchers with one Python path and validating them with a release Python path.
   - **Fix:** Preserve the installation's recorded bootstrap Python when validating existing managed launchers. Use that same value when updating settings and launchers, while continuing to reject unrelated files.
   - **If we skip it:** Updates can continue, but normal post-update configuration changes stop working through the documented managed launcher.

4. **Ollama requests require an unconfigured local proxy** (`src/agent_bridge/runtimes.py:528-529`, `tests/test_ollama_provider.py:40-41`).
   - **What this is:** The `ollama-anthropic` profile prepares environment variables for the Claude runtime.
   - **Problem:** A clean environment, or an explicitly empty `HTTPS_PROXY`, is changed to `http://127.0.0.1:1080`; `HTTP_PROXY` then inherits it. An isolated call with only a placeholder Ollama credential confirmed both proxy variables are added.
   - **Fix:** Remove the hardcoded proxy assignments and inherit the user's environment unchanged. Update the test and the README paragraph that describe this machine-specific default.
   - **If we skip it:** Ordinary users without a proxy on local port 1080 cannot use this provider, and explicitly clearing proxy settings does not select a direct connection.

5. **Accepted macOS service names do not identify the installed service during OTA** (`src/agent_bridge/updater.py:863-867`, `881`, `1296-1307`).
   - **What this is:** The installer accepts a `service_name` ending in `.plist` and removes that suffix when writing the LaunchAgent label.
   - **Problem:** With `service_name=com.example.bridge.plist`, the installed label is `com.example.bridge`. `Updater.service_managed()` and `_restart()` use the untrimmed name instead. The isolated service-manager test observed a query for `gui/<uid>/com.example.bridge.plist`, which did not match the installed label, so the updater considered the service unmanaged.
   - **Fix:** Normalize the LaunchAgent name once before writing settings, generating the plist, querying the service, and restarting it. Add a test using the accepted `.plist` spelling through installation and update checks.
   - **If we skip it:** This valid installation starts successfully but its scheduled updates remain deferred as `broker_service_not_migrated`.

6. **Changing the Web port makes the default browser URL fail** (`src/agent_bridge/web_monitor.py:65-70`, `2912-2921`).
   - **What this is:** `agent-bridge monitor --web --port 9000` changes the listening port while retaining the default loopback-only allowed-host settings.
   - **Problem:** The default list contains port 8080 but is not updated for the selected port. A real isolated loopback server on another port returned `400 Invalid Host header` for `GET /login` using its normal browser Host header.
   - **Fix:** When no explicit allowed-host list is supplied, derive the loopback Host entries from the actual listening port. Preserve explicit administrator lists and test a non-default port without `--allowed-hosts`.
   - **If we skip it:** Users who change the port to avoid a conflict see a working listener but cannot open its printed URL without discovering an extra option.

## Coverage and checks

Read the README, project and CI packaging configuration, `scripts/install_local.py`, CLI entry points, configuration loading, interactive/non-interactive initialization, host parsing and pin persistence, updater download/staging/activation/service installation, and Web authentication, request validation, rendering, and broker forwarding. Also inspected the Ollama environment setup, SSH command construction, and the SQLite initialization/migration entry points where they affect installation or rollback.

The reviewed code already checks management-token ownership and permissions, binds browser sessions to token rotation, requires Origin plus a CSRF token for cookie-authenticated mutations, and builds dashboard values with text DOM APIs. OTA downloads restrict HTTPS destinations and redirects, enforce size and time limits, and check the wheel's SHA-256 digest. Release activation has a separate symlink rollback flow and preserves uncertain restart outcomes. These checks do not resolve the six cases above.

Ran 172 existing tests on Python 3.13.15. All passed in the corrected test runner:

```python
import os
import sys
import unittest

sys.path[:0] = ["src", "tests"]
os.environ.pop("PYTHONPATH", None)
names = [
    "test_init_config", "test_updater", "test_monitor", "test_hosts",
    "test_web_topology", "test_broker_token_rotation", "test_ollama_provider",
]
suite = unittest.defaultTestLoader.loadTestsFromNames(names)
result = unittest.TextTestRunner().run(suite)
raise SystemExit(not result.wasSuccessful())
```

The first test invocation exported the review checkout through `PYTHONPATH`; that also affected the candidate wheel's subprocess and caused one false failure in its packaged-skill check. The corrected runner changes only its own import search path and removes inherited `PYTHONPATH`. The rerun passed all 172 tests, including the local HTTPS/CDN/proxy and real minimal-wheel staging test.

Six additional isolated probes confirmed the six findings. They used temporary directories, placeholder credentials, simulated service-manager calls, and short-lived loopback HTTP servers. No installed service, real session, real credential, production database, model runtime, remote host, or GitHub release was changed.

CI was inspected, not executed on GitHub. It tests Linux Python 3.11/3.13/3.14, checks packaging on Linux/macOS, and uses a tag-gated release job with asset checksums. No new packaging build or dependency installation was performed during this review. There is no CLI uninstall action, and no real service removal was attempted.

Verdict: Fix partial HTTP requests, preserve the previous service during migration failure, and keep bootstrap launcher validation stable across OTA before calling installation and updates ready for ordinary users. The proxy, macOS name, and alternate-port defects are also confirmed user-visible failures.

Not checked: Live systemd/launchd install, restart, rollback, or uninstall; fresh macOS execution; remote SSH execution; cross-version production-database rollback; the complete repository test suite; online release publication; paid model inference. SQLite rollback compatibility and live service recovery remain unverified.
