> Language: English | [中文](zh/install-verification.md)

# Isolated installation and first-task check

On 2026-11-17 (18:07–18:08 UTC on 2026-11-16), the then-current candidate `0.3.0` wheel was installed and used from a new HOME and new XDG, uv-tool, state, and project directories on Linux x86-64, with Python 3.13.15 and uv 0.13.8. Model execution used an unchanged copy of `tests/fixtures/fake_codex.py`. No real model, CLI account, shared Codex daemon, existing Bridge state, or installed user service was used.

All 17 checks passed, including 12 invalid-input cases. An earlier candidate failed when an unknown profile field produced a Python traceback; this new run confirmed a clear, redacted command error and unchanged saved settings.

## Isolation and package checks

Every temporary file and detailed report stayed in a private maintenance directory outside the source checkout. `TMPDIR`, HOME, XDG directories, `UV_TOOL_DIR`, `UV_TOOL_BIN_DIR`, and `AGENT_BRIDGE_STATE` pointed there. Existing uv package cache was reused with hardlinks; authentication data was not copied. Package build and installation used `--offline`, with Python downloads disabled.

A temporary `codex` wrapper launched only the repository RPC fixture through the installed tool's Python. The configured profile also explicitly pointed to that wrapper. A temporary `systemctl` command returned unavailable, and the saved worker launcher was `detached`. No MCP registration was written to a real client; an independent stdio MCP client performed initialization, tool listing, and a fixture-backed catalog query.

Both wheel and source distribution contained Apache-2.0 package metadata, LICENSE, NOTICE, and the companion skill. The wheel had 46 entries, including three packaged skill files. Every archive text entry passed the repository checker for known private-path, credential, email, and session-ID patterns; no prohibited runtime files were found. These pattern checks are not an exhaustive privacy audit. CLI help ran from the new uv tool installation.

The tested wheel SHA256 was `df3f7cfcf364f91b4e815987aceb9d91627dc1d7e024234d84eb7aee959aa0d4`. Earlier installation runs were preserved separately; this report describes the final code snapshot. A later source distribution rebuild may incorporate documentation changes without changing the tested wheel code.

## Commands and first-task sequence

The following commands use descriptive variables rather than the private run's paths or generated IDs. `SMOKE_ROOT` denotes a new, short private directory outside the checkout; `PYTHON` is an existing Python interpreter. Run from the repository root with the isolated environment described above. The `local` profile must point to the fixture wrapper, not an authenticated model CLI.

```sh
uv build --offline --python "$PYTHON" --out-dir "$SMOKE_ROOT/dist" --no-create-gitignore
uv tool install --offline --python "$PYTHON" "$SMOKE_ROOT/dist/cli_agent_bridge-0.3.0-py3-none-any.whl"
agent-bridge --help
agent-bridge init
agent-bridge init --non-interactive
agent-bridge init --non-interactive
agent-bridge doctor
agent-bridge serve
```

Interactive `init` ran in a real pseudo-terminal: Profiles -> add -> name `local` -> runtime `codex` -> provider `openai-codex` -> model `fixed-model` -> default quota group -> no effort -> temporary fixture executable -> no environment file -> back -> save. Remote hosts and services stayed unset. Both repeated initializations preserved the complete configuration bytes and the unverified profile flag.

After the broker became ready, an independent MCP client discovered 20 tools, including the nine original task/session tools. `agent_catalog(profile="local", refresh=true)` returned the fixture catalog containing `fixed-model`.

The CLI submitted this synthetic first task to the temporary project:

```sh
agent-bridge call agent_dispatch --json '{"profile":"local","cwd":"/example/project","access":"read-only","instruction":"Read the project README and summarize how to run it. Do not modify files.","request_id":"quickstart-task-1"}'
agent-bridge call agent_result --json '{"task_id":"<task_id>"}'
```

The exact same dispatch request was repeated. Both replies referred to the same task/session. A bounded Python loop read results until completion; only one task, one attempt, and one fixture turn invocation existed. The expected `fixture complete` result was checked. The temporary README was unchanged; the fixture's own `invocations.txt` recorded its single synthetic turn.

The result was then reviewed and its session closed using the current returned revisions:

```sh
agent-bridge call agent_review --json '{"task_id":"<task_id>","attempt_id":"<attempt_id>","decision":"accepted","evidence":"Checked the expected offline fixture result and its single invocation; real model inference was not tested.","expected_revision":0,"request_id":"quickstart-review-1"}'
agent-bridge call agent_inspect --json '{"session_id":"<session_id>"}'
agent-bridge call agent_control --json '{"session_id":"<session_id>","action":"close","expected_revision":0,"request_id":"quickstart-close-1"}'
```

The zeros above are placeholders: the run supplied the actual current result/session revisions. Closure was confirmed with `desired=closed` and `worker_alive=false`. After stopping and restarting only the isolated foreground broker, the same completed, accepted result and closed session remained, with no second fixture invocation.

## Errors, permissions, uninstall, and process exit

| Check | Result |
| --- | --- |
| First non-interactive init without settings | Nonzero exit with instructions to run interactive init or supply profile flags |
| `--parallel-limit 0` | Clear nonzero error; saved configuration unchanged |
| Invalid JSON supplied to `call` | Clear nonzero error; no additional task |
| Unknown profile on dispatch | Clear nonzero error; no additional task/session |
| Unknown field in profile JSON | Clear nonzero error; supplied profile name and field name redacted; saved configuration unchanged |
| Invalid profile field types | String `verified`, list `executable`, and string `tools` rejected without traceback or input-name disclosure; saved configuration unchanged |
| Invalid saved configuration | Malformed JSON, fractional parallel limit, boolean timeout, unknown top-level field, and invalid profile flag rejected; `doctor` did not change the supplied file |
| Profile environment file | Mode 0644 rejected; owner-only 0600 JSON string map accepted; non-string values rejected; shell syntax remained literal; parent thread identity and administrator token removed from the child environment |
| State permissions | Directory 0700; config, administrator token, and SQLite database owned by the user with mode 0600 |
| Service installation | No user service or LaunchAgent was installed |
| `uv tool uninstall cli-agent-bridge` | Tool command/environment removed; configuration and database retained with identical hashes |
| Process exit | All 44 recorded processes exited, including two broker instances, one worker, two fixture processes, and the MCP server/client; zero survivors and no forced cleanup |

The previously failing input now returns `invalid_config` with a clear explanation:

```sh
agent-bridge init --non-interactive --profile '{"name":"bad-example","runtime":"codex","model":"fixed-model","unknown_option":"example"}'
```

All 12 invalid-input commands exited nonzero without a Python traceback. Cases containing synthetic sensitive profile names and unknown field names did not echo those names. Saved configuration bytes were preserved, and no extra task or session was created. Full reproduction logs, package hashes, commands, process identities, and the synthetic state remain private. Packaged source/build inputs were hashed and did not change during this run.

Not tested by this installation check: macOS, Windows/WSL, Python 3.11/3.14, GitHub installation/download, real model inference or authentication, native filesystem sandbox enforcement, managed services, remote hosts, or live upgrades. The test used an explicit short state directory; it did not exercise the default state path. Fixture completion proves the installed CLI/MCP/queue/persistence flow, not provider or model quality.
