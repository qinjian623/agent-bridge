> Historical pre-fix review: this report records the implementation before the candidate fixes. See [current candidate checks](production-readiness.md) for completed checks and pending work; the original findings and results below are preserved.

What this repo does: It creates separate working directories for coding-agent sessions, captures their results, and transfers selected input files to trusted execution hosts. This review assumes one owner, a few trusted SSH nodes, concurrent tasks, and ordinary retries or interrupted uploads. It is a review of concrete reliability and security behavior, not an industrial certification.

Only this report was written. All reproductions used temporary directories, synthetic input, and mocked model/peer calls. No installed service, real session, production database, SSH connection, or broadcast was used.

## Must fix

1. **A normal upload fails on any file larger than one chunk** (`src/agent_bridge/peer_api.py:1048-1051`, `1068-1082`, `1089-1113`; caller: `src/agent_bridge/broker.py:1845-1854`).
   - **What this is:** The coordinator uploads each file in ascending 256 KiB chunks. The receiver must acknowledge an incomplete file and check its full digest after the final byte arrives.
   - **Problem:** `_assemble_blob` treats any contiguous prefix starting at zero as a complete file. The first chunk of a 262145-byte file therefore reaches full-file digest verification immediately and returns `workspace_conflict: blob digest does not match its content`. An ordinary public `Broker.api_call("agent_open", ...)` with that selected file failed before any remote delivery. A manifest larger than one chunk reaches the same premature completion path.
   - **Fix:** Use the declared manifest/file length to distinguish incomplete data from a complete blob. Acknowledge a valid shorter prefix without running full-blob verification; verify only when all declared bytes are present. Add a public broker test for a file spanning at least two chunks and a receiver test for a multi-chunk manifest.
   - **If we skip it:** Normal source files above 256 KiB cannot be delivered, despite the advertised 64 MiB file limit.

   Minimal reproduction inside the existing `BrokerWorkspaceDispatchTests` fixture:

   ```python
   (self.source / "large.bin").write_bytes(b"a" * (256 * 1024 + 1))
   response = await self.broker.api_call("agent_open", self.args(
       "large-input", {"snapshot_files": [*SELECTION, "large.bin"]},
   ))
   # Actual: response["error"]["code"] == "workspace_conflict".
   # Actual: self.client.deliver_params == []. Expected: accepted open.
   ```

   Implementation files: `src/agent_bridge/peer_api.py`, `src/agent_bridge/broker.py`. Test file: `tests/test_workspace_execution.py` (`ServiceWorkspaceTests`, `BrokerWorkspaceDispatchTests`).

2. **A rejected chunk replacement overwrites the previously accepted bytes** (`src/agent_bridge/peer_api.py:1042-1046`; conflict check: `src/agent_bridge/store.py:3081-3088`).
   - **What this is:** A repeated chunk at the same workspace, blob digest, and offset may repeat the same bytes, but different bytes must be refused without changing saved input.
   - **Problem:** `_workspace_chunk` replaces the chunk file before `record_workspace_chunk` detects the conflicting size/digest. For a declared file of 256 KiB plus 10 bytes, uploading its `b"0123456789"` tail first succeeds. Replacing that tail with `b"different!"` returns a conflict, but the saved file contains `different!` while SQLite retains the first digest. Uploading the correct leading chunk then fails with `stored chunk does not match its digest`; no complete blob backup exists. The error response hides an actual change to accepted data.
   - **Fix:** Preserve existing chunks when rejecting a conflict. Store new bytes under a filename containing their chunk digest, then let the SQLite record select the accepted chunk; a conflicting candidate must never replace the accepted file. Add a test that checks both the stored row and file bytes after rejection, and can still assemble the original upload.
   - **If we skip it:** A bad retry can damage an otherwise valid resumable upload. Later assembly fails with `stored chunk does not match its digest` until the original chunk is uploaded again.

   Minimal reproduction inside `ServiceWorkspaceTests`, after preparing and uploading a valid manifest declaring `content` as a selected file:

   ```python
   prefix, tail = b"a" * (256 * 1024), b"0123456789"
   content = prefix + tail
   blob = sha256(content)
   await self.action(
       action="chunk", workspace_id=workspace_id, blob_sha256=blob,
       offset=len(prefix), size=len(tail), chunk_sha256=sha256(tail),
       data=base64.b64encode(tail).decode(),
   )
   saved = self.store.get_workspace_chunks(workspace_id, blob)[0]
   response = await self.action_response(
       action="chunk", workspace_id=workspace_id, blob_sha256=blob,
       offset=len(prefix), size=10, chunk_sha256=sha256(b"different!"),
       data=base64.b64encode(b"different!").decode(),
   )
   assert not response["ok"]
   assert Path(saved["stored_path"]).read_bytes() == tail  # Fails.
   ```

   Implementation files: `src/agent_bridge/peer_api.py`, `src/agent_bridge/store.py`. Test files: `tests/test_workspace_execution.py`, `tests/test_workspace_protocol.py`. The existing conflict test checks the error code but never checks the accepted file afterward.

3. **Copy-mode task results lose their file contents when a later task edits the workspace** (`src/agent_bridge/workspace.py:454-465`; callers: `src/agent_bridge/worker.py:391-406`, `src/agent_bridge/store.py:1396-1405`, `src/agent_bridge/broker.py:2782-2789`).
   - **What this is:** Every completed writing task saves a result. Git mode saves a patch containing the changed bytes; copy mode saves a JSON change list containing hashes, permissions, and file paths.
   - **Problem:** Copy-mode paths point into the active session directory. A later task changes or removes the same files, so an earlier completed result no longer contains recoverable output matching its saved hash. This was verified with two tasks executed by the real `Worker` using a mocked runtime, opened and dispatched through public broker APIs. Reading the first task through public `agent_result`, including its full saved result artifact, still returned the active file path; there was no separate byte backup. The second task's write caused a digest mismatch, and its deletion left that path missing.
   - **Fix:** At capture time, copy each changed/new regular file into a result directory specific to that attempt and reference those retained files in the JSON. Preserve modes and verify the captured bytes before saving the result. Add a two-task worker test that reads the first result and its artifact after the second task overwrites/deletes the file.
   - **If we skip it:** Continuing a copy-mode session can destroy the only saved content of an earlier completed result. The result JSON and its hash survive, but cannot restore that content.

   Minimal reproduction sequence:

   ```python
   # Open workspace_mode="copy" with snapshot_files=["file.txt"].
   # Dispatch two tasks into that session. Mock only the model runtime.
   # Task 1 writes "task one output"; Worker captures and finishes it.
   first = (await broker.api_call("agent_result", {"task_id": first_id}))["result"]
   full = (await broker.api_call("agent_result", {
       "task_id": first_id,
       "artifact_id": first["result"]["artifact"]["artifact_id"],
       "limit": 65536,
   }))["result"]
   files = json.loads(full["content"])["files"]
   record = json.loads(Path(files["changes"]).read_text())["file.txt"]
   # Task 2 overwrites file.txt, then deletes it.
   assert Path(record["path"]).exists()  # Fails after task 2.
   ```

   Implementation files: `src/agent_bridge/workspace.py`, `src/agent_bridge/worker.py`, `src/agent_bridge/store.py`, `src/agent_bridge/artifacts.py`, `src/agent_bridge/broker.py`. Test files: `tests/test_workspace.py`, `tests/test_worker.py`, `tests/test_integration.py`. The existing worker helper for mocked native execution is suitable; no real CLI inference is needed.

4. **Patch delivery cannot add a file inside a new directory** (`src/agent_bridge/peer_api.py:1332-1338`; callers: `1375-1382`; path check: `1294-1305`).
   - **What this is:** A selected new file should be added to the detached base worktree. Its parent directories may be absent from the base commit.
   - **Problem:** `_verify_workspace_before` asks for `create_parents=False` before checking that a `put` with `before_sha256=None` is a new file. The path helper raises when any parent is missing. A valid patch containing `newdir/file.py`, with all required content uploaded, failed seal with `workspace path has a missing parent`; the transfer became `failed` and the temporary worktree was removed.
   - **Fix:** For a new `put`, validate existing path components and reject symlinks, while allowing missing parents. Create those parents through the existing safe write function. Keep strict existence checks for replacing or deleting a base file. Add a test that seals and attaches a patch with a new nested directory.
   - **If we skip it:** Common changes that add a package or source directory cannot be transferred in patch mode.

   Minimal reproduction inside `ServiceWorkspaceTests`:

   ```python
   (self.repo / "newdir").mkdir()
   (self.repo / "newdir/file.py").write_text("new input\n")
   manifest = create_workspace_manifest(
       "bridge", "new-directory", self.repo, "patch",
       base_commit=self.base, selected_files=["newdir/file.py"],
   )
   await self.prepare_and_seal(manifest)  # Fails during seal.
   ```

   Implementation file: `src/agent_bridge/peer_api.py`. Test file: `tests/test_workspace_execution.py`.

## Checks and limits

Read all of `src/agent_bridge/workspace.py`; the workspace manifest and request validators in `src/agent_bridge/peer_protocol.py`; workspace prepare/chunk/seal/abort, safe file writes, project checks, and session attachment in `src/agent_bridge/peer_api.py`; the corresponding local and remote broker open/dispatch/upload/result handlers; project normalization and child environment preparation in `src/agent_bridge/config.py`; CLI/MCP request forwarding; and worker result capture, result persistence, and artifact serialization. There are no separate `projects.py`, `project_catalog`, or `peer_workspace` modules in this checkout: project roots are a configuration dictionary, and workspace RPC handling lives in `peer_api.py`.

Static path checks reject absolute paths, parent traversal, `.git` components, and selected symlinks. The existing offline tests exercise these checks, stable source snapshots, raw Git filenames, permission changes, Git patch application, transfer revisions, replay, project changes, and one-session attachment. No additional arbitrary-path overwrite or directory escape was confirmed in the stated trust model.

The child environment removes the management token and sets `AGENT_BRIDGE_ROLE=child`; the public request helper refuses that role. Codex disables inherited MCP/plugins/apps and nested agents; Pi disables extensions/MCP and limits read-only tools; Claude uses explicit tool settings and disables its Agent/Task tools. The socket checks the operating-system user and management token. The tests below verified the child-role refusal and wrong-token refusal through the real local request code, plus MCP tool listing and local session open/inspect with mocked discovery. These are application restrictions, not isolation from malicious code running under the same OS account, as documented in `SECURITY.md`.

57 existing tests passed: 54 workspace tests in 1.975 seconds and three integration tests in 0.618 seconds. They used the existing installed dependency environment; this was not a clean install check.

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:tests python -m unittest \
  test_workspace test_workspace_protocol test_workspace_execution -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:tests python -m unittest \
  test_integration.IntegrationTests.test_child_client_cannot_control_owner \
  test_integration.IntegrationTests.test_wrong_token_rejected \
  test_integration.IntegrationTests.test_actual_mcp_client_preserves_original_tools_and_adds_group_tools -q
```

Additional temporary-directory assertions reproduced all four findings. The first was also reproduced through public `agent_open`; the third was checked through public open/dispatch/result APIs and the actual worker result-writing code, with only native inference mocked. No implementation or test file was changed.

Verdict: Fix all four before relying on remote source delivery or retained copy-mode results.

Not checked: unlisted modules in full, core task/notification/cancellation behavior covered by the separate review, installation/update/Web behavior covered by the separate review, paid inference, live SSH interruption or concurrent relay processes, crash/power-loss durability, macOS filesystem behavior, sustained large-output/storage growth, or the full repository test suite. No industrial certification or performance guarantee is implied.
