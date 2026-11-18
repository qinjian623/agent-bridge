import json
import tempfile
import unittest
from pathlib import Path

from agent_bridge import artifacts
from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.store import Store


class ArtifactTests(unittest.TestCase):
    def test_restored_state_reads_artifacts_by_id_with_old_absolute_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            original_state = Path(directory) / "original"
            restored_state = Path(directory) / "restored"
            store = Store(original_state)
            profile = Profile("test", "codex", "fixed")
            session = store.open(profile, directory, "read-only", "open")["session_id"]
            task = store.dispatch(session, "instruction", "task")["task_id"]
            original = {"answer": "original result"}
            with store.connection(write=True) as db:
                metadata = store.save_artifact(db, task, None, "result", original)
            original_state.rename(restored_state)
            old_path = Path(metadata["path"])
            old_path.parent.mkdir(parents=True)
            old_path.write_text(json.dumps({"answer": "replaced result"}, indent=2), encoding="utf-8")

            restored = Store(restored_state)
            artifact_id = metadata["artifact_id"]
            result = restored.read_artifact(task, artifact_id)
            self.assertEqual(original, json.loads(result["content"]))
            self.assertEqual(restored_state / "artifacts" / old_path.name, Path(result["path"]))
            self.assertEqual(metadata["sha256"], result["sha256"])
            self.assertEqual(metadata["path"], restored.public_task(task)["artifacts"][0]["path"])

    def test_artifact_read_rejects_symlinks_and_invalid_identifier(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            metadata = artifacts.save(state, {"answer": "original result"})
            artifact_id = metadata["artifact_id"]
            artifact = Path(metadata["path"])
            outside_file = Path(directory) / "outside.json"
            artifact.rename(outside_file)
            artifact.symlink_to(outside_file)
            with self.assertRaises(BridgeError) as error:
                artifacts.read(state, artifact_id, metadata, 0, 4000)
            self.assertEqual("artifact_changed", error.exception.code)
            artifact.unlink()
            outside_file.rename(artifact)

            outside_folder = Path(directory) / "outside"
            artifact.parent.rename(outside_folder)
            artifact.parent.symlink_to(outside_folder, target_is_directory=True)
            with self.assertRaises(BridgeError) as error:
                artifacts.read(state, artifact_id, metadata, 0, 4000)
            self.assertEqual("artifact_changed", error.exception.code)
            with self.assertRaises(OSError):
                artifacts.save(state, {"answer": "new result"})
            self.assertEqual([artifact.name], [path.name for path in outside_folder.iterdir()])

            with self.assertRaises(BridgeError) as error:
                artifacts.read(state, "../outside", metadata, 0, 4000)
            self.assertEqual("artifact_changed", error.exception.code)

    def test_large_result_is_private_compact_and_readable_without_splitting_unicode(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            profile = Profile("test", "codex", "fixed")
            session = store.open(profile, directory, "read-only", "open")["session_id"]
            store.worker_ready(session, "g", 1, "start", "native")
            task = store.dispatch(session, "instruction", "task")["task_id"]
            store.claim(Config({"test": profile}))
            attempt = store.begin_attempt(session, "g")["active_attempt"]
            original = {"answer": "中文🌲" * 10000, "native_usage_total": {"totalTokens": 100}}
            store.finish(session, "g", task, attempt, "completed", original, {"total_tokens": 100})
            public = store.public_task(task)
            self.assertLess(len(json.dumps(public, ensure_ascii=False).encode()), 40000)
            self.assertTrue(public["result"]["answer_truncated"])
            artifact = public["result"]["artifact"]
            self.assertEqual(0o600, Path(artifact["path"]).stat().st_mode & 0o777)
            offset, chunks = 0, []
            while True:
                result = store.read_artifact(task, artifact["artifact_id"], offset, 1011)
                chunks.append(result["content"])
                self.assertGreater(result["next_offset"], offset)
                offset = result["next_offset"]
                if result["eof"]:
                    break
            self.assertEqual(original, json.loads("".join(chunks)))
            self.assertEqual({"totalTokens": 100}, store.get_task(task)["attempts"][0]["result"]["native_usage_total"])
            with self.assertRaises(BridgeError):
                store.read_artifact("another-task", artifact["artifact_id"])
            with self.assertRaises(BridgeError):
                store.read_artifact(task, artifact["artifact_id"], -1)
            store.record(
                "tool_completed",
                {
                    "id": "command",
                    "command": "python -m unittest",
                    "exitCode": 1,
                    "aggregatedOutput": "failure detail\n" * 1000,
                },
                session,
                task,
                attempt,
            )
            event = store.events(task_id=task)[-1]
            self.assertEqual(1, event["data"]["exitCode"])
            self.assertNotIn("aggregatedOutput", event["data"])
            output = store.read_artifact(task, event["data"]["artifact"]["artifact_id"], limit=65536)
            self.assertIn("failure detail", output["content"])

    def test_old_attempt_can_be_read_without_overwriting_current_result(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            profile = Profile("test", "codex", "fixed")
            session = store.open(profile, directory, "read-only", "open")["session_id"]
            store.worker_ready(session, "g", 1, "start", "native")
            task = store.dispatch(session, "instruction", "task")["task_id"]
            store.claim(Config({"test": profile}))
            first = store.begin_attempt(session, "g")["active_attempt"]
            store.finish(session, "g", task, first, "failed", {"answer": "old"})
            store.retry(task, session, "instruction", "retry")
            store.claim(Config({"test": profile}))
            second = store.begin_attempt(session, "g")["active_attempt"]
            store.finish(session, "g", task, second, "completed", {"answer": "current"})
            result = store.public_task(task, first)
            self.assertEqual("old", result["attempts"][0]["result"]["answer"])
            self.assertEqual("current", result["result"]["answer"])
            self.assertEqual(2, result["attempt_count"])
