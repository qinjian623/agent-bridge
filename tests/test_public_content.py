import importlib.util
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/check_public_content.py"
SPEC = importlib.util.spec_from_file_location("check_public_content", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PublicContentTests(unittest.TestCase):
    def test_detects_private_data_but_allows_synthetic_examples(self):
        private = "/home/" + "private-person/project"
        token = "ghp_" + "x" * 36
        email = "person@" + "private-domain.org"
        native_id = "12345678-abcd-" + "7890-1234-abcdef012345"
        values = MODULE.issues("\n".join([private, token, email, native_id]))
        self.assertEqual([(1, "home path"), (2, "credential"), (3, "email"), (4, "session ID")], values)
        self.assertEqual([], MODULE.issues("/home/user/project /Users/alice/project test@example.com\n"
                                          "00000000-0000-7000-8000-000000000001 https://user:pass@github.com/a/b"))

    def test_never_prints_secret_matches_and_checks_untracked_files(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            secret = "sk-" + "a" * 40
            (root / "example.md").write_text(secret)
            output = StringIO()
            with patch.object(MODULE.subprocess, "check_output", return_value=b"example.md\0"), redirect_stdout(output):
                self.assertEqual(1, MODULE.check(root))
            self.assertNotIn(secret, output.getvalue())
            self.assertIn("example.md:1: possible credential", output.getvalue())

    def test_runtime_file_is_rejected_without_reading_it(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root / "auth.json").write_text("private authentication data")
            output = StringIO()
            with patch.object(MODULE.subprocess, "check_output", return_value=b"auth.json\0"), redirect_stdout(output):
                self.assertEqual(1, MODULE.check(root))
            self.assertNotIn("private authentication data", output.getvalue())


    def test_binary_database_sidecars_are_rejected_before_decoding(self):
        for filename in ["history.db", "history.db-wal", "state.sqlite-wal", "state.sqlite-shm",
                         "state.sqlite-journal", "state.sqlite3-wal"]:
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                (root / filename).write_bytes(b"\xff\x00private-runtime-data")
                output = StringIO()
                with patch.object(MODULE.subprocess, "check_output", return_value=(filename + "\0").encode()), redirect_stdout(output):
                    self.assertEqual(1, MODULE.check(root))
                self.assertNotIn("private-runtime-data", output.getvalue())


if __name__ == "__main__":
    unittest.main()
