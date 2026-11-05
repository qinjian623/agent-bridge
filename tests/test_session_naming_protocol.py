import tempfile
import unittest
from pathlib import Path

from agent_bridge.config import Profile
from agent_bridge.errors import BridgeError
from agent_bridge.peer_protocol import DeliverOpenSpec, DeliverParams, DeliverResult
from agent_bridge.store import Store, validate_session_name


class SessionNamingProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.store = Store(self.path)
        self.profile = Profile("test", "codex", "fixed-model", quota_group="shared")

    def name_rows(self):
        with self.store.connection() as db:
            return db.execute("SELECT session_id,name FROM session_names").fetchall()

    def test_typed_dict_structure(self):
        # peer_protocol uses postponed annotations, so NotRequired is only visible as annotation text.
        def text(value):
            return getattr(value, "__forward_arg__", value)

        spec = {key: text(value) for key, value in DeliverOpenSpec.__annotations__.items()}
        self.assertEqual(
            {"profile", "project_id", "access", "cwd", "model", "workspace_mode", "base", "snapshot_files", "name",
             "workspace_ref"},
            set(spec),
        )
        self.assertEqual("str", spec["profile"])
        self.assertEqual(
            set(spec) - {"profile"}, {key for key, value in spec.items() if value.startswith("NotRequired[")}
        )
        self.assertEqual("NotRequired[str]", spec["name"])
        self.assertEqual("NotRequired[str]", text(DeliverResult.__annotations__["name"]))
        self.assertEqual("NotRequired[DeliverOpenSpec]", text(DeliverParams.__annotations__["open"]))

    def test_sessions_name_present_and_absent(self):
        named = self.store.open(self.profile, str(self.path), "read-only", "a", name="  研究 session ")
        plain = self.store.open(self.profile, str(self.path), "read-only", "b")
        self.assertEqual("研究 session", named["name"])
        self.assertNotIn("name", plain)
        by_id = {item["session_id"] if "session_id" in item else item["id"]: item for item in self.store.sessions()}
        self.assertEqual("研究 session", by_id[named["session_id"]]["name"])
        self.assertNotIn("name", by_id[plain["session_id"]])
        self.assertEqual("test", by_id[plain["session_id"]]["profile"])

    def test_create_session_name_and_rollback(self):
        with self.store.connection(write=True) as db:
            sid, _ = self.store.create_session(db, self.profile, str(self.path), "read-only", name="kept")
        self.assertEqual([(sid, "kept")], [tuple(row) for row in self.name_rows()])
        with self.assertRaises(RuntimeError):
            with self.store.connection(write=True) as db:
                self.store.create_session(db, self.profile, str(self.path), "read-only", name="lost")
                raise RuntimeError("fail")
        self.assertEqual(["kept"], [row["name"] for row in self.name_rows()])
        self.assertEqual(1, len(self.store.sessions()))

    def test_invalid_name_leaves_no_rows(self):
        with self.assertRaises(BridgeError):
            self.store.open(self.profile, str(self.path), "read-only", "bad", name="a\nb")
        self.assertEqual([], self.name_rows())
        self.assertEqual([], self.store.sessions())

    def test_validate_session_name(self):
        self.assertIsNone(validate_session_name(None))
        self.assertEqual("x", validate_session_name("  x\t"))
        self.assertEqual("中文名称", validate_session_name(" 中文名称 "))
        self.assertEqual("a" * 128, validate_session_name("a" * 128))
        for bad in ("", "   ", "a" * 129, "a\x00b", "a\nb", "a\rb", "a\x07b", "a\tb", 5, b"x", ["x"]):
            with self.subTest(bad=bad), self.assertRaises(BridgeError):
                validate_session_name(bad)

    def test_open_fingerprint_name_contract(self):
        first = self.store.open(self.profile, str(self.path), "read-only", "same")
        again = self.store.open(self.profile, str(self.path), "read-only", "same", name=None)
        self.assertEqual(first, again)
        with self.assertRaisesRegex(BridgeError, "different content"):
            self.store.open(self.profile, str(self.path), "read-only", "same", name="other")
        named = self.store.open(self.profile, str(self.path), "read-only", "n", name=" x ")
        self.assertEqual(named, self.store.open(self.profile, str(self.path), "read-only", "n", name="x"))
        with self.assertRaisesRegex(BridgeError, "different content"):
            self.store.open(self.profile, str(self.path), "read-only", "n", name="y")
        with self.assertRaisesRegex(BridgeError, "different content"):
            self.store.open(self.profile, str(self.path), "read-only", "n")


if __name__ == "__main__":
    unittest.main()
