import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent_bridge.config import Config, Profile
from agent_bridge.errors import BridgeError
from agent_bridge.store import Store, validate_session_name


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.store = Store(self.path)
        self.profile = Profile("test", "codex", "fixed-model", quota_group="shared")
        self.config = Config({"test": self.profile}, parallel_limit=3, quota_limit=2)

    def session(self, key):
        sid = self.store.open(self.profile, str(self.path), "read-only", key)["session_id"]
        self.store.worker_ready(sid, "generation", 123, "start", "native-" + sid)
        return sid

    def run_task(self, sid, instruction="task", **kwargs):
        task = self.store.dispatch(sid, instruction, instruction, **kwargs)
        self.store.claim(self.config)
        active = self.store.begin_attempt(sid, "generation")
        return task["task_id"], active["active_attempt"]

    def test_request_id_is_atomic_and_conflicting_payload_rejected(self):
        sid = self.session("open")
        with ThreadPoolExecutor(8) as pool:
            results = list(pool.map(lambda _: self.store.dispatch(sid, "do once", "same"), range(16)))
        self.assertEqual(1, len({result["task_id"] for result in results}))
        with self.assertRaisesRegex(BridgeError, "different content"):
            self.store.dispatch(sid, "do something else", "same")

    def test_quota_serial_session_and_capacity_claims_are_atomic(self):
        sessions = [self.session("open" + str(i)) for i in range(4)]
        for i, sid in enumerate(sessions):
            self.store.dispatch(sid, "task", "task" + str(i))
        self.store.dispatch(sessions[0], "follow-up", "follow-up")
        with ThreadPoolExecutor(8) as pool:
            claims = list(pool.map(lambda _: self.store.claim(self.config), range(8)))
        self.assertEqual(2, sum(claim is not None for claim in claims))
        self.assertEqual(2, len({claim["session_id"] for claim in claims if claim}))

    def test_pause_revision_and_interrupted_resume(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid)
        with self.assertRaisesRegex(BridgeError, "Session changed"):
            self.store.control(sid, "pause", 0, "stale")
        self.store.control(sid, "pause", 1, "pause")
        self.store.finish(sid, "generation", tid, attempt, "interrupted", {"answer": "partial"})
        self.store.dispatch(sid, "next", "next")
        self.assertIsNone(self.store.claim(self.config))
        self.store.control(sid, "resume", 2, "resume")
        self.assertEqual("interrupted", self.store.get_task(tid)["status"])
        self.assertIsNotNone(self.store.claim(self.config))

    def test_review_releases_dependency_and_finish_does_not_duplicate(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid)
        self.store.dispatch(sid, "dependent", "dependent", dependencies=[tid])
        self.store.finish(sid, "generation", tid, attempt, "completed", {"answer": "done"}, {"total_tokens": 100})
        self.store.finish(sid, "generation", tid, attempt, "completed", {"answer": "done"}, {"total_tokens": 100})
        self.assertIsNone(self.store.claim(self.config))
        self.assertEqual(1, len([e for e in self.store.events(task_id=tid) if e["kind"] == "task_finished"]))
        self.assertEqual(100, self.store.budget()["reported_tokens"])
        self.store.review(tid, attempt, "accepted", "asserted actual output", "review", 1)
        self.assertIsNotNone(self.store.claim(self.config))

    def test_dead_worker_never_replays_running_task(self):
        sid = self.session("open")
        tid, _ = self.run_task(sid)
        self.store.worker_stopped(sid, "generation", "crashed")
        self.assertEqual("recovery_needed", self.store.get_task(tid)["status"])
        self.assertIsNone(self.store.claim(self.config))
        with self.assertRaisesRegex(BridgeError, "uncertain"):
            self.store.control(sid, "resume", 1, "resume")

    def test_progress_above_reservation_prevents_more_spending_and_survives_crash(self):
        self.config.token_budget = 100
        sid = self.session("open")
        tid, attempt = self.run_task(sid, reservation=20)
        self.store.progress_usage(sid, "generation", tid, attempt, {"total_tokens": 95, "complete": True})
        second = self.session("second")
        self.store.dispatch(second, "second", "second-task", reservation=10)
        self.assertIsNone(self.store.claim(self.config))
        self.store.worker_stopped(sid, "generation", "crashed")
        usage = self.store.get_task(tid)["attempts"][0]["usage"]
        self.assertEqual(95, usage["total_tokens"])
        self.assertFalse(usage["complete"])
        self.assertEqual(95, self.store.budget()["charged_tokens"])

    def test_transport_failure_preserves_partial_usage(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid, reservation=40)
        self.store.progress_usage(sid, "generation", tid, attempt, {"total_tokens": 15, "complete": True})
        self.store.finish(sid, "generation", tid, attempt, "recovery_needed", {"error": "transport lost"})
        usage = self.store.get_task(tid)["attempts"][0]["usage"]
        self.assertEqual(15, usage["total_tokens"])
        self.assertFalse(usage["complete"])
        self.assertEqual(40, self.store.budget()["charged_tokens"])

    def test_expired_permission_cannot_be_approved(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid)
        question = self.store.question(sid, tid, attempt, {"method": "permission"})
        self.assertEqual("waiting_input", self.store.get_task(tid)["status"])
        self.store.control(sid, "pause", 2, "pause")
        with self.assertRaisesRegex(BridgeError, "no longer pending"):
            self.store.respond(sid, question, {"decision": "allow"}, 3, "answer")

    def test_question_enqueues_one_parent_notice_tied_to_its_event(self):
        self.store.parent_control(
            "parent", "parent_register", 0, "register-parent", "native-parent", str(self.path),
        )
        sid = self.store.open(
            self.profile, str(self.path), "read-only", "open-parent", parent="parent",
        )["session_id"]
        self.store.worker_ready(sid, "generation", 123, "start", "native-" + sid)
        task_id, attempt_id = self.run_task(sid)
        question_id = self.store.question(
            sid, task_id, attempt_id, {"method": "permission"}, expires=0,
        )
        event = next(
            event for event in self.store.events(task_id=task_id)
            if event["kind"] == "input_required"
        )
        self.assertEqual(question_id, event["data"]["question_id"])
        with self.store.connection() as db:
            deliveries = [dict(row) for row in db.execute(
                "SELECT * FROM deliveries WHERE event_seq=?", (event["seq"],)
            )]
        self.assertEqual(1, len(deliveries))
        self.assertEqual("parent", deliveries[0]["parent_id"])
        self.assertEqual("expired", self.store.get_question(question_id)["status"])

    def test_unknown_usage_keeps_reservation(self):
        self.config.token_budget = 100
        sid = self.session("open")
        tid, attempt = self.run_task(sid, reservation=80)
        self.store.finish(sid, "generation", tid, attempt, "failed", {"error": "unknown usage"})
        self.store.dispatch(sid, "next", "next", reservation=30)
        self.assertIsNone(self.store.claim(self.config))
        self.assertEqual(80, self.store.budget()["charged_tokens"])

    def test_stale_worker_cannot_overwrite_result(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid)
        with self.assertRaisesRegex(BridgeError, "no longer owns"):
            self.store.finish(sid, "old-generation", tid, attempt, "completed", {"answer": "wrong"})
        self.assertEqual("running", self.store.get_task(tid)["status"])

    def test_explicit_continuation_creates_new_attempt_and_preserves_cost(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid, reservation=100)
        self.store.control(sid, "pause", 1, "pause")
        self.store.finish(sid, "generation", tid, attempt, "interrupted", {"answer": "partial"}, {"total_tokens": 50})
        self.store.control(sid, "resume", 2, "resume", continue_task_id=tid)
        next_attempt = self.store.claim(self.config)["attempt_id"]
        self.assertNotEqual(attempt, next_attempt)
        self.assertEqual(2, len(self.store.get_task(tid)["attempts"]))
        self.assertEqual(50, self.store.budget()["reported_tokens"])
        with self.assertRaisesRegex(BridgeError, "no longer owns"):
            self.store.finish(sid, "generation", tid, attempt, "completed", {"answer": "stale"})

    def test_cross_profile_retry_keeps_original_attempt_identity(self):
        first = self.session("first")
        tid, attempt = self.run_task(first)
        self.store.finish(first, "generation", tid, attempt, "failed", {"error": "failed"}, {"total_tokens": 10})
        other = self.session("other")
        self.store.retry(tid, other, "task", "retry")
        claimed = self.store.claim(self.config)
        self.assertEqual(other, claimed["session_id"])
        attempts = self.store.get_task(tid)["attempts"]
        self.assertEqual({first, other}, {a["session_id"] for a in attempts})

    def test_empty_drain_pauses_before_worker_start_and_leaves_queued_tasks(self):
        sid = self.store.open(self.profile, str(self.path), "read-only", "open")["session_id"]
        task = self.store.dispatch(sid, "later", "later")["task_id"]
        self.assertEqual("paused", self.store.control(sid, "pause_drain", 0, "drain")["desired"])
        self.assertFalse(self.store.worker_starting(sid, "g", 123, "start"))
        self.assertEqual("queued", self.store.get_task(task)["status"])

    def test_drain_running_task_waits_for_its_result(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid)
        self.assertEqual("draining", self.store.control(sid, "pause_drain", 1, "drain")["desired"])
        self.store.finish(sid, "generation", tid, attempt, "completed", {"answer": "done"})
        self.assertEqual("paused", self.store.get_session(sid)["desired"])

    def test_crash_emits_task_event_and_one_parent_notification_without_replay(self):
        self.store.parent_control("parent", "parent_register", 0, "register", "native-parent", str(self.path))
        sid = self.store.open(self.profile, str(self.path), "read-only", "open", "parent")["session_id"]
        self.store.worker_ready(sid, "generation", 123, "start", "native")
        tid, attempt = self.run_task(sid)
        after = self.store.events(task_id=tid)[-1]["seq"]
        self.store.worker_stopped(sid, "generation", "crashed")
        self.store.worker_stopped(sid, "generation", "crashed")
        events = self.store.events(after=after, task_id=tid)
        self.assertEqual(["task_recovery_needed"], [event["kind"] for event in events])
        self.assertEqual(attempt, events[0]["attempt_id"])
        self.assertEqual([events[0]["seq"]], [d["event_seq"] for d in self.store.get_parent("parent")["deliveries"]])
        self.assertIsNone(self.store.claim(self.config))

    def test_assigned_work_is_queued_and_notified_once_when_worker_stops(self):
        self.store.parent_control("parent", "parent_register", 0, "register", "native-parent", str(self.path))
        sid = self.store.open(self.profile, str(self.path), "read-only", "open", "parent")["session_id"]
        self.store.worker_ready(sid, "g", 123, "start", "native")
        tid = self.store.dispatch(sid, "task", "dispatch")["task_id"]
        self.store.claim(self.config)
        self.store.worker_stopped(sid, "g", "worker failed before starting assigned task")
        self.store.worker_stopped(sid, "g", "same failure")
        task = self.store.get_task(tid)
        self.assertEqual("queued", task["status"])
        self.assertIsNone(task["active_attempt"])
        self.assertIsNone(task["attempts"][0]["started"])
        self.assertEqual("cancelled", task["attempts"][0]["status"])
        self.assertEqual(1, len(self.store.get_parent("parent")["deliveries"]))

    def test_retry_reservation_does_not_change_unknown_cost_of_old_attempt(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid, reservation=80)
        self.store.finish(sid, "generation", tid, attempt, "failed", {"error": "usage lost"})
        result = self.store.retry(tid, sid, "task", "retry", reservation=10)
        self.assertEqual(result, self.store.retry(tid, sid, "task", "retry", reservation=10))
        self.assertEqual(80, self.store.budget()["charged_tokens"])
        self.assertEqual(80, self.store.get_task(tid)["attempts"][0]["reservation"])
        self.assertEqual(10, self.store.get_task(tid)["reservation"])
        self.config.token_budget = 100
        claimed = self.store.claim(self.config)
        self.assertIsNotNone(claimed)
        self.assertEqual(10, next(a for a in self.store.get_task(tid)["attempts"] if a["id"] == claimed["attempt_id"])["reservation"])

    def test_existing_database_keeps_attempt_reservations_during_migration(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid, reservation=80)
        self.store.finish(sid, "generation", tid, attempt, "failed", {"error": "usage lost"})
        with self.store.connection(write=True) as db:
            db.execute("ALTER TABLE attempts DROP COLUMN reservation")
        migrated = Store(self.path)
        migrated.retry(tid, sid, "task", "retry", reservation=10)
        self.assertEqual(80, migrated.budget()["charged_tokens"])
        self.assertEqual(80, migrated.get_task(tid)["attempts"][0]["reservation"])

    def test_session_list_identifies_models_without_private_auth_paths(self):
        profile = Profile("named", "pi", "model", provider="provider", effort="low", env_file="/private/auth.json")
        sid = self.store.open(profile, str(self.path), "read-only", "open")["session_id"]
        row = self.store.sessions()[0]
        self.assertEqual((sid, "named", "pi", "provider", "model", "low"), tuple(row[k] for k in ("id", "profile", "runtime", "provider", "model", "effort")))
        self.assertNotIn("env_file", str(row))
        self.assertNotIn("/private/auth.json", str(row))

    def test_shared_budget_cannot_be_overreserved_by_parallel_claims(self):
        self.config.token_budget = 100
        for i in range(4):
            sid = self.session("open" + str(i))
            self.store.dispatch(sid, "test", "task" + str(i), reservation=60)
        with ThreadPoolExecutor(8) as pool:
            claims = list(pool.map(lambda _: self.store.claim(self.config), range(8)))
        self.assertEqual(1, sum(c is not None for c in claims))

    def test_queue_explanation_agrees_with_dependency_fifo_and_budget(self):
        source = self.session("source")
        dep, attempt = self.run_task(source)
        self.store.finish(source, "generation", dep, attempt, "completed", {"answer": "done"}, {"total_tokens": 5})
        target = self.session("target")
        first = self.store.dispatch(target, "first", "first", dependencies=[dep], reservation=10)["task_id"]
        second = self.store.dispatch(target, "second", "second")["task_id"]
        self.assertEqual("dependencies", self.store.waiting_reason(first, self.config)["code"])
        self.assertEqual("session_queue", self.store.waiting_reason(second, self.config)["code"])
        self.assertIsNone(self.store.claim(self.config))
        with self.assertRaises(BridgeError):
            self.store.dispatch(target, "missing", "missing", dependencies=["not-created"])
        self.store.review(dep, attempt, "accepted", "checked", "accept", 1)
        self.config.token_budget = 10
        self.assertEqual("token_budget", self.store.waiting_reason(first, self.config)["code"])
        self.assertIsNone(self.store.claim(self.config))
        self.config.token_budget = 100
        self.assertEqual("ready", self.store.waiting_reason(first, self.config)["code"])
        self.assertEqual(first, self.store.claim(self.config)["task_id"])
        self.assertEqual("session_busy", self.store.waiting_reason(second, self.config)["code"])
        self.store.control(target, "pause", 1, "pause")
        self.assertEqual("paused", self.store.waiting_reason(second, self.config)["desired"])

    def test_deadline_rejects_answer_before_worker_has_processed_expiry(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid)
        question = self.store.question(sid, tid, attempt, {"method": "permission"}, expires=0)
        with self.assertRaisesRegex(BridgeError, "no longer pending"):
            self.store.respond(sid, question, {"decision": "allow"}, 2, "answer")
        self.assertEqual("expired", self.store.get_question(question)["status"])

    def test_first_output_is_recorded_once_and_late_events_cannot_change_it(self):
        sid = self.session("open")
        tid, attempt = self.run_task(sid)
        first = self.store.record("first_output", {"source": "delta"}, sid, tid, attempt)
        timestamp = self.store.get_task(tid)["attempts"][0]["first_output"]
        self.assertIsNotNone(first)
        self.assertIsNone(self.store.record("first_output", {}, sid, tid, attempt))
        self.store.finish(sid, "generation", tid, attempt, "completed", {"answer": "done"})
        self.assertIsNone(self.store.record("first_output", {}, sid, tid, attempt))
        self.assertEqual(timestamp, self.store.get_task(tid)["attempts"][0]["first_output"])
        self.assertEqual(1, sum(e["kind"] == "first_output" for e in self.store.events(task_id=tid)))

    def test_open_name_is_stored_with_session_and_reported_in_event(self):
        result = self.store.open(
            self.profile, str(self.path), "read-only", "open-name", name="  Pipeline Agent  "
        )
        self.assertEqual("Pipeline Agent", result["name"])
        self.assertEqual("Pipeline Agent", self.store.get_session(result["session_id"])["name"])
        opened = [
            event for event in self.store.events(session_id=result["session_id"])
            if event["kind"] == "session_opened"
        ]
        self.assertEqual([{"name": "Pipeline Agent"}], [event["data"] for event in opened])

    def test_open_name_is_part_of_request_fingerprint(self):
        first = self.store.open(self.profile, str(self.path), "read-only", "open-once", name="One")
        self.assertEqual(
            first, self.store.open(self.profile, str(self.path), "read-only", "open-once", name="  One ")
        )
        with self.assertRaisesRegex(BridgeError, "different content"):
            self.store.open(self.profile, str(self.path), "read-only", "open-once", name="Two")
        with self.assertRaisesRegex(BridgeError, "different content"):
            self.store.open(self.profile, str(self.path), "read-only", "open-once")

    def test_validate_session_name_trims_and_rejects_unsafe_values(self):
        self.assertIsNone(validate_session_name(None))
        self.assertEqual("会话 one", validate_session_name("  会话 one  "))
        self.assertEqual("x" * 128, validate_session_name("x" * 128))
        for invalid in ("", "   ", "x" * 129, "a\x00b", "a\nb", "a\rb", "a\tb", "\x01", "\x7f"):
            with self.assertRaises(BridgeError) as caught:
                validate_session_name(invalid)
            self.assertEqual("invalid_request", caught.exception.code)
        with self.assertRaises(BridgeError) as caught:
            validate_session_name(123)  # type: ignore[arg-type]
        self.assertEqual("invalid_request", caught.exception.code)

    def test_duplicate_session_names_are_allowed(self):
        first = self.store.open(self.profile, str(self.path), "read-only", "same-name-1", name="Shared")
        second = self.store.open(self.profile, str(self.path), "read-only", "same-name-2", name="Shared")
        self.assertNotEqual(first["session_id"], second["session_id"])
        self.assertEqual("Shared", self.store.get_session(first["session_id"])["name"])
        self.assertEqual("Shared", self.store.get_session(second["session_id"])["name"])

    def test_set_session_name_upserts_existing_session_only(self):
        sid = self.session("open")
        self.assertNotIn("name", self.store.get_session(sid))
        self.assertEqual("First", self.store.set_session_name(sid, " First "))
        self.assertEqual("First", self.store.get_session(sid)["name"])
        self.store.set_session_name(sid, "Second")
        self.assertEqual("Second", self.store.get_session(sid)["name"])
        with self.assertRaisesRegex(BridgeError, "sessions"):
            self.store.set_session_name("missing", "Name")
        with self.assertRaisesRegex(BridgeError, "1 to 128"):
            self.store.set_session_name(sid, "   ")

    def test_create_session_name_rolls_back_with_session(self):
        with self.assertRaises(RuntimeError):
            with self.store.connection(write=True) as db:
                session_id, _ = self.store.create_session(
                    db, self.profile, str(self.path), "read-only", name="Atomic"
                )
                raise RuntimeError("rollback")
        with self.store.connection() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sessions WHERE id=?", (session_id,)).fetchone())
            self.assertIsNone(
                db.execute("SELECT 1 FROM session_names WHERE session_id=?", (session_id,)).fetchone()
            )

    def test_create_session_writes_name_and_event_inside_caller_transaction(self):
        with self.store.connection(write=True) as db:
            session_id, _ = self.store.create_session(
                db, self.profile, str(self.path), "read-only", name=" Peer "
            )
            row = db.execute("SELECT name FROM session_names WHERE session_id=?", (session_id,)).fetchone()
            self.assertEqual("Peer", row["name"])
        self.assertEqual("Peer", self.store.get_session(session_id)["name"])
        opened = [
            event for event in self.store.events(session_id=session_id) if event["kind"] == "session_opened"
        ]
        self.assertEqual({"name": "Peer"}, opened[0]["data"])
