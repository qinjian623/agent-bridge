"""Focused checks for the embedded dashboard status and freshness helpers."""

from __future__ import annotations

import json
import shutil
import subprocess
import unittest

from agent_bridge.web_monitor import HTML_PAGE

NODE = shutil.which("node")
HELPERS_START = "    const approvalRequestIds = new Map();"
HELPERS_END = "    function runtimeLabel("
RUNNER = r"""
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const api = new Function(input.source + '\nreturn {bridgeStatus, nativeProcessStatus, hostStatus, statusBadgeClass, formatCost, connectionState, snapshotProvenance, coordinatorStatus, aggregateHasAttempts, aggregateHasCostGaps};')();
console.log(JSON.stringify(api[input.name](...input.args)));
"""
HOST_RUNNER = r"""
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
class Element {
  constructor(tag) { this.tag = tag; this.children = []; this.text = ''; this.attrs = {}; this.style = {}; this.title = ''; this.className = ''; }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  addEventListener() {}
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren(...children) { this.children = children; this.text = ''; }
  set textContent(value) { this.text = String(value); this.children = []; }
  get textContent() { return this.text + this.children.map((child) => child.textContent).join(' '); }
}
const body = new Element('tbody');
const stats = new Element('div');
const document = {
  createElement: (tag) => new Element(tag),
  getElementById: (id) => {
    if (id === 'hostTable') return body;
    if (id === 'hostStats') return stats;
    throw new Error('Unexpected element: ' + id);
  }
};
const render = new Function('document', 'hosts', 'coordinator', 'provenance',
  input.source + '\nrenderHosts(hosts, coordinator, provenance); return {body, stats};');
const output = render(document, input.hosts || [], input.coordinator || {}, input.provenance || null);
const serialize = (element) => ({tag: element.tag, text: element.textContent, title: element.title,
  className: element.className, attrs: element.attrs, children: element.children.map(serialize)});
console.log(JSON.stringify({rows: output.body.children.map(serialize), stats: serialize(output.stats)}));
"""


@unittest.skipUnless(NODE, "Node is required to execute the embedded dashboard JavaScript")
class DashboardHelperTests(unittest.TestCase):
    def run_helper(self, name, *args):
        start = HTML_PAGE.index(HELPERS_START)
        end = HTML_PAGE.index(HELPERS_END, start)
        result = subprocess.run(
            [NODE, "-e", RUNNER],
            input=json.dumps({"source": HTML_PAGE[start:end], "name": name, "args": list(args)}),
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        return json.loads(result.stdout)

    def test_bridge_idle_running_review_and_offline_are_distinct(self):
        idle = self.run_helper("bridgeStatus", {
            "observed": "idle", "alive": True, "desired": "open",
        })
        self.assertEqual("Idle \u00b7 no task", idle["label"])
        self.assertEqual("idle", idle["tone"])
        self.assertIn("no task is executing", idle["detail"])

        running = self.run_helper("bridgeStatus", {
            "observed": "running", "alive": True, "task": {"status": "running"},
        })
        self.assertEqual("Task running", running["label"])
        self.assertEqual("running", running["tone"])

        review = self.run_helper("bridgeStatus", {
            "observed": "idle", "alive": True,
            "task": {"status": "completed", "validation": "pending"},
        })
        self.assertEqual("Awaiting review", review["label"])
        self.assertEqual("review", review["tone"])
        self.assertIn("needs review", review["detail"])

        offline = self.run_helper("bridgeStatus", {
            "observed": "running", "alive": False, "desired": "open",
            "task": {"status": "running"},
        })
        self.assertEqual("Worker offline", offline["label"])
        self.assertEqual("danger", offline["tone"])
        self.assertNotIn("Task running", offline["label"])

    def test_missing_alive_is_unknown_not_exit_or_offline(self):
        unknown = self.run_helper("bridgeStatus", {"observed": "unknown"})
        self.assertEqual("Unknown", unknown["label"])
        self.assertNotEqual("Stopped", unknown["label"])

        legacy_running = self.run_helper("bridgeStatus", {
            "observed": "running", "task": {"status": "running"},
        })
        self.assertEqual("Task running", legacy_running["label"])
        self.assertNotEqual("Worker offline", legacy_running["label"])

        idle = self.run_helper("bridgeStatus", {"observed": "idle"})
        self.assertIn("liveness is not reported", idle["detail"])

        stopped = self.run_helper("bridgeStatus", {"observed": "stopped"})
        self.assertEqual("Stopped", stopped["label"])

        closed_without_alive = self.run_helper("bridgeStatus", {"observed": "unknown", "desired": "closed"})
        self.assertEqual("Unknown", closed_without_alive["label"])

    def test_bridge_waiting_recovery_and_completed_review_keep_priority(self):
        waiting = self.run_helper("bridgeStatus", {
            "observed": "waiting_input", "alive": True, "task": {"status": "waiting_input"},
        })
        self.assertEqual("Waiting input", waiting["label"])
        self.assertEqual("waiting", waiting["tone"])

        approval = self.run_helper("bridgeStatus", {
            "observed": "idle", "alive": False, "waiting_question": True,
        })
        self.assertEqual("Waiting approval", approval["label"])

        recovery = self.run_helper("bridgeStatus", {
            "observed": "recovery_needed", "alive": True,
            "task": {"status": "recovery_needed"},
        })
        self.assertEqual("Recovery needed", recovery["label"])
        self.assertEqual("recovery", recovery["tone"])

        completed_offline = self.run_helper("bridgeStatus", {
            "observed": "stopped", "alive": False,
            "task": {"status": "completed", "validation": "pending"},
        })
        self.assertEqual("Awaiting review", completed_offline["label"])

    def test_native_process_presence_is_not_labelled_as_inference(self):
        for proc in (
            {"kind": "worker", "observed": "running"},
            {"kind": "interactive", "observed": "active"},
            {"kind": "daemon", "observed": "running"},
        ):
            with self.subTest(proc=proc):
                status = self.run_helper("nativeProcessStatus", proc)
                self.assertNotEqual("Running", status["label"])
                self.assertIn("process", status["label"].lower())
                self.assertIn("does not prove", status["detail"].lower())
                self.assertIn("liveness is not reported", status["detail"])

        exited = self.run_helper("nativeProcessStatus", {"kind": "worker", "alive": False})
        self.assertEqual("Process exited", exited["label"])
        self.assertEqual("stopped", exited["tone"])

        alive = self.run_helper("nativeProcessStatus", {"kind": "worker", "alive": True})
        self.assertIn("A native process is present", alive["detail"])

    def test_unknown_costs_are_not_displayed_as_zero(self):
        missing = self.run_helper("formatCost", None, False, True, 4)
        self.assertEqual("Unknown", missing["text"])
        self.assertTrue(missing["unknown"])
        self.assertIn("not treated as zero", missing["title"])

        zero_incomplete = self.run_helper("formatCost", 0, False, True, 4)
        self.assertEqual("Unknown", zero_incomplete["text"])

        subtotal = self.run_helper("formatCost", 1.23456, False, True, 4)
        self.assertEqual("$1.2346*", subtotal["text"])
        self.assertIn("some costs are unknown", subtotal["title"])

        complete = self.run_helper("formatCost", 1.23456, True, True, 4)
        self.assertEqual("$1.2346", complete["text"])
        self.assertFalse(complete["unknown"])

        no_attempts = self.run_helper("formatCost", None, True, False, 3)
        self.assertEqual("\u2014", no_attempts["text"])

        no_known_coverage = self.run_helper("formatCost", 0, False, True, 4, 0)
        self.assertEqual("Unknown", no_known_coverage["text"])

        known_zero_subtotal = self.run_helper("formatCost", 0, False, True, 4, 2)
        self.assertEqual("$0.0000*", known_zero_subtotal["text"])

    def test_aggregate_cost_metadata_controls_unknown_cost_state(self):
        aggregate_attempts = self.run_helper(
            "aggregateHasAttempts", {"metrics": {"has_attempts": True}, "sessions": []}
        )
        self.assertTrue(aggregate_attempts)

        aggregate_no_attempts = self.run_helper(
            "aggregateHasAttempts",
            {"metrics": {"has_attempts": False}, "sessions": [{"has_attempts": True}]},
        )
        self.assertFalse(aggregate_no_attempts)

        legacy_attempts = self.run_helper(
            "aggregateHasAttempts", {"sessions": [{"attempt": {"id": "attempt-1"}}]}
        )
        self.assertTrue(legacy_attempts)

        self.assertTrue(self.run_helper("aggregateHasCostGaps", {"metrics": {"has_cost_gaps": True}}))
        self.assertTrue(self.run_helper("aggregateHasCostGaps", {"metrics": {"unknown_cost_sessions": 2}}))
        self.assertFalse(self.run_helper("aggregateHasCostGaps", {"metrics": {"cost_complete": True}}))

    def test_snapshot_provenance_never_defaults_to_online(self):
        broker = self.run_helper("snapshotProvenance", {
            "provenance": {"snapshot_source": "broker", "broker_connected": True},
        })
        self.assertEqual("broker", broker["source"])
        self.assertTrue(broker["brokerConnected"])
        self.assertEqual("Broker connected", self.run_helper("coordinatorStatus", broker)["label"])

        rpc = self.run_helper("snapshotProvenance", {"provenance": {"snapshot_source": "broker_rpc"}})
        self.assertTrue(rpc["brokerConnected"])

        fallback = self.run_helper("snapshotProvenance", {
            "provenance": {
                "snapshot_source": "local_db_fallback",
                "broker_connected": False,
                "detail": "No broker socket is available; local database data follows.",
            },
        })
        fallback_status = self.run_helper("coordinatorStatus", fallback)
        self.assertEqual("Local fallback", fallback_status["label"])
        self.assertNotEqual("online", fallback_status["tone"])
        self.assertIn("local database", fallback_status["detail"])

        for missing in ({}, {"legacy_broker": True}, {"provenance": {"source": "unknown"}}):
            with self.subTest(missing=missing):
                status = self.run_helper("coordinatorStatus", self.run_helper("snapshotProvenance", missing))
                self.assertEqual("Unknown", status["label"])
                self.assertNotEqual("online", status["tone"])

        contradictory = self.run_helper("snapshotProvenance", {"broker_connected": "false"})
        self.assertIsNone(contradictory["brokerConnected"])
        self.assertEqual("Unknown", self.run_helper("coordinatorStatus", contradictory)["label"])

    def test_connection_state_reports_live_stale_and_lost(self):
        connecting = self.run_helper("connectionState", 0, 1000, False)
        self.assertEqual("connecting", connecting["state"])
        self.assertIn("Connecting", connecting["label"])

        not_connected = self.run_helper("connectionState", 0, 1000, True)
        self.assertEqual("error", not_connected["state"])

        live = self.run_helper("connectionState", 1_000_000, 1_004_000, False)
        self.assertEqual("live", live["state"])
        self.assertIn("4s", live["label"])

        stale = self.run_helper("connectionState", 1_000_000, 1_020_000, False)
        self.assertEqual("stale", stale["state"])
        self.assertIn("20s", stale["label"])

        lost = self.run_helper("connectionState", 1_000_000, 1_004_000, True)
        self.assertEqual("error", lost["state"])
        self.assertIn("Connection lost", lost["label"])
        self.assertIn("retrying", lost["detail"])

    def test_unknown_host_state_is_not_offline(self):
        unknown = self.run_helper("hostStatus", {"state": None})
        self.assertEqual("Unknown", unknown["label"])
        self.assertNotEqual("offline", unknown["tone"])
        self.assertIn("not treated as offline", unknown["detail"])

        identity = self.run_helper("hostStatus", {"state": "identity_changed"})
        self.assertEqual("Identity changed", identity["label"])
        self.assertEqual("recovery", identity["tone"])

    def test_status_badge_classes_cover_every_tone(self):
        tones = {
            "running": "badge-running",
            "idle": "badge-idle",
            "waiting": "badge-waiting",
            "recovery": "badge-recovery",
            "review": "badge-review",
            "success": "badge-online",
            "danger": "badge-offline",
            "stopped": "badge-stopped",
            "native": "badge-native",
            "unknown": "badge-unknown",
        }
        for tone, expected in tones.items():
            with self.subTest(tone=tone):
                self.assertIn(expected, self.run_helper("statusBadgeClass", tone))
        self.assertIn("badge-unknown", self.run_helper("statusBadgeClass", "unrecognised"))


class DashboardHostRenderTests(unittest.TestCase):
    def render_hosts(self, provenance):
        helper_start = HTML_PAGE.index(HELPERS_START)
        helper_end = HTML_PAGE.index(HELPERS_END, helper_start)
        utility_start = HTML_PAGE.index("    function setText(")
        utility_end = HTML_PAGE.index("    function emptyRow(", utility_start)
        render_start = HTML_PAGE.index("    function renderHosts(")
        render_end = HTML_PAGE.index("    async function pingHost(", render_start)
        source = HTML_PAGE[helper_start:helper_end] + HTML_PAGE[utility_start:utility_end] + HTML_PAGE[render_start:render_end]
        result = subprocess.run(
            [NODE, "-e", HOST_RUNNER],
            input=json.dumps({
                "source": source,
                "hosts": [{"host_id": "worker-b", "state": "online"}],
                "coordinator": {"host_id": "coordinator", "node_id": "node-1"},
                "provenance": provenance,
            }),
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        return json.loads(result.stdout)

    def test_coordinator_row_uses_provenance_not_http_success(self):
        connected = self.render_hosts({"brokerConnected": True, "source": "broker", "detail": "Broker served it."})
        self.assertIn("Broker connected", json.dumps(connected["rows"][0]))
        self.assertIn("local (unix socket)", json.dumps(connected["rows"][0]))

        fallback = self.render_hosts({
            "brokerConnected": False,
            "source": "local_db_fallback",
            "detail": "Broker unavailable; local database data follows.",
        })
        fallback_text = json.dumps(fallback["rows"][0])
        self.assertIn("Local fallback", fallback_text)
        self.assertNotIn("Online", fallback_text)
        self.assertIn("local database (fallback)", fallback_text)
        self.assertIn("not verified", fallback_text)
        self.assertIn("Local fallback", fallback["stats"]["text"])

        unknown = self.render_hosts(None)
        unknown_text = json.dumps(unknown["rows"][0])
        self.assertIn("Unknown", unknown_text)
        self.assertNotIn("Online", unknown_text)
        self.assertIn("not reported", unknown_text)
        self.assertIn("Unknown", unknown["stats"]["text"])


class DashboardMarkupTests(unittest.TestCase):
    def test_accessibility_and_responsive_contract_are_present(self):
        for expected in (
            'id="connectionStatus"',
            'role="status"',
            'aria-live="polite"',
            'id="connectionBanner"',
            'aria-pressed="false"',
            'aria-describedby="topoScope"',
            'scope="col"',
            'class="skip-link"',
            "prefers-color-scheme: dark",
            "prefers-reduced-motion: reduce",
            ":focus-visible",
            "table-frame",
            "min-height: 44px",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, HTML_PAGE)

    def test_dashboard_keeps_text_only_rendering_and_no_decorative_animation(self):
        self.assertNotIn("innerHTML", HTML_PAGE)
        self.assertNotIn("@keyframes", HTML_PAGE)
        self.assertIn("textContent", HTML_PAGE)
        self.assertIn("createTextNode", HTML_PAGE)
