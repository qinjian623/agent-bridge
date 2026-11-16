"""Run the dashboard's aggregate cost display against coverage metadata."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import unittest

import test_web_ui

from agent_bridge.monitor import render_tui
from agent_bridge.native_monitor import filter_snapshot_dict, summarize_runtimes
from agent_bridge.web_monitor import HTML_PAGE

NODE = shutil.which("node")
RUNNER = r"""
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const output = {textContent: '', title: '', unknown: false};
output.classList = {toggle: (name, value) => { output.unknown = value; }};
const document = {getElementById: (id) => {
  if (id !== 'statCost') throw new Error('Unexpected element: ' + id);
  return output;
}};
if (input.session) {
  const render = new Function('session', input.helpers + '\n' + input.cost + '\nreturn cost;');
  const cost = render(input.data);
  output.textContent = cost.text;
  output.title = cost.title;
  output.unknown = cost.unknown;
} else {
  const render = new Function('data', 'document',
    'const metrics = data.metrics || {};\n' + input.helpers + '\n' + input.cost);
  render(input.data, document);
}
console.log(JSON.stringify(output));
"""


@unittest.skipUnless(NODE, "Node is required to execute the dashboard JavaScript")
class DashboardCostCoverageTests(unittest.TestCase):
    def render_cost(self, data, *, session=False):
        helpers_start = HTML_PAGE.index("    const approvalRequestIds = new Map();")
        helpers_end = HTML_PAGE.index("    function runtimeLabel(", helpers_start)
        if session:
            cost_start = HTML_PAGE.index("        const costNum = (typeof session.cost_usd")
            cost_end = HTML_PAGE.index("        const costCell =", cost_start)
        else:
            cost_start = HTML_PAGE.index("      const costComplete = metrics.cost_complete")
            cost_end = HTML_PAGE.index("      renderApprovals(", cost_start)
        result = subprocess.run(
            [NODE, "-e", RUNNER],
            input=json.dumps({
                "data": data, "session": session, "helpers": HTML_PAGE[helpers_start:helpers_end],
                "cost": HTML_PAGE[cost_start:cost_end],
            }),
            capture_output=True, text=True, timeout=5, check=True,
        )
        return json.loads(result.stdout)

    def test_idle_zero_does_not_count_as_known_attempt_cost(self):
        metrics = {
            "total_sessions": 2, "total_cost_usd": 0.0, "cost_complete": False,
            "known_cost_sessions": 1, "known_cost_attempt_sessions": 0,
            "has_attempts": True, "has_cost_gaps": True, "unknown_cost_sessions": 1,
        }
        page = [{"id": "idle", "has_attempts": False, "cost_usd": 0.0}]
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                coverage = dict(metrics)
                if legacy:
                    for key in ("has_attempts", "has_cost_gaps", "known_cost_attempt_sessions", "unknown_cost_sessions"):
                        coverage.pop(key)
                result = self.render_cost({"metrics": coverage, "sessions": page})
                self.assertEqual("Unknown", result["textContent"])
                self.assertTrue(result["unknown"])

    def test_known_attempt_zero_and_legacy_nonzero_subtotals_remain_visible(self):
        for metrics, expected in (
            ({"total_cost_usd": 0.0, "cost_complete": False, "known_cost_attempt_sessions": 1}, "$0.0000*"),
            ({"total_cost_usd": 0.25, "cost_complete": False}, "$0.2500*"),
            ({"total_cost_usd": 0.0, "cost_complete": True}, "$0.0000"),
        ):
            with self.subTest(metrics=metrics):
                result = self.render_cost({"metrics": metrics, "sessions": []})
                self.assertEqual(expected, result["textContent"])
                self.assertFalse(result["unknown"])

    def test_cost_views_agree_before_and_after_pagination(self):
        idle = {"id": "idle", "runtime": "codex", "has_attempts": False, "cost_usd": 0.0}
        unknown = {"id": "unknown", "runtime": "codex", "has_attempts": True, "cost_usd": None, "cost_complete": False}
        cases = (
            ([idle, unknown], 0.0, "Unknown", "unknown"),
            ([{**idle, "has_attempts": True, "cost_complete": False}, unknown], 0.0, "$0.0000*", "$0.000*"),
            ([{**idle, "has_attempts": True, "cost_usd": 0.25}, unknown], 0.25, "$0.2500*", "$0.250*"),
            ([{**idle, "cost_usd": None, "cost_complete": False}], 0.0, "$0.0000*", "$0.000*"),
        )
        for sessions, total, expected, runtime_expected in cases:
            for complete in (None, False, True):
                metrics = {"total_sessions": len(sessions), "total_cost_usd": total}
                if complete is not None:
                    metrics["cost_complete"] = complete
                for limit in (None, 1):
                    with self.subTest(sessions=sessions, complete=complete, limit=limit):
                        data = filter_snapshot_dict({
                            "coordinator": {}, "metrics": metrics, "sessions": sessions,
                            "runtimes": summarize_runtimes(sessions, []),
                        }, session_limit=limit)
                        wanted = expected
                        if not data["metrics"]["has_attempts"] and complete is not False:
                            wanted = "$0.0000"
                        self.assertEqual(wanted, self.render_cost(data)["textContent"])
                        tui = re.sub(r"\x1b\[[0-9;]*m", "", render_tui(data))
                        self.assertEqual(wanted.lower(), re.search(r"Cost: ([^ |]+)", tui).group(1))
                        section = tui.split("▶ Runtimes", 1)[1].split("▶ Bridge Sessions", 1)[0]
                        row = next(line for line in section.splitlines() if line.strip().lower().startswith("codex"))
                        self.assertEqual(runtime_expected, row.split()[-1])

    def test_session_known_zero_cost_is_not_unknown(self):
        session = {"id": "known", "has_attempts": True, "cost_usd": 0.0, "cost_complete": False}
        self.assertEqual("$0.000*", self.render_cost(session, session=True)["textContent"])
        self.assertEqual("Unknown", self.render_cost({**session, "cost_usd": None}, session=True)["textContent"])

    def test_unknown_legacy_page_does_not_infer_no_attempts(self):
        for complete in (None, False, True):
            metrics = {"total_sessions": 2, "total_cost_usd": 0.0}
            if complete is not None:
                metrics["cost_complete"] = complete
            raw = {
                "coordinator": {},
                "metrics": metrics,
                "sessions": [{"id": "idle", "has_attempts": False, "cost_usd": 0.0}],
            }
            data = filter_snapshot_dict(raw)
            expected = "$0.0000" if complete is True else "Unknown"
            self.assertEqual(expected, self.render_cost(data)["textContent"])
            self.assertEqual(expected, self.render_cost(raw)["textContent"])
            tui = re.sub(r"\x1b\[[0-9;]*m", "", render_tui(raw))
            self.assertEqual(expected.lower(), re.search(r"Cost: ([^ |]+)", tui).group(1))
        missing_attempts = {
            "coordinator": {},
            "metrics": {"total_sessions": 2, "total_cost_usd": 0.0, "cost_complete": False,
                        "known_cost_attempt_sessions": 0},
            "sessions": [{"id": "idle", "has_attempts": False, "cost_usd": 0.0}],
        }
        self.assertEqual("Unknown", self.render_cost(missing_attempts)["textContent"])
        self.assertIn("Cost: \x1b[33munknown", render_tui(missing_attempts))

    def test_explicit_unknown_broker_connectivity_stays_unknown(self):
        helper = test_web_ui.DashboardHelperTests()
        for state, connected in (("unknown", None), (None, None), ("fallback", False), ("unavailable", False)):
            raw = {"snapshot_source": "local_db_fallback", "broker_connected": None}
            if state is not None:
                raw["connection_state"] = state
            with self.subTest(state=state):
                provenance = helper.run_helper("snapshotProvenance", {"provenance": raw})
                self.assertIs(connected, provenance["brokerConnected"])
                status = helper.run_helper("coordinatorStatus", provenance)
                self.assertEqual("Local fallback", status["label"])
                if connected is None:
                    self.assertIn("connectivity is unknown", status["detail"])
