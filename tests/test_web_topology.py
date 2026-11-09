"""Execute the embedded topology code with synthetic data, without network or a browser."""

from __future__ import annotations

import html
import json
import math
import re
import shutil
import subprocess
import unittest

from agent_bridge.web_monitor import HTML_PAGE

NODE = shutil.which("node")
RUNNER = r"""
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
globalThis.fetch = () => { throw new Error('Topology tests must not access the network'); };
const original = JSON.stringify(input.data);
if (input.mode === 'layout') {
  const result = new Function('data', 'width', input.source + '\nreturn buildTopology(data, width);')(
    input.data, input.width);
  console.log(JSON.stringify({result, unchanged: original === JSON.stringify(input.data)}));
} else {
  class Element {
    constructor(tag) { this.tag = tag; this.attrs = {}; this.children = []; this.style = {}; this.text = ''; }
    setAttribute(name, value) { this.attrs[name] = String(value); }
    appendChild(child) { this.children.push(child); return child; }
    replaceChildren(...children) { this.children = children; this.text = ''; }
    set textContent(value) { this.text = String(value); this.children = []; }
    get textContent() { return this.text + this.children.map(child => child.textContent).join(''); }
    set innerHTML(value) { throw new Error('Topology must never interpret HTML'); }
  }
  const svg = new Element('svg');
  svg.parentElement = {clientWidth: input.width};
  const stats = new Element('div');
  const elements = {topoGraph: svg, topoStats: stats};
  const document = {
    createElementNS: (namespace, tag) => new Element(tag),
    getElementById: id => {
      if (!elements[id]) throw new Error('Unexpected element: ' + id);
      return elements[id];
    }
  };
  const setText = (element, value) => { element.textContent = value == null ? '' : String(value); };
  const render = new Function('document', 'SVG_NS', 'setText', 'data',
    input.source + '\nrenderTopology(data);');
  const serialize = element => ({tag: element.tag, attrs: element.attrs, style: element.style,
    text: element.text, children: element.children.map(serialize)});
  const frames = [];
  for (const data of input.frames || [input.data]) {
    render(document, 'http://www.w3.org/2000/svg', setText, data);
    frames.push({svg: serialize(svg), stats: stats.textContent});
  }
  console.log(JSON.stringify({frames, unchanged: original === JSON.stringify(input.data)}));
}
"""


def session(sid, **changes):
    return {"id": sid, "name": "Agent " + sid, "runtime": "codex", "host_id": "local",
            "desired": "open", "observed": "running", "alive": True, **changes}


def nodes(tree, tag=None):
    if tag is None or tree["tag"] == tag:
        yield tree
    for child in tree["children"]:
        yield from nodes(child, tag)


def path_points(path):
    tokens = re.findall(r"[MVHL]|-?\d+(?:\.\d+)?", path)
    points, index, x, y = [], 0, 0.0, 0.0
    while index < len(tokens):
        command = tokens[index]
        index += 1
        if command in ("M", "L"):
            x, y = float(tokens[index]), float(tokens[index + 1])
            index += 2
        elif command == "V":
            y = float(tokens[index])
            index += 1
        elif command == "H":
            x = float(tokens[index])
            index += 1
        else:
            raise AssertionError("Unexpected path command")
        points.append((x, y))
    return points


def distance_to_segment(center, start, end):
    dx, dy = end[0] - start[0], end[1] - start[1]
    length = dx * dx + dy * dy
    offset = 0 if not length else max(0, min(1, ((center[0] - start[0]) * dx + (center[1] - start[1]) * dy) / length))
    return math.hypot(center[0] - start[0] - offset * dx, center[1] - start[1] - offset * dy)


@unittest.skipUnless(NODE, "Node is required to execute the embedded topology JavaScript")
class WebTopologyTests(unittest.TestCase):
    def execute(self, data, *, width=800, render=False, frames=None):
        start = HTML_PAGE.index("    function svgNode(" if render else "    function buildTopology(")
        end = HTML_PAGE.index("    function renderDashboard(" if render else "    function renderTopology(", start)
        result = subprocess.run(
            [NODE, "-e", RUNNER],
            input=json.dumps({"source": HTML_PAGE[start:end], "data": data, "width": width,
                              "mode": "render" if render else "layout", "frames": frames}),
            capture_output=True, text=True, timeout=5, check=True,
        )
        value = json.loads(result.stdout)
        self.assertTrue(value["unchanged"], "Rendering must not mutate the snapshot")
        return value["frames"] if render else value["result"]

    def test_only_live_unclosed_local_workers_are_drawn(self):
        data = {"sessions": [
            session("live"), session("idle", observed="idle"),
            session("closed", desired="closed"), session("stopped", observed="stopped"),
            session("dead", alive=False), session("unverified", alive="true"),
            session("question-without-worker", alive=False, waiting_question=True),
            session("remote", host_id="linux-node-b"), session("recovery", alive=False, observed="recovery_needed"),
        ]}
        value = self.execute(data)
        self.assertEqual({"live", "idle"}, {node["session"]["id"] for node in value["hostNodes"][0]["sessions"]})
        self.assertEqual(2, value["liveCount"])
        self.assertEqual(1, value["recoveryCount"])

    def test_recent_history_never_hides_an_older_live_worker(self):
        data = {"sessions": [session(str(i), desired="closed", alive=False, observed="stopped") for i in range(100)]}
        data["sessions"].append(session("old-active", created=1))
        value = self.execute(data)
        self.assertEqual(["old-active"], [node["session"]["id"] for node in value["hostNodes"][0]["sessions"]])

    def test_all_live_workers_are_shown_without_four_or_six_item_limit(self):
        value = self.execute({"sessions": [session(str(i)) for i in range(9)]}, width=280)
        displayed = value["hostNodes"][0]["sessions"]
        self.assertEqual(9, len(displayed))
        self.assertEqual(9, value["liveCount"])
        self.assertGreater(value["width"], 280)
        self.assertTrue(all(node["y"] == 170 for node in displayed))
        self.assertTrue(all(right["x"] - left["x"] >= 44 for left, right in zip(displayed, displayed[1:], strict=False)))
        self.assertGreaterEqual(displayed[0]["x"] - 18, 0)
        self.assertLessEqual(displayed[-1]["x"] + 18, value["width"])

    def test_absent_host_means_local_but_remote_and_native_data_are_not_invented(self):
        local = session("legacy")
        local.pop("host_id")
        value = self.execute({"hosts": [{"host_id": "linux-node-b", "state": "online"}],
                              "sessions": [local, session("remote", host_id="linux-node-b")],
                              "native_group_members": [{"native_id": "native", "runtime": "codex"}]})
        self.assertEqual(1, value["liveCount"])
        self.assertEqual(["legacy"], [node["session"]["id"] for node in value["hostNodes"][0]["sessions"]])
        self.assertEqual([], value["hostNodes"][1]["sessions"])

    def test_layout_handles_zero_two_and_many_remote_hosts_at_narrow_width(self):
        for count in (0, 2, 12):
            with self.subTest(remote_hosts=count):
                value = self.execute({"hosts": [{"host_id": "host-" + str(i)} for i in range(count)]}, width=260)
                hosts = value["hostNodes"]
                self.assertEqual(count + 1, len(hosts))
                self.assertEqual(240, value["height"])
                self.assertEqual(1, sum(host["isCoord"] for host in hosts))
                self.assertTrue(all(host["y"] == 80 for host in hosts))
                self.assertTrue(all(right["x"] - left["x"] >= 180 for left, right in zip(hosts, hosts[1:], strict=False)))
                self.assertGreaterEqual(hosts[0]["x"] - 28, 0)
                self.assertLessEqual(hosts[-1]["x"] + 28, value["width"])

    def test_empty_view_explains_missing_local_and_remote_session_data(self):
        frame = self.execute({"hosts": [{"host_id": "linux-node-b", "state": "online"}]}, render=True)[0]
        labels = [node["text"] for node in nodes(frame["svg"], "text")]
        self.assertIn("No live Bridge workers", labels)
        self.assertIn("Sessions not collected", labels)
        self.assertIn("0 live local", frame["stats"])
        self.assertNotIn("data-session-id", json.dumps(frame["svg"]))

    def test_relay_paths_avoid_every_host_circle(self):
        for count in (2, 10):
            with self.subTest(remote_hosts=count):
                data = {"hosts": [{"host_id": "host-" + str(i), "state": "online"} for i in range(count)]}
                frame = self.execute(data, width=320, render=True)[0]
                circles = [node for node in nodes(frame["svg"], "circle") if "data-host-id" in node["attrs"]]
                paths = list(nodes(frame["svg"], "path"))
                self.assertEqual(count, len(paths))
                for path in paths:
                    points = path_points(path["attrs"]["d"])
                    self.assertEqual(20, points[1][1])
                    for circle in circles:
                        attrs = circle["attrs"]
                        center = float(attrs["cx"]), float(attrs["cy"])
                        for start, end in zip(points, points[1:], strict=False):
                            self.assertGreaterEqual(distance_to_segment(center, start, end) + 1e-7, float(attrs["r"]))

    def test_unknown_and_identity_failure_are_not_reported_as_offline(self):
        states = ["online", "offline", "identity_changed", "state_corrupted", "unknown", None]
        frame = self.execute({"hosts": [{"host_id": str(i), "state": state} for i, state in enumerate(states)]}, render=True)[0]
        labels = [node["text"] for node in nodes(frame["svg"], "text")]
        self.assertEqual(1, labels.count("OFFLINE"))
        self.assertEqual(1, labels.count("ID CHANGED"))
        self.assertEqual(1, labels.count("CORRUPT"))
        self.assertEqual(2, labels.count("UNKNOWN"))
        circles = {node["attrs"]["data-host-id"]: node for node in nodes(frame["svg"], "circle")}
        self.assertEqual("#d29922", circles["2"]["attrs"]["stroke"])
        self.assertEqual("#d29922", circles["3"]["attrs"]["stroke"])

    def test_tooltips_keep_full_names_status_and_id_as_literal_text(self):
        name = '<img src=x onerror="unexpected()"> a complete long agent name'
        sid = 'test-id-"-<script>'
        frame = self.execute({"sessions": [session(sid, name=name, observed="waiting_input", waiting_question=True)]}, render=True)[0]
        circle = next(node for node in nodes(frame["svg"], "circle") if "data-session-id" in node["attrs"])
        self.assertEqual(sid, circle["attrs"]["data-session-id"])
        title = next(nodes(circle, "title"))["text"]
        for expected in (name, sid, "waiting_input", "codex"):
            self.assertIn(expected, title)
        self.assertEqual("#d29922", circle["attrs"]["stroke"])
        self.assertFalse({"img", "script", "foreignObject"} & {node["tag"] for node in nodes(frame["svg"])})

    def test_refresh_removes_closed_workers_and_does_not_duplicate_nodes(self):
        initial = {"sessions": [session("finishes")], "hosts": [{"host_id": "linux-node-b", "state": "online"}]}
        final = {"sessions": [session("finishes", desired="closed", alive=False, observed="stopped")], "hosts": initial["hosts"]}
        frames = self.execute(initial, render=True, frames=[initial, final])
        self.assertIn("data-session-id", json.dumps(frames[0]["svg"]))
        self.assertNotIn("data-session-id", json.dumps(frames[1]["svg"]))
        self.assertEqual(2, len(list(nodes(frames[1]["svg"], "circle"))))
        self.assertEqual(1, len(list(nodes(frames[1]["svg"], "path"))))

    def test_connections_do_not_animate_as_if_message_traffic_were_measured(self):
        frame = self.execute({"hosts": [{"host_id": "linux-node-b", "state": "online"}],
                              "sessions": [session("running")]}, render=True)[0]
        self.assertEqual([], list(nodes(frame["svg"], "animate")))


class WebTopologyScopeTests(unittest.TestCase):
    def test_title_and_scope_match_the_data_that_is_actually_collected(self):
        text = html.unescape(HTML_PAGE)
        self.assertIn("Host Connections & Live Bridge Sessions", text)
        self.assertNotIn("Cluster & Communications Topology", text)
        self.assertIn("Remote/native sessions and message traffic are not collected.", text)
        self.assertIn("local Bridge workers with live processes", text)
        self.assertIn("overflow-x: auto", text)
