"""Lightweight embedded Web Monitor Dashboard for cli-agent-bridge.

Zero external dependencies; pure standard library http.server + single-file modern HTML5/CSS/JS.
"""
from __future__ import annotations

import json
import logging
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .config import state_directory
from .monitor import MonitorCollector

logger = logging.getLogger(__name__)

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>cli-agent-bridge | Multi-Agent Monitor</title>
  <style>
    :root {
      --bg: #0d1117;
      --card-bg: #161b22;
      --card-border: #30363d;
      --card-hover: #1c2128;
      --text: #e6edf3;
      --text-muted: #8b949e;
      --accent: #58a6ff;
      --green: #3fb950;
      --green-bg: rgba(63, 185, 80, 0.15);
      --yellow: #d29922;
      --yellow-bg: rgba(210, 153, 34, 0.15);
      --red: #f85149;
      --red-bg: rgba(248, 81, 73, 0.15);
      --blue: #388bfd;
      --blue-bg: rgba(56, 139, 253, 0.15);
      --purple: #bc8cff;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      font-size: 13px;
      line-height: 1.5;
    }
    header {
      background: var(--card-bg);
      border-bottom: 1px solid var(--card-border);
      padding: 12px 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      position: sticky;
      top: 0;
      z-index: 100;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
      font-size: 16px;
      font-weight: 600;
      color: #fff;
    }
    .brand .logo {
      width: 24px;
      height: 24px;
      background: linear-gradient(135deg, var(--accent), var(--purple));
      border-radius: 6px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 12px;
      color: #fff;
    }
    .nav-stats {
      display: flex;
      gap: 20px;
      align-items: center;
    }
    .stat-pill {
      display: flex;
      flex-direction: column;
      align-items: flex-end;
    }
    .stat-pill .val { font-size: 15px; font-weight: 700; color: #fff; }
    .stat-pill .lbl { font-size: 10px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; }

    main {
      padding: 24px;
      max-width: 1440px;
      margin: 0 auto;
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    /* Alert Banner */
    .alert-banner {
      background: var(--yellow-bg);
      border: 1px solid var(--yellow);
      border-radius: 8px;
      padding: 14px 18px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .alert-banner.danger {
      background: var(--red-bg);
      border-color: var(--red);
    }
    .alert-title { font-weight: 600; color: #fff; margin-bottom: 2px; }
    .btn {
      background: var(--green);
      color: #fff;
      border: none;
      padding: 6px 14px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 12px;
      font-weight: 600;
      transition: opacity 0.2s;
    }
    .btn:hover { opacity: 0.9; }
    .btn-secondary {
      background: var(--card-border);
      color: var(--text);
    }

    /* Grid Layout */
    .grid-2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
    }
    @media (max-width: 1024px) {
      .grid-2 { grid-template-columns: 1fr; }
    }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 8px;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }
    .card-header {
      padding: 12px 16px;
      border-bottom: 1px solid var(--card-border);
      font-weight: 600;
      display: flex;
      justify-content: space-between;
      align-items: center;
      background: rgba(255,255,255,0.02);
    }
    .card-body {
      padding: 16px;
      flex: 1;
      overflow-x: auto;
    }

    /* Topology SVG */
    .topo-container {
      width: 100%;
      height: 240px;
      background: #090d13;
      border-radius: 6px;
      position: relative;
    }
    svg.topo-svg {
      width: 100%;
      height: 100%;
    }

    /* Tables */
    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }
    th {
      color: var(--text-muted);
      font-weight: 500;
      padding: 8px 10px;
      border-bottom: 1px solid var(--card-border);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    td {
      padding: 10px;
      border-bottom: 1px solid rgba(48, 54, 61, 0.5);
      vertical-align: middle;
    }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: var(--card-hover); }

    /* Badges */
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 12px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
    }
    .badge-running { background: var(--green-bg); color: var(--green); border: 1px solid var(--green); }
    .badge-idle { background: var(--blue-bg); color: var(--blue); }
    .badge-waiting { background: var(--yellow-bg); color: var(--yellow); border: 1px solid var(--yellow); animation: pulse 1.5s infinite; }
    .badge-stopped { background: rgba(139, 148, 158, 0.15); color: var(--text-muted); }
    .badge-online { background: var(--green-bg); color: var(--green); }
    .badge-offline { background: var(--red-bg); color: var(--red); }

    @keyframes pulse {
      0% { opacity: 1; }
      50% { opacity: 0.5; }
      100% { opacity: 1; }
    }

    /* Event Log */
    .log-container {
      font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
      font-size: 12px;
      max-height: 260px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .log-entry {
      display: flex;
      gap: 12px;
      padding: 4px 8px;
      border-radius: 4px;
      background: rgba(0,0,0,0.2);
    }
    .log-time { color: var(--text-muted); min-width: 60px; }
    .log-kind { color: var(--accent); min-width: 120px; font-weight: 600; }
    .log-msg { color: var(--text); flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

    .pulse-dot {
      display: inline-block;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--green);
      margin-right: 6px;
      box-shadow: 0 0 8px var(--green);
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="logo">⚡</div>
      <span>cli-agent-bridge Monitor</span>
      <span id="coordBadge" style="font-size: 11px; color: var(--text-muted); font-weight: normal; margin-left: 8px;"></span>
    </div>
    <div class="nav-stats">
      <div class="stat-pill">
        <span class="val" id="statActive">0</span>
        <span class="lbl">Active Agents</span>
      </div>
      <div class="stat-pill">
        <span class="val" id="statTasks">0</span>
        <span class="lbl">Running Tasks</span>
      </div>
      <div class="stat-pill">
        <span class="val" id="statBroadcasts">0</span>
        <span class="lbl">Broadcasts</span>
      </div>
      <div class="stat-pill">
        <span class="val" id="statCost">$0.00</span>
        <span class="lbl">Total Cost</span>
      </div>
      <div style="margin-left: 10px;">
        <button class="btn btn-secondary" onclick="fetchData()">↻ Refresh</button>
      </div>
    </div>
  </header>

  <main>
    <!-- Pending Approvals Alert -->
    <div id="approvalSection" style="display: none;"></div>

    <!-- Cluster Topology Card -->
    <div class="card">
      <div class="card-header">
        <div><span class="pulse-dot"></span>Cluster & Communications Topology</div>
        <div id="topoStats" style="font-size: 12px; color: var(--text-muted);"></div>
      </div>
      <div class="card-body" style="padding: 10px;">
        <div class="topo-container">
          <svg class="topo-svg" id="topoGraph"></svg>
        </div>
      </div>
    </div>

    <!-- 2 Column Section -->
    <div class="grid-2">
      <!-- Active Agents & Sessions -->
      <div class="card">
        <div class="card-header">
          <div>Agents & Active Sessions</div>
          <div id="sessionCount" style="font-size: 12px; color: var(--text-muted);">0 sessions</div>
        </div>
        <div class="card-body" style="max-height: 420px; overflow-y: auto;">
          <table>
            <thead>
              <tr>
                <th>Agent / Session</th>
                <th>Runtime</th>
                <th>Status</th>
                <th>Goal / Task</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody id="sessionTable">
              <tr><td colspan="5" style="text-align:center; color:var(--text-muted);">Loading agents...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Communications & Groups -->
      <div class="card">
        <div class="card-header">
          <div>Communications & Groups</div>
          <div id="groupStats" style="font-size: 12px; color: var(--text-muted);">0 broadcasts</div>
        </div>
        <div class="card-body" style="max-height: 420px; overflow-y: auto;">
          <table>
            <thead>
              <tr>
                <th>Broadcast / Group</th>
                <th>Progress</th>
                <th>Message Preview</th>
                <th>Age</th>
              </tr>
            </thead>
            <tbody id="broadcastTable">
              <tr><td colspan="4" style="text-align:center; color:var(--text-muted);">No broadcasts yet</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Events Stream -->
    <div class="card">
      <div class="card-header">
        <div>Live Event Stream</div>
        <div style="font-size: 11px; color: var(--text-muted);">Auto-updating</div>
      </div>
      <div class="card-body">
        <div class="log-container" id="eventLog">
          <div style="color: var(--text-muted);">Waiting for events...</div>
        </div>
      </div>
    </div>
  </main>

  <script>
    let currentData = null;

    async function fetchData() {
      try {
        const res = await fetch('/api/snapshot');
        if (!res.ok) return;
        const data = await res.json();
        currentData = data;
        renderDashboard(data);
      } catch (e) {
        console.error("Fetch error:", e);
      }
    }

    async function approveQuestion(sessionId, questionId) {
      if (!confirm("Approve this tool request?")) return;
      try {
        const res = await fetch('/api/respond', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            session_id: sessionId,
            question_id: questionId,
            decision: "allow"
          })
        });
        const out = await res.json();
        if (out.ok) {
          fetchData();
        } else {
          alert("Approve error: " + (out.error || "unknown"));
        }
      } catch (e) {
        alert("Failed to send approval: " + e);
      }
    }

    function renderDashboard(data) {
      const metrics = data.metrics || {};
      const coord = data.coordinator || {};

      document.getElementById('coordBadge').innerText = `${coord.host_id || 'localhost'} • Node ${coord.node_id ? coord.node_id.substring(0,8) : '?'}`;
      document.getElementById('statActive').innerText = metrics.active_sessions || 0;
      document.getElementById('statTasks').innerText = metrics.running_tasks || 0;
      document.getElementById('statBroadcasts').innerText = metrics.total_broadcasts || 0;
      document.getElementById('statCost').innerText = `$${(metrics.total_cost_usd || 0).toFixed(4)}`;

      // 1. Approvals Alert
      const appSec = document.getElementById('approvalSection');
      const qList = data.pending_questions || [];
      if (qList.length > 0) {
        appSec.style.display = 'block';
        appSec.innerHTML = qList.map(q => `
          <div class="alert-banner">
            <div>
              <div class="alert-title">⚠ Action Required: Permission Approval for "${q.session_name || q.session_id.substring(0,8)}" (<code>${q.session_id.substring(0,8)}</code>)</div>
              <div style="color: var(--text); font-size: 12px;">Tool: <b>${q.tool}</b> | Command: <code>${q.command || q.description || '-'}</code> (${q.created_rel})</div>
            </div>
            <div>
              <button class="btn" onclick="approveQuestion('${q.session_id}', '${q.id}')">✓ Approve</button>
            </div>
          </div>
        `).join('');
      } else {
        appSec.style.display = 'none';
      }

      // 2. Sessions Table
      const sessions = data.sessions || [];
      document.getElementById('sessionCount').innerText = `${sessions.length} sessions (${metrics.active_sessions} active)`;
      const sTable = document.getElementById('sessionTable');
      if (sessions.length === 0) {
        sTable.innerHTML = `<tr><td colspan="5" style="text-align:center; color:var(--text-muted);">No sessions</td></tr>`;
      } else {
        sTable.innerHTML = sessions.slice(0, 15).map(s => {
          let bClass = 'badge-stopped';
          if (s.waiting_question) bClass = 'badge-waiting';
          else if (s.observed === 'running') bClass = 'badge-running';
          else if (s.observed === 'idle') bClass = 'badge-idle';

          const goal = s.task ? s.task.instruction : (s.observed === 'running' ? 'Active' : '-');
          const cost = s.attempt && s.attempt.cost_usd ? `$${s.attempt.cost_usd.toFixed(3)}` : '-';
          const sName = s.name || s.id.substring(0,10);

          return `
            <tr>
              <td>
                <div style="font-weight:600; color:#fff; font-size:13px;">${sName}</div>
                <div style="font-size:11px; color:var(--text-muted);">${s.profile} • <code>${s.id.substring(0,8)}</code></div>
              </td>
              <td><span style="font-weight:600; color:var(--accent);">${s.runtime}</span></td>
              <td><span class="badge ${bClass}">${s.waiting_question ? 'WAITING APPROVAL' : s.observed}</span></td>
              <td style="max-width:240px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${goal}">${goal}</td>
              <td style="color:var(--yellow);">${cost}</td>
            </tr>
          `;
        }).join('');
      }

      // 3. Broadcasts Table
      const broadcasts = data.broadcasts || [];
      document.getElementById('groupStats').innerText = `${broadcasts.length} broadcasts`;
      const bTable = document.getElementById('broadcastTable');
      if (broadcasts.length === 0) {
        bTable.innerHTML = `<tr><td colspan="4" style="text-align:center; color:var(--text-muted);">No broadcast communications recorded</td></tr>`;
      } else {
        bTable.innerHTML = broadcasts.map(b => `
          <tr>
            <td>
              <div style="font-weight:600; color:#fff;">${b.broadcast_id.substring(0,12)}</div>
              <div style="font-size:11px; color:var(--text-muted);">${b.group_id}</div>
            </td>
            <td>
              <span style="color:var(--green); font-weight:600;">${b.received}</span> / ${b.total} recv
              ${b.uncertain ? `<span style="color:var(--red);">(${b.uncertain} uncert)</span>` : ''}
            </td>
            <td style="max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${b.message_preview || '-'}</td>
            <td style="color:var(--text-muted);">${b.created_rel}</td>
          </tr>
        `).join('');
      }

      // 4. Events Log
      const events = data.events || [];
      const evLog = document.getElementById('eventLog');
      if (events.length > 0) {
        evLog.innerHTML = events.slice(0, 15).map(e => `
          <div class="log-entry">
            <span class="log-time">${e.created_rel}</span>
            <span class="log-kind">${e.kind}</span>
            <span class="log-msg"><b>${e.session_name || (e.session_id||'').substring(0,8)}</b> ${e.summary || ''}</span>
          </div>
        `).join('');
      }

      // 5. Topology SVG
      renderTopology(data);
    }

    function renderTopology(data) {
      const svg = document.getElementById('topoGraph');
      const w = svg.clientWidth || 800;
      const h = svg.clientHeight || 240;
      const centerX = w / 2;
      const centerY = h / 2;

      const sessions = (data.sessions || []).slice(0, 6);
      const hosts = (data.hosts || []);

      let elements = [];

      // Coordinator Center
      elements.push(`
        <circle cx="${centerX}" cy="${centerY}" r="28" fill="#1f2937" stroke="#58a6ff" stroke-width="2.5" />
        <text x="${centerX}" y="${centerY - 4}" text-anchor="middle" fill="#fff" font-size="11" font-weight="700">COORD</text>
        <text x="${centerX}" y="${centerY + 10}" text-anchor="middle" fill="#8b949e" font-size="9">${(data.coordinator.host_id || 'local').substring(0,10)}</text>
      `);

      // Radiating sessions/hosts
      const totalNodes = Math.max(sessions.length, 1);
      const radius = Math.min(centerX, centerY) - 45;

      sessions.forEach((s, i) => {
        const angle = (i / totalNodes) * 2 * Math.PI - Math.PI / 2;
        const x = centerX + radius * Math.cos(angle);
        const y = centerY + radius * Math.sin(angle);

        const isRunning = s.observed === 'running';
        const isWaiting = s.waiting_question;
        const strokeColor = isWaiting ? '#d29922' : (isRunning ? '#3fb950' : '#8b949e');
        const fillColor = isRunning ? '#064e3b' : '#111827';

        // Line
        elements.push(`
          <line x1="${centerX}" y1="${centerY}" x2="${x}" y2="${y}" stroke="${strokeColor}" stroke-dasharray="${isRunning ? '4 2' : 'none'}" stroke-width="1.5" opacity="0.7">
            ${isRunning ? '<animate attributeName="stroke-dashoffset" values="0;6" dur="0.8s" repeatCount="indefinite"/>' : ''}
          </line>
        `);

        // Node
        const displayName = s.name ? (s.name.length > 9 ? s.name.substring(0, 8) + '…' : s.name) : s.runtime.substring(0, 7);
        elements.push(`
          <circle cx="${x}" cy="${y}" r="24" fill="${fillColor}" stroke="${strokeColor}" stroke-width="2" />
          <text x="${x}" y="${y - 3}" text-anchor="middle" fill="#fff" font-size="9" font-weight="600">${displayName}</text>
          <text x="${x}" y="${y + 9}" text-anchor="middle" fill="${strokeColor}" font-size="8">${s.id.substring(0,6)}</text>
        `);
      });

      svg.innerHTML = elements.join('');
    }

    // Auto poll every 2s
    fetchData();
    setInterval(fetchData, 2000);
  </script>
</body>
</html>
"""


class WebMonitorHandler(BaseHTTPRequestHandler):
    collector: MonitorCollector | None = None
    state: Path | None = None

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Suppress default noisy console access logging

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
        elif self.path.startswith("/api/snapshot"):
            if self.collector is None:
                self.send_error(500, "Collector not configured")
                return
            snapshot = self.collector.collect(limit=25)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(snapshot).encode("utf-8"))
        else:
            self.send_error(404, "Not Found")

    def do_POST(self) -> None:
        if self.path == "/api/respond":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                payload = json.loads(body)
                session_id = payload.get("session_id")
                question_id = payload.get("question_id")
                decision = payload.get("decision", "allow")
                if not session_id or not question_id:
                    self.send_error(400, "Missing session_id or question_id")
                    return

                from .store import Store

                store = Store(self.state)
                # Inspect revision
                sess = store.get_session(session_id)
                revision = sess["revision"] if sess else 0
                req_id = f"web-approval-{int(time.time()*1000)}"
                store.respond(session_id, question_id, {"decision": decision}, revision, req_id)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok": true}')
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": str(e)}).encode("utf-8"))
        else:
            self.send_error(404, "Not Found")


def run_web_monitor(state: Path | str | None = None, host: str = "127.0.0.1", port: int = 8080) -> None:
    state_dir = state_directory(state)
    WebMonitorHandler.collector = MonitorCollector(state_dir)
    WebMonitorHandler.state = state_dir

    server = ThreadingHTTPServer((host, port), WebMonitorHandler)
    url = f"http://{host}:{port}"
    print(f"\033[36m⚡ cli-agent-bridge Web Monitor running at:\033[0m \033[1m\033[32m{url}\033[0m")
    print(f"\033[2mState directory: {state_dir} | Press Ctrl+C to stop\033[0m")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\033[2mWeb monitor stopped.\033[0m")
    finally:
        server.server_close()
