"""Lightweight embedded Web Monitor Dashboard for cli-agent-bridge.

Zero external dependencies; pure standard library http.server + single-file modern HTML5/CSS/JS.

Security model (Phase C, package C7):
- The long-lived management token is read from ``admin.token`` on every request and verified to be a
  regular file owned by the current UID with mode ``0600``. It never appears in URLs, HTML, cookies or logs.
- Browser access uses a short-lived in-memory web session (random 256-bit ID) carried in an HttpOnly,
  SameSite=Strict cookie. Sessions are bound to the token hash and invalidated when the token rotates.
- Every request checks the ``Host`` header; cookie-authenticated writes also require a matching ``Origin``
  and ``X-CSRF-Token``. Approval writes are forwarded to the local Broker over ``broker.sock``.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os  # noqa: F401 (kept so tests can patch ``agent_bridge.web_monitor.os``)
import secrets
import socket
import threading
import time
from http.cookies import CookieError, SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .config import MAX_TOKEN_BYTES, read_admin_token, safe_error, state_directory  # noqa: F401 (re-exported)
from .errors import BridgeError
from .monitor import MonitorCollector

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 8 * 3600
MAX_BODY_BYTES = 64 * 1024
MAX_BROKER_RESPONSE_BYTES = 16 * 1024 * 1024
BROKER_TIMEOUT_SECONDS = 30.0
SESSION_COOKIE = "bridge_session"
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
    "script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"
)
DEFAULT_ALLOWED_HOSTS = {
    "127.0.0.1",
    "localhost",
    "[::1]",
    "127.0.0.1:8080",
    "localhost:8080",
}

_HTML_HEAD = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="referrer" content="no-referrer">
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
      gap: 16px;
      margin-bottom: 12px;
    }
    .alert-banner.danger {
      background: var(--red-bg);
      border-color: var(--red);
    }
    .alert-title { font-weight: 600; color: #fff; margin-bottom: 2px; }
    .alert-actions { display: flex; gap: 8px; flex: 0 0 auto; }
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
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-secondary {
      background: var(--card-border);
      color: var(--text);
    }
    .btn-danger {
      background: var(--red);
      color: #fff;
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
      overflow-x: auto;
    }
    .topo-container:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
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
    .badge-node { background: rgba(56, 139, 253, 0.15); color: var(--accent); border: 1px solid rgba(88, 166, 255, 0.3); font-size: 10px; font-weight: 600; padding: 1px 6px; border-radius: 4px; }
    .badge-coord { background: rgba(188, 140, 255, 0.15); color: var(--purple); border: 1px solid rgba(188, 140, 255, 0.3); }
    .btn-sm { padding: 3px 8px; font-size: 11px; }

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
    .empty-cell { text-align: center; color: var(--text-muted); }
  </style>
</head>
<body>
"""
_HTML_BODY = """  <header>
    <div class="brand">
      <div class="logo">⚡</div>
      <span>cli-agent-bridge Monitor</span>
      <span id="coordBadge" style="font-size: 11px; color: var(--text-muted); font-weight: normal; margin-left: 8px;"></span>
    </div>
    <div class="nav-stats">
      <div class="stat-pill">
        <span class="val" id="statNodes">1/1</span>
        <span class="lbl">Cluster Nodes</span>
      </div>
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
      <div style="margin-left: 10px; display: flex; gap: 8px;">
        <button class="btn btn-secondary" id="refreshBtn" type="button">↻ Refresh</button>
        <button class="btn btn-danger" id="logoutBtn" type="button">Sign out</button>
      </div>
    </div>
  </header>

  <main>
    <!-- Pending Approvals Alert -->
    <div id="approvalSection" style="display: none;"></div>

    <!-- Cluster Topology Card -->
    <div class="card">
      <div class="card-header">
        <div><span class="pulse-dot"></span>Host Connections &amp; Live Bridge Sessions</div>
        <div id="topoStats" style="font-size: 12px; color: var(--text-muted);"></div>
      </div>
      <div class="card-body" style="padding: 10px;">
        <div class="topo-container" tabindex="0" aria-label="Host connections; scroll horizontally if needed">
          <svg class="topo-svg" id="topoGraph" aria-label="Host connections and live local Bridge sessions"></svg>
        </div>
        <p id="topoScope" style="margin: 8px 4px 0; color: var(--text-muted); font-size: 12px;">
          Host links show SSH connection status. Sessions shown: local Bridge workers with live processes.
          Remote/native sessions and message traffic are not collected.
        </p>
      </div>
    </div>

    <!-- Cluster Nodes Card -->
    <div class="card">
      <div class="card-header">
        <div>Cluster Nodes (Execution Hosts)</div>
        <div id="hostStats" style="font-size: 12px; color: var(--text-muted);">1 node</div>
      </div>
      <div class="card-body" style="max-height: 280px; overflow-y: auto;">
        <table>
          <thead>
            <tr>
              <th>Host ID</th>
              <th>Role</th>
              <th>Status</th>
              <th>SSH Target</th>
              <th>Node ID &amp; Generation</th>
              <th>Managed Projects</th>
              <th>Last Contact</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="hostTable">
            <tr><td class="empty-cell" colspan="8">Loading cluster nodes...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 2 Column Section -->
    <div class="grid-2">
      <!-- Active Agents & Sessions -->
      <div class="card">
        <div class="card-header">
          <div>Agents &amp; Active Sessions</div>
          <div id="sessionCount" style="font-size: 12px; color: var(--text-muted);">0 sessions</div>
        </div>
        <div class="card-body" style="max-height: 420px; overflow-y: auto;">
          <table>
            <thead>
              <tr>
                <th>Agent / Session</th>
                <th>Host</th>
                <th>Runtime</th>
                <th>Status</th>
                <th>Goal / Task</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody id="sessionTable">
              <tr><td class="empty-cell" colspan="6">Loading agents...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Communications & Groups -->
      <div class="card">
        <div class="card-header">
          <div>Communications &amp; Groups</div>
          <div id="groupStats" style="font-size: 12px; color: var(--text-muted);">0 broadcasts</div>
        </div>
        <div class="card-body" style="max-height: 420px; overflow-y: auto;">
          <table>
            <thead>
              <tr>
                <th>Broadcast / Group</th>
                <th>Progress</th>
                <th>Members</th>
                <th>Message Preview</th>
                <th>Age</th>
              </tr>
            </thead>
            <tbody id="broadcastTable">
              <tr><td class="empty-cell" colspan="5">No broadcasts yet</td></tr>
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

"""
_HTML_SCRIPT = """  <script>
    'use strict';

    const SVG_NS = 'http://www.w3.org/2000/svg';
    let currentData = null;
    let csrfToken = null;
    const approvalRequestIds = new Map();

    function setText(element, value) {
      element.textContent = value === null || value === undefined ? '' : String(value);
    }

    function makeElement(tagName, className, text) {
      const element = document.createElement(tagName);
      if (className) element.className = className;
      if (text !== undefined) setText(element, text);
      return element;
    }

    function emptyRow(columns, message) {
      const row = document.createElement('tr');
      const cell = makeElement('td', 'empty-cell', message);
      cell.colSpan = columns;
      row.appendChild(cell);
      return row;
    }

    function newRequestId() {
      if (window.crypto && typeof window.crypto.randomUUID === 'function') {
        return window.crypto.randomUUID();
      }
      const bytes = new Uint8Array(16);
      if (window.crypto && typeof window.crypto.getRandomValues === 'function') {
        window.crypto.getRandomValues(bytes);
      } else {
        for (let index = 0; index < bytes.length; index += 1) {
          bytes[index] = Math.floor(Math.random() * 256);
        }
      }
      return Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
    }

    async function loadSession() {
      try {
        const response = await fetch('/auth/session', { credentials: 'same-origin' });
        if (!response.ok) return false;
        const payload = await response.json();
        csrfToken = payload.csrf_token;
        return Boolean(csrfToken);
      } catch (error) {
        return false;
      }
    }

    async function fetchData() {
      try {
        const response = await fetch('/api/snapshot', { credentials: 'same-origin' });
        if (response.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (!response.ok) return;
        currentData = await response.json();
        renderDashboard(currentData);
      } catch (error) {
        console.error('Fetch error:', error);
      }
    }

    function findSessionRevision(sessionId) {
      const sessions = (currentData && currentData.sessions) || [];
      const session = sessions.find((item) => item.id === sessionId);
      return session && typeof session.revision === 'number' ? session.revision : null;
    }

    async function submitDecision(sessionId, questionId, decision, revision) {
      if (revision === null || revision === undefined) return;
      const key = sessionId + ':' + questionId;
      let requestId = approvalRequestIds.get(key);
      if (!requestId) {
        requestId = newRequestId();
        approvalRequestIds.set(key, requestId);
      }
      try {
        const response = await fetch('/api/respond', {
          method: 'POST',
          credentials: 'same-origin',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken || ''
          },
          body: JSON.stringify({
            session_id: sessionId,
            question_id: questionId,
            decision: decision,
            expected_revision: revision,
            request_id: requestId
          })
        });
        let payload = {};
        try { payload = await response.json(); } catch (error) { payload = {}; }
        if (response.ok && payload.ok) {
          approvalRequestIds.delete(key);
          await fetchData();
          return;
        }
        if (response.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (response.status === 409) {
          approvalRequestIds.delete(key);
          window.alert('This approval is stale or was already handled. Refreshing.');
          await fetchData();
          return;
        }
        // Keep the same request id for transient failures so a retry is idempotent.
        window.alert('Approval failed: ' + (payload.error || 'unknown error'));
      } catch (error) {
        window.alert('Failed to send approval: ' + error);
      }
    }

    function renderApprovals(questions) {
      const section = document.getElementById('approvalSection');
      section.replaceChildren();
      if (!questions.length) {
        section.style.display = 'none';
        return;
      }
      section.style.display = 'block';
      questions.forEach((question) => {
        const banner = makeElement('div', 'alert-banner');
        const info = makeElement('div');
        const title = makeElement('div', 'alert-title');
        const sessionName = question.session_name || String(question.session_id || '').substring(0, 8);
        setText(
          title,
          '⚠ Action Required: Permission Approval for "' + sessionName + '" (' +
          String(question.session_id || '').substring(0, 8) + ')'
        );
        info.appendChild(title);
        const detail = makeElement('div');
        detail.style.color = 'var(--text)';
        detail.style.fontSize = '12px';
        detail.appendChild(document.createTextNode('Tool: '));
        const toolName = makeElement('b', '', question.tool || '-');
        detail.appendChild(toolName);
        detail.appendChild(document.createTextNode(' | Command: '));
        detail.appendChild(makeElement('code', '', question.command || question.description || '-'));
        detail.appendChild(document.createTextNode(' (' + (question.created_rel || '-') + ')'));
        info.appendChild(detail);

        const actions = makeElement('div', 'alert-actions');
        const revision = findSessionRevision(question.session_id);
        const allowButton = makeElement('button', 'btn', '✓ Approve');
        allowButton.type = 'button';
        allowButton.disabled = revision === null;
        allowButton.addEventListener('click', () => {
          submitDecision(question.session_id, question.id, 'allow', revision);
        });
        const denyButton = makeElement('button', 'btn btn-secondary', '✕ Deny');
        denyButton.type = 'button';
        denyButton.disabled = revision === null;
        denyButton.addEventListener('click', () => {
          submitDecision(question.session_id, question.id, 'deny', revision);
        });
        actions.appendChild(allowButton);
        actions.appendChild(denyButton);

        banner.appendChild(info);
        banner.appendChild(actions);
        section.appendChild(banner);
      });
    }

    function renderHosts(hosts, coordinator) {
      const body = document.getElementById('hostTable');
      body.replaceChildren();
      const totalNodes = (hosts ? hosts.length : 0) + 1;
      document.getElementById('hostStats').textContent = totalNodes + ' nodes';

      // 1. Coordinator Row
      const cRow = document.createElement('tr');
      const cNameCell = document.createElement('td');
      const cNameLine = makeElement('div', '', (coordinator.host_id || 'coordinator') + ' ⚡');
      cNameLine.style.fontWeight = '700';
      cNameLine.style.color = '#fff';
      cNameCell.appendChild(cNameLine);
      cRow.appendChild(cNameCell);

      const cRoleCell = document.createElement('td');
      cRoleCell.appendChild(makeElement('span', 'badge badge-coord', 'Coordinator'));
      cRow.appendChild(cRoleCell);

      const cStatusCell = document.createElement('td');
      cStatusCell.appendChild(makeElement('span', 'badge badge-online', 'ONLINE'));
      cRow.appendChild(cStatusCell);

      const cTargetCell = document.createElement('td');
      cTargetCell.appendChild(makeElement('code', '', 'local (unix sock)'));
      cRow.appendChild(cTargetCell);

      const cNodeCell = document.createElement('td');
      cNodeCell.appendChild(makeElement('code', '', coordinator.node_id ? String(coordinator.node_id).substring(0, 16) : '-'));
      cRow.appendChild(cNodeCell);

      const cProjsCell = makeElement('td', '', 'bridge (local)');
      cRow.appendChild(cProjsCell);

      const cContactCell = makeElement('td', '', 'active');
      cContactCell.style.color = 'var(--green)';
      cRow.appendChild(cContactCell);

      const cActionCell = makeElement('td', '', '-');
      cActionCell.style.color = 'var(--text-muted)';
      cRow.appendChild(cActionCell);
      body.appendChild(cRow);

      // 2. Remote Worker Hosts
      if (hosts && hosts.length) {
        hosts.forEach((host) => {
          const row = document.createElement('tr');
          const nameCell = document.createElement('td');
          const nameLine = makeElement('div', '', host.host_id || '-');
          nameLine.style.fontWeight = '600';
          nameLine.style.color = '#fff';
          nameCell.appendChild(nameLine);
          row.appendChild(nameCell);

          const roleCell = document.createElement('td');
          roleCell.appendChild(makeElement('span', 'badge badge-idle', 'Worker Node'));
          row.appendChild(roleCell);

          const statusCell = document.createElement('td');
          const isOnline = host.state === 'online';
          const isOffline = host.state === 'offline';
          const badgeClass = isOnline ? 'badge-online' : (isOffline ? 'badge-offline' : 'badge-waiting');
          statusCell.appendChild(makeElement('span', 'badge ' + badgeClass, String(host.state || 'offline').toUpperCase()));
          row.appendChild(statusCell);

          const targetCell = document.createElement('td');
          targetCell.appendChild(makeElement('code', '', host.ssh_target || '-'));
          row.appendChild(targetCell);

          const nodeCell = document.createElement('td');
          const nodeStr = host.node_id ? String(host.node_id).substring(0, 16) + '…' : '-';
          nodeCell.appendChild(makeElement('code', '', nodeStr));
          row.appendChild(nodeCell);

          const projs = host.projects && typeof host.projects === 'object' ? Object.keys(host.projects).join(', ') : '-';
          const projsCell = makeElement('td', '', projs || '-');
          row.appendChild(projsCell);

          const contactCell = makeElement('td', '', host.last_contact_rel || '-');
          contactCell.style.color = 'var(--text-muted)';
          row.appendChild(contactCell);

          const actionCell = document.createElement('td');
          const pingButton = makeElement('button', 'btn btn-secondary btn-sm', 'Ping');
          pingButton.type = 'button';
          pingButton.addEventListener('click', () => {
            pingHost(host.host_id, pingButton);
          });
          actionCell.appendChild(pingButton);
          row.appendChild(actionCell);

          body.appendChild(row);
        });
      }
    }

    async function pingHost(hostId, button) {
      if (!hostId) return;
      setText(button, 'Pinging...');
      button.disabled = true;
      try {
        const response = await fetch('/api/host/ping', {
          method: 'POST',
          credentials: 'same-origin',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken || ''
          },
          body: JSON.stringify({ host_id: hostId })
        });
        if (response.ok) {
          await fetchData();
        } else {
          setText(button, 'Failed');
          setTimeout(() => { setText(button, 'Ping'); button.disabled = false; }, 2000);
        }
      } catch (error) {
        setText(button, 'Error');
        setTimeout(() => { setText(button, 'Ping'); button.disabled = false; }, 2000);
      }
    }

    function renderSessions(sessions, metrics) {
      const body = document.getElementById('sessionTable');
      body.replaceChildren();
      document.getElementById('sessionCount').textContent =
        sessions.length + ' sessions (' + (metrics.active_sessions || 0) + ' active)';
      if (!sessions.length) {
        body.appendChild(emptyRow(6, 'No sessions'));
        return;
      }
      sessions.slice(0, 15).forEach((session) => {
        const row = document.createElement('tr');
        const nameCell = document.createElement('td');
        const nameLine = makeElement('div', '', session.name || String(session.id || '').substring(0, 10));
        nameLine.style.fontWeight = '600';
        nameLine.style.color = '#fff';
        nameLine.style.fontSize = '13px';
        const metaLine = makeElement('div', '', (session.profile || '-') + ' • ');
        metaLine.style.fontSize = '11px';
        metaLine.style.color = 'var(--text-muted)';
        metaLine.appendChild(makeElement('code', '', String(session.id || '').substring(0, 8)));
        nameCell.appendChild(nameLine);
        nameCell.appendChild(metaLine);

        const hostCell = document.createElement('td');
        const hostName = session.host_id || 'local';
        const hostBadge = makeElement('span', 'badge-node', hostName);
        hostCell.appendChild(hostBadge);

        const runtimeCell = document.createElement('td');
        const runtimeText = makeElement('span', '', session.runtime || '-');
        runtimeText.style.fontWeight = '600';
        runtimeText.style.color = 'var(--accent)';
        runtimeCell.appendChild(runtimeText);

        const statusCell = document.createElement('td');
        let badgeClass = 'badge-stopped';
        if (session.waiting_question) badgeClass = 'badge-waiting';
        else if (session.observed === 'running') badgeClass = 'badge-running';
        else if (session.observed === 'idle') badgeClass = 'badge-idle';
        const statusText = session.waiting_question ? 'WAITING APPROVAL' : (session.observed || 'unknown');
        statusCell.appendChild(makeElement('span', 'badge ' + badgeClass, statusText));

        const goal = session.task ? session.task.instruction : (session.observed === 'running' ? 'Active' : '-');
        const goalCell = makeElement('td', '', goal || '-');
        goalCell.style.maxWidth = '240px';
        goalCell.style.overflow = 'hidden';
        goalCell.style.textOverflow = 'ellipsis';
        goalCell.style.whiteSpace = 'nowrap';
        goalCell.title = goal || '';

        const cost = session.attempt && typeof session.attempt.cost_usd === 'number'
          ? '$' + session.attempt.cost_usd.toFixed(3)
          : '-';
        const costCell = makeElement('td', '', cost);
        costCell.style.color = 'var(--yellow)';

        row.appendChild(nameCell);
        row.appendChild(hostCell);
        row.appendChild(runtimeCell);
        row.appendChild(statusCell);
        row.appendChild(goalCell);
        row.appendChild(costCell);
        body.appendChild(row);
      });
    }

    function renderBroadcasts(broadcasts) {
      const body = document.getElementById('broadcastTable');
      body.replaceChildren();
      document.getElementById('groupStats').textContent = broadcasts.length + ' broadcasts';
      if (!broadcasts.length) {
        body.appendChild(emptyRow(5, 'No broadcast communications recorded'));
        return;
      }
      broadcasts.forEach((broadcast) => {
        const row = document.createElement('tr');
        const idCell = document.createElement('td');
        const idLine = makeElement('div', '', String(broadcast.broadcast_id || '').substring(0, 12));
        idLine.style.fontWeight = '600';
        idLine.style.color = '#fff';
        const groupLine = makeElement('div', '', broadcast.group_id || '-');
        groupLine.style.fontSize = '11px';
        groupLine.style.color = 'var(--text-muted)';
        idCell.appendChild(idLine);
        idCell.appendChild(groupLine);

        const progressCell = document.createElement('td');
        const received = makeElement('span', '', broadcast.received);
        received.style.color = 'var(--green)';
        received.style.fontWeight = '600';
        progressCell.appendChild(received);
        progressCell.appendChild(document.createTextNode(' / ' + broadcast.total + ' recv'));
        if (broadcast.uncertain) {
          const uncertain = makeElement('span', '', ' (' + broadcast.uncertain + ' uncert)');
          uncertain.style.color = 'var(--red)';
          progressCell.appendChild(uncertain);
        }

        const membersCell = document.createElement('td');
        setText(membersCell, (broadcast.total || 0) + ' targets');
        membersCell.style.color = 'var(--text-muted)';

        const messageCell = makeElement('td', '', broadcast.message_preview || '-');
        messageCell.style.maxWidth = '200px';
        messageCell.style.overflow = 'hidden';
        messageCell.style.textOverflow = 'ellipsis';
        messageCell.style.whiteSpace = 'nowrap';

        const ageCell = makeElement('td', '', broadcast.created_rel || '-');
        ageCell.style.color = 'var(--text-muted)';

        row.appendChild(idCell);
        row.appendChild(progressCell);
        row.appendChild(membersCell);
        row.appendChild(messageCell);
        row.appendChild(ageCell);
        body.appendChild(row);
      });
    }

    function renderEvents(events) {
      const container = document.getElementById('eventLog');
      container.replaceChildren();
      if (!events.length) {
        const placeholder = makeElement('div', '', 'Waiting for events...');
        placeholder.style.color = 'var(--text-muted)';
        container.appendChild(placeholder);
        return;
      }
      events.slice(0, 15).forEach((event) => {
        const entry = makeElement('div', 'log-entry');
        entry.appendChild(makeElement('span', 'log-time', event.created_rel || '-'));
        entry.appendChild(makeElement('span', 'log-kind', event.kind || '-'));
        const message = makeElement('span', 'log-msg');
        const name = makeElement('b', '', event.session_name || String(event.session_id || '').substring(0, 8));
        message.appendChild(name);
        message.appendChild(document.createTextNode(' ' + (event.summary || '')));
        entry.appendChild(message);
        container.appendChild(entry);
      });
    }

    function svgNode(tagName, attributes) {
      const element = document.createElementNS(SVG_NS, tagName);
      Object.entries(attributes || {}).forEach(([name, value]) => {
        element.setAttribute(name, String(value));
      });
      return element;
    }

    function svgText(attributes, text) {
      const element = svgNode('text', { 'pointer-events': 'none', ...attributes });
      setText(element, text);
      return element;
    }

    function buildTopology(data, viewportWidth) {
      const local = (data.sessions || []).filter((session) => !session.host_id || session.host_id === 'local');
      const live = local.filter((session) => session.alive === true && session.desired !== 'closed' && session.observed !== 'stopped');
      const rank = (session) => session.waiting_question || session.observed === 'waiting_input' ? 0 : (session.observed === 'running' ? 1 : 2);
      live.sort((a, b) => rank(a) - rank(b));
      const hostNodes = [{ host_id: 'local', isCoord: true, data: data.coordinator || {}, sessions: live }];
      (data.hosts || []).filter((host) => host.host_id !== 'local').forEach((host) => {
        hostNodes.push({ host_id: host.host_id, isCoord: false, data: host, sessions: [] });
      });
      const columns = hostNodes.map((host) => Math.max(180, host.sessions.length * 44 + 28));
      const minimumWidth = columns.reduce((sum, width) => sum + width, 40);
      const width = Math.max(viewportWidth || 800, minimumWidth);
      const extra = (width - minimumWidth) / hostNodes.length;
      let left = 20;
      hostNodes.forEach((host, index) => {
        const columnWidth = columns[index] + extra;
        host.x = left + columnWidth / 2;
        host.y = 80;
        host.sessions = host.sessions.map((session, sessionIndex) => ({
          session: session, x: host.x + (sessionIndex - (host.sessions.length - 1) / 2) * 44, y: 170
        }));
        left += columnWidth;
      });
      return {
        width: width, height: 240, hostNodes: hostNodes, liveCount: live.length,
        recoveryCount: local.filter((session) => session.observed === 'recovery_needed').length
      };
    }

    function renderTopology(data) {
      const svg = document.getElementById('topoGraph');
      const layout = buildTopology(data, svg.parentElement.clientWidth || 800);
      const root = layout.hostNodes[0];
      svg.replaceChildren();
      svg.setAttribute('viewBox', '0 0 ' + layout.width + ' ' + layout.height);
      svg.style.width = layout.width + 'px';
      document.getElementById('topoStats').textContent = layout.liveCount + ' live local' +
        (layout.recoveryCount ? ' · ' + layout.recoveryCount + ' need recovery' : '');

      layout.hostNodes.slice(1).forEach((host) => {
        const online = host.data.state === 'online';
        svg.appendChild(svgNode('path', {
          d: 'M ' + root.x + ' 52 V 20 H ' + host.x + ' V 54',
          fill: 'none', stroke: online ? '#3fb950' : '#8b949e',
          'stroke-dasharray': '5 3', 'stroke-width': 2, opacity: 0.8,
          'data-link-host': host.host_id
        }));
        svg.appendChild(svgText({
          x: host.x, y: 12, 'text-anchor': 'middle', fill: '#8b949e', 'font-size': 9
        }, 'SSH'));
      });

      layout.hostNodes.forEach((host) => {
        const state = host.isCoord ? 'local' : (host.data.state || 'unknown');
        const stateLabel = { online: 'ONLINE', offline: 'OFFLINE', identity_changed: 'ID CHANGED', state_corrupted: 'CORRUPT' }[state] || 'UNKNOWN';
        const color = host.isCoord ? '#bc8cff' : (state === 'online' ? '#3fb950' : (state === 'offline' ? '#f85149' : '#d29922'));
        const circle = svgNode('circle', {
          cx: host.x, cy: host.y, r: host.isCoord ? 28 : 26,
          fill: '#161b22', stroke: color, 'stroke-width': 2.2,
          'data-host-id': host.host_id
        });
        const hostName = String(host.isCoord ? (host.data.host_id || 'local') : host.host_id);
        const title = svgNode('title');
        setText(title, hostName + ' · ' + state);
        circle.appendChild(title);
        svg.appendChild(circle);
        svg.appendChild(svgText({
          x: host.x, y: host.y - 4, 'text-anchor': 'middle', fill: '#fff', 'font-size': 10, 'font-weight': 600
        }, host.isCoord ? 'COORD' : hostName.substring(0, 9)));
        svg.appendChild(svgText({
          x: host.x, y: host.y + 10, 'text-anchor': 'middle', fill: color, 'font-size': 8
        }, host.isCoord ? hostName.substring(0, 10) : stateLabel));

        if (!host.sessions.length) {
          svg.appendChild(svgText({
            x: host.x, y: 170, 'text-anchor': 'middle', fill: '#8b949e', 'font-size': 10
          }, host.isCoord ? 'No live Bridge workers' : 'Sessions not collected'));
        }
        host.sessions.forEach((node) => {
          const session = node.session;
          const running = session.observed === 'running';
          const waiting = session.waiting_question || ['waiting_input', 'recovery_needed'].includes(session.observed);
          const color = waiting ? '#d29922' : (running ? '#3fb950' : '#8b949e');
          svg.appendChild(svgNode('line', {
            x1: host.x, y1: host.y + 28, x2: node.x, y2: node.y - 18,
            stroke: color, 'stroke-width': 1.2, opacity: 0.6
          }));
          const circle = svgNode('circle', {
            cx: node.x, cy: node.y, r: 18, fill: running ? '#064e3b' : '#111827',
            stroke: color, 'stroke-width': 1.8, 'data-session-id': session.id
          });
          const name = String(session.name || session.runtime || session.id || 'session');
          const title = svgNode('title');
          setText(title, name + ' · ' + String(session.runtime || '') + ' · ' + String(session.observed || 'unknown') + ' · ' + String(session.id || ''));
          circle.appendChild(title);
          svg.appendChild(circle);
          svg.appendChild(svgText({
            x: node.x, y: node.y + 3, 'text-anchor': 'middle', fill: '#fff', 'font-size': 8, 'font-weight': 600
          }, name.length > 7 ? name.substring(0, 6) + '…' : name));
        });
      });
    }

    function renderDashboard(data) {
      const metrics = data.metrics || {};
      const coordinator = data.coordinator || {};

      document.getElementById('coordBadge').textContent =
        (coordinator.host_id || 'localhost') + ' • Node ' +
        (coordinator.node_id ? String(coordinator.node_id).substring(0, 8) : '?');
      document.getElementById('statNodes').textContent =
        (metrics.online_hosts || 1) + '/' + (metrics.total_hosts || 1);
      document.getElementById('statActive').textContent = metrics.active_sessions || 0;
      document.getElementById('statTasks').textContent = metrics.running_tasks || 0;
      document.getElementById('statBroadcasts').textContent = metrics.total_broadcasts || 0;
      document.getElementById('statCost').textContent =
        '$' + Number(metrics.total_cost_usd || 0).toFixed(4);

      renderApprovals(data.pending_questions || []);
      renderHosts(data.hosts || [], coordinator);
      renderSessions(data.sessions || [], metrics);
      renderBroadcasts(data.broadcasts || []);
      renderEvents(data.events || []);
      renderTopology(data);
    }

    async function init() {
      const authenticated = await loadSession();
      if (!authenticated) {
        window.location.href = '/login';
        return;
      }
      await fetchData();
      window.setInterval(fetchData, 2000);
    }

    document.getElementById('refreshBtn').addEventListener('click', fetchData);
    document.getElementById('logoutBtn').addEventListener('click', async () => {
      try {
        await fetch('/auth/logout', { method: 'POST', credentials: 'same-origin' });
      } catch (error) {
        // Redirect regardless; the cookie is cleared server-side when possible.
      }
      window.location.href = '/login';
    });
    init();
  </script>
</body>
</html>
"""
HTML_PAGE = _HTML_HEAD + _HTML_BODY + _HTML_SCRIPT

LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="referrer" content="no-referrer">
  <title>cli-agent-bridge | Sign in</title>
  <style>
    :root { --bg:#0d1117; --card:#161b22; --border:#30363d; --text:#e6edf3; --muted:#8b949e; --accent:#58a6ff; --red:#f85149; }
    * { box-sizing:border-box; margin:0; padding:0; }
    body { background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif; font-size:13px; }
    main { min-height:100vh; display:flex; align-items:center; justify-content:center; padding:24px; }
    .card { width:100%; max-width:380px; background:var(--card); border:1px solid var(--border); border-radius:10px; padding:28px; }
    h1 { font-size:18px; margin-bottom:6px; }
    p { color:var(--muted); margin-bottom:20px; line-height:1.5; }
    label { display:block; font-weight:600; margin-bottom:6px; }
    input { width:100%; padding:10px 12px; border-radius:6px; border:1px solid var(--border); background:#0d1117; color:var(--text); font-size:14px; }
    input:focus { outline:2px solid var(--accent); outline-offset:1px; }
    button { width:100%; margin-top:16px; padding:10px 12px; border:none; border-radius:6px; background:var(--accent); color:#fff; font-size:14px; font-weight:600; cursor:pointer; }
    button:hover { opacity:.9; }
    .error { min-height:20px; margin-top:12px; color:var(--red); }
    .hint { margin-top:18px; color:var(--muted); font-size:11px; line-height:1.5; }
  </style>
</head>
<body>
  <main>
    <form class="card" id="loginForm" autocomplete="on">
      <h1>cli-agent-bridge Monitor</h1>
      <p>Sign in with the local admin token. The token is read from the private state directory and is never stored in the page.</p>
      <label for="token">Admin token</label>
      <input id="token" name="token" type="password" autocomplete="current-password" required>
      <button type="submit">Sign in</button>
      <div class="error" id="loginError" role="alert"></div>
      <div class="hint">Use SSH port forwarding or an HTTPS reverse proxy on untrusted networks.</div>
    </form>
  </main>
  <script>
    'use strict';
    const form = document.getElementById('loginForm');
    const tokenInput = document.getElementById('token');
    const errorBox = document.getElementById('loginError');
    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      errorBox.textContent = '';
      try {
        const response = await fetch('/auth/login', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token: tokenInput.value })
        });
        let payload = {};
        try { payload = await response.json(); } catch (error) { payload = {}; }
        if (response.ok && payload.ok) {
          window.location.href = '/';
          return;
        }
        errorBox.textContent = payload.error || 'Sign in failed';
      } catch (error) {
        errorBox.textContent = 'Sign in failed';
      }
    });
  </script>
</body>
</html>
"""


def _secure_equal(left: str, right: str) -> bool:
    """Constant-time string comparison that also tolerates non-ASCII input."""
    return hmac.compare_digest(
        left.encode("utf-8", "surrogatepass"), right.encode("utf-8", "surrogatepass")
    )


def normalize_host_entry(value: str | None) -> str:
    """Normalize a Host header / allowed-host entry to lowercase host[:port] form."""
    if not value:
        return ""
    text = value.strip().lower()
    if not text:
        return ""
    try:
        parsed = urlsplit("//" + text)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return ""
    if not hostname:
        return ""
    host = f"[{hostname}]" if ":" in hostname else hostname
    return f"{host}:{port}" if port is not None else host


def normalize_allowed_hosts(values: Any) -> set[str]:
    """Accept None, a comma-separated string, or an iterable of host entries."""
    if values is None:
        return set(DEFAULT_ALLOWED_HOSTS)
    if isinstance(values, str):
        values = [values]
    result: set[str] = set()
    for value in values:
        for entry in str(value).split(","):
            normalized = normalize_host_entry(entry)
            if normalized:
                result.add(normalized)
    return result or set(DEFAULT_ALLOWED_HOSTS)


def canonical_origin(value: str) -> str | None:
    """Return a canonical scheme://host[:port] origin, or None if invalid."""
    try:
        parsed = urlsplit(value.strip())
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if scheme not in {"http", "https"} or not hostname:
        return None
    host = f"[{hostname.lower()}]" if ":" in hostname else hostname.lower()
    default_port = 443 if scheme == "https" else 80
    if port is not None and port != default_port:
        return f"{scheme}://{host}:{port}"
    return f"{scheme}://{host}"


def host_allowed(host_header: str | None, allowed_hosts: set[str]) -> bool:
    normalized = normalize_host_entry(host_header)
    return bool(normalized) and normalized in allowed_hosts


def origin_allowed(origin: str | None, allowed_hosts: set[str], public_origin: str | None) -> bool:
    """Validate an Origin header against loopback/allowed hosts and the public origin."""
    if not origin:
        return False
    canonical = canonical_origin(origin)
    if canonical is None:
        return False
    if public_origin:
        expected = canonical_origin(public_origin)
        if expected is not None and canonical == expected:
            return True
    try:
        parsed = urlsplit(origin.strip())
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return False
    if not hostname:
        return False
    host = f"[{hostname.lower()}]" if ":" in hostname else hostname.lower()
    default_port = 443 if scheme == "https" else 80
    netloc = host if port is None or port == default_port else f"{host}:{port}"
    return netloc in allowed_hosts


class WebSessionManager:
    """Thread-safe in-memory web sessions bound to a hash of the current admin token."""

    def __init__(self, ttl: float = SESSION_TTL_SECONDS) -> None:
        self.ttl = ttl
        self._sessions: dict[str, dict[str, Any]] = {}
        self._token_hash: str | None = None
        self._lock = threading.RLock()

    @staticmethod
    def token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def sync_token(self, current_token: str | None) -> None:
        """Rotate state: clear all sessions when the token changes or becomes unavailable."""
        with self._lock:
            if not current_token:
                self._sessions.clear()
                self._token_hash = None
                return
            digest = self.token_hash(current_token)
            if self._token_hash is not None and not hmac.compare_digest(self._token_hash, digest):
                self._sessions.clear()
            self._token_hash = digest
            now = time.time()
            expired = [key for key, value in self._sessions.items() if value["expires_at"] <= now]
            for key in expired:
                self._sessions.pop(key, None)

    def create_session(self, token: str) -> tuple[str, dict[str, Any]]:
        session_id = secrets.token_urlsafe(32)
        record = {
            "csrf_token": secrets.token_urlsafe(32),
            "token_hash": self.token_hash(token),
            "expires_at": time.time() + self.ttl,
        }
        with self._lock:
            self._token_hash = record["token_hash"]
            self._sessions[session_id] = record
        return session_id, dict(record)

    def validate_session(self, session_id: str, current_token: str) -> dict[str, Any] | None:
        digest = self.token_hash(current_token)
        now = time.time()
        with self._lock:
            record = self._sessions.get(session_id)
            if record is None:
                return None
            if not hmac.compare_digest(record["token_hash"], digest):
                self._sessions.pop(session_id, None)
                return None
            if record["expires_at"] <= now:
                self._sessions.pop(session_id, None)
                return None
            return dict(record)

    def revoke_session(self, session_id: str | None) -> None:
        if not session_id:
            return
        with self._lock:
            self._sessions.pop(session_id, None)

    def revoke_all(self) -> None:
        with self._lock:
            self._sessions.clear()
            self._token_hash = None


class _HTTPError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class _BrokerUnavailable(Exception):
    pass


class _BrokerProtocolError(Exception):
    pass


class WebMonitorHandler(BaseHTTPRequestHandler):
    collector: MonitorCollector | None = None
    state: Path | None = None
    allowed_hosts: set[str] = set(DEFAULT_ALLOWED_HOSTS)
    public_origin: str | None = None
    sessions: WebSessionManager = WebSessionManager()
    server_version = "AgentBridgeMonitor/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Never log headers, cookies or bodies.

    def send_response(self, code: int, message: str | None = None) -> None:
        super().send_response(code, message)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", CONTENT_SECURITY_POLICY)

    # ------------------------------------------------------------------ response helpers

    def _send_bytes(
        self,
        status: int,
        content_type: str,
        body: bytes,
        extra_headers: list[tuple[str, str]] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for name, value in extra_headers or []:
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_json(
        self,
        status: int,
        payload: dict[str, Any],
        extra_headers: list[tuple[str, str]] | None = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send_bytes(status, "application/json; charset=utf-8", body, extra_headers)

    def _send_html(self, status: int, page: str) -> None:
        self._send_bytes(status, "text/html; charset=utf-8", page.encode("utf-8"))

    def _redirect(self, location: str) -> None:
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ------------------------------------------------------------------ request validation

    def _check_host(self) -> None:
        if not host_allowed(self.headers.get("Host"), self.allowed_hosts):
            raise _HTTPError(400, "Invalid Host header")

    def _check_optional_origin(self) -> None:
        origin = self.headers.get("Origin")
        if origin is not None and not origin_allowed(origin, self.allowed_hosts, self.public_origin):
            raise _HTTPError(403, "Origin is not allowed")

    def _check_cookie_origin(self) -> None:
        origin = self.headers.get("Origin")
        if origin is None:
            raise _HTTPError(403, "Origin header is required")
        if not origin_allowed(origin, self.allowed_hosts, self.public_origin):
            raise _HTTPError(403, "Origin is not allowed")

    def _cookie_session_id(self) -> str | None:
        raw = self.headers.get("Cookie")
        if not raw:
            return None
        cookie = SimpleCookie()
        try:
            cookie.load(raw)
        except CookieError:
            return None
        morsel = cookie.get(SESSION_COOKIE)
        return morsel.value if morsel is not None else None

    def _current_cookie_secure(self) -> bool:
        return bool(self.public_origin) and self.public_origin.lower().startswith("https://")

    def _load_token(self) -> str | None:
        if self.state is None:
            self.sessions.revoke_all()
            return None
        try:
            token = read_admin_token(self.state)
        except (BridgeError, ValueError, OSError):
            logger.debug("Web monitor request rejected: admin token unavailable or insecure")
            self.sessions.revoke_all()
            return None
        self.sessions.sync_token(token)
        return token

    def _identity(self, token: str) -> tuple[str, str | None, dict[str, Any] | None] | None:
        authorization = self.headers.get("Authorization", "")
        if authorization:
            scheme, _, value = authorization.partition(" ")
            if scheme.lower() == "bearer":
                candidate = value.strip()
                if candidate and _secure_equal(candidate, token):
                    return "bearer", None, None
                return None
        session_id = self._cookie_session_id()
        if session_id:
            record = self.sessions.validate_session(session_id, token)
            if record is not None:
                return "cookie", session_id, record
        return None

    def _declared_body_length(self) -> int:
        if self.headers.get("Transfer-Encoding"):
            raise _HTTPError(400, "Transfer-Encoding is not supported")
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            return 0
        try:
            length = int(raw_length)
        except (TypeError, ValueError):
            raise _HTTPError(400, "Invalid Content-Length") from None
        if length < 0:
            raise _HTTPError(400, "Invalid Content-Length")
        if length > MAX_BODY_BYTES:
            raise _HTTPError(413, "Request body too large")
        return length

    def _read_json_body(self) -> dict[str, Any]:
        length = self._declared_body_length()
        body = self.rfile.read(length) if length else b""
        if len(body) != length:
            raise _HTTPError(400, "Incomplete request body")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise _HTTPError(400, "Invalid JSON body") from None
        if not isinstance(payload, dict):
            raise _HTTPError(400, "JSON body must be an object")
        return payload

    # ------------------------------------------------------------------ endpoints

    def do_GET(self) -> None:
        try:
            self._check_host()
            path = urlsplit(self.path).path
            if path == "/login":
                self._send_html(200, LOGIN_PAGE)
                return
            token = self._load_token()
            identity = self._identity(token) if token is not None else None
            if path in ("/", "/index.html"):
                if identity is None:
                    self._redirect("/login")
                    return
                self._send_html(200, HTML_PAGE)
            elif path == "/auth/session":
                if identity is None:
                    raise _HTTPError(401, "Unauthorized")
                kind, _, record = identity
                if kind != "cookie" or record is None:
                    raise _HTTPError(401, "A web session cookie is required")
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "csrf_token": record["csrf_token"],
                        "expires_at": record["expires_at"],
                    },
                )
            elif path == "/api/snapshot":
                if identity is None:
                    raise _HTTPError(401, "Unauthorized")
                if self.collector is None:
                    raise _HTTPError(500, "Collector not configured")
                self._send_json(200, self.collector.collect(limit=25))
            else:
                raise _HTTPError(404, "Not Found")
        except _HTTPError as error:
            self._send_json(error.status, {"ok": False, "error": str(error)})

    def do_POST(self) -> None:
        try:
            self._check_host()
            # Reject oversized or chunked bodies before authentication or body parsing.
            self._declared_body_length()
            path = urlsplit(self.path).path
            token = self._load_token()
            if path == "/auth/login":
                if token is None:
                    raise _HTTPError(503, "Admin token unavailable")
                self._handle_login(token)
            elif path == "/auth/logout":
                if token is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_logout(token)
            elif path == "/api/respond":
                if token is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_respond(token)
            elif path == "/api/host/ping":
                if token is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_host_ping(token)
            elif path in ("/", "/index.html", "/auth/session", "/api/snapshot"):
                raise _HTTPError(405, "Method Not Allowed")
            else:
                raise _HTTPError(404, "Not Found")
        except _HTTPError as error:
            self._send_json(error.status, {"ok": False, "error": str(error)})

    def _handle_login(self, token: str) -> None:
        origin = self.headers.get("Origin")
        if origin is not None and not origin_allowed(origin, self.allowed_hosts, self.public_origin):
            raise _HTTPError(403, "Origin is not allowed")
        payload = self._read_json_body()
        supplied = payload.get("token")
        if not isinstance(supplied, str) or not supplied or not _secure_equal(supplied, token):
            raise _HTTPError(401, "Invalid token")
        session_id, record = self.sessions.create_session(token)
        cookie = f"{SESSION_COOKIE}={session_id}; HttpOnly; SameSite=Strict; Path=/"
        if self._current_cookie_secure():
            cookie += "; Secure"
        self._send_json(
            200,
            {"ok": True, "csrf_token": record["csrf_token"], "expires_at": record["expires_at"]},
            [("Set-Cookie", cookie)],
        )

    def _handle_logout(self, token: str) -> None:
        identity = self._identity(token)
        if identity is None:
            raise _HTTPError(401, "Unauthorized")
        kind, session_id, _ = identity
        if kind == "cookie":
            self._check_cookie_origin()
            self.sessions.revoke_session(session_id)
        else:
            self._check_optional_origin()
        cookie = f"{SESSION_COOKIE}=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/"
        if self._current_cookie_secure():
            cookie += "; Secure"
        self._send_json(200, {"ok": True}, [("Set-Cookie", cookie)])

    def _handle_respond(self, token: str) -> None:
        identity = self._identity(token)
        if identity is None:
            raise _HTTPError(401, "Unauthorized")
        kind, _, record = identity
        if kind == "cookie":
            self._check_cookie_origin()
            csrf = self.headers.get("X-CSRF-Token")
            if not csrf or record is None or not _secure_equal(csrf, record["csrf_token"]):
                raise _HTTPError(403, "Invalid CSRF token")
        else:
            self._check_optional_origin()

        payload = self._read_json_body()
        session_id = payload.get("session_id")
        question_id = payload.get("question_id")
        decision = payload.get("decision")
        expected_revision = payload.get("expected_revision")
        request_id = payload.get("request_id")
        if not isinstance(session_id, str) or not session_id.strip():
            raise _HTTPError(400, "session_id is required")
        if not isinstance(question_id, str) or not question_id.strip():
            raise _HTTPError(400, "question_id is required")
        if not isinstance(decision, str) or not decision:
            raise _HTTPError(400, "decision is required")
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
            raise _HTTPError(400, "expected_revision must be a nonnegative integer")
        if not isinstance(request_id, str) or not request_id or len(request_id) > 200:
            raise _HTTPError(400, "request_id of at most 200 characters is required")

        broker_args = {
            "token": token,
            "session_id": session_id,
            "action": "respond",
            "question_id": question_id,
            "answer": {"decision": decision},
            "expected_revision": expected_revision,
            "request_id": request_id,
        }
        try:
            reply = self._broker_call(token, broker_args)
        except _BrokerUnavailable:
            self._send_json(503, {"ok": False, "error": "Broker unavailable"})
            return
        except _BrokerProtocolError:
            self._send_json(502, {"ok": False, "error": "Broker response invalid"})
            return

        if "error" in reply:
            error = reply["error"]
            if isinstance(error, dict):
                code = str(error.get("code", "broker_error"))
                message = safe_error(error.get("message", error))
            else:
                code = "broker_error"
                message = safe_error(error)
            raise _HTTPError(self._http_status_for_broker_error(code), message)
        result = reply.get("result", reply)
        self._send_json(200, {"ok": True, "result": result})

    def _handle_host_ping(self, token: str) -> None:
        identity = self._identity(token)
        if identity is None:
            raise _HTTPError(401, "Unauthorized")
        kind, _, record = identity
        if kind == "cookie":
            self._check_cookie_origin()
            csrf = self.headers.get("X-CSRF-Token")
            if not csrf or record is None or not _secure_equal(csrf, record["csrf_token"]):
                raise _HTTPError(403, "Invalid CSRF token")
        else:
            self._check_optional_origin()

        payload = self._read_json_body()
        host_id = payload.get("host_id")
        if not isinstance(host_id, str) or not host_id.strip():
            raise _HTTPError(400, "host_id is required")

        try:
            reply = self._broker_call(token, "agent_catalog", {"host_id": host_id, "refresh": True})
            self._send_json(200, {"ok": True, "result": reply})
        except _BrokerUnavailable:
            self._send_json(200, {"ok": False, "error": "Broker unavailable"})
        except Exception as error:
            self._send_json(200, {"ok": False, "error": safe_error(error)})

    # ------------------------------------------------------------------ broker transport

    def _broker_call(
        self,
        token: str,
        method_or_args: str | dict[str, Any],
        args: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.state is None:
            raise _BrokerUnavailable
        if args is None and isinstance(method_or_args, dict):
            method = "agent_control"
            params = method_or_args
        else:
            method = str(method_or_args)
            params = args or {}
        socket_path = Path(self.state) / "broker.sock"
        message = {
            "version": 1,
            "token": token,
            "method": method,
            "params": params,
            "args": params,
        }
        payload = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(BROKER_TIMEOUT_SECONDS)
                connection.connect(str(socket_path))
                connection.sendall(payload)
                received = bytearray()
                while b"\n" not in received:
                    chunk = connection.recv(65536)
                    if not chunk:
                        break
                    received.extend(chunk)
                    if len(received) > MAX_BROKER_RESPONSE_BYTES:
                        raise _BrokerProtocolError("Broker response is too large")
        except _BrokerProtocolError:
            raise
        except OSError as error:
            raise _BrokerUnavailable from error
        if not received:
            raise _BrokerUnavailable
        raw = bytes(received).split(b"\n", 1)[0]
        try:
            reply = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise _BrokerProtocolError("Broker response is not valid JSON") from error
        if not isinstance(reply, dict):
            raise _BrokerProtocolError("Broker response is not an object")
        return reply

    @staticmethod
    def _http_status_for_broker_error(code: str) -> int:
        if code in {
            "stale_revision",
            "revision_conflict",
            "request_conflict",
            "expired_question",
            "request_uncertain",
        }:
            return 409
        if code == "forbidden":
            return 403
        if code.startswith("invalid_"):
            return 400
        if code in {"internal_error", "unknown_method"}:
            return 500
        return 400

    # ------------------------------------------------------------------ unsupported methods

    def _method_not_allowed(self) -> None:
        try:
            self._check_host()
        except _HTTPError as error:
            self._send_json(error.status, {"ok": False, "error": str(error)})
            return
        self._send_json(405, {"ok": False, "error": "Method Not Allowed"})

    do_HEAD = _method_not_allowed
    do_PUT = _method_not_allowed
    do_DELETE = _method_not_allowed
    do_PATCH = _method_not_allowed
    do_OPTIONS = _method_not_allowed


def run_web_monitor(
    state: Path | str | None = None,
    host: str = "127.0.0.1",
    port: int = 8080,
    allowed_hosts: Any = None,
    public_origin: str | None = None,
) -> None:
    state_dir = state_directory(state)
    # Refuse to start without a secure token file; the Broker owns token creation.
    read_admin_token(state_dir)
    if public_origin is not None:
        canonical = canonical_origin(public_origin)
        if canonical is None:
            raise ValueError(
                "public_origin must be an http(s) origin such as https://monitor.example.com"
            )
        public_origin = canonical
    hosts = normalize_allowed_hosts(allowed_hosts)

    WebMonitorHandler.collector = MonitorCollector(state_dir)
    WebMonitorHandler.state = state_dir
    WebMonitorHandler.allowed_hosts = hosts
    WebMonitorHandler.public_origin = public_origin
    WebMonitorHandler.sessions = WebSessionManager()

    server = ThreadingHTTPServer((host, port), WebMonitorHandler)
    url = f"http://{host}:{port}"
    print(f"\033[36m⚡ cli-agent-bridge Web Monitor running at:\033[0m \033[1m\033[32m{url}\033[0m")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        print(
            "\033[33mAuthentication is required. Configure --allowed-hosts with the browser-visible "
            "host:port, and use SSH forwarding or an HTTPS reverse proxy on untrusted networks.\033[0m"
        )
    print(f"\033[2mState directory: {state_dir} | Press Ctrl+C to stop\033[0m")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\033[2mWeb monitor stopped.\033[0m")
    finally:
        server.server_close()
