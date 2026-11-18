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
from urllib.parse import parse_qs, urlsplit

from .config import MAX_TOKEN_BYTES, read_admin_token, safe_error, state_directory  # noqa: F401 (re-exported)
from .errors import BridgeError
from .i18n import get_catalog
from .monitor import MonitorCollector
from .native_monitor import (
    MONITORED_RUNTIMES,
    collect_native_sessions,
    display_runtime,
    filter_snapshot_dict,
    is_event_window_truncated,
    matches_bridge_status,
    normalize_runtime,
    normalize_status_filter,
    parse_runtime_filter,
    should_widen_events,
    summarize_runtimes,
    unified_sessions,
)

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 8 * 3600
MAX_BODY_BYTES = 64 * 1024
MAX_BROKER_RESPONSE_BYTES = 16 * 1024 * 1024
BROKER_TIMEOUT_SECONDS = 30.0
SESSION_COOKIE = "bridge_session"
MAX_SSE_CLIENTS = 16
SSE_KEEPALIVE_SECONDS = 15.0
SSE_MAX_LIFETIME_SECONDS = 1800.0
SSE_SOCKET_TIMEOUT_SECONDS = 10.0
_SSE_CLIENTS_LOCK = threading.Lock()
_ACTIVE_SSE_CLIENTS = 0
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


def _script_json(value: Any) -> str:
    """Serialize trusted catalog data for a script element without terminating it early."""
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


_WEB_I18N_SCRIPT = """    const WEB_I18N_CATALOG = __WEB_I18N_CATALOG__;
    const WEB_LANGUAGE_STORAGE_KEY = 'cli-agent-bridge.web-language';
    let webLanguageChoice = 'auto';
    let webLanguage = 'en';

    function normalizeUiLanguage(value) {
      if (typeof value !== 'string') return null;
      const normalized = value.trim().split('.', 1)[0].split('@', 1)[0].replace(/_/g, '-').toLowerCase();
      if (normalized === 'auto') return 'auto';
      if (normalized === 'c' || normalized === 'posix' || normalized === 'en' || normalized.startsWith('en-')) {
        return 'en';
      }
      if (normalized === 'zh' || normalized === 'zh-cn' || normalized === 'zh-sg' ||
          normalized === 'zh-hans' || normalized.startsWith('zh-hans-')) {
        return 'zh-CN';
      }
      return null;
    }

    function readBrowserLanguagePreferences() {
      if (typeof navigator === 'undefined') return [];
      const preferences = Array.isArray(navigator.languages) ? navigator.languages.slice() : [];
      if (typeof navigator.language === 'string' && navigator.language) preferences.push(navigator.language);
      return preferences;
    }

    function resolveUiLanguage(choice) {
      const normalized = normalizeUiLanguage(choice);
      if (normalized === 'en' || normalized === 'zh-CN') return normalized;
      if (normalized !== 'auto') return 'en';
      // Auto follows the browser preference; unsupported browser locales use English.
      for (const preference of readBrowserLanguagePreferences()) {
        const language = normalizeUiLanguage(preference);
        if (language === 'en' || language === 'zh-CN') return language;
      }
      return 'en';
    }

    function readStoredUiLanguage() {
      try {
        if (typeof window === 'undefined' || !window.localStorage) return 'auto';
        return normalizeUiLanguage(window.localStorage.getItem(WEB_LANGUAGE_STORAGE_KEY)) || 'auto';
      } catch (error) {
        return 'auto';
      }
    }

    function storeUiLanguage(choice) {
      try {
        if (typeof window !== 'undefined' && window.localStorage) {
          window.localStorage.setItem(WEB_LANGUAGE_STORAGE_KEY, choice);
        }
      } catch (error) {
        // Storage can be unavailable in hardened or private browser contexts; keep the in-memory choice.
      }
    }

    function interpolateUi(message, values) {
      if (!values) return message;
      return message.replace(/\\{([a-zA-Z_][a-zA-Z0-9_]*)\\}/g, (match, key) => (
        Object.prototype.hasOwnProperty.call(values, key) ? String(values[key]) : match
      ));
    }

    function translateUi(message, values) {
      const key = message === null || message === undefined ? '' : String(message);
      const template = webLanguage === 'zh-CN' && Object.prototype.hasOwnProperty.call(WEB_I18N_CATALOG, key)
        ? WEB_I18N_CATALOG[key]
        : key;
      return interpolateUi(template, values);
    }

    function applyUiTranslations() {
      if (document.documentElement) {
        document.documentElement.lang = webLanguage;
        document.documentElement.setAttribute('data-language', webLanguage);
      }
      document.querySelectorAll('[data-i18n]').forEach((element) => {
        const key = element.getAttribute('data-i18n');
        if (key) element.textContent = translateUi(key);
      });
      document.querySelectorAll('[data-i18n-aria-label]').forEach((element) => {
        const key = element.getAttribute('data-i18n-aria-label');
        if (key) element.setAttribute('aria-label', translateUi(key));
      });
      document.querySelectorAll('[data-i18n-title]').forEach((element) => {
        const key = element.getAttribute('data-i18n-title');
        if (key) element.setAttribute('title', translateUi(key));
      });
      document.querySelectorAll('[data-i18n-placeholder]').forEach((element) => {
        const key = element.getAttribute('data-i18n-placeholder');
        if (key) element.setAttribute('placeholder', translateUi(key));
      });
    }

    function setUiLanguage(choice, persist) {
      webLanguageChoice = normalizeUiLanguage(choice) || 'en';
      webLanguage = resolveUiLanguage(webLanguageChoice);
      if (persist) storeUiLanguage(webLanguageChoice);
      const select = document.getElementById('languageSelect');
      if (select) select.value = webLanguageChoice;
      applyUiTranslations();
      if (typeof onUiLanguageChanged === 'function') onUiLanguageChanged();
    }

    function initializeUiLanguage() {
      const select = document.getElementById('languageSelect');
      if (select && !select.dataset.i18nBound) {
        select.dataset.i18nBound = 'true';
        select.addEventListener('change', () => setUiLanguage(select.value, true));
      }
      setUiLanguage(readStoredUiLanguage(), false);
    }

"""
_WEB_I18N_SCRIPT = _WEB_I18N_SCRIPT.replace(
    "__WEB_I18N_CATALOG__", _script_json(get_catalog("zh-CN"))
)

_HTML_HEAD = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="referrer" content="no-referrer">
  <title data-i18n="cli-agent-bridge | Multi-Agent Monitor">cli-agent-bridge | Multi-Agent Monitor</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --surface: #ffffff;
      --surface-muted: #f0f2f5;
      --surface-hover: #f6f8fa;
      --border: #d8dee4;
      --border-strong: #c7d0d9;
      --text: #1f2328;
      --text-muted: #59636e;
      --text-subtle: #6e7781;
      --accent: #0969da;
      --accent-soft: #ddf4ff;
      --green: #1a7f37;
      --green-soft: #dafbe1;
      --yellow: #8a5a00;
      --yellow-soft: #fff8c5;
      --red: #cf222e;
      --red-soft: #ffebe9;
      --purple: #8250df;
      --purple-soft: #fbefff;
      --topo-bg: #0d1117;
      --shadow: 0 1px 2px rgba(31, 35, 40, 0.05);
      --radius: 10px;
    }
    @media (prefers-color-scheme: dark) {
      :root {
        color-scheme: dark;
        --bg: #0d1117;
        --surface: #161b22;
        --surface-muted: #1c2128;
        --surface-hover: #1f2630;
        --border: #30363d;
        --border-strong: #3d444d;
        --text: #e6edf3;
        --text-muted: #9ba7b4;
        --text-subtle: #8b949e;
        --accent: #58a6ff;
        --accent-soft: rgba(56, 139, 253, 0.16);
        --green: #3fb950;
        --green-soft: rgba(63, 185, 80, 0.16);
        --yellow: #d29922;
        --yellow-soft: rgba(210, 153, 34, 0.16);
        --red: #f85149;
        --red-soft: rgba(248, 81, 73, 0.16);
        --purple: #bc8cff;
        --purple-soft: rgba(188, 140, 255, 0.16);
        --topo-bg: #090d13;
        --shadow: 0 1px 2px rgba(0, 0, 0, 0.32);
      }
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html { min-height: 100%; }
    body {
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      font-size: 14px;
      line-height: 1.5;
      text-rendering: optimizeLegibility;
      -webkit-font-smoothing: antialiased;
    }
    button, input { font: inherit; }
    button { touch-action: manipulation; }
    a { color: var(--accent); }
    .skip-link {
      position: absolute;
      left: 12px;
      top: -60px;
      z-index: 1000;
      padding: 10px 14px;
      border-radius: 8px;
      background: var(--surface);
      color: var(--text);
      border: 1px solid var(--accent);
      text-decoration: none;
      font-weight: 600;
    }
    .skip-link:focus { top: 12px; }
    :focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
    }
    .sr-only {
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }
    [hidden] { display: none !important; }

    /* Header */
    .app-header {
      position: sticky;
      top: 0;
      z-index: 50;
      background: var(--surface);
      border-bottom: 1px solid var(--border);
    }
    .header-inner {
      width: 100%;
      max-width: 1440px;
      margin: 0 auto;
      padding: 12px 24px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      flex-wrap: wrap;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
      flex: 1 1 340px;
    }
    .brand .logo {
      width: 32px;
      height: 32px;
      flex: 0 0 auto;
      background: var(--accent);
      border-radius: 8px;
      display: grid;
      place-items: center;
      font-size: 13px;
      font-weight: 800;
      color: #fff;
    }
    .brand-copy {
      display: flex;
      flex-direction: column;
      min-width: 0;
      line-height: 1.25;
    }
    .brand-name {
      color: var(--text);
      font-size: 15px;
      font-weight: 700;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .brand-sub {
      color: var(--text-muted);
      font-size: 11px;
    }
    .coord-badge {
      color: var(--text-muted);
      background: var(--surface-muted);
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 3px 9px;
      font-size: 11px;
      font-weight: 500;
      max-width: 280px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .header-actions {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
    }
    .connection-status {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      min-height: 32px;
      color: var(--text-muted);
      font-size: 12px;
      font-weight: 600;
      white-space: nowrap;
    }
    .connection-status::before {
      content: "";
      width: 7px;
      height: 7px;
      flex: 0 0 auto;
      border-radius: 50%;
      background: var(--text-subtle);
    }
    .connection-status[data-state="live"]::before { background: var(--green); }
    .connection-status[data-state="stale"]::before { background: var(--yellow); }
    .connection-status[data-state="error"]::before { background: var(--red); }
    .connection-status[data-state="live"] { color: var(--green); }
    .connection-status[data-state="stale"] { color: var(--yellow); }
    .connection-status[data-state="error"] { color: var(--red); }

    /* Buttons */
    .btn {
      min-height: 36px;
      padding: 0 14px;
      border: 1px solid transparent;
      border-radius: 8px;
      background: var(--accent);
      color: #fff;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      transition: background-color 0.15s, border-color 0.15s, color 0.15s;
    }
    .btn:hover { filter: brightness(1.06); }
    .btn:disabled { opacity: 0.55; cursor: not-allowed; filter: none; }
    .btn-secondary {
      background: var(--surface);
      color: var(--text);
      border-color: var(--border-strong);
    }
    .btn-secondary:hover { background: var(--surface-muted); filter: none; }
    .btn-danger {
      background: transparent;
      color: var(--red);
      border-color: var(--border-strong);
    }
    .btn-danger:hover { background: var(--red-soft); border-color: var(--red); filter: none; }
    .btn-sm { min-height: 32px; padding: 0 10px; font-size: 12px; }

    /* Layout */
    main {
      width: 100%;
      max-width: 1440px;
      margin: 0 auto;
      padding: 24px;
      display: flex;
      flex-direction: column;
      gap: 20px;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
    }
    .stat-card {
      min-width: 0;
      padding: 13px 15px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }
    .stat-label {
      display: block;
      margin-bottom: 3px;
      color: var(--text-muted);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .stat-value {
      display: block;
      color: var(--text);
      font-size: 22px;
      font-weight: 700;
      line-height: 1.2;
      font-variant-numeric: tabular-nums;
      overflow-wrap: anywhere;
    }
    .stat-value.is-unknown { color: var(--yellow); font-size: 16px; line-height: 1.65; }

    /* Filters and runtime cards */
    .toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      flex-wrap: wrap;
    }
    .filter-group { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; min-width: 0; }
    .filter-label { color: var(--text-muted); font-size: 12px; font-weight: 700; }
    .filter-chips { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
    .language-group { margin-left: auto; }
    .language-select {
      min-height: 32px;
      padding: 0 10px;
      border: 1px solid var(--border);
      border-radius: 999px;
      background: var(--surface);
      color: var(--text);
      font: inherit;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    .language-select:hover { background: var(--surface-muted); border-color: var(--border-strong); }
    .language-select option { background: var(--surface); color: var(--text); }
    .chip-btn {
      min-height: 32px;
      padding: 0 12px;
      border: 1px solid var(--border);
      border-radius: 999px;
      background: var(--surface);
      color: var(--text-muted);
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      transition: background-color 0.15s, border-color 0.15s, color 0.15s;
    }
    .chip-btn:hover { background: var(--surface-muted); color: var(--text); }
    .chip-btn.active {
      background: var(--accent-soft);
      border-color: var(--accent);
      color: var(--accent);
    }
    .runtime-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .runtime-card {
      min-width: 0;
      min-height: 98px;
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: var(--surface);
      color: var(--text);
      box-shadow: var(--shadow);
      cursor: pointer;
      text-align: left;
      display: flex;
      flex-direction: column;
      gap: 5px;
      transition: background-color 0.15s, border-color 0.15s;
    }
    .runtime-card:hover { background: var(--surface-hover); border-color: var(--border-strong); }
    .runtime-card.selected { border-color: var(--accent); background: var(--accent-soft); }
    .runtime-card-top { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }
    .runtime-title { font-weight: 700; font-size: 14px; color: var(--text); overflow-wrap: anywhere; }
    .runtime-live { flex: 0 0 auto; font-size: 12px; font-weight: 700; color: var(--text-muted); font-variant-numeric: tabular-nums; }
    .runtime-live.has-live { color: var(--green); }
    .runtime-meta {
      color: var(--text-muted);
      font-size: 11px;
      display: flex;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }

    /* Cards and tables */
    .card {
      min-width: 0;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .card-header {
      padding: 14px 16px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
      flex-wrap: wrap;
    }
    .card-heading { min-width: 0; }
    .card-title {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--text);
      font-size: 15px;
      font-weight: 700;
      line-height: 1.35;
    }
    .card-subtitle {
      margin-top: 3px;
      color: var(--text-muted);
      font-size: 12px;
      max-width: 820px;
    }
    .card-meta {
      flex: 0 0 auto;
      color: var(--text-muted);
      font-size: 12px;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .card-body { padding: 16px; }
    .grid-2 {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 20px;
    }

    .table-frame {
      width: 100%;
      max-height: 380px;
      overflow: auto;
      overscroll-behavior: contain;
      -webkit-overflow-scrolling: touch;
    }
    .table-frame:focus-visible,
    .log-container:focus-visible { outline: 2px solid var(--accent); outline-offset: -3px; }
    .table-frame--hosts { max-height: 340px; }
    .table-frame--sessions,
    .table-frame--broadcasts { max-height: 420px; }
    .table-frame--native { max-height: 360px; }
    table { width: 100%; border-collapse: separate; border-spacing: 0; text-align: left; }
    .data-table { font-size: 13px; }
    .data-table--hosts { min-width: 1020px; }
    .data-table--sessions { min-width: 760px; }
    .data-table--broadcasts { min-width: 650px; }
    .data-table--native { min-width: 780px; }
    .data-table--native td:nth-child(3) { max-width: 340px; }
    .data-table--native td:nth-child(3) .cell-title { overflow-wrap: anywhere; }
    th {
      position: sticky;
      top: 0;
      z-index: 2;
      padding: 9px 12px;
      background: var(--surface-muted);
      border-bottom: 1px solid var(--border);
      color: var(--text-muted);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    td {
      padding: 11px 12px;
      border-bottom: 1px solid var(--border);
      vertical-align: top;
      overflow-wrap: anywhere;
    }
    tbody tr:last-child td { border-bottom: none; }
    tbody tr:hover td { background: var(--surface-hover); }
    .empty-cell {
      padding: 28px 16px !important;
      color: var(--text-muted);
      text-align: center;
    }
    code {
      padding: 1px 5px;
      border-radius: 4px;
      background: var(--surface-muted);
      color: var(--text);
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .cell-title { color: var(--text); font-weight: 700; font-size: 13px; overflow-wrap: anywhere; }
    .cell-meta { color: var(--text-muted); font-size: 11px; margin-top: 2px; overflow-wrap: anywhere; }
    .cell-goal,
    .cell-kind { max-width: 280px; }
    .cell-goal-text,
    .cell-kind-text {
      display: block;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .cell-cost { color: var(--yellow); font-variant-numeric: tabular-nums; white-space: nowrap; }
    .cell-cost.is-empty { color: var(--text-muted); }
    .cell-muted { color: var(--text-muted); }

    /* Badges */
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      max-width: 100%;
      padding: 3px 8px;
      border: 1px solid transparent;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      line-height: 1.45;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .badge::before {
      content: "";
      width: 6px;
      height: 6px;
      flex: 0 0 auto;
      border-radius: 50%;
      background: currentColor;
      opacity: 0.75;
    }
    .badge-running,
    .badge-worker,
    .badge-online { background: var(--green-soft); color: var(--green); border-color: color-mix(in srgb, var(--green) 35%, transparent); }
    .badge-idle { background: var(--accent-soft); color: var(--accent); }
    .badge-waiting { background: var(--yellow-soft); color: var(--yellow); border-color: color-mix(in srgb, var(--yellow) 45%, transparent); }
    .badge-stopped,
    .badge-unknown { background: var(--surface-muted); color: var(--text-muted); border-color: var(--border); }
    .badge-offline { background: var(--red-soft); color: var(--red); border-color: color-mix(in srgb, var(--red) 35%, transparent); }
    .badge-review { background: var(--purple-soft); color: var(--purple); border-color: color-mix(in srgb, var(--purple) 35%, transparent); }
    .badge-recovery { background: var(--yellow-soft); color: var(--yellow); border-color: color-mix(in srgb, var(--yellow) 45%, transparent); }
    .badge-native { background: var(--purple-soft); color: var(--purple); border-color: color-mix(in srgb, var(--purple) 30%, transparent); }
    .badge-node {
      max-width: 170px;
      padding: 2px 6px;
      border-radius: 5px;
      background: var(--accent-soft);
      color: var(--accent);
      border: 1px solid color-mix(in srgb, var(--accent) 30%, transparent);
      font-size: 10px;
      font-weight: 700;
    }
    .badge-node::before { display: none; }
    .badge-coord { background: var(--purple-soft); color: var(--purple); border-color: color-mix(in srgb, var(--purple) 35%, transparent); }

    /* Alerts and approvals */
    .connection-banner {
      padding: 11px 14px;
      border: 1px solid color-mix(in srgb, var(--yellow) 55%, var(--border));
      border-radius: 8px;
      background: var(--yellow-soft);
      color: var(--text);
      font-size: 13px;
    }
    .approval-section { display: flex; flex-direction: column; gap: 10px; }
    .alert-banner {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 14px;
      flex-wrap: wrap;
      padding: 14px 16px;
      border: 1px solid color-mix(in srgb, var(--yellow) 55%, var(--border));
      border-radius: 8px;
      background: var(--yellow-soft);
    }
    .alert-banner.danger { border-color: var(--red); background: var(--red-soft); }
    .alert-info { flex: 1 1 360px; min-width: 0; }
    .alert-title { color: var(--text); font-weight: 700; margin-bottom: 3px; overflow-wrap: anywhere; }
    .alert-detail { color: var(--text); font-size: 12px; overflow-wrap: anywhere; }
    .alert-detail code { white-space: pre-wrap; background: var(--surface); }
    .alert-actions { display: flex; gap: 8px; flex: 0 0 auto; flex-wrap: wrap; }

    /* Topology */
    .topo-container {
      width: 100%;
      height: 240px;
      background: var(--topo-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      position: relative;
      overflow-x: auto;
      overflow-y: hidden;
      overscroll-behavior-x: contain;
    }
    .topo-container:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
    }
    svg.topo-svg {
      display: block;
      height: 100%;
      min-width: 100%;
    }
    .scope-note {
      margin: 9px 2px 0;
      color: var(--text-muted);
      font-size: 12px;
      line-height: 1.55;
    }
    .status-dot {
      display: inline-block;
      width: 8px;
      height: 8px;
      flex: 0 0 auto;
      border-radius: 50%;
      background: var(--green);
    }

    /* Events */
    .log-container {
      max-height: 280px;
      overflow: auto;
      display: flex;
      flex-direction: column;
      gap: 4px;
      overscroll-behavior: contain;
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
    }
    .log-entry {
      display: grid;
      grid-template-columns: 72px 142px minmax(0, 1fr);
      gap: 10px;
      padding: 6px 8px;
      border-radius: 5px;
      background: var(--surface-muted);
    }
    .log-time { color: var(--text-muted); font-variant-numeric: tabular-nums; }
    .log-kind { color: var(--accent); font-weight: 700; overflow-wrap: anywhere; }
    .log-msg { min-width: 0; color: var(--text); overflow-wrap: anywhere; }
    .empty-log { padding: 10px 2px; color: var(--text-muted); }

    /* Integration reference */
    .api-bar {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 8px 10px;
      padding: 12px 14px;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: var(--surface);
      color: var(--text-muted);
      font-size: 11px;
    }
    .api-bar .api-label { color: var(--text-muted); font-weight: 700; }
    .api-bar code {
      color: var(--accent);
      background: var(--accent-soft);
      overflow-wrap: anywhere;
    }

    @media (max-width: 1100px) {
      .summary-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .runtime-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid-2 { grid-template-columns: minmax(0, 1fr); }
    }
    @media (max-width: 680px) {
      .header-inner { padding: 10px 16px; align-items: flex-start; }
      .brand { flex-basis: 100%; flex-wrap: wrap; }
      .coord-badge { max-width: 100%; }
      .header-actions { width: 100%; justify-content: space-between; }
      .connection-status { order: -1; width: 100%; }
      main { padding: 16px; gap: 14px; }
      .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
      .stat-card { padding: 12px; }
      .stat-value { font-size: 20px; }
      .toolbar { align-items: stretch; flex-direction: column; }
      .filter-group { justify-content: space-between; }
      .filter-chips { width: 100%; }
      .language-group { margin-left: 0; }
      .language-select { flex: 1 1 auto; min-height: 44px; }
      .chip-btn { flex: 1 1 auto; }
      .card-header { padding: 12px 14px; }
      .card-body { padding: 14px; }
      .card-meta { white-space: normal; }
      .table-frame { max-height: 62vh; }
      .btn,
      .btn-sm,
      .chip-btn { min-height: 44px; }
      .alert-actions { width: 100%; }
      .alert-actions .btn { flex: 1 1 0; }
      .log-entry { grid-template-columns: minmax(0, 1fr); gap: 1px; padding: 8px; }
      .log-kind { font-size: 11px; }
    }
    @media (max-width: 380px) {
      .runtime-grid { grid-template-columns: minmax(0, 1fr); }
    }
    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after {
        animation: none !important;
        transition: none !important;
        scroll-behavior: auto !important;
      }
    }
  </style>
</head>
<body>
  <a class="skip-link" href="#main" data-i18n="Skip to dashboard">Skip to dashboard</a>
"""
_HTML_BODY = """  <header class="app-header">
    <div class="header-inner">
      <div class="brand">
        <div class="logo" aria-hidden="true">B</div>
        <div class="brand-copy">
          <span class="brand-name" data-i18n="cli-agent-bridge Monitor">cli-agent-bridge Monitor</span>
          <span class="brand-sub" data-i18n="Operations dashboard">Operations dashboard</span>
        </div>
        <span id="coordBadge" class="coord-badge"></span>
      </div>
      <div class="header-actions">
        <span id="connectionStatus" class="connection-status" data-state="connecting" role="status" aria-live="polite" data-i18n="Connecting…">Connecting&hellip;</span>
        <button class="btn btn-secondary" id="refreshBtn" type="button" data-i18n="Refresh">Refresh</button>
        <button class="btn btn-danger" id="logoutBtn" type="button" data-i18n="Sign out">Sign out</button>
      </div>
    </div>
  </header>

  <main id="main" tabindex="-1">
    <div id="connectionBanner" class="connection-banner" role="status" aria-live="polite" hidden></div>
    <div id="approvalSection" class="approval-section" aria-label="Pending approvals" data-i18n-aria-label="Pending approvals" style="display: none;"></div>

    <section class="summary-grid" aria-label="Snapshot summary" data-i18n-aria-label="Snapshot summary">
      <div class="stat-card">
        <span class="stat-label" data-i18n="Hosts online">Hosts online</span>
        <span class="stat-value" id="statNodes" title="Reachable Bridge execution hosts" data-i18n-title="Reachable Bridge execution hosts">&mdash;</span>
      </div>
      <div class="stat-card">
        <span class="stat-label" data-i18n="Bridge workers">Bridge workers</span>
        <span class="stat-value" id="statActive" title="Bridge worker processes alive or waiting; idle workers are not executing a task" data-i18n-title="Bridge worker processes alive or waiting; idle workers are not executing a task">&mdash;</span>
      </div>
      <div class="stat-card">
        <span class="stat-label" data-i18n="Native processes">Native processes</span>
        <span class="stat-value" id="statNative" title="Detected native CLI processes. Presence does not prove active inference." data-i18n-title="Detected native CLI processes. Presence does not prove active inference.">&mdash;</span>
      </div>
      <div class="stat-card">
        <span class="stat-label" data-i18n="Tasks running">Tasks running</span>
        <span class="stat-value" id="statTasks" title="Bridge tasks currently in the running state" data-i18n-title="Bridge tasks currently in the running state">&mdash;</span>
      </div>
      <div class="stat-card">
        <span class="stat-label" data-i18n="Broadcasts">Broadcasts</span>
        <span class="stat-value" id="statBroadcasts" title="Broadcast communications recorded in the snapshot" data-i18n-title="Broadcast communications recorded in the snapshot">&mdash;</span>
      </div>
      <div class="stat-card">
        <span class="stat-label" data-i18n="Estimated cost">Estimated cost</span>
        <span class="stat-value" id="statCost" title="Reported cost for known attempts" data-i18n-title="Reported cost for known attempts">&mdash;</span>
      </div>
    </section>

    <section class="toolbar" aria-label="Dashboard filters" data-i18n-aria-label="Dashboard filters">
      <div class="filter-group">
        <span class="filter-label" id="runtimeFilterLabel" data-i18n="Runtime">Runtime</span>
        <div class="filter-chips" id="runtimeFilterBar" role="group" aria-labelledby="runtimeFilterLabel">
          <button class="chip-btn active" type="button" data-runtime="all" aria-pressed="true" data-i18n="All runtimes">All runtimes</button>
          <button class="chip-btn" type="button" data-runtime="pi" aria-pressed="false">Pi</button>
          <button class="chip-btn" type="button" data-runtime="claude" aria-pressed="false">Claude</button>
          <button class="chip-btn" type="button" data-runtime="antigravity" aria-pressed="false">AGY</button>
          <button class="chip-btn" type="button" data-runtime="codex" aria-pressed="false">Codex</button>
        </div>
      </div>
      <div class="filter-group">
        <span class="filter-label" id="viewFilterLabel" data-i18n="View">View</span>
        <button class="chip-btn" type="button" id="activeOnlyBtn" aria-pressed="false" title="Hide stopped sessions and daemon processes" data-i18n-title="Hide stopped sessions and daemon processes">Active only</button>
      </div>
      <div class="filter-group language-group">
        <label class="filter-label" for="languageSelect" data-i18n="Language">Language</label>
        <select class="language-select" id="languageSelect" aria-label="Language" data-i18n-aria-label="Language">
          <option value="auto" data-i18n="Auto">Auto</option>
          <option value="en">English</option>
          <option value="zh-CN">简体中文</option>
        </select>
      </div>
    </section>

    <section aria-labelledby="runtimeOverviewTitle">
      <h2 class="sr-only" id="runtimeOverviewTitle" data-i18n="Runtime overview">Runtime overview</h2>
      <div class="runtime-grid" id="runtimeGrid"></div>
    </section>

    <section class="card" aria-labelledby="topologyTitle">
      <div class="card-header">
        <div class="card-heading">
          <h2 class="card-title" id="topologyTitle"><span class="status-dot" aria-hidden="true"></span><span data-i18n="Host Connections &amp; Live Bridge Sessions">Host Connections &amp; Live Bridge Sessions</span></h2>
          <p class="card-subtitle" data-i18n="SSH links and local Bridge workers. A live worker does not necessarily mean a task is executing.">SSH links and local Bridge workers. A live worker does not necessarily mean a task is executing.</p>
        </div>
        <div id="topoStats" class="card-meta"></div>
      </div>
      <div class="card-body">
        <div class="topo-container" tabindex="0" role="group" aria-label="Host connections; scroll horizontally if needed" data-i18n-aria-label="Host connections; scroll horizontally if needed" aria-describedby="topoScope">
          <svg class="topo-svg" id="topoGraph" aria-label="Host connections and live local Bridge sessions" data-i18n-aria-label="Host connections and live local Bridge sessions"></svg>
        </div>
        <p id="topoScope" class="scope-note" data-i18n="Host links show SSH connection status. Sessions shown: local Bridge workers with live processes. Remote/native sessions and message traffic are not collected. A live worker means the process exists; check the session status below for whether a task is executing.">Host links show SSH connection status. Sessions shown: local Bridge workers with live processes. Remote/native sessions and message traffic are not collected. A live worker means the process exists; check the session status below for whether a task is executing.</p>
      </div>
    </section>

    <section class="card" aria-labelledby="hostsTitle">
      <div class="card-header">
        <div class="card-heading">
          <h2 class="card-title" id="hostsTitle" data-i18n="Cluster Nodes (Execution Hosts)">Cluster Nodes (Execution Hosts)</h2>
          <p class="card-subtitle" data-i18n="Coordinator and configured worker hosts. Link status is not a measure of model activity.">Coordinator and configured worker hosts. Link status is not a measure of model activity.</p>
        </div>
        <div id="hostStats" class="card-meta" data-i18n="1 node">1 node</div>
      </div>
      <div class="table-frame table-frame--hosts" tabindex="0" role="region" aria-label="Cluster nodes table; scroll horizontally if needed" data-i18n-aria-label="Cluster nodes table; scroll horizontally if needed">
        <table class="data-table data-table--hosts">
          <thead>
            <tr>
              <th scope="col" data-i18n="Host ID">Host ID</th>
              <th scope="col" data-i18n="Role">Role</th>
              <th scope="col" data-i18n="Status">Status</th>
              <th scope="col" data-i18n="SSH Target">SSH Target</th>
              <th scope="col" data-i18n="Node ID &amp; Generation">Node ID &amp; Generation</th>
              <th scope="col" data-i18n="Managed Projects">Managed Projects</th>
              <th scope="col" data-i18n="Last Contact">Last Contact</th>
              <th scope="col" data-i18n="Action">Action</th>
            </tr>
          </thead>
          <tbody id="hostTable">
            <tr><td class="empty-cell" colspan="8" data-i18n="Loading cluster nodes…">Loading cluster nodes&hellip;</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <div class="grid-2">
      <section class="card" aria-labelledby="sessionsTitle">
        <div class="card-header">
          <div class="card-heading">
            <h2 class="card-title" id="sessionsTitle" data-i18n="Agents &amp; Sessions">Agents &amp; Sessions</h2>
            <p class="card-subtitle" data-i18n="Bridge status comes from worker and task state. Idle means no task is executing.">Bridge status comes from worker and task state. Idle means no task is executing.</p>
          </div>
          <div id="sessionCount" class="card-meta" data-i18n="0 sessions">0 sessions</div>
        </div>
        <div class="table-frame table-frame--sessions" tabindex="0" role="region" aria-label="Agents and sessions table; scroll horizontally if needed" data-i18n-aria-label="Agents and sessions table; scroll horizontally if needed">
          <table class="data-table data-table--sessions">
            <thead>
              <tr>
                <th scope="col" data-i18n="Agent / Session">Agent / Session</th>
                <th scope="col" data-i18n="Host">Host</th>
                <th scope="col" data-i18n="Runtime">Runtime</th>
                <th scope="col" data-i18n="Status">Status</th>
                <th scope="col" data-i18n="Goal / Task">Goal / Task</th>
                <th scope="col" data-i18n="Cost">Cost</th>
              </tr>
            </thead>
            <tbody id="sessionTable">
              <tr><td class="empty-cell" colspan="6" data-i18n="Loading agents and sessions…">Loading agents and sessions&hellip;</td></tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="card" aria-labelledby="broadcastsTitle">
        <div class="card-header">
          <div class="card-heading">
            <h2 class="card-title" id="broadcastsTitle" data-i18n="Communications &amp; Groups">Communications &amp; Groups</h2>
            <p class="card-subtitle" data-i18n="Broadcast delivery progress and recent group messages.">Broadcast delivery progress and recent group messages.</p>
          </div>
          <div id="groupStats" class="card-meta" data-i18n="0 broadcasts">0 broadcasts</div>
        </div>
        <div class="table-frame table-frame--broadcasts" tabindex="0" role="region" aria-label="Communications and groups table; scroll horizontally if needed" data-i18n-aria-label="Communications and groups table; scroll horizontally if needed">
          <table class="data-table data-table--broadcasts">
            <thead>
              <tr>
                <th scope="col" data-i18n="Broadcast / Group">Broadcast / Group</th>
                <th scope="col" data-i18n="Progress">Progress</th>
                <th scope="col" data-i18n="Members">Members</th>
                <th scope="col" data-i18n="Message Preview">Message Preview</th>
                <th scope="col" data-i18n="Age">Age</th>
              </tr>
            </thead>
            <tbody id="broadcastTable">
              <tr><td class="empty-cell" colspan="5" data-i18n="No broadcasts recorded yet">No broadcasts recorded yet</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>

    <section class="card" aria-labelledby="nativeTitle">
      <div class="card-header">
        <div class="card-heading">
          <h2 class="card-title" id="nativeTitle" data-i18n="Native CLI Processes &amp; Workers (pi · claude · agy · codex)">Native CLI Processes &amp; Workers (pi &middot; claude &middot; agy &middot; codex)</h2>
          <p class="card-subtitle" data-i18n="Process presence only. A detected process does not prove that the CLI is currently running inference.">Process presence only. A detected process does not prove that the CLI is currently running inference.</p>
        </div>
        <div id="nativeCount" class="card-meta" data-i18n="0 native">0 native</div>
      </div>
      <div class="table-frame table-frame--native" tabindex="0" role="region" aria-label="Native CLI processes table; scroll horizontally if needed" data-i18n-aria-label="Native CLI processes table; scroll horizontally if needed">
        <table class="data-table data-table--native">
          <thead>
            <tr>
              <th scope="col" data-i18n="Runtime">Runtime</th>
              <th scope="col">PID</th>
              <th scope="col" data-i18n="Session / Title">Session / Title</th>
              <th scope="col" data-i18n="Model">Model</th>
              <th scope="col" data-i18n="Process">Process</th>
              <th scope="col" data-i18n="Kind / CWD">Kind / CWD</th>
            </tr>
          </thead>
          <tbody id="nativeTable">
            <tr><td colspan="6" class="empty-cell" data-i18n="Loading native processes…">Loading native processes&hellip;</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="card" aria-labelledby="eventsTitle">
      <div class="card-header">
        <div class="card-heading">
          <h2 class="card-title" id="eventsTitle" data-i18n="Live Event Stream">Live Event Stream</h2>
          <p class="card-subtitle" data-i18n="Latest Bridge events from the current snapshot.">Latest Bridge events from the current snapshot.</p>
        </div>
        <div class="card-meta" data-i18n="Auto-updating">Auto-updating</div>
      </div>
      <div class="card-body">
        <div class="log-container" id="eventLog" tabindex="0" role="region" aria-label="Event stream; scroll for older events" data-i18n-aria-label="Event stream; scroll for older events" aria-live="off">
          <div class="empty-log" data-i18n="Waiting for events…">Waiting for events&hellip;</div>
        </div>
      </div>
    </section>

    <section class="api-bar" aria-label="Integration APIs" data-i18n-aria-label="Integration APIs">
      <span class="api-label" data-i18n="Integration APIs">Integration APIs</span>
      <code>GET /api/v1/health</code>
      <code>GET /api/v1/runtimes</code>
      <code>GET /api/v1/sessions?runtime=all&amp;source=all&amp;status=active</code>
      <code>GET /api/v1/events?after_seq=0</code>
      <code>GET /api/v1/stream</code>
      <code>GET /api/v1/schema</code>
      <code>GET /api/snapshot?runtime=agy</code>
    </section>
  </main>

"""
_HTML_SCRIPT = """  <script>
    'use strict';

/*__WEB_I18N__*/
    const SVG_NS = 'http://www.w3.org/2000/svg';
    let currentData = null;
    let csrfToken = null;
    let selectedRuntime = 'all';
    let activeOnly = false;
    const approvalRequestIds = new Map();

    function uiText(message, values) {
      if (typeof translateUi === 'function') return translateUi(message, values);
      if (!values) return message;
      return message.replace(/\\{([a-zA-Z_][a-zA-Z0-9_]*)\\}/g, (match, key) => (
        Object.prototype.hasOwnProperty.call(values, key) ? String(values[key]) : match
      ));
    }

    function localizeStatus(status) {
      if (!status || typeof status !== 'object') return status;
      const separator = typeof webLanguage === 'string' && webLanguage === 'zh-CN' ? '' : ' ';
      const detail = Array.isArray(status.detailParts)
        ? status.detailParts.map((part) => uiText(part.message, part.values)).join(separator)
        : uiText(status.detailKey || status.detail, status.detailValues || {});
      return { ...status, label: uiText(status.label), detail: detail };
    }

    function formatUiCount(value) {
      const number = Number(value);
      if (!Number.isFinite(number)) return value === null || value === undefined ? '' : String(value);
      try {
        return new Intl.NumberFormat(
          typeof webLanguage === 'string' && webLanguage === 'zh-CN' ? 'zh-CN' : 'en-US',
          { maximumFractionDigits: 0 }
        ).format(number);
      } catch (error) {
        return String(number);
      }
    }

    function formatUiRelative(value) {
      const text = value === null || value === undefined ? '' : String(value);
      if (!text || text === '-') return text;
      if (text === 'now') return uiText('now');
      const match = /^(\\d+)([smhd]) ago$/.exec(text);
      if (!match) return text;
      const templates = {
        s: '{count}s ago', m: '{count}m ago', h: '{count}h ago', d: '{count}d ago'
      };
      return uiText(templates[match[2]], { count: formatUiCount(match[1]) });
    }

    let lastSuccessfulFetch = 0;
    let lastFetchFailed = false;
    let fetchInFlight = false;
    let hasLoadedOnce = false;

    function normalizeToken(value) {
      return value === null || value === undefined ? '' : String(value).toLowerCase();
    }

    function bridgeStatus(session) {
      const item = session || {};
      const observed = normalizeToken(item.observed) || 'unknown';
      const task = item.task && typeof item.task === 'object' ? item.task : {};
      const taskStatus = normalizeToken(task.status);
      const validation = normalizeToken(task.validation);
      const alive = item.alive === true ? true : (item.alive === false ? false : null);

      if (item.waiting_question || observed === 'waiting_input' || taskStatus === 'waiting_input') {
        return {
          label: item.waiting_question ? 'Waiting approval' : 'Waiting input',
          tone: 'waiting',
          detail: item.waiting_question
            ? 'A tool permission request is waiting for a decision.'
            : 'The Bridge task is waiting for input.'
        };
      }
      if (observed === 'recovery_needed' || taskStatus === 'recovery_needed') {
        return {
          label: 'Recovery needed',
          tone: 'recovery',
          detail: 'Execution state is uncertain; inspect the session before retrying.'
        };
      }
      if (taskStatus === 'completed') {
        if (validation === 'pending') {
          return {
            label: 'Awaiting review',
            tone: 'review',
            detail: 'The task completed, but its result still needs review.'
          };
        }
        if (validation === 'accepted') {
          return { label: 'Completed \u00b7 accepted', tone: 'success', detail: 'The task completed and its result was accepted.' };
        }
        if (validation === 'rejected') {
          return { label: 'Completed \u00b7 rejected', tone: 'danger', detail: 'The task completed, but its result was rejected.' };
        }
        return { label: 'Completed', tone: 'success', detail: 'The latest task completed.' };
      }
      if (taskStatus === 'failed' || taskStatus === 'interrupted' || taskStatus === 'cancelled') {
        const label = taskStatus.charAt(0).toUpperCase() + taskStatus.slice(1);
        return {
          label: label,
          tone: 'danger',
          detail: 'The latest Bridge task is ' + taskStatus + '.',
          detailKey: 'The latest Bridge task is {status}.',
          detailValues: { status: taskStatus }
        };
      }
      if (alive === false) {
        if (observed === 'stopped' || item.desired === 'closed') {
          return { label: 'Stopped', tone: 'stopped', detail: 'The snapshot reports the Bridge worker process as not alive.' };
        }
        return { label: 'Worker offline', tone: 'danger', detail: 'The Bridge worker process is not alive; its task is not executing.' };
      }
      if (taskStatus === 'running' || taskStatus === 'assigned') {
        return { label: 'Task running', tone: 'running', detail: 'The Bridge task is executing.' };
      }
      if (taskStatus === 'queued' || taskStatus === 'pending') {
        return { label: 'Task queued', tone: 'idle', detail: 'A Bridge task is queued but not executing yet.' };
      }
      if (observed === 'running') {
        return {
          label: 'Worker running',
          tone: 'running',
          detail: 'The snapshot reports the Bridge worker as running; the task state is not reported.'
        };
      }
      if (observed === 'idle') {
        return {
          label: 'Idle \u00b7 no task',
          tone: 'idle',
          detail: alive === true
            ? 'The Bridge worker is alive, but no task is executing.'
            : 'The snapshot reports the Bridge worker as idle; process liveness is not reported.'
        };
      }
      if (observed === 'starting') {
        return { label: 'Starting', tone: 'idle', detail: 'The snapshot reports the Bridge worker as starting.' };
      }
      if (observed === 'stopped') {
        return { label: 'Stopped', tone: 'stopped', detail: 'The snapshot reports the Bridge worker as stopped.' };
      }
      if (alive === true) {
        return { label: 'Worker alive', tone: 'idle', detail: 'The Bridge worker is alive; no executing task is reported.' };
      }
      return { label: 'Unknown', tone: 'unknown', detail: 'The snapshot does not report a reliable Bridge status.' };
    }

    function nativeProcessStatus(proc) {
      const item = proc || {};
      const kind = normalizeToken(item.kind);
      const observed = normalizeToken(item.observed);
      const alive = item.alive === true ? true : (item.alive === false ? false : null);
      if (alive === false) {
        return {
          label: 'Process exited',
          tone: 'stopped',
          detail: 'The snapshot reports this native process as not alive.'
        };
      }
      const liveness = alive === true
        ? 'A native process is present.'
        : 'The snapshot lists a native process, but its liveness is not reported.';
      const presenceParts = [{
        message: alive === true
          ? 'A native process is present.'
          : 'The snapshot lists a native process, but its liveness is not reported.'
      }];
      if (kind === 'daemon' || observed === 'daemon') {
        return {
          label: 'Daemon process',
          tone: 'native',
          detail: liveness + ' Its presence does not prove current inference.',
          detailParts: [...presenceParts, { message: 'Its presence does not prove current inference.' }]
        };
      }
      if (kind === 'worker' || observed === 'running') {
        return {
          label: 'Worker process',
          tone: 'native',
          detail: liveness + ' This does not prove it is currently inferencing.',
          detailParts: [...presenceParts, { message: 'This does not prove it is currently inferencing.' }]
        };
      }
      if (kind === 'interactive' || observed === 'active') {
        return {
          label: 'Interactive process',
          tone: 'idle',
          detail: liveness + ' It may be idle or waiting for input; its presence does not prove current inference.',
          detailParts: [...presenceParts, { message: 'It may be idle or waiting for input; its presence does not prove current inference.' }]
        };
      }
      return {
        label: 'Process present',
        tone: 'native',
        detail: liveness + ' Its presence does not prove current inference.',
        detailParts: [...presenceParts, { message: 'Its presence does not prove current inference.' }]
      };
    }

    function hostStatus(host) {
      const state = normalizeToken(host && host.state) || 'unknown';
      const statuses = {
        online: { label: 'Online', tone: 'online', detail: 'The host link is online.' },
        offline: { label: 'Offline', tone: 'offline', detail: 'The host link is offline.' },
        identity_changed: { label: 'Identity changed', tone: 'recovery', detail: 'The host identity no longer matches the pinned identity.' },
        state_corrupted: { label: 'State corrupted', tone: 'recovery', detail: 'The host state could not be verified.' }
      };
      return statuses[state] || {
        label: 'Unknown',
        tone: 'unknown',
        detail: 'The host state is not reported; this is not treated as offline.'
      };
    }

    function snapshotProvenance(data) {
      const item = data && typeof data === 'object' ? data : {};
      const raw = item.provenance && typeof item.provenance === 'object' && !Array.isArray(item.provenance)
        ? item.provenance
        : {};
      const coordinator = item.coordinator && typeof item.coordinator === 'object' ? item.coordinator : {};
      const source = normalizeToken(
        raw.snapshot_source || raw.source || item.snapshot_source || coordinator.snapshot_source
      ) || 'unknown';
      const connectedValue = raw.broker_connected !== undefined
        ? raw.broker_connected
        : (item.broker_connected !== undefined ? item.broker_connected : coordinator.broker_connected);
      let brokerConnected = null;
      if (connectedValue === true || connectedValue === false) brokerConnected = connectedValue;
      const brokerState = normalizeToken(
        raw.broker_state || raw.connection_state || item.broker_state || coordinator.connection_state
      );
      if (brokerConnected === null) {
        if (brokerState === 'connected') brokerConnected = true;
        else if (brokerState === 'fallback' || brokerState === 'unavailable') brokerConnected = false;
        else if (source === 'broker' || source === 'broker_rpc' || source === 'broker_local') brokerConnected = true;
      }
      const detail = typeof raw.detail === 'string' && raw.detail.trim() ? raw.detail.trim() : '';
      return {
        source: source,
        brokerConnected: brokerConnected,
        known: brokerConnected !== null || ['broker', 'broker_rpc', 'broker_local', 'local_db_fallback'].includes(source),
        legacy: raw.legacy === true || item.legacy_broker === true,
        detail: detail
      };
    }

    function coordinatorStatus(provenance) {
      const state = provenance && typeof provenance === 'object' ? provenance : {};
      if (state.brokerConnected === true) {
        return {
          label: 'Broker connected',
          tone: 'success',
          detail: state.detail || 'The snapshot was served by the local broker.'
        };
      }
      if (state.source === 'local_db_fallback') {
        return {
          label: 'Local fallback',
          tone: 'recovery',
          detail: state.detail || (state.brokerConnected === false
            ? 'Broker unavailable; data is read directly from the local database. Host connectivity is not verified.'
            : 'Local database fallback was used; broker connectivity is unknown.')
        };
      }
      if (state.brokerConnected === false) {
        return {
          label: 'Broker unavailable',
          tone: 'danger',
          detail: state.detail || 'The snapshot reports that the local broker is unavailable.'
        };
      }
      if (state.source === 'local_db') {
        return {
          label: 'Local data',
          tone: 'recovery',
          detail: state.detail || 'Data came from the local database; broker connectivity is not reported.'
        };
      }
      return {
        label: 'Unknown',
        tone: 'unknown',
        detail: state.detail || 'The snapshot does not include broker connectivity provenance.'
      };
    }

    function aggregateHasAttempts(data) {
      const item = data && typeof data === 'object' ? data : {};
      const metrics = item.metrics && typeof item.metrics === 'object' ? item.metrics : {};
      if (typeof metrics.has_attempts === 'boolean') return metrics.has_attempts;
      const sessions = Array.isArray(item.sessions) ? item.sessions : [];
      const observed = sessions.some((session) => Boolean(
        session && (session.has_attempts || (session.attempt && typeof session.attempt === 'object'))
      ));
      if (!observed && metrics.total_sessions > sessions.length) return null;
      return observed;
    }

    function aggregateHasCostGaps(data) {
      const item = data && typeof data === 'object' ? data : {};
      const metrics = item.metrics && typeof item.metrics === 'object' ? item.metrics : {};
      if (typeof metrics.has_cost_gaps === 'boolean') return metrics.has_cost_gaps;
      if (typeof metrics.unknown_cost_sessions === 'number' && Number.isFinite(metrics.unknown_cost_sessions)) {
        return metrics.unknown_cost_sessions > 0;
      }
      if (metrics.cost_complete !== true && metrics.total_sessions > (Array.isArray(item.sessions) ? item.sessions.length : 0)) {
        return true;
      }
      return metrics.cost_complete === false;
    }

    function statusBadgeClass(tone) {
      const classes = {
        running: 'badge badge-running',
        idle: 'badge badge-idle',
        waiting: 'badge badge-waiting',
        recovery: 'badge badge-recovery',
        review: 'badge badge-review',
        success: 'badge badge-online',
        danger: 'badge badge-offline',
        stopped: 'badge badge-stopped',
        native: 'badge badge-native',
        unknown: 'badge badge-unknown'
      };
      return classes[tone] || classes.unknown;
    }

    function formatCost(value, complete, hasAttempts, digits, knownCostSessions) {
      const precision = Number.isInteger(digits) ? digits : 3;
      const isNumber = typeof value === 'number' && Number.isFinite(value);
      if (!isNumber) {
        if (hasAttempts || complete === false) {
          return { text: 'Unknown', title: 'Cost is incomplete or unavailable; it is not treated as zero.', unknown: true };
        }
        return { text: '\u2014', title: 'No cost-bearing attempts recorded.', unknown: false };
      }
      if (complete === false && value === 0) {
        const hasKnownCoverage = Number.isInteger(knownCostSessions)
          && knownCostSessions >= 0
          && (knownCostSessions > 0 || hasAttempts === false);
        if (!hasKnownCoverage) {
          return { text: 'Unknown', title: 'No known cost is available; the total is incomplete and is not treated as zero.', unknown: true };
        }
      }
      return {
        text: '$' + value.toFixed(precision) + (complete === false ? '*' : ''),
        title: complete === false ? 'Known subtotal; some costs are unknown.' : 'Reported cost.',
        unknown: false
      };
    }

    function connectionState(lastSuccess, now, failed) {
      const last = typeof lastSuccess === 'number' && Number.isFinite(lastSuccess) ? lastSuccess : 0;
      const current = typeof now === 'number' && Number.isFinite(now) ? now : Date.now();
      if (!last) {
        return failed
          ? {
            state: 'error',
            label: 'Not connected',
            detail: 'No dashboard snapshot has loaded; retrying.',
            labelKey: 'Not connected',
            detailKey: 'No dashboard snapshot has loaded; retrying.'
          }
          : {
            state: 'connecting',
            label: 'Connecting\u2026',
            detail: 'Waiting for the first dashboard snapshot.',
            labelKey: 'Connecting\u2026',
            detailKey: 'Waiting for the first dashboard snapshot.'
          };
      }
      const age = Math.max(0, Math.floor((current - last) / 1000));
      if (failed) {
        return {
          state: 'error',
          label: 'Connection lost',
          detail: 'Showing the last snapshot from ' + age + 's ago; retrying.',
          labelKey: 'Connection lost',
          detailKey: 'Showing the last snapshot from {age}s ago; retrying.',
          detailValues: { age: age }
        };
      }
      if (age >= 10) {
        return {
          state: 'stale',
          label: 'Stale \u00b7 ' + age + 's',
          detail: 'Last snapshot was ' + age + 's ago; waiting for a fresh update.',
          labelKey: 'Stale \u00b7 {age}s',
          labelValues: { age: age },
          detailKey: 'Last snapshot was {age}s ago; waiting for a fresh update.',
          detailValues: { age: age }
        };
      }
      return {
        state: 'live',
        label: age <= 1 ? 'Live \u00b7 just now' : 'Live \u00b7 ' + age + 's ago',
        detail: 'Last snapshot was ' + age + 's ago.',
        labelKey: age <= 1 ? 'Live \u00b7 just now' : 'Live \u00b7 {age}s ago',
        labelValues: { age: age },
        detailKey: 'Last snapshot was {age}s ago.',
        detailValues: { age: age }
      };
    }

    function runtimeLabel(runtime) {
      return runtime === 'antigravity' ? 'agy' : (runtime || '-');
    }

    function setRuntimeFilter(runtime) {
      selectedRuntime = runtime || 'all';
      document.querySelectorAll('#runtimeFilterBar .chip-btn').forEach((btn) => {
        const isSelected = btn.dataset.runtime === selectedRuntime;
        btn.classList.toggle('active', isSelected);
        btn.setAttribute('aria-pressed', String(isSelected));
      });
      if (currentData) renderDashboard(currentData);
    }

    function sessionMatchesFilter(session) {
      if (selectedRuntime !== 'all' && session.runtime !== selectedRuntime) return false;
      if (activeOnly) {
        const obs = session.observed || 'stopped';
        const isActive = obs === 'running' || obs === 'idle' || obs === 'waiting_input' ||
          obs === 'starting' || obs === 'recovery_needed' ||
          Boolean(session.alive) || Boolean(session.waiting_question);
        if (!isActive) return false;
      }
      return true;
    }

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

    function setConnectionBanner(messageKey) {
      const banner = document.getElementById('connectionBanner');
      if (!banner) return;
      if (!messageKey) {
        banner.hidden = true;
        delete banner.dataset.i18nMessage;
        setText(banner, '');
        return;
      }
      banner.dataset.i18nMessage = messageKey;
      banner.hidden = false;
      setText(banner, uiText(messageKey));
    }

    function retranslateConnectionBanner() {
      const banner = document.getElementById('connectionBanner');
      if (!banner || banner.hidden || !banner.dataset.i18nMessage) return;
      setText(banner, uiText(banner.dataset.i18nMessage));
    }

    function updateConnectionStatus() {
      const element = document.getElementById('connectionStatus');
      if (!element) return;
      const state = connectionState(lastSuccessfulFetch, Date.now(), lastFetchFailed);
      const label = uiText(state.labelKey || state.label, state.labelValues);
      const detail = uiText(state.detailKey || state.detail, state.detailValues);
      element.dataset.state = state.state;
      setText(element, label);
      element.title = detail;
      element.setAttribute('aria-label', label + '. ' + detail);
    }

    async function fetchData() {
      if (fetchInFlight) return;
      fetchInFlight = true;
      try {
        const response = await fetch('/api/snapshot', { credentials: 'same-origin' });
        if (response.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (!response.ok) {
          lastFetchFailed = true;
          setConnectionBanner(hasLoadedOnce
            ? 'Could not refresh the dashboard. Showing the last snapshot while retrying.'
            : 'Could not refresh the dashboard. Retrying the first snapshot.');
          updateConnectionStatus();
          return;
        }
        const payload = await response.json();
        currentData = payload;
        lastSuccessfulFetch = Date.now();
        lastFetchFailed = false;
        hasLoadedOnce = true;
        setConnectionBanner('');
        renderDashboard(currentData);
        document.getElementById('main').setAttribute('aria-busy', 'false');
        updateConnectionStatus();
      } catch (error) {
        lastFetchFailed = true;
        setConnectionBanner(hasLoadedOnce
          ? 'Could not reach the Bridge dashboard. Showing the last snapshot while retrying.'
          : 'Could not reach the Bridge dashboard. Retrying the first snapshot.');
        updateConnectionStatus();
        console.error('Fetch error:', error);
      } finally {
        fetchInFlight = false;
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
          window.alert(uiText('This approval is stale or was already handled. Refreshing.'));
          await fetchData();
          return;
        }
        // Keep the same request id for transient failures so a retry is idempotent.
        window.alert(uiText('Approval failed: {error}', { error: payload.error || uiText('unknown error') }));
      } catch (error) {
        window.alert(uiText('Failed to send approval: {error}', { error: error }));
      }
    }

    function renderApprovals(questions) {
      const section = document.getElementById('approvalSection');
      section.replaceChildren();
      if (!questions.length) {
        section.style.display = 'none';
        return;
      }
      section.style.display = 'flex';
      questions.forEach((question) => {
        const banner = makeElement('div', 'alert-banner');
        banner.setAttribute('role', 'group');
        const info = makeElement('div', 'alert-info');
        const title = makeElement('div', 'alert-title');
        const sessionName = question.session_name || String(question.session_id || '').substring(0, 8);
        setText(title, uiText('Approval needed for {name}', { name: sessionName }));
        info.appendChild(title);

        const detail = makeElement('div', 'alert-detail');
        const sessionId = String(question.session_id || '').substring(0, 8);
        const tool = question.tool || '-';
        const age = formatUiRelative(question.created_rel || '-');
        detail.appendChild(document.createTextNode(uiText('Session') + ' '));
        detail.appendChild(makeElement('code', '', sessionId));
        detail.appendChild(document.createTextNode(' \u00b7 ' + uiText('Tool') + ' '));
        detail.appendChild(makeElement('b', '', tool));
        detail.appendChild(document.createTextNode(' \u00b7 ' + age));
        detail.setAttribute('aria-label', uiText('Session {id} \u00b7 Tool {tool} \u00b7 {age}', {
          id: sessionId, tool: tool, age: age
        }));
        info.appendChild(detail);

        const command = makeElement('div', 'alert-detail');
        command.appendChild(makeElement('code', '', question.command || question.description || '-'));
        info.appendChild(command);

        const actions = makeElement('div', 'alert-actions');
        const revision = findSessionRevision(question.session_id);
        const allowButton = makeElement('button', 'btn', uiText('Approve'));
        allowButton.type = 'button';
        allowButton.disabled = revision === null;
        allowButton.title = uiText(revision === null ? 'Waiting for current session data' : 'Approve this request');
        allowButton.addEventListener('click', () => {
          submitDecision(question.session_id, question.id, 'allow', revision);
        });
        const denyButton = makeElement('button', 'btn btn-secondary', uiText('Deny'));
        denyButton.type = 'button';
        denyButton.disabled = revision === null;
        denyButton.title = uiText(revision === null ? 'Waiting for current session data' : 'Deny this request');
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

    function renderHosts(hosts, coordinator, provenance) {
      const body = document.getElementById('hostTable');
      body.replaceChildren();
      const totalNodes = (hosts ? hosts.length : 0) + 1;
      const coordinatorView = coordinatorStatus(provenance);
      const hostStats = document.getElementById('hostStats');
      const nodeCount = formatUiCount(totalNodes);
      hostStats.textContent = uiText(totalNodes === 1 ? '{count} node' : '{count} nodes', { count: nodeCount }) +
        ' \u00b7 ' + uiText(coordinatorView.label);
      hostStats.title = uiText(coordinatorView.detail);

      // 1. Coordinator Row
      const cRow = document.createElement('tr');
      const cNameCell = document.createElement('td');
      const coordinatorName = coordinator.host_id || 'coordinator';
      const cNameLine = makeElement('div', 'cell-title', coordinatorName);
      cNameLine.title = coordinatorName;
      cNameCell.appendChild(cNameLine);
      cRow.appendChild(cNameCell);

      const cRoleCell = document.createElement('td');
      cRoleCell.appendChild(makeElement('span', 'badge badge-coord', uiText('Coordinator')));
      cRow.appendChild(cRoleCell);

      const cStatusCell = document.createElement('td');
      const coordinatorLabel = uiText(coordinatorView.label);
      const coordinatorDetail = uiText(coordinatorView.detail);
      const coordinatorBadge = makeElement(
        'span', statusBadgeClass(coordinatorView.tone), coordinatorLabel
      );
      coordinatorBadge.title = coordinatorDetail;
      coordinatorBadge.setAttribute('aria-label', coordinatorLabel + '. ' + coordinatorDetail);
      cStatusCell.appendChild(coordinatorBadge);
      cRow.appendChild(cStatusCell);

      const cTargetCell = document.createElement('td');
      const targetText = uiText(provenance && provenance.brokerConnected === true
        ? 'local (unix socket)'
        : (provenance && provenance.source === 'local_db_fallback' ? 'local database (fallback)' : 'not reported'));
      cTargetCell.appendChild(makeElement('code', '', targetText));
      cRow.appendChild(cTargetCell);

      const cNodeCell = document.createElement('td');
      const coordinatorNode = coordinator.node_id ? String(coordinator.node_id) : '-';
      const coordinatorNodeCode = makeElement('code', '', coordinator.node_id ? coordinatorNode.substring(0, 16) : '-');
      coordinatorNodeCode.title = coordinatorNode;
      cNodeCell.appendChild(coordinatorNodeCode);
      cRow.appendChild(cNodeCell);

      const cProjsCell = makeElement('td', 'cell-muted', uiText('bridge (local)'));
      cRow.appendChild(cProjsCell);

      let contactText = uiText('unknown');
      if (provenance && provenance.brokerConnected === true) contactText = uiText('verified');
      else if (provenance && provenance.source === 'local_db_fallback') contactText = uiText('not verified');
      else if (provenance && provenance.brokerConnected === false) contactText = uiText('unavailable');
      const cContactCell = makeElement('td', 'cell-muted', contactText);
      cRow.appendChild(cContactCell);

      const cActionCell = makeElement('td', 'cell-muted', '\u2014');
      cRow.appendChild(cActionCell);
      body.appendChild(cRow);

      // 2. Remote Worker Hosts
      if (hosts && hosts.length) {
        hosts.forEach((host) => {
          const row = document.createElement('tr');
          const nameCell = document.createElement('td');
          const hostName = host.host_id || '-';
          const nameLine = makeElement('div', 'cell-title', hostName);
          nameLine.title = hostName;
          nameCell.appendChild(nameLine);
          row.appendChild(nameCell);

          const roleCell = document.createElement('td');
          roleCell.appendChild(makeElement('span', 'badge badge-idle', uiText('Worker node')));
          row.appendChild(roleCell);

          const statusCell = document.createElement('td');
          const linkStatus = hostStatus(host);
          const linkLabel = uiText(linkStatus.label);
          const linkDetail = uiText(linkStatus.detail);
          const statusBadge = makeElement('span', statusBadgeClass(linkStatus.tone), linkLabel);
          statusBadge.title = linkDetail;
          statusBadge.setAttribute('aria-label', linkLabel + '. ' + linkDetail);
          statusCell.appendChild(statusBadge);
          row.appendChild(statusCell);

          const targetCell = document.createElement('td');
          const targetCode = makeElement('code', '', host.ssh_target || '-');
          targetCode.title = host.ssh_target || '';
          targetCell.appendChild(targetCode);
          row.appendChild(targetCell);

          const nodeCell = document.createElement('td');
          const nodeStr = host.node_id ? String(host.node_id).substring(0, 16) + '\u2026' : '-';
          const nodeCode = makeElement('code', '', nodeStr);
          nodeCode.title = host.node_id ? String(host.node_id) : '';
          nodeCell.appendChild(nodeCode);
          row.appendChild(nodeCell);

          const projs = host.projects && typeof host.projects === 'object' ? Object.keys(host.projects).join(', ') : '-';
          const projsCell = makeElement('td', '', projs || '-');
          projsCell.title = projs || '';
          row.appendChild(projsCell);

          const contactCell = makeElement('td', 'cell-muted', formatUiRelative(host.last_contact_rel || '-'));
          row.appendChild(contactCell);

          const actionCell = document.createElement('td');
          const pingButton = makeElement('button', 'btn btn-secondary btn-sm', uiText('Ping'));
          pingButton.type = 'button';
          pingButton.setAttribute('aria-label', uiText('Ping host {name}', { name: hostName }));
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
      setText(button, uiText('Pinging...'));
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
          setText(button, uiText('Failed'));
          setTimeout(() => { setText(button, uiText('Ping')); button.disabled = false; }, 2000);
        }
      } catch (error) {
        setText(button, uiText('Error'));
        setTimeout(() => { setText(button, uiText('Ping')); button.disabled = false; }, 2000);
      }
    }

    function renderRuntimes(runtimes) {
      const grid = document.getElementById('runtimeGrid');
      if (!grid) return;
      grid.replaceChildren();
      const order = ['pi', 'claude', 'antigravity', 'codex'];
      order.forEach((key) => {
        const info = (runtimes && runtimes[key]) || {};
        const isSelected = selectedRuntime === key;
        const card = document.createElement('button');
        card.type = 'button';
        card.className = 'runtime-card' + (isSelected ? ' selected' : '');
        card.setAttribute('aria-pressed', String(isSelected));
        card.setAttribute('aria-label', uiText('Filter dashboard to {name} runtime', {
          name: info.display_name || runtimeLabel(key)
        }));
        card.addEventListener('click', () => {
          setRuntimeFilter(isSelected ? 'all' : key);
        });

        const bridgeActive = Number(info.bridge_active || 0);
        const bridgeRunning = Number(info.bridge_running || 0);
        const bridgeWaiting = Number(info.bridge_waiting || 0);
        const bridgeRecovery = Number(info.bridge_recovery || 0);
        const nativeActive = Number(info.native_active || 0);
        const top = makeElement('div', 'runtime-card-top');
        const label = info.display_name || runtimeLabel(key);
        top.appendChild(makeElement('span', 'runtime-title', label));
        const liveEl = makeElement(
          'span',
          'runtime-live' + (bridgeActive > 0 ? ' has-live' : ''),
          uiText('{count} bridge', { count: formatUiCount(bridgeActive) })
        );
        liveEl.title = uiText('Bridge worker processes alive or waiting');
        top.appendChild(liveEl);
        card.appendChild(top);

        const meta1 = makeElement('div', 'runtime-meta');
        let bridgeDetail = uiText('Bridge: {running} running \u00b7 {waiting} waiting', {
          running: formatUiCount(bridgeRunning), waiting: formatUiCount(bridgeWaiting)
        });
        if (bridgeRecovery > 0) {
          bridgeDetail += uiText(' \u00b7 {count} recovery', { count: formatUiCount(bridgeRecovery) });
        }
        meta1.appendChild(makeElement('span', '', bridgeDetail));
        card.appendChild(meta1);

        const meta2 = makeElement('div', 'runtime-meta');
        meta2.appendChild(makeElement('span', '', uiText(
          nativeActive === 1 ? 'Native: {count} process' : 'Native: {count} processes',
          { count: formatUiCount(nativeActive) }
        )));
        if (info.native_daemons) {
          meta2.appendChild(makeElement('span', '', uiText('+{count} daemon', {
            count: formatUiCount(info.native_daemons)
          })));
        }
        card.appendChild(meta2);
        grid.appendChild(card);
      });
    }

    function renderSessions(sessions, metrics) {
      const body = document.getElementById('sessionTable');
      body.replaceChildren();
      const filtered = (sessions || []).filter(sessionMatchesFilter);
      const total = sessions ? sessions.length : 0;
      const shown = Math.min(filtered.length, 60);
      const countText = uiText('{shown} of {total} shown', {
        shown: formatUiCount(shown), total: formatUiCount(total)
      }) +
        (filtered.length > 60 ? uiText(' (first 60)') : '') +
        uiText(' \u00b7 {count} live', { count: formatUiCount(metrics.active_sessions || 0) });
      const sessionCount = document.getElementById('sessionCount');
      setText(sessionCount, countText);
      sessionCount.title = uiText('Live includes running, idle, waiting, or alive worker processes.');
      if (!filtered.length) {
        const message = uiText(total
          ? 'No sessions match the current runtime and active filters'
          : 'No Bridge sessions recorded yet');
        body.appendChild(emptyRow(6, message));
        return;
      }
      filtered.slice(0, 60).forEach((session) => {
        const row = document.createElement('tr');
        const nameCell = document.createElement('td');
        const sessionName = session.name || String(session.id || '').substring(0, 10);
        const nameLine = makeElement('div', 'cell-title', sessionName);
        nameLine.title = sessionName;
        const modelSuffix = session.model ? ' \u00b7 ' + session.model : '';
        const metaLine = makeElement('div', 'cell-meta', (session.profile || '-') + modelSuffix + ' \u00b7 ');
        metaLine.appendChild(makeElement('code', '', String(session.id || '').substring(0, 8)));
        nameCell.appendChild(nameLine);
        nameCell.appendChild(metaLine);

        const hostCell = document.createElement('td');
        const hostName = session.host_id || 'local';
        const hostBadge = makeElement('span', 'badge-node', hostName);
        hostBadge.title = hostName;
        hostCell.appendChild(hostBadge);

        const runtimeCell = document.createElement('td');
        const runtimeText = makeElement('span', '', session.display_runtime || runtimeLabel(session.runtime));
        runtimeText.style.fontWeight = '600';
        runtimeText.style.color = 'var(--accent)';
        runtimeCell.appendChild(runtimeText);

        const statusCell = document.createElement('td');
        const status = localizeStatus(bridgeStatus(session));
        const statusBadge = makeElement('span', statusBadgeClass(status.tone), status.label);
        statusBadge.title = status.detail;
        statusBadge.setAttribute('aria-label', status.label + '. ' + status.detail);
        statusCell.appendChild(statusBadge);

        const goal = (session.task && session.task.instruction)
          ? session.task.instruction
          : (session.error ? session.error : (session.cwd || ''));
        const goalCell = makeElement('td', 'cell-goal');
        const goalText = makeElement('span', 'cell-goal-text', goal || '\u2014');
        goalText.title = goal || uiText('No task instruction recorded.');
        goalCell.appendChild(goalText);

        const costNum = (typeof session.cost_usd === 'number')
          ? session.cost_usd
          : (session.attempt && typeof session.attempt.cost_usd === 'number' ? session.attempt.cost_usd : null);
        const cost = formatCost(costNum, session.cost_complete, session.has_attempts, 3, costNum !== null ? 1 : undefined);
        const costCell = makeElement('td', 'cell-cost' + (cost.unknown ? ' is-unknown' : (cost.text === '\u2014' ? ' is-empty' : '')), uiText(cost.text));
        costCell.title = uiText(cost.title);

        row.appendChild(nameCell);
        row.appendChild(hostCell);
        row.appendChild(runtimeCell);
        row.appendChild(statusCell);
        row.appendChild(goalCell);
        row.appendChild(costCell);
        body.appendChild(row);
      });
    }

    function renderNativeSessions(nativeSessions, metrics) {
      const body = document.getElementById('nativeTable');
      if (!body) return;
      body.replaceChildren();
      const allNative = nativeSessions || [];
      const filtered = allNative.filter((proc) => {
        if (selectedRuntime !== 'all' && proc.runtime !== selectedRuntime) return false;
        if (activeOnly && proc.kind === 'daemon') return false;
        return true;
      });
      const shown = Math.min(filtered.length, 60);
      const nativeCount = document.getElementById('nativeCount');
      nativeCount.textContent = uiText('{shown} of {total} shown', {
        shown: formatUiCount(shown), total: formatUiCount(allNative.length)
      }) +
        (filtered.length > 60 ? uiText(' (first 60)') : '') +
        uiText(' \u00b7 {count} non-daemon', { count: formatUiCount(metrics.native_sessions || 0) });
      nativeCount.title = uiText('Detected native CLI processes. Presence does not prove active inference.');
      if (!filtered.length) {
        const message = uiText(allNative.length
          ? 'No native processes match the current filters'
          : 'No native CLI processes detected');
        body.appendChild(emptyRow(6, message));
        return;
      }
      filtered.slice(0, 60).forEach((proc) => {
        const row = document.createElement('tr');
        const rtCell = document.createElement('td');
        const rtEl = makeElement('span', 'badge badge-native', proc.display_runtime || runtimeLabel(proc.runtime));
        rtCell.appendChild(rtEl);

        const pidCell = document.createElement('td');
        pidCell.appendChild(makeElement('code', '', String(proc.pid || '-')));

        const title = proc.name || proc.native_id || proc.comm || '-';
        const titleCell = document.createElement('td');
        const titleLine = makeElement('div', 'cell-title', title);
        titleLine.title = (proc.native_id ? proc.native_id + ' \u2014 ' : '') + title;
        titleCell.appendChild(titleLine);

        const modelCell = makeElement('td', 'cell-muted', proc.model || '\u2014');

        const stateCell = document.createElement('td');
        const processStatus = localizeStatus(nativeProcessStatus(proc));
        const processBadge = makeElement('span', statusBadgeClass(processStatus.tone), processStatus.label);
        processBadge.title = processStatus.detail;
        processBadge.setAttribute('aria-label', processStatus.label + '. ' + processStatus.detail);
        stateCell.appendChild(processBadge);

        const kindText = (proc.kind || 'cli') + (proc.cwd ? ' \u00b7 ' + proc.cwd : '');
        const kindCell = makeElement('td', 'cell-kind cell-muted');
        const kindLine = makeElement('span', 'cell-kind-text', kindText);
        kindLine.title = kindText;
        kindCell.appendChild(kindLine);

        row.appendChild(rtCell);
        row.appendChild(pidCell);
        row.appendChild(titleCell);
        row.appendChild(modelCell);
        row.appendChild(stateCell);
        row.appendChild(kindCell);
        body.appendChild(row);
      });
    }

    function renderBroadcasts(broadcasts) {
      const body = document.getElementById('broadcastTable');
      body.replaceChildren();
      document.getElementById('groupStats').textContent = uiText(
        broadcasts.length === 1 ? '{count} broadcast' : '{count} broadcasts',
        { count: formatUiCount(broadcasts.length) }
      );
      if (!broadcasts.length) {
        body.appendChild(emptyRow(5, uiText('No broadcast communications recorded yet')));
        return;
      }
      broadcasts.forEach((broadcast) => {
        const row = document.createElement('tr');
        const idCell = document.createElement('td');
        const broadcastId = String(broadcast.broadcast_id || '');
        const idLine = makeElement('div', 'cell-title', broadcastId.substring(0, 12));
        idLine.title = broadcastId;
        const groupLine = makeElement('div', 'cell-meta', broadcast.group_id || '-');
        groupLine.title = broadcast.group_id || '';
        idCell.appendChild(idLine);
        idCell.appendChild(groupLine);

        const progressCell = document.createElement('td');
        const received = makeElement('span', '', formatUiCount(broadcast.received));
        const total = Number(broadcast.total || 0);
        received.style.color = 'var(--green)';
        received.style.fontWeight = '700';
        progressCell.appendChild(received);
        progressCell.appendChild(document.createTextNode(' / ' + uiText('{total} received', {
          total: formatUiCount(total)
        })));
        if (broadcast.uncertain) {
          const uncertain = makeElement('span', '', ' \u00b7 ' + uiText('{count} uncertain', {
            count: formatUiCount(broadcast.uncertain)
          }));
          uncertain.style.color = 'var(--red)';
          progressCell.appendChild(uncertain);
        }

        const membersCell = makeElement('td', 'cell-muted', uiText('{count} targets', {
          count: formatUiCount(total)
        }));

        const messageCell = makeElement('td', 'cell-goal');
        const messageText = makeElement('span', 'cell-goal-text', broadcast.message_preview || '\u2014');
        messageText.title = broadcast.message_preview || uiText('No message preview');
        messageCell.appendChild(messageText);

        const ageCell = makeElement('td', 'cell-muted', formatUiRelative(broadcast.created_rel || '-'));

        row.appendChild(idCell);
        row.appendChild(progressCell);
        row.appendChild(membersCell);
        row.appendChild(messageCell);
        row.appendChild(ageCell);
        body.appendChild(row);
      });
    }

    function renderEvents(events, sessions) {
      const container = document.getElementById('eventLog');
      container.replaceChildren();
      let list = events || [];
      if (selectedRuntime !== 'all') {
        const runtimeBySession = new Map();
        (sessions || []).forEach((item) => {
          if (item && item.id) runtimeBySession.set(item.id, item.runtime);
        });
        list = list.filter((ev) => {
          const evRt = ev.runtime || runtimeBySession.get(ev.session_id);
          return !evRt || evRt === selectedRuntime;
        });
      }
      if (!list.length) {
        const message = uiText(selectedRuntime === 'all'
          ? 'No Bridge events recorded yet. New activity will appear here.'
          : 'No events for this runtime yet. Clear the runtime filter to see all activity.');
        container.appendChild(makeElement('div', 'empty-log', message));
        return;
      }
      list.slice(0, 25).forEach((event) => {
        const entry = makeElement('div', 'log-entry');
        entry.appendChild(makeElement('span', 'log-time', formatUiRelative(event.created_rel || '-')));
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

    function topologyText(message, values) {
      if (typeof uiText === 'function') return uiText(message, values);
      if (!values) return message;
      return message.replace(/\\{([a-zA-Z_][a-zA-Z0-9_]*)\\}/g, (match, key) => (
        Object.prototype.hasOwnProperty.call(values, key) ? String(values[key]) : match
      ));
    }

    function renderTopology(data) {
      const svg = document.getElementById('topoGraph');
      const layout = buildTopology(data, svg.parentElement.clientWidth || 800);
      const root = layout.hostNodes[0];
      svg.replaceChildren();
      svg.setAttribute('viewBox', '0 0 ' + layout.width + ' ' + layout.height);
      svg.style.width = layout.width + 'px';
      document.getElementById('topoStats').textContent = topologyText('{count} live local', {
        count: layout.liveCount
      }) + (layout.recoveryCount
        ? topologyText(' · {count} need recovery', { count: layout.recoveryCount })
        : '');

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
        const stateLabel = topologyText(
          { online: 'ONLINE', offline: 'OFFLINE', identity_changed: 'ID CHANGED', state_corrupted: 'CORRUPT' }[state] || 'UNKNOWN'
        );
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
        }, host.isCoord ? topologyText('COORD') : hostName.substring(0, 9)));
        svg.appendChild(svgText({
          x: host.x, y: host.y + 10, 'text-anchor': 'middle', fill: color, 'font-size': 8
        }, host.isCoord ? hostName.substring(0, 10) : stateLabel));

        if (!host.sessions.length) {
          svg.appendChild(svgText({
            x: host.x, y: 170, 'text-anchor': 'middle', fill: '#8b949e', 'font-size': 10
          }, topologyText(host.isCoord ? 'No live Bridge workers' : 'Sessions not collected')));
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
      const provenance = snapshotProvenance(data);
      const provenanceView = coordinatorStatus(provenance);

      const coordinatorName = coordinator.host_id || 'localhost';
      const coordinatorNode = coordinator.node_id ? String(coordinator.node_id) : '?';
      const coordBadge = document.getElementById('coordBadge');
      setText(
        coordBadge,
        uiText('{name} \u00b7 Node {node} \u00b7 {status}', {
          name: coordinatorName,
          node: coordinatorNode.substring(0, 8),
          status: uiText(provenanceView.label)
        })
      );
      coordBadge.title = uiText('{name} \u00b7 node {node} \u00b7 {detail}', {
        name: coordinatorName,
        node: coordinatorNode,
        detail: uiText(provenanceView.detail)
      });

      const onlineHosts = typeof metrics.online_hosts === 'number' ? metrics.online_hosts : 0;
      const totalHosts = typeof metrics.total_hosts === 'number' ? metrics.total_hosts : 1;
      const statNodes = document.getElementById('statNodes');
      statNodes.classList.remove('is-unknown');
      if (provenance.brokerConnected === true) {
        statNodes.textContent = formatUiCount(onlineHosts) + '/' + formatUiCount(totalHosts);
        statNodes.title = uiText('{online} of {total} configured hosts are online.', {
          online: formatUiCount(onlineHosts), total: formatUiCount(totalHosts)
        });
      } else if (provenance.brokerConnected === false) {
        const remoteHosts = Array.isArray(data.hosts) ? data.hosts : [];
        const remoteOnline = remoteHosts.filter((host) => host && host.state === 'online').length;
        statNodes.textContent = formatUiCount(Math.min(remoteOnline, totalHosts)) + '/' + formatUiCount(totalHosts);
        statNodes.title = uiText(
          'Local broker is unavailable; coordinator status is not verified. Remote hosts online: {online} of {remote}.',
          { online: formatUiCount(remoteOnline), remote: formatUiCount(Math.max(0, totalHosts - 1)) }
        );
        statNodes.classList.add('is-unknown');
      } else if (provenance.source === 'local_db_fallback') {
        statNodes.textContent = '\u2014';
        statNodes.title = uiText('The snapshot came from a local database fallback; broker and coordinator status are unknown.');
        statNodes.classList.add('is-unknown');
      } else {
        statNodes.textContent = '\u2014';
        statNodes.title = uiText('Broker and coordinator provenance is not reported; host online status is unknown.');
        statNodes.classList.add('is-unknown');
      }
      document.getElementById('statActive').textContent = formatUiCount(metrics.active_sessions || 0);
      const statNative = document.getElementById('statNative');
      if (statNative) statNative.textContent = formatUiCount(metrics.native_sessions || 0);
      document.getElementById('statTasks').textContent = formatUiCount(metrics.running_tasks || 0);
      document.getElementById('statBroadcasts').textContent = formatUiCount(metrics.total_broadcasts || 0);

      const costComplete = metrics.cost_complete !== false && !aggregateHasCostGaps(data);
      const totalCostNum = typeof metrics.total_cost_usd === 'number' ? metrics.total_cost_usd : null;
      const hasAttempts = aggregateHasAttempts(data);
      const knownCostAttempts = typeof metrics.known_cost_attempt_sessions === 'number'
        ? metrics.known_cost_attempt_sessions
        : undefined;
      const cost = formatCost(totalCostNum, costComplete, hasAttempts, 4, knownCostAttempts);
      const statCost = document.getElementById('statCost');
      statCost.textContent = uiText(cost.text);
      statCost.title = uiText(cost.title);
      statCost.classList.toggle('is-unknown', cost.unknown);

      renderApprovals(data.pending_questions || []);
      renderRuntimes(data.runtimes || {});
      renderHosts(data.hosts || [], coordinator, provenance);
      renderSessions(data.sessions || [], metrics);
      renderNativeSessions(data.native_sessions || [], metrics);
      renderBroadcasts(data.broadcasts || []);
      renderEvents(data.events || [], data.sessions || []);
      const topoData = (selectedRuntime === 'all' && !activeOnly)
        ? data
        : { ...data, sessions: (data.sessions || []).filter(sessionMatchesFilter) };
      renderTopology(topoData);
    }

    async function init() {
      document.getElementById('main').setAttribute('aria-busy', 'true');
      updateConnectionStatus();
      const authenticated = await loadSession();
      if (!authenticated) {
        window.location.href = '/login';
        return;
      }
      await fetchData();
      window.setInterval(fetchData, 2000);
      window.setInterval(updateConnectionStatus, 1000);
      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') fetchData();
      });
    }

    document.querySelectorAll('#runtimeFilterBar .chip-btn').forEach((btn) => {
      btn.addEventListener('click', () => setRuntimeFilter(btn.dataset.runtime || 'all'));
    });
    const activeOnlyBtn = document.getElementById('activeOnlyBtn');
    function onUiLanguageChanged() {
      updateConnectionStatus();
      retranslateConnectionBanner();
      if (activeOnlyBtn) {
        setText(activeOnlyBtn, uiText(activeOnly ? 'Active only: On' : 'Active only'));
        activeOnlyBtn.title = uiText('Hide stopped sessions and daemon processes');
      }
      if (currentData) renderDashboard(currentData);
    }
    if (activeOnlyBtn) {
      activeOnlyBtn.addEventListener('click', () => {
        activeOnly = !activeOnly;
        activeOnlyBtn.classList.toggle('active', activeOnly);
        activeOnlyBtn.setAttribute('aria-pressed', String(activeOnly));
        setText(activeOnlyBtn, uiText(activeOnly ? 'Active only: On' : 'Active only'));
        if (currentData) renderDashboard(currentData);
      });
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
    initializeUiLanguage();
    init();
  </script>
</body>
</html>
"""
HTML_PAGE = (_HTML_HEAD + _HTML_BODY + _HTML_SCRIPT).replace("/*__WEB_I18N__*/", _WEB_I18N_SCRIPT)

LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="referrer" content="no-referrer">
  <title data-i18n="cli-agent-bridge | Sign in">cli-agent-bridge | Sign in</title>
  <style>
    :root {
      color-scheme: light;
      --bg:#f6f7f9; --card:#ffffff; --surface:#f0f2f5; --border:#d8dee4; --text:#1f2328; --muted:#59636e;
      --accent:#0969da; --red:#cf222e; --shadow:0 1px 2px rgba(31,35,40,.05);
    }
    @media (prefers-color-scheme: dark) {
      :root {
        color-scheme: dark;
        --bg:#0d1117; --card:#161b22; --surface:#1c2128; --border:#30363d; --text:#e6edf3; --muted:#9ba7b4;
        --accent:#58a6ff; --red:#f85149; --shadow:0 1px 2px rgba(0,0,0,.32);
      }
    }
    * { box-sizing:border-box; margin:0; padding:0; }
    body { min-height:100vh; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif; font-size:14px; line-height:1.5; }
    main { min-height:100vh; display:flex; align-items:center; justify-content:center; padding:24px; }
    .card { width:100%; max-width:380px; background:var(--card); border:1px solid var(--border); border-radius:10px; box-shadow:var(--shadow); padding:28px; }
    h1 { font-size:19px; line-height:1.3; margin-bottom:6px; }
    p { color:var(--muted); margin-bottom:20px; line-height:1.55; }
    label { display:block; font-weight:700; margin-bottom:6px; }
    input { width:100%; min-height:44px; padding:10px 12px; border-radius:7px; border:1px solid var(--border); background:var(--surface); color:var(--text); font-size:16px; }
    input:focus-visible { outline:2px solid var(--accent); outline-offset:2px; }
    button { width:100%; min-height:44px; margin-top:16px; padding:10px 12px; border:1px solid transparent; border-radius:7px; background:var(--accent); color:#fff; font-size:14px; font-weight:700; cursor:pointer; }
    button:hover { filter:brightness(1.06); }
    button:focus-visible { outline:2px solid var(--accent); outline-offset:2px; }
    .error { min-height:21px; margin-top:12px; color:var(--red); overflow-wrap:anywhere; }
    .hint { margin-top:18px; color:var(--muted); font-size:12px; line-height:1.55; }
    .language-row { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:18px; }
    .language-label { margin:0; color:var(--muted); font-size:12px; font-weight:700; }
    .language-select {
      min-height:34px; padding:4px 8px; border-radius:7px; border:1px solid var(--border);
      background:var(--surface); color:var(--text); font-size:12px; font-weight:600; cursor:pointer;
    }
    .language-select:hover { border-color:var(--accent); }
    .language-select option { background:var(--card); color:var(--text); }
    @media (prefers-reduced-motion: reduce) { *, *::before, *::after { transition:none !important; animation:none !important; } }
  </style>
</head>
<body>
  <main>
    <form class="card" id="loginForm" autocomplete="on">
      <div class="language-row">
        <label class="language-label" for="languageSelect" data-i18n="Language">Language</label>
        <select class="language-select" id="languageSelect" aria-label="Language" data-i18n-aria-label="Language">
          <option value="auto" data-i18n="Auto">Auto</option>
          <option value="en">English</option>
          <option value="zh-CN">简体中文</option>
        </select>
      </div>
      <h1 data-i18n="cli-agent-bridge Monitor">cli-agent-bridge Monitor</h1>
      <p data-i18n="Sign in with the local admin token. The token is read from the private state directory and is never stored in the page.">Sign in with the local admin token. The token is read from the private state directory and is never stored in the page.</p>
      <label for="token" data-i18n="Admin token">Admin token</label>
      <input id="token" name="token" type="password" autocomplete="current-password" autocapitalize="none" spellcheck="false" aria-describedby="loginHint" placeholder="Enter admin token" data-i18n-placeholder="Enter admin token" required>
      <button type="submit" data-i18n="Sign in">Sign in</button>
      <div class="error" id="loginError" role="alert" aria-live="polite"></div>
      <div class="hint" id="loginHint" data-i18n="Use SSH port forwarding or an HTTPS reverse proxy on untrusted networks.">Use SSH port forwarding or an HTTPS reverse proxy on untrusted networks.</div>
    </form>
  </main>
  <script>
    'use strict';
/*__WEB_I18N__*/
    const form = document.getElementById('loginForm');
    const tokenInput = document.getElementById('token');
    const errorBox = document.getElementById('loginError');
    function showLoginError(message) {
      errorBox.dataset.i18nSource = message;
      errorBox.textContent = translateUi(message);
    }
    function onUiLanguageChanged() {
      if (errorBox.dataset.i18nSource) errorBox.textContent = translateUi(errorBox.dataset.i18nSource);
    }
    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      errorBox.textContent = '';
      delete errorBox.dataset.i18nSource;
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
        showLoginError(payload.error || 'Sign in failed');
      } catch (error) {
        showLoginError('Sign in failed');
      }
    });
    initializeUiLanguage();
  </script>
</body>
</html>
"""
LOGIN_PAGE = LOGIN_PAGE.replace("/*__WEB_I18N__*/", _WEB_I18N_SCRIPT)


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


def loopback_allowed_hosts(port: int | None = None) -> set[str]:
    """Build the default Host allowlist for a loopback listener's actual port."""
    hosts = {"127.0.0.1", "localhost", "[::1]"}
    if port is not None:
        hosts.update(f"{name}:{port}" for name in ("127.0.0.1", "localhost", "[::1]"))
    return hosts


HTTP_REQUEST_TIMEOUT_SECONDS = 10.0
MAX_CONCURRENT_HTTP_REQUESTS = 64


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


class _DeadlineReader:
    """Buffered request reader with one absolute deadline for header parsing."""

    def __init__(self, reader: Any, connection: Any, deadline: float) -> None:
        self._reader = reader
        self._connection = connection
        self.deadline = deadline

    def _arm(self) -> None:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("request deadline exceeded")
        settimeout = getattr(self._connection, "settimeout", None)
        if settimeout is not None:
            try:
                settimeout(remaining)
            except (OSError, ValueError):
                pass

    def readline(self, size: int | None = -1) -> bytes:
        if size == 0:
            return b""
        limit = size if size is not None and size > 0 else None
        data = bytearray()
        while limit is None or len(data) < limit:
            self._arm()
            chunk = self._reader.read(1)
            if not chunk:
                break
            data.extend(chunk)
            if chunk == b"\n":
                break
        return bytes(data)

    def read1(self, size: int = -1) -> bytes:
        self._arm()
        return self._reader.read1(size)

    def read(self, size: int = -1) -> bytes:
        self._arm()
        return self._reader.read(size)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._reader, name)


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    """Threaded HTTP server with finite request reads and a hard client cap."""

    def __init__(
        self,
        server_address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        *,
        max_concurrent_requests: int | None = None,
        request_timeout: float | None = None,
        **server_options: Any,
    ) -> None:
        if max_concurrent_requests is None:
            max_concurrent_requests = MAX_CONCURRENT_HTTP_REQUESTS
        if request_timeout is None:
            request_timeout = HTTP_REQUEST_TIMEOUT_SECONDS
        if max_concurrent_requests < 1:
            raise ValueError("max_concurrent_requests must be positive")
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.request_timeout = request_timeout
        self._request_slots = threading.BoundedSemaphore(max_concurrent_requests)
        self._request_deadlines: dict[int, float] = {}
        self._request_deadlines_lock = threading.Lock()
        super().__init__(server_address, handler, **server_options)

    def _remember_request_deadline(self, request: Any) -> None:
        with self._request_deadlines_lock:
            self._request_deadlines[id(request)] = time.monotonic() + self.request_timeout

    def _forget_request_deadline(self, request: Any) -> None:
        with self._request_deadlines_lock:
            self._request_deadlines.pop(id(request), None)

    def take_request_deadline(self, request: Any) -> float | None:
        with self._request_deadlines_lock:
            return self._request_deadlines.pop(id(request), None)

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        try:
            request.settimeout(self.request_timeout)
        except (OSError, ValueError):
            self.shutdown_request(request)
            return
        self._remember_request_deadline(request)
        if not self._request_slots.acquire(blocking=False):
            self._reject_overloaded(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._forget_request_deadline(request)
            self._request_slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()

    def close_request(self, request: Any) -> None:
        self._forget_request_deadline(request)
        super().close_request(request)

    def _reject_overloaded(self, request: socket.socket) -> None:
        try:
            request.sendall(
                b"HTTP/1.1 503 Service Unavailable\r\n"
                b"Connection: close\r\n"
                b"Content-Length: 0\r\n"
                b"Cache-Control: no-store\r\n\r\n"
            )
        except OSError:
            pass
        finally:
            self.shutdown_request(request)

    def handle_error(self, request: socket.socket, client_address: Any) -> None:
        # Never print handler tracebacks: they can include request details and credentials.
        logger.debug("Web monitor request failed")


class WebMonitorHandler(BaseHTTPRequestHandler):
    collector: MonitorCollector | None = None
    state: Path | None = None
    allowed_hosts: set[str] = set(DEFAULT_ALLOWED_HOSTS)
    public_origin: str | None = None
    sessions: WebSessionManager = WebSessionManager()
    server_version = "AgentBridgeMonitor/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Never log headers, cookies or bodies.

    def setup(self) -> None:
        super().setup()
        timeout = float(getattr(self.server, "request_timeout", HTTP_REQUEST_TIMEOUT_SECONDS))
        take_deadline = getattr(self.server, "take_request_deadline", None)
        deadline = take_deadline(self.connection) if take_deadline is not None else None
        if deadline is None:
            deadline = time.monotonic() + timeout
        self._request_timeout = timeout
        self._first_request = True
        self._deadline_reader = _DeadlineReader(self.rfile, self.connection, deadline)
        self.rfile = self._deadline_reader

    def handle_one_request(self) -> None:
        if not self._first_request:
            self._deadline_reader.deadline = time.monotonic() + self._request_timeout
        self._first_request = False
        super().handle_one_request()

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

    def _read_exact_body(self, length: int) -> bytes:
        if not length:
            return b""
        timeout = float(getattr(self.server, "request_timeout", HTTP_REQUEST_TIMEOUT_SECONDS))
        deadline = time.monotonic() + timeout
        if isinstance(self.rfile, _DeadlineReader):
            self.rfile.deadline = deadline
        body = bytearray()
        while len(body) < length:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.close_connection = True
                raise _HTTPError(408, "Request body timed out")
            settimeout = getattr(self.connection, "settimeout", None)
            if settimeout is not None:
                try:
                    settimeout(min(timeout, remaining))
                except (OSError, ValueError):
                    pass
            try:
                chunk = self.rfile.read1(min(length - len(body), 65536))
            except TimeoutError:
                self.close_connection = True
                raise _HTTPError(408, "Request body timed out") from None
            except OSError:
                self.close_connection = True
                raise _HTTPError(400, "Unable to read request body") from None
            if not chunk:
                break
            body.extend(chunk)
        return bytes(body)

    def _read_json_body(self) -> dict[str, Any]:
        length = self._declared_body_length()
        body = self._read_exact_body(length)
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
                params = self._query_params()
                self._send_json(200, self._collect_filtered_snapshot(params, default_limit=25))
            elif path == "/api/v1/health":
                if identity is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_api_v1_health()
            elif path == "/api/v1/runtimes":
                if identity is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_api_v1_runtimes()
            elif path == "/api/v1/sessions":
                if identity is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_api_v1_sessions()
            elif path == "/api/v1/events":
                if identity is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_api_v1_events()
            elif path == "/api/v1/stream":
                if identity is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_api_v1_stream()
            elif path == "/api/v1/schema":
                if identity is None:
                    raise _HTTPError(401, "Unauthorized")
                self._handle_api_v1_schema()
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
            elif path in (
                "/",
                "/index.html",
                "/auth/session",
                "/api/snapshot",
                "/api/v1/health",
                "/api/v1/runtimes",
                "/api/v1/sessions",
                "/api/v1/events",
                "/api/v1/stream",
                "/api/v1/schema",
            ):
                raise _HTTPError(405, "Method Not Allowed")
            else:
                raise _HTTPError(404, "Not Found")
        except _HTTPError as error:
            self._send_json(error.status, {"ok": False, "error": str(error)})

    # ------------------------------------------------------------------ external v1 API

    def _query_params(self) -> dict[str, list[str]]:
        return parse_qs(urlsplit(self.path).query, keep_blank_values=False)

    @staticmethod
    def _first_param(params: dict[str, list[str]], name: str) -> str | None:
        values = params.get(name)
        if not values:
            return None
        value = str(values[-1]).strip()
        return value or None

    @classmethod
    def _parse_bool_param(cls, params: dict[str, list[str]], name: str, default: bool) -> bool:
        raw = cls._first_param(params, name)
        if raw is None:
            return default
        lower = raw.lower()
        if lower in {"1", "true", "yes", "on"}:
            return True
        if lower in {"0", "false", "no", "off"}:
            return False
        raise _HTTPError(400, f"Invalid boolean parameter {name!r}")

    @classmethod
    def _parse_int_param(
        cls,
        params: dict[str, list[str]],
        name: str,
        default: int,
        *,
        minimum: int = 0,
        maximum: int = 1000,
    ) -> int:
        raw = cls._first_param(params, name)
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError:
            raise _HTTPError(400, f"Invalid integer parameter {name!r}") from None
        if value < minimum or value > maximum:
            raise _HTTPError(400, f"Parameter {name!r} must be between {minimum} and {maximum}")
        return value

    def _require_collector(self) -> MonitorCollector:
        if self.collector is None:
            raise _HTTPError(500, "Collector not configured")
        return self.collector

    def _parse_filter_params(
        self,
        params: dict[str, list[str]],
        *,
        default_limit: int = 25,
        max_limit: int = 500,
        default_events: bool = True,
    ) -> dict[str, Any]:
        limit = self._parse_int_param(params, "limit", default_limit, minimum=1, maximum=max_limit)
        session_limit_raw = self._first_param(params, "session_limit")
        session_limit: int | None = None
        if session_limit_raw is not None:
            session_limit = self._parse_int_param(params, "session_limit", limit, minimum=1, maximum=1000)
        include_events = self._parse_bool_param(params, "include_events", default_events)
        include_native = self._parse_bool_param(params, "include_native", True)
        runtime_raw = self._first_param(params, "runtime")
        try:
            runtime = parse_runtime_filter(runtime_raw)
        except ValueError as error:
            raise _HTTPError(400, str(error)) from None
        source = self._first_param(params, "source")
        if source is not None:
            source = source.lower()
            if source not in {"all", "bridge", "native"}:
                raise _HTTPError(400, f"Unsupported source {source!r}")
        status_raw = self._first_param(params, "status")
        try:
            status = normalize_status_filter(status_raw)
        except ValueError as error:
            raise _HTTPError(400, str(error)) from None
        after_seq = None
        if self._first_param(params, "after_seq") is not None:
            after_seq = self._parse_int_param(params, "after_seq", 0, minimum=0, maximum=10**12)
        return {
            "limit": limit,
            "session_limit": session_limit,
            "include_events": include_events,
            "include_native": include_native,
            "runtime": runtime,
            "source": source,
            "status": status,
            "after_seq": after_seq,
        }

    def _upgrade_snapshot(
        self,
        snapshot: dict[str, Any],
        params: dict[str, list[str]],
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Backfill multi-runtime & native discovery if an older broker daemon replied."""
        if not isinstance(snapshot, dict) or "sessions" not in snapshot:
            return snapshot
        if snapshot.get("collector_version") and "runtimes" in snapshot and "native_sessions" in snapshot:
            upgraded = dict(snapshot)
        elif snapshot.get("legacy_broker") and "runtimes" in snapshot and "native_sessions" in snapshot:
            upgraded = dict(snapshot)
        else:
            collector = self.collector
            proc_root = getattr(collector, "proc_root", None) if collector is not None else None
            native_home = getattr(collector, "native_home", None) if collector is not None else None
            include_native = self._parse_bool_param(params, "include_native", True)

            sessions = list(snapshot.get("sessions") or [])
            live_pids: set[int] = set()
            live_native_ids: set[tuple[str, str]] = set()
            for sess in sessions:
                rt = normalize_runtime(sess.get("runtime")) or sess.get("runtime") or ""
                sess["runtime"] = rt
                sess.setdefault("display_runtime", display_runtime(rt))
                sess.setdefault("source", "bridge")
                sess.setdefault("managed", True)
                wpid = sess.get("worker_pid")
                obs = sess.get("observed")
                is_live = obs in ("running", "idle", "waiting_input", "starting") or bool(sess.get("alive"))
                if sess.get("alive") and isinstance(wpid, int) and wpid > 0:
                    live_pids.add(wpid)
                nid = sess.get("native_id")
                if is_live and nid and rt:
                    live_native_ids.add((rt, str(nid)))

            native_sessions: list[dict[str, Any]] = []
            if include_native and "native_sessions" not in snapshot:
                native_sessions = collect_native_sessions(
                    proc_root=proc_root,
                    home=native_home,
                    exclude_pids=live_pids,
                    exclude_native_ids=live_native_ids,
                )
            elif include_native and "native_sessions" in snapshot:
                native_sessions = list(snapshot.get("native_sessions") or [])

            metrics = dict(snapshot.get("metrics") or {})
            runtimes = summarize_runtimes(sessions, native_sessions)
            metrics.setdefault("native_sessions", sum(1 for n in native_sessions if n.get("kind") != "daemon"))
            metrics.setdefault("native_workers", sum(1 for n in native_sessions if n.get("kind") == "worker"))
            metrics.setdefault("native_daemons", sum(1 for n in native_sessions if n.get("kind") == "daemon"))
            metrics.setdefault(
                "total_all_active",
                int(metrics.get("active_sessions") or 0) + int(metrics.get("native_sessions") or 0),
            )
            events = list(snapshot.get("events") or [])
            if events and "latest_event_seq" not in metrics:
                metrics["latest_event_seq"] = max((int(ev.get("seq", 0)) for ev in events), default=0)

            upgraded = dict(snapshot)
            upgraded["sessions"] = sessions
            upgraded["native_sessions"] = native_sessions
            upgraded["runtimes"] = runtimes
            upgraded["metrics"] = metrics
            if not snapshot.get("collector_version"):
                upgraded["legacy_broker"] = True

        active_filters = filters or self._parse_filter_params(params)
        if any(
            (
                active_filters.get("runtime"),
                active_filters.get("source") not in (None, "all"),
                active_filters.get("status") not in (None, "all"),
                active_filters.get("session_limit") is not None,
                active_filters.get("after_seq") is not None,
            )
        ):
            upgraded = filter_snapshot_dict(
                upgraded,
                runtime=active_filters.get("runtime"),
                source=active_filters.get("source"),
                status=active_filters.get("status"),
                session_limit=active_filters.get("session_limit"),
                after_seq=active_filters.get("after_seq"),
                event_limit=active_filters.get("limit"),
            )
        return upgraded

    def _collect_filtered_snapshot_with_meta(
        self,
        params: dict[str, list[str]],
        *,
        default_limit: int = 25,
        max_limit: int = 500,
        default_events: bool = True,
        parsed_filters: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        collector = self._require_collector()
        filters = parsed_filters or self._parse_filter_params(
            params,
            default_limit=default_limit,
            max_limit=max_limit,
            default_events=default_events,
        )
        orig_session_limit = filters.get("session_limit")
        withhold_session_limit = (
            filters.get("status") not in (None, "all") and orig_session_limit is not None
        )
        collect_filters = (
            {**filters, "session_limit": None} if withhold_session_limit else filters
        )

        kwargs: dict[str, Any] = {
            "limit": collect_filters["limit"],
            "include_events": collect_filters["include_events"],
        }
        if collect_filters["runtime"] is not None:
            kwargs["runtime"] = collect_filters["runtime"]
        if collect_filters["source"] is not None and collect_filters["source"] != "all":
            kwargs["source"] = collect_filters["source"]
        if collect_filters["status"] is not None and collect_filters["status"] != "all":
            kwargs["status"] = collect_filters["status"]
        if collect_filters["session_limit"] is not None:
            kwargs["session_limit"] = collect_filters["session_limit"]
        if not collect_filters["include_native"]:
            kwargs["include_native"] = False
        if collect_filters["after_seq"] is not None:
            kwargs["after_seq"] = collect_filters["after_seq"]

        raw = collector.collect(**kwargs)
        upgraded = self._upgrade_snapshot(raw, params, filters=collect_filters)
        events_truncated_before_filter = False
        has_ev_filter = any(
            (
                collect_filters.get("runtime"),
                collect_filters.get("source") not in (None, "all"),
                collect_filters.get("status") not in (None, "all"),
                collect_filters.get("after_seq") is not None,
            )
        )
        wide_limit = max(int(collect_filters["limit"]), int(max_limit))
        post_events = list(upgraded.get("events") or [])
        widened = should_widen_events(
            raw,
            post_events,
            req_limit=collect_filters["limit"],
            max_limit=wide_limit,
            has_filter=has_ev_filter,
            source=collect_filters.get("source"),
            include_events=collect_filters["include_events"],
        )
        if widened:
            # Collector is either pre-v3 or had events dropped at the Web layer after hitting its page limit;
            # widen once to max_limit so matching/older events are not masked or silently skipped.
            wide_kwargs = dict(kwargs)
            wide_kwargs["limit"] = wide_limit
            raw = collector.collect(**wide_kwargs)
            upgraded = self._upgrade_snapshot(raw, params, filters=collect_filters)
            post_events = list(upgraded.get("events") or [])
        events_truncated_before_filter = is_event_window_truncated(
            raw,
            post_events,
            req_limit=collect_filters["limit"],
            wide_limit=wide_limit if widened else collect_filters["limit"],
            has_filter=has_ev_filter,
            source=collect_filters.get("source"),
            include_events=collect_filters["include_events"],
        )
        if widened and len(post_events) > collect_filters["limit"]:
            upgraded = {**upgraded, "events": post_events[: collect_filters["limit"]]}
        if withhold_session_limit and orig_session_limit is not None:
            upgraded = {
                **upgraded,
                "sessions": list(upgraded.get("sessions") or [])[:orig_session_limit],
                "native_sessions": list(upgraded.get("native_sessions") or [])[:orig_session_limit],
            }
        return upgraded, events_truncated_before_filter

    def _collect_filtered_snapshot(
        self,
        params: dict[str, list[str]],
        *,
        default_limit: int = 25,
        max_limit: int = 500,
        default_events: bool = True,
        parsed_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        upgraded, events_truncated = self._collect_filtered_snapshot_with_meta(
            params,
            default_limit=default_limit,
            max_limit=max_limit,
            default_events=default_events,
            parsed_filters=parsed_filters,
        )
        if events_truncated:
            upgraded = {**upgraded, "events_truncated": True}
        return upgraded

    def _handle_api_v1_health(self) -> None:
        params = self._query_params()
        include_native = self._parse_bool_param(params, "include_native", True)
        collector = self._require_collector()
        snapshot = self._upgrade_snapshot(
            collector.collect(limit=1, include_events=False, include_native=include_native),
            params,
        )
        self._send_json(
            200,
            {
                "ok": True,
                "version": "1.0",
                "timestamp": snapshot.get("timestamp"),
                "coordinator": snapshot.get("coordinator", {}),
                "metrics": snapshot.get("metrics", {}),
                "runtimes": snapshot.get("runtimes", {}),
            },
        )

    def _handle_api_v1_runtimes(self) -> None:
        params = self._query_params()
        include_native = self._parse_bool_param(params, "include_native", True)
        collector = self._require_collector()
        snapshot = self._upgrade_snapshot(
            collector.collect(limit=1, include_events=False, include_native=include_native),
            params,
        )
        self._send_json(
            200,
            {
                "ok": True,
                "timestamp": snapshot.get("timestamp"),
                "monitored_runtimes": list(MONITORED_RUNTIMES),
                "aliases": {"agy": "antigravity"},
                "runtimes": snapshot.get("runtimes", {}),
                "metrics": snapshot.get("metrics", {}),
            },
        )

    def _handle_api_v1_sessions(self) -> None:
        params = self._query_params()
        filters = self._parse_filter_params(
            params,
            default_limit=200,
            max_limit=1000,
            default_events=False,
        )
        offset = self._parse_int_param(params, "offset", 0, minimum=0, maximum=100000)
        sessions_filters = dict(filters)
        sessions_filters["session_limit"] = None
        snapshot = self._collect_filtered_snapshot(
            params,
            default_limit=200,
            max_limit=1000,
            default_events=False,
            parsed_filters=sessions_filters,
        )
        runtime = filters["runtime"]
        source = filters["source"] or "all"
        status = filters["status"] or "all"
        host_id = self._first_param(params, "host_id")
        limit = filters["limit"]
        if filters["session_limit"] is not None:
            limit = min(limit, filters["session_limit"])
        all_rows = unified_sessions(
            snapshot,
            runtime=runtime,
            source=source,
            status=status,
            host_id=host_id,
        )
        rows = all_rows[offset : offset + limit]
        self._send_json(
            200,
            {
                "ok": True,
                "timestamp": snapshot.get("timestamp"),
                "filters": {
                    "runtime": runtime or "all",
                    "source": source,
                    "status": status,
                    "host_id": host_id,
                    "offset": offset,
                    "limit": limit,
                },
                "count": len(rows),
                "total": len(all_rows),
                "offset": offset,
                "truncated": (offset + len(rows)) < len(all_rows),
                "runtimes": snapshot.get("runtimes", {}),
                "metrics": snapshot.get("metrics", {}),
                "sessions": rows,
            },
        )

    def _handle_api_v1_events(self) -> None:
        params = self._query_params()
        limit = self._parse_int_param(params, "limit", 50, minimum=1, maximum=500)
        after_seq = None
        if self._first_param(params, "after_seq") is not None:
            after_seq = self._parse_int_param(params, "after_seq", 0, minimum=0, maximum=10**12)
        try:
            runtime = parse_runtime_filter(self._first_param(params, "runtime"))
        except ValueError as error:
            raise _HTTPError(400, str(error)) from None
        raw_snapshot, events_truncated = self._collect_filtered_snapshot_with_meta(
            params,
            default_limit=50,
            max_limit=500,
            default_events=True,
            parsed_filters={
                "limit": limit,
                "session_limit": None,
                "runtime": runtime,
                "source": None,
                "status": None,
                "include_native": False,
                "include_events": True,
                "after_seq": after_seq,
            },
        )
        is_legacy = bool(
            raw_snapshot.get("legacy_broker")
            or (not raw_snapshot.get("collector_version") and "runtimes" not in raw_snapshot)
        )
        if (is_legacy or events_truncated) and after_seq is not None:
            msg = (
                "Connected broker daemon does not support incremental after_seq event queries; upgrade and restart the broker service"
                if is_legacy
                else "Collector event window truncated before filter; incremental after_seq queries require a server-side filtering collector"
            )
            raise _HTTPError(501, msg)
        if events_truncated:
            self._send_json(
                200,
                {
                    "ok": True,
                    "timestamp": raw_snapshot.get("timestamp"),
                    "after_seq": after_seq,
                    "next_seq": 0,
                    "count": 0,
                    "events": [],
                    "events_truncated": True,
                },
            )
            return
        events = list(raw_snapshot.get("events") or [])
        metrics = raw_snapshot.get("metrics") or {}
        max_event_seq = max((int(ev.get("seq", 0)) for ev in events), default=after_seq or 0)
        latest_db_seq = int(metrics.get("latest_event_seq") or 0)
        # Only jump cursor to latest_db_seq when not legacy and all matching events fit in this page.
        if not is_legacy and (after_seq is None or len(events) < limit):
            next_seq = max(max_event_seq, latest_db_seq, after_seq or 0)
        else:
            next_seq = max(max_event_seq, after_seq or 0)
        self._send_json(
            200,
            {
                "ok": True,
                "timestamp": raw_snapshot.get("timestamp"),
                "after_seq": after_seq,
                "next_seq": next_seq,
                "count": len(events),
                "events": events,
            },
        )

    def _handle_api_v1_schema(self) -> None:
        self._send_json(
            200,
            {
                "ok": True,
                "version": "1.0",
                "auth": {
                    "schemes": ["Authorization: Bearer <admin.token>", f"Cookie: {SESSION_COOKIE}=<id>"],
                    "csrf_required_for": ["POST /api/respond", "POST /api/host/ping"],
                    "cookie_origin_required_for": ["POST /api/respond", "POST /api/host/ping", "POST /auth/logout"],
                },
                "runtimes": [
                    {"id": rt, "display": display_runtime(rt)}
                    for rt in MONITORED_RUNTIMES
                ],
                "aliases": {"agy": "antigravity"},
                "endpoints": {
                    "GET /api/snapshot": {
                        "description": "Full monitor snapshot (backward-compatible) with optional runtime/source/status/session_limit/include_native/after_seq query filters.",
                    },
                    "GET /api/v1/health": {
                        "description": "Lightweight health, coordinator identity, metrics, and per-runtime counters.",
                    },
                    "GET /api/v1/runtimes": {
                        "description": "Per-runtime inventory across pi, claude, antigravity (agy), and codex.",
                    },
                    "GET /api/v1/sessions": {
                        "description": "Unified Bridge + native worker/session feed.",
                        "query": ["runtime=all|pi|claude|antigravity|agy|codex", "source=all|bridge|native", "status=all|active|running|idle|waiting|waiting_input|stopped", "host_id", "offset", "limit"],
                    },
                    "GET /api/v1/events": {
                        "description": "Bridge event feed ordered by seq. Without after_seq, returns the latest limit events; pass after_seq (e.g. after_seq=0 for full history) to paginate ascending without skipping events.",
                        "query": ["after_seq", "runtime", "limit"],
                    },
                    "GET /api/v1/stream": {
                        "description": "Server-Sent Events (SSE) stream emitting snapshot, event, and cursor frames. Non-legacy snapshot frames contain current state with events=[] while matching events arrive strictly as individual event frames (initial un-cursored connections emit the latest tail limit events and advance from that window; pass after_seq=0 or Last-Event-ID for strict gapless replay).",
                        "query": ["after_seq", "limit", "runtime", "source", "status", "interval", "once=1", "max_events", "max_seconds"],
                    },
                    "GET /api/v1/schema": {
                        "description": "Machine-readable schema describing monitor API endpoints and supported runtimes.",
                    },
                },
            },
        )

    def _handle_api_v1_stream(self) -> None:
        global _ACTIVE_SSE_CLIENTS
        params = self._query_params()
        self._require_collector()
        once = self._parse_bool_param(params, "once", False)
        max_events = self._parse_int_param(params, "max_events", 0, minimum=0, maximum=10000)
        interval_raw = self._first_param(params, "interval")
        interval = 1.0
        if interval_raw is not None:
            try:
                interval = float(interval_raw)
            except ValueError:
                raise _HTTPError(400, "Invalid interval parameter") from None
            if interval <= 0:
                raise _HTTPError(400, "interval must be positive")
            interval = max(0.2, min(10.0, interval))
        max_seconds_raw = self._first_param(params, "max_seconds")
        max_seconds = SSE_MAX_LIFETIME_SECONDS
        if max_seconds_raw is not None:
            try:
                parsed_seconds = float(max_seconds_raw)
            except ValueError:
                raise _HTTPError(400, "Invalid max_seconds parameter") from None
            if parsed_seconds <= 0:
                raise _HTTPError(400, "max_seconds must be positive")
            max_seconds = max(0.2, min(SSE_MAX_LIFETIME_SECONDS, parsed_seconds))

        # Validate filter params before opening the 200 text/event-stream response.
        poll_limit = 50
        filters = self._parse_filter_params(
            params,
            default_limit=poll_limit,
            max_limit=500,
            default_events=True,
        )
        if not filters["include_events"]:
            raise _HTTPError(400, "include_events=false is not supported on /api/v1/stream")
        runtime = filters["runtime"]
        last_event_id: int | None = None
        last_event_id_raw = self.headers.get("Last-Event-ID")
        if last_event_id_raw is not None and last_event_id_raw.strip() != "":
            try:
                parsed_id = int(last_event_id_raw.strip())
            except ValueError:
                raise _HTTPError(400, "Invalid Last-Event-ID header") from None
            if parsed_id < 0 or parsed_id > 10**12:
                raise _HTTPError(400, "Last-Event-ID out of range")
            last_event_id = parsed_id
        # Browser EventSource reconnects with Last-Event-ID while keeping original URL query params.
        initial_after_seq = last_event_id if last_event_id is not None else filters["after_seq"]

        def stream_authorized() -> bool:
            if self.state is None:
                return True
            live_token = self._load_token()
            return live_token is not None and self._identity(live_token) is not None

        poll_session_limit = filters["session_limit"]
        poll_status_filter = filters.get("status")
        needs_full_sessions_for_status = (
            poll_session_limit is not None and poll_status_filter not in (None, "all")
        )

        def collect_for_poll(after_cursor: int | None) -> tuple[dict[str, Any], bool]:
            poll_filters = dict(filters)
            poll_filters["after_seq"] = after_cursor
            poll_params = dict(params)
            if needs_full_sessions_for_status:
                poll_filters["session_limit"] = None
                poll_params.pop("session_limit", None)
            if after_cursor is not None:
                poll_params["after_seq"] = [str(after_cursor)]
            else:
                poll_params.pop("after_seq", None)
            return self._collect_filtered_snapshot_with_meta(
                poll_params,
                default_limit=poll_limit,
                max_limit=500,
                default_events=True,
                parsed_filters=poll_filters,
            )

        with _SSE_CLIENTS_LOCK:
            if _ACTIVE_SSE_CLIENTS >= MAX_SSE_CLIENTS:
                raise _HTTPError(503, "Too many active SSE stream connections")
            _ACTIVE_SSE_CLIENTS += 1

        try:
            initial_snap, initial_events_truncated = collect_for_poll(initial_after_seq)
            initial_legacy = bool(
                initial_snap.get("legacy_broker")
                or (not initial_snap.get("collector_version") and "runtimes" not in initial_snap)
            )
            if (initial_legacy or initial_events_truncated) and (initial_after_seq is not None or not once):
                msg = (
                    "Connected broker daemon does not support reliable incremental SSE streaming; upgrade and restart the broker service"
                    if initial_legacy
                    else "Collector event window truncated before filter; incremental SSE streaming requires a server-side filtering collector"
                )
                raise _HTTPError(501, msg)
            if not stream_authorized():
                raise _HTTPError(401, "Unauthorized")

            try:
                self.connection.settimeout(SSE_SOCKET_TIMEOUT_SECONDS)
            except OSError:
                pass

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            last_write_at = time.monotonic()
            started_at = last_write_at

            def write_sse(event_name: str, payload: dict[str, Any], event_id: str | int | None = None) -> None:
                nonlocal last_write_at
                lines: list[str] = []
                if event_id is not None:
                    lines.append(f"id: {event_id}")
                lines.append(f"event: {event_name}")
                lines.append(f"data: {json.dumps(payload, ensure_ascii=False)}")
                lines.append("")
                self.wfile.write(("\n".join(lines) + "\n").encode("utf-8"))
                self.wfile.flush()
                last_write_at = time.monotonic()

            def write_keepalive() -> None:
                nonlocal last_write_at
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
                last_write_at = time.monotonic()

            emitted = 0
            last_digest: str | None = None
            last_seq = initial_after_seq or 0
            client_cursor_seq: int | None = initial_after_seq
            has_cursor = initial_after_seq is not None
            pending_initial: tuple[dict[str, Any], bool] | None = (initial_snap, initial_events_truncated)

            try:
                while True:
                    if time.monotonic() - started_at >= max_seconds:
                        break
                    if not stream_authorized():
                        break
                    prev_last_seq = last_seq
                    if pending_initial is not None:
                        snap, events_truncated_before_filter = pending_initial
                        pending_initial = None
                    else:
                        snap, events_truncated_before_filter = collect_for_poll(last_seq if has_cursor else None)
                    # Re-check auth AFTER blocking collect() before flushing any sensitive frame.
                    if not stream_authorized():
                        break

                    snap_legacy = bool(
                        snap.get("legacy_broker")
                        or (not snap.get("collector_version") and "runtimes" not in snap)
                    )
                    latest_db_seq = int((snap.get("metrics") or {}).get("latest_event_seq") or 0)
                    source_filter = filters.get("source")
                    status_filter = filters.get("status")
                    allowed_session_ids = (
                        {
                            s.get("id")
                            for s in (snap.get("sessions") or [])
                            if s.get("id") and matches_bridge_status(s, status_filter)
                        }
                        if status_filter and status_filter != "all"
                        else None
                    )
                    if poll_session_limit is not None and needs_full_sessions_for_status:
                        snap = {
                            **snap,
                            "sessions": list(snap.get("sessions") or [])[:poll_session_limit],
                            "native_sessions": list(snap.get("native_sessions") or [])[:poll_session_limit],
                        }
                    raw_events = (
                        []
                        if (source_filter == "native" or events_truncated_before_filter)
                        else list(snap.get("events") or [])
                    )
                    events = [
                        ev
                        for ev in raw_events
                        if int(ev.get("seq", 0)) > last_seq
                        and not (runtime and ev.get("runtime") and ev.get("runtime") != runtime)
                        and (allowed_session_ids is None or ev.get("session_id") in allowed_session_ids)
                    ]
                    events.sort(key=lambda item: int(item.get("seq", 0)))

                    # Only advance the cursor pre-snapshot when no pending matching events await delivery
                    # and this poll did not hit a page boundary (before or after Web-layer filtering).
                    can_advance_past_db = (
                        not snap_legacy
                        and not events
                        and not events_truncated_before_filter
                        and len(raw_events) < filters["limit"]
                    )
                    if can_advance_past_db:
                        last_seq = max(last_seq, latest_db_seq)

                    if snap_legacy:
                        snapshot_event_id: int | None = None
                    elif events and not has_cursor:
                        # Never pre-acknowledge un-sent events if the client disconnects after snapshot.
                        snapshot_event_id = max(last_seq, int(events[0].get("seq", 0)) - 1)
                    else:
                        snapshot_event_id = last_seq

                    digest_metrics = snap.get("metrics")
                    if isinstance(digest_metrics, dict):
                        digest_metrics = {k: v for k, v in digest_metrics.items() if k != "latest_event_seq"}

                    def _strip_relative(val: Any) -> Any:
                        if isinstance(val, dict):
                            return {k: _strip_relative(v) for k, v in val.items() if not k.endswith("_rel")}
                        if isinstance(val, list):
                            return [_strip_relative(item) for item in val]
                        return val

                    digest_basis = _strip_relative(
                        {
                            "coordinator": snap.get("coordinator"),
                            "hosts": snap.get("hosts"),
                            "groups": snap.get("groups"),
                            "broadcasts": snap.get("broadcasts"),
                            "peer_queues": snap.get("peer_queues"),
                            "metrics": digest_metrics,
                            "runtimes": snap.get("runtimes"),
                            "sessions": snap.get("sessions"),
                            "native_sessions": snap.get("native_sessions"),
                            "pending_questions": snap.get("pending_questions"),
                        }
                    )
                    digest = hashlib.sha256(
                        json.dumps(digest_basis, sort_keys=True, ensure_ascii=False).encode("utf-8")
                    ).hexdigest()
                    if digest != last_digest:
                        if not stream_authorized():
                            return
                        # Deliver state in snapshot (with events=[] on non-legacy or truncated streams) and stream events as separate SSE frames
                        # so a disconnect after snapshot/partial events never replays already-processed inline snapshot events.
                        if events_truncated_before_filter:
                            snapshot_payload = {**snap, "events": [], "events_truncated": True}
                        elif snap_legacy:
                            snapshot_payload = snap
                        else:
                            snapshot_payload = {**snap, "events": []}
                        write_sse("snapshot", snapshot_payload, event_id=snapshot_event_id)
                        if snapshot_event_id is not None:
                            client_cursor_seq = snapshot_event_id
                        last_digest = digest
                        emitted += 1
                        if max_events and emitted >= max_events:
                            break
                    hit_max_events = False
                    for ev in events:
                        if not stream_authorized():
                            return
                        seq = int(ev.get("seq", 0))
                        last_seq = max(last_seq, seq)
                        write_sse("event", ev, event_id=seq)
                        client_cursor_seq = seq
                        emitted += 1
                        if max_events and emitted >= max_events:
                            hit_max_events = True
                            break
                    if snap_legacy:
                        break
                    if not hit_max_events and not events_truncated_before_filter:
                        raw_max_seq = max((int(ev.get("seq", 0)) for ev in raw_events), default=last_seq)
                        if len(raw_events) < filters["limit"]:
                            last_seq = max(last_seq, raw_max_seq, latest_db_seq)
                        elif not events and raw_events:
                            raw_min_seq = min(int(ev.get("seq", 0)) for ev in raw_events)
                            if raw_min_seq > prev_last_seq and raw_max_seq > last_seq:
                                last_seq = raw_max_seq
                    has_cursor = True
                    if (
                        not hit_max_events
                        and not (max_events and emitted >= max_events)
                        and (client_cursor_seq is None or last_seq > client_cursor_seq)
                    ):
                        if not stream_authorized():
                            return
                        write_sse("cursor", {"last_seq": last_seq}, event_id=last_seq)
                        client_cursor_seq = last_seq
                        emitted += 1
                        if max_events and emitted >= max_events:
                            hit_max_events = True
                    if once or events_truncated_before_filter or hit_max_events or (max_events and emitted >= max_events):
                        break
                    if len(raw_events) >= filters["limit"] and last_seq > prev_last_seq:
                        continue
                    now = time.monotonic()
                    if now - last_write_at >= SSE_KEEPALIVE_SECONDS:
                        write_keepalive()
                    remaining = max_seconds - (time.monotonic() - started_at)
                    if remaining <= 0:
                        break
                    time.sleep(min(interval, remaining))
            except (_HTTPError, BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                return
        finally:
            with _SSE_CLIENTS_LOCK:
                _ACTIVE_SSE_CLIENTS = max(0, _ACTIVE_SSE_CLIENTS - 1)

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
    hosts = normalize_allowed_hosts(allowed_hosts) if allowed_hosts is not None else None

    WebMonitorHandler.collector = MonitorCollector(state_dir)
    WebMonitorHandler.state = state_dir
    WebMonitorHandler.public_origin = public_origin
    WebMonitorHandler.sessions = WebSessionManager()

    server = BoundedThreadingHTTPServer((host, port), WebMonitorHandler)
    bound_port = int(server.server_address[1])
    if hosts is None:
        hosts = loopback_allowed_hosts(bound_port)
    WebMonitorHandler.allowed_hosts = hosts
    url = f"http://{host}:{bound_port}"
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
