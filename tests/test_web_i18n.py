"""Offline checks for the embedded dashboard and login English/Chinese UI."""

from __future__ import annotations

import html
import json
import re
import shutil
import subprocess
import unittest
from collections import Counter
from pathlib import Path

from agent_bridge import i18n_web
from agent_bridge.i18n import get_catalog, resolve_language, translate
from agent_bridge.i18n_web import MESSAGES
from agent_bridge.web_monitor import HTML_PAGE, LOGIN_PAGE, _script_json

NODE = shutil.which("node")
LANGUAGE_STORAGE_KEY = "cli-agent-bridge.web-language"
DASHBOARD_ERROR_KEY = (
    "Could not refresh the dashboard. Showing the last snapshot while retrying."
)

PAGE_RUNNER = r"""
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));

function decodeEntities(value) {
  return String(value)
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&hellip;/g, '\u2026')
    .replace(/&middot;/g, '\u00b7')
    .replace(/&mdash;/g, '\u2014')
    .replace(/&amp;/g, '&');
}

function datasetName(name) {
  return name.slice(5).replace(/-([a-z])/g, (match, letter) => letter.toUpperCase());
}

const allElements = [];
const byId = new Map();

function markAttached(node, value) {
  node._attached = value;
  if (node.nodeType === 1) node.children.forEach((child) => markAttached(child, value));
}

function makeNode(tag, attributes, attached) {
  const node = {
    nodeType: 1,
    tagName: String(tag).toUpperCase(),
    localName: String(tag).toLowerCase(),
    children: [],
    parentElement: null,
    attrs: {},
    dataset: {},
    style: {},
    listeners: {},
    _text: '',
    _classes: '',
    _attached: Boolean(attached),
    hidden: false,
    disabled: false,
    type: '',
    value: '',
    colSpan: 1
  };
  Object.entries(attributes || {}).forEach(([name, value]) => {
    node.attrs[name] = String(value);
    if (name === 'class') node._classes = String(value);
    if (name === 'id') byId.set(String(value), node);
    if (name.startsWith('data-')) node.dataset[datasetName(name)] = String(value);
    if (name === 'hidden') node.hidden = true;
    if (name === 'value') node.value = String(value);
    if (name === 'type') node.type = String(value);
  });
  Object.defineProperties(node, {
    className: {
      get() { return this._classes; },
      set(value) { this._classes = String(value); this.attrs.class = this._classes; }
    },
    textContent: {
      get() {
        return this._text + this.children.map((child) => child.textContent).join('');
      },
      set(value) {
        this.children.forEach((child) => markAttached(child, false));
        this._text = value === null || value === undefined ? '' : String(value);
        this.children = [];
      }
    },
    title: {
      get() { return this.attrs.title || ''; },
      set(value) { this.attrs.title = String(value); }
    },
    id: {
      get() { return this.attrs.id || ''; },
      set(value) { this.setAttribute('id', value); }
    }
  });
  node.classList = {
    add(name) {
      const names = node._classes.split(/\s+/).filter(Boolean);
      if (!names.includes(name)) names.push(name);
      node.className = names.join(' ');
    },
    remove(name) {
      node.className = node._classes.split(/\s+/).filter((item) => item && item !== name).join(' ');
    },
    contains(name) {
      return node._classes.split(/\s+/).includes(name);
    },
    toggle(name, force) {
      const present = node._classes.split(/\s+/).includes(name);
      const shouldAdd = force === undefined ? !present : Boolean(force);
      if (shouldAdd) node.classList.add(name);
      else node.classList.remove(name);
      return shouldAdd;
    }
  };
  node.setAttribute = function setAttribute(name, value) {
    this.attrs[name] = String(value);
    if (name === 'id') byId.set(String(value), this);
    if (name === 'class') this._classes = String(value);
    if (name.startsWith('data-')) this.dataset[datasetName(name)] = String(value);
  };
  node.getAttribute = function getAttribute(name) {
    return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null;
  };
  node.removeAttribute = function removeAttribute(name) {
    delete this.attrs[name];
    if (name.startsWith('data-')) delete this.dataset[datasetName(name)];
  };
  node.appendChild = function appendChild(child) {
    child.parentElement = this;
    markAttached(child, this._attached);
    this.children.push(child);
    return child;
  };
  node.replaceChildren = function replaceChildren(...children) {
    this.children.forEach((child) => markAttached(child, false));
    this.children = [];
    this._text = '';
    children.forEach((child) => this.appendChild(child));
  };
  node.addEventListener = function addEventListener(name, callback) {
    if (!this.listeners[name]) this.listeners[name] = [];
    this.listeners[name].push(callback);
  };
  node.dispatchEvent = function dispatchEvent(name, extra) {
    const event = Object.assign({
      type: name,
      target: this,
      currentTarget: this,
      preventDefault() {},
      stopPropagation() {}
    }, extra || {});
    (this.listeners[name] || []).slice().forEach((callback) => callback(event));
    return true;
  };
  allElements.push(node);
  return node;
}

function makeTextNode(value) {
  return {
    nodeType: 3,
    parentElement: null,
    _attached: false,
    get textContent() { return String(this._value); },
    set textContent(value) { this._value = String(value); },
    _value: String(value)
  };
}

const tagPattern = /<([a-zA-Z][a-zA-Z0-9:-]*)([^<>]*?)\/?>/g;
let tagMatch;
while ((tagMatch = tagPattern.exec(input.html))) {
  const attributes = {};
  const attrPattern = /([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*"([^"]*)"/g;
  let attrMatch;
  while ((attrMatch = attrPattern.exec(tagMatch[2]))) {
    attributes[attrMatch[1]] = decodeEntities(attrMatch[2]);
  }
  makeNode(tagMatch[1], attributes, true);
}

const document = {
  visibilityState: 'visible',
  documentElement: {
    lang: 'en',
    attrs: {},
    setAttribute(name, value) { this.attrs[name] = String(value); }
  },
  getElementById(id) {
    return byId.get(id) || null;
  },
  querySelectorAll(selector) {
    const live = allElements.filter((node) => node._attached);
    if (selector === '#runtimeFilterBar .chip-btn') {
      return live.filter((node) =>
        node._classes.split(/\s+/).includes('chip-btn') &&
        Object.prototype.hasOwnProperty.call(node.attrs, 'data-runtime')
      );
    }
    const attribute = selector.match(/^\[([^\]]+)\]$/);
    if (attribute) {
      return live.filter((node) =>
        Object.prototype.hasOwnProperty.call(node.attrs, attribute[1])
      );
    }
    return [];
  },
  createElement(tag) { return makeNode(tag, {}, false); },
  createElementNS(namespace, tag) { return makeNode(tag, {}, false); },
  createTextNode(value) { return makeTextNode(value); },
  addEventListener() {}
};

const topoGraph = document.getElementById('topoGraph');
if (topoGraph) topoGraph.parentElement = { clientWidth: 900 };

const store = Object.assign({}, input.storage || {});
const fetchCalls = [];
const alerts = [];
let intervalCalls = 0;
let networkForbidden = input.mode === 'login';
let networkViolation = null;

const windowObject = {
  location: { href: '/' },
  localStorage: null,
  crypto: {
    randomUUID() { return '00000000-0000-4000-8000-000000000000'; },
    getRandomValues(values) {
      values.fill(7);
      return values;
    }
  },
  setInterval() { intervalCalls += 1; return intervalCalls; },
  clearInterval() {},
  setTimeout() { return 0; },
  alert(message) { alerts.push(String(message)); }
};
if (input.storageMode === 'throw') {
  Object.defineProperty(windowObject, 'localStorage', {
    configurable: true,
    get() { throw new Error('storage is blocked'); }
  });
} else if (input.storageMode !== 'missing') {
  windowObject.localStorage = {
    getItem(key) { return Object.prototype.hasOwnProperty.call(store, key) ? store[key] : null; },
    setItem(key, value) { store[key] = String(value); },
    removeItem(key) { delete store[key]; }
  };
}

const navigator = Array.isArray(input.languages) && input.languages.length
  ? { languages: input.languages, language: input.languages[0] }
  : {};

function response(status, payload) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() { return payload; }
  };
}

async function fetchMock(url, options) {
  const method = options && options.method ? options.method : 'GET';
  fetchCalls.push({ url: String(url), method });
  if (networkForbidden) {
    networkViolation = String(url);
    throw new Error('Network call is forbidden during language switching: ' + url);
  }
  if (input.mode === 'dashboard') {
    if (url === '/auth/session') return response(200, { ok: true, csrf_token: 'csrf-raw' });
    if (url === '/api/snapshot') return response(200, input.snapshot);
  }
  throw new Error('Unexpected fetch: ' + url);
}

const consoleObject = { error() {}, log() {}, warn() {} };

const factory = new Function(
  'window', 'navigator', 'document', 'fetch', 'console',
  'setTimeout', 'setInterval', 'clearInterval',
  input.script + '\nreturn {' +
    'getLanguage: () => webLanguage, ' +
    'getChoice: () => webLanguageChoice, ' +
    'getCurrentData: typeof currentData === "undefined" ? () => null : () => currentData, ' +
    'setConnectionBanner: typeof setConnectionBanner === "function" ? setConnectionBanner : null, ' +
    'showLoginError: typeof showLoginError === "function" ? showLoginError : null' +
  '};'
);
const api = factory(
  windowObject, navigator, document, fetchMock, consoleObject,
  () => 0, () => { intervalCalls += 1; return intervalCalls; }, () => {}
);

function capture() {
  const live = allElements.filter((node) => node._attached);
  const i18nText = {};
  const ariaLabels = {};
  const titleText = {};
  const placeholders = {};
  live.forEach((node) => {
    if (node.attrs['data-i18n']) i18nText[node.attrs['data-i18n']] = node.textContent;
    if (node.attrs['data-i18n-aria-label']) {
      ariaLabels[node.attrs['data-i18n-aria-label']] = node.attrs['aria-label'];
    }
    if (node.attrs['data-i18n-title']) titleText[node.attrs['data-i18n-title']] = node.attrs.title;
    if (node.attrs['data-i18n-placeholder']) {
      placeholders[node.attrs['data-i18n-placeholder']] = node.attrs.placeholder;
    }
  });
  const select = byId.get('languageSelect');
  const banner = byId.get('connectionBanner');
  const token = byId.get('token');
  const error = byId.get('loginError');
  const textById = (id) => {
    const node = byId.get(id);
    return node ? node.textContent : null;
  };
  return {
    language: api.getLanguage(),
    choice: api.getChoice(),
    selected: select ? select.value : null,
    documentLanguage: document.documentElement.lang,
    i18nText,
    ariaLabels,
    titleText,
    placeholders,
    allText: live.map((node) => node.textContent).join('\n'),
    attrText: live.map((node) => Object.values(node.attrs).join('\n')).join('\n'),
    tags: Array.from(new Set(live.map((node) => node.tagName))),
    sessionCount: textById('sessionCount'),
    nativeCount: textById('nativeCount'),
    groupStats: textById('groupStats'),
    hostStats: textById('hostStats'),
    connectionStatus: textById('connectionStatus'),
    connectionBanner: banner ? banner.textContent : null,
    connectionBannerHidden: banner ? banner.hidden : null,
    error: error ? error.textContent : null,
    tokenValue: token ? token.value : null,
    storage: Object.assign({}, store)
  };
}

async function waitFor(predicate, label) {
  for (let attempt = 0; attempt < 500; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  throw new Error('Timed out waiting for ' + label);
}

(async () => {
  const select = byId.get('languageSelect');
  let result;
  if (input.mode === 'dashboard') {
    await waitFor(() => api.getCurrentData() !== null, 'the initial snapshot');
    await new Promise((resolve) => setImmediate(resolve));
    networkForbidden = true;
    api.setConnectionBanner(input.errorKey);
    const dataBefore = JSON.stringify(api.getCurrentData());
    result = {
      initial: capture(),
      fetchCalls: fetchCalls.slice()
    };
    select.value = 'en';
    select.dispatchEvent('change');
    result.en = capture();
    select.value = 'zh-CN';
    select.dispatchEvent('change');
    result.zh = capture();
    result.dataUnchanged = dataBefore === JSON.stringify(api.getCurrentData());
    result.alerts = alerts.slice();
    result.networkViolation = networkViolation;
    result.fetchCalls = fetchCalls.slice();
    result.intervalCalls = intervalCalls;
  } else {
    networkForbidden = true;
    const token = byId.get('token');
    token.value = input.tokenValue;
    api.showLoginError('Invalid token');
    result = { initial: capture() };
    select.value = 'en';
    select.dispatchEvent('change');
    result.en = capture();
    select.value = 'zh-CN';
    select.dispatchEvent('change');
    result.zh = capture();
    result.alerts = alerts.slice();
    result.networkViolation = networkViolation;
    result.fetchCalls = fetchCalls.slice();
  }
  console.log(JSON.stringify(result));
})().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
"""


def page_script(page: str) -> str:
    start = page.index("<script>") + len("<script>")
    end = page.index("</script>", start)
    return page[start:end]


def page_markup(page: str) -> str:
    return page[: page.index("<script>")]


def decode_js_string(value: str) -> str:
    """Decode the small set of JS escapes used by catalog lookup calls."""

    def unicode_escape(match: re.Match[str]) -> str:
        return chr(int(match.group(1), 16))

    value = re.sub(r"\\u([0-9a-fA-F]{4})", unicode_escape, value)
    return value.replace("\\'", "'").replace("\\n", "\n").replace("\\\\", "\\")


def static_keys(page: str) -> set[str]:
    keys: set[str] = set()
    for attribute in ("data-i18n", "data-i18n-aria-label", "data-i18n-title", "data-i18n-placeholder"):
        keys.update(html.unescape(value) for value in re.findall(attribute + r'="([^"]*)"', page))
    return keys


def literal_ui_keys(page: str) -> set[str]:
    keys: set[str] = set()
    pattern = re.compile(r"(?:uiText|translateUi|topologyText)\(\s*'((?:[^'\\]|\\.)*)'")
    keys.update(decode_js_string(value) for value in pattern.findall(page))
    return keys


def dashboard_snapshot() -> dict:
    raw_session_name = "Session 原始名 <b>RAW</b>"
    raw_instruction = "任务指令 raw <script>alert(1)</script> 保留"
    raw_model = "model-raw-Δ"
    raw_model_output = "MODEL OUTPUT RAW 不应显示"
    return {
        "coordinator": {"host_id": "coord-raw-host", "node_id": "node-raw-1234567890"},
        "provenance": {"snapshot_source": "broker", "broker_connected": True},
        "metrics": {
            "online_hosts": 2,
            "total_hosts": 2,
            "active_sessions": 1,
            "native_sessions": 1,
            "running_tasks": 1,
            "total_broadcasts": 1,
            "total_cost_usd": 1.2345,
            "cost_complete": True,
            "has_attempts": True,
        },
        "hosts": [{
            "host_id": "worker-raw-host",
            "state": "online",
            "ssh_target": "ssh-raw-target",
            "node_id": "worker-node-raw-42",
            "projects": {"RawProject": {}},
            "last_contact_rel": "30s ago",
        }],
        "sessions": [{
            "id": "session-id-RAW-42",
            "name": raw_session_name,
            "runtime": "codex",
            "display_runtime": "Codex",
            "host_id": "local",
            "profile": "profile-raw",
            "model": raw_model,
            "observed": "running",
            "alive": True,
            "desired": "open",
            "task": {
                "status": "running",
                "instruction": raw_instruction,
                "model_output": raw_model_output,
                "answer": raw_model_output,
            },
            "has_attempts": True,
            "cost_usd": 0.123,
            "cost_complete": True,
            "revision": 1,
        }],
        "runtimes": {
            "pi": {},
            "claude": {},
            "antigravity": {},
            "codex": {
                "display_name": "Codex",
                "bridge_active": 1,
                "bridge_running": 1,
                "bridge_waiting": 0,
                "bridge_recovery": 0,
                "native_active": 1,
                "native_daemons": 0,
            },
        },
        "native_sessions": [{
            "runtime": "codex",
            "display_runtime": "Codex",
            "pid": 4242,
            "name": "Native 原始进程名",
            "native_id": "native-id-RAW-99",
            "comm": "rawcomm",
            "model": raw_model,
            "kind": "worker",
            "observed": "running",
            "alive": True,
            "cwd": "/raw/path",
        }],
        "broadcasts": [{
            "broadcast_id": "broadcast-RAW-id-123",
            "group_id": "group-RAW-42",
            "received": 1,
            "total": 1,
            "message_preview": "RAW_BROADCAST <i>预览</i>",
            "created_rel": "2m ago",
            "uncertain": 0,
        }],
        "events": [{
            "seq": 1,
            "kind": "RAW_KIND_保留",
            "summary": "RAW_EVENT <b>保留</b> summary",
            "session_name": raw_session_name,
            "session_id": "session-id-RAW-42",
            "created_rel": "1d ago",
        }],
        "pending_questions": [],
    }


@unittest.skipUnless(NODE, "Node is required to execute the real embedded page scripts")
class WebPageScriptTests(unittest.TestCase):
    def run_page(self, page: str, mode: str, **payload):
        request = {
            "mode": mode,
            "script": page_script(page),
            "html": page_markup(page),
            **payload,
        }
        completed = subprocess.run(
            [NODE, "-e", PAGE_RUNNER],
            input=json.dumps(request),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        if completed.returncode != 0:
            self.fail(completed.stderr or completed.stdout)
        return json.loads(completed.stdout)

    def dashboard(self, **payload):
        return self.run_page(
            HTML_PAGE,
            "dashboard",
            snapshot=dashboard_snapshot(),
            errorKey=DASHBOARD_ERROR_KEY,
            **payload,
        )

    def login(self, **payload):
        return self.run_page(
            LOGIN_PAGE,
            "login",
            tokenValue="token-raw-Δ-42",
            **payload,
        )

    def assert_raw_user_content(self, view: dict, snapshot: dict):
        session = snapshot["sessions"][0]
        native = snapshot["native_sessions"][0]
        broadcast = snapshot["broadcasts"][0]
        event = snapshot["events"][0]
        for value in (
            session["name"],
            session["task"]["instruction"],
            session["model"],
            native["name"],
            broadcast["message_preview"],
            event["kind"],
            event["summary"],
        ):
            with self.subTest(value=value):
                self.assertIn(value, view["allText"])
        for value in (
            session["id"],
            native["native_id"],
            broadcast["broadcast_id"],
            snapshot["coordinator"]["host_id"],
            snapshot["hosts"][0]["host_id"],
        ):
            with self.subTest(title_value=value):
                self.assertIn(value, view["attrText"])
        for hidden_value in (session["task"]["model_output"], session["task"]["answer"]):
            self.assertNotIn(hidden_value, view["allText"])
            self.assertNotIn(hidden_value, view["attrText"])
        self.assertNotIn("IMG", view["tags"])
        self.assertNotIn("SCRIPT", view["tags"])

    _dashboard_cache = None
    _login_cache = None

    def dashboard_once(self):
        cls = type(self)
        if cls._dashboard_cache is None:
            cls._dashboard_cache = self.dashboard(languages=["zh-CN"])
        return cls._dashboard_cache

    def login_once(self):
        cls = type(self)
        if cls._login_cache is None:
            cls._login_cache = self.login(languages=["zh-CN"])
        return cls._login_cache

    def test_dashboard_auto_language_and_bidirectional_persistence(self):
        result = self.dashboard_once()
        initial, english, chinese = result["initial"], result["en"], result["zh"]

        self.assertEqual("zh-CN", initial["language"])
        self.assertEqual("auto", initial["choice"])
        self.assertEqual("auto", initial["selected"])
        self.assertEqual("zh-CN", initial["documentLanguage"])
        self.assertEqual({}, initial["storage"])
        self.assertEqual("主机 ID", initial["i18nText"]["Host ID"])
        self.assertEqual("正在加载集群节点…", initial["i18nText"]["Loading cluster nodes…"])
        self.assertEqual("语言", initial["i18nText"]["Language"])

        self.assertEqual("en", english["language"])
        self.assertEqual("en", english["choice"])
        self.assertEqual("en", english["selected"])
        self.assertEqual("Host ID", english["i18nText"]["Host ID"])
        self.assertEqual("en", english["storage"][LANGUAGE_STORAGE_KEY])

        self.assertEqual("zh-CN", chinese["language"])
        self.assertEqual("zh-CN", chinese["choice"])
        self.assertEqual("zh-CN", chinese["selected"])
        self.assertEqual("主机 ID", chinese["i18nText"]["Host ID"])
        self.assertEqual("zh-CN", chinese["storage"][LANGUAGE_STORAGE_KEY])

    def test_dashboard_dynamic_tables_status_and_error_use_real_redraw(self):
        result = self.dashboard_once()
        initial, english, chinese = result["initial"], result["en"], result["zh"]

        self.assertEqual("显示 1 / 1 · 1 个活动", initial["sessionCount"])
        self.assertEqual("1 条广播", initial["groupStats"])
        self.assertIn("任务运行中", initial["allText"])
        self.assertIn("工作进程", initial["allText"])
        self.assertEqual("无法刷新仪表盘。正在显示上次快照并重试。", initial["connectionBanner"])
        self.assertFalse(initial["connectionBannerHidden"])
        self.assertIn("实时", initial["connectionStatus"])

        self.assertEqual("1 of 1 shown · 1 live", english["sessionCount"])
        self.assertEqual("1 broadcast", english["groupStats"])
        self.assertIn("Task running", english["allText"])
        self.assertIn("Worker process", english["allText"])
        self.assertEqual(DASHBOARD_ERROR_KEY, english["connectionBanner"])
        self.assertIn("Live", english["connectionStatus"])

        self.assertEqual("显示 1 / 1 · 1 个活动", chinese["sessionCount"])
        self.assertIn("任务运行中", chinese["allText"])
        self.assertEqual("无法刷新仪表盘。正在显示上次快照并重试。", chinese["connectionBanner"])

    def test_dashboard_switch_never_fetches_sends_or_mutates_snapshot(self):
        result = self.dashboard_once()
        self.assertTrue(result["dataUnchanged"])
        self.assertEqual(
            [{"url": "/auth/session", "method": "GET"}, {"url": "/api/snapshot", "method": "GET"}],
            result["fetchCalls"],
        )
        self.assertEqual([], result["alerts"])
        self.assertIsNone(result["networkViolation"])
        self.assertGreaterEqual(result["intervalCalls"], 1)

    def test_dashboard_preserves_raw_user_content_and_escapes_text(self):
        result = self.dashboard_once()
        snapshot = dashboard_snapshot()
        for view in (result["initial"], result["en"], result["zh"]):
            self.assert_raw_user_content(view, snapshot)

    def test_dashboard_counts_and_relative_times_follow_display_locale(self):
        result = self.dashboard_once()
        initial, english = result["initial"], result["en"]
        self.assertIn("2 分钟前", initial["allText"])
        self.assertIn("1 天前", initial["allText"])
        self.assertIn("30 秒前", initial["allText"])
        self.assertIn("2m ago", english["allText"])
        self.assertIn("1d ago", english["allText"])
        self.assertIn("30s ago", english["allText"])

    def test_login_real_script_translates_shell_placeholder_and_aria(self):
        result = self.login_once()
        initial, english, chinese = result["initial"], result["en"], result["zh"]

        self.assertEqual("zh-CN", initial["language"])
        self.assertEqual("cli-agent-bridge | 登录", initial["i18nText"]["cli-agent-bridge | Sign in"])
        self.assertEqual("cli-agent-bridge 监控台", initial["i18nText"]["cli-agent-bridge Monitor"])
        self.assertEqual("管理员令牌", initial["i18nText"]["Admin token"])
        self.assertEqual("登录", initial["i18nText"]["Sign in"])
        self.assertEqual("语言", initial["i18nText"]["Language"])
        self.assertEqual("请输入管理员令牌", initial["placeholders"]["Enter admin token"])
        self.assertEqual("语言", initial["ariaLabels"]["Language"])

        self.assertEqual("en", english["language"])
        self.assertEqual("cli-agent-bridge | Sign in", english["i18nText"]["cli-agent-bridge | Sign in"])
        self.assertEqual("Admin token", english["i18nText"]["Admin token"])
        self.assertEqual("Enter admin token", english["placeholders"]["Enter admin token"])

        self.assertEqual("zh-CN", chinese["language"])
        self.assertEqual("cli-agent-bridge | 登录", chinese["i18nText"]["cli-agent-bridge | Sign in"])

    def test_login_real_error_callback_retranslates_both_ways(self):
        result = self.login_once()
        initial, english, chinese = result["initial"], result["en"], result["zh"]
        self.assertEqual("令牌无效", initial["error"])
        self.assertEqual("Invalid token", english["error"])
        self.assertEqual("令牌无效", chinese["error"])

    def test_login_language_switch_keeps_token_and_never_calls_network(self):
        result = self.login_once()
        initial, english, chinese = result["initial"], result["en"], result["zh"]
        for view in (initial, english, chinese):
            self.assertEqual("token-raw-Δ-42", view["tokenValue"])
        self.assertEqual("en", english["storage"][LANGUAGE_STORAGE_KEY])
        self.assertEqual("zh-CN", chinese["storage"][LANGUAGE_STORAGE_KEY])
        self.assertEqual([], result["fetchCalls"])
        self.assertEqual([], result["alerts"])
        self.assertIsNone(result["networkViolation"])

    def test_storage_access_failure_falls_back_to_auto_browser_language(self):
        blocked = self.login(languages=["zh-CN"], storageMode="throw")
        self.assertEqual("auto", blocked["initial"]["choice"])
        self.assertEqual("zh-CN", blocked["initial"]["language"])
        self.assertEqual([], blocked["fetchCalls"])

    def test_unknown_stored_locale_falls_back_to_auto(self):
        invalid_stored = self.login(
            languages=["zh-CN"],
            storage={LANGUAGE_STORAGE_KEY: "fr-FR"},
        )
        self.assertEqual("auto", invalid_stored["initial"]["choice"])
        self.assertEqual("zh-CN", invalid_stored["initial"]["language"])

    def test_browser_preference_order_and_unknown_locale_fall_back_to_english(self):
        unsupported = self.login(languages=["fr-FR"])
        self.assertEqual("auto", unsupported["initial"]["choice"])
        self.assertEqual("en", unsupported["initial"]["language"])

        preferred = self.login(languages=["fr-FR", "zh-CN", "en-GB"])
        self.assertEqual("auto", preferred["initial"]["choice"])
        self.assertEqual("zh-CN", preferred["initial"]["language"])


class WebCatalogTests(unittest.TestCase):
    def test_static_and_literal_ui_keys_exist_in_chinese_catalog(self):
        catalog = get_catalog("zh-CN")
        pages = (HTML_PAGE, LOGIN_PAGE)
        keys: set[str] = set()
        for page in pages:
            keys.update(static_keys(page))
            keys.update(literal_ui_keys(page))
        missing = sorted(key for key in keys if key and key not in catalog)
        self.assertEqual([], missing)

    def test_catalog_has_no_duplicate_keys_and_placeholder_parity(self):
        source = Path(i18n_web.__file__).read_text(encoding="utf-8")
        literal_keys = re.findall(r'^\s{4}"((?:[^"\\]|\\.)*)":', source, re.MULTILINE)
        self.assertEqual([], [key for key, count in Counter(literal_keys).items() if count > 1])
        placeholder_pattern = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
        for english, chinese in MESSAGES.items():
            with self.subTest(english=english):
                self.assertEqual(
                    set(placeholder_pattern.findall(english)),
                    set(placeholder_pattern.findall(chinese)),
                )

    def test_english_templates_remain_stable_and_catalog_is_a_copy(self):
        self.assertEqual("en", resolve_language("en"))
        self.assertEqual("zh-CN", resolve_language("zh_CN.UTF-8"))
        self.assertEqual("Hosts online", translate("Hosts online", language="en"))
        self.assertEqual("在线主机", translate("Hosts online", language="zh-CN"))
        self.assertEqual("任务运行中", translate("Task running", language="zh-CN"))
        self.assertEqual(
            "最近一次 Bridge 任务状态为 failed。",
            translate("The latest Bridge task is {status}.", language="zh-CN", status="failed"),
        )
        first = get_catalog("zh-CN")
        first["Hosts online"] = "changed"
        self.assertEqual("在线主机", get_catalog("zh-CN")["Hosts online"])

    def test_catalog_script_serialization_escapes_script_and_separator_characters(self):
        payload = '</script><script>alert(1)</script>\u2028\u2029&'
        encoded = _script_json({"message": payload})
        self.assertNotIn("</script", encoded)
        self.assertNotIn("<", encoded)
        self.assertNotIn(">", encoded)
        self.assertNotIn("&", encoded)
        self.assertNotIn("\u2028", encoded)
        self.assertNotIn("\u2029", encoded)
        self.assertEqual(payload, json.loads(encoded)["message"])

    def test_both_pages_have_a_lightweight_language_selector(self):
        for page in (HTML_PAGE, LOGIN_PAGE):
            with self.subTest(page=page[:80]):
                self.assertIn('id="languageSelect"', page)
                self.assertIn('value="auto"', page)
                self.assertIn('value="en"', page)
                self.assertIn('value="zh-CN"', page)
                self.assertIn("English", page)
                self.assertIn("简体中文", page)
                self.assertIn("data-i18n-aria-label=\"Language\"", page)
                self.assertEqual(1, page.count("</script>"))
        self.assertIn(LANGUAGE_STORAGE_KEY, HTML_PAGE)
        self.assertIn(LANGUAGE_STORAGE_KEY, LOGIN_PAGE)
        self.assertNotIn("innerHTML", HTML_PAGE)
        self.assertNotIn("innerHTML", LOGIN_PAGE)
        self.assertIn("navigator.languages", HTML_PAGE)
        self.assertIn("navigator.languages", LOGIN_PAGE)

    def test_catalog_does_not_contain_runtime_user_data_keys(self):
        raw = json.dumps(MESSAGES, ensure_ascii=False)
        self.assertNotIn("session-ABC_123", raw)
        self.assertNotIn("Agent 名称", raw)
        self.assertNotIn("<img", raw)
        self.assertNotIn("</script", raw)


if __name__ == "__main__":
    unittest.main()
