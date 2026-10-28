"""Read-only verification that registered child replies were scanned but not forwarded."""

import argparse
import json
import sqlite3
from pathlib import Path

from agent_bridge.config import Config
from agent_bridge.store import Store


def main(args):
    state = Path(args.state).resolve()
    config = Config.load(state / "config.json")
    if not config.telegram_database:
        raise RuntimeError("No existing Telegram database is configured")
    store = Store(state)
    records = json.loads(Path(args.report).read_text())
    if isinstance(records, dict):
        cases = records["cases"]
        records = [
            {"runtime": "codex", "native_session_id": row["parent_native_id"]}
            for row in cases
            if row.get("parent_native_id")
        ] + [row for row in cases if row.get("session_id")]
    checks = []
    db = sqlite3.connect(Path(config.telegram_database).as_uri() + "?mode=ro", uri=True)
    try:
        for record in records:
            session = store.get_session(record["session_id"]) if record.get("session_id") else None
            runtime = session["profile"]["runtime"] if session else record["runtime"]
            if runtime in {"pi", "antigravity"}:
                continue
            native = ("claude:" if runtime == "claude" else "") + (
                session["native_id"] if session else record["native_session_id"]
            )
            rows = db.execute("SELECT path,offset FROM files WHERE thread_id=?", (native,)).fetchall()
            scanned = bool(rows) and all(offset >= Path(path).stat().st_size for path, offset in rows)
            notifications = db.execute("SELECT COUNT(*) FROM outbox WHERE thread_id=?", (native,)).fetchone()[0]
            registered = bool(db.execute("SELECT 1 FROM external_children WHERE thread_id=?", (native,)).fetchone())
            checks.append(
                {
                    "runtime": runtime,
                    "scan_complete": scanned,
                    "outbox_rows": notifications,
                    "registered": registered,
                    "passed": scanned and registered and notifications == 0,
                }
            )
    finally:
        db.close()
    print(json.dumps(checks))
    assert checks and all(check["passed"] for check in checks), (
        "Do not claim suppressed delivery until the bot has actually scanned the child sessions"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--report", required=True)
    main(parser.parse_args())
