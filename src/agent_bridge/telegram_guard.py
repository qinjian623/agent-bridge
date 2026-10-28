"""Read only the existing bot's pause/removal flags; never read chat text or credentials."""

import sqlite3
from pathlib import Path


def register_child(database, runtime, native_id):
    if not database or runtime in {"pi", "antigravity"}:
        return
    db = sqlite3.connect(Path(database).expanduser().resolve().as_uri() + "?mode=rw", uri=True, timeout=5)
    try:
        version = db.execute("SELECT value FROM meta WHERE key='external_child_filter'").fetchone()
        if not version or version[0] != "1":
            raise RuntimeError("Existing Telegram service must support external child filtering before model execution")
        thread_id = "claude:" + native_id if runtime == "claude" else native_id
        with db:
            db.execute(
                "INSERT OR IGNORE INTO external_children(thread_id,owner) VALUES(?,'cli-agent-bridge')", (thread_id,)
            )
    finally:
        db.close()


def delivery_status(database, native_id):
    if not database:
        return "open"
    try:
        db = sqlite3.connect(Path(database).expanduser().resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
        try:
            removed = db.execute(
                "SELECT 1 FROM removed_topics WHERE thread_id=? OR topic_id IN (SELECT topic_id FROM topics WHERE thread_id=?)",
                (native_id, native_id),
            ).fetchone()
            if removed:
                return "closed"
            paused = db.execute(
                "SELECT 1 FROM inbox WHERE status IN ('uncertain','stopped','stop_pending','stopping') AND (thread_id=? OR topic_id IN (SELECT topic_id FROM topics WHERE thread_id=?)) LIMIT 1",
                (native_id, native_id),
            ).fetchone()
            return "paused" if paused else "open"
        finally:
            db.close()
    except (sqlite3.Error, OSError):
        return "unavailable"
