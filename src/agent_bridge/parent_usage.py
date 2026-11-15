from __future__ import annotations

import copy
import json
import math
import os
import time
from datetime import datetime
from pathlib import Path

from .errors import BridgeError
from .usage import TOKEN_FIELDS, priced_usage, validate_price

FIELDS = {
    "input_tokens": "input_tokens",
    "output_tokens": "output_tokens",
    "cached_input_tokens": "cache_read_tokens",
    "cache_write_input_tokens": "cache_write_tokens",
    "reasoning_output_tokens": "reasoning_tokens",
    "total_tokens": "total_tokens",
}


def start_meter(path, native_id, since=None, price=None):
    if since is not None and (
        type(since) not in (int, float) or not math.isfinite(since) or since < 0 or since > time.time()
    ):
        raise BridgeError("invalid_meter", "usage_since must be a past Unix timestamp or null for the current counter")
    path = Path(path).expanduser().resolve()
    stat = path.stat()
    if not path.is_file() or stat.st_uid != os.getuid():
        raise BridgeError("invalid_meter", "Usage transcript must be a local file owned by the current user")
    with path.open("rb") as stream:
        header = json.loads(stream.readline())
    if (
        not isinstance(header, dict)
        or header.get("type") != "session_meta"
        or not isinstance(header.get("payload"), dict)
        or header["payload"].get("id") != native_id
    ):
        raise BridgeError("invalid_meter", "Transcript header does not match the registered native session")
    meter = {
        "path": str(path),
        "identity": [stat.st_dev, stat.st_ino],
        "offset": 0,
        "since": since,
        "last": {},
        "model": "unknown",
        "models": {},
        "complete": True,
        "price": price,
        "stopped": False,
        "started_at": since if since is not None else time.time(),
    }
    meter = refresh_meter(meter, initializing=True)
    if meter.get("error"):
        raise BridgeError("invalid_meter", meter["error"])
    return meter


def refresh_meter(original, initializing=False):
    meter = copy.deepcopy(original)
    if meter["stopped"]:
        return meter
    try:
        with Path(meter["path"]).open("rb") as stream:
            stat = os.fstat(stream.fileno())
            if [stat.st_dev, stat.st_ino] != meter["identity"] or stat.st_size < meter["offset"]:
                raise ValueError("Native transcript was replaced or truncated; usage is incomplete")
            stream.seek(meter["offset"])
            while line := stream.readline():
                if not line.endswith(b"\n"):
                    break
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("Malformed native event")
                data = record.get("payload", {})
                if not isinstance(data, dict):
                    raise ValueError("Malformed native event payload")
                if record.get("type") == "turn_context" and data.get("model"):
                    meter["model"] = data["model"]
                if record.get("type") == "event_msg" and data.get("type") == "token_count":
                    info = data.get("info") or {}
                    if not isinstance(info, dict):
                        raise ValueError("Malformed native usage event")
                    raw = info.get("total_token_usage")
                    if raw is not None:
                        if (
                            not isinstance(raw, dict)
                            or "total_tokens" not in raw
                            or any(type(raw.get(key, 0)) is not int or raw.get(key, 0) < 0 for key in FIELDS)
                        ):
                            raise ValueError("Malformed native token counter")
                        counter = {name: raw.get(key, 0) for key, name in FIELDS.items()}
                        raw_timestamp = record.get("timestamp")
                        if not isinstance(raw_timestamp, str):
                            raise ValueError("Native counter has no timestamp")
                        timestamp = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
                        if timestamp.tzinfo is None:
                            raise ValueError("Native counter timestamp has no timezone")
                        timestamp = timestamp.timestamp()
                        baseline = initializing and (meter["since"] is None or timestamp < meter["since"])
                        delta = {key: value - meter["last"].get(key, 0) for key, value in counter.items()}
                        if not baseline:
                            if any(value < 0 for value in delta.values()):
                                meter.update(
                                    complete=False, error="Native counter reset; the missing interval is unknown"
                                )
                            else:
                                totals = meter["models"].setdefault(meter["model"], {key: 0 for key in counter})
                                for key, value in delta.items():
                                    totals[key] += value
                        meter["last"] = counter
                        meter["last_event_at"] = timestamp
                meter["offset"] = stream.tell()
        meter["checked_at"] = time.time()
    except (OSError, ValueError, TypeError, KeyError) as error:
        meter.update(complete=False, error=str(error))
    return meter


def meter_usage(meter):
    totals = {
        key: sum(counts.get(key, 0) for counts in meter["models"].values())
        for key in (*TOKEN_FIELDS, "reasoning_tokens", "total_tokens")
    }
    price = meter["price"]
    if price is not None:
        validate_price(price)
        if set(meter["models"]) - {price.get("model")}:
            price = None
    return priced_usage(
        {
            **totals,
            "complete": meter["complete"],
            "scope": "registered_parent_since_baseline",
            "input_includes_cache": True,
            "models": meter["models"],
            "since": meter["since"],
            "last_event_at": meter.get("last_event_at"),
            "stopped": meter["stopped"],
            "started_at": meter.get("started_at"),
            "stopped_at": meter.get("stopped_at"),
            "error": meter.get("error"),
        },
        price,
    )
