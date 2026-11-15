import json
import math
import re

_STATUS_KEYS = frozenset(
    {"status", "status_code", "statusCode", "http_status", "httpStatus", "code", "error_code", "errorCode"}
)
_LIMIT_PATTERN = re.compile(
    r"\b(?:http(?:/[0-9.]+)?|status(?:[ _-]?code)?|error(?:[ _-]?code)?)\b[\s:=#/\-\"'(\[]*\b429\b"
    r"|rate[ _-]?limit|too many requests|quota[ _-]?(?:exceeded|exhausted)"
    r"|insufficient_quota|usagelimitexceeded|resource[ _-]exhausted"
)


def _has_429_status(error):
    pending = [error]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            for key, item in value.items():
                if key in _STATUS_KEYS and (
                    (type(item) is int and item == 429)
                    or (isinstance(item, str) and item.strip() == "429")
                ):
                    return True
                if isinstance(item, (dict, list)):
                    pending.append(item)
        elif isinstance(value, list):
            pending.extend(value)
    return False


def provider_limit(error):
    """Read only terminal provider errors, never user prompts or successful answers."""
    text = json.dumps(error, ensure_ascii=False).lower()
    if not _has_429_status(error) and not _LIMIT_PATTERN.search(text):
        return None
    delay = None
    pending = [error]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"retry_after_seconds", "retryAfterSeconds", "retry_after", "retry-after"}:
                    try:
                        number = float(item)
                        if math.isfinite(number) and number >= 0:
                            delay = max(delay or 0, number)
                    except (TypeError, ValueError):
                        pass
                elif isinstance(item, (dict, list)):
                    pending.append(item)
        elif isinstance(value, list):
            pending.extend(value)
    return {
        "retry_after_seconds": delay,
        "manual_reset": delay is None
        and bool(re.search(r"insufficient_quota|quota[ _-]?(exceeded|exhausted)|usagelimitexceeded", text)),
    }
