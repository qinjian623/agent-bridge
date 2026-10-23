import json
import math
import re


def provider_limit(error):
    """Read only terminal provider errors, never user prompts or successful answers."""
    text = json.dumps(error, ensure_ascii=False).lower()
    if not re.search(
        r"\b429\b|rate[ _-]?limit|too many requests|quota[ _-]?(exceeded|exhausted)|insufficient_quota|usagelimitexceeded",
        text,
    ):
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
