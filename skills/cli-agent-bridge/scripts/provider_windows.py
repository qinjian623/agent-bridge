"""Legacy standalone Flash time-window example. The broker uses config.json routing rules."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

BEIJING = ZoneInfo("Asia/Shanghai")


def policy_at(moment: datetime, china_public_holiday: bool = False) -> dict:
    """Use UTC weekdays for Ollama and Beijing weekdays for official DeepSeek."""
    if moment.utcoffset() is None:
        raise ValueError("Time must include a timezone")
    utc = moment.astimezone(UTC)
    beijing = moment.astimezone(BEIJING)
    ollama_off_peak = utc.weekday() >= 5 or not 12 <= utc.hour < 18
    deepseek_peak = (
        beijing.weekday() < 5
        and not china_public_holiday
        and (9 <= beijing.hour < 12 or 14 <= beijing.hour < 18)
    )
    preferred = []
    if ollama_off_peak:
        preferred.append("claude-ollama")
    if not deepseek_peak:
        preferred.extend(("pi-deepseek", "claude-deepseek"))
    return {
        "at_utc": utc.isoformat(),
        "at_beijing": beijing.isoformat(),
        "ollama_off_peak": ollama_off_peak,
        "deepseek_off_peak": not deepseek_peak,
        "preferred_flash_profiles": preferred,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--at", type=datetime.fromisoformat, help="ISO 8601 time with timezone; defaults to now")
    parser.add_argument(
        "--china-public-holiday", action="store_true",
        help="Use only when this Beijing date is a confirmed Chinese public holiday",
    )
    args = parser.parse_args()
    try:
        result = policy_at(args.at or datetime.now(UTC), args.china_public_holiday)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
