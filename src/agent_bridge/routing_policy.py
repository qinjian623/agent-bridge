from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .errors import BridgeError

_DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


@dataclass(frozen=True)
class PeakRange:
    days: frozenset[int]
    start: int
    end: int


@dataclass(frozen=True)
class RoutingWindow:
    timezone: ZoneInfo
    holidays: frozenset[date]
    ranges: tuple[PeakRange, ...]

    def is_off_peak(self, moment: datetime) -> bool:
        local = moment.astimezone(self.timezone)
        day = local.date()
        minute = local.hour * 60 + local.minute
        for peak in self.ranges:
            start_day = day
            if peak.start < peak.end:
                if not peak.start <= minute < peak.end:
                    continue
            elif minute < peak.end:
                start_day -= timedelta(days=1)
            elif minute < peak.start:
                continue
            if start_day.weekday() in peak.days and start_day not in self.holidays:
                return False
        return True


@dataclass(frozen=True)
class PreferredWindow:
    match: tuple[tuple[str, str], ...]
    window_name: str
    window: RoutingWindow


@dataclass(frozen=True)
class RoutingPreference:
    id: str
    kinds: tuple[str, ...]
    strategies: tuple[str, ...]
    score_tolerance: float
    prefer: tuple[PreferredWindow, ...]


def _invalid(message: str) -> BridgeError:
    return BridgeError("invalid_config", message)


def _object(value, label, *, allowed=None, required=()):
    if not isinstance(value, dict) or (allowed is not None and value.keys() - allowed) or not value.keys() >= set(required):
        raise _invalid(f"{label} has missing or invalid fields")
    return value


def _text(value) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip() and value.isprintable()


def _strings(value, label, *, allow_empty=False) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not allow_empty) or not all(_text(item) for item in value):
        raise _invalid(f"{label} must be an array of strings")
    if len(set(value)) != len(value):
        raise _invalid(f"{label} must not contain duplicates")
    return tuple(value)


def _dates(value) -> frozenset[date]:
    holidays = _strings(value, "routing calendar holidays", allow_empty=True)
    try:
        dates = [date.fromisoformat(day) for day in holidays]
    except ValueError as error:
        raise _invalid("routing calendar holidays must use YYYY-MM-DD dates") from error
    if any(parsed.isoformat() != original for parsed, original in zip(dates, holidays, strict=True)):
        raise _invalid("routing calendar holidays must use YYYY-MM-DD dates")
    return frozenset(dates)


def _minute(value, *, last=False) -> int:
    if last and value == "24:00":
        return 1440
    if not isinstance(value, str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value):
        raise _invalid("routing peak range times must use HH:MM (end may be 24:00)")
    hour, minute = map(int, value.split(":"))
    return 60 * hour + minute


def _window(raw, calendars) -> RoutingWindow:
    raw = _object(raw, "routing window", allowed={"timezone", "calendar", "ranges"}, required=("timezone", "ranges"))
    if not _text(raw["timezone"]):
        raise _invalid("routing window timezone must be an IANA time zone")
    try:
        timezone = ZoneInfo(raw["timezone"])
    except (ValueError, ZoneInfoNotFoundError) as error:
        raise _invalid("routing window timezone must be an IANA time zone available on this host") from error
    calendar = raw.get("calendar")
    if "calendar" in raw and (not _text(calendar) or calendar not in calendars):
        raise _invalid("routing window calendar must name a configured calendar")
    if not isinstance(raw["ranges"], list):
        raise _invalid("routing window ranges must be an array")
    ranges = []
    for peak in raw["ranges"]:
        peak = _object(peak, "routing peak range", allowed={"days", "start", "end"}, required=("days", "start", "end"))
        days = _strings(peak["days"], "routing peak range days")
        if any(day not in _DAYS for day in days):
            raise _invalid("routing peak range days must use mon through sun")
        start, end = _minute(peak["start"]), _minute(peak["end"], last=True)
        if start == end:
            raise _invalid("routing peak range start and end must differ")
        ranges.append(PeakRange(frozenset(_DAYS.index(day) for day in days), start, end))
    return RoutingWindow(timezone, calendars.get(calendar, frozenset()), tuple(ranges))


def parse_routing(value, profiles) -> tuple[RoutingPreference, ...]:
    if value is None:
        return ()
    value = _object(value, "routing", allowed={"calendars", "windows", "preferences"})
    calendars = {}
    for name, raw in _object(value.get("calendars", {}), "routing.calendars").items():
        if not _text(name):
            raise _invalid("routing calendar names must be non-empty strings")
        raw = _object(raw, "routing calendar", allowed={"holidays"}, required=("holidays",))
        calendars[name] = _dates(raw["holidays"])
    windows = {}
    for name, raw in _object(value.get("windows", {}), "routing.windows").items():
        if not _text(name):
            raise _invalid("routing window names must be non-empty strings")
        windows[name] = _window(raw, calendars)

    preferences = value.get("preferences", [])
    if not isinstance(preferences, list):
        raise _invalid("routing.preferences must be an array")
    rules, ids = [], set()
    for raw in preferences:
        raw = _object(
            raw, "routing preference", allowed={"id", "when", "score_tolerance", "prefer"}, required=("id", "when", "prefer"),
        )
        rule_id = raw["id"]
        if not _text(rule_id) or rule_id in ids:
            raise _invalid("routing preference IDs must be unique non-empty strings")
        ids.add(rule_id)
        when = _object(raw["when"], "routing preference when", allowed={"kinds", "strategies"}, required=("kinds",))
        kinds = _strings(when["kinds"], "routing preference kinds")
        strategies = _strings(when.get("strategies", ["balanced"]), "routing preference strategies")
        if set(strategies) - {"balanced", "cost", "speed"}:
            raise _invalid("routing preference strategies must be balanced, cost or speed")
        tolerance = raw.get("score_tolerance", 0.1)
        if type(tolerance) not in (int, float) or not 0 <= tolerance <= 1:
            raise _invalid("routing preference score_tolerance must be between 0 and 1")
        if not isinstance(raw["prefer"], list) or not raw["prefer"]:
            raise _invalid("routing preference prefer must be a non-empty array")
        preferred = []
        for item in raw["prefer"]:
            item = _object(item, "routing preferred candidate", allowed={"match", "window"}, required=("match", "window"))
            match = _object(item["match"], "routing preferred candidate match", allowed={"profile", "runtime", "provider", "model"})
            if not match or not all(_text(val) for val in match.values()):
                raise _invalid("routing preferred candidate match must specify profile, runtime, provider or model")
            if "profile" in match and match["profile"] not in profiles:
                raise _invalid("routing preferred candidate references an unknown profile")
            if "runtime" in match and match["runtime"] not in {"codex", "claude", "pi", "antigravity"}:
                raise _invalid("routing preferred candidate runtime must be codex, claude, pi or antigravity")
            window = item["window"]
            if not _text(window) or window not in windows:
                raise _invalid("routing preferred candidate window must name a configured window")
            attrs = tuple(("name" if key == "profile" else key, val) for key, val in match.items())
            preferred.append(PreferredWindow(attrs, window, windows[window]))
        rules.append(RoutingPreference(rule_id, kinds, strategies, tolerance, tuple(preferred)))
    return tuple(rules)
