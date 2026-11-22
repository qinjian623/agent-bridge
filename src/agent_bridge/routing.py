from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from statistics import median

from .errors import BridgeError
from .parent_usage import meter_usage
from .store import validate_task_timeout


def task_samples(store, profile, kind):
    with store.connection() as db:
        rows = db.execute(
            "SELECT t.id,t.active_attempt,t.details,t.status,t.validation,a.id AS attempt_id,a.status AS attempt_status,r.decision AS attempt_validation,a.usage,a.started,a.finished,s.profile,s.parent FROM tasks t JOIN attempts a ON a.task_id=t.id JOIN sessions s ON s.id=a.session_id LEFT JOIN reviews r ON r.task_id=t.id AND r.attempt_id=a.id WHERE a.started IS NOT NULL ORDER BY t.created,a.started,a.id"
        ).fetchall()
        meters = {
            row["parent_id"]: meter_usage(json.loads(row["state"])) for row in db.execute("SELECT * FROM parent_usage")
        }
        counts = {
            row[0]: row[1]
            for row in db.execute(
                "SELECT parent,COUNT(DISTINCT task_id) FROM (SELECT s.parent,t.id AS task_id FROM tasks t JOIN sessions s ON s.id=t.session_id UNION ALL SELECT s.parent,a.task_id FROM attempts a JOIN sessions s ON s.id=a.session_id) GROUP BY parent"
            )
        }
    tasks = {}
    for row in rows:
        tasks.setdefault(row["id"], []).append(row)
    samples = []
    for attempts in tasks.values():
        selected = [
            attempt for attempt in attempts
            if all(
                json.loads(attempt["profile"]).get(key) == getattr(profile, key)
                for key in ("runtime", "provider", "model", "effort", "service_tier", "env_file")
            )
        ]
        if not selected:
            continue
        task = selected[-1]
        mixed = len(selected) != len(attempts)
        current = not mixed and any(row["attempt_id"] == task["active_attempt"] for row in selected)
        validation = task["validation"] if current else task["attempt_validation"]
        status = task["status"] if current else task["attempt_status"]
        details = json.loads(task["details"])
        if details.get("kind", "general") != kind or not details.get("learn_from_result", True):
            continue
        reviewed = validation in {"accepted", "rejected"} or status == "failed"
        if not reviewed:
            continue
        usages = [json.loads(row["usage"] or "{}") for row in attempts]
        parent = meters.get(task["parent"])
        complete = current and bool(parent and parent["stopped"] and parent["complete"] and counts.get(task["parent"]) == 1)
        complete = complete and all(row["finished"] is not None and row["parent"] == task["parent"] for row in attempts)
        complete = complete and all(u.get("complete") is True and u.get("total_tokens") is not None for u in usages)
        if parent:
            usages.append(parent)
        prices = [u.get("catalog_estimate", {}) for u in usages]
        samples.append(
            {
                "quality": validation == "accepted",
                "tokens": sum(u["total_tokens"] for u in usages) if complete else None,
                "seconds": parent["stopped_at"] - parent["started_at"]
                if complete and parent.get("stopped_at")
                else None,
                "usd": sum(p["usd"] for p in prices)
                if complete
                and all(p.get("usd") is not None and p.get("complete") and p.get("price_date") for p in prices)
                else None,
            }
        )
    return samples


def _prefer_off_peak(config, rule, candidates, chosen):
    moment = datetime.now(UTC)
    active = [target for target in rule.prefer if target.window.is_off_peak(moment)]
    decision = {
        "rule_id": rule.id,
        "applied": False,
        "evaluated_at_utc": moment.isoformat(),
        "active_windows": list(dict.fromkeys(target.window_name for target in active)),
    }
    if "score" in chosen:
        ceiling = chosen["score"] * (1 + rule.score_tolerance)
        candidates = [candidate for candidate in candidates if candidate["score"] <= ceiling]
    for target in active:
        matches = [
            candidate for candidate in candidates
            if all(getattr(config.profiles[candidate["profile"]], key) == val for key, val in target.match)
        ]
        if matches:
            decision.update(applied=True, window=target.window_name)
            return min(matches, key=lambda candidate: candidate.get("score", 0)), decision
    decision["skip_reason"] = "no_preferred_candidate" if active else "no_off_peak_window"
    return chosen, decision


def choose(config, store, requirements, profile_name=None, access="read-only", *, prefer_off_peak=True):
    supported = {
        "kind",
        "input_types",
        "tools",
        "input_tokens",
        "providers",
        "minimum_quality",
        "execute_locally",
        "hard_budget",
        "strategy",
        "learn_from_result",
        "task_timeout",
    }
    if not isinstance(requirements, dict) or set(requirements) - supported:
        raise BridgeError(
            "unsupported_requirement",
            "Only documented task requirements are supported; monetary caps and arbitrary deadlines are not supported",
        )
    validate_task_timeout(requirements.get("task_timeout"))
    if requirements.get("hard_budget"):
        raise BridgeError("hard_budget_unsupported", "The installed backends have no verified hard token limit")
    strategy = requirements.get("strategy", config.default_strategy)
    if strategy not in {"balanced", "cost", "speed"}:
        raise BridgeError("invalid_strategy", "strategy must be balanced, cost or speed")
    cooldowns = {
        row["quota_group"]: row
        for row in store.quotas()
        if row["blocked_until"] is None or row["blocked_until"] > time.time()
    }
    candidates, rejected = [], []
    for name, profile in config.profiles.items():
        if profile_name and name != profile_name:
            continue
        reasons = []
        if access == "read-only" and profile.runtime == "antigravity":
            reasons.append("Antigravity has no enforced read-only tool policy; use workspace-write")
        if not profile_name and profile.quota_group in cooldowns:
            reasons.append("provider quota is waiting; inspect quota_cooldowns")
        if not profile.verified and not profile_name:
            reasons.append("no verified inference sample")
        if not set(requirements.get("input_types", ["text"])) <= set(profile.input_types):
            reasons.append("unsupported input type")
        if not set(requirements.get("tools", [])) <= set(profile.tools):
            reasons.append("required tool unavailable")
        if access == "read-only" and (
            "edit" in requirements.get("tools", [])
            or profile.runtime in {"pi", "claude"}
            and "shell" in requirements.get("tools", [])
        ):
            reasons.append("required tool is disabled by read-only access")
        if requirements.get("input_tokens") and (
            not profile.context_window or requirements["input_tokens"] > profile.context_window
        ):
            reasons.append("context capacity unknown or too small")
        if requirements.get("providers") is not None and profile.provider not in requirements["providers"]:
            reasons.append("provider not permitted")
        try:
            profile.command()
        except BridgeError as error:
            reasons.append(str(error))
        if reasons:
            rejected.append({"profile": name, "reasons": reasons})
            continue
        samples = task_samples(store, profile, requirements.get("kind", "general"))
        quality = sum(s["quality"] for s in samples) / len(samples) if samples else None
        if quality is not None and quality < requirements.get("minimum_quality", 0.8) and not profile_name:
            rejected.append({"profile": name, "reasons": ["measured quality below requirement"], "quality": quality})
            continue
        candidates.append(
            {
                "profile": name,
                "quality": quality,
                "samples": len(samples),
                "tokens": median([s["tokens"] for s in samples])
                if samples and all(s["tokens"] is not None for s in samples)
                else None,
                "seconds": median([s["seconds"] for s in samples])
                if samples and all(s["seconds"] is not None for s in samples)
                else None,
                "usd": median([s["usd"] for s in samples])
                if samples and all(s["usd"] is not None for s in samples)
                else None,
            }
        )
    if requirements.get("execute_locally") and not profile_name:
        return {
            "execute_locally": True,
            "reason": "Caller marked this task cheaper to complete in the main session",
            "candidates": candidates,
            "excluded": rejected,
        }
    if not candidates:
        raise BridgeError("no_candidate", "No eligible profile: " + json.dumps(rejected, ensure_ascii=False))
    if profile_name:
        return {
            "profile": candidates[0]["profile"],
            "reason": "Explicit profile; no substitution",
            "candidates": candidates,
            "excluded": rejected,
        }
    # Use a single cost unit for the entire comparison, never dollars mixed with tokens.
    cost = "usd" if all(c["usd"] is not None and c["usd"] > 0 for c in candidates) else "tokens"
    measured = all(c["quality"] is not None and c[cost] and c["seconds"] and c["seconds"] > 0 for c in candidates)
    if measured:
        efficient = [
            c
            for c in candidates
            if not any(
                o[cost] <= c[cost]
                and o["seconds"] <= c["seconds"]
                and o["quality"] >= c["quality"]
                and (o[cost] < c[cost] or o["seconds"] < c["seconds"])
                for o in candidates
            )
        ]
        min_cost, min_time = min(c[cost] for c in efficient), min(c["seconds"] for c in efficient)
        for candidate in efficient:
            cost_weight = {"balanced": 0.5, "cost": 1.0, "speed": 0.0}[strategy]
            candidate["score"] = (
                cost_weight * candidate[cost] / min_cost + (1 - cost_weight) * candidate["seconds"] / min_time
            )
        chosen = min(efficient, key=lambda c: c["score"])
        reason = f"{strategy}: measured task totals include all attempts and parent overhead"
    else:
        chosen = candidates[0]
        reason = "Unmeasured: fixed verified profile order; no quality or savings claim"
    preference = None
    if prefer_off_peak:
        kind = requirements.get("kind", "general")
        rule = next((rule for rule in config.routing if kind in rule.kinds and strategy in rule.strategies), None)
        if rule is not None:
            chosen, preference = _prefer_off_peak(config, rule, efficient if measured else candidates, chosen)
            if preference["applied"]:
                reason = f"{strategy}: off-peak preference after measured scoring" if measured else (
                    "Unmeasured: off-peak preference among eligible profiles; no quality or savings claim"
                )
    return {
        "profile": chosen["profile"],
        "reason": reason,
        "objective": strategy,
        "cost_unit": cost,
        "candidates": candidates,
        "excluded": rejected,
        **({"preference": preference} if preference is not None else {}),
    }
