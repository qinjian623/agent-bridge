from __future__ import annotations

import math
import time

from .errors import BridgeError

TOKEN_FIELDS = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens")
CLAUDE_TOKEN_FIELDS = ("inputTokens", "outputTokens", "cacheReadInputTokens", "cacheCreationInputTokens")


def validate_price(price):
    if price is None:
        return
    if (
        not isinstance(price, dict)
        or price.get("currency") != "USD"
        or not all(isinstance(price.get(key), str) and price[key].strip() for key in ("source", "as_of"))
    ):
        raise BridgeError("invalid_price", "Prices require currency=USD, source, as_of and per_million rates")
    rates = price.get("per_million")
    if (
        not isinstance(rates, dict)
        or set(rates) - set(TOKEN_FIELDS)
        or not {"input_tokens", "output_tokens"} <= set(rates)
    ):
        raise BridgeError("invalid_price", "Specify input/output rates and any known cache rates per million tokens")
    if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in rates.values()):
        raise BridgeError("invalid_price", "Price rates must be finite nonnegative numbers")


def priced_usage(usage, price):
    result = dict(usage or {})
    result["actual_billed_usd"] = None
    if price and result.get("priced_models") and set(result["priced_models"]) != {price.get("model")}:
        price = None
    if not price or result.get("pricing_supported") is False:
        return result
    validate_price(price)
    tokens = {key: result.get(key) for key in TOKEN_FIELDS}
    if result.get("input_includes_cache") and tokens["input_tokens"] is not None:
        tokens["input_tokens"] -= (tokens["cache_read_tokens"] or 0) + (tokens["cache_write_tokens"] or 0)
    rates = price["per_million"]
    complete = result.get("complete") is True and all(
        value is not None and value >= 0 and (value == 0 or key in rates) for key, value in tokens.items()
    )
    result["catalog_estimate"] = {
        "usd": sum(value * rates.get(key, 0) for key, value in tokens.items()) / 1_000_000 if complete else None,
        "source": price["source"],
        "price_date": price["as_of"],
        "observed_at": time.time(),
        "complete": complete,
    }
    return result


def claude_counters(value, previous=None):
    value = value if isinstance(value, dict) else {}
    models = value.keys() | (previous or {}).keys()
    # Retain missing model names as unknown so their history is not counted as new later.
    return {
        model: counts if isinstance(counts := value.get(model), dict) and all(
            type(counts.get(field)) is int and counts[field] >= 0 for field in CLAUDE_TOKEN_FIELDS
        ) else None
        for model in models
    } or None


def claude_usage(before, after, fallback, reported_cost):
    """model_usage is cumulative across turns and native resume; usage is main-loop only."""
    mapping = dict(zip(TOKEN_FIELDS, CLAUDE_TOKEN_FIELDS, strict=True))
    valid = isinstance(before, dict) and isinstance(after, dict) and bool(after)
    deltas = {key: 0 for key in TOKEN_FIELDS}
    if valid:
        for model in before.keys() | after.keys():
            for key, native in mapping.items():
                old_model, new_model = before.get(model, {}), after.get(model)
                old = old_model.get(native, 0) if isinstance(old_model, dict) else None
                new = new_model.get(native) if isinstance(new_model, dict) else None
                if type(new) is not int or type(old) is not int or old < 0 or new < old:
                    valid = False
                else:
                    deltas[key] += new - old
    if not valid:
        mapping = dict(
            zip(
                TOKEN_FIELDS,
                ("input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"),
                strict=True,
            )
        )
        deltas = {key: fallback.get(native, 0) for key, native in mapping.items()}
    result = {
        **deltas,
        "total_tokens": sum(deltas.values()) if valid or fallback else None,
        "complete": valid,
        "scope": "attempt_all_models" if valid else "main_loop_attempt",
        "input_includes_cache": False,
        "reported_session_cost": reported_cost,
        "actual_billed_usd": None,
        "priced_models": list(after or {}),
    }
    if not valid:
        result["reason"] = "Missing or reset cumulative per-model counters; main-loop usage is partial"
    if valid and all("costUSD" in value for value in after.values()):
        old_cost = sum(value.get("costUSD", 0) for value in before.values())
        cost = sum(value["costUSD"] for value in after.values()) - old_cost
        result["catalog_estimate"] = {
            "usd": cost if cost >= 0 else None,
            "source": "claude_cli_model_usage",
            "price_date": None,
            "observed_at": time.time(),
            "complete": cost >= 0,
        }
    return result
