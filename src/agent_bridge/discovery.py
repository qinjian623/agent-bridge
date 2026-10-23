from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, replace
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .config import Profile
from .errors import BridgeError
from .runtimes import JsonProcess, claude_environment

CACHE_SECONDS = 300


def model_record(model, display_name=None, input_types=None, context_window=None, **metadata):
    if (
        not isinstance(model, str)
        or not model
        or len(model) > 256
        or model.startswith("-")
        or any(c.isspace() or ord(c) < 32 for c in model)
    ):
        raise BridgeError("invalid_catalog", "Model directory returned an invalid exact model ID")
    if input_types is not None and (
        not isinstance(input_types, list) or any(not isinstance(v, str) for v in input_types)
    ):
        raise BridgeError("invalid_catalog", "Invalid model input types")
    if context_window is not None and (type(context_window) is not int or context_window <= 0):
        raise BridgeError("invalid_catalog", "Invalid model context window")
    for key, value in metadata.items():
        if value is not None and (not isinstance(value, list) or any(not isinstance(v, str) for v in value)):
            raise BridgeError("invalid_catalog", f"Invalid model {key}")
    return {
        "model": model,
        "display_name": display_name if isinstance(display_name, str) else model,
        "input_types": input_types,
        "context_window": context_window,
        **metadata,
    }


async def reject_question(_):
    return {"decision": "decline"}


async def native_models(profile: Profile, cwd):
    if profile.runtime == "claude":
        environment = claude_environment(profile)
        host = urlsplit(environment.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")).hostname
        if host != {"anthropic": "api.anthropic.com", "deepseek-anthropic": "api.deepseek.com"}.get(profile.provider):
            raise BridgeError("discovery_unsupported", "No model directory is configured for this Claude endpoint")
        if profile.provider == "deepseek-anthropic":
            return await asyncio.to_thread(deepseek_models, environment), "DeepSeek GET /models"
        if profile.provider != "anthropic":
            raise BridgeError("discovery_unsupported", "No model directory is configured for this Claude provider")
        from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

        client = ClaudeSDKClient(
            options=ClaudeAgentOptions(
                cwd=str(cwd),
                model=profile.model,
                cli_path=profile.command(),
                env=environment,
                setting_sources=[],
                strict_mcp_config=True,
                mcp_servers={},
                tools=[],
            )
        )
        try:
            await client.connect()
            info = await client.get_server_info()
            rows = [
                model_record(m["resolvedModel"], m.get("displayName"), reasoning_efforts=m.get("supportedEffortLevels"))
                for m in (info or {}).get("models", [])
                if m.get("resolvedModel")
            ]
            return rows, "Claude SDK get_server_info.models (resolvedModel)"
        finally:
            await client.disconnect()

    argv = [profile.command()]
    if profile.runtime == "codex":
        # shortcut: custom Codex providers need their own directory API before discovery can label them correctly.
        if profile.provider not in {"openai", "openai-codex"}:
            raise BridgeError("discovery_unsupported", "Codex model discovery requires an explicit OpenAI provider")
        argv += ["app-server", "--stdio", "-c", 'model_provider="openai"']
    else:
        argv += ["--mode", "rpc", "--no-session", "--no-extensions", "--no-mcp", "--no-tools"]
    client = JsonProcess(argv, str(cwd), profile.environment(), lambda _: False, reject_question)
    try:
        await client.start()
        if profile.runtime == "pi":
            result = await client.request({"type": "get_available_models"})
            return [
                model_record(m["id"], m.get("name"), m.get("input"), m.get("contextWindow"))
                for m in result["models"]
                if m.get("provider") == profile.provider
            ], "Pi RPC get_available_models (configured provider only)"
        await client.request(
            {
                "method": "initialize",
                "params": {
                    "clientInfo": {"name": "cli_agent_bridge", "version": "0.1.0"},
                    "capabilities": {"experimentalApi": True},
                },
            }
        )
        await client.write({"method": "initialized"})
        rows, cursors, cursor = [], set(), None
        for _ in range(50):
            page = await client.request({"method": "model/list", "params": {"limit": 100, "cursor": cursor}})
            rows.extend(
                model_record(
                    m["model"],
                    m.get("displayName"),
                    m.get("inputModalities"),
                    reasoning_efforts=[e["reasoningEffort"] for e in m.get("supportedReasoningEfforts", [])],
                    service_tiers=[t["id"] for t in m.get("serviceTiers", [])],
                )
                for m in page["data"]
            )
            cursor = page.get("nextCursor")
            if not cursor:
                return rows, "Codex App Server model/list"
            if not isinstance(cursor, str) or cursor in cursors:
                break
            cursors.add(cursor)
        raise BridgeError("invalid_catalog", "Model directory pagination did not finish")
    finally:
        closing = asyncio.create_task(client.close())
        try:
            await asyncio.shield(closing)
        except asyncio.CancelledError:
            await closing
            raise


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def deepseek_models(environment):
    key = environment.get("ANTHROPIC_AUTH_TOKEN") or environment.get("ANTHROPIC_API_KEY")
    if not key:
        raise BridgeError(
            "discovery_auth_missing", "DeepSeek model discovery needs the profile's existing API credential"
        )
    proxies = {
        scheme: environment.get(scheme + "_proxy") or environment.get(scheme.upper() + "_PROXY")
        for scheme in ("http", "https")
    }
    opener = build_opener(ProxyHandler({k: v for k, v in proxies.items() if v}), NoRedirect())
    try:
        with opener.open(
            Request("https://api.deepseek.com/models", headers={"Authorization": "Bearer " + key}), timeout=10
        ) as response:
            body = response.read(2 * 1024 * 1024 + 1)
    except HTTPError as error:
        raise BridgeError("discovery_http_error", f"DeepSeek model directory returned HTTP {error.code}") from None
    if len(body) > 2 * 1024 * 1024:
        raise BridgeError("invalid_catalog", "Model directory response is too large")
    return [
        model_record(
            m["id"],
            m.get("name"),
            m.get("input_modalities"),
            m.get("context_window"),
            reasoning_efforts=(m.get("effort") or {}).get("supported_levels"),
        )
        for m in json.loads(body)["data"]
        if "anthropic_messages" in (m.get("api_capabilities") or {})
    ]


class ModelDirectory:
    def __init__(self, cwd):
        self.cwd, self.cache, self.locks = cwd, {}, {}

    async def get(self, profile, refresh=False):
        started = time.time()
        fingerprint = json.dumps(asdict(profile), sort_keys=True)
        async with self.locks.setdefault(profile.name, asyncio.Lock()):
            cached = self.cache.get(profile.name)
            if cached and cached[0] != fingerprint:
                cached = None
            previous = cached[1] if cached else None
            if previous and (previous["checked_at"] >= started or not refresh and previous["retry_after"] > started):
                return previous
            result = {
                "profile": profile.name,
                "runtime": profile.runtime,
                "provider": profile.provider,
                "checked_at": time.time(),
                "models": [],
                "source": None,
                "stale": False,
                "error": None,
            }
            try:
                async with asyncio.timeout(30):
                    rows, result["source"] = await native_models(profile, self.cwd)
                if not rows or len(rows) > 5000:
                    raise BridgeError("invalid_catalog", "Model directory is empty or exceeds 5000 entries")
                result["models"] = list(
                    {
                        m["model"]: {
                            **m,
                            "verified": profile.verified and m["model"] == profile.model,
                            "verified_at": profile.verified_at if m["model"] == profile.model else None,
                        }
                        for m in rows
                    }.values()
                )
                result["discovered_at"] = time.time()
                result["retry_after"] = result["discovered_at"] + CACHE_SECONDS
            except Exception as error:
                # Native errors may contain credential-bearing URLs or response bodies; never publish their text.
                result["error"] = {
                    "code": getattr(error, "code", "discovery_failed"),
                    "message": "Model discovery failed (" + type(error).__name__ + "); configured models remain usable",
                }
                result["retry_after"] = time.time() + 30
                result["discovered_at"] = previous.get("discovered_at") if previous else None
                if previous:
                    result.update(models=previous["models"], source=previous["source"], stale=bool(previous["models"]))
            result["checked_at"] = time.time()
            self.cache[profile.name] = fingerprint, result
            return result

    async def resolve(self, profile, model):
        if model is None or model == profile.model:
            return profile
        model_record(model)
        result = await self.get(profile)
        if result["error"] or result["stale"]:
            raise BridgeError("discovery_unavailable", "Cannot select a new model until its directory refresh succeeds")
        record = next((m for m in result["models"] if m["model"] == model), None)
        if not record:
            raise BridgeError("unknown_model", "Exact model ID is absent from this profile's current directory")
        efforts = record.get("reasoning_efforts")
        if profile.effort and efforts is not None and profile.effort not in efforts:
            raise BridgeError(
                "unsupported_model_settings", "This model does not support the profile's reasoning effort"
            )
        tier = "priority" if profile.service_tier == "fast" else profile.service_tier
        if tier and "service_tiers" in record and tier not in ["default", *record["service_tiers"]]:
            raise BridgeError("unsupported_model_settings", "This model does not support the profile's service tier")
        return replace(
            profile,
            model=model,
            verified=False,
            verified_at=None,
            price=None,
            context_window=record["context_window"],
            input_types=[v for v in profile.input_types if v in (record["input_types"] or ["text"])],
        ).validate()
