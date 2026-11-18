"""English and Simplified Chinese presentation without process-global locale changes."""
from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from functools import lru_cache

SUPPORTED_LANGUAGES = ("en", "zh-CN")
LANGUAGE_ENV = "AGENT_BRIDGE_LANG"
_language: ContextVar[str | None] = ContextVar("bridge_language", default=None)


def _canonical(value: str) -> str | None:
    value = value.strip().split(".", 1)[0].split("@", 1)[0].replace("_", "-").lower()
    if value == "auto":
        return "auto"
    if value in {"c", "posix", "en"} or value.startswith("en-"):
        return "en"
    if value in {"zh", "zh-cn", "zh-sg", "zh-hans"} or value.startswith("zh-hans-"):
        return "zh-CN"
    return None


def validate_language(value: str) -> str:
    if not isinstance(value, str) or (language := _canonical(value)) is None:
        raise ValueError("language must be en, zh-CN or auto")
    return language


def resolve_language(value: str | None = None) -> str:
    """Resolve an explicit choice, context or environment; unsupported locales use English."""
    value = value if value is not None else _language.get() or os.environ.get(LANGUAGE_ENV, "en")
    language = _canonical(value) if isinstance(value, str) else None
    if language == "auto":
        system = next((os.environ[k] for k in ("LC_ALL", "LC_MESSAGES", "LANG") if os.environ.get(k)), "en")
        language = _canonical(system)
    return language if language in SUPPORTED_LANGUAGES else "en"


def configured_language(value: str | None = None) -> str:
    """Apply an active CLI choice, environment, config, then the English default."""
    return resolve_language(_language.get() or (os.environ[LANGUAGE_ENV] if LANGUAGE_ENV in os.environ else value))


@contextmanager
def use_language(value: str | None):
    """Limit a language choice to this call/task and restore it on errors."""
    token = _language.set(resolve_language(value))
    try:
        yield _language.get()
    finally:
        _language.reset(token)


@lru_cache(maxsize=1)
def _chinese_catalog() -> dict[str, str]:
    from . import i18n_cli, i18n_core, i18n_notifications, i18n_web

    return {**i18n_core.MESSAGES, **i18n_cli.MESSAGES, **i18n_notifications.MESSAGES, **i18n_web.MESSAGES}


def get_catalog(language: str | None = None) -> dict[str, str]:
    """Return a copy suitable for serializing into the browser's language catalog."""
    return dict(_chinese_catalog()) if resolve_language(language) == "zh-CN" else {}


def translate(message: str, *, language: str | None = None, **values) -> str:
    translated = _chinese_catalog().get(message, message) if resolve_language(language) == "zh-CN" else message
    return translated.format(**values) if values else translated
