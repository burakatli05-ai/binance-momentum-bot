"""Delivery-only policy for public EARLY cards.

This module never changes detection, recording, Premium delivery, or order handling.
The Telegram terminal may override the environment default and persists that
single boolean on the Railway volume.
"""
import json
import os
import threading
from pathlib import Path
from typing import Optional

_TRUE_VALUES = {"1", "true", "yes", "on"}
_LOCK = threading.Lock()
_LOADED = False
_OVERRIDE: Optional[bool] = None


def _environment_default() -> bool:
    return os.getenv("EARLY_SIGNAL_TELEGRAM_ENABLED", "0").strip().lower() in _TRUE_VALUES


def _state_path() -> Path:
    return Path(
        os.getenv(
            "EARLY_NOTIFICATION_STATE_PATH",
            "/data/early_notification_terminal.json",
        ).strip()
        or "/data/early_notification_terminal.json"
    )


def _read_override() -> Optional[bool]:
    path = _state_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or "enabled" not in payload:
        return None
    return bool(payload["enabled"])


def _ensure_loaded() -> None:
    global _LOADED, _OVERRIDE
    if _LOADED:
        return
    with _LOCK:
        if not _LOADED:
            _OVERRIDE = _read_override()
            _LOADED = True


def early_signal_notifications_enabled() -> bool:
    _ensure_loaded()
    if _OVERRIDE is not None:
        return _OVERRIDE
    return _environment_default()


def early_signal_notification_status() -> dict:
    _ensure_loaded()
    return {
        "enabled": early_signal_notifications_enabled(),
        "source": "persistent" if _OVERRIDE is not None else "environment",
    }


def set_early_signal_notifications_enabled(enabled: bool) -> bool:
    """Persist the delivery switch atomically, then update the in-process cache."""
    global _LOADED, _OVERRIDE
    value = bool(enabled)
    path = _state_path()
    payload = json.dumps(
        {"version": 1, "enabled": value},
        sort_keys=True,
        ensure_ascii=True,
    )
    with _LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(payload + "\n", encoding="utf-8")
        os.replace(temporary, path)
        _OVERRIDE = value
        _LOADED = True
    return value
