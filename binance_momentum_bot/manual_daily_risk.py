"""One-per-TRT-day, explicitly approved extra LIVE risk budget.

This module has no exchange/network access and never modifies trade history.
It does not authorize trading or bypass cooldown/max-position gates.
"""

import json
import math

KEY = "manual_daily_risk_resume_live_v1"
VERSION = 1
MAX_EXTRA_USDT = 50.0
MAX_EXTRA_BALANCE_PCT = 5.0
MAX_TOTAL_DAILY_LOSS_PCT = 10.0


def _finite_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def max_extra_usdt(start_balance, daily_pct):
    start = _finite_number(start_balance)
    daily = _finite_number(daily_pct)
    if start is None or daily is None or start <= 0 or not 0.25 <= daily <= 25:
        return 0.0
    possible = min(
        MAX_EXTRA_USDT,
        start * MAX_EXTRA_BALANCE_PCT / 100.0,
        start * max(0.0, MAX_TOTAL_DAILY_LOSS_PCT - daily) / 100.0,
    )
    return max(0.0, math.floor(possible * 100 + 1e-9) / 100.0)


def encode_resume(*, local_date, start_balance, daily_pct, extra_usdt, actor, created_ts):
    cap = max_extra_usdt(start_balance, daily_pct)
    extra = _finite_number(extra_usdt)
    if extra is None or round(extra, 2) != extra or not 0 < extra <= cap:
        raise ValueError("Requested extra risk is invalid or exceeds the one-day cap")
    if not local_date or not actor or not isinstance(created_ts, int):
        raise ValueError("Missing exact authorization scope")
    return json.dumps({
        "version": VERSION,
        "local_date": str(local_date),
        "scope": "LIVE",
        "start_balance": float(start_balance),
        "daily_max_loss_pct": float(daily_pct),
        "extra_usdt": extra,
        "active": True,
        "actor": str(actor),
        "created_ts": created_ts,
    }, sort_keys=True, separators=(",", ":"))


def recorded_today(raw, today):
    """Fail closed against a second grant even if an earlier record was revoked."""
    try:
        record = json.loads(raw or "")
        return (isinstance(record, dict)
                and record.get("local_date") == today
                and record.get("scope") == "LIVE")
    except (ValueError, TypeError):
        return False


def active_extra(raw, daily_row, daily_pct, scope):
    if str(scope).upper() != "LIVE":
        return 0.0
    try:
        record = json.loads(raw or "")
        if not isinstance(record, dict) or record.get("version") != VERSION:
            return 0.0
        if record.get("scope") != "LIVE" or record.get("active") is not True:
            return 0.0
        if record.get("local_date") != daily_row["local_date"]:
            return 0.0
        start = _finite_number(record.get("start_balance"))
        configured = _finite_number(record.get("daily_max_loss_pct"))
        extra = _finite_number(record.get("extra_usdt"))
        if start is None or configured is None or extra is None:
            return 0.0
        if abs(start - float(daily_row["start_balance"])) > 1e-8:
            return 0.0
        if abs(configured - float(daily_pct)) > 1e-9:
            return 0.0
        if round(extra, 2) != extra or extra <= 0 or extra > max_extra_usdt(start, configured):
            return 0.0
        return extra
    except (TypeError, ValueError, KeyError, OverflowError):
        return 0.0


def revoke(raw, today):
    try:
        record = json.loads(raw or "")
        if not isinstance(record, dict) or record.get("local_date") != today or record.get("scope") != "LIVE":
            return None
        if record.get("active") is not True:
            return None
        record["active"] = False
        return json.dumps(record, sort_keys=True, separators=(",", ":"))
    except (ValueError, TypeError):
        return None


def permits_daily_lock(daily_row, extra):
    """Override applies ONLY to an existing daily loss lock, not other locks."""
    return (extra > 0
            and bool(daily_row.get("locked"))
            and str(daily_row.get("lock_reason") or "").startswith("DAILY_LOSS_"))
