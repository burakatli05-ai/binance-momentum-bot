"""Shared Binance REST cooldown; never bypass an exchange IP ban."""
import asyncio
import math
import re
import time


class RateLimitBlocked(RuntimeError):
    """Request was not sent because a new entry has no safe REST budget."""


def _positive(value):
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def observe(state, headers, status=200, body="", *, fallback_429=60.0, fallback_418=120.0):
    now = time.monotonic()
    try:
        raw = headers.get("X-MBX-USED-WEIGHT-1M")
        if raw is not None:
            state["used_weight_1m"] = max(0, int(float(raw)))
            state["observed_mono"] = now
    except (TypeError, ValueError, OverflowError):
        pass
    if int(status or 0) not in (418, 429):
        return 0.0
    retry = _positive(headers.get("Retry-After"))
    match = re.search(r"\bbanned\s+until\s+(\d{10,13})\b", str(body or ""), re.I)
    if match:
        epoch = int(match.group(1))
        if epoch >= 10**12:
            epoch /= 1000.0
        retry = max(retry or 0.0, epoch - time.time(), 1.0)
    if retry is None:
        retry = fallback_418 if int(status) == 418 else fallback_429
    prior = float(state.get("blocked_until_mono") or 0.0)
    deadline = max(prior, now + max(1.0, retry))
    state["blocked_until_mono"] = deadline
    return max(0.0, deadline - now)


async def gate(state, *, soft_weight, soft_cooldown, priority=False, reject_if_blocked=False):
    while True:
        now = time.monotonic()
        remaining = float(state.get("blocked_until_mono") or 0.0) - now
        if remaining > 0:
            if reject_if_blocked:
                raise RateLimitBlocked(f"BINANCE_REST_COOLDOWN: {remaining:.1f}s remaining; request not sent")
            await asyncio.sleep(remaining)
            continue
        used = int(state.get("used_weight_1m") or 0)
        observed = float(state.get("observed_mono") or 0.0)
        if observed and now - observed > 70.0:
            state["used_weight_1m"] = 0
            used = 0
        if used >= soft_weight and (reject_if_blocked or not priority):
            if reject_if_blocked:
                raise RateLimitBlocked(f"BINANCE_REST_SOFT_LIMIT: {used}; request not sent")
            await asyncio.sleep(soft_cooldown)
            continue
        return
