from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT = (ROOT / "binance_momentum_bot" / "bot.py").read_text(encoding="utf-8")


def block(start: str, end: str) -> str:
    return BOT.split(start, 1)[1].split(end, 1)[0]


def test_binance_rest_has_shared_weight_guard_and_429_cooldown():
    assert 'BINANCE_RATE_LIMIT_SOFT_WEIGHT' in BOT
    assert 'X-MBX-USED-WEIGHT-1M' in BOT
    assert 'Retry-After' in BOT
    assert 'async def _binance_rate_gate' in BOT
    fetch = block('async def fetch_json(', 'async def load_symbols')
    signed = block('async def binance_signed_request(', 'async def _at_get_account_config')
    assert 'await _binance_rate_gate(priority=False)' in fetch
    assert '_binance_observe_rate_headers(r.headers, r.status)' in fetch
    assert 'priority or method.upper() in ("POST", "DELETE")' in signed
    assert '_binance_observe_rate_headers(r.headers, r.status)' in signed


def test_account_config_is_cached_instead_of_polled_each_reconcile_tick():
    snapshot = block('async def _at_get_account_config', 'def _at_cache_account_balance')
    reconcile = block('async def autotrade_reconcile_loop', 'def _at_panel_text')
    assert 'BINANCE_ACCOUNT_CONFIG_CACHE_SECONDS' in snapshot
    assert '"/fapi/v1/accountConfig"' in snapshot
    assert 'positions_only=True' in reconcile
    assert 'avoiding accountConfig on' in snapshot


def test_reconcile_balance_poll_is_throttled_and_execution_uses_priority_lane():
    reconcile = block('async def autotrade_reconcile_loop', 'def _at_panel_text')
    entry = block('async def autotrade_handle_premium', 'def autotrade_on_tick')
    lookup = block('async def _at_query_order_by_client', 'async def _at_place_market_entry')
    assert 'cache_age >= 30' in snapshot
    assert '"/fapi/v3/balance"' in snapshot
    assert '_at_account_snapshot(session)' in entry
    snapshot = block('async def _at_account_snapshot', 'def _at_cache_account_balance')
    assert 'priority: bool = True' in snapshot
    assert 'priority=True' in lookup


def test_tp_fallback_race_check_does_not_fetch_full_account_snapshot():
    reconcile = block('async def autotrade_reconcile_loop', 'def _at_panel_text')
    assert reconcile.count('_at_account_snapshot(session, priority=True, positions_only=True)') == 2
    assert 'fresh_positions = await binance_signed_request' not in reconcile
