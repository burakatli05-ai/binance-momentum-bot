"""Read-only view of positions opened during the current Istanbul day."""
import asyncio
import json
from contextlib import closing
from datetime import datetime

import daytrades
import position_cycles
import trade_views
from position_observer import roe_values
from telegram_cards import IST, ownership, number

OPEN_STATUSES = ('OPEN', 'PARTIAL', 'PROTECTIVE_PARTIAL')


def _price_return(record):
    entry = record.get('entry_price')
    price = record.get('exit_price')
    if not entry or not price:
        return None
    direction = 1 if record.get('side') == 'LONG' else -1
    return (price / entry - 1) * 100 * direction


def _dry_record(bot, trade, now):
    result = dict(trade, ownership='BOT DRY', provenance='BOT_LEDGER')
    is_open = trade.get('status') in OPEN_STATUSES
    result['is_open'] = is_open
    if is_open:
        state = bot['states'].get(trade['symbol'])
        price = state.last_price if state and state.last_trade_receive_ms and 0 <= now-state.last_trade_receive_ms <= 10000 else None
        entry = trade.get('entry_price')
        lev = trade.get('leverage')
        ret = (price/entry-1) * (1 if trade.get('side') == 'LONG' else -1) if price and entry else None
        qty = trade.get('expected_qty') if trade.get('expected_qty') is not None else trade.get('qty')
        result['exit_price'] = price
        result['net_pnl'] = ret * entry * qty if ret is not None and entry and qty is not None else None
        result['roe'] = ret * lev * 100 if ret is not None and lev else None
        result['notional'] = abs(qty * price) if qty is not None and price else trade.get('notional_usdt')
    else:
        result['notional'] = trade.get('notional_usdt')
    result['price_return_pct'] = _price_return(result)
    return result


def _observer_openings(c, start, now):
    c.row_factory = __import__('sqlite3').Row
    rows = []
    for row in c.execute("""SELECT * FROM position_observer_events
        WHERE event='OPEN_OBSERVED' AND event_time_ms BETWEEN ? AND ?
        ORDER BY event_time_ms""", (start, now)):
        item = dict(row)
        try:
            detail = json.loads(item.get('detail_json') or '{}')
        except Exception:
            detail = {}
        if detail.get('adopted'):
            continue
        rows.append(item)
    return rows


def _active_states(c):
    c.row_factory = __import__('sqlite3').Row
    return {(r['symbol'], r['position_side']): dict(r) for r in c.execute(
        "SELECT * FROM position_observer_state WHERE active=1")}


def _match_closed(open_event, closed):
    candidates = [r for r in closed
        if r.get('symbol') == open_event.get('symbol')
        and (r.get('position_side') or 'BOTH') == (open_event.get('position_side') or 'BOTH')
        and r.get('opened_ts_ms') is not None]
    if not candidates:
        return None
    event_ts = int(open_event['event_time_ms'])
    near = [r for r in candidates if 0 <= event_ts-int(r['opened_ts_ms']) <= 120000]
    return min(near, key=lambda r: abs(event_ts-int(r['opened_ts_ms']))) if near else None


async def build(bot, session, cache):
    now = bot['now_ms']()
    start = daytrades.local_start(now)
    with closing(bot['db_connect']()) as c:
        ledger = daytrades.rows(c, 'SELECT * FROM autotrade_trades')
        observer = _observer_openings(c, start, now)
        active_states = _active_states(c)

    records = [_dry_record(bot, tr, now) for tr in ledger
               if tr.get('mode') == 'DRY' and start <= (tr.get('opened_ts_ms') or -1) <= now]
    notices = []

    async def history_request(path, params):
        if path not in ('/fapi/v3/positionRisk','/fapi/v1/income','/fapi/v1/userTrades','/fapi/v1/order'):
            raise ValueError('read-only account endpoint required')
        return await bot['binance_signed_request'](session, 'GET', path, params)

    closed_live = []
    try:
        closed_live, history_notices = await asyncio.wait_for(position_cycles.load(history_request, start, now, ledger), 45)
        notices.extend(history_notices)
        closed_live = [dict(r, is_open=False, price_return_pct=_price_return(r))
                       for r in closed_live if start <= (r.get('opened_ts_ms') or -1) <= now]
        records.extend(closed_live)
    except Exception:
        notices.append('LIVE kapanış geçmişi alınamadı; bugün açılıp kapanan bazı LIVE pozisyonlar eksik olabilir.')

    async def current_request(session_obj, method, path):
        if method != 'GET' or path not in ('/fapi/v3/positionRisk','/fapi/v1/symbolConfig'):
            raise ValueError('read-only positions required')
        return await asyncio.wait_for(bot['binance_signed_request'](session_obj, method, path), 15)

    try:
        positions = await current_request(session, 'GET', '/fapi/v3/positionRisk')
        if not isinstance(positions, list):
            raise ValueError('positions unavailable')
        if any(abs(float(p.get('positionAmt') or 0)) > 1e-12 for p in positions):
            await cache.refresh(session, current_request)
        opening_by_instance = {r.get('position_instance_id'): r for r in observer}
        for p in positions:
            amount = float(p.get('positionAmt') or 0)
            if abs(amount) <= 1e-12:
                continue
            side = p.get('positionSide') or 'BOTH'
            state = active_states.get((p['symbol'], side))
            if not state:
                continue
            opening = opening_by_instance.get(state.get('position_instance_id'))
            if not opening:
                continue
            entry = float(p['entryPrice']); mark = float(p['markPrice'])
            direction = 'LONG' if side == 'LONG' or side == 'BOTH' and amount > 0 else 'SHORT'
            lev, _ = cache.get(p['symbol'])
            roe, pnl, _ = roe_values(p, direction, entry, mark, lev)
            record = dict(symbol=p['symbol'], position_side=side, side=direction,
                ownership=opening.get('source') or state.get('source'), leverage=lev,
                opened_ts_ms=opening['event_time_ms'], closed_ts_ms=None,
                entry_price=entry, exit_price=mark, net_pnl=pnl, roe=roe,
                notional=abs(amount*mark), is_open=True,
                provenance='OPEN_OBSERVED', price_return_pct=None)
            record['price_return_pct'] = _price_return(record)
            records.append(record)
    except Exception:
        notices.append('LIVE açık pozisyonlar alınamadı; bugün açık kalan bazı LIVE pozisyonlar eksik olabilir.')

    represented = {(r.get('symbol'), r.get('position_side') or 'BOTH') for r in records if not r.get('is_open')}
    for opening in observer:
        key = (opening.get('symbol'), opening.get('position_side') or 'BOTH')
        if key in represented or _match_closed(opening, closed_live):
            continue
        if key in active_states:
            continue
        records.append(dict(symbol=opening['symbol'], position_side=opening.get('position_side') or 'BOTH',
            side=opening.get('direction'), ownership=opening.get('source'), leverage=opening.get('leverage'),
            opened_ts_ms=opening.get('event_time_ms'), closed_ts_ms=None,
            entry_price=opening.get('entry_price'), exit_price=None, net_pnl=None, roe=None,
            notional=None, is_open=False, provenance='OPEN_OBSERVED_ONLY'))

    records.sort(key=lambda r: r.get('opened_ts_ms') or 0)
    closed = [r for r in records if not r.get('is_open')]
    opened = [r for r in records if r.get('is_open')]
    realized = [r.get('net_pnl') for r in closed if r.get('net_pnl') is not None]
    unrealized = [r.get('net_pnl') for r in opened if r.get('net_pnl') is not None]
    wins = sum((r.get('net_pnl') or 0) > 0 for r in closed if r.get('net_pnl') is not None)
    losses = sum((r.get('net_pnl') or 0) < 0 for r in closed if r.get('net_pnl') is not None)
    summary = [
        f'Toplam {len(records)}  •  Açık {len(opened)} / Kapalı {len(closed)}',
        f'Kapalı: Kazanç {wins} / Kayıp {losses}  •  Realized {number(sum(realized) if realized else 0," USDT",True)}',
        f'Açık P/L: {number(sum(unrealized) if unrealized else 0," USDT",True)}',
    ]
    return dict(title='BUGÜN AÇILAN POZİSYONLAR',
        date=datetime.fromtimestamp(now/1000, IST).strftime('%d.%m.%Y'),
        summary=summary,
        cards=[trade_views.card(r, bool(r.get('is_open'))) for r in records],
        notices=notices,
        caption=False)
