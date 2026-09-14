"""Read-only, bounded fill reconstruction. No stored trading state is changed."""
import asyncio
from collections import defaultdict
from decimal import Decimal
import daytrades

DAY = 86400000
MAX_REQUESTS = 120


def decimal(value):
    result = Decimal(str(value))
    if not result.is_finite(): raise ValueError('nonfinite fill')
    return result


def anchor(positions, end):
    """Only snapshots not updated after the history cutoff can anchor inventory."""
    result = {}
    if not isinstance(positions, list): raise ValueError('position snapshot unavailable')
    for p in positions:
        key = (p['symbol'], p.get('positionSide') or 'BOTH')
        qty = decimal(p['positionAmt'])
        if key[1] == 'SHORT': qty = -abs(qty)
        if key[1] == 'LONG': qty = abs(qty)
        if key in result: raise ValueError('duplicate snapshot position')
        result[key] = (qty if (p.get('updateTime') is not None and int(p['updateTime']) <= end) or not qty and p.get('updateTime') is None else None)
    return result


def reconstruct(fills, orders, ledger, end_positions, start, end, complete=True):
    groups = defaultdict(list)
    for f in fills:
        groups[(f['symbol'], f.get('positionSide') or 'BOTH')].append(f)
    anchors = anchor(end_positions, end)
    output = []
    for key, values in groups.items():
        values = list({str(f['id']): f for f in values}.values())
        values.sort(key=lambda f: (int(f['time']), int(f['id'])))
        end_qty = anchors.get(key, Decimal(0))
        if not complete or end_qty is None:
            for r in daytrades.group_fills(values, orders, ledger):
                if start <= r['closed_ts_ms'] <= end:
                    r.update(cycle_complete=False, provenance='UNANCHORED_CLOSE_RECORD',
                             net_pnl=None, roe=None, entry_price=None, opened_ts_ms=None, leverage=None)
                    output.append(r)
            continue
        deltas = []
        for f in values:
            qty = decimal(f['qty'])
            if qty <= 0 or decimal(f['price']) <= 0 or f['side'] not in ('BUY', 'SELL'):
                raise ValueError('invalid fill')
            deltas.append(qty if f['side'] == 'BUY' else -qty)
        inventory = end_qty - sum(deltas, Decimal(0))
        cycle = None
        def fresh(qty, opened=None):
            return dict(symbol=key[0], position_side=key[1], side='LONG' if qty > 0 else 'SHORT',
                ownership='UNKNOWN OWNERSHIP', opened_ts_ms=opened, closed_ts_ms=None,
                entry_price=None, exit_price=None, leverage=None, roe=None, close_reason=None,
                entry_qty=Decimal(0), entry_quote=Decimal(0), exit_qty=Decimal(0), exit_quote=Decimal(0),
                gross=Decimal(0), fees=Decimal(0), fees_known=True, pnl_known=True,
                cycle_complete=opened is not None, fill_ids=[], entry_orders=set(), bot_matches=[],
                provenance='ANCHORED_COMPLETE_FILLS' if opened is not None else 'OPEN_BEFORE_LOOKBACK')
        if inventory: cycle = fresh(inventory)
        for f, delta in zip(values, deltas):
            order = orders.get((key[0], str(f['orderId'])), {})
            # Metadata contradicting inferred inventory signals a gap, not an opening.
            closing = inventory and (inventory > 0) != (delta > 0)
            if (order.get('reduceOnly') is True or order.get('closePosition') is True or
                decimal(f.get('realizedPnl') or 0) != 0) and not closing:
                raise ValueError('fill/position boundary inconsistent')
            if (order.get('reduceOnly') is True or order.get('closePosition') is True) and abs(delta) > abs(inventory):
                raise ValueError('reduce-only fill cannot reverse a position')
            if key[1] == 'LONG' and inventory + delta < 0 or key[1] == 'SHORT' and inventory + delta > 0:
                raise ValueError('hedge inventory inconsistent')
            remaining = abs(delta)
            while remaining:
                is_close = bool(inventory and (inventory > 0) != (delta > 0))
                amount = min(abs(inventory), remaining) if is_close else remaining
                if cycle is None: cycle = fresh(delta, int(f['time']))
                cycle['fill_ids'].append(str(f['id']))
                fee = decimal(f.get('commission') or 0) * amount / abs(delta)
                if f.get('commission') is None or f.get('commissionAsset') != 'USDT': cycle['fees_known'] = False
                cycle['fees'] += fee
                if is_close:
                    cycle['exit_qty'] += amount; cycle['exit_quote'] += amount * decimal(f['price'])
                    if f.get('realizedPnl') is None: cycle['pnl_known'] = False
                    cycle['gross'] += decimal(f.get('realizedPnl') or 0)
                    cycle['close_reason'] = {'STOP': 'STOP (emir türü)', 'STOP_MARKET': 'STOP (emir türü)',
                        'TAKE_PROFIT': 'TAKE PROFIT (emir türü)', 'TAKE_PROFIT_MARKET': 'TAKE PROFIT (emir türü)'}.get(order.get('origType') or order.get('type'))
                else:
                    cycle['entry_qty'] += amount; cycle['entry_quote'] += amount * decimal(f['price'])
                    cycle['entry_orders'].add(str(f['orderId']))
                    cycle['bot_matches'].extend(r for r in ledger if r.get('symbol') == key[0] and r.get('mode') == 'LIVE'
                        and str(r.get('entry_order_id')) == str(f['orderId']))
                inventory += amount if delta > 0 else -amount
                remaining -= amount
                if inventory == 0:
                    cycle['closed_ts_ms'] = int(f['time'])
                    cycle['exit_price'] = float(cycle['exit_quote'] / cycle['exit_qty'])
                    cycle['entry_price'] = float(cycle['entry_quote'] / cycle['entry_qty']) if cycle['cycle_complete'] else None
                    cycle['net_pnl'] = float(cycle['gross'] - cycle['fees']) if cycle['cycle_complete'] and cycle['fees_known'] and cycle['pnl_known'] else None
                    matches = cycle.pop('bot_matches')
                    # Use ledger leverage/margin only for an exactly matched entry order.
                    if matches and len(cycle['entry_orders']) == 1:
                        r = matches[0]; cycle['ownership'] = 'BOT LIVE'
                        cycle['leverage'] = r.get('leverage')
                        margin = r.get('margin_usdt')
                        cycle['roe'] = cycle['net_pnl'] / margin * 100 if cycle['net_pnl'] is not None and margin else None
                    cycle['fill_ids'] = sorted(set(cycle['fill_ids']))
                    for field in ('entry_qty','entry_quote','exit_qty','exit_quote','gross','fees','entry_orders'):
                        cycle.pop(field)
                    if start <= cycle['closed_ts_ms'] <= end: output.append(cycle)
                    cycle = None
        if inventory != end_qty: raise ValueError('inventory reconciliation failed')
    return sorted(output, key=lambda r: r['closed_ts_ms'])


async def load(request, start, end, ledger):
    """Look back seven days before local midnight; stop at an explicit API budget."""
    remaining = MAX_REQUESTS
    async def get(path, params):
        nonlocal remaining
        if remaining <= 0: raise RuntimeError('history request budget exhausted')
        remaining -= 1
        return await asyncio.wait_for(request(path, params), 15)
    before = await get('/fapi/v3/positionRisk', {})
    before_anchor = anchor(before, end)
    income = []
    income_complete = False
    for page in range(1, 21):
        batch = await get('/fapi/v1/income', dict(startTime=start, endTime=end, limit=1000, page=page))
        if not isinstance(batch, list): raise ValueError('income unavailable')
        income.extend(batch)
        if len(batch) < 1000: income_complete = True; break
    symbols = sorted({r['symbol'] for r in income if r.get('symbol', '').endswith('USDT')} |
                     {r['symbol'] for r in ledger if r.get('mode') == 'LIVE' and start <= (r.get('closed_ts_ms') or -1) <= end})
    notices = []
    if not income_complete: notices.append('Sembol keşfi kısmi: gelir API sınırı.')
    fills = []; orders = {}; complete_symbols = set()
    for symbol in symbols:
        try:
            windows = [(start-7*DAY, start-1), (start, end)]
            symbol_fills = []; complete = True
            while windows:
                lo, hi = windows.pop()
                batch = await get('/fapi/v1/userTrades', dict(symbol=symbol, startTime=lo, endTime=hi, limit=1000))
                if not isinstance(batch, list): raise ValueError('fills unavailable')
                if len(batch) >= 1000 and hi > lo:
                    mid = (lo+hi)//2; windows.extend(((lo,mid),(mid+1,hi))); continue
                if len(batch) >= 1000: complete = False
                if any(f.get('symbol') != symbol or not lo <= int(f['time']) <= hi for f in batch):
                    raise ValueError('fill outside requested window')
                symbol_fills.extend(batch)
            fills.extend(symbol_fills)
            if complete: complete_symbols.add(symbol)
            for oid in sorted({str(f['orderId']) for f in symbol_fills})[:30]:
                if remaining <= 2: break  # Reserve inventory revalidation.
                try:
                    order = await get('/fapi/v1/order', dict(symbol=symbol, orderId=oid))
                    if isinstance(order, dict) and str(order.get('orderId')) == oid: orders[(symbol, oid)] = order
                except Exception: pass  # Close reason/ownership remain unknown.
        except Exception:
            notices.append(symbol+': geçmiş eksik; tam döngü kabul edilmedi.')
    after = await get('/fapi/v3/positionRisk', {})
    after_anchor = anchor(after, end)
    output = []
    for symbol in symbols:
        keys = {key for key in before_anchor.keys() | after_anchor.keys() if key[0] == symbol}
        stable = all(before_anchor.get(k, Decimal(0)) == after_anchor.get(k, Decimal(0)) and after_anchor.get(k, Decimal(0)) is not None for k in keys)
        subset = [f for f in fills if f['symbol'] == symbol]
        try:
            output.extend(reconstruct(subset, orders, ledger, after, start, end, stable and symbol in complete_symbols))
        except (ValueError, KeyError, ArithmeticError):
            notices.append(symbol+': fill/pozisyon tutarsız; döngü alanları bilinmiyor.')
            output.extend(reconstruct(subset, orders, ledger, after, start, end, False))
    return output, notices
