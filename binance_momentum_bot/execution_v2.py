"""Long-only capped IOC execution. No credentials, network, or mode decisions here."""
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import math


class Blocked(ValueError):
    pass


class Uncertain(RuntimeError):
    """An order may exist: reconcile its client ID; never blindly resubmit."""


def positive(value):
    if isinstance(value, bool):
        raise Blocked('INVALID_NUMBER')
    try:
        value = float(value)
    except (ValueError, TypeError):
        raise Blocked('INVALID_NUMBER')
    if not math.isfinite(value) or value <= 0:
        raise Blocked('INVALID_NUMBER')
    return value


def quantize(value, step, up=False):
    value, step = positive(value), positive(step)
    result = (Decimal(str(value)) / Decimal(str(step))).to_integral_value(
        rounding=ROUND_UP if up else ROUND_DOWN) * Decimal(str(step))
    return float(result)


def capped_plan(signal, config, filters):
    price = positive(signal['price'])
    stop = quantize(signal['stop'], filters['tick'])
    target = positive(signal['target'])
    rr = positive(config['min_rr'])
    # (target-entry)/(entry-stop) >= rr, with absolute signal stop/target.
    rr_cap = (target + rr * stop) / (1 + rr)
    # Fallback reward is relative to actual entry. Include its smaller reward in
    # the same RR cap: entry*pct >= min_rr*(entry-stop). This also keeps a later
    # LIVE -> SHADOW transition consistent with the entry risk budget.
    fallback_fraction = positive(config.get('fallback_tp_pct', 1.)) / 100
    if rr > fallback_fraction:
        rr_cap = min(rr_cap, rr * stop / (rr - fallback_fraction))
    caps = dict(slippage=price * (1 + positive(config['slippage_pct']) / 100),
                band=positive(signal['entry_high']), rr=rr_cap)
    cap = quantize(min(caps.values()), filters['tick'])
    if not 0 < stop < cap < target:
        raise Blocked('INVALID_PRICE_GEOMETRY')
    qty = quantize(positive(config['margin']) * positive(config['leverage']) / cap, filters['step'])
    if (qty < positive(filters['min_qty']) or qty > positive(filters['max_qty'])
            or qty * cap < positive(filters['min_notional'])):
        raise Blocked('LOT_OR_NOTIONAL_FILTER')
    return dict(cap=cap, caps=caps, stop=stop, target=target, qty=qty,
                risk=qty * (cap - stop) + qty * cap * config['fee_reserve_pct'] / 100)


def marketable_price(ask, plan, tick):
    price = quantize(ask, tick, up=True)
    if price > plan['cap'] or price <= plan['stop']:
        raise Blocked('NO_CHASING_OR_INVALIDATED')
    return price


def reconcile_fills(order, fills, symbol):
    """Only exchange fills for this exact order count. Missing data never becomes a proxy."""
    expected = float(order.get('executedQty', 0))
    if not math.isfinite(expected) or expected < 0:
        raise Uncertain('INVALID_EXECUTED_QTY')
    unique = {}
    for fill in fills:
        if str(fill.get('orderId')) != str(order['orderId']) or fill.get('symbol') != symbol:
            raise Uncertain('FILL_ALLOCATION_MISMATCH')
        if fill.get('side') != 'BUY':
            raise Uncertain('FILL_SIDE_MISMATCH')
        key = str(fill['id'])
        if key in unique and fill != unique[key]:
            raise Uncertain('CONFLICTING_FILL_ID')
        unique[key] = fill
    qty = sum(positive(f['qty']) for f in unique.values())
    if not math.isclose(qty, expected, rel_tol=1e-9, abs_tol=1e-12):
        raise Uncertain('INCOMPLETE_FILL_ALLOCATION')
    if not qty:
        return dict(qty=0, vwap=None, fills=[])
    vwap = sum(positive(f['price']) * positive(f['qty']) for f in unique.values()) / qty
    return dict(qty=qty, vwap=vwap, fills=list(unique.values()))


async def submit_ioc(exchange, symbol, qty, price, client_id):
    try:
        order = await exchange.request('POST', '/fapi/v1/order', dict(
            symbol=symbol, side='BUY', type='LIMIT', timeInForce='IOC',
            quantity=format(qty, '.12f').rstrip('0').rstrip('.'),
            price=format(price, '.12f').rstrip('0').rstrip('.'),
            newClientOrderId=client_id, newOrderRespType='RESULT'))
    except Exception:
        try:
            order = await exchange.request('GET', '/fapi/v1/order',
                dict(symbol=symbol, origClientOrderId=client_id))
        except Exception as exc:
            raise Uncertain('ENTRY_RESPONSE_UNKNOWN') from exc
    return validate_ioc(order, symbol, client_id)


def validate_ioc(order, symbol, client_id):
    try:
        executed = float(order['executedQty'])
        if isinstance(order['executedQty'], bool) or not math.isfinite(executed) or executed < 0 or not order['orderId']:
            raise ValueError('invalid order quantity/identity')
    except (KeyError, ValueError, TypeError) as exc:
        raise Uncertain('INCOMPLETE_ORDER_RESPONSE') from exc
    if (order.get('symbol') != symbol or order.get('clientOrderId') != client_id
            or order.get('side') != 'BUY' or order.get('type') != 'LIMIT'
            or order.get('timeInForce') != 'IOC'):
        raise Uncertain('ORDER_IDENTITY_MISMATCH')
    if order.get('status') not in ('FILLED', 'EXPIRED', 'CANCELED', 'REJECTED'):
        raise Uncertain('IOC_NOT_TERMINAL')
    return order
