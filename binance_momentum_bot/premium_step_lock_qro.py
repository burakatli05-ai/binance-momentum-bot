"""Disabled Premium QRO production protocol. No import-time I/O.

Semantic port of hash-verified local-only QRO evidence; see provenance manifest.
REST observations are sequential, never claimed atomic. Unknown state quarantines.
No entry/market-close, account setters, credentials or retrying write transport.
"""
import asyncio
from contextlib import closing
from copy import deepcopy
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import json
import time
import premium_step_lock as sl

CONTRACT = 'PREMIUM_QRO_1.0'
MANAGEMENT_ENABLED = False
ACTUAL_TRIGGER_CERTIFIED = False
LIVE_ACTIVATION_READY = False
SCHEMA = {'qro_contract_version': 'TEXT', 'qro_state': 'TEXT'}
BUDGET = 'QRO_RECONCILIATION_BUDGET_EXHAUSTED'
MUTATION = 'MANUAL_OR_EXTERNAL_POSITION_MUTATION'
EXTRA = 'EXTRA_OR_UNOWNED_ORDER_FAIL_CLOSED'
REENTRY = 'SAME_SYMBOL_REENTRY_BLOCKED_RESIDUAL_STATE'
STATES = ('NEW_STOP_WORKING', 'NEW_STOP_PARTIALLY_FILLED', 'NEW_STOP_FILLED',
          'OLD_STOP_PARTIALLY_FILLED', 'OLD_STOP_FILLED', 'POSITION_PARTIALLY_REDUCED',
          'POSITION_FLAT_DURING_REPLACEMENT', 'POSITION_QUANTITY_CHANGED_EXTERNALLY',
          'TERMINAL_STATE_UNKNOWN', 'RESIDUAL_ORDER_CLEANUP_REQUIRED', 'QUARANTINED_MANUAL_REVIEW')
TERMINAL = {'CANCELED', 'EXPIRED', 'REJECTED', 'FINISHED'}
CHILD_TERMINAL = {'CANCELED', 'EXPIRED', 'FILLED'}
_locks = {}


class Block(ValueError):
    pass


class Uncertain(Exception):
    """Sanitized transport error; provider bodies and signed URLs never retained."""


def number(value, *, positive=False):
    if isinstance(value, bool):
        raise Block('INVALID_DECIMAL')
    try:
        value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise Block('INVALID_DECIMAL') from None
    if not value.is_finite() or (positive and value <= 0):
        raise Block('INVALID_DECIMAL')
    return value


def client_id(trade_id, revision):
    if type(trade_id) is not int or trade_id <= 0 or type(revision) is not int or revision < 0:
        raise Block('INVALID_QRO_IDENTITY')
    cid = f'QRO1-{trade_id}-{revision}'
    if len(cid) > 36:
        raise Block('INVALID_QRO_IDENTITY')
    return cid


def migrate(conn):
    cols = {r[1] for r in conn.execute('PRAGMA table_info(autotrade_trades)')}
    for name, kind in SCHEMA.items():
        if name not in cols:
            conn.execute(f'ALTER TABLE autotrade_trades ADD COLUMN {name} {kind}')


def new_trade_fields(tr):
    """Only for the NEW entry transaction; never call as a migration/recovery.

    Live entry remains blocked by the application's independent activation gate.
    The worker re-verifies the actual entry order and its fills before any write.
    """
    sl.validate_state(tr)
    if tr.get('stop_algo_id') or tr.get('stop_client_id') or tr.get('qro_state'):
        raise Block('EXISTING_POSITION_NOT_CONVERTIBLE')
    if not tr.get('entry_order_id') or not tr.get('entry_client_id') or tr.get('position_side') != 'BOTH':
        raise Block('VERIFIED_ENTRY_IDENTITY_REQUIRED')
    state = dict(version=CONTRACT, phase='AWAITING_INITIAL_STOP', revision=0,
                 active=None, pending=None, intents={}, cancels=[], children={}, child_status={},
                 reads=0, cancel_writes=0, query_failures={}, quarantine=False,
                 residual=False, unresolved=[], events=[], serial=0)
    return dict(qro_contract_version=CONTRACT, qro_state=encode(state))


def encode(state):
    return json.dumps(state, sort_keys=True, separators=(',', ':'))


class SQLiteStore:
    """Commit-before-effect with compare-and-swap; no swallowed persistence errors."""
    def __init__(self, connect):
        self.connect = connect

    def load(self, tid):
        with closing(self.connect()) as db:
            cur = db.execute('SELECT * FROM autotrade_trades WHERE id=?', (tid,))
            row = cur.fetchone()
            if row is None:
                raise Block('TRADE_NOT_FOUND')
            return dict(zip([d[0] for d in cur.description], row))

    def save(self, tid, expected, state, **fields):
        state = deepcopy(state)
        state['serial'] += 1
        fields['qro_state'] = encode(state)
        if set(fields) - {'qro_state', 'pending_stop_client_id', 'pending_stop_algo_id',
                         'pending_stop_price', 'stop_revision', 'replacement_status',
                         'stop_algo_id', 'stop_client_id', 'sl_active_stop_price', 'stop_price',
                         'manual_intervention', 'last_error'}:
            raise Block('INVALID_STATE_WRITE')
        with closing(self.connect()) as db:
            with db:
                cur = db.execute('UPDATE autotrade_trades SET '+','.join(k+'=?' for k in fields)
                    +' WHERE id=? AND qro_state=? AND qro_contract_version=?',
                    [*fields.values(), tid, expected, CONTRACT])
                if cur.rowcount != 1:
                    raise Block('QRO_CONCURRENT_OR_STALE_STATE')
        return state, fields['qro_state']


def request(tr, cid, qty, target):
    return dict(algoType='CONDITIONAL', symbol=tr['symbol'], side='SELL', positionSide='BOTH',
                type='STOP_MARKET', quantity=str(qty), reduceOnly='true', closePosition='false',
                triggerPrice=str(target), workingType='CONTRACT_PRICE', priceProtect='false', clientAlgoId=cid)


def verify_order(order, intent, tr):
    p = intent['params']
    try:
        owned = p['clientAlgoId'] == client_id(int(tr['id']), int(p['clientAlgoId'].rsplit('-',1)[1]))
    except (ValueError, IndexError, KeyError):
        owned = False
    if not owned:
        raise Block('QRO_CLIENT_ID_NOT_OWNED')
    exact = dict(symbol=tr['symbol'], side='SELL', positionSide='BOTH', orderType='STOP_MARKET',
                 algoType='CONDITIONAL', workingType='CONTRACT_PRICE', clientAlgoId=p['clientAlgoId'])
    if not isinstance(order, dict) or any(order.get(k) != v for k, v in exact.items()):
        raise Block('QRO_ORDER_IDENTITY_MISMATCH')
    if (not order.get('algoId') or order.get('reduceOnly') is not True
            or order.get('closePosition') is not False or order.get('priceProtect') is not False
            or number(order.get('quantity'), positive=True) != number(p['quantity'], positive=True)
            or number(order.get('triggerPrice'), positive=True) != number(p['triggerPrice'], positive=True)
            or intent.get('algoId') and str(order['algoId']) != intent['algoId']):
        raise Block('QRO_ORDER_SEMANTICS_MISMATCH')
    if order.get('algoStatus') not in {'NEW', *TERMINAL}:
        raise Block('TERMINAL_STATE_UNKNOWN')
    return str(order['algoId'])


class BinancePremiumStepLockQROExchange:
    """Actual USD-M endpoint/field adapter; a single-shot request callable is injected.

    Application binds the existing signed helper. Tests inject fake HTTP responses.
    Disabled by default, including reads. No generic helper behavior is changed.
    """
    def __init__(self, request_once, *, enabled=False, query_budget=80, cancel_budget=4,
                 clock=time.monotonic, wall=time.time):
        self.request_once = request_once
        self.enabled = enabled
        self.query_budget = min(80, max(0, int(query_budget)))
        self.cancel_budget = min(4, max(0, int(cancel_budget)))
        self.clock, self.wall = clock, wall
        self.save_state = None

    async def _call(self, method, route, params=None):
        if not self.enabled:
            raise Block('QRO_ADAPTER_DISABLED')
        allowed = {'/fapi/v1/positionSide/dual', '/fapi/v3/positionRisk', '/fapi/v1/exchangeInfo',
                   '/fapi/v2/ticker/price', '/fapi/v1/algoOrder', '/fapi/v1/openAlgoOrders',
                   '/fapi/v1/order', '/fapi/v1/openOrders', '/fapi/v1/userTrades'}
        if route not in allowed or method not in {'GET', 'POST', 'DELETE'}:
            raise Block('QRO_ROUTE_NOT_ALLOWED')
        if method == 'POST' and route != '/fapi/v1/algoOrder':
            raise Block('QRO_ROUTE_NOT_ALLOWED')
        if method == 'DELETE' and route not in {'/fapi/v1/algoOrder', '/fapi/v1/order'}:
            raise Block('QRO_ROUTE_NOT_ALLOWED')
        if self.save_state is None:
            raise Block('DURABLE_CONTEXT_REQUIRED')
        if method == 'GET':
            if self.s['reads'] >= self.query_budget:
                raise Block(BUDGET)
            self.save_state(reads=self.s['reads']+1)
        start = self.clock()
        try:
            value = await self.request_once(method, route, dict(params or {}))
        except Exception:
            raise Uncertain('QRO_TRANSPORT_UNCERTAIN') from None
        if self.clock()-start > 2:
            raise Uncertain('QRO_STALE_OBSERVATION')
        return value

    async def account_mode(self):
        mode = await self._call('GET', '/fapi/v1/positionSide/dual')
        if not isinstance(mode, dict) or mode.get('dualSidePosition') is not False:
            raise Block('QRO_ACCOUNT_MODE_NOT_CERTIFIED')
        return 'ONE_WAY'

    async def position(self, symbol):
        rows = await self._call('GET', '/fapi/v3/positionRisk', dict(symbol=symbol))
        if not isinstance(rows, list):
            raise Block(MUTATION)
        # USD-M v3 may omit inactive zero positions; an empty exact-symbol response is flat.
        if any(r.get('symbol') != symbol or r.get('positionSide') != 'BOTH' for r in rows) or len(rows) > 1:
            raise Block(MUTATION)
        qty = number(rows[0].get('positionAmt')) if rows else Decimal(0)
        if qty < 0:
            raise Block(MUTATION)
        return qty

    async def exchange_filters(self, symbol):
        data = await self._call('GET', '/fapi/v1/exchangeInfo')
        rows = [r for r in data.get('symbols', []) if r.get('symbol') == symbol]
        if len(rows) != 1 or rows[0].get('status') != 'TRADING' or rows[0].get('contractType') != 'PERPETUAL':
            raise Block('QRO_FILTERS_UNAVAILABLE')
        filters = {r['filterType']: r for r in rows[0]['filters']}
        for kind in ('PRICE_FILTER', 'LOT_SIZE', 'MARKET_LOT_SIZE', 'MIN_NOTIONAL'):
            if kind not in filters:
                raise Block('QRO_FILTERS_UNAVAILABLE')
        return filters

    async def current_market_reference(self, symbol):
        value = await self._call('GET', '/fapi/v2/ticker/price', dict(symbol=symbol))
        if (value.get('symbol') != symbol or type(value.get('time')) is not int
                or abs(self.wall()*1000-value['time']) > 2000):
            raise Block('INVALID_MARKET_REFERENCE_KEEP_OLD_STOP')
        return number(value.get('price'), positive=True)

    async def query_by_client_identity(self, symbol, cid):
        if cid not in self.s['intents']:
            raise Block('QRO_CLIENT_ID_NOT_OWNED')
        for _ in range(3):
            used = self.s['query_failures'].get(cid, 0)
            if used >= 3:
                raise Block(BUDGET)
            try:
                order = await self._call('GET', '/fapi/v1/algoOrder', dict(clientAlgoId=cid))
                oid = verify_order(order, self.s['intents'][cid], self.tr)
                intents = deepcopy(self.s['intents'])
                intents[cid]['algoId'] = oid
                self.save_state(intents=intents)
                return order
            except Uncertain:
                failures = dict(self.s['query_failures']); failures[cid] = used+1
                self.save_state(query_failures=failures)
        raise Block(BUDGET)

    async def open_bot_owned_algo_orders(self, symbol, trade_id):
        if trade_id != self.tr['id']:
            raise Block(EXTRA)
        rows = await self._call('GET', '/fapi/v1/openAlgoOrders', dict(symbol=symbol))
        if not isinstance(rows, list) or len(rows) > 2:
            raise Block(EXTRA)
        seen = set()
        for row in rows:
            cid = row.get('clientAlgoId')
            if cid not in self.s['intents'] or cid in seen:
                raise Block(EXTRA)
            verify_order(row, self.s['intents'][cid], self.tr)
            seen.add(cid)
        return rows

    async def _normal_orders(self):
        rows = await self._call('GET', '/fapi/v1/openOrders', dict(symbol=self.tr['symbol']))
        if not isinstance(rows, list):
            raise Block(EXTRA)
        for row in rows:
            if str(row.get('orderId')) not in self.s['children']:
                raise Block(EXTRA)
            self._verify_child(row, str(row['orderId']), self.s['children'][str(row['orderId'])])
        return rows

    def _verify_child(self, row, oid, cid):
        p = self.s['intents'][cid]['params']
        if (str(row.get('orderId')) != oid or row.get('symbol') != self.tr['symbol']
                or row.get('positionSide') != 'BOTH' or row.get('side') != 'SELL'
                or row.get('reduceOnly') is not True or row.get('type') != 'MARKET'
                or number(row.get('origQty'), positive=True) != number(p['quantity'])
                or row.get('status') not in {'NEW', 'PARTIALLY_FILLED', *CHILD_TERMINAL}):
            raise Block('TERMINAL_STATE_UNKNOWN')

    async def _fills(self, row, *, entry=False):
        oid = str(row['orderId'])
        fills = await self._call('GET', '/fapi/v1/userTrades', dict(symbol=self.tr['symbol'], orderId=oid, limit=1000))
        if not isinstance(fills, list) or len(fills) >= 1000 or len({r.get('id') for r in fills}) != len(fills):
            raise Block('TERMINAL_STATE_UNKNOWN')
        if any(str(r.get('orderId')) != oid or r.get('symbol') != self.tr['symbol']
               or r.get('side') != ('BUY' if entry else 'SELL') or r.get('positionSide') != 'BOTH'
               or r.get('id') is None for r in fills):
            raise Block('TERMINAL_STATE_UNKNOWN')
        qty = sum((number(r.get('qty'), positive=True) for r in fills), Decimal(0))
        if qty != number(row.get('executedQty')):
            raise Block('TERMINAL_STATE_UNKNOWN')
        return qty, (sum((number(r['qty'])*number(r['price'], positive=True) for r in fills), Decimal(0))/qty if qty else None)

    async def _entry(self):
        row = await self._call('GET', '/fapi/v1/order', dict(symbol=self.tr['symbol'], orderId=self.tr['entry_order_id']))
        qty = number(self.tr['expected_qty'], positive=True)
        if (str(row.get('orderId')) != str(self.tr['entry_order_id'])
                or row.get('clientOrderId') != self.tr['entry_client_id']
                or row.get('symbol') != self.tr['symbol'] or row.get('positionSide') != 'BOTH'
                or row.get('side') != 'BUY' or row.get('status') != 'FILLED'
                or number(row.get('origQty')) != qty):
            raise Block(MUTATION)
        used, vwap = await self._fills(row, entry=True)
        if used != qty or vwap != number(self.tr['sl_entry_vwap']):
            raise Block(MUTATION)

    def _event(self, phase):
        events = self.s['events']
        self.save_state(phase=phase, events=(events+[phase])[-64:] if not events or events[-1] != phase else events)

    async def _observe(self):
        start = self.clock()
        before = await self.position(self.tr['symbol'])
        orders, children, used, partial = {}, dict(self.s['children']), Decimal(0), False
        for cid in self.s['intents']:
            order = await self.query_by_client_identity(self.tr['symbol'], cid)
            orders[cid] = order
            child = order.get('actualOrderId')
            label = 'NEW' if cid == self.s['pending'] else 'OLD'
            if child not in (None, '', 0, '0'):
                oid = str(child)
                if oid in children and children[oid] != cid:
                    raise Block('TERMINAL_STATE_UNKNOWN')
                children[oid] = cid
                self.save_state(children=children)
                row = await self._call('GET', '/fapi/v1/order', dict(symbol=self.tr['symbol'], orderId=oid))
                self._verify_child(row, oid, cid)
                fill, _ = await self._fills(row)
                statuses = dict(self.s.get('child_status', {})); statuses[oid] = row['status']
                self.save_state(child_status=statuses)
                if not 0 <= fill <= number(self.s['intents'][cid]['params']['quantity']):
                    raise Block('TERMINAL_STATE_UNKNOWN')
                used += fill
                if row['status'] == 'PARTIALLY_FILLED' or 0 < fill < number(row['origQty']):
                    self._event(label+'_STOP_PARTIALLY_FILLED'); partial = True
                elif row['status'] == 'FILLED':
                    self._event(label+'_STOP_FILLED')
                elif row['status'] == 'NEW':
                    partial = True  # Triggered child not yet terminal: no replacement.
            elif order['algoStatus'] == 'FINISHED':
                raise Block('TERMINAL_STATE_UNKNOWN')
        algos = await self.open_bot_owned_algo_orders(self.tr['symbol'], self.tr['id'])
        normals = await self._normal_orders()
        after = await self.position(self.tr['symbol'])
        if self.clock()-start > 2:
            raise Block('QRO_OBSERVATION_WINDOW_EXCEEDED')
        if before != after:
            self._event('POSITION_PARTIALLY_REDUCED' if after < before else 'POSITION_QUANTITY_CHANGED_EXTERNALLY')
            raise Block(MUTATION)
        expected = number(self.tr['expected_qty'], positive=True)
        if after > 0 and after != expected-used:
            self._event('POSITION_QUANTITY_CHANGED_EXTERNALLY')
            raise Block(MUTATION)
        if used > expected:
            raise Block('TERMINAL_STATE_UNKNOWN')
        if after == 0:
            self._event('POSITION_FLAT_DURING_REPLACEMENT')
        elif partial or used:
            self._event('POSITION_PARTIALLY_REDUCED')
            raise Block('PARTIAL_FILL_REQUIRES_MANUAL_REVIEW')
        return after, orders, algos, normals

    async def create_quantity_reduce_only_stop(self, cid):
        intent = self.s['intents'][cid]
        if intent['post_attempted']:
            raise Block('QRO_REPOST_FORBIDDEN')
        intents = deepcopy(self.s['intents']); intents[cid]['post_attempted'] = True
        self.save_state(intents=intents)  # committed BEFORE single effect
        try:
            await self._call('POST', '/fapi/v1/algoOrder', intent['params'])
        except Uncertain:
            self._event('POST_UNCERTAIN')

    async def cancel_exact_owned_stop(self, cid, *, child_id=None):
        intent = self.s['intents'].get(cid)
        key = 'child:'+str(child_id) if child_id else cid
        if not intent or not intent.get('algoId') or child_id and self.s['children'].get(str(child_id)) != cid:
            raise Block('QRO_CLIENT_ID_NOT_OWNED')
        if key in self.s['cancels']:
            return  # every later recovery is query-only
        if self.s['cancel_writes'] >= self.cancel_budget:
            raise Block(BUDGET)
        self.save_state(cancels=self.s['cancels']+[key], cancel_writes=self.s['cancel_writes']+1)
        try:
            await self._call('DELETE', '/fapi/v1/order' if child_id else '/fapi/v1/algoOrder',
                dict(symbol=self.tr['symbol'], orderId=str(child_id)) if child_id else dict(algoId=intent['algoId']))
        except Uncertain:
            self._event('CANCEL_UNCERTAIN')

    async def cleanup_owned_residual_orders(self, observation):
        qty, orders, algos, normals = observation
        if qty != 0:
            raise Block('RESIDUAL_CLEANUP_REQUIRES_FLAT')
        self._event('RESIDUAL_ORDER_CLEANUP_REQUIRED')
        self.save_state(residual=True)
        for row in normals:
            oid = str(row['orderId'])
            await self.cancel_exact_owned_stop(self.s['children'][oid], child_id=oid)
        for row in algos:
            await self.cancel_exact_owned_stop(row['clientAlgoId'])
        # Both exact identities and child fill ledgers are read again, then zeros.
        for _ in range(3):
            obs = await self._observe()
            if obs[0] != 0:
                raise Block(MUTATION)
            if (not obs[2] and not obs[3] and all(o['algoStatus'] in TERMINAL for o in obs[1].values())
                    and all(v in CHILD_TERMINAL for v in self.s.get('child_status', {}).values())):
                if await self.verify_flat_and_clean():
                    self.save_state(phase='FLAT_CLEAN', pending=None, unresolved=[], residual=False,
                                    quarantine=False, fields=dict(pending_stop_client_id=None,
                                    pending_stop_algo_id=None, pending_stop_price=None,
                                    replacement_status='FLAT_CLEAN'))
                    return 'FLAT_CLEAN'
        raise Block(BUDGET)

    async def verify_flat_and_clean(self):
        a = await self.position(self.tr['symbol'])
        algos = await self.open_bot_owned_algo_orders(self.tr['symbol'], self.tr['id'])
        normals = await self._normal_orders()
        b = await self.position(self.tr['symbol'])
        return a == b == 0 and not algos and not normals

    async def same_symbol_reentry_safe(self, symbol, trades):
        if any(local_residual(t) for t in trades if t.get('symbol') == symbol):
            return False
        return await self.verify_flat_and_clean()

    async def check_reentry(self, symbol, trades, store):
        """Fresh exact zeros even after FLAT_CLEAN; disabled binding blocks reentry."""
        if not self.enabled or any(local_residual(t) for t in trades):
            return False
        if not trades:
            return False  # No QRO ownership context to query.
        tr = trades[-1]
        lock = _locks.setdefault((id(asyncio.get_running_loop()), tr['id']), asyncio.Lock())
        async with lock:
            self.tr = store.load(tr['id'])
            raw = self.tr['qro_state']; self.s = json.loads(raw)
            def save_state(fields=None, **values):
                nonlocal raw
                self.s, raw = store.save(tr['id'], raw, dict(self.s, **values), **(fields or {}))
            self.save_state = save_state
            try:
                await self.account_mode()
                return await self.same_symbol_reentry_safe(symbol, trades)
            except (Block, Uncertain, KeyError, TypeError):
                return False
            finally:
                self.save_state = None

    async def reconcile_trade(self, tid, store):
        if not self.enabled:
            return 'QRO_ADAPTER_DISABLED'
        # One worker per trade in-process; SQLite CAS also prevents concurrent effects.
        lock = _locks.setdefault((id(asyncio.get_running_loop()), tid), asyncio.Lock())
        async with lock:
            self.tr = store.load(tid)
            if self.tr.get('exit_profile') != sl.PROFILE or self.tr.get('qro_contract_version') != CONTRACT:
                return 'QRO_PROFILE_NOT_BOUND'
            sl.validate_state(self.tr)
            if self.tr.get('side') != 'LONG' or self.tr.get('position_side') != 'BOTH':
                return 'QRO_ACCOUNT_MODE_NOT_CERTIFIED'
            raw = self.tr.get('qro_state')
            self.s = json.loads(raw)
            if self.s['version'] != CONTRACT:
                raise Block('QRO_STATE_CONTRACT_MISMATCH')
            def save_state(fields=None, **values):
                nonlocal raw
                next_state = dict(self.s, **values)
                if next_state == self.s and not fields:
                    return
                self.s, raw = store.save(tid, raw, next_state, **(fields or {}))
            self.save_state = save_state
            if self.s['phase'] == 'FLAT_CLEAN':
                return 'FLAT_CLEAN'
            if self.s['quarantine'] or self.tr.get('manual_intervention'):
                # Quarantine forbids new protection/resize. It must still permit
                # bounded flat cleanup after a partial fill or operator close.
                # The durable remaining read budget survives restarts.
                try:
                    await self.account_mode()
                    obs = await self._observe()
                    if obs[0] == 0:
                        return await self.cleanup_owned_residual_orders(obs)
                except (Block, Uncertain, KeyError, TypeError, ArithmeticError):
                    pass
                finally:
                    self.save_state = None
                return 'QUARANTINED_MANUAL_REVIEW'
            try:
                revision = self.s['revision']
                active, pending = self.s['active'], self.s['pending']
                if (set(self.s['intents']) != {c for c in (active,pending) if c}
                        or pending and pending != client_id(tid,revision)
                        or active and active != client_id(tid,revision-int(bool(pending)))
                        or len(self.s['intents']) > 2 or active and active == pending):
                    raise Block('QRO_CLIENT_ID_NOT_OWNED')
                if number(self.tr.get('qty', self.tr['expected_qty'])) != number(self.tr['expected_qty']):
                    raise Block(MUTATION)
                await self.account_mode()
                await self._entry()
                if self.s['pending']:
                    # A durable intent may have posted before a crash: NEVER repost.
                    return await self._finish()
                obs = await self._observe()
                if obs[0] == 0:
                    return await self.cleanup_owned_residual_orders(obs)
                if obs[3] or len(obs[2]) != int(bool(self.s['active'])):
                    raise Block(EXTRA)
                active = self.s['active']
                target = number(self.tr['desired_stop_price'], positive=True)
                if active and target <= number(self.tr['sl_active_stop_price']):
                    # No unresolved episode: reset observation budget for next worker cycle.
                    self.save_state(reads=0, query_failures={})
                    return 'STABLE'
                if active and obs[1][active]['algoStatus'] != 'NEW':
                    raise Block('TERMINAL_STATE_UNKNOWN')
                filters = await self.exchange_filters(self.tr['symbol'])
                market = await self.current_market_reference(self.tr['symbol'])
                tick = number(filters['PRICE_FILTER']['tickSize'], positive=True)
                qty = obs[0]  # exact exchange source, not a DB-derived order size
                for kind in ('LOT_SIZE', 'MARKET_LOT_SIZE'):
                    f = filters[kind]; step = number(f['stepSize'], positive=True)
                    rounded = (qty/step).to_integral_value(rounding=ROUND_FLOOR)*step
                    if rounded != qty or not number(f['minQty']) <= qty <= number(f['maxQty']):
                        raise Block('QRO_INVALID_OR_DUST_QUANTITY')
                if qty*market < number(filters['MIN_NOTIONAL']['notional'], positive=True):
                    raise Block('QRO_MIN_NOTIONAL')
                derived = (sl.levels(self.tr['sl_entry_vwap'], self.tr['last_step_price'], tick)[2]
                           if active else number(self.tr['sl_initial_stop']))
                if target != derived or target % tick or target >= market:
                    return 'INVALID_TARGET_KEEP_OLD_STOP'
                if await self.position(self.tr['symbol']) != qty:
                    raise Block(MUTATION)
                revision = self.s['revision']+1 if active else 0
                cid = client_id(tid, revision)
                intents = deepcopy(self.s['intents'])
                if len(intents) >= 2 or cid in intents:
                    raise Block(EXTRA)
                intents[cid] = dict(params=request(self.tr, cid, qty, target), post_attempted=False)
                self.save_state(pending=cid, revision=revision, intents=intents,
                    phase='INTENT', unresolved=[cid], fields=dict(pending_stop_client_id=cid,
                    pending_stop_price=str(target), stop_revision=revision, replacement_status='INTENT'))
                await self.create_quantity_reduce_only_stop(cid)
                return await self._finish()
            except (Block, Uncertain, KeyError, TypeError, ArithmeticError) as exc:
                reason = str(exc) if isinstance(exc, Block) else 'TERMINAL_STATE_UNKNOWN'
                if reason == MUTATION:
                    self._event('POSITION_QUANTITY_CHANGED_EXTERNALLY')
                self._event(reason if reason in STATES else 'QUARANTINED_MANUAL_REVIEW')
                self.save_state(quarantine=True, fields=dict(last_error=reason,
                    replacement_status='QUARANTINED_MANUAL_REVIEW', manual_intervention=1))
                return reason
            finally:
                self.save_state = None

    async def _finish(self):
        cid, old = self.s['pending'], self.s['active']
        obs = await self._observe()
        if obs[0] == 0:
            return await self.cleanup_owned_residual_orders(obs)
        new = obs[1][cid]
        if new['algoStatus'] != 'NEW' or new.get('actualOrderId') not in (None, '', 0, '0'):
            raise Block('TERMINAL_STATE_UNKNOWN')
        if {r['clientAlgoId'] for r in obs[2]} != {c for c in (old, cid) if c and obs[1][c]['algoStatus'] == 'NEW'}:
            raise Block(EXTRA)
        self._event('NEW_STOP_WORKING')
        self.save_state(fields=dict(pending_stop_algo_id=str(new['algoId']), replacement_status='NEW_STOP_WORKING'))
        if old and obs[1][old]['algoStatus'] == 'NEW':
            await self.cancel_exact_owned_stop(old)
        for _ in range(3):
            final = await self._observe()  # dual query after cancel, even on response loss
            if final[0] == 0:
                return await self.cleanup_owned_residual_orders(final)
            if old and final[1][old]['algoStatus'] == 'NEW':
                continue
            if (old and final[1][old]['algoStatus'] != 'CANCELED'
                    or final[1][cid]['algoStatus'] != 'NEW'
                    or len(final[2]) != 1 or final[2][0]['clientAlgoId'] != cid or final[3]):
                raise Block('TERMINAL_STATE_UNKNOWN')
            target = self.s['intents'][cid]['params']['triggerPrice']
            self.save_state(active=cid, pending=None, intents={cid:self.s['intents'][cid]},
                cancels=[], cancel_writes=0, reads=0, query_failures={}, children={}, unresolved=[], phase='STABLE',
                fields=dict(stop_algo_id=str(new['algoId']), stop_client_id=cid, stop_price=float(target),
                sl_active_stop_price=target, pending_stop_client_id=None, pending_stop_algo_id=None,
                pending_stop_price=None, replacement_status='STABLE'))
            return 'REPLACED'
        raise Block(BUDGET)


def local_residual(tr):
    if tr.get('pending_stop_client_id') or tr.get('pending_stop_algo_id'):
        return True
    if tr.get('qro_contract_version') is None:
        return tr.get('exit_profile') == sl.PROFILE and tr.get('status') != 'CLOSED'
    try:
        s = json.loads(tr['qro_state'])
        return (s['version'] != CONTRACT or s['phase'] != 'FLAT_CLEAN' or s['pending']
                or s['unresolved'] or s['quarantine'] or s['residual'] or tr.get('manual_intervention'))
    except (ValueError, KeyError, TypeError):
        return True


def drain_blockers(trades, orders):
    trades = list(trades)
    blockers = []
    if any(local_residual(t) for t in trades):
        blockers.append(REENTRY)
    children = set()
    for tr in trades:
        if tr.get('qro_state'):
            try:
                children.update(json.loads(tr['qro_state']).get('children', {}))
            except (ValueError, TypeError):
                blockers.append('QRO_STATE_UNREADABLE')
    if orders is not None and any(str(o.get('clientAlgoId', o.get('clientOrderId', ''))).startswith('QRO1-')
                                 or str(o.get('orderId')) in children for o in orders):
        blockers.append('QRO_RESIDUAL_ORDER_RECONCILIATION_REQUIRED')
    return blockers
