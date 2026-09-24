"""Isolated Early pilot: durable intentions, fail-closed risk, explicit runtime opt-in.

The selector is the existing unvalidated FAST_EARLY_V2 research score. No V1
threshold or Premium risk/exit configuration is owned by this module.
"""
import asyncio
from contextlib import closing
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone, timedelta
import hashlib
import json
import math
import secrets
import time

from execution_v2 import Blocked, Uncertain, capped_plan, marketable_price, reconcile_fills, submit_ioc, quantize, validate_ioc
from position_management_v2 import Policy, ShadowPosition


@dataclass
class Config:
    margin: float = 5.
    leverage: int = 1
    min_score: float = 90.
    max_positions: int = 1
    daily_trades: int = 3
    daily_loss: float = 1.
    cooldown_s: int = 900
    slippage_pct: float = .10
    min_rr: float = 1.
    fee_reserve_pct: float = .20
    signal_ttl_ms: int = 3000
    retry: int = 0
    first_lock_pct: float = .25
    fallback_tp_pct: float = 1.
    execution: bool = True
    score_gate: bool = True

    def validate(self):
        limits = dict(margin=(5, 100), leverage=(1, 5), min_score=(85, 100),
            max_positions=(1, 3), daily_trades=(1, 20), daily_loss=(.1, 25),
            cooldown_s=(60, 86400), slippage_pct=(.01, .3), min_rr=(.5, 5),
            fee_reserve_pct=(.1, 2), signal_ttl_ms=(200, 5000), retry=(0, 1),
            first_lock_pct=(.20, .25), fallback_tp_pct=(.3, 3.))
        integers = {'leverage','max_positions','daily_trades','cooldown_s','signal_ttl_ms','retry'}
        for name, (low, high) in limits.items():
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int,float)) or not math.isfinite(value) or not low <= value <= high:
                raise Blocked('INVALID_CONFIG_' + name)
            if name in integers and type(value) is not int:
                raise Blocked('INTEGER_REQUIRED_' + name)
        if type(self.execution) is not bool or type(self.score_gate) is not bool:
            raise Blocked('BOOLEAN_REQUIRED')
        return self


class Pilot:
    def __init__(self, connect, *, live_allowed=False, score_validated=False, profit_live_allowed=False, clock=None):
        self.connect = connect
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.live_allowed = live_allowed
        self.score_validated = score_validated
        self.profit_live_allowed = profit_live_allowed
        self.lock = asyncio.Lock()  # also acquired by the narrow Premium entry wrapper
        self.mode = 'OFF'
        self.profit_mode = 'SHADOW'
        self.profit_live_pending = False
        self.pending = {}
        self.engines = {}
        self.active = {}
        self.halted = False
        self.last_error = ''
        self.generation = 0
        with closing(connect()) as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS early_v2_settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS early_v2_trades(id TEXT PRIMARY KEY,signal_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,mode TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL,
                    UNIQUE(signal_id,mode));
                CREATE TABLE IF NOT EXISTS early_v2_events(id INTEGER PRIMARY KEY,ts_ms INTEGER NOT NULL,
                    event TEXT NOT NULL,payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS early_v2_selector_shadow(
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    decision_ms INTEGER NOT NULL,
                    price REAL NOT NULL,
                    base_score REAL,
                    v2_score REAL,
                    v2_label TEXT NOT NULL,
                    qualified INTEGER NOT NULL,
                    min_score REAL NOT NULL,
                    reasons TEXT NOT NULL,
                    features TEXT NOT NULL
                );
            ''')
            saved = dict(db.execute('SELECT key,value FROM early_v2_settings'))
            valid = {f.name for f in fields(Config)}
            self.cfg = Config(**{k: json.loads(v) for k,v in saved.items() if k in valid}).validate()
            self.halted = json.loads(saved.get('halted', 'false'))
            for payload, in db.execute("SELECT payload FROM early_v2_trades WHERE status NOT IN ('CLOSED','NO_FILL','BLOCKED')"):
                tr = json.loads(payload)
                self.active[tr['id']] = tr
            db.commit()
        self.event('BOOT_OFF', {'recovered': len(self.active), 'profit_mode': self.profit_mode})

    def event(self, event, payload):
        with closing(self.connect()) as db:
            db.execute('INSERT INTO early_v2_events(ts_ms,event,payload) VALUES (?,?,?)',
                       (self.clock(), event, json.dumps(payload, allow_nan=False)))
            db.commit()

    def setting(self, key, value):
        with closing(self.connect()) as db:
            db.execute('INSERT OR REPLACE INTO early_v2_settings VALUES (?,?)', (key, json.dumps(value, allow_nan=False)))
            db.commit()

    def configure(self, key, value):
        if self.mode == 'LIVE':
            raise Blocked('SET_OFF_BEFORE_CONFIG_CHANGE')
        values = asdict(self.cfg)
        if key not in values:
            raise Blocked('UNKNOWN_SETTING')
        values[key] = value
        config = Config(**values).validate()
        self.setting(key, value)
        self.cfg = config
        self.generation += 1
        self.pending.clear()

    def record_selector(self, signal_id, symbol, decision_ms, price, base_score,
                            v2_score, v2_label, reasons, features):
        score = float(v2_score) if type(v2_score) in (int, float) and not isinstance(v2_score, bool) and math.isfinite(v2_score) else None
        qualified = bool(v2_label == 'FAST_EARLY_V2' and score is not None and score >= self.cfg.min_score)
        payload_reasons = json.dumps(list(reasons or []), ensure_ascii=False, allow_nan=False)
        payload_features = json.dumps(dict(features or {}), ensure_ascii=False, allow_nan=False)
        with closing(self.connect()) as db:
            db.execute(
                '''INSERT INTO early_v2_selector_shadow(
                    signal_id,symbol,decision_ms,price,base_score,v2_score,v2_label,
                    qualified,min_score,reasons,features
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(signal_id) DO UPDATE SET
                    symbol=excluded.symbol,decision_ms=excluded.decision_ms,price=excluded.price,
                    base_score=excluded.base_score,v2_score=excluded.v2_score,v2_label=excluded.v2_label,
                    qualified=excluded.qualified,min_score=excluded.min_score,
                    reasons=excluded.reasons,features=excluded.features''',
                (str(signal_id), str(symbol), int(decision_ms), float(price),
                 None if base_score is None else float(base_score), score, str(v2_label),
                 int(qualified), float(self.cfg.min_score), payload_reasons, payload_features)
            )
            db.commit()
        return qualified

    def selector_report(self, recent_limit=10):
        with closing(self.connect()) as db:
            rows = db.execute(
                '''SELECT signal_id,symbol,decision_ms,price,base_score,v2_score,v2_label,
                          qualified,min_score,reasons,features
                   FROM early_v2_selector_shadow ORDER BY decision_ms'''
            ).fetchall()
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            stages = []
            if 'entry_stage_forward_shadow' in tables:
                stages = db.execute(
                    '''SELECT id,symbol,created_ts_ms,mfe_pct,mae_pct,completed_60m
                       FROM entry_stage_forward_shadow
                       WHERE stage='EARLY' ORDER BY created_ts_ms'''
                ).fetchall()

        by_symbol = {}
        for st in stages:
            by_symbol.setdefault(str(st[1]), []).append(st)
        used = set()
        matched = []
        for row in rows:
            signal_id,symbol,decision_ms,price,base_score,v2_score,label,qualified,min_score,reasons_json,features_json = row
            best = None
            best_delta = None
            for st in by_symbol.get(str(symbol), ()):
                sid = int(st[0])
                if sid in used:
                    continue
                delta = abs(int(st[2]) - int(decision_ms))
                if delta <= 5000 and (best_delta is None or delta < best_delta):
                    best = st
                    best_delta = delta
            if best is not None:
                used.add(int(best[0]))
            matched.append((row,best,best_delta))

        thresholds = (0.2,0.5,1.0,2.0,3.0,5.0)
        def cohort_stats(want_qualified):
            subset = [(r,st,d) for r,st,d in matched if bool(r[7]) == bool(want_qualified)]
            mature = [(r,st,d) for r,st,d in subset if st is not None and int(st[5] or 0)]
            out = dict(total=len(subset),matched=sum(st is not None for _,st,_ in subset),mature60=len(mature))
            if mature:
                mfes=[float(st[3] or 0.0) for _,st,_ in mature]
                maes=[float(st[4] or 0.0) for _,st,_ in mature]
                out.update(
                    avg_mfe=sum(mfes)/len(mfes),
                    avg_mae=sum(maes)/len(maes),
                    reached={str(t):sum(x+1e-12>=t for x in mfes) for t in thresholds},
                    rates={str(t):sum(x+1e-12>=t for x in mfes)/len(mfes) for t in thresholds},
                )
            else:
                out.update(avg_mfe=None,avg_mae=None,
                           reached={str(t):0 for t in thresholds},
                           rates={str(t):None for t in thresholds})
            return out

        recent=[]
        for r,st,delta in matched[-max(0,int(recent_limit)):][::-1]:
            try:
                reasons=json.loads(r[9] or "[]")
            except Exception:
                reasons=[]
            recent.append(dict(
                signal_id=str(r[0]),symbol=str(r[1]),decision_ms=int(r[2]),
                v2_score=None if r[5] is None else float(r[5]),v2_label=str(r[6]),
                qualified=bool(r[7]),min_score=float(r[8]),
                matched_stage_id=None if st is None else int(st[0]),
                stage_time_delta_ms=None if delta is None else int(delta),
                mfe_pct=None if st is None else float(st[3] or 0.0),
                mae_pct=None if st is None else float(st[4] or 0.0),
                completed_60m=False if st is None else bool(st[5]),
                reasons=reasons[:6],
            ))
        threshold_stats = {}
        for threshold in (85.0, 90.0, 95.0, 98.0):
            eligible = [(r,st,d) for r,st,d in matched
                        if r[5] is not None and str(r[6]) == 'FAST_EARLY_V2'
                        and float(r[5]) + 1e-12 >= threshold]
            mature = [(r,st,d) for r,st,d in eligible if st is not None and int(st[5] or 0)]
            mfes = [float(st[3] or 0.0) for _,st,_ in mature]
            threshold_stats[str(int(threshold))] = dict(
                total=len(eligible),
                mature60=len(mature),
                avg_mfe=(sum(mfes)/len(mfes) if mfes else None),
                rates={
                    str(t):(sum(x+1e-12>=t for x in mfes)/len(mfes) if mfes else None)
                    for t in thresholds
                },
            )

        return dict(
            total=len(rows),
            qualified=sum(bool(r[7]) for r in rows),
            rejected=sum(not bool(r[7]) for r in rows),
            qualified_stats=cohort_stats(True),
            rejected_stats=cohort_stats(False),
            threshold_stats=threshold_stats,
            recent=recent,
        )

    def save(self, tr):
        with closing(self.connect()) as db:
            db.execute('''INSERT INTO early_v2_trades VALUES (?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET status=excluded.status,payload=excluded.payload''',
                (tr['id'], tr['signal_id'], tr['symbol'], tr['mode'], tr['status'], json.dumps(tr, allow_nan=False)))
            db.commit()
        if tr['status'] in ('CLOSED','NO_FILL','BLOCKED'):
            self.active.pop(tr['id'], None)
            self.engines.pop(tr['id'], None)
        else:
            self.active[tr['id']] = tr

    def kill(self, reason='USER_KILL'):
        # Synchronous: invalidates an in-flight coroutine's generation before its next POST.
        already = self.halted and self.last_error == reason
        self.generation += 1
        self.mode = 'OFF'
        self.profit_mode = 'SHADOW'
        self.profit_live_pending = False
        self.pending.clear()
        self.halted = True
        self.last_error = reason
        if not already:
            self.setting('halted', True)
            self.event('KILL', {'reason': reason})

    def unresolved(self):
        return any(t['status'] != 'OPEN' or t.get('pending_stop') or t.get('pending_client')
                   or t.get('pending_tp') or t.get('reconcile_error') or t.get('emergency_client') for t in self.active.values())

    def set_mode(self, mode):
        if mode not in ('OFF','DRY'):
            raise Blocked('LIVE_REQUIRES_SECOND_CONFIRMATION')
        if mode == 'DRY' and self.unresolved():
            raise Blocked('RECONCILIATION_REQUIRED')
        self.mode = mode
        self.profit_live_pending = False
        if mode == 'OFF' or self.profit_mode == 'LIVE':
            self.profit_mode = 'SHADOW'
        self.generation += 1
        self.pending.clear()
        if mode == 'DRY':
            self.halted = False
            self.setting('halted', False)
        self.event('MODE', {'mode': mode})

    def challenge(self, user, kind='LIVE'):
        if self.halted:
            raise Blocked('KILL_LATCH_SET_USE_DRY_AFTER_RECONCILIATION')
        if kind not in ('LIVE','PROFIT_LIVE'):
            raise Blocked('INVALID_CHALLENGE')
        token = secrets.token_hex(4)
        self.pending[kind] = (str(user), token, self.clock() + 120000, asdict(self.cfg))
        return token

    def confirm(self, user, token, *, ready, kind='LIVE'):
        p = self.pending.pop(kind, None)
        if not p or p[0] != str(user) or p[1] != token or p[2] < self.clock() or p[3] != asdict(self.cfg):
            raise Blocked('INVALID_OR_EXPIRED_CONFIRMATION')
        if not ready or self.halted or not self.live_allowed or not self.score_validated:
            raise Blocked('LIVE_CAPABILITY_OR_V2_VALIDATION_MISSING')
        if not self.cfg.execution or not self.cfg.score_gate:
            raise Blocked('EXECUTION_AND_V2_GATES_REQUIRED')
        if self.unresolved():
            raise Blocked('RECONCILIATION_REQUIRED')
        if kind == 'PROFIT_LIVE':
            if not self.profit_live_allowed or self.mode != 'LIVE':
                raise Blocked('PROFIT_LIVE_CAPABILITY_MISSING')
            self.profit_live_pending = True
        else:
            if self.mode != 'DRY':
                raise Blocked('START_FROM_DRY')
            self.mode = 'LIVE'
            self.generation += 1
        self.event(kind + '_CONFIRMED', {'user': str(user)})

    def wants_profit_live(self):
        return (self.mode == 'LIVE' and not self.halted and self.profit_live_allowed
                and (self.profit_mode == 'LIVE' or self.profit_live_pending))

    async def sync_profit(self, tr, exchange):
        if tr.get('emergency_client'):
            return
        try:
            if self.wants_profit_live():
                desired = quantize(tr.get('shadow_stop', tr['stop']), tr['filters']['tick'])
                if desired > tr['stop']:
                    await exchange.protect(tr, desired, self.save)
                    tr['stop'] = desired
                    self.save(tr)
                if self.wants_profit_live():
                    await exchange.cancel_fallback_tp(tr, self.save)
                # OFF/kill can arrive while the cancellation is in flight.
                if self.wants_profit_live():
                    tr['profit_policy'] = 'DYNAMIC'
                    self.save(tr)
                    return
            await exchange.ensure_fallback_tp(tr, self.save)
            tr['profit_policy'] = 'FALLBACK_TP'
            self.save(tr)
        except Exception as exc:
            self.kill('PROFIT_PROTECTION:' + type(exc).__name__ + ':' + str(exc))
            # Never deliberately leave an owned position with only initial SL.
            # The native SL remains while the one-shot reduce-only exit settles.
            await exchange.emergency(tr, self.save)
            raise

    def day(self, stamp):
        return datetime.fromtimestamp(stamp / 1000, timezone(timedelta(hours=3))).date().isoformat()

    def _attempt_stamp(self, tr):
        """Return the first real entry-attempt timestamp, never a mere reservation."""
        stamp = tr.get('order_attempted_ms')
        if type(stamp) in (int, float) and not isinstance(stamp, bool) and math.isfinite(stamp) and stamp > 0:
            return int(stamp)
        if tr.get('orders') or tr.get('pending_client') or tr.get('qty'):
            return int(tr.get('filled_ms') or tr.get('created_ms') or 0)
        if tr.get('mode') == 'DRY' and tr.get('status') in ('OPEN', 'CLOSED'):
            return int(tr.get('filled_ms') or tr.get('created_ms') or 0)
        return 0

    def report(self, mode=None):
        mode = mode or ('LIVE' if self.mode == 'LIVE' else 'DRY')
        with closing(self.connect()) as db:
            rows = [json.loads(r[0]) for r in db.execute('SELECT payload FROM early_v2_trades WHERE mode=?', (mode,))]
        today = self.day(self.clock())
        attempted = [(t, self._attempt_stamp(t)) for t in rows]
        attempted = [(t, stamp) for t, stamp in attempted if stamp and self.day(stamp) == today]
        closed = [t for t in rows if t['status'] == 'CLOSED' and self.day(t['closed_ms']) == today]
        return dict(mode=mode, date=today, attempts=len(attempted), closed=len(closed),
            net=sum(t['net'] for t in closed), loss=sum(max(0, -t['net']) for t in closed),
            wins=sum(t['net'] > 0 for t in closed),
            last_attempt=max((max(stamp, int(t.get('closed_ms') or 0)) for t, stamp in attempted), default=0),
            fill_count=sum(bool(t.get('qty')) for t, _ in attempted),
            partial_count=sum(bool(t.get('partial')) for t, _ in attempted),
            mean_slippage_pct=(sum(t.get('slippage_pct', 0) for t, _ in attempted if t.get('qty')) /
                               max(1, sum(bool(t.get('qty')) for t, _ in attempted))))

    def risk(self, symbol, proposed):
        if self.mode == 'OFF' or self.halted:
            raise Blocked('OFF_OR_KILL')
        if any(t['symbol'] == symbol for t in self.active.values()):
            raise Blocked('DUPLICATE_SYMBOL')
        if self.unresolved():
            raise Blocked('UNRECONCILED_TRADE')
        scoped = [t for t in self.active.values() if t['mode'] == self.mode]
        if len(scoped) >= self.cfg.max_positions:
            raise Blocked('MAX_POSITIONS')
        day = self.report(self.mode)
        if day['attempts'] >= self.cfg.daily_trades:
            raise Blocked('DAILY_TRADE_LIMIT')
        if day['last_attempt'] and self.clock() - day['last_attempt'] < self.cfg.cooldown_s * 1000:
            raise Blocked('COOLDOWN')
        if day['loss'] + sum(t['plan']['risk'] for t in scoped) + proposed > self.cfg.daily_loss:
            raise Blocked('DAILY_LOSS_RESERVE')

    def premium_confirm(self, symbol, signal_id, premium_mode):
        # A DRY Early must never suppress an actual Premium LIVE position.
        matching = [t for t in self.active.values() if t['symbol'] == symbol
                    and (t['mode'] == 'LIVE' or premium_mode == 'DRY')]
        for tr in matching:
            tr['classification'] = 'EARLY→PREMIUM_CONFIRMED'
            tr['premium_id'] = signal_id
            self.save(tr)
        return bool(matching)

    def still_entry_allowed(self, tr):
        if self.mode != tr['mode'] or self.halted or tr.get('generation') != self.generation:
            raise Blocked('ENTRY_MODE_CHANGED')
        if self.clock() - tr['signal']['ts_ms'] > self.cfg.signal_ttl_ms:
            raise Blocked('STALE_SIGNAL')
        if tr['mode'] == 'LIVE' and (not self.live_allowed or not self.score_validated):
            raise Blocked('LIVE_GATE')

    async def enter(self, signal, exchange, premium_busy=False):
        async with self.lock:
            tr = None
            phase = 'PRE_RESERVATION'
            try:
                if self.mode == 'OFF':
                    return
                self.cfg.validate()
                if (premium_busy() if callable(premium_busy) else premium_busy):
                    raise Blocked('PREMIUM_POSITION')
                score = signal.get('v2_score')
                if (not self.cfg.score_gate or not self.cfg.execution or signal.get('v2_label') != 'FAST_EARLY_V2'
                        or type(score) not in (int, float) or not math.isfinite(score)
                        or not self.cfg.min_score <= score <= 100):
                    raise Blocked('NOT_V2_QUALIFIED')
                if not 0 <= self.clock() - signal['ts_ms'] <= self.cfg.signal_ttl_ms:
                    raise Blocked('STALE_SIGNAL')
                phase = 'FILTERS'
                filters = await exchange.filters(signal['symbol'])
                plan = capped_plan(signal, asdict(self.cfg), filters)
                self.risk(signal['symbol'], plan['risk'])
                identity = hashlib.sha256((str(signal['id']) + ':' + self.mode).encode()).hexdigest()[:24]
                with closing(self.connect()) as db:
                    if db.execute('SELECT 1 FROM early_v2_trades WHERE id=?', (identity,)).fetchone():
                        raise Blocked('DUPLICATE_SIGNAL')
                tr = dict(id=identity, signal_id=str(signal['id']), symbol=signal['symbol'], mode=self.mode,
                    status='RESERVED', created_ms=self.clock(), signal=signal, plan=plan, filters=filters,
                    qty=0, orders=[], stops=[], classification='EARLY_V2', config=asdict(self.cfg), generation=self.generation,
                    order_attempted_ms=None, submit_count=0, blocked_reason=None)
                self.save(tr)  # reservation precedes every exchange mutation
                if tr['mode'] == 'LIVE':
                    phase = 'PREFLIGHT'
                    self.still_entry_allowed(tr)
                    await exchange.preflight(tr)
                for attempt in range(self.cfg.retry + 1):
                    phase = 'ASK'
                    self.still_entry_allowed(tr)
                    ask = await exchange.ask(tr['symbol'])
                    price = marketable_price(ask, plan, filters['tick'])
                    # Retry may use a lower/equal limit; it must never chase even inside cap.
                    if attempt and price > tr['limit']:
                        raise Blocked('RETRY_WOULD_CHASE')
                    self.still_entry_allowed(tr)
                    tr['limit'] = price
                    if tr['mode'] == 'DRY':
                        tr['order_attempted_ms'] = tr.get('order_attempted_ms') or self.clock()
                        tr['submit_count'] = max(1, int(tr.get('submit_count') or 0))
                        tr.update(qty=plan['qty'], vwap=price, reference='DRY_ASK_PROXY', status='OPEN',
                                  stop=plan['stop'], filled_ms=self.clock(), fills=[], partial=False)
                        self.save(tr)
                        self.opened(tr)
                        return
                    client = 'ev2-' + identity + '-' + str(attempt)
                    if not tr.get('order_attempted_ms'):
                        tr['order_attempted_ms'] = self.clock()
                        self.event('ORDER_ATTEMPT', {'signal_id': tr['signal_id'], 'symbol': tr['symbol'], 'client': client})
                    tr['submit_count'] = int(tr.get('submit_count') or 0) + 1
                    tr['pending_client'] = client
                    tr['status'] = 'SUBMITTING'
                    self.save(tr)
                    phase = 'SUBMIT'
                    order = await submit_ioc(exchange, tr['symbol'], plan['qty'], price, client)
                    phase = 'ORDER_ACK'
                    tr['orders'].append(order)
                    tr['pending_client'] = None
                    tr['status'] = 'RECONCILING'
                    self.save(tr)
                    if float(order.get('executedQty', 0)):
                        await self.accept_fill(tr, order, exchange)
                        return  # retain any positive partial, protect it; never top up
                    tr['status'] = 'RESERVED'
                    self.save(tr)
                tr['status'] = 'NO_FILL'
                self.save(tr)
            except Blocked as exc:
                if tr and tr['status'] == 'RESERVED':
                    if tr.get('order_attempted_ms'):
                        tr['status'] = 'NO_FILL'
                    else:
                        tr['status'] = 'BLOCKED'
                        tr['blocked_reason'] = str(exc)
                    self.save(tr)
                self.event('ENTRY_BLOCKED', {'signal_id': str(signal.get('id')), 'reason': str(exc),
                                             'attempted': bool(tr and tr.get('order_attempted_ms'))})
            except Exception as exc:
                # Only failures before a durable reservation are known to be
                # exchange-mutation-free. Once a reservation exists, preflight
                # may already have changed leverage/margin mode and a submit can
                # be ambiguous. Preserve fail-closed behavior there.
                reason = 'ENTRY_' + phase + ':' + type(exc).__name__ + ':' + str(exc)
                if tr is None:
                    self.event('ENTRY_PRE_RESERVATION_FAILED', {
                        'signal_id': str(signal.get('id')), 'phase': phase,
                        'reason': type(exc).__name__ + ':' + str(exc)})
                    return
                tr['blocked_reason'] = reason
                self.save(tr)
                self.kill(reason)

    async def accept_fill(self, tr, order, exchange):
        qty = float(order['executedQty'])
        if not math.isfinite(qty) or not 0 < qty <= tr['plan']['qty']:
            raise Uncertain('INVALID_ORDER_QUANTITY')
        tr['qty'] = qty
        tr['stop'] = tr['plan']['stop']
        self.save(tr)
        # Place native protection before requesting fill history. Uncertain stop
        # acknowledgement is queried by its durable client ID on the next recovery.
        if not tr.get('emergency_client'):
            try:
                await exchange.protect(tr, tr['stop'], self.save)
            except Exception:
                await exchange.emergency(tr, self.save)
                raise
        try:
            fills = await exchange.fills(tr['symbol'], order['orderId'])
            allocation = reconcile_fills(order, fills, tr['symbol'])
            if not tr.get('emergency_client') and not tr['plan']['stop'] < allocation['vwap'] <= tr['limit'] + 1e-10:
                raise Uncertain('FILL_OUTSIDE_ACCEPTED_PRICE_GEOMETRY')
        except Exception:
            await exchange.emergency(tr, self.save)
            raise
        tr.update(qty=allocation['qty'], vwap=allocation['vwap'], fills=allocation['fills'],
                  filled_ms=self.clock(), reference='BINANCE_FILL_VWAP', status='OPEN',
                  partial=allocation['qty'] < tr['plan']['qty'])
        self.opened(tr)
        await self.sync_profit(tr, exchange)

    def opened(self, tr):
        tr['slippage_pct'] = 100 * (tr['vwap'] / tr['signal']['price'] - 1)
        self.save(tr)
        if not tr.get('emergency_client'):
            self.engine(tr)

    def engine(self, tr):
        if tr['id'] in self.engines:
            return self.engines[tr['id']]
        cohort = dict(kind='EARLY', early_id=tr['signal_id'], symbol=tr['symbol'],
            decision_ms=tr['created_ms'], signal_price=tr['vwap'], initial_stop_price=tr['plan']['stop'])
        if tr['mode'] == 'LIVE':
            cohort.update(fills_complete=True, fills_available_ms=tr['filled_ms'], executed_qty=tr['qty'],
                fills=[dict(source='BINANCE', side='BUY', early_id=tr['signal_id'], symbol=tr['symbol'],
                    account_ref='BOT_CONFIGURED_ACCOUNT', order_id=f['orderId'], trade_id=f['id'],
                    price=float(f['price']), qty=float(f['qty']), event_ms=int(f['time'])) for f in tr['fills']])
        else:
            cohort['decision_ms'] = tr['filled_ms']
        engine = ShadowPosition(cohort, 365 * 86400000,
            Policy(first_lock_pct=tr['config']['first_lock_pct']), allow_signal_proxy=tr['mode']=='DRY')
        engine.stop = max(engine.stop, tr.get('shadow_stop', 0), tr.get('stop', 0))
        self.engines[tr['id']] = engine
        return engine

    def tick(self, symbol, price, event_ms, received_ms, trade_id):
        for tr in list(self.active.values()):
            if tr['symbol'] != symbol or tr['status'] != 'OPEN':
                continue
            engine = self.engine(tr)
            before = engine.stop
            engine.tick(dict(symbol=symbol, price=price, event_ms=event_ms, received_ms=received_ms, trade_id=trade_id))
            if engine.stop > before:
                tr['shadow_stop'] = engine.stop
                self.save(tr)
            if tr['mode'] == 'DRY':
                exit_event = engine.exits['dynamic']
                if self.profit_mode == 'OFF':
                    # Keep the reference SHADOW engine unchanged; simulate configured TP here.
                    target = quantize(tr['vwap'] * (1 + tr['config'].get('fallback_tp_pct', 1.) / 100),
                                      tr['filters']['tick'], up=True)
                    valid = (math.isfinite(price) and price > 0 and
                             0 <= received_ms - event_ms <= 1500 and event_ms >= tr['filled_ms'])
                    exit_event = None
                    if valid and price <= tr['plan']['stop']:
                        exit_event = dict(observed_price=price, reason='INITIAL_SL')
                    elif valid and price >= target:
                        exit_event = dict(observed_price=target, reason='FALLBACK_TP')
                if exit_event:
                    exit_price = exit_event['observed_price']
                    net = tr['qty'] * (exit_price - tr['vwap']) - tr['qty'] * (exit_price + tr['vwap']) * .0005
                    tr.update(status='CLOSED', closed_ms=received_ms, net=net, exit_price=exit_price,
                              cost_reference='DRY_FEE_0.05_PERCENT_PER_SIDE', close_reason=exit_event['reason'])
                    self.save(tr)

    async def reconcile(self, exchange):
        async with self.lock:
            for tr in list(self.active.values()):
                try:
                    if tr['mode'] == 'DRY':
                        if tr['status'] != 'OPEN':
                            tr['status'] = 'NO_FILL'
                            self.save(tr)
                        continue
                    if tr.get('pending_client'):
                        order = await exchange.request('GET', '/fapi/v1/order', dict(
                            symbol=tr['symbol'], origClientOrderId=tr['pending_client']))
                        validate_ioc(order, tr['symbol'], tr['pending_client'])
                        tr['orders'].append(order)
                        tr['pending_client'] = None
                        tr['status'] = 'RECONCILING'
                        self.save(tr)
                    if tr['status'] != 'OPEN':
                        filled = [o for o in tr['orders'] if float(o.get('executedQty', 0)) > 0]
                        if filled:
                            await self.accept_fill(tr, filled[-1], exchange)
                        else:
                            tr['status'] = 'NO_FILL'
                            self.save(tr)
                            continue
                    await exchange.reconcile_position(tr, self.save)
                    if tr.pop('reconcile_error', None):
                        self.save(tr)
                    if tr['status'] == 'CLOSED':
                        continue
                    self.engine(tr)
                    await self.sync_profit(tr, exchange)
                except Exception as exc:
                    tr['reconcile_error'] = type(exc).__name__ + ':' + str(exc)
                    self.save(tr)
                    self.kill('RECONCILIATION:' + type(exc).__name__ + ':' + str(exc))

            if self.profit_live_pending and self.wants_profit_live() and not self.unresolved():
                self.profit_live_pending = False
                self.profit_mode = 'LIVE'
                self.event('PROFIT_LIVE_READY', {})
