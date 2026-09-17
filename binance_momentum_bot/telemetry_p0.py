"""Additive research telemetry. No exchange, notifier or production gate dependencies.

All prices are proxies. Frozen experiment v1 is not a trading recommendation.
Time units: epoch milliseconds; return/threshold units: percentage points.
"""
import contextvars
import functools
import json
import logging
import math
import sqlite3
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass

log = logging.getLogger(__name__)
HORIZONS = (60, 300, 900, 1800, 3600)
FIELDS = ('score', 'gainer_rank', 'rank_velocity', 'rank_source_ts_ms', 'oi5', 'oi_accel5',
          'oi_source_ts_ms', 'oi_received_ts_ms', 'trade_event_ts_ms',
          'trade_received_ts_ms', 'book_event_ts_ms', 'book_received_ts_ms',
          'feature_ready_ts_ms', 'candidate_runup', 'chg5', 'chg60', 'chg30',
          'buy30', 'flow30', 'rel30', 'qv24', 'spread')


@dataclass(frozen=True)
class Experiment:
    version: str = 'early-ignition-shadow-v1'
    chg5_max: float = 1.5
    chg60_min: float = .5
    chg30_min: float = .2
    buy_min: float = .60
    buy_max: float = .80
    runup_max: float = .8
    relative_min: float = .2
    flow_min: float = 1.2
    flow_max: float = 10.
    volume_min: float = 5000000.
    spread_max: float = .15
    freshness_ms: int = 3000
    stop_pct: float = 1.
    target_pct: float = 2.
    fee_per_side_pct: float = .05
    slippage_per_side_pct: float = .02


EXPERIMENT = Experiment()


@contextmanager
def connection(connect):
    c = connect()
    try:
        with c:
            yield c
    finally:
        c.close()


def safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        log.exception('P0_TELEMETRY_ERROR operation=%s', getattr(fn, '__name__', 'unknown'))


def finite(value):
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (ValueError, TypeError):
        return None


def freeze(features, decision_ms):
    """Copy at observation time. Missing/nonfinite values never become zero."""
    values = {k: finite(features.get(k)) for k in FIELDS}
    reasons = {k: 'NONFINITE_OR_INVALID' if features.get(k) is not None else
               'NOT_OBSERVED_AT_DECISION' for k, v in values.items() if v is None}
    ages = {}
    for name in ('oi_source', 'oi_received', 'trade_event', 'trade_received',
                 'book_event', 'book_received', 'feature_ready', 'rank_source'):
        stamp = values[name + '_ts_ms']
        ages[name] = decision_ms - stamp if stamp is not None else None
        if stamp is not None and stamp > decision_ms:
            reasons[name + '_ts_ms'] = 'FUTURE_TIMESTAMP'
    return {'values': values, 'missingness': reasons, 'age_ms': ages,
            'stale': {k: None if v is None else v > (600000 if k.startswith('oi_') else 3000)
                      for k, v in ages.items()}}


def ignition(snapshot, cfg=EXPERIMENT):
    v, ages = snapshot['values'], snapshot['age_ms']
    needed = ('chg5', 'chg60', 'chg30', 'buy30', 'candidate_runup', 'rel30', 'flow30', 'qv24', 'spread')
    missing = [k for k in needed if v[k] is None]
    if missing:
        return False, ['MISSING:' + k for k in missing]
    checks = {
        'SLOW_IGNITION': v['chg5'] < cfg.chg5_max and v['chg60'] > cfg.chg60_min and v['chg30'] > cfg.chg30_min,
        'LIMITED_RUNUP': 0 <= v['candidate_runup'] <= cfg.runup_max,
        'RELATIVE_STRENGTH': v['rel30'] > cfg.relative_min,
        'BALANCED_FLOW': cfg.buy_min <= v['buy30'] <= cfg.buy_max and cfg.flow_min <= v['flow30'] <= cfg.flow_max,
        'LIQUIDITY': v['qv24'] >= cfg.volume_min and 0 <= v['spread'] <= cfg.spread_max,
        'FRESHNESS': all(ages[k] is not None and 0 <= ages[k] <= cfg.freshness_ms
                         for k in ('trade_event', 'trade_received', 'book_event', 'book_received')),
    }
    return all(checks.values()), [k for k, passed in checks.items() if not passed]


def migrate(c):
    c.executescript('''
    CREATE TABLE IF NOT EXISTS p0_autotrade_decisions (
      id INTEGER PRIMARY KEY, chain_id TEXT NOT NULL, signal_id INTEGER NOT NULL,
      symbol TEXT NOT NULL, decision_ts_ms INTEGER NOT NULL, mode TEXT NOT NULL,
      event TEXT NOT NULL, detail_json TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS p0_decision_signal ON p0_autotrade_decisions(signal_id,decision_ts_ms);
    CREATE TABLE IF NOT EXISTS p0_feature_snapshots (
      source_key TEXT PRIMARY KEY, symbol TEXT NOT NULL, stage TEXT NOT NULL,
      episode_id INTEGER, signal_id INTEGER, decision_ts_ms INTEGER NOT NULL,
      snapshot_json TEXT NOT NULL, experiment_json TEXT NOT NULL,
      ignition_selected INTEGER NOT NULL, ignition_reasons_json TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS p0_forward (
      source_key TEXT PRIMARY KEY, symbol TEXT NOT NULL, layer TEXT NOT NULL,
      episode_id INTEGER, parent_id TEXT, signal_id INTEGER,
      allow_decision_ts INTEGER NOT NULL, allow_reference_price REAL,
      fill_price REAL, fill_event_ts_ms INTEGER, fill_observed_ts_ms INTEGER, fill_source TEXT,
      status TEXT NOT NULL, state_json TEXT NOT NULL, experiment_json TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS p0_forward_status ON p0_forward(status,symbol);
    CREATE TABLE IF NOT EXISTS p0_forward_outcomes (
      source_key TEXT NOT NULL, horizon_s INTEGER NOT NULL, due_ts_ms INTEGER NOT NULL,
      observed_ts_ms INTEGER, event_ts_ms INTEGER, return_pct REAL, mfe_pct REAL, mae_pct REAL,
      reference_return_pct REAL, net_return_pct REAL, first_passage TEXT,
      first_passage_ts_ms INTEGER, peak_ts_ms INTEGER, gap INTEGER NOT NULL,
      missing_reason TEXT, PRIMARY KEY(source_key,horizon_s));
    CREATE TABLE IF NOT EXISTS p0_gap_events (
      source_key TEXT NOT NULL, kind TEXT NOT NULL, first_observed_ts_ms INTEGER NOT NULL,
      last_observed_ts_ms INTEGER NOT NULL, count INTEGER NOT NULL, detail_json TEXT NOT NULL,
      PRIMARY KEY(source_key,kind));
    ''')


_chain = contextvars.ContextVar('p0_autotrade_chain', default=None)


def decision(connect, signal_id, symbol, mode, event, detail=None):
    chain = _chain.get()
    stamp = int(time.time() * 1000)
    with connection(connect) as c:
        c.execute('INSERT INTO p0_autotrade_decisions VALUES (NULL,?,?,?,?,?,?,?)',
                  (chain or uuid.uuid4().hex, signal_id, symbol, stamp, mode, event, json.dumps(detail or {}, default=str)))
    log.info('P0_AUTOTRADE signal_id=%s event=%s mode=%s', signal_id, event, mode)


def audit_premium(fn):
    @functools.wraps(fn)
    async def wrapped(session, signal_id, symbol, m, plan):
        g = fn.__globals__
        mode = str(g['autotrade_cfg'].get('mode', 'OFF')).upper()
        token = _chain.set(uuid.uuid4().hex)
        record = lambda event, detail=None: safe(decision, g['db_connect'], signal_id, symbol, mode, event, detail)
        try:
            record('DECISION_START', {'execution': m.get('execution'), 'signal_generated_ts_ms': m.get('signal_generated_ts_ms')})
            if mode == 'OFF':
                record('SKIP_MODE_OFF', {'remaining_gates': 'NOT_EVALUATED'})
            elif mode not in ('DRY', 'LIVE'):
                record('UNKNOWN_MODE_OBSERVED')
            return await fn(session, signal_id, symbol, m, plan)
        except BaseException as exc:
            record('DECISION_ERROR', {'exception_type': type(exc).__name__})
            raise
        finally:
            record('DECISION_END')
            _chain.reset(token)
    return wrapped


class Telemetry:
    def __init__(self, connect):
        self.connect = connect
        self.active = {}
        self.by_symbol = defaultdict(set)
        self.counters = defaultdict(int)
        self.last_summary = int(time.time()*1000)
        with connection(connect) as c:
            c.row_factory = sqlite3.Row
            for row in c.execute("SELECT * FROM p0_forward WHERE status='OPEN'"):
                x = json.loads(row['state_json'])
                x['gap'] = 1
                self.active[x['key']] = x
                self.by_symbol[x['symbol']].add(x['key'])
                self._gap(c, x, 'RESTART_GAP', int(time.time()*1000))
                self._save(c, x)
        log.info('P0_TELEMETRY_READY version=%s recovered=%d shadow_only=1', EXPERIMENT.version, len(self.active))

    def _gap(self, c, x, kind, observed, detail=None):
        x['gap'] = 1
        self.counters['gap_events'] += 1
        c.execute('''INSERT INTO p0_gap_events VALUES (?,?,?,?,1,?)
          ON CONFLICT(source_key,kind) DO UPDATE SET last_observed_ts_ms=excluded.last_observed_ts_ms,count=count+1''',
                  (x['key'], kind, observed, observed, json.dumps(detail or {})))

    def _save(self, c, x, status='OPEN'):
        c.execute('''UPDATE p0_forward SET fill_price=?,fill_event_ts_ms=?,fill_observed_ts_ms=?,fill_source=?,status=?,state_json=? WHERE source_key=?''',
                  (x['fill'], x.get('fill_event'), x.get('fill_observed'), x.get('fill_source'), status, json.dumps(x), x['key']))

    def arm(self, key, symbol, layer, decision_ms, reference, episode_id=None, parent_id=None, signal_id=None):
        if finite(reference) is None or reference <= 0:
            return
        x = dict(key=key, symbol=symbol, decision=decision_ms, reference=reference, fill=None,
                 last_event=None, mfe=None, mae=None, peak_ts=None, first=None, first_ts=None,
                 horizons=[], gap=0, saved=decision_ms, config=asdict(EXPERIMENT))
        with connection(self.connect) as c:
            cur = c.execute('''INSERT OR IGNORE INTO p0_forward
              (source_key,symbol,layer,episode_id,parent_id,signal_id,allow_decision_ts,allow_reference_price,status,state_json,experiment_json)
              VALUES (?,?,?,?,?,?,?,?,'OPEN',?,?)''',
                            (key,symbol,layer,episode_id,parent_id,signal_id,decision_ms,reference,json.dumps(x),json.dumps(x['config'])))
            if not cur.rowcount:
                return
        self.active[key] = x
        self.by_symbol[symbol].add(key)
        self.counters['armed'] += 1
        if layer != 'CANDIDATE':
            log.info('P0_FORWARD_ARM layer=%s source=%s decision=%s', layer,key,decision_ms)

    def stage(self, key, symbol, stage, decision_ms, features, episode_id=None, signal_id=None):
        snapshot = freeze(features, decision_ms)
        selected, reasons = ignition(snapshot)
        with connection(self.connect) as c:
            cur = c.execute('INSERT OR IGNORE INTO p0_feature_snapshots VALUES (?,?,?,?,?,?,?,?,?,?)',
                (key,symbol,stage,episode_id,signal_id,decision_ms,json.dumps(snapshot),json.dumps(asdict(EXPERIMENT)),int(selected),json.dumps(reasons)))
            if not cur.rowcount:
                return
        self.counters['feature_snapshots'] += 1
        if stage in ('CANDIDATE','EARLY','PREMIUM'):
            self.arm(key,symbol,'CLASSIC' if stage=='PREMIUM' else stage,decision_ms,features.get('price'),episode_id,signal_id=signal_id)
        if selected and stage in ('CANDIDATE','EARLY'):
            # Same episode is a single experimental alert, regardless of repeated stage.
            dedup = f'ignition:{symbol}:{episode_id}' if episode_id else 'ignition:'+key
            self.arm(dedup,symbol,'IGNITION',decision_ms,features.get('price'),episode_id)

    def tick(self, symbol, price, event_ms, observed_ms, ask=None, book_event_ms=None, book_received_ms=None):
        keys = list(self.by_symbol.get(symbol, ()))
        if not keys or finite(price) is None or price <= 0:
            return
        dirty = []
        for key in keys:
            x = self.active[key]
            flags = []
            if event_ms <= x['decision']:
                continue
            if event_ms > observed_ms or observed_ms-event_ms > 3000 or (x['last_event'] is not None and event_ms < x['last_event']):
                flags.append('LATE_EVENT')
                with connection(self.connect) as c:
                    self._gap(c,x,'LATE_EVENT',observed_ms)
                continue
            if event_ms == x['last_event']:
                # Millisecond timestamps are not unique trade identifiers.
                continue
            # Finalize overdue horizons before adding a later tick to their extrema.
            due = [h for h in HORIZONS if h not in x['horizons'] and event_ms >= x['decision']+h*1000]
            previous = dict(x)
            if x['last_event'] is not None and event_ms-x['last_event'] > 10000:
                flags.append('EVENT_GAP')
            if x['fill'] is None and event_ms < x['decision']+HORIZONS[0]*1000:
                fresh = finite(ask) is not None and ask > 0 and all(
                    t is not None and x['decision'] < t <= observed_ms and observed_ms-t <= x['config']['freshness_ms']
                    for t in (book_event_ms,book_received_ms))
                if not fresh:
                    flags.append('STALE_ASK' if ask is not None else 'MISSING_ASK')
                if not due:  # Never manufacture a fill after the first horizon is lost.
                    x.update(fill=ask if fresh else price,fill_event=event_ms,fill_observed=observed_ms,
                             fill_source='FRESH_ASK_PROXY' if fresh else 'NEXT_TRADE_PROXY',mfe=0.,mae=0.,peak_ts=event_ms)
                    self.counters['fills'] += 1
            x['last_event'] = event_ms
            if x['fill'] is not None:
                ret = (price/x['fill']-1)*100
                if ret > x['mfe']:
                    x['peak_ts'] = event_ms
                x['mfe'],x['mae'] = max(x['mfe'],ret),min(x['mae'],ret)
                if x['first'] is None:
                    kind = 'STOP' if ret <= -x['config']['stop_pct'] else 'TARGET' if ret >= x['config']['target_pct'] else None
                    if kind:
                        x['first'],x['first_ts'] = kind,event_ms
            if due or flags or previous['fill'] != x['fill'] or previous['first'] != x['first'] or observed_ms-x['saved'] >= 15000:
                dirty.append((x,previous,due,flags))
        if not dirty:
            return
        with connection(self.connect) as c:
            for x,previous,due,flags in dirty:
                for kind in flags:
                    self._gap(c,x,kind,observed_ms)
                for h in due:
                    target=x['decision']+h*1000
                    late=event_ms-target > 5000
                    if late:
                        self._gap(c,x,'MISSING_HORIZON',observed_ms,{'horizon_s':h})
                    path=previous if event_ms>target else x
                    missing='LATE_HORIZON' if late else 'NO_FILL' if x['fill'] is None else None
                    ret=None if missing else (price/x['fill']-1)*100
                    # Extrema stop at deadline; terminal price has explicit <=5s sampling tolerance.
                    net=None if ret is None else ret-(2+ret/100)*(x['config']['fee_per_side_pct']+x['config']['slippage_per_side_pct'])
                    c.execute('INSERT OR IGNORE INTO p0_forward_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (x['key'],h,target,observed_ms,event_ms,ret,None if missing else path['mfe'],None if missing else path['mae'],
                         None if missing else (price/x['reference']-1)*100,net,path['first'],path['first_ts'],path['peak_ts'],x['gap'],missing))
                    x['horizons'].append(h)
                    self.counters['outcomes'] += 1
                x['saved']=observed_ms
                done=len(x['horizons'])==len(HORIZONS)
                self._save(c,x,'COMPLETE' if done else 'OPEN')
                if done:
                    self.active.pop(x['key'],None)
                    self.by_symbol[x['symbol']].discard(x['key'])

    def expire(self, now):
        if now-self.last_summary >= 60000:
            log.info('P0_TELEMETRY_COUNTS shadow_only=1 active=%d counts=%s',len(self.active),json.dumps(dict(self.counters),sort_keys=True))
            self.last_summary = now
        overdue=[x for x in self.active.values() if any(h not in x['horizons'] and now>x['decision']+h*1000+5000 for h in HORIZONS)]
        if not overdue:
            return
        with connection(self.connect) as c:
            for x in overdue:
                for h in HORIZONS:
                    due=x['decision']+h*1000
                    if h not in x['horizons'] and now>due+5000:
                        self._gap(c,x,'MISSING_HORIZON',now,{'horizon_s':h})
                        c.execute('INSERT OR IGNORE INTO p0_forward_outcomes(source_key,horizon_s,due_ts_ms,observed_ts_ms,gap,missing_reason) VALUES (?,?,?,?,1,?)',
                                  (x['key'],h,due,now,'NO_OBSERVATION'))
                        x['horizons'].append(h)
                        self.counters['missing_horizons'] += 1
                done=len(x['horizons'])==len(HORIZONS)
                self._save(c,x,'COMPLETE' if done else 'OPEN')
                if done:
                    self.active.pop(x['key'],None)
                    self.by_symbol[x['symbol']].discard(x['key'])
