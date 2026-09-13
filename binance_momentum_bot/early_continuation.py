"""Forward-only market discovery telemetry. No trading, network or notification code.

All scanner calls enqueue immutable observations. One worker owns SQLite and state.
Missing history and interrupted observations are explicit, never backfilled.
"""
from collections import defaultdict, deque
from decimal import Decimal
import hashlib
import json
import logging
import math
from pathlib import Path
import queue
import sqlite3
import threading
import time
import uuid

VERSION = 'early-continuation-v1'
HORIZONS = (300, 900, 1800, 3600)
TOUCHES = (1, 3, 5, 10, -1, -2, -3)
GAP_MS = 15_000
CLOSE_TOLERANCE_MS = 5_000
SAMPLE_MS = 60_000
CONFIG = dict(C1=[.40, .30, 3.0], T1=[.20, .10, 3.0],
              T2=[-1.50, -.40], T3=[1, 30], C2=[2, 1_800_000],
              horizons=HORIZONS, gap_ms=GAP_MS, close_tolerance_ms=CLOSE_TOLERANCE_MS,
              sample_ms=SAMPLE_MS)
OPERANDS = ('chg10', 'chg30', 'chg60', 'chg5', 'rel30', 'flow10', 'flow30',
            'flow60', 'buy30', 'book_imbalance', 'spread', 'qv24', 'trades10',
            'trades30', 'oi5', 'oi_prev5', 'oi_accel5', 'funding_rate_pct',
            'btc30', 'breakout', 'extended', 'ret3', 'ret5', 'ret10', 'max_dd10',
            'higher_lows', 'positive5', 'gainers_rank', 'rank_velocity', 'trend_score',
            'score', 'candidate_age_s', 'candidate_passes')

# Worker-only delays, in addition to SQLite's existing 100ms busy timeout.
LOCK_RETRY_DELAYS = (.05, .10, .20, .40, .80)


def transient_lock(error):
    if not isinstance(error, sqlite3.OperationalError):
        return False
    code = getattr(error, 'sqlite_errorcode', None)
    if code is not None:
        return code & 0xff in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
    # Compatibility with exceptions without native error codes; never retry arbitrary
    # OperationalError messages (missing schema, disk errors, corruption, etc.).
    message = str(error).lower()
    return message in ('database is locked', 'database is busy', 'database table is locked')


class WorkerConnection:
    """Retry the failed SQL operation in place, not the stateful Engine method.

    BUSY/LOCKED leave these single statements/COMMIT uncompleted. Retaining the
    transaction avoids duplicate C2 increments, event links or partial outcomes.
    A snapshot conflict that cannot recover in place exhausts the same finite budget.
    """
    def __init__(self, connection):
        self.connection = connection
        self.interruption = None
        self.reporting = False

    def call(self, operation, *args):
        for attempt in range(len(LOCK_RETRY_DELAYS)+1):
            try:
                return getattr(self.connection, operation)(*args)
            except sqlite3.OperationalError as error:
                if not transient_lock(error) or attempt == len(LOCK_RETRY_DELAYS):
                    raise
                if not self.reporting:
                    now = int(time.time()*1000)
                    if self.interruption is None:
                        self.interruption = dict(start=now, retries=0, operations=set())
                    self.interruption['retries'] += 1
                    self.interruption['operations'].add(operation)
                time.sleep(LOCK_RETRY_DELAYS[attempt])

    def execute(self, *args):
        return self.call('execute', *args)

    def executescript(self, script):
        # Only migrate() uses this, and every statement is idempotent CREATE IF NOT EXISTS.
        return self.call('executescript', script)

    def commit(self):
        return self.call('commit')

    def close(self):
        self.connection.close()

    def report_interruptions(self, engine):
        if self.interruption is None:
            return
        interruption = self.interruption
        self.reporting = True
        try:
            engine.gap(None, 'SQLITE_LOCK_RETRY', interruption['start'], int(time.time()*1000),
                       detail={'retries': interruption['retries'],
                               'operations': sorted(interruption['operations'])})
            self.commit()
            self.interruption = None
        finally:
            # Diagnostic writes have the same finite retry budget, without recursively
            # creating more diagnostics for their own contention.
            self.reporting = False


def dumps(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def finite(value):
    return isinstance(value, (int, float)) and math.isfinite(value)


def flags(m, prior_count=None):
    def known(*keys):
        return all(finite(m.get(k)) for k in keys)
    flow = m.get('flow30')
    # Decimal input strings preserve the specified .02/.10 == .20 boundary.
    eff = float(Decimal(str(m['chg30'])) / max(Decimal(str(flow)), Decimal('.10'))) if known('chg30', 'flow30') else None
    return dict(
        C1=(m['chg30'] >= .40 and m['rel30'] >= .30 and flow < 3.0)
            if known('chg30', 'rel30', 'flow30') else None,
        T1=(eff >= .20 and flow < 3.0) if eff is not None else None,
        T2=(-1.50 <= m['max_dd10'] <= -.40) if known('max_dd10') else None,
        T3=(1 <= m['gainers_rank'] <= 30) if known('gainers_rank') else None,
        C2=(prior_count >= 2) if prior_count is not None else None,
        flow_eff30=eff)


def migrate(c):
    c.executescript('''
        CREATE TABLE IF NOT EXISTS early_continuation_flags (
            event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, symbol TEXT NOT NULL,
            event_type TEXT NOT NULL, source_table TEXT, source_id INTEGER,
            trend_id TEXT, episode_id TEXT, source_episode_id INTEGER,
            parent_event_id TEXT, linkage_reason TEXT NOT NULL,
            nominal_ts INTEGER, ready_ts INTEGER, decision_ts INTEGER NOT NULL,
            source_event_ts INTEGER, receive_ts INTEGER, observed_ts INTEGER NOT NULL, fill_ts INTEGER,
            price REAL, C1 INTEGER, T1 INTEGER, T2 INTEGER, T3 INTEGER, C2 INTEGER,
            prior_candidate_count INTEGER, candidate_history_complete INTEGER NOT NULL,
            operands_json TEXT NOT NULL, app_version TEXT, code_hash TEXT NOT NULL,
            config_hash TEXT NOT NULL, research_version TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ec_flags_symbol_time ON early_continuation_flags(symbol,decision_ts);
        CREATE INDEX IF NOT EXISTS ec_flags_source ON early_continuation_flags(source_table,source_id);
        CREATE TABLE IF NOT EXISTS early_continuation_outcomes (
            event_id TEXT NOT NULL, horizon_s INTEGER NOT NULL, due_ts INTEGER NOT NULL,
            observed_ts INTEGER, source_event_ts INTEGER, close_price REAL, return_pct REAL,
            mfe_pct REAL, mae_pct REAL, touches_json TEXT NOT NULL, sample_count INTEGER NOT NULL,
            observation_gap INTEGER NOT NULL, status TEXT NOT NULL,
            PRIMARY KEY(event_id,horizon_s));
        CREATE INDEX IF NOT EXISTS ec_outcomes_due ON early_continuation_outcomes(due_ts);
        CREATE TABLE IF NOT EXISTS early_continuation_pending (
            event_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, deadline_ts INTEGER NOT NULL, state_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ec_pending_symbol ON early_continuation_pending(symbol,deadline_ts);
        CREATE TABLE IF NOT EXISTS research_gate_trace (
            id INTEGER PRIMARY KEY, event_id TEXT NOT NULL, symbol TEXT NOT NULL, stage TEXT NOT NULL,
            gate_name TEXT NOT NULL, observed_json TEXT, threshold_json TEXT, passed INTEGER,
            evaluation_ts INTEGER NOT NULL, terminal_blocker TEXT, evaluation_kind TEXT NOT NULL,
            code_hash TEXT NOT NULL, config_hash TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ec_gate_event ON research_gate_trace(event_id);
        CREATE INDEX IF NOT EXISTS ec_gate_symbol_time ON research_gate_trace(symbol,evaluation_ts);
        CREATE TABLE IF NOT EXISTS research_runtime_gaps (
            id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, symbol TEXT, kind TEXT NOT NULL,
            start_ts INTEGER, end_ts INTEGER NOT NULL, duration_ms INTEGER,
            restored_state INTEGER, detail_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS ec_gaps_symbol_time ON research_runtime_gaps(symbol,end_ts);
        CREATE TABLE IF NOT EXISTS early_continuation_runs (
            run_id TEXT PRIMARY KEY, started_ts INTEGER NOT NULL, heartbeat_ts INTEGER NOT NULL,
            code_hash TEXT NOT NULL, config_hash TEXT NOT NULL);
    ''')


class Engine:
    """Worker-thread-only state; ticks never consult a legacy trade lifecycle."""
    def __init__(self, c, config, code_hash, app_version, now):
        self.c, self.config, self.code_hash, self.app_version = c, config, code_hash, app_version
        self.config_hash = hashlib.sha256(dumps(dict(research=CONFIG, production=config)).encode()).hexdigest()
        self.run_id = uuid.uuid4().hex
        self.started = now
        self.history_since = now
        self.history = defaultdict(deque)
        self.history_count = defaultdict(int)
        self.links = {}
        self.last_tick = {}
        self.last_stale_report = {}
        self.active = {}
        self.by_symbol = defaultdict(set)
        migrate(c)
        previous = c.execute('SELECT MAX(heartbeat_ts) FROM early_continuation_runs').fetchone()[0]
        c.execute('INSERT INTO early_continuation_runs VALUES (?,?,?,?,?)',
                  (self.run_id, now, now, code_hash, self.config_hash))
        self.gap(None, 'PROCESS_START', previous, now, restored=0)
        # Resume only already-armed V1 observations; never read/replay historical source events.
        for eid, symbol, deadline, state in c.execute('SELECT * FROM early_continuation_pending'):
            p = json.loads(state)
            p['gap'] = True
            self.active[eid] = p
            self.by_symbol[symbol].add(eid)
            self.gap(symbol, 'RESTORED_OBSERVATION', p.get('last_receive'), now, restored=1)
        c.commit()

    def gap(self, symbol, kind, start, end, restored=None, detail=None):
        self.c.execute('INSERT INTO research_runtime_gaps(run_id,symbol,kind,start_ts,end_ts,duration_ms,restored_state,detail_json) VALUES (?,?,?,?,?,?,?,?)',
                       (self.run_id, symbol, kind, start, end,
                        max(0, end-start) if start is not None else None, restored, dumps(detail or {})))
        if kind != 'PROCESS_START':
            if symbol is None:
                self.links.clear()
                for p in self.active.values(): p['gap'] = True
            else:
                self.links.pop(symbol, None)
                for eid in self.by_symbol.get(symbol, ()):
                    self.active[eid]['gap'] = True
        if kind in ('QUEUE_LOSS', 'WORKER_ERROR', 'CLOCK_REGRESSION'):
            self.history.clear()
            self.history_count.clear()
            self.history_since = end

    def event(self, e):
        symbol, ts, kind = e['symbol'], e['decision_ts'], e['event_type']
        eid = e['event_id']
        if self.c.execute('SELECT 1 FROM early_continuation_flags WHERE event_id=?', (eid,)).fetchone():
            return
        hist = self.history[symbol]
        if hist and ts < hist[-1][0]:
            self.gap(None, 'CLOCK_REGRESSION', hist[-1][0], ts)
            hist = self.history[symbol]
        while hist and hist[0][0] < ts-1_800_000:
            self.history_count[symbol] -= hist.popleft()[1]
        prior = self.history_count[symbol] - (hist[-1][1] if hist and hist[-1][0] == ts else 0)
        complete = ts-self.history_since >= 1_800_000
        count = prior if complete or prior >= 2 else None
        m = dict(e['operands'])
        f = flags(m, count)
        m['flow_eff30'] = f.pop('flow_eff30')
        m['prior_candidate_count_observed'] = prior
        if kind == 'candidate_start':
            if hist and hist[-1][0] == ts:
                hist[-1][1] += 1
            else:
                hist.append([ts, 1])
            self.history_count[symbol] += 1
        link = self.links.get(symbol)
        source_episode = e.get('source_episode_id') or None
        reason = 'SAME_PROCESS_CONTIGUOUS'
        last = self.last_tick.get(symbol)
        continuous = last is not None and 0 <= ts-last[1] <= GAP_MS
        if (link is None or not continuous or
                (source_episode is not None and link['source_episode'] not in (None, source_episode)) or
                kind == 'candidate_start'):
            # A new candidate is an explicit episode boundary. A prior trend may be linked
            # only if no earlier candidate episode was attached and stream continuity is known.
            parent = None
            trend = None
            if (link and link['trend'] is not None and kind == 'candidate_start' and link['source_episode'] is None
                    and continuous):
                parent, trend = link['event'], link['trend']
                reason = 'CONTIGUOUS_TREND_TO_CANDIDATE'
            else:
                reason = 'NEW_OR_UNPROVEN_BOUNDARY'
            link = dict(episode=uuid.uuid4().hex if source_episode else None,
                        source_episode=source_episode, trend=trend, event=parent, ts=ts)
        if kind in ('trend_evaluation', 'TREND_BUILDUP') and link['trend'] is None:
            link['trend'] = uuid.uuid4().hex
        if source_episode and link['episode'] is None:
            link['episode'] = uuid.uuid4().hex
            link['source_episode'] = source_episode
        row = dict(event_id=eid, run_id=self.run_id, symbol=symbol, event_type=kind,
                   source_table=e.get('source_table'), source_id=e.get('source_id'),
                   trend_id=link['trend'], episode_id=link['episode'], source_episode_id=source_episode,
                   parent_event_id=link['event'], linkage_reason=reason,
                   nominal_ts=e.get('nominal_ts'), ready_ts=e.get('ready_ts'), decision_ts=ts,
                   source_event_ts=e.get('source_event_ts'), receive_ts=e.get('receive_ts'),
                   observed_ts=e['observed_ts'], fill_ts=None, price=e.get('price'), **f,
                   prior_candidate_count=count, candidate_history_complete=int(complete),
                   operands_json=dumps(m), app_version=self.app_version, code_hash=self.code_hash,
                   config_hash=self.config_hash, research_version=VERSION)
        self.c.execute('INSERT INTO early_continuation_flags ('+','.join(row)+') VALUES ('+','.join('?' for _ in row)+')', tuple(row.values()))
        self.links[symbol] = dict(link, event=eid, ts=ts)
        if kind in ('ttl_reject', 'breakdown_reject', 'continuity_reject', 'episode_reset'):
            self.links.pop(symbol, None)
        for g in e.get('gates', ()):
            self.c.execute('INSERT INTO research_gate_trace(event_id,symbol,stage,gate_name,observed_json,threshold_json,passed,evaluation_ts,terminal_blocker,evaluation_kind,code_hash,config_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                           (eid, symbol, kind, g['name'], dumps(g.get('observed')), dumps(g.get('threshold')),
                            g.get('passed'), ts, e.get('terminal'), e.get('evaluation_kind', 'OPERAND_SNAPSHOT'), self.code_hash, self.config_hash))
        if kind in ('candidate_start', 'early_alert', 'TREND_BUILDUP'):
            price = e.get('price')
            if finite(price) and price > 0:
                self.active[eid] = dict(event_id=eid, symbol=symbol, start=ts, price=price,
                    mfe=0., mae=0., touches={}, samples=0, gap=False, done=[], last_receive=None,
                    last_source=None)
                self.by_symbol[symbol].add(eid)
                self.c.execute('INSERT INTO early_continuation_pending VALUES (?,?,?,?)',
                               (eid, symbol, ts+3_600_000, dumps(self.active[eid])))

    def tick(self, symbol, price, source, receive):
        previous = self.last_tick.get(symbol)
        if previous and receive-previous[1] > GAP_MS:
            self.gap(symbol, 'SOURCE_HEARTBEAT_GAP', previous[1], receive)
        if source is None or not finite(price) or price <= 0 or receive-source > GAP_MS or source > receive+1000:
            if receive-self.last_stale_report.get(symbol, -GAP_MS) >= GAP_MS:
                self.gap(symbol, 'STALE_OR_MISSING_SOURCE', source, receive)
                self.last_stale_report[symbol] = receive
            for eid in self.by_symbol.get(symbol, ()): self.active[eid]['gap'] = True
            self.links.pop(symbol, None)
            return
        if previous and source <= previous[0]: return
        self.last_tick[symbol] = (source, receive)
        for eid in tuple(self.by_symbol.get(symbol, ())):
            p = self.active[eid]
            if source < p['start']: continue
            if receive-(p['last_receive'] or p['start']) > GAP_MS:
                if not p['gap']:
                    self.gap(symbol, 'OBSERVATION_DATA_GAP', p['last_receive'] or p['start'], receive)
                p['gap'] = True
            # Resolve horizons BEFORE adding a tick from beyond that horizon to extrema.
            for h in HORIZONS:
                due = p['start']+h*1000
                if h not in p['done'] and source >= due:
                    if source == due:
                        self.accumulate(p, price, source)
                    close_ok = source-due <= CLOSE_TOLERANCE_MS and receive-source <= CLOSE_TOLERANCE_MS
                    self.outcome(p, h, price if close_ok else None, source if close_ok else None, receive)
            if source < p['start']+3_600_000:
                self.accumulate(p, price, source)
            p['last_receive'], p['last_source'] = receive, source

    @staticmethod
    def accumulate(p, price, source):
        if p.get('accumulated_source') == source: return
        p['accumulated_source'] = source
        ret = (price/p['price']-1)*100
        p['mfe'], p['mae'] = max(p['mfe'], ret), min(p['mae'], ret)
        p['samples'] += 1
        for level in TOUCHES:
            if (ret >= level if level > 0 else ret <= level):
                p['touches'].setdefault(str(level), source)

    def outcome(self, p, h, price, source, observed):
        self.c.execute('INSERT OR IGNORE INTO early_continuation_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (p['event_id'], h, p['start']+h*1000, observed, source, price,
             (price/p['price']-1)*100 if price is not None else None,
             p['mfe'] if p['samples'] else None, p['mae'] if p['samples'] else None,
             dumps(p['touches']), p['samples'], int(p['gap']),
             'GAPPED' if p['gap'] else ('OBSERVED' if price is not None else 'MISSING_CLOSE')))
        p['done'].append(h)

    def flush(self, now):
        for eid, p in list(self.active.items()):
            last = p['last_receive'] or p['start']
            if now-last > GAP_MS and not p.get('stale_reported'):
                self.gap(p['symbol'], 'NO_FRESH_PRICE', last, now)
                p['stale_reported'] = True
            for h in HORIZONS:
                if h not in p['done'] and now > p['start']+h*1000+CLOSE_TOLERANCE_MS:
                    self.outcome(p, h, None, None, now)
            if len(p['done']) == len(HORIZONS):
                self.c.execute('DELETE FROM early_continuation_pending WHERE event_id=?', (eid,))
                self.by_symbol[p['symbol']].discard(eid)
                del self.active[eid]
            else:
                self.c.execute('INSERT INTO early_continuation_pending VALUES (?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET state_json=excluded.state_json',
                               (eid, p['symbol'], p['start']+3_600_000, dumps(p)))
        self.c.execute('UPDATE early_continuation_runs SET heartbeat_ts=? WHERE run_id=?', (now, self.run_id))
        self.c.commit()


class Collector:
    """Bounded, nonblocking producer. No DB/network work in a scanner call."""
    def __init__(self, path, config, app_version, code_hash, capacity=8192):
        self.path, self.config, self.app_version, self.code_hash = path, config, app_version, code_hash
        self.queue = queue.Queue(capacity)
        self.watch = {}
        self.samples = {}
        self.sources = {}
        self.enabled = True
        self.lost = 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, name='early-continuation', daemon=True)

    def submit(self, kind, payload):
        if not self.enabled: return False
        try:
            self.queue.put_nowait((kind, payload))
            return True
        except queue.Full:
            self.lost += 1
            return False

    def event(self, symbol, kind, operands, **meta):
        now = meta.pop('decision_ts', None) or int(time.time()*1000)
        if kind in ('candidate_evaluation', 'trend_evaluation'):
            key = (symbol, kind)
            if now-self.samples.get(key, 0) < SAMPLE_MS: return
            self.samples[key] = now
        clean = {k: (v if not isinstance(v, float) or math.isfinite(v) else None)
                 for k in OPERANDS for v in [operands.get(k)]}
        e = dict(meta, event_id=uuid.uuid4().hex, symbol=symbol, event_type=kind,
                 operands=clean, decision_ts=now, observed_ts=int(time.time()*1000))
        if self.submit('event', e):
            self.watch[symbol] = max(self.watch.get(symbol, 0), now+(3_600_000 if kind in ('candidate_start', 'early_alert', 'TREND_BUILDUP') else SAMPLE_MS+GAP_MS))

    def tick(self, symbol, price, source, receive):
        self.sources[symbol] = (source, receive)
        if self.watch.get(symbol, 0) >= receive:
            self.submit('tick', (symbol, price, source, receive))

    def run(self):
        c = None
        try:
            # mode=rw: never create a missing database or volume. Worker lock waits are bounded.
            c = WorkerConnection(sqlite3.connect(Path(self.path).resolve().as_uri()+'?mode=rw', uri=True, timeout=.1))
            engine = Engine(c, self.config, self.code_hash, self.app_version, int(time.time()*1000))
            c.report_interruptions(engine)
            for p in engine.active.values(): self.watch[p['symbol']] = p['start']+3_600_000
            last_flush = time.monotonic()
            reported_loss = 0
            while not self.stop.is_set() or not self.queue.empty():
                received = False
                try:
                    kind, payload = self.queue.get(timeout=.1)
                    received = True
                    if kind == 'event': engine.event(payload)
                    elif kind == 'tick': engine.tick(*payload)
                except queue.Empty:
                    pass
                now = int(time.time()*1000)
                if received and not self.queue.empty():
                    # Finalizing against wall time while older ticks await processing can
                    # censor valid data. Make this visible rather than implying full coverage.
                    oldest_delay = now - (payload.get('observed_ts', now) if kind == 'event' else payload[3])
                    if oldest_delay > CLOSE_TOLERANCE_MS and not getattr(self, '_backlog_reported', False):
                        engine.gap(None, 'WORKER_BACKLOG', now-oldest_delay, now)
                        self._backlog_reported = True
                if self.lost != reported_loss:
                    engine.gap(None, 'QUEUE_LOSS', None, now, detail={'dropped': self.lost-reported_loss})
                    reported_loss = self.lost
                if time.monotonic()-last_flush >= 1:
                    engine.flush(now)
                    last_flush = time.monotonic()
                else:
                    # Never hold a SQLite write transaction across a queue wait.
                    c.commit()
                c.report_interruptions(engine)
            engine.flush(int(time.time()*1000))
            c.report_interruptions(engine)
        except Exception:
            # Scanner remains operational; incomplete persisted observations recover as gapped.
            logging.getLogger(__name__).exception('Early continuation worker disabled; telemetry incomplete')
        finally:
            self.enabled = False
            if c is not None: c.close()


def start(path, config, app_version):
    code = hashlib.sha256()
    for name in ('early_continuation.py', 'early_continuation_bridge.py', 'bot.py'):
        code.update((Path(__file__).parent/name).read_bytes())
    worker = Collector(path, config, app_version, code.hexdigest())
    worker.thread.start()
    return worker
