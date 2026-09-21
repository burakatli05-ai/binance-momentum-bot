"""Isolated, bounded, batched research recorder. No trading or notification API.

The event loop owns the buffer. Only flush() (called via asyncio.to_thread) uses
SQLite. Model absence is an explicit abstention, never a made-up probability.
"""
import json
import logging
import math
import sqlite3
import time
from collections import defaultdict

log = logging.getLogger(__name__)
VERSION = 'quality-shadow-20260921-v1'
SCHEMA = 'quality-features-v1'
HORIZONS = (0, 15000, 30000, 60000, 90000, 180000)
TOUCHES = (-3., -1., -.5, .5, 1., 6., 8., 10., 15.)
MODELS = {
    'PREMIUM_V2_SHADOW_SCORE': ('PREMIUM', 0, 'GOOD_VS_BAD_GRAY_EXCLUDED'),
    'RUNNER_OBSERVED_12H10_PROB_T0': ('EARLY', 0, 'OBSERVED_LOWER_BOUND'),
    'PREMIUM_STOP3_RISK_T60': ('PREMIUM', 60000, '60m MAE<=-3 research label'),
}

def clean(value):
    if isinstance(value, dict): return {str(k):clean(v) for k,v in value.items()}
    if isinstance(value, (list,tuple)): return [clean(v) for v in value]
    if isinstance(value,float) and not math.isfinite(value): return None
    return value

def encode(value): return json.dumps(clean(value),sort_keys=True,separators=(',',':'),allow_nan=False)

def migrate(c):
    # Owned tables only; older production schemas are never altered.
    c.executescript('''
    CREATE TABLE IF NOT EXISTS quality_shadow_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS quality_shadow_cohorts(
      key TEXT PRIMARY KEY,symbol TEXT NOT NULL,kind TEXT NOT NULL,episode_id INTEGER,
      signal_id INTEGER,radar_id INTEGER,anchor_price REAL,decision_ms INTEGER NOT NULL,
      wave_key TEXT NOT NULL,recovery_gap INTEGER NOT NULL,payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS quality_shadow_snapshots(
      key TEXT NOT NULL,horizon_ms INTEGER NOT NULL,nominal_ms INTEGER NOT NULL,
      decision_ms INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(key,horizon_ms));
    CREATE TABLE IF NOT EXISTS quality_shadow_scores(
      key TEXT NOT NULL,model TEXT NOT NULL,model_version TEXT NOT NULL,
      decision_ms INTEGER NOT NULL,probability_raw REAL,payload TEXT NOT NULL,
      PRIMARY KEY(key,model,model_version));
    CREATE TABLE IF NOT EXISTS quality_shadow_prices(
      key TEXT NOT NULL,bucket_ms INTEGER NOT NULL,width_ms INTEGER NOT NULL,
      payload TEXT NOT NULL,PRIMARY KEY(key,bucket_ms,width_ms));
    CREATE TABLE IF NOT EXISTS quality_shadow_touches(
      key TEXT NOT NULL,threshold REAL NOT NULL,event_ms INTEGER NOT NULL,
      observed_ms INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(key,threshold));
    CREATE TABLE IF NOT EXISTS quality_shadow_gaps(
      key TEXT NOT NULL,start_ms INTEGER NOT NULL,end_ms INTEGER NOT NULL,
      reason TEXT NOT NULL,PRIMARY KEY(key,start_ms,end_ms,reason));
    ''')

class Recorder:
    def __init__(self, connect, now=None, limit=20000):
        self.connect=connect;self.limit=limit;self.pending=[];self.dropped=0
        self.active={};self.by_symbol=defaultdict(set);self.buckets={}
        now=now or int(time.time()*1000)
        c=connect()
        try:
            migrate(c)
            c.execute('INSERT OR IGNORE INTO quality_shadow_meta VALUES (?,?)',('activation_ms',str(now)))
            self.activation=int(c.execute("SELECT value FROM quality_shadow_meta WHERE key='activation_ms'").fetchone()[0])
            c.row_factory=sqlite3.Row
            for r in c.execute('SELECT * FROM quality_shadow_cohorts WHERE decision_ms>?',(now-43200000,)):
                item=json.loads(r['payload']);item['recovery_gap']=1
                item['snapshots']={x[0] for x in c.execute('SELECT horizon_ms FROM quality_shadow_snapshots WHERE key=?',(item['key'],))}
                item['touches']={x[0] for x in c.execute('SELECT threshold FROM quality_shadow_touches WHERE key=?',(item['key'],))}
                self._remember(item)
                self._gap(item,item['decision_ms'],now,'RESTART_UNOBSERVED_INTERVAL')
            c.commit()
        finally:c.close()

    def _remember(self,item):
        self.active[item['key']]=item;self.by_symbol[item['symbol']].add(item['key'])

    def _append(self,sql,args):
        if len(self.pending)>=self.limit:
            self.dropped+=1
            if self.dropped==1:log.error('QUALITY_SHADOW_BACKPRESSURE buffer full; observations dropped')
            return False
        self.pending.append((sql,args));return True

    def _gap(self,item,start,end,reason):
        self._append('INSERT OR IGNORE INTO quality_shadow_gaps VALUES (?,?,?,?)',(item['key'],start,end,reason))

    def arm(self,kind,identity,symbol,episode_id,price,decision_ms,features=None,recovered=False):
        if kind not in ('EARLY','PREMIUM') or not price or price<=0:return
        key=kind+':'+str(identity)
        if key in self.active:return
        item=dict(key=key,kind=kind,symbol=symbol,episode_id=episode_id,
                  signal_id=identity if kind=='PREMIUM' else None,radar_id=identity if kind=='EARLY' else None,
                  anchor_price=price,decision_ms=int(decision_ms),
                  wave_key=f'{symbol}:episode:{episode_id}' if episode_id else f'{symbol}:unknown:{identity}',
                  wave_definition='ONLINE_EPISODE_KEY; offline chained12h regroup required',
                  recovery_gap=int(recovered),snapshots=set(),touches=set(),last_event_ms=0)
        payload={k:v for k,v in item.items() if k not in ('snapshots','touches')}
        accepted=self._append('INSERT OR IGNORE INTO quality_shadow_cohorts VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                     tuple(item[k] for k in ('key','symbol','kind','episode_id','signal_id','radar_id','anchor_price','decision_ms','wave_key','recovery_gap'))+(encode(payload),))
        if not accepted:return
        self._remember(item)
        if not recovered:self.snapshot(item,0,decision_ms,features or {})

    def snapshot(self,item,horizon,now,features):
        if horizon in item['snapshots']:return
        nominal=item['decision_ms']+horizon
        flags=[]
        if now-nominal>2000:flags.append('LATE_SNAPSHOT')
        if item['recovery_gap']:flags.append('RESTART_GAP')
        sources=features.get('_sources',{})
        source_flags={}
        for name,source in sources.items():
            ts=source.get('source_ms');available=source.get('available_ms')
            if not ts or not available:source_flags[name]='MISSING_SOURCE_TIME'
            elif ts>available or available>now:source_flags[name]='FUTURE_OR_INVALID_TIME'
            elif now-ts>source.get('max_age_ms',0):source_flags[name]='STALE'
            else:source_flags[name]='OK'
        missing=[f for f in ('chg30','flow30','book_imbalance','compression_ratio','oi_accel5','bid_ratio','listing_age_days') if features.get(f) is None]
        payload=dict(model_version=VERSION,feature_schema_version=SCHEMA,
                     symbol=item['symbol'],episode_id=item['episode_id'],wave_key=item['wave_key'],
                     signal_id=item['signal_id'],radar_id=item['radar_id'],anchor_price=item['anchor_price'],
                     nominal_ms=nominal,decision_time=now,raw_features=features,
                     feature_sources=sources,feature_source_flags=source_flags,missing=missing,gap_flags=flags,
                     coverage={'present_features':len(sources),'scheduled_horizon_ms':horizon})
        if not self._append('INSERT OR IGNORE INTO quality_shadow_snapshots VALUES (?,?,?,?,?)',
                            (item['key'],horizon,nominal,now,encode(payload))):return
        item['snapshots'].add(horizon)
        for model,(kind,offset,label) in MODELS.items():
            if item['kind']!=kind or horizon!=offset:continue
            score=dict(payload,label_quality=label,status='ABSTAIN_NO_PROVENANCE_VALIDATED_FIT',
                       reason=['Historical feature source times not independently verified'],
                       contributions={},probability_raw=None,promotion_allowed=False)
            self._append('INSERT OR IGNORE INTO quality_shadow_scores VALUES (?,?,?,?,?,?)',
                         (item['key'],model,VERSION,now,None,encode(score)))

    def due_symbols(self,now):
        return {x['symbol'] for x in self.active.values() if any(h not in x['snapshots'] and now>=x['decision_ms']+h for h in HORIZONS)}

    def sample(self,now,features):
        for item in list(self.active.values()):
            for h in HORIZONS:
                if h not in item['snapshots'] and now>=item['decision_ms']+h:
                    self.snapshot(item,h,now,features.get(item['symbol'],{}))
            if now>=item['decision_ms']+43200000:
                if not item['last_event_ms'] or now-item['last_event_ms']>2000:
                    self._gap(item,item['last_event_ms'] or item['decision_ms'],now,'NO_RECENT_PRICE')
                self.by_symbol[item['symbol']].discard(item['key']);del self.active[item['key']]
        for key,bucket in list(self.buckets.items()):
            if now>=bucket['bucket_ms']+bucket['width_ms']:
                self._append('INSERT OR IGNORE INTO quality_shadow_prices VALUES (?,?,?,?)',
                             (key[0],bucket['bucket_ms'],bucket['width_ms'],encode(bucket)))
                del self.buckets[key]

    def tick(self,symbol,price,event_ms,observed_ms,bid=None,ask=None,book_event_ms=None,book_received_ms=None):
        # No SQL, file access or network in this hot path.
        if not math.isfinite(price) or price<=0:return
        for key in tuple(self.by_symbol.get(symbol,())):
            item=self.active[key];age=event_ms-item['decision_ms']
            if age<0 or age>43200000 or event_ms>observed_ms or event_ms<=item['last_event_ms']:continue
            previous=item['last_event_ms'] or item['decision_ms']
            if event_ms-previous>2000:self._gap(item,previous,event_ms,'TRADE_OBSERVATION_GAP')
            item['last_event_ms']=event_ms
            ret=100*(price/item['anchor_price']-1)
            for threshold in TOUCHES:
                if threshold not in item['touches'] and (ret>=threshold if threshold>0 else ret<=threshold):
                    if self._append('INSERT OR IGNORE INTO quality_shadow_touches VALUES (?,?,?,?,?)',
                                    (key,threshold,event_ms,observed_ms,encode({'price':price,'quality':'FIRST_OBSERVED_TOUCH','recovery_gap':item['recovery_gap']}))):
                        item['touches'].add(threshold)
            width=1000 if age<900000 else 60000
            bucket_ms=item['decision_ms']+(age//width)*width;bk=(key,bucket_ms,width)
            if bk not in self.buckets:
                if len(self.buckets)>=self.limit:self.dropped+=1;continue
                self.buckets[bk]=dict(bucket_ms=bucket_ms,width_ms=width,open=price,high=price,low=price,close=price,n=0,
                                     first_event_ms=event_ms,last_event_ms=event_ms,max_gap_ms=event_ms-previous)
            b=self.buckets[bk];b.update(high=max(b['high'],price),low=min(b['low'],price),close=price,n=b['n']+1,last_event_ms=event_ms,
                                      max_gap_ms=max(b['max_gap_ms'],event_ms-previous),bid=bid,ask=ask,
                                      book_event_ms=book_event_ms,book_received_ms=book_received_ms,received_ms=observed_ms)

    def take_batch(self):
        batch=self.pending;self.pending=[]
        if self.dropped:
            now=int(time.time()*1000)
            batch.append(('INSERT OR IGNORE INTO quality_shadow_gaps VALUES (?,?,?,?)',
                          ('GLOBAL',now,now,'BUFFER_OVERFLOW_DROPPED_'+str(self.dropped))))
            self.dropped=0
        return batch

    def flush(self,batch):
        if not batch:return
        c=self.connect()
        try:
            c.execute('PRAGMA busy_timeout=1000')
            with c:
                for sql,args in batch:c.execute(sql,args)
        finally:c.close()
        log.info('QUALITY_SHADOW_BATCH rows=%d models=ABSTAIN shadow_only=1',len(batch))

    def discover(self,now):
        """Off-loop recovery of a signal lost between production save and batch flush."""
        c=self.connect();rows=[]
        try:
            tables={x[0] for x in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for table,kind in [('signals_v2','PREMIUM'),('radar_signals','EARLY')]:
                if table not in tables:continue
                where=' AND notified=1' if kind=='EARLY' else ''
                stamp='COALESCE(notify_ts,ts)' if kind=='EARLY' else 'ts'
                for r in c.execute(f'SELECT id,symbol,episode_id,price,{stamp} FROM {table} WHERE {stamp}>=?'+where,(max(self.activation,now-43200000)//1000,)):
                    rows.append((kind,*r))
        finally:c.close()
        return rows
