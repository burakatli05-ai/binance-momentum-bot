"""Independent Premium counterfactuals. No exchange/order or production-state API."""
from collections import deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import math
import queue
import sqlite3
import time
import uuid

import research_v5135 as cost_model

IST = timezone(timedelta(hours=3))
MODELS = {"ALT_WIDE_60": (3.5, 6.0, 40.0, 10), "ALT_CONTROL_60": (3.0, 6.0, 45.0, 10)}
HORIZONS = (1800, 2700, 3600, 5400, 7200)


def day(ts):
    return datetime.fromtimestamp(ts/1000, IST).date().isoformat()


def migrate(c):
    c.executescript("""
    CREATE TABLE IF NOT EXISTS alt_runtime(key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS alt_daily(
        model_name TEXT NOT NULL,local_date TEXT NOT NULL,starting_balance REAL NOT NULL,
        realized_net_pnl REAL NOT NULL DEFAULT 0,consecutive_stops INTEGER NOT NULL DEFAULT 0,
        cooldown_until INTEGER NOT NULL DEFAULT 0,unpriced_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(model_name,local_date));
    CREATE TABLE IF NOT EXISTS alt_research_trades(
        research_trade_id TEXT PRIMARY KEY,model_name TEXT NOT NULL,signal_id INTEGER NOT NULL,symbol TEXT NOT NULL,
        episode_id INTEGER,candidate_id INTEGER,candidate_to_premium_age REAL,ready_ts INTEGER NOT NULL,
        decision_ts INTEGER NOT NULL,fill_ts INTEGER,fill_event_ts INTEGER,entry_reference_price REAL,
        fill_price REAL,fill_source TEXT,margin REAL NOT NULL,leverage INTEGER NOT NULL,notional REAL NOT NULL,
        qty REAL,slippage_pct REAL NOT NULL,cost_snapshot TEXT NOT NULL,stop_pct REAL NOT NULL,tp_pct REAL NOT NULL,
        stop_price REAL,tp_price REAL,timeout_at INTEGER,first_touch_order TEXT,tp_hit_ts INTEGER,stop_hit_ts INTEGER,
        timeout_ts INTEGER,terminal_reason TEXT,terminal_ts INTEGER,exit_price REAL,mfe REAL DEFAULT 0,mae REAL DEFAULT 0,
        max_favorable_excursion REAL DEFAULT 0,max_adverse_excursion REAL DEFAULT 0,
        gross_return REAL,fees REAL,slippage_cost REAL,net_return REAL,gross_pnl REAL,net_pnl REAL,
        lifecycle_status TEXT NOT NULL,skip_reason TEXT,projected_risk REAL,concurrent_positions INTEGER,
        remaining_daily_budget REAL,observation_gap INTEGER NOT NULL DEFAULT 0,last_event_ts INTEGER,
        last_price REAL,last_price_ts INTEGER,UNIQUE(model_name,signal_id));
    CREATE INDEX IF NOT EXISTS alt_trades_status ON alt_research_trades(lifecycle_status,symbol);
    CREATE TABLE IF NOT EXISTS alt_lifecycle_events(
        id INTEGER PRIMARY KEY,research_trade_id TEXT NOT NULL,event TEXT NOT NULL,event_ts INTEGER NOT NULL,
        observed_ts INTEGER NOT NULL,price REAL,detail_json TEXT NOT NULL,
        UNIQUE(research_trade_id,event,event_ts));
    CREATE TABLE IF NOT EXISTS alt_forward_outcomes(
        research_trade_id TEXT NOT NULL,horizon_s INTEGER NOT NULL,target_ts INTEGER NOT NULL,
        observed_ts INTEGER NOT NULL,price REAL,return_pct REAL,status TEXT NOT NULL,
        PRIMARY KEY(research_trade_id,horizon_s));
    CREATE TABLE IF NOT EXISTS alt_premium_context(
        signal_id INTEGER PRIMARY KEY,symbol TEXT NOT NULL,episode_id INTEGER,candidate_id INTEGER,
        candidate_to_premium_age REAL,premium_ts INTEGER NOT NULL,ready_ts INTEGER NOT NULL,
        premium_price REAL NOT NULL,context_json TEXT NOT NULL,session_key TEXT NOT NULL,
        premium_sequence_number INTEGER NOT NULL,session_premium_count INTEGER NOT NULL,
        time_since_previous_premium REAL,previous_signal_id INTEGER,previous_outcome_at_decision TEXT,
        peak_ts INTEGER,peak_price REAL,peak_return REAL,post_peak_drawdown REAL DEFAULT 0,
        post_returns_json TEXT NOT NULL DEFAULT '{}',giveback_json TEXT NOT NULL DEFAULT '{}',
        tags_json TEXT NOT NULL DEFAULT '[]',observation_gap INTEGER NOT NULL DEFAULT 0,
        telemetry_until INTEGER NOT NULL,last_event_ts INTEGER);
    CREATE VIEW IF NOT EXISTS alt_linked_context AS
        SELECT a.signal_id,a.symbol,p.status AS progress_status,e.composite_state,
               v.oi_regime,l.horizon_ms,l.reference_kind,l.reference_price AS initial_anchor,
               l.current_mid_reference_price,l.current_mid_metrics_json
        FROM alt_premium_context a LEFT JOIN premium_progress_validation p ON p.signal_id=a.signal_id
        LEFT JOIN premium_execution_composite e ON e.signal_id=a.signal_id
        LEFT JOIN premium_context v ON v.signal_id=a.signal_id
        LEFT JOIN premium_liquidity_snapshots l ON l.signal_id=a.signal_id;
    """)


def past_context(candles, candidate_ts):
    """Closed candles strictly before candidate; independent of candidate resets."""
    rows = sorted((x for x in candles if x[0]+60000<=candidate_ts), key=lambda x:x[0])
    result = {"cutoff_ts":candidate_ts,"source":"CLOSED_1M_CANDLES","trend_age_proxy_s":None}
    for minutes in (3,5,10):
        window = [x for x in rows if candidate_ts-minutes*60000<=x[0]+60000<=candidate_ts]
        # Require complete consecutive minute history; partial history is UNKNOWN.
        valid = len(window)>=minutes and all(b[0]-a[0]==60000 for a,b in zip(window[-minutes:],window[-minutes+1:])) if minutes>1 else bool(window)
        window=window[-minutes:]
        if valid and window[0][1]>0:
            result[f"pre_candidate_{minutes}m_return"]=(window[-1][4]/window[0][1]-1)*100
            result[f"pre_candidate_{minutes}m_runup"]=(max(x[2] for x in window)/window[0][1]-1)*100
        else:
            result[f"pre_candidate_{minutes}m_return"]=None
            result[f"pre_candidate_{minutes}m_runup"]=None
    streak=0
    for a,b in reversed(list(zip(rows,rows[1:]))):
        if b[0]-a[0]!=60000 or b[4]<=a[4]:break
        streak+=1
    if rows:result["trend_age_proxy_s"]=streak*60
    result["proxy_definition"]="consecutive higher closed-minute closes before candidate, not true trend age"
    return result


class ShadowEngine:
    def __init__(self,connect,starting_balance=2000):
        if not math.isfinite(starting_balance) or starting_balance<=0:raise ValueError("shadow balance must be positive")
        self.connect,self.starting_balance=connect,starting_balance
        self.inbox=queue.Queue(maxsize=50000)
        self.dropped=False
        self.trades={};self.contexts={};self.symbols=set()
        with closing(connect()) as c,c:
            c.row_factory=sqlite3.Row
            c.execute("INSERT OR IGNORE INTO alt_runtime VALUES ('initial_balance',?)",(str(starting_balance),))
            self.starting_balance=float(c.execute("SELECT value FROM alt_runtime WHERE key='initial_balance'").fetchone()[0])
            # First activation starts after existing history; restart uses durable cursor.
            latest=c.execute("SELECT COALESCE(MAX(id),0) FROM signals_v2").fetchone()[0]
            c.execute("INSERT OR IGNORE INTO alt_runtime VALUES ('premium_cursor',?)",(str(latest),))
            for row in c.execute("SELECT * FROM alt_research_trades WHERE fill_ts IS NOT NULL OR lifecycle_status='ARMED'"):
                r=dict(row)
                complete={v[0] for v in c.execute("SELECT horizon_s FROM alt_forward_outcomes WHERE research_trade_id=?",(r['research_trade_id'],))}
                if r['lifecycle_status']=='OPEN' or r['lifecycle_status']=='ARMED' or len(complete)<len(HORIZONS):
                    r['_horizons']=complete;r['observation_gap']=1;self.trades[r['research_trade_id']]=r
                    c.execute("UPDATE alt_research_trades SET observation_gap=1 WHERE research_trade_id=?",(r['research_trade_id'],))
            for row in c.execute("SELECT * FROM alt_premium_context WHERE telemetry_until>?",(int(time.time()*1000),)):
                r=dict(row);r['observation_gap']=1;self.contexts[r['signal_id']]=r
                c.execute("UPDATE alt_premium_context SET observation_gap=1 WHERE signal_id=?",(r['signal_id'],))
        self._symbols()

    def _symbols(self):
        self.symbols={x['symbol'] for x in self.trades.values()}|{x['symbol'] for x in self.contexts.values()}

    def feed(self,symbol,price,event_ts,observed_ts,ask=None):
        if symbol in self.symbols:
            try:self.inbox.put_nowait((symbol,price,event_ts,observed_ts,ask))
            except queue.Full:self.dropped=True

    def event(self,c,r,event,ts,observed,price=None,detail=None):
        c.execute("INSERT OR IGNORE INTO alt_lifecycle_events(research_trade_id,event,event_ts,observed_ts,price,detail_json) VALUES (?,?,?,?,?,?)",
                  (r['research_trade_id'],event,ts,observed,price,json.dumps(detail or {})))

    def persist(self,c,table,key,r):
        fields={k:v for k,v in r.items() if not k.startswith('_') and k!=key}
        c.execute(f"UPDATE {table} SET "+','.join(k+'=?' for k in fields)+f" WHERE {key}=?",(*fields.values(),r[key]))

    def ledger(self,c,model,now):
        date=day(now)
        # Reconstruct only this model, including cross-midnight cooldown. Never production tables.
        rows=c.execute("SELECT terminal_reason,net_pnl,terminal_ts FROM alt_research_trades WHERE model_name=? AND lifecycle_status='CLOSED' ORDER BY terminal_ts,research_trade_id",(model,)).fetchall()
        prior=sum(r[1] or 0 for r in rows if day(r[2])<date)
        c.execute("INSERT OR IGNORE INTO alt_daily(model_name,local_date,starting_balance) VALUES (?,?,?)",(model,date,max(0,self.starting_balance+prior)))
        balance=c.execute("SELECT starting_balance FROM alt_daily WHERE model_name=? AND local_date=?",(model,date)).fetchone()[0]
        realized=sum(r[1] or 0 for r in rows if day(r[2])==date)
        streak=0;cooldown=0
        for reason,net,ts in rows:
            if reason=='SL':
                streak+=1
                if streak>=4:cooldown=ts+3600000
            else:streak=0
        unknown=c.execute("SELECT COUNT(*) FROM alt_research_trades WHERE model_name=? AND lifecycle_status='UNPRICED_TIMEOUT'",(model,)).fetchone()[0]
        c.execute("UPDATE alt_daily SET realized_net_pnl=?,consecutive_stops=?,cooldown_until=?,unpriced_count=? WHERE model_name=? AND local_date=?",(realized,streak,cooldown,unknown,model,date))
        return dict(balance=balance,realized=realized,cooldown=cooldown,unknown=unknown)

    @staticmethod
    def stop_risk(r):
        snapshot=json.loads(r['cost_snapshot'])
        fee,slip=cost_model.costs(r['notional'],r['notional']*(1-r['stop_pct']/100),snapshot['fee_pct_per_side'],snapshot['slippage_pct_per_side'])
        return r['notional']*r['stop_pct']/100+fee+slip

    def admit(self,c,r,now):
        ledger=self.ledger(c,r['model_name'],now)
        opened=c.execute("SELECT notional,stop_pct,cost_snapshot FROM alt_research_trades WHERE model_name=? AND lifecycle_status IN ('ARMED','OPEN') AND research_trade_id!=?",(r['model_name'],r['research_trade_id'])).fetchall()
        risk=sum(self.stop_risk(dict(v)) for v in opened)
        proposed=max(0,-ledger['realized'])+risk+self.stop_risk(r)
        remaining=ledger['balance']*.03-max(0,-ledger['realized'])-risk
        reason='UNPRICED_RISK' if ledger['unknown'] else 'CAPACITY' if len(opened)>=3 else 'COOLDOWN' if now<ledger['cooldown'] else 'DAILY_RISK' if proposed>ledger['balance']*.03+1e-9 else None
        return reason,proposed,len(opened),remaining

    def on_premium(self,source,context=None,now=None,c=None):
        now=now or int(time.time()*1000)
        if c is None:
            with closing(self.connect()) as con,con:
                con.row_factory=sqlite3.Row
                return self.on_premium(source,context,now,con)
        sid=source['id'];symbol=source['symbol'];age=source.get('candidate_to_premium_age')
        if c.execute("SELECT 1 FROM alt_premium_context WHERE signal_id=?",(sid,)).fetchone():return
        premium_ts=source.get('premium_ts') or int(source['ts']*1000)
        session=day(premium_ts)
        prev=c.execute("SELECT signal_id,premium_ts FROM alt_premium_context WHERE symbol=? AND session_key=? ORDER BY premium_ts DESC LIMIT 1",(symbol,session)).fetchone()
        count=c.execute("SELECT COUNT(*) FROM alt_premium_context WHERE symbol=? AND session_key=?",(symbol,session)).fetchone()[0]+1
        seq=c.execute("SELECT COUNT(*) FROM alt_premium_context WHERE symbol=? AND episode_id IS ? AND session_key=?",(symbol,source.get('episode_id'),session)).fetchone()[0]+1
        outcome=None
        if prev:
            row=c.execute("SELECT first_event FROM signal_paths WHERE signal_id=?",(prev[0],)).fetchone()
            outcome=(row[0] if row else None) or 'OPEN_OR_UNKNOWN'
        ctx=dict(signal_id=sid,symbol=symbol,episode_id=source.get('episode_id'),candidate_id=source.get('candidate_id'),
                 candidate_to_premium_age=age,premium_ts=premium_ts,ready_ts=now,premium_price=source['price'],context_json=json.dumps(context or {}),
                 session_key=session,premium_sequence_number=seq,session_premium_count=count,time_since_previous_premium=(premium_ts-prev[1])/1000 if prev else None,
                 previous_signal_id=prev[0] if prev else None,previous_outcome_at_decision=outcome,peak_ts=premium_ts,peak_price=source['price'],peak_return=0.0,
                 post_peak_drawdown=0.0,post_returns_json='{}',giveback_json='{}',tags_json='[]',observation_gap=0,telemetry_until=premium_ts+3900000,last_event_ts=None)
        c.execute("INSERT INTO alt_premium_context("+','.join(ctx)+") VALUES ("+','.join('?' for _ in ctx)+")",tuple(ctx.values()))
        self.contexts[sid]=ctx
        for model,(stop,tp,margin,lev) in MODELS.items():
            snapshot=json.dumps(dict(version='5.13.5',fee_pct_per_side=cost_model.DRY_FEE_PCT,slippage_pct_per_side=cost_model.DRY_SLIPPAGE_PCT))
            r=dict(research_trade_id=uuid.uuid5(uuid.NAMESPACE_URL,f'momentum:{model}:{sid}').hex,model_name=model,signal_id=sid,symbol=symbol,
                   episode_id=source.get('episode_id'),candidate_id=source.get('candidate_id'),candidate_to_premium_age=age,ready_ts=now,decision_ts=now,
                   entry_reference_price=source['price'],margin=margin,leverage=lev,notional=margin*lev,slippage_pct=cost_model.DRY_SLIPPAGE_PCT,
                   cost_snapshot=snapshot,stop_pct=stop,tp_pct=tp,lifecycle_status='ARMED',mfe=0.0,mae=0.0,max_favorable_excursion=0.0,max_adverse_excursion=0.0,observation_gap=0)
            reason,risk,concurrent,remaining=self.admit(c,r,now)
            if age is None or not math.isfinite(age) or age<0:reason='AGE_UNKNOWN'
            elif age>45:reason='AGE_GT_45'
            elif now-premium_ts>30000:reason='STALE_PREMIUM_AFTER_RESTART'
            r.update(skip_reason=reason,projected_risk=risk,concurrent_positions=concurrent,remaining_daily_budget=remaining)
            if reason:r['lifecycle_status']='SKIPPED'
            c.execute("INSERT INTO alt_research_trades("+','.join(r)+") VALUES ("+','.join('?' for _ in r)+")",tuple(r.values()))
            self.event(c,r,'SKIP' if reason else 'ARMED',now,now,source['price'],dict(reason=reason,projected_risk=risk,concurrent_positions=concurrent,remaining_daily_budget=remaining))
            if not reason:self.trades[r['research_trade_id']]=r
        self._symbols()

    def ingest(self,c,candles,now):
        cursor=int(c.execute("SELECT value FROM alt_runtime WHERE key='premium_cursor'").fetchone()[0])
        for raw in c.execute("SELECT * FROM signals_v2 WHERE id>? ORDER BY id LIMIT 200",(cursor,)).fetchall():
            source=dict(raw);sid=source['id']
            if source['level']=='CONFIRMED':
                candidate=c.execute("SELECT id,candidate_age_s FROM candidate_events WHERE symbol=? AND episode_id IS ? AND event='premium_signal' AND ts BETWEEN ? AND ? ORDER BY id DESC LIMIT 1",
                                    (source['symbol'],source.get('episode_id'),source['ts']-2,source['ts']+2)).fetchone()
                source['candidate_to_premium_age']=candidate[1] if candidate else None
                started=c.execute("SELECT id,ts FROM candidate_events WHERE symbol=? AND episode_id IS ? AND event='candidate_start' AND ts<=? ORDER BY id DESC LIMIT 1",(source['symbol'],source.get('episode_id'),source['ts'])).fetchone()
                source['candidate_id']=started[0] if started else None
                ctx=past_context(candles.get(source['symbol'],()),started[1]*1000) if started else {"source":"UNKNOWN_CANDIDATE"}
                self.on_premium(source,ctx,now,c)
            c.execute("UPDATE alt_runtime SET value=? WHERE key='premium_cursor'",(str(sid),))

    def close(self,c,r,reason,price,ts,observed):
        if r['lifecycle_status']!='OPEN':return
        gross=(price-r['fill_price'])*r['qty']
        snapshot=json.loads(r['cost_snapshot'])
        fees,slip=cost_model.costs(r['notional'],r['qty']*price,snapshot['fee_pct_per_side'],snapshot['slippage_pct_per_side'])
        r.update(lifecycle_status='CLOSED',terminal_reason=reason,terminal_ts=observed,exit_price=price,first_touch_order=reason,
                 gross_pnl=gross,fees=fees,slippage_cost=slip,net_pnl=gross-fees-slip,gross_return=gross/r['notional']*100,net_return=(gross-fees-slip)/r['notional']*100)
        r[{'TP':'tp_hit_ts','SL':'stop_hit_ts','TIME_EXIT':'timeout_ts'}[reason]]=ts
        self.event(c,r,reason,ts,observed,price)
        self.persist(c,'alt_research_trades','research_trade_id',r)
        self.ledger(c,r['model_name'],observed)

    def tick(self,c,symbol,price,ts,observed,ask=None):
        if not math.isfinite(price) or price<=0 or ts>observed+1000:return
        for r in list(self.trades.values()):
            if r['symbol']!=symbol or ts<=r['decision_ts'] or observed<=r['decision_ts'] or ts<=(r.get('last_event_ts') or 0):continue
            r['last_event_ts']=ts
            if observed-ts>3000:
                r['observation_gap']=1;continue
            r['last_price']=price;r['last_price_ts']=ts
            if r['lifecycle_status']=='ARMED':
                reason,risk,count,remaining=self.admit(c,r,observed)
                if observed-r['decision_ts']>30000:reason='NO_FRESH_FILL'
                if reason:
                    r.update(lifecycle_status='SKIPPED',skip_reason=reason,projected_risk=risk,concurrent_positions=count,remaining_daily_budget=remaining)
                    self.event(c,r,'SKIP_AT_FILL',ts,observed,price,{'reason':reason})
                else:
                    fill=ask if ask and math.isfinite(ask) and ask>0 else price
                    r.update(lifecycle_status='OPEN',fill_ts=observed,fill_event_ts=ts,fill_price=fill,fill_source='FRESH_ASK' if fill==ask else 'NEXT_TRADE_PROXY',
                             qty=r['notional']/fill,stop_price=fill*(1-r['stop_pct']/100),tp_price=fill*(1+r['tp_pct']/100),timeout_at=observed+3600000)
                    self.event(c,r,'FILL',ts,observed,fill,{'projected_risk':risk})
                self.persist(c,'alt_research_trades','research_trade_id',r)
            elif r['lifecycle_status']=='OPEN':
                ret=(price/r['fill_price']-1)*100
                r['mfe']=max(r['mfe'],ret);r['mae']=min(r['mae'],ret)
                r['max_favorable_excursion']=r['notional']*r['mfe']/100;r['max_adverse_excursion']=r['notional']*r['mae']/100
                # Timeout has priority over price touches observed after the 60m boundary.
                reason='TIME_EXIT' if ts>=r['timeout_at'] else 'SL' if price<=r['stop_price'] else 'TP' if price>=r['tp_price'] else None
                if reason and ts-r['timeout_at']<=5000:self.close(c,r,reason,price,ts,observed)
            if r.get('fill_ts'):
                for h in HORIZONS:
                    target=r['fill_ts']+h*1000
                    if ts>=target and h not in r.setdefault('_horizons',set()):
                        timely=ts-target<=5000
                        c.execute("INSERT OR IGNORE INTO alt_forward_outcomes VALUES (?,?,?,?,?,?,?)",(r['research_trade_id'],h,target,observed,price if timely else None,(price/r['fill_price']-1)*100 if timely else None,'OBSERVED' if timely else 'MISSING_WINDOW'))
                        r['_horizons'].add(h)
        for x in list(self.contexts.values()):
            if x['symbol']!=symbol or ts<=x['premium_ts'] or ts<=(x.get('last_event_ts') or 0):continue
            x['last_event_ts']=ts
            if observed-ts>3000:x['observation_gap']=1;continue
            ret=(price/x['premium_price']-1)*100
            post=json.loads(x['post_returns_json']);giveback=json.loads(x['giveback_json'])
            for h in (60,180,300):
                if ts>=x['premium_ts']+h*1000 and str(h) not in post:
                    post[str(h)]={'return_pct':ret if ts-x['premium_ts']-h*1000<=5000 else None,'observed_ts':observed}
            if ts<=x['premium_ts']+3600000 and price>x['peak_price']:
                x.update(peak_price=price,peak_ts=ts,peak_return=ret);giveback={}
            drawdown=(price/x['peak_price']-1)*100
            x['post_peak_drawdown']=min(x['post_peak_drawdown'],drawdown)
            for h in (60,180,300):
                if ts>=x['peak_ts']+h*1000 and str(h) not in giveback:
                    giveback[str(h)]={'return_pct':drawdown if ts-x['peak_ts']-h*1000<=5000 else None,'observed_ts':observed}
            tags=set(json.loads(x['tags_json']))
            ctx=json.loads(x['context_json'])
            if (ctx.get('pre_candidate_10m_runup') or 0)>=3:tags.add('LATE_STAGE_CONTINUATION')
            if x['peak_return']>=2 and drawdown<=-2:tags.add('EXHAUSTION_LIKE')
            path=c.execute("SELECT first_event,tp2_hit_s FROM signal_paths WHERE signal_id=?",(x['signal_id'],)).fetchone() if drawdown<=-1 else None
            if path and str(path[0]).upper() in ('TP2','TP2_FIRST') and path[1] is not None and ts<=x['premium_ts']+path[1]*1000+300000:tags.add('TP2_FIRST_FAST_REVERSAL')
            x.update(post_returns_json=json.dumps(post),giveback_json=json.dumps(giveback),tags_json=json.dumps(sorted(tags)))

    def step(self,candles=None,now=None,events=None):
        now=now or int(time.time()*1000)
        batch=list(events or [])
        while len(batch)<5000:
            try:batch.append(self.inbox.get_nowait())
            except queue.Empty:break
        with closing(self.connect()) as c,c:
            c.row_factory=sqlite3.Row
            c.execute('BEGIN IMMEDIATE')
            self.ingest(c,candles or {},now)
            if self.dropped:
                for r in (*self.trades.values(),*self.contexts.values()):r['observation_gap']=1
                self.dropped=False
            for args in batch:self.tick(c,*args)
            for r in list(self.trades.values()):
                if r['lifecycle_status']=='ARMED' and now-r['decision_ts']>30000:
                    r.update(lifecycle_status='SKIPPED',skip_reason='NO_FRESH_FILL')
                    self.event(c,r,'NO_FRESH_FILL',now,now)
                if r['lifecycle_status']=='OPEN' and now>r['timeout_at']+5000:
                    r.update(lifecycle_status='UNPRICED_TIMEOUT',terminal_reason='UNPRICED_TIMEOUT',terminal_ts=now,timeout_ts=r['timeout_at'],observation_gap=1)
                    self.event(c,r,'UNPRICED_TIMEOUT',r['timeout_at'],now)
                self.persist(c,'alt_research_trades','research_trade_id',r)
                if r['lifecycle_status']=='SKIPPED' or r.get('fill_ts') and now>=r['fill_ts']+7205000:
                    if r.get('fill_ts'):
                        for h in HORIZONS:
                            c.execute("INSERT OR IGNORE INTO alt_forward_outcomes VALUES (?,?,?,?,NULL,NULL,'MISSING_WINDOW')",(r['research_trade_id'],h,r['fill_ts']+h*1000,now))
                    self.trades.pop(r['research_trade_id'],None)
            for x in list(self.contexts.values()):
                self.persist(c,'alt_premium_context','signal_id',x)
                if now>x['telemetry_until']:self.contexts.pop(x['signal_id'],None)
        self._symbols()
