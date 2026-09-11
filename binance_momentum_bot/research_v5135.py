"""Additive, forward-only measurements. No order API or production gates."""
import hashlib
import json
import math
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from collections import defaultdict


@contextmanager
def connection(connect):
    c=connect()
    try:
        with c:
            yield c
    finally:
        c.close()
from pathlib import Path


def flag(name, default=False):
    return os.getenv(name, "1" if default else "0").strip().lower() in ("1", "true", "yes", "on")


def nonnegative(name, default):
    value = float(os.getenv(name, str(default)))
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite and nonnegative")
    return value


DRY_FEE_PCT = nonnegative("DRY_FEE_PCT_PER_SIDE", 0.05)
DRY_SLIPPAGE_PCT = nonnegative("DRY_SLIPPAGE_PCT_PER_SIDE", 0.02)
REENTRY_CONFIRM_S = max(1, nonnegative("REENTRY_SHADOW_CONFIRM_SECONDS", 5))
REENTRY_TIMEOUT_S = max(60, nonnegative("REENTRY_SHADOW_TIMEOUT_SECONDS", 3600))


def costs(entry_notional, exit_notional, fee=DRY_FEE_PCT, slippage=DRY_SLIPPAGE_PCT):
    turnover = abs(entry_notional) + abs(exit_notional)
    return turnover * fee / 100, turnover * slippage / 100


def migrate(conn):
    additions = {
        "autotrade_trades": {"gross_pnl": "REAL", "slippage_cost": "REAL", "cost_model_version": "TEXT",
                             "fee_pct_per_side": "REAL", "slippage_pct_per_side": "REAL"},
        "premium_liquidity_snapshots": {"reference_kind": "TEXT", "current_mid_reference_price": "REAL",
                                        "current_mid_metrics_json": "TEXT", "feature_ready_time_ms": "INTEGER"},
        "entry_stage_forward_shadow": {"feature_ready_time_ms": "INTEGER", "decision_time_ms": "INTEGER",
                                        "nominal_time_ms": "INTEGER", "causal_cohort_id": "TEXT"},
        "research_events": {"feature_ready_time_ms": "INTEGER", "decision_time_ms": "INTEGER", "causal_cohort_id": "TEXT"},
        "premium_liquidity_transition_v3": {"feature_ready_time_ms": "INTEGER", "decision_time_ms": "INTEGER", "causal_cohort_id": "TEXT"},
        "position_observer_state": {"position_instance_id": "TEXT", "leverage_source": "TEXT", "margin_source": "TEXT"},
    }
    for table, fields in additions.items():
        existing = {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}
        for name, decl in fields.items():
            if name not in existing:
                conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {decl}')
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS causal_cohorts (
            id TEXT PRIMARY KEY, source_key TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL, stage TEXT NOT NULL,
            signal_id INTEGER, episode_id INTEGER, premium_ordinal INTEGER, parent_id TEXT,
            nominal_time_ms INTEGER, feature_ready_time_ms INTEGER NOT NULL, decision_time_ms INTEGER NOT NULL,
            first_executable_price REAL, fill_time_ms INTEGER, fill_event_time_ms INTEGER, fill_source TEXT,
            status TEXT NOT NULL, stop_pct REAL NOT NULL, tp_pct REAL NOT NULL, stop_price REAL, tp_price REAL,
            first_event TEXT, first_event_time_ms INTEGER, exit_price REAL, gross_pnl_pct REAL, fees_pct REAL,
            slippage_pct REAL, net_pnl_pct REAL, combined_net_pnl_pct REAL, mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
            fee_pct_per_side REAL NOT NULL, slippage_pct_per_side REAL NOT NULL,
            reclaim_price REAL, reclaim_since_ms INTEGER, deadline_ms INTEGER NOT NULL,
            last_event_time_ms INTEGER, observation_gap INTEGER DEFAULT 0, config_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS causal_cohorts_symbol_status ON causal_cohorts(symbol,status);
        CREATE TABLE IF NOT EXISTS causal_cohort_events (
            id INTEGER PRIMARY KEY, cohort_id TEXT NOT NULL, event TEXT NOT NULL, event_time_ms INTEGER NOT NULL,
            observed_time_ms INTEGER NOT NULL, price REAL, detail_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS causal_cohort_outcomes (
            cohort_id TEXT NOT NULL, horizon_s INTEGER NOT NULL, observed_time_ms INTEGER NOT NULL,
            price REAL NOT NULL, return_pct REAL NOT NULL, mfe_pct REAL NOT NULL, mae_pct REAL NOT NULL,
            PRIMARY KEY(cohort_id,horizon_s));
        CREATE TABLE IF NOT EXISTS premium_fatigue_shadow (
            signal_id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, episode_key TEXT NOT NULL,
            premium_ordinal INTEGER NOT NULL, ordinal_group TEXT NOT NULL, decision_time_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS position_observer_events (
            id INTEGER PRIMARY KEY, position_instance_id TEXT NOT NULL, symbol TEXT NOT NULL, position_side TEXT NOT NULL,
            event TEXT NOT NULL, event_time_ms INTEGER NOT NULL, entry_price REAL, current_price REAL,
            direction TEXT, source TEXT, leverage REAL, leverage_source TEXT, margin_source TEXT,
            roe REAL, detail_json TEXT NOT NULL, notification_delivery TEXT NOT NULL,
            delivery_time_ms INTEGER);
        CREATE TABLE IF NOT EXISTS measurement_migrations (version TEXT PRIMARY KEY, applied_time_ms INTEGER NOT NULL);
    """)
    conn.execute("INSERT OR IGNORE INTO measurement_migrations VALUES ('5.13.5',?)", (int(time.time()*1000),))


class Measurements:
    def __init__(self, connect):
        self.connect = connect
        self.active = {}
        self.by_symbol = defaultdict(set)
        with connection(connect) as c:
            c.row_factory = sqlite3.Row
            for row in c.execute("SELECT * FROM causal_cohorts WHERE status IN ('ARMED','OPEN','WATCH')"):
                item = dict(row)
                item["observation_gap"] = 1
                self.active[item["id"]] = item
                self.by_symbol[item["symbol"]].add(item["id"])
                if item["status"]=="WATCH":
                    item["reclaim_since_ms"]=None
                    c.execute("UPDATE causal_cohorts SET reclaim_since_ms=NULL WHERE id=?",(item["id"],))
                item["_horizons"]={r[0] for r in c.execute("SELECT horizon_s FROM causal_cohort_outcomes WHERE cohort_id=?",(item["id"],))}
                c.execute("UPDATE causal_cohorts SET observation_gap=1 WHERE id=?", (item["id"],))
            self.symbols = {x["symbol"] for x in self.active.values()}
        c.close()

    def event(self, c, item, kind, event_ms, observed_ms, price=None, detail=None):
        c.execute("INSERT INTO causal_cohort_events(cohort_id,event,event_time_ms,observed_time_ms,price,detail_json) VALUES (?,?,?,?,?,?)",
                  (item["id"], kind, event_ms, observed_ms, price, json.dumps(detail or {})))

    def arm(self, source_key, symbol, stage, *, signal_id=None, episode_id=0, nominal_ms=None,
            stop_pct=1.0, tp_pct=2.0, ready_ms=None, parent=None):
        now = int(time.time()*1000)
        with connection(self.connect) as c:
            existing = c.execute("SELECT id FROM causal_cohorts WHERE source_key=?", (source_key,)).fetchone()
            if existing:
                return existing[0]
            ordinal = None
            if signal_id:
                r = c.execute("SELECT premium_ordinal FROM premium_fatigue_shadow WHERE signal_id=?", (signal_id,)).fetchone()
                ordinal = r[0] if r else None
            item = dict(id=uuid.uuid4().hex, source_key=source_key, symbol=symbol, stage=stage,
                        signal_id=signal_id, episode_id=episode_id, premium_ordinal=ordinal,
                        parent_id=parent["id"] if parent else None, nominal_time_ms=nominal_ms,
                        feature_ready_time_ms=min(now, ready_ms or now), decision_time_ms=now,
                        status="WATCH" if parent else "ARMED", stop_pct=abs(stop_pct), tp_pct=abs(tp_pct),
                        fee_pct_per_side=DRY_FEE_PCT, slippage_pct_per_side=DRY_SLIPPAGE_PCT,
                        reclaim_price=parent["first_executable_price"] if parent else None,
                        deadline_ms=now+int(REENTRY_TIMEOUT_S*1000), observation_gap=0,
                        config_json=json.dumps({"version":"5.13.5", "shadow":True, "reclaim_confirm_s":REENTRY_CONFIRM_S}),
                        mfe_pct=0.0, mae_pct=0.0)
            cols = list(item)
            c.execute(f"INSERT INTO causal_cohorts({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})", tuple(item.values()))
            self.event(c, item, item["status"], now, now)
        c.close()
        self.active[item["id"]] = item
        self.by_symbol[symbol].add(item["id"])
        self.symbols.add(symbol)
        return item["id"]

    def fatigue(self, signal_id, symbol, episode_id, ready_ms=None):
        now = ready_ms or int(time.time()*1000)
        # Episode ID is authoritative. Missing episode gets an explicit, bounded 6h bucket.
        key = f"episode:{episode_id}" if episode_id else f"unassigned-6h:{now//21600000}"
        with connection(self.connect) as c:
            if not c.execute("SELECT 1 FROM premium_fatigue_shadow WHERE signal_id=?", (signal_id,)).fetchone():
                n = c.execute("SELECT COUNT(*) FROM premium_fatigue_shadow WHERE symbol=? AND episode_key=?", (symbol,key)).fetchone()[0]+1
                c.execute("INSERT INTO premium_fatigue_shadow VALUES (?,?,?,?,?,?)", (signal_id,symbol,key,n,str(n) if n<3 else "3+",now))
        c.close()

    def tick(self, symbol, price, event_ms, observed_ms, executable_ask=None):
        if symbol not in self.symbols or price <= 0:
            return
        selected=[self.active[cid] for cid in self.by_symbol[symbol]
                  if event_ms>self.active[cid]["decision_time_ms"] and event_ms>(self.active[cid].get("last_event_time_ms") or 0)]
        if not selected: return
        # Inspect every event for first passage, but batch nonterminal extrema persistence.
        quiet=all(x["status"]=="OPEN" and x["stop_price"]<price<x["tp_price"]
                  and observed_ms<x["deadline_ms"] and observed_ms-x.get("_saved_ms",0)<1000
                  and not any(observed_ms-x["fill_time_ms"]>=h*1000 and h not in x.get("_horizons",set()) for h in (60,300,900,1800,3600))
                  for x in selected)
        if quiet:
            for x in selected:
                ret=(price/x["first_executable_price"]-1)*100
                x["mfe_pct"]=max(x["mfe_pct"],ret);x["mae_pct"]=min(x["mae_pct"],ret)
                x["last_event_time_ms"]=event_ms
            return
        children = []
        with connection(self.connect) as c:
            for x in selected:
                if x["symbol"] != symbol or event_ms <= x["decision_time_ms"] or event_ms <= (x.get("last_event_time_ms") or 0):
                    continue
                x["last_event_time_ms"] = event_ms
                changes = {"last_event_time_ms":event_ms}
                if x["status"] == "WATCH":
                    if observed_ms >= x["deadline_ms"]:
                        changes.update(status="TIMEOUT", first_event="WATCH_TIMEOUT", first_event_time_ms=observed_ms)
                        parent = c.execute("SELECT net_pnl_pct FROM causal_cohorts WHERE id=?", (x["parent_id"],)).fetchone()
                        changes["combined_net_pnl_pct"] = parent[0] if parent else None
                    elif price >= x["reclaim_price"]:
                        since = x.get("reclaim_since_ms")
                        changes["reclaim_since_ms"] = since or observed_ms
                        if since and observed_ms-since >= json.loads(x["config_json"])["reclaim_confirm_s"]*1000:
                            changes.update(status="ARMED", feature_ready_time_ms=observed_ms,
                                           decision_time_ms=observed_ms, reclaim_since_ms=None)
                    else:
                        changes["reclaim_since_ms"] = None
                elif x["status"] == "ARMED":
                    if observed_ms >= x["deadline_ms"]:
                        changes.update(status="TIMEOUT", first_event="NO_FILL_TIMEOUT", first_event_time_ms=observed_ms)
                    elif observed_ms-event_ms <= 3000:
                        fill = executable_ask if executable_ask and executable_ask>0 else price
                        changes.update(status="OPEN", first_executable_price=fill, fill_time_ms=observed_ms,
                                       fill_event_time_ms=event_ms, fill_source="FRESH_ASK" if executable_ask else "NEXT_TRADE_PROXY",
                                       stop_price=fill*(1-x["stop_pct"]/100), tp_price=fill*(1+x["tp_pct"]/100),
                                       deadline_ms=observed_ms+int(REENTRY_TIMEOUT_S*1000))
                elif x["status"] == "OPEN":
                    fill = x["first_executable_price"]
                    ret = (price/fill-1)*100
                    changes.update(mfe_pct=max(x["mfe_pct"],ret), mae_pct=min(x["mae_pct"],ret))
                    age = (observed_ms-x["fill_time_ms"])/1000
                    for horizon in (60,300,900,1800,3600):
                        if age >= horizon and horizon not in x.get("_horizons",set()):
                            c.execute("INSERT OR IGNORE INTO causal_cohort_outcomes VALUES (?,?,?,?,?,?,?)",
                                      (x["id"],horizon,observed_ms,price,ret,changes["mfe_pct"],changes["mae_pct"]))
                            x.setdefault("_horizons",set()).add(horizon)
                    terminal = "STOP" if price <= x["stop_price"] else "TP" if price >= x["tp_price"] else "TIMEOUT" if observed_ms>=x["deadline_ms"] else None
                    if terminal:
                        fee, slip = costs(100,100*price/fill,x["fee_pct_per_side"],x["slippage_pct_per_side"])
                        net = ret-fee-slip
                        changes.update(status="CLOSED", first_event=terminal, first_event_time_ms=observed_ms,
                                       exit_price=price, gross_pnl_pct=ret, fees_pct=fee, slippage_pct=slip, net_pnl_pct=net)
                        if x["parent_id"]:
                            parent = c.execute("SELECT net_pnl_pct FROM causal_cohorts WHERE id=?", (x["parent_id"],)).fetchone()
                            changes["combined_net_pnl_pct"] = net+(parent[0] or 0)
                        elif terminal=="STOP" and x["stage"]=="PREMIUM":
                            children.append(dict(x))
                if changes.get("status"):
                    self.event(c,x,changes.get("first_event") or changes["status"],event_ms,observed_ms,price,changes)
                c.execute("UPDATE causal_cohorts SET "+",".join(k+"=?" for k in changes)+" WHERE id=?",(*changes.values(),x["id"]))
                x.update(changes)
                x["_saved_ms"]=observed_ms
                if x["status"] in ("CLOSED","TIMEOUT"):
                    self.active.pop(x["id"],None)
                    self.by_symbol[symbol].discard(x["id"])
        c.close()
        for parent in children:
            self.arm("reentry:"+parent["id"],symbol,"REENTRY", signal_id=parent["signal_id"],
                     episode_id=parent["episode_id"],stop_pct=parent["stop_pct"],tp_pct=parent["tp_pct"],parent=parent)
        if not self.by_symbol[symbol]: self.symbols.discard(symbol)


    def expire(self, observed_ms):
        expired=[x for x in self.active.values() if observed_ms>=x["deadline_ms"]]
        if not expired: return
        with connection(self.connect) as c:
            for x in expired:
                # No quote at deadline: do not manufacture a timeout fill/P&L.
                kind="DATA_UNAVAILABLE_TIMEOUT" if x["status"]=="OPEN" else "WATCH_TIMEOUT" if x["status"]=="WATCH" else "NO_FILL_TIMEOUT"
                combined=None
                if x["parent_id"] and x["status"]!="OPEN":
                    row=c.execute("SELECT net_pnl_pct FROM causal_cohorts WHERE id=?",(x["parent_id"],)).fetchone()
                    combined=row[0] if row else None
                c.execute("UPDATE causal_cohorts SET status='TIMEOUT',first_event=?,first_event_time_ms=?,combined_net_pnl_pct=? WHERE id=?",(kind,observed_ms,combined,x["id"]))
                self.event(c,x,kind,observed_ms,observed_ms)
                self.active.pop(x["id"],None)
                self.by_symbol[x["symbol"]].discard(x["id"])
                if not self.by_symbol[x["symbol"]]: self.symbols.discard(x["symbol"])



def backup_manifest(db_path, *, version, deployment_id, config):
    """Inspect only the completed backup and restore into a separate in-memory DB."""
    raw = Path(db_path).read_bytes()
    c = sqlite3.connect(Path(db_path).resolve().as_uri()+"?mode=ro", uri=True)
    try:
        quick = [r[0] for r in c.execute("PRAGMA quick_check")]
        integrity = [r[0] for r in c.execute("PRAGMA integrity_check")]
        tables = {}
        for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
            quoted = '"'+name.replace('"','""')+'"'
            timestamps = {}
            for col in c.execute(f"PRAGMA table_info({quoted})"):
                field = col[1]
                if field=="ts" or field.endswith(("_ts","_ms")) and any(s in field for s in ("time","event","created","observed","updated","closed","opened","ts")):
                    q = '"'+field.replace('"','""')+'"'
                    timestamps[field] = c.execute(f"SELECT MIN({q}),MAX({q}) FROM {quoted}").fetchone()
            tables[name] = {"rows":c.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0],"timestamp_ranges":timestamps}
        restored = sqlite3.connect(":memory:")
        try:
            c.backup(restored)
            smoke = [r[0] for r in restored.execute("PRAGMA integrity_check")]
            restore_counts = all(restored.execute('SELECT COUNT(*) FROM "'+n.replace('"','""')+'"').fetchone()[0]==v["rows"] for n,v in tables.items())
        finally:
            restored.close()
        identity = hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest()
        return dict(sha256=hashlib.sha256(raw).hexdigest(),size_bytes=len(raw),version=version,
                    deployment_id=deployment_id,config_identity=identity,config=config,tables=tables,
                    quick_check=quick,integrity_check=integrity,restore_smoke_check=smoke,
                    restore_row_counts_match=restore_counts,created_time_ms=int(time.time()*1000))
    finally:
        c.close()
