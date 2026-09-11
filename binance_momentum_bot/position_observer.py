"""Read-only Futures position observer with durable identity and event history."""
import json
import time
import uuid


class LeverageCache:
    def __init__(self, ttl=300):
        self.ttl = ttl
        self.expires = 0
        self.values = {}
        self.retry_after = 0

    async def refresh(self, session, request):
        now = time.monotonic()
        if now < self.expires or now < self.retry_after:
            return
        try:
            rows = await request(session,"GET","/fapi/v1/symbolConfig")
            if not isinstance(rows,list):
                raise ValueError("symbolConfig is not an array")
            values = {}
            for row in rows:
                lev = float(row.get("leverage") or 0)
                if 0 < lev <= 1000:
                    values[row["symbol"]] = lev
            self.values, self.expires = values, now+self.ttl
        except Exception:
            # Expired configuration must not silently become a guessed 1x.
            self.values = {}
            self.retry_after = now+30

    def get(self, symbol):
        lev = self.values.get(symbol) if time.monotonic()<self.expires else None
        return lev, "symbolConfig" if lev else "UNKNOWN"


def roe_values(p, direction, entry, mark, leverage):
    pnl = float(p.get("unRealizedProfit",p.get("unrealizedProfit",0)) or 0)
    margin = float(p.get("positionInitialMargin") or 0)
    if margin>0:
        return pnl/margin*100,pnl,"positionInitialMargin"
    if leverage and entry>0:
        signed = (mark/entry-1)*100*(1 if direction=="LONG" else -1)
        return signed*leverage,pnl,"PRICE_RETURN_X_SYMBOL_CONFIG_LEVERAGE"
    return None,pnl,"UNKNOWN"


class PositionObserver:
    def __init__(self, connect, request, send, source, zone, milestones, confirm_s, ttl=300):
        self.connect,self.request,self.send,self.source,self.zone = connect,request,send,source,zone
        self.milestones,self.confirm_s = milestones,confirm_s
        self.cache = LeverageCache(ttl)

    def event(self,c,state,event,mark,roe,detail=None,notify=False):
        now = int(time.time()*1000)
        result = c.execute("""INSERT INTO position_observer_events
            (position_instance_id,symbol,position_side,event,event_time_ms,entry_price,current_price,direction,source,leverage,leverage_source,margin_source,roe,detail_json,notification_delivery)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (state["position_instance_id"],state["symbol"],state["position_side"],event,now,state["entry_price"],mark,
             state["direction"],state["source"],state["leverage"],state["leverage_source"],state["margin_source"],roe,
             json.dumps(detail or {}),"PENDING" if notify else "NOT_REQUESTED"))
        return result.lastrowid

    async def poll(self,session):
        positions = await self.request(session,"GET","/fapi/v3/positionRisk")
        if not isinstance(positions,list):
            raise ValueError("positionRisk is not an array")
        await self.cache.refresh(session,self.request)
        active = set()
        now = int(time.time())
        c = self.connect()
        try:
            c.row_factory = __import__('sqlite3').Row
            for p in positions:
                amount = float(p.get("positionAmt") or 0)
                if abs(amount)<1e-12:
                    continue
                sym,side = p["symbol"],p.get("positionSide") or "BOTH"
                key = sym,side
                active.add(key)
                direction = "LONG" if side=="LONG" or side=="BOTH" and amount>0 else "SHORT"
                entry,mark = float(p.get("entryPrice") or 0),float(p.get("markPrice") or 0)
                if entry<=0 or mark<=0:
                    continue
                lev,lev_source = self.cache.get(sym)
                roe,pnl,margin_source = roe_values(p,direction,entry,mark,lev)
                zone,move = self.zone(direction,mark,entry)
                row = c.execute("SELECT * FROM position_observer_state WHERE symbol=? AND position_side=?",key).fetchone()
                old = dict(row) if row else None
                reset = old is None or not old["active"] or old["direction"]!=direction or abs(old["entry_price"]-entry)/entry*100>0.01
                instance = uuid.uuid4().hex if reset or not old.get("position_instance_id") else old["position_instance_id"]
                s = dict(symbol=sym,position_side=side,direction=direction,entry_price=entry,qty=abs(amount),leverage=lev,
                         source=self.source(sym,side),leverage_source=lev_source,margin_source=margin_source,
                         position_instance_id=instance,zone=zone if reset else old["zone"],pending_zone=None if reset else old["pending_zone"],
                         pending_since_ts=0 if reset else old["pending_since_ts"],profit_hits_json="[]" if reset else old["profit_hits_json"],
                         loss_hits_json="[]" if reset else old["loss_hits_json"],last_roe=roe,last_unrealized_pnl=pnl,active=1,updated_ts=now)
                if reset:
                    if old and old.get("position_instance_id") and old["active"]:
                        self.event(c,old,"RESET",mark,old["last_roe"],{"next_instance_id":instance})
                    self.event(c,s,"OPEN_OBSERVED",mark,roe,{"entry_observation_only":True})
                else:
                    if not old.get("position_instance_id"):
                        self.event(c,s,"ADOPT_EXISTING",mark,roe)
                    if zone!="NEUTRAL" and zone!=s["zone"]:
                        if s["pending_zone"]!=zone:
                            s["pending_zone"],s["pending_since_ts"] = zone,now
                        elif now-int(s["pending_since_ts"] or 0)>=self.confirm_s:
                            s["zone"],s["pending_zone"],s["pending_since_ts"] = zone,None,0
                            self.event(c,s,"ENTRY_CROSS_"+zone,mark,roe,{"move_pct":move},True)
                    else:
                        s["pending_zone"],s["pending_since_ts"] = None,0
                for field,sign in (("profit_hits_json",1),("loss_hits_json",-1)):
                    hits = set(json.loads(s[field] or "[]"))
                    crossed = [v for v in self.milestones if roe is not None and roe*sign>=v and v not in hits]
                    if crossed:
                        self.event(c,s,"ROE_PROFIT" if sign==1 else "ROE_LOSS",mark,roe,{"milestones":crossed,"pnl":pnl},True)
                        s[field] = json.dumps(sorted(hits.union(crossed)))
                columns = list(s)
                c.execute("INSERT INTO position_observer_state("+",".join(columns)+") VALUES ("+",".join("?" for _ in columns)+") ON CONFLICT(symbol,position_side) DO UPDATE SET "+",".join(k+"=excluded."+k for k in columns),tuple(s.values()))
            for row in c.execute("SELECT * FROM position_observer_state WHERE active=1").fetchall():
                if (row["symbol"],row["position_side"]) not in active:
                    old = dict(row)
                    old["position_instance_id"] = old.get("position_instance_id") or uuid.uuid4().hex
                    self.event(c,old,"CLOSE_OBSERVED",None,old["last_roe"],{"exact_close_time_unknown":True})
                    c.execute("UPDATE position_observer_state SET active=0,updated_ts=? WHERE symbol=? AND position_side=?",(now,row["symbol"],row["position_side"]))
            c.commit()
            deliveries = [dict(r) for r in c.execute("SELECT * FROM position_observer_events WHERE notification_delivery='PENDING'")]
        finally:
            c.close()
        # No SQLite write transaction held during Telegram awaits.
        for e in deliveries:
            lev_text = f"{e['leverage']:g}x" if e["leverage"] else "UNKNOWN"
            roe_text = f"{e['roe']:+.2f}%" if e["roe"] is not None else "UNKNOWN"
            text = (f"👁 {e['symbol']} — {e['event']}\n{e['direction']} · {lev_text} · {e['source']}\n"
                    f"Entry: {e['entry_price']:g} | Anlık: {e['current_price']:g}\nROE: {roe_text}\n"
                    f"{e['detail_json']}\nLeverage: {e['leverage_source']} | Margin: {e['margin_source']}")
            try:
                ok = bool(await self.send(session,text))
            except Exception:
                ok = False
            c = self.connect()
            try:
                c.execute("UPDATE position_observer_events SET notification_delivery=?,delivery_time_ms=? WHERE id=?",("DELIVERED" if ok else "FAILED",int(time.time()*1000),e["id"])); c.commit()
            finally:
                c.close()
