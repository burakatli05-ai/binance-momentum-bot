"""Read-only Futures position observer with durable identity and event history."""
import asyncio
import logging
import math
import json
import time
import uuid
from telegram_cards import ownership, number, timestamp

# Notification-only hysteresis (ROE percentage points) and cooldown. Not trade gates.
RECOVERY_HYSTERESIS = 1.0
ALERT_COOLDOWN_S = 60
REQUEST_TIMEOUT_S = 15
log = logging.getLogger(__name__)


def roe_crossings(memory, roe, milestones, loss_hits, now):
    losses, recoveries = [], []
    if roe is None: return losses, recoveries
    for level in milestones:
        item = memory.setdefault(str(level), dict(down_armed=level not in loss_hits, up_armed=False, last_alert=None))
        if roe <= -level-RECOVERY_HYSTERESIS: item['up_armed'] = True
        if roe >= -level+RECOVERY_HYSTERESIS: item['down_armed'] = True
        ready = item['last_alert'] is None or now-item['last_alert'] >= ALERT_COOLDOWN_S
        if ready and roe <= -level and item['down_armed']:
            losses.append(level); item['down_armed'] = False; item['last_alert'] = now
        elif ready and roe > -level and item['up_armed'] and level in (5,10,20):
            recoveries.append(level); item['up_armed'] = False; item['last_alert'] = now
    return losses, recoveries


class LeverageCache:
    def __init__(self, ttl=300):
        self.ttl = ttl
        self.expires = 0
        self.values = {}
        self.retry_after = 0

    async def refresh(self, session, request, force=False):
        now = time.monotonic()
        if not force and (now < self.expires or now < self.retry_after):
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
    raw_pnl = p.get("unRealizedProfit",p.get("unrealizedProfit"))
    pnl = float(raw_pnl) if raw_pnl is not None else None
    margin = float(p.get("positionInitialMargin") or 0)
    if margin>0 and pnl is not None:
        return pnl/margin*100,pnl,"positionInitialMargin"
    if leverage and entry>0:
        signed = (mark/entry-1)*100*(1 if direction=="LONG" else -1)
        return signed*leverage,pnl,"PRICE_RETURN_X_SYMBOL_CONFIG_LEVERAGE"
    return None,pnl,"UNKNOWN"


class PositionObserver:
    def __init__(self, connect, request, send, source, zone, milestones, confirm_s, ttl=300):
        self.connect,self.send,self.source,self.zone = connect,send,source,zone
        self._request = request
        self.baselined = False
        self.stage = "idle"
        self.milestones,self.confirm_s = milestones,confirm_s
        self.cache = LeverageCache(ttl)

    async def request(self, session, method, path):
        self.stage = path.rsplit('/', 1)[-1]
        return await asyncio.wait_for(self._request(session, method, path), REQUEST_TIMEOUT_S)

    async def safe_delivery(self, session):
        try:
            self.stage = 'delivery'
            await self.deliver_pending(session)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning('Position observer delivery failed (%s); durable queue retained', type(error).__name__)

    async def poll(self, session):
        try:
            return await self._poll(session)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning('Position observer poll failed at %s (%s); retry next poll', self.stage, type(error).__name__)
            return False

    def event(self,c,state,event,mark,roe,detail=None,notify=False):
        now = int(time.time()*1000)
        detail=dict(detail or {})
        detail.setdefault("pnl",state.get("last_unrealized_pnl"))
        result = c.execute("""INSERT INTO position_observer_events
            (position_instance_id,symbol,position_side,event,event_time_ms,entry_price,current_price,direction,source,leverage,leverage_source,margin_source,roe,detail_json,notification_delivery)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (state["position_instance_id"],state["symbol"],state["position_side"],event,now,state["entry_price"],mark,
             state["direction"],state["source"],state["leverage"],state["leverage_source"],state["margin_source"],roe,
             json.dumps(detail or {}),"PENDING" if notify else "NOT_REQUESTED"))
        return result.lastrowid

    async def _poll(self,session):
        await self.safe_delivery(session)
        positions = await self.request(session,"GET","/fapi/v3/positionRisk")
        if not isinstance(positions,list):
            raise ValueError("positionRisk is not an array")
        # Validate the complete snapshot before any state/close mutation. A malformed
        # response must never masquerade as a missing (closed) position.
        seen = set()
        for p in positions:
            key = (p['symbol'], p.get('positionSide') or 'BOTH')
            if not key[0] or key[1] not in ('BOTH', 'LONG', 'SHORT') or key in seen:
                raise ValueError('invalid position identity')
            seen.add(key)
            amount = float(p['positionAmt'])
            if not math.isfinite(amount): raise ValueError('invalid position amount')
            if abs(amount) > 1e-12:
                for field in ('entryPrice', 'markPrice'):
                    value = float(p[field])
                    if not math.isfinite(value) or value <= 0: raise ValueError('invalid position price')
                for field in ('unRealizedProfit', 'unrealizedProfit', 'positionInitialMargin'):
                    if p.get(field) is not None and not math.isfinite(float(p[field])):
                        raise ValueError('invalid position metric')
        # New position basis forces an authoritative refresh before any new card.
        with __import__('contextlib').closing(self.connect()) as before:
            before.row_factory = __import__('sqlite3').Row
            previous = {(r['symbol'],r['position_side']):dict(r) for r in before.execute('SELECT * FROM position_observer_state')}
        force = False
        for p in positions:
            amount=float(p.get('positionAmt') or 0)
            if abs(amount)<1e-12: continue
            side=p.get('positionSide') or 'BOTH';entry=float(p.get('entryPrice') or 0)
            direction='LONG' if side=='LONG' or side=='BOTH' and amount>0 else 'SHORT'
            old=previous.get((p['symbol'],side))
            if not old or not old['active'] or not old.get('position_instance_id') or old['direction']!=direction or entry>0 and abs(old['entry_price']-entry)/entry*100>0.01:
                force=True
        await self.cache.refresh(session,self.request,force=force)
        self.stage = "state transaction"
        active = set()
        now = int(time.time())
        c = self.connect()
        try:
            c.row_factory = __import__('sqlite3').Row
            c.execute('''CREATE TABLE IF NOT EXISTS position_observer_ux_state (
                position_instance_id TEXT PRIMARY KEY, state_json TEXT NOT NULL)''')
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
                ux_row = c.execute('SELECT state_json FROM position_observer_ux_state WHERE position_instance_id=?',(instance,)).fetchone()
                ux = json.loads(ux_row[0]) if ux_row else {'roe':{},'entry_last_alert':None}
                # On upgrade/restart, a previously observed deep loss can arm recovery;
                # no existing downward-hit history is reset.
                if not ux_row and not reset and old.get('last_roe') is not None:
                    for level in self.milestones:
                        ux['roe'][str(level)] = dict(down_armed=level not in json.loads(s['loss_hits_json'] or '[]'),
                            up_armed=old['last_roe'] <= -level-RECOVERY_HYSTERESIS,last_alert=None)
                if reset:
                    if old and old.get("position_instance_id") and old["active"]:
                        self.event(c,old,"RESET",mark,old["last_roe"],{"next_instance_id":instance})
                    # First successful snapshot is adoption, including positions opened
                    # while this process was offline. Same-direction basis changes (e.g.
                    # scale-in) are not a new opening announcement.
                    announce = self.baselined and (old is None or not old['active'] or old['direction'] != direction)
                    self.event(c,s,"OPEN_OBSERVED",mark,roe,{"entry_observation_only":True,
                        "adopted":not announce},announce)
                else:
                    if not old.get("position_instance_id"):
                        self.event(c,s,"ADOPT_EXISTING",mark,roe)
                    if zone!="NEUTRAL" and zone!=s["zone"]:
                        if s["pending_zone"]!=zone:
                            s["pending_zone"],s["pending_since_ts"] = zone,now
                        elif (now-int(s["pending_since_ts"] or 0)>=self.confirm_s and
                              (ux['entry_last_alert'] is None or now-ux['entry_last_alert']>=ALERT_COOLDOWN_S)):
                            s["zone"],s["pending_zone"],s["pending_since_ts"] = zone,None,0
                            self.event(c,s,"ENTRY_CROSS_"+zone,mark,roe,{"move_pct":move},True)
                            ux['entry_last_alert'] = now
                    else:
                        s["pending_zone"],s["pending_since_ts"] = None,0
                for field,sign in (("profit_hits_json",1),):
                    hits = set(json.loads(s[field] or "[]"))
                    crossed = [v for v in self.milestones if roe is not None and roe*sign>=v and v not in hits]
                    if crossed:
                        self.event(c,s,"ROE_PROFIT" if sign==1 else "ROE_LOSS",mark,roe,{"milestones":crossed,"pnl":pnl},True)
                        s[field] = json.dumps(sorted(hits.union(crossed)))
                loss_hits=set(json.loads(s['loss_hits_json'] or '[]'))
                losses,recoveries=roe_crossings(ux['roe'],roe,self.milestones,loss_hits,now)
                if losses:
                    self.event(c,s,'ROE_LOSS',mark,roe,{'milestones':losses,'pnl':pnl},True)
                    s['loss_hits_json']=json.dumps(sorted(loss_hits.union(losses)))
                if recoveries:self.event(c,s,'ROE_RECOVERY',mark,roe,{'milestones':recoveries,'pnl':pnl},True)
                c.execute('INSERT INTO position_observer_ux_state VALUES (?,?) ON CONFLICT(position_instance_id) DO UPDATE SET state_json=excluded.state_json',
                          (instance,json.dumps(ux)))
                columns = list(s)
                c.execute("INSERT INTO position_observer_state("+",".join(columns)+") VALUES ("+",".join("?" for _ in columns)+") ON CONFLICT(symbol,position_side) DO UPDATE SET "+",".join(k+"=excluded."+k for k in columns),tuple(s.values()))
            for row in c.execute("SELECT * FROM position_observer_state WHERE active=1").fetchall():
                if (row["symbol"],row["position_side"]) not in active:
                    old = dict(row)
                    old["position_instance_id"] = old.get("position_instance_id") or uuid.uuid4().hex
                    self.event(c,old,"CLOSE_OBSERVED",None,old["last_roe"],{"exact_close_time_unknown":True},True)
                    c.execute("UPDATE position_observer_state SET active=0,updated_ts=? WHERE symbol=? AND position_side=?",(now,row["symbol"],row["position_side"]))
            c.commit()
            self.baselined = True

        finally:
            c.close()
        await self.safe_delivery(session)
        return True

    async def deliver_pending(self, session):
        now = int(time.time()*1000)
        c = self.connect()
        try:
            c.row_factory = __import__('sqlite3').Row
            deliveries = [dict(r) for r in c.execute("""SELECT * FROM position_observer_events
                WHERE notification_delivery IN ('PENDING','FAILED') AND attempt_count<4
                AND (last_attempt_time IS NULL OR last_attempt_time +
                     CASE attempt_count WHEN 1 THEN 30000 WHEN 2 THEN 60000 ELSE 120000 END <= ?)
                ORDER BY id LIMIT 50""", (now,))]
        finally:
            c.close()
        # One initial attempt + at most three retries, reserved durably before I/O.
        # No SQLite write transaction held during Telegram awaits.
        for e in deliveries:
            c = self.connect()
            try:
                claimed = c.execute("""UPDATE position_observer_events SET attempt_count=attempt_count+1,
                    last_attempt_time=?,notification_delivery='FAILED'
                    WHERE id=? AND attempt_count=? AND notification_delivery IN ('PENDING','FAILED')""",
                    (int(time.time()*1000), e["id"], e["attempt_count"])).rowcount
                c.commit()
            finally:
                c.close()
            if not claimed:
                continue
            text = render_card(e)
            try:
                ok = bool(await asyncio.wait_for(self.send(session,text), REQUEST_TIMEOUT_S))
            except Exception:
                ok = False
            c = self.connect()
            try:
                c.execute("UPDATE position_observer_events SET notification_delivery=?,delivery_time_ms=? WHERE id=?",("DELIVERED" if ok else "FAILED",int(time.time()*1000),e["id"])); c.commit()
            finally:
                c.close()


class OpeningCard(str):
    def __new__(cls, text, event):
        value = super().__new__(cls, text)
        value.opening_event = dict(event)
        return value


def render_card(event):
    detail=json.loads(event.get('detail_json') or '{}')
    kind=event['event']
    label={'ENTRY_CROSS_LOSS':'🔴 GİRİŞ ALTINA İNDİ','ENTRY_CROSS_PROFIT':'🟢 GİRİŞ ÜZERİNE ÇIKTI',
           'CLOSE_OBSERVED':'🔵 POZİSYON KAPANDI','OPEN_OBSERVED':'🟢 POZİSYON AÇILDI',
           'RESET':'👁 POZİSYON BİLGİSİ YENİLENDİ','ADOPT_EXISTING':'👁 POZİSYON İZLENİYOR'}.get(kind,'👁 POZİSYON BİLDİRİMİ')
    if detail.get('milestones'):
        values=' / '.join(f'{"+" if kind=="ROE_PROFIT" else "-"}%{n:g}' for n in detail['milestones'])
        label=('🟢 ROE '+values+' ÜZERİNE TOPARLANDI') if kind=='ROE_RECOVERY' else ('🟢 ' if kind=='ROE_PROFIT' else '🔴 ')+'ROE '+values
    closed=kind=='CLOSE_OBSERVED'
    text = (f'{label}\n\n{event["symbol"]} · {event.get("direction") or "—"}\n'
            f'{ownership(event.get("source"))}\n'
            f'Giriş: {number(event.get("entry_price"))} | {"Kapanış" if closed else "Anlık"}: {number(event.get("current_price"))}\n'
            f'Kaldıraç: {number(event.get("leverage"),"x")} | {"Son gözlem ROE" if closed else "ROE"}: {number(event.get("roe"),"%",True)}\n'
            f'{"Son gözlem P/L" if closed else "P/L"}: {number(detail.get("pnl")," USDT",True)}'+
            ('\nKesin kapanış fiyatı/gerçekleşmiş P/L bu gözlemde yok.' if closed else '')+
            ('\nGözlem: '+timestamp(event.get('event_time_ms')) if kind=='OPEN_OBSERVED' else ''))

    return OpeningCard(text,event) if kind=='OPEN_OBSERVED' else text
