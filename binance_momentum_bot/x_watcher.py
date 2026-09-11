"""X API v2 timeline observer; deliberately has no trading dependency."""
import asyncio
import json
import logging
import os
import re
import time
from urllib.parse import urlparse

from research_v5135 import flag

log = logging.getLogger(__name__)
X_WATCHER_ENABLED = flag("X_WATCHER_ENABLED", True)
X_WATCHER_NOTIFY = flag("X_WATCHER_NOTIFY", True)
X_WATCHER_ACCOUNTS = [x.strip().lstrip("@") for x in os.getenv("X_WATCHER_ACCOUNTS", "chartexpt").split(",") if x.strip()]
X_POLL_SECONDS = max(30, int(os.getenv("X_WATCHER_POLL_SECONDS", "60")))
X_WATCH_TIMEOUT_SECONDS = max(60, int(os.getenv("X_WATCH_TIMEOUT_SECONDS", "86400")))


def classify(text, symbols):
    lower = text.lower()
    matches = set()
    for token in re.findall(r"\$([A-Za-z0-9]+)\b|\b([A-Z][A-Z0-9]{1,18})\b", text):
        name = (token[0] or token[1]).upper()
        for candidate in (name, name+"USDT", "1000"+name+"USDT"):
            if candidate in symbols:
                matches.add(candidate)
    symbol = next(iter(matches)) if len(matches)==1 else None
    result = bool(re.search(r"target\s*(?:\d+\s*)?(?:hit|reached)|tp\s*\d*\s*(?:hit|done)|accuracy|\+\s*%?\d+(?:\.\d+)?\s*%|\+\s*%\s*\d+|%\s*\d+.*yapt[ıi]",lower))
    conditional = bool(re.search(r"\b(if|wait|when|once|unless|reclaim|pullback|breakout|above|below)\b|bekle|olursa|market.*(?:alma|girme)|don.t.*market",lower))
    condition = None
    # Only explicit direction + numeric level is executable as a shadow condition.
    level = re.search(r"\b(?:break(?:out)?\s+above|reclaim\s+(?:above\s+)?|above\s+)\$?(\d+(?:\.\d+)?)\b",lower)
    below = re.search(r"\b(?:pullback\s+(?:to\s+)?|below\s+)\$?(\d+(?:\.\d+)?)\b",lower)
    if not result and symbol and (level or below):
        value = float((level or below).group(1))
        all_levels=re.findall(r"\b(?:above|below|reclaim|pullback to)\s+\$?(\d+(?:\.\d+)?)",lower)
        if value>0 and len(set(all_levels))<=1:
            condition = {"operator":"CROSS_ABOVE" if level else "CROSS_BELOW", "price":value}
    category = "RESULT_UPDATE" if result else "WATCH_SETUP" if conditional else "ACTIONABLE_SETUP" if symbol and re.search(r"\b(entry|long|short|buy|sell|setup)\b",lower) else "MARKET_COMMENTARY" if symbol or re.search(r"market|bitcoin|crypto|btc|eth",lower) else "NOISE"
    comments = {
        "RESULT_UPDATE":"Geçmiş sonuç paylaşımı; yeni işlem fırsatı olarak değerlendirilmez.",
        "WATCH_SETUP":"Koşul teyidi bekleniyor (WAIT_CONFIRMATION). " + ("Açık fiyat seviyesi shadow olarak izleniyor." if condition else "Net fiyat koşulu yok; otomatik tetik kurulmadı."),
        "ACTIONABLE_SETUP":"İşlem fikri içeren paylaşım; yalnız takip kaydıdır, bot emri üretmez.",
        "MARKET_COMMENTARY":"Piyasa yorumu; yalnız bağlam ve takip için kaydedildi.",
        "NOISE":"Net piyasa veya kurulum bilgisi saptanmadı.",
    }
    return {"category":category,"symbol":symbol,"condition":condition,"comment_tr":comments[category],"analysis_source":"RULE_BASED_TR_V1"}


def migrate(c):
    c.executescript("""
        CREATE TABLE IF NOT EXISTS x_watcher_accounts(account TEXT PRIMARY KEY, user_id TEXT, since_id TEXT, bootstrapped INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS x_watcher_tweets(
            tweet_id TEXT PRIMARY KEY,account TEXT NOT NULL,created_at TEXT,observed_time_ms INTEGER NOT NULL,
            original_text TEXT NOT NULL,images_json TEXT NOT NULL,url TEXT NOT NULL,analysis_json TEXT NOT NULL,
            delivery TEXT NOT NULL,watch_status TEXT,watch_deadline_ms INTEGER,last_price REAL,
            trigger_time_ms INTEGER,trigger_price REAL,trigger_delivery TEXT);
        CREATE TABLE IF NOT EXISTS x_watcher_deliveries(
            id INTEGER PRIMARY KEY,tweet_id TEXT NOT NULL,kind TEXT NOT NULL,part INTEGER NOT NULL,
            attempted_time_ms INTEGER NOT NULL,delivered INTEGER NOT NULL);
    """)


class XWatcher:
    def __init__(self, connect, market, send, media_send):
        self.connect, self.market, self.send, self.media_send = connect, market, send, media_send
        self.token = os.getenv("X_BEARER_TOKEN", "").strip()
        self.status = "DISABLED" if not X_WATCHER_ENABLED else "MISSING_X_BEARER_TOKEN" if not self.token else "STARTING"

    async def request(self, session, path, params=None):
        async with session.get("https://api.x.com/2/"+path, params=params,
                               headers={"Authorization":"Bearer "+self.token}, timeout=20) as response:
            if response.status != 200:
                raise RuntimeError(f"X API HTTP {response.status}")
            data = await response.json()
            if data.get("errors"):
                raise RuntimeError("X API returned partial/error response")
            return data

    async def poll_account(self, session, account):
        c = self.connect()
        try:
            c.execute("INSERT OR IGNORE INTO x_watcher_accounts(account) VALUES (?)",(account,)); c.commit()
            uid,since,bootstrap = c.execute("SELECT user_id,since_id,bootstrapped FROM x_watcher_accounts WHERE account=?",(account,)).fetchone()
        finally:
            c.close()
        if not uid:
            data = await self.request(session,"users/by/username/"+account)
            uid = data["data"]["id"]
        params = {"max_results":100,"tweet.fields":"created_at,attachments","expansions":"attachments.media_keys","media.fields":"type,url"}
        if since:
            params["since_id"] = since
        collected = []
        while True:
            data = await self.request(session,f"users/{uid}/tweets",params)
            media = {m["media_key"]:m for m in data.get("includes",{}).get("media",[])}
            collected.extend((tweet,media) for tweet in data.get("data",[]))
            token = data.get("meta",{}).get("next_token")
            if not token or not bootstrap:
                break
            params["pagination_token"] = token
        now = int(time.time()*1000)
        c = self.connect()
        try:
            for tweet,media in sorted(collected,key=lambda x:int(x[0]["id"])):
                photos = [media[k].get("url") for k in tweet.get("attachments",{}).get("media_keys",[]) if k in media and media[k].get("type")=="photo"]
                photos = [u for u in photos if u and urlparse(u).scheme=="https" and urlparse(u).hostname=="pbs.twimg.com"][:2]
                analysis = classify(tweet["text"],self.market(None))
                condition = analysis["condition"] if bootstrap else None
                c.execute("""INSERT OR IGNORE INTO x_watcher_tweets(tweet_id,account,created_at,observed_time_ms,original_text,images_json,url,analysis_json,delivery,watch_status,watch_deadline_ms)
                             VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                          (tweet["id"],account,tweet.get("created_at"),now,tweet["text"],json.dumps(photos),f"https://x.com/{account}/status/{tweet['id']}",json.dumps(analysis,ensure_ascii=False),
                           "PENDING" if bootstrap and X_WATCHER_NOTIFY else "SEEN_BOOTSTRAP" if not bootstrap else "MUTED",
                           "WATCH" if condition else None,now+X_WATCH_TIMEOUT_SECONDS*1000 if condition else None))
            newest = str(max(int(t[0]["id"]) for t in collected)) if collected else since
            c.execute("UPDATE x_watcher_accounts SET user_id=?,since_id=?,bootstrapped=1 WHERE account=?",(uid,newest,account))
            c.commit()
        finally:
            c.close()

    async def deliver(self, session, tid, kind, part, callback):
        c = self.connect()
        try:
            if c.execute("SELECT 1 FROM x_watcher_deliveries WHERE tweet_id=? AND kind=? AND part=? AND delivered=1",(tid,kind,part)).fetchone():
                return True
        finally:
            c.close()
        ok = bool(await callback())
        c = self.connect()
        try:
            c.execute("INSERT INTO x_watcher_deliveries(tweet_id,kind,part,attempted_time_ms,delivered) VALUES (?,?,?,?,?)",(tid,kind,part,int(time.time()*1000),int(ok))); c.commit()
        finally:
            c.close()
        return ok

    async def process(self, session):
        c = self.connect()
        try:
            c.row_factory = __import__('sqlite3').Row
            rows = [dict(r) for r in c.execute("SELECT * FROM x_watcher_tweets WHERE delivery='PENDING' OR watch_status='WATCH' OR trigger_delivery='PENDING'")]
        finally:
            c.close()
        for row in rows:
            analysis = json.loads(row["analysis_json"])
            market = self.market(analysis["symbol"]) if analysis["symbol"] else {}
            if row["delivery"]=="PENDING" and X_WATCHER_NOTIFY:
                details = ""
                if market:
                    def val(k):
                        v=market.get(k)
                        return "UNKNOWN" if v is None else f"{v:+.2f}%"
                    details = f"\nFutures: {analysis['symbol']} | Fiyat: {market.get('price') or 'UNKNOWN'} | 5dk: {val('chg5')} | 24s: {val('chg24')}"
                text = f"X SHADOW TAKİP — @{row['account']}\n\n{row['original_text']}\n\n{row['url']}\n{analysis['category']}\n{analysis['comment_tr']}\nYorum: kural tabanlı Türkçe analiz{details}"
                ok = True
                for i,start in enumerate(range(0,len(text),3500)):
                    chunk = text[start:start+3500]
                    ok = await self.deliver(session,row["tweet_id"],"TEXT",i,lambda chunk=chunk:self.send(session,chunk)) and ok
                for i,url in enumerate(json.loads(row["images_json"])):
                    ok = await self.deliver(session,row["tweet_id"],"PHOTO",i,lambda url=url:self.media_send(session,url)) and ok
                if ok:
                    self.update(row["tweet_id"],delivery="DELIVERED")
            if row["watch_status"]=="WATCH":
                now = int(time.time()*1000)
                px = market.get("price")
                prev = row["last_price"]
                cond = analysis["condition"]
                if now >= row["watch_deadline_ms"]:
                    self.update(row["tweet_id"],watch_status="EXPIRED")
                elif px and cond:
                    level = cond["price"]
                    crossed = prev is not None and (prev<level<=px if cond["operator"]=="CROSS_ABOVE" else prev>level>=px)
                    self.update(row["tweet_id"],last_price=px)
                    if crossed:
                        self.update(row["tweet_id"],watch_status="TRIGGERED",trigger_time_ms=now,trigger_price=px,trigger_delivery="PENDING" if X_WATCHER_NOTIFY else "MUTED")
            if row["trigger_delivery"]=="PENDING" and X_WATCHER_NOTIFY:
                text = f"X SHADOW TETİK\n{analysis['symbol']} | {row['trigger_price']}\n{analysis['condition']}\n{row['url']}\nKoşul gözlendi; yalnız shadow takip, işlem emri değildir."
                if await self.deliver(session,row["tweet_id"],"TRIGGER",0,lambda:self.send(session,text)):
                    self.update(row["tweet_id"],trigger_delivery="DELIVERED")

    def update(self, tid, **fields):
        c = self.connect()
        try:
            c.execute("UPDATE x_watcher_tweets SET "+",".join(k+"=?" for k in fields)+" WHERE tweet_id=?",(*fields.values(),tid)); c.commit()
        finally:
            c.close()

    async def run(self, session, stop):
        while not stop.is_set():
            if X_WATCHER_ENABLED and self.token:
                try:
                    errors=[]
                    for account in X_WATCHER_ACCOUNTS:
                        try:
                            await self.poll_account(session,account)
                        except Exception as exc:
                            errors.append(type(exc).__name__)
                    await self.process(session)
                    self.status = "ERROR:"+",".join(errors) if errors else "TRACKING_ONLY_SHADOW"
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.status = "ERROR:"+type(exc).__name__
                    log.warning("X watcher poll failed (%s)",type(exc).__name__)
            await asyncio.sleep(X_POLL_SECONDS)
