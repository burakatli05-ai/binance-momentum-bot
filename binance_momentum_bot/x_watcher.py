"""X API v2 timeline observer; deliberately has no trading dependency."""
import asyncio
import json
import logging
import os
import re
import time
import math
from datetime import datetime
from urllib.parse import urlparse

from research_v5135 import flag

log = logging.getLogger(__name__)
X_WATCHER_ENABLED = flag("X_WATCHER_ENABLED", True)
X_WATCHER_NOTIFY = flag("X_WATCHER_NOTIFY", True)
X_WATCHER_ACCOUNTS = [x.strip().lstrip("@") for x in os.getenv("X_WATCHER_ACCOUNTS", "chartexpt").split(",") if x.strip()]
X_POLL_SECONDS = max(30, int(os.getenv("X_WATCHER_POLL_SECONDS", "60")))
HORIZONS = (300, 900, 1800, 3600, 14400, 86400)


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
    # Descriptive text only: never compile levels into executable predicates.
    condition = text if conditional and not result else None
    bullish = bool(re.search(r"\b(long|buy|bullish|upside)\b", lower))
    bearish = bool(re.search(r"\b(short|sell|bearish|downside)\b", lower))
    direction = "UP" if bullish and not bearish else "DOWN" if bearish and not bullish else "UNKNOWN"
    category = "RESULT_UPDATE" if result else "WATCH_SETUP" if conditional else "PREDICTION" if symbol and re.search(r"\b(entry|long|short|buy|sell|setup)\b",lower) else "MARKET_COMMENTARY" if symbol or re.search(r"market|bitcoin|crypto|btc|eth",lower) else "NOISE"
    comments = {
        "RESULT_UPDATE":"Geçmiş sonuç paylaşımı; yeni işlem fırsatı olarak değerlendirilmez.",
        "WATCH_SETUP":"Koşullu tahmin; yalnız bilgi ve performans ölçümü için kaydedildi.",
        "PREDICTION":"İşlem fikri içeren paylaşım; yalnız takip kaydıdır, bot emri üretmez.",
        "MARKET_COMMENTARY":"Piyasa yorumu; yalnız bağlam ve takip için kaydedildi.",
        "NOISE":"Net piyasa veya kurulum bilgisi saptanmadı.",
    }
    return {"category":category,"symbol":symbol,"condition":condition,"comment_tr":comments[category],"direction":direction,"analysis_source":"RULE_BASED_TR_V2"}


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

    fields = {r[1] for r in c.execute("PRAGMA table_info(x_watcher_tweets)")}
    for name, decl in {"tweet_time_ms":"INTEGER", "symbol":"TEXT", "market_context_json":"TEXT",
                       "tweet_price":"REAL", "price_time_ms":"INTEGER", "price_source":"TEXT",
                       "measurement_status":"TEXT", "measurement_attempt_ms":"INTEGER"}.items():
        if name not in fields:
            c.execute(f"ALTER TABLE x_watcher_tweets ADD COLUMN {name} {decl}")
    c.executescript("""
        CREATE TABLE IF NOT EXISTS x_forward_outcomes(
            tweet_id TEXT NOT NULL, horizon_s INTEGER NOT NULL, target_time_ms INTEGER NOT NULL,
            price_time_ms INTEGER NOT NULL, measured_time_ms INTEGER NOT NULL, price REAL NOT NULL,
            return_pct REAL NOT NULL, directional_return_pct REAL, price_source TEXT NOT NULL,
            PRIMARY KEY(tweet_id,horizon_s));
        UPDATE x_watcher_tweets SET watch_status='RETIRED_INFORMATION_ONLY'
            WHERE watch_status='WATCH';
        UPDATE x_watcher_tweets SET trigger_delivery='CANCELLED_INFORMATION_ONLY'
            WHERE trigger_delivery='PENDING';
    """)

    # Adopt legacy rows without inventing a historical market snapshot.
    for tid, created, original, encoded in c.execute("SELECT tweet_id,created_at,original_text,analysis_json FROM x_watcher_tweets WHERE measurement_status IS NULL").fetchall():
        old_symbol = json.loads(encoded).get("symbol")
        analysis = classify(original, {old_symbol} if old_symbol else set())
        try:
            stamp = datetime.fromisoformat((created or "").replace("Z", "+00:00"))
            tweet_ms = int(stamp.timestamp()*1000) if stamp.tzinfo else None
        except ValueError:
            tweet_ms = None
        c.execute("""UPDATE x_watcher_tweets SET symbol=?,tweet_time_ms=?,analysis_json=?,
            measurement_status=? WHERE tweet_id=?""", (analysis["symbol"],tweet_ms,json.dumps(analysis,ensure_ascii=False),
            "PENDING" if tweet_ms and analysis["symbol"] else "UNMEASURABLE",tid))


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
                try:
                    stamp = datetime.fromisoformat(tweet.get("created_at", "").replace("Z", "+00:00"))
                    tweet_ms = int(stamp.timestamp()*1000) if stamp.tzinfo else None
                except (ValueError, TypeError):
                    tweet_ms = None
                context = {"observed_time_ms":now, "basis":"INGESTION_SNAPSHOT",
                           "market":self.market(analysis["symbol"]) if analysis["symbol"] else {}}

                c.execute("""INSERT OR IGNORE INTO x_watcher_tweets(tweet_id,account,created_at,observed_time_ms,original_text,images_json,url,analysis_json,delivery,tweet_time_ms,symbol,market_context_json,measurement_status)
                             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          (tweet["id"],account,tweet.get("created_at"),now,tweet["text"],json.dumps(photos),f"https://x.com/{account}/status/{tweet['id']}",json.dumps(analysis,ensure_ascii=False),
                           "PENDING" if bootstrap and X_WATCHER_NOTIFY else "SEEN_BOOTSTRAP" if not bootstrap else "MUTED",
                           tweet_ms,analysis["symbol"],json.dumps(context),
                           "PENDING" if tweet_ms and analysis["symbol"] else "UNMEASURABLE"))
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
            rows = [dict(r) for r in c.execute("SELECT * FROM x_watcher_tweets WHERE delivery='PENDING'")]
        finally:
            c.close()
        for row in rows:
            analysis = json.loads(row["analysis_json"])
            market = json.loads(row["market_context_json"] or "{}").get("market", {})
            if row["delivery"]=="PENDING" and X_WATCHER_NOTIFY:
                details = ""
                if market:
                    def val(k):
                        v=market.get(k)
                        return "UNKNOWN" if v is None else f"{v:+.2f}%"
                    details = f"\nGözlem anındaki Futures bağlamı: {analysis['symbol']} | Fiyat: {market.get('price') or 'UNKNOWN'} | 5dk: {val('chg5')} | 24s: {val('chg24')}"
                text = f"X BİLGİ — @{row['account']}\n\n{row['original_text']}\n\n{row['url']}\n{analysis['category']}\n{analysis['comment_tr']}\nYorum: kural tabanlı Türkçe analiz{details}"
                ok = True
                for i,start in enumerate(range(0,len(text),3500)):
                    chunk = text[start:start+3500]
                    ok = await self.deliver(session,row["tweet_id"],"TEXT",i,lambda chunk=chunk:self.send(session,chunk)) and ok
                for i,url in enumerate(json.loads(row["images_json"])):
                    ok = await self.deliver(session,row["tweet_id"],"PHOTO",i,lambda url=url:self.media_send(session,url)) and ok
                if ok:
                    self.update(row["tweet_id"],delivery="DELIVERED")
        await self.measure_outcomes(session)

    async def historical_price(self, session, symbol, target_ms):
        # Minute-open proxy, at most 59.999s before the requested timestamp.
        # Never substitute the ingestion/current price or the candle close.
        minute = target_ms // 60000 * 60000
        async with session.get("https://fapi.binance.com/fapi/v1/klines",
                               params={"symbol":symbol,"interval":"1m","startTime":minute,
                                       "endTime":minute+59999,"limit":1}, timeout=20) as response:
            if response.status != 200:
                raise RuntimeError(f"Historical price HTTP {response.status}")
            rows = await response.json()
        if not isinstance(rows, list) or not rows or int(rows[0][0]) != minute:
            return None
        price = float(rows[0][1])
        if not math.isfinite(price) or price <= 0:
            return None
        return price, minute

    async def measure_outcomes(self, session):
        now = int(time.time()*1000)
        c = self.connect()
        try:
            c.row_factory = __import__('sqlite3').Row
            rows = [dict(r) for r in c.execute("""SELECT * FROM x_watcher_tweets
                WHERE measurement_status IN ('PENDING','WAITING_DATA')
                AND (measurement_attempt_ms IS NULL OR measurement_attempt_ms<=?)
                ORDER BY COALESCE(measurement_attempt_ms,0),observed_time_ms LIMIT 10""", (now-60000,))]
        finally:
            c.close()
        for row in rows:
            self.update(row["tweet_id"], measurement_attempt_ms=now)
            try:
                await self.measure_tweet(session, row, now)
            except Exception as exc:
                self.update(row["tweet_id"], measurement_status="WAITING_DATA")
                log.warning("X outcome unavailable (%s)", type(exc).__name__)

    async def measure_tweet(self, session, row, now):
        tid, start = row["tweet_id"], row["tweet_time_ms"]
        if start > now:
            return
        source = "BINANCE_FUTURES_1M_OPEN_PROXY"
        entry = row["tweet_price"]
        if entry is None:
            sample = await self.historical_price(session, row["symbol"], start)
            if sample is None:
                self.update(tid, measurement_status="WAITING_DATA")
                return
            entry, stamp = sample
            self.update(tid, tweet_price=entry, price_time_ms=stamp, price_source=source)
        c = self.connect()
        try:
            done = {r[0] for r in c.execute("SELECT horizon_s FROM x_forward_outcomes WHERE tweet_id=?", (tid,))}
        finally:
            c.close()
        analysis = json.loads(row["analysis_json"])
        direction = analysis.get("direction", "UNKNOWN")
        for horizon in HORIZONS:
            target = start+horizon*1000
            if horizon in done or target > now:
                continue
            sample = await self.historical_price(session, row["symbol"], target)
            if sample is None:
                continue
            price, stamp = sample
            change = (price/entry-1)*100
            # Conditional posts are not evaluated as if their condition occurred.
            signed = (change if direction == "UP" else -change) if direction != "UNKNOWN" and analysis["category"] == "PREDICTION" else None
            c = self.connect()
            try:
                c.execute("INSERT OR IGNORE INTO x_forward_outcomes VALUES (?,?,?,?,?,?,?,?,?)",
                          (tid,horizon,target,stamp,now,price,change,signed,source))
                c.commit()
            finally:
                c.close()
            done.add(horizon)
        self.update(tid, measurement_status="COMPLETE" if len(done)==len(HORIZONS) else "WAITING_DATA")

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
                    self.status = "ERROR:"+",".join(errors) if errors else "INFORMATION_AND_OUTCOMES_ONLY"
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.status = "ERROR:"+type(exc).__name__
                    log.warning("X watcher poll failed (%s)",type(exc).__name__)
            await asyncio.sleep(X_POLL_SECONDS)
