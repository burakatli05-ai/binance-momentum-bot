import asyncio
import json
import logging
import os
import signal
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from statistics import mean
from typing import Deque, Dict, Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

REST = "https://fapi.binance.com"
WS = "wss://fstream.binance.com/stream"
IST = timezone(timedelta(hours=3))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "5000000"))
RVOL_EARLY = float(os.getenv("RVOL_EARLY", "2.0"))
RVOL_STRONG = float(os.getenv("RVOL_STRONG", "3.0"))
TAKER_BUY_EARLY = float(os.getenv("TAKER_BUY_EARLY", "0.58"))
TAKER_BUY_STRONG = float(os.getenv("TAKER_BUY_STRONG", "0.64"))
EARLY_SCORE = int(os.getenv("EARLY_SCORE", "60"))
STRONG_SCORE = int(os.getenv("STRONG_SCORE", "78"))
EXTREME_SCORE = int(os.getenv("EXTREME_SCORE", "90"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "300"))
OI_MIN_INTERVAL = int(os.getenv("OI_MIN_INTERVAL", "45"))
BOOTSTRAP_CANDLES = int(os.getenv("BOOTSTRAP_CANDLES", "30"))
DB_PATH = os.getenv("DB_PATH", "signals.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("momentum-bot")


@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float
    taker_buy_quote: float
    closed: bool = True


@dataclass
class SymbolState:
    candles: Deque[Candle] = field(default_factory=lambda: deque(maxlen=180))
    live: Optional[Candle] = None
    pct24: float = 0.0
    quote_volume24: float = 0.0
    last_price: float = 0.0
    oi_last: Optional[float] = None
    oi_last_ts: float = 0.0
    oi_baseline: Optional[float] = None
    oi_baseline_ts: float = 0.0
    last_alert_ts: float = 0.0
    last_level: int = 0
    eval_inflight: bool = False
    last_eval_ts: float = 0.0


states: Dict[str, SymbolState] = defaultdict(SymbolState)
symbols = []
stop_event = asyncio.Event()


def pct_change(new: float, old: float) -> float:
    if not old:
        return 0.0
    return (new / old - 1.0) * 100.0


def fmt_money(x: float) -> str:
    if x >= 1_000_000_000:
        return f"{x/1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"{x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"{x/1_000:.1f}K"
    return f"{x:.0f}"


def fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:.4f}"
    if x >= 0.01:
        return f"{x:.6f}"
    return f"{x:.8f}"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            level TEXT NOT NULL,
            score INTEGER NOT NULL,
            price REAL NOT NULL,
            chg1 REAL, chg3 REAL, chg5 REAL, chg15 REAL, chg24 REAL,
            rvol REAL, minute_quote_volume REAL, avg_quote_volume REAL,
            taker_buy_ratio REAL, oi_change REAL, breakout INTEGER
        )
        """
    )
    conn.commit()
    conn.close()


def save_signal(row: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO signals
        (ts,symbol,level,score,price,chg1,chg3,chg5,chg15,chg24,rvol,minute_quote_volume,
         avg_quote_volume,taker_buy_ratio,oi_change,breakout)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(time.time()), row["symbol"], row["level"], row["score"], row["price"],
            row["chg1"], row["chg3"], row["chg5"], row["chg15"], row["chg24"],
            row["rvol"], row["minute_qv"], row["avg_qv"], row["taker_buy"],
            row.get("oi_change"), int(row["breakout"]),
        ),
    )
    conn.commit()
    conn.close()


async def telegram_send(session: aiohttp.ClientSession, text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials missing; alert printed only:\n%s", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    if "🪙 " in text:
        sym = text.split("🪙 ", 1)[1].split("\n", 1)[0].strip()
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {"text": "Binance Futures", "url": "https://www.binance.com/en/futures/" + sym}
            ]]
        }
    async with session.post(url, json=payload, timeout=10) as r:
        if r.status != 200:
            log.error("Telegram error %s: %s", r.status, await r.text())


async def fetch_json(session, path, params=None):
    async with session.get(REST + path, params=params, timeout=12) as r:
        r.raise_for_status()
        return await r.json()


async def load_symbols(session):
    info = await fetch_json(session, "/fapi/v1/exchangeInfo")
    out = []
    for s in info["symbols"]:
        if (
            s.get("quoteAsset") == "USDT"
            and s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"
        ):
            out.append(s["symbol"])
    return sorted(out)


async def bootstrap_symbol(session, sem, symbol):
    async with sem:
        try:
            data = await fetch_json(session, "/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "limit": BOOTSTRAP_CANDLES})
            st = states[symbol]
            st.candles.clear()
            for k in data[:-1]:
                st.candles.append(Candle(
                    open_time=int(k[0]), open=float(k[1]), high=float(k[2]), low=float(k[3]), close=float(k[4]),
                    quote_volume=float(k[7]), taker_buy_quote=float(k[10]), closed=True
                ))
            if data:
                k = data[-1]
                st.live = Candle(
                    open_time=int(k[0]), open=float(k[1]), high=float(k[2]), low=float(k[3]), close=float(k[4]),
                    quote_volume=float(k[7]), taker_buy_quote=float(k[10]), closed=False
                )
                st.last_price = float(k[4])
        except Exception as e:
            log.warning("Bootstrap failed %s: %s", symbol, e)


async def bootstrap_all(session):
    sem = asyncio.Semaphore(8)
    await asyncio.gather(*(bootstrap_symbol(session, sem, s) for s in symbols))


async def get_oi_change(session, symbol: str) -> Optional[float]:
    st = states[symbol]
    now = time.time()
    if now - st.oi_last_ts < OI_MIN_INTERVAL and st.oi_last is not None:
        if st.oi_baseline:
            return pct_change(st.oi_last, st.oi_baseline)
        return None
    try:
        d = await fetch_json(session, "/fapi/v1/openInterest", {"symbol": symbol})
        oi = float(d["openInterest"])
        if st.oi_baseline is None or now - st.oi_baseline_ts > 300:
            st.oi_baseline = st.oi_last if st.oi_last is not None else oi
            st.oi_baseline_ts = now
        st.oi_last = oi
        st.oi_last_ts = now
        if st.oi_baseline:
            return pct_change(oi, st.oi_baseline)
    except Exception as e:
        log.debug("OI failed %s: %s", symbol, e)
    return None


def compute_metrics(symbol: str):
    st = states[symbol]
    live = st.live
    if not live or len(st.candles) < 20 or live.close <= 0:
        return None
    closed = list(st.candles)
    avg_qv = mean(c.quote_volume for c in closed[-20:]) or 1.0
    elapsed = max(1, min(60, (int(time.time() * 1000) - live.open_time) / 1000))
    projected_qv = live.quote_volume * (60.0 / elapsed)
    rvol = projected_qv / avg_qv
    taker = live.taker_buy_quote / live.quote_volume if live.quote_volume > 0 else 0.0

    def ago_change(minutes):
        # Compare current price with close roughly N minutes ago.
        if len(closed) < minutes:
            return 0.0
        return pct_change(live.close, closed[-minutes].close)

    chg1 = pct_change(live.close, live.open)
    chg3 = ago_change(3)
    chg5 = ago_change(5)
    chg15 = ago_change(15)
    prior_high = max(c.high for c in closed[-15:])
    breakout = live.close > prior_high

    return {
        "symbol": symbol, "price": live.close, "chg1": chg1, "chg3": chg3, "chg5": chg5,
        "chg15": chg15, "chg24": st.pct24, "rvol": rvol, "minute_qv": live.quote_volume,
        "avg_qv": avg_qv, "projected_qv": projected_qv, "taker_buy": taker,
        "breakout": breakout, "qv24": st.quote_volume24,
    }


def score_metrics(m: dict) -> int:
    score = 0
    # Relative volume: max 30
    r = m["rvol"]
    score += 30 if r >= 6 else 25 if r >= 4 else 20 if r >= 3 else 12 if r >= 2 else 5 if r >= 1.5 else 0
    # 1m acceleration: max 20
    c1 = m["chg1"]
    score += 20 if c1 >= 2.5 else 16 if c1 >= 1.5 else 12 if c1 >= 0.8 else 7 if c1 >= 0.4 else 0
    # 5/15m trend quality: max 15
    if m["chg5"] > 0 and m["chg15"] > 0:
        score += 8
    if m["chg5"] >= 2.0:
        score += 4
    if m["chg15"] >= 3.0:
        score += 3
    # Aggressive buyers: max 20
    t = m["taker_buy"]
    score += 20 if t >= 0.72 else 16 if t >= 0.66 else 12 if t >= 0.60 else 6 if t >= 0.55 else 0
    # Breakout: 10
    if m["breakout"]:
        score += 10
    # Real liquidity / absolute activity: 5
    if m["projected_qv"] >= 1_000_000:
        score += 5
    elif m["projected_qv"] >= 300_000:
        score += 3
    return min(score, 100)


def qualifies(m, score):
    # Early candidate must have BOTH activity and positive price action.
    return (
        m["qv24"] >= MIN_24H_QUOTE_VOLUME
        and m["rvol"] >= RVOL_EARLY
        and m["chg1"] >= 0.35
        and m["taker_buy"] >= TAKER_BUY_EARLY
        and score >= EARLY_SCORE
    )


def classify(score, m):
    if score >= EXTREME_SCORE and m["rvol"] >= RVOL_STRONG and m["taker_buy"] >= TAKER_BUY_STRONG:
        return 3, "🔴 AŞIRI MOMENTUM"
    if score >= STRONG_SCORE:
        return 2, "🟠 GÜÇLÜ MOMENTUM"
    return 1, "🟡 ERKEN MOMENTUM"


def build_message(m):
    oi_line = "⚪ Open Interest: ölçüm birikiyor" if m.get("oi_change") is None else f"📈 Open Interest (≈5 dk): {m['oi_change']:+.2f}%"
    breakout_line = "🚀 15 dk tepe KIRILDI" if m["breakout"] else "🎯 15 dk tepe henüz kırılmadı"
    return (
        f"{m['level']}\n\n"
        f"🪙 {m['symbol']}\n"
        f"💰 Fiyat: {fmt_price(m['price'])}\n\n"
        f"⚡ 1 dk: {m['chg1']:+.2f}%\n"
        f"📈 3 dk: {m['chg3']:+.2f}%\n"
        f"🔥 5 dk: {m['chg5']:+.2f}%\n"
        f"📈 15 dk: {m['chg15']:+.2f}%\n"
        f"🌐 24 saat: {m['chg24']:+.2f}%\n\n"
        f"💥 Hacim patlaması: {m['rvol']:.1f}x\n"
        f"💵 Bu dakika hacim: {fmt_money(m['minute_qv'])} USDT\n"
        f"📊 Ortalama 1 dk hacim: {fmt_money(m['avg_qv'])} USDT\n"
        f"💵 24s hacim: {fmt_money(m['qv24'])} USDT\n\n"
        f"🟢 Taker Buy: %{m['taker_buy']*100:.1f}\n"
        f"{oi_line}\n"
        f"{breakout_line}\n\n"
        f"⭐ Momentum Skoru: {m['score']}/100\n"
        f"⏰ {datetime.now(IST).strftime('%H:%M:%S')}"
    )


async def evaluate(session, symbol):
    st = states[symbol]
    now = time.time()
    if st.eval_inflight or now - st.last_eval_ts < 1.5:
        return
    st.eval_inflight = True
    st.last_eval_ts = now
    try:
        m = compute_metrics(symbol)
        if not m:
            return
        score = score_metrics(m)
        if not qualifies(m, score):
            return

        # OI only for genuine candidates; then give a small confirmation bonus/penalty.
        oi_change = await get_oi_change(session, symbol)
        m["oi_change"] = oi_change
        if oi_change is not None:
            if oi_change >= 1.0:
                score = min(100, score + 5)
            elif oi_change <= -1.5:
                score = max(0, score - 5)
        m["score"] = score

        level_num, level_name = classify(score, m)
        m["level"] = level_name
        now = time.time()
        # Allow immediate re-alert only if the signal escalates to a higher level.
        if now - st.last_alert_ts < COOLDOWN_SECONDS and level_num <= st.last_level:
            return

        st.last_alert_ts = now
        st.last_level = level_num
        text = build_message(m)
        log.info("SIGNAL %s score=%d rvol=%.2f taker=%.2f", symbol, score, m["rvol"], m["taker_buy"])
        save_signal(m)
        await telegram_send(session, text)
    finally:
        st.eval_inflight = False


async def ticker_ws(session):
    url = WS + "?streams=!ticker@arr"
    while not stop_event.is_set():
        try:
            async with session.ws_connect(url, heartbeat=30, receive_timeout=60) as ws:
                log.info("Ticker stream connected")
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    payload = json.loads(msg.data).get("data", [])
                    for t in payload:
                        sym = t.get("s")
                        if sym in states:
                            st = states[sym]
                            st.last_price = float(t.get("c", 0) or 0)
                            st.pct24 = float(t.get("P", 0) or 0)
                            st.quote_volume24 = float(t.get("q", 0) or 0)
        except Exception as e:
            log.warning("Ticker WS reconnecting: %s", e)
            await asyncio.sleep(3)


async def kline_ws(session):
    # One 1m stream per symbol. Binance combined streams support up to 1024 streams/connection.
    streams = "/".join(f"{s.lower()}@kline_1m" for s in symbols)
    url = WS + "?streams=" + streams
    while not stop_event.is_set():
        try:
            async with session.ws_connect(url, heartbeat=30, receive_timeout=60, max_msg_size=2**22) as ws:
                log.info("Kline stream connected for %d symbols", len(symbols))
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    data = json.loads(msg.data).get("data", {})
                    k = data.get("k")
                    if not k:
                        continue
                    symbol = k["s"]
                    if symbol not in states:
                        continue
                    candle = Candle(
                        open_time=int(k["t"]), open=float(k["o"]), high=float(k["h"]), low=float(k["l"]),
                        close=float(k["c"]), quote_volume=float(k["q"]), taker_buy_quote=float(k["Q"]),
                        closed=bool(k["x"]),
                    )
                    st = states[symbol]
                    st.live = candle
                    st.last_price = candle.close
                    if candle.closed:
                        if not st.candles or st.candles[-1].open_time != candle.open_time:
                            st.candles.append(candle)
                        else:
                            st.candles[-1] = candle
                    # Evaluate live kline updates; cooldown suppresses spam.
                    if st.quote_volume24 >= MIN_24H_QUOTE_VOLUME:
                        asyncio.create_task(evaluate(session, symbol))
        except Exception as e:
            log.warning("Kline WS reconnecting: %s", e)
            await asyncio.sleep(3)


async def reset_levels_loop():
    while not stop_event.is_set():
        now = time.time()
        for st in states.values():
            if st.last_alert_ts and now - st.last_alert_ts > COOLDOWN_SECONDS * 2:
                st.last_level = 0
        await asyncio.sleep(30)


async def main():
    global symbols
    init_db()
    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        symbols = await load_symbols(session)
        for s in symbols:
            states[s]  # initialize
        log.info("Tracking %d active USDT perpetual contracts", len(symbols))
        log.info("Bootstrapping %d one-minute candles per symbol...", BOOTSTRAP_CANDLES)
        await bootstrap_all(session)
        log.info("Bootstrap complete")
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            await telegram_send(session, f"✅ Momentum Scanner başladı\n\n🪙 İzlenen kontrat: {len(symbols)}\n💵 Min 24s hacim: {fmt_money(MIN_24H_QUOTE_VOLUME)} USDT\n⭐ Erken sinyal skoru: {EARLY_SCORE}+")
        await asyncio.gather(ticker_ws(session), kline_ws(session), reset_levels_loop())


def request_stop(*_):
    stop_event.set()


if __name__ == "__main__":
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, request_stop)
        except Exception:
            pass
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
