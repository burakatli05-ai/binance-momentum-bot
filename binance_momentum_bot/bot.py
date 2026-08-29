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
from typing import Deque, Dict, Optional, List, Tuple, Set

import aiohttp
from dotenv import load_dotenv

load_dotenv()

REST = "https://fapi.binance.com"
WS_MARKET = "wss://fstream.binance.com/market/stream"
WS_PUBLIC = "wss://fstream.binance.com/public/stream"
IST = timezone(timedelta(hours=3))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "5000000"))
EARLY_SCORE = int(os.getenv("EARLY_SCORE", "58"))
STRONG_SCORE = int(os.getenv("STRONG_SCORE", "74"))
EXTREME_SCORE = int(os.getenv("EXTREME_SCORE", "88"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "1200"))
CONFIRM_INTERVAL_SECONDS = int(os.getenv("CONFIRM_INTERVAL_SECONDS", "15"))
CONFIRM_REQUIRED = int(os.getenv("CONFIRM_REQUIRED", "4"))
CANDIDATE_TTL_SECONDS = int(os.getenv("CANDIDATE_TTL_SECONDS", "120"))
CONFIRM_MIN_SCORE = int(os.getenv("CONFIRM_MIN_SCORE", "64"))
ENTRY_MIN_SCORE = int(os.getenv("ENTRY_MIN_SCORE", "78"))
BOOTSTRAP_CANDLES = int(os.getenv("BOOTSTRAP_CANDLES", "30"))
AGGTRADE_CHUNK = int(os.getenv("AGGTRADE_CHUNK", "80"))
EVAL_MIN_INTERVAL = float(os.getenv("EVAL_MIN_INTERVAL", "1.0"))

# Gainers radar. Baseline is built silently on startup; alerts only fire on later entries/moves.
GAINERS_TOP_N = int(os.getenv("GAINERS_TOP_N", "50"))
GAINERS_POLL_SECONDS = int(os.getenv("GAINERS_POLL_SECONDS", "30"))
GAINERS_RAPID_WINDOW_SECONDS = int(os.getenv("GAINERS_RAPID_WINDOW_SECONDS", "600"))
GAINERS_RAPID_MIN_POSITIONS = int(os.getenv("GAINERS_RAPID_MIN_POSITIONS", "25"))
GAINERS_RAPID_MAX_RANK = int(os.getenv("GAINERS_RAPID_MAX_RANK", "100"))
GAINERS_ALERT_COOLDOWN_SECONDS = int(os.getenv("GAINERS_ALERT_COOLDOWN_SECONDS", "1800"))
GAINERS_REENTRY_MIN_OUT_SECONDS = int(os.getenv("GAINERS_REENTRY_MIN_OUT_SECONDS", "300"))

# Early-momentum gates. Defaults are intentionally sensitive; use /top to inspect near-misses.
MIN_CHG_10S = float(os.getenv("MIN_CHG_10S", "0.10"))
MIN_CHG_30S = float(os.getenv("MIN_CHG_30S", "0.22"))
MIN_BUY_RATIO_30S = float(os.getenv("MIN_BUY_RATIO_30S", "0.57"))
MIN_FLOW_X_10S = float(os.getenv("MIN_FLOW_X_10S", "1.6"))
MIN_FLOW_X_30S = float(os.getenv("MIN_FLOW_X_30S", "1.4"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.45"))

DB_PATH = os.getenv("DB_PATH", "signals.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("momentum-v5")


@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float
    taker_buy_quote: float


@dataclass
class TradeSample:
    ts_ms: int
    price: float
    quote: float
    aggressive_buy: bool


@dataclass
class SymbolState:
    candles: Deque[Candle] = field(default_factory=lambda: deque(maxlen=180))
    trades: Deque[TradeSample] = field(default_factory=lambda: deque(maxlen=6000))
    short_liqs: Deque[Tuple[int, float]] = field(default_factory=lambda: deque(maxlen=1000))
    long_liqs: Deque[Tuple[int, float]] = field(default_factory=lambda: deque(maxlen=1000))
    pct24: float = 0.0
    quote_volume24: float = 0.0
    last_price: float = 0.0
    bid_price: float = 0.0
    ask_price: float = 0.0
    bid_qty: float = 0.0
    ask_qty: float = 0.0
    last_alert_ts: float = 0.0
    last_alert_price: float = 0.0
    last_level: int = 0
    last_eval_ts: float = 0.0
    eval_inflight: bool = False
    agg_events: int = 0
    minute_open_time: int = 0
    minute_open: float = 0.0
    minute_high: float = 0.0
    minute_low: float = 0.0
    minute_close: float = 0.0
    minute_quote: float = 0.0
    minute_buy_quote: float = 0.0
    candidate_since: float = 0.0
    candidate_last_check: float = 0.0
    candidate_checks: int = 0
    candidate_passes: int = 0
    candidate_prices: Deque[float] = field(default_factory=lambda: deque(maxlen=4))
    candidate_scores: Deque[int] = field(default_factory=lambda: deque(maxlen=4))
    buy_signal_ts: float = 0.0


@dataclass
class PendingOutcome:
    signal_id: int
    symbol: str
    entry_price: float
    created_ts: float
    mfe: float = 0.0
    mae: float = 0.0
    completed: set = field(default_factory=set)


states: Dict[str, SymbolState] = defaultdict(SymbolState)
symbols: List[str] = []
stop_event = asyncio.Event()
pending_outcomes: List[PendingOutcome] = []
stream_health = {
    "ticker": 0.0,
    "book": 0.0,
    "liq": 0.0,
    "agg": 0.0,
}
trade_event_count = 0
telegram_offset = 0

# Gainers state
gainers_initialized = False
gainers_current_top: Set[str] = set()
gainers_prev_rank: Dict[str, int] = {}
gainers_rank_history: Dict[str, Deque[Tuple[float, int, float]]] = defaultdict(lambda: deque(maxlen=80))
gainers_last_entry_alert: Dict[str, float] = defaultdict(float)
gainers_last_rapid_alert: Dict[str, float] = defaultdict(float)
gainers_left_top_at: Dict[str, float] = defaultdict(float)


def pct_change(new: float, old: float) -> float:
    return ((new / old) - 1.0) * 100.0 if old else 0.0


def fmt_money(x: float) -> str:
    x = float(x or 0)
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


def now_ms() -> int:
    return int(time.time() * 1000)


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signals_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            level TEXT NOT NULL,
            score INTEGER NOT NULL,
            price REAL NOT NULL,
            chg10 REAL, chg30 REAL, chg60 REAL, chg5 REAL, chg15 REAL, chg24 REAL,
            flow10 REAL, flow30 REAL, flow60 REAL,
            buy10 REAL, buy30 REAL, buy60 REAL,
            spread REAL, book_imbalance REAL,
            short_liq REAL, long_liq REAL,
            oi5 REAL, breakout INTEGER, extended INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            signal_id INTEGER NOT NULL,
            horizon_s INTEGER NOT NULL,
            return_pct REAL,
            mfe_pct REAL,
            mae_pct REAL,
            ts INTEGER NOT NULL,
            PRIMARY KEY(signal_id, horizon_s)
        )
        """
    )
    conn.commit()
    conn.close()


def save_signal(m: dict) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        """
        INSERT INTO signals_v2
        (ts,symbol,level,score,price,chg10,chg30,chg60,chg5,chg15,chg24,
         flow10,flow30,flow60,buy10,buy30,buy60,spread,book_imbalance,
         short_liq,long_liq,oi5,breakout,extended)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(time.time()), m["symbol"], m["level"], m["score"], m["price"],
            m["chg10"], m["chg30"], m["chg60"], m["chg5"], m["chg15"], m["chg24"],
            m["flow10"], m["flow30"], m["flow60"], m["buy10"], m["buy30"], m["buy60"],
            m["spread"], m["book_imbalance"], m["short_liq"], m["long_liq"],
            m.get("oi5"), int(m["breakout"]), int(m["extended"]),
        ),
    )
    signal_id = cur.lastrowid
    conn.commit()
    conn.close()
    return signal_id


def save_outcome(signal_id: int, horizon_s: int, ret: float, mfe: float, mae: float):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO signal_outcomes(signal_id,horizon_s,return_pct,mfe_pct,mae_pct,ts) VALUES (?,?,?,?,?,?)",
        (signal_id, horizon_s, ret, mfe, mae, int(time.time())),
    )
    conn.commit()
    conn.close()


async def telegram_send(session: aiohttp.ClientSession, text: str, symbol: Optional[str] = None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials missing; alert printed only:\n%s", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    if symbol:
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {"text": "Binance Futures", "url": "https://www.binance.com/en/futures/" + symbol}
            ]]
        }
    try:
        async with session.post(url, json=payload, timeout=10) as r:
            if r.status != 200:
                log.error("Telegram error %s: %s", r.status, await r.text())
    except Exception as e:
        log.warning("Telegram send failed: %s", e)


async def fetch_json(session, path, params=None):
    async with session.get(REST + path, params=params, timeout=15) as r:
        r.raise_for_status()
        return await r.json()


async def load_symbols(session):
    info = await fetch_json(session, "/fapi/v1/exchangeInfo")
    out = []
    for s in info.get("symbols", []):
        if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            out.append(s["symbol"])
    return sorted(out)


async def bootstrap_tickers(session):
    data = await fetch_json(session, "/fapi/v1/ticker/24hr")
    allowed = set(symbols)
    for t in data:
        sym = t.get("symbol")
        if sym in allowed:
            st = states[sym]
            st.last_price = float(t.get("lastPrice", 0) or 0)
            st.pct24 = float(t.get("priceChangePercent", 0) or 0)
            st.quote_volume24 = float(t.get("quoteVolume", 0) or 0)


async def bootstrap_symbol(session, sem, symbol):
    async with sem:
        try:
            data = await fetch_json(session, "/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "limit": BOOTSTRAP_CANDLES})
            st = states[symbol]
            st.candles.clear()
            for k in data[:-1]:
                st.candles.append(Candle(
                    open_time=int(k[0]), open=float(k[1]), high=float(k[2]), low=float(k[3]), close=float(k[4]),
                    quote_volume=float(k[7]), taker_buy_quote=float(k[10])
                ))
        except Exception as e:
            log.debug("Bootstrap failed %s: %s", symbol, e)


async def bootstrap_all(session):
    sem = asyncio.Semaphore(10)
    await asyncio.gather(*(bootstrap_symbol(session, sem, s) for s in symbols))



def update_minute_candle(st: SymbolState, ts_ms: int, price: float, quote: float, aggressive_buy: bool):
    bucket = (ts_ms // 60_000) * 60_000
    if st.minute_open_time == 0:
        st.minute_open_time = bucket
        st.minute_open = st.minute_high = st.minute_low = st.minute_close = price
        st.minute_quote = quote
        st.minute_buy_quote = quote if aggressive_buy else 0.0
        return
    if bucket > st.minute_open_time:
        st.candles.append(Candle(
            open_time=st.minute_open_time,
            open=st.minute_open,
            high=st.minute_high,
            low=st.minute_low,
            close=st.minute_close,
            quote_volume=st.minute_quote,
            taker_buy_quote=st.minute_buy_quote,
        ))
        st.minute_open_time = bucket
        st.minute_open = st.minute_high = st.minute_low = st.minute_close = price
        st.minute_quote = quote
        st.minute_buy_quote = quote if aggressive_buy else 0.0
        return
    st.minute_close = price
    st.minute_high = max(st.minute_high, price)
    st.minute_low = min(st.minute_low, price)
    st.minute_quote += quote
    if aggressive_buy:
        st.minute_buy_quote += quote

def prune_deque_by_ts(dq, cutoff_ms, attr_index=None):
    while dq:
        ts = dq[0].ts_ms if hasattr(dq[0], "ts_ms") else dq[0][0]
        if ts >= cutoff_ms:
            break
        dq.popleft()


def trades_window(st: SymbolState, seconds: int):
    cutoff = now_ms() - seconds * 1000
    arr = [t for t in st.trades if t.ts_ms >= cutoff]
    if not arr:
        return 0.0, 0.0, 0.0, 0.0
    qv = sum(t.quote for t in arr)
    buy_qv = sum(t.quote for t in arr if t.aggressive_buy)
    buy_ratio = buy_qv / qv if qv else 0.0
    chg = pct_change(arr[-1].price, arr[0].price) if len(arr) >= 2 else 0.0
    return qv, buy_ratio, chg, float(len(arr))


def liq_window(st: SymbolState, seconds=60):
    cutoff = now_ms() - seconds * 1000
    prune_deque_by_ts(st.short_liqs, cutoff)
    prune_deque_by_ts(st.long_liqs, cutoff)
    return sum(v for _, v in st.short_liqs), sum(v for _, v in st.long_liqs)


def synthetic_trend(st: SymbolState):
    """Use closed 1m bootstrap candles plus live trade price. Good enough for 5/15m context."""
    price = st.last_price
    c = list(st.candles)
    if not price or len(c) < 15:
        return 0.0, 0.0, False, 0.0
    chg5 = pct_change(price, c[-5].close)
    chg15 = pct_change(price, c[-15].close)
    prior_high = max(x.high for x in c[-15:])
    breakout = price > prior_high
    avg_qv = mean(x.quote_volume for x in c[-20:]) if len(c) >= 20 else mean(x.quote_volume for x in c)
    return chg5, chg15, breakout, max(avg_qv, 1.0)


def compute_metrics(symbol: str):
    st = states[symbol]
    if st.quote_volume24 < MIN_24H_QUOTE_VOLUME or not st.last_price:
        return None
    if len(st.trades) < 2:
        return None

    q10, buy10, chg10, n10 = trades_window(st, 10)
    q30, buy30, chg30, n30 = trades_window(st, 30)
    q60, buy60, chg60, n60 = trades_window(st, 60)
    chg5, chg15, breakout, avg1m = synthetic_trend(st)

    expected10 = avg1m / 6.0
    expected30 = avg1m / 2.0
    expected60 = avg1m
    flow10 = q10 / expected10 if expected10 else 0.0
    flow30 = q30 / expected30 if expected30 else 0.0
    flow60 = q60 / expected60 if expected60 else 0.0

    spread = 0.0
    book_imbalance = 0.5
    if st.bid_price > 0 and st.ask_price > 0:
        mid = (st.bid_price + st.ask_price) / 2.0
        spread = ((st.ask_price - st.bid_price) / mid) * 100.0 if mid else 0.0
        bid_notional = st.bid_price * st.bid_qty
        ask_notional = st.ask_price * st.ask_qty
        denom = bid_notional + ask_notional
        book_imbalance = bid_notional / denom if denom else 0.5

    short_liq, long_liq = liq_window(st, 60)
    btc30 = trades_window(states["BTCUSDT"], 30)[2] if "BTCUSDT" in states else 0.0
    rel30 = chg30 - btc30
    extended = chg15 >= 8.0 or chg5 >= 5.0

    return {
        "symbol": symbol, "price": st.last_price, "chg10": chg10, "chg30": chg30, "chg60": chg60,
        "chg5": chg5, "chg15": chg15, "chg24": st.pct24,
        "q10": q10, "q30": q30, "q60": q60,
        "buy10": buy10, "buy30": buy30, "buy60": buy60,
        "flow10": flow10, "flow30": flow30, "flow60": flow60,
        "trades10": n10, "trades30": n30,
        "avg1m": avg1m, "qv24": st.quote_volume24,
        "spread": spread, "book_imbalance": book_imbalance,
        "short_liq": short_liq, "long_liq": long_liq,
        "btc30": btc30, "rel30": rel30,
        "breakout": breakout, "extended": extended,
    }


def score_metrics(m: dict) -> int:
    score = 0

    # 1) Money-flow acceleration (max 28)
    f = max(m["flow10"], m["flow30"])
    score += 28 if f >= 6 else 24 if f >= 4 else 19 if f >= 2.5 else 14 if f >= 1.7 else 8 if f >= 1.2 else 0

    # 2) Price acceleration — emphasis on the last 10-30 seconds (max 27)
    c10, c30 = m["chg10"], m["chg30"]
    if c10 >= 0.70 or c30 >= 1.20:
        score += 27
    elif c10 >= 0.40 or c30 >= 0.70:
        score += 22
    elif c10 >= 0.22 or c30 >= 0.40:
        score += 16
    elif c10 >= 0.10 or c30 >= 0.22:
        score += 10

    # 3) Aggressive buyer dominance. Real outcomes showed that extreme 85-95%
    # taker-buy can be late-stage FOMO/absorption, so the sweet spot is rewarded most.
    b = m["buy30"]
    if 0.64 <= b <= 0.82:
        score += 18
    elif 0.60 <= b < 0.64 or 0.82 < b <= 0.88:
        score += 12
    elif 0.56 <= b < 0.60:
        score += 7
    elif b > 0.88:
        score += 6

    # 4) Microstructure / order book (max 8)
    bi = m["book_imbalance"]
    score += 8 if bi >= 0.68 else 5 if bi >= 0.60 else 2 if bi >= 0.55 else 0

    # 5) Relative strength against BTC (max 7)
    r = m["rel30"]
    score += 7 if r >= 0.70 else 5 if r >= 0.40 else 3 if r >= 0.20 else 0

    # 6) Breakout and short squeeze confirmation (max 10)
    if m["breakout"]:
        score += 5
    if m["short_liq"] >= 250_000:
        score += 5
    elif m["short_liq"] >= 50_000:
        score += 3

    # Avoid chasing a move that is already stretched unless it is still accelerating hard.
    if m["extended"] and m["chg30"] < 0.70:
        score -= 10
    if m["spread"] > 0.25:
        score -= 5
    # Saturated buying without proportional price progress often marked exhaustion.
    if m["buy30"] >= 0.86 and m["chg30"] < 0.55:
        score -= 10
    if m["flow30"] >= 12 and m["chg30"] < 0.45:
        score -= 7
    # Very high raw scores were not the best cohort in the first live sample.
    # Keep momentum visible, but avoid interpreting raw intensity as entry quality.
    return max(0, min(100, score))


def qualifies(m: dict, score: int) -> bool:
    fast_price = m["chg10"] >= MIN_CHG_10S or m["chg30"] >= MIN_CHG_30S
    fast_flow = m["flow10"] >= MIN_FLOW_X_10S or m["flow30"] >= MIN_FLOW_X_30S
    enough_trades = m["trades10"] >= 2 or m["trades30"] >= 4
    return (
        m["qv24"] >= MIN_24H_QUOTE_VOLUME
        and fast_price
        and fast_flow
        and m["buy30"] >= MIN_BUY_RATIO_30S
        and m["spread"] <= MAX_SPREAD_PCT
        and enough_trades
        and score >= EARLY_SCORE
    )


def classify(score: int, m: dict):
    if score >= EXTREME_SCORE and m["chg30"] >= 0.70:
        return 3, "🔴 ÇOK GÜÇLÜ YÜKSELİŞ"
    if score >= STRONG_SCORE:
        return 2, "🟠 YÜKSELİŞ HIZLANIYOR"
    return 1, "🟡 YÜKSELİŞ BAŞLIYOR"


def reset_candidate(st: SymbolState):
    st.candidate_since = 0.0
    st.candidate_last_check = 0.0
    st.candidate_checks = 0
    st.candidate_passes = 0
    st.candidate_prices.clear()
    st.candidate_scores.clear()


def continuity_pass(m: dict, score: int) -> bool:
    # Require persistence, but reject likely late-stage buyer saturation.
    absorption = (m["buy30"] >= 0.86 and m["chg30"] < 0.55) or (m["flow30"] >= 12 and m["chg30"] < 0.45)
    return (
        score >= CONFIRM_MIN_SCORE
        and m["chg30"] >= 0.12
        and m["chg60"] >= 0.30
        and m["flow30"] >= 1.5
        and 0.58 <= m["buy30"] <= 0.92
        and m["spread"] <= min(MAX_SPREAD_PCT, 0.30)
        and not m["extended"]
        and not absorption
    )


def rise_probability(m: dict, score: int, st: SymbolState) -> int:
    """Empirical heuristic for chance of a meaningful post-signal rise, not a probability model."""
    r = 50
    # First live sample: 77-82 raw momentum cohort was strongest; >82 was not monotonic.
    if 77 <= score <= 84: r += 15
    elif 72 <= score < 77: r += 8
    elif score > 84: r += 5
    if 0.64 <= m["buy30"] <= 0.78: r += 12
    elif 0.78 < m["buy30"] <= 0.84: r += 5
    elif m["buy30"] > 0.88: r -= 10
    if 0.45 <= m["chg30"] <= 1.20: r += 8
    elif m["chg30"] > 1.6: r -= 5
    if 0.8 <= m["chg60"] <= 2.0: r += 8
    if 5 <= m["flow30"] <= 15: r += 7
    elif m["flow30"] > 20: r -= 4
    if 0.55 <= m["book_imbalance"] <= 0.82: r += 5
    elif m["book_imbalance"] > 0.90: r -= 3
    prices = list(st.candidate_prices)
    if len(prices) >= 4 and prices[-1] > prices[-2] > prices[-3]: r += 8
    if m["extended"]: r -= 15
    return max(0, min(100, r))
def entry_quality(m: dict, score: int, st: SymbolState) -> int:
    # Entry quality is deliberately separate from momentum intensity.
    q = 48
    prices = list(st.candidate_prices)
    if len(prices) >= 4:
        steps = [prices[i] / prices[i-1] - 1 for i in range(1, len(prices))]
        if all(x >= -0.0005 for x in steps[-3:]) and sum(x > 0 for x in steps[-3:]) >= 2:
            q += 18
        if prices[-1] < max(prices[:-1]) * 0.995:
            q -= 12
    if 0.64 <= m["buy30"] <= 0.80: q += 12
    elif 0.80 < m["buy30"] <= 0.85: q += 5
    elif m["buy30"] >= 0.88: q -= 12
    if 5.0 <= m["flow30"] <= 15.0: q += 8
    elif 2.0 <= m["flow30"] < 5.0: q += 4
    elif m["flow30"] > 20.0: q -= 6
    if 0.40 <= m["chg30"] <= 1.20: q += 8
    elif m["chg30"] > 1.8: q -= 10
    if 0.55 <= m["book_imbalance"] <= 0.82: q += 5
    elif m["book_imbalance"] > 0.92: q -= 4
    if m["rel30"] >= 0.20: q += 3
    if m["breakout"]: q += 3
    if m.get("oi5") is not None:
        if 0.0 <= m["oi5"] <= 0.8: q += 3
        elif m["oi5"] <= -1.0: q -= 5
        elif m["oi5"] > 2.0: q -= 3
    if m["extended"] or m["chg5"] > 4.0: q -= 15
    return max(0, min(100, q))


async def get_oi_5m(session, symbol: str) -> Optional[float]:
    try:
        d = await fetch_json(session, "/futures/data/openInterestHist", {"symbol": symbol, "period": "5m", "limit": 2})
        if isinstance(d, list) and len(d) >= 2:
            old = float(d[-2].get("sumOpenInterest", 0) or 0)
            new = float(d[-1].get("sumOpenInterest", 0) or 0)
            return pct_change(new, old)
    except Exception as e:
        log.debug("OI history failed %s: %s", symbol, e)
    return None


def build_message(m: dict):
    oi_line = "⚪ OI 5 dk: veri yok" if m.get("oi5") is None else f"📈 OI 5 dk: {m['oi5']:+.2f}%"
    breakout_line = "🚀 15 dk tepe üstünde" if m["breakout"] else "🎯 15 dk tepe henüz kırılmadı"
    return (
        "🟢 AL ADAYI — SÜREKLİLİK TEYİTLİ\n\n"
        f"🪙 {m['symbol']}\n"
        f"💰 Fiyat: {fmt_price(m['price'])}\n\n"
        f"⚡ 30 sn: {m['chg30']:+.2f}%\n"
        f"🔥 60 sn: {m['chg60']:+.2f}%\n"
        f"📈 5 dk: {m['chg5']:+.2f}%\n\n"
        f"💥 Hacim akışı 30 sn: {m['flow30']:.1f}x\n"
        f"🟢 Agresif alış: %{m['buy30']*100:.1f}\n"
        f"📚 Bid baskısı: %{m['book_imbalance']*100:.1f}\n"
        f"₿ BTC'ye göre güç: {m['rel30']:+.2f}%\n"
        f"{oi_line}\n"
        f"{breakout_line}\n\n"
        f"✅ {m['confirm_passes']}/{CONFIRM_REQUIRED} süreklilik kontrolü geçti\n"
        f"📈 Yükseliş potansiyeli: {m['rise_score']}/100\n"
        f"⚡ Momentum yoğunluğu: {m['score']}/100\n"
        f"🎯 Giriş kalitesi: {m['entry_quality']}/100\n\n"
        "Not: Kural tabanlı momentum uyarısıdır; kâr garantisi veya otomatik emir değildir.\n"
        f"⏰ {datetime.now(IST).strftime('%H:%M:%S')}"
    )


async def evaluate(session, symbol: str):
    st = states[symbol]
    now = time.time()
    if st.eval_inflight or now - st.last_eval_ts < EVAL_MIN_INTERVAL:
        return
    st.eval_inflight = True
    st.last_eval_ts = now
    try:
        m = compute_metrics(symbol)
        if not m:
            return
        score = score_metrics(m)

        # Aday oluşumu sessizdir: Telegram bildirimi gönderilmez.
        if st.candidate_since == 0.0:
            if not qualifies(m, score):
                return
            st.candidate_since = now
            st.candidate_last_check = now
            st.candidate_checks = 1
            st.candidate_passes = 1 if continuity_pass(m, score) else 0
            st.candidate_prices.append(m["price"])
            st.candidate_scores.append(score)
            log.info("CANDIDATE %s score=%d", symbol, score)
            return

        # Zaman aşımı veya belirgin bozulma: adayı sessizce bırak.
        if now - st.candidate_since > CANDIDATE_TTL_SECONDS:
            reset_candidate(st)
            return
        if m["chg30"] < -0.20 or m["buy30"] < 0.50 or m["flow30"] < 0.8:
            reset_candidate(st)
            return

        # Sadece belirlenen aralıkta süreklilik kontrolü yap.
        if now - st.candidate_last_check < CONFIRM_INTERVAL_SECONDS:
            return
        st.candidate_last_check = now
        st.candidate_checks += 1
        st.candidate_prices.append(m["price"])
        st.candidate_scores.append(score)

        if continuity_pass(m, score):
            # Fiyatın en azından önceki kontrolden daha aşağıda olmamasını iste.
            prices = list(st.candidate_prices)
            price_ok = len(prices) < 2 or prices[-1] >= prices[-2] * 0.999
            if price_ok:
                st.candidate_passes += 1
        else:
            # Bir zayıf kontrol toleransı; art arda bozulma adayı sonlandırır.
            if st.candidate_checks - st.candidate_passes >= 2:
                reset_candidate(st)
                return

        if st.candidate_passes < CONFIRM_REQUIRED:
            return

        # Son teyitte OI alınır; tüm adaylar için REST tüketilmez.
        oi5 = await get_oi_5m(session, symbol)
        m["oi5"] = oi5
        if oi5 is not None:
            if oi5 >= 1.0:
                score = min(100, score + 4)
            elif oi5 <= -1.5:
                score = max(0, score - 4)
        m["score"] = score
        quality = entry_quality(m, score, st)
        rise_score = rise_probability(m, score, st)
        m["entry_quality"] = quality
        m["rise_score"] = rise_score
        m["confirm_passes"] = st.candidate_passes
        m["level"] = "CONFIRMED"

        # Sadece yüksek kaliteli ve aşırı uzamamış devam hareketi Telegram'a gider.
        if quality < ENTRY_MIN_SCORE or rise_score < 70 or m["extended"]:
            reset_candidate(st)
            return
        if now - st.buy_signal_ts < COOLDOWN_SECONDS:
            reset_candidate(st)
            return

        st.buy_signal_ts = now
        st.last_alert_ts = now
        st.last_alert_price = m["price"]
        signal_id = save_signal(m)
        pending_outcomes.append(PendingOutcome(signal_id, symbol, m["price"], now))
        log.info("CONFIRMED %s momentum=%d rise=%d quality=%d", symbol, score, rise_score, quality)
        await telegram_send(session, build_message(m), symbol=symbol)
        reset_candidate(st)
    finally:
        st.eval_inflight = False


async def ticker_ws(session):
    global stream_health
    url = WS_MARKET + "?streams=!ticker@arr"
    while not stop_event.is_set():
        try:
            async with session.ws_connect(url, heartbeat=30, receive_timeout=70) as ws:
                log.info("Ticker stream connected")
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    stream_health["ticker"] = time.time()
                    payload = json.loads(msg.data).get("data", [])
                    if not isinstance(payload, list):
                        continue
                    for t in payload:
                        if t.get("st") not in (None, 1):
                            continue
                        sym = t.get("s")
                        if sym in states:
                            st = states[sym]
                            st.last_price = float(t.get("c", 0) or 0)
                            st.pct24 = float(t.get("P", 0) or 0)
                            st.quote_volume24 = float(t.get("q", 0) or 0)
        except Exception as e:
            log.warning("Ticker WS reconnecting: %s", e)
            await asyncio.sleep(2)


async def book_ws(session):
    url = WS_PUBLIC + "?streams=!bookTicker"
    while not stop_event.is_set():
        try:
            async with session.ws_connect(url, heartbeat=30, receive_timeout=70) as ws:
                log.info("BookTicker stream connected")
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    stream_health["book"] = time.time()
                    d = json.loads(msg.data).get("data", {})
                    if not isinstance(d, dict) or d.get("st") not in (None, 1):
                        continue
                    sym = d.get("s")
                    if sym in states:
                        st = states[sym]
                        st.bid_price = float(d.get("b", 0) or 0)
                        st.bid_qty = float(d.get("B", 0) or 0)
                        st.ask_price = float(d.get("a", 0) or 0)
                        st.ask_qty = float(d.get("A", 0) or 0)
        except Exception as e:
            log.warning("Book WS reconnecting: %s", e)
            await asyncio.sleep(2)


async def liquidation_ws(session):
    url = WS_MARKET + "?streams=!forceOrder@arr"
    while not stop_event.is_set():
        try:
            async with session.ws_connect(url, heartbeat=30, receive_timeout=70) as ws:
                log.info("Liquidation stream connected")
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    stream_health["liq"] = time.time()
                    d = json.loads(msg.data).get("data", {})
                    # Depending on stream mode, payload can be one event or an array.
                    events = d if isinstance(d, list) else [d]
                    for ev in events:
                        if not isinstance(ev, dict):
                            continue
                        o = ev.get("o", {})
                        sym = o.get("s")
                        if sym not in states:
                            continue
                        price = float(o.get("ap", 0) or o.get("p", 0) or 0)
                        qty = float(o.get("q", 0) or 0)
                        quote = price * qty
                        ts = int(o.get("T", 0) or ev.get("E", now_ms()))
                        side = o.get("S")
                        if side == "BUY":  # short positions forced to buy back
                            states[sym].short_liqs.append((ts, quote))
                        elif side == "SELL":
                            states[sym].long_liqs.append((ts, quote))
        except Exception as e:
            log.warning("Liquidation WS reconnecting: %s", e)
            await asyncio.sleep(2)


async def aggtrade_chunk_ws(session, chunk: List[str], idx: int):
    global trade_event_count
    streams = "/".join(f"{s.lower()}@aggTrade" for s in chunk)
    url = WS_MARKET + "?streams=" + streams
    while not stop_event.is_set():
        try:
            async with session.ws_connect(url, heartbeat=30, receive_timeout=70, max_msg_size=2**23) as ws:
                log.info("AggTrade stream %d connected for %d symbols", idx, len(chunk))
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    d = json.loads(msg.data).get("data", {})
                    if not isinstance(d, dict) or d.get("st") not in (None, 1):
                        continue
                    sym = d.get("s")
                    if sym not in states:
                        continue
                    price = float(d.get("p", 0) or 0)
                    qty = float(d.get("q", 0) or 0)
                    if not price or not qty:
                        continue
                    ts = int(d.get("T", now_ms()))
                    aggressive_buy = not bool(d.get("m", False))
                    st = states[sym]
                    st.last_price = price
                    st.agg_events += 1
                    trade_event_count += 1
                    quote = price * qty
                    st.trades.append(TradeSample(ts, price, quote, aggressive_buy))
                    update_minute_candle(st, ts, price, quote, aggressive_buy)
                    prune_deque_by_ts(st.trades, ts - 120_000)
                    stream_health["agg"] = time.time()
                    if st.quote_volume24 >= MIN_24H_QUOTE_VOLUME:
                        asyncio.create_task(evaluate(session, sym))
        except Exception as e:
            log.warning("AggTrade WS %d reconnecting: %s", idx, e)
            await asyncio.sleep(2)


async def outcome_loop():
    horizons = (60, 180, 300, 900, 1800, 3600)
    while not stop_event.is_set():
        now = time.time()
        remove = []
        for p in list(pending_outcomes):
            price = states[p.symbol].last_price
            if not price:
                continue
            ret = pct_change(price, p.entry_price)
            p.mfe = max(p.mfe, ret)
            p.mae = min(p.mae, ret)
            age = now - p.created_ts
            for h in horizons:
                if age >= h and h not in p.completed:
                    save_outcome(p.signal_id, h, ret, p.mfe, p.mae)
                    p.completed.add(h)
            if 3600 in p.completed:
                remove.append(p)
        for p in remove:
            if p in pending_outcomes:
                pending_outcomes.remove(p)
        await asyncio.sleep(2)


async def reset_levels_loop():
    while not stop_event.is_set():
        now = time.time()
        for st in states.values():
            if st.last_alert_ts and now - st.last_alert_ts > COOLDOWN_SECONDS * 2:
                st.last_level = 0
        await asyncio.sleep(30)


def current_top(limit=10):
    rows = []
    for sym in symbols:
        m = compute_metrics(sym)
        if not m:
            continue
        score = score_metrics(m)
        rows.append((score, sym, m))
    rows.sort(key=lambda x: x[0], reverse=True)
    return rows[:limit]


def signal_count_today():
    try:
        conn = sqlite3.connect(DB_PATH)
        since = int(time.time()) - 86400
        n = conn.execute("SELECT COUNT(*) FROM signals_v2 WHERE ts>=?", (since,)).fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0



def gainers_ranked():
    """Rank active, liquid USDT perpetuals by 24h price change."""
    rows = []
    for sym in symbols:
        st = states[sym]
        if st.quote_volume24 < MIN_24H_QUOTE_VOLUME or not st.last_price:
            continue
        rows.append((st.pct24, sym, st))
    rows.sort(key=lambda x: x[0], reverse=True)
    return rows


def build_gainers_entry_message(symbol: str, rank: int, prev_rank: Optional[int]):
    st = states[symbol]
    m = compute_metrics(symbol)
    prior = f"#{prev_rank}" if prev_rank else "TOP {0} dışı".format(GAINERS_TOP_N)
    lines = [
        "🏆 GAINERS LİSTESİNE GİRDİ",
        "",
        f"🪙 {symbol}",
        f"📈 24s yükseliş: {st.pct24:+.2f}%",
        "",
        f"🏅 Yeni sıra: #{rank}",
        f"⬆️ Önceki sıra: {prior}",
    ]
    if m:
        score = score_metrics(m)
        lines += [
            "",
            f"⚡ 30 sn: {m['chg30']:+.2f}%",
            f"📈 5 dk: {m['chg5']:+.2f}%",
            f"📈 15 dk: {m['chg15']:+.2f}%",
            f"💥 Hacim akışı: {m['flow30']:.1f}x",
            f"🟢 Agresif alış: %{m['buy30']*100:.0f}",
            f"⭐ Anlık momentum: {score}/100",
        ]
        if states[symbol].buy_signal_ts and time.time() - states[symbol].buy_signal_ts <= 900:
            lines.append("✅ Süreklilik teyitli momentum da mevcut")
    lines += ["", f"⏰ {datetime.now(IST).strftime('%H:%M:%S')}"]
    return "\n".join(lines)


def build_gainers_rapid_message(symbol: str, old_rank: int, new_rank: int, old_pct: float):
    st = states[symbol]
    m = compute_metrics(symbol)
    climbed = old_rank - new_rank
    lines = [
        "🚀 GAINERS SIRALAMASINDA HIZLI YÜKSELİŞ",
        "",
        f"🪙 {symbol}",
        f"📈 24s: {st.pct24:+.2f}% (önce {old_pct:+.2f}%)",
        "",
        f"⬆️ Yaklaşık {GAINERS_RAPID_WINDOW_SECONDS//60} dk önce: #{old_rank}",
        f"🏅 Şimdi: #{new_rank}",
        f"🚀 Yükseldiği sıra: {climbed}",
    ]
    if m:
        score = score_metrics(m)
        lines += [
            "",
            f"⚡ 30 sn: {m['chg30']:+.2f}%",
            f"📈 5 dk: {m['chg5']:+.2f}%",
            f"💥 Hacim akışı: {m['flow30']:.1f}x",
            f"🟢 Agresif alış: %{m['buy30']*100:.0f}",
            f"⭐ Anlık momentum: {score}/100",
        ]
        if states[symbol].buy_signal_ts and time.time() - states[symbol].buy_signal_ts <= 900:
            lines.append("✅ Süreklilik teyitli momentum da mevcut")
    lines += ["", f"⏰ {datetime.now(IST).strftime('%H:%M:%S')}"]
    return "\n".join(lines)


async def gainers_loop(session):
    """Alert on TOP-N entry and unusually fast rank climbs without startup spam."""
    global gainers_initialized, gainers_current_top, gainers_prev_rank
    while not stop_event.is_set():
        try:
            ranked = gainers_ranked()
            if not ranked:
                await asyncio.sleep(GAINERS_POLL_SECONDS)
                continue

            now = time.time()
            rank_map = {sym: i + 1 for i, (_, sym, _) in enumerate(ranked)}
            pct_map = {sym: pct for pct, sym, _ in ranked}
            top_now = set(sym for _, sym, _ in ranked[:GAINERS_TOP_N])

            # Keep ~10m rank history for rapid-climb detection.
            for sym, rank in rank_map.items():
                hist = gainers_rank_history[sym]
                hist.append((now, rank, pct_map[sym]))
                cutoff = now - max(GAINERS_RAPID_WINDOW_SECONDS * 2, 1200)
                while hist and hist[0][0] < cutoff:
                    hist.popleft()

            if not gainers_initialized:
                gainers_current_top = top_now
                gainers_prev_rank = rank_map
                gainers_initialized = True
                log.info("Gainers baseline ready: TOP %d", GAINERS_TOP_N)
                await asyncio.sleep(GAINERS_POLL_SECONDS)
                continue

            # Mark exits; a re-entry alert requires some time outside the list.
            for sym in gainers_current_top - top_now:
                gainers_left_top_at[sym] = now

            # TOP-N new entry alerts.
            entrants = sorted(top_now - gainers_current_top, key=lambda x: rank_map.get(x, 99999))
            for sym in entrants:
                left_at = gainers_left_top_at.get(sym, 0.0)
                first_seen_entry = gainers_last_entry_alert.get(sym, 0.0) == 0.0
                was_out_long_enough = left_at == 0.0 or now - left_at >= GAINERS_REENTRY_MIN_OUT_SECONDS
                cooldown_ok = now - gainers_last_entry_alert.get(sym, 0.0) >= GAINERS_ALERT_COOLDOWN_SECONDS
                if cooldown_ok and (first_seen_entry or was_out_long_enough):
                    await telegram_send(session, build_gainers_entry_message(sym, rank_map[sym], gainers_prev_rank.get(sym)), symbol=sym)
                    gainers_last_entry_alert[sym] = now
                    log.info("GAINERS ENTRY %s rank=%d pct=%.2f", sym, rank_map[sym], pct_map[sym])

            # Rapid rank climb alerts. Compare with a sample at least WINDOW seconds old.
            for sym, new_rank in rank_map.items():
                if new_rank > GAINERS_RAPID_MAX_RANK or pct_map[sym] <= 0:
                    continue
                hist = gainers_rank_history[sym]
                old = None
                target = now - GAINERS_RAPID_WINDOW_SECONDS
                for sample in hist:
                    if sample[0] <= target:
                        old = sample
                    else:
                        break
                if old is None:
                    continue
                _, old_rank, old_pct = old
                climbed = old_rank - new_rank
                if climbed < GAINERS_RAPID_MIN_POSITIONS:
                    continue
                if now - gainers_last_rapid_alert.get(sym, 0.0) < GAINERS_ALERT_COOLDOWN_SECONDS:
                    continue
                # If the same cycle already announced TOP-N entry, avoid duplicate Telegram spam.
                if sym in entrants and now - gainers_last_entry_alert.get(sym, 0.0) < 5:
                    continue
                await telegram_send(session, build_gainers_rapid_message(sym, old_rank, new_rank, old_pct), symbol=sym)
                gainers_last_rapid_alert[sym] = now
                log.info("GAINERS RAPID %s %d->%d pct=%.2f", sym, old_rank, new_rank, pct_map[sym])

            gainers_current_top = top_now
            gainers_prev_rank = rank_map
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Gainers loop error: %s", e)
        await asyncio.sleep(GAINERS_POLL_SECONDS)


async def telegram_command_loop(session):
    global telegram_offset
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    while not stop_event.is_set():
        try:
            params = {"timeout": 20, "offset": telegram_offset, "allowed_updates": json.dumps(["message"])}
            async with session.get(url, params=params, timeout=25) as r:
                data = await r.json()
            for upd in data.get("result", []):
                telegram_offset = max(telegram_offset, int(upd.get("update_id", 0)) + 1)
                msg = upd.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue
                text = str(msg.get("text", "")).strip().lower()
                if text == "/status":
                    age = lambda k: (time.time() - stream_health[k]) if stream_health[k] else 9999
                    healthy = all(age(k) < 90 for k in ("ticker", "book", "agg"))
                    await telegram_send(session,
                        f"{'✅' if healthy else '⚠️'} Scanner durumu\n\n"
                        f"🪙 Kontrat: {len(symbols)}\n"
                        f"💵 Min 24s hacim: {fmt_money(MIN_24H_QUOTE_VOLUME)} USDT\n"
                        f"⚡ AggTrade olayları: {trade_event_count:,}\n"
                        f"🚨 Son 24s sinyal: {signal_count_today()}\n"
                        f"📡 ticker: {age('ticker'):.0f} sn | book: {age('book'):.0f} sn | agg: {age('agg'):.0f} sn\n"
                        f"⭐ Aday eşiği: {EARLY_SCORE}+\n"
                        f"🎯 Teyit: {CONFIRM_REQUIRED} kontrol × {CONFIRM_INTERVAL_SECONDS} sn | giriş kalitesi {ENTRY_MIN_SCORE}+ | yükseliş 70+\n"
                        f"🏆 Gainers: TOP {GAINERS_TOP_N} giriş + {GAINERS_RAPID_WINDOW_SECONDS//60} dk’da {GAINERS_RAPID_MIN_POSITIONS}+ sıra yükseliş"
                    )
                elif text == "/top":
                    rows = current_top(10)
                    if not rows:
                        await telegram_send(session, "Henüz yeterli canlı trade verisi birikmedi. 30-60 sn sonra tekrar /top yaz.")
                    else:
                        lines = ["📊 ŞU AN ISINAN COINLER\n"]
                        for score, sym, m in rows:
                            lines.append(f"{score:>3}/100  {sym} | 30sn {m['chg30']:+.2f}% | flow {m['flow30']:.1f}x | buy %{m['buy30']*100:.0f}")
                        lines.append("\nNot: /top alarm değil; alarm eşiğine yaklaşanları gösterir.")
                        await telegram_send(session, "\n".join(lines))
                elif text == "/gainers":
                    ranked = gainers_ranked()[:min(GAINERS_TOP_N, 20)]
                    if not ranked:
                        await telegram_send(session, "Gainers verisi henüz hazır değil.")
                    else:
                        lines = [f"🏆 FUTURES GAINERS — İlk {len(ranked)}\n"]
                        for i, (pct, sym, _) in enumerate(ranked, 1):
                            lines.append(f"#{i:<2} {sym}  {pct:+.2f}%")
                        lines.append(f"\nOtomatik yeni giriş alarm bölgesi: TOP {GAINERS_TOP_N}")
                        await telegram_send(session, "\n".join(lines))
                elif text == "/stats":
                    conn = sqlite3.connect(DB_PATH)
                    row = conn.execute("""SELECT COUNT(*), SUM(CASE WHEN o.mfe_pct>=0.5 THEN 1 ELSE 0 END), SUM(CASE WHEN o.mfe_pct>=1 THEN 1 ELSE 0 END), SUM(CASE WHEN o.mfe_pct>=2 THEN 1 ELSE 0 END), AVG(o.mfe_pct), AVG(o.mae_pct) FROM signals_v2 s JOIN signal_outcomes o ON o.signal_id=s.id AND o.horizon_s=3600""").fetchone()
                    conn.close()
                    n = row[0] or 0
                    if not n:
                        await telegram_send(session, "Henüz tamamlanmış 60 dk performans verisi yok.")
                    else:
                        await telegram_send(session, f"📊 60 DK SİNYAL PERFORMANSI\n\nTamamlanan: {n}\n+%0.5 gördü: %{100*row[1]/n:.1f}\n+%1 gördü: %{100*row[2]/n:.1f}\n+%2 gördü: %{100*row[3]/n:.1f}\nOrt. maksimum yükseliş: {row[4]:+.2f}%\nOrt. maksimum ters hareket: {row[5]:+.2f}%\n\nNot: Maksimum yükseliş, sinyal sonrası fırsatı ölçer; son fiyatı değil.")
                elif text in ("/test", "test"):
                    await telegram_send(session, "✅ Bot çalışıyor. Binance canlı akışlarını dinliyorum. /status, /top, /gainers ve /stats kullanabilirsin.")
                elif text in ("/help", "/start"):
                    await telegram_send(session,
                        "🤖 Momentum Scanner V5 — Data-tuned Momentum + Gainers\n\n"
                        "/status — bağlantı ve sinyal durumu\n"
                        "/top — şu an ısınan ilk 10 coin\n"
                        "/gainers — güncel Futures gainers\n"
                        "/stats — tamamlanan sinyallerin 60 dk performansı\n"
                        "/test — Telegram testi\n\n"
                        "Momentum adayları sessiz izlenir. Telegram yalnızca süreklilik teyidinde, gainers TOP bölgesine yeni girişte veya hızlı sıra yükselişinde bildirim gönderir."
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("Telegram command polling: %s", e)
            await asyncio.sleep(3)


async def main():
    global symbols
    init_db()
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        symbols = await load_symbols(session)
        for s in symbols:
            states[s]
        await bootstrap_tickers(session)
        log.info("Tracking %d active USDT perpetual contracts", len(symbols))
        log.info("Bootstrapping %d closed 1m candles per symbol", BOOTSTRAP_CANDLES)
        await bootstrap_all(session)
        log.info("Bootstrap complete")

        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            await telegram_send(session,
                f"✅ Momentum Scanner V4 başladı\n\n"
                f"🪙 İzlenen kontrat: {len(symbols)}\n"
                f"⚡ 100ms aggTrade ile erken momentum taraması\n"
                f"💵 Min 24s hacim: {fmt_money(MIN_24H_QUOTE_VOLUME)} USDT\n"
                f"⭐ Sessiz aday skoru: {EARLY_SCORE}+\n"
                f"🎯 Teyit: {CONFIRM_REQUIRED} × {CONFIRM_INTERVAL_SECONDS} sn | giriş kalitesi {ENTRY_MIN_SCORE}+\n\n"
                f"🏆 Gainers: TOP {GAINERS_TOP_N} + hızlı sıra yükselişi\n\n"
                f"Komutlar: /status  /top  /gainers  /test"
            )

        chunks = [symbols[i:i + AGGTRADE_CHUNK] for i in range(0, len(symbols), AGGTRADE_CHUNK)]
        tasks = [
            ticker_ws(session), book_ws(session), liquidation_ws(session),
            outcome_loop(), reset_levels_loop(), telegram_command_loop(session), gainers_loop(session),
        ]
        tasks.extend(aggtrade_chunk_ws(session, c, i + 1) for i, c in enumerate(chunks))
        await asyncio.gather(*tasks)


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
