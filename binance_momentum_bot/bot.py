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
CONFIRM_REQUIRED = int(os.getenv("CONFIRM_REQUIRED", "3"))
CANDIDATE_TTL_SECONDS = int(os.getenv("CANDIDATE_TTL_SECONDS", "120"))
CONFIRM_MIN_SCORE = int(os.getenv("CONFIRM_MIN_SCORE", "62"))
ENTRY_MIN_SCORE = int(os.getenv("ENTRY_MIN_SCORE", "72"))
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
RISE_MIN_SCORE = int(os.getenv("RISE_MIN_SCORE", "66"))

# V5.4 quality-first alerting. Confirmed momentum is still tracked, but Telegram
# "ALIM FIRSATI" is reserved for stricter, trade-quality setups.
PREMIUM_MIN_MOMENTUM_SCORE = int(os.getenv("PREMIUM_MIN_MOMENTUM_SCORE", "64"))
PREMIUM_ENTRY_MIN_SCORE = int(os.getenv("PREMIUM_ENTRY_MIN_SCORE", "78"))
PREMIUM_RISE_MIN_SCORE = int(os.getenv("PREMIUM_RISE_MIN_SCORE", "70"))
PREMIUM_MIN_CHG30 = float(os.getenv("PREMIUM_MIN_CHG30", "0.35"))
PREMIUM_MAX_CHG30 = float(os.getenv("PREMIUM_MAX_CHG30", "1.40"))
PREMIUM_MIN_CHG60 = float(os.getenv("PREMIUM_MIN_CHG60", "0.70"))
PREMIUM_MAX_CHG60 = float(os.getenv("PREMIUM_MAX_CHG60", "2.50"))
PREMIUM_MIN_FLOW30 = float(os.getenv("PREMIUM_MIN_FLOW30", "1.80"))
PREMIUM_MIN_BUY30 = float(os.getenv("PREMIUM_MIN_BUY30", "0.62"))
PREMIUM_MAX_BUY30 = float(os.getenv("PREMIUM_MAX_BUY30", "0.80"))
PREMIUM_MAX_BOOK_IMBALANCE = float(os.getenv("PREMIUM_MAX_BOOK_IMBALANCE", "0.90"))
PREMIUM_MAX_CANDIDATE_RUNUP_PCT = float(os.getenv("PREMIUM_MAX_CANDIDATE_RUNUP_PCT", "2.00"))
PREMIUM_REQUIRE_BREAKOUT = os.getenv("PREMIUM_REQUIRE_BREAKOUT", "1").strip() not in ("0", "false", "False")

# A selective pre-signal warning. It is explicitly a radar/watch alert, not a buy call.
EARLY_ALERT_ENABLED = os.getenv("EARLY_ALERT_ENABLED", "1").strip() not in ("0", "false", "False")
EARLY_ALERT_MIN_SCORE = int(os.getenv("EARLY_ALERT_MIN_SCORE", "64"))
EARLY_ALERT_MIN_CHG30 = float(os.getenv("EARLY_ALERT_MIN_CHG30", "0.25"))
EARLY_ALERT_MIN_CHG60 = float(os.getenv("EARLY_ALERT_MIN_CHG60", "0.40"))
EARLY_ALERT_MIN_FLOW30 = float(os.getenv("EARLY_ALERT_MIN_FLOW30", "2.00"))
EARLY_ALERT_MIN_BUY30 = float(os.getenv("EARLY_ALERT_MIN_BUY30", "0.62"))
EARLY_ALERT_MAX_BUY30 = float(os.getenv("EARLY_ALERT_MAX_BUY30", "0.80"))
EARLY_ALERT_MAX_BOOK = float(os.getenv("EARLY_ALERT_MAX_BOOK", "0.90"))
EARLY_ALERT_COOLDOWN_SECONDS = int(os.getenv("EARLY_ALERT_COOLDOWN_SECONDS", "1800"))
# V5.4: every qualifying early radar can be recorded internally, but Telegram waits for
# 2/3 continuity plus a stricter notification gate. This keeps the research data rich
# while reducing user-facing noise.
EARLY_RADAR_RECORD_COOLDOWN_SECONDS = int(os.getenv("EARLY_RADAR_RECORD_COOLDOWN_SECONDS", "600"))
EARLY_NOTIFY_MIN_SCORE = int(os.getenv("EARLY_NOTIFY_MIN_SCORE", "70"))
EARLY_NOTIFY_MIN_CHG30 = float(os.getenv("EARLY_NOTIFY_MIN_CHG30", "0.30"))
EARLY_NOTIFY_MIN_CHG60 = float(os.getenv("EARLY_NOTIFY_MIN_CHG60", "0.45"))
EARLY_NOTIFY_MIN_FLOW30 = float(os.getenv("EARLY_NOTIFY_MIN_FLOW30", "2.20"))
EARLY_NOTIFY_MIN_BUY30 = float(os.getenv("EARLY_NOTIFY_MIN_BUY30", "0.62"))
EARLY_NOTIFY_MAX_BUY30 = float(os.getenv("EARLY_NOTIFY_MAX_BUY30", "0.80"))
EARLY_NOTIFY_MAX_CHG5 = float(os.getenv("EARLY_NOTIFY_MAX_CHG5", "2.50"))

# Optional one-shot continuation message after a confirmed setup has already moved.
CONTINUATION_ALERT_ENABLED = os.getenv("CONTINUATION_ALERT_ENABLED", "1").strip() not in ("0", "false", "False")
CONTINUATION_MIN_MFE_PCT = float(os.getenv("CONTINUATION_MIN_MFE_PCT", "2.00"))
CONTINUATION_MIN_SCORE = int(os.getenv("CONTINUATION_MIN_SCORE", "72"))

# V5.5 observer-only research layer. These settings never gate Premium creation,
# never alter TP1/TP2, and never place orders. They only store path data and
# optionally send clearly labelled SHADOW test notifications.
SHADOW_EXIT_ENABLED = os.getenv("SHADOW_EXIT_ENABLED", "1").strip() not in ("0", "false", "False")
SHADOW_EXIT_NOTIFY = os.getenv("SHADOW_EXIT_NOTIFY", "1").strip() not in ("0", "false", "False")
SHADOW_MIN_PEAK_MFE_PCT = float(os.getenv("SHADOW_MIN_PEAK_MFE_PCT", "1.00"))
SHADOW_PROTECT_MIN_PEAK_PCT = float(os.getenv("SHADOW_PROTECT_MIN_PEAK_PCT", "1.50"))
SHADOW_PROTECT_DRAWDOWN_PCT = float(os.getenv("SHADOW_PROTECT_DRAWDOWN_PCT", "0.60"))
SHADOW_EXIT_DRAWDOWN_PCT = float(os.getenv("SHADOW_EXIT_DRAWDOWN_PCT", "1.00"))
SHADOW_HARD_DRAWDOWN_PCT = float(os.getenv("SHADOW_HARD_DRAWDOWN_PCT", "2.00"))
SHADOW_MIN_AGE_SECONDS = int(os.getenv("SHADOW_MIN_AGE_SECONDS", "30"))
WAVE_PULLBACK_LEVELS = (0.50, 1.00, 1.50, 2.00)

# V5.6 measurement-first research. None of these settings gate Premium creation.
GAINERS_NOTIFY = os.getenv("GAINERS_NOTIFY", "0").strip() not in ("0", "false", "False")
RESEARCH_ENABLED = os.getenv("RESEARCH_ENABLED", "1").strip() not in ("0", "false", "False")
PREBREAKOUT_ENABLED = os.getenv("PREBREAKOUT_ENABLED", "1").strip() not in ("0", "false", "False")
PREBREAKOUT_COOLDOWN_SECONDS = int(os.getenv("PREBREAKOUT_COOLDOWN_SECONDS", "1200"))
SECOND_WAVE_ENABLED = os.getenv("SECOND_WAVE_ENABLED", "1").strip() not in ("0", "false", "False")
SECOND_WAVE_COOLDOWN_SECONDS = int(os.getenv("SECOND_WAVE_COOLDOWN_SECONDS", "600"))
SECOND_WAVE_MAX_GAP_SECONDS = int(os.getenv("SECOND_WAVE_MAX_GAP_SECONDS", "21600"))
FLOW_STRUCTURE_COOLDOWN_SECONDS = int(os.getenv("FLOW_STRUCTURE_COOLDOWN_SECONDS", "900"))
RESEARCH_HORIZONS = (60, 300, 900, 1800, 3600)
GAINERS_OUTCOME_HORIZONS = (60, 300, 900, 1800, 3600)
SHADOW_OUTCOME_HORIZONS = (30, 60, 300, 900)
ANCHOR_MAX_AGE_SECONDS = int(os.getenv("ANCHOR_MAX_AGE_SECONDS", "21600"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("momentum-v5.6")


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
    funding_rate_pct: float = 0.0
    funding_ts: float = 0.0
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
    candidate_prices: Deque[float] = field(default_factory=lambda: deque(maxlen=6))
    candidate_scores: Deque[int] = field(default_factory=lambda: deque(maxlen=6))
    buy_signal_ts: float = 0.0
    early_alert_ts: float = 0.0
    early_alert_price: float = 0.0
    radar_record_ts: float = 0.0
    active_radar_id: int = 0
    active_radar_notified: bool = False
    episode_id: int = 0
    episode_started_ts: float = 0.0
    episode_start_price: float = 0.0
    episode_peak_price: float = 0.0
    episode_had_early: bool = False
    episode_had_premium: bool = False
    episode_anchor_avg1m: float = 0.0
    prev_meaningful_episode_id: int = 0
    prev_meaningful_ts: float = 0.0
    prev_meaningful_price: float = 0.0
    prev_meaningful_peak_price: float = 0.0
    anchor_avg1m: float = 0.0
    anchor_ts: float = 0.0
    last_second_wave_ts: float = 0.0
    last_prebreakout_ts: float = 0.0
    last_flow_structure_ts: float = 0.0


@dataclass
class PendingOutcome:
    signal_id: int
    symbol: str
    entry_price: float
    created_ts: float
    target1: float = 0.0
    target2: float = 0.0
    invalidation: float = 0.0
    entry_low: float = 0.0
    entry_high: float = 0.0
    entry_touch_s: Optional[float] = None
    path_entry_price: float = 0.0
    target_before_entry_s: Optional[float] = None
    mfe: float = 0.0
    mae: float = 0.0
    mfe_before_tp1: float = 0.0
    mae_before_tp1: float = 0.0
    trade_mfe: float = 0.0
    trade_mae: float = 0.0
    tp1_hit_s: Optional[float] = None
    tp2_hit_s: Optional[float] = None
    invalidation_hit_s: Optional[float] = None
    first_event: Optional[str] = None
    completed: set = field(default_factory=set)
    continuation_sent: bool = False
    peak_price: float = 0.0
    peak_mfe_pct: float = 0.0
    peak_s: float = 0.0
    pullbacks_seen: set = field(default_factory=set)
    shadow_protect_sent: bool = False
    shadow_exit_sent: bool = False
    wave_dirty: bool = False
    first_wave_peak_price: float = 0.0
    first_wave_peak_mfe_pct: float = 0.0
    first_wave_peak_s: float = 0.0
    first_wave_end_s: Optional[float] = None
    first_wave_end_reason: str = ""
    wave_no: int = 1
    wave_active: bool = True
    wave_start_price: float = 0.0
    wave_start_s: float = 0.0
    wave_peak_price: float = 0.0
    wave_peak_s: float = 0.0
    wave_last_end_price: float = 0.0
    wave_last_end_s: float = 0.0


@dataclass
class PendingRadar:
    radar_id: int
    symbol: str
    entry_price: float
    created_ts: float
    mfe: float = 0.0
    mae: float = 0.0
    completed: set = field(default_factory=set)


@dataclass
class PendingGainer:
    event_id: int
    symbol: str
    entry_price: float
    created_ts: float
    mfe: float = 0.0
    mae: float = 0.0
    completed: set = field(default_factory=set)


@dataclass
class PendingResearch:
    event_id: int
    symbol: str
    entry_price: float
    created_ts: float
    mfe: float = 0.0
    mae: float = 0.0
    completed: set = field(default_factory=set)


@dataclass
class PendingShadowEvent:
    event_id: int
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
pending_radars: List[PendingRadar] = []
pending_gainers: List[PendingGainer] = []
pending_research: List[PendingResearch] = []
pending_shadow_events: List[PendingShadowEvent] = []
stream_health = {
    "ticker": 0.0,
    "book": 0.0,
    "liq": 0.0,
    "mark": 0.0,
    "agg": 0.0,
}
agg_stream_health: Dict[int, float] = {}
stream_reconnects = defaultdict(int)
trade_event_count = 0
telegram_offset = 0

# Diagnostic funnel counters for the current deployment/session.
funnel_started_ts = time.time()
funnel_counts = defaultdict(int)

def funnel_hit(name: str):
    funnel_counts[name] += 1

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


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def ensure_column(conn, table: str, column: str, decl: str):
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db():
    conn = db_connect()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            event TEXT NOT NULL,
            price REAL, score INTEGER,
            chg30 REAL, chg60 REAL, chg5 REAL,
            flow30 REAL, buy30 REAL, book_imbalance REAL, rel30 REAL,
            breakout INTEGER, candidate_age_s REAL, confirm_passes INTEGER,
            gainer_rank INTEGER, qv24 REAL, note TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_meta (
            signal_id INTEGER PRIMARY KEY,
            entry_quality INTEGER, rise_score INTEGER, candidate_runup REAL,
            gainer_rank INTEGER, qv24 REAL, premium INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gainers_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            event TEXT NOT NULL,
            rank_now INTEGER, rank_old INTEGER, pct24 REAL, price REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_paths (
            signal_id INTEGER PRIMARY KEY,
            entry_low REAL, entry_high REAL, entry_touch_s REAL, path_entry_price REAL,
            target1 REAL, target2 REAL, invalidation REAL, target_before_entry_s REAL,
            tp1_hit_s REAL, tp2_hit_s REAL, invalidation_hit_s REAL,
            first_event TEXT,
            mfe_before_tp1 REAL, mae_before_tp1 REAL, trade_mfe REAL, trade_mae REAL,
            completed_60m INTEGER DEFAULT 0,
            updated_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS radar_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            score INTEGER,
            chg30 REAL, chg60 REAL, chg5 REAL,
            flow30 REAL, buy30 REAL, book_imbalance REAL, rel30 REAL,
            breakout INTEGER, gainer_rank INTEGER,
            notified INTEGER DEFAULT 0, notify_ts INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS radar_outcomes (
            radar_id INTEGER NOT NULL,
            horizon_s INTEGER NOT NULL,
            return_pct REAL, mfe_pct REAL, mae_pct REAL,
            ts INTEGER NOT NULL,
            PRIMARY KEY(radar_id, horizon_s)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_radar_links (
            signal_id INTEGER PRIMARY KEY,
            radar_id INTEGER,
            symbol TEXT NOT NULL,
            early_ts INTEGER, premium_ts INTEGER NOT NULL,
            early_price REAL, premium_price REAL NOT NULL,
            early_to_premium_s REAL, price_cost_pct REAL,
            early_notified INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_wave_tracking (
            signal_id INTEGER PRIMARY KEY,
            peak_price REAL, peak_mfe_pct REAL, peak_s REAL,
            pullback_0_5_s REAL, pullback_1_0_s REAL, pullback_1_5_s REAL, pullback_2_0_s REAL,
            max_drawdown_from_peak_pct REAL DEFAULT 0,
            completed_60m INTEGER DEFAULT 0,
            updated_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shadow_exit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            signal_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            event TEXT NOT NULL,
            age_s REAL, price REAL, return_pct REAL,
            peak_mfe_pct REAL, drawdown_from_peak_pct REAL,
            score INTEGER, chg30 REAL, chg60 REAL, flow30 REAL, buy30 REAL,
            book_imbalance REAL, rel30 REAL, breakout INTEGER, reason TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            local_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            kind TEXT NOT NULL,
            ordinal INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS momentum_episodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            start_ts INTEGER NOT NULL,
            end_ts INTEGER,
            start_price REAL,
            end_price REAL,
            start_score INTEGER,
            end_reason TEXT,
            had_early INTEGER DEFAULT 0,
            had_premium INTEGER DEFAULT 0,
            peak_price REAL,
            peak_return_pct REAL,
            anchor_avg1m REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gainers_outcomes (
            event_id INTEGER NOT NULL,
            horizon_s INTEGER NOT NULL,
            return_pct REAL, mfe_pct REAL, mae_pct REAL,
            ts INTEGER NOT NULL,
            PRIMARY KEY(event_id, horizon_s)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL,
            episode_id INTEGER,
            price REAL NOT NULL,
            score INTEGER,
            chg10 REAL, chg30 REAL, chg60 REAL, chg5 REAL, chg15 REAL,
            flow10 REAL, flow30 REAL, flow60 REAL,
            buy30 REAL, book_imbalance REAL, rel30 REAL, spread REAL,
            breakout INTEGER, gainer_rank INTEGER, rank_velocity REAL,
            compression_ratio REAL, dist15high_pct REAL,
            flow_eff30 REAL, flow_eff60 REAL, anchor_flow30 REAL,
            oi5 REAL, note TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_outcomes (
            event_id INTEGER NOT NULL,
            horizon_s INTEGER NOT NULL,
            return_pct REAL, mfe_pct REAL, mae_pct REAL,
            ts INTEGER NOT NULL,
            PRIMARY KEY(event_id, horizon_s)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shadow_event_outcomes (
            shadow_event_id INTEGER NOT NULL,
            horizon_s INTEGER NOT NULL,
            return_pct REAL, mfe_pct REAL, mae_pct REAL,
            ts INTEGER NOT NULL,
            PRIMARY KEY(shadow_event_id, horizon_s)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_wave_events (
            signal_id INTEGER NOT NULL,
            wave_no INTEGER NOT NULL,
            start_s REAL, start_price REAL,
            peak_s REAL, peak_price REAL, peak_mfe_pct REAL,
            end_s REAL, end_price REAL, drawdown_pct REAL,
            end_reason TEXT,
            PRIMARY KEY(signal_id, wave_no)
        )
        """
    )

    # Safe schema extensions for persistent DBs created by earlier versions.
    ensure_column(conn, "signals_v2", "episode_id", "INTEGER")
    ensure_column(conn, "signals_v2", "daily_notice_no", "INTEGER")
    ensure_column(conn, "signals_v2", "flow_eff30", "REAL")
    ensure_column(conn, "signals_v2", "flow_eff60", "REAL")
    ensure_column(conn, "signals_v2", "squeeze_risk", "INTEGER")
    ensure_column(conn, "signals_v2", "funding_rate_pct", "REAL")
    ensure_column(conn, "candidate_events", "episode_id", "INTEGER")
    ensure_column(conn, "radar_signals", "episode_id", "INTEGER")
    ensure_column(conn, "radar_signals", "daily_notice_no", "INTEGER")
    ensure_column(conn, "premium_radar_links", "episode_id", "INTEGER")
    ensure_column(conn, "signal_meta", "flow_eff30", "REAL")
    ensure_column(conn, "signal_meta", "flow_eff60", "REAL")
    ensure_column(conn, "signal_meta", "squeeze_risk", "INTEGER")
    ensure_column(conn, "signal_meta", "anchor_flow30", "REAL")
    ensure_column(conn, "research_events", "funding_rate_pct", "REAL")
    ensure_column(conn, "research_events", "short_liq", "REAL")
    ensure_column(conn, "research_events", "long_liq", "REAL")
    ensure_column(conn, "gainers_events", "score", "INTEGER")
    ensure_column(conn, "gainers_events", "chg30", "REAL")
    ensure_column(conn, "gainers_events", "chg60", "REAL")
    ensure_column(conn, "gainers_events", "chg5", "REAL")
    ensure_column(conn, "gainers_events", "flow30", "REAL")
    ensure_column(conn, "gainers_events", "buy30", "REAL")
    ensure_column(conn, "gainers_events", "book_imbalance", "REAL")
    ensure_column(conn, "gainers_events", "rel30", "REAL")
    ensure_column(conn, "gainers_events", "breakout", "INTEGER")
    ensure_column(conn, "gainers_events", "rank_delta", "INTEGER")
    ensure_column(conn, "gainers_events", "rank_velocity_per_min", "REAL")
    ensure_column(conn, "shadow_exit_events", "daily_notice_no", "INTEGER")
    ensure_column(conn, "premium_wave_tracking", "first_wave_peak_price", "REAL")
    ensure_column(conn, "premium_wave_tracking", "first_wave_peak_mfe_pct", "REAL")
    ensure_column(conn, "premium_wave_tracking", "first_wave_peak_s", "REAL")
    ensure_column(conn, "premium_wave_tracking", "first_wave_end_s", "REAL")
    ensure_column(conn, "premium_wave_tracking", "first_wave_end_reason", "TEXT")
    ensure_column(conn, "premium_wave_tracking", "wave_count", "INTEGER DEFAULT 1")
    conn.commit()
    conn.close()


def save_signal(m: dict) -> int:
    conn = db_connect()
    cur = conn.execute(
        """
        INSERT INTO signals_v2
        (ts,symbol,level,score,price,chg10,chg30,chg60,chg5,chg15,chg24,
         flow10,flow30,flow60,buy10,buy30,buy60,spread,book_imbalance,
         short_liq,long_liq,oi5,breakout,extended,episode_id,daily_notice_no,flow_eff30,flow_eff60,squeeze_risk)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(time.time()), m["symbol"], m["level"], m["score"], m["price"],
            m["chg10"], m["chg30"], m["chg60"], m["chg5"], m["chg15"], m["chg24"],
            m["flow10"], m["flow30"], m["flow60"], m["buy10"], m["buy30"], m["buy60"],
            m["spread"], m["book_imbalance"], m["short_liq"], m["long_liq"],
            m.get("oi5"), int(m["breakout"]), int(m["extended"]),
            m.get("episode_id"), m.get("daily_notice_no"), m.get("flow_eff30"), m.get("flow_eff60"), int(bool(m.get("squeeze_risk", False))),
        ),
    )
    signal_id = cur.lastrowid
    conn.commit()
    conn.close()
    return signal_id


def save_outcome(signal_id: int, horizon_s: int, ret: float, mfe: float, mae: float):
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO signal_outcomes(signal_id,horizon_s,return_pct,mfe_pct,mae_pct,ts) VALUES (?,?,?,?,?,?)",
        (signal_id, horizon_s, ret, mfe, mae, int(time.time())),
    )
    conn.commit()
    conn.close()


def init_signal_path(signal_id: int, entry_low: float, entry_high: float, target1: float, target2: float, invalidation: float,
                     entry_touch_s: Optional[float] = None, path_entry_price: float = 0.0):
    conn = db_connect()
    conn.execute(
        """INSERT OR REPLACE INTO signal_paths
        (signal_id,entry_low,entry_high,entry_touch_s,path_entry_price,target1,target2,invalidation,updated_ts)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (signal_id,entry_low,entry_high,entry_touch_s,path_entry_price,target1,target2,invalidation,int(time.time())),
    )
    conn.commit()
    conn.close()


def save_signal_path(p: PendingOutcome, completed_60m: bool = False):
    conn = db_connect()
    conn.execute(
        """INSERT INTO signal_paths
        (signal_id,entry_low,entry_high,entry_touch_s,path_entry_price,target1,target2,invalidation,target_before_entry_s,
         tp1_hit_s,tp2_hit_s,invalidation_hit_s,first_event,mfe_before_tp1,mae_before_tp1,trade_mfe,trade_mae,completed_60m,updated_ts)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(signal_id) DO UPDATE SET
          entry_low=excluded.entry_low,entry_high=excluded.entry_high,entry_touch_s=excluded.entry_touch_s,path_entry_price=excluded.path_entry_price,
          target1=excluded.target1,target2=excluded.target2,invalidation=excluded.invalidation,target_before_entry_s=excluded.target_before_entry_s,
          tp1_hit_s=excluded.tp1_hit_s,tp2_hit_s=excluded.tp2_hit_s,invalidation_hit_s=excluded.invalidation_hit_s,
          first_event=excluded.first_event,mfe_before_tp1=excluded.mfe_before_tp1,mae_before_tp1=excluded.mae_before_tp1,
          trade_mfe=excluded.trade_mfe,trade_mae=excluded.trade_mae,
          completed_60m=MAX(signal_paths.completed_60m,excluded.completed_60m),updated_ts=excluded.updated_ts""",
        (p.signal_id,p.entry_low,p.entry_high,p.entry_touch_s,p.path_entry_price,p.target1,p.target2,p.invalidation,p.target_before_entry_s,
         p.tp1_hit_s,p.tp2_hit_s,p.invalidation_hit_s,p.first_event,p.mfe_before_tp1,p.mae_before_tp1,p.trade_mfe,p.trade_mae,
         1 if completed_60m else 0,int(time.time())),
    )
    conn.commit()
    conn.close()


def save_radar_signal(symbol: str, m: dict, score: int) -> int:
    conn = db_connect()
    cur = conn.execute(
        """INSERT INTO radar_signals
        (ts,symbol,price,score,chg30,chg60,chg5,flow30,buy30,book_imbalance,rel30,breakout,gainer_rank,notified,episode_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)""",
        (int(time.time()),symbol,m["price"],score,m.get("chg30"),m.get("chg60"),m.get("chg5"),m.get("flow30"),
         m.get("buy30"),m.get("book_imbalance"),m.get("rel30"),int(bool(m.get("breakout",False))),gainers_prev_rank.get(symbol),states[symbol].episode_id or None),
    )
    rid = cur.lastrowid
    conn.commit(); conn.close()
    return rid


def mark_radar_notified(radar_id: int, daily_notice_no: Optional[int] = None):
    conn = db_connect()
    conn.execute("UPDATE radar_signals SET notified=1, notify_ts=?, daily_notice_no=? WHERE id=?", (int(time.time()), daily_notice_no, radar_id))
    conn.commit(); conn.close()


def save_radar_outcome(radar_id: int, horizon_s: int, ret: float, mfe: float, mae: float):
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO radar_outcomes(radar_id,horizon_s,return_pct,mfe_pct,mae_pct,ts) VALUES (?,?,?,?,?,?)",
        (radar_id,horizon_s,ret,mfe,mae,int(time.time())),
    )
    conn.commit(); conn.close()


def save_premium_radar_link(signal_id: int, symbol: str, premium_price: float, premium_ts: float, radar_id: int):
    if not radar_id:
        return
    conn = db_connect()
    row = conn.execute("SELECT ts,price,notified FROM radar_signals WHERE id=?", (radar_id,)).fetchone()
    if row:
        early_ts, early_price, notified = row
        dt = max(0.0, premium_ts - float(early_ts))
        cost = pct_change(premium_price, float(early_price)) if early_price else None
        conn.execute(
            """INSERT OR REPLACE INTO premium_radar_links
            (signal_id,radar_id,symbol,early_ts,premium_ts,early_price,premium_price,early_to_premium_s,price_cost_pct,early_notified,episode_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (signal_id,radar_id,symbol,int(early_ts),int(premium_ts),float(early_price),premium_price,dt,cost,int(notified or 0),states[symbol].episode_id or None),
        )
        conn.commit()
    conn.close()


def save_wave_tracking(p: PendingOutcome, drawdown_from_peak: float = 0.0, completed_60m: bool = False):
    vals = {0.5: None, 1.0: None, 1.5: None, 2.0: None}
    for level, hit_s in p.pullbacks_seen:
        vals[float(level)] = hit_s
    conn = db_connect()
    conn.execute(
        """INSERT INTO premium_wave_tracking
        (signal_id,peak_price,peak_mfe_pct,peak_s,pullback_0_5_s,pullback_1_0_s,pullback_1_5_s,pullback_2_0_s,
         max_drawdown_from_peak_pct,completed_60m,updated_ts,first_wave_peak_price,first_wave_peak_mfe_pct,
         first_wave_peak_s,first_wave_end_s,first_wave_end_reason,wave_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(signal_id) DO UPDATE SET
          peak_price=excluded.peak_price,peak_mfe_pct=excluded.peak_mfe_pct,peak_s=excluded.peak_s,
          pullback_0_5_s=COALESCE(premium_wave_tracking.pullback_0_5_s,excluded.pullback_0_5_s),
          pullback_1_0_s=COALESCE(premium_wave_tracking.pullback_1_0_s,excluded.pullback_1_0_s),
          pullback_1_5_s=COALESCE(premium_wave_tracking.pullback_1_5_s,excluded.pullback_1_5_s),
          pullback_2_0_s=COALESCE(premium_wave_tracking.pullback_2_0_s,excluded.pullback_2_0_s),
          max_drawdown_from_peak_pct=MAX(premium_wave_tracking.max_drawdown_from_peak_pct,excluded.max_drawdown_from_peak_pct),
          completed_60m=MAX(premium_wave_tracking.completed_60m,excluded.completed_60m),updated_ts=excluded.updated_ts,
          first_wave_peak_price=COALESCE(premium_wave_tracking.first_wave_peak_price,excluded.first_wave_peak_price),
          first_wave_peak_mfe_pct=COALESCE(premium_wave_tracking.first_wave_peak_mfe_pct,excluded.first_wave_peak_mfe_pct),
          first_wave_peak_s=COALESCE(premium_wave_tracking.first_wave_peak_s,excluded.first_wave_peak_s),
          first_wave_end_s=COALESCE(premium_wave_tracking.first_wave_end_s,excluded.first_wave_end_s),
          first_wave_end_reason=COALESCE(premium_wave_tracking.first_wave_end_reason,excluded.first_wave_end_reason),
          wave_count=MAX(COALESCE(premium_wave_tracking.wave_count,1),excluded.wave_count)""",
        (p.signal_id,p.peak_price,p.peak_mfe_pct,p.peak_s,vals[0.5],vals[1.0],vals[1.5],vals[2.0],drawdown_from_peak,
         1 if completed_60m else 0,int(time.time()),p.first_wave_peak_price or None,p.first_wave_peak_mfe_pct or None,
         p.first_wave_peak_s or None,p.first_wave_end_s,p.first_wave_end_reason or None,max(1,p.wave_no)),
    )
    conn.commit(); conn.close()



def save_shadow_event(p: PendingOutcome, event: str, age: float, price: float, ret: float, drawdown: float,
                      m: Optional[dict], score: Optional[int], reason: str, daily_notice_no: Optional[int] = None) -> int:
    m = m or {}
    conn = db_connect()
    cur = conn.execute(
        """INSERT INTO shadow_exit_events
        (ts,signal_id,symbol,event,age_s,price,return_pct,peak_mfe_pct,drawdown_from_peak_pct,score,chg30,chg60,flow30,buy30,book_imbalance,rel30,breakout,reason,daily_notice_no)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (int(time.time()),p.signal_id,p.symbol,event,age,price,ret,p.peak_mfe_pct,drawdown,score,m.get("chg30"),m.get("chg60"),
         m.get("flow30"),m.get("buy30"),m.get("book_imbalance"),m.get("rel30"),int(bool(m.get("breakout",False))),reason[:500],daily_notice_no),
    )
    event_id = int(cur.lastrowid)
    conn.commit(); conn.close()
    pending_shadow_events.append(PendingShadowEvent(event_id, p.symbol, price, time.time()))
    return event_id



def shadow_weakness_score(m: dict, score: int, drawdown: float) -> Tuple[int, List[str]]:
    points = 0
    reasons: List[str] = []
    if drawdown >= SHADOW_EXIT_DRAWDOWN_PCT:
        points += 2; reasons.append(f"tepeden -%{drawdown:.2f}")
    elif drawdown >= SHADOW_PROTECT_DRAWDOWN_PCT:
        points += 1; reasons.append(f"tepeden -%{drawdown:.2f}")
    if m.get("chg30", 0) <= -0.15:
        points += 2; reasons.append(f"30sn {m['chg30']:+.2f}%")
    if m.get("chg60", 0) <= 0.00:
        points += 1; reasons.append(f"60sn {m['chg60']:+.2f}%")
    if m.get("buy30", 1) < 0.52:
        points += 2; reasons.append(f"buy %{m['buy30']*100:.1f}")
    elif m.get("buy30", 1) < 0.58:
        points += 1; reasons.append(f"buy %{m['buy30']*100:.1f}")
    if m.get("flow30", 99) < 1.0:
        points += 1; reasons.append(f"flow {m['flow30']:.1f}x")
    if m.get("rel30", 0) < -0.15:
        points += 1; reasons.append(f"BTC relatif {m['rel30']:+.2f}%")
    if score < 55:
        points += 2; reasons.append(f"momentum {score}")
    elif score < 65:
        points += 1; reasons.append(f"momentum {score}")
    return points, reasons


def build_shadow_message(p: PendingOutcome, event: str, price: float, ret: float, drawdown: float, m: dict, score: int, reasons: List[str], notice_no: Optional[int] = None):
    title = "🧪 SHADOW — KÂR KORUMA ADAYI" if event == "PROTECT" else "🧪 SHADOW — ÇIKIŞ ADAYI"
    notice_line = f"🔔 Bu coin için günün {notice_no}. bildirimi\n" if notice_no else ""
    return (
        f"{title}\n\n"
        f"🪙 {p.symbol}\n"
        f"{notice_line}"
        f"💰 Premium: {fmt_price(p.entry_price)} | Anlık: {fmt_price(price)}\n"
        f"📈 Anlık getiri: {ret:+.2f}% | Session tepe: +%{p.peak_mfe_pct:.2f}\n"
        f"↘️ Tepeden geri çekilme: -%{drawdown:.2f}\n"
        f"⭐ Momentum: {score}/100 | 30sn {m['chg30']:+.2f}% | 60sn {m['chg60']:+.2f}%\n"
        f"💥 Flow {m['flow30']:.1f}x | Buy %{m['buy30']*100:.1f} | BTC relatif {m['rel30']:+.2f}%\n"
        f"🧭 Neden: {', '.join(reasons[:5]) if reasons else 'gözlemsel zayıflama'}\n\n"
        "⚠️ SHADOW TEST: Bu gerçek satış emri/sinyali değildir. Şimdilik işlem kararında dikkate alma; model doğrulaması için kaydediliyor.\n"
        f"⏰ {datetime.now(IST).strftime('%H:%M:%S')}"
    )



def save_candidate_event(symbol: str, event: str, m: Optional[dict] = None, score: Optional[int] = None, st: Optional[SymbolState] = None, note: str = ""):
    try:
        m = m or {}
        st = st or states[symbol]
        rank = gainers_prev_rank.get(symbol)
        age = (time.time() - st.candidate_since) if st.candidate_since else 0.0
        conn = db_connect()
        conn.execute(
            """INSERT INTO candidate_events
            (ts,symbol,event,price,score,chg30,chg60,chg5,flow30,buy30,book_imbalance,rel30,breakout,candidate_age_s,confirm_passes,gainer_rank,qv24,note,episode_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (int(time.time()), symbol, event, m.get("price"), score, m.get("chg30"), m.get("chg60"), m.get("chg5"),
             m.get("flow30"), m.get("buy30"), m.get("book_imbalance"), m.get("rel30"), int(bool(m.get("breakout", False))),
             age, st.candidate_passes, rank, m.get("qv24"), note[:500], st.episode_id or None),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug("candidate event save failed %s %s: %r", symbol, event, e)


def save_signal_meta(signal_id: int, m: dict):
    try:
        conn = db_connect()
        conn.execute(
            """INSERT OR REPLACE INTO signal_meta
            (signal_id,entry_quality,rise_score,candidate_runup,gainer_rank,qv24,premium,flow_eff30,flow_eff60,squeeze_risk,anchor_flow30)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (signal_id, m.get("entry_quality"), m.get("rise_score"), m.get("candidate_runup"),
             gainers_prev_rank.get(m.get("symbol", "")), m.get("qv24"), 1,
             m.get("flow_eff30"), m.get("flow_eff60"), int(bool(m.get("squeeze_risk",False))), m.get("anchor_flow30")),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug("signal meta save failed id=%s: %r", signal_id, e)


def save_gainers_event(symbol: str, event: str, rank_now: Optional[int], rank_old: Optional[int]) -> int:
    try:
        st = states[symbol]
        m = compute_metrics(symbol)
        score = score_metrics(m) if m else None
        vel = rank_velocity_per_min(symbol, rank_now)
        delta = (rank_old - rank_now) if rank_old and rank_now else None
        conn = db_connect()
        cur = conn.execute(
            """INSERT INTO gainers_events
            (ts,symbol,event,rank_now,rank_old,pct24,price,score,chg30,chg60,chg5,flow30,buy30,book_imbalance,rel30,breakout,rank_delta,rank_velocity_per_min)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (int(time.time()), symbol, event, rank_now, rank_old, st.pct24, st.last_price,
             score, m.get("chg30") if m else None, m.get("chg60") if m else None, m.get("chg5") if m else None,
             m.get("flow30") if m else None, m.get("buy30") if m else None, m.get("book_imbalance") if m else None,
             m.get("rel30") if m else None, int(bool(m.get("breakout",False))) if m else None, delta, vel),
        )
        event_id = int(cur.lastrowid)
        conn.commit(); conn.close()
        if st.last_price:
            pending_gainers.append(PendingGainer(event_id, symbol, st.last_price, time.time()))
        if RESEARCH_ENABLED and m:
            add_research_event("GAINERS_RESCAN", symbol, m, score, f"{event}; rank={rank_now}; old={rank_old}; velocity={vel:.2f}/min")
        return event_id
    except Exception as e:
        log.debug("gainers event save failed %s %s: %r", symbol, event, e)
        return 0



def next_daily_notice_no(symbol: str, kind: str) -> int:
    """Persist a single per-symbol, per-local-day ordinal across all user-facing message kinds."""
    local_date = datetime.now(IST).date().isoformat()
    conn = db_connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT COALESCE(MAX(ordinal),0) FROM notification_log WHERE local_date=? AND symbol=?",
            (local_date, symbol),
        ).fetchone()
        ordinal = int((row[0] or 0) + 1)
        conn.execute(
            "INSERT INTO notification_log(ts,local_date,symbol,kind,ordinal) VALUES (?,?,?,?,?)",
            (int(time.time()), local_date, symbol, kind, ordinal),
        )
        conn.commit()
        return ordinal
    finally:
        conn.close()


def rank_velocity_per_min(symbol: str, rank_now: Optional[int] = None, window_seconds: int = 600) -> float:
    hist = gainers_rank_history.get(symbol)
    if not hist:
        return 0.0
    now = time.time()
    current_rank = rank_now if rank_now is not None else (hist[-1][1] if hist else None)
    if current_rank is None:
        return 0.0
    target = now - window_seconds
    old = None
    for sample in hist:
        if sample[0] <= target:
            old = sample
        else:
            break
    if old is None:
        old = hist[0]
    elapsed_min = max((now - old[0]) / 60.0, 1e-6)
    return (float(old[1]) - float(current_rank)) / elapsed_min


def start_episode(symbol: str, m: dict, score: int) -> int:
    st = states[symbol]
    if st.episode_id:
        return st.episode_id
    conn = db_connect()
    cur = conn.execute(
        """INSERT INTO momentum_episodes(symbol,start_ts,start_price,start_score,peak_price,peak_return_pct,anchor_avg1m)
        VALUES (?,?,?,?,?,?,?)""",
        (symbol, int(time.time()), m.get("price"), score, m.get("price"), 0.0, m.get("avg1m")),
    )
    eid = int(cur.lastrowid)
    conn.commit(); conn.close()
    st.episode_id = eid
    st.episode_started_ts = time.time()
    st.episode_start_price = float(m.get("price") or 0.0)
    st.episode_peak_price = st.episode_start_price
    st.episode_had_early = False
    st.episode_had_premium = False
    st.episode_anchor_avg1m = float(m.get("avg1m") or 0.0)
    st.anchor_avg1m = st.episode_anchor_avg1m
    st.anchor_ts = time.time()
    return eid


def update_episode_peak(symbol: str, price: float):
    st = states[symbol]
    if not st.episode_id or not price:
        return
    if price > st.episode_peak_price:
        st.episode_peak_price = price


def mark_episode_early(symbol: str):
    st = states[symbol]
    if not st.episode_id:
        return
    st.episode_had_early = True
    conn = db_connect()
    conn.execute("UPDATE momentum_episodes SET had_early=1 WHERE id=?", (st.episode_id,))
    conn.commit(); conn.close()


def mark_episode_premium(symbol: str):
    st = states[symbol]
    if not st.episode_id:
        return
    st.episode_had_premium = True
    conn = db_connect()
    conn.execute("UPDATE momentum_episodes SET had_premium=1 WHERE id=?", (st.episode_id,))
    conn.commit(); conn.close()


def end_episode(symbol: str, reason: str, m: Optional[dict] = None, score: Optional[int] = None):
    st = states[symbol]
    if not st.episode_id:
        return
    price = float((m or {}).get("price") or st.last_price or 0.0)
    update_episode_peak(symbol, price)
    peak_ret = pct_change(st.episode_peak_price, st.episode_start_price) if st.episode_start_price else 0.0
    conn = db_connect()
    conn.execute(
        """UPDATE momentum_episodes SET end_ts=?,end_price=?,end_reason=?,had_early=?,had_premium=?,peak_price=?,peak_return_pct=? WHERE id=?""",
        (int(time.time()), price, reason[:100], int(st.episode_had_early), int(st.episode_had_premium),
         st.episode_peak_price or None, peak_ret, st.episode_id),
    )
    conn.commit(); conn.close()
    if st.episode_had_early or st.episode_had_premium:
        st.prev_meaningful_episode_id = st.episode_id
        st.prev_meaningful_ts = time.time()
        st.prev_meaningful_price = price or st.episode_start_price
        st.prev_meaningful_peak_price = st.episode_peak_price
    st.episode_id = 0
    st.episode_started_ts = 0.0
    st.episode_start_price = 0.0
    st.episode_peak_price = 0.0
    st.episode_had_early = False
    st.episode_had_premium = False
    st.episode_anchor_avg1m = 0.0


def save_gainers_outcome(event_id: int, horizon_s: int, ret: float, mfe: float, mae: float):
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO gainers_outcomes(event_id,horizon_s,return_pct,mfe_pct,mae_pct,ts) VALUES (?,?,?,?,?,?)",
        (event_id,horizon_s,ret,mfe,mae,int(time.time())),
    )
    conn.commit(); conn.close()


def save_research_outcome(event_id: int, horizon_s: int, ret: float, mfe: float, mae: float):
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO research_outcomes(event_id,horizon_s,return_pct,mfe_pct,mae_pct,ts) VALUES (?,?,?,?,?,?)",
        (event_id,horizon_s,ret,mfe,mae,int(time.time())),
    )
    conn.commit(); conn.close()


def save_shadow_event_outcome(event_id: int, horizon_s: int, ret: float, mfe: float, mae: float):
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO shadow_event_outcomes(shadow_event_id,horizon_s,return_pct,mfe_pct,mae_pct,ts) VALUES (?,?,?,?,?,?)",
        (event_id,horizon_s,ret,mfe,mae,int(time.time())),
    )
    conn.commit(); conn.close()


def save_wave_event(p: PendingOutcome, end_s: float, end_price: float, drawdown_pct: float, reason: str):
    conn = db_connect()
    conn.execute(
        """INSERT OR REPLACE INTO premium_wave_events
        (signal_id,wave_no,start_s,start_price,peak_s,peak_price,peak_mfe_pct,end_s,end_price,drawdown_pct,end_reason)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (p.signal_id,p.wave_no,p.wave_start_s,p.wave_start_price,p.wave_peak_s,p.wave_peak_price,
         pct_change(p.wave_peak_price, p.entry_price) if p.wave_peak_price else 0.0,end_s,end_price,drawdown_pct,reason[:100]),
    )
    conn.commit(); conn.close()


def add_research_event(event_type: str, symbol: str, m: dict, score: Optional[int], note: str = "") -> int:
    if not RESEARCH_ENABLED or not m or not m.get("price"):
        return 0
    vel = rank_velocity_per_min(symbol, gainers_prev_rank.get(symbol))
    conn = db_connect()
    cur = conn.execute(
        """INSERT INTO research_events
        (ts,symbol,event_type,episode_id,price,score,chg10,chg30,chg60,chg5,chg15,flow10,flow30,flow60,buy30,
         book_imbalance,rel30,spread,breakout,gainer_rank,rank_velocity,compression_ratio,dist15high_pct,
         flow_eff30,flow_eff60,anchor_flow30,oi5,note)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (int(time.time()),symbol,event_type,states[symbol].episode_id or None,m.get("price"),score,m.get("chg10"),m.get("chg30"),
         m.get("chg60"),m.get("chg5"),m.get("chg15"),m.get("flow10"),m.get("flow30"),m.get("flow60"),m.get("buy30"),
         m.get("book_imbalance"),m.get("rel30"),m.get("spread"),int(bool(m.get("breakout",False))),gainers_prev_rank.get(symbol),
         vel,m.get("compression_ratio"),m.get("dist15high_pct"),m.get("flow_eff30"),m.get("flow_eff60"),m.get("anchor_flow30"),
         m.get("oi5"),note[:500]),
    )
    event_id = int(cur.lastrowid)
    conn.commit(); conn.close()
    pending_research.append(PendingResearch(event_id, symbol, float(m["price"]), time.time()))
    return event_id


def maybe_record_research(symbol: str, m: dict, score: int, now: float):
    """Shadow-only hypothesis collection; it never changes qualifies/continuity/Premium decisions."""
    if not RESEARCH_ENABLED:
        return
    st = states[symbol]
    if PREBREAKOUT_ENABLED and now - st.last_prebreakout_ts >= PREBREAKOUT_COOLDOWN_SECONDS:
        pre = (
            m.get("compression_ratio", 99) <= 0.70
            and m.get("dist15high_pct", 99) <= 0.40
            and -0.10 <= m.get("chg30", 0) <= 0.35
            and m.get("flow30", 0) >= 1.15
            and 0.55 <= m.get("buy30", 0) <= 0.80
            and m.get("rel30", 0) >= 0.0
            and m.get("spread", 99) <= min(MAX_SPREAD_PCT, 0.30)
            and not m.get("extended", False)
        )
        if pre:
            st.last_prebreakout_ts = now
            add_research_event("PRE_BREAKOUT", symbol, m, score, "compression + near 15m high; shadow only")

    if now - st.last_flow_structure_ts >= FLOW_STRUCTURE_COOLDOWN_SECONDS:
        low_progress = m.get("flow30",0) >= 6.0 and abs(m.get("chg30",0)) <= 0.18
        saturated = m.get("buy30",0) >= 0.82 and m.get("flow30",0) >= 4.0 and m.get("chg30",0) < 0.30
        if low_progress or saturated:
            st.last_flow_structure_ts = now
            add_research_event("FLOW_LOW_PROGRESS", symbol, m, score,
                               f"flow={m.get('flow30',0):.2f}x; chg30={m.get('chg30',0):+.3f}; eff30={m.get('flow_eff30',0):+.4f}")

    if SECOND_WAVE_ENABLED and st.prev_meaningful_ts and now - st.prev_meaningful_ts <= SECOND_WAVE_MAX_GAP_SECONDS:
        if now - st.last_second_wave_ts >= SECOND_WAVE_COOLDOWN_SECONDS:
            reaccel = (
                (m.get("chg30",0) >= 0.22 or m.get("chg60",0) >= 0.40)
                and m.get("flow30",0) >= 1.40
                and m.get("buy30",0) >= 0.56
                and m.get("spread",99) <= min(MAX_SPREAD_PCT,0.30)
                and not m.get("extended",False)
            )
            if reaccel:
                st.last_second_wave_ts = now
                gap = now - st.prev_meaningful_ts
                add_research_event("SECOND_WAVE", symbol, m, score,
                                   f"prev_episode={st.prev_meaningful_episode_id}; gap={gap:.0f}s; prev_peak={st.prev_meaningful_peak_price:.10g}")


def update_pending_tick(symbol: str, price: float, tick_ts: float):
    """Event-level MFE/MAE and Premium path accounting from aggTrade. No signal decision is made here."""
    if not price:
        return
    for p in list(pending_outcomes):
        if p.symbol != symbol or tick_ts < p.created_ts:
            continue
        age = max(0.0, tick_ts - p.created_ts)
        ret = pct_change(price, p.entry_price)
        p.mfe = max(p.mfe, ret)
        p.mae = min(p.mae, ret)
        if price > (p.peak_price or p.entry_price):
            p.peak_price = price
            p.peak_mfe_pct = max(p.peak_mfe_pct, ret)
            p.peak_s = age
            p.wave_dirty = True
        if p.wave_active and price > (p.wave_peak_price or p.wave_start_price or p.entry_price):
            p.wave_peak_price = price
            p.wave_peak_s = age
        path_changed = False
        if p.entry_touch_s is None:
            if p.target1 and p.target_before_entry_s is None and price >= p.target1:
                p.target_before_entry_s = age
                p.first_event = p.first_event or "TARGET_BEFORE_ENTRY"
                path_changed = True
            elif p.entry_high and price <= p.entry_high:
                if p.invalidation and price <= p.invalidation:
                    if p.invalidation_hit_s is None:
                        p.invalidation_hit_s = age
                    p.first_event = p.first_event or "INVALIDATION_BEFORE_ENTRY"
                else:
                    p.entry_touch_s = age
                    p.path_entry_price = price
                path_changed = True
        if p.entry_touch_s is not None:
            trade_ret = pct_change(price, p.path_entry_price or p.entry_price)
            p.trade_mfe = max(p.trade_mfe, trade_ret)
            p.trade_mae = min(p.trade_mae, trade_ret)
            if p.tp1_hit_s is None:
                p.mfe_before_tp1 = max(p.mfe_before_tp1, trade_ret)
                p.mae_before_tp1 = min(p.mae_before_tp1, trade_ret)
            if p.target1 and p.tp1_hit_s is None and price >= p.target1:
                p.tp1_hit_s = age
                p.first_event = p.first_event or "TP1"
                path_changed = True
            if p.target2 and p.tp2_hit_s is None and price >= p.target2:
                p.tp2_hit_s = age
                path_changed = True
            if p.invalidation and p.invalidation_hit_s is None and price <= p.invalidation:
                p.invalidation_hit_s = age
                p.first_event = p.first_event or "INVALIDATION"
                path_changed = True
        if path_changed:
            save_signal_path(p)

    # Observer-only trackers also benefit from tick-level extrema, while their horizon snapshots remain scheduled.
    for collection in (pending_radars, pending_gainers, pending_research, pending_shadow_events):
        for obj in list(collection):
            if obj.symbol != symbol or tick_ts < obj.created_ts:
                continue
            ret = pct_change(price, obj.entry_price)
            obj.mfe = max(obj.mfe, ret)
            obj.mae = min(obj.mae, ret)



def recover_pending_tracking():
    """Recover observer/path trackers after a restart when DB_PATH is persistent. Candidate continuity itself is never reconstructed."""
    now = time.time()
    conn = db_connect()
    try:
        conn.execute("UPDATE momentum_episodes SET end_ts=?,end_reason=COALESCE(end_reason,'RESTART_BOUNDARY') WHERE end_ts IS NULL", (int(now),))
        conn.commit()
        rows = conn.execute(
            """SELECT s.id,s.symbol,s.price,s.ts,
               p.entry_low,p.entry_high,p.entry_touch_s,p.path_entry_price,p.target1,p.target2,p.invalidation,p.target_before_entry_s,
               p.tp1_hit_s,p.tp2_hit_s,p.invalidation_hit_s,p.first_event,p.mfe_before_tp1,p.mae_before_tp1,p.trade_mfe,p.trade_mae,
               w.peak_price,w.peak_mfe_pct,w.peak_s,w.pullback_0_5_s,w.pullback_1_0_s,w.pullback_1_5_s,w.pullback_2_0_s,
               w.first_wave_peak_price,w.first_wave_peak_mfe_pct,w.first_wave_peak_s,w.first_wave_end_s,w.first_wave_end_reason,w.wave_count
               FROM signals_v2 s
               LEFT JOIN signal_paths p ON p.signal_id=s.id
               LEFT JOIN premium_wave_tracking w ON w.signal_id=s.id
               WHERE s.ts>=? AND NOT EXISTS(SELECT 1 FROM signal_outcomes o WHERE o.signal_id=s.id AND o.horizon_s=3600)""",
            (int(now)-3700,),
        ).fetchall()
        for r in rows:
            sid,sym,entry,ts = r[:4]
            if sym not in states: continue
            p=PendingOutcome(signal_id=sid,symbol=sym,entry_price=float(entry),created_ts=float(ts),
                entry_low=float(r[4] or 0),entry_high=float(r[5] or 0),entry_touch_s=r[6],path_entry_price=float(r[7] or 0),
                target1=float(r[8] or 0),target2=float(r[9] or 0),invalidation=float(r[10] or 0),target_before_entry_s=r[11],
                tp1_hit_s=r[12],tp2_hit_s=r[13],invalidation_hit_s=r[14],first_event=r[15],
                mfe_before_tp1=float(r[16] or 0),mae_before_tp1=float(r[17] or 0),trade_mfe=float(r[18] or 0),trade_mae=float(r[19] or 0))
            mm=conn.execute("SELECT MAX(mfe_pct),MIN(mae_pct) FROM signal_outcomes WHERE signal_id=?",(sid,)).fetchone()
            p.mfe=float(mm[0] or 0); p.mae=float(mm[1] or 0)
            p.completed={int(x[0]) for x in conn.execute("SELECT horizon_s FROM signal_outcomes WHERE signal_id=?",(sid,)).fetchall()}
            p.peak_price=float(r[20] or entry); p.peak_mfe_pct=float(r[21] or p.mfe); p.peak_s=float(r[22] or 0)
            for level,val in zip(WAVE_PULLBACK_LEVELS,r[23:27]):
                if val is not None: p.pullbacks_seen.add((float(level),float(val)))
            p.first_wave_peak_price=float(r[27] or 0); p.first_wave_peak_mfe_pct=float(r[28] or 0); p.first_wave_peak_s=float(r[29] or 0)
            p.first_wave_end_s=r[30]; p.first_wave_end_reason=r[31] or ""; p.wave_no=int(r[32] or 1)
            p.wave_start_price=float(entry); p.wave_peak_price=p.peak_price or float(entry); p.wave_peak_s=p.peak_s
            if p.first_wave_end_s is not None:
                p.wave_active = False
                p.wave_last_end_price = states[sym].last_price or p.peak_price or float(entry)
                p.wave_last_end_s = max(0.0, now-float(ts))
            sev={x[0] for x in conn.execute("SELECT event FROM shadow_exit_events WHERE signal_id=?",(sid,)).fetchall()}
            p.shadow_protect_sent = "PROTECT" in sev
            p.shadow_exit_sent = "EXIT" in sev
            cts=conn.execute("SELECT 1 FROM candidate_events WHERE symbol=? AND event='continuation_alert' AND ts BETWEEN ? AND ? LIMIT 1",(sym,int(ts),int(ts)+1800)).fetchone()
            p.continuation_sent = bool(cts)
            pending_outcomes.append(p)

        for table,idcol,klass,target,hmin,hmax in [
            ("radar_signals","id",PendingRadar,pending_radars,60,3600),
            ("gainers_events","id",PendingGainer,pending_gainers,60,3600),
            ("research_events","id",PendingResearch,pending_research,60,3600),
            ("shadow_exit_events","id",PendingShadowEvent,pending_shadow_events,30,900),
        ]:
            outcome_table={"radar_signals":"radar_outcomes","gainers_events":"gainers_outcomes","research_events":"research_outcomes","shadow_exit_events":"shadow_event_outcomes"}[table]
            fk={"radar_signals":"radar_id","gainers_events":"event_id","research_events":"event_id","shadow_exit_events":"shadow_event_id"}[table]
            price_col="price"
            q=(f"SELECT {idcol},symbol,{price_col},ts FROM {table} e WHERE ts>=? "
               f"AND NOT EXISTS(SELECT 1 FROM {outcome_table} o WHERE o.{fk}=e.{idcol} AND o.horizon_s=?) "
               f"AND (e.ts>=? OR EXISTS(SELECT 1 FROM {outcome_table} o2 WHERE o2.{fk}=e.{idcol}))")
            for eid,sym,entry,ts in conn.execute(q,(int(now)-hmax-120,hmax,int(now)-hmin)).fetchall():
                if sym not in states or not entry: continue
                obj=klass(int(eid),sym,float(entry),float(ts))
                obj.completed={int(x[0]) for x in conn.execute(f"SELECT horizon_s FROM {outcome_table} WHERE {fk}=?",(eid,)).fetchall()}
                # Preserve any already-known extrema across restart.
                mm=conn.execute(f"SELECT MAX(mfe_pct),MIN(mae_pct) FROM {outcome_table} WHERE {fk}=?",(eid,)).fetchone()
                obj.mfe=float(mm[0] or 0); obj.mae=float(mm[1] or 0)
                target.append(obj)
        log.info("Recovered trackers: premium=%d radar=%d gainers=%d research=%d shadow=%d",
                 len(pending_outcomes),len(pending_radars),len(pending_gainers),len(pending_research),len(pending_shadow_events))
    except Exception as e:
        log.warning("Tracker recovery failed: %r", e)
    finally:
        conn.close()


telegram_send_lock = asyncio.Lock()


async def telegram_send(session: aiohttp.ClientSession, text: str, symbol: Optional[str] = None) -> bool:
    """Send a Telegram message reliably.

    Retries transient network/5xx/429 failures and logs the real exception type,
    HTTP status and Telegram response body so Railway logs are actionable.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials missing; alert printed only:\n%s", text)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    if symbol:
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {"text": "Binance Futures", "url": "https://www.binance.com/en/futures/" + symbol}
            ]]
        }

    timeout = aiohttp.ClientTimeout(total=15, connect=6, sock_read=10)
    max_attempts = 4

    # Serialize Telegram writes. This prevents several gainers/signal/command
    # messages from hitting Telegram at exactly the same moment.
    async with telegram_send_lock:
        for attempt in range(1, max_attempts + 1):
            try:
                async with session.post(url, json=payload, timeout=timeout) as r:
                    body = await r.text()
                    if r.status == 200:
                        try:
                            data = json.loads(body)
                        except Exception:
                            data = {"ok": True}
                        if data.get("ok", True):
                            return True
                        log.warning("Telegram API ok=false attempt=%d body=%s", attempt, body[:1000])
                    elif r.status == 429:
                        retry_after = 2
                        try:
                            data = json.loads(body)
                            retry_after = int(data.get("parameters", {}).get("retry_after", 2))
                        except Exception:
                            pass
                        log.warning("Telegram rate limited (429), retry_after=%ss body=%s", retry_after, body[:1000])
                        if attempt < max_attempts:
                            await asyncio.sleep(min(max(retry_after, 1), 30))
                            continue
                    else:
                        log.warning("Telegram HTTP %s attempt=%d body=%s", r.status, attempt, body[:1000])
                        # 4xx errors other than 429 are usually permanent for this payload.
                        if 400 <= r.status < 500:
                            return False
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(
                    "Telegram send exception attempt=%d/%d type=%s repr=%r",
                    attempt, max_attempts, type(e).__name__, e,
                )

            if attempt < max_attempts:
                await asyncio.sleep(min(2 ** (attempt - 1), 8))

    log.error("Telegram message abandoned after %d attempts; preview=%r", max_attempts, text[:160])
    return False


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


def compression_context(st: SymbolState) -> Tuple[float, float]:
    c = list(st.candles)
    price = st.last_price
    if not price or len(c) < 15:
        return 1.0, 999.0
    recent = c[-5:]
    prior = c[-15:-5]
    recent_range = ((max(x.high for x in recent) - min(x.low for x in recent)) / max(price, 1e-12)) * 100.0
    prior_range = ((max(x.high for x in prior) - min(x.low for x in prior)) / max(price, 1e-12)) * 100.0 if prior else recent_range
    compression_ratio = recent_range / max(prior_range, 1e-9)
    prior_high = max(x.high for x in c[-15:])
    dist15high_pct = max(0.0, ((prior_high - price) / max(price, 1e-12)) * 100.0)
    return compression_ratio, dist15high_pct


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
    compression_ratio, dist15high_pct = compression_context(st)
    flow_eff30 = chg30 / max(flow30, 0.10)
    flow_eff60 = chg60 / max(flow30, 0.10)
    anchor_flow30 = 0.0
    if st.anchor_avg1m > 0 and (time.time() - st.anchor_ts) <= ANCHOR_MAX_AGE_SECONDS:
        anchor_expected30 = st.anchor_avg1m / 2.0
        anchor_flow30 = q30 / anchor_expected30 if anchor_expected30 else 0.0

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
        "funding_rate_pct": st.funding_rate_pct if st.funding_ts and time.time()-st.funding_ts < 120 else None,
        "compression_ratio": compression_ratio, "dist15high_pct": dist15high_pct,
        "flow_eff30": flow_eff30, "flow_eff60": flow_eff60, "anchor_flow30": anchor_flow30,
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
    st.active_radar_id = 0
    st.active_radar_notified = False


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
    if len(prices) >= 3 and prices[-1] > prices[-2] > prices[-3]: r += 8
    if m["extended"]: r -= 15
    return max(0, min(100, r))
def entry_quality(m: dict, score: int, st: SymbolState) -> int:
    # Entry quality is deliberately separate from momentum intensity.
    q = 48
    prices = list(st.candidate_prices)
    if len(prices) >= 3:
        steps = [prices[i] / prices[i-1] - 1 for i in range(1, len(prices))]
        recent_steps = steps[-min(3, len(steps)):]
        if all(x >= -0.0005 for x in recent_steps) and sum(x > 0 for x in recent_steps) >= max(1, len(recent_steps)-1):
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


def candidate_runup_pct(st: SymbolState, current_price: float) -> float:
    prices = list(st.candidate_prices)
    if not prices or prices[0] <= 0:
        return 0.0
    return pct_change(current_price, prices[0])


def early_watch_pass(m: dict, score: int) -> bool:
    return (
        EARLY_ALERT_ENABLED
        and score >= EARLY_ALERT_MIN_SCORE
        and m["chg30"] >= EARLY_ALERT_MIN_CHG30
        and m["chg60"] >= EARLY_ALERT_MIN_CHG60
        and m["flow30"] >= EARLY_ALERT_MIN_FLOW30
        and EARLY_ALERT_MIN_BUY30 <= m["buy30"] <= EARLY_ALERT_MAX_BUY30
        and m["book_imbalance"] <= EARLY_ALERT_MAX_BOOK
        and m["spread"] <= min(MAX_SPREAD_PCT, 0.30)
        and not m["extended"]
        and (m["breakout"] or m["rel30"] >= 0.15)
    )


def early_notify_pass(m: dict, score: int, st: SymbolState) -> bool:
    """User-facing early alert: stricter than internal radar and requires 2/3 continuity."""
    return (
        st.candidate_passes >= 2
        and score >= EARLY_NOTIFY_MIN_SCORE
        and m["chg30"] >= EARLY_NOTIFY_MIN_CHG30
        and m["chg60"] >= EARLY_NOTIFY_MIN_CHG60
        and m["flow30"] >= EARLY_NOTIFY_MIN_FLOW30
        and EARLY_NOTIFY_MIN_BUY30 <= m["buy30"] <= EARLY_NOTIFY_MAX_BUY30
        and m["chg5"] <= EARLY_NOTIFY_MAX_CHG5
        and m["book_imbalance"] <= EARLY_ALERT_MAX_BOOK
        and m["spread"] <= min(MAX_SPREAD_PCT, 0.30)
        and not m["extended"]
        and (m["breakout"] or m["rel30"] >= 0.20)
    )


def premium_trade_guard(m: dict, score: int, quality: int, rise_score: int, st: SymbolState):
    reasons = []
    runup = candidate_runup_pct(st, m["price"])
    if score < PREMIUM_MIN_MOMENTUM_SCORE: reasons.append(f"momentum {score}<{PREMIUM_MIN_MOMENTUM_SCORE}")
    if quality < PREMIUM_ENTRY_MIN_SCORE: reasons.append(f"giriş kalitesi {quality}<{PREMIUM_ENTRY_MIN_SCORE}")
    if rise_score < PREMIUM_RISE_MIN_SCORE: reasons.append(f"yükseliş {rise_score}<{PREMIUM_RISE_MIN_SCORE}")
    if PREMIUM_REQUIRE_BREAKOUT and not m["breakout"]: reasons.append("15dk breakout yok")
    if not (PREMIUM_MIN_BUY30 <= m["buy30"] <= PREMIUM_MAX_BUY30): reasons.append("agresif alış tatlı bölge dışında")
    if m["book_imbalance"] > PREMIUM_MAX_BOOK_IMBALANCE: reasons.append("bid baskısı aşırı/tek taraflı")
    if not (PREMIUM_MIN_CHG30 <= m["chg30"] <= PREMIUM_MAX_CHG30): reasons.append("30sn hareket uygun aralık dışında")
    if not (PREMIUM_MIN_CHG60 <= m["chg60"] <= PREMIUM_MAX_CHG60): reasons.append("60sn hareket uygun aralık dışında")
    if m["flow30"] < PREMIUM_MIN_FLOW30: reasons.append("hacim akışı yetersiz")
    if runup > PREMIUM_MAX_CANDIDATE_RUNUP_PCT: reasons.append(f"adaydan beri +%{runup:.2f} uzamış")
    if m["extended"]: reasons.append("hareket uzamış")
    return len(reasons) == 0, reasons, runup


def build_early_message(m: dict, score: int, st: SymbolState):
    rank = gainers_prev_rank.get(m["symbol"])
    rank_line = f"🏆 Gainers sırası: #{rank}" if rank else "🏆 Gainers: TOP sıralamada değil/henüz veri yok"
    notice = m.get("daily_notice_no")
    notice_line = f"🔔 Bu coin için günün {notice}. bildirimi\n" if notice else ""
    return (
        "👀 ERKEN MOMENTUM — İZLE / TEYİT BEKLE\n\n"
        f"🪙 {m['symbol']}\n"
        f"{notice_line}"
        f"💰 Fiyat: {fmt_price(m['price'])}\n\n"
        f"⚡ 30 sn: {m['chg30']:+.2f}% | 60 sn: {m['chg60']:+.2f}%\n"
        f"📈 5 dk: {m['chg5']:+.2f}%\n"
        f"💥 Hacim akışı: {m['flow30']:.1f}x\n"
        f"🟢 Agresif alış: %{m['buy30']*100:.1f}\n"
        f"₿ BTC relatif güç: {m['rel30']:+.2f}%\n"
        f"{rank_line}\n"
        f"⭐ Momentum: {score}/100\n\n"
        "Bu bir alım sinyali değildir. Bot hareketin erken safhasını 2/3 süreklilikte fark etti; Premium için 3/3 süreklilik ve işlem kalitesi teyidi bekleniyor.\n"
        f"⏰ {datetime.now(IST).strftime('%H:%M:%S')}"
    )



def build_continuation_message(p: PendingOutcome, m: dict, score: int):
    notice = m.get("daily_notice_no")
    notice_line = f"🔔 Bu coin için günün {notice}. bildirimi\n" if notice else ""
    return (
        "🚀 MOMENTUM DEVAMI — HEDEF SONRASI GÜÇ SÜRÜYOR\n\n"
        f"🪙 {p.symbol}\n"
        f"{notice_line}"
        f"💰 İlk sinyal: {fmt_price(p.entry_price)} | Anlık: {fmt_price(m['price'])}\n"
        f"📈 Sinyal sonrası MFE: +%{p.mfe:.2f}\n"
        f"⚡ 30 sn: {m['chg30']:+.2f}% | 60 sn: {m['chg60']:+.2f}%\n"
        f"💥 Flow: {m['flow30']:.1f}x | Buy: %{m['buy30']*100:.1f}\n"
        f"⭐ Momentum: {score}/100\n\n"
        "Bu mesaj yeni giriş çağrısı değildir; teyitli hareketin TP2 sonrasında da canlı kaldığını belirtir.\n"
        f"⏰ {datetime.now(IST).strftime('%H:%M:%S')}"
    )



def build_manual_analysis(symbol: str, m: dict, score: int, quality: int, rise_score: int, plan: dict):
    st = states[symbol]
    early = early_watch_pass(m, score)
    # Manual analysis has no guaranteed 3/3 history; verdict intentionally remains conservative.
    if st.candidate_passes >= CONFIRM_REQUIRED:
        guard_ok, reasons, runup = premium_trade_guard(m, score, quality, rise_score, st)
    else:
        guard_ok, reasons, runup = False, [f"süreklilik {st.candidate_passes}/{CONFIRM_REQUIRED}"], candidate_runup_pct(st, m["price"])
    verdict = "🟢 Güçlü kurulum" if guard_ok else ("🟡 Erken momentum / teyit bekle" if early else "⚪ Şu an premium giriş teyidi yok")
    rank = gainers_prev_rank.get(symbol)
    why = "; ".join(reasons[:3]) if reasons else "premium filtreler uyumlu"
    oi_line = "veri yok" if m.get("oi5") is None else f"{m['oi5']:+.2f}%"
    return (
        f"🔎 {symbol} — ANLIK ANALİZ\n\n"
        f"{verdict}\n"
        f"⭐ Momentum: {score}/100 | 📈 Yükseliş: {rise_score}/100 | 🎯 Giriş: {quality}/100\n"
        f"✅ Süreklilik: {st.candidate_passes}/{CONFIRM_REQUIRED} | aday run-up: {runup:+.2f}%\n\n"
        f"⚡ 30 sn {m['chg30']:+.2f}% | 60 sn {m['chg60']:+.2f}% | 5 dk {m['chg5']:+.2f}%\n"
        f"💥 Flow {m['flow30']:.1f}x | 🟢 Buy %{m['buy30']*100:.1f} | 📚 Bid %{m['book_imbalance']*100:.1f}\n"
        f"₿ BTC relatif {m['rel30']:+.2f}% | OI 5dk {oi_line} | Breakout {'evet' if m['breakout'] else 'hayır'}\n"
        f"🏆 Gainers: {'#'+str(rank) if rank else '—'}\n\n"
        f"🧭 Neden: {why}\n\n"
        "📍 Kural tabanlı bölge\n"
        f"🟩 {fmt_price(plan['entry_low'])} – {fmt_price(plan['entry_high'])}\n"
        f"🎯 TP1 {fmt_price(plan['target1'])} | TP2 {fmt_price(plan['target2'])}\n"
        f"🛑 Geçersizlik {fmt_price(plan['invalidation'])}\n\n"
        "Not: Bu analiz emir vermez ve kâr garantisi değildir; özellikle teyit yoksa bekleme/risk kontrolü daha önemlidir."
    )


def estimate_trade_plan(symbol: str, m: dict) -> dict:
    """Rule-based indicative entry/target levels from recent volatility; not an order recommendation."""
    st = states[symbol]
    price = float(m["price"])
    cs = list(st.candles)[-10:]
    ranges = [((c.high - c.low) / c.close) * 100 for c in cs if c.close > 0]
    atr_pct = mean(ranges[-5:]) if ranges else max(0.45, abs(m.get("chg60", 0.0)))
    atr_pct = max(0.30, min(2.50, atr_pct))

    recent_lows = [c.low for c in cs[-3:] if c.low > 0]
    recent_highs = [c.high for c in cs[-5:] if c.high > 0]
    support = min(recent_lows) if recent_lows else price * (1 - atr_pct / 100)
    resistance = max(recent_highs) if recent_highs else price * (1 + atr_pct / 100)

    # Prefer a small pullback instead of chasing the current tick.
    pullback = max(0.15, min(0.60, atr_pct * 0.35))
    entry_low = price * (1 - pullback / 100)
    entry_high = price * (1 - 0.03 / 100)
    if m.get("chg30", 0) < 0.45 and not m.get("extended"):
        entry_high = price * (1 + 0.05 / 100)

    # Do not place the lower edge materially below nearby short-term support.
    if support < price:
        entry_low = max(entry_low, support * 0.998)
    if entry_low >= entry_high:
        entry_low = price * (1 - max(0.15, pullback) / 100)
        entry_high = price

    entry_mid = (entry_low + entry_high) / 2
    stop_risk_pct = max(0.55, min(1.50, atr_pct * 0.80))
    invalidation = entry_mid * (1 - stop_risk_pct / 100)
    if support < entry_mid:
        support_stop = support * 0.997
        # Keep invalidation close enough to remain a short-term momentum setup.
        invalidation = max(invalidation, support_stop)

    actual_risk = max(0.25, pct_change(entry_mid, invalidation))
    actual_risk = abs(actual_risk)
    t1_pct = max(0.65, actual_risk * 1.15, atr_pct * 0.75)
    t2_pct = max(1.20, actual_risk * 1.90, atr_pct * 1.35)
    target1 = entry_mid * (1 + t1_pct / 100)
    target2 = entry_mid * (1 + t2_pct / 100)
    if resistance > entry_mid:
        target1 = max(target1, resistance * 1.001)
        target2 = max(target2, target1 * (1 + max(0.45, atr_pct * 0.55) / 100))

    rr1 = (target1 - entry_mid) / max(1e-12, entry_mid - invalidation)
    rr2 = (target2 - entry_mid) / max(1e-12, entry_mid - invalidation)
    return {
        "entry_low": entry_low,
        "entry_high": entry_high,
        "invalidation": invalidation,
        "target1": target1,
        "target2": target2,
        "rr1": rr1,
        "rr2": rr2,
        "atr_pct": atr_pct,
    }


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
    plan = m.get("trade_plan") or estimate_trade_plan(m["symbol"], m)
    notice = m.get("daily_notice_no")
    notice_line = f"🔔 Bu coin için günün {notice}. bildirimi\n" if notice else ""
    return (
        "🟢 ALIM FIRSATI — PREMIUM + SÜREKLİLİK TEYİTLİ\n\n"
        f"🪙 {m['symbol']}\n"
        f"{notice_line}"
        f"💰 Anlık fiyat: {fmt_price(m['price'])}\n\n"
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
        f"🎯 Giriş kalitesi: {m['entry_quality']}/100\n"
        f"🧭 İlk adaydan beri: {m.get('candidate_runup', 0.0):+.2f}%\n\n"
        "📍 TAHMİNİ İŞLEM BÖLGESİ\n"
        f"🟩 Alım bölgesi: {fmt_price(plan['entry_low'])} – {fmt_price(plan['entry_high'])}\n"
        f"🎯 Kâr al 1: {fmt_price(plan['target1'])}  (R/R ~{plan['rr1']:.1f})\n"
        f"🎯 Kâr al 2: {fmt_price(plan['target2'])}  (R/R ~{plan['rr2']:.1f})\n"
        f"🛑 Geçersizlik: {fmt_price(plan['invalidation'])}\n\n"
        "⚠️ Fiyat alım bölgesinin üstündeyse kovalamak yerine yeniden teyit/pullback beklemek daha güvenlidir.\n"
        "Not: Seviyeler son volatilite ve kısa vadeli destek/dirençten türetilen kural tabanlı tahminlerdir; kâr garantisi veya otomatik emir değildir.\n"
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
        # V5.6 research collectors are observer-only; they cannot create a Premium.
        maybe_record_research(symbol, m, score, now)
        update_episode_peak(symbol, m["price"])

        # Aday oluşumu sessizdir: Telegram bildirimi gönderilmez.
        if st.candidate_since == 0.0:
            if not qualifies(m, score):
                return
            start_episode(symbol, m, score)
            funnel_hit("candidate")
            st.candidate_since = now
            st.candidate_last_check = now
            st.candidate_checks = 1
            st.candidate_passes = 1 if continuity_pass(m, score) else 0
            st.candidate_prices.append(m["price"])
            st.candidate_scores.append(score)
            save_candidate_event(symbol, "candidate_start", m, score, st)
            log.info("CANDIDATE %s score=%d episode=%s", symbol, score, st.episode_id)
            if early_watch_pass(m, score) and now - st.radar_record_ts >= EARLY_RADAR_RECORD_COOLDOWN_SECONDS:
                st.radar_record_ts = now
                st.active_radar_id = save_radar_signal(symbol, m, score)
                st.active_radar_notified = False
                mark_episode_early(symbol)
                pending_radars.append(PendingRadar(st.active_radar_id, symbol, m["price"], now))
                funnel_hit("early_radar")
                save_candidate_event(symbol, "early_radar", m, score, st)
            return

        if now - st.candidate_since > CANDIDATE_TTL_SECONDS:
            funnel_hit("ttl_reject")
            save_candidate_event(symbol, "ttl_reject", m, score, st)
            if st.candidate_passes >= CONFIRM_REQUIRED:
                add_research_event("REJECT_TTL_3OF3", symbol, m, score, "candidate TTL after 3/3")
            end_episode(symbol, "TTL_REJECT", m, score)
            reset_candidate(st)
            return
        if m["chg30"] < -0.20 or m["buy30"] < 0.50 or m["flow30"] < 0.8:
            funnel_hit("breakdown_reject")
            save_candidate_event(symbol, "breakdown_reject", m, score, st)
            end_episode(symbol, "BREAKDOWN_REJECT", m, score)
            reset_candidate(st)
            return

        if now - st.candidate_last_check < CONFIRM_INTERVAL_SECONDS:
            return
        st.candidate_last_check = now
        st.candidate_checks += 1
        st.candidate_prices.append(m["price"])
        st.candidate_scores.append(score)

        if continuity_pass(m, score):
            prices = list(st.candidate_prices)
            price_ok = len(prices) < 2 or prices[-1] >= prices[-2] * 0.999
            if price_ok:
                st.candidate_passes += 1
                funnel_hit("confirm_pass")
                save_candidate_event(symbol, "confirm_pass", m, score, st)
                if (not st.active_radar_id and early_watch_pass(m, score)
                        and now - st.radar_record_ts >= EARLY_RADAR_RECORD_COOLDOWN_SECONDS):
                    st.radar_record_ts = now
                    st.active_radar_id = save_radar_signal(symbol, m, score)
                    st.active_radar_notified = False
                    mark_episode_early(symbol)
                    pending_radars.append(PendingRadar(st.active_radar_id, symbol, m["price"], now))
                    funnel_hit("early_radar")
                    save_candidate_event(symbol, "early_radar", m, score, st, "created at confirm stage")
                if (st.active_radar_id and not st.active_radar_notified
                        and now - st.early_alert_ts >= EARLY_ALERT_COOLDOWN_SECONDS
                        and early_notify_pass(m, score, st)):
                    st.early_alert_ts = now
                    st.early_alert_price = m["price"]
                    st.active_radar_notified = True
                    m["daily_notice_no"] = next_daily_notice_no(symbol, "EARLY")
                    mark_radar_notified(st.active_radar_id, m["daily_notice_no"])
                    funnel_hit("early_alert")
                    save_candidate_event(symbol, "early_alert", m, score, st, "V5.6 2/3 selective notify; thresholds unchanged")
                    await telegram_send(session, build_early_message(m, score, st), symbol=symbol)
        else:
            if st.candidate_checks - st.candidate_passes >= 2:
                funnel_hit("continuity_reject")
                save_candidate_event(symbol, "continuity_reject", m, score, st)
                end_episode(symbol, "CONTINUITY_REJECT", m, score)
                reset_candidate(st)
                return

        if st.candidate_passes < CONFIRM_REQUIRED:
            return

        # Son teyitte OI alınır; production skor davranışı V5.5 ile aynıdır.
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
        m["candidate_runup"] = candidate_runup_pct(st, m["price"])
        m["episode_id"] = st.episode_id or None
        m["squeeze_risk"] = bool(
            oi5 is not None and oi5 <= 0.0 and m.get("flow30",0) >= 5.0
            and m.get("buy30",0) >= 0.64 and m.get("book_imbalance",1.0) < 0.50
        )
        add_research_event("CONFIRMED_3OF3", symbol, m, score,
                           f"quality={quality}; rise={rise_score}; runup={m['candidate_runup']:.2f}; squeeze={int(m['squeeze_risk'])}")
        if m["squeeze_risk"]:
            add_research_event("SQUEEZE_RISK", symbol, m, score, "OI<=0 + strong flow/buy + weak bid; shadow only")

        # Production V5.5 gates below are intentionally unchanged.
        if quality < ENTRY_MIN_SCORE:
            funnel_hit("quality_reject")
            save_candidate_event(symbol, "quality_reject", m, score, st, f"quality={quality}")
            add_research_event("REJECT_QUALITY", symbol, m, score, f"quality={quality}")
            end_episode(symbol, "QUALITY_REJECT", m, score)
            reset_candidate(st)
            return
        if rise_score < RISE_MIN_SCORE:
            funnel_hit("rise_reject")
            save_candidate_event(symbol, "rise_reject", m, score, st, f"rise={rise_score}")
            add_research_event("REJECT_RISE", symbol, m, score, f"rise={rise_score}")
            end_episode(symbol, "RISE_REJECT", m, score)
            reset_candidate(st)
            return
        if m["extended"]:
            funnel_hit("extended_reject")
            save_candidate_event(symbol, "extended_reject", m, score, st)
            add_research_event("REJECT_EXTENDED", symbol, m, score, "extended after 3/3")
            end_episode(symbol, "EXTENDED_REJECT", m, score)
            reset_candidate(st)
            return

        premium_ok, premium_reasons, runup = premium_trade_guard(m, score, quality, rise_score, st)
        m["candidate_runup"] = runup
        if not premium_ok:
            funnel_hit("premium_reject")
            note = "; ".join(premium_reasons)
            save_candidate_event(symbol, "premium_reject", m, score, st, note)
            add_research_event("REJECT_PREMIUM", symbol, m, score, note)
            end_episode(symbol, "PREMIUM_REJECT", m, score)
            reset_candidate(st)
            return
        if now - st.buy_signal_ts < COOLDOWN_SECONDS:
            save_candidate_event(symbol, "cooldown_reject", m, score, st)
            add_research_event("REJECT_COOLDOWN", symbol, m, score, "production cooldown")
            end_episode(symbol, "COOLDOWN_REJECT", m, score)
            reset_candidate(st)
            return

        st.buy_signal_ts = now
        st.last_alert_ts = now
        st.last_alert_price = m["price"]
        m["trade_plan"] = estimate_trade_plan(symbol, m)
        m["daily_notice_no"] = next_daily_notice_no(symbol, "PREMIUM")
        mark_episode_premium(symbol)
        funnel_hit("telegram_signal")
        save_candidate_event(symbol, "premium_signal", m, score, st, f"quality={quality}; rise={rise_score}; runup={runup:.2f}")
        signal_id = save_signal(m)
        save_signal_meta(signal_id, m)
        save_premium_radar_link(signal_id, symbol, m["price"], now, st.active_radar_id)
        plan = m["trade_plan"]
        entry_touch = 0.0 if plan["entry_low"] <= m["price"] <= plan["entry_high"] else None
        path_entry_price = m["price"] if entry_touch is not None else 0.0
        init_signal_path(signal_id, plan["entry_low"], plan["entry_high"], plan["target1"], plan["target2"], plan["invalidation"], entry_touch, path_entry_price)
        po = PendingOutcome(
            signal_id, symbol, m["price"], now, plan["target1"], plan["target2"], plan["invalidation"],
            plan["entry_low"], plan["entry_high"], entry_touch, path_entry_price
        )
        po.peak_price = m["price"]
        po.wave_start_price = m["price"]
        po.wave_peak_price = m["price"]
        pending_outcomes.append(po)
        save_wave_tracking(po)
        log.info("PREMIUM CONFIRMED %s momentum=%d rise=%d quality=%d runup=%.2f episode=%s", symbol, score, rise_score, quality, runup, st.episode_id)
        await telegram_send(session, build_message(m), symbol=symbol)
        end_episode(symbol, "PREMIUM", m, score)
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
                agg_stream_health[idx] = time.time()
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
                    agg_stream_health[idx] = stream_health["agg"]
                    update_episode_peak(sym, price)
                    update_pending_tick(sym, price, ts / 1000.0)
                    if st.quote_volume24 >= MIN_24H_QUOTE_VOLUME:
                        asyncio.create_task(evaluate(session, sym))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            stream_reconnects[f"agg_{idx}"] += 1
            log.warning("AggTrade WS %d reconnecting: %s", idx, e)
            await asyncio.sleep(2)



async def outcome_loop(session):
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
            path_changed = False

            # Session peak is distinct from the first structural wave.
            wave_changed = bool(p.wave_dirty)
            p.wave_dirty = False
            if price > (p.peak_price or p.entry_price):
                p.peak_price = price
                p.peak_mfe_pct = max(p.peak_mfe_pct, pct_change(price, p.entry_price))
                p.peak_s = age
                wave_changed = True
            drawdown_from_peak = max(0.0, -pct_change(price, p.peak_price or price))

            if not p.wave_start_price:
                p.wave_start_price = p.entry_price
                p.wave_peak_price = p.entry_price
            if p.wave_active:
                if price > (p.wave_peak_price or p.wave_start_price):
                    p.wave_peak_price = price
                    p.wave_peak_s = age
                    wave_changed = True
                wave_dd = max(0.0, -pct_change(price, p.wave_peak_price or price))
                # Research definition: a wave is segmented at the first >=1% pullback after 30s.
                # It is NOT interpreted as a production exit; V5.5 data showed many later higher highs.
                if age >= 30 and wave_dd >= 1.0:
                    if p.wave_no == 1 and p.first_wave_end_s is None:
                        p.first_wave_peak_price = p.wave_peak_price
                        p.first_wave_peak_mfe_pct = pct_change(p.wave_peak_price, p.entry_price) if p.wave_peak_price else 0.0
                        p.first_wave_peak_s = p.wave_peak_s
                        p.first_wave_end_s = age
                        p.first_wave_end_reason = "FIRST_1PCT_PULLBACK"
                    save_wave_event(p, age, price, wave_dd, "PULLBACK_1PCT")
                    p.wave_active = False
                    p.wave_last_end_price = price
                    p.wave_last_end_s = age
                    wave_changed = True
            else:
                # A new wave only starts after a genuine new high over the previous wave peak.
                if p.wave_peak_price and price >= p.wave_peak_price * 1.001:
                    p.wave_no += 1
                    p.wave_active = True
                    p.wave_start_price = p.wave_last_end_price or price
                    p.wave_start_s = p.wave_last_end_s
                    p.wave_peak_price = price
                    p.wave_peak_s = age
                    wave_changed = True

            m_shadow = None
            sc_shadow = None
            # Legacy session-peak pullback thresholds are retained for continuity with V5.5 analysis.
            for pb in WAVE_PULLBACK_LEVELS:
                if drawdown_from_peak >= pb and not any(abs(float(x[0])-pb) < 1e-9 for x in p.pullbacks_seen):
                    p.pullbacks_seen.add((pb, age))
                    m_shadow = m_shadow or compute_metrics(p.symbol)
                    sc_shadow = score_metrics(m_shadow) if m_shadow else None
                    save_shadow_event(p, f"PULLBACK_{pb:.1f}", age, price, ret, drawdown_from_peak, m_shadow, sc_shadow, f"first -{pb:.1f}% from session peak")
                    wave_changed = True
            if wave_changed:
                save_wave_tracking(p, drawdown_from_peak)

            # SHADOW notifications remain test-only. Thresholds are unchanged from V5.5.
            if SHADOW_EXIT_ENABLED and age >= SHADOW_MIN_AGE_SECONDS and p.peak_mfe_pct >= SHADOW_MIN_PEAK_MFE_PCT:
                m_shadow = m_shadow or compute_metrics(p.symbol)
                if m_shadow:
                    sc_shadow = score_metrics(m_shadow)
                    weakness, weak_reasons = shadow_weakness_score(m_shadow, sc_shadow, drawdown_from_peak)
                    if (not p.shadow_protect_sent and p.peak_mfe_pct >= SHADOW_PROTECT_MIN_PEAK_PCT
                            and drawdown_from_peak >= SHADOW_PROTECT_DRAWDOWN_PCT and weakness >= 3):
                        p.shadow_protect_sent = True
                        notice = next_daily_notice_no(p.symbol, "SHADOW_PROTECT") if SHADOW_EXIT_NOTIFY else None
                        save_shadow_event(p, "PROTECT", age, price, ret, drawdown_from_peak, m_shadow, sc_shadow, "; ".join(weak_reasons), notice)
                        if SHADOW_EXIT_NOTIFY:
                            await telegram_send(session, build_shadow_message(p, "PROTECT", price, ret, drawdown_from_peak, m_shadow, sc_shadow, weak_reasons, notice), symbol=p.symbol)
                    hard_exit = drawdown_from_peak >= SHADOW_HARD_DRAWDOWN_PCT
                    structured_exit = drawdown_from_peak >= SHADOW_EXIT_DRAWDOWN_PCT and weakness >= 4
                    invalid_exit = p.entry_touch_s is not None and p.invalidation and price <= p.invalidation
                    if not p.shadow_exit_sent and (hard_exit or structured_exit or invalid_exit):
                        p.shadow_exit_sent = True
                        extra = list(weak_reasons)
                        if hard_exit: extra.append("sert tepe geri çekilmesi")
                        if invalid_exit: extra.append("geçersizlik seviyesi")
                        notice = next_daily_notice_no(p.symbol, "SHADOW_EXIT") if SHADOW_EXIT_NOTIFY else None
                        save_shadow_event(p, "EXIT", age, price, ret, drawdown_from_peak, m_shadow, sc_shadow, "; ".join(extra), notice)
                        if SHADOW_EXIT_NOTIFY:
                            await telegram_send(session, build_shadow_message(p, "EXIT", price, ret, drawdown_from_peak, m_shadow, sc_shadow, extra, notice), symbol=p.symbol)

            # Fallback path accounting at loop frequency; aggTrade already updates these at event level.
            if p.entry_touch_s is None:
                if p.target1 and p.target_before_entry_s is None and price >= p.target1:
                    p.target_before_entry_s = age
                    p.first_event = p.first_event or "TARGET_BEFORE_ENTRY"
                    path_changed = True
                elif p.entry_high and price <= p.entry_high:
                    if p.invalidation and price <= p.invalidation:
                        p.invalidation_hit_s = p.invalidation_hit_s if p.invalidation_hit_s is not None else age
                        p.first_event = p.first_event or "INVALIDATION_BEFORE_ENTRY"
                    else:
                        p.entry_touch_s = age
                        p.path_entry_price = price
                    path_changed = True
            if p.entry_touch_s is not None:
                trade_ret = pct_change(price, p.path_entry_price or p.entry_price)
                p.trade_mfe = max(p.trade_mfe, trade_ret)
                p.trade_mae = min(p.trade_mae, trade_ret)
                if p.tp1_hit_s is None:
                    p.mfe_before_tp1 = max(p.mfe_before_tp1, trade_ret)
                    p.mae_before_tp1 = min(p.mae_before_tp1, trade_ret)
                if p.target1 and p.tp1_hit_s is None and price >= p.target1:
                    p.tp1_hit_s = age
                    p.first_event = p.first_event or "TP1"
                    path_changed = True
                if p.target2 and p.tp2_hit_s is None and price >= p.target2:
                    p.tp2_hit_s = age
                    path_changed = True
                if p.invalidation and p.invalidation_hit_s is None and price <= p.invalidation:
                    p.invalidation_hit_s = age
                    p.first_event = p.first_event or "INVALIDATION"
                    path_changed = True
            if path_changed:
                save_signal_path(p)

            t2_ret = pct_change(p.target2, p.entry_price) if p.target2 else CONTINUATION_MIN_MFE_PCT
            continuation_trigger = max(CONTINUATION_MIN_MFE_PCT, t2_ret)
            if CONTINUATION_ALERT_ENABLED and not p.continuation_sent and age <= 1800 and p.mfe >= continuation_trigger:
                m = compute_metrics(p.symbol)
                if m:
                    sc = score_metrics(m)
                    if (sc >= CONTINUATION_MIN_SCORE and m["chg30"] >= 0.15 and m["flow30"] >= 2.0
                            and 0.60 <= m["buy30"] <= 0.84 and not m["extended"]):
                        p.continuation_sent = True
                        funnel_hit("continuation_alert")
                        save_candidate_event(p.symbol, "continuation_alert", m, sc, states[p.symbol], f"mfe={p.mfe:.2f}")
                        m["daily_notice_no"] = next_daily_notice_no(p.symbol, "CONTINUATION")
                        await telegram_send(session, build_continuation_message(p, m, sc), symbol=p.symbol)
            for h in horizons:
                if age >= h and h not in p.completed:
                    save_outcome(p.signal_id, h, ret, p.mfe, p.mae)
                    p.completed.add(h)
            if 3600 in p.completed:
                if p.wave_active:
                    wave_dd = max(0.0, -pct_change(price, p.wave_peak_price or price))
                    save_wave_event(p, age, price, wave_dd, "SESSION_END")
                if p.first_wave_end_s is None:
                    p.first_wave_peak_price = p.peak_price
                    p.first_wave_peak_mfe_pct = p.peak_mfe_pct
                    p.first_wave_peak_s = p.peak_s
                    p.first_wave_end_s = age
                    p.first_wave_end_reason = "NO_1PCT_PULLBACK_60M"
                save_signal_path(p, completed_60m=True)
                save_wave_tracking(p, max(0.0, -pct_change(price, p.peak_price or price)), completed_60m=True)
                remove.append(p)
        for p in remove:
            if p in pending_outcomes:
                pending_outcomes.remove(p)

        radar_remove = []
        for r in list(pending_radars):
            price = states[r.symbol].last_price
            if not price: continue
            ret = pct_change(price, r.entry_price); r.mfe=max(r.mfe,ret); r.mae=min(r.mae,ret)
            age = now-r.created_ts
            for h in (60,180,300,900,1800,3600):
                if age>=h and h not in r.completed:
                    save_radar_outcome(r.radar_id,h,ret,r.mfe,r.mae); r.completed.add(h)
            if 3600 in r.completed: radar_remove.append(r)
        for r in radar_remove:
            if r in pending_radars: pending_radars.remove(r)

        g_remove=[]
        for g in list(pending_gainers):
            price=states[g.symbol].last_price
            if not price: continue
            ret=pct_change(price,g.entry_price); g.mfe=max(g.mfe,ret); g.mae=min(g.mae,ret); age=now-g.created_ts
            for h in GAINERS_OUTCOME_HORIZONS:
                if age>=h and h not in g.completed:
                    save_gainers_outcome(g.event_id,h,ret,g.mfe,g.mae); g.completed.add(h)
            if max(GAINERS_OUTCOME_HORIZONS) in g.completed: g_remove.append(g)
        for g in g_remove:
            if g in pending_gainers: pending_gainers.remove(g)

        research_remove=[]
        for r in list(pending_research):
            price=states[r.symbol].last_price
            if not price: continue
            ret=pct_change(price,r.entry_price); r.mfe=max(r.mfe,ret); r.mae=min(r.mae,ret); age=now-r.created_ts
            for h in RESEARCH_HORIZONS:
                if age>=h and h not in r.completed:
                    save_research_outcome(r.event_id,h,ret,r.mfe,r.mae); r.completed.add(h)
            if max(RESEARCH_HORIZONS) in r.completed: research_remove.append(r)
        for r in research_remove:
            if r in pending_research: pending_research.remove(r)

        shadow_remove=[]
        for s in list(pending_shadow_events):
            price=states[s.symbol].last_price
            if not price: continue
            ret=pct_change(price,s.entry_price); s.mfe=max(s.mfe,ret); s.mae=min(s.mae,ret); age=now-s.created_ts
            for h in SHADOW_OUTCOME_HORIZONS:
                if age>=h and h not in s.completed:
                    save_shadow_event_outcome(s.event_id,h,ret,s.mfe,s.mae); s.completed.add(h)
            if max(SHADOW_OUTCOME_HORIZONS) in s.completed: shadow_remove.append(s)
        for s in shadow_remove:
            if s in pending_shadow_events: pending_shadow_events.remove(s)

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
        conn = db_connect()
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
        "🏆 GAINERS RADAR — TOP LİSTEYE GİRDİ",
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
        "🚀 GAINERS RADAR — HIZLI SIRA YÜKSELİŞİ",
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
    """Keep Gainers rank history and outcomes in the background; Telegram push is optional and OFF by default."""
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
                log.info("Gainers baseline ready: TOP %d (push=%s)", GAINERS_TOP_N, GAINERS_NOTIFY)
                await asyncio.sleep(GAINERS_POLL_SECONDS)
                continue

            for sym in gainers_current_top - top_now:
                gainers_left_top_at[sym] = now

            entrants = sorted(top_now - gainers_current_top, key=lambda x: rank_map.get(x, 99999))
            for sym in entrants:
                left_at = gainers_left_top_at.get(sym, 0.0)
                first_seen_entry = gainers_last_entry_alert.get(sym, 0.0) == 0.0
                was_out_long_enough = left_at == 0.0 or now - left_at >= GAINERS_REENTRY_MIN_OUT_SECONDS
                cooldown_ok = now - gainers_last_entry_alert.get(sym, 0.0) >= GAINERS_ALERT_COOLDOWN_SECONDS
                if cooldown_ok and (first_seen_entry or was_out_long_enough):
                    save_gainers_event(sym, "top_entry", rank_map[sym], gainers_prev_rank.get(sym))
                    if GAINERS_NOTIFY:
                        await telegram_send(session, build_gainers_entry_message(sym, rank_map[sym], gainers_prev_rank.get(sym)), symbol=sym)
                    gainers_last_entry_alert[sym] = now
                    log.info("GAINERS ENTRY %s rank=%d pct=%.2f push=%s", sym, rank_map[sym], pct_map[sym], GAINERS_NOTIFY)

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
                if sym in entrants and now - gainers_last_entry_alert.get(sym, 0.0) < 5:
                    continue
                save_gainers_event(sym, "rapid_climb", new_rank, old_rank)
                if GAINERS_NOTIFY:
                    await telegram_send(session, build_gainers_rapid_message(sym, old_rank, new_rank, old_pct), symbol=sym)
                gainers_last_rapid_alert[sym] = now
                log.info("GAINERS RAPID %s %d->%d pct=%.2f push=%s", sym, old_rank, new_rank, pct_map[sym], GAINERS_NOTIFY)

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
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30, connect=6, sock_read=25)) as r:
                raw = await r.text()
                if r.status != 200:
                    log.warning("Telegram getUpdates HTTP %s body=%s", r.status, raw[:1000])
                    await asyncio.sleep(3)
                    continue
                try:
                    data = json.loads(raw)
                except Exception as e:
                    log.warning("Telegram getUpdates invalid JSON type=%s repr=%r body=%r", type(e).__name__, e, raw[:500])
                    await asyncio.sleep(3)
                    continue
                if not data.get("ok", True):
                    log.warning("Telegram getUpdates ok=false body=%s", raw[:1000])
                    await asyncio.sleep(3)
                    continue
            for upd in data.get("result", []):
                telegram_offset = max(telegram_offset, int(upd.get("update_id", 0)) + 1)
                msg = upd.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue
                raw_text = str(msg.get("text", "")).strip()
                text = raw_text.lower()
                if text == "/status":
                    age = lambda k: (time.time() - stream_health[k]) if stream_health[k] else 9999
                    agg_ages = [(time.time() - t) for t in agg_stream_health.values() if t]
                    agg_oldest = max(agg_ages) if agg_ages else 9999
                    agg_stale = sum(1 for a in agg_ages if a >= 90)
                    expected_chunks = max(1, (len(symbols) + AGGTRADE_CHUNK - 1) // AGGTRADE_CHUNK)
                    healthy = age("ticker") < 90 and age("book") < 90 and len(agg_ages) >= expected_chunks and agg_stale == 0
                    await telegram_send(session,
                        f"{'✅' if healthy else '⚠️'} Scanner durumu — V5.6\n\n"
                        f"🪙 Kontrat: {len(symbols)}\n"
                        f"💵 Min 24s hacim: {fmt_money(MIN_24H_QUOTE_VOLUME)} USDT\n"
                        f"⚡ AggTrade olayları: {trade_event_count:,}\n"
                        f"🚨 Son 24s sinyal: {signal_count_today()}\n"
                        f"📡 ticker: {age('ticker'):.0f} sn | book: {age('book'):.0f} sn | agg genel: {age('agg'):.0f} sn\n"
                        f"🧩 AggTrade chunk: {len(agg_ages)}/{expected_chunks} | en eski: {agg_oldest:.0f} sn | stale: {agg_stale}\n"
                        f"⭐ Aday eşiği: {EARLY_SCORE}+\n"
                        f"🎯 Teyit: {CONFIRM_REQUIRED} × {CONFIRM_INTERVAL_SECONDS} sn | premium momentum {PREMIUM_MIN_MOMENTUM_SCORE}+ | giriş {PREMIUM_ENTRY_MIN_SCORE}+ | yükseliş {PREMIUM_RISE_MIN_SCORE}+\n"
                        f"👀 Erken bildirim: 2/3 süreklilik + skor {EARLY_NOTIFY_MIN_SCORE}+ (production eşikleri aynı)\n"
                        f"🧪 Shadow Exit: {'açık' if SHADOW_EXIT_ENABLED else 'kapalı'} | Telegram: {'açık' if SHADOW_EXIT_NOTIFY else 'kapalı'} | sadece test\n"
                        f"🔬 Second-wave / Pre-Breakout research: {'açık' if RESEARCH_ENABLED else 'kapalı'}\n"
                        f"🏆 Gainers: arka plan kayıt AÇIK | Telegram push: {'açık' if GAINERS_NOTIFY else 'kapalı'} | TOP {GAINERS_TOP_N}"
                    )
                elif text == "/top":
                    rows = current_top(10)
                    if not rows:
                        await telegram_send(session, "Henüz yeterli canlı trade verisi birikmedi. 30-60 sn sonra tekrar /top yaz.")
                    else:
                        lines = ["📊 ŞU AN ISINAN COINLER\n"]
                        for score, sym, m in rows:
                            st = states[sym]
                            marker = "🎯" if (st.candidate_passes >= 2 and continuity_pass(m, score)) else "·"
                            lines.append(f"{marker} {score:>3}/100  {sym} | 30sn {m['chg30']:+.2f}% | flow {m['flow30']:.1f}x | buy %{m['buy30']*100:.0f}")
                        lines.append("\n🎯 = alım fırsatına yaklaşan ve süreklilik gösteren aday. Bot uygun olursa otomatik gönderir.")
                        await telegram_send(session, "\n".join(lines))
                elif text == "/gainers":
                    ranked = gainers_ranked()[:min(GAINERS_TOP_N, 20)]
                    if not ranked:
                        await telegram_send(session, "Gainers verisi henüz hazır değil.")
                    else:
                        lines = [f"🏆 FUTURES GAINERS — İlk {len(ranked)}\n"]
                        for i, (pct, sym, _) in enumerate(ranked, 1):
                            lines.append(f"#{i:<2} {sym}  {pct:+.2f}%")
                        lines.append(f"\nArka plan araştırma bölgesi: TOP {GAINERS_TOP_N} | otomatik Telegram push: {'açık' if GAINERS_NOTIFY else 'kapalı'}")
                        await telegram_send(session, "\n".join(lines))
                elif text == "/funnel":
                    mins = max(1, int((time.time() - funnel_started_ts) / 60))
                    await telegram_send(session,
                        "📊 SİNYAL FİLTRESİ — BU DEPLOY\n\n"
                        f"⏱ Çalışma: {mins} dk\n"
                        f"👀 Aday oluştu: {funnel_counts['candidate']}\n"
                        f"✅ Süreklilik kontrolü geçti: {funnel_counts['confirm_pass']}\n"
                        f"❌ Süreklilik bozuldu: {funnel_counts['continuity_reject'] + funnel_counts['breakdown_reject']}\n"
                        f"❌ Giriş kalitesi yetersiz: {funnel_counts['quality_reject']}\n"
                        f"❌ Yükseliş skoru yetersiz: {funnel_counts['rise_reject']}\n"
                        f"❌ Hareket uzamış: {funnel_counts['extended_reject']}\n"
                        f"🧱 Premium filtreden elendi: {funnel_counts['premium_reject']}\n"
                        f"🧪 İç radar kaydı: {funnel_counts['early_radar']}\n"
                        f"👀 Seçici erken uyarı: {funnel_counts['early_alert']}\n"
                        f"🟢 Premium alım fırsatı: {funnel_counts['telegram_signal']}\n"
                        f"🚀 Momentum devamı: {funnel_counts['continuation_alert']}\n\n"
                        "Bu ekran hangi filtrenin adayları elediğini gösterir."
                    )
                elif text == "/stats":
                    conn = db_connect()
                    row = conn.execute("""SELECT COUNT(*), SUM(CASE WHEN o.mfe_pct>=0.5 THEN 1 ELSE 0 END), SUM(CASE WHEN o.mfe_pct>=1 THEN 1 ELSE 0 END), SUM(CASE WHEN o.mfe_pct>=2 THEN 1 ELSE 0 END), AVG(o.mfe_pct), AVG(o.mae_pct) FROM signals_v2 s JOIN signal_outcomes o ON o.signal_id=s.id AND o.horizon_s=3600""").fetchone()
                    path = conn.execute("""SELECT
                        SUM(CASE WHEN entry_touch_s IS NOT NULL THEN 1 ELSE 0 END),
                        SUM(CASE WHEN entry_touch_s IS NOT NULL AND first_event='TP1' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN entry_touch_s IS NOT NULL AND first_event='INVALIDATION' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN entry_touch_s IS NOT NULL AND tp2_hit_s IS NOT NULL THEN 1 ELSE 0 END),
                        AVG(CASE WHEN entry_touch_s IS NOT NULL THEN mae_before_tp1 END),
                        SUM(CASE WHEN target_before_entry_s IS NOT NULL THEN 1 ELSE 0 END),
                        COUNT(*)
                        FROM signal_paths WHERE completed_60m=1""").fetchone()
                    conn.close()
                    n = row[0] or 0
                    if not n:
                        await telegram_send(session, "Henüz tamamlanmış 60 dk performans verisi yok.")
                    else:
                        msg = (f"📊 60 DK SİNYAL PERFORMANSI\n\nTamamlanan: {n}\n+%0.5 gördü: %{100*row[1]/n:.1f}\n+%1 gördü: %{100*row[2]/n:.1f}\n+%2 gördü: %{100*row[3]/n:.1f}\nOrt. maksimum yükseliş: {row[4]:+.2f}%\nOrt. maksimum ters hareket: {row[5]:+.2f}%")
                        pn = path[0] or 0
                        total_paths = path[6] or 0
                        if total_paths:
                            msg += f"\n\n🧭 V5.4 İŞLEM YOLU ({total_paths})\nAlım bölgesi temas etti: %{100*pn/total_paths:.1f}\nHedefe alım bölgesi gelmeden kaçtı: {int(path[5] or 0)}"
                        if pn:
                            msg += (f"\nTP1, geçersizlikten önce: %{100*(path[1] or 0)/pn:.1f}\nGeçersizlik önce: %{100*(path[2] or 0)/pn:.1f}\nTP2 gördü: %{100*(path[3] or 0)/pn:.1f}\nTP1'e kadar ort. ters hareket: {(path[4] or 0):+.2f}%")
                        msg += "\n\nNot: MFE tek başına başarı sayılmaz; V5.4 hedef/geçersizlik sırasını da ölçer."
                        await telegram_send(session, msg)
                elif text == "/radarstats":
                    conn = db_connect()
                    row = conn.execute("""SELECT COUNT(*),
                        SUM(CASE WHEN o.mfe_pct>=0.5 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN o.mfe_pct>=1 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN o.mfe_pct>=2 THEN 1 ELSE 0 END),
                        AVG(o.mfe_pct), AVG(o.mae_pct), SUM(r.notified)
                        FROM radar_signals r JOIN radar_outcomes o ON o.radar_id=r.id AND o.horizon_s=3600""").fetchone()
                    conn.close()
                    n = row[0] or 0
                    if not n:
                        await telegram_send(session, "Henüz tamamlanmış 60 dk radar performansı yok.")
                    else:
                        await telegram_send(session,
                            f"👀 60 DK ERKEN RADAR PERFORMANSI\n\nİç radar kaydı: {n}\nTelegram'a bildirilen: {int(row[6] or 0)}\n+%0.5 gördü: %{100*(row[1] or 0)/n:.1f}\n+%1 gördü: %{100*(row[2] or 0)/n:.1f}\n+%2 gördü: %{100*(row[3] or 0)/n:.1f}\nOrt. MFE: {(row[4] or 0):+.2f}%\nOrt. MAE: {(row[5] or 0):+.2f}%\n\nİç radar tüm araştırma örneklerini tutar; Telegram yalnız 2/3 süreklilikteki seçici alt kümeyi bildirir.")
                elif text == "/shadowstats":
                    conn = db_connect()
                    ev = conn.execute("""SELECT COUNT(DISTINCT CASE WHEN event='PROTECT' THEN signal_id END),
                        COUNT(DISTINCT CASE WHEN event='EXIT' THEN signal_id END),
                        COUNT(DISTINCT signal_id) FROM shadow_exit_events""").fetchone()
                    wave = conn.execute("""SELECT COUNT(*),AVG(peak_mfe_pct),AVG(peak_s),
                        SUM(CASE WHEN pullback_0_5_s IS NOT NULL THEN 1 ELSE 0 END),
                        SUM(CASE WHEN pullback_1_0_s IS NOT NULL THEN 1 ELSE 0 END),
                        SUM(CASE WHEN pullback_1_5_s IS NOT NULL THEN 1 ELSE 0 END),
                        SUM(CASE WHEN pullback_2_0_s IS NOT NULL THEN 1 ELSE 0 END)
                        FROM premium_wave_tracking WHERE completed_60m=1""").fetchone()
                    links = conn.execute("""SELECT COUNT(*),SUM(early_notified),AVG(early_to_premium_s),AVG(price_cost_pct) FROM premium_radar_links""").fetchone()
                    conn.close()
                    wn = wave[0] or 0
                    await telegram_send(session,
                        f"🧪 V5.6 SHADOW / DALGA İSTATİSTİĞİ\n\n"
                        f"Shadow kâr-koruma adayı: {int(ev[0] or 0)}\n"
                        f"Shadow çıkış adayı: {int(ev[1] or 0)}\n"
                        f"Shadow event görülen Premium: {int(ev[2] or 0)}\n\n"
                        f"60 dk tamamlanan dalga: {wn}\n"
                        f"Ort. tepe MFE: {(wave[1] or 0):+.2f}%\n"
                        f"Ort. tepe zamanı: {(wave[2] or 0):.0f} sn\n"
                        f"Tepeden -%0.5 gördü: {int(wave[3] or 0)} | -%1: {int(wave[4] or 0)} | -%1.5: {int(wave[5] or 0)} | -%2: {int(wave[6] or 0)}\n\n"
                        f"Erken radar→Premium bağlantısı: {int(links[0] or 0)}\n"
                        f"Bunlardan Telegram erken uyarılı: {int(links[1] or 0)}\n"
                        f"Ort. erken→Premium süre: {(links[2] or 0):.1f} sn\n"
                        f"Ort. teyit fiyat maliyeti: {(links[3] or 0):+.2f}%\n\n"
                        "Not: Shadow bildirimleri test verisidir; işlem kararı değildir.")
                elif text == "/researchstats":
                    conn = db_connect()
                    types = conn.execute("SELECT event_type,COUNT(*) FROM research_events GROUP BY event_type ORDER BY COUNT(*) DESC").fetchall()
                    mature = conn.execute("""SELECT r.event_type,COUNT(*),AVG(o.mfe_pct),AVG(o.mae_pct),AVG(o.return_pct)
                        FROM research_events r JOIN research_outcomes o ON o.event_id=r.id AND o.horizon_s=3600
                        GROUP BY r.event_type ORDER BY COUNT(*) DESC""").fetchall()
                    gout = conn.execute("""SELECT COUNT(*),AVG(o.mfe_pct),AVG(o.return_pct)
                        FROM gainers_events g JOIN gainers_outcomes o ON o.event_id=g.id AND o.horizon_s=3600""").fetchone()
                    sh = conn.execute("""SELECT COUNT(*),AVG(o.return_pct),AVG(o.mfe_pct),AVG(o.mae_pct)
                        FROM shadow_exit_events e JOIN shadow_event_outcomes o ON o.shadow_event_id=e.id AND o.horizon_s=900
                        WHERE e.event='EXIT'""").fetchone()
                    conn.close()
                    lines=["🔬 V5.6 RESEARCH ÖZETİ", ""]
                    if types:
                        lines.append("Kayıtlar: " + " | ".join(f"{t}:{n}" for t,n in types[:8]))
                    if mature:
                        lines.append("\n60 dk olgun araştırma grupları:")
                        for t,n,mfe,mae,ret in mature[:8]:
                            lines.append(f"• {t}: n={n} | MFE {(mfe or 0):+.2f}% | MAE {(mae or 0):+.2f}% | 60dk {(ret or 0):+.2f}%")
                    if gout and gout[0]:
                        lines.append(f"\n🏆 Gainers 60dk: n={int(gout[0])} | MFE {(gout[1] or 0):+.2f}% | kapanış {(gout[2] or 0):+.2f}%")
                    if sh and sh[0]:
                        lines.append(f"🧪 Shadow EXIT sonrası 15dk: n={int(sh[0])} | getiri {(sh[1] or 0):+.2f}% | MFE {(sh[2] or 0):+.2f}% | MAE {(sh[3] or 0):+.2f}%")
                    lines.append("\nBu veriler production filtresi değildir; V5.6 yalnız ölçer.")
                    await telegram_send(session, "\n".join(lines))
                elif text.startswith("/analiz ") or (not text.startswith("/") and text not in ("test",) and 1 <= len(raw_text) <= 20):
                    token = raw_text.split(maxsplit=1)[1] if text.startswith("/analiz ") else raw_text
                    token = token.strip().upper().replace("/", "")
                    sym = token if token.endswith("USDT") else token + "USDT"
                    if sym not in states:
                        await telegram_send(session, f"❌ {token} için aktif USDT perpetual bulamadım. Örnek: /analiz TUT")
                    else:
                        m = compute_metrics(sym)
                        if not m:
                            await telegram_send(session, f"⏳ {sym} için henüz yeterli canlı veri yok. 30-60 sn sonra tekrar dene.")
                        else:
                            sc = score_metrics(m)
                            m["oi5"] = await get_oi_5m(session, sym)
                            if m["oi5"] is not None:
                                if m["oi5"] >= 1.0: sc = min(100, sc + 4)
                                elif m["oi5"] <= -1.5: sc = max(0, sc - 4)
                            q = entry_quality(m, sc, states[sym])
                            rscore = rise_probability(m, sc, states[sym])
                            plan = estimate_trade_plan(sym, m)
                            await telegram_send(session, build_manual_analysis(sym, m, sc, q, rscore, plan), symbol=sym)
                elif text in ("/test", "test"):
                    await telegram_send(session, "✅ Bot çalışıyor. /status, /top, /gainers, /funnel, /stats, /radarstats, /shadowstats, /researchstats ve /analiz COIN kullanabilirsin.")
                elif text in ("/help", "/start"):
                    await telegram_send(session,
                        "🤖 Momentum Scanner V5.6 — Quality-First + Measurement Research\n\n"
                        "/status — bağlantı ve sinyal durumu\n"
                        "/top — şu an ısınan ilk 10 coin\n"
                        "/gainers — güncel Futures gainers\n"
                        "/funnel — adayların hangi filtrelerde elendiği\n"
                        "/stats — premium sinyal + işlem yolu performansı\n"
                        "/radarstats — erken radarların 60 dk performansı\n"
                        "/shadowstats — Shadow Exit + ilk dalga + erken→Premium özeti\n"
                        "/researchstats — second-wave / pre-breakout / Gainers research özeti\n"
                        "/analiz COIN — bir coini anlık analiz et\n"
                        "/test — Telegram testi\n\n"
                        "Premium seçim kuralları V5.5 ile aynıdır. V5.6 yeni ölçüm/research katmanları ekler; SHADOW bildirimleri işlem sinyali değildir."
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Telegram command polling exception type=%s repr=%r", type(e).__name__, e)
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
        recover_pending_tracking()

        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            await telegram_send(session,
                f"✅ Momentum Scanner V5.6 başladı — QUALITY FIRST + MEASUREMENT RESEARCH\n\n"
                f"🪙 İzlenen kontrat: {len(symbols)}\n"
                f"⚡ 100ms aggTrade + event-level Premium path takibi\n"
                f"💵 Min 24s hacim: {fmt_money(MIN_24H_QUOTE_VOLUME)} USDT\n"
                f"⭐ Sessiz aday skoru: {EARLY_SCORE}+\n"
                f"🎯 Teyit: {CONFIRM_REQUIRED} × {CONFIRM_INTERVAL_SECONDS} sn | premium momentum {PREMIUM_MIN_MOMENTUM_SCORE}+ | giriş {PREMIUM_ENTRY_MIN_SCORE}+ | yükseliş {PREMIUM_RISE_MIN_SCORE}+\n"
                f"👀 Erken radar: 2/3 seçici Telegram; production eşikleri değişmedi\n"
                f"🧪 Shadow Exit + first-wave/session-peak + post-shadow outcome: AÇIK; işlem sinyali değil\n"
                f"🔬 Second-wave / Pre-Breakout / reject outcome araştırması: {'AÇIK' if RESEARCH_ENABLED else 'KAPALI'}\n"
                f"🏆 Gainers: arka plan rank-velocity/outcome AÇIK | Telegram push: {'AÇIK' if GAINERS_NOTIFY else 'KAPALI'}\n\n"
                f"Komutlar: /status  /top  /gainers  /funnel  /stats  /radarstats  /shadowstats  /researchstats  /analiz COIN  /test"
            )

        chunks = [symbols[i:i + AGGTRADE_CHUNK] for i in range(0, len(symbols), AGGTRADE_CHUNK)]
        tasks = [
            ticker_ws(session), book_ws(session), liquidation_ws(session),
            outcome_loop(session), reset_levels_loop(), telegram_command_loop(session), gainers_loop(session),
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
