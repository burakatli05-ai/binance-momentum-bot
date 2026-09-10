import asyncio
import json
import logging
import os
import signal
import sqlite3
import time
import tempfile
import zipfile
import hashlib
import hmac
import math
import secrets
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from urllib.parse import urlencode
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from statistics import mean, median
from typing import Deque, Dict, Optional, List, Tuple, Set

import aiohttp
from dotenv import load_dotenv

load_dotenv()

REST = "https://fapi.binance.com"
WS_MARKET = "wss://fstream.binance.com/market/stream"
WS_PUBLIC = "wss://fstream.binance.com/public/stream"
IST = timezone(timedelta(hours=3))

BOT_VERSION = "5.13.4"
RESEARCH_LOGIC_VERSION = "v5134-trend-build-liq-transition-position-observer-risk-guard"
PROCESS_STARTED_TS_MS = int(time.time() * 1000)

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
SHADOW_OUTCOME_HORIZONS = (15, 30, 60, 120, 300, 900)
ANCHOR_MAX_AGE_SECONDS = int(os.getenv("ANCHOR_MAX_AGE_SECONDS", "21600"))

# V5.7: execution-quality / phase measurement. Production Premium gates above remain unchanged.
MICRO_SNAPSHOT_HORIZONS_MS = (1000, 3000, 5000, 10000, 15000, 20000, 30000, 60000)
ENTRY_ACCEPTANCE_HORIZON_MS = int(os.getenv("ENTRY_ACCEPTANCE_HORIZON_MS", "15000"))
MAX_SYMBOL_TRADE_STALE_S = float(os.getenv("MAX_SYMBOL_TRADE_STALE_S", "10"))
MAX_SYMBOL_BOOK_STALE_S = float(os.getenv("MAX_SYMBOL_BOOK_STALE_S", "5"))
MAX_EVENT_RECEIVE_LAG_MS = int(os.getenv("MAX_EVENT_RECEIVE_LAG_MS", "3000"))
EXEC_VALID_MAX_DRIFT_PCT = float(os.getenv("EXEC_VALID_MAX_DRIFT_PCT", "0.15"))
EXEC_CHASE_MAX_DRIFT_PCT = float(os.getenv("EXEC_CHASE_MAX_DRIFT_PCT", "0.45"))
EXEC_MIN_LIVE_RR1 = float(os.getenv("EXEC_MIN_LIVE_RR1", "0.45"))
RUNNER_SHADOW_ENABLED = os.getenv("RUNNER_SHADOW_ENABLED", "1").strip() not in ("0", "false", "False")
RECLAIM_SHADOW_ENABLED = os.getenv("RECLAIM_SHADOW_ENABLED", "1").strip() not in ("0", "false", "False")
RECLAIM_MAX_AGE_SECONDS = int(os.getenv("RECLAIM_MAX_AGE_SECONDS", "1800"))

# Telegram join-request approval is opt-in via TELEGRAM_APPROVAL_CHAT_ID.
# Bot must be an administrator of that channel/group with can_invite_users permission.
TELEGRAM_APPROVAL_CHAT_ID = os.getenv("TELEGRAM_APPROVAL_CHAT_ID", "").strip()
TELEGRAM_ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "").strip() or TELEGRAM_CHAT_ID
TELEGRAM_ADMIN_USER_ID = os.getenv("TELEGRAM_ADMIN_USER_ID", "").strip()
JOIN_REQUEST_APPROVAL_ENABLED = bool(TELEGRAM_APPROVAL_CHAT_ID) and os.getenv("JOIN_REQUEST_APPROVAL_ENABLED", "1").strip() not in ("0", "false", "False")

# V5.7.1: public channel broadcast. By default reuse the approved-members channel.
# Commands, /test, research statistics and join approvals stay in the private admin chat.
TELEGRAM_BROADCAST_CHAT_ID = os.getenv("TELEGRAM_BROADCAST_CHAT_ID", "").strip() or TELEGRAM_APPROVAL_CHAT_ID
TELEGRAM_BROADCAST_ENABLED = bool(TELEGRAM_BROADCAST_CHAT_ID) and os.getenv("TELEGRAM_BROADCAST_ENABLED", "1").strip() not in ("0", "false", "False")
PUBLIC_NOTIFICATION_KINDS = {"EARLY", "PREMIUM", "CONTINUATION"}

# V5.8 measurement-first layer. Production Premium detector thresholds remain unchanged.
NEAR_MISS_ENABLED = os.getenv("NEAR_MISS_ENABLED", "1").strip() not in ("0", "false", "False")
NEAR_MISS_MIN_QV24 = float(os.getenv("NEAR_MISS_MIN_QV24", "2500000"))
NEAR_MISS_COOLDOWN_SECONDS = int(os.getenv("NEAR_MISS_COOLDOWN_SECONDS", "600"))
IGNITION_SHADOW_ENABLED = os.getenv("IGNITION_SHADOW_ENABLED", "1").strip() not in ("0", "false", "False")
IGNITION_COOLDOWN_SECONDS = int(os.getenv("IGNITION_COOLDOWN_SECONDS", "600"))
IGNITION_MIN_SHADOW_SCORE = int(os.getenv("IGNITION_MIN_SHADOW_SCORE", "60"))
FAST_EARLY_SHADOW_SCORE = int(os.getenv("FAST_EARLY_SHADOW_SCORE", "75"))
MISSED_RUNNER_HORIZON_S = int(os.getenv("MISSED_RUNNER_HORIZON_S", "900"))
MISSED_RUNNER_MIN_MFE_PCT = float(os.getenv("MISSED_RUNNER_MIN_MFE_PCT", "2.0"))

LIQUIDITY_RESEARCH_ENABLED = os.getenv("LIQUIDITY_RESEARCH_ENABLED", "1").strip() not in ("0", "false", "False")
LIQUIDITY_DEPTH_LIMIT = int(os.getenv("LIQUIDITY_DEPTH_LIMIT", "500"))
LIQUIDITY_SNAPSHOT_HORIZONS_MS = (0, 5000, 15000, 30000)
LIQUIDITY_BANDS_PCT = (0.10, 0.25, 0.50, 1.00)
LIQUIDITY_WALL_MATCH_TOLERANCE_PCT = float(os.getenv("LIQUIDITY_WALL_MATCH_TOLERANCE_PCT", "0.03"))

PROGRESS_VALIDATION_HORIZON_MS = int(os.getenv("PROGRESS_VALIDATION_HORIZON_MS", "60000"))

# V5.9: forward-calibrated SHADOW refinements. These never gate Premium creation.
# Latest forward sample showed that static wall size alone was weak, while wall persistence,
# nearby bid support and clear-time evolution were materially more informative.
LIQUIDITY_EVOLUTION_ENABLED = os.getenv("LIQUIDITY_EVOLUTION_ENABLED", "1").strip() not in ("0", "false", "False")
LIQ_EVOLUTION_SUPPORT_SCORE = int(os.getenv("LIQ_EVOLUTION_SUPPORT_SCORE", "30"))
LIQ_EVOLUTION_HOSTILE_SCORE = int(os.getenv("LIQ_EVOLUTION_HOSTILE_SCORE", "-30"))
IGNITION_V2_ENABLED = os.getenv("IGNITION_V2_ENABLED", "1").strip() not in ("0", "false", "False")
IGNITION_V2_MIN_SCORE = int(os.getenv("IGNITION_V2_MIN_SCORE", "80"))
FAST_EARLY_V2_SCORE = int(os.getenv("FAST_EARLY_V2_SCORE", "85"))

# V5.10: SHADOW-only post-Premium execution-risk research.
# Production Premium/Early/TP/stop rules stay byte-for-byte unchanged.
POST_PREMIUM_RISK_ENABLED = os.getenv("POST_PREMIUM_RISK_ENABLED", "1").strip() not in ("0", "false", "False")
LIQUIDITY_V2_ENABLED = os.getenv("LIQUIDITY_V2_ENABLED", "1").strip() not in ("0", "false", "False")
LIQ_V2_SUPPORT_SCORE = int(os.getenv("LIQ_V2_SUPPORT_SCORE", "35"))
LIQ_V2_HOSTILE_SCORE = int(os.getenv("LIQ_V2_HOSTILE_SCORE", "-35"))
POST_RISK_BID_RATIO_MAX = float(os.getenv("POST_RISK_BID_RATIO_MAX", "0.25"))
POST_RISK_BID_RETAIN_MAX = float(os.getenv("POST_RISK_BID_RETAIN_MAX", "0.50"))
POST_RISK_WALL_REMAIN_MIN = float(os.getenv("POST_RISK_WALL_REMAIN_MIN", "0.90"))
POST_RISK_ASK_GROWTH_MIN = float(os.getenv("POST_RISK_ASK_GROWTH_MIN", "1.05"))
POST_RISK_CLEAR_S_MIN = float(os.getenv("POST_RISK_CLEAR_S_MIN", "15.0"))
DB_BACKUP_ENABLED = os.getenv("DB_BACKUP_ENABLED", "1").strip() not in ("0", "false", "False")

# V5.10.1: keep V5.10 Liquidity V2 frozen and add a clean ablation that uses only
# the four forward-supported aggregate-book metrics. This is exploratory SHADOW only.
LIQUIDITY_CORE_ENABLED = os.getenv("LIQUIDITY_CORE_ENABLED", "1").strip() not in ("0", "false", "False")
LIQ_CORE_SUPPORT_SCORE = int(os.getenv("LIQ_CORE_SUPPORT_SCORE", str(LIQ_V2_SUPPORT_SCORE)))
LIQ_CORE_HOSTILE_SCORE = int(os.getenv("LIQ_CORE_HOSTILE_SCORE", str(LIQ_V2_HOSTILE_SCORE)))

# Event-level missed-runner counts can over-count the same move. Group research events into
# symbol/time episodes for a more honest first-wave coverage metric.
DISCOVERY_EPISODE_GAP_S = int(os.getenv("DISCOVERY_EPISODE_GAP_S", "600"))
DISCOVERY_EPISODE_MAX_S = int(os.getenv("DISCOVERY_EPISODE_MAX_S", "1800"))

# V5.11: SHADOW execution-gate simulator. IMPORTANT: this does NOT delay, suppress, or alter
# the production Premium alert. Latest analysis showed a look-ahead trap: many apparently
# "good 15/30s confirmations" had already reached TP1 before the confirmation horizon.
# We therefore measure a real delayed-entry counterfactual before considering a production gate.
EXECUTION_GATE_SHADOW_ENABLED = os.getenv("EXECUTION_GATE_SHADOW_ENABLED", "1").strip() not in ("0", "false", "False")
EXECUTION_GATE_HORIZON_MS = int(os.getenv("EXECUTION_GATE_HORIZON_MS", "15000"))
EXECUTION_GATE_DATA_TIMEOUT_MS = int(os.getenv("EXECUTION_GATE_DATA_TIMEOUT_MS", "25000"))
GATE_COUNTERFACTUAL_HORIZONS_S = (15, 30, 60, 120, 300, 900)
ABSORPTION_RISK_SHADOW_ENABLED = os.getenv("ABSORPTION_RISK_SHADOW_ENABLED", "1").strip() not in ("0", "false", "False")
ABSORPTION_FLOW30_MIN = float(os.getenv("ABSORPTION_FLOW30_MIN", "8.0"))
ABSORPTION_BID_MAX = float(os.getenv("ABSORPTION_BID_MAX", "0.30"))

# V5.12: execution-quality classifier. SHADOW ONLY; production Premium remains unchanged.
EXECUTION_GATE_V2_SHADOW_ENABLED = os.getenv("EXECUTION_GATE_V2_SHADOW_ENABLED", "1").strip() not in ("0", "false", "False")
OVEREXTENDED_60_PCT = float(os.getenv("OVEREXTENDED_60_PCT", "2.0"))
LOCAL_TOP_MFE15_MAX = float(os.getenv("LOCAL_TOP_MFE15_MAX", "0.25"))
LOCAL_TOP_MAE30_MIN = float(os.getenv("LOCAL_TOP_MAE30_MIN", "-0.35"))
EXECUTION_GATE_V21_SHADOW_ENABLED = os.getenv("EXECUTION_GATE_V21_SHADOW_ENABLED", "1").strip() not in ("0", "false", "False")

# V5.13.2: forward-only exit/delayed-entry audit. SHADOW ONLY.
# These counters never suppress Premiums, never alter public TP/stop levels and never place orders.
FORWARD_STRATEGY_SHADOW_ENABLED = os.getenv("FORWARD_STRATEGY_SHADOW_ENABLED", "1").strip() not in ("0", "false", "False")
FORWARD_RUNNER_TARGET_PCT = float(os.getenv("FORWARD_RUNNER_TARGET_PCT", "5.0"))
WAIT_RECLAIM_FORWARD_ENABLED = os.getenv("WAIT_RECLAIM_FORWARD_ENABLED", "1").strip() not in ("0", "false", "False")
WAIT_RECLAIM_MAX_DELAY_S = int(os.getenv("WAIT_RECLAIM_MAX_DELAY_S", "180"))
FORWARD_STRATEGY_HORIZON_S = int(os.getenv("FORWARD_STRATEGY_HORIZON_S", "3600"))

# V5.13.3: stage-entry + exit-policy forward cohorts. SHADOW ONLY.
# Purpose: separate late discovery, bad selection and exit-management problems without touching production.
STAGE_ENTRY_FORWARD_ENABLED = os.getenv("STAGE_ENTRY_FORWARD_ENABLED", "1").strip() not in ("0", "false", "False")
STAGE_ENTRY_HORIZON_S = int(os.getenv("STAGE_ENTRY_HORIZON_S", "3600"))
SECONDARY_60_FORWARD_ENABLED = os.getenv("SECONDARY_60_FORWARD_ENABLED", "1").strip() not in ("0", "false", "False")
FORWARD_FEE_ROUNDTRIP_PCT = float(os.getenv("FORWARD_FEE_ROUNDTRIP_PCT", "0.10"))
FORWARD_BE_BUFFER_10_PCT = float(os.getenv("FORWARD_BE_BUFFER_10_PCT", "0.10"))
FORWARD_BE_BUFFER_15_PCT = float(os.getenv("FORWARD_BE_BUFFER_15_PCT", "0.15"))
FORWARD_LATE_RUNNER_25_ACTIVATE_PCT = float(os.getenv("FORWARD_LATE_RUNNER_25_ACTIVATE_PCT", "2.50"))
FORWARD_LATE_RUNNER_25_TRAIL_PCT = float(os.getenv("FORWARD_LATE_RUNNER_25_TRAIL_PCT", "1.00"))
FORWARD_LATE_RUNNER_30_ACTIVATE_PCT = float(os.getenv("FORWARD_LATE_RUNNER_30_ACTIVATE_PCT", "3.00"))
FORWARD_LATE_RUNNER_30_TRAIL_PCT = float(os.getenv("FORWARD_LATE_RUNNER_30_TRAIL_PCT", "1.25"))

# V5.13.4: real-early / trend-build research. This is observer-only and never opens a trade.
TREND_BUILDUP_ENABLED = os.getenv("TREND_BUILDUP_ENABLED", "1").strip() not in ("0", "false", "False")
TREND_BUILDUP_NOTIFY = os.getenv("TREND_BUILDUP_NOTIFY", "1").strip() not in ("0", "false", "False")
TREND_BUILDUP_MIN_SCORE = int(os.getenv("TREND_BUILDUP_MIN_SCORE", "82"))
TREND_BUILDUP_CONFIRM_PASSES = max(2, int(os.getenv("TREND_BUILDUP_CONFIRM_PASSES", "3")))
TREND_BUILDUP_CONFIRM_INTERVAL_S = max(3.0, float(os.getenv("TREND_BUILDUP_CONFIRM_INTERVAL_S", "5")))
TREND_BUILDUP_COOLDOWN_S = max(300, int(os.getenv("TREND_BUILDUP_COOLDOWN_S", "1800")))
LIQUIDITY_TRANSITION_V3_ENABLED = os.getenv("LIQUIDITY_TRANSITION_V3_ENABLED", "1").strip() not in ("0", "false", "False")

# Position observer is informational only: it never places/cancels/changes an order.
POSITION_OBSERVER_ENABLED = os.getenv("POSITION_OBSERVER_ENABLED", "1").strip() not in ("0", "false", "False")
POSITION_OBSERVER_POLL_SECONDS = max(3, int(os.getenv("POSITION_OBSERVER_POLL_SECONDS", "5")))
POSITION_ENTRY_HYSTERESIS_PCT = max(0.01, float(os.getenv("POSITION_ENTRY_HYSTERESIS_PCT", "0.10")))
POSITION_ENTRY_CONFIRM_SECONDS = max(0, int(os.getenv("POSITION_ENTRY_CONFIRM_SECONDS", "5")))
POSITION_ROE_MILESTONES = tuple(sorted({abs(float(x)) for x in os.getenv("POSITION_ROE_MILESTONES", "5,10,20").split(",") if x.strip()})) or (5.0,10.0,20.0)
AUTO_TRADE_RISK_FEE_PCT = max(0.0, float(os.getenv("AUTO_TRADE_RISK_FEE_PCT", "0.10")))

# V5.13: AutoTrade execution infrastructure. SAFE BY DEFAULT.
# LIVE can never start automatically after a deploy/restart. The default path is OFF -> DRY -> explicit LIVE confirmation.
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
AUTO_TRADE_LIVE_ALLOWED = os.getenv("AUTO_TRADE_LIVE_ALLOWED", "0").strip().lower() in ("1", "true", "yes", "on")
AUTO_TRADE_BOOT_MODE = os.getenv("AUTO_TRADE_BOOT_MODE", "OFF").strip().upper()
if AUTO_TRADE_BOOT_MODE not in ("OFF", "DRY"):
    AUTO_TRADE_BOOT_MODE = "OFF"  # never boot LIVE
AUTO_TRADE_MARGIN_USDT_DEFAULT = float(os.getenv("AUTO_TRADE_MARGIN_USDT", "200"))
AUTO_TRADE_LEVERAGE_DEFAULT = int(os.getenv("AUTO_TRADE_LEVERAGE", "10"))
AUTO_TRADE_MAX_OPEN_POSITIONS_DEFAULT = int(os.getenv("AUTO_TRADE_MAX_OPEN_POSITIONS", "3"))
AUTO_TRADE_DAILY_MAX_LOSS_PCT_DEFAULT = float(os.getenv("AUTO_TRADE_DAILY_MAX_LOSS_PCT", "3.0"))
AUTO_TRADE_MAX_CONSECUTIVE_STOPS_DEFAULT = int(os.getenv("AUTO_TRADE_MAX_CONSECUTIVE_STOPS", "4"))
AUTO_TRADE_STOP_COOLDOWN_MINUTES_DEFAULT = int(os.getenv("AUTO_TRADE_STOP_COOLDOWN_MINUTES", "60"))
AUTO_TRADE_SIM_BALANCE_USDT = float(os.getenv("AUTO_TRADE_SIM_BALANCE_USDT", "2000"))
AUTO_TRADE_MARGIN_TYPE_DEFAULT = os.getenv("AUTO_TRADE_MARGIN_TYPE", "ISOLATED").strip().upper()
AUTO_TRADE_MAX_ENTRY_SLIPPAGE_PCT_DEFAULT = float(os.getenv("AUTO_TRADE_MAX_ENTRY_SLIPPAGE_PCT", "0.30"))
AUTO_TRADE_RECONCILE_SECONDS = max(2, int(os.getenv("AUTO_TRADE_RECONCILE_SECONDS", "5")))
AUTO_TRADE_EXIT_PROFILE_DEFAULT = os.getenv("AUTO_TRADE_EXIT_PROFILE", "CURRENT_TP2").strip().upper()
if AUTO_TRADE_EXIT_PROFILE_DEFAULT not in ("CURRENT_TP2", "PARTIAL_RUNNER"):
    AUTO_TRADE_EXIT_PROFILE_DEFAULT = "CURRENT_TP2"
AUTO_TRADE_RUNNER_FRACTION_DEFAULT = float(os.getenv("AUTO_TRADE_RUNNER_FRACTION", "0.50"))
AUTO_TRADE_RUNNER_TARGET_PCT_DEFAULT = float(os.getenv("AUTO_TRADE_RUNNER_TARGET_PCT", "5.0"))
AUTO_TRADE_CLIENT_PREFIX = os.getenv("AUTO_TRADE_CLIENT_PREFIX", "MBOT").strip()[:8] or "MBOT"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("momentum-v5.13.4")


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
    mark_price: float = 0.0
    mark_ts: float = 0.0
    last_price: float = 0.0
    last_trade_event_ms: int = 0
    last_trade_receive_ms: int = 0
    last_book_event_ms: int = 0
    last_book_receive_ms: int = 0
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
    minute_high_ts_ms: int = 0
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
    episode_peak_ts: float = 0.0
    episode_low_price: float = 0.0
    episode_low_ts: float = 0.0
    episode_had_early: bool = False
    episode_had_premium: bool = False
    episode_anchor_avg1m: float = 0.0
    prev_meaningful_episode_id: int = 0
    prev_meaningful_ts: float = 0.0
    prev_meaningful_price: float = 0.0
    prev_meaningful_peak_price: float = 0.0
    prev_meaningful_low_price: float = 0.0
    anchor_avg1m: float = 0.0
    anchor_ts: float = 0.0
    last_second_wave_ts: float = 0.0
    last_prebreakout_ts: float = 0.0
    last_flow_structure_ts: float = 0.0
    # V5.8 observer-only cooldowns.
    last_near_miss_ts: float = 0.0
    last_ignition_ts: float = 0.0
    last_fast_early_shadow_ts: float = 0.0
    # V5.13.4 trend-build persistence is independent from momentum_episode resets.
    trend_build_passes: int = 0
    trend_build_last_check: float = 0.0
    trend_build_last_notify: float = 0.0


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

    # V5.7 micro-execution / breakout-acceptance shadow tracking.
    signal_generated_ts_ms: int = 0
    breakout_reference_price: float = 0.0
    micro_completed: set = field(default_factory=set)
    acceptance_finalized: bool = False
    acceptance_last_ts: float = 0.0
    acceptance_above_s: float = 0.0
    acceptance_total_s: float = 0.0
    acceptance_min_dist_pct: float = 999.0
    acceptance_close_dist_pct: float = 0.0
    acceptance_reclaim_count: int = 0
    acceptance_first_reclaim_ms: Optional[int] = None
    acceptance_was_above: Optional[bool] = None
    acceptance_max_pullback_signal_pct: float = 0.0
    acceptance_max_pullback_peak_pct: float = 0.0
    acceptance_new_high_count: int = 0
    acceptance_first_new_high_ms: Optional[int] = None
    acceptance_peak_price: float = 0.0
    acceptance_status: str = "PENDING"
    reclaim_event_sent: bool = False
    runner_exit_sent: bool = False
    runner_peak_price: float = 0.0
    # V5.8 depth/progress tracking. Never gates Premium creation.
    liquidity_completed: set = field(default_factory=set)
    progress_finalized: bool = False
    # V5.10 post-Premium risk research bookkeeping. SHADOW only.
    liq_risk_15_saved: bool = False
    liq_risk_30_saved: bool = False
    fail_risk_60_saved: bool = False
    # V5.11 delayed-entry gate simulator. SHADOW only; production Premium is still immediate.
    gate_shadow_finalized: bool = False
    gate_shadow_decision: str = "PENDING"
    gate_shadow_ts: float = 0.0
    gate_shadow_price: float = 0.0
    gate_shadow_mfe: float = 0.0
    gate_shadow_mae: float = 0.0
    gate_shadow_tp1_hit_s: Optional[float] = None
    gate_shadow_tp2_hit_s: Optional[float] = None
    gate_shadow_stop_hit_s: Optional[float] = None
    gate_shadow_first_event: Optional[str] = None
    gate_shadow_completed: set = field(default_factory=set)
    sticky_early_hostile: bool = False
    # V5.13.2 forward strategy audit. SHADOW only.
    execution_status_at_signal: str = "UNKNOWN"
    forward_be_hit_s: Optional[float] = None
    forward_runner5_hit_s: Optional[float] = None
    forward_mfe_after_tp1: float = 0.0
    forward_mae_after_tp1: float = 0.0
    forward_mfe_after_tp2: float = 0.0
    forward_mae_after_tp2: float = 0.0
    delayed_shadows: dict = field(default_factory=dict)


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


@dataclass
class PendingStageEntry:
    row_id: int
    symbol: str
    stage: str
    episode_id: int
    entry_price: float
    created_ts: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    signal_id: Optional[int] = None
    decision: str = ""
    entry_age_s: Optional[float] = None
    tp1_hit_s: Optional[float] = None
    tp2_hit_s: Optional[float] = None
    stop_hit_s: Optional[float] = None
    be0_hit_s: Optional[float] = None
    be10_hit_s: Optional[float] = None
    be15_hit_s: Optional[float] = None
    mfe: float = 0.0
    mae: float = 0.0
    runner25_active_s: Optional[float] = None
    runner25_peak: float = 0.0
    runner25_exit_s: Optional[float] = None
    runner25_exit_price: Optional[float] = None
    runner30_active_s: Optional[float] = None
    runner30_peak: float = 0.0
    runner30_exit_s: Optional[float] = None
    runner30_exit_price: Optional[float] = None
    completed_60m: bool = False


states: Dict[str, SymbolState] = defaultdict(SymbolState)
symbols: List[str] = []
stop_event = asyncio.Event()
pending_outcomes: List[PendingOutcome] = []
pending_radars: List[PendingRadar] = []
pending_gainers: List[PendingGainer] = []
pending_research: List[PendingResearch] = []
pending_shadow_events: List[PendingShadowEvent] = []
pending_stage_entries: List[PendingStageEntry] = []
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

# V5.13 AutoTrade runtime state. Settings are persisted in SQLite; LIVE is forced OFF on every process start.
autotrade_cfg = {
    "mode": AUTO_TRADE_BOOT_MODE,
    "trade_margin_usdt": AUTO_TRADE_MARGIN_USDT_DEFAULT,
    "leverage": AUTO_TRADE_LEVERAGE_DEFAULT,
    "max_open_positions": AUTO_TRADE_MAX_OPEN_POSITIONS_DEFAULT,
    "daily_max_loss_pct": AUTO_TRADE_DAILY_MAX_LOSS_PCT_DEFAULT,
    "max_consecutive_stops": AUTO_TRADE_MAX_CONSECUTIVE_STOPS_DEFAULT,
    "stop_cooldown_minutes": AUTO_TRADE_STOP_COOLDOWN_MINUTES_DEFAULT,
    "margin_type": AUTO_TRADE_MARGIN_TYPE_DEFAULT,
    "max_entry_slippage_pct": AUTO_TRADE_MAX_ENTRY_SLIPPAGE_PCT_DEFAULT,
    "exit_profile": AUTO_TRADE_EXIT_PROFILE_DEFAULT,
    "runner_fraction": AUTO_TRADE_RUNNER_FRACTION_DEFAULT,
    "runner_target_pct": AUTO_TRADE_RUNNER_TARGET_PCT_DEFAULT,
}
autotrade_active: Dict[int, dict] = {}
autotrade_active_by_symbol: Dict[str, Set[int]] = defaultdict(set)
autotrade_live_confirm: Dict[str, Tuple[str, float]] = {}
autotrade_pending_setting: Dict[str, Tuple[str, str, float]] = {}
# Cached Binance Futures balance for the Telegram control panel. This is informational only;
# risk-lock calculations continue to use the frozen daily start balance in autotrade_daily.
autotrade_account_cache = {
    "wallet_balance": None,
    "available_balance": None,
    "updated_ts": 0.0,
    "error": "",
}
exchange_filters: Dict[str, dict] = {}

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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_context (
            signal_id INTEGER PRIMARY KEY,
            premium_ordinal INTEGER,
            signal_generated_ts_ms INTEGER,
            breakout_reference_price REAL,
            dist_breakout_pct REAL,
            prev_1m_high REAL, prev_3m_high REAL,
            dist_prev_1m_high_pct REAL, dist_prev_3m_high_pct REAL,
            current_1m_range_pct REAL, current_1m_body_pct REAL, current_1m_upper_wick_pct REAL,
            episode_age_s REAL, distance_from_episode_low_pct REAL,
            dist_episode_peak_pct REAL, seconds_since_episode_peak REAL,
            oi_prev5 REAL, oi_accel5 REAL, oi_regime TEXT,
            phase_risk TEXT, phase_risk_points INTEGER,
            trade_data_age_ms INTEGER, book_data_age_ms INTEGER, event_receive_lag_ms INTEGER,
            signal_bid REAL, signal_ask REAL, signal_mark REAL,
            execution_status TEXT, signal_to_ask_drift_pct REAL, entry_band_distance_pct REAL,
            live_rr1 REAL, live_rr2 REAL, stop_risk_pct REAL,
            updated_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_micro_snapshots (
            signal_id INTEGER NOT NULL,
            horizon_ms INTEGER NOT NULL,
            observed_ts_ms INTEGER NOT NULL,
            age_ms INTEGER NOT NULL,
            last_price REAL, bid REAL, ask REAL, mark_price REAL,
            return_pct REAL, mfe_pct REAL, mae_pct REAL, spread_bps REAL,
            chg30 REAL, chg60 REAL, flow30 REAL, buy30 REAL, book_imbalance REAL, rel30 REAL,
            PRIMARY KEY(signal_id, horizon_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_entry_validation (
            signal_id INTEGER PRIMARY KEY,
            horizon_ms INTEGER NOT NULL,
            finalized_ts_ms INTEGER NOT NULL,
            breakout_reference_price REAL,
            time_above_ratio REAL, min_dist_breakout_pct REAL, close_dist_breakout_pct REAL,
            reclaim_count INTEGER, first_reclaim_ms INTEGER,
            max_pullback_signal_pct REAL, max_pullback_peak_pct REAL,
            new_high_count INTEGER, first_new_high_ms INTEGER,
            status TEXT, reason TEXT,
            updated_ts INTEGER NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_liquidity_snapshots (
            signal_id INTEGER NOT NULL,
            horizon_ms INTEGER NOT NULL,
            observed_ts_ms INTEGER NOT NULL,
            request_delay_ms INTEGER,
            reference_price REAL,
            mid_price REAL, best_bid REAL, best_ask REAL,
            bid_010 REAL, bid_025 REAL, bid_050 REAL, bid_100 REAL,
            ask_010 REAL, ask_025 REAL, ask_050 REAL, ask_100 REAL,
            ask_before_tp1 REAL,
            bid_ratio_025 REAL,
            largest_ask_wall_price REAL, largest_ask_wall_notional REAL,
            largest_ask_wall_distance_pct REAL, largest_ask_wall_ratio REAL,
            largest_bid_wall_price REAL, largest_bid_wall_notional REAL,
            largest_bid_wall_distance_pct REAL, largest_bid_wall_ratio REAL,
            aggressive_buy_speed_usdt_s REAL,
            ask025_clear_s REAL, tp1_clear_s REAL,
            wall_persisted INTEGER, wall_remaining_ratio REAL,
            wall_replenished INTEGER, wall_cancelled INTEGER,
            barrier_label TEXT, absorption_flag INTEGER,
            depth_levels INTEGER, ask_coverage_pct REAL, bid_coverage_pct REAL,
            ask025_vs_initial REAL, bid025_vs_initial REAL, tp1_ask_vs_initial REAL,
            bid_ratio_delta REAL, ask025_clear_vs_initial REAL, tp1_clear_vs_initial REAL,
            dynamic_score INTEGER, dynamic_state TEXT, dynamic_reason TEXT,
            updated_ts INTEGER NOT NULL,
            PRIMARY KEY(signal_id, horizon_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_progress_validation (
            signal_id INTEGER PRIMARY KEY,
            finalized_ts_ms INTEGER NOT NULL,
            ret30 REAL, ret60 REAL, mfe60 REAL, mae60 REAL,
            rel30_60 REAL, flow30_60 REAL, buy30_60 REAL,
            status TEXT, reason TEXT,
            updated_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_execution_composite (
            signal_id INTEGER PRIMARY KEY,
            finalized_ts_ms INTEGER NOT NULL,
            liq_state_5 TEXT, liq_score_5 INTEGER,
            liq_state_15 TEXT, liq_score_15 INTEGER,
            liq_state_30 TEXT, liq_score_30 INTEGER,
            progress_status TEXT, composite_state TEXT, reason TEXT,
            updated_ts INTEGER NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_failure_risk (
            signal_id INTEGER NOT NULL,
            horizon_ms INTEGER NOT NULL,
            observed_ts_ms INTEGER NOT NULL,
            stage TEXT NOT NULL,
            risk_state TEXT NOT NULL,
            raw_risk_state TEXT,
            risk_score INTEGER,
            price REAL, return_pct REAL,
            progress_status TEXT,
            liquidity_v1_state TEXT, liquidity_v1_score INTEGER,
            liquidity_v2_state TEXT, liquidity_v2_score INTEGER,
            liquidity_core_state TEXT, liquidity_core_score INTEGER,
            bid_ratio_025 REAL, bid025_vs_initial REAL, ask025_vs_initial REAL,
            wall_remaining_ratio REAL, ask025_clear_s REAL, tp1_clear_s REAL,
            wall_distance_pct REAL, wall_ratio REAL,
            trade_active INTEGER, terminal_event TEXT, terminal_age_s REAL,
            entry_touched_before_horizon INTEGER, tp1_before_horizon INTEGER,
            tp2_before_horizon INTEGER, stop_before_horizon INTEGER,
            price_state TEXT, micro_mfe REAL, micro_mae REAL,
            reason TEXT, updated_ts INTEGER NOT NULL,
            PRIMARY KEY(signal_id, horizon_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_conflict_events (
            signal_id INTEGER NOT NULL,
            event_code TEXT NOT NULL,
            horizon_ms INTEGER NOT NULL,
            observed_ts_ms INTEGER NOT NULL,
            price REAL, return_pct REAL,
            details TEXT, updated_ts INTEGER NOT NULL,
            PRIMARY KEY(signal_id, event_code)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS failure_risk_reclaims (
            signal_id INTEGER PRIMARY KEY,
            risk_event_id INTEGER,
            risk_event TEXT, risk_age_s REAL, risk_price REAL,
            stop_s REAL, reclaim_age_s REAL, reclaim_after_risk_s REAL,
            reclaim_price REAL, bucket TEXT, score INTEGER,
            updated_ts INTEGER NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_execution_gate_shadow (
            signal_id INTEGER PRIMARY KEY,
            gate_horizon_ms INTEGER NOT NULL, observed_ts_ms INTEGER NOT NULL,
            decision TEXT NOT NULL, reason TEXT,
            liquidity_v1_state TEXT, liquidity_v1_score INTEGER,
            liquidity_v2_state TEXT, liquidity_v2_score INTEGER,
            liquidity_core_state TEXT, liquidity_core_score INTEGER,
            any_hostile INTEGER, sticky_early_hostile INTEGER,
            recovered_30 INTEGER, persistent_hostile_30 INTEGER,
            absorption_risk INTEGER,
            original_price REAL, gate_price REAL, signal_to_gate_pct REAL,
            micro_mfe REAL, micro_mae REAL,
            trade_active INTEGER, terminal_event TEXT, execution_status TEXT,
            tp1_remaining_pct REAL, tp2_remaining_pct REAL, stop_risk_pct REAL,
            gate_tp1_hit_s REAL, gate_tp2_hit_s REAL, gate_stop_hit_s REAL,
            gate_first_event TEXT, gate_mfe REAL, gate_mae REAL, completed_60m INTEGER DEFAULT 0,
            updated_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_execution_gate_v2_shadow (
            signal_id INTEGER PRIMARY KEY,
            finalized_ts_ms INTEGER NOT NULL,
            decision TEXT NOT NULL, reason TEXT,
            progress_status TEXT, composite_state TEXT,
            gate_v1_decision TEXT, sticky_early_hostile INTEGER,
            chg30_signal REAL, chg60_signal REAL, overextended_60 INTEGER,
            phase_risk TEXT, live_rr1 REAL,
            mfe15 REAL, mae15 REAL, mfe30 REAL, mae30 REAL, mfe60 REAL, mae60 REAL,
            local_top_proxy INTEGER,
            trade_active_60 INTEGER, terminal_event_60 TEXT, terminal_age_s REAL,
            production_gate INTEGER NOT NULL DEFAULT 0,
            updated_ts INTEGER NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_gate_counterfactual (
            signal_id INTEGER NOT NULL, post_gate_horizon_s INTEGER NOT NULL,
            observed_ts_ms INTEGER NOT NULL, return_pct REAL, mfe_pct REAL, mae_pct REAL,
            PRIMARY KEY(signal_id, post_gate_horizon_s)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS missed_runner_audit (
            source_event_id INTEGER PRIMARY KEY,
            event_type TEXT NOT NULL,
            event_ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            start_price REAL NOT NULL,
            horizon_s INTEGER NOT NULL,
            return_pct REAL, mfe_pct REAL, mae_pct REAL,
            early_count INTEGER, premium_count INTEGER,
            classification TEXT NOT NULL,
            updated_ts INTEGER NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_episode_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            start_ts INTEGER NOT NULL,
            last_event_ts INTEGER NOT NULL,
            event_count INTEGER NOT NULL DEFAULT 0,
            runner_event_count INTEGER NOT NULL DEFAULT 0,
            missed_event_count INTEGER NOT NULL DEFAULT 0,
            premium_captured INTEGER NOT NULL DEFAULT 0,
            early_captured INTEGER NOT NULL DEFAULT 0,
            max_runner_size INTEGER NOT NULL DEFAULT 0,
            max_mfe_pct REAL, worst_mae_pct REAL,
            first_event_id INTEGER, first_event_type TEXT,
            first_premium_ts INTEGER, first_early_ts INTEGER,
            blocker_counts_json TEXT,
            updated_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_episode_symbol_last ON discovery_episode_audit(symbol,last_event_ts)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_deployments (
            started_ts_ms INTEGER PRIMARY KEY,
            bot_version TEXT NOT NULL,
            research_logic_version TEXT NOT NULL,
            db_path TEXT, note TEXT, updated_ts INTEGER NOT NULL
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
    ensure_column(conn, "signals_v2", "premium_ordinal", "INTEGER")
    ensure_column(conn, "signals_v2", "oi_prev5", "REAL")
    ensure_column(conn, "signals_v2", "oi_accel5", "REAL")
    ensure_column(conn, "signals_v2", "oi_regime", "TEXT")
    ensure_column(conn, "signals_v2", "rel30", "REAL")
    ensure_column(conn, "signals_v2", "btc30", "REAL")
    ensure_column(conn, "signals_v2", "gainer_rank", "INTEGER")
    ensure_column(conn, "signals_v2", "rank_velocity", "REAL")
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
    ensure_column(conn, "research_events", "origin_signal_id", "INTEGER")
    ensure_column(conn, "research_events", "oi_prev5", "REAL")
    ensure_column(conn, "research_events", "oi_accel5", "REAL")
    ensure_column(conn, "research_events", "oi_regime", "TEXT")
    ensure_column(conn, "research_events", "btc30", "REAL")
    ensure_column(conn, "research_events", "price_accel10", "REAL")
    ensure_column(conn, "research_events", "flow_accel10", "REAL")
    ensure_column(conn, "research_events", "shadow_score", "INTEGER")
    ensure_column(conn, "research_events", "shadow_label", "TEXT")
    ensure_column(conn, "research_events", "gate_failures", "TEXT")
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
    ensure_column(conn, "premium_context", "phase_risk", "TEXT")
    ensure_column(conn, "premium_context", "phase_risk_points", "INTEGER")
    ensure_column(conn, "premium_context", "execution_status", "TEXT")
    ensure_column(conn, "premium_context", "signal_to_ask_drift_pct", "REAL")
    ensure_column(conn, "premium_context", "entry_band_distance_pct", "REAL")
    ensure_column(conn, "premium_context", "live_rr1", "REAL")
    ensure_column(conn, "premium_context", "live_rr2", "REAL")
    ensure_column(conn, "premium_context", "stop_risk_pct", "REAL")
    ensure_column(conn, "premium_context", "rel30", "REAL")
    ensure_column(conn, "premium_context", "btc30", "REAL")
    ensure_column(conn, "premium_context", "gainer_rank", "INTEGER")
    ensure_column(conn, "premium_context", "rank_velocity", "REAL")
    ensure_column(conn, "notification_log", "signal_id", "INTEGER")
    ensure_column(conn, "notification_log", "send_start_ts_ms", "INTEGER")
    ensure_column(conn, "notification_log", "send_done_ts_ms", "INTEGER")
    # V5.13.1 actionable 15/30s execution-quality cohort. SHADOW ONLY.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS premium_execution_gate_v21_shadow (
               signal_id INTEGER PRIMARY KEY,
               decision_15 TEXT, reason_15 TEXT, trade_active_15 INTEGER,
               decision_30 TEXT, reason_30 TEXT, trade_active_30 INTEGER,
               final_decision TEXT, actionable_horizon_ms INTEGER,
               overextended_60 INTEGER DEFAULT 0, production_gate INTEGER DEFAULT 0,
               updated_ts INTEGER NOT NULL
           )"""
    )

    # V5.13.2 forward exit/delayed-entry strategy audit. SHADOW ONLY.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS premium_exit_forward_shadow (
               signal_id INTEGER PRIMARY KEY,
               entry_price REAL, tp1_price REAL, tp2_price REAL, stop_price REAL,
               be_hit_s REAL, runner5_hit_s REAL, runner_target_pct REAL,
               mfe_after_tp1 REAL DEFAULT 0, mae_after_tp1 REAL DEFAULT 0,
               mfe_after_tp2 REAL DEFAULT 0, mae_after_tp2 REAL DEFAULT 0,
               close60_price REAL, completed_60m INTEGER DEFAULT 0,
               current_outcome TEXT, current_return_pct REAL,
               full_be_outcome TEXT, full_be_return_pct REAL,
               partial50_be_outcome TEXT, partial50_be_return_pct REAL,
               tp2_runner50_outcome TEXT, tp2_runner50_return_pct REAL,
               updated_ts INTEGER NOT NULL
           )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS premium_delayed_entry_shadow (
               signal_id INTEGER NOT NULL, strategy TEXT NOT NULL,
               source_status TEXT, decision TEXT, horizon_ms INTEGER, pullback_s REAL,
               entry_age_s REAL, entry_price REAL, stop_price REAL, tp1_price REAL, tp2_price REAL,
               tp1_hit_s REAL, tp2_hit_s REAL, stop_hit_s REAL, be_hit_s REAL,
               mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
               close60_price REAL, completed_60m INTEGER DEFAULT 0,
               current_outcome TEXT, current_return_pct REAL,
               be_outcome TEXT, be_return_pct REAL, no_entry_reason TEXT,
               updated_ts INTEGER NOT NULL, PRIMARY KEY(signal_id,strategy)
           )"""
    )

    # V5.13.3 unified stage-entry / policy cohort. SHADOW ONLY.
    # It tracks CANDIDATE, EARLY, PREMIUM, GATE15/30 and SECONDARY60 with true forward event order.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS entry_stage_forward_shadow (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               symbol TEXT NOT NULL, episode_id INTEGER, stage TEXT NOT NULL, signal_id INTEGER,
               decision TEXT, created_ts_ms INTEGER NOT NULL, entry_age_s REAL, entry_price REAL NOT NULL,
               stop_price REAL, tp1_price REAL, tp2_price REAL,
               tp1_hit_s REAL, tp2_hit_s REAL, stop_hit_s REAL,
               be0_hit_s REAL, be10_hit_s REAL, be15_hit_s REAL,
               mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
               runner25_active_s REAL, runner25_exit_s REAL, runner25_exit_price REAL, runner25_peak REAL,
               runner30_active_s REAL, runner30_exit_s REAL, runner30_exit_price REAL, runner30_peak REAL,
               became_premium INTEGER DEFAULT 0, close60_price REAL, completed_60m INTEGER DEFAULT 0,
               current_outcome TEXT, current_return_pct REAL,
               be0_outcome TEXT, be0_return_pct REAL, be10_outcome TEXT, be10_return_pct REAL, be15_outcome TEXT, be15_return_pct REAL,
               late25_outcome TEXT, late25_return_pct REAL, late30_outcome TEXT, late30_return_pct REAL,
               fee_adjusted_current_pct REAL, fee_adjusted_be0_pct REAL, fee_adjusted_be10_pct REAL, fee_adjusted_be15_pct REAL,
               fee_adjusted_late25_pct REAL, fee_adjusted_late30_pct REAL,
               updated_ts INTEGER NOT NULL
           )"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stage_shadow_symbol_episode ON entry_stage_forward_shadow(symbol,episode_id,stage)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stage_shadow_signal ON entry_stage_forward_shadow(signal_id,stage)")

    # V5.13 AutoTrade persistence. These tables are independent from scanner/research tables.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS autotrade_settings (
               key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_ts INTEGER NOT NULL
           )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS autotrade_trades (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               signal_id INTEGER UNIQUE, symbol TEXT NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL,
               side TEXT NOT NULL DEFAULT 'LONG', position_side TEXT,
               margin_usdt REAL NOT NULL, leverage INTEGER NOT NULL, notional_usdt REAL NOT NULL,
               entry_signal_price REAL, entry_price REAL, qty REAL, expected_qty REAL,
               stop_price REAL, tp1_price REAL, tp2_price REAL, runner_price REAL,
               exit_profile TEXT, runner_fraction REAL, runner_target_pct REAL,
               tp1_hit INTEGER DEFAULT 0, partial_realized_pnl REAL DEFAULT 0,
               entry_order_id TEXT, entry_client_id TEXT,
               tp1_algo_id TEXT, tp1_client_id TEXT,
               tp2_algo_id TEXT, tp2_client_id TEXT,
               stop_algo_id TEXT, stop_client_id TEXT,
               opened_ts_ms INTEGER, closed_ts_ms INTEGER, close_reason TEXT, exit_price REAL,
               realized_pnl REAL DEFAULT 0, commission REAL DEFAULT 0, net_pnl REAL DEFAULT 0,
               manual_intervention INTEGER DEFAULT 0, last_error TEXT, updated_ts INTEGER NOT NULL
           )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS autotrade_events (
               id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, trade_id INTEGER,
               signal_id INTEGER, symbol TEXT, event TEXT NOT NULL, detail TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS autotrade_daily (
               local_date TEXT NOT NULL, scope TEXT NOT NULL, start_balance REAL NOT NULL, realized_net_pnl REAL DEFAULT 0,
               consecutive_stops INTEGER DEFAULT 0, locked INTEGER DEFAULT 0, lock_reason TEXT, cooldown_until_ts INTEGER DEFAULT 0,
               updated_ts INTEGER NOT NULL, PRIMARY KEY(local_date, scope)
           )"""
    )

    ensure_column(conn, "notification_log", "telegram_message_id", "INTEGER")
    ensure_column(conn, "notification_log", "live_bid", "REAL")
    ensure_column(conn, "notification_log", "live_ask", "REAL")
    ensure_column(conn, "notification_log", "price_drift_pct", "REAL")
    ensure_column(conn, "notification_log", "entry_status", "TEXT")
    ensure_column(conn, "momentum_episodes", "low_price", "REAL")
    ensure_column(conn, "momentum_episodes", "low_return_pct", "REAL")
    ensure_column(conn, "premium_liquidity_snapshots", "ask025_vs_initial", "REAL")
    ensure_column(conn, "premium_liquidity_snapshots", "bid025_vs_initial", "REAL")
    ensure_column(conn, "premium_liquidity_snapshots", "tp1_ask_vs_initial", "REAL")
    ensure_column(conn, "premium_liquidity_snapshots", "bid_ratio_delta", "REAL")
    ensure_column(conn, "premium_liquidity_snapshots", "ask025_clear_vs_initial", "REAL")
    ensure_column(conn, "premium_liquidity_snapshots", "tp1_clear_vs_initial", "REAL")
    ensure_column(conn, "premium_liquidity_snapshots", "dynamic_score", "INTEGER")
    ensure_column(conn, "premium_liquidity_snapshots", "dynamic_state", "TEXT")
    ensure_column(conn, "premium_liquidity_snapshots", "dynamic_reason", "TEXT")
    ensure_column(conn, "premium_liquidity_snapshots", "liquidity_v2_score", "INTEGER")
    ensure_column(conn, "premium_liquidity_snapshots", "liquidity_v2_state", "TEXT")
    ensure_column(conn, "premium_liquidity_snapshots", "liquidity_v2_reason", "TEXT")
    ensure_column(conn, "premium_liquidity_snapshots", "liquidity_core_score", "INTEGER")
    ensure_column(conn, "premium_liquidity_snapshots", "liquidity_core_state", "TEXT")
    ensure_column(conn, "premium_liquidity_snapshots", "liquidity_core_reason", "TEXT")
    ensure_column(conn, "premium_failure_risk", "raw_risk_state", "TEXT")
    ensure_column(conn, "premium_failure_risk", "liquidity_core_state", "TEXT")
    ensure_column(conn, "premium_failure_risk", "liquidity_core_score", "INTEGER")
    ensure_column(conn, "premium_failure_risk", "trade_active", "INTEGER")
    ensure_column(conn, "premium_failure_risk", "terminal_event", "TEXT")
    ensure_column(conn, "premium_failure_risk", "terminal_age_s", "REAL")
    ensure_column(conn, "premium_failure_risk", "entry_touched_before_horizon", "INTEGER")
    ensure_column(conn, "premium_failure_risk", "tp1_before_horizon", "INTEGER")
    ensure_column(conn, "premium_failure_risk", "tp2_before_horizon", "INTEGER")
    ensure_column(conn, "premium_failure_risk", "stop_before_horizon", "INTEGER")
    ensure_column(conn, "premium_failure_risk", "price_state", "TEXT")
    ensure_column(conn, "premium_failure_risk", "micro_mfe", "REAL")
    ensure_column(conn, "premium_failure_risk", "micro_mae", "REAL")
    ensure_column(conn, "autotrade_daily", "cooldown_until_ts", "INTEGER DEFAULT 0")
    ensure_column(conn, "premium_execution_composite", "trade_active", "INTEGER")
    ensure_column(conn, "premium_execution_composite", "terminal_event", "TEXT")
    ensure_column(conn, "premium_execution_composite", "terminal_age_s", "REAL")
    ensure_column(conn, "premium_execution_gate_shadow", "recovered_30", "INTEGER")
    ensure_column(conn, "premium_execution_gate_shadow", "persistent_hostile_30", "INTEGER")
    ensure_column(conn, "premium_execution_gate_shadow", "absorption_risk", "INTEGER")
    ensure_column(conn, "premium_execution_gate_shadow", "gate_tp1_hit_s", "REAL")
    ensure_column(conn, "premium_execution_gate_shadow", "gate_tp2_hit_s", "REAL")
    ensure_column(conn, "premium_execution_gate_shadow", "gate_stop_hit_s", "REAL")
    ensure_column(conn, "premium_execution_gate_shadow", "gate_first_event", "TEXT")
    ensure_column(conn, "premium_execution_gate_shadow", "gate_mfe", "REAL")
    ensure_column(conn, "premium_execution_gate_shadow", "gate_mae", "REAL")
    ensure_column(conn, "premium_execution_gate_shadow", "completed_60m", "INTEGER DEFAULT 0")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS premium_liquidity_transition_v3 (
               signal_id INTEGER PRIMARY KEY, finalized_ts_ms INTEGER NOT NULL,
               liq_state_5 TEXT, liq_state_15 TEXT, liq_state_30 TEXT,
               liq_score_5 INTEGER, liq_score_15 INTEGER, liq_score_30 INTEGER,
               oi_regime TEXT, barrier_30 TEXT, ask025_vs_initial_30 REAL, bid025_vs_initial_30 REAL,
               transition_state TEXT, reason TEXT, updated_ts INTEGER NOT NULL
           )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS position_observer_state (
               symbol TEXT NOT NULL, position_side TEXT NOT NULL, direction TEXT NOT NULL,
               entry_price REAL NOT NULL, qty REAL NOT NULL, leverage INTEGER, source TEXT,
               zone TEXT, pending_zone TEXT, pending_since_ts INTEGER DEFAULT 0,
               profit_hits_json TEXT DEFAULT '[]', loss_hits_json TEXT DEFAULT '[]',
               last_roe REAL, last_unrealized_pnl REAL, active INTEGER DEFAULT 1, updated_ts INTEGER NOT NULL,
               PRIMARY KEY(symbol,position_side)
           )"""
    )
    conn.execute(
        """INSERT OR IGNORE INTO bot_deployments
           (started_ts_ms,bot_version,research_logic_version,db_path,note,updated_ts)
           VALUES (?,?,?,?,?,?)""",
        (PROCESS_STARTED_TS_MS,BOT_VERSION,RESEARCH_LOGIC_VERSION,DB_PATH,
         "V5.13.4: trend-build REAL EARLY watch + liquidity/OI transition V3 shadow + all-position observer + projected-risk guard; production Premium thresholds unchanged",int(time.time())),
    )
    conn.commit()
    conn.close()


def save_signal(m: dict) -> int:
    conn = db_connect()
    cur = conn.execute(
        """
        INSERT INTO signals_v2
        (ts,symbol,level,score,price,chg10,chg30,chg60,chg5,chg15,chg24,
         flow10,flow30,flow60,buy10,buy30,buy60,spread,book_imbalance,
         short_liq,long_liq,oi5,breakout,extended,episode_id,daily_notice_no,flow_eff30,flow_eff60,squeeze_risk,
         funding_rate_pct,premium_ordinal,oi_prev5,oi_accel5,oi_regime,rel30,btc30,gainer_rank,rank_velocity)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(time.time()), m["symbol"], m["level"], m["score"], m["price"],
            m["chg10"], m["chg30"], m["chg60"], m["chg5"], m["chg15"], m["chg24"],
            m["flow10"], m["flow30"], m["flow60"], m["buy10"], m["buy30"], m["buy60"],
            m["spread"], m["book_imbalance"], m["short_liq"], m["long_liq"],
            m.get("oi5"), int(m["breakout"]), int(m["extended"]),
            m.get("episode_id"), m.get("daily_notice_no"), m.get("flow_eff30"), m.get("flow_eff60"), int(bool(m.get("squeeze_risk", False))),
            m.get("funding_rate_pct"), m.get("premium_ordinal"), m.get("oi_prev5"), m.get("oi_accel5"), m.get("oi_regime"),
            m.get("rel30"), m.get("btc30"), gainers_prev_rank.get(m.get("symbol","")),
            rank_velocity_per_min(m.get("symbol",""), gainers_prev_rank.get(m.get("symbol",""))),
        ),
    )
    signal_id = cur.lastrowid
    conn.commit()
    conn.close()
    return signal_id


def next_premium_ordinal(symbol: str) -> int:
    """Per-symbol Premium sequence for the Istanbul calendar day; unlike daily_notice_no it ignores EARLY/SHADOW messages."""
    local_date = datetime.now(IST).date().isoformat()
    conn = db_connect()
    try:
        row = conn.execute(
            """SELECT COUNT(*) FROM signals_v2
               WHERE symbol=? AND level='CONFIRMED' AND date(ts,'unixepoch','+3 hours')=?""",
            (symbol, local_date),
        ).fetchone()
        return int((row[0] or 0) + 1)
    finally:
        conn.close()


def oi_regime_label(oi5: Optional[float]) -> str:
    if oi5 is None:
        return "UNKNOWN"
    if oi5 > 0.05:
        return "POS_GT_005"
    if oi5 < -0.05:
        return "NEG_LT_M005"
    return "NEUTRAL"


def save_premium_context(signal_id: int, m: dict):
    st = states[m["symbol"]]
    conn = db_connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO premium_context
            (signal_id,premium_ordinal,signal_generated_ts_ms,breakout_reference_price,dist_breakout_pct,
             prev_1m_high,prev_3m_high,dist_prev_1m_high_pct,dist_prev_3m_high_pct,
             current_1m_range_pct,current_1m_body_pct,current_1m_upper_wick_pct,
             episode_age_s,distance_from_episode_low_pct,dist_episode_peak_pct,seconds_since_episode_peak,
             oi_prev5,oi_accel5,oi_regime,phase_risk,phase_risk_points,trade_data_age_ms,book_data_age_ms,event_receive_lag_ms,
             signal_bid,signal_ask,signal_mark,execution_status,signal_to_ask_drift_pct,entry_band_distance_pct,live_rr1,live_rr2,stop_risk_pct,
             rel30,btc30,gainer_rank,rank_velocity,updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                signal_id, m.get("premium_ordinal"), m.get("signal_generated_ts_ms"), m.get("breakout_reference_price"),
                m.get("dist_breakout_pct"), m.get("prev_1m_high"), m.get("prev_3m_high"),
                m.get("dist_prev_1m_high_pct"), m.get("dist_prev_3m_high_pct"), m.get("current_1m_range_pct"),
                m.get("current_1m_body_pct"), m.get("current_1m_upper_wick_pct"), m.get("episode_age_s"),
                m.get("distance_from_episode_low_pct"), m.get("dist_episode_peak_pct"), m.get("seconds_since_episode_peak"),
                m.get("oi_prev5"), m.get("oi_accel5"), m.get("oi_regime"), m.get("phase_risk"), m.get("phase_risk_points"),
                m.get("trade_data_age_ms"), m.get("book_data_age_ms"), m.get("event_receive_lag_ms"), st.bid_price or None, st.ask_price or None,
                st.mark_price or None, (m.get("execution") or {}).get("status"), (m.get("execution") or {}).get("drift_pct"),
                (m.get("execution") or {}).get("band_distance_pct"), (m.get("execution") or {}).get("live_rr1"),
                (m.get("execution") or {}).get("live_rr2"), (m.get("execution") or {}).get("stop_risk_pct"),
                m.get("rel30"), m.get("btc30"), gainers_prev_rank.get(m.get("symbol","")),
                rank_velocity_per_min(m.get("symbol",""), gainers_prev_rank.get(m.get("symbol",""))), int(time.time()),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def save_micro_snapshot(p: PendingOutcome, horizon_ms: int, observed_ts_ms: int, price: float):
    st = states[p.symbol]
    m = compute_metrics(p.symbol)
    mid = (st.bid_price + st.ask_price) / 2.0 if st.bid_price > 0 and st.ask_price > 0 else 0.0
    spread_bps = ((st.ask_price - st.bid_price) / mid) * 10000.0 if mid else None
    ret = pct_change(price, p.entry_price)
    conn = db_connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO premium_micro_snapshots
            (signal_id,horizon_ms,observed_ts_ms,age_ms,last_price,bid,ask,mark_price,return_pct,mfe_pct,mae_pct,spread_bps,
             chg30,chg60,flow30,buy30,book_imbalance,rel30)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                p.signal_id, horizon_ms, observed_ts_ms, max(0, int(observed_ts_ms - p.signal_generated_ts_ms)),
                price, st.bid_price or None, st.ask_price or None, st.mark_price or None, ret, p.mfe, p.mae, spread_bps,
                m.get("chg30") if m else None, m.get("chg60") if m else None, m.get("flow30") if m else None,
                m.get("buy30") if m else None, m.get("book_imbalance") if m else None, m.get("rel30") if m else None,
            ),
        )
        conn.commit()
    finally:
        conn.close()



def _depth_levels(raw_levels) -> List[Tuple[float, float]]:
    out = []
    for row in raw_levels or []:
        try:
            px, qty = float(row[0]), float(row[1])
            if px > 0 and qty > 0:
                out.append((px, qty))
        except Exception:
            continue
    return out


def analyze_depth_snapshot(symbol: str, data: dict, reference_price: float, target1: float,
                           m: Optional[dict] = None, initial_wall_price: Optional[float] = None,
                           initial_wall_notional: Optional[float] = None) -> dict:
    """Convert a Binance depth snapshot into distance-normalized liquidity features."""
    bids = _depth_levels(data.get("bids"))
    asks = _depth_levels(data.get("asks"))
    if not bids or not asks or not reference_price:
        return {}
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = (best_bid + best_ask) / 2.0
    band_vals = {}
    for band in LIQUIDITY_BANDS_PCT:
        key = int(round(band * 100))
        band_vals[f"ask_{key:03d}"] = sum(px * qty for px,qty in asks if 0 <= pct_change(px, reference_price) <= band)
        band_vals[f"bid_{key:03d}"] = sum(px * qty for px,qty in bids if 0 <= -pct_change(px, reference_price) <= band)

    ask_before_tp1 = 0.0
    if target1 and target1 > reference_price:
        ask_before_tp1 = sum(px * qty for px,qty in asks if reference_price <= px <= target1)

    ask_near = [(px, px*qty) for px,qty in asks if 0 <= pct_change(px, reference_price) <= 1.0]
    bid_near = [(px, px*qty) for px,qty in bids if 0 <= -pct_change(px, reference_price) <= 1.0]
    ask_notionals = [n for _,n in ask_near]
    bid_notionals = [n for _,n in bid_near]
    ask_med = median(ask_notionals) if ask_notionals else 0.0
    bid_med = median(bid_notionals) if bid_notionals else 0.0
    ask_wall_px, ask_wall_n = max(ask_near, key=lambda x:x[1]) if ask_near else (None,0.0)
    bid_wall_px, bid_wall_n = max(bid_near, key=lambda x:x[1]) if bid_near else (None,0.0)
    ask_wall_ratio = ask_wall_n / max(ask_med, 1e-9) if ask_wall_n else 0.0
    bid_wall_ratio = bid_wall_n / max(bid_med, 1e-9) if bid_wall_n else 0.0
    ask_wall_dist = pct_change(ask_wall_px, reference_price) if ask_wall_px else None
    bid_wall_dist = -pct_change(bid_wall_px, reference_price) if bid_wall_px else None

    m = m or {}
    buy_speed = max(
        (m.get("q10",0) * m.get("buy10",0) / 10.0),
        (m.get("q30",0) * m.get("buy30",0) / 30.0),
        1.0,
    )
    ask025 = band_vals.get("ask_025", 0.0)
    bid025 = band_vals.get("bid_025", 0.0)
    ask025_clear_s = ask025 / buy_speed if ask025 else 0.0
    tp1_clear_s = ask_before_tp1 / buy_speed if ask_before_tp1 else 0.0
    bid_ratio025 = bid025 / max(bid025 + ask025, 1e-9)

    wall_persisted = None
    wall_remaining_ratio = None
    if initial_wall_price and initial_wall_notional:
        tol = LIQUIDITY_WALL_MATCH_TOLERANCE_PCT
        current_near = sum(px*qty for px,qty in asks if abs(pct_change(px, initial_wall_price)) <= tol)
        wall_remaining_ratio = current_near / max(initial_wall_notional, 1e-9)
        wall_persisted = int(wall_remaining_ratio >= 0.40)

    barrier = "LOW"
    if ((ask_wall_dist is not None and ask_wall_dist <= 0.25 and ask_wall_ratio >= 5.0 and ask025_clear_s >= 4.0)
            or tp1_clear_s >= 8.0):
        barrier = "HIGH"
    elif ((ask_wall_dist is not None and ask_wall_dist <= 0.50 and ask_wall_ratio >= 3.0)
            or ask025_clear_s >= 3.0 or tp1_clear_s >= 5.0):
        barrier = "MEDIUM"

    absorption = bool(
        m.get("flow30",0) >= 3.0 and m.get("buy30",0) >= 0.64
        and m.get("flow_eff30",0) < 0.08
        and (barrier == "HIGH" or (wall_persisted == 1 and (wall_remaining_ratio or 0) >= 0.60))
    )
    ask_coverage = pct_change(asks[-1][0], reference_price) if asks else 0.0
    bid_coverage = -pct_change(bids[-1][0], reference_price) if bids else 0.0
    return {
        **band_vals,
        "mid_price": mid, "best_bid": best_bid, "best_ask": best_ask,
        "ask_before_tp1": ask_before_tp1, "bid_ratio_025": bid_ratio025,
        "largest_ask_wall_price": ask_wall_px, "largest_ask_wall_notional": ask_wall_n,
        "largest_ask_wall_distance_pct": ask_wall_dist, "largest_ask_wall_ratio": ask_wall_ratio,
        "largest_bid_wall_price": bid_wall_px, "largest_bid_wall_notional": bid_wall_n,
        "largest_bid_wall_distance_pct": bid_wall_dist, "largest_bid_wall_ratio": bid_wall_ratio,
        "aggressive_buy_speed_usdt_s": buy_speed, "ask025_clear_s": ask025_clear_s, "tp1_clear_s": tp1_clear_s,
        "wall_persisted": wall_persisted, "wall_remaining_ratio": wall_remaining_ratio,
        "wall_replenished": (int(wall_remaining_ratio >= 0.90) if wall_remaining_ratio is not None else None),
        "wall_cancelled": (int(wall_remaining_ratio <= 0.15) if wall_remaining_ratio is not None else None),
        "barrier_label": barrier, "absorption_flag": int(absorption),
        "depth_levels": min(len(bids), len(asks)), "ask_coverage_pct": ask_coverage, "bid_coverage_pct": bid_coverage,
    }


def _safe_ratio(num, den):
    try:
        if num is None or den is None or float(den) <= 0:
            return None
        return float(num) / float(den)
    except Exception:
        return None


def classify_liquidity_evolution(feat: dict, initial: Optional[dict]) -> dict:
    """V5.9 SHADOW classifier for *change* in local order-book conditions.

    Static wall size was not sufficiently discriminative in the first V5.8 forward sample.
    This classifier intentionally focuses on persistence/replenishment, nearby bid support,
    ask-liquidity change and estimated time-to-clear. It never gates Premium creation.
    """
    if not LIQUIDITY_EVOLUTION_ENABLED or not initial:
        return {
            "ask025_vs_initial": None, "bid025_vs_initial": None, "tp1_ask_vs_initial": None,
            "bid_ratio_delta": None, "ask025_clear_vs_initial": None, "tp1_clear_vs_initial": None,
            "dynamic_score": None, "dynamic_state": "BASELINE", "dynamic_reason": "signal snapshot",
        }

    ask_ratio = _safe_ratio(feat.get("ask_025"), initial.get("ask_025"))
    bid_ratio = _safe_ratio(feat.get("bid_025"), initial.get("bid_025"))
    tp1_ask_ratio = _safe_ratio(feat.get("ask_before_tp1"), initial.get("ask_before_tp1"))
    clear_ratio = _safe_ratio(feat.get("ask025_clear_s"), initial.get("ask025_clear_s"))
    tp1_clear_ratio = _safe_ratio(feat.get("tp1_clear_s"), initial.get("tp1_clear_s"))
    bid_delta = None
    if feat.get("bid_ratio_025") is not None and initial.get("bid_ratio_025") is not None:
        bid_delta = float(feat.get("bid_ratio_025")) - float(initial.get("bid_ratio_025"))

    score = 0
    reasons = []
    local_bid = feat.get("bid_ratio_025")
    wall_remaining = feat.get("wall_remaining_ratio")
    clear_s = feat.get("ask025_clear_s")

    # Nearby bid/ask balance: broad buckets on purpose; forward n is still small.
    if local_bid is not None:
        if local_bid >= 0.55:
            score += 25; reasons.append(f"bid25 %{local_bid*100:.0f}")
        elif local_bid >= 0.45:
            score += 15; reasons.append(f"bid25 %{local_bid*100:.0f}")
        elif local_bid <= 0.15:
            score -= 25; reasons.append(f"bid25 zayıf %{local_bid*100:.0f}")
        elif local_bid <= 0.25:
            score -= 15; reasons.append(f"bid25 zayıf %{local_bid*100:.0f}")

    # Persistence of the initial largest ask wall.
    if wall_remaining is not None:
        if wall_remaining <= 0.60:
            score += 25; reasons.append(f"wall kaldı %{wall_remaining*100:.0f}")
        elif wall_remaining <= 0.80:
            score += 15; reasons.append(f"wall eriyor %{wall_remaining*100:.0f}")
        elif wall_remaining >= 1.05:
            score -= 25; reasons.append(f"wall büyüyor %{wall_remaining*100:.0f}")
        elif wall_remaining >= 0.90:
            score -= 15; reasons.append(f"wall kalıcı %{wall_remaining*100:.0f}")

    # Aggregate ask liquidity near the signal, not just one displayed wall.
    if ask_ratio is not None:
        if ask_ratio <= 0.70:
            score += 15; reasons.append(f"ask25 %{ask_ratio*100:.0f}")
        elif ask_ratio <= 0.85:
            score += 10; reasons.append(f"ask25 azalıyor %{ask_ratio*100:.0f}")
        elif ask_ratio >= 1.15:
            score -= 15; reasons.append(f"ask25 artıyor %{ask_ratio*100:.0f}")
        elif ask_ratio >= 1.05:
            score -= 8; reasons.append(f"ask25 +%{(ask_ratio-1)*100:.0f}")

    # Whether bid support is disappearing or holding relative to the signal snapshot.
    if bid_ratio is not None:
        if bid_ratio >= 0.90:
            score += 10; reasons.append("bid25 korunuyor")
        elif bid_ratio <= 0.35:
            score -= 15; reasons.append(f"bid25 çözüldü %{bid_ratio*100:.0f}")
        elif bid_ratio <= 0.60:
            score -= 8; reasons.append(f"bid25 zayıfladı %{bid_ratio*100:.0f}")

    # Flow-adjusted time needed to clear the nearby asks.
    if clear_s is not None:
        if clear_s <= 8.0:
            score += 10; reasons.append(f"clear {clear_s:.1f}s")
        elif clear_s <= 15.0:
            score += 5; reasons.append(f"clear {clear_s:.1f}s")
        elif clear_s >= 25.0:
            score -= 15; reasons.append(f"clear yavaş {clear_s:.1f}s")
        elif clear_s >= 15.0:
            score -= 8; reasons.append(f"clear {clear_s:.1f}s")

    if feat.get("wall_cancelled"):
        score += 10; reasons.append("wall çekildi")
    if feat.get("wall_replenished"):
        score -= 10; reasons.append("wall yenileniyor")
    if feat.get("absorption_flag"):
        score -= 10; reasons.append("absorption")

    score = max(-100, min(100, int(round(score))))
    if score >= LIQ_EVOLUTION_SUPPORT_SCORE:
        state = "SUPPORTIVE"
    elif score <= LIQ_EVOLUTION_HOSTILE_SCORE:
        state = "HOSTILE"
    else:
        state = "MIXED"
    return {
        "ask025_vs_initial": ask_ratio, "bid025_vs_initial": bid_ratio,
        "tp1_ask_vs_initial": tp1_ask_ratio, "bid_ratio_delta": bid_delta,
        "ask025_clear_vs_initial": clear_ratio, "tp1_clear_vs_initial": tp1_clear_ratio,
        "dynamic_score": score, "dynamic_state": state,
        "dynamic_reason": "; ".join(reasons[:8]) or "mixed liquidity evolution",
    }


def classify_liquidity_v2(feat: dict, initial: Optional[dict]) -> dict:
    """V5.10 SHADOW classifier focused on the *local liquidity regime*.

    Forward data weakened the idea that one displayed wall (or absorption flag) is
    enough. V2 therefore gives most weight to aggregate ask clearance, bid survival,
    and flow-adjusted clear time. The original V5.9 classifier is preserved in parallel
    for honest forward comparison. This function never gates Premium creation.
    """
    if not LIQUIDITY_V2_ENABLED or not initial:
        return {"liquidity_v2_score": None, "liquidity_v2_state": "BASELINE", "liquidity_v2_reason": "signal snapshot"}
    ask_ratio = _safe_ratio(feat.get("ask_025"), initial.get("ask_025"))
    bid_retain = _safe_ratio(feat.get("bid_025"), initial.get("bid_025"))
    bid_ratio = feat.get("bid_ratio_025")
    wall_remaining = feat.get("wall_remaining_ratio")
    clear_s = feat.get("ask025_clear_s")
    score = 0
    reasons = []

    # Current nearby bid/ask balance: strongest weight after forward validation.
    if bid_ratio is not None:
        if bid_ratio >= 0.65:
            score += 30; reasons.append(f"bid25 güçlü %{bid_ratio*100:.0f}")
        elif bid_ratio >= 0.50:
            score += 20; reasons.append(f"bid25 %{bid_ratio*100:.0f}")
        elif bid_ratio <= 0.15:
            score -= 30; reasons.append(f"bid25 çöktü %{bid_ratio*100:.0f}")
        elif bid_ratio <= 0.25:
            score -= 20; reasons.append(f"bid25 zayıf %{bid_ratio*100:.0f}")

    # Bid survival relative to the signal snapshot.
    if bid_retain is not None:
        if bid_retain >= 0.90:
            score += 15; reasons.append("bid korunuyor")
        elif bid_retain <= 0.45:
            score -= 20; reasons.append(f"bid kaldı %{bid_retain*100:.0f}")
        elif bid_retain <= 0.60:
            score -= 10; reasons.append(f"bid zayıfladı %{bid_retain*100:.0f}")

    # Aggregate asks near the signal are more important than one wall identity.
    if ask_ratio is not None:
        if ask_ratio <= 0.70:
            score += 20; reasons.append(f"ask25 çözüldü %{ask_ratio*100:.0f}")
        elif ask_ratio <= 0.85:
            score += 10; reasons.append(f"ask25 azalıyor %{ask_ratio*100:.0f}")
        elif ask_ratio >= 1.25:
            score -= 25; reasons.append(f"ask25 büyüdü %{ask_ratio*100:.0f}")
        elif ask_ratio >= 1.10:
            score -= 15; reasons.append(f"ask25 arttı %{ask_ratio*100:.0f}")

    # Flow-adjusted time to clear nearby asks.
    if clear_s is not None:
        if clear_s <= 8.0:
            score += 15; reasons.append(f"clear {clear_s:.1f}s")
        elif clear_s <= 15.0:
            score += 8; reasons.append(f"clear {clear_s:.1f}s")
        elif clear_s >= 25.0:
            score -= 20; reasons.append(f"clear yavaş {clear_s:.1f}s")
        elif clear_s >= 15.0:
            score -= 10; reasons.append(f"clear {clear_s:.1f}s")

    # Initial wall persistence stays context, but receives less weight than in V1.
    if wall_remaining is not None:
        if wall_remaining <= 0.25:
            score += 10; reasons.append(f"ilk wall %{wall_remaining*100:.0f}")
        elif wall_remaining >= 1.05:
            score -= 10; reasons.append(f"ilk wall büyüyor %{wall_remaining*100:.0f}")
    if feat.get("wall_cancelled"):
        score += 5; reasons.append("wall çekildi")
    if feat.get("wall_replenished"):
        score -= 5; reasons.append("wall yenileniyor")
    if feat.get("absorption_flag"):
        # Forward winners also showed absorption; record it but do not punish it by itself.
        reasons.append("absorption gözlendi")

    score = max(-100, min(100, int(round(score))))
    state = "SUPPORTIVE" if score >= LIQ_V2_SUPPORT_SCORE else "HOSTILE" if score <= LIQ_V2_HOSTILE_SCORE else "MIXED"
    return {"liquidity_v2_score": score, "liquidity_v2_state": state, "liquidity_v2_reason": "; ".join(reasons[:9]) or "mixed local liquidity"}


def classify_liquidity_core(feat: dict, initial: Optional[dict]) -> dict:
    """V5.10.1 exploratory SHADOW ablation of Liquidity V2.

    It intentionally uses ONLY the four aggregate-book features that survived the latest
    chronological holdout: current bid ratio, bid survival, aggregate ask change and
    flow-adjusted ask-clear time. Thresholds/weights are copied from V2 and are NOT retuned.
    This makes the next live cohort a clean test of whether single-wall identity adds value.
    """
    if not LIQUIDITY_CORE_ENABLED or not initial:
        return {"liquidity_core_score": None, "liquidity_core_state": "BASELINE", "liquidity_core_reason": "signal snapshot"}
    ask_ratio = _safe_ratio(feat.get("ask_025"), initial.get("ask_025"))
    bid_retain = _safe_ratio(feat.get("bid_025"), initial.get("bid_025"))
    bid_ratio = feat.get("bid_ratio_025")
    clear_s = feat.get("ask025_clear_s")
    score = 0
    reasons = []

    if bid_ratio is not None:
        if bid_ratio >= 0.65:
            score += 30; reasons.append(f"bid25 güçlü %{bid_ratio*100:.0f}")
        elif bid_ratio >= 0.50:
            score += 20; reasons.append(f"bid25 %{bid_ratio*100:.0f}")
        elif bid_ratio <= 0.15:
            score -= 30; reasons.append(f"bid25 çöktü %{bid_ratio*100:.0f}")
        elif bid_ratio <= 0.25:
            score -= 20; reasons.append(f"bid25 zayıf %{bid_ratio*100:.0f}")

    if bid_retain is not None:
        if bid_retain >= 0.90:
            score += 15; reasons.append("bid korunuyor")
        elif bid_retain <= 0.45:
            score -= 20; reasons.append(f"bid kaldı %{bid_retain*100:.0f}")
        elif bid_retain <= 0.60:
            score -= 10; reasons.append(f"bid zayıfladı %{bid_retain*100:.0f}")

    if ask_ratio is not None:
        if ask_ratio <= 0.70:
            score += 20; reasons.append(f"ask25 çözüldü %{ask_ratio*100:.0f}")
        elif ask_ratio <= 0.85:
            score += 10; reasons.append(f"ask25 azalıyor %{ask_ratio*100:.0f}")
        elif ask_ratio >= 1.25:
            score -= 25; reasons.append(f"ask25 büyüdü %{ask_ratio*100:.0f}")
        elif ask_ratio >= 1.10:
            score -= 15; reasons.append(f"ask25 arttı %{ask_ratio*100:.0f}")

    if clear_s is not None:
        if clear_s <= 8.0:
            score += 15; reasons.append(f"clear {clear_s:.1f}s")
        elif clear_s <= 15.0:
            score += 8; reasons.append(f"clear {clear_s:.1f}s")
        elif clear_s >= 25.0:
            score -= 20; reasons.append(f"clear yavaş {clear_s:.1f}s")
        elif clear_s >= 15.0:
            score -= 10; reasons.append(f"clear {clear_s:.1f}s")

    score = max(-100, min(100, int(round(score))))
    state = "SUPPORTIVE" if score >= LIQ_CORE_SUPPORT_SCORE else "HOSTILE" if score <= LIQ_CORE_HOSTILE_SCORE else "MIXED"
    return {
        "liquidity_core_score": score,
        "liquidity_core_state": state,
        "liquidity_core_reason": "; ".join(reasons[:8]) or "mixed aggregate liquidity",
    }


def _terminal_context(p: PendingOutcome, horizon_ms: int) -> dict:
    """Return actionability context at a post-Premium horizon.

    Failure-risk should only be interpreted while an executable trade is still live. A
    TP1/TP2 already reached before the horizon must not later be called a failure simply
    because price retraced (the AKE-type contamination seen in the holdout).
    """
    h = max(0.0, float(horizon_ms) / 1000.0)
    entry_touched = p.entry_touch_s is not None and float(p.entry_touch_s) <= h
    stop_s = float(p.invalidation_hit_s) if p.invalidation_hit_s is not None else None
    tp1_s = float(p.tp1_hit_s) if p.tp1_hit_s is not None else None
    tp2_s = float(p.tp2_hit_s) if p.tp2_hit_s is not None else None

    stop_before = bool(entry_touched and stop_s is not None and stop_s <= h and (tp1_s is None or stop_s < tp1_s))
    tp1_before = bool(entry_touched and tp1_s is not None and tp1_s <= h and (stop_s is None or tp1_s < stop_s))
    tp2_before = bool(entry_touched and tp2_s is not None and tp2_s <= h and (stop_s is None or tp2_s < stop_s))

    terminal_event = None
    terminal_age_s = None
    if not entry_touched:
        terminal_event = "NO_ENTRY_YET"
    elif tp2_before:
        terminal_event, terminal_age_s = "TP2_REACHED", tp2_s
    elif tp1_before:
        terminal_event, terminal_age_s = "TP1_REACHED", tp1_s
    elif stop_before:
        terminal_event, terminal_age_s = "STOPPED", stop_s

    trade_active = bool(entry_touched and not tp1_before and not stop_before)
    return {
        "trade_active": int(trade_active),
        "terminal_event": terminal_event,
        "terminal_age_s": terminal_age_s,
        "entry_touched_before_horizon": int(entry_touched),
        "tp1_before_horizon": int(tp1_before),
        "tp2_before_horizon": int(tp2_before),
        "stop_before_horizon": int(stop_before),
    }


def _save_conflict_event(signal_id: int, event_code: str, horizon_ms: int, observed_ts_ms: int,
                         price: Optional[float], ret: Optional[float], details: str):
    conn = db_connect()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO premium_conflict_events
               (signal_id,event_code,horizon_ms,observed_ts_ms,price,return_pct,details,updated_ts)
               VALUES (?,?,?,?,?,?,?,?)""",
            (signal_id,event_code,horizon_ms,observed_ts_ms,price,ret,details[:500],int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()


def _risk_event_exists(signal_id: int, event: str) -> bool:
    conn = db_connect()
    try:
        return bool(conn.execute("SELECT 1 FROM shadow_exit_events WHERE signal_id=? AND event=? LIMIT 1", (signal_id,event)).fetchone())
    finally:
        conn.close()


def save_post_premium_risk(p: PendingOutcome, horizon_ms: int, observed_ts_ms: int,
                           progress_status: Optional[str] = None) -> Optional[str]:
    """Persist a SHADOW post-Premium state without contaminating resolved trades.

    V5.10.1 keeps the V5.10 liquidity thresholds frozen, but separates *observation*
    from *actionability*. A trade that already reached TP1/TP2, already stopped, or has
    not even touched the entry band is still recorded, yet it cannot emit a failure-risk
    shadow event. This prevents post-win retracements from being mislabeled as failures.
    """
    if not POST_PREMIUM_RISK_ENABLED:
        return None

    liq_horizon = 15000 if horizon_ms <= 15000 else 30000
    conn = db_connect()
    try:
        row = conn.execute(
            """SELECT dynamic_state,dynamic_score,liquidity_v2_state,liquidity_v2_score,
                      liquidity_core_state,liquidity_core_score,
                      bid_ratio_025,bid025_vs_initial,ask025_vs_initial,wall_remaining_ratio,
                      ask025_clear_s,tp1_clear_s,largest_ask_wall_distance_pct,largest_ask_wall_ratio
               FROM premium_liquidity_snapshots WHERE signal_id=? AND horizon_ms=?""",
            (p.signal_id,liq_horizon),
        ).fetchone()
        micro = conn.execute(
            """SELECT last_price,return_pct,mfe_pct,mae_pct
               FROM premium_micro_snapshots WHERE signal_id=? AND horizon_ms=?""",
            (p.signal_id,horizon_ms),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None

    (v1_state,v1_score,v2_state,v2_score,core_state,core_score,
     bid_ratio,bid_retain,ask_ratio,wall_remaining,clear_s,tp1_clear_s,wall_dist,wall_ratio) = row

    terminal = _terminal_context(p, horizon_ms)
    trade_active = bool(terminal["trade_active"])

    st = states[p.symbol]
    price = (micro[0] if micro and micro[0] is not None else (st.last_price or p.entry_price))
    ret = (micro[1] if micro and micro[1] is not None else pct_change(price, p.entry_price))
    micro_mfe = micro[2] if micro else None
    micro_mae = micro[3] if micro else None
    if ret is None:
        price_state = "UNKNOWN"
    elif ret <= -0.10:
        price_state = "NEGATIVE"
    elif ret >= 0.10:
        price_state = "POSITIVE"
    else:
        price_state = "FLAT"

    # Keep V5.10 risk ingredients frozen. New price context is measured but not used
    # to retune the classifier; the next live cohort should decide whether it adds edge.
    bid_collapse = ((bid_ratio is not None and bid_ratio <= POST_RISK_BID_RATIO_MAX) or
                    (bid_retain is not None and bid_retain <= POST_RISK_BID_RETAIN_MAX))
    ask_pressure = ((wall_remaining is not None and wall_remaining >= POST_RISK_WALL_REMAIN_MIN) or
                    (ask_ratio is not None and ask_ratio >= POST_RISK_ASK_GROWTH_MIN) or
                    (clear_s is not None and clear_s >= POST_RISK_CLEAR_S_MIN))
    support = ((bid_ratio is not None and bid_ratio >= 0.55) and
               (ask_ratio is None or ask_ratio <= 0.85) and
               (clear_s is None or clear_s <= 12.0))
    blocking_mode = ((bid_ratio is not None and bid_ratio <= 0.25) and
                     (wall_remaining is not None and wall_remaining >= 0.90) and
                     (clear_s is not None and clear_s >= 15.0))
    rotating_ask_mode = ((wall_remaining is not None and wall_remaining <= 0.25) and
                         (bid_ratio is not None and bid_ratio <= 0.15) and
                         (ask_ratio is not None and ask_ratio >= 1.05))

    risk_score = 0
    reasons = []
    if bid_collapse:
        risk_score += 2; reasons.append("bid collapse")
    if ask_pressure:
        risk_score += 2; reasons.append("ask pressure")
    if wall_remaining is not None and wall_remaining >= 1.05:
        risk_score += 1; reasons.append(f"wall %{wall_remaining*100:.0f}")
    if ask_ratio is not None and ask_ratio >= 1.15:
        risk_score += 1; reasons.append(f"ask25 %{ask_ratio*100:.0f}")
    if support:
        risk_score -= 2; reasons.append("book support")
    reasons.append(f"price={price_state}")

    if horizon_ms <= 30000:
        stage = "LIQUIDITY_15S" if horizon_ms <= 15000 else "LIQUIDITY_30S"
        if blocking_mode:
            raw_state = "BLOCKING_LIQUIDITY"
        elif rotating_ask_mode:
            raw_state = "ROTATING_ASK_PRESSURE"
        elif bid_collapse and ask_pressure:
            raw_state = "HIGH_LIQ_RISK"
        elif support:
            raw_state = "LIQ_SUPPORT"
        else:
            raw_state = "WATCH"
    else:
        stage = "FAILURE_60S"
        ps = progress_status or "UNKNOWN"
        if ps in ("REJECTION", "STALL") and blocking_mode:
            raw_state = "HIGH_FAILURE_RISK_BLOCKING"
            risk_score += 4
        elif ps in ("REJECTION", "STALL") and rotating_ask_mode:
            raw_state = "HIGH_FAILURE_RISK_ROTATING"
            risk_score += 4
        elif ps in ("REJECTION", "STALL") and bid_collapse and ask_pressure:
            raw_state = "HIGH_FAILURE_RISK"
            risk_score += 3
        elif ps == "PROGRESS" and support:
            raw_state = "STRONG_CONTINUATION"
            risk_score -= 2
        elif ps == "PROGRESS" and (bid_collapse and ask_pressure):
            raw_state = "CONFLICT_PROGRESS_VS_BOOK"
        elif ps in ("REJECTION", "STALL") and support:
            raw_state = "CONFLICT_PRICE_VS_BOOK"
        elif ps in ("REJECTION", "STALL") or bid_collapse or ask_pressure:
            raw_state = "CAUTION"
        else:
            raw_state = "NEUTRAL"
        reasons.append(f"progress={ps}")

    # Actionability guard: still store the observation, but do not call a resolved or
    # never-entered trade a failure. Raw state remains in the reason for later analysis.
    if not trade_active:
        risk_state = terminal.get("terminal_event") or "NOT_ACTIVE"
        reasons.append(f"raw_state={raw_state}")
        reasons.append("actionability=resolved")
    else:
        risk_state = raw_state
        reasons.append("actionability=live")

    conn = db_connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO premium_failure_risk
               (signal_id,horizon_ms,observed_ts_ms,stage,risk_state,raw_risk_state,risk_score,price,return_pct,progress_status,
                liquidity_v1_state,liquidity_v1_score,liquidity_v2_state,liquidity_v2_score,
                liquidity_core_state,liquidity_core_score,
                bid_ratio_025,bid025_vs_initial,ask025_vs_initial,wall_remaining_ratio,ask025_clear_s,tp1_clear_s,
                wall_distance_pct,wall_ratio,trade_active,terminal_event,terminal_age_s,
                entry_touched_before_horizon,tp1_before_horizon,tp2_before_horizon,stop_before_horizon,
                price_state,micro_mfe,micro_mae,reason,updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (p.signal_id,horizon_ms,observed_ts_ms,stage,risk_state,raw_state,risk_score,price,ret,progress_status,
             v1_state,v1_score,v2_state,v2_score,core_state,core_score,
             bid_ratio,bid_retain,ask_ratio,wall_remaining,clear_s,tp1_clear_s,wall_dist,wall_ratio,
             terminal["trade_active"],terminal["terminal_event"],terminal["terminal_age_s"],
             terminal["entry_touched_before_horizon"],terminal["tp1_before_horizon"],terminal["tp2_before_horizon"],terminal["stop_before_horizon"],
             price_state,micro_mfe,micro_mae,"; ".join(reasons)[:500],int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()

    # Conflict cohorts are actionable only while the trade is live. Resolved observations
    # remain available in the raw liquidity tables without polluting failure statistics.
    if trade_active:
        if wall_remaining is not None and wall_remaining <= 0.25 and (
                (bid_ratio is not None and bid_ratio <= 0.15) or (bid_retain is not None and bid_retain <= 0.25)) and (
                ask_ratio is not None and ask_ratio >= 1.05):
            _save_conflict_event(p.signal_id,"WALL_CLEARED_BID_COLLAPSE",horizon_ms,observed_ts_ms,price,ret,
                                 f"wall={wall_remaining:.3f}; bid_ratio={bid_ratio}; bid_retain={bid_retain}; ask_ratio={ask_ratio}")
        if horizon_ms >= 60000:
            if progress_status == "PROGRESS" and v2_state == "HOSTILE":
                _save_conflict_event(p.signal_id,"PROGRESS_HOSTILE_V2",horizon_ms,observed_ts_ms,price,ret,
                                     f"v2={v2_score}; core={core_state}; risk={risk_state}")
            if progress_status in ("REJECTION","STALL") and v2_state == "SUPPORTIVE":
                _save_conflict_event(p.signal_id,"WEAK_PRICE_SUPPORTIVE_BOOK_V2",horizon_ms,observed_ts_ms,price,ret,
                                     f"progress={progress_status}; v2={v2_score}; core={core_state}; risk={risk_state}")
            if wall_remaining is not None and wall_remaining >= 0.90 and progress_status == "PROGRESS":
                _save_conflict_event(p.signal_id,"WALL_PERSISTENT_STRONG_PROGRESS",horizon_ms,observed_ts_ms,price,ret,
                                     f"wall={wall_remaining:.3f}; v2={v2_state}; core={core_state}; risk={risk_state}")

    # Only live/executable trades can create actionability shadow events. No Telegram exit.
    if trade_active and horizon_ms <= 30000 and risk_state in ("BLOCKING_LIQUIDITY","ROTATING_ASK_PRESSURE","HIGH_LIQ_RISK"):
        event_name = "LIQ_RISK_15" if horizon_ms <= 15000 else "LIQ_RISK_30"
        if not _risk_event_exists(p.signal_id,event_name):
            m = compute_metrics(p.symbol)
            sc = score_metrics(m) if m else None
            save_shadow_event(p,event_name,max(0.0,(observed_ts_ms-(p.signal_generated_ts_ms or int(p.created_ts*1000)))/1000.0),
                              price,ret,max(0.0,-pct_change(price,p.peak_price or price)),m,sc,
                              f"V5.11 shadow; state={risk_state}; {'; '.join(reasons)}",None)
            if horizon_ms <= 15000:
                p.liq_risk_15_saved = True
            else:
                p.liq_risk_30_saved = True
    if trade_active and horizon_ms >= 60000 and risk_state in ("HIGH_FAILURE_RISK","HIGH_FAILURE_RISK_BLOCKING","HIGH_FAILURE_RISK_ROTATING") and not _risk_event_exists(p.signal_id,"FAIL_RISK_60"):
        m = compute_metrics(p.symbol)
        sc = score_metrics(m) if m else None
        save_shadow_event(p,"FAIL_RISK_60",max(0.0,(observed_ts_ms-(p.signal_generated_ts_ms or int(p.created_ts*1000)))/1000.0),
                          price,ret,max(0.0,-pct_change(price,p.peak_price or price)),m,sc,
                          f"V5.11 shadow; state={risk_state}; {'; '.join(reasons)}",None)
        p.fail_risk_60_saved = True
    if horizon_ms in (15000, 30000):
        maybe_finalize_execution_gate_v21(p.signal_id)
    return risk_state


def maybe_finalize_execution_gate_v21(signal_id: int):
    """V5.13.1 actionable 15/30s cohort logger. SHADOW ONLY.

    Uses only information available by each horizon. It deliberately does not
    suppress Premiums or place/delay AutoTrade orders; that requires another
    forward cohort with delayed-entry P/L.
    """
    if not EXECUTION_GATE_V21_SHADOW_ENABLED:
        return
    conn = db_connect()
    try:
        ev = conn.execute("SELECT status FROM premium_entry_validation WHERE signal_id=?", (signal_id,)).fetchone()
        entry_status = ev[0] if ev else None
        sig = conn.execute("SELECT chg60 FROM signals_v2 WHERE id=?", (signal_id,)).fetchone()
        over60 = int(bool(sig and sig[0] is not None and float(sig[0]) >= OVEREXTENDED_60_PCT))
        rows = conn.execute(
            """SELECT horizon_ms,price_state,liquidity_core_state,trade_active,terminal_event
               FROM premium_failure_risk WHERE signal_id=? AND horizon_ms IN (15000,30000)""", (signal_id,)
        ).fetchall()
        byh={int(r[0]):r for r in rows}
        def classify(h):
            r=byh.get(h)
            if not r: return None, None, None
            _,price_state,liq_state,active,terminal=r
            if not int(active or 0):
                return f"RESOLVED_BEFORE_{h//1000}", f"terminal={terminal or 'not_active'}", int(active or 0)
            if entry_status == "PASS" and price_state == "POSITIVE" and liq_state == "SUPPORTIVE":
                return ("EARLY_ALLOW_15" if h==15000 else "ALLOW_30"), f"entry=PASS; price=POSITIVE; liq=SUPPORTIVE; over60={over60}", 1
            if h==30000 and price_state == "NEGATIVE" and liq_state == "HOSTILE":
                return "BLOCK_30", f"price=NEGATIVE; liq=HOSTILE; entry={entry_status}; over60={over60}", 1
            if h==15000 and price_state == "NEGATIVE" and liq_state == "HOSTILE":
                return "WATCH_NEG_HOSTILE_15", f"price=NEGATIVE; liq=HOSTILE; entry={entry_status}; over60={over60}", 1
            return ("HOLD_15" if h==15000 else "HOLD_30"), f"entry={entry_status}; price={price_state}; liq={liq_state}; over60={over60}", 1
        d15,r15,a15=classify(15000); d30,r30,a30=classify(30000)
        final=None; ah=None
        if d15 == "EARLY_ALLOW_15": final,ah=d15,15000
        elif d30 in ("ALLOW_30","BLOCK_30"): final,ah=d30,30000
        elif d30: final,ah=d30,30000
        elif d15: final,ah=d15,15000
        conn.execute(
            """INSERT INTO premium_execution_gate_v21_shadow
               (signal_id,decision_15,reason_15,trade_active_15,decision_30,reason_30,trade_active_30,
                final_decision,actionable_horizon_ms,overextended_60,production_gate,updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,0,?)
               ON CONFLICT(signal_id) DO UPDATE SET
                 decision_15=excluded.decision_15,reason_15=excluded.reason_15,trade_active_15=excluded.trade_active_15,
                 decision_30=excluded.decision_30,reason_30=excluded.reason_30,trade_active_30=excluded.trade_active_30,
                 final_decision=excluded.final_decision,actionable_horizon_ms=excluded.actionable_horizon_ms,
                 overextended_60=excluded.overextended_60,production_gate=0,updated_ts=excluded.updated_ts""",
            (signal_id,d15,r15,a15,d30,r30,a30,final,ah,over60,int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()
    _arm_gate_v21_forward_shadow(signal_id,final,ah)


def maybe_finalize_execution_composite(signal_id: int, finalized_ts_ms: Optional[int] = None,
                                       p: Optional[PendingOutcome] = None):
    """Combine 30s liquidity with 60s progress, while separately storing actionability.

    The raw composite remains comparable with V5.9/V5.10. V5.10.1 adds whether the
    trade was still live at 60s so post-TP retracements are not mixed into exit research.
    """
    conn = db_connect()
    try:
        prog = conn.execute(
            "SELECT status FROM premium_progress_validation WHERE signal_id=?", (signal_id,)
        ).fetchone()
        if not prog:
            return
        liq = {}
        for h, state, score in conn.execute(
            "SELECT horizon_ms,dynamic_state,dynamic_score FROM premium_liquidity_snapshots WHERE signal_id=? AND horizon_ms IN (5000,15000,30000)",
            (signal_id,),
        ).fetchall():
            liq[int(h)] = (state, score)
        if 30000 not in liq:
            return
        progress = prog[0] or "UNKNOWN"
        liq30 = liq.get(30000, ("UNKNOWN", None))[0] or "UNKNOWN"
        if progress == "PROGRESS" and liq30 == "SUPPORTIVE":
            composite = "STRONG_CONTINUATION"
        elif progress == "REJECTION" and liq30 == "HOSTILE":
            composite = "FAIL_RISK"
        elif progress == "PROGRESS" and liq30 == "HOSTILE":
            composite = "CONFLICT_PROGRESS_VS_BOOK"
        elif progress == "REJECTION" and liq30 == "SUPPORTIVE":
            composite = "CONFLICT_PRICE_VS_BOOK"
        elif progress in ("STALL", "MIXED") or liq30 == "HOSTILE":
            composite = "CAUTION"
        else:
            composite = "NEUTRAL"

        if p is not None:
            term = _terminal_context(p, 60000)
        else:
            path = conn.execute(
                "SELECT entry_touch_s,tp1_hit_s,tp2_hit_s,invalidation_hit_s FROM signal_paths WHERE signal_id=?",
                (signal_id,),
            ).fetchone()
            if path:
                entry_s,tp1_s,tp2_s,stop_s = path
                h = 60.0
                entry_touched = entry_s is not None and float(entry_s) <= h
                stop_before = bool(entry_touched and stop_s is not None and float(stop_s) <= h and (tp1_s is None or float(stop_s) < float(tp1_s)))
                tp1_before = bool(entry_touched and tp1_s is not None and float(tp1_s) <= h and (stop_s is None or float(tp1_s) < float(stop_s)))
                tp2_before = bool(entry_touched and tp2_s is not None and float(tp2_s) <= h and (stop_s is None or float(tp2_s) < float(stop_s)))
                if not entry_touched:
                    terminal_event, terminal_age = "NO_ENTRY_YET", None
                elif tp2_before:
                    terminal_event, terminal_age = "TP2_REACHED", float(tp2_s)
                elif tp1_before:
                    terminal_event, terminal_age = "TP1_REACHED", float(tp1_s)
                elif stop_before:
                    terminal_event, terminal_age = "STOPPED", float(stop_s)
                else:
                    terminal_event, terminal_age = None, None
                term = {"trade_active": int(entry_touched and not tp1_before and not stop_before),
                        "terminal_event": terminal_event, "terminal_age_s": terminal_age}
            else:
                term = {"trade_active": None, "terminal_event": None, "terminal_age_s": None}

        reason = f"progress={progress}; liq30={liq30}; active={term.get('trade_active')}; terminal={term.get('terminal_event') or 'none'}"
        conn.execute(
            """INSERT OR REPLACE INTO premium_execution_composite
            (signal_id,finalized_ts_ms,liq_state_5,liq_score_5,liq_state_15,liq_score_15,liq_state_30,liq_score_30,
             progress_status,composite_state,reason,trade_active,terminal_event,terminal_age_s,updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                signal_id, int(finalized_ts_ms or now_ms()),
                liq.get(5000,(None,None))[0], liq.get(5000,(None,None))[1],
                liq.get(15000,(None,None))[0], liq.get(15000,(None,None))[1],
                liq.get(30000,(None,None))[0], liq.get(30000,(None,None))[1],
                progress, composite, reason,
                term.get("trade_active"), term.get("terminal_event"), term.get("terminal_age_s"), int(time.time()),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    _maybe_arm_secondary_60(signal_id, p)
    maybe_finalize_execution_gate_v2(signal_id, finalized_ts_ms)


def maybe_finalize_execution_gate_v2(signal_id: int, finalized_ts_ms: Optional[int] = None):
    """V5.12 SHADOW execution-quality classifier; production alerts are untouched."""
    if not EXECUTION_GATE_V2_SHADOW_ENABLED:
        return
    conn = db_connect()
    try:
        comp = conn.execute(
            """SELECT progress_status,composite_state,trade_active,terminal_event,terminal_age_s
               FROM premium_execution_composite WHERE signal_id=?""", (signal_id,)
        ).fetchone()
        if not comp:
            return
        progress, composite, trade_active, terminal_event, terminal_age = comp
        sig = conn.execute("SELECT chg30,chg60 FROM signals_v2 WHERE id=?", (signal_id,)).fetchone()
        ctx = conn.execute("SELECT phase_risk,live_rr1 FROM premium_context WHERE signal_id=?", (signal_id,)).fetchone()
        gate = conn.execute("SELECT decision,sticky_early_hostile FROM premium_execution_gate_shadow WHERE signal_id=?", (signal_id,)).fetchone()
        micros = conn.execute("""SELECT horizon_ms,mfe_pct,mae_pct FROM premium_micro_snapshots
                                 WHERE signal_id=? AND horizon_ms IN (15000,30000,60000)""", (signal_id,)).fetchall()
        md = {int(h):(mfe,mae) for h,mfe,mae in micros}
        chg30 = sig[0] if sig else None
        chg60 = sig[1] if sig else None
        phase = ctx[0] if ctx else None
        live_rr1 = ctx[1] if ctx else None
        gate_v1 = gate[0] if gate else None
        sticky = int(bool(gate[1])) if gate else 0
        mfe15,mae15 = md.get(15000,(None,None)); mfe30,mae30 = md.get(30000,(None,None)); mfe60,mae60 = md.get(60000,(None,None))
        over60 = int(chg60 is not None and float(chg60) >= OVEREXTENDED_60_PCT)
        local_top = int(mfe15 is not None and mae30 is not None and float(mfe15) <= LOCAL_TOP_MFE15_MAX and float(mae30) <= LOCAL_TOP_MAE30_MIN)
        reasons=[]
        if gate_v1 == "FAST_TARGET_BEFORE_GATE":
            decision = "FAST_WIN_BEFORE_GATE"; reasons.append("target before 15s gate")
        elif gate_v1 == "STOP_BEFORE_GATE" or terminal_event == "STOPPED":
            decision = "BLOCK_STOP_FIRST"; reasons.append("stop/invalidation before V2 horizon")
        elif composite == "STRONG_CONTINUATION":
            decision = "ALLOW_STRONG_CONTINUATION"; reasons.append("PROGRESS + SUPPORTIVE")
        elif progress == "REJECTION" and composite == "FAIL_RISK":
            decision = "BLOCK_REJECTION_FAIL_RISK"; reasons.append("REJECTION + FAIL_RISK")
        elif sticky and progress == "REJECTION":
            decision = "BLOCK_HOSTILE_REJECTION_CANDIDATE"; reasons.append("sticky hostile + REJECTION")
        elif over60 and progress in ("REJECTION","STALL","MIXED"):
            decision = "HOLD_OVEREXTENDED_60"; reasons.append(f"chg60>={OVEREXTENDED_60_PCT:g}% + {progress}")
        elif progress == "PROGRESS" and composite in ("NEUTRAL","CONFLICT_PROGRESS_VS_BOOK"):
            decision = "ALLOW_PROGRESS_CANDIDATE"; reasons.append(f"PROGRESS + {composite}")
        else:
            decision = "HOLD_RESEARCH"; reasons.append(f"progress={progress}; composite={composite}")
        if local_top: reasons.append("local_top_proxy")
        if over60: reasons.append("OVEREXTENDED_60")
        if sticky: reasons.append("sticky_hostile")
        reasons.append("PRODUCTION_GATE=OFF")
        conn.execute("""INSERT OR REPLACE INTO premium_execution_gate_v2_shadow
            (signal_id,finalized_ts_ms,decision,reason,progress_status,composite_state,gate_v1_decision,sticky_early_hostile,
             chg30_signal,chg60_signal,overextended_60,phase_risk,live_rr1,mfe15,mae15,mfe30,mae30,mfe60,mae60,local_top_proxy,
             trade_active_60,terminal_event_60,terminal_age_s,production_gate,updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (signal_id,int(finalized_ts_ms or now_ms()),decision,"; ".join(reasons)[:600],progress,composite,gate_v1,sticky,
             chg30,chg60,over60,phase,live_rr1,mfe15,mae15,mfe30,mae30,mfe60,mae60,local_top,trade_active,terminal_event,terminal_age,0,int(time.time())))
        conn.commit()
    finally:
        conn.close()


def _gate_execution_status(p: PendingOutcome, gate_price: Optional[float]) -> Tuple[str, Optional[float], Optional[float], Optional[float]]:
    if not gate_price or gate_price <= 0:
        return "UNKNOWN", None, None, None
    tp1_remaining = pct_change(p.target1, gate_price) if p.target1 else None
    tp2_remaining = pct_change(p.target2, gate_price) if p.target2 else None
    stop_risk = abs(pct_change(p.invalidation, gate_price)) if p.invalidation else None
    if p.invalidation and gate_price <= p.invalidation:
        status = "INVALIDATED"
    elif tp1_remaining is not None and tp1_remaining <= 0:
        status = "TARGET_ALREADY_PASSED"
    elif tp1_remaining is not None and stop_risk and tp1_remaining / max(stop_risk, 1e-9) < EXEC_MIN_LIVE_RR1:
        status = "CHASED"
    else:
        status = "OBSERVABLE"
    return status, tp1_remaining, tp2_remaining, stop_risk


def maybe_finalize_execution_gate_shadow(p: PendingOutcome, observed_ts_ms: int) -> bool:
    """Freeze a 15s *counterfactual* gate decision without touching production alerts.

    This explicitly guards against look-ahead bias: if TP1/TP2/stop already happened before
    the gate horizon, the sample is labelled as such instead of pretending a delayed entry
    could have captured the original result.
    """
    if not EXECUTION_GATE_SHADOW_ENABLED or p.gate_shadow_finalized:
        return bool(p.gate_shadow_finalized)
    signal_ms = p.signal_generated_ts_ms or int(p.created_ts * 1000)
    age_ms = max(0, int(observed_ts_ms) - int(signal_ms))
    if age_ms < EXECUTION_GATE_HORIZON_MS:
        return False

    conn = db_connect()
    try:
        liq = conn.execute(
            """SELECT dynamic_state,dynamic_score,liquidity_v2_state,liquidity_v2_score,
                      liquidity_core_state,liquidity_core_score
               FROM premium_liquidity_snapshots WHERE signal_id=? AND horizon_ms=?""",
            (p.signal_id, EXECUTION_GATE_HORIZON_MS),
        ).fetchone()
        micro = conn.execute(
            """SELECT last_price,return_pct,mfe_pct,mae_pct
               FROM premium_micro_snapshots WHERE signal_id=? AND horizon_ms=?""",
            (p.signal_id, EXECUTION_GATE_HORIZON_MS),
        ).fetchone()
        sig = conn.execute(
            """SELECT flow30,book_imbalance FROM signals_v2 WHERE id=?""", (p.signal_id,)
        ).fetchone()
    finally:
        conn.close()

    if (not liq or not micro) and age_ms < EXECUTION_GATE_DATA_TIMEOUT_MS:
        return False

    term = _terminal_context(p, EXECUTION_GATE_HORIZON_MS)
    v1_state = liq[0] if liq else "MISSING"
    v1_score = liq[1] if liq else None
    v2_state = liq[2] if liq else "MISSING"
    v2_score = liq[3] if liq else None
    core_state = liq[4] if liq else "MISSING"
    core_score = liq[5] if liq else None
    gate_price = float(micro[0]) if micro and micro[0] else float(states[p.symbol].last_price or p.entry_price)
    gate_ret = micro[1] if micro else pct_change(gate_price, p.entry_price)
    gate_mfe = micro[2] if micro else p.mfe
    gate_mae = micro[3] if micro else p.mae
    states3 = (v1_state, v2_state, core_state)
    complete = all(x in ("SUPPORTIVE", "MIXED", "HOSTILE") for x in states3)
    any_hostile = any(x == "HOSTILE" for x in states3)
    p.sticky_early_hostile = bool(any_hostile)
    absorption = bool(
        ABSORPTION_RISK_SHADOW_ENABLED and sig and sig[0] is not None and sig[1] is not None
        and float(sig[0]) >= ABSORPTION_FLOW30_MIN and float(sig[1]) < ABSORPTION_BID_MAX
    )

    if term.get("tp2_before_horizon") or term.get("tp1_before_horizon"):
        decision = "FAST_TARGET_BEFORE_GATE"
    elif term.get("stop_before_horizon"):
        decision = "STOP_BEFORE_GATE"
    elif not term.get("entry_touched_before_horizon"):
        decision = "NO_ENTRY_BEFORE_GATE"
    elif not complete:
        decision = "DATA_INCOMPLETE"
    elif any_hostile:
        decision = "WOULD_REJECT_HOSTILE_15"
    else:
        decision = "WOULD_PASS_15"

    exec_status, tp1_remaining, tp2_remaining, stop_risk = _gate_execution_status(p, gate_price)
    reasons = [f"v1={v1_state}", f"v2={v2_state}", f"core={core_state}", f"ret15={gate_ret:+.3f}%"]
    if absorption:
        reasons.append("absorption_shadow")
    if term.get("terminal_event"):
        reasons.append(f"terminal={term['terminal_event']}")
    reasons.append("PRODUCTION_GATE=OFF")

    p.gate_shadow_finalized = True
    p.gate_shadow_decision = decision
    p.gate_shadow_ts = signal_ms / 1000.0 + EXECUTION_GATE_HORIZON_MS / 1000.0
    p.gate_shadow_price = gate_price
    p.gate_shadow_mfe = 0.0
    p.gate_shadow_mae = 0.0

    conn = db_connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO premium_execution_gate_shadow
               (signal_id,gate_horizon_ms,observed_ts_ms,decision,reason,
                liquidity_v1_state,liquidity_v1_score,liquidity_v2_state,liquidity_v2_score,
                liquidity_core_state,liquidity_core_score,any_hostile,sticky_early_hostile,
                recovered_30,persistent_hostile_30,absorption_risk,
                original_price,gate_price,signal_to_gate_pct,micro_mfe,micro_mae,
                trade_active,terminal_event,execution_status,tp1_remaining_pct,tp2_remaining_pct,stop_risk_pct,
                gate_tp1_hit_s,gate_tp2_hit_s,gate_stop_hit_s,gate_first_event,gate_mfe,gate_mae,completed_60m,updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (p.signal_id,EXECUTION_GATE_HORIZON_MS,observed_ts_ms,decision,"; ".join(reasons)[:500],
             v1_state,v1_score,v2_state,v2_score,core_state,core_score,int(any_hostile),int(any_hostile),
             None,None,int(absorption),p.entry_price,gate_price,pct_change(gate_price,p.entry_price),gate_mfe,gate_mae,
             term.get("trade_active"),term.get("terminal_event"),exec_status,tp1_remaining,tp2_remaining,stop_risk,
             None,None,None,None,0.0,0.0,0,int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()
    return True


def update_gate_recovery_30(signal_id: int):
    if not EXECUTION_GATE_SHADOW_ENABLED:
        return
    conn = db_connect()
    try:
        gate = conn.execute(
            "SELECT sticky_early_hostile FROM premium_execution_gate_shadow WHERE signal_id=?", (signal_id,)
        ).fetchone()
        if not gate or not gate[0]:
            return
        row = conn.execute(
            """SELECT dynamic_state,liquidity_v2_state,liquidity_core_state
               FROM premium_liquidity_snapshots WHERE signal_id=? AND horizon_ms=30000""", (signal_id,)
        ).fetchone()
        if not row:
            return
        hostile30 = any(x == "HOSTILE" for x in row)
        conn.execute(
            """UPDATE premium_execution_gate_shadow SET recovered_30=?,persistent_hostile_30=?,updated_ts=? WHERE signal_id=?""",
            (int(not hostile30), int(hostile30), int(time.time()), signal_id),
        )
        conn.commit()
    finally:
        conn.close()


def save_gate_counterfactual_snapshot(p: PendingOutcome, horizon_s: int, observed_ts_ms: int, price: float):
    if not p.gate_shadow_finalized or not p.gate_shadow_price:
        return
    ret = pct_change(price, p.gate_shadow_price)
    conn = db_connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO premium_gate_counterfactual
               (signal_id,post_gate_horizon_s,observed_ts_ms,return_pct,mfe_pct,mae_pct)
               VALUES (?,?,?,?,?,?)""",
            (p.signal_id,horizon_s,observed_ts_ms,ret,p.gate_shadow_mfe,p.gate_shadow_mae),
        )
        conn.commit()
    finally:
        conn.close()


def save_gate_shadow_path(p: PendingOutcome, completed_60m: Optional[int] = None):
    if not p.gate_shadow_finalized:
        return
    conn = db_connect()
    try:
        conn.execute(
            """UPDATE premium_execution_gate_shadow
               SET gate_tp1_hit_s=?,gate_tp2_hit_s=?,gate_stop_hit_s=?,gate_first_event=?,
                   gate_mfe=?,gate_mae=?,completed_60m=COALESCE(?,completed_60m),updated_ts=?
               WHERE signal_id=?""",
            (p.gate_shadow_tp1_hit_s,p.gate_shadow_tp2_hit_s,p.gate_shadow_stop_hit_s,p.gate_shadow_first_event,
             p.gate_shadow_mfe,p.gate_shadow_mae,completed_60m,int(time.time()),p.signal_id),
        )
        conn.commit()
    finally:
        conn.close()


def finalize_liquidity_transition_v3(signal_id: int, finalized_ts_ms: Optional[int] = None):
    """Forward-only 5→15→30s liquidity/OI transition classifier. Research only."""
    if not LIQUIDITY_TRANSITION_V3_ENABLED: return
    conn=db_connect()
    try:
        rows=conn.execute("""SELECT horizon_ms,liquidity_v2_state,liquidity_v2_score,barrier_label,ask025_vs_initial,bid025_vs_initial
                             FROM premium_liquidity_snapshots WHERE signal_id=? AND horizon_ms IN (5000,15000,30000)""",(signal_id,)).fetchall()
        d={int(r[0]):r for r in rows}
        if 30000 not in d: return
        ctx=conn.execute("SELECT oi_regime FROM premium_context WHERE signal_id=?",(signal_id,)).fetchone()
        oi=(ctx[0] if ctx else "UNKNOWN") or "UNKNOWN"
        def val(h,i,default=None): return d.get(h,[None]*6)[i] if h in d else default
        s5,s15,s30=val(5000,1,"MISSING"),val(15000,1,"MISSING"),val(30000,1,"MISSING")
        q5,q15,q30=val(5000,2),val(15000,2),val(30000,2)
        barrier=val(30000,3,"UNKNOWN"); askr=val(30000,4); bidr=val(30000,5)
        reasons=[]
        support_votes=sum(x=="SUPPORTIVE" for x in (s5,s15,s30)); hostile_votes=sum(x=="HOSTILE" for x in (s5,s15,s30))
        if oi=="POS_GT_005": reasons.append("OI+")
        if s30=="SUPPORTIVE": reasons.append("30s supportive")
        if s30=="HOSTILE": reasons.append("30s hostile")
        if askr is not None and askr<=0.85: reasons.append("ask depletion")
        if askr is not None and askr>=1.10: reasons.append("ask refill")
        if bidr is not None and bidr>=0.60: reasons.append("bid retained")
        if bidr is not None and bidr<=0.45: reasons.append("bid lost")
        if barrier=="HIGH": reasons.append("high barrier")
        if oi=="POS_GT_005" and support_votes>=2 and barrier!="HIGH" and (askr is None or askr<=1.0): state="EARLY_SUPPORT"
        elif hostile_votes>=2 or (s30=="HOSTILE" and barrier=="HIGH") or (oi!="POS_GT_005" and s30=="HOSTILE"): state="FAIL_RISK"
        else: state="MIXED_WATCH"
        conn.execute("""INSERT OR REPLACE INTO premium_liquidity_transition_v3
            (signal_id,finalized_ts_ms,liq_state_5,liq_state_15,liq_state_30,liq_score_5,liq_score_15,liq_score_30,oi_regime,barrier_30,
             ask025_vs_initial_30,bid025_vs_initial_30,transition_state,reason,updated_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (signal_id,int(finalized_ts_ms or now_ms()),s5,s15,s30,q5,q15,q30,oi,barrier,askr,bidr,state,"; ".join(reasons),int(time.time())))
        conn.commit()
    finally: conn.close()


async def capture_liquidity_snapshot(session: aiohttp.ClientSession, p: PendingOutcome, horizon_ms: int):
    if not LIQUIDITY_RESEARCH_ENABLED:
        return
    started = now_ms()
    try:
        data = await fetch_json(session, "/fapi/v1/depth", {"symbol": p.symbol, "limit": LIQUIDITY_DEPTH_LIMIT})
        observed = now_ms()
        m = compute_metrics(p.symbol)
        initial_wall_price = initial_wall_notional = None
        initial = None
        if horizon_ms:
            conn = db_connect()
            row = conn.execute(
                """SELECT largest_ask_wall_price,largest_ask_wall_notional,ask_025,bid_025,ask_before_tp1,
                          bid_ratio_025,ask025_clear_s,tp1_clear_s
                   FROM premium_liquidity_snapshots WHERE signal_id=? AND horizon_ms=0""",
                (p.signal_id,),
            ).fetchone()
            conn.close()
            if row:
                initial_wall_price, initial_wall_notional = row[0], row[1]
                initial = {
                    "ask_025": row[2], "bid_025": row[3], "ask_before_tp1": row[4],
                    "bid_ratio_025": row[5], "ask025_clear_s": row[6], "tp1_clear_s": row[7],
                }
        feat = analyze_depth_snapshot(
            p.symbol, data, p.entry_price, p.target1, m,
            initial_wall_price=initial_wall_price, initial_wall_notional=initial_wall_notional,
        )
        if not feat:
            return
        evo = classify_liquidity_evolution(feat, initial)
        evo_v2 = classify_liquidity_v2(feat, initial)
        evo_core = classify_liquidity_core(feat, initial)
        conn = db_connect()
        conn.execute(
            """INSERT OR REPLACE INTO premium_liquidity_snapshots
            (signal_id,horizon_ms,observed_ts_ms,request_delay_ms,reference_price,mid_price,best_bid,best_ask,
             bid_010,bid_025,bid_050,bid_100,ask_010,ask_025,ask_050,ask_100,ask_before_tp1,bid_ratio_025,
             largest_ask_wall_price,largest_ask_wall_notional,largest_ask_wall_distance_pct,largest_ask_wall_ratio,
             largest_bid_wall_price,largest_bid_wall_notional,largest_bid_wall_distance_pct,largest_bid_wall_ratio,
             aggressive_buy_speed_usdt_s,ask025_clear_s,tp1_clear_s,wall_persisted,wall_remaining_ratio,wall_replenished,wall_cancelled,
             barrier_label,absorption_flag,depth_levels,ask_coverage_pct,bid_coverage_pct,
             ask025_vs_initial,bid025_vs_initial,tp1_ask_vs_initial,bid_ratio_delta,ask025_clear_vs_initial,tp1_clear_vs_initial,
             dynamic_score,dynamic_state,dynamic_reason,liquidity_v2_score,liquidity_v2_state,liquidity_v2_reason,
             liquidity_core_score,liquidity_core_state,liquidity_core_reason,updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                p.signal_id,horizon_ms,observed,max(0,observed-started),p.entry_price,
                feat.get("mid_price"),feat.get("best_bid"),feat.get("best_ask"),
                feat.get("bid_010"),feat.get("bid_025"),feat.get("bid_050"),feat.get("bid_100"),
                feat.get("ask_010"),feat.get("ask_025"),feat.get("ask_050"),feat.get("ask_100"),
                feat.get("ask_before_tp1"),feat.get("bid_ratio_025"),
                feat.get("largest_ask_wall_price"),feat.get("largest_ask_wall_notional"),
                feat.get("largest_ask_wall_distance_pct"),feat.get("largest_ask_wall_ratio"),
                feat.get("largest_bid_wall_price"),feat.get("largest_bid_wall_notional"),
                feat.get("largest_bid_wall_distance_pct"),feat.get("largest_bid_wall_ratio"),
                feat.get("aggressive_buy_speed_usdt_s"),feat.get("ask025_clear_s"),feat.get("tp1_clear_s"),
                feat.get("wall_persisted"),feat.get("wall_remaining_ratio"),feat.get("wall_replenished"),feat.get("wall_cancelled"),
                feat.get("barrier_label"),feat.get("absorption_flag"),feat.get("depth_levels"),feat.get("ask_coverage_pct"),feat.get("bid_coverage_pct"),
                evo.get("ask025_vs_initial"),evo.get("bid025_vs_initial"),evo.get("tp1_ask_vs_initial"),evo.get("bid_ratio_delta"),
                evo.get("ask025_clear_vs_initial"),evo.get("tp1_clear_vs_initial"),evo.get("dynamic_score"),evo.get("dynamic_state"),evo.get("dynamic_reason"),
                evo_v2.get("liquidity_v2_score"),evo_v2.get("liquidity_v2_state"),evo_v2.get("liquidity_v2_reason"),
                evo_core.get("liquidity_core_score"),evo_core.get("liquidity_core_state"),evo_core.get("liquidity_core_reason"),
                int(time.time()),
            ),
        )
        conn.commit(); conn.close()
        if horizon_ms == 15000:
            save_post_premium_risk(p, 15000, observed, None)
            maybe_finalize_execution_gate_shadow(p, observed)
        if horizon_ms == 30000:
            save_post_premium_risk(p, 30000, observed, None)
            update_gate_recovery_30(p.signal_id)
            maybe_finalize_execution_composite(p.signal_id, observed, p)
            finalize_liquidity_transition_v3(p.signal_id, observed)
            # If the 60s progress row already exists (e.g. depth request was delayed),
            # finalize failure-risk now instead of losing the sample.
            conn_p = db_connect()
            try:
                prow = conn_p.execute("SELECT status,finalized_ts_ms FROM premium_progress_validation WHERE signal_id=?", (p.signal_id,)).fetchone()
            finally:
                conn_p.close()
            if prow:
                save_post_premium_risk(p, 60000, int(prow[1] or observed), prow[0])
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.debug("Liquidity snapshot failed %s id=%s h=%sms: %r", p.symbol, p.signal_id, horizon_ms, e)


def finalize_progress_validation(p: PendingOutcome, observed_ts_ms: int):
    if p.progress_finalized:
        return
    conn = db_connect()
    try:
        rows = conn.execute(
            """SELECT horizon_ms,return_pct,mfe_pct,mae_pct,rel30,flow30,buy30
               FROM premium_micro_snapshots WHERE signal_id=? AND horizon_ms IN (30000,60000)""",
            (p.signal_id,),
        ).fetchall()
        d = {int(r[0]): r for r in rows}
        r30, r60 = d.get(30000), d.get(60000)
        if not r60:
            return
        ret30 = r30[1] if r30 else None
        ret60, mfe60, mae60, rel60, flow60, buy60 = r60[1],r60[2],r60[3],r60[4],r60[5],r60[6]
        reasons = []
        if mae60 is not None and mae60 <= -0.50:
            status = "REJECTION"
            reasons.append(f"MAE60 {mae60:.2f}%")
        elif ret60 is not None and ret60 <= -0.35:
            status = "REJECTION"
            reasons.append(f"ret60 {ret60:.2f}%")
        elif mfe60 is not None and mfe60 >= 0.50 and ((ret30 or 0) >= 0.10 or (ret60 or 0) >= 0.15):
            status = "PROGRESS"
            reasons.append(f"MFE60 {mfe60:.2f}%")
            if rel60 is not None and rel60 >= 0:
                reasons.append(f"rel60 {rel60:+.2f}%")
        elif mfe60 is not None and mfe60 < 0.50 and abs(ret60 or 0) < 0.15:
            status = "STALL"
            reasons.append(f"MFE60 {mfe60:.2f}%")
        else:
            status = "MIXED"
            reasons.append("mixed 30/60s path")
        conn.execute(
            """INSERT OR REPLACE INTO premium_progress_validation
            (signal_id,finalized_ts_ms,ret30,ret60,mfe60,mae60,rel30_60,flow30_60,buy30_60,status,reason,updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (p.signal_id,observed_ts_ms,ret30,ret60,mfe60,mae60,rel60,flow60,buy60,status,"; ".join(reasons),int(time.time())),
        )
        conn.commit()
        p.progress_finalized = True
    finally:
        conn.close()
    if p.progress_finalized:
        maybe_finalize_execution_composite(p.signal_id, observed_ts_ms, p)
        save_post_premium_risk(p, 60000, observed_ts_ms, status)


def _update_discovery_episode_audit(conn: sqlite3.Connection, source_event_id: int, event_type: str,
                                    event_ts: int, symbol: str, mfe: float, mae: float,
                                    early_count: int, premium_count: int, classification: str,
                                    gate_failures: Optional[str]):
    """Deduplicate dense research events into coarse first-wave episodes.

    Event-level missed-runner counts are useful for enrichment, but they can count the same
    coin move many times. This episode layer is deliberately simple and frozen: same symbol,
    gap <= DISCOVERY_EPISODE_GAP_S, total episode span <= DISCOVERY_EPISODE_MAX_S.
    It is research-only and never gates alerts.
    """
    event_ts = int(event_ts)
    latest = conn.execute(
        """SELECT id,start_ts,last_event_ts,event_count,runner_event_count,missed_event_count,
                  premium_captured,early_captured,max_runner_size,max_mfe_pct,worst_mae_pct,
                  first_premium_ts,first_early_ts,blocker_counts_json
           FROM discovery_episode_audit WHERE symbol=? ORDER BY last_event_ts DESC LIMIT 1""",
        (symbol,),
    ).fetchone()
    use_existing = False
    if latest:
        ep_id,start_ts,last_ts,*_ = latest
        use_existing = (event_ts - int(last_ts) <= DISCOVERY_EPISODE_GAP_S and
                        event_ts - int(start_ts) <= DISCOVERY_EPISODE_MAX_S)
    if not use_existing:
        cur = conn.execute(
            """INSERT INTO discovery_episode_audit
               (symbol,start_ts,last_event_ts,event_count,runner_event_count,missed_event_count,
                premium_captured,early_captured,max_runner_size,max_mfe_pct,worst_mae_pct,
                first_event_id,first_event_type,first_premium_ts,first_early_ts,blocker_counts_json,updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol,event_ts,event_ts,0,0,0,0,0,0,None,None,
             source_event_id,event_type,None,None,"{}",int(time.time())),
        )
        ep_id = int(cur.lastrowid)
        latest = conn.execute(
            """SELECT id,start_ts,last_event_ts,event_count,runner_event_count,missed_event_count,
                      premium_captured,early_captured,max_runner_size,max_mfe_pct,worst_mae_pct,
                      first_premium_ts,first_early_ts,blocker_counts_json
               FROM discovery_episode_audit WHERE id=?""", (ep_id,)
        ).fetchone()

    (ep_id,start_ts,last_ts,event_count,runner_event_count,missed_event_count,
     premium_captured,early_captured,max_runner_size,max_mfe,worst_mae,
     first_premium_ts,first_early_ts,blocker_json) = latest

    if mfe >= 5.0:
        runner_size = 5
    elif mfe >= 3.0:
        runner_size = 3
    elif mfe >= MISSED_RUNNER_MIN_MFE_PCT:
        runner_size = 2
    else:
        runner_size = 0

    end_ts = event_ts + MISSED_RUNNER_HORIZON_S
    p_row = conn.execute(
        "SELECT MIN(ts) FROM signals_v2 WHERE symbol=? AND ts BETWEEN ? AND ?",
        (symbol,event_ts,end_ts),
    ).fetchone()
    e_row = conn.execute(
        """SELECT MIN(COALESCE(notify_ts,ts)) FROM radar_signals
           WHERE symbol=? AND notified=1 AND COALESCE(notify_ts,ts) BETWEEN ? AND ?""",
        (symbol,event_ts,end_ts),
    ).fetchone()
    p_ts = p_row[0] if p_row and p_row[0] is not None else None
    e_ts = e_row[0] if e_row and e_row[0] is not None else None

    try:
        blocker_counts = json.loads(blocker_json or "{}")
        if not isinstance(blocker_counts, dict):
            blocker_counts = {}
    except Exception:
        blocker_counts = {}
    for token in (gate_failures or "").split(";"):
        token = token.strip()
        if token:
            blocker_counts[token] = int(blocker_counts.get(token, 0)) + 1

    conn.execute(
        """UPDATE discovery_episode_audit SET
           last_event_ts=?, event_count=?, runner_event_count=?, missed_event_count=?,
           premium_captured=?, early_captured=?, max_runner_size=?, max_mfe_pct=?, worst_mae_pct=?,
           first_premium_ts=?, first_early_ts=?, blocker_counts_json=?, updated_ts=?
           WHERE id=?""",
        (
            max(int(last_ts),event_ts), int(event_count)+1,
            int(runner_event_count)+(1 if runner_size else 0),
            int(missed_event_count)+(1 if classification.startswith("MISSED_RUNNER_") else 0),
            int(bool(premium_captured or premium_count)), int(bool(early_captured or early_count)),
            max(int(max_runner_size or 0), runner_size),
            max(float(max_mfe) if max_mfe is not None else float("-inf"), float(mfe)) if mfe is not None else max_mfe,
            min(float(worst_mae) if worst_mae is not None else float("inf"), float(mae)) if mae is not None else worst_mae,
            min([x for x in (first_premium_ts,p_ts) if x is not None], default=None),
            min([x for x in (first_early_ts,e_ts) if x is not None], default=None),
            json.dumps(blocker_counts, ensure_ascii=False, sort_keys=True), int(time.time()), int(ep_id),
        ),
    )


def save_missed_runner_audit(source_event_id: int, ret: float, mfe: float, mae: float):
    """At 15m, classify whether a research/near-miss setup became a runner and whether user-facing layers captured it."""
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT ts,symbol,event_type,price,gate_failures FROM research_events WHERE id=?", (source_event_id,)
        ).fetchone()
        if not row:
            return
        event_ts,symbol,event_type,start_price,gate_failures = row
        if event_type not in {
            "NEAR_MISS_CANDIDATE","IGNITION_SHADOW","IGNITION_LOW_VOLUME","FAST_EARLY_SHADOW",
            "IGNITION_V2_SHADOW","IGNITION_V2_LOW_VOLUME","FAST_EARLY_V2_SHADOW",
            "CANDIDATE_REJECT_AUDIT","PRE_BREAKOUT","SECOND_WAVE"
        }:
            return
        end_ts = int(event_ts) + MISSED_RUNNER_HORIZON_S
        early_count = conn.execute(
            """SELECT COUNT(*) FROM radar_signals
               WHERE symbol=? AND notified=1 AND COALESCE(notify_ts,ts) BETWEEN ? AND ?""",
            (symbol,event_ts,end_ts),
        ).fetchone()[0]
        premium_count = conn.execute(
            "SELECT COUNT(*) FROM signals_v2 WHERE symbol=? AND ts BETWEEN ? AND ?",
            (symbol,event_ts,end_ts),
        ).fetchone()[0]
        if mfe >= 5.0: size = "5P"
        elif mfe >= 3.0: size = "3P"
        elif mfe >= MISSED_RUNNER_MIN_MFE_PCT: size = "2P"
        else: size = "NO_RUN"
        if size == "NO_RUN":
            classification = "NO_RUN"
        elif premium_count:
            classification = f"CAPTURED_RUNNER_{size}"
        elif early_count:
            classification = f"EARLY_ONLY_RUNNER_{size}"
        else:
            classification = f"MISSED_RUNNER_{size}"
        conn.execute(
            """INSERT OR REPLACE INTO missed_runner_audit
            (source_event_id,event_type,event_ts,symbol,start_price,horizon_s,return_pct,mfe_pct,mae_pct,early_count,premium_count,classification,updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (source_event_id,event_type,event_ts,symbol,start_price,MISSED_RUNNER_HORIZON_S,ret,mfe,mae,
             int(early_count or 0),int(premium_count or 0),classification,int(time.time())),
        )
        _update_discovery_episode_audit(conn, source_event_id, event_type, int(event_ts), symbol, mfe, mae,
                                        int(early_count or 0), int(premium_count or 0), classification, gate_failures)
        conn.commit()
    finally:
        conn.close()


def classify_acceptance(p: PendingOutcome) -> Tuple[str, str]:
    ratio = p.acceptance_above_s / max(p.acceptance_total_s, 1e-9)
    # Shadow-only coarse state labels. They do not gate Premium creation or alter TP/stop.
    if p.acceptance_total_s < 5.0:
        return "WARN", f"insufficient event coverage {p.acceptance_total_s:.1f}s"
    fail_reasons = []
    if p.acceptance_max_pullback_peak_pct >= 0.50:
        fail_reasons.append(f"peak pullback {p.acceptance_max_pullback_peak_pct:.2f}%")
    if p.breakout_reference_price and ratio < 0.40:
        fail_reasons.append(f"breakout üstü süre %{ratio*100:.0f}")
    if p.breakout_reference_price and p.acceptance_close_dist_pct < -0.20:
        fail_reasons.append(f"breakout altı {p.acceptance_close_dist_pct:.2f}%")
    if fail_reasons:
        return "FAIL", "; ".join(fail_reasons)
    pass_reasons = []
    if ratio >= 0.70:
        pass_reasons.append(f"breakout üstü %{ratio*100:.0f}")
    if p.acceptance_max_pullback_peak_pct < 0.50:
        pass_reasons.append(f"peak pullback {p.acceptance_max_pullback_peak_pct:.2f}%")
    if p.acceptance_new_high_count >= 1:
        pass_reasons.append(f"new-high {p.acceptance_new_high_count}")
    if (not p.breakout_reference_price or ratio >= 0.70) and p.acceptance_max_pullback_peak_pct < 0.50:
        return "PASS", "; ".join(pass_reasons)
    return "WARN", "; ".join(pass_reasons) or "mixed acceptance"


def finalize_entry_validation(p: PendingOutcome, observed_ts_ms: int):
    if p.acceptance_finalized:
        return
    p.acceptance_finalized = True
    p.acceptance_status, reason = classify_acceptance(p)
    ratio = p.acceptance_above_s / max(p.acceptance_total_s, 1e-9)
    conn = db_connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO premium_entry_validation
            (signal_id,horizon_ms,finalized_ts_ms,breakout_reference_price,time_above_ratio,min_dist_breakout_pct,
             close_dist_breakout_pct,reclaim_count,first_reclaim_ms,max_pullback_signal_pct,max_pullback_peak_pct,
             new_high_count,first_new_high_ms,status,reason,updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                p.signal_id, ENTRY_ACCEPTANCE_HORIZON_MS, observed_ts_ms, p.breakout_reference_price or None, ratio,
                None if p.acceptance_min_dist_pct == 999.0 else p.acceptance_min_dist_pct, p.acceptance_close_dist_pct,
                p.acceptance_reclaim_count, p.acceptance_first_reclaim_ms, p.acceptance_max_pullback_signal_pct,
                p.acceptance_max_pullback_peak_pct, p.acceptance_new_high_count, p.acceptance_first_new_high_ms,
                p.acceptance_status, reason[:500], int(time.time()),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    # Keep the state observable in the research table without changing production behavior.
    m = compute_metrics(p.symbol)
    if m:
        add_research_event(
            f"ENTRY_ACCEPT_{p.acceptance_status}", p.symbol, m, score_metrics(m),
            f"ratio={ratio:.3f}; min_dist={p.acceptance_min_dist_pct:.3f}; close_dist={p.acceptance_close_dist_pct:.3f}; "
            f"pb_signal={p.acceptance_max_pullback_signal_pct:.3f}; pb_peak={p.acceptance_max_pullback_peak_pct:.3f}; "
            f"new_highs={p.acceptance_new_high_count}; {reason}", origin_signal_id=p.signal_id,
        )


def link_notification_to_signal(symbol: str, kind: str, ordinal: Optional[int], signal_id: int):
    if not ordinal:
        return
    local_date = datetime.now(IST).date().isoformat()
    conn = db_connect()
    try:
        conn.execute(
            "UPDATE notification_log SET signal_id=? WHERE local_date=? AND symbol=? AND kind=? AND ordinal=?",
            (signal_id, local_date, symbol, kind, ordinal),
        )
        conn.commit()
    finally:
        conn.close()


def update_notification_delivery(symbol: str, kind: Optional[str], ordinal: Optional[int], *, signal_id: Optional[int] = None,
                                 send_start_ts_ms: Optional[int] = None, send_done_ts_ms: Optional[int] = None,
                                 telegram_message_id: Optional[int] = None, live_bid: Optional[float] = None,
                                 live_ask: Optional[float] = None, price_drift_pct: Optional[float] = None,
                                 entry_status: Optional[str] = None):
    if not kind or not ordinal:
        return
    local_date = datetime.now(IST).date().isoformat()
    conn = db_connect()
    try:
        conn.execute(
            """UPDATE notification_log SET
               signal_id=COALESCE(?,signal_id),send_start_ts_ms=COALESCE(?,send_start_ts_ms),
               send_done_ts_ms=COALESCE(?,send_done_ts_ms),telegram_message_id=COALESCE(?,telegram_message_id),
               live_bid=COALESCE(?,live_bid),live_ask=COALESCE(?,live_ask),price_drift_pct=COALESCE(?,price_drift_pct),
               entry_status=COALESCE(?,entry_status)
               WHERE local_date=? AND symbol=? AND kind=? AND ordinal=?""",
            (signal_id, send_start_ts_ms, send_done_ts_ms, telegram_message_id, live_bid, live_ask, price_drift_pct,
             entry_status, local_date, symbol, kind, ordinal),
        )
        conn.commit()
    finally:
        conn.close()


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


def _forward_translated_levels(p: PendingOutcome, fill: float) -> dict:
    """Translate original TP/stop geometry to a counterfactual delayed fill. Research only."""
    base = ((float(p.entry_low) + float(p.entry_high)) / 2.0) if p.entry_low and p.entry_high else float(p.entry_price or fill)
    base = max(base, 1e-12)
    stop_pct = max(0.05, abs((float(p.invalidation) / base - 1.0) * 100.0)) if p.invalidation else 0.65
    tp1_pct = max(0.05, (float(p.target1) / base - 1.0) * 100.0) if p.target1 else 0.65
    tp2_pct = max(tp1_pct, (float(p.target2) / base - 1.0) * 100.0) if p.target2 else max(1.20, tp1_pct)
    return {
        "stop": float(fill) * (1.0 - stop_pct / 100.0),
        "tp1": float(fill) * (1.0 + tp1_pct / 100.0),
        "tp2": float(fill) * (1.0 + tp2_pct / 100.0),
    }


def _forward_first(a: Optional[float], b: Optional[float]) -> Optional[str]:
    if a is None and b is None:
        return None
    if a is None:
        return "B"
    if b is None:
        return "A"
    return "A" if float(a) <= float(b) else "B"


def _stage_levels_from_plan(plan: dict, fill: float) -> dict:
    """Translate the contemporaneous plan geometry to the actual shadow fill. Research only."""
    base = float(plan.get("entry_mid") or ((float(plan.get("entry_low") or fill) + float(plan.get("entry_high") or fill)) / 2.0))
    base = max(base, 1e-12)
    stop_pct = max(0.05, abs((float(plan.get("invalidation") or base * 0.9935) / base - 1.0) * 100.0))
    tp1_pct = max(0.05, (float(plan.get("target1") or base * 1.0065) / base - 1.0) * 100.0)
    tp2_pct = max(tp1_pct, (float(plan.get("target2") or base * 1.0120) / base - 1.0) * 100.0)
    return {"stop": fill * (1.0 - stop_pct / 100.0), "tp1": fill * (1.0 + tp1_pct / 100.0), "tp2": fill * (1.0 + tp2_pct / 100.0)}


def _stage_policy_results(x: PendingStageEntry, close_price: Optional[float] = None) -> dict:
    """Event-order results for one entry-stage cohort. All returns are unlevered percentages."""
    entry = float(x.entry_price or 0)
    if entry <= 0:
        return {}
    stop_ret = pct_change(x.stop_price, entry) if x.stop_price else -0.65
    tp1_ret = pct_change(x.tp1_price, entry) if x.tp1_price else 0.65
    tp2_ret = pct_change(x.tp2_price, entry) if x.tp2_price else 1.20
    mark_ret = pct_change(float(close_price), entry) if close_price else None

    def unresolved():
        return ("M2M60", mark_ret) if mark_ret is not None else ("OPEN", None)

    # Legacy current policy.
    if x.tp2_hit_s is not None and (x.stop_hit_s is None or float(x.tp2_hit_s) < float(x.stop_hit_s)):
        current = ("TP2", tp2_ret)
    elif x.stop_hit_s is not None:
        current = ("STOP", stop_ret)
    else:
        current = unresolved()

    def buffered_policy(hit_s: Optional[float], buffer_pct: float):
        if x.stop_hit_s is not None and (x.tp1_hit_s is None or float(x.stop_hit_s) < float(x.tp1_hit_s)):
            return ("STOP", stop_ret)
        if x.tp1_hit_s is None:
            return unresolved()
        b = hit_s if hit_s is not None and float(hit_s) >= float(x.tp1_hit_s) else None
        t = x.tp2_hit_s if x.tp2_hit_s is not None and float(x.tp2_hit_s) >= float(x.tp1_hit_s) else None
        first = _forward_first(b, t)
        if first == "B":
            return ("TP2", tp2_ret)
        if first == "A":
            return ("BE" if buffer_pct == 0 else f"BE_MINUS_{buffer_pct:.2f}", -float(buffer_pct))
        if mark_ret is not None:
            return ("M2M60_AFTER_TP1", mark_ret)
        return ("OPEN_AFTER_TP1", None)

    be0 = buffered_policy(x.be0_hit_s, 0.0)
    be10 = buffered_policy(x.be10_hit_s, float(FORWARD_BE_BUFFER_10_PCT))
    be15 = buffered_policy(x.be15_hit_s, float(FORWARD_BE_BUFFER_15_PCT))

    def late_runner(exit_s: Optional[float], exit_price: Optional[float], label: str):
        # This policy uses BE0 protection after TP1, realizes 50% at TP2, then runs the rest.
        if x.stop_hit_s is not None and (x.tp1_hit_s is None or float(x.stop_hit_s) < float(x.tp1_hit_s)):
            return ("STOP", stop_ret)
        if x.tp1_hit_s is None:
            return unresolved()
        b_before_tp2 = x.be0_hit_s if x.be0_hit_s is not None and (x.tp2_hit_s is None or float(x.be0_hit_s) < float(x.tp2_hit_s)) else None
        if b_before_tp2 is not None:
            return ("BE_BEFORE_TP2", 0.0)
        if x.tp2_hit_s is None:
            if mark_ret is not None:
                return ("M2M60_BEFORE_TP2", mark_ret)
            return ("OPEN_BEFORE_TP2", None)
        # After TP2, remaining half has BE floor and optional delayed trailing exit.
        be_after = x.be0_hit_s if x.be0_hit_s is not None and float(x.be0_hit_s) >= float(x.tp2_hit_s) else None
        trail_after = exit_s if exit_s is not None and float(exit_s) >= float(x.tp2_hit_s) else None
        first = _forward_first(be_after, trail_after)
        if first == "B" and exit_price:
            rr = pct_change(float(exit_price), entry)
            return (f"TP2+{label}", 0.5 * tp2_ret + 0.5 * rr)
        if first == "A":
            return ("TP2+BE", 0.5 * tp2_ret)
        if mark_ret is not None:
            return ("TP2+M2M60", 0.5 * tp2_ret + 0.5 * mark_ret)
        return ("OPEN_AFTER_TP2", None)

    late25 = late_runner(x.runner25_exit_s, x.runner25_exit_price, "LATE25_TRAIL")
    late30 = late_runner(x.runner30_exit_s, x.runner30_exit_price, "LATE30_TRAIL")

    def fee_adj(v):
        return (float(v) - float(FORWARD_FEE_ROUNDTRIP_PCT)) if v is not None else None

    return {
        "current": current, "be0": be0, "be10": be10, "be15": be15, "late25": late25, "late30": late30,
        "fee_current": fee_adj(current[1]), "fee_be0": fee_adj(be0[1]), "fee_be10": fee_adj(be10[1]),
        "fee_be15": fee_adj(be15[1]), "fee_late25": fee_adj(late25[1]), "fee_late30": fee_adj(late30[1]),
    }


def _save_stage_entry(x: PendingStageEntry, close_price: Optional[float] = None, completed_60m: bool = False):
    if not STAGE_ENTRY_FORWARD_ENABLED:
        return
    res = _stage_policy_results(x, close_price if completed_60m else None)
    if not res:
        return
    conn = db_connect()
    try:
        conn.execute(
            """UPDATE entry_stage_forward_shadow SET
               signal_id=?,decision=?,entry_age_s=?,tp1_hit_s=?,tp2_hit_s=?,stop_hit_s=?,be0_hit_s=?,be10_hit_s=?,be15_hit_s=?,
               mfe_pct=?,mae_pct=?,runner25_active_s=?,runner25_exit_s=?,runner25_exit_price=?,runner25_peak=?,
               runner30_active_s=?,runner30_exit_s=?,runner30_exit_price=?,runner30_peak=?,became_premium=?,
               close60_price=COALESCE(?,close60_price),completed_60m=MAX(completed_60m,?),
               current_outcome=?,current_return_pct=?,be0_outcome=?,be0_return_pct=?,be10_outcome=?,be10_return_pct=?,be15_outcome=?,be15_return_pct=?,
               late25_outcome=?,late25_return_pct=?,late30_outcome=?,late30_return_pct=?,
               fee_adjusted_current_pct=?,fee_adjusted_be0_pct=?,fee_adjusted_be10_pct=?,fee_adjusted_be15_pct=?,fee_adjusted_late25_pct=?,fee_adjusted_late30_pct=?,updated_ts=?
               WHERE id=?""",
            (x.signal_id,x.decision,x.entry_age_s,x.tp1_hit_s,x.tp2_hit_s,x.stop_hit_s,x.be0_hit_s,x.be10_hit_s,x.be15_hit_s,
             x.mfe,x.mae,x.runner25_active_s,x.runner25_exit_s,x.runner25_exit_price,x.runner25_peak,
             x.runner30_active_s,x.runner30_exit_s,x.runner30_exit_price,x.runner30_peak,int(bool(x.signal_id)),
             float(close_price) if completed_60m and close_price else None,int(bool(completed_60m)),
             res["current"][0],res["current"][1],res["be0"][0],res["be0"][1],res["be10"][0],res["be10"][1],res["be15"][0],res["be15"][1],
             res["late25"][0],res["late25"][1],res["late30"][0],res["late30"][1],
             res["fee_current"],res["fee_be0"],res["fee_be10"],res["fee_be15"],res["fee_late25"],res["fee_late30"],int(time.time()),x.row_id)
        )
        conn.commit()
    finally:
        conn.close()


def _stage_exists(symbol: str, episode_id: int, stage: str, signal_id: Optional[int] = None) -> bool:
    conn = db_connect()
    try:
        if signal_id is not None:
            row = conn.execute("SELECT 1 FROM entry_stage_forward_shadow WHERE signal_id=? AND stage=? LIMIT 1",(int(signal_id),stage)).fetchone()
        else:
            row = conn.execute("SELECT 1 FROM entry_stage_forward_shadow WHERE symbol=? AND episode_id=? AND stage=? LIMIT 1",(symbol,int(episode_id or 0),stage)).fetchone()
        return bool(row)
    finally:
        conn.close()


def _arm_stage_entry(symbol: str, stage: str, m: dict, episode_id: int, created_ts: Optional[float] = None,
                     signal_id: Optional[int] = None, decision: str = "", entry_age_s: Optional[float] = None,
                     entry_price: Optional[float] = None, levels: Optional[dict] = None):
    """Create a true forward entry cohort at the moment the stage becomes observable."""
    if not STAGE_ENTRY_FORWARD_ENABLED:
        return None
    created = float(created_ts if created_ts is not None else time.time())
    fill = float(entry_price or m.get("price") or 0)
    if fill <= 0:
        return None
    if _stage_exists(symbol, int(episode_id or 0), stage, signal_id=signal_id if stage not in ("CANDIDATE","EARLY") else None):
        return None
    if levels is None:
        try:
            plan = estimate_trade_plan(symbol, m)
            levels = _stage_levels_from_plan(plan, fill)
        except Exception as e:
            log.debug("stage plan failed %s %s: %r",symbol,stage,e)
            return None
    conn=db_connect()
    try:
        cur=conn.execute(
            """INSERT INTO entry_stage_forward_shadow
               (symbol,episode_id,stage,signal_id,decision,created_ts_ms,entry_age_s,entry_price,stop_price,tp1_price,tp2_price,became_premium,updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol,int(episode_id or 0),stage,signal_id,decision,int(created*1000),entry_age_s,fill,levels.get("stop"),levels.get("tp1"),levels.get("tp2"),int(bool(signal_id)),int(time.time()))
        )
        rid=int(cur.lastrowid); conn.commit()
    finally:
        conn.close()
    obj=PendingStageEntry(rid,symbol,stage,int(episode_id or 0),fill,created,float(levels.get("stop") or 0),float(levels.get("tp1") or 0),float(levels.get("tp2") or 0),
                          signal_id=signal_id,decision=decision,entry_age_s=entry_age_s)
    pending_stage_entries.append(obj)
    return obj


def _arm_stage_from_pending(p: PendingOutcome, stage: str, age_s: float, price: float, decision: str):
    if not STAGE_ENTRY_FORWARD_ENABLED or not price or price <= 0:
        return None
    levels=_forward_translated_levels(p,float(price))
    episode_id=0
    conn=db_connect()
    try:
        r=conn.execute("SELECT episode_id FROM candidate_events WHERE symbol=? AND event='premium_signal' AND ts BETWEEN ? AND ? ORDER BY id DESC LIMIT 1",
                       (p.symbol,int(p.created_ts)-2,int(p.created_ts)+2)).fetchone()
        episode_id=int(r[0] or 0) if r else 0
    finally:
        conn.close()
    return _arm_stage_entry(p.symbol,stage,{"price":price},episode_id,created_ts=p.created_ts+float(age_s),signal_id=p.signal_id,
                            decision=decision,entry_age_s=float(age_s),entry_price=float(price),levels=levels)


def _link_stage_entries_to_premium(symbol: str, episode_id: int, signal_id: int):
    """Label earlier CANDIDATE/EARLY cohorts that actually matured into this Premium."""
    if not STAGE_ENTRY_FORWARD_ENABLED:
        return
    conn=db_connect()
    try:
        conn.execute("UPDATE entry_stage_forward_shadow SET signal_id=?,became_premium=1,updated_ts=? WHERE symbol=? AND episode_id=? AND stage IN ('CANDIDATE','EARLY') AND signal_id IS NULL",
                     (int(signal_id),int(time.time()),symbol,int(episode_id or 0)))
        conn.commit()
    finally:
        conn.close()
    for x in pending_stage_entries:
        if x.symbol==symbol and int(x.episode_id or 0)==int(episode_id or 0) and x.stage in ("CANDIDATE","EARLY") and x.signal_id is None:
            x.signal_id=int(signal_id)


def _update_stage_entries_tick(symbol: str, price: float, tick_ts: float):
    if not STAGE_ENTRY_FORWARD_ENABLED or not price:
        return
    for x in list(pending_stage_entries):
        if x.symbol != symbol or tick_ts < x.created_ts or x.completed_60m:
            continue
        age=max(0.0,tick_ts-x.created_ts)
        ret=pct_change(price,x.entry_price)
        x.mfe=max(x.mfe,ret); x.mae=min(x.mae,ret)
        changed=False
        if x.tp1_price and x.tp1_hit_s is None and price >= x.tp1_price:
            x.tp1_hit_s=age; changed=True
        if x.tp2_price and x.tp2_hit_s is None and price >= x.tp2_price:
            x.tp2_hit_s=age; changed=True
        if x.stop_price and x.stop_hit_s is None and price <= x.stop_price:
            x.stop_hit_s=age; changed=True
        if x.tp1_hit_s is not None and age > float(x.tp1_hit_s):
            if x.be0_hit_s is None and price <= x.entry_price:
                x.be0_hit_s=age; changed=True
            if x.be10_hit_s is None and price <= x.entry_price*(1.0-float(FORWARD_BE_BUFFER_10_PCT)/100.0):
                x.be10_hit_s=age; changed=True
            if x.be15_hit_s is None and price <= x.entry_price*(1.0-float(FORWARD_BE_BUFFER_15_PCT)/100.0):
                x.be15_hit_s=age; changed=True
        # Delayed runner candidates only become active after TP2 and the activation threshold.
        if x.tp2_hit_s is not None and age >= float(x.tp2_hit_s):
            if x.runner25_active_s is None and ret >= float(FORWARD_LATE_RUNNER_25_ACTIVATE_PCT):
                x.runner25_active_s=age; x.runner25_peak=price; changed=True
            if x.runner25_active_s is not None and x.runner25_exit_s is None:
                old_peak=float(x.runner25_peak or 0)
                if price > old_peak:
                    x.runner25_peak=price; changed=True
                dd=max(0.0,-pct_change(price,x.runner25_peak or price))
                if dd >= float(FORWARD_LATE_RUNNER_25_TRAIL_PCT):
                    x.runner25_exit_s=age; x.runner25_exit_price=price; changed=True
            if x.runner30_active_s is None and ret >= float(FORWARD_LATE_RUNNER_30_ACTIVATE_PCT):
                x.runner30_active_s=age; x.runner30_peak=price; changed=True
            if x.runner30_active_s is not None and x.runner30_exit_s is None:
                old_peak=float(x.runner30_peak or 0)
                if price > old_peak:
                    x.runner30_peak=price; changed=True
                dd=max(0.0,-pct_change(price,x.runner30_peak or price))
                if dd >= float(FORWARD_LATE_RUNNER_30_TRAIL_PCT):
                    x.runner30_exit_s=age; x.runner30_exit_price=price; changed=True
        if changed:
            _save_stage_entry(x)


def _maybe_arm_secondary_60(signal_id: int, p: Optional[PendingOutcome] = None):
    """HOLD_30 -> 60s PROGRESS+STRONG_CONTINUATION recovery cohort. SHADOW ONLY."""
    if not (STAGE_ENTRY_FORWARD_ENABLED and SECONDARY_60_FORWARD_ENABLED):
        return
    conn=db_connect()
    try:
        gate=conn.execute("SELECT decision_30 FROM premium_execution_gate_v21_shadow WHERE signal_id=?",(signal_id,)).fetchone()
        comp=conn.execute("SELECT progress_status,composite_state,terminal_event FROM premium_execution_composite WHERE signal_id=?",(signal_id,)).fetchone()
        px=conn.execute("SELECT last_price FROM premium_micro_snapshots WHERE signal_id=? AND horizon_ms=60000",(signal_id,)).fetchone()
    finally:
        conn.close()
    if not gate or gate[0] != "HOLD_30" or not comp or comp[0] != "PROGRESS" or comp[1] != "STRONG_CONTINUATION" or not px or not px[0]:
        return
    if str(comp[2] or "") in ("STOPPED","TP2_REACHED"):
        return
    if p is None:
        p=next((z for z in pending_outcomes if z.signal_id==signal_id),None)
    if not p:
        return
    _arm_stage_from_pending(p,"SECONDARY60",60.0,float(px[0]),"HOLD30_TO_PROGRESS_STRONG")


def _forward_exit_results(p: PendingOutcome, close_price: Optional[float] = None) -> dict:
    """Compute four exit policies from event order. Fees/slippage are deliberately excluded."""
    if p.entry_touch_s is None:
        return {}
    entry = float(p.path_entry_price or p.entry_price or 0)
    if entry <= 0:
        return {}
    tp1_ret = pct_change(float(p.target1), entry) if p.target1 else 0.0
    tp2_ret = pct_change(float(p.target2), entry) if p.target2 else 0.0
    stop_ret = pct_change(float(p.invalidation), entry) if p.invalidation else 0.0
    runner_price = entry * (1.0 + float(FORWARD_RUNNER_TARGET_PCT) / 100.0)
    runner_ret = pct_change(runner_price, entry)
    mark_ret = pct_change(float(close_price), entry) if close_price else None
    tp1_s, tp2_s, stop_s = p.tp1_hit_s, p.tp2_hit_s, p.invalidation_hit_s
    be_s, runner_s = p.forward_be_hit_s, p.forward_runner5_hit_s

    def unresolved():
        return ("M2M60", mark_ret) if mark_ret is not None else ("OPEN", None)

    if tp2_s is not None and (stop_s is None or float(tp2_s) < float(stop_s)):
        current = ("TP2", tp2_ret)
    elif stop_s is not None:
        current = ("STOP", stop_ret)
    else:
        current = unresolved()

    if stop_s is not None and (tp1_s is None or float(stop_s) < float(tp1_s)):
        full_be = ("STOP", stop_ret)
        partial = ("STOP", stop_ret)
        runner = ("STOP", stop_ret)
    elif tp1_s is not None:
        next_be = be_s if be_s is not None and float(be_s) >= float(tp1_s) else None
        next_tp2 = tp2_s if tp2_s is not None and float(tp2_s) >= float(tp1_s) else None
        first = _forward_first(next_be, next_tp2)
        if first == "B":
            full_be = ("TP2", tp2_ret)
            partial = ("TP1+TP2", 0.5 * tp1_ret + 0.5 * tp2_ret)
            after_be = next_be if next_be is not None and float(next_be) >= float(next_tp2) else None
            after_runner = runner_s if runner_s is not None and float(runner_s) >= float(next_tp2) else None
            rfirst = _forward_first(after_be, after_runner)
            if rfirst == "B":
                runner = ("TP2+RUNNER5", 0.5 * tp2_ret + 0.5 * runner_ret)
            elif rfirst == "A":
                runner = ("TP2+BE", 0.5 * tp2_ret)
            elif mark_ret is not None:
                runner = ("TP2+M2M60", 0.5 * tp2_ret + 0.5 * mark_ret)
            else:
                runner = ("OPEN_AFTER_TP2", None)
        elif first == "A":
            full_be = ("BE", 0.0)
            partial = ("TP1+BE", 0.5 * tp1_ret)
            runner = ("BE_BEFORE_TP2", 0.0)
        else:
            if mark_ret is not None:
                full_be = ("M2M60_AFTER_TP1", mark_ret)
                partial = ("TP1+M2M60", 0.5 * tp1_ret + 0.5 * mark_ret)
                runner = ("M2M60_BEFORE_TP2", mark_ret)
            else:
                full_be = partial = runner = ("OPEN_AFTER_TP1", None)
    else:
        full_be = unresolved()
        partial = unresolved()
        runner = unresolved()

    return {
        "entry": entry,
        "current_outcome": current[0], "current_return_pct": current[1],
        "full_be_outcome": full_be[0], "full_be_return_pct": full_be[1],
        "partial50_be_outcome": partial[0], "partial50_be_return_pct": partial[1],
        "tp2_runner50_outcome": runner[0], "tp2_runner50_return_pct": runner[1],
    }


def _save_forward_exit_shadow(p: PendingOutcome, close_price: Optional[float] = None, completed_60m: bool = False):
    if not FORWARD_STRATEGY_SHADOW_ENABLED or p.entry_touch_s is None:
        return
    res = _forward_exit_results(p, close_price if completed_60m else None)
    if not res:
        return
    conn = db_connect()
    try:
        conn.execute(
            """INSERT INTO premium_exit_forward_shadow
               (signal_id,entry_price,tp1_price,tp2_price,stop_price,be_hit_s,runner5_hit_s,runner_target_pct,
                mfe_after_tp1,mae_after_tp1,mfe_after_tp2,mae_after_tp2,close60_price,completed_60m,
                current_outcome,current_return_pct,full_be_outcome,full_be_return_pct,
                partial50_be_outcome,partial50_be_return_pct,tp2_runner50_outcome,tp2_runner50_return_pct,updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(signal_id) DO UPDATE SET
                 entry_price=excluded.entry_price,tp1_price=excluded.tp1_price,tp2_price=excluded.tp2_price,stop_price=excluded.stop_price,
                 be_hit_s=excluded.be_hit_s,runner5_hit_s=excluded.runner5_hit_s,runner_target_pct=excluded.runner_target_pct,
                 mfe_after_tp1=excluded.mfe_after_tp1,mae_after_tp1=excluded.mae_after_tp1,
                 mfe_after_tp2=excluded.mfe_after_tp2,mae_after_tp2=excluded.mae_after_tp2,
                 close60_price=COALESCE(excluded.close60_price,premium_exit_forward_shadow.close60_price),
                 completed_60m=MAX(premium_exit_forward_shadow.completed_60m,excluded.completed_60m),
                 current_outcome=excluded.current_outcome,current_return_pct=excluded.current_return_pct,
                 full_be_outcome=excluded.full_be_outcome,full_be_return_pct=excluded.full_be_return_pct,
                 partial50_be_outcome=excluded.partial50_be_outcome,partial50_be_return_pct=excluded.partial50_be_return_pct,
                 tp2_runner50_outcome=excluded.tp2_runner50_outcome,tp2_runner50_return_pct=excluded.tp2_runner50_return_pct,
                 updated_ts=excluded.updated_ts""",
            (p.signal_id,res["entry"],p.target1,p.target2,p.invalidation,p.forward_be_hit_s,p.forward_runner5_hit_s,
             FORWARD_RUNNER_TARGET_PCT,p.forward_mfe_after_tp1,p.forward_mae_after_tp1,p.forward_mfe_after_tp2,p.forward_mae_after_tp2,
             float(close_price) if completed_60m and close_price else None,int(bool(completed_60m)),
             res["current_outcome"],res["current_return_pct"],res["full_be_outcome"],res["full_be_return_pct"],
             res["partial50_be_outcome"],res["partial50_be_return_pct"],res["tp2_runner50_outcome"],res["tp2_runner50_return_pct"],int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()


def _save_delayed_shadow(p: PendingOutcome, strategy: str, sh: dict, close_price: Optional[float] = None, completed_60m: bool = False):
    if not FORWARD_STRATEGY_SHADOW_ENABLED:
        return
    entry = float(sh.get("entry_price") or 0)
    current_outcome = be_outcome = None
    current_ret = be_ret = None
    if entry > 0:
        tp2_ret = pct_change(float(sh.get("tp2") or 0), entry) if sh.get("tp2") else 0.0
        stop_ret = pct_change(float(sh.get("stop") or 0), entry) if sh.get("stop") else 0.0
        mark_ret = pct_change(float(close_price), entry) if completed_60m and close_price else None
        tp1_s,tp2_s,stop_s,be_s = sh.get("tp1_hit_s"),sh.get("tp2_hit_s"),sh.get("stop_hit_s"),sh.get("be_hit_s")
        if tp2_s is not None and (stop_s is None or float(tp2_s) < float(stop_s)):
            current_outcome,current_ret="TP2",tp2_ret
        elif stop_s is not None:
            current_outcome,current_ret="STOP",stop_ret
        elif mark_ret is not None:
            current_outcome,current_ret="M2M60",mark_ret
        else:
            current_outcome="OPEN"
        if stop_s is not None and (tp1_s is None or float(stop_s) < float(tp1_s)):
            be_outcome,be_ret="STOP",stop_ret
        elif tp1_s is not None:
            b = be_s if be_s is not None and float(be_s) >= float(tp1_s) else None
            t = tp2_s if tp2_s is not None and float(tp2_s) >= float(tp1_s) else None
            first=_forward_first(b,t)
            if first=="B": be_outcome,be_ret="TP2",tp2_ret
            elif first=="A": be_outcome,be_ret="BE",0.0
            elif mark_ret is not None: be_outcome,be_ret="M2M60_AFTER_TP1",mark_ret
            else: be_outcome="OPEN_AFTER_TP1"
        elif mark_ret is not None:
            be_outcome,be_ret="M2M60",mark_ret
        else:
            be_outcome="OPEN"
    conn=db_connect()
    try:
        conn.execute(
            """INSERT INTO premium_delayed_entry_shadow
               (signal_id,strategy,source_status,decision,horizon_ms,pullback_s,entry_age_s,entry_price,stop_price,tp1_price,tp2_price,
                tp1_hit_s,tp2_hit_s,stop_hit_s,be_hit_s,mfe_pct,mae_pct,close60_price,completed_60m,
                current_outcome,current_return_pct,be_outcome,be_return_pct,no_entry_reason,updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(signal_id,strategy) DO UPDATE SET
                 source_status=excluded.source_status,decision=excluded.decision,horizon_ms=excluded.horizon_ms,
                 pullback_s=excluded.pullback_s,entry_age_s=excluded.entry_age_s,entry_price=excluded.entry_price,
                 stop_price=excluded.stop_price,tp1_price=excluded.tp1_price,tp2_price=excluded.tp2_price,
                 tp1_hit_s=excluded.tp1_hit_s,tp2_hit_s=excluded.tp2_hit_s,stop_hit_s=excluded.stop_hit_s,be_hit_s=excluded.be_hit_s,
                 mfe_pct=excluded.mfe_pct,mae_pct=excluded.mae_pct,
                 close60_price=COALESCE(excluded.close60_price,premium_delayed_entry_shadow.close60_price),
                 completed_60m=MAX(premium_delayed_entry_shadow.completed_60m,excluded.completed_60m),
                 current_outcome=excluded.current_outcome,current_return_pct=excluded.current_return_pct,
                 be_outcome=excluded.be_outcome,be_return_pct=excluded.be_return_pct,
                 no_entry_reason=excluded.no_entry_reason,updated_ts=excluded.updated_ts""",
            (p.signal_id,strategy,p.execution_status_at_signal,sh.get("decision"),sh.get("horizon_ms"),sh.get("pullback_s"),
             sh.get("entry_age_s"),sh.get("entry_price"),sh.get("stop"),sh.get("tp1"),sh.get("tp2"),
             sh.get("tp1_hit_s"),sh.get("tp2_hit_s"),sh.get("stop_hit_s"),sh.get("be_hit_s"),
             float(sh.get("mfe") or 0),float(sh.get("mae") or 0),float(close_price) if completed_60m and close_price else None,
             int(bool(completed_60m)),current_outcome,current_ret,be_outcome,be_ret,sh.get("no_entry_reason"),int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()


def _arm_delayed_shadow(p: PendingOutcome, strategy: str, age_s: float, price: float, decision: str,
                        horizon_ms: Optional[int] = None, pullback_s: Optional[float] = None):
    if not FORWARD_STRATEGY_SHADOW_ENABLED or not price or price <= 0:
        return
    sh=p.delayed_shadows.get(strategy) or {}
    if sh.get("entry_price"):
        return
    lv=_forward_translated_levels(p,float(price))
    sh.update({"decision":decision,"horizon_ms":horizon_ms,"pullback_s":pullback_s,"entry_age_s":float(age_s),"entry_price":float(price),
               "stop":lv["stop"],"tp1":lv["tp1"],"tp2":lv["tp2"],"tp1_hit_s":None,"tp2_hit_s":None,"stop_hit_s":None,"be_hit_s":None,
               "mfe":0.0,"mae":0.0,"no_entry_reason":None})
    p.delayed_shadows[strategy]=sh
    _save_delayed_shadow(p,strategy,sh)


def _update_forward_strategy_shadows(p: PendingOutcome, price: float, age: float, completed_60m: bool = False):
    """Forward-only event logger for exit policies, WAIT_RECLAIM and Gate V2.1 delayed entries."""
    if not FORWARD_STRATEGY_SHADOW_ENABLED:
        return
    changed_exit=False
    if p.entry_touch_s is not None:
        entry=float(p.path_entry_price or p.entry_price or 0)
        if entry > 0:
            if p.tp1_hit_s is not None and age >= float(p.tp1_hit_s):
                r=pct_change(price,entry)
                p.forward_mfe_after_tp1=max(p.forward_mfe_after_tp1,r)
                p.forward_mae_after_tp1=min(p.forward_mae_after_tp1,r)
                if p.forward_be_hit_s is None and age > float(p.tp1_hit_s) and price <= entry:
                    p.forward_be_hit_s=age; changed_exit=True
            if p.tp2_hit_s is not None and age >= float(p.tp2_hit_s):
                r=pct_change(price,entry)
                p.forward_mfe_after_tp2=max(p.forward_mfe_after_tp2,r)
                p.forward_mae_after_tp2=min(p.forward_mae_after_tp2,r)
                runner_price=entry*(1.0+float(FORWARD_RUNNER_TARGET_PCT)/100.0)
                if p.forward_runner5_hit_s is None and age >= float(p.tp2_hit_s) and price >= runner_price:
                    p.forward_runner5_hit_s=age; changed_exit=True
            if changed_exit or completed_60m:
                _save_forward_exit_shadow(p,price if completed_60m else None,completed_60m)

    if WAIT_RECLAIM_FORWARD_ENABLED and p.execution_status_at_signal == "WAIT_RECLAIM":
        sh=p.delayed_shadows.get("WAIT_RECLAIM")
        if sh is None:
            sh={"decision":"WAIT_PULLBACK_RECLAIM","horizon_ms":None,"pullback_s":None,"entry_age_s":None,"entry_price":None,
                "stop":None,"tp1":None,"tp2":None,"tp1_hit_s":None,"tp2_hit_s":None,"stop_hit_s":None,"be_hit_s":None,
                "mfe":0.0,"mae":0.0,"no_entry_reason":None}
            p.delayed_shadows["WAIT_RECLAIM"]=sh
        if not sh.get("entry_price") and not sh.get("no_entry_reason"):
            if age <= WAIT_RECLAIM_MAX_DELAY_S:
                if sh.get("pullback_s") is None and p.entry_high and price <= p.entry_high and (not p.invalidation or price > p.invalidation):
                    sh["pullback_s"]=float(age); _save_delayed_shadow(p,"WAIT_RECLAIM",sh)
                elif sh.get("pullback_s") is not None and age > float(sh["pullback_s"]) and price >= p.entry_high and (not p.invalidation or price > p.invalidation):
                    _arm_delayed_shadow(p,"WAIT_RECLAIM",age,price,"PULLBACK_THEN_RECLAIM",pullback_s=float(sh["pullback_s"]))
                    sh=p.delayed_shadows["WAIT_RECLAIM"]
            else:
                sh["no_entry_reason"]="NO_RECLAIM_WITHIN_WINDOW"; _save_delayed_shadow(p,"WAIT_RECLAIM",sh)

    for strategy,sh in list(p.delayed_shadows.items()):
        entry=float(sh.get("entry_price") or 0)
        if entry <= 0:
            if completed_60m:
                _save_delayed_shadow(p,strategy,sh,price,True)
            continue
        if age < float(sh.get("entry_age_s") or 0):
            continue
        r=pct_change(price,entry)
        sh["mfe"]=max(float(sh.get("mfe") or 0),r)
        sh["mae"]=min(float(sh.get("mae") or 0),r)
        changed=False
        if sh.get("tp1") and sh.get("tp1_hit_s") is None and price >= float(sh["tp1"]): sh["tp1_hit_s"]=age; changed=True
        if sh.get("tp2") and sh.get("tp2_hit_s") is None and price >= float(sh["tp2"]): sh["tp2_hit_s"]=age; changed=True
        if sh.get("stop") and sh.get("stop_hit_s") is None and price <= float(sh["stop"]): sh["stop_hit_s"]=age; changed=True
        if sh.get("tp1_hit_s") is not None and sh.get("be_hit_s") is None and age > float(sh["tp1_hit_s"]) and price <= entry:
            sh["be_hit_s"]=age; changed=True
        if changed or completed_60m:
            _save_delayed_shadow(p,strategy,sh,price if completed_60m else None,completed_60m)


def _arm_gate_v21_forward_shadow(signal_id: int, decision: Optional[str], horizon_ms: Optional[int]):
    if not FORWARD_STRATEGY_SHADOW_ENABLED or decision not in ("EARLY_ALLOW_15","ALLOW_30") or not horizon_ms:
        return
    p=next((x for x in pending_outcomes if x.signal_id==signal_id),None)
    if not p or (p.delayed_shadows.get("GATE_V21") or {}).get("entry_price"):
        return
    conn=db_connect()
    try:
        row=conn.execute("SELECT last_price FROM premium_micro_snapshots WHERE signal_id=? AND horizon_ms=?",(signal_id,int(horizon_ms))).fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        return
    _arm_delayed_shadow(p,"GATE_V21",float(horizon_ms)/1000.0,float(row[0]),str(decision),int(horizon_ms))
    _arm_stage_from_pending(p, "GATE15" if int(horizon_ms)==15000 else "GATE30", float(horizon_ms)/1000.0, float(row[0]), str(decision))


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
    notice_line = f"🔔 Bu coin için günün {notice_no}. kullanıcı bildirimi\n" if notice_no else ""
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
    st.episode_peak_ts = time.time()
    st.episode_low_price = st.episode_start_price
    st.episode_low_ts = time.time()
    st.episode_had_early = False
    st.episode_had_premium = False
    st.episode_anchor_avg1m = float(m.get("avg1m") or 0.0)
    st.anchor_avg1m = st.episode_anchor_avg1m
    st.anchor_ts = time.time()
    return eid


def update_episode_peak(symbol: str, price: float, tick_ts: Optional[float] = None):
    st = states[symbol]
    if not st.episode_id or not price:
        return
    ts = tick_ts or time.time()
    if price > st.episode_peak_price:
        st.episode_peak_price = price
        st.episode_peak_ts = ts
    if not st.episode_low_price or price < st.episode_low_price:
        st.episode_low_price = price
        st.episode_low_ts = ts


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
    low_ret = pct_change(st.episode_low_price, st.episode_start_price) if st.episode_start_price and st.episode_low_price else 0.0
    conn = db_connect()
    conn.execute(
        """UPDATE momentum_episodes SET end_ts=?,end_price=?,end_reason=?,had_early=?,had_premium=?,peak_price=?,peak_return_pct=?,low_price=?,low_return_pct=? WHERE id=?""",
        (int(time.time()), price, reason[:100], int(st.episode_had_early), int(st.episode_had_premium),
         st.episode_peak_price or None, peak_ret, st.episode_low_price or None, low_ret, st.episode_id),
    )
    conn.commit(); conn.close()
    if st.episode_had_early or st.episode_had_premium:
        st.prev_meaningful_episode_id = st.episode_id
        st.prev_meaningful_ts = time.time()
        st.prev_meaningful_price = price or st.episode_start_price
        st.prev_meaningful_peak_price = st.episode_peak_price
        st.prev_meaningful_low_price = price or st.episode_low_price or st.episode_start_price
    st.episode_id = 0
    st.episode_started_ts = 0.0
    st.episode_start_price = 0.0
    st.episode_peak_price = 0.0
    st.episode_peak_ts = 0.0
    st.episode_low_price = 0.0
    st.episode_low_ts = 0.0
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


def add_research_event(event_type: str, symbol: str, m: dict, score: Optional[int], note: str = "",
                       origin_signal_id: Optional[int] = None, shadow_score: Optional[int] = None,
                       shadow_label: Optional[str] = None, gate_failures: Optional[List[str]] = None) -> int:
    if not RESEARCH_ENABLED or not m or not m.get("price"):
        return 0
    vel = rank_velocity_per_min(symbol, gainers_prev_rank.get(symbol))
    price_accel10 = (m.get("chg10", 0.0) * 3.0) - m.get("chg30", 0.0)
    flow_accel10 = m.get("flow10", 0.0) - m.get("flow30", 0.0)
    conn = db_connect()
    cur = conn.execute(
        """INSERT INTO research_events
        (ts,symbol,event_type,episode_id,price,score,chg10,chg30,chg60,chg5,chg15,flow10,flow30,flow60,buy30,
         book_imbalance,rel30,spread,breakout,gainer_rank,rank_velocity,compression_ratio,dist15high_pct,
         flow_eff30,flow_eff60,anchor_flow30,oi5,note,funding_rate_pct,short_liq,long_liq,origin_signal_id,oi_prev5,oi_accel5,oi_regime,
         btc30,price_accel10,flow_accel10,shadow_score,shadow_label,gate_failures)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (int(time.time()),symbol,event_type,states[symbol].episode_id or None,m.get("price"),score,m.get("chg10"),m.get("chg30"),
         m.get("chg60"),m.get("chg5"),m.get("chg15"),m.get("flow10"),m.get("flow30"),m.get("flow60"),m.get("buy30"),
         m.get("book_imbalance"),m.get("rel30"),m.get("spread"),int(bool(m.get("breakout",False))),gainers_prev_rank.get(symbol),
         vel,m.get("compression_ratio"),m.get("dist15high_pct"),m.get("flow_eff30"),m.get("flow_eff60"),m.get("anchor_flow30"),
         m.get("oi5"),note[:500],m.get("funding_rate_pct"),m.get("short_liq"),m.get("long_liq"),origin_signal_id,
         m.get("oi_prev5"),m.get("oi_accel5"),m.get("oi_regime"),m.get("btc30"),price_accel10,flow_accel10,
         shadow_score,shadow_label,(";".join(gate_failures or []))[:500]),
    )
    event_id = int(cur.lastrowid)
    conn.commit(); conn.close()
    pending_research.append(PendingResearch(event_id, symbol, float(m["price"]), time.time()))
    return event_id


def candidate_gate_failures(m: dict, score: int) -> List[str]:
    """Explain the exact silent-candidate blockers without changing qualifies()."""
    failures = []
    if m.get("qv24", 0) < MIN_24H_QUOTE_VOLUME:
        failures.append(f"QV24<{MIN_24H_QUOTE_VOLUME:.0f}")
    if not (m.get("chg10", 0) >= MIN_CHG_10S or m.get("chg30", 0) >= MIN_CHG_30S):
        failures.append("PRICE_ACCEL")
    if not (m.get("flow10", 0) >= MIN_FLOW_X_10S or m.get("flow30", 0) >= MIN_FLOW_X_30S):
        failures.append("FLOW_ACCEL")
    if m.get("buy30", 0) < MIN_BUY_RATIO_30S:
        failures.append("BUY30")
    if m.get("spread", 99) > MAX_SPREAD_PCT:
        failures.append("SPREAD")
    if not (m.get("trades10", 0) >= 2 or m.get("trades30", 0) >= 4):
        failures.append("TRADE_COUNT")
    if score < EARLY_SCORE:
        failures.append("SCORE")
    return failures


def near_miss_candidate_pass(m: dict, score: int, failures: List[str]) -> bool:
    if not NEAR_MISS_ENABLED or not failures:
        return False
    # Do not log thousands of random weak coins: require at least a plausible ignition skeleton.
    if m.get("qv24", 0) < NEAR_MISS_MIN_QV24:
        return False
    if len(failures) > 3:
        return False
    price_ok = m.get("chg10", 0) >= 0.06 or m.get("chg30", 0) >= 0.12
    flow_ok = m.get("flow10", 0) >= 1.15 or m.get("flow30", 0) >= 1.05
    return (
        price_ok and flow_ok and m.get("buy30", 0) >= 0.54
        and m.get("spread", 99) <= 0.60 and score >= 48
        and (m.get("trades10", 0) >= 2 or m.get("trades30", 0) >= 4)
    )


def ignition_shadow_score(m: dict, base_score: Optional[int] = None) -> Tuple[int, str, List[str]]:
    """V5.9 first-wave/ignition V2 score (SHADOW only).

    Forward V5.8 data suggested that explosive runners were characterized more by
    price acceleration, BTC-relative strength and flow-to-price efficiency than by
    raw flow or very high aggressive-buy share. The earlier shadow score over-rewarded
    raw flow acceleration and buyer saturation. This V2 score is intentionally a
    research score and never changes Candidate/Early/Premium production gates.
    """
    pts = 0
    reasons = []
    c10, c30, c60 = m.get("chg10",0), m.get("chg30",0), m.get("chg60",0)
    price_accel = c10 * 3.0 - c30
    flow_accel = m.get("flow10",0) - m.get("flow30",0)
    rel = m.get("rel30",0)
    eff = m.get("flow_eff30",0)
    buy = m.get("buy30",0)
    flow30 = m.get("flow30",0)
    dist15 = m.get("dist15high_pct",0)
    raw_score = int(base_score if base_score is not None else (m.get("score",0) or 0))

    if c10 >= 0.30: pts += 20; reasons.append(f"10s {c10:+.2f}")
    elif c10 >= 0.20: pts += 15; reasons.append(f"10s {c10:+.2f}")
    elif c10 >= 0.12: pts += 8

    if c30 >= 0.50: pts += 20; reasons.append(f"30s {c30:+.2f}")
    elif c30 >= 0.30: pts += 15; reasons.append(f"30s {c30:+.2f}")
    elif c30 >= 0.20: pts += 8

    if c60 >= 0.60: pts += 15; reasons.append(f"60s {c60:+.2f}")
    elif c60 >= 0.40: pts += 10
    elif c60 >= 0.25: pts += 5

    if price_accel >= 0.50: pts += 12; reasons.append(f"priceΔ {price_accel:+.2f}")
    elif price_accel >= 0.30: pts += 8; reasons.append(f"priceΔ {price_accel:+.2f}")
    elif price_accel >= 0.15: pts += 4

    if rel >= 0.80: pts += 25; reasons.append(f"rel {rel:+.2f}")
    elif rel >= 0.40: pts += 20; reasons.append(f"rel {rel:+.2f}")
    elif rel >= 0.20: pts += 10
    elif rel >= 0.10: pts += 5

    if eff >= 0.60: pts += 25; reasons.append(f"eff {eff:.3f}")
    elif eff >= 0.40: pts += 20; reasons.append(f"eff {eff:.3f}")
    elif eff >= 0.20: pts += 10
    elif eff >= 0.10: pts += 5

    # Early efficient runners did not require buyer saturation. Reward a moderate band,
    # and penalize the very high buy-share regime that often represented late absorption.
    if 0.55 <= buy <= 0.70: pts += 10; reasons.append(f"buy %{buy*100:.0f}")
    elif 0.70 < buy <= 0.78: pts += 3
    elif buy > 0.82: pts -= 12; reasons.append(f"buy-sat %{buy*100:.0f}")

    # Raw flow is context, not the thesis. Moderate flow can be enough if price efficiency is high.
    if 0.70 <= flow30 <= 3.0: pts += 8
    elif flow30 > 6.0: pts -= 5
    if flow_accel > 3.0: pts -= 5

    # Some first waves have room before the old 15m high; do not require breakout here.
    if dist15 >= 1.0: pts += 6
    elif dist15 >= 0.50: pts += 3

    if raw_score >= 75: pts += 10
    elif raw_score >= 60: pts += 5
    if m.get("spread",99) <= 0.15: pts += 3
    if m.get("extended",False): pts -= 15

    pts = max(0, min(100, pts))
    label = "FAST_EARLY_V2" if pts >= FAST_EARLY_V2_SCORE else "IGNITION_V2" if pts >= IGNITION_V2_MIN_SCORE else "NONE"
    return pts, label, reasons

def maybe_record_v59_research(symbol: str, m: dict, score: int, now: float, candidate_active: bool):
    """Near-miss + ignition observer. It can run below the 5M production volume gate."""
    st = states[symbol]
    if IGNITION_SHADOW_ENABLED and IGNITION_V2_ENABLED:
        ish_score, ish_label, ish_reasons = ignition_shadow_score(m, score)
        if ish_label != "NONE" and now - st.last_ignition_ts >= IGNITION_COOLDOWN_SECONDS:
            st.last_ignition_ts = now
            event_type = "FAST_EARLY_V2_SHADOW" if ish_label == "FAST_EARLY_V2" else "IGNITION_V2_SHADOW"
            if m.get("qv24",0) < MIN_24H_QUOTE_VOLUME:
                event_type = "IGNITION_V2_LOW_VOLUME"
            add_research_event(
                event_type, symbol, m, score,
                f"first-wave shadow; qv24={m.get('qv24',0):.0f}; reasons={','.join(ish_reasons[:6])}",
                shadow_score=ish_score, shadow_label=ish_label,
            )

    if not candidate_active and now - st.last_near_miss_ts >= NEAR_MISS_COOLDOWN_SECONDS:
        failures = candidate_gate_failures(m, score)
        if near_miss_candidate_pass(m, score, failures):
            st.last_near_miss_ts = now
            add_research_event(
                "NEAR_MISS_CANDIDATE", symbol, m, score,
                f"silent candidate near miss; blockers={','.join(failures)}",
                shadow_score=score, shadow_label="NEAR_MISS", gate_failures=failures,
            )


def record_candidate_reject_audit(symbol: str, m: dict, score: int, reason: str):
    failures = candidate_gate_failures(m, score)
    add_research_event(
        "CANDIDATE_REJECT_AUDIT", symbol, m, score,
        f"candidate_exit={reason}; current_blockers={','.join(failures) if failures else 'none'}",
        shadow_score=score, shadow_label=reason[:40], gate_failures=failures,
    )


def _trend_build_context(symbol: str, m: dict) -> dict:
    """Slow-trend context independent from candidate/episode resets. Uses only already-streamed data."""
    st=states[symbol]
    cs=list(st.candles)
    px=float(m.get("price") or 0)
    if px <= 0 or len(cs) < 12:
        return {}
    closes=[float(c.close) for c in cs[-12:]]+[px]
    lows=[float(c.low) for c in cs[-10:]]
    def ret_n(n):
        base=closes[-(n+1)] if len(closes) > n else 0
        return pct_change(px,base) if base else 0.0
    ret3,ret5,ret10=ret_n(3),ret_n(5),ret_n(10)
    last5=closes[-6:]
    positive5=sum(1 for a,b in zip(last5,last5[1:]) if b>a)/max(1,len(last5)-1)
    higher_lows=sum(1 for a,b in zip(lows[-6:],lows[-5:]) if b>=a)/max(1,len(lows[-5:])) if len(lows)>=6 else 0.0
    peak=closes[-11]
    max_dd=0.0
    for v in closes[-10:]:
        peak=max(peak,v)
        if peak: max_dd=min(max_dd,pct_change(v,peak))
    rank=gainers_prev_rank.get(symbol)
    rv=rank_velocity_per_min(symbol,rank) if rank else 0.0
    return {"ret3":ret3,"ret5":ret5,"ret10":ret10,"positive5":positive5,"higher_lows":higher_lows,"max_dd10":max_dd,"rank":rank,"rank_velocity":rv}


def _trend_build_score(symbol: str, m: dict) -> Tuple[int,dict,List[str]]:
    c=_trend_build_context(symbol,m)
    if not c: return 0,c,["insufficient history"]
    sc=0; why=[]
    def add(cond,pts,msg):
        nonlocal sc
        if cond: sc+=pts; why.append(msg)
    add(0.20 <= c["ret3"] <= 2.8,14,f"3m {c['ret3']:+.2f}%")
    add(0.40 <= c["ret5"] <= 4.5,14,f"5m {c['ret5']:+.2f}%")
    add(0.70 <= c["ret10"] <= 7.0,14,f"10m {c['ret10']:+.2f}%")
    add(c["positive5"] >= 0.60,12,f"pozitif kapanış %{c['positive5']*100:.0f}")
    add(c["higher_lows"] >= 0.60,12,f"higher-low %{c['higher_lows']*100:.0f}")
    add(c["max_dd10"] >= -1.50,10,f"10m max PB {c['max_dd10']:.2f}%")
    add(m.get("rel30",0) >= 0.20,10,f"rel30 {m.get('rel30',0):+.2f}%")
    add(1.15 <= m.get("flow30",0) <= 8.0,8,f"flow {m.get('flow30',0):.1f}x")
    add(0.55 <= m.get("buy30",0) <= 0.82,3,f"buy %{m.get('buy30',0)*100:.0f}")
    add((c.get("rank") is not None and c["rank"] <= 100) or c.get("rank_velocity",0) >= 5,3,f"rank {c.get('rank') or '-'}")
    # Penalize obvious late-stage vertical extension; do not hard-block mature trends.
    if c["ret5"] > 5.0 or c["ret10"] > 8.0: sc-=15; why.append("uzamış trend")
    if m.get("chg30",0) < -0.20: sc-=15; why.append("30s geri çekilme")
    if m.get("spread",99) > min(MAX_SPREAD_PCT,0.30): sc-=10; why.append("spread")
    return max(0,min(100,int(round(sc)))),c,why


async def maybe_trend_build_up(session, symbol: str, m: dict, score: int, now: float):
    """Selective REAL EARLY watch. Notification is informational/research-only, never an AutoTrade trigger."""
    if not TREND_BUILDUP_ENABLED: return
    st=states[symbol]
    if now-st.trend_build_last_check < TREND_BUILDUP_CONFIRM_INTERVAL_S: return
    st.trend_build_last_check=now
    tscore,ctx,reasons=_trend_build_score(symbol,m)
    if tscore >= TREND_BUILDUP_MIN_SCORE:
        st.trend_build_passes += 1
    else:
        st.trend_build_passes = 0
        return
    if st.trend_build_passes < TREND_BUILDUP_CONFIRM_PASSES: return
    if now-st.trend_build_last_notify < TREND_BUILDUP_COOLDOWN_S: return
    st.trend_build_last_notify=now; st.trend_build_passes=0
    mm=dict(m)
    try:
        oi5,oi_prev5,oi_accel5=await get_oi_context(session,symbol)
    except Exception:
        oi5=oi_prev5=oi_accel5=None
    mm["oi5"]=oi5; mm["oi_prev5"]=oi_prev5; mm["oi_accel5"]=oi_accel5; mm["oi_regime"]=oi_regime_label(oi5)
    add_research_event("TREND_BUILDUP",symbol,mm,score,
        f"trend_score={tscore}; ret3={ctx.get('ret3',0):+.3f}; ret5={ctx.get('ret5',0):+.3f}; ret10={ctx.get('ret10',0):+.3f}; "
        f"positive5={ctx.get('positive5',0):.3f}; higher_lows={ctx.get('higher_lows',0):.3f}; max_dd10={ctx.get('max_dd10',0):+.3f}; oi={mm.get('oi_regime')}; reasons={','.join(reasons[:8])}",
        shadow_score=tscore,shadow_label="REAL_EARLY_WATCH")
    if TREND_BUILDUP_NOTIFY and TELEGRAM_ADMIN_CHAT_ID:
        oi_txt="veri yok" if oi5 is None else f"{oi5:+.2f}% ({oi_regime_label(oi5)})"
        await telegram_send(session,
            f"🧭 ERKEN TREND — YAPI OLUŞUYOR (SHADOW)\n\n{symbol} | {fmt_price(mm['price'])}\n"
            f"Trend skoru: {tscore}/100 | 3m {ctx.get('ret3',0):+.2f}% | 5m {ctx.get('ret5',0):+.2f}% | 10m {ctx.get('ret10',0):+.2f}%\n"
            f"Higher-low: %{ctx.get('higher_lows',0)*100:.0f} | 10m max pullback {ctx.get('max_dd10',0):.2f}%\n"
            f"Flow {mm.get('flow30',0):.1f}x | Rel30 {mm.get('rel30',0):+.2f}% | OI {oi_txt}\n"
            f"Gainers: #{ctx.get('rank') or '-'} | rank hızı {ctx.get('rank_velocity',0):+.1f}/dk\n\n"
            "⚠️ Bu AL sinyali değildir. Premium öncesi gerçek trend oluşumunu forward ölçen erken-watch katmanıdır.",
            chat_id=TELEGRAM_ADMIN_CHAT_ID)


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
            pre_score, pre_label, pre_reasons = ignition_shadow_score(m, score)
            add_research_event("PRE_BREAKOUT", symbol, m, score,
                               "compression + near 15m high; shadow only",
                               shadow_score=pre_score, shadow_label=pre_label)

    if now - st.last_flow_structure_ts >= FLOW_STRUCTURE_COOLDOWN_SECONDS:
        low_progress = m.get("flow30",0) >= 6.0 and abs(m.get("chg30",0)) <= 0.18
        saturated = m.get("buy30",0) >= 0.82 and m.get("flow30",0) >= 4.0 and m.get("chg30",0) < 0.30
        if low_progress or saturated:
            st.last_flow_structure_ts = now
            add_research_event("FLOW_LOW_PROGRESS", symbol, m, score,
                               f"flow={m.get('flow30',0):.2f}x; chg30={m.get('chg30',0):+.3f}; eff30={m.get('flow_eff30',0):+.4f}")

    if SECOND_WAVE_ENABLED and st.prev_meaningful_ts and now - st.prev_meaningful_ts <= SECOND_WAVE_MAX_GAP_SECONDS:
        if now - st.last_second_wave_ts >= SECOND_WAVE_COOLDOWN_SECONDS:
            prev_peak = st.prev_meaningful_peak_price
            prev_low = st.prev_meaningful_low_price or st.prev_meaningful_price
            prior_pullback = max(0.0, -pct_change(prev_low, prev_peak)) if prev_peak and prev_low else 0.0
            reclaim_from_low = pct_change(m.get("price",0), prev_low) if prev_low else 0.0
            dist_prev_peak = pct_change(m.get("price",0), prev_peak) if prev_peak else -999.0
            # V5.7 tightens the RESEARCH definition: there must have been a real pullback, followed by re-acceleration/reclaim.
            # This remains observer-only and cannot create a Premium by itself.
            reaccel = (
                prior_pullback >= 0.60
                and reclaim_from_low >= 0.35
                and dist_prev_peak >= -1.50
                and (m.get("chg30",0) >= 0.22 or m.get("chg60",0) >= 0.40)
                and m.get("flow30",0) >= 1.40
                and m.get("buy30",0) >= 0.56
                and m.get("spread",99) <= min(MAX_SPREAD_PCT,0.30)
                and not m.get("extended",False)
            )
            if reaccel:
                st.last_second_wave_ts = now
                gap = now - st.prev_meaningful_ts
                sw_score = 0
                if 0.60 <= m.get("buy30",0) <= 0.75: sw_score += 25
                elif m.get("buy30",0) >= 0.56: sw_score += 12
                if m.get("rel30",0) >= 0.40: sw_score += 25
                elif m.get("rel30",0) >= 0.15: sw_score += 15
                if m.get("flow_eff30",0) >= 0.20: sw_score += 25
                elif m.get("flow_eff30",0) >= 0.10: sw_score += 15
                if reclaim_from_low >= 0.60: sw_score += 15
                if -0.80 <= dist_prev_peak <= 0.50: sw_score += 10
                sw_score = min(100, sw_score)
                add_research_event("SECOND_WAVE", symbol, m, score,
                                   f"prev_episode={st.prev_meaningful_episode_id}; gap={gap:.0f}s; pullback={prior_pullback:.2f}%; "
                                   f"reclaim={reclaim_from_low:.2f}%; dist_prev_peak={dist_prev_peak:.2f}%",
                                   shadow_score=sw_score, shadow_label=("STRONG_REENTRY_WATCH" if sw_score >= 70 else "SECOND_WAVE"))


def update_pending_tick(symbol: str, price: float, tick_ts: float):
    """Event-level MFE/MAE and Premium path accounting from aggTrade. No signal decision is made here."""
    if not price:
        return
    _update_stage_entries_tick(symbol, price, tick_ts)
    for p in list(pending_outcomes):
        if p.symbol != symbol or tick_ts < p.created_ts:
            continue
        age = max(0.0, tick_ts - p.created_ts)
        ret = pct_change(price, p.entry_price)
        p.mfe = max(p.mfe, ret)
        p.mae = min(p.mae, ret)

        observed_ms = int(tick_ts * 1000)
        signal_ms = p.signal_generated_ts_ms or int(p.created_ts * 1000)
        age_ms = max(0, observed_ms - signal_ms)

        # Exact-ish event-level 1/3/5/10/15/20/30/60s snapshots. The first aggTrade at/after each horizon wins.
        for horizon_ms in MICRO_SNAPSHOT_HORIZONS_MS:
            if age_ms >= horizon_ms and horizon_ms not in p.micro_completed:
                save_micro_snapshot(p, horizon_ms, observed_ms, price)
                p.micro_completed.add(horizon_ms)

        # True breakout-acceptance shadow state. This is deliberately post-signal and cannot create/block Premium.
        if not p.acceptance_finalized and age_ms <= ENTRY_ACCEPTANCE_HORIZON_MS + 5000:
            if p.acceptance_last_ts:
                dt = min(1.0, max(0.0, tick_ts - p.acceptance_last_ts))
                p.acceptance_total_s += dt
                if p.acceptance_was_above:
                    p.acceptance_above_s += dt
            above = True if not p.breakout_reference_price else price >= p.breakout_reference_price
            if p.acceptance_was_above is False and above:
                p.acceptance_reclaim_count += 1
                if p.acceptance_first_reclaim_ms is None:
                    p.acceptance_first_reclaim_ms = age_ms
            p.acceptance_was_above = above
            p.acceptance_last_ts = tick_ts
            if p.breakout_reference_price:
                dist = pct_change(price, p.breakout_reference_price)
                p.acceptance_min_dist_pct = min(p.acceptance_min_dist_pct, dist)
                p.acceptance_close_dist_pct = dist
            p.acceptance_max_pullback_signal_pct = max(p.acceptance_max_pullback_signal_pct, max(0.0, -ret))
            if not p.acceptance_peak_price:
                p.acceptance_peak_price = p.entry_price
            if price > p.acceptance_peak_price:
                p.acceptance_peak_price = price
            # Count structural +0.10% high milestones rather than every tiny aggTrade uptick.
            high_steps = int(max(0.0, pct_change(p.acceptance_peak_price, p.entry_price)) / 0.10)
            if high_steps > p.acceptance_new_high_count:
                p.acceptance_new_high_count = high_steps
                if p.acceptance_first_new_high_ms is None:
                    p.acceptance_first_new_high_ms = age_ms
            p.acceptance_max_pullback_peak_pct = max(
                p.acceptance_max_pullback_peak_pct,
                max(0.0, -pct_change(price, p.acceptance_peak_price or price)),
            )
            if age_ms >= ENTRY_ACCEPTANCE_HORIZON_MS:
                finalize_entry_validation(p, observed_ms)

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

        _update_forward_strategy_shadows(p, price, age, completed_60m=False)

        # V5.11: true post-gate path, measured from the 15s counterfactual gate price.
        # This is what prevents us from mistaking pre-gate winners for evidence that delayed entry works.
        if p.gate_shadow_finalized and p.gate_shadow_price and tick_ts >= p.gate_shadow_ts:
            gate_age = max(0.0, tick_ts - p.gate_shadow_ts)
            gate_ret = pct_change(price, p.gate_shadow_price)
            p.gate_shadow_mfe = max(p.gate_shadow_mfe, gate_ret)
            p.gate_shadow_mae = min(p.gate_shadow_mae, gate_ret)
            gate_changed = False
            if p.target1 and p.gate_shadow_tp1_hit_s is None and price >= p.target1:
                p.gate_shadow_tp1_hit_s = gate_age
                p.gate_shadow_first_event = p.gate_shadow_first_event or "TP1"
                gate_changed = True
            if p.target2 and p.gate_shadow_tp2_hit_s is None and price >= p.target2:
                p.gate_shadow_tp2_hit_s = gate_age
                gate_changed = True
            if p.invalidation and p.gate_shadow_stop_hit_s is None and price <= p.invalidation:
                p.gate_shadow_stop_hit_s = gate_age
                p.gate_shadow_first_event = p.gate_shadow_first_event or "INVALIDATION"
                gate_changed = True
            for gh in GATE_COUNTERFACTUAL_HORIZONS_S:
                if gate_age >= gh and gh not in p.gate_shadow_completed:
                    save_gate_counterfactual_snapshot(p, gh, observed_ms, price)
                    p.gate_shadow_completed.add(gh)
            if gate_changed:
                save_gate_shadow_path(p)

        # SKYAI-class shadow: stop first, then a genuine reclaim while momentum remains constructive.
        if (RECLAIM_SHADOW_ENABLED and p.invalidation_hit_s is not None and not p.reclaim_event_sent
                and age <= RECLAIM_MAX_AGE_SECONDS):
            reclaim_level = max(p.entry_price, p.breakout_reference_price or 0.0)
            if reclaim_level and price >= reclaim_level:
                m_reclaim = compute_metrics(symbol)
                if m_reclaim:
                    sc_reclaim = score_metrics(m_reclaim)
                    if (sc_reclaim >= PREMIUM_MIN_MOMENTUM_SCORE and m_reclaim.get("chg30",0) >= 0.10
                            and m_reclaim.get("flow30",0) >= 1.50 and m_reclaim.get("buy30",0) >= 0.58
                            and not m_reclaim.get("extended",False)):
                        p.reclaim_event_sent = True
                        reclaim_bucket = "FAST_0_5M" if age <= 300 else "MEDIUM_5_15M" if age <= 900 else "LATE_15_30M"
                        add_research_event(
                            "RECLAIM_AFTER_STOP", symbol, m_reclaim, sc_reclaim,
                            f"age={age:.1f}s; stop_s={p.invalidation_hit_s:.1f}; reclaim_level={reclaim_level:.10g}; "
                            f"signal_ret={ret:+.3f}%; bucket={reclaim_bucket}", origin_signal_id=p.signal_id,
                            shadow_score=sc_reclaim, shadow_label=reclaim_bucket,
                        )
                        # Link reclaim to the most recent V5.10 failure-risk event, if one existed.
                        conn_r = db_connect()
                        try:
                            risk_row = conn_r.execute(
                                """SELECT id,event,age_s,price FROM shadow_exit_events
                                   WHERE signal_id=? AND event IN ('FAIL_RISK_60','LIQ_RISK_30','LIQ_RISK_15')
                                   ORDER BY age_s DESC LIMIT 1""", (p.signal_id,)
                            ).fetchone()
                            if risk_row:
                                conn_r.execute(
                                    """INSERT OR REPLACE INTO failure_risk_reclaims
                                       (signal_id,risk_event_id,risk_event,risk_age_s,risk_price,stop_s,reclaim_age_s,reclaim_after_risk_s,
                                        reclaim_price,bucket,score,updated_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                    (p.signal_id,risk_row[0],risk_row[1],risk_row[2],risk_row[3],p.invalidation_hit_s,age,
                                     max(0.0,age-float(risk_row[2] or 0)),price,reclaim_bucket,sc_reclaim,int(time.time())),
                                )
                                conn_r.commit()
                        finally:
                            conn_r.close()

        # Runner-only shadow exit: arm only after TP2, avoiding the known too-early legacy Shadow Exit problem.
        if RUNNER_SHADOW_ENABLED and p.tp2_hit_s is not None and not p.runner_exit_sent:
            p.runner_peak_price = max(p.runner_peak_price or price, price)
            runner_dd = max(0.0, -pct_change(price, p.runner_peak_price or price))
            trail_pct = max(0.75, min(2.00, max(p.peak_mfe_pct, 1.0) * 0.35))
            if age >= p.tp2_hit_s + 15 and runner_dd >= trail_pct:
                p.runner_exit_sent = True
                m_runner = compute_metrics(symbol)
                sc_runner = score_metrics(m_runner) if m_runner else None
                save_shadow_event(
                    p, "RUNNER_EXIT", age, price, ret, runner_dd, m_runner, sc_runner,
                    f"post-TP2 runner trail; trail={trail_pct:.2f}%; tp2_s={p.tp2_hit_s:.1f}; shadow only", None,
                )

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
            ctx=conn.execute("SELECT signal_generated_ts_ms,breakout_reference_price,execution_status FROM premium_context WHERE signal_id=?",(sid,)).fetchone()
            p.signal_generated_ts_ms=int((ctx[0] if ctx and ctx[0] else int(float(ts)*1000)))
            p.breakout_reference_price=float((ctx[1] if ctx and ctx[1] else 0) or 0)
            p.execution_status_at_signal=str((ctx[2] if ctx and len(ctx)>2 and ctx[2] else "UNKNOWN"))
            fx=conn.execute("SELECT be_hit_s,runner5_hit_s,mfe_after_tp1,mae_after_tp1,mfe_after_tp2,mae_after_tp2 FROM premium_exit_forward_shadow WHERE signal_id=?",(sid,)).fetchone()
            if fx:
                p.forward_be_hit_s=fx[0]; p.forward_runner5_hit_s=fx[1]
                p.forward_mfe_after_tp1=float(fx[2] or 0); p.forward_mae_after_tp1=float(fx[3] or 0)
                p.forward_mfe_after_tp2=float(fx[4] or 0); p.forward_mae_after_tp2=float(fx[5] or 0)
            for ds in conn.execute("""SELECT strategy,decision,horizon_ms,pullback_s,entry_age_s,entry_price,stop_price,tp1_price,tp2_price,tp1_hit_s,tp2_hit_s,stop_hit_s,be_hit_s,mfe_pct,mae_pct,no_entry_reason FROM premium_delayed_entry_shadow WHERE signal_id=? AND completed_60m=0""",(sid,)).fetchall():
                p.delayed_shadows[str(ds[0])]={"decision":ds[1],"horizon_ms":ds[2],"pullback_s":ds[3],"entry_age_s":ds[4],"entry_price":ds[5],"stop":ds[6],"tp1":ds[7],"tp2":ds[8],"tp1_hit_s":ds[9],"tp2_hit_s":ds[10],"stop_hit_s":ds[11],"be_hit_s":ds[12],"mfe":float(ds[13] or 0),"mae":float(ds[14] or 0),"no_entry_reason":ds[15]}
            p.micro_completed={int(x[0]) for x in conn.execute("SELECT horizon_ms FROM premium_micro_snapshots WHERE signal_id=?",(sid,)).fetchall()}
            p.liquidity_completed={int(x[0]) for x in conn.execute("SELECT horizon_ms FROM premium_liquidity_snapshots WHERE signal_id=?",(sid,)).fetchall()}
            p.progress_finalized=bool(conn.execute("SELECT 1 FROM premium_progress_validation WHERE signal_id=?",(sid,)).fetchone())
            ev=conn.execute("SELECT status FROM premium_entry_validation WHERE signal_id=?",(sid,)).fetchone()
            if ev:
                p.acceptance_finalized=True; p.acceptance_status=str(ev[0] or "UNKNOWN")
            p.reclaim_event_sent=bool(conn.execute("SELECT 1 FROM research_events WHERE origin_signal_id=? AND event_type='RECLAIM_AFTER_STOP' LIMIT 1",(sid,)).fetchone())
            p.runner_exit_sent=bool(conn.execute("SELECT 1 FROM shadow_exit_events WHERE signal_id=? AND event='RUNNER_EXIT' LIMIT 1",(sid,)).fetchone())
            p.liq_risk_15_saved=bool(conn.execute("SELECT 1 FROM shadow_exit_events WHERE signal_id=? AND event='LIQ_RISK_15' LIMIT 1",(sid,)).fetchone())
            p.liq_risk_30_saved=bool(conn.execute("SELECT 1 FROM shadow_exit_events WHERE signal_id=? AND event='LIQ_RISK_30' LIMIT 1",(sid,)).fetchone())
            p.fail_risk_60_saved=bool(conn.execute("SELECT 1 FROM shadow_exit_events WHERE signal_id=? AND event='FAIL_RISK_60' LIMIT 1",(sid,)).fetchone())
            gate=conn.execute("""SELECT decision,observed_ts_ms,gate_price,gate_tp1_hit_s,gate_tp2_hit_s,gate_stop_hit_s,gate_first_event,gate_mfe,gate_mae,sticky_early_hostile
                                FROM premium_execution_gate_shadow WHERE signal_id=?""",(sid,)).fetchone()
            if gate:
                p.gate_shadow_finalized=True; p.gate_shadow_decision=str(gate[0] or "UNKNOWN")
                p.gate_shadow_ts=float(gate[1] or p.signal_generated_ts_ms)/1000.0
                p.gate_shadow_price=float(gate[2] or 0); p.gate_shadow_tp1_hit_s=gate[3]; p.gate_shadow_tp2_hit_s=gate[4]; p.gate_shadow_stop_hit_s=gate[5]
                p.gate_shadow_first_event=gate[6]; p.gate_shadow_mfe=float(gate[7] or 0); p.gate_shadow_mae=float(gate[8] or 0); p.sticky_early_hostile=bool(gate[9])
                p.gate_shadow_completed={int(x[0]) for x in conn.execute("SELECT post_gate_horizon_s FROM premium_gate_counterfactual WHERE signal_id=?",(sid,)).fetchall()}
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
        # V5.13.3 stage-entry cohorts survive deploy/restart when the DB is persistent.
        for r in conn.execute(
            """SELECT id,symbol,stage,episode_id,entry_price,created_ts_ms,stop_price,tp1_price,tp2_price,signal_id,decision,entry_age_s,
                      tp1_hit_s,tp2_hit_s,stop_hit_s,be0_hit_s,be10_hit_s,be15_hit_s,mfe_pct,mae_pct,
                      runner25_active_s,runner25_exit_s,runner25_exit_price,runner25_peak,
                      runner30_active_s,runner30_exit_s,runner30_exit_price,runner30_peak,completed_60m
               FROM entry_stage_forward_shadow WHERE completed_60m=0 AND created_ts_ms>=?""", (int((now-STAGE_ENTRY_HORIZON_S-120)*1000),)
        ).fetchall():
            if r[1] not in states: continue
            x=PendingStageEntry(int(r[0]),str(r[1]),str(r[2]),int(r[3] or 0),float(r[4]),float(r[5])/1000.0,
                                float(r[6] or 0),float(r[7] or 0),float(r[8] or 0),signal_id=(int(r[9]) if r[9] is not None else None),
                                decision=str(r[10] or ""),entry_age_s=r[11],tp1_hit_s=r[12],tp2_hit_s=r[13],stop_hit_s=r[14],
                                be0_hit_s=r[15],be10_hit_s=r[16],be15_hit_s=r[17],mfe=float(r[18] or 0),mae=float(r[19] or 0),
                                runner25_active_s=r[20],runner25_exit_s=r[21],runner25_exit_price=r[22],runner25_peak=float(r[23] or 0),
                                runner30_active_s=r[24],runner30_exit_s=r[25],runner30_exit_price=r[26],runner30_peak=float(r[27] or 0),
                                completed_60m=bool(r[28]))
            pending_stage_entries.append(x)
        log.info("Recovered trackers: premium=%d radar=%d gainers=%d research=%d shadow=%d stage=%d",
                 len(pending_outcomes),len(pending_radars),len(pending_gainers),len(pending_research),len(pending_shadow_events),len(pending_stage_entries))
    except Exception as e:
        log.warning("Tracker recovery failed: %r", e)
    finally:
        conn.close()


telegram_send_lock = asyncio.Lock()


async def telegram_send(session: aiohttp.ClientSession, text: str, symbol: Optional[str] = None,
                        notification_kind: Optional[str] = None, notification_ordinal: Optional[int] = None,
                        signal_id: Optional[int] = None, signal_price: Optional[float] = None,
                        entry_status: Optional[str] = None, chat_id: Optional[str] = None,
                        reply_markup: Optional[dict] = None, track_delivery: bool = True) -> bool:
    """Send a Telegram message reliably.

    Retries transient network/5xx/429 failures and logs the real exception type,
    HTTP status and Telegram response body so Railway logs are actionable.
    """
    target_chat_id = str(chat_id or TELEGRAM_CHAT_ID)
    if not TELEGRAM_BOT_TOKEN or not target_chat_id:
        log.warning("Telegram credentials/chat missing; alert printed only:\n%s", text)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": target_chat_id, "text": text, "disable_web_page_preview": True}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    elif symbol:
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
        send_start_ms = now_ms()
        live_bid = live_ask = drift = None
        if symbol and symbol in states:
            st_live = states[symbol]
            live_bid = st_live.bid_price or st_live.last_price or None
            live_ask = st_live.ask_price or st_live.last_price or None
            if signal_price and live_ask:
                drift = pct_change(live_ask, signal_price)
        if track_delivery:
            update_notification_delivery(
                symbol or "", notification_kind, notification_ordinal, signal_id=signal_id, send_start_ts_ms=send_start_ms,
                live_bid=live_bid, live_ask=live_ask, price_drift_pct=drift, entry_status=entry_status,
            )
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
                            msg_id = None
                            try:
                                msg_id = int((data.get("result") or {}).get("message_id"))
                            except Exception:
                                msg_id = None
                            if track_delivery:
                                update_notification_delivery(
                                    symbol or "", notification_kind, notification_ordinal, signal_id=signal_id,
                                    send_done_ts_ms=now_ms(), telegram_message_id=msg_id, live_bid=live_bid, live_ask=live_ask,
                                    price_drift_pct=drift, entry_status=entry_status,
                                )
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


async def telegram_public_alert(session: aiohttp.ClientSession, text: str, symbol: Optional[str] = None,
                                notification_kind: Optional[str] = None, notification_ordinal: Optional[int] = None,
                                signal_id: Optional[int] = None, signal_price: Optional[float] = None,
                                entry_status: Optional[str] = None) -> bool:
    """Send a trading alert to the private owner chat and, when enabled, the subscriber channel.

    Delivery/latency metrics are recorded only for the primary private send so the second channel
    delivery cannot overwrite execution measurements. Admin commands/research messages do not use
    this wrapper and therefore remain private.
    """
    primary_ok = await telegram_send(
        session, text, symbol=symbol, notification_kind=notification_kind,
        notification_ordinal=notification_ordinal, signal_id=signal_id,
        signal_price=signal_price, entry_status=entry_status, track_delivery=True,
    )
    if (TELEGRAM_BROADCAST_ENABLED and notification_kind in PUBLIC_NOTIFICATION_KINDS
            and str(TELEGRAM_BROADCAST_CHAT_ID) != str(TELEGRAM_CHAT_ID)):
        channel_ok = await telegram_send(
            session, text, symbol=symbol, notification_kind=notification_kind,
            notification_ordinal=notification_ordinal, signal_id=signal_id,
            signal_price=signal_price, entry_status=entry_status,
            chat_id=TELEGRAM_BROADCAST_CHAT_ID, track_delivery=False,
        )
        if not channel_ok:
            log.warning("Public channel broadcast failed kind=%s symbol=%s chat=%s",
                        notification_kind, symbol, TELEGRAM_BROADCAST_CHAT_ID)
    return primary_ok


async def telegram_api_call(session: aiohttp.ClientSession, method: str, payload: dict) -> dict:
    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "description": "bot token missing"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15, connect=6, sock_read=10)) as r:
            text = await r.text()
            try:
                data = json.loads(text)
            except Exception:
                data = {"ok": False, "description": text[:500]}
            if r.status != 200 or not data.get("ok", False):
                log.warning("Telegram %s failed HTTP=%s body=%s", method, r.status, text[:1000])
            return data
    except Exception as e:
        log.warning("Telegram %s exception: %r", method, e)
        return {"ok": False, "description": repr(e)}


async def handle_join_request(session: aiohttp.ClientSession, req: dict):
    """Never auto-approve: every matching request is sent to the configured admin chat with explicit buttons."""
    if not JOIN_REQUEST_APPROVAL_ENABLED:
        return
    chat = req.get("chat") or {}
    chat_id = str(chat.get("id", ""))
    if chat_id != str(TELEGRAM_APPROVAL_CHAT_ID):
        return
    user = req.get("from") or {}
    user_id = str(user.get("id", ""))
    if not user_id:
        return
    username = user.get("username")
    full_name = " ".join(x for x in [user.get("first_name"), user.get("last_name")] if x) or "—"
    invite = req.get("invite_link") or {}
    invite_name = invite.get("name") or "onaylı davet linki"
    bio = str(req.get("bio") or "").strip()
    text = (
        "👤 YENİ KATILIM TALEBİ\n\n"
        f"Kanal/Grup: {chat.get('title') or chat_id}\n"
        f"Ad: {full_name}\n"
        f"Username: @{username if username else '—'}\n"
        f"User ID: {user_id}\n"
        f"Kaynak: {invite_name}\n"
        f"Bio: {bio[:250] if bio else '—'}\n\n"
        "Sen onaylamadan kullanıcı içeri alınmaz."
    )
    markup = {
        "inline_keyboard": [[
            {"text": "✅ KABUL ET", "callback_data": f"jr:a:{chat_id}:{user_id}"},
            {"text": "❌ REDDET", "callback_data": f"jr:d:{chat_id}:{user_id}"},
        ]]
    }
    await telegram_send(session, text, chat_id=TELEGRAM_ADMIN_CHAT_ID, reply_markup=markup)


async def handle_join_callback(session: aiohttp.ClientSession, cb: dict):
    data = str(cb.get("data") or "")
    if not data.startswith("jr:"):
        return False
    callback_id = cb.get("id")
    actor = cb.get("from") or {}
    actor_id = str(actor.get("id", ""))
    msg_chat_id = str(((cb.get("message") or {}).get("chat") or {}).get("id", ""))
    if msg_chat_id != str(TELEGRAM_ADMIN_CHAT_ID) or (TELEGRAM_ADMIN_USER_ID and actor_id != TELEGRAM_ADMIN_USER_ID):
        if callback_id:
            await telegram_api_call(session, "answerCallbackQuery", {"callback_query_id": callback_id, "text": "Bu işlem için yetkin yok.", "show_alert": True})
        return True
    try:
        _, action, chat_id, user_id = data.split(":", 3)
    except ValueError:
        return True
    if str(chat_id) != str(TELEGRAM_APPROVAL_CHAT_ID):
        return True
    method = "approveChatJoinRequest" if action == "a" else "declineChatJoinRequest"
    result = await telegram_api_call(session, method, {"chat_id": chat_id, "user_id": int(user_id)})
    ok = bool(result.get("ok"))
    if callback_id:
        await telegram_api_call(
            session, "answerCallbackQuery",
            {"callback_query_id": callback_id, "text": ("Kabul edildi." if action == "a" else "Reddedildi.") if ok else "İşlem başarısız.", "show_alert": not ok},
        )
    msg = cb.get("message") or {}
    if ok and msg.get("message_id"):
        original = str(msg.get("text") or "")
        suffix = "\n\n✅ KABUL EDİLDİ" if action == "a" else "\n\n❌ REDDEDİLDİ"
        await telegram_api_call(
            session, "editMessageText",
            {"chat_id": msg_chat_id, "message_id": msg["message_id"], "text": original + suffix, "reply_markup": {"inline_keyboard": []}},
        )
    return True


async def create_approval_invite_link(session: aiohttp.ClientSession) -> Optional[str]:
    if not JOIN_REQUEST_APPROVAL_ENABLED:
        return None
    result = await telegram_api_call(
        session, "createChatInviteLink",
        {"chat_id": TELEGRAM_APPROVAL_CHAT_ID, "name": "Momentum Admin Approval", "creates_join_request": True},
    )
    if result.get("ok"):
        return str((result.get("result") or {}).get("invite_link") or "") or None
    return None


async def fetch_json(session, path, params=None):
    async with session.get(REST + path, params=params, timeout=15) as r:
        r.raise_for_status()
        return await r.json()


async def load_symbols(session):
    info = await fetch_json(session, "/fapi/v1/exchangeInfo")
    out = []
    exchange_filters.clear()
    for s in info.get("symbols", []):
        if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            sym = s["symbol"]
            out.append(sym)
            filt = {x.get("filterType"): x for x in s.get("filters", [])}
            exchange_filters[sym] = {
                "price_precision": int(s.get("pricePrecision", 8) or 8),
                "qty_precision": int(s.get("quantityPrecision", 8) or 8),
                "tick_size": float((filt.get("PRICE_FILTER") or {}).get("tickSize", 0) or 0),
                "step_size": float((filt.get("MARKET_LOT_SIZE") or filt.get("LOT_SIZE") or {}).get("stepSize", 0) or 0),
                "min_qty": float((filt.get("MARKET_LOT_SIZE") or filt.get("LOT_SIZE") or {}).get("minQty", 0) or 0),
                "max_qty": float((filt.get("MARKET_LOT_SIZE") or filt.get("LOT_SIZE") or {}).get("maxQty", 0) or 0),
                "min_notional": float((filt.get("MIN_NOTIONAL") or filt.get("NOTIONAL") or {}).get("notional", 0) or 0),
            }
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
        st.minute_high_ts_ms = ts_ms
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
        st.minute_high_ts_ms = ts_ms
        return
    st.minute_close = price
    if price > st.minute_high:
        st.minute_high = price
        st.minute_high_ts_ms = ts_ms
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


def phase_context(st: SymbolState) -> dict:
    """Lookahead-free phase context at the current live tick.

    Closed candles define the historical references; the live 1m candle is tracked separately.
    """
    c = list(st.candles)
    price = st.last_price
    if not price:
        return {}
    prev_1m_high = c[-1].high if len(c) >= 1 else 0.0
    prev_3m_high = max((x.high for x in c[-3:]), default=0.0)
    breakout_ref = max((x.high for x in c[-15:]), default=0.0)
    current_range_pct = ((st.minute_high - st.minute_low) / price) * 100.0 if st.minute_low > 0 and st.minute_high > 0 else 0.0
    current_body_pct = abs(pct_change(st.minute_close, st.minute_open)) if st.minute_open > 0 and st.minute_close > 0 else 0.0
    upper_wick_pct = ((st.minute_high - max(st.minute_open, st.minute_close)) / price) * 100.0 if st.minute_high > 0 else 0.0
    now = time.time()
    episode_age = max(0.0, now - st.episode_started_ts) if st.episode_started_ts else 0.0
    distance_from_low = pct_change(price, st.episode_low_price) if st.episode_low_price else 0.0
    dist_episode_peak = pct_change(price, st.episode_peak_price) if st.episode_peak_price else 0.0
    seconds_since_episode_peak = max(0.0, now - st.episode_peak_ts) if st.episode_peak_ts else 0.0
    recv_now = now_ms()
    trade_age_ms = max(0, recv_now - st.last_trade_receive_ms) if st.last_trade_receive_ms else None
    book_age_ms = max(0, recv_now - st.last_book_receive_ms) if st.last_book_receive_ms else None
    receive_lag_ms = max(0, st.last_trade_receive_ms - st.last_trade_event_ms) if st.last_trade_event_ms and st.last_trade_receive_ms else None
    return {
        "breakout_reference_price": breakout_ref or None,
        "dist_breakout_pct": pct_change(price, breakout_ref) if breakout_ref else None,
        "prev_1m_high": prev_1m_high or None,
        "prev_3m_high": prev_3m_high or None,
        "dist_prev_1m_high_pct": pct_change(price, prev_1m_high) if prev_1m_high else None,
        "dist_prev_3m_high_pct": pct_change(price, prev_3m_high) if prev_3m_high else None,
        "current_1m_range_pct": current_range_pct,
        "current_1m_body_pct": current_body_pct,
        "current_1m_upper_wick_pct": max(0.0, upper_wick_pct),
        "episode_age_s": episode_age,
        "distance_from_episode_low_pct": distance_from_low,
        "dist_episode_peak_pct": dist_episode_peak,
        "seconds_since_episode_peak": seconds_since_episode_peak,
        "trade_data_age_ms": trade_age_ms,
        "book_data_age_ms": book_age_ms,
        "event_receive_lag_ms": receive_lag_ms,
    }


def market_data_fresh(symbol: str) -> Tuple[bool, List[str]]:
    """Correctness guard only: refuse trade-grade evaluation on clearly stale symbol data."""
    st = states[symbol]
    now = now_ms()
    reasons = []
    if not st.last_trade_receive_ms:
        reasons.append("aggTrade timestamp missing")
    elif now - st.last_trade_receive_ms > MAX_SYMBOL_TRADE_STALE_S * 1000:
        reasons.append(f"aggTrade stale {(now-st.last_trade_receive_ms)/1000:.1f}s")
    if not st.last_book_receive_ms or not st.bid_price or not st.ask_price:
        reasons.append("bookTicker missing")
    elif now - st.last_book_receive_ms > MAX_SYMBOL_BOOK_STALE_S * 1000:
        reasons.append(f"book stale {(now-st.last_book_receive_ms)/1000:.1f}s")
    if st.last_trade_event_ms and st.last_trade_receive_ms and st.last_trade_receive_ms - st.last_trade_event_ms > MAX_EVENT_RECEIVE_LAG_MS:
        reasons.append(f"event lag {st.last_trade_receive_ms-st.last_trade_event_ms}ms")
    return not reasons, reasons


def compute_metrics(symbol: str, ignore_volume_gate: bool = False):
    st = states[symbol]
    if (not ignore_volume_gate and st.quote_volume24 < MIN_24H_QUOTE_VOLUME) or not st.last_price:
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
    flow_eff60 = chg60 / max(flow60, 0.10)
    anchor_flow30 = 0.0
    if st.anchor_avg1m > 0 and (time.time() - st.anchor_ts) <= ANCHOR_MAX_AGE_SECONDS:
        anchor_expected30 = st.anchor_avg1m / 2.0
        anchor_flow30 = q30 / anchor_expected30 if anchor_expected30 else 0.0
    phase = phase_context(st)

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
        **phase,
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
    notice_line = f"🔔 Bu coin için günün {notice}. kullanıcı bildirimi\n" if notice else ""
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
    notice_line = f"🔔 Bu coin için günün {notice}. kullanıcı bildirimi\n" if notice else ""
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
    oi_line = "veri yok" if m.get("oi5") is None else f"{m['oi5']:+.2f}% ({m.get('oi_regime','UNKNOWN')})"
    ex = m.get("execution") or compute_execution_context(symbol, m, plan)
    return (
        f"🔎 {symbol} — ANLIK ANALİZ\n\n"
        f"{verdict}\n"
        f"⭐ Momentum: {score}/100 | 📈 Yükseliş: {rise_score}/100 | 🎯 Giriş: {quality}/100\n"
        f"✅ Süreklilik: {st.candidate_passes}/{CONFIRM_REQUIRED} | aday run-up: {runup:+.2f}%\n\n"
        f"⚡ 30 sn {m['chg30']:+.2f}% | 60 sn {m['chg60']:+.2f}% | 5 dk {m['chg5']:+.2f}%\n"
        f"💥 Flow {m['flow30']:.1f}x | 🟢 Buy %{m['buy30']*100:.1f} | 📚 Bid %{m['book_imbalance']*100:.1f}\n"
        f"₿ BTC relatif {m['rel30']:+.2f}% | OI 5dk {oi_line} | Breakout {'evet' if m['breakout'] else 'hayır'}\n"
        f"🏆 Gainers: {'#'+str(rank) if rank else '—'} | 🧪 Faz riski: {m.get('phase_risk','UNKNOWN')}\n"
        f"📏 Episode dip→anlık {m.get('distance_from_episode_low_pct',0):+.2f}% | tepe→anlık {m.get('dist_episode_peak_pct',0):+.2f}%\n\n"
        f"🧭 Neden: {why}\n\n"
        "🧭 Execution\n"
        f"Canlı ask {fmt_price(ex['live_ask'])} | kayma {ex['drift_pct']:+.2f}% | band mesafesi {ex['band_distance_pct']:+.2f}%\n"
        f"{ex['label']}\n\n"
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
        "entry_mid": entry_mid,
        "invalidation": invalidation,
        "target1": target1,
        "target2": target2,
        "rr1": rr1,
        "rr2": rr2,
        "atr_pct": atr_pct,
    }


async def get_oi_context(session, symbol: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return latest 5m OI change, previous 5m OI change and acceleration (percentage-point delta)."""
    try:
        d = await fetch_json(session, "/futures/data/openInterestHist", {"symbol": symbol, "period": "5m", "limit": 3})
        if isinstance(d, list) and len(d) >= 2:
            vals = [float(x.get("sumOpenInterest", 0) or 0) for x in d[-3:]]
            oi5 = pct_change(vals[-1], vals[-2]) if len(vals) >= 2 else None
            oi_prev5 = pct_change(vals[-2], vals[-3]) if len(vals) >= 3 else None
            oi_accel5 = (oi5 - oi_prev5) if oi5 is not None and oi_prev5 is not None else None
            return oi5, oi_prev5, oi_accel5
    except Exception as e:
        log.debug("OI history failed %s: %s", symbol, e)
    return None, None, None


async def get_oi_5m(session, symbol: str) -> Optional[float]:
    oi5, _, _ = await get_oi_context(session, symbol)
    return oi5


def phase_risk_shadow(m: dict) -> Tuple[str, int, List[str]]:
    """V5.7 research-only exhaustion/late-phase flag. Never used as a Premium hard gate."""
    pts = 0
    reasons: List[str] = []
    oi5 = m.get("oi5")
    if oi5 is not None and oi5 < -0.05:
        pts += 2; reasons.append(f"OI {oi5:+.2f}%")
    if m.get("chg5", 0) >= 3.0:
        pts += 2; reasons.append(f"5dk {m.get('chg5',0):+.2f}%")
    elif m.get("chg5", 0) >= 2.2:
        pts += 1; reasons.append(f"5dk {m.get('chg5',0):+.2f}%")
    if m.get("current_1m_range_pct", 0) >= 2.0:
        pts += 1; reasons.append(f"1m range {m.get('current_1m_range_pct',0):.2f}%")
    if m.get("current_1m_upper_wick_pct", 0) >= 0.50:
        pts += 1; reasons.append(f"üst fitil {m.get('current_1m_upper_wick_pct',0):.2f}%")
    if m.get("dist_episode_peak_pct", 0) <= -0.40:
        pts += 2; reasons.append(f"episode tepeden {m.get('dist_episode_peak_pct',0):.2f}%")
    if m.get("distance_from_episode_low_pct", 0) >= 2.5:
        pts += 1; reasons.append(f"episode dibinden +{m.get('distance_from_episode_low_pct',0):.2f}%")
    if m.get("flow30", 0) >= 12 and m.get("chg30", 0) < 0.80:
        pts += 1; reasons.append("yüksek flow / sınırlı ilerleme")
    label = "HIGH" if pts >= 4 else "MEDIUM" if pts >= 2 else "LOW"
    return label, pts, reasons


def compute_execution_context(symbol: str, m: dict, plan: dict) -> dict:
    """Informational live-entry geometry. It does not alter Premium selection or place an order."""
    st = states[symbol]
    signal_price = float(m.get("price") or 0.0)
    live_bid = st.bid_price or st.last_price or signal_price
    live_ask = st.ask_price or st.last_price or signal_price
    drift_pct = pct_change(live_ask, signal_price) if signal_price else 0.0
    if plan["entry_low"] <= live_ask <= plan["entry_high"]:
        band_distance_pct = 0.0
    elif live_ask > plan["entry_high"]:
        band_distance_pct = pct_change(live_ask, plan["entry_high"])
    else:
        band_distance_pct = pct_change(live_ask, plan["entry_low"])
    remaining_tp1_pct = pct_change(plan["target1"], live_ask) if live_ask else 0.0
    remaining_tp2_pct = pct_change(plan["target2"], live_ask) if live_ask else 0.0
    stop_risk_pct = abs(pct_change(plan["invalidation"], live_ask)) if live_ask else 0.0
    live_rr1 = max(0.0, remaining_tp1_pct) / max(stop_risk_pct, 1e-9)
    live_rr2 = max(0.0, remaining_tp2_pct) / max(stop_risk_pct, 1e-9)
    if live_ask <= plan["invalidation"]:
        status = "INVALIDATED"
        label = "🔴 Kural tabanlı: geçersizlik seviyesi aşıldı — giriş yok"
    elif drift_pct > EXEC_CHASE_MAX_DRIFT_PCT or remaining_tp1_pct <= 0 or live_rr1 < EXEC_MIN_LIVE_RR1:
        status = "CHASED"
        label = "⚠️ Kural tabanlı: giriş bölgesi kaçtı — kovalamayın"
    elif drift_pct <= EXEC_VALID_MAX_DRIFT_PCT and band_distance_pct <= EXEC_VALID_MAX_DRIFT_PCT and live_rr1 >= 0.75:
        status = "VALID"
        label = "✅ Kural tabanlı: giriş bölgesi hâlâ yakın/uygulanabilir"
    else:
        status = "WAIT_RECLAIM"
        label = "🟡 Kural tabanlı: pullback / yeniden kabul bekleme bölgesi"
    generated_ms = int(m.get("signal_generated_ts_ms") or now_ms())
    return {
        "signal_price": signal_price,
        "live_bid": live_bid,
        "live_ask": live_ask,
        "drift_pct": drift_pct,
        "band_distance_pct": band_distance_pct,
        "remaining_tp1_pct": remaining_tp1_pct,
        "remaining_tp2_pct": remaining_tp2_pct,
        "stop_risk_pct": stop_risk_pct,
        "live_rr1": live_rr1,
        "live_rr2": live_rr2,
        "signal_age_ms": max(0, now_ms() - generated_ms),
        "status": status,
        "label": label,
    }


def build_message(m: dict):
    oi_line = "⚪ OI 5 dk: veri yok" if m.get("oi5") is None else f"📈 OI 5 dk: {m['oi5']:+.2f}% ({m.get('oi_regime','UNKNOWN')})"
    oi_accel_line = "" if m.get("oi_accel5") is None else f" | ΔOI ivme {m['oi_accel5']:+.2f} puan"
    breakout_ref = m.get("breakout_reference_price")
    breakout_line = (
        f"🚀 Önceki 15 kapalı 1dk tepe üstünde ({fmt_price(breakout_ref)})"
        if m["breakout"] and breakout_ref
        else (f"🎯 Önceki 15 kapalı 1dk tepe henüz kırılmadı ({fmt_price(breakout_ref)})" if breakout_ref else "🎯 Breakout referansı yok")
    )
    plan = m.get("trade_plan") or estimate_trade_plan(m["symbol"], m)
    ex = m.get("execution") or compute_execution_context(m["symbol"], m, plan)
    notice = m.get("daily_notice_no")
    premium_ordinal = m.get("premium_ordinal")
    notice_line = f"🔔 Bu coin için günün {notice}. kullanıcı bildirimi\n" if notice else ""
    premium_line = f"🧩 Günün {premium_ordinal}. Premium'u\n" if premium_ordinal else ""
    if ex["status"] == "VALID":
        header = "🟢 ALIM FIRSATI — PREMIUM + SÜREKLİLİK TEYİTLİ"
    elif ex["status"] == "WAIT_RECLAIM":
        header = "🟡 PREMIUM SETUP — MOMENTUM TEYİTLİ, GİRİŞ BEKLEME BÖLGESİ"
    elif ex["status"] == "CHASED":
        header = "⚠️ PREMIUM MOMENTUM — GİRİŞ BÖLGESİ KAÇMIŞ OLABİLİR"
    else:
        header = "🔴 PREMIUM MOMENTUM — GİRİŞ GEÇERSİZ"
    return (
        f"{header}\n\n"
        f"🪙 {m['symbol']}\n"
        f"{notice_line}"
        f"{premium_line}"
        f"💰 Sinyal fiyatı: {fmt_price(m['price'])}\n\n"
        f"⚡ 30 sn: {m['chg30']:+.2f}%\n"
        f"🔥 60 sn: {m['chg60']:+.2f}%\n"
        f"📈 5 dk: {m['chg5']:+.2f}%\n\n"
        f"💥 Hacim akışı 30 sn: {m['flow30']:.1f}x\n"
        f"🟢 Agresif alış: %{m['buy30']*100:.1f}\n"
        f"📚 Bid baskısı: %{m['book_imbalance']*100:.1f}\n"
        f"₿ BTC'ye göre güç: {m['rel30']:+.2f}%\n"
        f"{oi_line}{oi_accel_line}\n"
        f"{breakout_line}\n\n"
        f"✅ {m['confirm_passes']}/{CONFIRM_REQUIRED} süreklilik kontrolü geçti\n"
        f"📈 Yükseliş kural skoru: {m['rise_score']}/100\n"
        f"⚡ Momentum yoğunluğu: {m['score']}/100\n"
        f"🎯 Giriş kalite kural skoru: {m['entry_quality']}/100\n"
        f"🧭 İlk adaydan beri: {m.get('candidate_runup', 0.0):+.2f}%\n"
        f"📏 Episode dibinden: {m.get('distance_from_episode_low_pct', 0.0):+.2f}% | episode tepesine göre: {m.get('dist_episode_peak_pct', 0.0):+.2f}%\n"
        f"🧪 Faz/Exhaustion riski: {m.get('phase_risk','UNKNOWN')} ({m.get('phase_risk_points',0)} puan, SHADOW)\n\n"
        "🧭 EXECUTION DURUMU (kural tabanlı)\n"
        f"Canlı ask: {fmt_price(ex['live_ask'])} | sinyalden kayma: {ex['drift_pct']:+.2f}%\n"
        f"Alım bandına mesafe: {ex['band_distance_pct']:+.2f}% | sinyal yaşı: {ex['signal_age_ms']/1000:.1f} sn\n"
        f"Kalan TP1: {ex['remaining_tp1_pct']:+.2f}% | stop riski: {ex['stop_risk_pct']:.2f}% | canlı R/R1 ~{ex['live_rr1']:.2f}\n"
        f"{ex['label']}\n\n"
        "📍 TAHMİNİ İŞLEM BÖLGESİ\n"
        f"🟩 Alım bölgesi: {fmt_price(plan['entry_low'])} – {fmt_price(plan['entry_high'])}\n"
        f"🎯 Kâr al 1: {fmt_price(plan['target1'])}  (R/R ~{plan['rr1']:.1f})\n"
        f"🎯 Kâr al 2: {fmt_price(plan['target2'])}  (R/R ~{plan['rr2']:.1f})\n"
        f"🛑 Geçersizlik: {fmt_price(plan['invalidation'])}\n\n"
        f"⚠️ Rise/Entry/Momentum değerleri olasılık değildir; kural tabanlı skorlardır. V{BOT_VERSION} liquidity V2/CORE, ignition V2, 15/60 sn acceptance+progress ve reclaim davranışını SHADOW olarak ölçer.\n"
        "Not: Seviyeler kural tabanlı tahminlerdir; kâr garantisi veya otomatik emir değildir.\n"
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
        # V5.8 can inspect plausible ignition/near-miss setups below the 5M production volume gate.
        # Production logic still returns here before candidate creation when qv24 is below MIN_24H_QUOTE_VOLUME.
        m = compute_metrics(symbol, ignore_volume_gate=True)
        if not m:
            return
        score = score_metrics(m)
        maybe_record_v59_research(symbol, m, score, now, candidate_active=bool(st.candidate_since))
        if m.get("qv24", 0) < MIN_24H_QUOTE_VOLUME:
            return
        # V5.13.4 real-early trend watcher is independent from candidate/episode resets.
        await maybe_trend_build_up(session, symbol, m, score, now)
        # Existing V5.7 research collectors remain observer-only.
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
            _arm_stage_entry(symbol, "CANDIDATE", m, st.episode_id or 0, created_ts=now, decision="QUALIFIES_START")
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
            record_candidate_reject_audit(symbol, m, score, "TTL_REJECT")
            end_episode(symbol, "TTL_REJECT", m, score)
            reset_candidate(st)
            return
        if m["chg30"] < -0.20 or m["buy30"] < 0.50 or m["flow30"] < 0.8:
            funnel_hit("breakdown_reject")
            save_candidate_event(symbol, "breakdown_reject", m, score, st)
            record_candidate_reject_audit(symbol, m, score, "BREAKDOWN_REJECT")
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
                    save_candidate_event(symbol, "early_alert", m, score, st, "V5.7 2/3 selective notify; production thresholds unchanged")
                    _arm_stage_entry(symbol, "EARLY", m, st.episode_id or 0, created_ts=now, decision="PUBLIC_EARLY_2OF3")
                    await telegram_public_alert(session, build_early_message(m, score, st), symbol=symbol,
                                                notification_kind="EARLY", notification_ordinal=m.get("daily_notice_no"), signal_price=m.get("price"))
        else:
            if st.candidate_checks - st.candidate_passes >= 2:
                funnel_hit("continuity_reject")
                save_candidate_event(symbol, "continuity_reject", m, score, st)
                record_candidate_reject_audit(symbol, m, score, "CONTINUITY_REJECT")
                end_episode(symbol, "CONTINUITY_REJECT", m, score)
                reset_candidate(st)
                return

        if st.candidate_passes < CONFIRM_REQUIRED:
            return

        fresh, stale_reasons = market_data_fresh(symbol)
        if not fresh:
            funnel_hit("stale_data_wait")
            save_candidate_event(symbol, "stale_data_wait", m, score, st, "; ".join(stale_reasons))
            return

        # Son teyitte OI alınır. V5.7 geçmiş 5dk ve ivmeyi de kaydeder; production OI score davranışı değişmez.
        oi5, oi_prev5, oi_accel5 = await get_oi_context(session, symbol)
        fresh_after_oi, stale_after_oi = market_data_fresh(symbol)
        if not fresh_after_oi:
            funnel_hit("stale_after_oi_wait")
            save_candidate_event(symbol, "stale_after_oi_wait", m, score, st, "; ".join(stale_after_oi))
            return
        m["oi5"] = oi5
        m["oi_prev5"] = oi_prev5
        m["oi_accel5"] = oi_accel5
        m["oi_regime"] = oi_regime_label(oi5)
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
        phase_label, phase_pts, phase_reasons = phase_risk_shadow(m)
        m["phase_risk"] = phase_label
        m["phase_risk_points"] = phase_pts
        add_research_event("CONFIRMED_3OF3", symbol, m, score,
                           f"quality={quality}; rise={rise_score}; runup={m['candidate_runup']:.2f}; squeeze={int(m['squeeze_risk'])}; "
                           f"phase={phase_label}:{phase_pts}; phase_reasons={','.join(phase_reasons[:4])}")
        if m["squeeze_risk"]:
            add_research_event("SQUEEZE_RISK", symbol, m, score, "OI<=0 + strong flow/buy + weak bid; shadow only")

        # Production V5.5 gates below are intentionally unchanged.
        if quality < ENTRY_MIN_SCORE:
            funnel_hit("quality_reject")
            save_candidate_event(symbol, "quality_reject", m, score, st, f"quality={quality}")
            add_research_event("REJECT_QUALITY", symbol, m, score, f"quality={quality}")
            record_candidate_reject_audit(symbol, m, score, "QUALITY_REJECT")
            end_episode(symbol, "QUALITY_REJECT", m, score)
            reset_candidate(st)
            return
        if rise_score < RISE_MIN_SCORE:
            funnel_hit("rise_reject")
            save_candidate_event(symbol, "rise_reject", m, score, st, f"rise={rise_score}")
            add_research_event("REJECT_RISE", symbol, m, score, f"rise={rise_score}")
            record_candidate_reject_audit(symbol, m, score, "RISE_REJECT")
            end_episode(symbol, "RISE_REJECT", m, score)
            reset_candidate(st)
            return
        if m["extended"]:
            funnel_hit("extended_reject")
            save_candidate_event(symbol, "extended_reject", m, score, st)
            add_research_event("REJECT_EXTENDED", symbol, m, score, "extended after 3/3")
            record_candidate_reject_audit(symbol, m, score, "EXTENDED_REJECT")
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
            record_candidate_reject_audit(symbol, m, score, "PREMIUM_REJECT")
            end_episode(symbol, "PREMIUM_REJECT", m, score)
            reset_candidate(st)
            return
        if now - st.buy_signal_ts < COOLDOWN_SECONDS:
            save_candidate_event(symbol, "cooldown_reject", m, score, st)
            add_research_event("REJECT_COOLDOWN", symbol, m, score, "production cooldown")
            record_candidate_reject_audit(symbol, m, score, "COOLDOWN_REJECT")
            end_episode(symbol, "COOLDOWN_REJECT", m, score)
            reset_candidate(st)
            return

        m["signal_generated_ts_ms"] = now_ms()
        signal_generated_ts = m["signal_generated_ts_ms"] / 1000.0
        st.buy_signal_ts = signal_generated_ts
        st.last_alert_ts = signal_generated_ts
        st.last_alert_price = m["price"]
        m["premium_ordinal"] = next_premium_ordinal(symbol)
        m["trade_plan"] = estimate_trade_plan(symbol, m)
        m["daily_notice_no"] = next_daily_notice_no(symbol, "PREMIUM")
        m["execution"] = compute_execution_context(symbol, m, m["trade_plan"])
        mark_episode_premium(symbol)
        funnel_hit("telegram_signal")
        save_candidate_event(symbol, "premium_signal", m, score, st,
                             f"quality={quality}; rise={rise_score}; runup={runup:.2f}; oi_regime={m.get('oi_regime')}; exec={m['execution']['status']}")
        signal_id = save_signal(m)
        save_signal_meta(signal_id, m)
        save_premium_context(signal_id, m)
        link_notification_to_signal(symbol, "PREMIUM", m.get("daily_notice_no"), signal_id)
        save_premium_radar_link(signal_id, symbol, m["price"], signal_generated_ts, st.active_radar_id)
        plan = m["trade_plan"]
        entry_touch = 0.0 if plan["entry_low"] <= m["price"] <= plan["entry_high"] else None
        path_entry_price = m["price"] if entry_touch is not None else 0.0
        init_signal_path(signal_id, plan["entry_low"], plan["entry_high"], plan["target1"], plan["target2"], plan["invalidation"], entry_touch, path_entry_price)
        po = PendingOutcome(
            signal_id, symbol, m["price"], signal_generated_ts, plan["target1"], plan["target2"], plan["invalidation"],
            plan["entry_low"], plan["entry_high"], entry_touch, path_entry_price
        )
        po.signal_generated_ts_ms = int(m["signal_generated_ts_ms"])
        po.breakout_reference_price = float(m.get("breakout_reference_price") or 0.0)
        po.execution_status_at_signal = str((m.get("execution") or {}).get("status") or "UNKNOWN")
        po.peak_price = m["price"]
        po.acceptance_peak_price = m["price"]
        po.acceptance_last_ts = signal_generated_ts
        po.acceptance_was_above = True if not po.breakout_reference_price else m["price"] >= po.breakout_reference_price
        if po.breakout_reference_price:
            initial_dist = pct_change(m["price"], po.breakout_reference_price)
            po.acceptance_min_dist_pct = initial_dist
            po.acceptance_close_dist_pct = initial_dist
        po.wave_start_price = m["price"]
        po.wave_peak_price = m["price"]
        pending_outcomes.append(po)
        _link_stage_entries_to_premium(symbol, int(m.get("episode_id") or 0), signal_id)
        _arm_stage_entry(symbol, "PREMIUM", m, int(m.get("episode_id") or 0), created_ts=signal_generated_ts, signal_id=signal_id,
                         decision="IMMEDIATE_PREMIUM", entry_age_s=0.0, entry_price=m["price"], levels=_forward_translated_levels(po, float(m["price"])))
        save_wave_tracking(po)
        if LIQUIDITY_RESEARCH_ENABLED:
            po.liquidity_completed.add(0)
            asyncio.create_task(capture_liquidity_snapshot(session, po, 0))
        log.info("PREMIUM CONFIRMED %s momentum=%d rise=%d quality=%d runup=%.2f oi=%s exec=%s episode=%s",
                 symbol, score, rise_score, quality, runup, m.get("oi_regime"), m["execution"]["status"], st.episode_id)
        # AutoTrade is a separate execution layer. OFF does nothing; DRY records/simulates; LIVE is triple-locked.
        try:
            await autotrade_handle_premium(session, signal_id, symbol, m, plan)
        except Exception as e:
            _at_log_event("ENTRY_ERROR",signal_id=signal_id,symbol=symbol,detail=repr(e))
            log.error("AutoTrade premium handler %s: %r",symbol,e)
            if str(autotrade_cfg.get("mode")) == "LIVE":
                await telegram_send(session,f"🚨 AutoTrade {symbol} giriş hatası: {e}",chat_id=TELEGRAM_ADMIN_CHAT_ID)
        await telegram_public_alert(
            session, build_message(m), symbol=symbol, notification_kind="PREMIUM", notification_ordinal=m.get("daily_notice_no"),
            signal_id=signal_id, signal_price=m["price"], entry_status=m["execution"]["status"]
        )
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
                            # Avoid replacing a fresher aggTrade price with the slower 24h ticker snapshot.
                            if not st.last_trade_receive_ms or now_ms() - st.last_trade_receive_ms > 2000:
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
                        recv_ms = now_ms()
                        st.last_book_receive_ms = recv_ms
                        st.last_book_event_ms = int(d.get("E", recv_ms) or recv_ms)
                        st.bid_price = float(d.get("b", 0) or 0)
                        st.bid_qty = float(d.get("B", 0) or 0)
                        st.ask_price = float(d.get("a", 0) or 0)
                        st.ask_qty = float(d.get("A", 0) or 0)
        except Exception as e:
            log.warning("Book WS reconnecting: %s", e)
            await asyncio.sleep(2)


def apply_mark_price_event(d: dict, recv: Optional[float] = None) -> bool:
    """Apply one Binance mark-price event. Split out for deterministic testing."""
    if not isinstance(d, dict) or d.get("st") not in (None, 1):
        return False
    sym = d.get("s")
    if sym not in states:
        return False
    recv = float(recv if recv is not None else time.time())
    st = states[sym]
    try:
        mark = float(d.get("p", 0) or 0)
    except Exception:
        mark = 0.0
    if mark > 0:
        st.mark_price = mark
        st.mark_ts = recv
    rate = d.get("r")
    if rate is not None:
        try:
            st.funding_rate_pct = float(rate) * 100.0
            st.funding_ts = recv
        except Exception:
            pass
    return True


async def mark_price_ws(session):
    """All-market mark-price stream; also carries the latest funding rate for perpetuals."""
    url = WS_MARKET + "?streams=!markPrice@arr@1s"
    while not stop_event.is_set():
        try:
            async with session.ws_connect(url, heartbeat=30, receive_timeout=70) as ws:
                log.info("MarkPrice stream connected")
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    stream_health["mark"] = time.time()
                    payload = json.loads(msg.data).get("data", [])
                    events = payload if isinstance(payload, list) else [payload]
                    recv = time.time()
                    for d in events:
                        apply_mark_price_event(d, recv)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            stream_reconnects["mark"] += 1
            log.warning("MarkPrice WS reconnecting: %s", e)
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
                    recv_ms = now_ms()
                    st.last_trade_event_ms = ts
                    st.last_trade_receive_ms = recv_ms
                    st.last_price = price
                    st.agg_events += 1
                    trade_event_count += 1
                    quote = price * qty
                    st.trades.append(TradeSample(ts, price, quote, aggressive_buy))
                    update_minute_candle(st, ts, price, quote, aggressive_buy)
                    prune_deque_by_ts(st.trades, ts - 120_000)
                    stream_health["agg"] = time.time()
                    agg_stream_health[idx] = stream_health["agg"]
                    update_episode_peak(sym, price, ts / 1000.0)
                    if not st.episode_id and st.prev_meaningful_ts:
                        st.prev_meaningful_low_price = min(st.prev_meaningful_low_price or price, price)
                    update_pending_tick(sym, price, ts / 1000.0)
                    autotrade_on_tick(sym, price, ts / 1000.0)
                    if st.quote_volume24 >= min(MIN_24H_QUOTE_VOLUME, NEAR_MISS_MIN_QV24):
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

            # Fallback snapshot/finalization path in case a symbol has no aggTrade exactly after a micro horizon.
            observed_ms = now_ms()
            signal_ms = p.signal_generated_ts_ms or int(p.created_ts * 1000)
            age_ms = max(0, observed_ms - signal_ms)
            for horizon_ms in MICRO_SNAPSHOT_HORIZONS_MS:
                if age_ms >= horizon_ms and horizon_ms not in p.micro_completed:
                    save_micro_snapshot(p, horizon_ms, observed_ms, price)
                    p.micro_completed.add(horizon_ms)
            if LIQUIDITY_RESEARCH_ENABLED:
                for liq_h in LIQUIDITY_SNAPSHOT_HORIZONS_MS:
                    if age_ms >= liq_h and liq_h not in p.liquidity_completed:
                        p.liquidity_completed.add(liq_h)
                        asyncio.create_task(capture_liquidity_snapshot(session, p, liq_h))
            if age_ms >= ENTRY_ACCEPTANCE_HORIZON_MS and not p.acceptance_finalized:
                finalize_entry_validation(p, observed_ms)
            if age_ms >= PROGRESS_VALIDATION_HORIZON_MS and not p.progress_finalized:
                finalize_progress_validation(p, observed_ms)
            if EXECUTION_GATE_SHADOW_ENABLED and age_ms >= EXECUTION_GATE_HORIZON_MS and not p.gate_shadow_finalized:
                maybe_finalize_execution_gate_shadow(p, observed_ms)

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
                            await telegram_send(session, build_shadow_message(p, "PROTECT", price, ret, drawdown_from_peak, m_shadow, sc_shadow, weak_reasons, notice),
                                                symbol=p.symbol, notification_kind="SHADOW_PROTECT", notification_ordinal=notice, signal_id=p.signal_id, signal_price=price)
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
                            await telegram_send(session, build_shadow_message(p, "EXIT", price, ret, drawdown_from_peak, m_shadow, sc_shadow, extra, notice),
                                                symbol=p.symbol, notification_kind="SHADOW_EXIT", notification_ordinal=notice, signal_id=p.signal_id, signal_price=price)

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

            _update_forward_strategy_shadows(p, price, age, completed_60m=False)

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
                        await telegram_public_alert(session, build_continuation_message(p, m, sc), symbol=p.symbol,
                                                   notification_kind="CONTINUATION", notification_ordinal=m.get("daily_notice_no"), signal_id=p.signal_id, signal_price=m.get("price"))
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
                if p.gate_shadow_finalized:
                    save_gate_shadow_path(p, completed_60m=1)
                _update_forward_strategy_shadows(p, price, age, completed_60m=True)
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
                    save_research_outcome(r.event_id,h,ret,r.mfe,r.mae)
                    if h == MISSED_RUNNER_HORIZON_S:
                        save_missed_runner_audit(r.event_id, ret, r.mfe, r.mae)
                    r.completed.add(h)
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

        stage_remove=[]
        for x in list(pending_stage_entries):
            price=states[x.symbol].last_price
            if not price: continue
            age=now-x.created_ts
            ret=pct_change(price,x.entry_price); x.mfe=max(x.mfe,ret); x.mae=min(x.mae,ret)
            if age >= STAGE_ENTRY_HORIZON_S:
                x.completed_60m=True
                _save_stage_entry(x,price,True)
                stage_remove.append(x)
        for x in stage_remove:
            if x in pending_stage_entries: pending_stage_entries.remove(x)

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



def create_consistent_db_backup() -> Tuple[str, str]:
    """Create a consistent SQLite backup, validate it, and include metadata in the zip."""
    if not DB_BACKUP_ENABLED:
        raise RuntimeError("DB backup disabled")
    tmpdir = tempfile.mkdtemp(prefix="momentum_db_")
    db_copy = os.path.join(tmpdir, "signals_backup.db")
    zip_path = os.path.join(tmpdir, f"signals_backup_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.zip")
    src = db_connect()
    dst = sqlite3.connect(db_copy, timeout=30)
    try:
        # Passive checkpoint keeps writers unblocked while reducing WAL-only tail risk.
        try: src.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception as e: log.warning("DB backup passive checkpoint: %r", e)
        # SQLite Backup API is safe against a live WAL database and avoids raw-file copies.
        src.backup(dst)
        dst.commit()
    finally:
        dst.close(); src.close()

    chk = sqlite3.connect(db_copy, timeout=30)
    try:
        qrow = chk.execute("PRAGMA quick_check").fetchone()
        quick = str(qrow[0]) if qrow else "unknown"
        irows = chk.execute("PRAGMA integrity_check").fetchall()
        integrity = "ok" if irows and all(str(r[0]).lower() == "ok" for r in irows) else "; ".join(str(r[0]) for r in irows[:5]) or "unknown"
        table_count = chk.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
    finally:
        chk.close()

    healthy = quick.lower() == "ok" and integrity.lower() == "ok"
    health = f"quick={quick}; integrity={integrity}; valid={'YES' if healthy else 'NO'}"
    if not healthy:
        log.error("DB BACKUP HEALTH WARNING: %s", health)
    info = (
        f"bot_version={BOT_VERSION}\n"
        f"research_logic_version={RESEARCH_LOGIC_VERSION}\n"
        f"created_ist={datetime.now(IST).isoformat()}\n"
        f"source_db={DB_PATH}\n"
        f"{health}\n"
        f"table_count={table_count}\n"
    )
    info_path = os.path.join(tmpdir, "backup_info.txt")
    with open(info_path, "w", encoding="utf-8") as f:
        f.write(info)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(db_copy, arcname="signals.db")
        zf.write(info_path, arcname="backup_info.txt")
    return zip_path, health


async def telegram_send_document(session: aiohttp.ClientSession, file_path: str, caption: str = "") -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    form = aiohttp.FormData()
    form.add_field("chat_id", str(TELEGRAM_CHAT_ID))
    if caption:
        form.add_field("caption", caption[:1024])
    with open(file_path, "rb") as f:
        form.add_field("document", f, filename=os.path.basename(file_path), content_type="application/zip")
        try:
            async with session.post(url, data=form, timeout=aiohttp.ClientTimeout(total=180, connect=10, sock_read=170)) as r:
                body = await r.text()
                if r.status == 200:
                    return True
                log.warning("Telegram sendDocument HTTP %s body=%s", r.status, body[:1000])
        except Exception as e:
            log.warning("Telegram sendDocument failed: %r", e)
    return False



# ============================== V5.13 AUTOTRADE SAFE EXECUTION ==============================

def _at_local_date() -> str:
    return datetime.now(IST).date().isoformat()


def _at_admin_allowed(chat_id: str, user_id: str, *, require_user_id: bool = False) -> bool:
    if str(chat_id) != str(TELEGRAM_ADMIN_CHAT_ID):
        return False
    if TELEGRAM_ADMIN_USER_ID:
        return str(user_id) == str(TELEGRAM_ADMIN_USER_ID)
    return not require_user_id


def _at_bool(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _at_log_event(event: str, *, trade_id=None, signal_id=None, symbol=None, detail=""):
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO autotrade_events(ts_ms,trade_id,signal_id,symbol,event,detail) VALUES (?,?,?,?,?,?)",
            (now_ms(), trade_id, signal_id, symbol, event, str(detail)[:2000]),
        )
        conn.commit()
    finally:
        conn.close()


def _at_save_setting(key: str, value):
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO autotrade_settings(key,value,updated_ts) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts",
            (key, str(value), int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()


def load_autotrade_settings():
    defaults = dict(autotrade_cfg)
    conn = db_connect()
    try:
        rows = dict(conn.execute("SELECT key,value FROM autotrade_settings").fetchall())
    finally:
        conn.close()
    converters = {
        "trade_margin_usdt": float, "leverage": int, "max_open_positions": int,
        "daily_max_loss_pct": float, "max_consecutive_stops": int, "stop_cooldown_minutes": int,
        "max_entry_slippage_pct": float, "runner_fraction": float, "runner_target_pct": float,
        "mode": str, "margin_type": str, "exit_profile": str,
    }
    for k, conv in converters.items():
        if k in rows:
            try: autotrade_cfg[k] = conv(rows[k])
            except Exception: autotrade_cfg[k] = defaults[k]
    autotrade_cfg["trade_margin_usdt"] = max(5.0, float(autotrade_cfg["trade_margin_usdt"]))
    autotrade_cfg["leverage"] = max(1, min(125, int(autotrade_cfg["leverage"])))
    autotrade_cfg["max_open_positions"] = max(1, min(20, int(autotrade_cfg["max_open_positions"])))
    autotrade_cfg["daily_max_loss_pct"] = max(0.25, min(25.0, float(autotrade_cfg["daily_max_loss_pct"])))
    autotrade_cfg["max_consecutive_stops"] = max(1, min(20, int(autotrade_cfg["max_consecutive_stops"])))
    autotrade_cfg["stop_cooldown_minutes"] = max(5, min(720, int(autotrade_cfg["stop_cooldown_minutes"])))
    autotrade_cfg["runner_fraction"] = max(0.05, min(0.95, float(autotrade_cfg["runner_fraction"])))
    autotrade_cfg["runner_target_pct"] = max(0.25, min(25.0, float(autotrade_cfg["runner_target_pct"])))
    autotrade_cfg["exit_profile"] = str(autotrade_cfg["exit_profile"]).upper()
    if autotrade_cfg["exit_profile"] not in ("CURRENT_TP2", "PARTIAL_RUNNER"):
        autotrade_cfg["exit_profile"] = "CURRENT_TP2"
    # Never resume LIVE after a deploy/restart. Existing live positions are still reconciled/managed.
    persisted_mode = str(autotrade_cfg.get("mode", "OFF")).upper()
    autotrade_cfg["mode"] = "DRY" if persisted_mode == "DRY" and AUTO_TRADE_BOOT_MODE == "DRY" else "OFF"
    _at_save_setting("mode", autotrade_cfg["mode"])
    recover_autotrade_active()
    _at_repair_daily_from_trade_history("DRY")
    _at_repair_daily_from_trade_history("LIVE")


def recover_autotrade_active():
    autotrade_active.clear(); autotrade_active_by_symbol.clear()
    conn = db_connect()
    try:
        rows = conn.execute(
            """SELECT * FROM autotrade_trades WHERE status IN ('OPEN','PARTIAL','PROTECTIVE_PARTIAL') ORDER BY id"""
        ).fetchall()
        cols = [d[0] for d in conn.execute("SELECT * FROM autotrade_trades LIMIT 0").description]
    finally:
        conn.close()
    for row in rows:
        tr = dict(zip(cols, row))
        autotrade_active[int(tr["id"])] = tr
        autotrade_active_by_symbol[str(tr["symbol"])].add(int(tr["id"]))


def _at_scope(scope: Optional[str] = None) -> str:
    if scope:
        return "LIVE" if str(scope).upper() == "LIVE" else "DRY"
    return "LIVE" if str(autotrade_cfg.get("mode", "OFF")).upper() == "LIVE" else "DRY"


def _at_daily_row(start_balance: Optional[float] = None, scope: Optional[str] = None) -> dict:
    d = _at_local_date(); sc = _at_scope(scope)
    conn = db_connect()
    try:
        row = conn.execute("SELECT local_date,scope,start_balance,realized_net_pnl,consecutive_stops,locked,lock_reason,cooldown_until_ts FROM autotrade_daily WHERE local_date=? AND scope=?", (d,sc)).fetchone()
        if row is None:
            sb = float(start_balance if start_balance is not None else AUTO_TRADE_SIM_BALANCE_USDT)
            conn.execute("INSERT INTO autotrade_daily(local_date,scope,start_balance,realized_net_pnl,consecutive_stops,locked,lock_reason,cooldown_until_ts,updated_ts) VALUES (?,?,?,?,?,?,?,?,?)",
                         (d, sc, sb, 0.0, 0, 0, None, 0, int(time.time())))
            conn.commit(); row = (d, sc, sb, 0.0, 0, 0, None, 0)
        elif start_balance is not None and float(row[3] or 0)==0 and int(row[4] or 0)==0 and not bool(row[5]):
            conn.execute("UPDATE autotrade_daily SET start_balance=?,updated_ts=? WHERE local_date=? AND scope=?",(float(start_balance),int(time.time()),d,sc)); conn.commit()
            row = (row[0],row[1],float(start_balance),row[3],row[4],row[5],row[6],row[7])
        return {"local_date":row[0],"scope":row[1],"start_balance":float(row[2]),"realized_net_pnl":float(row[3] or 0),"consecutive_stops":int(row[4] or 0),"locked":bool(row[5]),"lock_reason":row[6] or "","cooldown_until_ts":int(row[7] or 0)}
    finally:
        conn.close()


def _at_update_daily(net_pnl: float, close_reason: str, scope: Optional[str] = None):
    sc=_at_scope(scope); r = _at_daily_row(scope=sc)
    pnl = float(r["realized_net_pnl"]) + float(net_pnl or 0)
    streak = int(r["consecutive_stops"])
    reason = str(close_reason or "").upper()
    if reason == "STOP": streak += 1
    elif float(net_pnl or 0) > 0: streak = 0
    limit = float(r["start_balance"]) * float(autotrade_cfg["daily_max_loss_pct"]) / 100.0
    locked = bool(r["locked"]); lock_reason = str(r["lock_reason"] or "")
    cooldown_until = int(r.get("cooldown_until_ts") or 0)
    if pnl <= -limit:
        locked = True; lock_reason = f"DAILY_LOSS_{autotrade_cfg['daily_max_loss_pct']:.2f}PCT"
    # Clean-backup forward test showed a 4-stop streak can happen during an otherwise profitable day.
    # Therefore the streak is a temporary circuit-breaker; only the daily loss limit is a hard day lock.
    if (not locked) and streak >= int(autotrade_cfg["max_consecutive_stops"]):
        cooldown_until = max(cooldown_until, int(time.time()) + int(autotrade_cfg["stop_cooldown_minutes"])*60)
        streak = 0
        lock_reason = ""
    conn = db_connect()
    try:
        conn.execute("UPDATE autotrade_daily SET realized_net_pnl=?,consecutive_stops=?,locked=?,lock_reason=?,cooldown_until_ts=?,updated_ts=? WHERE local_date=? AND scope=?",
                     (pnl, streak, int(locked), lock_reason or None, cooldown_until, int(time.time()), r["local_date"],sc))
        conn.commit()
    finally: conn.close()
    return _at_daily_row(scope=sc)

def _at_repair_daily_from_trade_history(scope: str):
    """Rebuild today's bot ledger from persisted closed trades; never makes the ledger less conservative."""
    sc=_at_scope(scope); r=_at_daily_row(scope=sc)
    now_local=datetime.now(IST); start_local=datetime.combine(now_local.date(), datetime.min.time(), tzinfo=IST)
    start_ms=int(start_local.timestamp()*1000); end_ms=start_ms+86400000
    conn=db_connect()
    try:
        rows=conn.execute("""SELECT close_reason,net_pnl,closed_ts_ms FROM autotrade_trades
                             WHERE mode=? AND status='CLOSED' AND closed_ts_ms>=? AND closed_ts_ms<? ORDER BY closed_ts_ms,id""",
                          (sc,start_ms,end_ms)).fetchall()
        if not rows: return
        hist_pnl=sum(float(x[1] or 0) for x in rows)
        streak=0
        for reason,pnl,_ in rows:
            if str(reason or '').upper()=="STOP": streak+=1
            elif float(pnl or 0)>0: streak=0
        existing=float(r.get("realized_net_pnl") or 0)
        # If the ledger and trade history disagree, keep the more loss-conservative value.
        repaired=min(existing,hist_pnl) if (existing<0 or hist_pnl<0) else hist_pnl
        limit=float(r["start_balance"])*float(autotrade_cfg["daily_max_loss_pct"])/100.0
        locked=bool(r["locked"]) or repaired<=-limit
        lock_reason=(r.get("lock_reason") or (f"DAILY_LOSS_{autotrade_cfg['daily_max_loss_pct']:.2f}PCT" if locked else None))
        cooldown=int(r.get("cooldown_until_ts") or 0)
        if streak>=int(autotrade_cfg["max_consecutive_stops"]):
            cooldown=max(cooldown,int((int(rows[-1][2] or 0)/1000)+int(autotrade_cfg["stop_cooldown_minutes"])*60))
        conn.execute("UPDATE autotrade_daily SET realized_net_pnl=?,consecutive_stops=?,locked=?,lock_reason=?,cooldown_until_ts=?,updated_ts=? WHERE local_date=? AND scope=?",
                     (repaired,streak,int(locked),lock_reason,cooldown,int(time.time()),r["local_date"],sc)); conn.commit()
    finally: conn.close()


def _at_open_count(mode: Optional[str] = None) -> int:
    if mode:
        return sum(1 for x in autotrade_active.values() if str(x.get("mode")).upper() == str(mode).upper())
    return len(autotrade_active)


def _at_trade_worst_case_risk_usdt(tr: dict) -> float:
    entry=float(tr.get("entry_price") or 0); stop=float(tr.get("stop_price") or 0)
    qty=abs(float(tr.get("expected_qty") or tr.get("qty") or 0))
    if entry<=0 or stop<=0 or qty<=0: return 0.0
    gross=max(0.0,(entry-stop)*qty)
    fee=(entry*qty)*AUTO_TRADE_RISK_FEE_PCT/100.0
    return gross+fee


def _at_open_worst_case_risk(scope: str) -> float:
    sc=_at_scope(scope)
    return sum(_at_trade_worst_case_risk_usdt(x) for x in autotrade_active.values()
               if str(x.get("mode")).upper()==sc and str(x.get("status")) in ("OPEN","PARTIAL","PROTECTIVE_PARTIAL"))


def _at_risk_allowed(scope: Optional[str] = None, proposed_risk_usdt: float = 0.0) -> Tuple[bool, str]:
    sc=_at_scope(scope); r = _at_daily_row(scope=sc)
    if r["locked"]: return False, r["lock_reason"] or "RISK_LOCK"
    cooldown_until=int(r.get("cooldown_until_ts") or 0)
    if cooldown_until > int(time.time()):
        mins=max(1, math.ceil((cooldown_until-time.time())/60.0)); return False, f"STOP_COOLDOWN_{mins}MIN"
    if _at_open_count(sc) >= int(autotrade_cfg["max_open_positions"]): return False, "MAX_OPEN_POSITIONS"
    limit=float(r["start_balance"])*float(autotrade_cfg["daily_max_loss_pct"])/100.0
    realized_loss=max(0.0,-float(r.get("realized_net_pnl") or 0))
    open_risk=_at_open_worst_case_risk(sc)
    projected=realized_loss+open_risk+max(0.0,float(proposed_risk_usdt or 0))
    if projected > limit+1e-9:
        return False, f"PROJECTED_DAILY_RISK {projected:.2f}>{limit:.2f} (realized_loss={realized_loss:.2f}; open={open_risk:.2f}; new={float(proposed_risk_usdt or 0):.2f})"
    return True, "OK"

def _at_quantize(value: float, step: float, rounding=ROUND_DOWN) -> float:
    if not step or step <= 0:
        return float(value)
    dv, ds = Decimal(str(value)), Decimal(str(step))
    return float((dv / ds).to_integral_value(rounding=rounding) * ds)


def _at_qty(symbol: str, notional: float, price: float) -> float:
    f = exchange_filters.get(symbol, {})
    q = float(notional) / max(float(price), 1e-12)
    q = _at_quantize(q, float(f.get("step_size") or 0), ROUND_DOWN)
    if f.get("min_qty") and q < float(f["min_qty"]):
        q = float(f["min_qty"])
    if f.get("max_qty") and q > float(f["max_qty"]):
        q = float(f["max_qty"])
    return q


def _at_price(symbol: str, price: float) -> float:
    return _at_quantize(float(price), float(exchange_filters.get(symbol, {}).get("tick_size") or 0), ROUND_HALF_UP)


def _at_levels_from_fill(symbol: str, fill: float, plan: dict) -> dict:
    base = float(plan.get("entry_mid") or ((float(plan["entry_low"])+float(plan["entry_high"]))/2.0))
    stop_pct = max(0.05, abs((float(plan["invalidation"])/base - 1.0)*100.0))
    tp1_pct = max(0.05, (float(plan["target1"])/base - 1.0)*100.0)
    tp2_pct = max(tp1_pct, (float(plan["target2"])/base - 1.0)*100.0)
    return {
        "stop": _at_price(symbol, fill*(1-stop_pct/100.0)),
        "tp1": _at_price(symbol, fill*(1+tp1_pct/100.0)),
        "tp2": _at_price(symbol, fill*(1+tp2_pct/100.0)),
        "runner": _at_price(symbol, max(fill*(1+float(autotrade_cfg["runner_target_pct"])/100.0), fill*(1+tp1_pct/100.0)*1.001)),
    }


def _at_client(tag: str, signal_id: int) -> str:
    raw = f"{AUTO_TRADE_CLIENT_PREFIX}-{tag}-{signal_id}-{int(time.time())%1000000}"
    return raw[:36]


async def binance_signed_request(session: aiohttp.ClientSession, method: str, path: str, params: Optional[dict] = None, *, timeout_s: int = 15):
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET eksik")
    data = dict(params or {})
    data.setdefault("recvWindow", 5000)
    data["timestamp"] = now_ms()
    # Binance signs the URL-encoded query/body exactly.
    qs = urlencode([(k, str(v).lower() if isinstance(v, bool) else str(v)) for k,v in data.items() if v is not None])
    sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    signed = qs + "&signature=" + sig
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY, "Content-Type":"application/x-www-form-urlencoded"}
    url = REST + path
    try:
        if method.upper() == "GET":
            req = session.get(url + "?" + signed, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s))
        elif method.upper() == "DELETE":
            req = session.delete(url + "?" + signed, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s))
        else:
            req = session.request(method.upper(), url, data=signed, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s))
        async with req as r:
            body = await r.text()
            try: payload = json.loads(body)
            except Exception: payload = {"code": r.status, "msg": body[:500]}
            if r.status >= 400 or (isinstance(payload, dict) and int(payload.get("code", 0) or 0) < 0):
                raise RuntimeError(f"Binance {method} {path}: {r.status} {payload}")
            return payload
    except asyncio.CancelledError:
        raise


async def _at_account_snapshot(session):
    cfg = await binance_signed_request(session, "GET", "/fapi/v1/accountConfig")
    bal = await binance_signed_request(session, "GET", "/fapi/v3/balance")
    usdt = next((x for x in bal if x.get("asset") == "USDT"), None) or {}
    positions = await binance_signed_request(session, "GET", "/fapi/v3/positionRisk")
    return cfg, usdt, positions


def _at_cache_account_balance(usdt: Optional[dict] = None, error: str = ""):
    if usdt is not None:
        try:
            autotrade_account_cache["wallet_balance"] = float(usdt.get("balance", 0) or 0)
            autotrade_account_cache["available_balance"] = float(usdt.get("availableBalance", 0) or 0)
            autotrade_account_cache["updated_ts"] = time.time()
            autotrade_account_cache["error"] = ""
        except Exception as e:
            autotrade_account_cache["error"] = str(e)[:250]
    elif error:
        autotrade_account_cache["error"] = str(error)[:250]


async def _at_query_order_by_client(session, symbol: str, client_id: str):
    try:
        return await binance_signed_request(session, "GET", "/fapi/v1/order", {"symbol":symbol,"origClientOrderId":client_id})
    except Exception:
        return None


async def _at_place_market_entry(session, symbol: str, qty: float, position_side: str, client_id: str):
    params = {"symbol":symbol,"side":"BUY","type":"MARKET","quantity":qty,"newClientOrderId":client_id,"newOrderRespType":"RESULT"}
    if position_side != "BOTH": params["positionSide"] = position_side
    try:
        return await binance_signed_request(session, "POST", "/fapi/v1/order", params, timeout_s=20)
    except Exception as e:
        # Never blind-retry an uncertain order. Query the unique client id first.
        q = await _at_query_order_by_client(session, symbol, client_id)
        if q and str(q.get("status")) in ("NEW","PARTIALLY_FILLED","FILLED"):
            return q
        raise e


async def _at_place_algo(session, *, symbol: str, order_type: str, trigger_price: float, position_side: str,
                         client_id: str, quantity: Optional[float] = None, close_position: bool = False):
    params = {"algoType":"CONDITIONAL","symbol":symbol,"side":"SELL","type":order_type,
              "triggerPrice":trigger_price,"workingType":"CONTRACT_PRICE","clientAlgoId":client_id,"newOrderRespType":"RESULT"}
    if position_side != "BOTH": params["positionSide"] = position_side
    if close_position:
        params["closePosition"] = "true"
    elif quantity is not None:
        params["quantity"] = quantity
        if position_side == "BOTH": params["reduceOnly"] = "true"
    return await binance_signed_request(session, "POST", "/fapi/v1/algoOrder", params)


async def _at_cancel_algo(session, algo_id):
    if not algo_id: return
    try:
        await binance_signed_request(session, "DELETE", "/fapi/v1/algoOrder", {"algoId":algo_id})
    except Exception as e:
        log.debug("AutoTrade cancel algo %s: %r", algo_id, e)


async def _at_cancel_trade_algos(session, tr: dict):
    for k in ("tp1_algo_id","tp2_algo_id","stop_algo_id"):
        await _at_cancel_algo(session, tr.get(k))


async def _at_emergency_close(session, symbol: str, qty: float, position_side: str, signal_id: int):
    params={"symbol":symbol,"side":"SELL","type":"MARKET","quantity":qty,"newClientOrderId":_at_client("EMG",signal_id),"newOrderRespType":"RESULT"}
    if position_side == "BOTH": params["reduceOnly"]="true"
    else: params["positionSide"] = position_side
    return await binance_signed_request(session,"POST","/fapi/v1/order",params,timeout_s=20)


def _at_insert_trade(signal_id: int, symbol: str, mode: str, signal_price: float, entry_price: float, qty: float, levels: dict, plan: dict,
                     *, position_side="BOTH", entry_order_id=None, entry_client_id=None) -> int:
    margin=float(autotrade_cfg["trade_margin_usdt"]); lev=int(autotrade_cfg["leverage"]); notional=margin*lev
    conn=db_connect()
    try:
        cur=conn.execute("""INSERT OR IGNORE INTO autotrade_trades
            (signal_id,symbol,mode,status,side,position_side,margin_usdt,leverage,notional_usdt,entry_signal_price,entry_price,qty,expected_qty,
             stop_price,tp1_price,tp2_price,runner_price,exit_profile,runner_fraction,runner_target_pct,entry_order_id,entry_client_id,opened_ts_ms,updated_ts)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (signal_id,symbol,mode,"OPEN","LONG",position_side,margin,lev,notional,signal_price,entry_price,qty,qty,levels["stop"],levels["tp1"],levels["tp2"],levels["runner"],
             autotrade_cfg["exit_profile"],autotrade_cfg["runner_fraction"],autotrade_cfg["runner_target_pct"],str(entry_order_id or ""),entry_client_id or "",now_ms(),int(time.time())))
        if cur.rowcount == 0:
            row=conn.execute("SELECT id FROM autotrade_trades WHERE signal_id=?",(signal_id,)).fetchone(); trade_id=int(row[0])
        else: trade_id=int(cur.lastrowid)
        conn.commit()
    finally: conn.close()
    recover_autotrade_active()
    return trade_id


def _at_update_trade(trade_id: int, **fields):
    if not fields: return
    fields["updated_ts"] = int(time.time())
    keys=list(fields); vals=[fields[k] for k in keys]
    conn=db_connect()
    try:
        conn.execute("UPDATE autotrade_trades SET "+",".join(f"{k}=?" for k in keys)+" WHERE id=?", vals+[trade_id])
        conn.commit()
    finally: conn.close()
    recover_autotrade_active()


def _at_close_trade(trade_id: int, reason: str, exit_price: float, realized_pnl: float, commission: float = 0.0):
    tr=autotrade_active.get(trade_id) or {}
    mode=str(tr.get("mode") or "DRY")
    net=float(realized_pnl or 0)-float(commission or 0)
    _at_update_trade(trade_id,status="CLOSED",closed_ts_ms=now_ms(),close_reason=reason,exit_price=exit_price,realized_pnl=realized_pnl,commission=commission,net_pnl=net)
    daily=_at_update_daily(net,reason,scope=mode)
    _at_log_event("CLOSE",trade_id=trade_id,detail=f"reason={reason}; net={net:.4f}; daily={daily['realized_net_pnl']:.4f}")
    return daily


async def autotrade_handle_premium(session, signal_id: int, symbol: str, m: dict, plan: dict):
    mode=str(autotrade_cfg.get("mode","OFF")).upper()
    if mode == "OFF": return
    if mode == "DRY":
        allowed, why = _at_risk_allowed("DRY")
        if not allowed:
            _at_log_event("ENTRY_BLOCKED",signal_id=signal_id,symbol=symbol,detail=why)
            return
    if autotrade_active_by_symbol.get(symbol):
        _at_log_event("ENTRY_BLOCKED",signal_id=signal_id,symbol=symbol,detail="BOT_POSITION_ALREADY_ACTIVE")
        return
    signal_price=float(m.get("price") or 0)
    live_ask=float(states[symbol].ask_price or signal_price)
    if signal_price and live_ask and pct_change(live_ask,signal_price) > float(autotrade_cfg["max_entry_slippage_pct"]):
        _at_log_event("ENTRY_BLOCKED",signal_id=signal_id,symbol=symbol,detail=f"SLIPPAGE {pct_change(live_ask,signal_price):.3f}%")
        return
    margin=float(autotrade_cfg["trade_margin_usdt"]); lev=int(autotrade_cfg["leverage"]); notional=margin*lev
    entry_ref=live_ask or signal_price
    if mode == "DRY":
        qty=_at_qty(symbol,notional,entry_ref)
        levels=_at_levels_from_fill(symbol,entry_ref,plan)
        proposed=max(0.0,(entry_ref-float(levels["stop"]))*qty)+(entry_ref*qty)*AUTO_TRADE_RISK_FEE_PCT/100.0
        allowed,why=_at_risk_allowed("DRY",proposed)
        if not allowed:
            _at_log_event("ENTRY_BLOCKED",signal_id=signal_id,symbol=symbol,detail=why); return
        tid=_at_insert_trade(signal_id,symbol,"DRY",signal_price,entry_ref,qty,levels,plan)
        _at_log_event("DRY_OPEN",trade_id=tid,signal_id=signal_id,symbol=symbol,detail=f"entry={entry_ref}; qty={qty}; profile={autotrade_cfg['exit_profile']}")
        await telegram_send(session, f"🟡 DRY RUN — {symbol}\n{margin:.0f} USDT × {lev}x | giriş ~{fmt_price(entry_ref)}\nStop {fmt_price(levels['stop'])} | TP2 {fmt_price(levels['tp2'])}\nGerçek emir gönderilmedi.", chat_id=TELEGRAM_ADMIN_CHAT_ID)
        return
    if mode != "LIVE": return
    if not AUTO_TRADE_LIVE_ALLOWED:
        _at_log_event("LIVE_BLOCKED",signal_id=signal_id,symbol=symbol,detail="AUTO_TRADE_LIVE_ALLOWED=0")
        return
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        _at_log_event("LIVE_BLOCKED",signal_id=signal_id,symbol=symbol,detail="API_KEY_MISSING")
        return
    cfg, usdt, positions = await _at_account_snapshot(session)
    _at_cache_account_balance(usdt)
    if not cfg.get("canTrade", False):
        raise RuntimeError("Binance Futures API canTrade=false")
    # Initialize today's risk base from actual wallet balance if this is the first live interaction of the day.
    _at_daily_row(float(usdt.get("balance",0) or 0), scope="LIVE")
    available=float(usdt.get("availableBalance",0) or 0)
    if available < margin * 1.05:
        _at_log_event("ENTRY_BLOCKED",signal_id=signal_id,symbol=symbol,detail=f"AVAILABLE_BALANCE {available:.2f}")
        return
    # Manual position isolation: do not enter a symbol that already has any non-zero position not owned by this bot.
    existing=[x for x in positions if x.get("symbol")==symbol and abs(float(x.get("positionAmt",0) or 0))>1e-12]
    if existing:
        _at_log_event("ENTRY_BLOCKED",signal_id=signal_id,symbol=symbol,detail="MANUAL_OR_EXISTING_POSITION")
        await telegram_send(session,f"⚠️ AutoTrade {symbol} girişini atladı: Binance'ta bu sembolde zaten açık pozisyon var. Manuel pozisyona dokunulmadı.",chat_id=TELEGRAM_ADMIN_CHAT_ID)
        return
    hedge=bool(cfg.get("dualSidePosition")); position_side="LONG" if hedge else "BOTH"
    # Configure only an empty symbol; manual/open symbols were blocked above.
    try:
        await binance_signed_request(session,"POST","/fapi/v1/leverage",{"symbol":symbol,"leverage":lev})
    except Exception as e:
        raise RuntimeError(f"Leverage ayarlanamadı: {e}")
    if str(autotrade_cfg["margin_type"]).upper() in ("ISOLATED","CROSSED"):
        try:
            await binance_signed_request(session,"POST","/fapi/v1/marginType",{"symbol":symbol,"marginType":str(autotrade_cfg['margin_type']).upper()})
        except Exception as e:
            # Binance -4046: no need to change margin type. Treat as benign.
            if "-4046" not in str(e): log.warning("AutoTrade marginType %s: %r",symbol,e)
    qty=_at_qty(symbol,notional,entry_ref)
    proposed_levels=_at_levels_from_fill(symbol,entry_ref,plan)
    proposed=max(0.0,(entry_ref-float(proposed_levels["stop"]))*qty)+(entry_ref*qty)*AUTO_TRADE_RISK_FEE_PCT/100.0
    allowed,why=_at_risk_allowed("LIVE",proposed)
    if not allowed:
        _at_log_event("ENTRY_BLOCKED",signal_id=signal_id,symbol=symbol,detail=why); return
    f=exchange_filters.get(symbol,{})
    if qty<=0 or (f.get("min_notional") and qty*entry_ref < float(f["min_notional"])):
        raise RuntimeError(f"Quantity/minNotional geçersiz: qty={qty}")
    entry_client=_at_client("E",signal_id)
    order=await _at_place_market_entry(session,symbol,qty,position_side,entry_client)
    exec_qty=float(order.get("executedQty") or order.get("cumQty") or qty)
    fill=float(order.get("avgPrice") or 0)
    if fill<=0:
        cq=float(order.get("cumQuote") or 0); fill=(cq/exec_qty) if cq and exec_qty else entry_ref
    levels=_at_levels_from_fill(symbol,fill,plan)
    tid=_at_insert_trade(signal_id,symbol,"LIVE",signal_price,fill,exec_qty,levels,plan,position_side=position_side,entry_order_id=order.get("orderId"),entry_client_id=entry_client)
    try:
        # Protective STOP first. If it cannot be created, flatten immediately.
        stop_client=_at_client("S",signal_id)
        stop=await _at_place_algo(session,symbol=symbol,order_type="STOP_MARKET",trigger_price=levels["stop"],position_side=position_side,client_id=stop_client,close_position=True)
        _at_update_trade(tid,stop_algo_id=str(stop.get("algoId") or ""),stop_client_id=stop_client)
    except Exception as e:
        _at_update_trade(tid,last_error=f"STOP_CREATE_FAILED {e}")
        try: await _at_emergency_close(session,symbol,exec_qty,position_side,signal_id)
        finally:
            _at_close_trade(tid,"EMERGENCY_CLOSE",fill,0,0)
        await telegram_send(session,f"🚨 {symbol} koruyucu STOP kurulamadı; pozisyon acil market emirle kapatılmaya çalışıldı. Hata: {e}",chat_id=TELEGRAM_ADMIN_CHAT_ID)
        return
    try:
        profile=str(autotrade_cfg["exit_profile"])
        if profile == "PARTIAL_RUNNER":
            runner_frac=float(autotrade_cfg["runner_fraction"])
            runner_qty=_at_quantize(exec_qty*runner_frac,float(f.get("step_size") or 0),ROUND_DOWN)
            tp1_qty=_at_quantize(exec_qty-runner_qty,float(f.get("step_size") or 0),ROUND_DOWN)
            if tp1_qty>0:
                c1=_at_client("T1",signal_id); o1=await _at_place_algo(session,symbol=symbol,order_type="TAKE_PROFIT_MARKET",trigger_price=levels["tp1"],position_side=position_side,client_id=c1,quantity=tp1_qty)
                _at_update_trade(tid,tp1_algo_id=str(o1.get("algoId") or ""),tp1_client_id=c1)
            if runner_qty>0:
                c2=_at_client("R",signal_id); o2=await _at_place_algo(session,symbol=symbol,order_type="TAKE_PROFIT_MARKET",trigger_price=levels["runner"],position_side=position_side,client_id=c2,quantity=runner_qty)
                _at_update_trade(tid,tp2_algo_id=str(o2.get("algoId") or ""),tp2_client_id=c2)
        else:
            c2=_at_client("T2",signal_id); o2=await _at_place_algo(session,symbol=symbol,order_type="TAKE_PROFIT_MARKET",trigger_price=levels["tp2"],position_side=position_side,client_id=c2,close_position=True)
            _at_update_trade(tid,tp2_algo_id=str(o2.get("algoId") or ""),tp2_client_id=c2)
    except Exception as e:
        _at_update_trade(tid,status="PROTECTIVE_PARTIAL",last_error=f"TP_CREATE_FAILED {e}")
        await telegram_send(session,f"⚠️ {symbol} pozisyonu açık ve STOP korumalı; TP emri kurulamadı. Manuel kontrol gerekli. {e}",chat_id=TELEGRAM_ADMIN_CHAT_ID)
    _at_log_event("LIVE_OPEN",trade_id=tid,signal_id=signal_id,symbol=symbol,detail=f"fill={fill}; qty={exec_qty}; lev={lev}")
    await telegram_send(session,f"🟢 LIVE AÇILDI — {symbol}\n{margin:.0f} USDT × {lev}x | fill {fmt_price(fill)}\nStop {fmt_price(levels['stop'])} | profil {autotrade_cfg['exit_profile']}\nTrade ID #{tid}",chat_id=TELEGRAM_ADMIN_CHAT_ID)


def autotrade_on_tick(symbol: str, price: float, tick_ts: float):
    ids=list(autotrade_active_by_symbol.get(symbol) or [])
    if not ids: return
    for tid in ids:
        tr=autotrade_active.get(tid)
        if not tr or tr.get("mode") != "DRY" or tr.get("status") not in ("OPEN","PARTIAL"): continue
        entry=float(tr.get("entry_price") or 0); qty=float(tr.get("qty") or 0); expected=float(tr.get("expected_qty") or qty)
        stop=float(tr.get("stop_price") or 0); tp1=float(tr.get("tp1_price") or 0); tp2=float(tr.get("tp2_price") or 0); runner=float(tr.get("runner_price") or 0)
        profile=str(tr.get("exit_profile") or "CURRENT_TP2")
        if stop and price <= stop:
            partial=float(tr.get("partial_realized_pnl") or 0)
            pnl=partial + expected*(price-entry)
            daily=_at_close_trade(tid,"STOP",price,pnl,0)
            continue
        if profile == "PARTIAL_RUNNER":
            if not int(tr.get("tp1_hit") or 0) and tp1 and price >= tp1:
                frac=1-float(tr.get("runner_fraction") or 0.5)
                close_qty=qty*frac; part=close_qty*(price-entry); remain=max(0.0,qty-close_qty)
                _at_update_trade(tid,status="PARTIAL",tp1_hit=1,partial_realized_pnl=part,expected_qty=remain)
                tr=autotrade_active.get(tid) or tr; expected=remain
            if runner and price >= runner and int((autotrade_active.get(tid) or tr).get("tp1_hit") or 0):
                tr2=autotrade_active.get(tid) or tr; part=float(tr2.get("partial_realized_pnl") or 0); remain=float(tr2.get("expected_qty") or 0)
                _at_close_trade(tid,"RUNNER",price,part+remain*(price-entry),0)
        elif tp2 and price >= tp2:
            _at_close_trade(tid,"TP2",price,qty*(price-entry),0)


async def _at_algo_state(session, algo_id):
    if not algo_id: return None
    try: return await binance_signed_request(session,"GET","/fapi/v1/algoOrder",{"algoId":algo_id})
    except Exception: return None


async def _at_order_net_pnl(session, symbol: str, order_id) -> Tuple[float,float,float]:
    if not order_id: return 0.0,0.0,0.0
    try:
        trades=await binance_signed_request(session,"GET","/fapi/v1/userTrades",{"symbol":symbol,"orderId":order_id})
        realized=sum(float(x.get("realizedPnl",0) or 0) for x in trades)
        commission=sum(float(x.get("commission",0) or 0) for x in trades if x.get("commissionAsset") in (None,"USDT"))
        q=sum(float(x.get("qty",0) or 0) for x in trades); quote=sum(float(x.get("quoteQty",0) or 0) for x in trades)
        avg=(quote/q) if q else 0.0
        return realized,commission,avg
    except Exception:
        return 0.0,0.0,0.0


async def _at_window_net_pnl(session, symbol: str, opened_ts_ms: int) -> Tuple[float,float,float]:
    try:
        trades=await binance_signed_request(session,"GET","/fapi/v1/userTrades",{"symbol":symbol,"startTime":max(0,int(opened_ts_ms)-1000),"limit":1000})
        realized=sum(float(x.get("realizedPnl",0) or 0) for x in trades)
        commission=sum(float(x.get("commission",0) or 0) for x in trades if x.get("commissionAsset") in (None,"USDT"))
        sells=[x for x in trades if str(x.get("side"))=="SELL"]
        q=sum(float(x.get("qty",0) or 0) for x in sells); quote=sum(float(x.get("quoteQty",0) or 0) for x in sells)
        return realized,commission,(quote/q if q else 0.0)
    except Exception:
        return 0.0,0.0,0.0


def _po_bot_source(symbol: str, position_side: str) -> str:
    for tr in autotrade_active.values():
        if str(tr.get("mode")).upper()=="LIVE" and str(tr.get("status")) in ("OPEN","PARTIAL","PROTECTIVE_PARTIAL") and str(tr.get("symbol"))==symbol:
            ps=str(tr.get("position_side") or "BOTH")
            if ps==position_side or ps=="BOTH" or position_side=="BOTH": return "BOT"
    return "MANUAL"


def _po_zone(direction: str, mark: float, entry: float) -> Tuple[str,float]:
    if entry<=0 or mark<=0: return "NEUTRAL",0.0
    move=pct_change(mark,entry)
    signed=move if direction=="LONG" else -move
    if signed>=POSITION_ENTRY_HYSTERESIS_PCT: return "PROFIT",signed
    if signed<=-POSITION_ENTRY_HYSTERESIS_PCT: return "LOSS",signed
    return "NEUTRAL",signed


def _po_roe(p: dict, direction: str, entry: float, mark: float, lev: int) -> Tuple[float,float]:
    upnl=float(p.get("unRealizedProfit",p.get("unrealizedProfit",0)) or 0)
    margin=float(p.get("positionInitialMargin",0) or 0)
    if margin>0: return upnl/margin*100.0,upnl
    _,signed=_po_zone(direction,mark,entry)
    return signed*max(1,lev),upnl


async def position_observer_loop(session):
    """Observe every real Binance Futures position, manual or bot-owned. Notification-only."""
    while not stop_event.is_set():
        try:
            if not POSITION_OBSERVER_ENABLED or not BINANCE_API_KEY or not BINANCE_API_SECRET:
                await asyncio.sleep(POSITION_OBSERVER_POLL_SECONDS); continue
            positions=await binance_signed_request(session,"GET","/fapi/v3/positionRisk")
            active_keys=set(); nowi=int(time.time())
            for p in positions:
                amt=float(p.get("positionAmt",0) or 0)
                if abs(amt)<=1e-12: continue
                sym=str(p.get("symbol") or ""); ps=str(p.get("positionSide") or "BOTH")
                direction="LONG" if (ps=="LONG" or (ps=="BOTH" and amt>0)) else "SHORT"
                entry=float(p.get("entryPrice",0) or 0); mark=float(p.get("markPrice",0) or states[sym].mark_price or states[sym].last_price or 0)
                lev=max(1,int(float(p.get("leverage",1) or 1))); qty=abs(amt); source=_po_bot_source(sym,ps)
                if entry<=0 or mark<=0: continue
                key=(sym,ps); active_keys.add(key); zone,signed_move=_po_zone(direction,mark,entry); roe,upnl=_po_roe(p,direction,entry,mark,lev)
                conn=db_connect()
                row=conn.execute("SELECT direction,entry_price,qty,zone,pending_zone,pending_since_ts,profit_hits_json,loss_hits_json,active FROM position_observer_state WHERE symbol=? AND position_side=?",key).fetchone()
                if row is None:
                    conn.execute("""INSERT INTO position_observer_state(symbol,position_side,direction,entry_price,qty,leverage,source,zone,pending_zone,pending_since_ts,profit_hits_json,loss_hits_json,last_roe,last_unrealized_pnl,active,updated_ts)
                                    VALUES (?,?,?,?,?,?,?,?,NULL,0,'[]','[]',?,?,1,?)""",(sym,ps,direction,entry,qty,lev,source,zone,roe,upnl,nowi)); conn.commit(); conn.close()
                    profit_hits=set(); loss_hits=set(); old_zone=zone
                else:
                    old_dir,old_entry,old_qty,old_zone,pending,pending_since,ph,lh,was_active=row
                    # Average-entry or side change establishes a new position basis; reset milestones safely.
                    reset=(not bool(was_active) or str(old_dir)!=direction or abs(float(old_entry)-entry)/entry*100.0>0.01)
                    profit_hits=set() if reset else set(json.loads(ph or '[]')); loss_hits=set() if reset else set(json.loads(lh or '[]'))
                    if reset: old_zone=zone; pending=None; pending_since=0
                    # Hysteresis + persistence for entry cross notifications.
                    if zone!="NEUTRAL" and zone!=old_zone:
                        if pending!=zone: pending=zone; pending_since=nowi
                        elif nowi-int(pending_since or 0)>=POSITION_ENTRY_CONFIRM_SECONDS:
                            old_zone=zone; pending=None; pending_since=0
                            icon="🟢" if zone=="PROFIT" else "🔴"; label="KÂR BÖLGESİNE GEÇTİ" if zone=="PROFIT" else "ZARAR BÖLGESİNE GEÇTİ"
                            await telegram_send(session,f"{icon} {sym} — {label}\n{direction} · {lev}x · {source}\nEntry: {fmt_price(entry)} | Anlık: {fmt_price(mark)}\nEntry'ye göre: {signed_move:+.2f}% | ROE: {roe:+.2f}%\nAçık PnL: {upnl:+.2f} USDT",chat_id=TELEGRAM_ADMIN_CHAT_ID)
                    elif zone==old_zone or zone=="NEUTRAL": pending=None; pending_since=0
                    # Milestones fire once per position basis. If several are crossed in one poll, send one compact message.
                    newp=[x for x in POSITION_ROE_MILESTONES if roe>=x and x not in profit_hits]
                    newl=[x for x in POSITION_ROE_MILESTONES if roe<=-x and x not in loss_hits]
                    if newp:
                        profit_hits.update(newp); top=max(newp); crossed="/".join(f"+%{x:g}" for x in newp)
                        await telegram_send(session,f"🚀 {sym} — KÂR +%{top:g}'A ULAŞTI\n{direction} · {lev}x · {source}\nEntry: {fmt_price(entry)} | Anlık: {fmt_price(mark)}\nROE: {roe:+.2f}% | Açık PnL: {upnl:+.2f} USDT\nGeçilen seviye: {crossed}",chat_id=TELEGRAM_ADMIN_CHAT_ID)
                    if newl:
                        loss_hits.update(newl); top=max(newl); crossed="/".join(f"-%{x:g}" for x in newl)
                        await telegram_send(session,f"⚠️ {sym} — ZARAR -%{top:g}'A ULAŞTI\n{direction} · {lev}x · {source}\nEntry: {fmt_price(entry)} | Anlık: {fmt_price(mark)}\nROE: {roe:+.2f}% | Açık PnL: {upnl:+.2f} USDT\nGeçilen seviye: {crossed}",chat_id=TELEGRAM_ADMIN_CHAT_ID)
                    conn.execute("""UPDATE position_observer_state SET direction=?,entry_price=?,qty=?,leverage=?,source=?,zone=?,pending_zone=?,pending_since_ts=?,profit_hits_json=?,loss_hits_json=?,last_roe=?,last_unrealized_pnl=?,active=1,updated_ts=? WHERE symbol=? AND position_side=?""",
                        (direction,entry,qty,lev,source,old_zone,pending,int(pending_since or 0),json.dumps(sorted(profit_hits)),json.dumps(sorted(loss_hits)),roe,upnl,nowi,sym,ps)); conn.commit(); conn.close()
            conn=db_connect()
            rows=conn.execute("SELECT symbol,position_side FROM position_observer_state WHERE active=1").fetchall()
            for k in rows:
                if tuple(k) not in active_keys: conn.execute("UPDATE position_observer_state SET active=0,updated_ts=? WHERE symbol=? AND position_side=?",(nowi,k[0],k[1]))
            conn.commit(); conn.close()
            await asyncio.sleep(POSITION_OBSERVER_POLL_SECONDS)
        except asyncio.CancelledError: raise
        except Exception as e:
            log.warning("Position observer: %r",e); await asyncio.sleep(max(5,POSITION_OBSERVER_POLL_SECONDS))


async def autotrade_reconcile_loop(session):
    last_daily = ""
    while not stop_event.is_set():
        try:
            # Daily risk row exists even in DRY mode. Existing LIVE trades are managed regardless of current mode.
            if _at_local_date() != last_daily:
                _at_daily_row(scope="DRY"); last_daily=_at_local_date()
            live=[x for x in autotrade_active.values() if x.get("mode")=="LIVE" and x.get("status") in ("OPEN","PARTIAL","PROTECTIVE_PARTIAL")]
            # Keep a fresh informational Futures-balance snapshot for /settings even while AutoTrade is OFF/DRY.
            # This does not enable trading and does not alter the LIVE daily risk base.
            if BINANCE_API_KEY and BINANCE_API_SECRET and not live and (time.time()-float(autotrade_account_cache.get("updated_ts") or 0) >= 30):
                try:
                    _, usdt_cache, _ = await _at_account_snapshot(session)
                    _at_cache_account_balance(usdt_cache)
                except Exception as e:
                    _at_cache_account_balance(error=str(e))
            if live and BINANCE_API_KEY and BINANCE_API_SECRET:
                cfg, usdt, positions = await _at_account_snapshot(session)
                _at_cache_account_balance(usdt)
                posmap=defaultdict(float)
                for p in positions:
                    sym=str(p.get("symbol") or ""); ps=str(p.get("positionSide") or "BOTH"); amt=float(p.get("positionAmt",0) or 0)
                    if abs(amt)>1e-12: posmap[(sym,ps)] += amt
                for tr in list(live):
                    tid=int(tr["id"]); sym=str(tr["symbol"]); ps=str(tr.get("position_side") or "BOTH")
                    actual=abs(float(posmap.get((sym,ps),0.0)))
                    expected=abs(float(tr.get("expected_qty") or tr.get("qty") or 0))
                    if actual <= 1e-12:
                        stopst=await _at_algo_state(session,tr.get("stop_algo_id")); tp2st=await _at_algo_state(session,tr.get("tp2_algo_id")); tp1st=await _at_algo_state(session,tr.get("tp1_algo_id"))
                        reason="MANUAL_CLOSE"; exit_order=None
                        for name,st in (("STOP",stopst),("RUNNER" if tr.get("exit_profile")=="PARTIAL_RUNNER" else "TP2",tp2st),("TP1",tp1st)):
                            if st and int(st.get("triggerTime",0) or 0)>0 and st.get("actualOrderId"):
                                reason=name; exit_order=st.get("actualOrderId")
                                if name != "TP1": break
                        if reason=="MANUAL_CLOSE":
                            realized,comm,exit_px=await _at_window_net_pnl(session,sym,int(tr.get("opened_ts_ms") or 0))
                        else:
                            realized,comm,exit_px=await _at_order_net_pnl(session,sym,exit_order)
                            _,entry_comm,_=await _at_order_net_pnl(session,sym,tr.get("entry_order_id")); comm += entry_comm
                            realized += float(tr.get("partial_realized_pnl") or 0); comm += float(tr.get("commission") or 0)
                        await _at_cancel_trade_algos(session,tr)
                        daily=_at_close_trade(tid,reason,exit_px,realized,comm)
                        await telegram_send(session,f"{'🛑' if reason=='STOP' else '✅'} AutoTrade kapandı — {sym} | {reason}\nNet P/L: {realized-comm:+.2f} USDT\nGünlük: {daily['realized_net_pnl']:+.2f} USDT | stop serisi {daily['consecutive_stops']}/{autotrade_cfg['max_consecutive_stops']}",chat_id=TELEGRAM_ADMIN_CHAT_ID)
                        continue
                    tol=max(float(exchange_filters.get(sym,{}).get("step_size") or 0)*1.5, expected*0.01)
                    if expected and abs(actual-expected)>tol:
                        # First see if the bot's TP1 explains the quantity change.
                        tp1st=await _at_algo_state(session,tr.get("tp1_algo_id")) if tr.get("tp1_algo_id") and not int(tr.get("tp1_hit") or 0) else None
                        if tp1st and int(tp1st.get("triggerTime",0) or 0)>0 and actual < expected:
                            part_pnl,part_comm,_=await _at_order_net_pnl(session,sym,tp1st.get("actualOrderId"))
                            _at_update_trade(tid,status="PARTIAL",tp1_hit=1,expected_qty=actual,partial_realized_pnl=part_pnl,commission=part_comm)
                        else:
                            # Same-symbol manual edits cannot be safely separated in One-Way mode. Stop managing and cancel only BOT-owned algos.
                            await _at_cancel_trade_algos(session,tr)
                            _at_update_trade(tid,status="MANUAL_INTERVENTION",manual_intervention=1,expected_qty=actual,last_error=f"qty expected={expected} actual={actual}")
                            _at_log_event("MANUAL_INTERVENTION",trade_id=tid,symbol=sym,detail=f"expected={expected}; actual={actual}")
                            await telegram_send(session,f"🚨 MANUAL INTERVENTION — {sym}\nPozisyon miktarı bot kaydından farklı. Bot kendi TP/SL emirlerini iptal etti ve bu birleşmiş pozisyonu artık yönetmeyecek. Binance'tan manuel yönetmen gerekiyor.",chat_id=TELEGRAM_ADMIN_CHAT_ID)
            await asyncio.sleep(AUTO_TRADE_RECONCILE_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("AutoTrade reconcile: %r",e)
            await asyncio.sleep(max(5,AUTO_TRADE_RECONCILE_SECONDS))


def _at_panel_text() -> str:
    scope="LIVE" if str(autotrade_cfg.get("mode"))=="LIVE" else "DRY"
    r=_at_daily_row(scope=scope)
    lim=r["start_balance"]*float(autotrade_cfg["daily_max_loss_pct"])/100
    # Distance from current realized P/L to the daily lock threshold (-limit).
    # Example: limit 60 and P/L -14.20 => 45.80 USDT remaining.
    remaining=max(0.0, lim + float(r["realized_net_pnl"])) if not r["locked"] else 0.0
    live_ready=bool(AUTO_TRADE_LIVE_ALLOWED and BINANCE_API_KEY and BINANCE_API_SECRET and TELEGRAM_ADMIN_USER_ID)
    wallet=autotrade_account_cache.get("wallet_balance")
    available=autotrade_account_cache.get("available_balance")
    if wallet is None:
        balance_line = "💵 Futures bakiye: API bağlı değil" if not (BINANCE_API_KEY and BINANCE_API_SECRET) else "💵 Futures bakiye: bağlantı bekleniyor"
    else:
        age=max(0,int(time.time()-float(autotrade_account_cache.get("updated_ts") or time.time())))
        balance_line = f"💵 Futures bakiye: {float(wallet):,.2f} USDT"
        if available is not None:
            balance_line += f" | serbest {float(available):,.2f}"
        balance_line += f" ({age} sn)"
    return (
        "⚙️ AUTOTRADE KONTROL PANELİ\n\n"
        f"Mod: {autotrade_cfg['mode']}\n"
        f"{balance_line}\n"
        f"İşlem: {float(autotrade_cfg['trade_margin_usdt']):.0f} USDT × {int(autotrade_cfg['leverage'])}x = {float(autotrade_cfg['trade_margin_usdt'])*int(autotrade_cfg['leverage']):,.0f} USDT notional\n"
        f"Max açık pozisyon: {int(autotrade_cfg['max_open_positions'])}\n"
        f"Günlük zarar: %{float(autotrade_cfg['daily_max_loss_pct']):.2f} (~{lim:.2f} USDT)\n"
        f"📉 Günlük P/L ({r['scope']}): {r['realized_net_pnl']:+.2f} USDT\n"
        f"🛡 Günlük limite kalan: {remaining:.2f} USDT\n"
        f"Ardışık stop: {r['consecutive_stops']}/{int(autotrade_cfg['max_consecutive_stops'])} → {int(autotrade_cfg['stop_cooldown_minutes'])} dk cooldown\n"
        f"Risk kilidi: {'AÇIK — '+r['lock_reason'] if r['locked'] else 'kapalı'}\n"
        f"Cooldown: {max(0, math.ceil((int(r.get('cooldown_until_ts') or 0)-time.time())/60))} dk\n"
        f"Çıkış profili: {autotrade_cfg['exit_profile']}\n"
        f"LIVE altyapı kilidi: {'hazır' if live_ready else 'kilitli'}\n\n"
        "Ayar değişiklikleri yalnız yeni işlemleri etkiler. Deploy/restart sonrası LIVE otomatik olarak OFF olur."
    )


def _at_panel_markup():
    return {"inline_keyboard":[
        [{"text":f"💰 {float(autotrade_cfg['trade_margin_usdt']):.0f} USDT","callback_data":"at:size"},{"text":f"⚡ {int(autotrade_cfg['leverage'])}x","callback_data":"at:lev"}],
        [{"text":f"📊 Max {int(autotrade_cfg['max_open_positions'])}","callback_data":"at:maxpos"},{"text":f"🛡 Günlük %{float(autotrade_cfg['daily_max_loss_pct']):g}","callback_data":"at:dloss"}],
        [{"text":f"🧱 Stop serisi {int(autotrade_cfg['max_consecutive_stops'])}","callback_data":"at:streak"},{"text":"📈 Pozisyonlar","callback_data":"at:positions"}],
        [{"text":"⚪ OFF","callback_data":"at:mode:OFF"},{"text":"🟡 DRY RUN","callback_data":"at:mode:DRY"},{"text":"🔴 LIVE","callback_data":"at:mode:LIVE"}],
        [{"text":"🔄 Yenile","callback_data":"at:panel"}],
    ]}


def _at_choice_markup(kind: str, values: list):
    rows=[]
    for i in range(0,len(values),3):
        rows.append([{"text":str(v),"callback_data":f"at:set:{kind}:{v}"} for v in values[i:i+3]])
    rows.append([{"text":"⬅️ Geri","callback_data":"at:panel"}])
    return {"inline_keyboard":rows}


async def _at_show_positions(session, chat_id: str):
    rows=list(autotrade_active.values())
    if not rows:
        await telegram_send(session,"📈 Botun yönettiği açık pozisyon yok. Manuel Binance pozisyonları bu listeye dahil edilmez.",chat_id=chat_id); return
    lines=["📈 BOT POZİSYONLARI\n"]
    for tr in rows:
        lines.append(f"#{tr['id']} {tr['symbol']} | {tr['mode']} {tr['status']} | {float(tr['margin_usdt']):.0f}×{int(tr['leverage'])} | entry {fmt_price(float(tr.get('entry_price') or 0))}")
    await telegram_send(session,"\n".join(lines),chat_id=chat_id)


async def handle_autotrade_callback(session: aiohttp.ClientSession, cb: dict):
    data=str(cb.get("data") or "")
    if not data.startswith("at:"): return False
    callback_id=cb.get("id"); actor=cb.get("from") or {}; uid=str(actor.get("id", "")); msg=cb.get("message") or {}; chat_id=str((msg.get("chat") or {}).get("id", ""))
    if not _at_admin_allowed(chat_id,uid):
        if callback_id: await telegram_api_call(session,"answerCallbackQuery",{"callback_query_id":callback_id,"text":"Bu işlem için yetkin yok.","show_alert":True})
        return True
    parts=data.split(":")
    action=parts[1] if len(parts)>1 else ""
    if action=="panel":
        await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":_at_panel_text(),"reply_markup":_at_panel_markup()})
    elif action=="size":
        await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":"💰 Yeni işlem başına marjin seç:","reply_markup":_at_choice_markup("trade_margin_usdt",[100,150,200,250,300,400,500])})
    elif action=="lev":
        await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":"⚡ Yeni kaldıraç seç:","reply_markup":_at_choice_markup("leverage",[3,5,7,10,15,20])})
    elif action=="maxpos":
        await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":"📊 Maksimum açık bot pozisyonu:","reply_markup":_at_choice_markup("max_open_positions",[1,2,3,4,5,6])})
    elif action=="dloss":
        await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":"🛡 Günlük maksimum net zarar (%):","reply_markup":_at_choice_markup("daily_max_loss_pct",[1,2,2.5,3,4,5])})
    elif action=="streak":
        await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":"🧱 Ardışık stop limiti:","reply_markup":_at_choice_markup("max_consecutive_stops",[2,3,4,5,6])})
    elif action=="positions":
        await _at_show_positions(session,chat_id)
    elif action=="mode" and len(parts)>=3:
        target=parts[2].upper()
        if target=="LIVE":
            if not _at_admin_allowed(chat_id,uid,require_user_id=True):
                await telegram_api_call(session,"answerCallbackQuery",{"callback_query_id":callback_id,"text":"LIVE için TELEGRAM_ADMIN_USER_ID gerekli.","show_alert":True}); return True
            code=f"{secrets.randbelow(1000000):06d}"; autotrade_live_confirm[uid]=(code,time.time()+120)
            await telegram_send(session,f"🔴 LIVE aktivasyon isteği\n\nKod: {code}\n120 saniye içinde şu komutu yaz:\n/autotrade confirm {code}\n\nRailway'de AUTO_TRADE_LIVE_ALLOWED=1 ve Binance API anahtarları yoksa LIVE yine açılmaz.",chat_id=chat_id)
        else:
            autotrade_cfg["mode"] = target if target in ("OFF","DRY") else "OFF"; _at_save_setting("mode",autotrade_cfg["mode"])
            await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":_at_panel_text(),"reply_markup":_at_panel_markup()})
    elif action=="set" and len(parts)>=4:
        key,val=parts[2],parts[3]
        token=secrets.token_hex(3); autotrade_pending_setting[token]=(key,val,time.time()+120)
        await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":f"⚠️ Ayar değişikliği\n{key}: {autotrade_cfg.get(key)} → {val}\n\nYalnız yeni işlemler etkilenecek.","reply_markup":{"inline_keyboard":[[{"text":"✅ Onayla","callback_data":f"at:confirm:{token}"},{"text":"❌ Vazgeç","callback_data":"at:panel"}]]}})
    elif action=="confirm" and len(parts)>=3:
        token=parts[2]; pending=autotrade_pending_setting.pop(token,None)
        if pending and pending[2]>=time.time():
            key,val,_=pending
            conv=float if key in ("trade_margin_usdt","daily_max_loss_pct") else int
            autotrade_cfg[key]=conv(val); _at_save_setting(key,autotrade_cfg[key])
        await telegram_api_call(session,"editMessageText",{"chat_id":chat_id,"message_id":msg.get("message_id"),"text":_at_panel_text(),"reply_markup":_at_panel_markup()})
    if callback_id:
        await telegram_api_call(session,"answerCallbackQuery",{"callback_query_id":callback_id})
    return True


async def _at_try_live_enable(session, chat_id: str, user_id: str, code: str) -> str:
    if not _at_admin_allowed(chat_id,user_id,require_user_id=True): return "❌ LIVE için yetkili kullanıcı ID'si eşleşmiyor."
    p=autotrade_live_confirm.pop(user_id,None)
    if not p or p[1]<time.time() or p[0]!=code: return "❌ LIVE onay kodu geçersiz veya süresi dolmuş."
    if not AUTO_TRADE_LIVE_ALLOWED: return "❌ Railway'de AUTO_TRADE_LIVE_ALLOWED=1 değil. LIVE kilitli."
    if not BINANCE_API_KEY or not BINANCE_API_SECRET: return "❌ Binance API Key/Secret Railway Variables içinde yok."
    allowed,why=_at_risk_allowed("LIVE")
    if not allowed: return f"❌ Risk kilidi nedeniyle LIVE açılamadı: {why}"
    try:
        cfg,usdt,positions=await _at_account_snapshot(session)
        _at_cache_account_balance(usdt)
        if not cfg.get("canTrade",False): return "❌ Binance API canTrade=false. Futures trading izni açık değil."
        _at_daily_row(float(usdt.get("balance",0) or 0), scope="LIVE")
    except Exception as e: return f"❌ Binance bağlantı testi başarısız: {e}"
    autotrade_cfg["mode"]="LIVE"; _at_save_setting("mode","LIVE")
    return "🔴 AUTOTRADE LIVE AÇILDI. Yeni uygun Premiumlar gerçek Futures emrine dönüşebilir. Deploy/restart olursa tekrar OFF'a döner."


async def _at_command(session, raw_text: str, chat_id: str, user_id: str) -> bool:
    text=raw_text.strip(); low=text.lower()
    if low in ("/settings","/tradesettings"):
        if not _at_admin_allowed(chat_id,user_id): return True
        await telegram_send(session,_at_panel_text(),chat_id=chat_id,reply_markup=_at_panel_markup()); return True
    if low=="/riskstatus":
        if not _at_admin_allowed(chat_id,user_id): return True
        await telegram_send(session,_at_panel_text(),chat_id=chat_id); return True
    if low=="/positions":
        if not _at_admin_allowed(chat_id,user_id): return True
        await _at_show_positions(session,chat_id); return True
    if low.startswith("/autotrade"):
        if not _at_admin_allowed(chat_id,user_id): return True
        parts=text.split()
        if len(parts)==1:
            await telegram_send(session,_at_panel_text(),chat_id=chat_id,reply_markup=_at_panel_markup()); return True
        cmd=parts[1].lower()
        if cmd in ("off","dry"):
            autotrade_cfg["mode"]="OFF" if cmd=="off" else "DRY"; _at_save_setting("mode",autotrade_cfg["mode"])
            await telegram_send(session,f"✅ AutoTrade modu: {autotrade_cfg['mode']}. Açık LIVE pozisyonların koruma/yönetimi varsa devam eder.",chat_id=chat_id); return True
        if cmd=="live":
            if not _at_admin_allowed(chat_id,user_id,require_user_id=True):
                await telegram_send(session,"❌ LIVE aktivasyonu için Railway'de TELEGRAM_ADMIN_USER_ID tanımlı olmalı.",chat_id=chat_id); return True
            code=f"{secrets.randbelow(1000000):06d}"; autotrade_live_confirm[user_id]=(code,time.time()+120)
            await telegram_send(session,f"🔴 LIVE onay kodu: {code}\n120 sn içinde /autotrade confirm {code}",chat_id=chat_id); return True
        if cmd=="confirm" and len(parts)>=3:
            await telegram_send(session,await _at_try_live_enable(session,chat_id,user_id,parts[2]),chat_id=chat_id); return True
        return True
    mapping={"/tradesize":"trade_margin_usdt","/leverage":"leverage","/maxpositions":"max_open_positions","/dailyloss":"daily_max_loss_pct","/stopstreak":"max_consecutive_stops"}
    first=low.split()[0] if low else ""
    if first in mapping:
        if not _at_admin_allowed(chat_id,user_id): return True
        parts=text.split()
        if len(parts)<2:
            await telegram_send(session,f"Kullanım: {first} DEĞER",chat_id=chat_id); return True
        key=mapping[first]
        try: val=float(parts[1]) if key in ("trade_margin_usdt","daily_max_loss_pct") else int(parts[1])
        except Exception:
            await telegram_send(session,"❌ Geçersiz değer.",chat_id=chat_id); return True
        bounds={"trade_margin_usdt":(5,100000),"leverage":(1,125),"max_open_positions":(1,20),"daily_max_loss_pct":(0.25,25),"max_consecutive_stops":(1,20)}
        lo,hi=bounds[key]
        if not (lo<=val<=hi):
            await telegram_send(session,f"❌ Değer {lo}–{hi} aralığında olmalı.",chat_id=chat_id); return True
        token=secrets.token_hex(3); autotrade_pending_setting[token]=(key,str(val),time.time()+120)
        markup={"inline_keyboard":[[{"text":"✅ Onayla","callback_data":f"at:confirm:{token}"},{"text":"❌ Vazgeç","callback_data":"at:panel"}]]}
        await telegram_send(session,f"⚠️ {key}: {autotrade_cfg.get(key)} → {val}\nYalnız yeni işlemler etkilenecek.",chat_id=chat_id,reply_markup=markup); return True
    if low.startswith("/runner"):
        if not _at_admin_allowed(chat_id,user_id): return True
        parts=text.split()
        if len(parts)>=2 and parts[1].lower() in ("off","current"):
            autotrade_cfg["exit_profile"]="CURRENT_TP2"; _at_save_setting("exit_profile","CURRENT_TP2")
            await telegram_send(session,"✅ Çıkış profili CURRENT_TP2. Yalnız yeni işlemler etkilenir.",chat_id=chat_id); return True
        if len(parts)>=3:
            try: tp1_pct=float(parts[1]); target=float(parts[2])
            except Exception:
                await telegram_send(session,"Kullanım: /runner 50 5  (TP1'de %50 kapat, kalan +%5 runner) veya /runner off",chat_id=chat_id); return True
            if not (5<=tp1_pct<=95 and 0.25<=target<=25):
                await telegram_send(session,"❌ TP1 payı %5–95, runner hedefi %0.25–25 olmalı.",chat_id=chat_id); return True
            autotrade_cfg["exit_profile"]="PARTIAL_RUNNER"; autotrade_cfg["runner_fraction"]=(100-tp1_pct)/100.0; autotrade_cfg["runner_target_pct"]=target
            for k in ("exit_profile","runner_fraction","runner_target_pct"): _at_save_setting(k,autotrade_cfg[k])
            await telegram_send(session,f"✅ Yeni işlemler: TP1'de %{tp1_pct:g} kapat + kalan %{100-tp1_pct:g} runner → +%{target:g}.",chat_id=chat_id); return True
        await telegram_send(session,f"Runner profili: {autotrade_cfg['exit_profile']} | kalan pay %{float(autotrade_cfg['runner_fraction'])*100:.0f} | hedef +%{float(autotrade_cfg['runner_target_pct']):g}",chat_id=chat_id); return True
    return False

# ============================ END V5.13 AUTOTRADE SAFE EXECUTION ============================


async def telegram_command_loop(session):
    global telegram_offset
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    while not stop_event.is_set():
        try:
            params = {"timeout": 20, "offset": telegram_offset, "allowed_updates": json.dumps(["message","chat_join_request","callback_query"])}
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
                if upd.get("chat_join_request"):
                    await handle_join_request(session, upd["chat_join_request"])
                    continue
                if upd.get("callback_query"):
                    handled = await handle_join_callback(session, upd["callback_query"])
                    if not handled:
                        handled = await handle_autotrade_callback(session, upd["callback_query"])
                    if handled:
                        continue
                msg = upd.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id not in {str(TELEGRAM_CHAT_ID), str(TELEGRAM_ADMIN_CHAT_ID)}:
                    continue
                user_id = str((msg.get("from") or {}).get("id", ""))
                raw_text = str(msg.get("text", "")).strip()
                text = raw_text.lower()
                if text == "/myid":
                    await telegram_send(
                        session,
                        f"🆔 Telegram User ID: {user_id}\n💬 Chat ID: {chat_id}",
                        chat_id=chat_id,
                    )
                    continue
                if await _at_command(session, raw_text, chat_id, user_id):
                    continue
                if text == "/status":
                    age = lambda k: (time.time() - stream_health[k]) if stream_health[k] else 9999
                    agg_ages = [(time.time() - t) for t in agg_stream_health.values() if t]
                    agg_oldest = max(agg_ages) if agg_ages else 9999
                    agg_stale = sum(1 for a in agg_ages if a >= 90)
                    expected_chunks = max(1, (len(symbols) + AGGTRADE_CHUNK - 1) // AGGTRADE_CHUNK)
                    healthy = age("ticker") < 90 and age("book") < 90 and len(agg_ages) >= expected_chunks and agg_stale == 0
                    await telegram_send(session,
                        f"{'✅' if healthy else '⚠️'} Scanner durumu — V{BOT_VERSION}\n\n"
                        f"🪙 Kontrat: {len(symbols)}\n"
                        f"💵 Min 24s hacim: {fmt_money(MIN_24H_QUOTE_VOLUME)} USDT\n"
                        f"⚡ AggTrade olayları: {trade_event_count:,}\n"
                        f"🚨 Son 24s sinyal: {signal_count_today()}\n"
                        f"📡 ticker: {age('ticker'):.0f} sn | book: {age('book'):.0f} sn | agg: {age('agg'):.0f} sn | mark: {age('mark'):.0f} sn\n"
                        f"🧩 AggTrade chunk: {len(agg_ages)}/{expected_chunks} | en eski: {agg_oldest:.0f} sn | stale: {agg_stale}\n"
                        f"⭐ Aday eşiği: {EARLY_SCORE}+\n"
                        f"🎯 Teyit: {CONFIRM_REQUIRED} × {CONFIRM_INTERVAL_SECONDS} sn | premium momentum {PREMIUM_MIN_MOMENTUM_SCORE}+ | giriş {PREMIUM_ENTRY_MIN_SCORE}+ | yükseliş {PREMIUM_RISE_MIN_SCORE}+\n"
                        f"👀 Erken bildirim: 2/3 süreklilik + skor {EARLY_NOTIFY_MIN_SCORE}+ (production eşikleri aynı)\n"
                        f"🧪 Shadow Exit: {'açık' if SHADOW_EXIT_ENABLED else 'kapalı'} | Telegram: {'açık' if SHADOW_EXIT_NOTIFY else 'kapalı'} | sadece test\n"
                        f"🔬 Second-wave / Pre-Breakout research: {'açık' if RESEARCH_ENABLED else 'kapalı'}\n"
                        f"🧭 1–60sn micro snapshot + 15sn entry acceptance: AÇIK (Shadow; Premium filtresini değiştirmez)\n"
                        f"🔁 Stop sonrası reclaim: {'açık' if RECLAIM_SHADOW_ENABLED else 'kapalı'} | gözlem ≤{RECLAIM_MAX_AGE_SECONDS//60}dk | TP2 runner: {'açık' if RUNNER_SHADOW_ENABLED else 'kapalı'} (Shadow)\n"
                        f"⚡ Ignition V2/Near-Miss audit: {'açık' if IGNITION_SHADOW_ENABLED and NEAR_MISS_ENABLED and IGNITION_V2_ENABLED else 'kısmi/kapalı'} | V2 eşik {IGNITION_V2_MIN_SCORE}/{FAST_EARLY_V2_SCORE} | audit hacim tabanı {fmt_money(NEAR_MISS_MIN_QV24)}\n"
                        f"🧱 Liquidity depth: {'açık' if LIQUIDITY_RESEARCH_ENABLED else 'kapalı'} | {LIQUIDITY_DEPTH_LIMIT} seviye | 0/5/15/30sn (Shadow)\n"
                        f"🌊 Dynamic liquidity V1: {'açık' if LIQUIDITY_EVOLUTION_ENABLED else 'kapalı'} | SUPPORT ≥{LIQ_EVOLUTION_SUPPORT_SCORE} / HOSTILE ≤{LIQ_EVOLUTION_HOSTILE_SCORE} (Shadow)\n"
                        f"🧠 Liquidity Regime V2: {'açık' if LIQUIDITY_V2_ENABLED else 'kapalı'} | SUPPORT ≥{LIQ_V2_SUPPORT_SCORE} / HOSTILE ≤{LIQ_V2_HOSTILE_SCORE} (Shadow)\n"
                        f"🧪 15sn Execution Gate Simulator: {'açık' if EXECUTION_GATE_SHADOW_ENABLED else 'kapalı'} | PRODUCTION GATE KAPALI | pre-gate TP/stop bias guard açık\n"
                        f"🚦 Post-Premium Failure Risk: {'açık' if POST_PREMIUM_RISK_ENABLED else 'kapalı'} | 15/30sn liquidity + 60sn progress | PUBLIC ALERT YOK\n"
                        f"🧭 60sn Acceptance+Progress + composite: AÇIK (Shadow)\n"
                        f"🧪 Execution Gate V2 Quality: {'açık' if EXECUTION_GATE_V2_SHADOW_ENABLED else 'kapalı'} | Progress+Composite+Sticky+Over60 | PRODUCTION KAPALI\n"                        f"🧪 Gate V2.1 15/30sn: {'açık' if EXECUTION_GATE_V21_SHADOW_ENABLED else 'kapalı'} | PASS+POSITIVE+SUPPORTIVE / NEGATIVE+HOSTILE | SHADOW\n"
                        f"🧪 Forward strateji audit: {'açık' if FORWARD_STRATEGY_SHADOW_ENABLED else 'kapalı'} | TP1→BE + TP2→%50/+%{FORWARD_RUNNER_TARGET_PCT:g} + WAIT_RECLAIM + Gate V2.1 gecikmeli fill | SHADOW\n"
                        f"🧪 V5.13.4 stage audit + REAL EARLY + Liquidity/OI Transition V3: {'açık' if STAGE_ENTRY_FORWARD_ENABLED else 'kapalı'} | SHADOW\n"
                        f"👁 Pozisyon gözlemcisi: tüm gerçek Futures pozisyonları | entry-cross + ROE %5/%10/%20\n"
                        f"🤖 AutoTrade: {autotrade_cfg['mode']} | {float(autotrade_cfg['trade_margin_usdt']):.0f} USDT × {int(autotrade_cfg['leverage'])}x | max {int(autotrade_cfg['max_open_positions'])} | günlük %{float(autotrade_cfg['daily_max_loss_pct']):g} HARD | {int(autotrade_cfg['max_consecutive_stops'])} stop→{int(autotrade_cfg['stop_cooldown_minutes'])}dk cooldown | LIVE kilidi {'AÇIK' if AUTO_TRADE_LIVE_ALLOWED else 'KAPALI'}\n"
                        f"👥 Kanal katılım onayı: {'açık' if JOIN_REQUEST_APPROVAL_ENABLED else 'kapalı/kanal ID yok'}\n"
                        f"📣 Abone kanal yayını: {'açık' if TELEGRAM_BROADCAST_ENABLED else 'kapalı'} | Early + Premium + Continuation\n"
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
                        SUM(CASE WHEN entry_touch_s IS NOT NULL AND tp1_hit_s IS NOT NULL AND (invalidation_hit_s IS NULL OR tp1_hit_s<invalidation_hit_s) THEN 1 ELSE 0 END),
                        SUM(CASE WHEN entry_touch_s IS NOT NULL AND invalidation_hit_s IS NOT NULL AND (tp1_hit_s IS NULL OR invalidation_hit_s<tp1_hit_s) THEN 1 ELSE 0 END),
                        SUM(CASE WHEN entry_touch_s IS NOT NULL AND tp2_hit_s IS NOT NULL THEN 1 ELSE 0 END),
                        SUM(CASE WHEN entry_touch_s IS NOT NULL AND tp2_hit_s IS NOT NULL AND (invalidation_hit_s IS NULL OR tp2_hit_s<invalidation_hit_s) THEN 1 ELSE 0 END),
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
                        total_paths = path[7] or 0
                        if total_paths:
                            msg += f"\n\n🧭 V{BOT_VERSION} İŞLEM YOLU ({total_paths})\nAlım bölgesi temas etti: %{100*pn/total_paths:.1f}\nHedefe alım bölgesi gelmeden kaçtı: {int(path[6] or 0)}"
                        if pn:
                            msg += (f"\nTP1, geçersizlikten önce: %{100*(path[1] or 0)/pn:.1f}\nGeçersizlik TP1'den önce: %{100*(path[2] or 0)/pn:.1f}\nTP2 herhangi zamanda: %{100*(path[3] or 0)/pn:.1f}\nTP2, geçersizlikten önce: %{100*(path[4] or 0)/pn:.1f}\nTP1'e kadar ort. ters hareket: {(path[5] or 0):+.2f}%")
                        msg += "\n\nNot: MFE tek başına başarı sayılmaz; asıl executable metrik TP/invalidasyon sırasıdır."
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
                        f"🧪 V{BOT_VERSION} SHADOW / DALGA İSTATİSTİĞİ\n\n"
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
                elif text == "/latencystats":
                    conn = db_connect()
                    lat = conn.execute(
                        """SELECT COUNT(*),
                           AVG(n.send_start_ts_ms-c.signal_generated_ts_ms),
                           AVG(n.send_done_ts_ms-n.send_start_ts_ms),
                           AVG(n.price_drift_pct),MAX(n.price_drift_pct)
                           FROM premium_context c JOIN notification_log n ON n.signal_id=c.signal_id AND n.kind='PREMIUM'
                           WHERE n.send_start_ts_ms IS NOT NULL"""
                    ).fetchone()
                    snap = conn.execute(
                        """SELECT horizon_ms,COUNT(*),AVG(return_pct),AVG(mfe_pct),AVG(mae_pct)
                           FROM premium_micro_snapshots GROUP BY horizon_ms ORDER BY horizon_ms"""
                    ).fetchall()
                    conn.close()
                    n = int((lat[0] if lat else 0) or 0)
                    lines = [f"⏱ V{BOT_VERSION} TELEGRAM / EXECUTION LATENCY", ""]
                    if n:
                        lines.append(f"Premium ölçümü: n={n}")
                        lines.append(f"Signal→send-start ort.: {((lat[1] or 0)/1000):.3f} sn")
                        lines.append(f"Telegram HTTP send ort.: {((lat[2] or 0)/1000):.3f} sn")
                        lines.append(f"Send-start ask drift ort.: {(lat[3] or 0):+.3f}% | max {(lat[4] or 0):+.3f}%")
                    else:
                        lines.append(f"Henüz yeni V{BOT_VERSION} Premium delivery ölçümü yok.")
                    if snap:
                        lines.append("\nPremium sonrası micro path:")
                        for h,cnt,ret,mfe,mae in snap:
                            lines.append(f"• {h/1000:g}s n={cnt}: ret {(ret or 0):+.2f}% | MFE {(mfe or 0):+.2f}% | MAE {(mae or 0):+.2f}%")
                    lines.append("\nBu ekran gerçek Telegram/market gözlemleridir; fill/slippage garantisi değildir.")
                    await telegram_send(session, "\n".join(lines))
                elif text == "/entrystats":
                    conn = db_connect()
                    rows = conn.execute(
                        """SELECT v.status,COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           AVG(v.time_above_ratio),AVG(v.max_pullback_peak_pct)
                           FROM premium_entry_validation v JOIN signal_paths p ON p.signal_id=v.signal_id
                           GROUP BY v.status ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    oi_rows = conn.execute(
                        """SELECT COALESCE(c.oi_regime,'UNKNOWN'),COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_context c JOIN signal_paths p ON p.signal_id=c.signal_id
                           GROUP BY COALESCE(c.oi_regime,'UNKNOWN') ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    phase_rows = conn.execute(
                        """SELECT COALESCE(c.phase_risk,'UNKNOWN'),COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_context c JOIN signal_paths p ON p.signal_id=c.signal_id
                           GROUP BY COALESCE(c.phase_risk,'UNKNOWN') ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    exec_rows = conn.execute(
                        """SELECT COALESCE(c.execution_status,'UNKNOWN'),COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           AVG(c.signal_to_ask_drift_pct),AVG(c.live_rr1)
                           FROM premium_context c JOIN signal_paths p ON p.signal_id=c.signal_id
                           GROUP BY COALESCE(c.execution_status,'UNKNOWN') ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    micro = conn.execute(
                        "SELECT horizon_ms,COUNT(*) FROM premium_micro_snapshots GROUP BY horizon_ms ORDER BY horizon_ms"
                    ).fetchall()
                    reclaim = conn.execute(
                        "SELECT COUNT(*) FROM research_events WHERE event_type='RECLAIM_AFTER_STOP'"
                    ).fetchone()[0]
                    runner = conn.execute(
                        "SELECT COUNT(*) FROM shadow_exit_events WHERE event='RUNNER_EXIT'"
                    ).fetchone()[0]
                    progress_rows = conn.execute(
                        """SELECT v.status,COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_progress_validation v JOIN signal_paths p ON p.signal_id=v.signal_id
                           GROUP BY v.status ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    liq_rows = conn.execute(
                        """SELECT l.barrier_label,COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           AVG(l.ask025_clear_s),AVG(l.largest_ask_wall_distance_pct)
                           FROM premium_liquidity_snapshots l JOIN signal_paths p ON p.signal_id=l.signal_id
                           WHERE l.horizon_ms=0 GROUP BY l.barrier_label ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    conn.close()
                    lines = [f"🧭 V{BOT_VERSION} ENTRY / EXECUTION SHADOW", ""]
                    if rows:
                        lines.append("15sn acceptance:")
                        for status,n,wins,ratio,pb in rows:
                            lines.append(f"• {status}: n={n} | TP2-before-stop %{100*(wins or 0)/max(n,1):.1f} | breakout üstü %{100*(ratio or 0):.0f} | peak PB {(pb or 0):.2f}%")
                    else:
                        lines.append("Henüz 15sn entry-validation tamamlanmadı.")
                    if oi_rows:
                        lines.append("\nOI rejimi (forward kayıtlar):")
                        for regime,n,wins in oi_rows:
                            lines.append(f"• {regime}: n={n} | TP2-before-stop %{100*(wins or 0)/max(n,1):.1f}")
                    if phase_rows:
                        lines.append("\nPhase risk (SHADOW):")
                        for risk,n,wins in phase_rows:
                            lines.append(f"• {risk}: n={n} | TP2-before-stop %{100*(wins or 0)/max(n,1):.1f}")
                    if exec_rows:
                        lines.append("\nSinyal-anı execution sınıfı:")
                        for status,n,wins,drift,rr in exec_rows:
                            lines.append(f"• {status}: n={n} | TP2-before-stop %{100*(wins or 0)/max(n,1):.1f} | drift {(drift or 0):+.2f}% | RR1 {(rr or 0):.2f}")
                    if micro:
                        lines.append("\nMicro snapshot: " + " | ".join(f"{h/1000:g}s:{n}" for h,n in micro))
                    if progress_rows:
                        lines.append("\n60sn Acceptance + Progress (SHADOW):")
                        for status,n,wins in progress_rows:
                            lines.append(f"• {status}: n={n} | TP2-before-stop %{100*(wins or 0)/max(n,1):.1f}")
                    if liq_rows:
                        lines.append("\nLiquidity Barrier @ signal (SHADOW):")
                        for label,n,wins,clear_s,dist in liq_rows:
                            lines.append(f"• {label or 'UNKNOWN'}: n={n} | TP2-before-stop %{100*(wins or 0)/max(n,1):.1f} | ask25 clear {(clear_s or 0):.1f}s | wall dist {(dist or 0):.2f}%")
                    lines.append(f"\n🔁 Stop sonrası reclaim research: {int(reclaim or 0)}")
                    lines.append(f"🏃 TP2 sonrası runner-exit shadow: {int(runner or 0)}")
                    lines.append("\nAcceptance/phase/reclaim/runner sonuçları SHADOW araştırmasıdır; Premium detector eşiklerini değiştirmez.")
                    await telegram_send(session, "\n".join(lines))
                elif text in ("/v58stats", "/v59stats", "/v510stats", "/v511stats", "/riskstats"):
                    conn = db_connect()
                    ignition = conn.execute(
                        """SELECT r.event_type,COUNT(*),AVG(o.mfe_pct),AVG(o.mae_pct),
                                  SUM(CASE WHEN o.mfe_pct>=2.0 THEN 1 ELSE 0 END),
                                  SUM(CASE WHEN o.mfe_pct>=5.0 THEN 1 ELSE 0 END)
                           FROM research_events r LEFT JOIN research_outcomes o
                             ON o.event_id=r.id AND o.horizon_s=900
                           WHERE r.event_type IN ('IGNITION_V2_SHADOW','IGNITION_V2_LOW_VOLUME','FAST_EARLY_V2_SHADOW','NEAR_MISS_CANDIDATE',
                                                  'IGNITION_SHADOW','IGNITION_LOW_VOLUME','FAST_EARLY_SHADOW')
                           GROUP BY r.event_type ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    missed = conn.execute(
                        "SELECT classification,COUNT(*) FROM missed_runner_audit GROUP BY classification ORDER BY COUNT(*) DESC"
                    ).fetchall()
                    blockers = conn.execute(
                        """SELECT r.gate_failures,COUNT(*)
                           FROM missed_runner_audit m JOIN research_events r ON r.id=m.source_event_id
                           WHERE m.classification LIKE 'MISSED_RUNNER_%' AND COALESCE(r.gate_failures,'')<>''
                           GROUP BY r.gate_failures ORDER BY COUNT(*) DESC LIMIT 5"""
                    ).fetchall()
                    static_liq = conn.execute(
                        """SELECT COALESCE(l.barrier_label,'UNKNOWN'),COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           AVG(l.ask025_clear_s),AVG(l.largest_ask_wall_ratio)
                           FROM premium_liquidity_snapshots l JOIN signal_paths p ON p.signal_id=l.signal_id
                           WHERE l.horizon_ms=0
                           GROUP BY COALESCE(l.barrier_label,'UNKNOWN') ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    dyn_liq = conn.execute(
                        """SELECT l.horizon_ms,COALESCE(l.dynamic_state,'UNKNOWN'),COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           AVG(l.dynamic_score),AVG(l.wall_remaining_ratio),AVG(l.bid_ratio_025),AVG(l.ask025_clear_s)
                           FROM premium_liquidity_snapshots l JOIN signal_paths p ON p.signal_id=l.signal_id
                           WHERE l.horizon_ms IN (15000,30000)
                           GROUP BY l.horizon_ms,COALESCE(l.dynamic_state,'UNKNOWN')
                           ORDER BY l.horizon_ms,COUNT(*) DESC"""
                    ).fetchall()
                    progress = conn.execute(
                        """SELECT v.status,COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_progress_validation v JOIN signal_paths p ON p.signal_id=v.signal_id
                           GROUP BY v.status ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    composite = conn.execute(
                        """SELECT c.composite_state,COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_execution_composite c JOIN signal_paths p ON p.signal_id=c.signal_id
                           WHERE c.trade_active=1
                           GROUP BY c.composite_state ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    reclaim = conn.execute(
                        """SELECT COALESCE(shadow_label,'UNLABELED'),COUNT(*)
                           FROM research_events WHERE event_type='RECLAIM_AFTER_STOP'
                           GROUP BY COALESCE(shadow_label,'UNLABELED') ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    liq_v2 = conn.execute(
                        """SELECT l.horizon_ms,COALESCE(l.liquidity_v2_state,'UNKNOWN'),COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           AVG(l.liquidity_v2_score),AVG(l.bid_ratio_025),AVG(l.ask025_vs_initial)
                           FROM premium_liquidity_snapshots l JOIN signal_paths p ON p.signal_id=l.signal_id
                           WHERE l.horizon_ms IN (15000,30000)
                           GROUP BY l.horizon_ms,COALESCE(l.liquidity_v2_state,'UNKNOWN')
                           ORDER BY l.horizon_ms,l.liquidity_v2_state"""
                    ).fetchall()
                    liq_core = conn.execute(
                        """SELECT l.horizon_ms,COALESCE(l.liquidity_core_state,'UNKNOWN'),COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           AVG(l.liquidity_core_score),AVG(l.bid_ratio_025),AVG(l.ask025_vs_initial)
                           FROM premium_liquidity_snapshots l JOIN signal_paths p ON p.signal_id=l.signal_id
                           WHERE l.horizon_ms IN (15000,30000) AND l.liquidity_core_state IS NOT NULL
                           GROUP BY l.horizon_ms,COALESCE(l.liquidity_core_state,'UNKNOWN')
                           ORDER BY l.horizon_ms,l.liquidity_core_state"""
                    ).fetchall()
                    risk = conn.execute(
                        """SELECT r.risk_state,COUNT(*),
                           SUM(CASE WHEN p.tp1_hit_s IS NULL OR (p.invalidation_hit_s IS NOT NULL AND p.invalidation_hit_s<p.tp1_hit_s) THEN 1 ELSE 0 END),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_failure_risk r JOIN signal_paths p ON p.signal_id=r.signal_id
                           WHERE r.horizon_ms=60000 AND r.trade_active=1
                           GROUP BY r.risk_state ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    resolved_risk = conn.execute(
                        """SELECT COALESCE(r.terminal_event,'UNKNOWN'),COUNT(*)
                           FROM premium_failure_risk r
                           WHERE r.horizon_ms=60000 AND r.trade_active=0
                           GROUP BY COALESCE(r.terminal_event,'UNKNOWN') ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    conflicts = conn.execute(
                        """SELECT c.event_code,COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_conflict_events c JOIN signal_paths p ON p.signal_id=c.signal_id
                           GROUP BY c.event_code ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    risk_reclaims = conn.execute("SELECT COUNT(*) FROM failure_risk_reclaims").fetchone()[0]
                    discovery_eps = conn.execute(
                        """SELECT COUNT(*),
                           SUM(CASE WHEN max_runner_size>=2 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN max_runner_size>=2 AND premium_captured=0 AND early_captured=0 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN max_runner_size>=2 AND premium_captured=1 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN max_runner_size>=2 AND premium_captured=0 AND early_captured=1 THEN 1 ELSE 0 END)
                           FROM discovery_episode_audit"""
                    ).fetchone()
                    conn.close()
                    lines=[f"🧪 V{BOT_VERSION} EXECUTION RISK / LIQUIDITY", ""]
                    if ignition:
                        lines.append("⚡ Ignition / near-miss 15dk (SHADOW):")
                        for t,n,mfe,mae,m2,m5 in ignition[:8]:
                            lines.append(f"• {t}: n={n} | MFE {(mfe or 0):+.2f}% | +2 %{100*(m2 or 0)/max(n,1):.1f} | +5 %{100*(m5 or 0)/max(n,1):.1f}")
                    else:
                        lines.append(f"⚡ Henüz yeni V{BOT_VERSION} ignition V2 forward kaydı yok.")
                    if missed:
                        lines.append("\n🎯 Missed-runner audit:")
                        for c,n in missed[:7]:
                            lines.append(f"• {c}: {n}")
                    if blockers:
                        lines.append("\n🚧 Missed-runner en sık blocker:")
                        for b,n in blockers:
                            lines.append(f"• {b}: {n}")
                    if static_liq:
                        lines.append("\n🧱 Statik Liquidity Barrier @ signal:")
                        for label,n,wins,clear_s,wall_ratio in static_liq:
                            lines.append(f"• {label}: n={n} | TP2-first %{100*(wins or 0)/max(n,1):.1f} | clear {(clear_s or 0):.1f}s | wall× {(wall_ratio or 0):.1f}")
                    if dyn_liq:
                        lines.append("\n🌊 Dinamik liquidity evolution:")
                        for h,state,n,wins,score,remain,bidratio,clear_s in dyn_liq:
                            lines.append(f"• {h/1000:g}s {state}: n={n} | TP2-first %{100*(wins or 0)/max(n,1):.1f} | score {(score or 0):+.0f} | wall %{100*(remain or 0):.0f} | bid25 %{100*(bidratio or 0):.0f}")
                    if liq_v2:
                        lines.append("\n🌊 Liquidity Regime V2 (SHADOW):")
                        for h,state,n,wins,score,bidratio,askratio in liq_v2:
                            lines.append(f"• {h/1000:g}s {state}: n={n} | TP2-first %{100*(wins or 0)/max(n,1):.1f} | score {(score or 0):+.0f} | bid25 %{100*(bidratio or 0):.0f} | ask25× {(askratio or 0):.2f}")
                    if liq_core:
                        lines.append("\n🧬 Liquidity CORE (4-metric ablation, SHADOW):")
                        for h,state,n,wins,score,bidratio,askratio in liq_core:
                            lines.append(f"• {h/1000:g}s {state}: n={n} | TP2-first %{100*(wins or 0)/max(n,1):.1f} | score {(score or 0):+.0f} | bid25 %{100*(bidratio or 0):.0f} | ask25× {(askratio or 0):.2f}")
                    if risk:
                        lines.append("\n🚦 Post-Premium Failure Risk 60s — yalnız 60s'de hâlâ aktif işlemler:")
                        for state,n,tp1fail,wins in risk:
                            lines.append(f"• {state}: n={n} | TP1-fail %{100*(tp1fail or 0)/max(n,1):.1f} | TP2-first %{100*(wins or 0)/max(n,1):.1f}")
                    if resolved_risk:
                        lines.append("\n✅ 60s öncesi çözülmüş/entry olmayan gözlemler (failure-risk'e dahil değil):")
                        lines.extend(f"• {state}: {n}" for state,n in resolved_risk)
                    if conflicts:
                        lines.append("\n⚔️ Conflict cohortları:")
                        for code,n,wins in conflicts[:8]:
                            lines.append(f"• {code}: n={n} | TP2-first %{100*(wins or 0)/max(n,1):.1f}")
                    if risk_reclaims:
                        lines.append(f"\n🔁 Failure-risk sonrası stop→reclaim bağlantısı: {int(risk_reclaims)}")
                    if progress:
                        lines.append("\n🧭 60sn Progress:")
                        for status,n,wins in progress:
                            lines.append(f"• {status}: n={n} | TP2-first %{100*(wins or 0)/max(n,1):.1f}")
                    if composite:
                        lines.append("\n🧩 Progress + Liquidity composite:")
                        for state,n,wins in composite:
                            lines.append(f"• {state}: n={n} | TP2-first %{100*(wins or 0)/max(n,1):.1f}")
                    if reclaim:
                        lines.append("\n🔁 Reclaim:")
                        lines.extend(f"• {label}: {n}" for label,n in reclaim)
                    if discovery_eps and discovery_eps[0]:
                        total,runner,missed_ep,prem_ep,early_ep = discovery_eps
                        lines.append(f"\n🔭 Dedup discovery episodes: toplam {int(total or 0)} | runner≥2% {int(runner or 0)} | missed {int(missed_ep or 0)} | Premium captured {int(prem_ep or 0)} | Early-only {int(early_ep or 0)}")
                    lines.append(f"\nTüm V{BOT_VERSION} yeni sınıfları SHADOW araştırmasıdır; Premium/TP/stop production kurallarını değiştirmez. 15sn gate yalnız counterfactual ölçer; pre-gate TP/stop look-ahead guard aktif.")
                    await telegram_send(session, "\n".join(lines))
                elif text == "/gatev2stats":
                    conn = db_connect()
                    rows = conn.execute("""SELECT v.decision,COUNT(*),
                           SUM(CASE WHEN p.tp1_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp1_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           SUM(CASE WHEN p.invalidation_hit_s IS NOT NULL AND (p.tp1_hit_s IS NULL OR p.invalidation_hit_s<p.tp1_hit_s) THEN 1 ELSE 0 END),
                           AVG(CASE WHEN p.mae_before_tp1 IS NOT NULL THEN p.mae_before_tp1 END)
                           FROM premium_execution_gate_v2_shadow v LEFT JOIN signal_paths p ON p.signal_id=v.signal_id
                           GROUP BY v.decision ORDER BY COUNT(*) DESC""").fetchall()
                    over = conn.execute("""SELECT COUNT(*),
                           SUM(CASE WHEN p.tp1_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp1_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           SUM(CASE WHEN p.invalidation_hit_s IS NOT NULL AND (p.tp1_hit_s IS NULL OR p.invalidation_hit_s<p.tp1_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_execution_gate_v2_shadow v LEFT JOIN signal_paths p ON p.signal_id=v.signal_id WHERE v.overextended_60=1""").fetchone()
                    localtop = conn.execute("""SELECT COUNT(*),
                           SUM(CASE WHEN p.tp1_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp1_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           SUM(CASE WHEN p.invalidation_hit_s IS NOT NULL AND (p.tp1_hit_s IS NULL OR p.invalidation_hit_s<p.tp1_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_execution_gate_v2_shadow v LEFT JOIN signal_paths p ON p.signal_id=v.signal_id WHERE v.local_top_proxy=1""").fetchone()
                    conn.close()
                    lines=[f"🧪 V{BOT_VERSION} EXECUTION GATE V2 — SHADOW", "", "⚠️ Production Premium değişmedi; V2 yalnız araştırmadır."]
                    for d,n,w,l,mae in rows:
                        resolved=(w or 0)+(l or 0)
                        lines.append(f"• {d}: n={n} | TP1-first {int(w or 0)} | stop-first {int(l or 0)} | win %{100*(w or 0)/max(resolved,1):.1f} | MAE {(mae or 0):+.2f}%")
                    if over and over[0]:
                        resolved=(over[1] or 0)+(over[2] or 0); lines.append(f"\n🔥 OVEREXTENDED_60 ≥{OVEREXTENDED_60_PCT:g}%: n={over[0]} | TP1-first {int(over[1] or 0)} | stop-first {int(over[2] or 0)} | win %{100*(over[1] or 0)/max(resolved,1):.1f}")
                    if localtop and localtop[0]:
                        resolved=(localtop[1] or 0)+(localtop[2] or 0); lines.append(f"⛰️ Local-top proxy: n={localtop[0]} | TP1-first {int(localtop[1] or 0)} | stop-first {int(localtop[2] or 0)} | win %{100*(localtop[1] or 0)/max(resolved,1):.1f}")
                    await telegram_send(session, "\n".join(lines))
                elif text == "/lateentrystats":
                    conn = db_connect()
                    rows = conn.execute("""SELECT s.symbol,s.ts,s.price,v.chg30_signal,v.chg60_signal,v.live_rr1,v.progress_status,v.composite_state,v.decision,p.tp1_hit_s,p.tp2_hit_s,p.invalidation_hit_s
                           FROM premium_execution_gate_v2_shadow v JOIN signals_v2 s ON s.id=v.signal_id LEFT JOIN signal_paths p ON p.signal_id=v.signal_id
                           WHERE v.overextended_60=1 OR v.local_top_proxy=1 OR v.decision IN ('BLOCK_REJECTION_FAIL_RISK','BLOCK_HOSTILE_REJECTION_CANDIDATE','BLOCK_STOP_FIRST')
                           ORDER BY s.ts DESC LIMIT 15""").fetchall()
                    conn.close()
                    lines=[f"⛰️ V{BOT_VERSION} LATE-ENTRY / EXECUTION AUDIT", ""]
                    if not rows: lines.append("Henüz V2 adayı yok.")
                    for sym,ts,px,c30,c60,rr,prog,comp,dec,tp1,tp2,stop in rows:
                        dt=datetime.fromtimestamp(int(ts), IST).strftime("%d.%m %H:%M")
                        path=("STOP→" + ("TP1" if tp1 is not None else "—")) if stop is not None and (tp1 is None or stop < tp1) else ("TP1-first" if tp1 is not None else "unresolved")
                        lines.append(f"• {sym} {dt} @{px:.10g} | 30s {(c30 or 0):+.2f}% 60s {(c60 or 0):+.2f}% | RR {(rr or 0):.2f} | {prog}/{comp} | {dec} | {path}")
                    lines.append("\nAmaç: sonradan yükseleni değil, stop görmeden TP'ye giden executable entry'yi seçmek.")
                    await telegram_send(session, "\n".join(lines))
                elif text == "/gatestats":
                    conn = db_connect()
                    decisions = conn.execute(
                        """SELECT g.decision,COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           SUM(CASE WHEN p.invalidation_hit_s IS NOT NULL AND (p.tp1_hit_s IS NULL OR p.invalidation_hit_s<p.tp1_hit_s) THEN 1 ELSE 0 END),
                           AVG(g.signal_to_gate_pct),AVG(g.gate_mfe),AVG(g.gate_mae)
                           FROM premium_execution_gate_shadow g LEFT JOIN signal_paths p ON p.signal_id=g.signal_id
                           GROUP BY g.decision ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    gate_paths = conn.execute(
                        """SELECT g.decision,COUNT(*),
                           SUM(CASE WHEN g.gate_tp2_hit_s IS NOT NULL AND (g.gate_stop_hit_s IS NULL OR g.gate_tp2_hit_s<g.gate_stop_hit_s) THEN 1 ELSE 0 END),
                           SUM(CASE WHEN g.gate_stop_hit_s IS NOT NULL AND (g.gate_tp1_hit_s IS NULL OR g.gate_stop_hit_s<g.gate_tp1_hit_s) THEN 1 ELSE 0 END),
                           AVG(g.gate_mfe),AVG(g.gate_mae)
                           FROM premium_execution_gate_shadow g WHERE g.completed_60m=1
                           GROUP BY g.decision ORDER BY COUNT(*) DESC"""
                    ).fetchall()
                    sticky = conn.execute(
                        """SELECT COUNT(*),SUM(COALESCE(recovered_30,0)),SUM(COALESCE(persistent_hostile_30,0))
                           FROM premium_execution_gate_shadow WHERE sticky_early_hostile=1"""
                    ).fetchone()
                    absorb = conn.execute(
                        """SELECT COUNT(*),
                           SUM(CASE WHEN p.tp2_hit_s IS NOT NULL AND (p.invalidation_hit_s IS NULL OR p.tp2_hit_s<p.invalidation_hit_s) THEN 1 ELSE 0 END),
                           SUM(CASE WHEN p.invalidation_hit_s IS NOT NULL AND (p.tp1_hit_s IS NULL OR p.invalidation_hit_s<p.tp1_hit_s) THEN 1 ELSE 0 END)
                           FROM premium_execution_gate_shadow g JOIN signal_paths p ON p.signal_id=g.signal_id
                           WHERE g.absorption_risk=1"""
                    ).fetchone()
                    cf = conn.execute(
                        """SELECT post_gate_horizon_s,COUNT(*),AVG(return_pct),AVG(mfe_pct),AVG(mae_pct)
                           FROM premium_gate_counterfactual GROUP BY post_gate_horizon_s ORDER BY post_gate_horizon_s"""
                    ).fetchall()
                    conn.close()
                    lines=[f"🧪 V{BOT_VERSION} 15SN EXECUTION GATE — SHADOW", ""]
                    lines.append("⚠️ Production Premium hâlâ anında gider. Bu ekran gecikmeli giriş filtresini gerçek post-gate fiyat yoluyla test eder.")
                    if decisions:
                        lines.append("\nOrijinal sinyal sonucuna göre gate cohortları:")
                        for d,n,w,stp,dr,mfe,mae in decisions:
                            lines.append(f"• {d}: n={n} | orig TP2-first %{100*(w or 0)/max(n,1):.1f} | stop-first %{100*(stp or 0)/max(n,1):.1f} | 15sn kayma {(dr or 0):+.2f}%")
                    if gate_paths:
                        lines.append("\nGerçek 15sn gate fiyatından SONRA (look-ahead temiz):")
                        for d,n,w,stp,mfe,mae in gate_paths:
                            lines.append(f"• {d}: n={n} | post-gate TP2-first %{100*(w or 0)/max(n,1):.1f} | stop-first %{100*(stp or 0)/max(n,1):.1f} | MFE {(mfe or 0):+.2f}% | MAE {(mae or 0):+.2f}%")
                    if sticky and sticky[0]:
                        lines.append(f"\n🧷 Early HOSTILE sticky: n={int(sticky[0] or 0)} | 30sn toparladı {int(sticky[1] or 0)} | 30sn hostile kaldı {int(sticky[2] or 0)}")
                    if absorb and absorb[0]:
                        lines.append(f"🧲 Absorption-risk SHADOW (flow≥{ABSORPTION_FLOW30_MIN:g}x & bid<{ABSORPTION_BID_MAX*100:.0f}%): n={int(absorb[0])} | TP2-first %{100*(absorb[1] or 0)/max(absorb[0],1):.1f} | stop-first %{100*(absorb[2] or 0)/max(absorb[0],1):.1f}")
                    if cf:
                        lines.append("\nGate fiyatından sonraki counterfactual yol:")
                        for h,n,ret,mfe,mae in cf:
                            lines.append(f"• +{h}s: n={n} | ret {(ret or 0):+.2f}% | MFE {(mfe or 0):+.2f}% | MAE {(mae or 0):+.2f}%")
                    lines.append("\nKarar kriteri: gate ancak post-gate net expectancy'yi iyileştirirse production'a aday olacak; pre-gate hedef görenler başarı hanesine yazılmayacak.")
                    await telegram_send(session, "\n".join(lines))
                elif text == "/discoverystats":
                    conn = db_connect()
                    summary = conn.execute(
                        """SELECT COUNT(*),
                           SUM(CASE WHEN max_runner_size>=2 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN max_runner_size>=2 AND premium_captured=0 AND early_captured=0 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN max_runner_size>=2 AND premium_captured=1 THEN 1 ELSE 0 END),
                           SUM(CASE WHEN max_runner_size>=2 AND premium_captured=0 AND early_captured=1 THEN 1 ELSE 0 END),
                           AVG(CASE WHEN max_runner_size>=2 THEN max_mfe_pct END)
                           FROM discovery_episode_audit"""
                    ).fetchone()
                    top_missed = conn.execute(
                        """SELECT symbol,start_ts,max_runner_size,max_mfe_pct,event_count,blocker_counts_json
                           FROM discovery_episode_audit
                           WHERE max_runner_size>=2 AND premium_captured=0 AND early_captured=0
                           ORDER BY COALESCE(max_mfe_pct,0) DESC LIMIT 8"""
                    ).fetchall()
                    conn.close()
                    total,runner,missed_ep,prem_ep,early_ep,avg_mfe = summary or (0,0,0,0,0,0)
                    lines=[f"🔭 V{BOT_VERSION} DISCOVERY EPISODE AUDIT", ""]
                    lines.append(f"Toplam dedup episode: {int(total or 0)}")
                    lines.append(f"Runner episode ≥+%2: {int(runner or 0)} | ort. max MFE {(avg_mfe or 0):+.2f}%")
                    if runner:
                        lines.append(f"Premium captured: {int(prem_ep or 0)} (%{100*(prem_ep or 0)/runner:.1f})")
                        lines.append(f"Early-only: {int(early_ep or 0)} (%{100*(early_ep or 0)/runner:.1f})")
                        lines.append(f"Missed: {int(missed_ep or 0)} (%{100*(missed_ep or 0)/runner:.1f})")
                    if top_missed:
                        lines.append("\nEn güçlü missed first-wave episodes:")
                        for sym,ts,size,mfe,events,blockers_json in top_missed:
                            try:
                                bc=json.loads(blockers_json or "{}")
                                topb=sorted(bc.items(), key=lambda x:x[1], reverse=True)[:2]
                                blockers_txt=", ".join(f"{k}×{v}" for k,v in topb) or "—"
                            except Exception:
                                blockers_txt="—"
                            dt=datetime.fromtimestamp(int(ts), IST).strftime("%d.%m %H:%M")
                            lines.append(f"• {sym} {dt}: max +{(mfe or 0):.2f}% | size {int(size or 0)}P | event {int(events or 0)} | blocker {blockers_txt}")
                    lines.append("\nBu ekran event sayısını değil, aynı coin/hareketi zaman bazlı tek episode altında toplar. Production AL filtresi değildir.")
                    await telegram_send(session, "\n".join(lines))
                elif text == "/dbhealth":
                    try:
                        def _check():
                            c = db_connect()
                            try:
                                q = c.execute("PRAGMA quick_check").fetchone()
                                i = c.execute("PRAGMA integrity_check").fetchall()
                                quick = str(q[0]) if q else "unknown"
                                integrity = "ok" if i and all(str(r[0]).lower()=="ok" for r in i) else "; ".join(str(r[0]) for r in i[:5]) or "unknown"
                                return quick, integrity
                            finally:
                                c.close()
                        quick, integrity = await asyncio.to_thread(_check)
                        await telegram_send(session, f"🗄️ DB health | quick={quick} | integrity={integrity}\nBot: V{BOT_VERSION}\n\nCanlı DB dosyasını doğrudan kopyalama; /backupdb ile tutarlı snapshot al.")
                    except Exception as e:
                        await telegram_send(session, f"⚠️ DB health kontrolü başarısız: {type(e).__name__}: {e}")
                elif text == "/backupdb":
                    if not DB_BACKUP_ENABLED:
                        await telegram_send(session, "⚪ DB backup özelliği kapalı.")
                    else:
                        await telegram_send(session, "🗄️ Tutarlı SQLite snapshot hazırlanıyor…")
                        try:
                            zip_path, health = await asyncio.to_thread(create_consistent_db_backup)
                            ok = await telegram_send_document(session, zip_path, f"Momentum Scanner V{BOT_VERSION} DB backup | {health}")
                            if not ok:
                                await telegram_send(session, "⚠️ Backup hazırlandı ama Telegram dosya gönderimi başarısız oldu. Railway logunu kontrol et.")
                        except Exception as e:
                            await telegram_send(session, f"❌ DB backup başarısız: {type(e).__name__}: {e}")
                elif text == "/joinstatus":
                    await telegram_send(
                        session,
                        "👥 KATILIM ONAY DURUMU\n\n"
                        f"Onay sistemi: {'✅ AÇIK' if JOIN_REQUEST_APPROVAL_ENABLED else '⚪ KAPALI'}\n"
                        f"Kanal/Grup ID: {TELEGRAM_APPROVAL_CHAT_ID or 'ayarlı değil'}\n"
                        f"Admin onay sohbeti: {TELEGRAM_ADMIN_CHAT_ID or 'ayarlı değil'}\n"
                        f"Admin user kısıtı: {TELEGRAM_ADMIN_USER_ID or 'yalnız admin sohbeti kontrolü'}\n\n"
                        "Onay açıkken bot join request'i sana getirir; ✅/❌ butonuna sen basmadan karar verilmez."
                    )
                elif text == "/joinlink":
                    if not JOIN_REQUEST_APPROVAL_ENABLED:
                        await telegram_send(session, "❌ Önce Railway'de TELEGRAM_APPROVAL_CHAT_ID ayarlanmalı ve bot kanalda admin olmalı.")
                    else:
                        link = await create_approval_invite_link(session)
                        if link:
                            await telegram_send(session, f"✅ Yönetici onaylı davet linki oluşturuldu:\n{link}\n\nBu linkte kullanıcı doğrudan katılmaz; önce onay isteği gönderir.")
                        else:
                            await telegram_send(session, "❌ Onaylı link oluşturulamadı. Botun kanalda admin ve can_invite_users yetkili olduğundan emin ol.")
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
                    lines=[f"🔬 V{BOT_VERSION} RESEARCH ÖZETİ", ""]
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
                    lines.append(f"\nBu veriler production filtresi değildir; V{BOT_VERSION} forward ölçüm katmanlarını doğrular.")
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
                            oi5, oi_prev5, oi_accel5 = await get_oi_context(session, sym)
                            m["oi5"] = oi5; m["oi_prev5"] = oi_prev5; m["oi_accel5"] = oi_accel5; m["oi_regime"] = oi_regime_label(oi5)
                            if m["oi5"] is not None:
                                if m["oi5"] >= 1.0: sc = min(100, sc + 4)
                                elif m["oi5"] <= -1.5: sc = max(0, sc - 4)
                            q = entry_quality(m, sc, states[sym])
                            rscore = rise_probability(m, sc, states[sym])
                            phase_label, phase_pts, _ = phase_risk_shadow(m)
                            m["phase_risk"] = phase_label; m["phase_risk_points"] = phase_pts
                            m["signal_generated_ts_ms"] = now_ms()
                            plan = estimate_trade_plan(sym, m)
                            m["execution"] = compute_execution_context(sym, m, plan)
                            await telegram_send(session, build_manual_analysis(sym, m, sc, q, rscore, plan), symbol=sym)
                elif text in ("/test", "test"):
                    await telegram_send(session, "✅ Bot çalışıyor. /status, /top, /gainers, /funnel, /stats, /radarstats, /shadowstats, /entrystats, /latencystats, /researchstats, /v511stats, /v510stats, /riskstats, /gatestats, /discoverystats, /dbhealth, /backupdb, /joinstatus, /settings, /riskstatus, /positions, /myid ve /analiz COIN kullanabilirsin.")
                elif text in ("/help", "/start"):
                    await telegram_send(session,
                        f"🤖 Momentum Scanner V{BOT_VERSION} — Execution Risk / Liquidity Regime Research\n\n"
                        "/status — bağlantı ve sinyal durumu\n"
                        "/myid — Telegram kullanıcı ve sohbet ID bilgisi\n"
                        "/top — şu an ısınan ilk 10 coin\n"
                        "/gainers — güncel Futures gainers\n"
                        "/funnel — adayların hangi filtrelerde elendiği\n"
                        "/stats — premium sinyal + işlem yolu performansı\n"
                        "/radarstats — erken radarların 60 dk performansı\n"
                        "/shadowstats — Shadow Exit + ilk dalga + erken→Premium özeti\n"
                        "/entrystats — 15sn acceptance / micro execution özeti\n"
                        "/latencystats — Telegram send + 1–60sn execution latency özeti\n"
                        "/researchstats — genel research özeti\n"
                        "/v511stats — liquidity V2/CORE / active failure-risk / conflict / reclaim özeti\n"
                        "/v510stats — geriye dönük aynı özet aliası\n"
                        "/riskstats — /v511stats kısa yolu\n"
                        "/gatestats — 15sn delayed-entry gate / look-ahead temiz counterfactual\n"
                        "/discoverystats — dedup first-wave runner episode özeti\n"
                        "/dbhealth — SQLite quick_check + integrity_check\n"
                        "/backupdb — tutarlı DB snapshotını Telegrama gönder\n"
                        "/joinstatus — kanal katılım onayı durumu\n"
                        "/joinlink — yönetici onaylı davet linki oluştur\n"
                        "/settings — AutoTrade butonlu kontrol paneli\n"
                        "/autotrade off|dry|live — execution modu (LIVE ikinci onaylı)\n"
                        "/tradesize N — işlem başına marjin; /leverage N; /maxpositions N\n"
                        "/dailyloss N — günlük yüzde zarar limiti; /stopstreak N — ardışık stop kilidi\n"
                        "/riskstatus — günlük P/L/risk kilidi; /positions — yalnız botun yönettiği pozisyonlar\n"
                        "/runner 50 5 — yeni işlemlerde %50 TP1 + %50 +%5 runner; /runner off — mevcut TP2\n"
                        "/analiz COIN — bir coini anlık analiz et\n"
                        "/test — Telegram testi\n\n"
                        f"Premium seçim eşikleri değişmedi. V{BOT_VERSION} liquidity V2/CORE, sticky early-liquidity, gerçek post-gate counterfactual, ignition V2, failure-risk ve reclaim/runner katmanlarını SHADOW olarak ölçer; bunlar işlem sinyali değildir."
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Telegram command polling exception type=%s repr=%r", type(e).__name__, e)
            await asyncio.sleep(3)


async def main():
    global symbols
    init_db()
    load_autotrade_settings()
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
                f"✅ Momentum Scanner V{BOT_VERSION} başladı — EXECUTION RISK / LIQUIDITY REGIME RESEARCH\n\n"
                f"🪙 İzlenen kontrat: {len(symbols)}\n"
                f"⚡ 100ms aggTrade + event-level Premium path + 1–60sn micro execution takibi\n"
                f"💵 Min 24s hacim: {fmt_money(MIN_24H_QUOTE_VOLUME)} USDT\n"
                f"⭐ Sessiz aday skoru: {EARLY_SCORE}+\n"
                f"🎯 Teyit: {CONFIRM_REQUIRED} × {CONFIRM_INTERVAL_SECONDS} sn | premium momentum {PREMIUM_MIN_MOMENTUM_SCORE}+ | giriş {PREMIUM_ENTRY_MIN_SCORE}+ | yükseliş {PREMIUM_RISE_MIN_SCORE}+\n"
                f"👀 Erken radar: 2/3 seçici Telegram; production eşikleri değişmedi\n"
                f"🧪 Shadow Exit + first-wave/session-peak + post-shadow outcome: AÇIK; işlem sinyali değil\n"
                f"🔬 Second-wave / Pre-Breakout / reject outcome araştırması: {'AÇIK' if RESEARCH_ENABLED else 'KAPALI'}\n"
                f"🧭 15sn breakout acceptance + 60sn Progress + stop sonrası reclaim + TP2 runner: SHADOW\n"
                f"🧪 V5.13.4: stage audit + REAL EARLY trend-build + liquidity/OI transition V3: SHADOW\n"
                f"🌊 Liquidity V1 + frozen Regime V2 + CORE ablation 5/15/30sn: SHADOW | tek wall hard filter değil\n"
                f"🚦 Post-Premium Failure Risk 15/30/60sn: yalnız aktif/executable trade actionability | public alert yok\n"
                f"🔭 Discovery episode audit: event tekrarlarını dedup eder | gap {DISCOVERY_EPISODE_GAP_S}s\n"
                f"⚡ Ignition V2: SHADOW | price acceleration + BTC-relative + flow efficiency odaklı\n"
                f"💸 Funding/mark stream: AÇIK | OI 5m + OI ivmesi: kayıt AÇIK\n"
                f"👥 Join-request onayı: {'AÇIK' if JOIN_REQUEST_APPROVAL_ENABLED else 'KAPALI (TELEGRAM_APPROVAL_CHAT_ID yok)'}\n"
                f"📣 Abone kanal yayını: {'AÇIK' if TELEGRAM_BROADCAST_ENABLED else 'KAPALI'} | Early + Premium + Continuation\n"
                f"🏆 Gainers: arka plan rank-velocity/outcome AÇIK | Telegram push: {'AÇIK' if GAINERS_NOTIFY else 'KAPALI'}\n"
                f"👁 Pozisyon gözlemcisi: tüm gerçek Futures pozisyonları | entry-cross + ROE %5/%10/%20\n"
                f"🤖 AutoTrade: {autotrade_cfg['mode']} | {float(autotrade_cfg['trade_margin_usdt']):.0f} USDT × {int(autotrade_cfg['leverage'])}x | max {int(autotrade_cfg['max_open_positions'])} | günlük %{float(autotrade_cfg['daily_max_loss_pct']):g} | 4-stop koruması {int(autotrade_cfg['max_consecutive_stops'])} | LIVE varsayılan KAPALI\n\n"
                f"Komutlar: /status  /top  /gainers  /funnel  /stats  /radarstats  /shadowstats  /entrystats  /latencystats  /researchstats  /v511stats  /v510stats  /riskstats  /gatestats  /discoverystats  /dbhealth  /backupdb  /joinstatus  /settings  /autotrade  /riskstatus  /positions  /analiz COIN  /test"
            )

        chunks = [symbols[i:i + AGGTRADE_CHUNK] for i in range(0, len(symbols), AGGTRADE_CHUNK)]
        tasks = [
            ticker_ws(session), book_ws(session), mark_price_ws(session), liquidation_ws(session),
            outcome_loop(session), reset_levels_loop(), telegram_command_loop(session), gainers_loop(session), autotrade_reconcile_loop(session), position_observer_loop(session),
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
