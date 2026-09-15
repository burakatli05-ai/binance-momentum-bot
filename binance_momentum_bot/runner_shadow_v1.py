"""Shadow-only early-runner selection and non-runner veto research.

This module never changes production Premium gates, sizing, exits or order behavior.
It consumes already-observable stage features plus subsequent trade ticks and writes
additive telemetry only. Optional Telegram messages are explicitly SHADOW/TEST and
are sent only to the private admin chat from a daemon worker.
"""
from __future__ import annotations

import json
import math
import os
import queue
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional, Tuple

MODEL_VERSION = "runner-score-v1.0"
VETO_VERSION = "non-runner-veto-v1.0"
RUNNER_SCORE_V1_ENABLED = os.getenv("RUNNER_SCORE_V1_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
RUNNER_SCORE_V1_NOTIFY = os.getenv("RUNNER_SCORE_V1_NOTIFY", "1").strip().lower() not in ("0", "false", "no", "off")
RUNNER_WATCH_MIN_SCORE = int(os.getenv("RUNNER_WATCH_MIN_SCORE", "70"))
RUNNER_FAST_ALLOW_SCORE = int(os.getenv("RUNNER_FAST_ALLOW_SCORE", "80"))
RUNNER_REACQUIRE_MIN_SCORE = int(os.getenv("RUNNER_REACQUIRE_MIN_SCORE", "72"))
RUNNER_REACQUIRE_WINDOW_S = int(os.getenv("RUNNER_REACQUIRE_WINDOW_S", "1800"))
RUNNER_OUTCOME_HORIZON_S = int(os.getenv("RUNNER_OUTCOME_HORIZON_S", "3600"))
RUNNER_PERSIST_INTERVAL_MS = int(os.getenv("RUNNER_PERSIST_INTERVAL_MS", "15000"))
RUNNER_DATA_GRACE_S = int(os.getenv("RUNNER_DATA_GRACE_S", "300"))


def _finite(value: Any) -> Optional[float]:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _first(features: Dict[str, Any], *names: str) -> Optional[float]:
    for name in names:
        if name in features:
            x = _finite(features.get(name))
            if x is not None:
                return x
    return None


def _bool(features: Dict[str, Any], *names: str) -> Optional[bool]:
    for name in names:
        if name in features:
            v = features.get(name)
            if isinstance(v, str):
                return v.strip().lower() in ("1", "true", "yes", "y", "on")
            return bool(v)
    return None


def _phase(features: Dict[str, Any]) -> Optional[str]:
    for name in ("phase", "phase_risk", "faz", "faz_riski"):
        v = features.get(name)
        if v is not None:
            return str(v).upper()
    return None


def _pct_buy(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return x / 100.0 if x > 1.0 else x


def score_features(features: Dict[str, Any]) -> Tuple[int, Dict[str, float], list[str]]:
    """Deterministic, explainable prior score using only contemporaneous features."""
    f = dict(features or {})
    contributions: Dict[str, float] = {}
    reasons: list[str] = []
    score = 25.0

    chg30 = _first(f, "chg30", "chg_30s", "chg30s")
    chg60 = _first(f, "chg60", "chg_60s", "chg60s")
    rel30 = _first(f, "rel30", "btc_rel30", "relative30")
    rel60 = _first(f, "rel60", "btc_rel60", "relative60")
    rank = _first(f, "gainer_rank", "gainers_rank", "rank")
    flow = _first(f, "flow30", "flow_30s", "flow30x")
    buy = _pct_buy(_first(f, "buy30", "buy_ratio", "buy_ratio30"))
    book = _pct_buy(_first(f, "book_imbalance", "bid_ratio", "bid_pct"))
    runup = _first(f, "candidate_runup", "candidate_runup_pct", "episode_progress_pct", "dip_to_now_pct")
    oi5 = _first(f, "oi5", "oi_5m", "oi5m", "oi_change_5m")
    momentum = _first(f, "momentum_score", "score")
    entry = _first(f, "entry_score", "entry_quality", "quality")
    rise = _first(f, "rise_score", "yukselis_score")
    breakout = _bool(f, "breakout", "is_breakout")
    phase = _phase(f)

    def add(key: str, value: float, reason: Optional[str] = None) -> None:
        nonlocal score
        score += value
        contributions[key] = round(value, 3)
        if reason:
            reasons.append(reason)

    if chg30 is not None:
        if chg30 >= 1.2: add("chg30", 13, "PRICE_ACCEL_30_STRONG")
        elif chg30 >= 0.8: add("chg30", 11, "PRICE_ACCEL_30")
        elif chg30 >= 0.5: add("chg30", 8, "PRICE_ACCEL_30")
        elif chg30 >= 0.25: add("chg30", 4)
        elif chg30 <= 0: add("chg30", -6, "PRICE_30_WEAK")
    if chg60 is not None:
        if chg60 >= 1.5: add("chg60", 13, "PRICE_ACCEL_60_STRONG")
        elif chg60 >= 1.0: add("chg60", 10, "PRICE_ACCEL_60")
        elif chg60 >= 0.7: add("chg60", 7)
        elif chg60 >= 0.4: add("chg60", 3)
        elif chg60 < 0: add("chg60", -6, "PRICE_60_WEAK")
    if rel30 is not None:
        if rel30 >= 0.7: add("rel30", 15, "BTC_RELATIVE_LEADER")
        elif rel30 >= 0.4: add("rel30", 10, "BTC_RELATIVE_STRONG")
        elif rel30 >= 0.2: add("rel30", 5)
        elif rel30 < 0: add("rel30", -5, "BTC_RELATIVE_WEAK")
    elif rel60 is not None:
        if rel60 >= 0.7: add("rel60", 10, "BTC_RELATIVE_STRONG")
        elif rel60 < 0: add("rel60", -4, "BTC_RELATIVE_WEAK")
    if rank is not None and rank > 0:
        if rank <= 10: add("gainer_rank", 15, "GAINERS_TOP10")
        elif rank <= 30: add("gainer_rank", 10, "GAINERS_TOP30")
        elif rank <= 60: add("gainer_rank", 4)
        elif rank > 100: add("gainer_rank", -6, "GAINERS_WEAK_RANK")
    if flow is not None:
        efficiency = (chg30 / max(flow, 0.1)) if chg30 is not None else None
        if 1.2 <= flow <= 6.0: add("flow30", 6, "FLOW_HEALTHY")
        elif 6.0 < flow <= 10.0:
            add("flow30", 3 if (efficiency is None or efficiency >= 0.08) else -2,
                "FLOW_STRONG" if (efficiency is None or efficiency >= 0.08) else "FLOW_INEFFICIENT")
        elif flow > 10.0:
            add("flow30", -4 if (efficiency is not None and efficiency >= 0.08) else -9, "FLOW_EXTREME")
        elif flow < 0.8: add("flow30", -4, "FLOW_WEAK")
        if efficiency is not None:
            if efficiency >= 0.20: add("flow_efficiency", 5, "FLOW_EFFICIENT")
            elif efficiency < 0.04 and flow >= 4: add("flow_efficiency", -5, "FLOW_ABSORPTION_RISK")
    if buy is not None:
        if 0.55 <= buy <= 0.72: add("buy30", 5, "BUY_BALANCED_STRONG")
        elif 0.72 < buy <= 0.80: add("buy30", 2)
        elif buy > 0.80: add("buy30", -8, "BUY_EXHAUSTION_RISK")
        elif buy < 0.48: add("buy30", -4, "BUY_WEAK")
    if book is not None:
        if 0.35 <= book <= 0.85: add("book", 2)
        elif book > 0.90: add("book", -8, "BOOK_ONE_SIDED")
        elif book < 0.10: add("book", -4, "BID_SUPPORT_WEAK")
    if runup is not None:
        if runup > 3.0: add("runup", -9, "ALREADY_RUN_HIGH")
        elif runup > 2.0: add("runup", -6, "ALREADY_RUN")
        elif runup > 1.2: add("runup", -3)
        elif 0 <= runup <= 0.8: add("runup", 2)
    if phase == "MEDIUM": add("phase", -3, "PHASE_MEDIUM")
    elif phase == "HIGH": add("phase", -6, "PHASE_HIGH")
    elif phase == "LOW": add("phase", 2)
    if oi5 is not None:
        if oi5 >= 0.20: add("oi5", 3, "OI_EXPANDING")
        elif oi5 >= 0.05: add("oi5", 1)
        elif oi5 <= -0.30: add("oi5", -4, "OI_CONTRACTING")
    if breakout is True: add("breakout", 4, "BREAKOUT")
    if momentum is not None:
        add("momentum", max(-4.0, min(5.0, (momentum - 65.0) / 5.0)))
    if entry is not None:
        add("entry", max(-3.0, min(3.0, (entry - 70.0) / 6.0)))
    if rise is not None:
        add("rise", max(-3.0, min(3.0, (rise - 65.0) / 6.0)))

    final = int(round(max(0.0, min(100.0, score))))
    if final >= 80: reasons.append("RUNNER_SCORE_HIGH")
    elif final >= 70: reasons.append("RUNNER_SCORE_WATCH")
    else: reasons.append("RUNNER_SCORE_LOW")
    return final, contributions, reasons


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def migrate(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS runner_score_v1_shadow (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_key TEXT NOT NULL UNIQUE,
        symbol TEXT NOT NULL,
        stage TEXT NOT NULL,
        event_ts_ms INTEGER NOT NULL,
        episode_id INTEGER,
        signal_id INTEGER,
        price REAL,
        score INTEGER NOT NULL,
        model_version TEXT NOT NULL,
        raw_features_json TEXT NOT NULL,
        contributions_json TEXT NOT NULL,
        reason_codes_json TEXT NOT NULL,
        mfe_60_pct REAL NOT NULL DEFAULT 0,
        mae_60_pct REAL NOT NULL DEFAULT 0,
        last_observed_ms INTEGER,
        observation_gap INTEGER NOT NULL DEFAULT 0,
        outcome_label TEXT,
        outcome_ts_ms INTEGER
    );
    CREATE INDEX IF NOT EXISTS runner_score_v1_symbol_time ON runner_score_v1_shadow(symbol,event_ts_ms);
    CREATE INDEX IF NOT EXISTS runner_score_v1_stage_score ON runner_score_v1_shadow(stage,score);
    CREATE INDEX IF NOT EXISTS runner_score_v1_outcome ON runner_score_v1_shadow(outcome_label,event_ts_ms);

    CREATE TABLE IF NOT EXISTS runner_watch_v1_shadow (
        watch_id TEXT PRIMARY KEY,
        parent_watch_id TEXT,
        score_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        kind TEXT NOT NULL,
        anchor_ts_ms INTEGER NOT NULL,
        anchor_price REAL NOT NULL,
        anchor_score INTEGER NOT NULL,
        expires_ts_ms INTEGER NOT NULL,
        outcome_due_ts_ms INTEGER NOT NULL,
        state TEXT NOT NULL,
        decision TEXT,
        decision_ts_ms INTEGER,
        decision_price REAL,
        reason_codes_json TEXT NOT NULL,
        t15_json TEXT,
        t30_json TEXT,
        mfe_pct REAL NOT NULL DEFAULT 0,
        mae_pct REAL NOT NULL DEFAULT 0,
        last_observed_ms INTEGER,
        observation_gap INTEGER NOT NULL DEFAULT 0,
        notify_state TEXT NOT NULL DEFAULT 'NOT_REQUESTED',
        classic_premium_ts_ms INTEGER,
        created_ts_ms INTEGER NOT NULL,
        updated_ts_ms INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS runner_watch_v1_score_kind ON runner_watch_v1_shadow(score_id,kind);
    CREATE INDEX IF NOT EXISTS runner_watch_v1_symbol_state ON runner_watch_v1_shadow(symbol,state,anchor_ts_ms);

    CREATE TABLE IF NOT EXISTS non_runner_veto_v1_shadow (
        watch_id TEXT NOT NULL,
        horizon_s INTEGER NOT NULL,
        observed_ts_ms INTEGER NOT NULL,
        price REAL NOT NULL,
        return_pct REAL NOT NULL,
        mfe_pct REAL NOT NULL,
        mae_pct REAL NOT NULL,
        decision TEXT NOT NULL,
        reason_codes_json TEXT NOT NULL,
        metrics_json TEXT NOT NULL,
        model_version TEXT NOT NULL,
        PRIMARY KEY(watch_id,horizon_s)
    );

    CREATE TABLE IF NOT EXISTS runner_watch_outcome_v1_shadow (
        watch_id TEXT PRIMARY KEY,
        matured_ts_ms INTEGER NOT NULL,
        mfe_pct REAL NOT NULL,
        mae_pct REAL NOT NULL,
        label TEXT NOT NULL,
        observation_gap INTEGER NOT NULL,
        model_version TEXT NOT NULL
    );
    """)


class TelegramShadowNotifier:
    def __init__(self):
        self.enabled = RUNNER_SCORE_V1_NOTIFY
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = (os.getenv("TELEGRAM_ADMIN_CHAT_ID", "").strip()
                        or os.getenv("TELEGRAM_CHAT_ID", "").strip())
        self._queue: "queue.Queue[str]" = queue.Queue(maxsize=200)
        self._started = False
        self._lock = threading.Lock()

    def send(self, text: str) -> bool:
        if not (self.enabled and self.token and self.chat_id):
            return False
        try:
            self._queue.put_nowait(text)
        except queue.Full:
            return False
        with self._lock:
            if not self._started:
                self._started = True
                threading.Thread(target=self._worker, name="runner-shadow-telegram", daemon=True).start()
        return True

    def _worker(self) -> None:
        while True:
            text = self._queue.get()
            try:
                body = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text}).encode()
                req = urllib.request.Request(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=12) as response:
                    response.read(64)
            except Exception:
                pass
            finally:
                self._queue.task_done()


@dataclass
class ScorePath:
    id: int
    source_key: str
    symbol: str
    stage: str
    event_ts_ms: int
    price: float
    score: int
    mfe: float = 0.0
    mae: float = 0.0
    last_observed_ms: Optional[int] = None
    last_saved_ms: int = 0
    gap: int = 0


@dataclass
class Watch:
    watch_id: str
    score_id: int
    symbol: str
    kind: str
    anchor_ts_ms: int
    anchor_price: float
    anchor_score: int
    expires_ts_ms: int
    outcome_due_ts_ms: int
    state: str
    parent_watch_id: Optional[str] = None
    reasons: list[str] = field(default_factory=list)
    mfe: float = 0.0
    mae: float = 0.0
    last_observed_ms: Optional[int] = None
    t15_done: bool = False
    t30_done: bool = False
    gap: int = 0


class RunnerShadowV1:
    def __init__(self, connect, notifier: Optional[TelegramShadowNotifier] = None):
        self.connect = connect
        self.enabled = RUNNER_SCORE_V1_ENABLED
        self.notifier = notifier or TelegramShadowNotifier()
        self.score_paths: Dict[int, ScorePath] = {}
        self.score_by_symbol: Dict[str, set[int]] = defaultdict(set)
        self.watches: Dict[str, Watch] = {}
        self.watch_by_symbol: Dict[str, set[str]] = defaultdict(set)
        if not self.enabled:
            return
        now = int(time.time() * 1000)
        with self.connect() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                "SELECT * FROM runner_score_v1_shadow WHERE outcome_label IS NULL AND event_ts_ms>=?",
                (now - (RUNNER_OUTCOME_HORIZON_S + RUNNER_DATA_GRACE_S) * 1000,),
            ).fetchall()
            for r in rows:
                c.execute("UPDATE runner_score_v1_shadow SET observation_gap=1 WHERE id=?", (r["id"],))
                p = ScorePath(r["id"], r["source_key"], r["symbol"], r["stage"], r["event_ts_ms"],
                              float(r["price"] or 0), int(r["score"]), float(r["mfe_60_pct"] or 0),
                              float(r["mae_60_pct"] or 0), r["last_observed_ms"], now, 1)
                if p.price > 0:
                    self.score_paths[p.id] = p
                    self.score_by_symbol[p.symbol].add(p.id)
            rows = c.execute(
                "SELECT * FROM runner_watch_v1_shadow WHERE outcome_due_ts_ms>=? AND watch_id NOT IN "
                "(SELECT watch_id FROM runner_watch_outcome_v1_shadow)",
                (now - RUNNER_DATA_GRACE_S * 1000,),
            ).fetchall()
            for r in rows:
                c.execute("UPDATE runner_watch_v1_shadow SET observation_gap=1 WHERE watch_id=?", (r["watch_id"],))
                w = Watch(r["watch_id"], r["score_id"], r["symbol"], r["kind"], r["anchor_ts_ms"],
                          float(r["anchor_price"]), int(r["anchor_score"]), r["expires_ts_ms"],
                          r["outcome_due_ts_ms"], r["state"], r["parent_watch_id"],
                          json.loads(r["reason_codes_json"] or "[]"), float(r["mfe_pct"] or 0),
                          float(r["mae_pct"] or 0), r["last_observed_ms"], bool(r["t15_json"]),
                          bool(r["t30_json"]), 1)
                self.watches[w.watch_id] = w
                self.watch_by_symbol[w.symbol].add(w.watch_id)

    def _merge_candidate_event(self, symbol: str, event_ts_ms: int, features: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(features or {})
        try:
            with self.connect() as c:
                c.row_factory = sqlite3.Row
                sec = event_ts_ms // 1000
                row = c.execute(
                    "SELECT * FROM candidate_events WHERE symbol=? AND ts BETWEEN ? AND ? ORDER BY ABS(ts-?),id DESC LIMIT 1",
                    (symbol, sec - 2, sec + 2, sec),
                ).fetchone()
                if row:
                    aliases = {
                        "score": "score", "chg30": "chg30", "chg60": "chg60", "chg5": "chg5",
                        "flow30": "flow30", "buy30": "buy30", "book_imbalance": "book_imbalance",
                        "rel30": "rel30", "breakout": "breakout", "candidate_age_s": "candidate_age_s",
                        "confirm_passes": "confirm_passes", "gainer_rank": "gainer_rank", "qv24": "qv24",
                    }
                    for dst, src in aliases.items():
                        if merged.get(dst) is None and row[src] is not None:
                            merged[dst] = row[src]
        except sqlite3.Error:
            pass
        return merged

    def on_stage(self, source_key: str, symbol: str, stage: str, *, event_ts_ms: int,
                 episode_id: int = 0, signal_id: Optional[int] = None,
                 price: Optional[float] = None, features: Optional[Dict[str, Any]] = None) -> Optional[int]:
        if not self.enabled:
            return None
        stage = str(stage).upper()
        merged = self._merge_candidate_event(symbol, event_ts_ms, dict(features or {}))
        px = _finite(price) or _first(merged, "price", "entry_price")
        if not px or px <= 0:
            return None
        score, contributions, reasons = score_features(merged)
        with self.connect() as c:
            c.row_factory = sqlite3.Row
            row = c.execute("SELECT id FROM runner_score_v1_shadow WHERE source_key=?", (source_key,)).fetchone()
            if row:
                return int(row["id"])
            cur = c.execute(
                "INSERT INTO runner_score_v1_shadow(source_key,symbol,stage,event_ts_ms,episode_id,signal_id,price,score,model_version,raw_features_json,contributions_json,reason_codes_json,last_observed_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (source_key, symbol, stage, event_ts_ms, int(episode_id or 0), signal_id, px, score,
                 MODEL_VERSION, _json(merged), _json(contributions), _json(reasons), event_ts_ms),
            )
            score_id = int(cur.lastrowid)
        path = ScorePath(score_id, source_key, symbol, stage, event_ts_ms, px, score, last_observed_ms=event_ts_ms)
        self.score_paths[score_id] = path
        self.score_by_symbol[symbol].add(score_id)
        if stage == "PREMIUM":
            self._mark_classic_premium(symbol, event_ts_ms)
        elif stage == "EARLY" and score >= RUNNER_WATCH_MIN_SCORE:
            self._create_watch(path, "FAST", reasons)
        elif stage == "CANDIDATE":
            self._maybe_reacquire(path, merged, reasons)
        return score_id

    def _create_watch(self, path: ScorePath, kind: str, reasons: Iterable[str], parent: Optional[Watch] = None) -> Optional[Watch]:
        now = int(time.time() * 1000)
        watch_id = uuid.uuid4().hex
        expires = path.event_ts_ms + RUNNER_REACQUIRE_WINDOW_S * 1000
        outcome_due = path.event_ts_ms + RUNNER_OUTCOME_HORIZON_S * 1000
        try:
            with self.connect() as c:
                existing = c.execute("SELECT watch_id FROM runner_watch_v1_shadow WHERE score_id=? AND kind=?", (path.id, kind)).fetchone()
                if existing:
                    return self.watches.get(existing[0])
                c.execute(
                    "INSERT INTO runner_watch_v1_shadow(watch_id,parent_watch_id,score_id,symbol,kind,anchor_ts_ms,anchor_price,anchor_score,expires_ts_ms,outcome_due_ts_ms,state,reason_codes_json,mfe_pct,mae_pct,last_observed_ms,created_ts_ms,updated_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (watch_id, parent.watch_id if parent else None, path.id, path.symbol, kind, path.event_ts_ms,
                     path.price, path.score, expires, outcome_due, "WAIT_15", _json(list(reasons)), 0.0, 0.0,
                     path.event_ts_ms, now, now),
                )
        except sqlite3.IntegrityError:
            return None
        w = Watch(watch_id, path.id, path.symbol, kind, path.event_ts_ms, path.price, path.score,
                  expires, outcome_due, "WAIT_15", parent.watch_id if parent else None, list(reasons),
                  last_observed_ms=path.event_ts_ms)
        self.watches[watch_id] = w
        self.watch_by_symbol[path.symbol].add(watch_id)
        return w

    def _maybe_reacquire(self, path: ScorePath, features: Dict[str, Any], reasons: list[str]) -> None:
        if path.score < RUNNER_REACQUIRE_MIN_SCORE:
            return
        rank = _first(features, "gainer_rank", "gainers_rank", "rank")
        rel30 = _first(features, "rel30", "btc_rel30", "relative30")
        chg30 = _first(features, "chg30", "chg_30s", "chg30s")
        if rank is not None and rank > 30:
            return
        if rel30 is not None and rel30 < 0.40:
            return
        if chg30 is not None and chg30 < 0.40:
            return
        eligible = []
        for wid in list(self.watch_by_symbol.get(path.symbol, ())):
            w = self.watches.get(wid)
            if not w or w.kind != "FAST":
                continue
            if path.event_ts_ms <= w.anchor_ts_ms + 30000 or path.event_ts_ms > w.expires_ts_ms:
                continue
            if w.state not in ("WATCH_REACQUIRE", "BLOCK", "EXPIRED_REACQUIRE"):
                continue
            eligible.append(w)
        if not eligible:
            return
        parent = max(eligible, key=lambda w: w.anchor_ts_ms)
        self._create_watch(path, "REACQUIRE", list(reasons) + ["REACQUIRE_AFTER_EARLY", f"PARENT_{parent.state}"], parent)

    def _mark_classic_premium(self, symbol: str, ts_ms: int) -> None:
        ids = []
        for wid in list(self.watch_by_symbol.get(symbol, ())):
            w = self.watches.get(wid)
            if w and ts_ms >= w.anchor_ts_ms and ts_ms <= w.expires_ts_ms:
                ids.append(w.watch_id)
        if ids:
            with self.connect() as c:
                c.executemany(
                    "UPDATE runner_watch_v1_shadow SET classic_premium_ts_ms=COALESCE(classic_premium_ts_ms,?),updated_ts_ms=? WHERE watch_id=?",
                    [(ts_ms, int(time.time() * 1000), wid) for wid in ids],
                )

    @staticmethod
    def _metrics(w: Watch, price: float, observed_ms: int) -> Dict[str, Any]:
        ret = (price / w.anchor_price - 1.0) * 100.0
        return {"age_s": round((observed_ms - w.anchor_ts_ms) / 1000.0, 3),
                "return_pct": round(ret, 5), "mfe_pct": round(w.mfe, 5), "mae_pct": round(w.mae, 5)}

    @staticmethod
    def _decision_15(w: Watch, metrics: Dict[str, Any]) -> Tuple[str, list[str]]:
        ret, mfe, mae = metrics["return_pct"], metrics["mfe_pct"], metrics["mae_pct"]
        if ret <= -0.45 and mfe < 0.15:
            return "BLOCK", ["15S_REJECTION", "NO_PROGRESS"]
        allow_score = RUNNER_FAST_ALLOW_SCORE if w.kind == "FAST" else max(RUNNER_REACQUIRE_MIN_SCORE + 4, 76)
        if w.anchor_score >= allow_score and ret >= 0.12 and mfe >= 0.25 and mae > -0.45:
            return "ALLOW", ["15S_PROGRESS", "MFE_BUILDING", "DRAWDOWN_CONTROLLED"]
        return "WAIT_30", ["15S_INCONCLUSIVE"]

    @staticmethod
    def _decision_30(w: Watch, metrics: Dict[str, Any]) -> Tuple[str, list[str]]:
        ret, mfe, mae = metrics["return_pct"], metrics["mfe_pct"], metrics["mae_pct"]
        if ret <= -0.25 and mfe < 0.20:
            return "BLOCK", ["30S_REJECTION", "NO_NEW_PROGRESS"]
        if mfe < 0.12:
            return "BLOCK", ["30S_NO_PROGRESS"]
        if w.anchor_score >= 74 and mfe >= 0.40 and ret >= 0.03 and mae > -0.75:
            return "ALLOW", ["30S_RESCUE", "MFE_PROGRESS", "DRAWDOWN_ACCEPTABLE"]
        if w.anchor_score >= 78 and ret >= 0.15 and mfe >= 0.30 and mae > -0.60:
            return "ALLOW", ["30S_PROGRESS", "DRAWDOWN_CONTROLLED"]
        return "WATCH_REACQUIRE", ["30S_NOT_CONFIRMED", "KEEP_WAVE_MEMORY"]

    def on_tick(self, symbol: str, price: float, event_ms: int, observed_ms: int) -> None:
        if not self.enabled or price <= 0:
            return
        self._update_score_paths(symbol, price, event_ms, observed_ms)
        self._update_watches(symbol, price, event_ms, observed_ms)

    def _update_score_paths(self, symbol: str, price: float, event_ms: int, observed_ms: int) -> None:
        ids = list(self.score_by_symbol.get(symbol, ()))
        if not ids:
            return
        done = []
        updates = []
        for sid in ids:
            p = self.score_paths.get(sid)
            if not p or event_ms <= p.event_ts_ms:
                continue
            ret = (price / p.price - 1.0) * 100.0
            p.mfe = max(p.mfe, ret)
            p.mae = min(p.mae, ret)
            p.last_observed_ms = observed_ms
            due = observed_ms >= p.event_ts_ms + RUNNER_OUTCOME_HORIZON_S * 1000
            if due:
                label = "GAPPED" if p.gap else "RUNNER" if p.mfe >= 6.0 else "NON_RUNNER" if p.mfe < 2.0 else "GRAY"
                updates.append((p.mfe, p.mae, observed_ms, p.gap, label, observed_ms, p.id))
                done.append(p.id)
            elif observed_ms - p.last_saved_ms >= RUNNER_PERSIST_INTERVAL_MS:
                updates.append((p.mfe, p.mae, observed_ms, p.gap, None, None, p.id))
                p.last_saved_ms = observed_ms
        if updates:
            with self.connect() as c:
                c.executemany(
                    "UPDATE runner_score_v1_shadow SET mfe_60_pct=?,mae_60_pct=?,last_observed_ms=?,observation_gap=?,outcome_label=COALESCE(?,outcome_label),outcome_ts_ms=COALESCE(?,outcome_ts_ms) WHERE id=?",
                    updates,
                )
        for sid in done:
            p = self.score_paths.pop(sid, None)
            if p:
                self.score_by_symbol[p.symbol].discard(sid)

    def _update_watches(self, symbol: str, price: float, event_ms: int, observed_ms: int) -> None:
        ids = list(self.watch_by_symbol.get(symbol, ()))
        if not ids:
            return
        for wid in ids:
            w = self.watches.get(wid)
            if not w or event_ms <= w.anchor_ts_ms:
                continue
            ret = (price / w.anchor_price - 1.0) * 100.0
            w.mfe = max(w.mfe, ret)
            w.mae = min(w.mae, ret)
            w.last_observed_ms = observed_ms
            if w.gap and w.state in ("WAIT_15", "WAIT_30"):
                w.state = "WATCH_REACQUIRE"
                self._persist_watch(w, ["RESTART_DATA_GAP", "KEEP_REACQUIRE_ONLY"])
            age = observed_ms - w.anchor_ts_ms
            if not w.t15_done and age >= 15000 and not w.gap:
                metrics = self._metrics(w, price, observed_ms)
                decision, reasons = self._decision_15(w, metrics)
                self._record_veto(w, 15, observed_ms, price, metrics, decision, reasons)
                w.t15_done = True
                if decision == "ALLOW":
                    self._allow(w, observed_ms, price, reasons, metrics)
                elif decision == "BLOCK":
                    w.state = "WATCH_REACQUIRE"
                    self._persist_watch(w, reasons, decision="BLOCK")
                else:
                    w.state = "WAIT_30"
                    self._persist_watch(w, reasons)
            if not w.t30_done and age >= 30000 and w.state in ("WAIT_30", "WAIT_15") and not w.gap:
                metrics = self._metrics(w, price, observed_ms)
                decision, reasons = self._decision_30(w, metrics)
                self._record_veto(w, 30, observed_ms, price, metrics, decision, reasons)
                w.t30_done = True
                if decision == "ALLOW":
                    self._allow(w, observed_ms, price, reasons, metrics)
                else:
                    w.state = "WATCH_REACQUIRE"
                    self._persist_watch(w, reasons, decision="BLOCK" if decision == "BLOCK" else None)
            if observed_ms >= w.expires_ts_ms and w.state == "WATCH_REACQUIRE":
                w.state = "EXPIRED_REACQUIRE"
                self._persist_watch(w, ["REACQUIRE_WINDOW_EXPIRED"])
            if observed_ms >= w.outcome_due_ts_ms:
                self._finalize_watch(w, observed_ms)

    def _record_veto(self, w: Watch, horizon_s: int, observed_ms: int, price: float,
                     metrics: Dict[str, Any], decision: str, reasons: list[str]) -> None:
        with self.connect() as c:
            c.execute(
                "INSERT OR IGNORE INTO non_runner_veto_v1_shadow(watch_id,horizon_s,observed_ts_ms,price,return_pct,mfe_pct,mae_pct,decision,reason_codes_json,metrics_json,model_version) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (w.watch_id, horizon_s, observed_ms, price, metrics["return_pct"], metrics["mfe_pct"],
                 metrics["mae_pct"], decision, _json(reasons), _json(metrics), VETO_VERSION),
            )
            col = "t15_json" if horizon_s == 15 else "t30_json"
            c.execute(f"UPDATE runner_watch_v1_shadow SET {col}=?,updated_ts_ms=? WHERE watch_id=?",
                      (_json({"decision": decision, "reasons": reasons, **metrics}), observed_ms, w.watch_id))

    def _persist_watch(self, w: Watch, reasons: list[str], decision: Optional[str] = None) -> None:
        w.reasons = list(dict.fromkeys(w.reasons + list(reasons)))
        with self.connect() as c:
            c.execute(
                "UPDATE runner_watch_v1_shadow SET state=?,decision=COALESCE(?,decision),reason_codes_json=?,mfe_pct=?,mae_pct=?,last_observed_ms=?,observation_gap=?,updated_ts_ms=? WHERE watch_id=?",
                (w.state, decision, _json(w.reasons), w.mfe, w.mae, w.last_observed_ms, w.gap,
                 int(time.time() * 1000), w.watch_id),
            )

    def _allow(self, w: Watch, observed_ms: int, price: float, reasons: list[str], metrics: Dict[str, Any]) -> None:
        w.state = "ALLOW"
        w.reasons = list(dict.fromkeys(w.reasons + list(reasons)))
        queued = self.notifier.send(self._message(w, price, metrics, reasons))
        with self.connect() as c:
            c.execute(
                "UPDATE runner_watch_v1_shadow SET state='ALLOW',decision='ALLOW',decision_ts_ms=?,decision_price=?,reason_codes_json=?,mfe_pct=?,mae_pct=?,last_observed_ms=?,notify_state=?,updated_ts_ms=? WHERE watch_id=?",
                (observed_ms, price, _json(w.reasons), w.mfe, w.mae, observed_ms,
                 "QUEUED" if queued else "NOT_CONFIGURED", observed_ms, w.watch_id),
            )

    def _message(self, w: Watch, price: float, metrics: Dict[str, Any], reasons: list[str]) -> str:
        title = "🧪 FAST RUNNER PREMIUM — SHADOW" if w.kind == "FAST" else "🧪 REACQUIRE PREMIUM — SHADOW"
        return (
            f"{title}\n\n{w.symbol}\nRunner Score: {w.anchor_score}/100\nShadow Premium: {price:.10g}\n"
            f"Teyit: {metrics['age_s']:.0f} sn | Δ {metrics['return_pct']:+.2f}% | MFE {metrics['mfe_pct']:+.2f}% | MAE {metrics['mae_pct']:+.2f}%\n"
            f"Karar: ALLOW\nNeden: {', '.join(reasons[:5])}\n\n"
            "⚠️ TEST / SHADOW — işlem açmaz, public kanala gönderilmez."
        )

    def _finalize_watch(self, w: Watch, observed_ms: int) -> None:
        label = "GAPPED" if w.gap else "RUNNER" if w.mfe >= 6.0 else "NON_RUNNER" if w.mfe < 2.0 else "GRAY"
        with self.connect() as c:
            c.execute(
                "INSERT OR REPLACE INTO runner_watch_outcome_v1_shadow(watch_id,matured_ts_ms,mfe_pct,mae_pct,label,observation_gap,model_version) VALUES(?,?,?,?,?,?,?)",
                (w.watch_id, observed_ms, w.mfe, w.mae, label, w.gap, MODEL_VERSION),
            )
            c.execute("UPDATE runner_watch_v1_shadow SET updated_ts_ms=? WHERE watch_id=?", (observed_ms, w.watch_id))
        self.watches.pop(w.watch_id, None)
        self.watch_by_symbol[w.symbol].discard(w.watch_id)

    def expire(self, observed_ms: int) -> None:
        if not self.enabled:
            return
        stale_scores = [p for p in self.score_paths.values()
                        if observed_ms >= p.event_ts_ms + (RUNNER_OUTCOME_HORIZON_S + RUNNER_DATA_GRACE_S) * 1000]
        if stale_scores:
            with self.connect() as c:
                for p in stale_scores:
                    c.execute(
                        "UPDATE runner_score_v1_shadow SET mfe_60_pct=?,mae_60_pct=?,observation_gap=1,outcome_label='GAPPED',outcome_ts_ms=? WHERE id=?",
                        (p.mfe, p.mae, observed_ms, p.id),
                    )
                    self.score_paths.pop(p.id, None)
                    self.score_by_symbol[p.symbol].discard(p.id)
        stale_watches = [w for w in self.watches.values()
                         if observed_ms >= w.outcome_due_ts_ms + RUNNER_DATA_GRACE_S * 1000]
        for w in stale_watches:
            w.gap = 1
            self._finalize_watch(w, observed_ms)
