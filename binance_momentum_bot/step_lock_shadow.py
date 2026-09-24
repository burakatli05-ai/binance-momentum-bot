"""Forward-only Early V1 step-lock research shadow.

This module never places orders, never calls Telegram, and never gates production
signals. It records the exact aggTrade path for public Early notifications.

Policy:
- initial protective stop: -2.00%
- first profit lock: +0.20%
- next lock: +0.50%
- then +0.25pp steps (+0.75, +1.00, ... +4.75)
- final full-position take profit: +5.00%
- no partial exits
- the lock is monotonic and can only move upward

All percentages are unlevered price-return percentage points.
"""
from contextlib import closing
import json
import math
import sqlite3


VERSION = "step-lock-v1-20260923"
EPS = 1e-9
STEP_LEVELS = (0.20, 0.50) + tuple(round(x / 100, 2) for x in range(75, 500, 25))
DEFAULT_INITIAL_STOP_PCT = -2.0
DEFAULT_FINAL_TP_PCT = 5.0
DEFAULT_COST_PCT = 0.14
MAX_RECEIVE_LAG_MS = 2000
MAX_EVENT_GAP_MS = 2000
POSTSTOP_WINDOW_MS = 60 * 60 * 1000
POSTSTOP_LEVELS = (0.0, 0.20, 0.50, 1.0, 2.0, 3.0, 5.0)


def _finite(value, name, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name}: finite number required")
    if positive and value <= 0:
        raise ValueError(f"{name}: positive number required")
    return float(value)


class StepLockShadow:
    """Durable, forward-only, full-position step-lock telemetry."""

    def __init__(self, connect, *, initial_stop_pct=DEFAULT_INITIAL_STOP_PCT,
                 final_tp_pct=DEFAULT_FINAL_TP_PCT, cost_pct=DEFAULT_COST_PCT):
        self.connect = connect
        self.initial_stop_pct = _finite(initial_stop_pct, "initial_stop_pct")
        self.final_tp_pct = _finite(final_tp_pct, "final_tp_pct", True)
        self.cost_pct = _finite(cost_pct, "cost_pct")
        if not self.initial_stop_pct < 0:
            raise ValueError("initial stop must be negative")
        if self.final_tp_pct <= STEP_LEVELS[-1]:
            raise ValueError("final target must be above last lock step")
        if self.cost_pct < 0:
            raise ValueError("cost must be nonnegative")
        self.active = {}
        self.by_symbol = {}
        self.poststop = {}
        self.poststop_by_symbol = {}
        self._init_db()
        self._recover()

    def _init_db(self):
        with closing(self.connect()) as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS early_step_lock_shadow_v1(
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    episode_id INTEGER,
                    entry_price REAL NOT NULL,
                    decision_ms INTEGER NOT NULL,
                    initial_stop_pct REAL NOT NULL,
                    final_tp_pct REAL NOT NULL,
                    current_lock_pct REAL,
                    peak_pct REAL NOT NULL DEFAULT 0,
                    trough_pct REAL NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    close_reason TEXT,
                    exit_event_ms INTEGER,
                    exit_observed_ms INTEGER,
                    exit_trade_id INTEGER,
                    exit_level_pct REAL,
                    observed_exit_pct REAL,
                    gross_pct REAL,
                    net_pct REAL,
                    data_flags TEXT NOT NULL DEFAULT '[]',
                    last_event_ms INTEGER,
                    last_received_ms INTEGER,
                    last_trade_id INTEGER,
                    version TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_step_lock_open_symbol
                    ON early_step_lock_shadow_v1(status,symbol);
                CREATE TABLE IF NOT EXISTS early_step_lock_events_v1(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL,
                    event_ms INTEGER NOT NULL,
                    observed_ms INTEGER NOT NULL,
                    trade_id INTEGER,
                    event TEXT NOT NULL,
                    level_pct REAL,
                    price REAL,
                    return_pct REAL,
                    details TEXT NOT NULL DEFAULT '{}',
                    version TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_step_lock_events_signal
                    ON early_step_lock_events_v1(signal_id,event_ms,id);
                CREATE TABLE IF NOT EXISTS early_step_lock_poststop_v1(
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_event_ms INTEGER NOT NULL,
                    watch_until_ms INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    peak_after_pct REAL NOT NULL,
                    trough_after_pct REAL NOT NULL,
                    trough_before_020_pct REAL NOT NULL,
                    first_positive_ms INTEGER,
                    first_020_ms INTEGER,
                    first_050_ms INTEGER,
                    first_100_ms INTEGER,
                    first_200_ms INTEGER,
                    first_300_ms INTEGER,
                    first_500_ms INTEGER,
                    first_minus3_ms INTEGER,
                    data_flags TEXT NOT NULL DEFAULT '[]',
                    last_event_ms INTEGER,
                    last_received_ms INTEGER,
                    last_trade_id INTEGER,
                    version TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_step_lock_poststop_symbol
                    ON early_step_lock_poststop_v1(status,symbol);
                """
            )
            db.commit()

    def _recover(self):
        with closing(self.connect()) as db:
            rows = db.execute(
                """SELECT signal_id,symbol,episode_id,entry_price,decision_ms,
                          initial_stop_pct,final_tp_pct,current_lock_pct,peak_pct,trough_pct,
                          data_flags,last_event_ms,last_received_ms,last_trade_id
                   FROM early_step_lock_shadow_v1 WHERE status='OPEN'"""
            ).fetchall()
        for row in rows:
            state = dict(
                signal_id=str(row[0]), symbol=row[1], episode_id=row[2],
                entry_price=float(row[3]), decision_ms=int(row[4]),
                initial_stop_pct=float(row[5]), final_tp_pct=float(row[6]),
                current_lock_pct=None if row[7] is None else float(row[7]),
                peak_pct=float(row[8]), trough_pct=float(row[9]),
                flags=set(json.loads(row[10] or "[]")),
                last_event_ms=row[11], last_received_ms=row[12], last_trade_id=row[13],
            )
            self._activate(state)
        with closing(self.connect()) as db:
            watches = db.execute(
                """SELECT signal_id,symbol,entry_price,exit_event_ms,watch_until_ms,
                          peak_after_pct,trough_after_pct,trough_before_020_pct,
                          first_positive_ms,first_020_ms,first_050_ms,first_100_ms,
                          first_200_ms,first_300_ms,first_500_ms,first_minus3_ms,
                          data_flags,last_event_ms,last_received_ms,last_trade_id
                   FROM early_step_lock_poststop_v1 WHERE status='WATCHING'"""
            ).fetchall()
        for row in watches:
            watch = dict(
                signal_id=str(row[0]), symbol=row[1], entry_price=float(row[2]),
                exit_event_ms=int(row[3]), watch_until_ms=int(row[4]),
                peak_after_pct=float(row[5]), trough_after_pct=float(row[6]),
                trough_before_020_pct=float(row[7]), first_positive_ms=row[8],
                first_020_ms=row[9], first_050_ms=row[10], first_100_ms=row[11],
                first_200_ms=row[12], first_300_ms=row[13], first_500_ms=row[14],
                first_minus3_ms=row[15], flags=set(json.loads(row[16] or "[]")),
                last_event_ms=row[17], last_received_ms=row[18], last_trade_id=row[19],
            )
            self._activate_poststop(watch)

    def _activate(self, state):
        self.active[state["signal_id"]] = state
        self.by_symbol.setdefault(state["symbol"], set()).add(state["signal_id"])

    def _deactivate(self, state):
        self.active.pop(state["signal_id"], None)
        ids = self.by_symbol.get(state["symbol"])
        if ids is not None:
            ids.discard(state["signal_id"])
            if not ids:
                self.by_symbol.pop(state["symbol"], None)

    def _activate_poststop(self, watch):
        self.poststop[watch["signal_id"]] = watch
        self.poststop_by_symbol.setdefault(watch["symbol"], set()).add(watch["signal_id"])

    def _deactivate_poststop(self, watch):
        self.poststop.pop(watch["signal_id"], None)
        ids = self.poststop_by_symbol.get(watch["symbol"])
        if ids is not None:
            ids.discard(watch["signal_id"])
            if not ids:
                self.poststop_by_symbol.pop(watch["symbol"], None)

    def _save_poststop(self, watch, status="WATCHING"):
        with closing(self.connect()) as db:
            db.execute(
                """UPDATE early_step_lock_poststop_v1
                   SET status=?,peak_after_pct=?,trough_after_pct=?,trough_before_020_pct=?,
                       first_positive_ms=?,first_020_ms=?,first_050_ms=?,first_100_ms=?,first_200_ms=?,
                       first_300_ms=?,first_500_ms=?,first_minus3_ms=?,data_flags=?,
                       last_event_ms=?,last_received_ms=?,last_trade_id=?
                   WHERE signal_id=?""",
                (status, watch["peak_after_pct"], watch["trough_after_pct"],
                 watch["trough_before_020_pct"], watch["first_positive_ms"],
                 watch["first_020_ms"], watch["first_050_ms"],
                 watch["first_100_ms"], watch["first_200_ms"], watch["first_300_ms"],
                 watch["first_500_ms"], watch["first_minus3_ms"],
                 json.dumps(sorted(watch["flags"])), watch["last_event_ms"],
                 watch["last_received_ms"], watch["last_trade_id"], watch["signal_id"]),
            )
            db.commit()

    def _arm_poststop(self, state, event_ms):
        watch = dict(
            signal_id=state["signal_id"], symbol=state["symbol"], entry_price=state["entry_price"],
            exit_event_ms=int(event_ms), watch_until_ms=int(event_ms) + POSTSTOP_WINDOW_MS,
            peak_after_pct=self.initial_stop_pct, trough_after_pct=self.initial_stop_pct,
            trough_before_020_pct=self.initial_stop_pct, first_positive_ms=None, first_020_ms=None, first_050_ms=None, first_100_ms=None,
            first_200_ms=None, first_300_ms=None, first_500_ms=None, first_minus3_ms=None,
            flags=set(state["flags"]), last_event_ms=state["last_event_ms"],
            last_received_ms=state["last_received_ms"], last_trade_id=state["last_trade_id"],
        )
        with closing(self.connect()) as db:
            db.execute(
                """INSERT OR IGNORE INTO early_step_lock_poststop_v1(
                    signal_id,symbol,entry_price,exit_event_ms,watch_until_ms,status,
                    peak_after_pct,trough_after_pct,trough_before_020_pct,data_flags,
                    last_event_ms,last_received_ms,last_trade_id,version)
                    VALUES (?,?,?,?,?,'WATCHING',?,?,?,?,?,?,?,?)""",
                (watch["signal_id"], watch["symbol"], watch["entry_price"], watch["exit_event_ms"],
                 watch["watch_until_ms"], watch["peak_after_pct"], watch["trough_after_pct"],
                 watch["trough_before_020_pct"], json.dumps(sorted(watch["flags"])), watch["last_event_ms"],
                 watch["last_received_ms"], watch["last_trade_id"], VERSION),
            )
            db.commit()
        self._activate_poststop(watch)

    def _tick_poststop(self, symbol, price, event_ms, received_ms, trade_id):
        for signal_id in list(self.poststop_by_symbol.get(symbol, ())):
            watch = self.poststop.get(signal_id)
            if not watch or event_ms <= watch["exit_event_ms"]:
                continue
            if event_ms > watch["watch_until_ms"]:
                self._save_poststop(watch, "DONE")
                self._deactivate_poststop(watch)
                continue
            if event_ms > received_ms or received_ms - event_ms > MAX_RECEIVE_LAG_MS:
                watch["flags"].add("STALE_OR_FUTURE_TRADE")
                self._save_poststop(watch)
                continue
            if watch["last_trade_id"] is not None:
                if trade_id <= watch["last_trade_id"] or event_ms < (watch["last_event_ms"] or 0):
                    watch["flags"].add("OUT_OF_ORDER_TRADE")
                    self._save_poststop(watch)
                    continue
                if trade_id != watch["last_trade_id"] + 1:
                    watch["flags"].add("MISSING_AGG_TRADE_IDS")
                if watch["last_event_ms"] is not None and event_ms - watch["last_event_ms"] > MAX_EVENT_GAP_MS:
                    watch["flags"].add("TRADE_OBSERVATION_GAP")
            ret = 100.0 * (price / watch["entry_price"] - 1.0)
            watch["peak_after_pct"] = max(watch["peak_after_pct"], ret)
            watch["trough_after_pct"] = min(watch["trough_after_pct"], ret)
            if watch["first_020_ms"] is None:
                watch["trough_before_020_pct"] = min(watch["trough_before_020_pct"], ret)
            level_fields = ((0.0,"first_positive_ms"),(0.20,"first_020_ms"),(0.50,"first_050_ms"),
                            (1.0,"first_100_ms"),(2.0,"first_200_ms"),(3.0,"first_300_ms"),
                            (5.0,"first_500_ms"))
            for level,field in level_fields:
                if watch[field] is None and ret + EPS >= level:
                    watch[field] = int(event_ms)
            if watch["first_minus3_ms"] is None and ret <= -3.0 + EPS:
                watch["first_minus3_ms"] = int(event_ms)
            watch["last_event_ms"] = int(event_ms)
            watch["last_received_ms"] = int(received_ms)
            watch["last_trade_id"] = int(trade_id)
            self._save_poststop(watch)

    def _event(self, state, event, event_ms, observed_ms, trade_id=None,
               level_pct=None, price=None, return_pct=None, details=None):
        with closing(self.connect()) as db:
            db.execute(
                """INSERT INTO early_step_lock_events_v1(
                    signal_id,event_ms,observed_ms,trade_id,event,level_pct,price,
                    return_pct,details,version) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (state["signal_id"], int(event_ms), int(observed_ms), trade_id, event,
                 level_pct, price, return_pct,
                 json.dumps(details or {}, sort_keys=True, allow_nan=False), VERSION),
            )
            db.commit()

    def _save_open(self, state):
        with closing(self.connect()) as db:
            db.execute(
                """UPDATE early_step_lock_shadow_v1
                   SET current_lock_pct=?,peak_pct=?,trough_pct=?,data_flags=?,
                       last_event_ms=?,last_received_ms=?,last_trade_id=?
                   WHERE signal_id=?""",
                (state["current_lock_pct"], state["peak_pct"], state["trough_pct"],
                 json.dumps(sorted(state["flags"])), state["last_event_ms"],
                 state["last_received_ms"], state["last_trade_id"], state["signal_id"]),
            )
            db.commit()

    def _flag(self, state, flag):
        if flag not in state["flags"]:
            state["flags"].add(flag)
            self._save_open(state)

    def arm(self, signal_id, symbol, entry_price, decision_ms, episode_id=None):
        signal_id = str(signal_id)
        entry_price = _finite(entry_price, "entry_price", True)
        if not symbol or not isinstance(symbol, str):
            raise ValueError("symbol required")
        if isinstance(decision_ms, bool) or not isinstance(decision_ms, int) or decision_ms <= 0:
            raise ValueError("decision_ms must be a positive integer")
        with closing(self.connect()) as db:
            existing = db.execute(
                "SELECT symbol,entry_price,decision_ms,status FROM early_step_lock_shadow_v1 WHERE signal_id=?",
                (signal_id,),
            ).fetchone()
            if existing:
                if existing[0] != symbol or not math.isclose(float(existing[1]), entry_price, rel_tol=1e-12) or int(existing[2]) != decision_ms:
                    raise ValueError("signal identity conflict")
                return False
            db.execute(
                """INSERT INTO early_step_lock_shadow_v1(
                    signal_id,symbol,episode_id,entry_price,decision_ms,initial_stop_pct,
                    final_tp_pct,current_lock_pct,peak_pct,trough_pct,status,data_flags,version)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (signal_id, symbol, episode_id, entry_price, decision_ms,
                 self.initial_stop_pct, self.final_tp_pct, None, 0.0, 0.0,
                 "OPEN", "[]", VERSION),
            )
            db.commit()
        state = dict(
            signal_id=signal_id, symbol=symbol, episode_id=episode_id,
            entry_price=entry_price, decision_ms=decision_ms,
            initial_stop_pct=self.initial_stop_pct, final_tp_pct=self.final_tp_pct,
            current_lock_pct=None, peak_pct=0.0, trough_pct=0.0, flags=set(),
            last_event_ms=None, last_received_ms=None, last_trade_id=None,
        )
        self._activate(state)
        self._event(state, "ARM", decision_ms, decision_ms, level_pct=self.initial_stop_pct,
                    price=entry_price, return_pct=0.0,
                    details={"final_tp_pct": self.final_tp_pct, "no_partial_exits": True})
        return True

    def _close(self, state, reason, price, ret, event_ms, observed_ms, trade_id, level_pct):
        # final TP is modeled as a resting full-position target at exactly +5%.
        gross = self.final_tp_pct if reason == "FINAL_TP" else ret
        net = gross - self.cost_pct
        with closing(self.connect()) as db:
            db.execute(
                """UPDATE early_step_lock_shadow_v1
                   SET status='CLOSED',close_reason=?,exit_event_ms=?,exit_observed_ms=?,
                       exit_trade_id=?,exit_level_pct=?,observed_exit_pct=?,gross_pct=?,net_pct=?,
                       peak_pct=?,trough_pct=?,data_flags=?,last_event_ms=?,last_received_ms=?,
                       last_trade_id=? WHERE signal_id=?""",
                (reason, int(event_ms), int(observed_ms), trade_id, level_pct, ret,
                 gross, net, state["peak_pct"], state["trough_pct"],
                 json.dumps(sorted(state["flags"])), state["last_event_ms"],
                 state["last_received_ms"], state["last_trade_id"], state["signal_id"]),
            )
            db.commit()
        self._event(state, "CLOSE", event_ms, observed_ms, trade_id,
                    level_pct=level_pct, price=price, return_pct=ret,
                    details={"reason": reason, "gross_pct": gross, "net_pct": net,
                             "cost_pct": self.cost_pct})
        if reason == "INITIAL_SL":
            self._arm_poststop(state, event_ms)
        self._deactivate(state)

    def tick(self, symbol, price, event_ms, received_ms, trade_id):
        price = _finite(price, "price", True)
        if isinstance(event_ms, bool) or not isinstance(event_ms, int):
            return
        if isinstance(received_ms, bool) or not isinstance(received_ms, int):
            return
        if isinstance(trade_id, bool) or not isinstance(trade_id, int) or trade_id < 0:
            return
        ids = list(self.by_symbol.get(symbol, ()))
        for signal_id in ids:
            state = self.active.get(signal_id)
            if not state or event_ms < state["decision_ms"]:
                continue
            if event_ms > received_ms or received_ms - event_ms > MAX_RECEIVE_LAG_MS:
                self._flag(state, "STALE_OR_FUTURE_TRADE")
                continue
            if state["last_received_ms"] is not None and received_ms < state["last_received_ms"]:
                self._flag(state, "RECEIVE_ORDER")
                continue
            if state["last_trade_id"] is not None:
                if trade_id <= state["last_trade_id"] or event_ms < state["last_event_ms"]:
                    self._flag(state, "OUT_OF_ORDER_TRADE")
                    continue
                if trade_id != state["last_trade_id"] + 1:
                    state["flags"].add("MISSING_AGG_TRADE_IDS")
                if event_ms - state["last_event_ms"] > MAX_EVENT_GAP_MS:
                    state["flags"].add("TRADE_OBSERVATION_GAP")

            ret = 100.0 * (price / state["entry_price"] - 1.0)
            state["peak_pct"] = max(state["peak_pct"], ret)
            state["trough_pct"] = min(state["trough_pct"], ret)
            state["last_event_ms"] = event_ms
            state["last_received_ms"] = received_ms
            state["last_trade_id"] = trade_id

            # Previously active protection is checked before a new ratchet from
            # this trade. That prevents favorable same-trade reordering.
            if ret + EPS >= state["final_tp_pct"]:
                self._close(state, "FINAL_TP", price, ret, event_ms, received_ms,
                            trade_id, state["final_tp_pct"])
                continue
            if state["current_lock_pct"] is not None:
                if ret <= state["current_lock_pct"] + EPS:
                    self._close(state, "STEP_LOCK", price, ret, event_ms, received_ms,
                                trade_id, state["current_lock_pct"])
                    continue
            elif ret <= state["initial_stop_pct"] + EPS:
                self._close(state, "INITIAL_SL", price, ret, event_ms, received_ms,
                            trade_id, state["initial_stop_pct"])
                continue

            eligible = [level for level in STEP_LEVELS if ret + EPS >= level]
            if eligible:
                new_lock = max(eligible)
                old = state["current_lock_pct"]
                if old is None or new_lock > old + EPS:
                    state["current_lock_pct"] = new_lock
                    self._event(state, "LOCK_ARM", event_ms, received_ms, trade_id,
                                level_pct=new_lock, price=price, return_pct=ret,
                                details={"previous_lock_pct": old})
            self._save_open(state)
        self._tick_poststop(symbol, price, event_ms, received_ms, trade_id)

    def summary(self, *, notional_usdt=2000.0, recent_limit=10):
        """Return a read-only aggregate snapshot for Telegram/research review."""
        notional_usdt = _finite(notional_usdt, "notional_usdt", True)
        if isinstance(recent_limit, bool) or not isinstance(recent_limit, int) or not 0 <= recent_limit <= 50:
            raise ValueError("recent_limit must be an integer between 0 and 50")
        with closing(self.connect()) as db:
            db.execute("PRAGMA query_only=ON")
            rows = db.execute(
                """SELECT signal_id,symbol,decision_ms,current_lock_pct,peak_pct,trough_pct,
                          status,close_reason,exit_level_pct,net_pct,data_flags,last_received_ms
                   FROM early_step_lock_shadow_v1 ORDER BY decision_ms DESC"""
            ).fetchall()
            armed = db.execute(
                """SELECT level_pct,COUNT(DISTINCT signal_id)
                   FROM early_step_lock_events_v1
                   WHERE event='LOCK_ARM' AND level_pct IS NOT NULL
                   GROUP BY level_pct ORDER BY level_pct"""
            ).fetchall()
        status_counts = {}
        reason_counts = {}
        exit_levels = {}
        open_locks = {}
        flag_counts = {}
        closed_net = []
        latest_ms = 0
        for row in rows:
            signal_id,symbol,decision_ms,current_lock,peak,trough,status,reason,exit_level,net_pct,flags_json,last_received_ms = row
            status_counts[status] = status_counts.get(status, 0) + 1
            if reason:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            if status == "CLOSED" and exit_level is not None:
                key = round(float(exit_level), 8)
                exit_levels[key] = exit_levels.get(key, 0) + 1
            if status == "OPEN":
                key = None if current_lock is None else round(float(current_lock), 8)
                open_locks[key] = open_locks.get(key, 0) + 1
            if net_pct is not None and status == "CLOSED":
                closed_net.append(float(net_pct))
            try:
                flags = json.loads(flags_json or "[]")
            except Exception:
                flags = ["INVALID_DATA_FLAGS_JSON"]
            for flag in flags:
                flag_counts[str(flag)] = flag_counts.get(str(flag), 0) + 1
            for stamp in (decision_ms, last_received_ms):
                if isinstance(stamp, (int, float)) and not isinstance(stamp, bool):
                    latest_ms = max(latest_ms, int(stamp))
        reached_levels = {}
        for level in STEP_LEVELS + (self.final_tp_pct,):
            reached_levels[round(float(level), 8)] = sum(float(row[4]) + EPS >= level for row in rows)
        armed_levels = {round(float(level), 8): int(count) for level,count in armed}
        total_net_pct = sum(closed_net)
        closed_count = len(closed_net)
        recent = []
        for row in rows[:recent_limit]:
            signal_id,symbol,decision_ms,current_lock,peak,trough,status,reason,exit_level,net_pct,flags_json,last_received_ms = row
            try:
                flags = json.loads(flags_json or "[]")
            except Exception:
                flags = ["INVALID_DATA_FLAGS_JSON"]
            recent.append(dict(
                signal_id=str(signal_id), symbol=symbol, decision_ms=int(decision_ms),
                current_lock_pct=None if current_lock is None else float(current_lock),
                peak_pct=float(peak), trough_pct=float(trough), status=status,
                close_reason=reason, exit_level_pct=None if exit_level is None else float(exit_level),
                net_pct=None if net_pct is None else float(net_pct), data_flags=flags,
                last_received_ms=None if last_received_ms is None else int(last_received_ms),
            ))
        return dict(
            version=VERSION, total=len(rows), open=status_counts.get("OPEN", 0),
            closed=status_counts.get("CLOSED", 0), status_counts=status_counts,
            close_reason_counts=reason_counts, exit_level_counts=exit_levels,
            open_lock_counts=open_locks, reached_level_counts=reached_levels,
            armed_level_counts=armed_levels,
            flagged_signals=sum(1 for row in rows if (row[10] or "[]") != "[]"),
            flag_counts=flag_counts, latest_ms=latest_ms,
            closed_net_pct_sum=total_net_pct,
            closed_net_pct_avg=(total_net_pct / closed_count if closed_count else None),
            notional_usdt=notional_usdt,
            closed_net_usdt=total_net_pct * notional_usdt / 100.0,
            closed_net_usdt_avg=(total_net_pct * notional_usdt / 100.0 / closed_count if closed_count else None),
            recent=recent,
        )

    def initial_sl_recovery_review(self, *, limit=20):
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer between 1 and 100")
        with closing(self.connect()) as db:
            db.execute("PRAGMA query_only=ON")
            losses = db.execute(
                """SELECT signal_id,symbol,decision_ms,exit_event_ms,peak_pct,trough_pct,
                          observed_exit_pct,net_pct,data_flags
                   FROM early_step_lock_shadow_v1
                   WHERE status='CLOSED' AND close_reason='INITIAL_SL'
                   ORDER BY decision_ms DESC LIMIT ?""",
                (int(limit),),
            ).fetchall()
            stages = db.execute(
                """SELECT id,symbol,created_ts_ms,mfe_pct,mae_pct,completed_60m
                   FROM entry_stage_forward_shadow WHERE stage='EARLY'"""
            ).fetchall()
            stage_by_symbol = {}
            for st in stages:
                stage_by_symbol.setdefault(st[1], []).append(st)
            radar = {}
            post = {}
            for row in losses:
                rid = int(row[0])
                radar[rid] = db.execute(
                    "SELECT ts,notify_ts,price FROM radar_signals WHERE id=?", (rid,)
                ).fetchone()
                post[str(row[0])] = db.execute(
                    """SELECT status,peak_after_pct,trough_after_pct,trough_before_020_pct,
                              first_positive_ms,first_020_ms,first_050_ms,first_100_ms,
                              first_200_ms,first_300_ms,first_500_ms,first_minus3_ms,data_flags
                       FROM early_step_lock_poststop_v1 WHERE signal_id=?""",
                    (str(row[0]),),
                ).fetchone()
        used=set()
        items=[]
        for row in losses:
            signal_id,symbol,decision_ms,exit_event_ms,peak,trough,observed_exit,net_pct,flags_json=row
            try:
                flags=json.loads(flags_json or "[]")
            except Exception:
                flags=["INVALID_DATA_FLAGS_JSON"]
            candidates=[st for st in stage_by_symbol.get(symbol,())
                        if st[0] not in used and abs(int(st[2])-int(decision_ms)) <= 5000]
            stage=min(candidates,key=lambda st:abs(int(st[2])-int(decision_ms))) if candidates else None
            if stage:
                used.add(stage[0])
                _,_,stage_ms,stage_mfe,stage_mae,stage_done=stage
                stage_mfe=None if stage_mfe is None else float(stage_mfe)
                stage_mae=None if stage_mae is None else float(stage_mae)
            else:
                stage_ms=stage_mfe=stage_mae=stage_done=None
            historical='UNKNOWN'
            if stage_mfe is not None and stage_mae is not None:
                if stage_mfe + EPS >= 0.20 and stage_mae > -3.0 + EPS:
                    historical='WOULD_SURVIVE_MINUS3_AND_REACH_020'
                elif stage_mfe + EPS < 0.20 and stage_mae <= -3.0 + EPS:
                    historical='WOULD_HIT_MINUS3_NO_020'
                elif stage_mfe + EPS < 0.20 and stage_mae > -3.0 + EPS:
                    historical='SURVIVES_MINUS3_BUT_NO_020_WITHIN_60M'
                else:
                    historical='ORDER_UNKNOWN_BOTH_MINUS3_AND_020_OCCUR'
            postrow=post.get(str(signal_id))
            exact=None
            if postrow:
                (pstatus,ppeak,ptrough,ptrough020,p0,p020,p050,p100,p200,p300,p500,pminus3,pflags)=postrow
                if p020 is not None and (pminus3 is None or int(p020) < int(pminus3)):
                    verdict='YES'
                elif pminus3 is not None and (p020 is None or int(pminus3) < int(p020)):
                    verdict='NO'
                else:
                    verdict='PENDING'
                exact=dict(
                    status=pstatus,peak_after_pct=float(ppeak),trough_after_pct=float(ptrough),
                    trough_before_020_pct=float(ptrough020),first_positive_ms=p0,first_020_ms=p020,
                    first_050_ms=p050,first_100_ms=p100,first_200_ms=p200,first_300_ms=p300,
                    first_500_ms=p500,first_minus3_ms=pminus3,minus3_would_save_to_020=verdict,
                    data_flags=json.loads(pflags or '[]'))
            r=radar.get(int(signal_id))
            radar_created_ms=(int(r[0])*1000 if r and r[0] is not None else None)
            radar_notify_ms=(int(r[1])*1000 if r and r[1] is not None else None)
            items.append(dict(
                signal_id=str(signal_id),symbol=symbol,decision_ms=int(decision_ms),
                exit_event_ms=None if exit_event_ms is None else int(exit_event_ms),
                pre_stop_peak_pct=float(peak),pre_stop_trough_pct=float(trough),
                observed_exit_pct=None if observed_exit is None else float(observed_exit),
                net_pct=None if net_pct is None else float(net_pct),data_flags=flags,
                radar_created_ms=radar_created_ms,radar_notify_ms=radar_notify_ms,
                radar_lead_before_public_early_s=(None if radar_created_ms is None else (int(decision_ms)-radar_created_ms)/1000.0),
                stage_matched=bool(stage),stage_time_delta_ms=(None if stage_ms is None else int(stage_ms)-int(decision_ms)),
                public_early_mfe_60m=stage_mfe,public_early_mae_60m=stage_mae,
                public_early_completed_60m=bool(stage_done) if stage is not None else False,
                historical_minus3_assessment=historical,exact_poststop=exact))
        return dict(
            total=len(items),initial_stop_pct=self.initial_stop_pct,
            note='Historical counterfactuals use PUBLIC_EARLY-aligned stage data; radar_outcomes are intentionally excluded because radar starts before public Early. Exact post-stop order is available only for new watches.',
            items=items)

    def runner_review(self, *, exit_level_pct=0.20, recent_limit=100):
        """Read-only review of Step Lock exits against the existing 60m EARLY forward cohort.

        Step Lock is armed from the Early adapter immediately after the production
        EARLY stage row is created, but historical Step Lock rows do not persist
        episode_id. Match by symbol + nearest EARLY-stage timestamp within 5 seconds.
        """
        exit_level_pct = _finite(exit_level_pct, "exit_level_pct")
        if isinstance(recent_limit, bool) or not isinstance(recent_limit, int) or not 1 <= recent_limit <= 500:
            raise ValueError("recent_limit must be an integer between 1 and 500")
        with closing(self.connect()) as db:
            db.execute("PRAGMA query_only=ON")
            exits = db.execute(
                """SELECT signal_id,symbol,episode_id,decision_ms,exit_event_ms,
                          exit_level_pct,observed_exit_pct,net_pct,peak_pct,trough_pct,data_flags
                   FROM early_step_lock_shadow_v1
                   WHERE status='CLOSED'
                     AND close_reason='STEP_LOCK'
                     AND ABS(COALESCE(exit_level_pct,999)-?) < 0.000001
                   ORDER BY decision_ms DESC
                   LIMIT ?""",
                (float(exit_level_pct), int(recent_limit)),
            ).fetchall()
            if exits:
                symbols = sorted({row[1] for row in exits})
                lo = min(int(row[3]) for row in exits) - 5000
                hi = max(int(row[3]) for row in exits) + 5000
                placeholders = ",".join("?" for _ in symbols)
                stage_cols = {r[1] for r in db.execute("PRAGMA table_info(entry_stage_forward_shadow)").fetchall()}
                opt = lambda name: name if name in stage_cols else f"NULL AS {name}"
                stages = db.execute(
                    f"""SELECT id,symbol,episode_id,created_ts_ms,entry_price,mfe_pct,mae_pct,
                               close60_price,completed_60m,{opt('tp1_price')},{opt('tp1_hit_s')},
                               {opt('tp2_price')},{opt('tp2_hit_s')}
                        FROM entry_stage_forward_shadow
                        WHERE stage='EARLY' AND symbol IN ({placeholders})
                          AND created_ts_ms BETWEEN ? AND ?
                        ORDER BY created_ts_ms""",
                    tuple(symbols) + (lo, hi),
                ).fetchall()
            else:
                stages = []
            p0_forward = {}
            p0_outcomes = {}
            if stages:
                source_keys = [f"stage:{int(st[0])}" for st in stages]
                ph = ",".join("?" for _ in source_keys)
                try:
                    for prow in db.execute(
                        f"""SELECT source_key,allow_reference_price,fill_price,fill_event_ts_ms
                            FROM p0_forward WHERE source_key IN ({ph})""",
                        tuple(source_keys),
                    ).fetchall():
                        p0_forward[str(prow[0])] = prow
                    for orow in db.execute(
                        f"""SELECT source_key,horizon_s,mfe_pct,mae_pct,peak_ts_ms,gap,missing_reason
                            FROM p0_forward_outcomes WHERE source_key IN ({ph})
                            ORDER BY source_key,horizon_s""",
                        tuple(source_keys),
                    ).fetchall():
                        p0_outcomes.setdefault(str(orow[0]), []).append(orow)
                except sqlite3.OperationalError:
                    pass
        by_symbol = {}
        for stage in stages:
            by_symbol.setdefault(stage[1], []).append(stage)
        used_stage_ids = set()
        items = []
        thresholds = (0.50,0.75,1.00,1.25,1.50,2.00,3.00,5.00)
        reached = {level: 0 for level in thresholds}
        clean_reached = {level: 0 for level in thresholds}
        matched = mature = clean = 0
        for row in exits:
            (signal_id,symbol,episode_id,decision_ms,exit_event_ms,exit_level,observed_exit,
             net_pct,step_peak,step_trough,flags_json) = row
            candidates = [
                st for st in by_symbol.get(symbol, ())
                if st[0] not in used_stage_ids and abs(int(st[3]) - int(decision_ms)) <= 5000
            ]
            stage = min(candidates, key=lambda st: abs(int(st[3]) - int(decision_ms))) if candidates else None
            if stage:
                used_stage_ids.add(stage[0])
                (stage_id,_,stage_episode,stage_created_ms,stage_entry,stage_mfe,stage_mae,
                 close60_price,completed_60m,tp1_price,tp1_hit_s,tp2_price,tp2_hit_s) = stage
            else:
                stage_id=stage_episode=stage_created_ms=stage_entry=stage_mfe=stage_mae=close60_price=completed_60m=None
                tp1_price=tp1_hit_s=tp2_price=tp2_hit_s=None
            try:
                flags = json.loads(flags_json or "[]")
            except Exception:
                flags = ["INVALID_DATA_FLAGS_JSON"]
            is_clean = not flags
            clean += int(is_clean)
            has_stage = stage_id is not None
            matched += int(has_stage)
            is_mature = bool(completed_60m) if has_stage else False
            mature += int(is_mature)
            mfe = None if stage_mfe is None else float(stage_mfe)
            if mfe is not None:
                for level in thresholds:
                    if mfe + EPS >= level:
                        reached[level] += 1
                        if is_clean:
                            clean_reached[level] += 1
            tp1_return_pct = None
            tp1_after_exit_s = None
            if stage_entry is not None and tp1_price is not None and float(stage_entry) > 0:
                tp1_return_pct = 100.0 * (float(tp1_price) / float(stage_entry) - 1.0)
                if tp1_hit_s is not None and exit_event_ms is not None and stage_created_ms is not None:
                    tp1_event_ms = int(stage_created_ms + float(tp1_hit_s) * 1000.0)
                    tp1_after_exit_s = (tp1_event_ms - int(exit_event_ms)) / 1000.0

            p0_key = None if stage_id is None else f"stage:{int(stage_id)}"
            p0_ref = p0_forward.get(p0_key) if p0_key else None
            p0_timing = []
            first_half_upper = None
            if p0_ref:
                reference = None if p0_ref[1] is None else float(p0_ref[1])
                fill = None if p0_ref[2] is None else float(p0_ref[2])
                for o in p0_outcomes.get(p0_key, ()):
                    _,h,mfe_o,mae_o,peak_ts,gap_o,missing_o = o
                    peak_ref_pct = None
                    if reference and fill and mfe_o is not None:
                        peak_price = fill * (1.0 + float(mfe_o) / 100.0)
                        peak_ref_pct = 100.0 * (peak_price / reference - 1.0)
                    after_exit_peak_s = None
                    if peak_ts is not None and exit_event_ms is not None:
                        after_exit_peak_s = (int(peak_ts) - int(exit_event_ms)) / 1000.0
                    rec = dict(horizon_s=int(h), mfe_fill_pct=None if mfe_o is None else float(mfe_o),
                               mae_fill_pct=None if mae_o is None else float(mae_o),
                               peak_reference_pct=peak_ref_pct,
                               peak_ts_ms=None if peak_ts is None else int(peak_ts),
                               peak_after_exit_s=after_exit_peak_s,
                               gap=int(gap_o or 0), missing_reason=missing_o)
                    p0_timing.append(rec)
                    if (first_half_upper is None and peak_ref_pct is not None and
                            peak_ref_pct + EPS >= 0.50 and peak_ts is not None and
                            exit_event_ms is not None and int(peak_ts) >= int(exit_event_ms)):
                        first_half_upper = dict(horizon_s=int(h),
                                                peak_ts_ms=int(peak_ts),
                                                upper_bound_after_exit_s=after_exit_peak_s,
                                                peak_reference_pct=peak_ref_pct,
                                                gap=int(gap_o or 0),
                                                missing_reason=missing_o)

            items.append(dict(
                signal_id=str(signal_id), symbol=symbol, episode_id=episode_id,
                decision_ms=int(decision_ms), exit_event_ms=None if exit_event_ms is None else int(exit_event_ms),
                exit_level_pct=None if exit_level is None else float(exit_level),
                observed_exit_pct=None if observed_exit is None else float(observed_exit),
                net_pct=None if net_pct is None else float(net_pct),
                step_peak_pct=float(step_peak), step_trough_pct=float(step_trough),
                data_flags=flags, stage_matched=has_stage,
                stage_episode_id=stage_episode,
                stage_created_ms=None if stage_created_ms is None else int(stage_created_ms),
                stage_time_delta_ms=None if stage_created_ms is None else int(stage_created_ms)-int(decision_ms),
                stage_id=None if stage_id is None else int(stage_id),
                stage_entry_price=None if stage_entry is None else float(stage_entry),
                stage_mfe_pct=mfe, stage_mae_pct=None if stage_mae is None else float(stage_mae),
                stage_tp1_return_pct=tp1_return_pct,
                stage_tp1_hit_s=None if tp1_hit_s is None else float(tp1_hit_s),
                stage_tp1_after_exit_s=tp1_after_exit_s,
                stage_tp2_return_pct=(None if stage_entry is None or tp2_price is None or float(stage_entry) <= 0
                                      else 100.0 * (float(tp2_price) / float(stage_entry) - 1.0)),
                stage_tp2_hit_s=None if tp2_hit_s is None else float(tp2_hit_s),
                p0_half_reach_upper_bound=first_half_upper,
                p0_timing=p0_timing,
                close60_price=None if close60_price is None else float(close60_price),
                completed_60m=is_mature,
            ))
        return dict(
            exit_level_pct=float(exit_level_pct), total=len(items), matched_stage=matched,
            mature_60m=mature, clean_signals=clean, reached_after_exit_proxy=reached,
            clean_reached_after_exit_proxy=clean_reached, items=items,
        )


    def historical_profit_review(self, *, notional_usdt=2000.0):
        """Read-only profitability study for public EARLY cohorts.

        Full-history rows provide exact 60m extrema but not arbitrary +/-0.20
        first-touch order. Quality-shadow price buckets provide 1s order for the
        first 15m and 60s buckets afterwards, so arbitrary micro-cut rules are
        evaluated separately on that later exact-ish cohort.
        """
        notional_usdt = _finite(notional_usdt, "notional_usdt", True)
        cuts = (0.10, 0.20, 0.30, 0.50, 1.00)
        future_levels = (0.20, 0.50, 1.00, 2.00, 3.00, 5.00)
        with closing(self.connect()) as db:
            db.execute("PRAGMA query_only=ON")
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            stage_rows = db.execute(
                """SELECT id,symbol,created_ts_ms,entry_price,mfe_pct,mae_pct,close60_price,
                          current_outcome,fee_adjusted_current_pct
                   FROM entry_stage_forward_shadow
                   WHERE stage='EARLY' AND completed_60m=1
                   ORDER BY created_ts_ms"""
            ).fetchall() if "entry_stage_forward_shadow" in tables else []
            stage_all = db.execute(
                """SELECT COUNT(*),MIN(created_ts_ms),MAX(created_ts_ms)
                   FROM entry_stage_forward_shadow WHERE stage='EARLY'"""
            ).fetchone() if "entry_stage_forward_shadow" in tables else (0,None,None)
            step_rows = db.execute(
                """SELECT signal_id,net_pct,data_flags FROM early_step_lock_shadow_v1
                   WHERE status='CLOSED' AND net_pct IS NOT NULL"""
            ).fetchall() if "early_step_lock_shadow_v1" in tables else []

            quality_meta = []
            gap_keys = set()
            price_rows = []
            if {"quality_shadow_cohorts","quality_shadow_prices"}.issubset(tables):
                quality_meta = db.execute(
                    """SELECT key,symbol,anchor_price,decision_ms,recovery_gap
                       FROM quality_shadow_cohorts WHERE kind='EARLY'"""
                ).fetchall()
                if "quality_shadow_gaps" in tables:
                    gap_keys = {str(r[0]) for r in db.execute(
                        """SELECT DISTINCT key FROM quality_shadow_gaps
                           WHERE key LIKE 'EARLY:%'"""
                    ).fetchall()}
                # Stream only the first 60m for public EARLY cohorts.
                price_rows = db.execute(
                    """SELECT c.key,c.anchor_price,c.decision_ms,c.recovery_gap,
                              p.bucket_ms,p.width_ms,p.payload
                       FROM quality_shadow_cohorts c
                       JOIN quality_shadow_prices p ON p.key=c.key
                       WHERE c.kind='EARLY'
                         AND p.bucket_ms>=c.decision_ms
                         AND p.bucket_ms<c.decision_ms+3600000
                       ORDER BY c.key,p.bucket_ms"""
                ).fetchall()

            hist = {d: dict(up_no_down=0, down_no_up=0, both=0, neither=0,
                            down_no_up_hits_minus2=0,
                            both_future={str(x):0 for x in future_levels})
                    for d in cuts}
            mfe_reach = {str(x):0 for x in future_levels}
            mae_reach = {str(-x):0 for x in (0.20,0.50,1.00,2.00,3.00)}
            close_returns = []
            for row in stage_rows:
                mfe = float(row[4] or 0.0)
                mae = float(row[5] or 0.0)
                if row[6] is not None and row[3]:
                    close_returns.append(100.0 * (float(row[6]) / float(row[3]) - 1.0))
                for level in future_levels:
                    if mfe + EPS >= level:
                        mfe_reach[str(level)] += 1
                for level in (0.20,0.50,1.00,2.00,3.00):
                    if mae <= -level + EPS:
                        mae_reach[str(-level)] += 1
                for d in cuts:
                    up = mfe + EPS >= 0.20
                    down = mae <= -d + EPS
                    h = hist[d]
                    if up and not down:
                        h["up_no_down"] += 1
                    elif down and not up:
                        h["down_no_up"] += 1
                        if mae <= -2.0 + EPS:
                            h["down_no_up_hits_minus2"] += 1
                    elif up and down:
                        h["both"] += 1
                        for level in future_levels:
                            if mfe + EPS >= level:
                                h["both_future"][str(level)] += 1
                    else:
                        h["neither"] += 1

            qmeta = {
                str(key): dict(symbol=symbol, anchor=float(anchor), decision_ms=int(decision),
                               clean=(not int(recovery or 0) and str(key) not in gap_keys))
                for key,symbol,anchor,decision,recovery in quality_meta
                if anchor is not None and float(anchor) > 0
            }
            qstate = {}
            path_models = ("low_high", "directional", "high_low")
            sim_policies = {"baseline_minus2": 2.0, "hybrid_minus020": 0.20}

            def fresh_sim():
                return dict(status="OPEN", current_lock_pct=None, gross_pct=None,
                            reason=None, last_ret_pct=0.0)

            def advance_sim(sim, ret, initial_cut):
                if sim["status"] != "OPEN":
                    return
                sim["last_ret_pct"] = float(ret)
                if ret + EPS >= self.final_tp_pct:
                    sim.update(status="CLOSED", gross_pct=self.final_tp_pct, reason="FINAL_TP")
                    return
                lock = sim["current_lock_pct"]
                if lock is not None:
                    if ret <= lock + EPS:
                        sim.update(status="CLOSED", gross_pct=float(lock), reason="STEP_LOCK")
                        return
                elif ret <= -float(initial_cut) + EPS:
                    sim.update(status="CLOSED", gross_pct=-float(initial_cut),
                               reason=("MICRO_CUT" if float(initial_cut) < 1.999 else "INITIAL_SL"))
                    return
                eligible = [level for level in STEP_LEVELS if ret + EPS >= level]
                if eligible:
                    new_lock = max(eligible)
                    if lock is None or new_lock > lock + EPS:
                        sim["current_lock_pct"] = float(new_lock)

            def bucket_points(payload, anchor, model):
                op = 100.0 * (float(payload.get("open")) / anchor - 1.0)
                hi = 100.0 * (float(payload.get("high")) / anchor - 1.0)
                lo = 100.0 * (float(payload.get("low")) / anchor - 1.0)
                cl = 100.0 * (float(payload.get("close")) / anchor - 1.0)
                if model == "low_high":
                    return (op, lo, hi, cl)
                if model == "high_low":
                    return (op, hi, lo, cl)
                # Standard OHLC path heuristic: bullish bucket assumes low then high;
                # bearish bucket assumes high then low.
                return (op, lo, hi, cl) if cl >= op else (op, hi, lo, cl)

            def fresh_state():
                return {
                    "mfe": 0.0, "mae": 0.0, "last_close_ret": 0.0,
                    "cuts": {d: {"first": None, "first_bucket_ms": None,
                                 "post_down_mfe": None} for d in cuts},
                    "sims": {
                        policy: {model: fresh_sim() for model in path_models}
                        for policy in sim_policies
                    },
                }
            current_key = None
            state = None
            for key,anchor,decision,recovery,bucket_ms,width_ms,payload_json in price_rows:
                key = str(key)
                if key != current_key:
                    if current_key is not None and state is not None:
                        qstate[current_key] = state
                    current_key = key
                    state = fresh_state()
                try:
                    payload = json.loads(payload_json or "{}")
                    high = float(payload.get("high"))
                    low = float(payload.get("low"))
                    close_price = float(payload.get("close"))
                    open_price = float(payload.get("open"))
                    if min(high, low, close_price, open_price) <= 0:
                        continue
                except Exception:
                    continue
                anchor = float(anchor)
                hi = 100.0 * (high / anchor - 1.0)
                lo = 100.0 * (low / anchor - 1.0)
                state["last_close_ret"] = 100.0 * (close_price / anchor - 1.0)
                state["mfe"] = max(state["mfe"], hi)
                state["mae"] = min(state["mae"], lo)
                for policy, initial_cut in sim_policies.items():
                    for model in path_models:
                        for ret in bucket_points(payload, anchor, model):
                            advance_sim(state["sims"][policy][model], ret, initial_cut)
                for d in cuts:
                    cs = state["cuts"][d]
                    if cs["first"] is None:
                        hit_up = hi + EPS >= 0.20
                        hit_down = lo <= -d + EPS
                        if hit_up and hit_down:
                            cs["first"] = "AMBIGUOUS_SAME_BUCKET"
                            cs["first_bucket_ms"] = int(bucket_ms)
                        elif hit_up:
                            cs["first"] = "UP_020"
                            cs["first_bucket_ms"] = int(bucket_ms)
                        elif hit_down:
                            cs["first"] = "DOWN_CUT"
                            cs["first_bucket_ms"] = int(bucket_ms)
                            cs["post_down_mfe"] = hi
                    elif cs["first"] == "DOWN_CUT":
                        cs["post_down_mfe"] = max(float(cs["post_down_mfe"] or -999.0), hi)
            if current_key is not None and state is not None:
                qstate[current_key] = state

        hybrid_scenarios = {}
        for policy in sim_policies:
            policy_out = {}
            for model in path_models:
                net_sum = 0.0
                gross_sum = 0.0
                wins = losses = flat = 0
                reasons = {}
                resolved = 0
                marked_60m = 0
                for key, st in qstate.items():
                    sim = st["sims"][policy][model]
                    if sim["status"] == "CLOSED":
                        gross = float(sim["gross_pct"])
                        reason = str(sim["reason"])
                    else:
                        gross = float(st["last_close_ret"])
                        reason = "MARK_60M"
                        marked_60m += 1
                    net = gross - self.cost_pct
                    gross_sum += gross
                    net_sum += net
                    resolved += 1
                    reasons[reason] = reasons.get(reason, 0) + 1
                    if net > EPS:
                        wins += 1
                    elif net < -EPS:
                        losses += 1
                    else:
                        flat += 1
                policy_out[model] = dict(
                    cohort=resolved, gross_pct_sum=gross_sum, net_pct_sum=net_sum,
                    net_usdt=net_sum * notional_usdt / 100.0,
                    avg_net_pct=(net_sum / resolved if resolved else None),
                    avg_net_usdt=(net_sum * notional_usdt / 100.0 / resolved if resolved else None),
                    wins=wins, losses=losses, flat=flat,
                    win_rate=(wins / resolved if resolved else None),
                    marked_at_60m=marked_60m, close_reasons=reasons,
                    cost_pct=self.cost_pct,
                )
            hybrid_scenarios[policy] = policy_out

        exact = {}
        for d in cuts:
            out = dict(total=0,clean_total=0,up_first=0,down_first=0,ambiguous=0,none=0,
                       clean_up_first=0,clean_down_first=0,clean_ambiguous=0,clean_none=0,
                       down_first_later={str(x):0 for x in future_levels},
                       clean_down_first_later={str(x):0 for x in future_levels})
            for key,meta in qmeta.items():
                st = qstate.get(key)
                if not st:
                    continue
                out["total"] += 1
                clean = bool(meta["clean"])
                if clean:
                    out["clean_total"] += 1
                cs = st["cuts"][d]
                first = cs["first"]
                if first == "UP_020":
                    out["up_first"] += 1
                    if clean: out["clean_up_first"] += 1
                elif first == "DOWN_CUT":
                    out["down_first"] += 1
                    if clean: out["clean_down_first"] += 1
                    later = float(cs["post_down_mfe"] if cs["post_down_mfe"] is not None else -999.0)
                    for level in future_levels:
                        if later + EPS >= level:
                            out["down_first_later"][str(level)] += 1
                            if clean:
                                out["clean_down_first_later"][str(level)] += 1
                elif first == "AMBIGUOUS_SAME_BUCKET":
                    out["ambiguous"] += 1
                    if clean: out["clean_ambiguous"] += 1
                else:
                    out["none"] += 1
                    if clean: out["clean_none"] += 1
            true_bad = out["down_first"] - out["down_first_later"]["0.2"]
            clean_true_bad = out["clean_down_first"] - out["clean_down_first_later"]["0.2"]
            out["down_first_no_later_020"] = true_bad
            out["clean_down_first_no_later_020"] = clean_true_bad
            out["cut_precision_no_later_020"] = (true_bad / out["down_first"] if out["down_first"] else None)
            out["clean_cut_precision_no_later_020"] = (
                clean_true_bad / out["clean_down_first"] if out["clean_down_first"] else None)
            exact[str(d)] = out

        # Counterfactual on the actual Step Lock cohort: before +0.20, replace -2
        # with a micro-cut. Same round-trip cost is applied to both paths.
        step_cf = {}
        step_by_key = {}
        for signal_id,net_pct,flags_json in step_rows:
            try:
                flags = json.loads(flags_json or "[]")
            except Exception:
                flags = ["INVALID_DATA_FLAGS_JSON"]
            step_by_key["EARLY:"+str(signal_id)] = (float(net_pct), not flags)
        for d in cuts:
            base_sum = prop_sum = clean_base = clean_prop = 0.0
            used = cut_count = clean_used = clean_cut = ambiguous = 0
            baseline_positive_cut = 0
            for key,(base_net,step_clean) in step_by_key.items():
                st = qstate.get(key)
                meta = qmeta.get(key)
                if not st or not meta:
                    continue
                first = st["cuts"][d]["first"]
                if first == "AMBIGUOUS_SAME_BUCKET":
                    ambiguous += 1
                    continue
                if first not in ("UP_020","DOWN_CUT"):
                    continue
                used += 1
                base_sum += base_net
                proposed = base_net
                if first == "DOWN_CUT":
                    cut_count += 1
                    proposed = -float(d) - self.cost_pct
                    if base_net > 0:
                        baseline_positive_cut += 1
                prop_sum += proposed
                if step_clean and meta["clean"]:
                    clean_used += 1
                    clean_base += base_net
                    if first == "DOWN_CUT":
                        clean_cut += 1
                    clean_prop += proposed
            step_cf[str(d)] = dict(
                matched_unambiguous=used, cut_count=cut_count, ambiguous=ambiguous,
                baseline_positive_that_would_be_cut=baseline_positive_cut,
                baseline_net_pct_sum=base_sum, proposed_net_pct_sum=prop_sum,
                delta_net_pct_sum=prop_sum-base_sum,
                delta_usdt=(prop_sum-base_sum)*notional_usdt/100.0,
                clean_matched_unambiguous=clean_used, clean_cut_count=clean_cut,
                clean_delta_net_pct_sum=clean_prop-clean_base,
                clean_delta_usdt=(clean_prop-clean_base)*notional_usdt/100.0,
                assumption="ideal threshold fill; same %.2fpp round-trip cost" % self.cost_pct,
            )

        full = {}
        total_hist = len(stage_rows)
        for d in cuts:
            h = hist[d]
            definite_saves = h["down_no_up_hits_minus2"]
            full[str(d)] = dict(
                **h,
                total_completed_60m=total_hist,
                definite_minus2_to_microcut_saves=definite_saves,
                ideal_saving_per_saved_trade_pct=2.0-float(d),
                ideal_saving_per_saved_trade_usdt=(2.0-float(d))*notional_usdt/100.0,
                ideal_total_saving_usdt_if_all_definite=definite_saves*(2.0-float(d))*notional_usdt/100.0,
                caveat="MFE/MAE know both extrema but not order when both thresholds were touched.",
            )

        return dict(
            version="early-microcut-profit-review-v1",
            notional_usdt=notional_usdt,
            full_history=dict(
                early_rows_all=int(stage_all[0] or 0),
                completed_60m=total_hist,
                first_created_ms=stage_all[1], last_created_ms=stage_all[2],
                mfe_reach=mfe_reach, mae_reach=mae_reach,
                microcut_bounds=full,
            ),
            quality_exactish=dict(
                cohort_count=len(qmeta),
                price_path_count=len(qstate),
                resolution="1s buckets first 15m; 60s buckets afterwards; same-bucket dual touch is ambiguous",
                microcut_first_touch=exact,
                hybrid_step_ladder_60m=hybrid_scenarios,
                hybrid_method=(
                    "Three intra-bucket path assumptions are reported: low_high (pessimistic), "
                    "directional OHLC heuristic, high_low (optimistic). Policy hybrid_minus020 exits "
                    "at -0.20 before +0.20; otherwise applies Step Lock levels +0.20,+0.50,+0.75...+5. "
                    "Any position still open at 60m is marked to the last observed close; all outcomes "
                    "subtract the same 0.14pp round-trip cost."
                ),
            ),
            current_step_lock_counterfactual=step_cf,
            interpretation=dict(
                all_history="Use for broad bounds and definite no-+0.20 losers; arbitrary +/-0.20 order is unknown when both occurred.",
                exactish="Use quality-shadow buckets for first-touch ordering; exclude ambiguous same-bucket rows.",
                counterfactual="Uses actual Step Lock realized net for survivors and idealized micro-cut threshold fills for early cuts.",
            ),
        )



    def runner_filter_price_action_review(self):
        """Read-only search for early-dip filters that preserve later runners."""
        waits = (15, 30, 45, 60, 90, 120, 180, 300)
        cut_prices = (-0.20, -0.15, -0.10, -0.05, 0.0, 0.05)
        depth_levels = (-0.30, -0.40, -0.50, -0.75, -1.00)
        reclaim_levels = (-0.10, -0.05, 0.0, 0.05)
        with closing(self.connect()) as db:
            db.execute("PRAGMA query_only=ON")
            rows = db.execute(
                """SELECT c.key,c.anchor_price,c.decision_ms,
                          p.bucket_ms,p.width_ms,p.payload
                   FROM quality_shadow_cohorts c
                   JOIN quality_shadow_prices p ON p.key=c.key
                   WHERE c.kind='EARLY'
                     AND p.bucket_ms>=c.decision_ms
                     AND p.bucket_ms<c.decision_ms+3600000
                   ORDER BY c.key,p.bucket_ms"""
            ).fetchall()
        paths = {}
        for key,anchor,decision,bucket_ms,width_ms,payload_json in rows:
            key = str(key)
            try:
                p = json.loads(payload_json or "{}")
                op,hi,lo,cl = map(float,(p.get("open"),p.get("high"),p.get("low"),p.get("close")))
                if min(op,hi,lo,cl,float(anchor)) <= 0:
                    continue
                max_gap = int(p.get("max_gap_ms") or 0)
            except Exception:
                continue
            a=float(anchor)
            rec=dict(
                bucket_ms=int(bucket_ms), width_ms=int(width_ms),
                open_pct=100.0*(op/a-1.0), high_pct=100.0*(hi/a-1.0),
                low_pct=100.0*(lo/a-1.0), close_pct=100.0*(cl/a-1.0),
                max_gap_ms=max_gap,
            )
            item=paths.setdefault(key,dict(anchor=a,decision_ms=int(decision),buckets=[]))
            item["buckets"].append(rec)

        cohorts=[]
        for key,item in paths.items():
            bs=item["buckets"]
            first_down=None
            first_up=None
            ambiguous=False
            mfe=-999.0
            mae=999.0
            max_gap_5m=0
            for idx,b in enumerate(bs):
                mfe=max(mfe,b["high_pct"])
                mae=min(mae,b["low_pct"])
                if b["bucket_ms"] < item["decision_ms"]+300000:
                    max_gap_5m=max(max_gap_5m,b["max_gap_ms"])
                hit_d=b["low_pct"] <= -0.20 + EPS
                hit_u=b["high_pct"] >= 0.20 - EPS
                if first_down is None and first_up is None and hit_d and hit_u:
                    ambiguous=True
                    break
                if first_down is None and hit_d:
                    first_down=idx
                if first_up is None and hit_u:
                    first_up=idx
                if first_down is not None or first_up is not None:
                    # We only need the first touch order; continue scanning for MFE/MAE.
                    pass
            if ambiguous or first_down is None:
                continue
            if first_up is not None and first_up < first_down:
                continue
            down_bucket=bs[first_down]
            down_ms=down_bucket["bucket_ms"]
            later_up = first_up is not None and first_up > first_down
            true_bad = not later_up
            runner1 = later_up and mfe >= 1.0 - EPS
            runner2 = later_up and mfe >= 2.0 - EPS
            cohorts.append(dict(
                key=key,buckets=bs,down_idx=first_down,down_ms=down_ms,
                first_up_idx=first_up,true_bad=true_bad,recover=later_up,
                runner1=runner1,runner2=runner2,mfe=mfe,mae=mae,
                max_gap_5m=max_gap_5m,
            ))

        totals=dict(
            early_dip=len(cohorts),
            true_bad=sum(x["true_bad"] for x in cohorts),
            recover=sum(x["recover"] for x in cohorts),
            runner1=sum(x["runner1"] for x in cohorts),
            runner2=sum(x["runner2"] for x in cohorts),
            gap5m_gt2s=sum(x["max_gap_5m"]>2000 for x in cohorts),
        )

        def evaluate(selected, label, params):
            selected=list(selected)
            n=len(selected)
            caught=sum(x["true_bad"] for x in selected)
            recover_wrong=sum(x["recover"] for x in selected)
            r1_wrong=sum(x["runner1"] for x in selected)
            r2_wrong=sum(x["runner2"] for x in selected)
            return dict(
                rule=label,params=params,cut_count=n,true_bad_caught=caught,
                recover_wrong_cut=recover_wrong,runner1_wrong_cut=r1_wrong,
                runner2_wrong_cut=r2_wrong,
                precision=(caught/n if n else None),
                recall=(caught/totals["true_bad"] if totals["true_bad"] else None),
                runner1_retention=(1-r1_wrong/totals["runner1"] if totals["runner1"] else None),
                runner2_retention=(1-r2_wrong/totals["runner2"] if totals["runner2"] else None),
            )

        rules=[]
        for wait_s in waits:
            for threshold in cut_prices:
                chosen=[]
                for x in cohorts:
                    bs=x["buckets"]
                    target=x["down_ms"]+wait_s*1000
                    # If +0.20 is reached before confirmation time, protect the runner.
                    if x["first_up_idx"] is not None and bs[x["first_up_idx"]]["bucket_ms"] <= target:
                        continue
                    prior=[b for b in bs[x["down_idx"]:] if b["bucket_ms"] <= target]
                    if not prior:
                        continue
                    price=prior[-1]["close_pct"]
                    if price <= threshold + EPS:
                        chosen.append(x)
                rules.append(evaluate(
                    chosen,"wait_then_price",
                    dict(wait_s=wait_s,close_lte_pct=threshold),
                ))

        # Depth failure: cut only if a deeper adverse level is reached before a
        # specified reclaim level; +0.20 first automatically survives.
        for depth in depth_levels:
            for reclaim in reclaim_levels:
                chosen=[]
                for x in cohorts:
                    hit=None
                    for b in x["buckets"][x["down_idx"]:]:
                        if b["high_pct"] >= 0.20 - EPS:
                            break
                        hit_depth=b["low_pct"] <= depth + EPS
                        hit_reclaim=b["high_pct"] >= reclaim - EPS
                        if hit_depth and hit_reclaim:
                            hit="AMBIG"
                            break
                        if hit_reclaim:
                            hit="RECLAIM"
                            break
                        if hit_depth:
                            hit="DEPTH"
                            break
                    if hit=="DEPTH":
                        chosen.append(x)
                rules.append(evaluate(
                    chosen,"depth_before_reclaim",
                    dict(depth_pct=depth,reclaim_pct=reclaim),
                ))

        eligible=[
            r for r in rules
            if r["runner1_retention"] is not None and r["runner2_retention"] is not None
            and r["runner1_retention"] >= 0.90 - EPS
            and r["runner2_retention"] >= 0.95 - EPS
        ]
        eligible.sort(key=lambda r:(
            -(r["true_bad_caught"]),
            -(r["precision"] or 0.0),
            -(r["runner2_retention"] or 0.0),
            -(r["runner1_retention"] or 0.0),
        ))
        balanced=sorted(rules,key=lambda r:(
            -(r["true_bad_caught"] - 2*r["runner2_wrong_cut"] - r["runner1_wrong_cut"]),
            -(r["precision"] or 0.0),
        ))
        return dict(
            version="runner-filter-price-action-v1",
            resolution="1s OHLC buckets first 15m, 60s afterwards; same-bucket dual touch excluded",
            totals=totals,
            target="catch TRUE_BAD after -0.20 first while preserving >=90% RUNNER_1 and >=95% RUNNER_2",
            best_strict=eligible[:10],
            best_balanced=balanced[:10],
            all_rule_count=len(rules),
            caveats=[
                "Intra-bucket high/low order is unknown; rules treat same-bucket conflicting touch as ambiguous.",
                "Price-close confirmation uses bucket close, not exact tick at the requested second.",
                "Data gaps are reported separately and should be stress-tested before promotion.",
            ],
        )



    def combined_profit_loss_optimizer(self, *, notional_usdt=2000.0):
        """Read-only, time-split search for runner-preserving loss filters and profit locks."""
        notional_usdt = _finite(notional_usdt, "notional_usdt", True)
        horizons = (30000, 60000, 90000, 180000)
        features = (
            "chg10","chg30","chg60","flow10","flow30","flow60",
            "buy10","buy30","buy60","book_imbalance","compression_ratio",
            "oi5","oi_accel5","rel30","gainer_rank","rank_velocity",
            "candidate_runup","dist_episode_peak_pct","distance_from_episode_low_pct",
            "seconds_since_episode_peak","episode_age_s","spread",
        )
        with closing(self.connect()) as db:
            db.execute("PRAGMA query_only=ON")
            path_rows = db.execute(
                """SELECT c.key,c.anchor_price,c.decision_ms,c.recovery_gap,
                          p.bucket_ms,p.width_ms,p.payload
                   FROM quality_shadow_cohorts c
                   JOIN quality_shadow_prices p ON p.key=c.key
                   WHERE c.kind='EARLY'
                     AND p.bucket_ms>=c.decision_ms
                     AND p.bucket_ms<c.decision_ms+3600000
                   ORDER BY c.key,p.bucket_ms"""
            ).fetchall()
            snap_rows = db.execute(
                """SELECT s.key,s.horizon_ms,s.nominal_ms,s.payload
                   FROM quality_shadow_snapshots s
                   JOIN quality_shadow_cohorts c ON c.key=s.key
                   WHERE c.kind='EARLY' AND s.horizon_ms IN (30000,60000,90000,180000)
                   ORDER BY s.key,s.horizon_ms"""
            ).fetchall()

        paths = {}
        for key,anchor,decision,recovery,bucket_ms,width_ms,payload_json in path_rows:
            key=str(key)
            try:
                p=json.loads(payload_json or "{}")
                op,hi,lo,cl=map(float,(p.get("open"),p.get("high"),p.get("low"),p.get("close")))
                if min(op,hi,lo,cl,float(anchor)) <= 0:
                    continue
                max_gap=int(p.get("max_gap_ms") or 0)
            except Exception:
                continue
            a=float(anchor)
            item=paths.setdefault(key,dict(
                key=key,anchor=a,decision_ms=int(decision),recovery_gap=bool(recovery),
                buckets=[],snapshots={},mfe=-999.0,mae=999.0,first_down_ms=None,
                first_up_ms=None,ambiguous_first=False,max_gap_5m=0,
            ))
            rec=dict(
                bucket_ms=int(bucket_ms),width_ms=int(width_ms),
                open_pct=100.0*(op/a-1.0),high_pct=100.0*(hi/a-1.0),
                low_pct=100.0*(lo/a-1.0),close_pct=100.0*(cl/a-1.0),
                max_gap_ms=max_gap,
            )
            item["buckets"].append(rec)
            item["mfe"]=max(item["mfe"],rec["high_pct"])
            item["mae"]=min(item["mae"],rec["low_pct"])
            if rec["bucket_ms"] < item["decision_ms"]+300000:
                item["max_gap_5m"]=max(item["max_gap_5m"],max_gap)
            if item["first_down_ms"] is None and item["first_up_ms"] is None:
                hd=rec["low_pct"] <= -0.20 + EPS
                hu=rec["high_pct"] >= 0.20 - EPS
                if hd and hu:
                    item["ambiguous_first"]=True
                elif hd:
                    item["first_down_ms"]=rec["bucket_ms"]
                elif hu:
                    item["first_up_ms"]=rec["bucket_ms"]
            else:
                if item["first_down_ms"] is None and rec["low_pct"] <= -0.20 + EPS:
                    item["first_down_ms"]=rec["bucket_ms"]
                if item["first_up_ms"] is None and rec["high_pct"] >= 0.20 - EPS:
                    item["first_up_ms"]=rec["bucket_ms"]

        for key,horizon,nominal,payload_json in snap_rows:
            key=str(key)
            if key not in paths:
                continue
            try:
                payload=json.loads(payload_json or "{}")
                raw=payload.get("raw_features") or {}
                source_flags=payload.get("feature_source_flags") or {}
                gap_flags=payload.get("gap_flags") or []
            except Exception:
                continue
            clean={}
            for name in features:
                value=raw.get(name)
                if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(float(value)):
                    continue
                sf=source_flags.get(name)
                if sf is not None and sf!="OK":
                    continue
                clean[name]=float(value)
            price=raw.get("price")
            price_ret=None
            if isinstance(price,(int,float)) and math.isfinite(float(price)) and float(price)>0:
                price_ret=100.0*(float(price)/paths[key]["anchor"]-1.0)
            paths[key]["snapshots"][int(horizon)]=dict(
                nominal_ms=int(nominal),features=clean,price_ret=price_ret,
                gap_flags=list(gap_flags),
            )

        usable=[p for p in paths.values() if p["buckets"]]
        usable.sort(key=lambda x:x["decision_ms"])
        split=max(1,min(len(usable)-1,int(len(usable)*0.70))) if len(usable)>1 else len(usable)
        train_keys={p["key"] for p in usable[:split]}
        test_keys={p["key"] for p in usable[split:]}

        def is_early_dip(p):
            if p["ambiguous_first"] or p["first_down_ms"] is None:
                return False
            return p["first_up_ms"] is None or p["first_down_ms"] < p["first_up_ms"]

        for p in usable:
            p["early_dip"]=is_early_dip(p)
            p["recover"]=bool(p["early_dip"] and p["first_up_ms"] is not None and p["first_up_ms"]>p["first_down_ms"])
            p["true_bad"]=bool(p["early_dip"] and not p["recover"])
            p["runner1"]=bool(p["recover"] and p["mfe"]>=1.0-EPS)
            p["runner2"]=bool(p["recover"] and p["mfe"]>=2.0-EPS)

        train_dips=[p for p in usable if p["key"] in train_keys and p["early_dip"]]
        test_dips=[p for p in usable if p["key"] in test_keys and p["early_dip"]]

        def qtile(values,q):
            vals=sorted(values)
            if not vals:
                return None
            pos=(len(vals)-1)*q
            lo=int(math.floor(pos));hi=int(math.ceil(pos))
            if lo==hi:return vals[lo]
            return vals[lo]+(vals[hi]-vals[lo])*(pos-lo)

        def rule_trigger(p,rule):
            h=rule["horizon_ms"]
            snap=p["snapshots"].get(h)
            if not snap or p["first_down_ms"] is None:
                return False
            t=p["decision_ms"]+h
            if p["first_down_ms"]>t:
                return False
            if p["first_up_ms"] is not None and p["first_up_ms"]<=t:
                return False
            for cond in rule["conditions"]:
                v=snap["features"].get(cond["feature"])
                if v is None:
                    return False
                if cond["op"]=="<=" and not (v<=cond["threshold"]+EPS):
                    return False
                if cond["op"]==">=" and not (v>=cond["threshold"]-EPS):
                    return False
            return True

        def rule_stats(rule, cohort):
            all_r1=sum(p["runner1"] for p in cohort)
            all_r2=sum(p["runner2"] for p in cohort)
            all_bad=sum(p["true_bad"] for p in cohort)
            selected=[p for p in cohort if rule_trigger(p,rule)]
            caught=sum(p["true_bad"] for p in selected)
            wrong=sum(p["recover"] for p in selected)
            r1_wrong=sum(p["runner1"] for p in selected)
            r2_wrong=sum(p["runner2"] for p in selected)
            return dict(
                eligible=len(cohort),cut_count=len(selected),true_bad_caught=caught,
                recover_wrong_cut=wrong,runner1_wrong_cut=r1_wrong,runner2_wrong_cut=r2_wrong,
                precision=(caught/len(selected) if selected else None),
                recall=(caught/all_bad if all_bad else None),
                runner1_retention=(1-r1_wrong/all_r1 if all_r1 else None),
                runner2_retention=(1-r2_wrong/all_r2 if all_r2 else None),
            )

        feature_coverage={}
        singles=[]
        for h in horizons:
            eligible=[
                p for p in train_dips
                if p["first_down_ms"] is not None and p["first_down_ms"]<=p["decision_ms"]+h
                and (p["first_up_ms"] is None or p["first_up_ms"]>p["decision_ms"]+h)
                and h in p["snapshots"]
            ]
            cov={}
            for name in features:
                vals=[p["snapshots"][h]["features"][name] for p in eligible
                      if name in p["snapshots"][h]["features"]]
                coverage=len(vals)/len(eligible) if eligible else 0.0
                cov[name]=dict(n=len(vals),coverage=coverage)
                if coverage < 0.60 or len(vals)<20:
                    continue
                thresholds=sorted(set(
                    round(float(qtile(vals,q)),10) for q in (0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.80,0.90)
                    if qtile(vals,q) is not None
                ))
                for threshold in thresholds:
                    for op in ("<=",">="):
                        rule=dict(horizon_ms=h,conditions=[dict(feature=name,op=op,threshold=threshold)])
                        st=rule_stats(rule,train_dips)
                        if st["cut_count"]>=5:
                            singles.append(dict(rule=rule,train=st))
            feature_coverage[str(h)]=cov

        def rank_key(item):
            st=item["train"]
            r1=st["runner1_retention"] if st["runner1_retention"] is not None else 0.0
            r2=st["runner2_retention"] if st["runner2_retention"] is not None else 0.0
            precision=st["precision"] or 0.0
            return (st["true_bad_caught"]-2*st["runner2_wrong_cut"]-st["runner1_wrong_cut"],
                    precision,r2,r1)

        singles.sort(key=rank_key,reverse=True)
        pairs=[]
        by_h={}
        for item in singles[:80]:
            by_h.setdefault(item["rule"]["horizon_ms"],[]).append(item)
        for h,items in by_h.items():
            top=items[:18]
            for i in range(len(top)):
                for j in range(i+1,len(top)):
                    c1=top[i]["rule"]["conditions"][0];c2=top[j]["rule"]["conditions"][0]
                    if c1["feature"]==c2["feature"]:
                        continue
                    rule=dict(horizon_ms=h,conditions=[c1,c2])
                    st=rule_stats(rule,train_dips)
                    if st["cut_count"]>=5:
                        pairs.append(dict(rule=rule,train=st))
        candidates=singles+pairs
        strict=[
            x for x in candidates
            if (x["train"]["runner1_retention"] is not None and x["train"]["runner1_retention"]>=0.90-EPS
                and x["train"]["runner2_retention"] is not None and x["train"]["runner2_retention"]>=0.95-EPS)
        ]
        strict.sort(key=lambda x:(
            x["train"]["true_bad_caught"],x["train"]["precision"] or 0.0,
            x["train"]["runner2_retention"] or 0.0,x["train"]["runner1_retention"] or 0.0
        ),reverse=True)
        top_rules=[]
        for item in strict[:12]:
            enriched=dict(rule=item["rule"],train=item["train"],test=rule_stats(item["rule"],test_dips))
            top_rules.append(enriched)
        no_rule=dict(horizon_ms=None,conditions=[])

        rungs=list(STEP_LEVELS)
        profit_policies=[dict(name=f"lag_{lag}",kind="lag",lag=lag) for lag in (0,1,2,3,4)]
        for activation in (0.50,0.75,1.00,1.25):
            for trail in (0.25,0.50,0.75,1.00):
                if trail>=activation+0.50:
                    continue
                profit_policies.append(dict(
                    name=f"trail_a{activation:g}_d{trail:g}",kind="trail",
                    activation=activation,trail=trail,floor=0.0,
                ))

        def apply_profit_state(state,ret,policy):
            if state["closed"]:
                return
            # Existing stop is checked before a favorable observation can raise it.
            stop=state["stop"]
            if stop is not None and ret<=stop+EPS:
                state.update(closed=True,gross=float(stop),reason="PROFIT_STOP")
                return
            if ret<=-2.0+EPS and state["peak"]<0.20-EPS:
                state.update(closed=True,gross=-2.0,reason="INITIAL_SL")
                return
            if ret+EPS>=5.0:
                state.update(closed=True,gross=5.0,reason="FINAL_TP")
                return
            state["peak"]=max(state["peak"],ret)
            if policy["kind"]=="lag":
                reached=[i for i,x in enumerate(rungs) if ret+EPS>=x]
                if reached:
                    idx=max(reached)-int(policy["lag"])
                    if idx>=0:
                        new=float(rungs[idx])
                        if state["stop"] is None or new>state["stop"]+EPS:
                            state["stop"]=new
            else:
                if state["peak"]+EPS>=float(policy["activation"]):
                    new=max(float(policy["floor"]),state["peak"]-float(policy["trail"]))
                    if state["stop"] is None or new>state["stop"]+EPS:
                        state["stop"]=new

        def points_for_bucket(b):
            op,hi,lo,cl=b["open_pct"],b["high_pct"],b["low_pct"],b["close_pct"]
            return (op,lo,hi,cl) if cl>=op else (op,hi,lo,cl)

        # Precompute each profit policy once per path. Combined loss-filter
        # candidates can then replace the precomputed outcome at their trigger
        # time instead of replaying the entire 60m path for every combination.
        policy_cache={}
        for policy in profit_policies:
            by_key={}
            for p in usable:
                state=dict(closed=False,gross=None,reason=None,stop=None,peak=-999.0,close_ms=None)
                for b in p["buckets"]:
                    if state["closed"]:
                        break
                    for ret in points_for_bucket(b):
                        was=state["closed"]
                        apply_profit_state(state,ret,policy)
                        if not was and state["closed"]:
                            state["close_ms"]=int(b["bucket_ms"])
                            break
                if not state["closed"]:
                    last=p["buckets"][-1]["close_pct"]
                    state.update(closed=True,gross=float(last),reason="MARK_60M",
                                 close_ms=int(p["buckets"][-1]["bucket_ms"]))
                by_key[p["key"]]=dict(
                    net=float(state["gross"])-self.cost_pct,
                    reason=state["reason"],
                    close_ms=int(state["close_ms"]),
                )
            policy_cache[policy["name"]]=by_key

        def simulate_set(keys,policy,rule):
            cohort=[p for p in usable if p["key"] in keys]
            cache=policy_cache[policy["name"]]
            net_sum=0.0;wins=losses=0;reasons={}
            for p in cohort:
                base=cache[p["key"]]
                net=float(base["net"]);reason=base["reason"]
                if rule.get("horizon_ms") is not None and rule_trigger(p,rule):
                    trigger_ms=int(p["decision_ms"]+rule["horizon_ms"])
                    if trigger_ms <= int(base["close_ms"]):
                        snap=p["snapshots"].get(rule["horizon_ms"])
                        gross=snap.get("price_ret") if snap else None
                        if gross is not None:
                            net=float(gross)-self.cost_pct
                            reason="FAILURE_FILTER"
                net_sum+=net
                if net>EPS:wins+=1
                elif net<-EPS:losses+=1
                reasons[reason]=reasons.get(reason,0)+1
            n=len(cohort)
            return dict(
                n=n,net_pct_sum=net_sum,net_usdt=net_sum*notional_usdt/100.0,
                avg_net_pct=(net_sum/n if n else None),
                avg_net_usdt=(net_sum*notional_usdt/100.0/n if n else None),
                wins=wins,losses=losses,win_rate=(wins/n if n else None),
                reasons=reasons,
            )

        loss_rules=[no_rule]+[x["rule"] for x in top_rules[:8]]
        combos=[]
        for policy in profit_policies:
            for rule in loss_rules:
                train=simulate_set(train_keys,policy,rule)
                combos.append(dict(policy=policy,loss_rule=rule,train=train))
        combos.sort(key=lambda x:x["train"]["net_pct_sum"],reverse=True)

        baseline_policy=dict(name="lag_0",kind="lag",lag=0)
        baseline_train=simulate_set(train_keys,baseline_policy,no_rule)
        baseline_test=simulate_set(test_keys,baseline_policy,no_rule)

        profit_policy_results=[]
        for policy in profit_policies:
            tr=simulate_set(train_keys,policy,no_rule)
            te=simulate_set(test_keys,policy,no_rule)
            profit_policy_results.append(dict(
                policy=policy,train=tr,test=te,
                train_delta_vs_baseline_usdt=tr["net_usdt"]-baseline_train["net_usdt"],
                test_delta_vs_baseline_usdt=te["net_usdt"]-baseline_test["net_usdt"],
            ))
        profit_policy_results.sort(key=lambda x:x["train"]["net_usdt"],reverse=True)

        baseline_loss_rule_results=[]
        for enriched in top_rules:
            rule=enriched["rule"]
            tr=simulate_set(train_keys,baseline_policy,rule)
            te=simulate_set(test_keys,baseline_policy,rule)
            baseline_loss_rule_results.append(dict(
                loss_rule=rule,train=tr,test=te,
                train_delta_vs_baseline_usdt=tr["net_usdt"]-baseline_train["net_usdt"],
                test_delta_vs_baseline_usdt=te["net_usdt"]-baseline_test["net_usdt"],
                train_classification=enriched["train"],
                test_classification=enriched["test"],
            ))
        baseline_loss_rule_results.sort(
            key=lambda x:x["train_delta_vs_baseline_usdt"], reverse=True
        )

        best=combos[0] if combos else dict(policy=baseline_policy,loss_rule=no_rule,train=baseline_train)
        best_test=simulate_set(test_keys,best["policy"],best["loss_rule"])
        best_profit_only=max(
            (x for x in combos if x["loss_rule"].get("horizon_ms") is None),
            key=lambda x:x["train"]["net_pct_sum"],
            default=dict(policy=baseline_policy,loss_rule=no_rule,train=baseline_train),
        )
        best_profit_only_test=simulate_set(test_keys,best_profit_only["policy"],no_rule)

        return dict(
            version="combined-profit-loss-optimizer-v1",
            notional_usdt=notional_usdt,
            cohort=dict(total=len(usable),train=len(train_keys),test=len(test_keys),split="chronological 70/30"),
            early_dip=dict(
                train=len(train_dips),test=len(test_dips),
                train_true_bad=sum(p["true_bad"] for p in train_dips),
                test_true_bad=sum(p["true_bad"] for p in test_dips),
                train_runner1=sum(p["runner1"] for p in train_dips),
                test_runner1=sum(p["runner1"] for p in test_dips),
                train_runner2=sum(p["runner2"] for p in train_dips),
                test_runner2=sum(p["runner2"] for p in test_dips),
            ),
            feature_coverage=feature_coverage,
            candidate_loss_rules=top_rules,
            profitability=dict(
                baseline=dict(policy=baseline_policy,train=baseline_train,test=baseline_test),
                profit_policy_leaderboard=profit_policy_results,
                baseline_plus_loss_rule_leaderboard=baseline_loss_rule_results,
                best_profit_only=dict(policy=best_profit_only["policy"],train=best_profit_only["train"],
                                      test=best_profit_only_test),
                best_combined=dict(policy=best["policy"],loss_rule=best["loss_rule"],
                                   train=best["train"],test=best_test),
                test_delta_vs_baseline_usdt=best_test["net_usdt"]-baseline_test["net_usdt"],
                profit_only_test_delta_vs_baseline_usdt=best_profit_only_test["net_usdt"]-baseline_test["net_usdt"],
            ),
            caveats=[
                "Optimizer selects on the earlier 70% and reports profitability on the later 30% holdout.",
                "Price paths use 1s OHLC buckets for first 15m and 60s buckets afterwards; directional OHLC ordering is a heuristic.",
                "Feature rules only use scheduled snapshots whose individual source flag is OK when such provenance exists.",
                "The study is shadow research; fills are idealized at modeled stops/snapshot prices and use %.2fpp round-trip cost." % self.cost_pct,
                "Observed trade gaps remain common and should block live promotion until forward validation confirms the result.",
            ],
        )


    def get(self, signal_id):
        with closing(self.connect()) as db:
            row = db.execute(
                """SELECT signal_id,symbol,entry_price,decision_ms,current_lock_pct,peak_pct,
                          trough_pct,status,close_reason,exit_level_pct,observed_exit_pct,
                          gross_pct,net_pct,data_flags,version
                   FROM early_step_lock_shadow_v1 WHERE signal_id=?""",
                (str(signal_id),),
            ).fetchone()
        if not row:
            return None
        keys = ("signal_id","symbol","entry_price","decision_ms","current_lock_pct","peak_pct",
                "trough_pct","status","close_reason","exit_level_pct","observed_exit_pct",
                "gross_pct","net_pct","data_flags","version")
        out = dict(zip(keys, row))
        out["data_flags"] = json.loads(out["data_flags"] or "[]")
        return out
