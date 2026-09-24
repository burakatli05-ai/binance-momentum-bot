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
                stages = db.execute(
                    f"""SELECT id,symbol,episode_id,created_ts_ms,entry_price,mfe_pct,mae_pct,
                               close60_price,completed_60m
                        FROM entry_stage_forward_shadow
                        WHERE stage='EARLY' AND symbol IN ({placeholders})
                          AND created_ts_ms BETWEEN ? AND ?
                        ORDER BY created_ts_ms""",
                    tuple(symbols) + (lo, hi),
                ).fetchall()
            else:
                stages = []
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
                stage_id,_,stage_episode,stage_created_ms,stage_entry,stage_mfe,stage_mae,close60_price,completed_60m = stage
            else:
                stage_id=stage_episode=stage_created_ms=stage_entry=stage_mfe=stage_mae=close60_price=completed_60m=None
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
                stage_entry_price=None if stage_entry is None else float(stage_entry),
                stage_mfe_pct=mfe, stage_mae_pct=None if stage_mae is None else float(stage_mae),
                close60_price=None if close60_price is None else float(close60_price),
                completed_60m=is_mature,
            ))
        return dict(
            exit_level_pct=float(exit_level_pct), total=len(items), matched_stage=matched,
            mature_60m=mature, clean_signals=clean, reached_after_exit_proxy=reached,
            clean_reached_after_exit_proxy=clean_reached, items=items,
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
