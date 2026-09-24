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
            reached = db.execute(
                """SELECT level_pct,COUNT(DISTINCT signal_id)
                   FROM early_step_lock_events_v1
                   WHERE event='LOCK_ARM' AND level_pct IS NOT NULL
                   GROUP BY level_pct ORDER BY level_pct"""
            ).fetchall()
            final_tp = db.execute(
                "SELECT COUNT(*) FROM early_step_lock_shadow_v1 WHERE close_reason='FINAL_TP'"
            ).fetchone()[0]
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
        reached_levels = {round(float(level), 8): int(count) for level,count in reached}
        if final_tp:
            reached_levels[round(self.final_tp_pct, 8)] = int(final_tp)
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
            flagged_signals=sum(1 for row in rows if (row[10] or "[]") != "[]"),
            flag_counts=flag_counts, latest_ms=latest_ms,
            closed_net_pct_sum=total_net_pct,
            closed_net_pct_avg=(total_net_pct / closed_count if closed_count else None),
            notional_usdt=notional_usdt,
            closed_net_usdt=total_net_pct * notional_usdt / 100.0,
            closed_net_usdt_avg=(total_net_pct * notional_usdt / 100.0 / closed_count if closed_count else None),
            recent=recent,
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
