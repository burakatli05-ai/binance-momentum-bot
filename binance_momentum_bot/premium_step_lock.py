"""Premium-only, Decimal step lock and durable replacement protocol.

LIVE entry is deliberately blocked until exchange overlap capability is proven.
The protocol below is transport-independent; the bot does not bind a write adapter.
Early StepLockShadow state is never read or written here.
"""
from collections import Counter
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, localcontext
import hashlib
import json

PROFILE = "PREMIUM_STEP_LOCK_V1"
VERSION = "1.0"
PROFILES = ("CURRENT_TP2", "PARTIAL_RUNNER", PROFILE)
UNKNOWN = "UNKNOWN_EXIT_PROFILE_FAIL_CLOSED"
LIVE_BLOCK_REASON = "PREMIUM_STEP_LOCK_V1_LIVE_BLOCKED_SAFE_STOP_REPLACEMENT_NOT_PROVEN"
SPEC = {
    "name": PROFILE, "version": VERSION, "scope": "new_premium_autotrade_long_positions_only",
    "first_trigger_bp": 50, "first_lock_bp": 25, "step_interval_bp": 50,
    "maximum_trigger_bp": 1000, "maximum_lock_bp": 950,
    "reference": "verified_exchange_fill_vwap", "initial_stop": "existing_premium_plan_from_fill",
    "rounding": "floor_to_price_filter_tick", "stop_monotonic": True,
    "partial_sales": False, "tp1": False, "tp2_limit": False, "runner": False,
    "close_at_maximum_trigger": False, "frozen_at_entry": True,
    "default_profile": "CURRENT_TP2", "legacy_positions_migrated": False,
}
SPEC_BYTES = (json.dumps(SPEC, sort_keys=True, indent=2) + "\n").encode("utf-8")
CONTRACT_HASH = hashlib.sha256(SPEC_BYTES).hexdigest()
SCHEMA = {
    "exit_profile_version": "TEXT", "profile_contract_hash": "TEXT",
    "sl_entry_vwap": "TEXT", "sl_initial_stop": "TEXT", "sl_fill_ts_ms": "INTEGER",
    "highest_trigger_bp": "INTEGER", "current_lock_bp": "INTEGER",
    "desired_stop_price": "TEXT", "sl_active_stop_price": "TEXT",
    "pending_stop_algo_id": "TEXT", "pending_stop_client_id": "TEXT",
    "pending_stop_price": "TEXT", "stop_revision": "INTEGER",
    "last_step_event_ts_ms": "INTEGER", "last_step_price": "TEXT",
    "replacement_status": "TEXT",
}
FROZEN_FIELDS = {"exit_profile", "exit_profile_version", "profile_contract_hash",
                 "sl_entry_vwap", "sl_initial_stop", "sl_fill_ts_ms"}
WORKING = {"NEW", "WORKING"}
TERMINAL = {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}


def decimal(value):
    if isinstance(value, bool):
        raise ValueError("INVALID_DECIMAL")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("INVALID_DECIMAL") from None
    if not result.is_finite() or result <= 0:
        raise ValueError("INVALID_DECIMAL")
    return result


def profile(value, *, legacy=False):
    # Only absent legacy DB fields inherit the historical default. Empty/unknown
    # configured values are errors, never an implicit strategy change.
    if value is None and legacy:
        return "CURRENT_TP2"
    if value not in PROFILES:
        raise ValueError(UNKNOWN)
    return value


def migrate(conn):
    columns = {r[1] for r in conn.execute("PRAGMA table_info(autotrade_trades)")}
    for name, kind in SCHEMA.items():
        if name not in columns:
            conn.execute(f"ALTER TABLE autotrade_trades ADD COLUMN {name} {kind}")
    # No UPDATE/backfill: old trades retain their exact values and profile.


def levels(entry, price, tick_size):
    entry, price, tick = map(decimal, (entry, price, tick_size))
    with localcontext() as ctx:
        ctx.prec = 60
        trigger = min(1000, int(((price-entry)*10000/(entry*50)).to_integral_value(rounding=ROUND_FLOOR))*50)
        if trigger < 50:
            return 0, 0, None
        lock = 25 if trigger == 50 else trigger-50
        exact = entry*(1+Decimal(lock)/10000)
        stop = (exact/tick).to_integral_value(rounding=ROUND_FLOOR)*tick
    return trigger, lock, stop


def client_id(trade_id, revision):
    if int(trade_id) <= 0 or int(revision) < 0:
        raise ValueError("INVALID_STOP_IDENTITY")
    value = f"PSL1-{int(trade_id)}-{int(revision)}"
    if len(value) > 36:
        raise ValueError("STOP_IDENTITY_TOO_LONG")
    return value


def entry_fields(entry, initial_stop, fill_ts_ms, *, dry=False):
    entry, stop = decimal(entry), decimal(initial_stop)
    if stop >= entry or int(fill_ts_ms) <= 0:
        raise ValueError("INVALID_ENTRY_STATE")
    return dict(exit_profile_version=VERSION, profile_contract_hash=CONTRACT_HASH,
                sl_entry_vwap=str(entry), sl_initial_stop=str(stop), sl_fill_ts_ms=int(fill_ts_ms),
                highest_trigger_bp=0, current_lock_bp=0, desired_stop_price=str(stop),
                sl_active_stop_price=str(stop), stop_revision=0,
                last_step_event_ts_ms=0, last_step_price=None,
                replacement_status="DRY_STABLE" if dry else "AWAITING_INITIAL_STOP")


def validate_state(tr):
    if profile(tr.get("exit_profile"), legacy=True) != PROFILE:
        raise ValueError("WRONG_PROFILE")
    if tr.get("exit_profile_version") != VERSION or tr.get("profile_contract_hash") != CONTRACT_HASH:
        raise ValueError("PROFILE_CONTRACT_MISMATCH")
    entry, initial, active, desired = map(decimal, (tr.get("sl_entry_vwap"), tr.get("sl_initial_stop"),
                                                      tr.get("sl_active_stop_price"), tr.get("desired_stop_price")))
    if initial >= entry or active < initial or desired < active or int(tr.get("sl_fill_ts_ms") or 0) <= 0:
        raise ValueError("STOP_STATE_MISMATCH")
    trigger, lock = int(tr.get("highest_trigger_bp") or 0), int(tr.get("current_lock_bp") or 0)
    if trigger not in range(0, 1001, 50) or lock != (0 if trigger == 0 else 25 if trigger == 50 else trigger-50):
        raise ValueError("STEP_STATE_MISMATCH")
    ceiling = initial if trigger == 0 else max(initial, entry*(1+Decimal(lock)/10000))
    if desired > ceiling:
        raise ValueError("STOP_ABOVE_CONTRACT_LOCK")
    if tr.get("pending_stop_client_id"):
        if tr["pending_stop_client_id"] != client_id(tr["id"], tr["stop_revision"]):
            raise ValueError("PENDING_IDENTITY_MISMATCH")
        if not active < decimal(tr.get("pending_stop_price")) <= desired:
            raise ValueError("PENDING_PRICE_MISMATCH")


def tick_fields(tr, price, event_ms, tick_size, *, last_seen_ms=0):
    """No network or writes. Returns only a new highest-step durable update."""
    validate_state(tr)
    price = decimal(price)
    if isinstance(event_ms, bool) or not isinstance(event_ms, int) or event_ms <= 0:
        return {}
    if event_ms < int(tr["sl_fill_ts_ms"]) or event_ms <= max(last_seen_ms, int(tr.get("last_step_event_ts_ms") or 0)):
        return {}
    trigger, lock, stop = levels(tr["sl_entry_vwap"], price, tick_size)
    if trigger <= int(tr.get("highest_trigger_bp") or 0):
        return {}
    fields = dict(highest_trigger_bp=trigger, current_lock_bp=lock,
                  last_step_event_ts_ms=event_ms, last_step_price=str(price))
    # A reached step is durable even if tick-size makes it non-actionable.
    if stop > max(decimal(tr["sl_active_stop_price"]), decimal(tr["desired_stop_price"])) and stop < price:
        fields["desired_stop_price"] = str(stop)
        if not tr.get("pending_stop_client_id"):
            fields["replacement_status"] = "DESIRED"
    return fields


def verify_order(order, tr, cid, target, *, order_id=None, working=True):
    """Strict ownership + close-all semantics, for BOTH and LONG only."""
    if not isinstance(order, dict) or not order.get("algoId"):
        raise ValueError("STOP_ACK_MISSING")
    if (str(order.get("clientAlgoId")) != str(cid) or order.get("symbol") != tr["symbol"]
            or order.get("side") != "SELL" or order.get("positionSide") != tr.get("position_side")
            or tr.get("position_side") not in ("BOTH", "LONG")
            or order.get("orderType", order.get("type")) != "STOP_MARKET"
            or order.get("workingType") != "CONTRACT_PRICE"
            or str(order.get("closePosition")).lower() != "true"
            or Decimal(str(order.get("quantity", "0"))) != 0
            or decimal(order.get("triggerPrice")) != decimal(target)
            or (order_id and str(order["algoId"]) != str(order_id))):
        raise ValueError("STOP_OWNERSHIP_MISMATCH")
    if working and order.get("algoStatus") not in WORKING:
        raise ValueError("STOP_NOT_WORKING")
    return str(order["algoId"])


def verify_position(tr, positions):
    matches = [p for p in positions if p.get("symbol") == tr["symbol"] and Decimal(str(p.get("positionAmt", 0))) != 0]
    if (len(matches) != 1 or matches[0].get("positionSide") != tr.get("position_side")
            or decimal(matches[0].get("positionAmt")) != decimal(tr["expected_qty"])
            or tr.get("side") != "LONG" or tr.get("manual_intervention")):
        raise ValueError("MANUAL_INTERVENTION_POSITION_MISMATCH")


async def reconcile(trade_id, load, persist, exchange, *, overlap_proven=False):
    """Durable protocol for a future approved adapter; tests use a fake exchange.

    persist must commit before returning. Query uncertainty is never absence.
    A persisted INTENT is query-only after restart (no blind re-POST). At most
    one pending revision may coexist with the old active stop.
    """
    tr = load(trade_id)
    validate_state(tr)
    verify_position(tr, await exchange.positions())
    active = await exchange.query(tr["stop_client_id"])
    verify_order(active, tr, tr["stop_client_id"], tr["sl_active_stop_price"],
                 order_id=tr["stop_algo_id"], working=not bool(tr.get("pending_stop_client_id")))
    open_orders = await exchange.open_orders(tr["symbol"])
    allowed = {tr["stop_client_id"], tr.get("pending_stop_client_id")}
    if any(o.get("clientAlgoId") not in allowed for o in open_orders):
        raise ValueError("UNOWNED_OR_EXTRA_ORDER")
    cid = tr.get("pending_stop_client_id")
    if not cid:
        if not overlap_proven:
            return LIVE_BLOCK_REASON
        target = decimal(tr["desired_stop_price"])
        if target <= decimal(tr["sl_active_stop_price"]):
            return "STABLE"
        tick, market = map(decimal, await exchange.price_filter_and_market(tr["symbol"]))
        derived = levels(tr["sl_entry_vwap"], tr["last_step_price"], tick)[2]
        if target != derived or target % tick != 0 or target >= market:
            return "INVALID_TARGET_KEEP_OLD_STOP"
        rev = int(tr["stop_revision"])+1
        cid = client_id(trade_id, rev)
        persist(trade_id, pending_stop_client_id=cid, pending_stop_price=str(target),
                stop_revision=rev, replacement_status="INTENT")
        tr = load(trade_id)
        try:
            await exchange.post(tr, cid, str(target))
        except Exception:
            persist(trade_id, replacement_status="POST_UNCERTAIN")
    # Always query, even after an apparently successful POST. No ACK, no cancel.
    try:
        new = await exchange.query(cid)
        new_id = verify_order(new, tr, cid, tr["pending_stop_price"], order_id=tr.get("pending_stop_algo_id"))
    except Exception:
        persist(trade_id, replacement_status="QUERY_UNCERTAIN")
        return "QUERY_UNCERTAIN"
    persist(trade_id, pending_stop_algo_id=new_id, replacement_status="NEW_CONFIRMED")
    # Recheck position and new protection immediately before touching the old stop.
    verify_position(tr, await exchange.positions())
    verify_order(await exchange.query(cid), tr, cid, tr["pending_stop_price"], order_id=new_id)
    if active.get("algoStatus") not in TERMINAL:
        if active.get("algoStatus") not in WORKING:
            return "OLD_STOP_TRIGGERING_RECONCILE_REQUIRED"
        persist(trade_id, replacement_status="CANCEL_UNCERTAIN")
        try:
            await exchange.cancel(tr["stop_algo_id"])
        except Exception:
            pass
    old = await exchange.query(tr["stop_client_id"])
    verify_order(old, tr, tr["stop_client_id"], tr["sl_active_stop_price"], order_id=tr["stop_algo_id"], working=False)
    verify_order(await exchange.query(cid), tr, cid, tr["pending_stop_price"], order_id=new_id)
    if old.get("algoStatus") not in TERMINAL:
        return "CANCEL_UNCERTAIN"
    verify_position(tr, await exchange.positions())
    # Only acknowledge the price actually installed; newer desired ticks survive.
    latest = load(trade_id)
    persist(trade_id, stop_algo_id=new_id, stop_client_id=cid,
            stop_price=float(tr["pending_stop_price"]), sl_active_stop_price=tr["pending_stop_price"],
            pending_stop_client_id=None, pending_stop_algo_id=None, pending_stop_price=None,
            replacement_status="DESIRED" if decimal(latest["desired_stop_price"]) > decimal(tr["pending_stop_price"]) else "STABLE")
    return "REPLACED"


def rollback_readiness(trades, *, mode, global_profile, exchange_orders=None):
    rows = list(trades)
    opened = [t for t in rows if t.get("status") != "CLOSED"]
    counts = Counter("CURRENT_TP2" if t.get("exit_profile") is None else str(t["exit_profile"]) for t in opened)
    pending = sum(bool(t.get("pending_stop_client_id") or t.get("pending_stop_algo_id")) for t in rows)
    step_orders = None if exchange_orders is None else sum(str(o.get("clientAlgoId", "")).startswith("PSL1-") for o in exchange_orders)
    blockers = []
    if mode != "OFF": blockers.append("ENTRIES_NOT_OFF")
    if global_profile != "CURRENT_TP2": blockers.append("GLOBAL_PROFILE_NOT_CURRENT_TP2")
    if counts[PROFILE]: blockers.append("ROLLBACK_UNSAFE_OPEN_STEP_LOCK_POSITION")
    if pending: blockers.append("PENDING_STOP_REPLACEMENT")
    if step_orders is None: blockers.append("EXCHANGE_RECONCILIATION_REQUIRED")
    elif step_orders: blockers.append("BOT_OWNED_STEP_LOCK_STOP_REMAINS")
    if any(p not in PROFILES for p in counts): blockers.append(UNKNOWN)
    return dict(open_by_profile=dict(counts), pending_replacements=pending,
                bot_owned_step_lock_stops=step_orders, exchange_reconciliation_required=step_orders is None,
                rollback_safe=not blockers, blockers=blockers)
