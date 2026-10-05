"""Shadow-only telemetry for H1 Early checkpoint latency.

This module never sends notifications, places orders, changes thresholds, or gates
production decisions. It only records when the existing Public Early gate first
becomes eligible after 2/3 continuity and compares that timestamp with the actual
Public Early timestamp produced by production cadence.
"""
import json
import logging
import time

log = logging.getLogger(__name__)
VERSION = "h1-early-checkpoint-latency-shadow-v1"
_state = {}


def migrate(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS early_checkpoint_latency_shadow_v1 (
               episode_id INTEGER PRIMARY KEY,
               symbol TEXT NOT NULL,
               candidate_start_ts_ms INTEGER NOT NULL,
               first_pass2_ts_ms INTEGER,
               first_ready_ts_ms INTEGER,
               first_ready_price REAL,
               first_ready_confirm_passes INTEGER,
               prior_eval_ts_ms INTEGER,
               prior_failed_gates_json TEXT NOT NULL DEFAULT '[]',
               would_create_radar INTEGER,
               actual_early_ts_ms INTEGER,
               actual_early_price REAL,
               actual_confirm_passes INTEGER,
               seconds_saved REAL,
               price_saved_pct REAL,
               end_ts_ms INTEGER,
               end_reason TEXT,
               status TEXT NOT NULL,
               restart_gap INTEGER NOT NULL DEFAULT 0,
               version TEXT NOT NULL,
               updated_ts_ms INTEGER NOT NULL
           )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_h1_checkpoint_status ON early_checkpoint_latency_shadow_v1(status,symbol)"
    )


def _pct_change(new, old):
    return ((float(new) / float(old)) - 1.0) * 100.0 if old else None


def _load_existing(connect, episode_id):
    c = connect()
    try:
        c.row_factory = None
        row = c.execute(
            """SELECT first_pass2_ts_ms,first_ready_ts_ms,first_ready_price,
                      first_ready_confirm_passes,prior_eval_ts_ms,
                      prior_failed_gates_json,would_create_radar,
                      actual_early_ts_ms,status,restart_gap
               FROM early_checkpoint_latency_shadow_v1 WHERE episode_id=?""",
            (int(episode_id),),
        ).fetchone()
        if not row:
            return None
        return dict(
            first_pass2_ts_ms=row[0],
            first_ready_ts_ms=row[1],
            first_ready_price=row[2],
            first_ready_confirm_passes=row[3],
            prior_eval_ts_ms=row[4],
            prior_failed=json.loads(row[5] or "[]"),
            would_create_radar=row[6],
            actual_early_ts_ms=row[7],
            status=row[8],
            restart_gap=int(row[9] or 0),
        )
    finally:
        c.close()


def observe(connect, *, episode_id, symbol, candidate_start_ts_ms, observed_ts_ms,
            price, confirm_passes, ready, failed_gates, would_create_radar=False):
    """Observe one decision-time snapshot. Failures are isolated from production."""
    try:
        episode_id = int(episode_id or 0)
        confirm_passes = int(confirm_passes or 0)
        if episode_id <= 0 or confirm_passes < 2:
            return
        observed_ts_ms = int(observed_ts_ms)
        failed = tuple(sorted({str(x) for x in (failed_gates or [])}))
        state = _state.get(episode_id)
        if state is None:
            existing = _load_existing(connect, episode_id)
            state = dict(
                first_pass2_ts_ms=(existing or {}).get("first_pass2_ts_ms"),
                first_ready_ts_ms=(existing or {}).get("first_ready_ts_ms"),
                first_ready_price=(existing or {}).get("first_ready_price"),
                last_eval_ts_ms=(existing or {}).get("prior_eval_ts_ms"),
                last_failed=tuple((existing or {}).get("prior_failed") or ()),
                restart_gap=1 if existing else 0,
                persisted=bool(existing),
            )
            _state[episode_id] = state

        if state["first_pass2_ts_ms"] is None:
            state["first_pass2_ts_ms"] = observed_ts_ms

        if not state["persisted"]:
            c = connect()
            try:
                c.execute(
                    """INSERT OR IGNORE INTO early_checkpoint_latency_shadow_v1(
                           episode_id,symbol,candidate_start_ts_ms,first_pass2_ts_ms,
                           status,restart_gap,version,updated_ts_ms)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        episode_id, str(symbol), int(candidate_start_ts_ms),
                        int(state["first_pass2_ts_ms"]), "TRACKING",
                        int(state["restart_gap"]), VERSION, observed_ts_ms,
                    ),
                )
                c.commit()
            finally:
                c.close()
            state["persisted"] = True

        if ready and state["first_ready_ts_ms"] is None:
            prior_ts = state.get("last_eval_ts_ms")
            prior_failed = list(state.get("last_failed") or ())
            state["first_ready_ts_ms"] = observed_ts_ms
            state["first_ready_price"] = float(price)
            c = connect()
            try:
                c.execute(
                    """UPDATE early_checkpoint_latency_shadow_v1
                       SET first_ready_ts_ms=?,first_ready_price=?,
                           first_ready_confirm_passes=?,prior_eval_ts_ms=?,
                           prior_failed_gates_json=?,would_create_radar=?,
                           status='READY',restart_gap=?,updated_ts_ms=?
                       WHERE episode_id=?""",
                    (
                        observed_ts_ms, float(price), confirm_passes, prior_ts,
                        json.dumps(prior_failed, sort_keys=True),
                        int(bool(would_create_radar)), int(state["restart_gap"]),
                        observed_ts_ms, episode_id,
                    ),
                )
                c.commit()
            finally:
                c.close()
            log.info(
                "H1_CHECKPOINT_READY symbol=%s episode=%s passes=%s prior_failed=%s",
                symbol, episode_id, confirm_passes,
                ",".join(prior_failed) if prior_failed else "NONE",
            )
            return

        if state["first_ready_ts_ms"] is None:
            state["last_eval_ts_ms"] = observed_ts_ms
            state["last_failed"] = failed
    except Exception:
        log.exception("H1_CHECKPOINT_SHADOW_ERROR operation=observe")


def public_early(connect, *, episode_id, symbol, candidate_start_ts_ms,
                 actual_ts_ms, actual_price, confirm_passes):
    """Record the production Public Early and compute checkpoint-only delay."""
    try:
        episode_id = int(episode_id or 0)
        if episode_id <= 0:
            return
        actual_ts_ms = int(actual_ts_ms)
        row = _load_existing(connect, episode_id)
        if row is None:
            # A Public Early can occur on the exact 2/3 checkpoint. Preserve it
            # as a zero-latency observation rather than inventing an earlier time.
            c = connect()
            try:
                c.execute(
                    """INSERT OR IGNORE INTO early_checkpoint_latency_shadow_v1(
                           episode_id,symbol,candidate_start_ts_ms,first_pass2_ts_ms,
                           first_ready_ts_ms,first_ready_price,first_ready_confirm_passes,
                           prior_failed_gates_json,would_create_radar,
                           actual_early_ts_ms,actual_early_price,actual_confirm_passes,
                           seconds_saved,price_saved_pct,status,restart_gap,
                           version,updated_ts_ms)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        episode_id, str(symbol), int(candidate_start_ts_ms),
                        actual_ts_ms, actual_ts_ms, float(actual_price),
                        int(confirm_passes or 0), "[]", 0, actual_ts_ms,
                        float(actual_price), int(confirm_passes or 0),
                        0.0, 0.0, "PUBLIC_EARLY", 0, VERSION, actual_ts_ms,
                    ),
                )
                c.commit()
            finally:
                c.close()
            _state.pop(episode_id, None)
            log.info(
                "H1_CHECKPOINT_PUBLIC symbol=%s episode=%s saved_s=0.000 price_saved_pct=0.000000",
                symbol, episode_id,
            )
            return

        ready_ts = row.get("first_ready_ts_ms")
        ready_price = row.get("first_ready_price")
        saved_s = None if ready_ts is None else max(0.0, (actual_ts_ms - int(ready_ts)) / 1000.0)
        price_saved = None if ready_price is None else _pct_change(actual_price, ready_price)
        c = connect()
        try:
            c.execute(
                """UPDATE early_checkpoint_latency_shadow_v1
                   SET actual_early_ts_ms=?,actual_early_price=?,
                       actual_confirm_passes=?,seconds_saved=?,price_saved_pct=?,
                       status='PUBLIC_EARLY',updated_ts_ms=?
                   WHERE episode_id=?""",
                (
                    actual_ts_ms, float(actual_price), int(confirm_passes or 0),
                    saved_s, price_saved, actual_ts_ms, episode_id,
                ),
            )
            c.commit()
        finally:
            c.close()
        _state.pop(episode_id, None)
        log.info(
            "H1_CHECKPOINT_PUBLIC symbol=%s episode=%s saved_s=%s price_saved_pct=%s",
            symbol, episode_id,
            "UNKNOWN" if saved_s is None else f"{saved_s:.3f}",
            "UNKNOWN" if price_saved is None else f"{price_saved:.6f}",
        )
    except Exception:
        log.exception("H1_CHECKPOINT_SHADOW_ERROR operation=public_early")


def end(connect, *, episode_id, end_ts_ms=None, reason=""):
    """Close a tracked episode without changing production episode state."""
    try:
        episode_id = int(episode_id or 0)
        if episode_id <= 0:
            return
        row = _load_existing(connect, episode_id)
        _state.pop(episode_id, None)
        if row is None or row.get("status") == "PUBLIC_EARLY":
            return
        end_ts_ms = int(end_ts_ms or time.time() * 1000)
        status = "READY_NO_PUBLIC" if row.get("first_ready_ts_ms") is not None else "NO_READY"
        c = connect()
        try:
            c.execute(
                """UPDATE early_checkpoint_latency_shadow_v1
                   SET end_ts_ms=?,end_reason=?,status=?,updated_ts_ms=?
                   WHERE episode_id=?""",
                (end_ts_ms, str(reason)[:100], status, end_ts_ms, episode_id),
            )
            c.commit()
        finally:
            c.close()
        if status == "READY_NO_PUBLIC":
            log.info(
                "H1_CHECKPOINT_END episode=%s status=%s reason=%s",
                episode_id, status, str(reason)[:100],
            )
    except Exception:
        log.exception("H1_CHECKPOINT_SHADOW_ERROR operation=end")


def reset_runtime_state():
    """Test helper; production never needs to call this."""
    _state.clear()
