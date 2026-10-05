"""Recognize exact reviewed additive research hooks; preserve all legacy AST fingerprints."""
import ast

SNIPPETS = [
'''if early_v2:
    early_v2.arm(st.active_radar_id, symbol, m, score)''',
'''if early_v2:
    await early_v2.premium(session, signal_id, symbol, m, plan)
else:
    await autotrade_handle_premium(session, signal_id, symbol, m, plan)''',
'''if early_v2:
    early_v2.tick(sym, price, ts, recv_ms, d.get("a"))''',
'''if early_v2 and await early_v2.callback(session, upd["callback_query"]):
    continue''',
'''if early_v2 and raw_text and await early_v2.command(session, raw_text, chat_id, user_id):
    continue''',
'''if early_v2:
    await telegram_send(session, "Premium AutoTrader: " + autotrade_cfg["mode"] + "\\n" + early_v2.status(), chat_id=chat_id)''',
'global early_v2',
'early_v2 = early_v2_adapter.Integration(globals())',
'early_v2 = None',
'import early_v2_adapter',
' tasks.append(early_v2.run(session))'.strip(),
'''_h1_observe_early_checkpoint_shadow(symbol, m, score, now)''',
'''early_checkpoint_shadow.public_early(
    db_connect,
    episode_id=st.episode_id,
    symbol=symbol,
    candidate_start_ts_ms=int(st.candidate_since * 1000),
    actual_ts_ms=int(now * 1000),
    actual_price=m["price"],
    confirm_passes=st.candidate_passes,
)''',
'''early_checkpoint_shadow.end(db_connect, episode_id=st.episode_id, end_ts_ms=now_ms(), reason=reason)''',
'''early_checkpoint_shadow.migrate(conn)''',
'''if os.getenv("RAILWAY_SERVICE_ID") == "c5e4a28a-8829-4434-bfee-16297373244f":
    try:
        early_exit_export.emit(
            DB_PATH,
            printer=lambda *parts, **kwargs: log.info("%s", " ".join(str(x) for x in parts)),
        )
    except Exception as exc:
        log.error("EARLY_EXIT_EXPORT_V1 ERROR %s", type(exc).__name__)''',
'import early_checkpoint_shadow',
'import early_exit_export',
'''def _early_notify_failures(m: dict, score: int, st: SymbolState) -> List[str]:
    """Shadow explanation of the existing Public Early gate; never changes a decision."""
    failed = []
    if st.candidate_passes < 2: failed.append("CONFIRM_LT_2")
    if score < EARLY_NOTIFY_MIN_SCORE: failed.append("SCORE")
    if m["chg30"] < EARLY_NOTIFY_MIN_CHG30: failed.append("CHG30")
    if m["chg60"] < EARLY_NOTIFY_MIN_CHG60: failed.append("CHG60")
    if m["flow30"] < EARLY_NOTIFY_MIN_FLOW30: failed.append("FLOW30")
    if not (EARLY_NOTIFY_MIN_BUY30 <= m["buy30"] <= EARLY_NOTIFY_MAX_BUY30): failed.append("BUY30")
    if m["chg5"] > EARLY_NOTIFY_MAX_CHG5: failed.append("CHG5_MAX")
    if m["book_imbalance"] > EARLY_ALERT_MAX_BOOK: failed.append("BOOK")
    if m["spread"] > min(MAX_SPREAD_PCT, 0.30): failed.append("SPREAD")
    if m["extended"]: failed.append("EXTENDED")
    if not (m["breakout"] or m["rel30"] >= 0.20): failed.append("BREAKOUT_OR_REL30")
    return failed''',
'''def _h1_observe_early_checkpoint_shadow(symbol: str, m: dict, score: int, now: float):
    """Measure when unchanged Public Early conditions first become true between 15s checkpoints."""
    try:
        st = states[symbol]
        if not st.episode_id or st.candidate_passes < 2 or st.active_radar_notified:
            return
        failures = _early_notify_failures(m, score, st)
        can_create_radar = bool(
            st.active_radar_id or (
                early_watch_pass(m, score)
                and now - st.radar_record_ts >= EARLY_RADAR_RECORD_COOLDOWN_SECONDS
            )
        )
        cooldown_ok = now - st.early_alert_ts >= EARLY_ALERT_COOLDOWN_SECONDS
        ready = bool(can_create_radar and cooldown_ok and not failures)
        shadow_failures = list(failures)
        if not can_create_radar: shadow_failures.append("NO_RADAR_ELIGIBILITY")
        if not cooldown_ok: shadow_failures.append("EARLY_COOLDOWN")
        early_checkpoint_shadow.observe(
            db_connect,
            episode_id=st.episode_id,
            symbol=symbol,
            candidate_start_ts_ms=int(st.candidate_since * 1000),
            observed_ts_ms=int(now * 1000),
            price=m["price"],
            confirm_passes=st.candidate_passes,
            ready=ready,
            failed_gates=shadow_failures,
            would_create_radar=bool(not st.active_radar_id and can_create_radar),
        )
    except Exception:
        log.exception("H1_CHECKPOINT_SHADOW_ERROR operation=bot_observe")''',
]
APPROVED={ast.dump(ast.parse(s).body[0],include_attributes=False) for s in SNIPPETS}

class StripEarlyHooks(ast.NodeTransformer):
    def visit(self,node):
        if ast.dump(node,include_attributes=False) in APPROVED:
            return node.orelse if isinstance(node,ast.If) and node.orelse else None
        return super().visit(node)
