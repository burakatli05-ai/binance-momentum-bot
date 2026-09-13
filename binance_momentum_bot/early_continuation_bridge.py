"""Read-only snapshots of existing scanner operands; never a decision authority."""
import time

worker = None


def gate_snapshot(stage, m, score, s, c, now):
    gates = []

    def add(name, observed, threshold, passed):
        gates.append(dict(name=name, observed=observed, threshold=threshold, passed=bool(passed)))

    def ge(key, threshold, value=None):
        v = m[key] if value is None else value
        add(key, v, {'min': threshold}, v >= threshold)

    def le(key, threshold):
        add(key, m[key], {'max': threshold}, m[key] <= threshold)

    def between(key, low, high):
        add(key, m[key], {'min': low, 'max': high}, low <= m[key] <= high)

    if stage in ('candidate_evaluation', 'candidate_active_evaluation'):
        ge('qv24', c['MIN_24H_QUOTE_VOLUME'])
        if not s.candidate_since:
            add('fast_price', [m['chg10'], m['chg30']], [c['MIN_CHG_10S'], c['MIN_CHG_30S']],
                m['chg10'] >= c['MIN_CHG_10S'] or m['chg30'] >= c['MIN_CHG_30S'])
            add('fast_flow', [m['flow10'], m['flow30']], [c['MIN_FLOW_X_10S'], c['MIN_FLOW_X_30S']],
                m['flow10'] >= c['MIN_FLOW_X_10S'] or m['flow30'] >= c['MIN_FLOW_X_30S'])
            ge('buy30', c['MIN_BUY_RATIO_30S'])
            le('spread', c['MAX_SPREAD_PCT'])
            add('enough_trades', [m['trades10'], m['trades30']], [2, 4], m['trades10'] >= 2 or m['trades30'] >= 4)
            ge('score', c['EARLY_SCORE'], score)
        else:
            age = now-s.candidate_since
            add('candidate_ttl', age, {'max': c['CANDIDATE_TTL_SECONDS']}, age <= c['CANDIDATE_TTL_SECONDS'])
            for key, threshold in [('chg30', -.20), ('buy30', .50), ('flow30', .8)]: ge(key, threshold)
            add('confirm_interval', now-s.candidate_last_check, {'min': c['CONFIRM_INTERVAL_SECONDS']},
                now-s.candidate_last_check >= c['CONFIRM_INTERVAL_SECONDS'])
    elif stage == 'continuity_evaluation':
        ge('score', c['CONFIRM_MIN_SCORE'], score)
        ge('chg30', .12)
        ge('chg60', .30)
        ge('flow30', 1.5)
        between('buy30', .58, .92)
        le('spread', min(c['MAX_SPREAD_PCT'], .30))
        add('not_extended', m['extended'], False, not m['extended'])
        absorption = (m['buy30'] >= .86 and m['chg30'] < .55) or (m['flow30'] >= 12 and m['chg30'] < .45)
        add('not_absorption', [m['buy30'], m['chg30'], m['flow30']],
            {'buy_min': .86, 'chg_max_exclusive': .55, 'or_flow_min': 12, 'or_chg_max_exclusive': .45}, not absorption)
        prices = list(s.candidate_prices)
        add('price_continuity', prices[-2:], {'min_previous_ratio': .999},
            len(prices) < 2 or prices[-1] >= prices[-2]*.999)
    elif stage in ('early_watch_evaluation', 'early_notify_evaluation'):
        notify = stage == 'early_notify_evaluation'
        prefix = 'EARLY_NOTIFY_' if notify else 'EARLY_ALERT_'
        if notify:
            add('active_radar', s.active_radar_id, 'present', bool(s.active_radar_id))
            add('not_already_notified', s.active_radar_notified, False, not s.active_radar_notified)
            add('alert_cooldown', now-s.early_alert_ts, {'min': c['EARLY_ALERT_COOLDOWN_SECONDS']},
                now-s.early_alert_ts >= c['EARLY_ALERT_COOLDOWN_SECONDS'])
            ge('candidate_passes', 2, s.candidate_passes)
        else:
            add('alert_enabled', c['EARLY_ALERT_ENABLED'], True, c['EARLY_ALERT_ENABLED'])
        ge('score', c[prefix+'MIN_SCORE'], score)
        for key in ('chg30', 'chg60', 'flow30'): ge(key, c[prefix+'MIN_'+key.upper()])
        between('buy30', c[prefix+'MIN_BUY30'], c[prefix+'MAX_BUY30'])
        if notify: le('chg5', c['EARLY_NOTIFY_MAX_CHG5'])
        le('book_imbalance', c['EARLY_ALERT_MAX_BOOK'])
        le('spread', min(c['MAX_SPREAD_PCT'], .30))
        add('not_extended', m['extended'], False, not m['extended'])
        rel = .20 if notify else .15
        add('breakout_or_relative_strength', [m['breakout'], m['rel30']], {'breakout': True, 'rel30_min': rel},
            m['breakout'] or m['rel30'] >= rel)
        if not notify:
            add('radar_cooldown', now-s.radar_record_ts, {'min': c['EARLY_RADAR_RECORD_COOLDOWN_SECONDS']},
                now-s.radar_record_ts >= c['EARLY_RADAR_RECORD_COOLDOWN_SECONDS'])
    return gates


def capture(bot, symbol, kind, m, score=None, source_id=None, context=None, trend_score=None, terminal=None, note=None, source_table=None):
    if worker is None: return
    try:
        s = bot['states'][symbol]
        if kind == 'candidate_evaluation' and s.candidate_since:
            kind = 'candidate_active_evaluation'
        now = time.time()
        # Do not calculate rolling candle context at scanner frequency for sampled captures.
        if kind in ('candidate_evaluation', 'trend_evaluation'):
            if int(now*1000)-worker.samples.get((symbol, kind), 0) < 60_000: return
        ctx = context if context is not None else bot['_trend_build_context'](symbol, m)
        operands = dict(m)
        operands.update(ctx)
        operands.update(score=score, trend_score=trend_score, gainers_rank=bot['gainers_prev_rank'].get(symbol),
                        candidate_age_s=(now-s.candidate_since) if s.candidate_since else None,
                        candidate_passes=s.candidate_passes)
        gates = gate_snapshot(kind, operands, score, s, bot, now)
        recorded = terminal is not None or source_id is not None
        if recorded:
            gates.append(dict(name='recorded_transition', observed={'event': kind, 'reason': terminal, 'note': note}, threshold=None, passed=None))
        blocker = terminal if ('reject' in kind or 'wait' in kind or kind == 'episode_reset') else None
        first_failure = next((g['name'] for g in gates if g['passed'] is False), None)
        source, receive = worker.sources.get(symbol, (None, None))
        worker.event(symbol, kind, operands, decision_ts=int(now*1000),
            source_table=source_table or ('candidate_events' if source_id is not None else None), source_id=source_id,
            source_episode_id=s.episode_id or None,
            nominal_ts=None, ready_ts=None,
            source_event_ts=source, receive_ts=receive,
            price=m.get('price'), gates=gates, terminal=blocker or first_failure,
            evaluation_kind='RECORDED_TRANSITION' if recorded else 'SHADOW_OPERAND_SNAPSHOT')
    except Exception:
        bot['log'].exception('Early continuation capture failed')


def tick(symbol, price, source, receive):
    if worker is not None:
        worker.tick(symbol, price, source, receive)


def research(bot, source_id, kind, symbol, m, score, trend_score):
    if kind == 'TREND_BUILDUP':
        capture(bot, symbol, kind, m, score, source_id=source_id,
                source_table='research_events', trend_score=trend_score)


def start(bot):
    global worker
    import early_continuation
    config = {k: v for k, v in bot.items() if k.isupper() and
              k.startswith(('MIN_', 'MAX_', 'EARLY_', 'CONFIRM_', 'CANDIDATE_', 'TREND_BUILDUP_', 'PREMIUM_'))
              and isinstance(v, (str, int, float, bool))}
    worker = early_continuation.start(bot['DB_PATH'], config, bot['BOT_VERSION'])
