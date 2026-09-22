"""Pure, long-only SHADOW counterfactual. No bot, database, exchange or IO imports.

Percentages are unlevered percentage points. Replay starts only when the complete
entry fill allocation was available; it never backdates a reconciled VWAP.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import math


VERSION = 'pm-v2-shadow-20260922-v1'
EPS = 1e-9


def number(value, name, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f'{name}: finite number required')
    if positive and value <= 0:
        raise ValueError(f'{name}: positive number required')
    return value


@dataclass(frozen=True)
class Policy:
    first_lock_pct: float = .25
    confirm_trades: int = 3
    confirm_ms: int = 200
    max_gap_ms: int = 2000
    freshness_ms: int = 2000
    trail_floor_pct: float = .4
    trail_peak_fraction: float = 1 / 3
    runner_pct: float = 3.
    early_exit_extra_pct: float = 1.

    def __post_init__(self):
        for name, value in asdict(self).items():
            number(value, name, True)
        if not .20 <= self.first_lock_pct <= .25:
            raise ValueError('first lock must be .20 to .25 percent')
        if type(self.confirm_trades) is not int or self.confirm_trades < 2:
            raise ValueError('confirmation requires at least two distinct trades')
        if not 0 < self.trail_peak_fraction < 1 or self.confirm_ms > self.max_gap_ms:
            raise ValueError('invalid trailing or confirmation policy')

    @property
    def digest(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()


def entry_reference(cohort, allow_signal_proxy=False):
    """Accept only explicitly allocated, complete Binance BUY fills for this Early.

    Do not infer Early fills by symbol, timestamp proximity or a later Premium link.
    Unknown commissions are handled in the cost model, not silently set to zero.
    """
    decision = number(cohort['decision_ms'], 'decision_ms', True)
    fills = cohort.get('fills', [])
    if not fills:
        if not allow_signal_proxy:
            return None
        return dict(price=number(cohort['signal_price'], 'signal_price', True),
                    start_ms=decision, kind='SIGNAL_PRICE_PROXY', quantity=None,
                    fill_ids=[])
    if cohort.get('fills_complete') is not True:
        raise ValueError('entry allocation must be complete before replay')
    available = number(cohort['fills_available_ms'], 'fills_available_ms', True)
    seen = {}
    quantity = quote = 0.
    for fill in fills:
        if (fill.get('source') != 'BINANCE' or fill.get('side') != 'BUY'
                or fill.get('early_id') != cohort['early_id']
                or fill.get('symbol') != cohort['symbol']):
            raise ValueError('fill provenance/allocation mismatch')
        key = tuple(str(fill[k]) for k in ('account_ref', 'symbol', 'order_id', 'trade_id'))
        if any(not x or x == 'None' for x in key):
            raise ValueError('missing fill identity')
        price = number(fill['price'], 'fill.price', True)
        qty = number(fill['qty'], 'fill.qty', True)
        stamp = number(fill['event_ms'], 'fill.event_ms', True)
        if not decision <= stamp <= available:
            raise ValueError('fill time outside causal entry interval')
        value = (price, qty, stamp)
        if key in seen:
            if seen[key] != value:
                raise ValueError('conflicting duplicate fill')
            continue
        seen[key] = value
        quantity += qty
        quote += price * qty
    if len({k[0] for k in seen}) != 1:
        raise ValueError('multiple accounts in one entry allocation')
    expected = number(cohort['executed_qty'], 'executed_qty', True)
    if not math.isclose(quantity, expected, rel_tol=1e-9, abs_tol=1e-12):
        raise ValueError('fill quantity reconciliation failed')
    return dict(price=quote / quantity, start_ms=available,
                kind='BINANCE_FILL_VWAP', quantity=quantity, fill_ids=list(seen))


class Evidence:
    """Constant-memory confirmation block, also works at high tick rates."""
    def __init__(self):
        self.clear()

    def clear(self):
        self.count = 0
        self.first = self.last = self.low = None

    def append(self, point):
        stamp, ret = point
        if not self.count:
            self.first = stamp
            self.low = ret
        self.count += 1
        self.last = stamp
        self.low = min(self.low, ret)


class ShadowPosition:
    """One Early and one horizon; feed raw trades in observed arrival order.

    Memory is bounded by confirmation count, not tick count. Both hypothetical
    exits freeze independently; path measurements continue through the horizon.
    """
    def __init__(self, cohort, horizon_ms, policy=None, allow_signal_proxy=False):
        if cohort.get('kind') != 'EARLY':
            raise ValueError('only Early V1 events are accepted')
        if not cohort.get('early_id') or not cohort.get('symbol'):
            raise ValueError('Early identity and symbol required')
        self.cohort = dict(cohort)
        self.policy = policy or Policy()
        self.horizon = number(horizon_ms, 'horizon_ms', True)
        self.entry = entry_reference(cohort, allow_signal_proxy)
        self.flags = set(cohort.get('gap_flags', []))
        self.window = Evidence()
        self.breach = Evidence()
        self.last = None
        self.last_received = 0
        self.touches = {}
        self.mfe = self.mae = self.peak = self.drawdown = 0.
        self.confirmed_peak = 0.
        self.exits = {'fixed': None, 'dynamic': None}
        self.stop_updates = 0
        self.post_exit_peak = None
        if self.entry:
            self.initial_stop = number(cohort['initial_stop_price'], 'initial_stop_price', True)
            if self.initial_stop >= self.entry['price']:
                raise ValueError('initial protective stop must be below entry VWAP')
            self.stop = self.initial_stop
            self.deadline = self.entry['start_ms'] + self.horizon

    def _confirmed(self, window):
        return (window.count >= self.policy.confirm_trades
                and window.last - window.first >= self.policy.confirm_ms)

    def _exit(self, strategy, reason, tick, observed_price):
        self.exits[strategy] = dict(reason=reason, event_ms=tick['event_ms'],
                                    observed_ms=tick['received_ms'],
                                    observed_price=observed_price,
                                    execution_model='TRADE_PRICE_EXIT_PROXY')

    def tick(self, tick):
        if not self.entry:
            return
        try:
            price = number(tick['price'], 'trade.price', True)
            stamp = number(tick['event_ms'], 'trade.event_ms', True)
            received = number(tick['received_ms'], 'trade.received_ms', True)
            trade_id = tick['trade_id']
            if type(trade_id) is not int or trade_id < 0:
                raise ValueError('integer aggregate trade id required')
            if tick['symbol'] != self.cohort['symbol']:
                raise ValueError('trade symbol mismatch')
        except (ValueError, KeyError, TypeError):
            self.flags.add('INVALID_TRADE')
            self.window.clear(); self.breach.clear()
            return
        if stamp < self.entry['start_ms'] or stamp > self.deadline:
            return
        if (stamp > received or received - stamp > self.policy.freshness_ms
                or received < self.last_received):
            self.flags.add('STALE_FUTURE_OR_RECEIVE_ORDER')
            self.window.clear(); self.breach.clear()
            return
        if self.last:
            if trade_id == self.last['trade_id']:
                if (stamp, price) != (self.last['event_ms'], self.last['price']):
                    self.flags.add('CONFLICTING_TRADE_ID')
                return
            if trade_id < self.last['trade_id'] or stamp < self.last['event_ms']:
                self.flags.add('OUT_OF_ORDER_TRADE')
                self.window.clear(); self.breach.clear()
                return
        previous = self.last['event_ms'] if self.last else self.entry['start_ms']
        if stamp - previous > self.policy.max_gap_ms:
            self.flags.add('TRADE_OBSERVATION_GAP')
            self.window.clear(); self.breach.clear()
        if self.last and trade_id != self.last['trade_id'] + 1:
            self.flags.add('MISSING_AGG_TRADE_IDS')
            self.window.clear(); self.breach.clear()
        self.last = dict(tick)
        self.last_received = received
        ret = 100 * (price / self.entry['price'] - 1)
        self.mfe = max(self.mfe, ret)
        self.mae = min(self.mae, ret)
        self.peak = max(self.peak, ret)
        self.drawdown = max(self.drawdown, self.peak - ret)
        for level in (.5, 1., -3.):
            touched = ret + EPS >= level if level > 0 else ret - EPS <= level
            if touched and level not in self.touches:
                self.touches[level] = dict(event_ms=stamp, trade_id=trade_id,
                                          observed_ms=received)
        if self.exits['dynamic']:
            self.post_exit_peak = ret if self.post_exit_peak is None else max(self.post_exit_peak, ret)
        if self.exits['fixed'] is None:
            if price <= self.initial_stop + self.entry['price']*EPS/100:
                self._exit('fixed', 'INITIAL_SL', tick, price)
            elif ret + EPS >= 1.:
                # Resting fixed TP: do not award favorable price gaps above +1%.
                self._exit('fixed', 'FIXED_TP_1', tick, self.entry['price'] * 1.01)
        if self.exits['dynamic'] is not None:
            return
        # The initial protective stop remains an immediate floor even while a
        # higher profit stop is waiting for confirmation.
        if price <= self.initial_stop + self.entry['price']*EPS/100:
            self._exit('dynamic', 'INITIAL_SL', tick, price)
            return
        # Check the previously active stop before any ratchet from this tick.
        if self.stop > self.initial_stop and price <= self.stop + self.entry['price']*EPS/100:
            self.breach.append((stamp, ret))
            if self._confirmed(self.breach):
                self._exit('dynamic', 'CONFIRMED_PROFIT_LOCK', tick, price)
                return
        else:
            self.breach.clear()
        if ret + EPS < .5:
            self.window.clear()
            return
        self.window.append((stamp, ret))
        if not self._confirmed(self.window):
            return
        # The minimum across the confirmation window must support the new peak;
        # a single upward wick can neither arm nor ratchet a profit stop.
        peak = self.window.low
        self.window.clear()
        self.confirmed_peak = max(self.confirmed_peak, peak)
        lock = None
        if self.confirmed_peak + EPS >= 1.2:
            gap = max(self.policy.trail_floor_pct,
                      self.confirmed_peak * self.policy.trail_peak_fraction)
            lock = max(.5, self.confirmed_peak - gap)
        elif self.confirmed_peak + EPS >= .8:
            lock = .5
        elif self.confirmed_peak + EPS >= .5:
            lock = self.policy.first_lock_pct
        if lock is not None:
            candidate = self.entry['price'] * (1 + lock / 100)
            if candidate > self.stop + self.entry['price']*EPS/100:
                self.stop = candidate
                self.stop_updates += 1
                self.breach.clear()

    def finish(self, observation_end_ms, coverage_complete=False):
        base = dict(version=VERSION, policy=asdict(self.policy), policy_hash=self.policy.digest,
                    shadow_only=True, early_id=self.cohort['early_id'],
                    symbol=self.cohort['symbol'], episode_id=self.cohort.get('episode_id'),
                    decision_ms=self.cohort['decision_ms'], horizon_ms=self.horizon,
                    reference=self.entry, flags=sorted(self.flags))
        if not self.entry:
            return dict(base, status='NO_BINANCE_FILL', eligible=False)
        number(observation_end_ms, 'observation_end_ms', True)
        complete = (coverage_complete is True and observation_end_ms >= self.deadline
                    and self.last is not None
                    and self.last_received <= observation_end_ms
                    and self.deadline - self.last['event_ms'] <= self.policy.freshness_ms
                    and not self.flags)
        exits = {k: dict(v) if v else None for k, v in self.exits.items()}
        # Horizon liquidation is an explicit mark proxy, never a claimed fill.
        if complete:
            for key in exits:
                if exits[key] is None:
                    exits[key] = dict(reason='HORIZON_MARK_PROXY', event_ms=self.deadline,
                                      observed_ms=self.last['received_ms'],
                                      observed_price=self.last['price'],
                                      execution_model='FRESH_LAST_TRADE_MARK_PROXY')
        costs = self.cohort.get('costs', {})
        outcomes = {}
        for strategy, event in exits.items():
            item = dict(event) if event else dict(reason='OPEN_CENSORED')
            item.update(gross_pct=None, net_pct=None, costs_complete=False,
                        runner_capture=None, early_exit=None)
            if event:
                gross = 100 * (event['observed_price'] / self.entry['price'] - 1)
                item['gross_pct'] = gross
                # Each strategy pays funding only up to its own exit time.
                required = ('entry_fee_pct', 'exit_fee_pct', 'exit_slippage_pct', 'funding')
                known = all(costs.get(k) is not None for k in required)
                if known:
                    for key in required[:-1]:
                        if number(costs[key], key) < 0:
                            raise ValueError('negative fee or slippage')
                    if costs['exit_slippage_pct'] >= 100:
                        raise ValueError('invalid slippage')
                    funding = 0.
                    funding_ids = set()
                    for charge in costs['funding']:
                        ts = number(charge['event_ms'], 'funding time', True)
                        amount = number(charge['pct'], 'funding pct')
                        if ts in funding_ids:
                            raise ValueError('duplicate funding timestamp')
                        funding_ids.add(ts)
                        if self.entry['start_ms'] < ts <= event['event_ms']:
                            funding += amount
                    ratio = event['observed_price'] / self.entry['price'] * (1 - costs['exit_slippage_pct']/100)
                    item.update(costs_complete=True, net_pct=100*(ratio-1)
                                - costs['entry_fee_pct'] - costs['exit_fee_pct']*ratio - funding,
                                funding_pct=funding, exit_price_proxy=ratio*self.entry['price'])
                if complete and self.mfe + EPS >= self.policy.runner_pct:
                    item['runner_capture'] = gross / self.mfe
                    later_peak = self.post_exit_peak if strategy == 'dynamic' else self.mfe
                    item['early_exit'] = (event['reason'] != 'HORIZON_MARK_PROXY'
                                          and later_peak is not None
                                          and later_peak - gross + EPS >= self.policy.early_exit_extra_pct)
            outcomes[strategy] = item
        # Same-tick crossed positive barriers are tied, never arbitrarily ordered.
        touch_groups = {}
        for level, touch in self.touches.items():
            key = (touch['event_ms'], touch['trade_id'])
            touch_groups.setdefault(key, []).append(level)
        order = [dict(event_ms=k[0], trade_id=k[1], levels=sorted(v))
                 for k, v in sorted(touch_groups.items())]
        eligible = complete and all(x['costs_complete'] for x in outcomes.values())
        return dict(base, status='COMPLETE' if complete else 'INCOMPLETE_PATH',
                    eligible=eligible, path_complete=complete, deadline_ms=self.deadline,
                    mfe_pct=self.mfe if self.last else None,
                    mae_pct=self.mae if self.last else None,
                    peak_to_trough_drawdown_proxy_pp=self.drawdown if self.last else None,
                    stop_price=self.stop, initial_stop_price=self.initial_stop,
                    initial_stop_source=self.cohort.get('initial_stop_source', 'SUPPLIED_EXISTING_SL'),
                    confirmed_peak_pct=self.confirmed_peak, stop_updates=self.stop_updates,
                    first_touch_order=order, first_touches={str(k):v for k,v in self.touches.items()},
                    outcomes=outcomes, cost_assumptions=costs,
                    metric_kind='UNLEVERED_COUNTERFACTUAL_NOT_REALIZED_PNL')


def summarize(records):
    """Paired full-horizon expectancy; never mix VWAP and signal-price cohorts."""
    groups = {}
    for row in records:
        kind = (row.get('reference') or {}).get('kind', 'NO_BINANCE_FILL')
        key = f"{kind}:{row['horizon_ms']}:{row['policy_hash']}"
        group = groups.setdefault(key, dict(total=0, eligible=0, excluded=0, rows=[]))
        group['total'] += 1
        if row['eligible']:
            group['eligible'] += 1
            group['rows'].append(row)
        else:
            group['excluded'] += 1
    for group in groups.values():
        rows = sorted(group.pop('rows'), key=lambda r:(r['decision_ms'], str(r['early_id'])))
        group['coverage'] = group['eligible']/group['total']
        for strategy in ('fixed', 'dynamic'):
            values = [r['outcomes'][strategy] for r in rows]
            net = [x['net_pct'] for x in values]
            capture = [x['runner_capture'] for x in values if x['runner_capture'] is not None]
            exits = [x['early_exit'] for x in values if x['early_exit'] is not None]
            equity = peak = dd = 0.
            for val in net:
                equity += val; peak = max(peak, equity); dd = max(dd, peak-equity)
            group[strategy] = dict(net_expectancy_pct=sum(net)/len(net) if net else None,
                                   sequential_equal_notional_drawdown_proxy_pp=dd if net else None,
                                   mean_runner_capture=sum(capture)/len(capture) if capture else None,
                                   runner_n=len(capture),
                                   early_exit_rate=sum(exits)/len(exits) if exits else None)
        group['paired_net_delta_pct'] = (group['dynamic']['net_expectancy_pct']
                                        - group['fixed']['net_expectancy_pct']) if rows else None
    return dict(version=VERSION, shadow_only=True, groups=groups,
                warning='Conditional on complete paired paths and explicit costs; not portfolio P/L. '
                        'Overlapping episodes are not independent; no promotion decision.')
