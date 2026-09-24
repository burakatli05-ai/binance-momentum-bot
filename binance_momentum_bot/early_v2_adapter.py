"""Binance/Telegram adapter for the isolated Early pilot. Imported by bot only.

No account-wide cancel/close calls. One-way, USDT, isolated-margin pilot only.
"""
import asyncio
import json
import math
import logging
import os
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone

from early_autotrader_v2 import Pilot
from execution_v2 import Blocked, Uncertain, positive, quantize
import step_lock_shadow


def flag(name):
    return os.getenv(name, '0').strip().lower() in ('1','true','yes','on')


class Binance:
    def __init__(self, bot, session, pilot):
        self.b = bot
        self.session = session
        self.pilot = pilot

    async def request(self, method, path, params):
        return await self.b['binance_signed_request'](self.session, method, path, params)

    async def filters(self, symbol):
        # LIMIT uses LOT_SIZE, never the legacy Premium MARKET_LOT_SIZE cache.
        info = await self.b['fetch_json'](self.session, '/fapi/v1/exchangeInfo')
        item = next((s for s in info['symbols'] if s['symbol'] == symbol), None)
        if not item or item['status'] != 'TRADING' or item.get('marginAsset') != 'USDT':
            raise Blocked('UNSUPPORTED_CONTRACT')
        fs = {f['filterType']: f for f in item['filters']}
        return dict(tick=positive(fs['PRICE_FILTER']['tickSize']), step=positive(fs['LOT_SIZE']['stepSize']),
                    min_qty=positive(fs['LOT_SIZE']['minQty']), max_qty=positive(fs['LOT_SIZE']['maxQty']),
                    min_notional=positive(fs['MIN_NOTIONAL']['notional']))

    async def ask(self, symbol):
        st = self.b['states'][symbol]
        now = self.pilot.clock()
        if (not 0 <= now - st.last_book_receive_ms <= 1500
                or not 0 <= now - st.last_book_event_ms <= 1500):
            raise Blocked('STALE_BOOK')
        return positive(st.ask_price)

    async def preflight(self, tr):
        cfg, usdt, positions = await self.b['_at_account_snapshot'](self.session)
        if cfg.get('dualSidePosition') or not cfg.get('canTrade'):
            raise Blocked('ONE_WAY_CAN_TRADE_REQUIRED')
        if float(usdt.get('availableBalance', 0)) < tr['config']['margin'] * 1.1:
            raise Blocked('INSUFFICIENT_AVAILABLE_MARGIN')
        symbol = tr['symbol']
        if any(p.get('symbol') == symbol and abs(float(p.get('positionAmt', 0))) > 1e-12 for p in positions):
            raise Blocked('EXISTING_EXCHANGE_POSITION')
        normal = await self.request('GET', '/fapi/v1/openOrders', {'symbol': symbol})
        algos = await self.request('GET', '/fapi/v1/openAlgoOrders', {'symbol': symbol})
        if normal or algos:
            raise Blocked('EXISTING_EXCHANGE_ORDERS')
        self.pilot.still_entry_allowed(tr)
        await self.request('POST', '/fapi/v1/leverage', dict(symbol=symbol, leverage=tr['config']['leverage']))
        self.pilot.still_entry_allowed(tr)
        try:
            await self.request('POST', '/fapi/v1/marginType', dict(symbol=symbol, marginType='ISOLATED'))
        except Exception as exc:
            if '-4046' not in str(exc):
                raise

    async def fills(self, symbol, order_id):
        result = await self.request('GET', '/fapi/v1/userTrades', dict(symbol=symbol, orderId=order_id, limit=1000))
        if not isinstance(result, list) or len(result) >= 1000:
            raise Uncertain('FILL_PAGE_TRUNCATED_OR_INVALID')
        return result

    async def protect(self, tr, stop_price, save):
        pending = tr.get('pending_stop')
        current = next((s for s in reversed(tr['stops']) if s.get('active')), None)
        if not pending and current and float(current['price']) >= stop_price:
            return
        if not pending:
            pending = dict(client='ev2s-' + tr['id'] + '-' + str(len(tr['stops'])), price=stop_price)
            tr['pending_stop'] = pending
            save(tr)
            try:
                response = await self.request('POST', '/fapi/v1/algoOrder', dict(
                    algoType='CONDITIONAL', symbol=tr['symbol'], side='SELL', type='STOP_MARKET',
                    quantity=str(tr['qty']), triggerPrice=str(stop_price), reduceOnly='true',
                    positionSide='BOTH', workingType='CONTRACT_PRICE', clientAlgoId=pending['client']))
            except Exception:
                response = await self.request('GET', '/fapi/v1/algoOrder', {'clientAlgoId': pending['client']})
        else:
            response = await self.request('GET', '/fapi/v1/algoOrder', {'clientAlgoId': pending['client']})
        if (not response.get('algoId') or response.get('clientAlgoId') != pending['client']
                or response.get('algoStatus') not in ('NEW','WORKING')):
            raise Uncertain('PROTECTIVE_STOP_NOT_ACKNOWLEDGED')
        tr['stops'].append(dict(id=response['algoId'], client=pending['client'], price=pending['price'], active=True))
        tr['pending_stop'] = None
        tr['stop'] = max(tr.get('stop', 0), pending['price'])
        save(tr)
        # Acknowledge new reduce-only protection before canceling the old one.
        for old in tr['stops'][:-1]:
            if old.get('active'):
                await self.cancel_stop(old)
                old['active'] = False
                save(tr)

    async def resolve_pending_tp(self, tr, save, response=None):
        pending = tr.get('pending_tp')
        if not pending:
            return
        if response is None:
            response = await self.request('GET', '/fapi/v1/algoOrder', {'clientAlgoId': pending['client']})
        if (not response.get('algoId') or response.get('clientAlgoId') != pending['client']
                or response.get('symbol') != tr['symbol'] or response.get('side') != 'SELL'
                or response.get('orderType') != 'TAKE_PROFIT_MARKET'
                or str(response.get('reduceOnly')).lower() != 'true'
                or response.get('positionSide') != 'BOTH'
                or response.get('workingType') != 'CONTRACT_PRICE'
                or not math.isclose(float(response.get('quantity', 0)), tr['qty'], rel_tol=1e-9)
                or not math.isclose(float(response.get('triggerPrice', 0)), pending['price'], rel_tol=1e-9)):
            raise Uncertain('FALLBACK_TP_IDENTITY_OR_QUANTITY_MISMATCH')
        tr.setdefault('take_profits', []).append(dict(id=response['algoId'], client=pending['client'],
            price=pending['price'], active=True))
        tr['pending_tp'] = None
        save(tr)
        return response

    async def ensure_fallback_tp(self, tr, save):
        if tr.get('pending_tp'):
            await self.resolve_pending_tp(tr, save)
        orders = tr.setdefault('take_profits', [])
        for tp in orders:
            if not tp.get('active'):
                continue
            state = await self.request('GET', '/fapi/v1/algoOrder', {'algoId': tp['id']})
            if state.get('algoStatus') in ('NEW', 'WORKING'):
                return
            if state.get('algoStatus') in ('CANCELED', 'EXPIRED', 'REJECTED') and str(state.get('actualOrderId', '0')) in ('', '0', 'None'):
                tp['active'] = False
                save(tr)
            else:
                raise Uncertain('FALLBACK_TP_TRIGGERING_RECONCILE_REQUIRED')
        price = quantize(positive(tr['vwap']) * (1 + tr['config'].get('fallback_tp_pct', 1.) / 100),
                         tr['filters']['tick'], up=True)
        if price <= tr['vwap'] or price <= tr['stop']:
            raise Uncertain('FALLBACK_TP_ALREADY_BEHIND_STOP')
        tr['fallback_tp_price'] = price
        pending = dict(client='ev2t-' + tr['id'] + '-' + str(len(orders)), price=price)
        tr['pending_tp'] = pending
        save(tr)  # persisted before POST; unknown response never causes a duplicate
        try:
            response = await self.request('POST', '/fapi/v1/algoOrder', dict(
                algoType='CONDITIONAL', symbol=tr['symbol'], side='SELL', type='TAKE_PROFIT_MARKET',
                quantity=str(tr['qty']), triggerPrice=str(price), reduceOnly='true',
                positionSide='BOTH', workingType='CONTRACT_PRICE', clientAlgoId=pending['client']))
        except Exception:
            response = await self.request('GET', '/fapi/v1/algoOrder', {'clientAlgoId': pending['client']})
        state = await self.resolve_pending_tp(tr, save, response)
        if state.get('algoStatus') not in ('NEW', 'WORKING'):
            raise Uncertain('FALLBACK_TP_NOT_ACKNOWLEDGED')

    async def cancel_fallback_tp(self, tr, save):
        if tr.get('pending_tp'):
            await self.resolve_pending_tp(tr, save)
        for tp in tr.get('take_profits', []):
            if not tp.get('active'):
                continue
            # A DELETE ACK alone is insufficient: a triggering TP can race it.
            try:
                await self.request('DELETE', '/fapi/v1/algoOrder', {'algoId': tp['id']})
            except Exception:
                pass
            state = await self.request('GET', '/fapi/v1/algoOrder', {'algoId': tp['id']})
            if (state.get('algoStatus') not in ('CANCELED', 'EXPIRED', 'REJECTED')
                    or str(state.get('actualOrderId', '0')) not in ('', '0', 'None')):
                raise Uncertain('FALLBACK_TP_CANCEL_UNCONFIRMED')
            tp['active'] = False
            save(tr)

    async def emergency(self, tr, save):
        # Protective failure only, never an entry fallback. Persist ID before POST.
        cfg, _, positions = await self.b['_at_account_snapshot'](self.session)
        qty = sum(float(p['positionAmt']) for p in positions if p.get('symbol') == tr['symbol'])
        if cfg.get('dualSidePosition') or not math.isclose(qty, tr['qty'], rel_tol=1e-9, abs_tol=1e-12):
            raise Uncertain('EMERGENCY_OWNERSHIP_UNCERTAIN')
        if tr.get('emergency_client'):
            return
        tr['emergency_client'] = 'ev2x-' + tr['id']
        save(tr)
        try:
            await self.request('POST', '/fapi/v1/order', dict(symbol=tr['symbol'], side='SELL', type='MARKET',
                quantity=str(tr['qty']), reduceOnly='true', newClientOrderId=tr['emergency_client']))
        except Exception:
            # No second close after uncertainty. Recovery queries the durable ID.
            pass

    async def cancel_stop(self, stop):
        try:
            await self.request('DELETE', '/fapi/v1/algoOrder', {'algoId': stop['id']})
        except Exception:
            state = await self.request('GET', '/fapi/v1/algoOrder', {'algoId': stop['id']})
            if state.get('algoStatus') not in ('CANCELED','FINISHED','EXPIRED','REJECTED'):
                raise Uncertain('STOP_CANCEL_UNKNOWN')

    async def reconcile_position(self, tr, save):
        cfg, _, positions = await self.b['_at_account_snapshot'](self.session)
        if cfg.get('dualSidePosition'):
            raise Uncertain('ACCOUNT_MODE_CHANGED')
        qty = sum(float(p['positionAmt']) for p in positions if p.get('symbol') == tr['symbol'])
        if qty and not math.isclose(qty, tr['qty'], rel_tol=1e-9, abs_tol=1e-12):
            raise Uncertain('POSITION_OWNERSHIP_OR_QUANTITY_CHANGED')
        if qty:
            if tr.get('pending_stop'):
                await self.protect(tr, tr['pending_stop']['price'], save)
            current = next((s for s in reversed(tr['stops']) if s.get('active')), None)
            if not current:
                await self.protect(tr, tr['stop'], save)
                return
            state = await self.request('GET', '/fapi/v1/algoOrder', {'algoId': current['id']})
            if state.get('algoStatus') in ('CANCELED','EXPIRED','REJECTED'):
                current['active'] = False
                save(tr)
                try:
                    await self.protect(tr, tr['stop'], save)
                except Exception:
                    await self.emergency(tr, save)
                    raise
            elif state.get('algoStatus') not in ('NEW','WORKING'):
                raise Uncertain('STOP_TRIGGERING_OR_MISSING_RECONCILE_REQUIRED')
            return
        # Position is flat: all bot-owned stops must be canceled before releasing
        # symbol ownership. Determine exits from their exact actualOrderId values.
        exit_fills = {}
        if tr.get('pending_stop'):
            pending = tr['pending_stop']
            try:
                state = await self.request('GET', '/fapi/v1/algoOrder', {'clientAlgoId': pending['client']})
            except Exception as exc:
                # Unknown stops retain symbol ownership; manual audit may be needed.
                raise Uncertain('PENDING_STOP_RECONCILIATION_REQUIRED') from exc
            tr['stops'].append(dict(id=state['algoId'],client=pending['client'],price=pending['price'],active=True))
            tr['pending_stop'] = None
            save(tr)
        if tr.get('emergency_client'):
            order = await self.request('GET', '/fapi/v1/order', dict(symbol=tr['symbol'],origClientOrderId=tr['emergency_client']))
            if order.get('status') != 'FILLED' or order.get('side') != 'SELL':
                raise Uncertain('EMERGENCY_CLOSE_UNRECONCILED')
            for f in await self.fills(tr['symbol'], order['orderId']):
                if f.get('side') != 'SELL' or str(f.get('orderId')) != str(order['orderId']) or f.get('symbol') != tr['symbol']:
                    raise Uncertain('EMERGENCY_FILL_IDENTITY_MISMATCH')
                exit_fills[str(f['id'])] = f
        if tr.get('pending_tp'):
            await self.resolve_pending_tp(tr, save)
        for stop in tr['stops'] + tr.get('take_profits', []):
            state = await self.request('GET', '/fapi/v1/algoOrder', {'algoId': stop['id']})
            order_id = state.get('actualOrderId')
            if order_id and str(order_id) != '0':
                for f in await self.fills(tr['symbol'], order_id):
                    if f.get('side') != 'SELL' or str(f.get('orderId')) != str(order_id) or f.get('symbol') != tr['symbol']:
                        raise Uncertain('EXIT_FILL_IDENTITY_MISMATCH')
                    key = str(f['id'])
                    if key in exit_fills and exit_fills[key] != f:
                        raise Uncertain('EXIT_FILL_CONFLICT')
                    exit_fills[key] = f
            if stop.get('active'):
                await self.cancel_stop(stop)
                stop['active'] = False
                save(tr)
        total = sum(positive(f['qty']) for f in exit_fills.values())
        if not math.isclose(total, tr['qty'], rel_tol=1e-9, abs_tol=1e-12):
            raise Uncertain('EXIT_FILL_RECONCILIATION_REQUIRED')
        all_fills = tr['fills'] + list(exit_fills.values())
        if any(f.get('commissionAsset') != 'USDT' for f in all_fills):
            raise Uncertain('NON_USDT_COMMISSION_REQUIRES_RECONCILIATION')
        end = max(int(f['time']) for f in exit_fills.values())
        funding = await self.request('GET', '/fapi/v1/income', dict(symbol=tr['symbol'], incomeType='FUNDING_FEE',
            startTime=tr['created_ms'], endTime=end, limit=1000))
        if not isinstance(funding, list) or len(funding) >= 1000 or any(f.get('asset') != 'USDT' for f in funding):
            raise Uncertain('FUNDING_RECONCILIATION_REQUIRED')
        commissions = sum(float(f['commission']) for f in all_fills)
        gross = sum(float(f['realizedPnl']) for f in exit_fills.values())
        funded = sum(float(f['income']) for f in funding)
        if not all(math.isfinite(x) for x in (commissions, gross, funded)):
            raise Uncertain('NONFINITE_COSTS')
        tr.update(status='CLOSED', closed_ms=end, net=gross-commissions+funded,
                  gross=gross, commissions=commissions, funding=funded, exit_fills=list(exit_fills.values()),
                  exit_price=sum(float(f['price'])*float(f['qty']) for f in exit_fills.values())/total)
        save(tr)


class Integration:
    def __init__(self, bot):
        self.b = bot
        self.pilot = Pilot(bot['db_connect'], live_allowed=flag('EARLY_V2_LIVE_ALLOWED'),
            score_validated=flag('EARLY_V2_SCORE_VALIDATED'), profit_live_allowed=flag('EARLY_V2_PROFIT_LIVE_ALLOWED'))
        step_enabled = os.getenv('STEP_LOCK_SHADOW_ENABLED', '1').strip().lower() not in ('0','false','no','off')
        self.step_lock = step_lock_shadow.StepLockShadow(bot['db_connect']) if step_enabled else None
        for name in ('min_score','margin','leverage','daily_loss','daily_trades','retry','max_positions','fallback_tp_pct'):
            value = os.getenv('EARLY_V2_' + name.upper())
            if value is not None:
                self.pilot.configure(name, json.loads(value))
        self.queue = asyncio.Queue(maxsize=20)
        self._last_step_report_ms = 0
        if flag('EARLY_V2_AUTO_DRY'):
            try:
                self.pilot.set_mode('DRY')
                logging.getLogger(__name__).info('EarlyV2 auto-dry enabled')
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    'EarlyV2 auto-dry failed: %s:%s', type(exc).__name__, exc
                )
        logging.getLogger(__name__).info(
            'EarlyV2 startup: mode=%s profit_mode=%s live_allowed=%s profit_live_allowed=%s fallback_tp_pct=%s Premium=%s',
            self.pilot.mode, self.pilot.profit_mode, int(self.pilot.live_allowed),
            int(self.pilot.profit_live_allowed), self.pilot.cfg.fallback_tp_pct,
            bot.get('autotrade_cfg', {}).get('mode', 'UNKNOWN'))
        logging.getLogger(__name__).info('Step Lock V1 shadow: %s | SL -2%% | final TP +5%% | no partial exits',
                                         'ON' if self.step_lock else 'OFF')
        if self.step_lock:
            self._log_step_lock_report()
            try:
                self._write_daily_candidate_profit_review()
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "EARLY_DAILY_CANDIDATE_REVIEW_FAILED %s", type(exc).__name__
                )
            threading.Thread(
                target=self._write_historical_profit_review,
                name="early-historical-profit-review",
                daemon=True,
            ).start()
            threading.Thread(
                target=self._write_combined_optimizer_report,
                name="early-combined-optimizer",
                daemon=True,
            ).start()

    def arm(self, radar_id, symbol, m, base_score):
        decision_ms = self.pilot.clock()
        if self.step_lock:
            try:
                state = self.b.get('states', {}).get(symbol)
                episode_id = getattr(state, 'episode_id', None) if state is not None else None
                self.step_lock.arm(radar_id, symbol, float(m['price']), decision_ms, episode_id)
            except Exception as exc:
                self.pilot.event('STEP_LOCK_ARM_FAILED', {'signal_id': str(radar_id), 'symbol': symbol,
                                                          'reason': type(exc).__name__})
        try:
            score, label, reasons = self.b['ignition_shadow_score'](m, base_score)
            selector_features = {
                key: m.get(key) for key in (
                    'chg10','chg30','chg60','flow10','flow30','flow60','buy30',
                    'rel30','flow_eff30','dist15high_pct','spread','qv24',
                    'oi5','oi_accel5','compression_ratio','extended'
                )
            }
            qualified = self.pilot.record_selector(
                radar_id, symbol, decision_ms, float(m['price']), base_score,
                score, label, reasons, selector_features
            )
            self.pilot.event('SELECTOR_DECISION', {
                'signal_id': str(radar_id), 'symbol': symbol, 'score': score,
                'label': label, 'qualified': bool(qualified), 'min_score': self.pilot.cfg.min_score
            })
            if self.pilot.mode == 'OFF' or not qualified:
                return
            plan = self.b['estimate_trade_plan'](symbol, m)
            self.queue.put_nowait(dict(
                id=radar_id, symbol=symbol, price=float(m['price']),
                v2_score=score, v2_label=label, ts_ms=decision_ms,
                entry_high=plan['entry_high'], stop=plan['invalidation'], target=plan['target1']
            ))
        except asyncio.QueueFull:
            self.pilot.event('CANDIDATE_REJECTED', {
                'signal_id': str(radar_id), 'symbol': symbol, 'reason': 'QUEUE_FULL'
            })
        except Exception as exc:
            self.pilot.event('CANDIDATE_REJECTED', {
                'signal_id': str(radar_id), 'symbol': symbol,
                'reason': type(exc).__name__ + ':' + str(exc)
            })

    async def premium(self, session, signal_id, symbol, m, plan):
        async with self.pilot.lock:
            if self.pilot.premium_confirm(symbol, signal_id, self.b['autotrade_cfg']['mode']):
                return
            return await self.b['autotrade_handle_premium'](session, signal_id, symbol, m, plan)

    def tick(self, symbol, price, event_ms, received_ms, trade_id):
        if self.step_lock:
            try:
                self.step_lock.tick(symbol, float(price), int(event_ms), int(received_ms), int(trade_id))
            except Exception as exc:
                self.pilot.event('STEP_LOCK_TICK_FAILED', {'symbol': symbol, 'reason': type(exc).__name__})
        try:
            self.pilot.tick(symbol, price, event_ms, received_ms, trade_id)
        except Exception as exc:
            self.pilot.kill('TICK:' + type(exc).__name__)

    def _log_step_lock_report(self):
        if not self.step_lock:
            return
        now = self.pilot.clock()
        if self._last_step_report_ms and now - self._last_step_report_ms < 300000:
            return
        self._last_step_report_ms = now
        try:
            report = self.step_lock.summary(notional_usdt=2000.0, recent_limit=5)
            payload = {
                "version": report["version"],
                "total": report["total"],
                "open": report["open"],
                "closed": report["closed"],
                "close_reasons": report["close_reason_counts"],
                "exit_levels": report["exit_level_counts"],
                "open_locks": report["open_lock_counts"],
                "reached": {str(level): report["reached_level_counts"].get(level, 0)
                            for level in (0.2,0.5,0.75,1.0,1.5,2.0,3.0,4.0,5.0)},
                "closed_net_usdt_200x10": round(float(report["closed_net_usdt"]), 6),
                "closed_avg_usdt_200x10": (None if report["closed_net_usdt_avg"] is None
                                            else round(float(report["closed_net_usdt_avg"]), 6)),
                "flagged_signals": report["flagged_signals"],
                "flag_counts": report["flag_counts"],
                "latest_ms": report["latest_ms"],
                "recent": [
                    {
                        "id": row["signal_id"], "symbol": row["symbol"], "decision_ms": row["decision_ms"],
                        "peak": round(row["peak_pct"], 5), "trough": round(row["trough_pct"], 5),
                        "status": row["status"], "lock": row["current_lock_pct"],
                        "reason": row["close_reason"], "exit_level": row["exit_level_pct"],
                        "net_pct": row["net_pct"], "flags": row["data_flags"],
                    }
                    for row in report["recent"]
                ],
            }
            logging.getLogger(__name__).info(
                "STEP_LOCK_REPORT %s", json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False)
            )
            runner = self.step_lock.runner_review(exit_level_pct=0.20, recent_limit=200)
            runner_payload = {
                "exit_level_pct": runner["exit_level_pct"],
                "total": runner["total"],
                "matched_stage": runner["matched_stage"],
                "mature_60m": runner["mature_60m"],
                "clean_signals": runner["clean_signals"],
                "reached_after_exit_proxy": {str(k): v for k,v in runner["reached_after_exit_proxy"].items()},
                "clean_reached_after_exit_proxy": {str(k): v for k,v in runner["clean_reached_after_exit_proxy"].items()},
                "items": [
                    {
                        "id": row["signal_id"], "symbol": row["symbol"], "decision_ms": row["decision_ms"],
                        "exit_event_ms": row["exit_event_ms"], "exit_net_pct": row["net_pct"],
                        "step_peak_pct": round(row["step_peak_pct"],5),
                        "stage_mfe_pct": (None if row["stage_mfe_pct"] is None else round(row["stage_mfe_pct"],5)),
                        "stage_mae_pct": (None if row["stage_mae_pct"] is None else round(row["stage_mae_pct"],5)),
                        "stage_tp1_return_pct": row.get("stage_tp1_return_pct"),
                        "stage_tp1_hit_s": row.get("stage_tp1_hit_s"),
                        "stage_tp1_after_exit_s": row.get("stage_tp1_after_exit_s"),
                        "p0_half_reach_upper_bound": row.get("p0_half_reach_upper_bound"),
                        "p0_timing": row.get("p0_timing"),
                        "mature_60m": row["completed_60m"], "flags": row["data_flags"],
                    }
                    for row in runner["items"]
                ],
            }
            logging.getLogger(__name__).info(
                "STEP_LOCK_RUNNER_REVIEW %s",
                json.dumps(runner_payload, sort_keys=True, ensure_ascii=False, allow_nan=False)
            )
            report_path = os.getenv("STEP_LOCK_REPORT_PATH", "/data/step_lock_latest.json").strip()
            if report_path:
                try:
                    target = Path(report_path)
                    tmp = target.with_name(target.name + ".tmp")
                    initial_sl = self.step_lock.initial_sl_recovery_review(limit=20)
                    payload_file = {
                        "generated_ms": now,
                        "step_lock": payload,
                        "runner_review": runner_payload,
                        "initial_sl_recovery": initial_sl,
                    }
                    tmp.write_text(
                        json.dumps(payload_file, sort_keys=True, ensure_ascii=False, allow_nan=False),
                        encoding="utf-8",
                    )
                    os.replace(tmp, target)
                except Exception as file_exc:
                    logging.getLogger(__name__).warning(
                        "STEP_LOCK_REPORT_FILE_FAILED %s", type(file_exc).__name__
                    )
        except Exception as exc:
            logging.getLogger(__name__).warning("STEP_LOCK_REPORT_FAILED %s", type(exc).__name__)

    def _write_historical_profit_review(self):
        if not self.step_lock:
            return None
        report = self.step_lock.historical_profit_review(notional_usdt=2000.0)
        report["runner_filter_price_action"] = self.step_lock.runner_filter_price_action_review()
        path = os.getenv("EARLY_HISTORICAL_REVIEW_PATH", "/data/early_historical_profit_review.json").strip()
        if path:
            target = Path(path)
            tmp = target.with_name(target.name + ".tmp")
            tmp.write_text(
                json.dumps(report, sort_keys=True, ensure_ascii=False, allow_nan=False),
                encoding="utf-8",
            )
            os.replace(tmp, target)
        logging.getLogger(__name__).info(
            "EARLY_HISTORICAL_PROFIT_REVIEW rows=%s quality_paths=%s",
            report["full_history"]["completed_60m"],
            report["quality_exactish"]["price_path_count"],
        )
        return report

    def _write_combined_optimizer_report(self):
        if not self.step_lock:
            return None
        report = self.step_lock.combined_profit_loss_optimizer(notional_usdt=2000.0)
        path = os.getenv(
            "EARLY_COMBINED_OPTIMIZER_PATH",
            "/data/combined_profit_loss_optimizer.json"
        ).strip()
        if path:
            target = Path(path)
            tmp = target.with_name(target.name + ".tmp")
            tmp.write_text(
                json.dumps(report, sort_keys=True, ensure_ascii=False, allow_nan=False),
                encoding="utf-8",
            )
            os.replace(tmp, target)
        logging.getLogger(__name__).info(
            "EARLY_COMBINED_OPTIMIZER train=%s test=%s delta_usdt=%s",
            report["cohort"]["train"],
            report["cohort"]["test"],
            report["profitability"]["test_delta_vs_baseline_usdt"],
        )
        return report

    def _write_daily_candidate_profit_review(self):
        if not self.step_lock:
            return None
        trt = timezone(timedelta(hours=3))
        now = datetime.now(trt)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        report = self.step_lock.daily_candidate_profit_review(
            start_ms=int(start.timestamp() * 1000),
            end_ms=int(now.timestamp() * 1000),
            notional_usdt=2000.0,
        )
        report["local_date"] = start.date().isoformat()
        report["generated_local"] = now.isoformat()
        path = os.getenv(
            "EARLY_DAILY_CANDIDATE_REVIEW_PATH",
            "/data/daily_candidate_profit_review.json",
        ).strip()
        if path:
            target = Path(path)
            tmp = target.with_name(target.name + ".tmp")
            tmp.write_text(
                json.dumps(report, sort_keys=True, ensure_ascii=False, allow_nan=False),
                encoding="utf-8",
            )
            os.replace(tmp, target)
        logging.getLogger(__name__).info(
            "EARLY_DAILY_CANDIDATE_REVIEW date=%s eligible=%s candidate_usdt=%s baseline_usdt=%s",
            report["local_date"], report["eligible"],
            report["all_to_now"]["candidate"]["net_usdt"],
            report["all_to_now"]["baseline"]["net_usdt"],
        )
        return report

    async def run(self, session):
        exchange = Binance(self.b, session, self.pilot)
        if self.step_lock:
            try:
                await asyncio.to_thread(self._write_daily_candidate_profit_review)
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "EARLY_DAILY_CANDIDATE_REVIEW_FAILED %s", type(exc).__name__
                )
            try:
                await asyncio.to_thread(self._write_historical_profit_review)
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "EARLY_HISTORICAL_PROFIT_REVIEW_FAILED %s", type(exc).__name__
                )
        while not self.b['stop_event'].is_set():
            self._log_step_lock_report()
            try:
                await self.pilot.reconcile(exchange)
                try:
                    candidate = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    candidate = None
                if candidate:
                    await self.pilot.enter(candidate, exchange,
                        premium_busy=lambda: bool(self.b['autotrade_active_by_symbol'].get(candidate['symbol'])))
            except Exception as exc:
                self.pilot.kill('WORKER:' + type(exc).__name__)
            await asyncio.sleep(1)

    @staticmethod
    def _step_level(value):
        if value is None:
            return "yok"
        sign = "+" if float(value) >= 0 else ""
        return f"{sign}%{float(value):g}"

    def step_lock_report_text(self, report, *, include_recent=False):
        closed = int(report["closed"])
        net_usdt = float(report["closed_net_usdt"])
        avg_usdt = report["closed_net_usdt_avg"]
        avg_pct = report["closed_net_pct_avg"]
        latest_ms = int(report.get("latest_ms") or 0)
        latest = "veri yok"
        if latest_ms:
            latest = datetime.fromtimestamp(
                latest_ms / 1000, timezone(timedelta(hours=3))
            ).strftime("%d.%m %H:%M:%S TRT")
        exits = report.get("exit_level_counts") or {}
        exit_text = ", ".join(
            f"{self._step_level(level)}:{count}"
            for level,count in sorted(exits.items(), key=lambda item: float(item[0]))
            if count
        ) or "henüz kapanış yok"
        locks = report.get("open_lock_counts") or {}
        open_text = ", ".join(
            f"{self._step_level(level)}:{count}"
            for level,count in sorted(locks.items(), key=lambda item: -999 if item[0] is None else float(item[0]))
            if count
        ) or "açık işlem yok"
        reached = report.get("reached_level_counts") or {}
        selected = (0.20,0.50,0.75,1.00,1.50,2.00,3.00,4.00,5.00)
        reached_text = " | ".join(
            f"{self._step_level(level)} {int(reached.get(level, 0))}"
            for level in selected
        )
        reasons = report.get("close_reason_counts") or {}
        quality = report.get("flag_counts") or {}
        quality_text = ", ".join(
            f"{name}:{count}" for name,count in
            sorted(quality.items(), key=lambda item: (-item[1], item[0]))[:4]
        ) or "temiz"
        avg_line = "—" if avg_usdt is None else f"{avg_usdt:+.2f} USDT / {float(avg_pct):+.3f}%"
        lines = [
            "📊 STEP LOCK V1 — SHADOW",
            "",
            f"Toplam Early: {report['total']} | Kapalı: {closed} | Açık: {report['open']}",
            f"Kapanış: STEP {int(reasons.get('STEP_LOCK',0))} | -%2 SL {int(reasons.get('INITIAL_SL',0))} | +%5 TP {int(reasons.get('FINAL_TP',0))}",
            f"Net (yalnız kapananlar, 200×10): {net_usdt:+.2f} USDT",
            f"Ortalama kapanan: {avg_line}",
            "",
            "🎯 Ulaşılan seviye (peak):",
            reached_text,
            "",
            f"🚪 Çıkış seviyeleri: {exit_text}",
            f"🔒 Açıkların mevcut kilidi: {open_text}",
            "",
            f"🧪 Veri bayraklı sinyal: {report['flagged_signals']}/{report['total']} | {quality_text}",
            f"🕒 Son gözlem: {latest}",
            "ℹ️ Açık işlemler toplam P/L'ye dahil değildir; bu rapor yalnız read-only shadow verisidir.",
        ]
        if include_recent:
            recent = report.get("recent") or []
            lines.extend(["", "🧾 Son sinyaller:"])
            for row in recent[:10]:
                ts = datetime.fromtimestamp(
                    row["decision_ms"] / 1000, timezone(timedelta(hours=3))
                ).strftime("%H:%M:%S")
                if row["status"] == "CLOSED":
                    state = f"{row.get('close_reason') or 'CLOSED'} {self._step_level(row.get('exit_level_pct'))} net {float(row.get('net_pct') or 0):+.3f}%"
                else:
                    state = f"OPEN kilit {self._step_level(row.get('current_lock_pct'))}"
                lines.append(
                    f"{ts} {row['symbol']} | peak {row['peak_pct']:+.2f}% / dip {row['trough_pct']:+.2f}% | {state}"
                )
        return "\n".join(lines)

    def status(self):
        p = self.pilot
        selector = p.selector_report(recent_limit=0)
        return (f'⚡ Early AutoTrader V2: {p.mode} | {p.cfg.margin:g} USDT × {p.cfg.leverage}x | '
            f'skor ≥{p.cfg.min_score:g} | max {p.cfg.max_positions} | Profit Lock {p.profit_mode}\n'
            f'V2 doğrulama: {"ON" if p.score_validated else "YOK — yalnız DRY"} | '
            f'Kill: {"AKTİF" if p.halted else "kapalı"} | Fallback TP +%{p.cfg.fallback_tp_pct:g}\n'
            f'Selector shadow: {selector["qualified"]}/{selector["total"]} seçildi | '
            f'60dk olgun: {selector["qualified_stats"]["mature60"]}\n'
            f'Profit LIVE geçişi: {"bekliyor" if p.profit_live_pending else "yok"}\n')

    def markup(self):
        buttons = [('Mod','mode'),('Tutar','margin'),('Kaldıraç','leverage'),('Min V2 Score','min_score'),
            ('Max Pozisyon','max_positions'),('Risk Ayarları','risk'),('Execution V2','execution'),
            ('Profit Lock','profit'),('Pozisyonlar','positions'),('Günlük Rapor','report'),('Kill Switch','kill'),('Fallback TP','fallback_tp_pct')]
        return {'inline_keyboard': [[{'text':label, 'callback_data':'ev2:'+action} for label,action in buttons[i:i+2]]
                                    for i in range(0,len(buttons),2)]}

    async def show(self, session, chat, message=None, markup=None):
        await self.b['telegram_send'](session, message or self.status(), chat_id=chat,
            reply_markup=markup or self.markup())

    async def callback(self, session, cb):
        data = str(cb.get('data',''))
        if not data.startswith('ev2:'):
            return False
        chat = str(((cb.get('message') or {}).get('chat') or {}).get('id',''))
        user = str((cb.get('from') or {}).get('id',''))
        await self.b['telegram_api_call'](session, 'answerCallbackQuery', {'callback_query_id':cb.get('id')})
        if not self.b['_at_admin_allowed'](chat,user,require_user_id=True):
            return True
        await self.command(session, '/earlyv2 ' + data[4:].replace(':',' '), chat, user)
        return True

    async def command(self, session, raw, chat, user):
        if not raw.strip():
            return False
        root = raw.split(maxsplit=1)[0].lower()
        if root not in ('/earlyv2', '/steplock'):
            return False
        if not self.b['_at_admin_allowed'](chat,user,require_user_id=True):
            if root == '/steplock':
                await self.b['telegram_send'](session, 'Yetkili kullanıcı ID gerekli.', chat_id=chat)
            else:
                await self.show(session, chat, 'Yetkili kullanıcı ID gerekli.')
            return True
        if root == '/steplock':
            if not self.step_lock:
                await self.b['telegram_send'](session, 'Step Lock shadow kapalı.', chat_id=chat)
                return True
            parts = raw.split()[1:]
            include_recent = bool(parts and parts[0].lower() in ('recent','son','detay'))
            report = self.step_lock.summary(notional_usdt=2000.0, recent_limit=10 if include_recent else 0)
            await self.b['telegram_send'](
                session, self.step_lock_report_text(report, include_recent=include_recent), chat_id=chat
            )
            return True
        p = self.pilot
        parts = raw.split()[1:]
        cmd = parts[0].lower() if parts else 'menu'
        try:
            message = None
            if cmd == 'kill':
                p.kill()
                message = 'Early kapatıldı. Yeni giriş durdu; mevcut borsa stopları ve uzlaştırma devam ediyor.'
            elif cmd in ('off','dry'):
                p.set_mode(cmd.upper())
            elif cmd == 'mode':
                await self.show(session,chat,self.status(),{'inline_keyboard':[[
                    {'text': x, 'callback_data':'ev2:'+x.lower()} for x in ('OFF','DRY','LIVE')]]})
                return True
            elif cmd in ('live','profitlive'):
                kind = 'LIVE' if cmd == 'live' else 'PROFIT_LIVE'
                token = p.challenge(chat + ':' + user, kind)
                action = 'confirm' if kind == 'LIVE' else 'confirmprofit'
                message = f'{kind} ikinci onayı: 120 sn içinde /earlyv2 {action} {token}'
            elif cmd in ('confirm','confirmprofit') and len(parts) == 2:
                ready = bool(self.b['BINANCE_API_KEY'] and self.b['BINANCE_API_SECRET'])
                p.confirm(chat+':'+user, parts[1], ready=ready, kind='LIVE' if cmd=='confirm' else 'PROFIT_LIVE')
            elif cmd == 'set' and len(parts) == 3:
                p.configure(parts[1], json.loads(parts[2]))
            elif cmd in ('margin','leverage','min_score','max_positions','fallback_tp_pct'):
                choices = dict(margin=(5,10,25,50,100), leverage=(1,2,3,5), min_score=(85,90,95,98),
                    max_positions=(1,2,3), fallback_tp_pct=(.3,.5,.8,1,1.5,2,3))[cmd]
                message = f'{cmd}: {getattr(p.cfg,cmd)}\nDeğiştir: /earlyv2 set {cmd} SAYI\nAyar değişimi için önce OFF.'
                await self.show(session, chat, message, {'inline_keyboard': [
                    [{'text':str(value), 'callback_data':f'ev2:set:{cmd}:{value}'} for value in choices],
                    [{'text':'Geri', 'callback_data':'ev2:menu'}]]})
                return True
            elif cmd == 'risk':
                message = (f'Günlük en fazla {p.cfg.daily_trades} giriş denemesi; zarar rezervi {p.cfg.daily_loss:g} USDT; '
                    f'cooldown {p.cfg.cooldown_s} sn.\n/earlyv2 set daily_trades 3\n/earlyv2 set daily_loss 1\n'
                    '/earlyv2 set cooldown_s 900\nBütçe Premium’dan ayrı; sembol çakışması engellenir.')
            elif cmd == 'execution':
                message = (f'Execution V2: {p.cfg.execution}\nLIMIT IOC; partial: KEEP_PROTECT_NO_TOPUP; retry={p.cfg.retry}\n'
                    f'Slippage %{p.cfg.slippage_pct:g}; min RR {p.cfg.min_rr:g}\n'
                    '/earlyv2 set execution true\n/earlyv2 set slippage_pct 0.1\n/earlyv2 set min_rr 1\n'
                    '/earlyv2 set retry 0\nKapalıysa giriş engellenir.')
            elif cmd == 'profit':
                message = (f'Profit Lock: {p.profit_mode}; ilk kilit +%{p.cfg.first_lock_pct:g}\n'
                    '/earlyv2 profitshadow\n/earlyv2 profitoff\n/earlyv2 profitlive\n'
                    f'/earlyv2 set first_lock_pct 0.2\nFallback TP +%{p.cfg.fallback_tp_pct:g} (OFF/SHADOW).\n'
                    '/earlyv2 set fallback_tp_pct 1\nAyar yalnız yeni pozisyonlar için. LIVE ayrı ikinci onaylıdır.')
            elif cmd in ('profitshadow','profitoff'):
                p.profit_mode = 'SHADOW' if cmd=='profitshadow' else 'OFF'
                p.profit_live_pending = False
                p.pending.pop('PROFIT_LIVE',None)
            elif cmd == 'positions':
                message = '\n'.join(f"{t['symbol']} {t['mode']} {t['status']} {t['classification']} | qty={t['qty']} | VWAP={t.get('vwap','bekleniyor')} | SL={t.get('stop','bekleniyor')} | TP={t.get('fallback_tp_price','yok')} | {t.get('profit_policy','bekleniyor')}"
                                    for t in p.active.values()) or 'Early pozisyonu yok.'
            elif cmd == 'selector':
                sr = p.selector_report(recent_limit=10)
                q = sr['qualified_stats']; rj = sr['rejected_stats']
                def rate_text(stats, level):
                    value = stats['rates'].get(str(level))
                    return '-' if value is None else f'%{100*value:.1f}'
                lines = [
                    '🧪 EARLY V2 SELECTOR SHADOW',
                    f'Toplam: {sr["total"]} | seçilen: {sr["qualified"]} | reddedilen: {sr["rejected"]}',
                    f'Seçilen 60dk n={q["mature60"]}: +0.5 {rate_text(q,0.5)} | +1 {rate_text(q,1.0)} | +2 {rate_text(q,2.0)} | +3 {rate_text(q,3.0)} | +5 {rate_text(q,5.0)}',
                    f'Reddedilen 60dk n={rj["mature60"]}: +0.5 {rate_text(rj,0.5)} | +1 {rate_text(rj,1.0)} | +2 {rate_text(rj,2.0)} | +3 {rate_text(rj,3.0)} | +5 {rate_text(rj,5.0)}',
                ]
                if sr['recent']:
                    lines.append('')
                    lines.append('Son kararlar:')
                    for row in sr['recent'][:10]:
                        lines.append(
                            f'{row["symbol"]} score {row["v2_score"]} {row["v2_label"]} '
                            f'{"✅" if row["qualified"] else "❌"} | '
                            f'MFE {("-" if row["mfe_pct"] is None else f"{row["mfe_pct"]:+.2f}%")}'
                        )
                message = '\n'.join(lines)
            elif cmd == 'report':
                message = '\n'.join(json.dumps(p.report(mode), ensure_ascii=False) for mode in ('DRY','LIVE'))
            elif cmd not in ('menu','status'):
                message = 'Komut tanınmadı. /earlyv2 menüsünü kullanın. Selector raporu: /earlyv2 selector'
            await self.show(session, chat, message)
        except (Blocked, ValueError, TypeError) as exc:
            await self.show(session,chat,'İşlem uygulanmadı: '+str(exc))
        return True
