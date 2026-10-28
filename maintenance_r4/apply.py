"""Apply only the locally tested R4 changes to the pinned R3 source."""
from pathlib import Path
import hashlib
root=Path('binance_momentum_bot')
expected={'bot.py':'2e6ef44cdc5b5d9a81683df9a28aebb8bff4650e','early_v2_adapter.py':'4ab1ec5b1b104999510306fa7142fb079d68581d'}
for name,sha in expected.items():
 b=(root/name).read_bytes()
 assert hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()==sha, name
p=root/'bot.py';s=p.read_text()
def replace(old,new,count=1):
 global s
 assert s.count(old)==count,(old[:100],s.count(old),count)
 s=s.replace(old,new)
replace('from dotenv import load_dotenv','from notification_policy import early_signal_notifications_enabled\nfrom runtime_io import loop_health\nfrom dotenv import load_dotenv')
replace('    # Deadline includes semaphore wait, unlike the old per-request timeout.', '''    if notification_kind == "EARLY" and not early_signal_notifications_enabled():
        log.info("TG_SUPPRESSED kind=EARLY symbol=%s reason=USER_POLICY delivered=0", symbol)
        return False
    # Deadline includes semaphore wait, unlike the old per-request timeout.''')
replace('    """Durable notification admission only; no Telegram or exchange I/O."""','''    """Durable notification admission only; no Telegram or exchange I/O."""
    if notification_kind == "EARLY" and not early_signal_notifications_enabled():
        log.info("TG_SUPPRESSED kind=EARLY symbol=%s reason=USER_POLICY delivered=0", symbol)
        return Submission(0, "SUPPRESSED_BY_POLICY")''')
replace('''    primary_ok = await telegram_send(
        session, text, symbol=symbol, notification_kind=notification_kind,''','''    if notification_kind == "EARLY" and not early_signal_notifications_enabled():
        log.info("TG_SUPPRESSED kind=EARLY symbol=%s reason=USER_POLICY delivered=0", symbol)
        return False
    primary_ok = await telegram_send(
        session, text, symbol=symbol, notification_kind=notification_kind,''')
replace('early_v2.arm(st.active_radar_id, symbol, m, score)','await early_v2.arm_async(st.active_radar_id, symbol, m, score)')
replace('early_v2.tick(sym, price, ts, recv_ms, d.get("a"))','await early_v2.tick_async(sym, price, ts, recv_ms, d.get("a"))')
replace('        log.info("Bootstrap complete")','''        log.info("Bootstrap complete")
        log.info("NOTIFICATION_POLICY version=R4 early_signal_telegram=%s premium_telegram=UNCHANGED execution_telegram=UNCHANGED research_recording=UNCHANGED",
                 int(early_signal_notifications_enabled()))''')
lines=s.splitlines(True)
for i,line in enumerate(lines):
 if 'Erken radar: 2/3' in line:
  lines[i]='                f"EARLY sinyal Telegram: {\'ACIK\' if early_signal_notifications_enabled() else \'KAPALI\'} | radar kaydi ve sinyal esikleri degismedi\\n"\n'
 if 'Early + Premium + Continuation' in line:
  lines[i]=line.replace('Early + Premium + Continuation','Premium + Continuation | EARLY: {\'ACIK\' if early_signal_notifications_enabled() else \'KAPALI\'}')
s=''.join(lines)
replace('        await asyncio.gather(*tasks)','''        tasks.append(loop_health(stop_event))
        try:
            await asyncio.gather(*tasks)
        finally:
            await early_v2.close_io()''')
p.write_text(s)
p=root/'early_v2_adapter.py';s=p.read_text()
replace('import step_lock_shadow','import step_lock_shadow\nfrom runtime_io import SerialIO\nfrom notification_policy import early_signal_notifications_enabled')
replace('        self.queue = asyncio.Queue(maxsize=20)','        self._shadow_io = SerialIO("legacy-shadow-io")\n        self.queue = asyncio.Queue(maxsize=20)')
replace('''    def arm(self, radar_id, symbol, m, base_score):
        decision_ms = self.pilot.clock()
        if self.step_lock:''','''    async def arm_async(self, radar_id, symbol, m, base_score):
        decision_ms = self.pilot.clock()
        frozen = dict(m)
        state = self.b.get('states', {}).get(symbol)
        episode_id = getattr(state, 'episode_id', None) if state is not None else None
        if self.step_lock:
            try:
                await self._shadow_io.call(self.step_lock.arm, radar_id, symbol,
                                           float(frozen['price']), decision_ms, episode_id)
            except Exception as exc:
                self.pilot.event('STEP_LOCK_ARM_FAILED', {'signal_id': str(radar_id),
                    'symbol': symbol, 'reason': type(exc).__name__})
        return self.arm(radar_id, symbol, frozen, base_score,
                        _skip_step_shadow=True, _decision_ms=decision_ms)

    async def tick_async(self, symbol, price, event_ms, received_ms, trade_id):
        if self.step_lock:
            try:
                await self._shadow_io.call(self.step_lock.tick, symbol, float(price),
                                           int(event_ms), int(received_ms), int(trade_id))
            except Exception as exc:
                self.pilot.event('STEP_LOCK_TICK_FAILED', {'symbol': symbol,
                    'reason': type(exc).__name__})
        # Financial pilot state remains on its original event-loop thread.
        return self.tick(symbol, price, event_ms, received_ms, trade_id, _skip_step_shadow=True)

    async def close_io(self):
        await self._shadow_io.close()

    def arm(self, radar_id, symbol, m, base_score, *, _skip_step_shadow=False, _decision_ms=None):
        decision_ms = self.pilot.clock() if _decision_ms is None else _decision_ms
        if self.step_lock and not _skip_step_shadow:''')
replace('''    def tick(self, symbol, price, event_ms, received_ms, trade_id):
        if self.step_lock:''','''    def tick(self, symbol, price, event_ms, received_ms, trade_id, *, _skip_step_shadow=False):
        if self.step_lock and not _skip_step_shadow:''')
replace('''    def _queue_notify(self, kind, text):
        if not flag('EARLY_V2_NOTIFY'):''','''    def _queue_notify(self, kind, text):
        if kind == 'selector' and not early_signal_notifications_enabled():
            logging.getLogger(__name__).info('TG_SUPPRESSED kind=EARLY_V2_SELECTOR reason=USER_POLICY delivered=0')
            return
        if not flag('EARLY_V2_NOTIFY'):''')
replace("            self._log_step_lock_report()\n            try:\n                await self.pilot.reconcile(exchange)","            await asyncio.to_thread(self._log_step_lock_report)\n            try:\n                await self.pilot.reconcile(exchange)")
replace('            report = self.step_lock.summary(notional_usdt=2000.0, recent_limit=10 if include_recent else 0)','            report = await asyncio.to_thread(self.step_lock.summary, notional_usdt=2000.0, recent_limit=10 if include_recent else 0)')
p.write_text(s)
expected_final={'bot.py':'b59a9962bcacf53f5fdf9d6cf83fb1df439b9e4bee75d4c8694973b295735592','early_v2_adapter.py':'b1e02cc74dc75b52491628eeb7da43d9a031b0014befecff9d8585ee4cca11a9','runtime_io.py':'afe6f9d79b5d7e6ae4f18c38db1c316dd778e96aa1827a4639bb602874686446','notification_policy.py':'a90176e47c51ea5177bb55ec5bf57f705cc73ea3f780a8f0b47b2e96c1e68049'}
for name,sha in expected_final.items():
 assert hashlib.sha256((root/name).read_bytes()).hexdigest()==sha, 'CANDIDATE_HASH_MISMATCH:'+name
 print('VERIFIED_RUNTIME_SHA256',name,sha)
