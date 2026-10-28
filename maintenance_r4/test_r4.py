"""R4 acceptance: real modules, synthetic DB, no external requests/orders."""
import asyncio
import ast
from collections import deque
from contextlib import ExitStack, closing
import importlib.util
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch, AsyncMock, Mock

ROOT=Path(__file__).resolve().parents[1]
APP=Path(os.environ.get('R4_APP',str(ROOT/'candidate/binance_momentum_bot')))
BASE=Path(os.environ.get('R4_BASE',str(ROOT/'baseline/binance_momentum_bot')))
sys.path.insert(0,str(APP))
os.environ.update(PYTHON_DOTENV_DISABLED='1',AUTO_TRADE_LIVE_ALLOWED='0',
    EARLY_V2_LIVE_ALLOWED='0',EARLY_V2_BOOT_DRY='0',EARLY_V2_AUTO_DRY='0',
    STEP_LOCK_SHADOW_ENABLED='0',RESEARCH_EXPORT_ENABLED='0',EARLY_SIGNAL_TELEGRAM_ENABLED='0',
    TELEGRAM_BOT_TOKEN='',BINANCE_API_KEY='',BINANCE_API_SECRET='')
import bot
import early_v2_adapter
from runtime_io import SerialIO, loop_health
from notification_policy import early_signal_notifications_enabled
from telegram_dispatch import NotificationDispatch
from step_lock_shadow import StepLockShadow

class R4Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.stack.enter_context(patch('aiohttp.ClientSession._request',side_effect=AssertionError('NETWORK_FORBIDDEN')))
        self.stack.enter_context(patch('subprocess.Popen',side_effect=AssertionError('PROCESS_FORBIDDEN')))
        self.stack.enter_context(patch.object(bot,'DB_PATH',str(Path(self.tmp.name)/'signals.db')))
        self.stack.enter_context(patch.dict(os.environ,EARLY_SIGNAL_TELEGRAM_ENABLED='0'))
        self.stack.enter_context(patch.dict(bot.autotrade_cfg,mode='OFF'))
        bot.init_db()

    async def test_policy_default_and_explicit_enable(self):
        with patch.dict(os.environ,{},clear=True):self.assertFalse(early_signal_notifications_enabled())
        for value in ('0','false','off','no',''):
            with patch.dict(os.environ,EARLY_SIGNAL_TELEGRAM_ENABLED=value):self.assertFalse(early_signal_notifications_enabled())
        with patch.dict(os.environ,EARLY_SIGNAL_TELEGRAM_ENABLED='1'):self.assertTrue(early_signal_notifications_enabled())

    async def test_public_early_never_calls_private_or_broadcast(self):
        sender=AsyncMock(side_effect=AssertionError('EARLY_NETWORK_FORBIDDEN'))
        with patch.object(bot,'telegram_send',sender),patch.object(bot,'TELEGRAM_BROADCAST_ENABLED',True):
            result=await bot.telegram_public_alert(None,'synthetic',symbol='TESTUSDT',notification_kind='EARLY')
            self.assertIs(result,False);sender.assert_not_called()

    async def test_direct_early_sender_is_not_false_ack(self):
        inner=AsyncMock(side_effect=AssertionError('EARLY_NETWORK_FORBIDDEN'))
        with patch.object(bot,'_telegram_send_with_ack',inner):
            self.assertIs(await bot.telegram_send(None,'synthetic',notification_kind='EARLY'),False)
            inner.assert_not_called()

    async def test_early_no_outbox_but_premium_and_position_persist(self):
        delivered=[]
        async def sender(text,meta):delivered.append(meta['notification_kind']);return True
        with patch.object(bot,'TELEGRAM_CHAT_ID','owner'),patch.object(bot,'TELEGRAM_BROADCAST_CHAT_ID','channel'),patch.object(bot,'TELEGRAM_BROADCAST_ENABLED',True):
            async with NotificationDispatch(str(Path(self.tmp.name)/'outbox.db'),sender) as outbox:
                with patch.object(bot,'telegram_dispatcher',outbox):
                    muted=bot.telegram_queue_public_alert(None,'early',symbol='TESTUSDT',notification_kind='EARLY')
                    self.assertEqual(muted.status,'SUPPRESSED_BY_POLICY')
                    self.assertEqual(outbox._db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0],0)
                    bot.telegram_queue_public_alert(None,'premium',symbol='TESTUSDT',signal_id=100,notification_kind='PREMIUM')
                    for kind in ('OPEN','STOP','CLOSE'):
                        self.assertEqual(bot.telegram_notify(None,kind,signal_id=100,notification_kind=kind).status,'COMMITTED')
                    await outbox.join()
                    self.assertNotIn('EARLY',delivered)
                    for kind in ('PREMIUM','OPEN','STOP','CLOSE'):self.assertIn(kind,delivered)

    async def test_selector_muted_but_execution_notifications_preserved(self):
        integration=early_v2_adapter.Integration.__new__(early_v2_adapter.Integration)
        integration.notify_queue=asyncio.Queue(maxsize=10)
        with patch.dict(os.environ,EARLY_V2_NOTIFY='1'):
            integration._queue_notify('selector','signal')
            self.assertTrue(integration.notify_queue.empty())
            for kind in ('open','close'):integration._queue_notify(kind,'position')
            self.assertEqual(integration.notify_queue.qsize(),2)

    async def test_real_evaluate_muted_early_keeps_recording_and_premium_gate(self):
        recorded=[];gates=[];net=AsyncMock(side_effect=AssertionError('NETWORK_FORBIDDEN'))
        now=time.time()
        st=SimpleNamespace(eval_inflight=False,last_eval_ts=0,candidate_since=now-10,candidate_last_check=0,
            candidate_checks=2,candidate_passes=2,candidate_prices=deque([100.]),candidate_scores=deque([90]),
            active_radar_id=7,active_radar_notified=False,early_alert_ts=0,episode_id=8)
        def recorder(*a,**k):recorded.append(a)
        def fresh(*a):gates.append(1);return False,['SYNTHETIC_STOP_BEFORE_ORDER']
        with ExitStack() as stack:
            for name in ['maybe_record_v59_research','maybe_record_research','update_episode_peak',
                         'funnel_hit','save_candidate_event','_h1_observe_early_checkpoint_shadow',
                         'mark_radar_notified','quality_arm','_arm_stage_entry']:
                stack.enter_context(patch.object(bot,name,recorder))
            stack.enter_context(patch.object(bot,'states',{'TESTUSDT':st}))
            stack.enter_context(patch.object(bot,'early_v2',None))
            stack.enter_context(patch.object(bot,'maybe_trend_build_up',AsyncMock()))
            stack.enter_context(patch.object(bot,'compute_metrics',return_value=dict(qv24=1e9,price=100.,chg30=1.,buy30=.8,flow30=2.)))
            stack.enter_context(patch.object(bot,'score_metrics',return_value=90))
            stack.enter_context(patch.object(bot,'continuity_pass',return_value=True))
            stack.enter_context(patch.object(bot,'early_notify_pass',return_value=True))
            stack.enter_context(patch.object(bot,'next_daily_notice_no',return_value=1))
            stack.enter_context(patch.object(bot,'build_early_message',return_value='synthetic'))
            stack.enter_context(patch.object(bot.early_checkpoint_shadow,'public_early',recorder))
            stack.enter_context(patch.object(bot,'market_data_fresh',fresh))
            stack.enter_context(patch.object(bot,'telegram_send',net))
            for name,value in {'MIN_24H_QUOTE_VOLUME':1,'CANDIDATE_TTL_SECONDS':1000,'CONFIRM_INTERVAL_SECONDS':0,'EVAL_MIN_INTERVAL':0,'CONFIRM_REQUIRED':3,'EARLY_ALERT_COOLDOWN_SECONDS':1}.items():
                stack.enter_context(patch.object(bot,name,value))
            await asyncio.wait_for(bot.evaluate(None,'TESTUSDT'),.5)
        self.assertEqual(gates,[1]);self.assertFalse(st.eval_inflight);net.assert_not_called()
        self.assertTrue(st.active_radar_notified)
        self.assertTrue(any(a and a[0]=='EARLY' for a in recorded))
        self.assertTrue(any(len(a)>1 and a[1]=='early_alert' for a in recorded))

    async def test_serial_worker_fifo_single_owner_and_values(self):
        worker=SerialIO('test-serial');events=[];owners=[]
        def work(n):
            events.append(n);owners.append(threading.get_ident());time.sleep(.005);return n*2
        try:
            results=await asyncio.gather(*(worker.call(work,i) for i in range(20)))
            self.assertEqual(events,list(range(20)));self.assertEqual(results,[i*2 for i in range(20)])
            self.assertEqual(len(set(owners)),1);self.assertNotEqual(owners[0],threading.get_ident())
        finally:await worker.close()

    async def test_cancel_does_not_release_writer_before_real_completion(self):
        worker=SerialIO('cancel');entered=threading.Event();release=threading.Event();events=[]
        def write():entered.set();release.wait(2);events.append('commit');return 1
        task=asyncio.create_task(worker.call(write))
        while not entered.is_set():await asyncio.sleep(.001)
        task.cancel()
        later=asyncio.create_task(worker.call(lambda:events.append('next')))
        await asyncio.sleep(.02);self.assertEqual(events,[]);self.assertFalse(later.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):await task
        await later;self.assertEqual(events,['commit','next']);await worker.close()
        with self.assertRaises(RuntimeError):await worker.call(lambda:None)

    async def test_serial_failure_propagates_and_worker_recovers(self):
        worker=SerialIO('error')
        try:
            with self.assertRaises(sqlite3.OperationalError):await worker.call(lambda:(_ for _ in ()).throw(sqlite3.OperationalError('synthetic')))
            self.assertEqual(await worker.call(lambda:7),7)
        finally:await worker.close()

    async def test_real_shadow_records_same_sequence_and_restart_state(self):
        worker=SerialIO('shadow');paths=[Path(self.tmp.name)/('shadow%d.db'%i) for i in (1,2)]
        objects=[StepLockShadow(lambda p=p:sqlite3.connect(p)) for p in paths]
        original,offloop=objects
        try:
            original.arm('1','TESTUSDT',100.,1000,7)
            await worker.call(offloop.arm,'1','TESTUSDT',100.,1000,7)
            for i,price in enumerate((100.1,100.21,100.51,100.76,100.74,100.9),1):
                original.tick('TESTUSDT',price,1000+i*100,1000+i*100,i)
                await worker.call(offloop.tick,'TESTUSDT',price,1000+i*100,1000+i*100,i)
            for table in ('early_step_lock_shadow_v1','early_step_lock_events_v1','early_step_lock_poststop_v1'):
                with sqlite3.connect(paths[0]) as c:expected=c.execute('SELECT * FROM '+table).fetchall()
                with sqlite3.connect(paths[1]) as c:actual=c.execute('SELECT * FROM '+table).fetchall()
                self.assertEqual(expected,actual,table)
            recovered=StepLockShadow(lambda:sqlite3.connect(paths[1]))
            self.assertEqual(recovered.get('1')['close_reason'],'STEP_LOCK')
        finally:await worker.close()

    async def test_db_lock_does_not_stall_loop_and_commit_is_awaited(self):
        path=Path(self.tmp.name)/'lock.db'
        def connect():return sqlite3.connect(path,timeout=2)
        shadow=StepLockShadow(connect);shadow.arm('1','TESTUSDT',100.,1000,7)
        locked=threading.Event()
        def hold():
            with connect() as c:
                c.execute('BEGIN IMMEDIATE');locked.set();time.sleep(.20);c.rollback()
        thread=threading.Thread(target=hold);thread.start()
        while not locked.is_set():await asyncio.sleep(.001)
        worker=SerialIO('shadow-lock');start=time.monotonic()
        work=asyncio.create_task(worker.call(shadow.tick,'TESTUSDT',100.21,1100,1100,1))
        await asyncio.sleep(.02);lag=time.monotonic()-start
        self.assertLess(lag,.15);self.assertFalse(work.done())
        await work;thread.join();self.assertAlmostEqual(shadow.get('1')['current_lock_pct'],.2)
        await worker.close()

    async def test_adapter_thread_boundary_preserves_timestamps_and_financial_thread(self):
        integration=early_v2_adapter.Integration.__new__(early_v2_adapter.Integration)
        integration._shadow_io=SerialIO('adapter');captured=[];owner=threading.get_ident()
        class Shadow:
            def arm(self,*args):captured.append(('arm',args,threading.get_ident()))
            def tick(self,*args):captured.append(('tick',args,threading.get_ident()))
        integration.step_lock=Shadow();integration.pilot=SimpleNamespace(clock=lambda:12345,event=lambda *a:None)
        integration.b={'states':{'TESTUSDT':SimpleNamespace(episode_id=9)}}
        integration.arm=lambda *a,**kw:captured.append(('pilot-arm',kw,threading.get_ident()))
        integration.tick=lambda *a,**kw:captured.append(('pilot-tick',a,threading.get_ident()))
        try:
            await integration.arm_async('1','TESTUSDT',{'price':100.},88)
            await integration.tick_async('TESTUSDT',100.1,12346,12347,5)
            self.assertEqual(captured[0][1],('1','TESTUSDT',100.,12345,9))
            self.assertEqual(captured[2][1],('TESTUSDT',100.1,12346,12347,5))
            self.assertNotEqual(captured[0][2],owner);self.assertEqual(captured[0][2],captured[2][2])
            self.assertEqual(captured[1][2],owner);self.assertEqual(captured[3][2],owner)
            self.assertEqual(captured[1][1]['_decision_ms'],12345)
        finally:await integration.close_io()

    async def test_watchdog_no_secrets_and_clean_stop(self):
        stop=asyncio.Event()
        with self.assertLogs('runtime_io',level='WARNING') as cm:
            task=asyncio.create_task(loop_health(stop,interval=.02,stall_seconds=.06,report_seconds=.05))
            await asyncio.sleep(.03)
            secret='SYNTHETIC_NEVER_LOG_THIS'
            time.sleep(.15)
            await asyncio.sleep(.03);stop.set();await task
        self.assertTrue(any('MAIN_LOOP_STALL' in s for s in cm.output))
        self.assertTrue(all(secret not in s for s in cm.output))

    def test_financial_and_filter_functions_unchanged(self):
        def functions(path):
            return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text()).body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        old=functions(BASE/'bot.py');new=functions(APP/'bot.py')
        changed={k for k in old if old[k]!=new[k]}
        self.assertEqual(changed,{'telegram_send','telegram_notify','telegram_public_alert','evaluate','aggtrade_chunk_ws','main','telegram_command_loop'})
        self.assertEqual((APP/'step_lock_shadow.py').read_bytes(),(BASE/'step_lock_shadow.py').read_bytes())
        self.assertEqual((APP/'early_autotrader_v2.py').read_bytes(),(BASE/'early_autotrader_v2.py').read_bytes())
        self.assertEqual((APP/'telegram_dispatch.py').read_bytes(),(BASE/'telegram_dispatch.py').read_bytes())

if __name__=='__main__':unittest.main(verbosity=2)
