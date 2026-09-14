import ast
import asyncio
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import test_v5135 as fixtures
import daytrades
import telegram_ux as ux
import telegram_cards as cards
from position_observer import PositionObserver, render_card, roe_crossings, ALERT_COOLDOWN_S

bot=fixtures.bot


class MenuTests(unittest.TestCase):
    def test_aliases_and_keyboard_commands(self):
        for text in ('/menu','menu','menü','/start',' MENÜ ','/start@scanner'):
            self.assertEqual('/menu',ux.normalize(text))
        self.assertEqual('/daytrades',ux.normalize('/daytrade'))
        self.assertEqual('/daytrades',ux.normalize('/DAYTRADE@scanner'))
        self.assertEqual(['/status','/positions','/daytrades','/daytradeslist','/top','/altstats','/settings','/help'],[command for _,command in ux.MENU])
        for row in ux.menu_markup()['keyboard']:
            for button in row:self.assertIn(ux.normalize(button['text']),[command for _,command in ux.MENU])
        self.assertNotIn('latestexport',json.dumps(ux.menu_markup()))

    def test_menu_and_help_are_read_only_and_admin_gated(self):
        send=AsyncMock()
        with patch.object(bot,'_at_admin_allowed',return_value=True),patch.object(bot,'telegram_send',send),patch.object(bot,'_at_save_setting') as save:
            for alias in ('menu','menü','/menu','/start'):
                self.assertTrue(asyncio.run(bot._at_command(None,alias,'admin','user')))
                self.assertEqual(ux.menu_markup(),send.await_args.kwargs['reply_markup'])
            asyncio.run(bot._at_command(None,'/help','admin','user'))
            self.assertIn('/latestexport',send.await_args.args[1]);save.assert_not_called()
        with patch.object(bot,'_at_admin_allowed',return_value=False),patch.object(bot,'telegram_send',send):
            send.reset_mock();asyncio.run(bot._at_command(None,'/menu','bad','bad'));send.assert_not_awaited()

    def test_mode_needs_actor_bound_confirmation_and_live_stays_locked(self):
        ux.pending_modes.clear()
        saved=dict(bot.autotrade_cfg)
        try:
            bot.autotrade_cfg['mode']='OFF'
            with patch.object(bot,'_at_admin_allowed',return_value=True),patch.object(bot,'telegram_send',new_callable=AsyncMock),patch.object(bot,'telegram_api_call',new_callable=AsyncMock),patch.object(bot,'_at_save_setting') as save:
                asyncio.run(bot._at_command(None,'/autotrade dry','admin','user'))
                self.assertEqual('OFF',bot.autotrade_cfg['mode']);save.assert_not_called()
                token=next(iter(ux.pending_modes))
                def callback(user, data='uxmode:'+token):return {'id':'q','data':data,'from':{'id':user},'message':{'chat':{'id':'admin'}}}
                asyncio.run(bot.handle_autotrade_callback(None,callback('other')))
                self.assertEqual('OFF',bot.autotrade_cfg['mode'])
                asyncio.run(bot.handle_autotrade_callback(None,callback('user')))
                self.assertEqual('DRY',bot.autotrade_cfg['mode']);self.assertEqual(1,save.call_count)
                asyncio.run(bot.handle_autotrade_callback(None,callback('user')))
                self.assertEqual(1,save.call_count)
                asyncio.run(bot._at_command(None,'/autotrade live','admin','user'))
                self.assertFalse(bot.AUTO_TRADE_LIVE_ALLOWED);self.assertEqual('DRY',bot.autotrade_cfg['mode'])
        finally:bot.autotrade_cfg.update(saved);ux.pending_modes.clear()

    def test_pagination_preserves_text_under_telegram_limit(self):
        text=('🟢'*4000)+'\n'+'coin\n'*2000
        result=cards.pages(text)
        self.assertGreater(len(result),1)
        for page in result:self.assertLessEqual(len(page.encode('utf-16-le'))//2,3500)
        self.assertEqual(text.replace('\n',''),''.join(result).replace('\n',''))

    def test_ownership_display_only(self):
        expected={'BOT DRY':cards.BOT_DRY,'BOT':cards.BOT_LIVE,'BOT LIVE':cards.BOT_LIVE,
                  'MANUAL':cards.MANUAL_LIVE,'UNKNOWN OWNERSHIP':cards.MANUAL_LIVE}
        for source,label in expected.items():self.assertEqual(label,cards.ownership(source))
        self.assertEqual('UNKNOWN OWNERSHIP',daytrades.classify('X','1',None,[]))

    def test_reportstatus_is_read_only_and_does_not_assert_fresh_check(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);sid='sample';(root/sid).mkdir()
            (root/'latest.json').write_text(json.dumps({'snapshot_id':sid}))
            (root/sid/'manifest.json').write_text(json.dumps({'valid':True,'integrity_check':['ok'],'unchanged':False,'created_time_ms':1000}))
            (root/'daily-2026-09-14.json').write_text(json.dumps({'local_date':'2026-09-14','snapshot':{'snapshot_id':sid,'valid':True}}))
            class Worker:
                def state(self):return {'daily_date':'2026-09-14'}
            worker=Worker();worker.root=root
            before={str(p):p.read_bytes() for p in root.rglob('*') if p.is_file()}
            text=ux.report_status(worker)
            self.assertIn('kayıtlı sonuç',text);self.assertIn('hazır',text)
            self.assertEqual(before,{str(p):p.read_bytes() for p in root.rglob('*') if p.is_file()})

    def test_production_and_startup_research_baselines_unchanged(self):
        root=fixtures.ROOT
        baseline=json.loads((root/'tests/telegram_ux_baseline.json').read_text())
        tree=ast.parse((root/'binance_momentum_bot/bot.py').read_text(encoding='utf-8'))
        tree.body=[n for n in tree.body if (not isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) or n.name not in baseline['ui_functions'])
                   and not (isinstance(n,ast.Import) and ast.unparse(n)=='import telegram_ux')]
        self.assertEqual(baseline['protected_bot_ast'],hashlib.sha256(ast.dump(tree,include_attributes=False).encode()).hexdigest())
        for path,digest in baseline['files'].items():self.assertEqual(digest,hashlib.sha256((root/path).read_bytes()).hexdigest(),path)
        self.assertFalse(bot.AUTO_TRADE_LIVE_ALLOWED)


class DayTests(unittest.TestCase):
    setUp=fixtures.DatabaseCase.setUp
    sql=fixtures.DatabaseCase.sql

    def seed(self):
        now=int(datetime(2026,9,14,12,tzinfo=cards.IST).timestamp()*1000)
        self.sql("INSERT INTO autotrade_trades(symbol,mode,status,side,margin_usdt,leverage,notional_usdt,opened_ts_ms,closed_ts_ms,entry_price,exit_price,net_pnl,close_reason,updated_ts) VALUES('XUSDT','DRY','CLOSED','LONG',100,10,1000,?,?,100,102,19,'TP2',0)",(now-60000,now))
        return now

    def test_summary_and_list_fields_today_and_midnight(self):
        now=self.seed()
        async def request(path,params):return []
        with closing(bot.db_connect()) as c:
            summary=asyncio.run(daytrades.report(c,request,now))
            listing=asyncio.run(daytrades.trade_list(c,request,now))
            tomorrow=asyncio.run(daytrades.trade_list(c,request,now+86400000))
        self.assertIn('BUGÜNÜN İŞLEM ÖZETİ',summary)
        for text in ('Toplam: 1','Kapalı: 1','Açık: 0','+19.00 USDT','+19.00%'):self.assertIn(text,summary)
        for text in ('XUSDT','🤖 BOT / DRY','LONG','10x','Açılış:','Kapanış:','Entry: 100','Exit: 102','TP2','+19.00%'):self.assertIn(text,listing)
        self.assertNotIn('XUSDT',tomorrow)
        self.assertEqual(datetime(2026,9,14,0,tzinfo=cards.IST).timestamp()*1000,daytrades.local_start(now))

    def test_manual_close_groups_dedup_and_no_invented_historical_leverage(self):
        fills=[dict(symbol='XUSDT',positionSide='SHORT',side='BUY',orderId=7,id=i,time=i,realizedPnl='2',commission='.1',commissionAsset='USDT',qty='1',price='99') for i in (1,2)]
        result=daytrades.group_fills(fills,{},[])
        self.assertEqual(1,len(result));self.assertAlmostEqual(3.8,result[0]['net_pnl'])
        self.assertEqual('SHORT',result[0]['side']);self.assertEqual(99,result[0]['exit_price'])
        self.assertIsNone(result[0]['leverage']);self.assertIsNone(result[0]['opened_ts_ms']);self.assertIsNone(result[0]['roe'])

    def test_failed_history_does_not_claim_zero_live_trades(self):
        async def request(path,params):raise RuntimeError('private detail')
        with closing(bot.db_connect()) as c:message=asyncio.run(daytrades.report(c,request,int(time.time()*1000)))
        self.assertIn('LIVE geçmiş alınamadı',message);self.assertNotIn('private detail',message)

    def test_alias_and_list_route_are_admin_only_and_get_only(self):
        now=self.seed()
        with patch.object(bot,'_at_admin_allowed',return_value=True),patch.object(bot,'telegram_send',new_callable=AsyncMock) as send,patch.object(bot,'binance_signed_request',new=AsyncMock(return_value=[])) as request,patch.object(bot,'now_ms',return_value=now):
            asyncio.run(bot._at_command(None,'/daytrade','admin','user'))
            self.assertIn('BUGÜNÜN İŞLEM ÖZETİ',send.await_args.args[1])
            asyncio.run(bot._at_command(None,'/daytradeslist','admin','user'))
            self.assertIn('XUSDT',send.await_args.args[1])
            self.assertTrue(all(call.args[1]=='GET' for call in request.await_args_list))
        with patch.object(bot,'_at_admin_allowed',return_value=False),patch.object(bot,'binance_signed_request',new_callable=AsyncMock) as request:
            asyncio.run(bot._at_command(None,'/daytradeslist','bad','bad'));request.assert_not_awaited()

    def test_open_positions_include_live_and_dry(self):
        now=self.seed();self.sql("UPDATE autotrade_trades SET status='OPEN',closed_ts_ms=NULL")
        live=dict(symbol='BUSDT',positionSide='SHORT',positionAmt='-1',entryPrice='100',markPrice='99',unRealizedProfit='1',positionInitialMargin='10')
        async def request(session,method,path,*args):
            self.assertEqual('GET',method)
            return [live] if path.endswith('positionRisk') else [dict(symbol='BUSDT',leverage=10)]
        ux.leverage_cache.expires=0
        with patch.object(bot,'binance_signed_request',request):text=asyncio.run(ux.show_positions(vars(bot),None))
        for value in ('XUSDT','BUSDT','🤖 BOT / DRY','👤 MANUEL / LIVE','SHORT'):self.assertIn(value,text)


class ObserverTests(unittest.TestCase):
    setUp=fixtures.DatabaseCase.setUp
    rows=fixtures.DatabaseCase.rows

    def test_bidirectional_boundaries_cooldown_and_rearm(self):
        memory={};hits=set()
        loss,up=roe_crossings(memory,-20,[5,10,20],hits,100)
        self.assertEqual([5,10,20],loss);hits.update(loss)
        self.assertEqual([],up)
        # Arm -20 recovery with a meaningful dip, then cross above. Cooldown retains pending eligibility.
        roe_crossings(memory,-21,[5,10,20],hits,101)
        self.assertEqual(([],[]),roe_crossings(memory,-19,[5,10,20],hits,120))
        self.assertEqual(([],[20]),roe_crossings(memory,-19,[5,10,20],hits,160))
        self.assertEqual(([],[]),roe_crossings(memory,-20.1,[5,10,20],hits,161))
        self.assertEqual(([20],[]),roe_crossings(memory,-20.1,[5,10,20],hits,220))
        for ts,roe in enumerate((-19.9,-20.1,-19.9,-20.1),300):
            self.assertEqual(([],[]),roe_crossings(memory,roe,[5,10,20],hits,ts))
        self.assertEqual(([],[5,10]),roe_crossings(memory,-4,[5,10,20],hits,400))

    def test_recovery_persists_across_restart_and_close_is_blue(self):
        p=dict(symbol='XUSDT',positionSide='BOTH',positionAmt='1',entryPrice='100',markPrice='97.5',unRealizedProfit='-2.5',positionInitialMargin='10')
        positions=[p]
        async def request(session,method,path):return positions if path.endswith('positionRisk') else [dict(symbol='XUSDT',leverage=10)]
        send=AsyncMock(return_value=True)
        def observer():return PositionObserver(bot.db_connect,request,send,lambda *_:'MANUAL',bot._po_zone,[5,10,20],0)
        with patch('position_observer.time.time',return_value=100):asyncio.run(observer().poll(None))
        loss=self.rows("SELECT * FROM position_observer_events WHERE event='ROE_LOSS'")
        self.assertEqual([5,10,20],json.loads(loss[0]['detail_json'])['milestones'])
        p.update(markPrice='98.5',unRealizedProfit='-1.5')
        with patch('position_observer.time.time',return_value=161):asyncio.run(observer().poll(None))
        recovery=self.rows("SELECT * FROM position_observer_events WHERE event='ROE_RECOVERY'")
        self.assertEqual(1,len(recovery));self.assertIn('-%20 ÜZERİNE TOPARLANDI',render_card(recovery[0]))
        with patch('position_observer.time.time',return_value=222):asyncio.run(observer().poll(None))
        self.assertEqual(1,len(self.rows("SELECT * FROM position_observer_events WHERE event='ROE_RECOVERY'")))
        positions.clear()
        with patch('position_observer.time.time',return_value=300):asyncio.run(observer().poll(None))
        close=self.rows("SELECT * FROM position_observer_events WHERE event='CLOSE_OBSERVED'")[0]
        self.assertIn('🔵 POZİSYON KAPANDI',render_card(close));self.assertEqual('DELIVERED',close['notification_delivery'])

    def test_entry_cross_profit_and_loss_use_existing_confirmation_and_hysteresis(self):
        p=dict(symbol='XUSDT',positionSide='BOTH',positionAmt='1',entryPrice='100',markPrice='99',unRealizedProfit='-1',positionInitialMargin='10')
        async def request(session,method,path):return [p] if path.endswith('positionRisk') else []
        observer=PositionObserver(bot.db_connect,request,AsyncMock(return_value=True),lambda *_:'MANUAL',bot._po_zone,[5,10,20],5)
        def poll(ts,price):
            p['markPrice']=str(price)
            with patch('position_observer.time.time',return_value=ts):asyncio.run(observer.poll(None))
        poll(100,99);poll(101,101);poll(106,101)
        event=self.rows("SELECT * FROM position_observer_events WHERE event='ENTRY_CROSS_PROFIT'")[0]
        self.assertIn('🟢 GİRİŞ ÜZERİNE ÇIKTI',render_card(event))
        poll(107,99);poll(112,99)
        self.assertFalse(self.rows("SELECT * FROM position_observer_events WHERE event='ENTRY_CROSS_LOSS'"))
        poll(166,99)
        event=self.rows("SELECT * FROM position_observer_events WHERE event='ENTRY_CROSS_LOSS'")[0]
        self.assertIn('🔴 GİRİŞ ALTINA İNDİ',render_card(event))

    def test_missing_roe_and_neutral_jitter_do_not_notify(self):
        memory={}
        self.assertEqual(([],[]),roe_crossings(memory,None,[5,10,20],set(),100))
        self.assertEqual({},memory)
        for value in (99.99,100,100.01):self.assertEqual('NEUTRAL',bot._po_zone('LONG',value,100)[0])


if __name__=='__main__':unittest.main()
