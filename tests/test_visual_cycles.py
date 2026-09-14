import asyncio
from contextlib import closing
from io import BytesIO
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch
import test_v5135 as fixtures
import position_cycles as cycles
import trade_views as views
import visual_cards as visual
import telegram_ux as ux
from telegram_cards import MANUAL_LIVE, BOT_DRY, BOT_LIVE

bot=fixtures.bot


def fill(i, side, qty, price, pnl=0, ts=None, position='BOTH', order=None):
    return dict(id=i, symbol='XUSDT', positionSide=position, side=side, qty=str(qty),price=str(price),
        realizedPnl=str(pnl), commission='0.1',commissionAsset='USDT',time=ts or i*1000,orderId=order or i)


class CycleTests(unittest.TestCase):
    def run_cycle(self, fills, **kwargs):
        return cycles.reconstruct(fills,kwargs.get('orders',{}),kwargs.get('ledger',[]),kwargs.get('positions',[]),kwargs.get('start',0),100000,kwargs.get('complete',True))

    def test_manual_full_cycle_partial_fills_and_cross_midnight(self):
        data=[fill(1,'BUY',1,100,ts=49000),fill(2,'BUY',2,103,ts=49500),
              fill(3,'SELL',1,105,3,ts=51000),fill(4,'SELL',2,106,8,ts=52000)]
        result=self.run_cycle(data+[data[2]],start=50000)
        self.assertEqual(1,len(result));r=result[0]
        self.assertEqual(49000,r['opened_ts_ms']);self.assertEqual(52000,r['closed_ts_ms'])
        self.assertEqual(102,r['entry_price']);self.assertAlmostEqual(105+2/3,r['exit_price'])
        self.assertAlmostEqual(10.6,r['net_pnl']);self.assertTrue(r['cycle_complete'])
        self.assertEqual('UNKNOWN OWNERSHIP',r['ownership']);self.assertIsNone(r['leverage']);self.assertIsNone(r['roe'])

    def test_hedge_long_short_are_separate(self):
        data=[fill(1,'BUY',1,100,position='LONG'),fill(2,'SELL',2,100,position='SHORT'),
              fill(3,'SELL',1,102,2,position='LONG'),fill(4,'BUY',2,99,2,position='SHORT')]
        result=self.run_cycle(data)
        self.assertEqual(['LONG','SHORT'],[r['side'] for r in result])
        self.assertTrue(all(r['cycle_complete'] for r in result))

    def test_reversal_splits_fee_and_close_pnl_without_double_count(self):
        result=self.run_cycle([fill(1,'BUY',1,100),fill(2,'SELL',3,101,1),fill(3,'BUY',2,99,4)])
        self.assertEqual(2,len(result));self.assertEqual(['LONG','SHORT'],[r['side'] for r in result])
        self.assertAlmostEqual(4.7,sum(r['net_pnl'] for r in result))
        self.assertEqual(101,result[1]['entry_price'])

    def test_open_before_history_stays_unknown_then_next_cycle_is_complete(self):
        result=self.run_cycle([fill(1,'SELL',1,102,2),fill(2,'BUY',1,100),fill(3,'SELL',1,101,1)])
        self.assertEqual(2,len(result));self.assertFalse(result[0]['cycle_complete'])
        for field in ('entry_price','opened_ts_ms','net_pnl','leverage','roe'):self.assertIsNone(result[0][field])
        self.assertTrue(result[1]['cycle_complete']);self.assertAlmostEqual(.8,result[1]['net_pnl'])

    def test_nonflat_anchor_prevents_false_completed_cycle(self):
        position=dict(symbol='XUSDT',positionSide='BOTH',positionAmt='2',updateTime=1000)
        self.assertEqual([],self.run_cycle([fill(1,'BUY',3,100),fill(2,'SELL',1,101,1)],positions=[position]))

    def test_missing_fees_pnl_or_anchor_does_not_invent_net(self):
        for field,value in (('commissionAsset','BNB'),('commission',None),('realizedPnl',None)):
            data=[fill(1,'BUY',1,100),fill(2,'SELL',1,101,1)];data[1][field]=value
            self.assertIsNone(self.run_cycle(data)[0]['net_pnl'])
        data=[fill(1,'BUY',1,100),fill(2,'SELL',1,101,1)]
        r=self.run_cycle(data,complete=False)[0]
        self.assertFalse(r['cycle_complete']);self.assertIsNone(r['net_pnl'])
        newer=dict(symbol='XUSDT',positionSide='BOTH',positionAmt='0',updateTime=100001)
        self.assertFalse(self.run_cycle(data,positions=[newer])[0]['cycle_complete'])

    def test_bot_ledger_evidence_enriches_only_matching_entry(self):
        ledger=[dict(mode='LIVE',symbol='XUSDT',entry_order_id='1',leverage=10,margin_usdt=10)]
        r=self.run_cycle([fill(1,'BUY',1,100),fill(2,'SELL',1,102,2)],ledger=ledger)[0]
        self.assertEqual('BOT LIVE',r['ownership']);self.assertEqual(10,r['leverage']);self.assertAlmostEqual(18,r['roe'])
        self.assertEqual('UNKNOWN OWNERSHIP',self.run_cycle([fill(5,'BUY',1,100),fill(6,'SELL',1,102,2)],ledger=ledger)[0]['ownership'])

    def test_close_reason_is_order_evidence_not_guessed_from_market(self):
        data=[fill(1,'BUY',1,100),fill(2,'SELL',1,101,1)]
        for kind,expected in [('MARKET',None),('STOP_MARKET','STOP (emir türü)')]:
            r=self.run_cycle(data,orders={('XUSDT','2'):dict(origType=kind,reduceOnly=True)})[0]
            self.assertEqual(expected,r['close_reason'])
        with self.assertRaises(ValueError):self.run_cycle(data,orders={('XUSDT','1'):dict(reduceOnly=True)})

    def test_loader_bounds_read_only_calls_and_midnight_lookback(self):
        start=10*cycles.DAY;end=start+10000
        data=[fill(1,'BUY',1,100,ts=start-1000),fill(2,'SELL',1,101,1,ts=start+1000)]
        calls=[]
        async def request(path,params):
            calls.append((path,params))
            if path.endswith('positionRisk'):return []
            if path.endswith('income'):return [dict(symbol='XUSDT')]
            if path.endswith('userTrades'):return [f for f in data if params['startTime']<=f['time']<=params['endTime']]
            return dict(orderId=params['orderId'])
        result,notices=asyncio.run(cycles.load(request,start,end,[]))
        self.assertFalse(notices);self.assertEqual(1,len(result));self.assertTrue(result[0]['cycle_complete'])
        self.assertTrue(any(p.get('startTime')==start-7*cycles.DAY for _,p in calls))
        self.assertTrue(all(path in ('/fapi/v3/positionRisk','/fapi/v1/income','/fapi/v1/userTrades','/fapi/v1/order') for path,_ in calls))


class VisualTests(unittest.TestCase):
    setUp=fixtures.DatabaseCase.setUp
    sql=fixtures.DatabaseCase.sql

    def view(self, count=3):
        data=[dict(symbol='XUSDT',ownership='UNKNOWN OWNERSHIP',side='LONG',leverage=None,entry_price=100,
            exit_price=102,net_pnl=2 if i%2==0 else -2,roe=None,opened_ts_ms=1000,closed_ts_ms=2000,
            provenance='ANCHORED_COMPLETE_FILLS') for i in range(count)]
        return dict(title='BUGÜN KAPANAN İŞLEMLER',date='14.09.2026',summary=views.summary(data),cards=[views.card(r) for r in data],notices=['—: veri yok.'])

    def test_png_mobile_pages_green_red_and_unknown(self):
        from PIL import Image
        view=self.view();batches=visual.batches(view)
        self.assertEqual([2,1],[len(b) for b in batches])
        png=visual.render(view,batches[0],1,2)
        image=Image.open(BytesIO(png));self.assertEqual('PNG',image.format)
        self.assertLess(sum(image.size),10000);self.assertLess(len(png),9000000)
        colors=set(image.get_flattened_data());self.assertIn((54,225,160),colors);self.assertIn((255,114,136),colors)
        self.assertIn('—',visual.text_page(view,batches[0],1,2))

    def test_render_error_upload_error_and_raw_fallback(self):
        view=self.view();send=AsyncMock(return_value=True);context=dict(telegram_send=send)
        with patch.object(visual,'render',side_effect=RuntimeError('font unavailable')):
            self.assertTrue(asyncio.run(visual.deliver(context,None,view,'admin')))
        self.assertEqual(2,send.await_count)
        send.reset_mock()
        with patch.object(visual,'send_photo',new=AsyncMock(return_value=False)) as photo:
            asyncio.run(visual.deliver(context,None,view,'admin'))
            self.assertEqual(2,photo.await_count);self.assertEqual(2,send.await_count)
        with patch.object(visual,'render') as render:
            asyncio.run(visual.deliver(context,None,view,'admin',raw=True));render.assert_not_called()

    def test_successful_first_page_is_not_resent_on_second_page_failure(self):
        send=AsyncMock(return_value=True)
        with patch.object(visual,'send_photo',new=AsyncMock(side_effect=[True,False])):
            asyncio.run(visual.deliver(dict(telegram_send=send),None,self.view(),'admin'))
        self.assertEqual(1,send.await_count);self.assertIn('Sayfa 2/2',send.await_args.args[1])

    def test_commands_admin_only_and_bot_dry_ledger_preserved(self):
        now=bot.now_ms()
        self.sql("INSERT INTO autotrade_trades(symbol,mode,status,side,margin_usdt,leverage,notional_usdt,opened_ts_ms,closed_ts_ms,entry_price,exit_price,net_pnl,close_reason,updated_ts) VALUES('DRYUSDT','DRY','CLOSED','LONG',100,10,1000,?,?,100,102,19,'TP2',0)",(now-1000,now))
        with patch.object(bot,'_at_admin_allowed',return_value=True),patch.object(bot,'binance_signed_request',new=AsyncMock(return_value=[])) as request,patch.object(bot,'telegram_send',new=AsyncMock(return_value=True)) as send:
            asyncio.run(bot._at_command(None,'/daytradesraw','admin','user'))
            text=send.await_args.args[1]
            for value in ('DRYUSDT',BOT_DRY,'+19.00 USDT','TP2'):self.assertIn(value,text)
            self.assertTrue(all(c.args[1]=='GET' for c in request.await_args_list))
        with patch.object(bot,'_at_admin_allowed',return_value=False),patch.object(bot,'binance_signed_request',new_callable=AsyncMock) as request:
            for command in ('/daytradeslist','/daytradesraw','/positions','/positionsraw'):
                asyncio.run(bot._at_command(None,command,'bad','bad'))
            request.assert_not_awaited()

    def test_open_positions_totals_and_missing_metrics(self):
        positions=[dict(symbol='XUSDT',positionSide='LONG',positionAmt='2',entryPrice='100',markPrice='101',
            unRealizedProfit='2',positionInitialMargin='10')]
        async def request(session,method,path):
            self.assertEqual('GET',method)
            return positions if path.endswith('positionRisk') else []
        ux.leverage_cache.expires=0
        with patch.object(bot,'binance_signed_request',request):view=asyncio.run(views.opened(vars(bot),None,ux.leverage_cache))
        self.assertIn('Açık 1',view['summary'][0]);self.assertIn('LONG 1',view['summary'][0])
        self.assertTrue(any('202 USDT' in s for s in view['summary']))
        self.assertEqual(MANUAL_LIVE,view['cards'][0]['source'])

    def test_open_notification_uses_shared_renderer_and_text_fallback(self):
        from position_observer import render_card
        event=dict(event='OPEN_OBSERVED',symbol='XUSDT',source='BOT',direction='LONG',entry_price=100,
            current_price=100,leverage=10,roe=0,event_time_ms=1000,detail_json='{"pnl":0}')
        message=render_card(event)
        with patch.object(visual,'deliver',new=AsyncMock(return_value=True)) as delivery:
            self.assertTrue(asyncio.run(bot._observer_send(None,message)))
            view=delivery.await_args.args[2];self.assertEqual('🟢 POZİSYON AÇILDI',view['title'])
            self.assertEqual(BOT_LIVE,view['cards'][0]['source'])


if __name__=='__main__':unittest.main()
