import asyncio
import unittest
from unittest.mock import AsyncMock, patch
import test_v5135 as fixtures
import position_observer as po

bot = fixtures.bot


class OpenTests(unittest.TestCase):
    setUp = fixtures.DatabaseCase.setUp
    rows = fixtures.DatabaseCase.rows

    def setup_observer(self, source='MANUAL'):
        self.positions = []
        self.error = None
        async def request(session, method, path):
            self.assertEqual('GET', method)
            if self.error: raise self.error
            return self.positions if path.endswith('positionRisk') else []
        self.send = AsyncMock(return_value=True)
        self.factory = lambda: po.PositionObserver(bot.db_connect, request, self.send,
            lambda *_: source, bot._po_zone, [5, 10, 20], 0)
        return self.factory()

    def position(self):
        return dict(symbol='XUSDT', positionSide='BOTH', positionAmt='1',
                    entryPrice='100', markPrice='100', unRealizedProfit='0', positionInitialMargin='10')

    def openings(self):
        return self.rows("SELECT * FROM position_observer_events WHERE event='OPEN_OBSERVED' AND notification_delivery!='NOT_REQUESTED'")

    def test_manual_open_once_and_restart_silent(self):
        observer = self.setup_observer()
        asyncio.run(observer.poll(None))
        self.positions.append(self.position())
        asyncio.run(observer.poll(None)); asyncio.run(observer.poll(None))
        self.assertEqual(1, len(self.openings()))
        card = self.send.await_args.args[1]
        for text in ('🟢 POZİSYON AÇILDI', '👤 MANUEL / LIVE', 'XUSDT', 'LONG', 'Gözlem:'):
            self.assertIn(text, card)
        self.assertEqual('DELIVERED', self.openings()[0]['notification_delivery'])
        asyncio.run(self.factory().poll(None))
        self.assertEqual(1, len(self.openings())); self.assertEqual(1, self.send.await_count)

    def test_bot_open_and_close_reopen_new_instance(self):
        observer = self.setup_observer('BOT')
        asyncio.run(observer.poll(None)); self.positions.append(self.position())
        asyncio.run(observer.poll(None))
        self.assertIn('⚡ BOT / LIVE', self.send.await_args.args[1])
        first = self.openings()[0]['position_instance_id']
        self.positions.clear(); asyncio.run(observer.poll(None))
        self.assertIn('🔵 POZİSYON KAPANDI', self.send.await_args.args[1])
        self.positions.append(self.position()); asyncio.run(observer.poll(None))
        self.assertEqual(2, len(self.openings()))
        self.assertNotEqual(first, self.openings()[1]['position_instance_id'])

    def test_first_snapshot_adopts_and_scale_in_does_not_announce(self):
        observer = self.setup_observer(); self.positions.append(self.position())
        asyncio.run(observer.poll(None)); self.assertFalse(self.openings())
        self.positions[0].update(positionAmt='2', entryPrice='101', markPrice='101')
        asyncio.run(observer.poll(None)); self.assertFalse(self.openings())
        self.send.assert_not_awaited()

    def test_timeout_runtime_invalid_snapshot_and_cancellation(self):
        observer = self.setup_observer()
        self.error = TimeoutError('private detail')
        with self.assertLogs(po.log, level='WARNING') as logs:
            self.assertFalse(asyncio.run(observer.poll(None)))
        self.assertNotIn('private detail', str(logs.output)); self.assertFalse(observer.baselined)
        self.error = None; self.positions.append(self.position())
        asyncio.run(observer.poll(None)); self.assertFalse(self.openings())
        self.error = RuntimeError('secret')
        self.assertFalse(asyncio.run(observer.poll(None)))
        self.error = None; self.positions[:] = [dict(symbol='broken')]
        self.assertFalse(asyncio.run(observer.poll(None)))
        self.assertEqual(1, self.rows('SELECT active FROM position_observer_state')[0]['active'])
        self.assertFalse(self.rows("SELECT * FROM position_observer_events WHERE event='CLOSE_OBSERVED'"))
        self.error = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError): asyncio.run(observer.poll(None))

    def test_request_deadline_and_failed_delivery_does_not_block_observation(self):
        observer = self.setup_observer()
        async def hang(*args): await asyncio.sleep(10)
        observer._request = hang
        with patch.object(po, 'REQUEST_TIMEOUT_S', .01):
            self.assertFalse(asyncio.run(observer.poll(None)))
        observer = self.factory()
        with patch.object(observer, 'deliver_pending', side_effect=RuntimeError('busy')):
            self.assertTrue(asyncio.run(observer.poll(None)))
        self.positions.append(self.position())
        self.send.return_value = False
        asyncio.run(observer.poll(None)); asyncio.run(observer.poll(None))
        self.assertEqual(1, len(self.openings()))
        self.assertEqual('FAILED', self.openings()[0]['notification_delivery'])


if __name__ == '__main__': unittest.main()
