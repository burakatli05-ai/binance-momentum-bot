import ast
import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock,MagicMock,patch
import zipfile

import test_v5135 as fixtures
import research_export as exports

bot=fixtures.bot


class LatestExportCase(unittest.TestCase):
    setUp=fixtures.DatabaseCase.setUp

    def snapshot(self):
        worker=exports.Exporter(bot.DB_PATH,Path(self.tmp.name)/'exports')
        directory,manifest=worker.snapshot(1800000000000)
        self.assertTrue(manifest['valid'])
        return worker,directory,manifest

    def test_small_and_full_members_and_metadata(self):
        worker,directory,manifest=self.snapshot()
        exports.atomic_json(directory/'summary.json',{'known':'summary'})
        info={'snapshot_id':manifest['snapshot_id'],'created_time_ms':123}
        exports.atomic_json(worker.root/'latest-summary.json',info)
        original=(directory/'signals.db').read_bytes()
        for full in (False,True):
            with zipfile.ZipFile(worker.download_bundle(include_db=full)) as z:
                self.assertEqual({'manifest.json','summary.json','metadata.json'}|({'signals.db'} if full else set()),set(z.namelist()))
                self.assertEqual({'known':'summary'},json.loads(z.read('summary.json')))
                meta=json.loads(z.read('metadata.json'))
                self.assertEqual(full,meta['includes_database'])
                self.assertEqual(info,meta['summary_source'])
                self.assertEqual(manifest['sha256'],meta['snapshot_sha256'])
                if full:self.assertEqual(original,z.read('signals.db'))
        self.assertEqual(original,(directory/'signals.db').read_bytes())

    def test_default_small_missing_summary_is_explicit(self):
        worker,_,_=self.snapshot()
        with zipfile.ZipFile(worker.download_bundle()) as z:
            self.assertNotIn('signals.db',z.namelist())
            self.assertEqual('NO_SUMMARY_AVAILABLE',json.loads(z.read('summary.json'))['status'])
            self.assertIsNone(json.loads(z.read('metadata.json'))['summary_source'])

    def test_both_bundles_still_reject_corrupt_snapshot(self):
        worker,directory,_=self.snapshot()
        with (directory/'signals.db').open('ab') as f:f.write(b'corrupted')
        for full in (False,True):
            with self.assertRaisesRegex(ValueError,'hash mismatch'):worker.download_bundle(include_db=full)

    def test_admin_only_and_route_modes(self):
        worker=MagicMock();worker.download_bundle.return_value=Path(self.tmp.name)/'bundle.zip'
        with patch.object(bot,'export_worker',worker),patch.object(bot,'telegram_send_document',new_callable=AsyncMock) as send:
            with patch.object(bot,'_at_admin_allowed',return_value=False):
                for cmd in ('/latestexport','/latestexportfull'):asyncio.run(bot._at_command(None,cmd,'bad','bad'))
            worker.download_bundle.assert_not_called();send.assert_not_awaited()
            with patch.object(bot,'_at_admin_allowed',return_value=True):
                for cmd,full in (('/latestexport',False),('/latestexportfull',True)):
                    asyncio.run(bot._at_command(None,cmd,'admin','user'))
                    worker.download_bundle.assert_called_with(include_db=full)
                    self.assertEqual('admin',send.await_args.kwargs['chat_id'])

    def test_bundle_failure_not_silent(self):
        worker=MagicMock();worker.download_bundle.side_effect=ValueError('private detail')
        with patch.object(bot,'export_worker',worker),patch.object(bot,'_at_admin_allowed',return_value=True),patch.object(bot,'telegram_send',new_callable=AsyncMock) as send:
            asyncio.run(bot._at_command(None,'/latestexportfull','admin','user'))
            self.assertIn('paketi hazırlanamadı',send.await_args.args[1]);self.assertNotIn('private detail',send.await_args.args[1])

    def document(self,session,size=4,limit=50_000_000):
        file=Path(self.tmp.name)/'document.zip';file.write_bytes(b'x'*size)
        with patch.object(bot,'TELEGRAM_BOT_TOKEN','test-token'),patch.object(bot,'TELEGRAM_DOCUMENT_MAX_BYTES',limit),patch.object(bot,'telegram_send',new_callable=AsyncMock) as notify:
            ok=asyncio.run(bot.telegram_send_document(session,str(file),chat_id='admin'))
            return ok,notify

    def response(self,status=200,body=None,error=None):
        response=SimpleNamespace(status=status,json=AsyncMock(return_value={'ok':True} if body is None else body,side_effect=error))
        context=MagicMock();context.__aenter__=AsyncMock(return_value=response);context.__aexit__=AsyncMock(return_value=False)
        return SimpleNamespace(post=MagicMock(return_value=context))

    def test_oversize_skips_network_and_reports_bytes_limit(self):
        session=self.response();ok,notify=self.document(session,size=5,limit=4)
        self.assertFalse(ok);session.post.assert_not_called()
        self.assertIn('5 bayt',notify.await_args.args[1]);self.assertIn('Telegram dosya limiti',notify.await_args.args[1])
        self.assertEqual('admin',notify.await_args.kwargs['chat_id'])

    def test_exact_limit_and_ok_response_succeed(self):
        session=self.response();ok,notify=self.document(session,size=4,limit=4)
        self.assertTrue(ok);notify.assert_not_awaited();session.post.assert_called_once()

    def test_http_failure_false_ok_and_network_error_notify(self):
        for session in (self.response(413,{'ok':False}),self.response(200,{'ok':False}),self.response(error=TimeoutError())):
            with self.subTest(session=session):
                ok,notify=self.document(session)
                self.assertFalse(ok);self.assertIn('Dosya gönderilemedi',notify.await_args.args[1])

    def test_missing_file_and_notification_failure_are_logged(self):
        with patch.object(bot,'TELEGRAM_BOT_TOKEN','test-token'),patch.object(bot,'telegram_send',new_callable=AsyncMock,side_effect=RuntimeError()),patch.object(bot.log,'error') as log:
            self.assertFalse(asyncio.run(bot.telegram_send_document(None,str(Path(self.tmp.name)/'absent'),chat_id='admin')))
            self.assertEqual(2,log.call_count)

    def test_export_snapshot_scheduler_and_sha_unchanged(self):
        source=ast.parse(Path(exports.__file__).read_text(encoding='utf-8'))
        nodes={n.name:n for n in source.body if isinstance(n,ast.FunctionDef)}
        cls=next(n for n in source.body if isinstance(n,ast.ClassDef) and n.name=='Exporter')
        nodes.update({n.name:n for n in cls.body if isinstance(n,ast.FunctionDef)})
        expected=json.loads((Path(__file__).parent/'latestexport_baseline.json').read_text())
        for name,digest in expected.items():
            self.assertEqual(digest,hashlib.sha256(ast.dump(nodes[name],include_attributes=False).encode()).hexdigest(),name)
