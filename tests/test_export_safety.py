import asyncio
import ast
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'binance_momentum_bot'))
import research_export as ex
from export_safety import Policy, ExportSkipped, Safety, GIB


class ExportSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name)
        self.source=self.base/'signals.db'
        with closing(sqlite3.connect(self.source)) as c:
            c.execute('create table sample(id integer, value text)')
            c.execute("insert into sample values(1,'production')");c.commit()
        self.original=self.source.read_bytes()
        self.worker=ex.Exporter(self.source,self.base/'research_exports')
        self.root=self.worker.root
        self.disk=patch('export_safety.shutil.disk_usage',return_value=SimpleNamespace(free=90*GIB,used=10*GIB))
        self.disk.start();self.addCleanup(self.disk.stop)

    def snap(self,n=0):
        return self.worker.snapshot(1789800000000+n*3600000)

    def test_atomic_publish_and_source_unchanged(self):
        original=ex.inspect_snapshot
        def inspect(path,checkpoint):
            self.assertTrue(path.parent.name.startswith('.partial-'))
            self.assertFalse((self.root/'latest.json').exists())
            return original(path,checkpoint)
        with patch.object(ex,'inspect_snapshot',side_effect=inspect):
            directory,manifest=self.snap()
        self.assertTrue(manifest['valid'])
        self.assertEqual(directory,self.worker.latest()[0])
        self.assertEqual([],list(self.root.glob('.partial-*')))
        self.assertEqual(self.original,self.source.read_bytes())

    def test_retention_bounds_successes_and_preserves_source(self):
        for n in range(7):self.assertTrue(self.snap(n)[1]['valid'])
        self.assertEqual(3,len(list(self.root.glob('20*'))))
        self.assertEqual(self.original,self.source.read_bytes())

    def test_latest_summary_pointer_removed_before_pruning(self):
        directory,manifest=self.snap()
        ex.atomic_json(self.root/'latest-summary.json',{'snapshot_id':manifest['snapshot_id']})
        for n in range(1,4):self.snap(n)
        self.assertFalse(directory.exists())
        self.assertFalse((self.root/'latest-summary.json').exists())

    def test_failed_validation_never_publishes(self):
        _,good=self.snap()
        with patch.object(ex,'inspect_snapshot',return_value={'valid':False}):
            directory,bad=self.snap(1)
        self.assertFalse(bad['valid']);self.assertFalse(directory.exists())
        self.assertEqual(good['snapshot_id'],self.worker.latest()[1]['snapshot_id'])

    def test_disk_guard_skips_before_backup_or_partial_creation(self):
        with patch('export_safety.shutil.disk_usage',return_value=SimpleNamespace(free=8*GIB,used=90*GIB)),patch.object(ex,'inspect_snapshot') as inspect:
            directory,m=self.snap()
        self.assertEqual('MIN_FREE',m['skipped_export_reason'])
        inspect.assert_not_called();self.assertFalse(directory.exists())
        self.assertEqual(self.original,self.source.read_bytes())

    def test_budget_guard(self):
        with patch.object(self.worker.safety,'usage',return_value=20*GIB):
            _,m=self.snap()
        self.assertEqual('EXPORT_BUDGET',m['skipped_export_reason'])

    def test_space_loss_during_backup_is_isolated(self):
        with patch.object(self.worker.safety,'checkpoint',side_effect=ExportSkipped('MIN_FREE_DURING_EXPORT')):
            directory,m=self.snap()
        self.assertFalse(m['valid']);self.assertFalse(directory.exists())
        self.assertEqual(self.original,self.source.read_bytes())

    def test_restore_smoke_checks_reserve(self):
        def stop():raise ExportSkipped('MIN_FREE_DURING_EXPORT')
        with self.assertRaises((ExportSkipped,sqlite3.OperationalError)):
            ex.inspect_snapshot(self.source,stop)

    def test_stale_partial_and_legacy_incomplete_removed_fresh_preserved(self):
        for name in ['.partial-20260901T000000Z-aaaaaaaa','20260901T010000Z-bbbbbbbb','20260901T020000Z-cccccccc']:
            p=self.root/name;p.mkdir();(p/'signals.db').write_bytes(b'incomplete')
            if 'cccccccc' not in name:
                os.utime(p/'signals.db',(0,0));os.utime(p,(0,0))
        with self.worker.safety.locked():result=self.worker.safety.cleanup()
        self.assertEqual(2,result['incomplete_snapshot_count'])
        self.assertEqual(20,result['cleanup_bytes'])
        self.assertTrue((self.root/'20260901T020000Z-cccccccc').exists())

    def test_unknown_directory_and_unknown_files_preserved(self):
        for name in ['other-production','20260901T000000Z-aaaaaaaa']:
            p=self.root/name;p.mkdir();(p/'important.db').write_bytes(b'keep')
            os.utime(p/'important.db',(0,0));os.utime(p,(0,0))
        self.worker.safety.cleanup()
        self.assertEqual(2,len(list(self.root.glob('*/important.db'))))

    def test_source_ancestor_and_parent_are_rejected(self):
        for root in [self.source,self.base,self.base.parent]:
            with self.assertRaises(ValueError):ex.Exporter(self.source,root)

    def test_symlinks_cannot_escape_cleanup(self):
        linked=self.root/'20260901T000000Z-aaaaaaaa'
        try:linked.symlink_to(self.base,target_is_directory=True)
        except OSError as e:self.skipTest(str(e))
        self.worker.safety.cleanup()
        self.assertTrue(self.source.exists())
        with self.assertRaises(ValueError):self.worker.safety.remove(linked)
        linked.unlink()

    def test_hardlink_to_source_is_never_deleted(self):
        p=self.root/'20260901T000000Z-aaaaaaaa';p.mkdir()
        os.link(self.source,p/'signals.db')
        with self.assertRaises(ValueError):self.worker.safety.remove(p)
        self.assertEqual(self.original,self.source.read_bytes())

    def test_pointer_traversal_rejected(self):
        ex.atomic_json(self.root/'latest.json',{'snapshot_id':'..'})
        with self.assertRaises(ValueError):self.worker.latest()
        self.assertEqual('ERROR',self.worker.run_due()['status'])

    def test_full_disk_lease_error_does_not_escape_worker(self):
        with patch.object(ex.sqlite3,'connect',side_effect=sqlite3.OperationalError('disk full')):
            self.assertEqual('ERROR',self.worker.run_due()['status'])

    def test_scheduler_and_release_errors_do_not_escape(self):
        with patch.object(self.worker,'_run_due',side_effect=OSError('disk full')):
            self.assertEqual('ERROR',self.worker.run_due()['status'])
        with patch.object(self.worker,'_run_leased',side_effect=OSError('release failed')):
            self.assertEqual('ERROR',self.worker.run_due()['status'])

    def test_cross_process_lock_and_crash_release(self):
        code="from export_safety import Safety; from pathlib import Path; import sys; s=Safety(sys.argv[1],sys.argv[2]); c=s.locked(); c.__enter__(); print('LOCKED',flush=True); sys.stdin.read()"
        env=dict(os.environ,PYTHONPATH=str(Path(ex.__file__).parent))
        process=subprocess.Popen([sys.executable,'-u','-c',code,str(self.source),str(self.root)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,env=env)
        try:
            self.assertEqual('LOCKED',process.stdout.readline().strip())
            self.assertEqual('BUSY',self.worker.run_due()['status'])
        finally:
            process.kill();process.wait();process.stdin.close();process.stdout.close()
        with self.worker.safety.locked():pass

    def test_async_bot_task_and_db_writer_survive_export_failure(self):
        async def exercise():
            async def writer():
                with closing(sqlite3.connect(self.source)) as c:
                    c.execute("insert into sample values(2,'still running')");c.commit()
                return 'alive'
            with patch.object(self.worker,'_run_due',side_effect=OSError('disk full')):
                result,alive=await asyncio.gather(asyncio.to_thread(self.worker.run_due),writer())
            self.assertEqual('ERROR',result['status']);self.assertEqual('alive',alive)
        asyncio.run(exercise())
        with closing(sqlite3.connect(self.source)) as c:self.assertEqual(2,c.execute('select count(*) from sample').fetchone()[0])

    def test_bundle_disk_guard_and_atomic_output(self):
        directory,_=self.snap()
        with patch.object(self.worker.safety,'preflight',side_effect=ExportSkipped('MIN_FREE')):
            with self.assertRaises(ExportSkipped):self.worker.download_bundle(include_db=True)
        self.assertFalse((directory/'export-full.zip').exists())
        bundle=self.worker.download_bundle(include_db=True)
        self.assertTrue(bundle.exists());self.assertFalse(bundle.with_name(bundle.name+'.partial').exists())

    def test_invalid_policy_fails_closed(self):
        for key,value in [('RESEARCH_EXPORT_RETAIN','0'),('RESEARCH_EXPORT_MIN_FREE_BYTES','1'),('RESEARCH_EXPORT_TIMEOUT_SECONDS','900')]:
            with patch.dict(os.environ,{key:value}):
                with self.assertRaises(ValueError):Policy.from_env()

    def test_dry_run_does_not_delete_or_change_pointers(self):
        self.worker.safety.policy=Policy(retain=6)
        for n in range(5):self.snap(n)
        before={str(p):p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        self.worker.safety.policy=Policy()
        result=self.worker.safety.cleanup(dry_run=True)
        self.assertGreater(result['cleanup_bytes'],0)
        self.assertEqual(before,{str(p):p.read_bytes() for p in self.root.rglob('*') if p.is_file()})

    def test_startup_launch_failure_continues(self):
        path=Path(ex.__file__).parent/'startup.py'
        tree=ast.parse(path.read_text(encoding='utf-8'))
        main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
        node=next(n for n in main.body if isinstance(n,ast.If) and ast.unparse(n.test)=='export_enabled')
        def fail(*args,**kwargs):raise OSError('cannot spawn exporter')
        scope=dict(export_enabled=True,os=os,sys=sys,subprocess=SimpleNamespace(Popen=fail),json=json,target=self.source)
        with patch('builtins.print') as log:
            exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
        self.assertTrue(json.loads(log.call_args.args[0])['production_continues'])

    def test_bot_exporter_initialization_failure_continues(self):
        path=Path(ex.__file__).parent/'bot.py'
        tree=ast.parse(path.read_text(encoding='utf-8'))
        main=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='main')
        node=next(n for n in main.body if isinstance(n,ast.If) and ast.unparse(n.test)=='RESEARCH_EXPORT_ENABLED')
        def fail(*args,**kwargs):raise OSError('disk full')
        scope=dict(RESEARCH_EXPORT_ENABLED=True,research_export=SimpleNamespace(Exporter=fail),DB_PATH=str(self.source),
                   os=os,BOT_VERSION='test',alt_shadow=SimpleNamespace(MODELS={}),
                   audit=SimpleNamespace(DRY_FEE_PCT=0,DRY_SLIPPAGE_PCT=0),ALT_SHADOW_ENABLED=False,
                   log=SimpleNamespace(error=lambda *a:None))
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
        self.assertIsNone(scope['export_worker'])


if __name__=='__main__':unittest.main()
