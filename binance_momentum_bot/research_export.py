"""SQLite-safe export and repository scheduler. Never starts or repairs the bot DB."""
import argparse
import asyncio
import re
import stat
import zlib
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import time
import tempfile
import uuid
import zipfile

from export_safety import Safety, ExportSkipped, serialized, telemetry, no_links

import assistant_bridge
from research_reports import summary

IST=timezone(timedelta(hours=3))


def atomic_json(path,value):
    no_links(path)
    temp=path.with_name(path.name+'.tmp')
    no_links(temp)
    if temp.exists() and temp.stat().st_nlink != 1:raise ValueError('linked temporary file')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    os.replace(temp,path)


def readonly(path):
    return sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)


def inspect_snapshot(path, checkpoint=lambda: None):
    with closing(readonly(path)) as c:
        c.set_progress_handler(lambda: (checkpoint() or 0), 10000)
        checkpoint()
        quick=[r[0] for r in c.execute('PRAGMA quick_check')]
        integrity=[r[0] for r in c.execute('PRAGMA integrity_check')]
        tables={}
        for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall():
            q='"'+name.replace('"','""')+'"'
            ranges={}
            for field in [r[1] for r in c.execute(f'PRAGMA table_info({q})')]:
                if field=='ts' or field.endswith(('_ts','_ms','_time')):
                    f='"'+field.replace('"','""')+'"'
                    ranges[field]=c.execute(f'SELECT MIN({f}),MAX({f}) FROM {q}').fetchone()
            tables[name]={'rows':c.execute(f'SELECT COUNT(*) FROM {q}').fetchone()[0],'timestamp_ranges':ranges}
        # Isolated disk destination avoids duplicating a large production DB in RAM.
        with tempfile.TemporaryDirectory(prefix='restore-smoke-',dir=Path(path).parent) as temporary:
            with closing(sqlite3.connect(Path(temporary)/'restored.db')) as restored:
                c.backup(restored,pages=512,progress=lambda *args: checkpoint(),sleep=.05)
                restored.set_progress_handler(lambda: (checkpoint() or 0), 10000)
                smoke=[r[0] for r in restored.execute('PRAGMA quick_check')]
        return dict(quick_check=quick,integrity_check=integrity,restore_smoke_check=smoke,
                    tables=tables,table_count=len(tables),valid=quick==integrity==smoke==['ok'])


def watermarks(c):
    queries={
        'premium':"SELECT COUNT(*),MAX(id) FROM signals_v2 WHERE level='CONFIRMED'",
        'current':'SELECT COUNT(*),MAX(ts) FROM signal_outcomes',
        'dry':"SELECT COUNT(*),MAX(closed_ts_ms) FROM autotrade_trades WHERE mode='DRY' AND closed_ts_ms IS NOT NULL",
        'ALT_WIDE_60':"SELECT COUNT(*),MAX(terminal_ts) FROM alt_research_trades WHERE model_name='ALT_WIDE_60' AND lifecycle_status='CLOSED'",
        'ALT_CONTROL_60':"SELECT COUNT(*),MAX(terminal_ts) FROM alt_research_trades WHERE model_name='ALT_CONTROL_60' AND lifecycle_status='CLOSED'",
        'causal':"SELECT COUNT(*),MAX(first_event_time_ms) FROM causal_cohorts WHERE net_pnl_pct IS NOT NULL"}
    return {k:list(c.execute(sql).fetchone()) for k,sql in queries.items()}


class Exporter:
    def __init__(self,source,root,version='5.13.5+alt-shadow',deployment='UNKNOWN',config=None,policy=None):
        self.safety=Safety(source,root,policy)
        self.source,self.root=self.safety.source,self.safety.root
        self.version,self.deployment=version,deployment
        # Callers pass a whitelist of public research settings only. Never serialize environment.
        self.config=config or {}
        telemetry('policy',retain=self.safety.policy.retain,min_free_bytes=self.safety.policy.min_free,
                  budget_bytes=self.safety.policy.budget,stale_seconds=self.safety.policy.stale_seconds)

    def state(self):
        p=no_links(self.root/'scheduler.json')
        return json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}

    @serialized
    def latest(self):
        p=no_links(self.root/'latest.json')
        if not p.exists():return None
        pointer=json.loads(p.read_text(encoding='utf-8'))
        directory=no_links(self.root/pointer['snapshot_id']).resolve()
        if directory.parent!=self.root:raise ValueError('invalid snapshot pointer')
        self.safety.owned_files(directory)
        manifest=json.loads((directory/'manifest.json').read_text(encoding='utf-8'))
        if not manifest['valid']:raise ValueError('invalid snapshot')
        with (directory/'signals.db').open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
        if digest!=manifest['sha256']:raise ValueError('snapshot hash mismatch')
        return directory,manifest

    @serialized
    def download_bundle(self, *, include_db=False):
        latest=self.latest()
        if not latest:return None
        directory,manifest=latest
        bundle=directory/('export-full.zip' if include_db else 'export-small.zip')
        self.safety.preflight((directory/'signals.db').stat().st_size+64*1024**2 if include_db else 64*1024**2)
        deadline=time.monotonic()+self.safety.policy.timeout
        temporary=bundle.with_name(bundle.name+'.partial')
        no_links(temporary)
        no_links(bundle)
        def write(z,path,name):
            with path.open('rb') as src,z.open(name,'w') as dst:
                while chunk:=src.read(1024**2):
                    self.safety.checkpoint(deadline)
                    dst.write(chunk)
        metadata={'snapshot_id':manifest['snapshot_id'],'snapshot_created_time_ms':manifest['created_time_ms'],
                  'snapshot_sha256':manifest['sha256'],'includes_database':include_db,'summary_source':None}
        with zipfile.ZipFile(temporary,'w',zipfile.ZIP_DEFLATED) as z:
            if include_db:write(z,directory/'signals.db','signals.db')
            write(z,directory/'manifest.json','manifest.json')
            pointer=self.root/'latest-summary.json'
            if pointer.exists():
                info=json.loads(pointer.read_text(encoding='utf-8'))
                summary_dir=no_links(self.root/info['snapshot_id']).resolve()
                if summary_dir.parent!=self.root:raise ValueError('invalid summary pointer')
                self.safety.owned_files(summary_dir)
                write(z,summary_dir/'summary.json','summary.json')
                metadata['summary_source']=info
            else:
                z.writestr('summary.json',json.dumps({'status':'NO_SUMMARY_AVAILABLE'}))
            z.writestr('metadata.json',json.dumps(metadata,ensure_ascii=False,indent=2))
        os.replace(temporary,bundle)
        return bundle

    @serialized
    def snapshot(self,now):
        sid=datetime.fromtimestamp(now/1000,timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
        directory=self.root/('.partial-'+sid)
        final=self.root/sid
        dest=directory/'signals.db'
        manifest={'snapshot_id':sid,'created_time_ms':now,'source_db_path':str(self.source),
                  'version':self.version,'deployment_id':self.deployment,
                  'config_identity':hashlib.sha256(json.dumps(self.config,sort_keys=True).encode()).hexdigest(),'valid':False}
        try:
            self.safety.cleanup()
            with closing(readonly(self.source)) as src:
                page_size=src.execute('PRAGMA page_size').fetchone()[0]
                size=src.execute('PRAGMA page_count').fetchone()[0]*page_size
            # DB + restore-smoke copy + growth/WAL/zip allowance; no source write.
            estimated=max(size,self.source.stat().st_size)*3+256*1024**2
            self.safety.preflight(estimated)
            deadline=time.monotonic()+self.safety.policy.timeout
            def checkpoint():self.safety.checkpoint(deadline)
            directory.mkdir()
            with closing(readonly(self.source)) as src,closing(sqlite3.connect(dest)) as target:
                def progress(status,remaining,total):
                    checkpoint()
                    if total*page_size > estimated//3:
                        raise ExportSkipped('SOURCE_GREW_BEYOND_ESTIMATE')
                src.backup(target,pages=512,progress=progress,sleep=.05)
            manifest.update(inspect_snapshot(dest,checkpoint))
            if not manifest['valid']:raise ValueError('snapshot integrity failed')
            checkpoint()
            with dest.open('rb') as f:manifest['sha256']=hashlib.file_digest(f,'sha256').hexdigest()
            manifest['size_bytes']=dest.stat().st_size
            old=self.latest()
            manifest['unchanged']=bool(old and old[1]['sha256']==manifest['sha256'])
            atomic_json(directory/'manifest.json',manifest)
            checkpoint()
            os.replace(directory,final)
            atomic_json(self.root/'latest.json',{'snapshot_id':sid})
            self.safety.cleanup()
            telemetry('snapshot_complete',snapshot_id=sid,size_bytes=manifest['size_bytes'])
            return final,manifest
        except Exception as exc:
            manifest.update(valid=False,error=type(exc).__name__)
            if isinstance(exc,ExportSkipped):manifest['skipped_export_reason']=str(exc)
            telemetry('snapshot_failed',snapshot_id=sid,error=type(exc).__name__,
                      skipped_export_reason=manifest.get('skipped_export_reason'))
            if directory.exists():
                try:self.safety.remove(directory)
                except Exception as cleanup_error:
                    telemetry('partial_cleanup_failed',snapshot_id=sid,error=type(cleanup_error).__name__)
            return directory,manifest


    def _premium_area(self, *, reserve=False):
        """Separate bounded namespace; never delete an existing archive."""
        self.safety.validate_root()
        area = no_links(self.root/'premium-files')
        if area.exists():
            if not area.is_dir() or area.stat().st_dev != self.safety.identity[0]:
                raise ValueError('PREMIUM_AREA_INVALID')
        else:
            area.mkdir()
        entries = list(area.iterdir())
        for entry in entries:
            no_links(entry)
            if not entry.is_dir() or entry.stat().st_dev != self.safety.identity[0]:
                raise ValueError('PREMIUM_AREA_UNKNOWN_CONTENT')
            if not re.fullmatch(r'(?:\.partial-)?premium-\d{8}T\d{6}Z-[0-9a-f]{8}', entry.name):
                raise ValueError('PREMIUM_AREA_UNKNOWN_CONTENT')
        if reserve and len(entries) >= self.safety.policy.retain:
            raise ExportSkipped('PREMIUM_CAPACITY')
        return area

    def _premium_regular(self, path):
        no_links(path)
        st = path.stat()
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_dev != self.safety.identity[0]:
            raise ValueError('PREMIUM_LINK_OR_FILE')
        return st

    def _verify_premium_bundle(self, bundle, checkpoint):
        """Bounded independent disk read; no source DB access."""
        size = self._premium_regular(bundle).st_size
        if size > assistant_bridge.MAX_RAW_BYTES + 2*1024**2:
            raise ValueError('PREMIUM_FILE_LIMIT')
        checkpoint()
        with zipfile.ZipFile(bundle) as archive:
            infos = archive.infolist()
            if (len(infos) != 2 or {i.filename for i in infos} != {'payload.json.zlib','manifest.json'}
                    or any(i.compress_type != zipfile.ZIP_STORED for i in infos)):
                raise ValueError('PREMIUM_FILE_MEMBERS')
            info = archive.getinfo('manifest.json')
            if info.file_size > 1024**2:
                raise ValueError('PREMIUM_MANIFEST_LIMIT')
            meta = json.loads(archive.read(info))
            info = archive.getinfo('payload.json.zlib')
            if info.file_size > assistant_bridge.MAX_RAW_BYTES + 65536:
                raise ValueError('PREMIUM_COMPRESSED_LIMIT')
            compressed = archive.read(info)
        checkpoint()
        decoder = zlib.decompressobj()
        raw = decoder.decompress(compressed, assistant_bridge.MAX_RAW_BYTES+1)
        if len(raw) > assistant_bridge.MAX_RAW_BYTES:
            raise ValueError('PREMIUM_BYTE_LIMIT')
        if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise ValueError('PREMIUM_COMPRESSED_INVALID')
        digest = hashlib.sha256(raw).hexdigest()
        payload = json.loads(raw)
        assistant_bridge.validate_live_payload(payload)
        if (meta.get('payload_sha256') != digest or meta.get('raw_bytes') != len(raw)
                or meta.get('compressed_bytes') != len(compressed)
                or meta.get('snapshot_id') != payload['snapshot_id']
                or meta.get('schema_sha256') != payload['logical_export']['schema_sha256']
                or meta.get('source_read_at_ms') != payload['logical_export']['read_at_ms']
                or meta.get('source_read_completed_at_ms') != payload['logical_export']['read_completed_at_ms']
                or meta.get('table_rows') != {k:len(v['rows']) for k,v in payload['tables'].items()}
                or meta.get('premium_count') != payload['logical_export']['premium_count']):
            raise ValueError('PREMIUM_FILE_MANIFEST')
        checkpoint()
        with bundle.open('rb') as handle:
            file_hash = hashlib.file_digest(handle,'sha256').hexdigest()
        checkpoint()
        return meta, raw, file_hash, size

    def _store_premium_file(self, payload, raw, compressed, digest, checkpoint):
        """Persist exactly the bytes already reconciled by emit_live, once."""
        checkpoint()
        if len(raw) > assistant_bridge.MAX_RAW_BYTES or len(compressed) > assistant_bridge.MAX_RAW_BYTES+65536:
            raise ValueError('PREMIUM_BYTE_LIMIT')
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError('PREMIUM_PAYLOAD_HASH')
        area = self._premium_area(reserve=True)
        sid = payload['snapshot_id']
        if not isinstance(sid,str) or not re.fullmatch(r'premium-\d{8}T\d{6}Z-[0-9a-f]{8}',sid):
            raise ValueError('PREMIUM_FILE_ID')
        directory = no_links(area/('.partial-'+sid))
        final = no_links(area/sid)
        pointer = no_links(self.root/'premium-latest.json')
        temporary = no_links(self.root/'premium-latest.json.tmp')
        if directory.exists() or final.exists() or temporary.exists():
            raise ExportSkipped('PREMIUM_STAGING_EXISTS')
        if pointer.exists():self._premium_regular(pointer)
        ranges = {}
        for table,data in payload['tables'].items():
            ranges[table] = {}
            for i,column in enumerate(data['columns']):
                if column == 'ts' or column.endswith(('_ts','_ms','_time')):
                    values = [row[i] for row in data['rows'] if row[i] is not None]
                    numeric = [v for v in values if type(v) in (int,float)]
                    ranges[table][column] = {
                        'min':min(numeric) if numeric else None,
                        'max':max(numeric) if numeric else None,
                        'null_count':len(data['rows'])-len(values),
                        'nonnumeric_count':len(values)-len(numeric),
                        'unit':'AS_RECORDED; see payload schema and source contract'}
            checkpoint()
        logical = payload['logical_export']
        meta = dict(format='premium-file-v1', snapshot_id=sid,
                    file_generated_at_ms=int(time.time()*1000),
                    source_read_at_ms=logical['read_at_ms'],
                    source_read_completed_at_ms=logical['read_completed_at_ms'],
                    payload_sha256=digest, raw_bytes=len(raw),compressed_bytes=len(compressed),
                    schema_sha256=logical['schema_sha256'],premium_count=logical['premium_count'],
                    table_rows={k:len(v['rows']) for k,v in payload['tables'].items()},
                    recorded_time_ranges=ranges, source_identity=payload.get('source_identity'),
                    deployment_id=payload.get('deployment_id'),
                    source_kind=logical['kind'],logical_not_restorable=True,
                    as_of='NOT_PROVEN; recorded context/outcomes are not decision-time attestations',
                    capacity='No automatic archive deletion; occupied generations count against existing retain policy',
                    pointer_durability='Atomic visibility; power-loss persistence not attested')
        encoded = assistant_bridge._json_bytes(meta)
        if len(encoded)>1024**2:raise ValueError('PREMIUM_MANIFEST_LIMIT')
        created_pointer_temp = False
        directory.mkdir()
        try:
            bundle = directory/'premium.zip'
            with bundle.open('xb') as handle:
                with zipfile.ZipFile(handle,'w',zipfile.ZIP_STORED) as archive:
                    archive.writestr('payload.json.zlib',compressed)
                    checkpoint()
                    archive.writestr('manifest.json',encoded)
                handle.flush();os.fsync(handle.fileno())
            with (directory/'manifest.json').open('xb') as handle:
                handle.write(encoded);handle.flush();os.fsync(handle.fileno())
            actual, recovered, file_hash, size = self._verify_premium_bundle(bundle,checkpoint)
            sidecar=directory/'manifest.json'
            self._premium_regular(sidecar)
            if recovered != raw or actual != meta or sidecar.read_bytes() != encoded:
                raise ValueError('PREMIUM_FILE_ROUNDTRIP')
            checkpoint()
            os.replace(directory,final)
            # Files are immutable after publication; downloads need no copy/delete.
            # This does not change latest.json (the physical snapshot pointer).
            info = dict(format='premium-file-pointer-v1',snapshot_id=sid,
                        file_name='premium.zip',file_sha256=file_hash,file_bytes=size,
                        payload_sha256=digest,manifest_sha256=hashlib.sha256(encoded).hexdigest(),
                        file_verified_at_ms=int(time.time()*1000))
            checkpoint()
            with temporary.open('xb') as handle:
                created_pointer_temp=True
                handle.write(assistant_bridge._json_bytes(info))
                handle.flush();os.fsync(handle.fileno())
            checkpoint()
            os.replace(temporary,pointer)  # commit only after readback; last fallible write
            return info
        finally:
            # Only this attempt's unfinished files; never a published generation.
            if directory.exists():
                for name in ('premium.zip','manifest.json'):
                    p=directory/name
                    if p.exists():
                        self._premium_regular(p);p.unlink()
                directory.rmdir()
            if created_pointer_temp and temporary.exists():
                self._premium_regular(temporary);temporary.unlink()

    @serialized
    def latest_premium_file(self):
        pointer = no_links(self.root/'premium-latest.json')
        if not pointer.exists():return None
        if self._premium_regular(pointer).st_size>4096:
            raise ValueError('PREMIUM_POINTER_LIMIT')
        info=json.loads(pointer.read_text(encoding='utf8'))
        sid=info.get('snapshot_id')
        if (info.get('format')!='premium-file-pointer-v1' or info.get('file_name')!='premium.zip'
                or not isinstance(sid,str) or not re.fullmatch(r'premium-\d{8}T\d{6}Z-[0-9a-f]{8}',sid)):
            raise ValueError('PREMIUM_POINTER_INVALID')
        area=self._premium_area()
        directory=no_links(area/sid)
        if directory.resolve().parent!=area:raise ValueError('PREMIUM_POINTER_PATH')
        manifest_path=directory/'manifest.json'
        if self._premium_regular(manifest_path).st_size>1024**2:
            raise ValueError('PREMIUM_MANIFEST_LIMIT')
        encoded=manifest_path.read_bytes()
        if hashlib.sha256(encoded).hexdigest()!=info['manifest_sha256']:
            raise ValueError('PREMIUM_MANIFEST_HASH')
        deadline=time.monotonic()+min(self.safety.policy.timeout,30)
        def checkpoint():self.safety.checkpoint(deadline)
        bundle=directory/'premium.zip'
        meta,raw,digest,size=self._verify_premium_bundle(bundle,checkpoint)
        if (meta!=json.loads(encoded) or meta['snapshot_id']!=sid or digest!=info['file_sha256']
                or size!=info['file_bytes'] or meta['payload_sha256']!=info['payload_sha256']):
            raise ValueError('PREMIUM_FILE_HASH')
        return bundle,meta,info

    @serialized
    def premium_bridge(self,now,printer=print):
        """Bounded logical Premium cut; independent of restorable DB snapshots."""
        sid='premium-'+datetime.fromtimestamp(now/1000,timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
        file_info=None
        file_error=None
        file_status='NOT_PUBLISHED'
        def persist(payload,raw,compressed,digest,check):
            nonlocal file_info,file_error,file_status
            guard_error=None
            def checkpoint():
                nonlocal guard_error
                try:check()
                except Exception as exc:
                    guard_error=exc
                    raise
            try:
                file_info=self._store_premium_file(payload,raw,compressed,digest,checkpoint)
                file_status='VERIFIED_PUBLISHED'
            except (ExportSkipped,OSError,ValueError,zipfile.BadZipFile,zlib.error) as exc:
                if guard_error is not None:raise guard_error
                if isinstance(exc,ExportSkipped) and str(exc) not in ('PREMIUM_CAPACITY','PREMIUM_STAGING_EXISTS'):
                    raise
                if isinstance(exc,ValueError) and str(exc) in ('PREMIUM_TIME_LIMIT','PREMIUM_ROW_LIMIT','PREMIUM_BYTE_LIMIT','export root changed'):
                    raise
                # Only the extra file publication may fail independently;
                # the existing shared guard still gates legacy emit.
                file_status='SKIPPED' if isinstance(exc,ExportSkipped) else 'ERROR'
                file_error={'type':type(exc).__name__,'reason':str(exc)}
                check()
        try:
            # Reserve only the bounded bridge allowance, never the full DB size.
            # The same disk/budget/checkpoint policy and worker lock still apply.
            self.safety.preflight(64*1024**2)
            deadline=time.monotonic()+min(self.safety.policy.timeout,30)
            def checkpoint():self.safety.checkpoint(deadline)
            checkpoint()
            code_hashes={}
            for name in ('research_export.py','assistant_bridge.py','analysis_export.py','export_safety.py'):
                with Path(__file__).with_name(name).open('rb') as source:
                    code_hashes[name]=hashlib.file_digest(source,'sha256').hexdigest()
            manifest={'snapshot_id':sid,'created_time_ms':now,'deployment_id':self.deployment,
                      'version':self.version,'source_code_sha256':code_hashes,
                      'config_identity':hashlib.sha256(json.dumps(self.config,sort_keys=True).encode()).hexdigest()}
            meta=assistant_bridge.emit_live(self.source,manifest,printer=printer,checkpoint=checkpoint,
                                            persist=persist)
            telemetry('premium_bridge_complete',snapshot_id=sid,sha256=meta['sha256'],raw_bytes=meta['raw_bytes'])
            return dict(meta,status='VALID',file_status=file_status,file_info=file_info,file_error=file_error,
                        log_status='COMPLETE')
        except Exception as exc:
            result={'status':'ERROR','error':type(exc).__name__,
                    'file_status':file_status,'file_info':file_info,'file_error':file_error,
                    'log_status':'INCOMPLETE_OR_NOT_RUN'}
            if isinstance(exc,ExportSkipped):
                result.update(status='SKIPPED',skipped_export_reason=str(exc))
            telemetry('premium_bridge_failed',snapshot_id=sid,**result)
            return result

    def run_due(self,now=None):
        # Includes lock/lease acquisition, metadata writes, reporting and lease release.
        try:
            with self.safety.locked():return self._run_leased(now)
        except ExportSkipped as exc:
            telemetry('skipped',skipped_export_reason=str(exc))
            return {'status':'BUSY' if str(exc)=='BUSY' else 'SKIPPED','skipped_export_reason':str(exc)}
        except Exception as exc:
            telemetry('worker_error',error=type(exc).__name__)
            return {'status':'ERROR','error':type(exc).__name__}

    def _run_leased(self,now=None):
        """Single worker; durable pre-claim avoids duplicate hourly attempts after restart."""
        now=int(time.time()*1000) if now is None else now
        # Cross-process lease is a separate DB, never the source. Release on crash via timeout.
        no_links(self.root/'lease.sqlite')
        with closing(sqlite3.connect(self.root/'lease.sqlite',timeout=1)) as lock,lock:
            lock.execute('CREATE TABLE IF NOT EXISTS lease(id INTEGER PRIMARY KEY,until_ms INTEGER)')
            lock.execute('BEGIN IMMEDIATE')
            row=lock.execute('SELECT until_ms FROM lease WHERE id=1').fetchone()
            if row and row[0]>now:return {'status':'BUSY'}
            lock.execute('INSERT OR REPLACE INTO lease VALUES(1,?)',(now+600000,))
        try:return self._run_due(now)
        finally:
            with closing(sqlite3.connect(self.root/'lease.sqlite')) as lock,lock:
                lock.execute('UPDATE lease SET until_ms=0 WHERE id=1')

    def _run_due(self,now):
        state=self.state();result={'status':'NOT_DUE'}
        if now-state.get('last_hourly_attempt',-3600000)>=3600000:
            state['last_hourly_attempt']=now;atomic_json(self.root/'scheduler.json',state)
            # This cut uses the existing hourly claim and runs before the full
            # snapshot. Neither snapshot success nor its summaries gate it.
            bridge_result=self.premium_bridge(now)
            state['premium_bridge_last_attempt_ms']=now
            state['premium_bridge_last_result']=bridge_result
            if bridge_result['status']=='VALID':
                state['premium_bridge_last_success']={'created_time_ms':now,'sha256':bridge_result['sha256']}
            atomic_json(self.root/'scheduler.json',state)
            directory,manifest=self.snapshot(now)
            result={'status':'VALID' if manifest['valid'] else 'INVALID','snapshot_id':manifest['snapshot_id']}
            result['assistant_bridge']=bridge_result
            if manifest.get('skipped_export_reason'):
                result.update(status='SKIPPED',skipped_export_reason=manifest['skipped_export_reason'])
            if manifest['valid']:
                with closing(readonly(directory/'signals.db')) as c:
                    marks=watermarks(c);old=state.get('watermarks',{})
                    new={k:max(0,v[0]-old.get(k,[0])[0]) for k,v in marks.items()}
                    mature=any(new[k] for k in new if k!='premium')
                    if not manifest['unchanged'] and mature:
                        payload={'new_counts':new,'latest_watermarks':marks,'integrity':manifest['integrity_check'],'summary':summary(c)}
                        atomic_json(directory/'summary.json',payload)
                        atomic_json(self.root/'latest-summary.json',{'snapshot_id':manifest['snapshot_id'],'created_time_ms':now})
                        result['summary']='NEW_MATURE_DATA'
                    else:result['summary']='UNCHANGED' if manifest['unchanged'] else 'NO_NEW_MATURE_DATA'
                    state['watermarks']=marks
        local=datetime.fromtimestamp(now/1000,IST);date=local.date().isoformat()
        if local.hour>=22 and state.get('daily_date')!=date:
            latest=self.latest()
            if latest:
                directory,manifest=latest
                # Report the available valid snapshot, clearly disclose its cutoff.
                start=int(local.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1000)
                with closing(readonly(directory/'signals.db')) as c:
                    payload={'local_date':date,'generated_time_ms':now,'snapshot':manifest,'report':summary(c,start,now)}
                atomic_json(self.root/f'daily-{date}.json',payload)
                state['daily_date']=date;result['daily']=date
        atomic_json(self.root/'scheduler.json',state)
        return result



async def handle_premium_download(bot, session, chat_id, user_id):
    """Select an existing verified file; never trigger a source read or export."""
    if not bot['_at_admin_allowed'](chat_id,user_id):
        return True
    if not str(chat_id).isdigit() or int(chat_id)<=0 or str(chat_id)!=str(user_id):
        return True
    async def notify(message):
        try:
            return await bot['telegram_send'](session,message,chat_id=chat_id)
        except Exception as exc:
            bot['log'].error('Premium download notification failed: %s',type(exc).__name__)
            return False
    worker=bot['export_worker']
    if worker is None:
        await notify('Premium dosya exportu kapalı; ayar değiştirilmedi.')
        return True
    try:
        result=await asyncio.to_thread(worker.latest_premium_file)
        if result is None:
            await notify('Doğrulanmış Premium dosyası yok. Bu komut yeni export üretmez.')
            return True
        bundle,meta,info=result
        caption=(f"Premium kayıt dosyası | {meta['snapshot_id']} | N={meta['premium_count']}\n"
                 f"Kaynak okuma UTC epoch-ms: {meta['source_read_at_ms']}\n"
                 f"ZIP SHA256: {info['file_sha256']}\nPayload SHA256: {meta['payload_sha256']}")
        accepted=await bot['telegram_send_document'](session,str(bundle),caption,chat_id=chat_id)
        await notify('Telegram API dosyayı kabul etti; teslim için indirip ZIP/payload hashini doğrulayın.'
                     if accepted else 'Premium dosyası gönderilemedi; teslim tamamlanmadı.')
    except Exception as exc:
        bot['log'].error('Premium file download failed: %s',type(exc).__name__)
        await notify(f'Premium dosyası doğrulanamadı/gönderilemedi: {type(exc).__name__}.')
    return True


def main():
    logging.basicConfig(level=logging.INFO)
    parser=argparse.ArgumentParser();parser.add_argument('--source',default='/data/signals.db')
    parser.add_argument('--output',default='/data/research_exports');parser.add_argument('--loop',action='store_true')
    args=parser.parse_args();exporter=Exporter(args.source,args.output,deployment=os.getenv('RAILWAY_DEPLOYMENT_ID','UNKNOWN'))
    while True:
        print(json.dumps(exporter.run_due(),ensure_ascii=False),flush=True)
        if not args.loop:break
        time.sleep(30)


if __name__=='__main__':main()
