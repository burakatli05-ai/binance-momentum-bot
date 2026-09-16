"""SQLite-safe export and repository scheduler. Never starts or repairs the bot DB."""
import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import tempfile
import uuid
import zipfile

import assistant_bridge
from research_reports import summary

IST=timezone(timedelta(hours=3))


def atomic_json(path,value):
    temp=path.with_name(path.name+'.tmp')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    os.replace(temp,path)


def readonly(path):
    return sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)


def inspect_snapshot(path):
    with closing(readonly(path)) as c:
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
                c.backup(restored)
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
    def __init__(self,source,root,version='5.13.5+alt-shadow',deployment='UNKNOWN',config=None):
        self.source=Path(source).resolve();self.root=Path(root).resolve()
        if self.root==self.source or self.source.parent==self.root:
            raise ValueError('use a dedicated export subdirectory')
        self.root.mkdir(parents=True,exist_ok=True)
        self.version,self.deployment=version,deployment
        # Callers pass a whitelist of public research settings only. Never serialize environment.
        self.config=config or {}

    def state(self):
        p=self.root/'scheduler.json'
        return json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}

    def latest(self):
        p=self.root/'latest.json'
        if not p.exists():return None
        pointer=json.loads(p.read_text(encoding='utf-8'))
        directory=(self.root/pointer['snapshot_id']).resolve()
        if directory.parent!=self.root:raise ValueError('invalid snapshot pointer')
        manifest=json.loads((directory/'manifest.json').read_text(encoding='utf-8'))
        if not manifest['valid']:raise ValueError('invalid snapshot')
        with (directory/'signals.db').open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
        if digest!=manifest['sha256']:raise ValueError('snapshot hash mismatch')
        return directory,manifest

    def download_bundle(self, *, include_db=False):
        latest=self.latest()
        if not latest:return None
        directory,manifest=latest
        bundle=directory/('export-full.zip' if include_db else 'export-small.zip')
        metadata={'snapshot_id':manifest['snapshot_id'],'snapshot_created_time_ms':manifest['created_time_ms'],
                  'snapshot_sha256':manifest['sha256'],'includes_database':include_db,'summary_source':None}
        with zipfile.ZipFile(bundle,'w',zipfile.ZIP_DEFLATED) as z:
            if include_db:z.write(directory/'signals.db','signals.db')
            z.write(directory/'manifest.json','manifest.json')
            pointer=self.root/'latest-summary.json'
            if pointer.exists():
                info=json.loads(pointer.read_text(encoding='utf-8'))
                summary_dir=(self.root/info['snapshot_id']).resolve()
                if summary_dir.parent!=self.root:raise ValueError('invalid summary pointer')
                z.write(summary_dir/'summary.json','summary.json')
                metadata['summary_source']=info
            else:
                z.writestr('summary.json',json.dumps({'status':'NO_SUMMARY_AVAILABLE'}))
            z.writestr('metadata.json',json.dumps(metadata,ensure_ascii=False,indent=2))
        return bundle

    def snapshot(self,now):
        sid=datetime.fromtimestamp(now/1000,timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
        directory=self.root/sid;directory.mkdir()
        dest=directory/'signals.db'
        manifest={'snapshot_id':sid,'created_time_ms':now,'source_db_path':str(self.source),
                  'version':self.version,'deployment_id':self.deployment,
                  'config_identity':hashlib.sha256(json.dumps(self.config,sort_keys=True).encode()).hexdigest(),'valid':False}
        try:
            with closing(readonly(self.source)) as src,closing(sqlite3.connect(dest)) as target:
                deadline=time.monotonic()+120
                def progress(status,remaining,total):
                    if time.monotonic()>deadline:raise TimeoutError('snapshot time limit')
                src.backup(target,pages=512,progress=progress,sleep=.05)
            manifest.update(inspect_snapshot(dest))
            with dest.open('rb') as f:manifest['sha256']=hashlib.file_digest(f,'sha256').hexdigest()
            manifest['size_bytes']=dest.stat().st_size
            old=self.latest()
            manifest['unchanged']=bool(old and old[1]['sha256']==manifest['sha256'])
        except Exception as exc:
            manifest.update(valid=False,error=type(exc).__name__)
        atomic_json(directory/'manifest.json',manifest)
        if manifest['valid']:atomic_json(self.root/'latest.json',{'snapshot_id':sid})
        return directory,manifest

    def run_due(self,now=None):
        """Single worker; durable pre-claim avoids duplicate hourly attempts after restart."""
        now=int(time.time()*1000) if now is None else now
        # Cross-process lease is a separate DB, never the source. Release on crash via timeout.
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
            directory,manifest=self.snapshot(now)
            result={'status':'VALID' if manifest['valid'] else 'INVALID','snapshot_id':manifest['snapshot_id']}
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
                try:
                    bridge_meta=assistant_bridge.emit(directory/'signals.db',manifest)
                    result['assistant_bridge']={
                        'chunks':bridge_meta['chunks'],
                        'compressed_bytes':bridge_meta['compressed_bytes'],
                        'sha256':bridge_meta['sha256'],
                    }
                except Exception as exc:
                    # Bridge failure must never block the research scheduler or production bot.
                    print(f"{assistant_bridge.LOG_PREFIX} ERROR {manifest['snapshot_id']} {type(exc).__name__}",flush=True)
                    result['assistant_bridge']='ERROR'
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


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--source',default='/data/signals.db')
    parser.add_argument('--output',default='/data/research_exports');parser.add_argument('--loop',action='store_true')
    args=parser.parse_args();exporter=Exporter(args.source,args.output,deployment=os.getenv('RAILWAY_DEPLOYMENT_ID','UNKNOWN'))
    while True:
        print(json.dumps(exporter.run_due(),ensure_ascii=False),flush=True)
        if not args.loop:break
        time.sleep(30)


if __name__=='__main__':main()
