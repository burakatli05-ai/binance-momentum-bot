"""Notification-only durable SQLite outbox; no exchange API or credentials.

COMMITTED means locally durable, not delivered. Ambiguous Telegram sends can
produce duplicates on retry. Pending records retain their original event time.
"""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import time
import uuid

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class Submission:
    notification_id: int
    status: str
    def __bool__(self):
        raise TypeError('Admission is not a delivery acknowledgement')

@dataclass(frozen=True)
class DeliveryResult:
    confirmed: bool
    retry_after: float = 0.0

class NotificationDispatch:
    def __init__(self, path, sender, *, capacity=10000, workers=3,
                 send_timeout=22.0, drain_seconds=2.0, retry_delays=(2.,5.,10.),
                 poll_interval=.1):
        if capacity<1 or workers<2 or send_timeout<=0 or drain_seconds<0:
            raise ValueError('Invalid outbox bounds')
        self.path=str(path); self.sender=sender; self.capacity=capacity
        self.workers=workers; self.send_timeout=send_timeout
        self.drain_seconds=drain_seconds
        self.retry_delays=tuple(max(0.,float(x)) for x in retry_delays)
        self.poll_interval=max(.01,float(poll_interval))
        self._db=None; self._lock=None; self._task=None; self._unavailable=None
        self._active={}; self._closing=False; self._wake=asyncio.Event()

    @staticmethod
    def _now():
        return int(time.time()*1000)

    async def __aenter__(self):
        if self._db is not None: raise RuntimeError('Outbox already running')
        try:
            p=Path(self.path); p.parent.mkdir(parents=True,exist_ok=True)
            self._lock=open(str(p)+'.lock','a+b')
            fcntl.flock(self._lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            self._db=sqlite3.connect(self.path,timeout=.05,isolation_level=None)
            self._db.row_factory=sqlite3.Row
            self._db.execute('PRAGMA journal_mode=WAL')
            self._db.execute('PRAGMA synchronous=FULL')
            self._db.execute('''CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,event_key TEXT NOT NULL UNIQUE,
                stream_key TEXT NOT NULL,kind TEXT NOT NULL,lane TEXT NOT NULL,
                text TEXT NOT NULL,metadata TEXT NOT NULL,event_ms INTEGER NOT NULL,
                status TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
                due_ms INTEGER NOT NULL,ack_ms INTEGER,last_error TEXT,
                recovery_count INTEGER NOT NULL DEFAULT 0)''')
            self._db.execute('CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status,lane,due_ms,id)')
            self._db.execute('CREATE INDEX IF NOT EXISTS outbox_stream ON outbox(stream_key,id,status)')
            recovered=self._db.execute("UPDATE outbox SET status='PENDING',due_ms=?,recovery_count=recovery_count+1,last_error='CRASH_UNCONFIRMED' WHERE status='SENDING'",(self._now(),)).rowcount
            # Preserve all unconfirmed records. Only acknowledged audit rows expire.
            self._db.execute("DELETE FROM outbox WHERE status='ACK_CONFIRMED' AND ack_ms<?",(self._now()-7*86400000,))
            self._closing=False
            self._task=asyncio.create_task(self._pump(),name='telegram-durable-outbox')
            log.info('TG_OUTBOX_READY version=R3 recovered_unconfirmed=%d',recovered)
            return self
        except BaseException as error:
            if self._db is not None: self._db.close();self._db=None
            if self._lock is not None: self._lock.close();self._lock=None
            if isinstance(error,(OSError,sqlite3.Error)) and not isinstance(error,BlockingIOError):
                # Notification storage must not terminate ongoing order/stop
                # management. Fail visibly without pretending to queue/deliver.
                self._unavailable=type(error).__name__
                log.critical('TG_OUTBOX_UNAVAILABLE type=%s notifications_not_admitted=1',self._unavailable)
                return self
            raise

    def submit(self,key,kind,text,metadata,*,event_id=None,event_ms=None):
        if self._unavailable is not None:
            log.error('TG_ADMISSION outcome=NOT_ADMITTED_STORAGE')
            return Submission(0,'NOT_ADMITTED_STORAGE')
        if self._closing or self._db is None:
            return Submission(0,'NOT_ADMITTED_SHUTDOWN')
        allowed={'chat_id','symbol','notification_kind','notification_ordinal',
                 'signal_id','signal_price','entry_status','track_delivery',
                 'reply_markup','dispatch_lane'}
        if not isinstance(metadata,dict) or set(metadata)-allowed:
            return Submission(0,'NOT_ADMITTED_METADATA')
        try:
            payload=json.dumps(metadata,ensure_ascii=False,allow_nan=False,sort_keys=True)
            stream=json.dumps(key,ensure_ascii=False,allow_nan=False)
            text=str(text)
            if not text or len(text)>65536: return Submission(0,'NOT_ADMITTED_TEXT')
            event=str(event_id or uuid.uuid4().hex)
            event_time=self._now() if event_ms is None else int(event_ms)
            parts=[text[i:i+3500] for i in range(0,len(text),3500)]
            lane='broadcast' if metadata.get('dispatch_lane')=='broadcast' else 'critical'
            self._db.execute('BEGIN IMMEDIATE')
            previous=self._db.execute('SELECT id FROM outbox WHERE event_key=?',(event+':0',)).fetchone()
            if previous:
                rows=self._db.execute('SELECT text,metadata,stream_key,kind FROM outbox WHERE substr(event_key,1,?)=? ORDER BY id',(len(event)+1,event+':')).fetchall()
                same=len(rows)==len(parts) and all(r['text']==parts[i] and r['metadata']==payload and r['stream_key']==stream and r['kind']==str(kind) for i,r in enumerate(rows))
                self._db.execute('ROLLBACK')
                return Submission(previous['id'],'ALREADY_COMMITTED' if same else 'NOT_ADMITTED_ID_CONFLICT')
            count=self._db.execute("SELECT count(*) FROM outbox WHERE status!='ACK_CONFIRMED'").fetchone()[0]
            if count+len(parts)>self.capacity:
                self._db.execute('ROLLBACK')
                log.error('TG_ADMISSION outcome=NOT_ADMITTED_CAPACITY kind=%s',kind)
                return Submission(0,'NOT_ADMITTED_CAPACITY')
            first=None
            for i,part in enumerate(parts):
                cur=self._db.execute('''INSERT INTO outbox
                    (event_key,stream_key,kind,lane,text,metadata,event_ms,status,due_ms)
                    VALUES (?,?,?,?,?,?,?,'PENDING',?)''',
                    (event+':'+str(i),stream,str(kind),lane,part,payload,event_time,self._now()))
                first=first or cur.lastrowid
            self._db.execute('COMMIT');self._wake.set()
            log.info('TG_ADMISSION id=%s kind=%s outcome=COMMITTED event_ms=%s parts=%s',first,kind,event_time,len(parts))
            return Submission(first,'COMMITTED')
        except Exception as error:
            if self._db.in_transaction: self._db.execute('ROLLBACK')
            log.error('TG_ADMISSION outcome=NOT_ADMITTED_STORAGE error_type=%s',type(error).__name__)
            return Submission(0,'NOT_ADMITTED_STORAGE')

    def _claim(self,lane):
        self._db.execute('BEGIN IMMEDIATE')
        try:
            row=self._db.execute('''SELECT a.* FROM outbox a
                WHERE a.status='PENDING' AND a.lane=? AND a.due_ms<=?
                AND NOT EXISTS (SELECT 1 FROM outbox b WHERE b.stream_key=a.stream_key
                    AND b.id<a.id AND b.status IN ('PENDING','SENDING'))
                ORDER BY a.id LIMIT 1''',(lane,self._now())).fetchone()
            if row:
                self._db.execute("UPDATE outbox SET status='SENDING',attempts=attempts+1 WHERE id=?",(row['id'],))
            self._db.execute('COMMIT')
            return dict(row) if row else None
        except BaseException:
            self._db.execute('ROLLBACK');raise

    async def _pump(self):
        try:
            while not self._closing:
                self._wake.clear()
                lanes=[lane for _,lane in self._active.values()]
                for lane,slots in [('critical',self.workers-1),('broadcast',1)]:
                    for _ in range(max(0,slots-lanes.count(lane))):
                        try: row=self._claim(lane)
                        except sqlite3.Error as e:
                            log.error('TG_OUTBOX_READ_FAILED type=%s',type(e).__name__);break
                        if not row: break
                        t=asyncio.create_task(self._deliver(row),name='tg-outbox-'+str(row['id']))
                        self._active[row['id']]=(t,lane)
                try: await asyncio.wait_for(self._wake.wait(),self.poll_interval)
                except asyncio.TimeoutError: pass
        finally:
            tasks=[t for t,_ in self._active.values()]
            if tasks:
                _,pending=await asyncio.wait(tasks,timeout=self.drain_seconds)
                for t in pending: t.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
            self._active.clear()

    async def _deliver(self,row):
        status='UNCONFIRMED';error=None;retry=0.
        age=max(0.,(self._now()-row['event_ms'])/1000.)
        stamp=datetime.fromtimestamp(row['event_ms']/1000.,timezone.utc).isoformat(timespec='seconds')
        prefix='GECIKMIS BILDIRIM - guncel giris talimati degildir.\n' if age>=5 else ''
        body=prefix+f"Bot olay zamani (UTC): {stamp}\nOlay #{row['id']} | gecikme {age:.1f} sn\n\n"+row['text']
        try:
            meta=json.loads(row['metadata']);meta.pop('dispatch_lane',None)
            r=await asyncio.wait_for(self.sender(body,meta),self.send_timeout)
            confirmed=r.confirmed is True if isinstance(r,DeliveryResult) else r is True
            retry=max(0.,r.retry_after) if isinstance(r,DeliveryResult) else 0.
            if confirmed: status='ACK_CONFIRMED'
        except asyncio.CancelledError:
            status='PENDING';error='CANCELLED_UNCONFIRMED';raise
        except Exception as e: error=type(e).__name__
        finally:
            attempt=row['attempts']+1;due=self._now()
            if status=='UNCONFIRMED' and attempt<=len(self.retry_delays):
                status='PENDING';due+=int(max(self.retry_delays[attempt-1],retry)*1000)
            elif status=='UNCONFIRMED': status='EXHAUSTED_UNCONFIRMED'
            try:
                self._db.execute('UPDATE outbox SET status=?,due_ms=?,ack_ms=?,last_error=? WHERE id=?',
                    (status,due,self._now() if status=='ACK_CONFIRMED' else None,error,row['id']))
            except sqlite3.Error as e:
                log.error('TG_OUTBOX_RESULT_WRITE_FAILED id=%s type=%s',row['id'],type(e).__name__)
            self._active.pop(row['id'],None);self._wake.set()
            log.info('TG_OUTBOX_RESULT id=%s kind=%s outcome=%s attempt=%s event_ms=%s age_s=%.3f',row['id'],row['kind'],status,attempt,row['event_ms'],age)

    async def join(self,timeout=5.):
        async def empty():
            while self._active or self._db.execute("SELECT 1 FROM outbox WHERE status IN ('PENDING','SENDING') LIMIT 1").fetchone():
                self._wake.set();await asyncio.sleep(.01)
        await asyncio.wait_for(empty(),timeout)

    async def __aexit__(self,exc_type,exc,tb):
        self._closing=True;self._wake.set()
        try:
            if self._task: await self._task
        finally:
            if self._db is not None: self._db.close();self._db=None
            if self._lock is not None: self._lock.close();self._lock=None
