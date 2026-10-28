"""Ordered off-loop I/O and bounded loop-lag evidence. No exchange operations.

Calls await completion, including cancellation drain; no detached write queue,
no shared SQLite connections, no sampling or dropping of research price events.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
import functools
import logging
import os
import sys
import threading
import time

log = logging.getLogger(__name__)


class SerialIO:
    def __init__(self, name):
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=name)
        self._lock = asyncio.Lock()
        self._closed = False
        self.name = name

    async def call(self, function, *args, **kwargs):
        # One submitted operation at a time. Waiting callers do not build an
        # unbounded executor queue. Inputs are frozen by the caller.
        async with self._lock:
            if self._closed:
                raise RuntimeError('I/O worker closed')
            loop = asyncio.get_running_loop()
            context = copy_context()
            work = functools.partial(function, *args, **kwargs)
            future = loop.run_in_executor(self._pool, context.run, work)
            cancelled = False
            try:
                while True:
                    try:
                        result = await asyncio.shield(future)
                        break
                    except asyncio.CancelledError:
                        # sqlite3 cannot be cancelled by cancelling its future.
                        # Do not release ownership until the actual write ends.
                        cancelled = True
            except BaseException:
                if cancelled:
                    log.error('SERIAL_IO_CANCELLED_OPERATION_FAILED worker=%s', self.name)
                    raise asyncio.CancelledError() from None
                raise
            if cancelled:
                raise asyncio.CancelledError()
            return result

    async def close(self):
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            self._pool.shutdown(wait=True)


async def loop_health(stop_event, *, interval=.25, stall_seconds=2., report_seconds=30.):
    """Watchdog emits file/line/function only; never locals, SQL, URLs or secrets."""
    loop_thread = threading.get_ident()
    done = threading.Event()
    last_beat = [time.monotonic()]
    def watch():
        last_report = 0.
        while not done.wait(max(.05, interval)):
            now = time.monotonic()
            age = now-last_beat[0]
            if age < stall_seconds or now-last_report < report_seconds:
                continue
            last_report = now
            frame = sys._current_frames().get(loop_thread)
            stack = []
            while frame is not None and len(stack) < 8:
                stack.append('%s:%d:%s' % (os.path.basename(frame.f_code.co_filename),
                                          frame.f_lineno, frame.f_code.co_name))
                frame = frame.f_back
            del frame
            log.warning('MAIN_LOOP_STALL lag_ms=%.3f stack=%s', age*1000, ' <- '.join(stack))
    watcher = threading.Thread(target=watch, name='runtime-lag-witness', daemon=True)
    watcher.start()
    last_report = time.monotonic()
    maximum = 0.
    try:
        while not stop_event.is_set():
            expected = time.monotonic()+interval
            await asyncio.sleep(interval)
            now = time.monotonic()
            maximum = max(maximum, max(0., now-expected)*1000)
            last_beat[0] = now
            if now-last_report >= report_seconds:
                log.info('RUNTIME_HEALTH version=R4 loop_lag_max_ms=%.3f', maximum)
                maximum = 0.
                last_report = now
    finally:
        done.set()
        watcher.join(timeout=interval+.1)
