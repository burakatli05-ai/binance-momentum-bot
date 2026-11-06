"""Fail-fast supervision and lock-safe radar outcome persistence.

This module is intentionally isolated from strategy, risk, order, and notification
logic.  It patches only the production wrapper's runtime task group and the
research-only radar outcome writer.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from typing import Any, Awaitable, Iterable, MutableMapping

_RADAR_SQL = (
    "INSERT OR REPLACE INTO radar_outcomes"
    "(radar_id,horizon_s,return_pct,mfe_pct,mae_pct,ts) "
    "VALUES (?,?,?,?,?,?)"
)


class RuntimeSupervisor:
    VERSION = "runtime-supervisor-r1"
    _ALLOWED_EARLY_COMPLETION = frozenset({
        "early_exit_export_once",
        "quality_shadow_loop",
    })

    def __init__(
        self,
        runtime: MutableMapping[str, Any],
        *,
        hard_exit_seconds: float = 20.0,
        retry_limit: int = 100_000,
    ) -> None:
        self.runtime = runtime
        self.log = runtime.get("log") or logging.getLogger(__name__)
        self.hard_exit_seconds = max(1.0, float(hard_exit_seconds))
        self.retry_limit = max(1, int(retry_limit))
        self._original_gather = asyncio.gather
        self._original_save_radar_outcome = runtime["save_radar_outcome"]
        self._runtime_main_code = runtime["main"].__code__
        self._pending: "OrderedDict[tuple[int, int], tuple[int, int, float, float, float, int]]" = OrderedDict()
        self._pending_lock = threading.Lock()
        self._last_lock_log = 0.0
        self._lock_defer_until = 0.0
        self._installed = False
        self._hard_exit_stop: threading.Event | None = None
        self._hard_exit_thread: threading.Thread | None = None

    def install(self) -> None:
        if self._installed:
            return
        self.runtime["save_radar_outcome"] = self.save_radar_outcome
        asyncio.gather = self._patched_gather  # type: ignore[assignment]
        self._installed = True
        self.log.info("RUNTIME_SUPERVISOR_INSTALLED version=%s", self.VERSION)

    def restore(self) -> None:
        if not self._installed:
            return
        asyncio.gather = self._original_gather  # type: ignore[assignment]
        self.runtime["save_radar_outcome"] = self._original_save_radar_outcome
        self._installed = False

    @staticmethod
    def _is_database_lock(exc: BaseException) -> bool:
        text = str(exc).lower()
        return "database is locked" in text or "database is busy" in text

    def _write_radar_rows(
        self,
        rows: Iterable[tuple[int, int, float, float, float, int]],
        *,
        timeout_ms: int,
    ) -> None:
        materialized = list(rows)
        if not materialized:
            return
        conn = sqlite3.connect(
            self.runtime["DB_PATH"],
            timeout=max(0.001, timeout_ms / 1000.0),
        )
        try:
            conn.execute("PRAGMA busy_timeout=%d" % max(1, int(timeout_ms)))
            conn.executemany(_RADAR_SQL, materialized)
            conn.commit()
        finally:
            conn.close()

    def _defer_row(self, row: tuple[int, int, float, float, float, int]) -> None:
        key = (row[0], row[1])
        with self._pending_lock:
            if key not in self._pending and len(self._pending) >= self.retry_limit:
                raise RuntimeError("RADAR_OUTCOME_RETRY_QUEUE_FULL")
            self._pending[key] = row
            self._pending.move_to_end(key)
            size = len(self._pending)
        now = time.monotonic()
        if self._last_lock_log == 0.0 or now - self._last_lock_log >= 30.0:
            self._last_lock_log = now
            self.log.warning(
                "RADAR_OUTCOME_DB_LOCK_DEFERRED queue=%d version=%s",
                size,
                self.VERSION,
            )

    def save_radar_outcome(
        self,
        radar_id: int,
        horizon_s: int,
        ret: float,
        mfe: float,
        mae: float,
    ) -> bool:
        """Bound event-loop blocking and defer only transient SQLite lock errors."""
        row = (
            int(radar_id),
            int(horizon_s),
            float(ret),
            float(mfe),
            float(mae),
            int(time.time()),
        )
        # Once a lock is observed, avoid repeated 50 ms event-loop stalls while
        # the background writer owns retry responsibility.
        if time.monotonic() < self._lock_defer_until or self.pending_count():
            self._defer_row(row)
            return False
        try:
            self._write_radar_rows((row,), timeout_ms=50)
            return True
        except sqlite3.OperationalError as exc:
            if not self._is_database_lock(exc):
                raise
            self._lock_defer_until = time.monotonic() + 1.0
            self._defer_row(row)
            return False

    def _take_pending(self, limit: int = 250) -> list[tuple[int, int, float, float, float, int]]:
        rows: list[tuple[int, int, float, float, float, int]] = []
        with self._pending_lock:
            while self._pending and len(rows) < limit:
                _, row = self._pending.popitem(last=False)
                rows.append(row)
        return rows

    def _requeue(self, rows: Iterable[tuple[int, int, float, float, float, int]]) -> None:
        materialized = list(rows)
        with self._pending_lock:
            for row in reversed(materialized):
                key = (row[0], row[1])
                # A newer row for the same primary key may have arrived while
                # this batch was waiting on SQLite. Never overwrite it with the
                # older failed-batch value.
                if key in self._pending:
                    continue
                self._pending[key] = row
                self._pending.move_to_end(key, last=False)

    def pending_count(self) -> int:
        with self._pending_lock:
            return len(self._pending)

    async def _radar_retry_loop(self) -> None:
        backoff = 0.5
        stop_event = self.runtime["stop_event"]
        while not stop_event.is_set():
            rows = self._take_pending()
            if not rows:
                await asyncio.sleep(0.5)
                continue
            try:
                await asyncio.to_thread(self._write_radar_rows, rows, timeout_ms=5_000)
            except sqlite3.OperationalError as exc:
                self._requeue(rows)
                if not self._is_database_lock(exc):
                    self.log.exception("RADAR_OUTCOME_RETRY_DB_ERROR pending=%d", self.pending_count())
                    raise
                self._lock_defer_until = time.monotonic() + 1.0
                backoff = min(10.0, backoff * 2.0)
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                self._requeue(rows)
                raise
            except Exception:
                self._requeue(rows)
                self.log.exception("RADAR_OUTCOME_RETRY_FAILED pending=%d", self.pending_count())
                raise
            else:
                self.log.info("RADAR_OUTCOME_RETRY_FLUSHED rows=%d pending=%d", len(rows), self.pending_count())
                backoff = 0.5
        pending = self.pending_count()
        if pending:
            self.log.warning("RADAR_OUTCOME_RETRY_STOP pending=%d", pending)

    @staticmethod
    def _awaitable_name(awaitable: Awaitable[Any], index: int) -> str:
        if isinstance(awaitable, asyncio.Task):
            coro = awaitable.get_coro()
        else:
            coro = awaitable
        code = getattr(coro, "cr_code", None)
        if code is not None:
            return str(code.co_name)
        name = getattr(coro, "__qualname__", None) or getattr(coro, "__name__", None)
        return str(name or "awaitable_%d" % index)

    def _patched_gather(self, *aws: Awaitable[Any], **kwargs: Any) -> Awaitable[list[Any]]:
        try:
            caller_code = sys._getframe(1).f_code
        except (ValueError, AttributeError):
            caller_code = None
        if caller_code is self._runtime_main_code and not kwargs.get("return_exceptions", False):
            return self._supervise(aws)
        return self._original_gather(*aws, **kwargs)

    def _arm_hard_exit(self, reason: str) -> None:
        if self._hard_exit_thread and self._hard_exit_thread.is_alive():
            return
        stop = threading.Event()
        self._hard_exit_stop = stop

        def watchdog() -> None:
            if stop.wait(self.hard_exit_seconds):
                return
            message = (
                "RUNTIME_SUPERVISOR_HARD_EXIT_TIMEOUT "
                "seconds=%.1f reason=%s\n" % (self.hard_exit_seconds, reason)
            )
            try:
                sys.stderr.write(message)
                sys.stderr.flush()
            finally:
                os._exit(70)

        thread = threading.Thread(target=watchdog, name="runtime-hard-exit", daemon=True)
        self._hard_exit_thread = thread
        thread.start()

    def disarm_hard_exit(self) -> None:
        if self._hard_exit_stop:
            self._hard_exit_stop.set()
        thread = self._hard_exit_thread
        if thread and thread.is_alive():
            thread.join(timeout=0.2)
        self._hard_exit_stop = None
        self._hard_exit_thread = None

    async def _cancel_and_drain(
        self,
        records: list[tuple[asyncio.Future[Any], str]],
        *,
        reason: str,
    ) -> None:
        self.runtime["stop_event"].set()
        self._arm_hard_exit(reason)
        cancelled = 0
        for task, _ in records:
            if not task.done():
                task.cancel()
                cancelled += 1
        self.log.error("RUNTIME_SUPERVISOR_CANCELLING reason=%s tasks=%d", reason, cancelled)
        await self._original_gather(*(task for task, _ in records), return_exceptions=True)
        self.log.error("RUNTIME_SUPERVISOR_DRAINED reason=%s", reason)

    async def _supervise(self, aws: Iterable[Awaitable[Any]]) -> list[Any]:
        records: list[tuple[asyncio.Future[Any], str]] = []
        original_tasks: list[asyncio.Future[Any]] = []
        for index, awaitable in enumerate(aws):
            name = self._awaitable_name(awaitable, index)
            task = asyncio.ensure_future(awaitable)
            if isinstance(task, asyncio.Task):
                task.set_name("runtime:%s:%d" % (name, index))
            records.append((task, name))
            original_tasks.append(task)
        retry_task = asyncio.create_task(self._radar_retry_loop(), name="runtime:radar_outcome_retry")
        records.append((retry_task, "radar_outcome_retry"))
        remaining = {task for task, _ in records}
        names = {task: name for task, name in records}
        self.log.info("RUNTIME_SUPERVISOR_ARMED version=%s tasks=%d", self.VERSION, len(records))

        try:
            while remaining:
                done, _ = await asyncio.wait(remaining, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    remaining.discard(task)
                    name = names[task]
                    if task.cancelled():
                        if self.runtime["stop_event"].is_set():
                            continue
                        exc: BaseException = RuntimeError("RUNTIME_TASK_CANCELLED:%s" % name)
                    else:
                        exc = task.exception()  # type: ignore[assignment]
                        if exc is None:
                            if self.runtime["stop_event"].is_set() or name in self._ALLOWED_EARLY_COMPLETION:
                                continue
                            exc = RuntimeError("RUNTIME_TASK_RETURNED:%s" % name)
                    if exc is not None:
                        reason = "%s:%s" % (name, type(exc).__name__)
                        self.log.error(
                            "RUNTIME_SUPERVISOR_TASK_FAILED task=%s type=%s detail=%s",
                            name,
                            type(exc).__name__,
                            str(exc),
                            exc_info=(type(exc), exc, exc.__traceback__),
                        )
                        await self._cancel_and_drain(records, reason=reason)
                        raise exc
            return [None if task.cancelled() else task.result() for task in original_tasks]
        except asyncio.CancelledError:
            await self._cancel_and_drain(records, reason="main_cancelled")
            raise


def run_runtime(runtime: MutableMapping[str, Any]) -> None:
    """Run the loaded bot namespace with scoped runtime patches."""
    supervisor = RuntimeSupervisor(runtime)
    supervisor.install()
    request_stop = runtime["request_stop"]
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, request_stop)
        except Exception:
            pass
    try:
        asyncio.run(runtime["main"]())
    except KeyboardInterrupt:
        pass
    finally:
        supervisor.restore()
        supervisor.disarm_hard_exit()
