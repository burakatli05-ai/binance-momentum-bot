"""Disk policy for research exports only; never opens the production DB for writing."""
from contextlib import contextmanager
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import threading
import time

LOG = logging.getLogger('research_export')
GIB = 1024 ** 3
SID = re.compile(r'\d{8}T\d{6}Z-[0-9a-f]{8}\Z')
FILES = {'signals.db', 'signals.db-wal', 'signals.db-shm', 'signals.db-journal',
         'manifest.json', 'manifest.json.tmp', 'summary.json', 'summary.json.tmp',
         'export.zip', 'export-full.zip', 'export-small.zip',
         'export-full.zip.partial', 'export-small.zip.partial'}


class ExportSkipped(RuntimeError):
    pass


def telemetry(event, **fields):
    LOG.info('RESEARCH_EXPORT %s', json.dumps(dict(event=event, **fields), sort_keys=True))


def no_links(path):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        if part.is_symlink() or (hasattr(part, 'is_junction') and part.is_junction()):
            raise ValueError('export path contains a link')
    return path


@dataclass(frozen=True)
class Policy:
    retain: int = 3
    min_free: int = 8 * GIB
    budget: int = 20 * GIB
    stale_seconds: int = 6 * 3600
    timeout: int = 300

    @classmethod
    def from_env(cls):
        values = {}
        for field, env in [('retain', 'RESEARCH_EXPORT_RETAIN'), ('min_free', 'RESEARCH_EXPORT_MIN_FREE_BYTES'),
                           ('budget', 'RESEARCH_EXPORT_BUDGET_BYTES'), ('stale_seconds', 'RESEARCH_EXPORT_STALE_SECONDS'),
                           ('timeout', 'RESEARCH_EXPORT_TIMEOUT_SECONDS')]:
            if env in os.environ:
                values[field] = int(os.environ[env])
        result = cls(**values)
        if not (1 <= result.retain <= 24 and result.min_free >= 4 * GIB and
                result.budget >= GIB and result.stale_seconds >= 3600 and 30 <= result.timeout <= 300):
            raise ValueError('unsafe research export policy')
        return result


class Safety:
    def __init__(self, source, root, policy=None):
        self.source = Path(source).resolve()
        self.root = no_links(root).resolve()
        if self.root == self.source or self.root in self.source.parents:
            raise ValueError('use a dedicated export subdirectory')
        self.root.mkdir(parents=True, exist_ok=True)
        self.identity = (self.root.stat().st_dev, self.root.stat().st_ino)
        self.policy = policy or Policy.from_env()
        self.mutex = threading.RLock()
        self.depth = 0

    def validate_root(self):
        no_links(self.root)
        s = self.root.stat()
        if (s.st_dev, s.st_ino) != self.identity:
            raise ValueError('export root changed')

    @contextmanager
    def locked(self):
        if not self.mutex.acquire(blocking=False):
            raise ExportSkipped('BUSY')
        handle = None
        try:
            self.validate_root()
            if not self.depth:
                path = no_links(self.root / '.export.lock')
                if path.exists() and path.stat().st_nlink != 1:
                    raise ValueError('linked export lock')
                handle = path.open('a+b')
                try:
                    if os.name == 'nt':
                        import msvcrt
                        if path.stat().st_size == 0:
                            handle.write(b'0'); handle.flush()
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    raise ExportSkipped('BUSY') from exc
            self.depth += 1
            try:
                yield
            finally:
                self.depth -= 1
        finally:
            if handle is not None:
                handle.close()  # OS lock also releases on process death; no lease expiry race.
            self.mutex.release()

    def usage(self):
        total = 0
        for base, dirs, files in os.walk(self.root, followlinks=False):
            dirs[:] = [d for d in dirs if not (Path(base)/d).is_symlink()]
            for name in files:
                p = Path(base)/name
                if not p.is_symlink():
                    total += p.stat().st_size
        return total

    def owned_files(self, directory):
        """Fail closed for unknown content, mount points, hardlinks and symlinks."""
        self.validate_root()
        no_links(directory)
        if directory.parent != self.root or directory.resolve().parent != self.root:
            raise ValueError('outside export root')
        name = directory.name.removeprefix('.partial-')
        if not SID.fullmatch(name) or not directory.is_dir():
            raise ValueError('unrecognized snapshot')
        found = []
        for base, dirs, files in os.walk(directory, followlinks=False):
            for child in dirs:
                p = Path(base)/child
                no_links(p)
                if Path(base) != directory or not child.startswith('restore-smoke-'):
                    raise ValueError('unknown export subdirectory')
                if p.stat().st_dev != self.identity[0]:
                    raise ValueError('mounted export subdirectory')
            for child in files:
                p = Path(base)/child
                no_links(p)
                s = p.stat()
                allowed = child in FILES if Path(base) == directory else child in {'restored.db', 'restored.db-wal', 'restored.db-shm', 'restored.db-journal'}
                if not allowed or not stat.S_ISREG(s.st_mode) or s.st_nlink != 1 or s.st_dev != self.identity[0]:
                    raise ValueError('unknown or linked export file')
                found.append(p)
        return found

    def remove(self, directory):
        files = self.owned_files(directory)
        size = sum(p.stat().st_size for p in files)
        # Explicit files only. Never use a recursive deletion against a computed path.
        for p in files:
            no_links(p)
            if p.stat().st_nlink != 1:
                raise ValueError('export file changed')
            p.unlink()
        for p in directory.iterdir():
            no_links(p)
            p.rmdir()
        directory.rmdir()
        return size

    def cleanup(self, now=None, dry_run=False):
        now = time.time() if now is None else now
        successes, incomplete = [], []
        unknown = 0
        for p in self.root.iterdir():
            if not (SID.fullmatch(p.name) or (p.name.startswith('.partial-') and SID.fullmatch(p.name[9:]))):
                continue
            try:
                files = self.owned_files(p)
                manifest = p/'manifest.json'
                data = json.loads(manifest.read_text()) if manifest.exists() else {}
                if (SID.fullmatch(p.name) and data.get('valid') is True and
                        data.get('snapshot_id') == p.name and (p/'signals.db').is_file() and
                        (p/'signals.db').stat().st_size == data.get('size_bytes') and
                        re.fullmatch('[0-9a-f]{64}', data.get('sha256', ''))):
                    successes.append(p)
                else:
                    age = now - max([p.stat().st_mtime] + [f.stat().st_mtime for f in files])
                    if age >= self.policy.stale_seconds:
                        incomplete.append(p)
            except (OSError, ValueError, TypeError):
                unknown += 1
        successes.sort(key=lambda p: p.name, reverse=True)
        keep = successes[:self.policy.retain]
        # Preserve a valid latest pointer even if timestamps are unusual.
        pointer = no_links(self.root/'latest.json')
        if pointer.exists():
            sid = json.loads(pointer.read_text())['snapshot_id']
            pinned = next((p for p in successes if p.name == sid), None)
            if pinned is None:
                raise ValueError('latest pointer is not a validated snapshot')
            if pinned not in keep:
                keep = keep[:self.policy.retain-1] + [pinned]
        obsolete = [p for p in successes if p not in keep]
        summary_pointer = no_links(self.root/'latest-summary.json')
        if summary_pointer.exists():
            sid = json.loads(summary_pointer.read_text())['snapshot_id']
            if sid in {p.name for p in obsolete + incomplete}:
                # Remove the pointer first; consumers get explicit NO_SUMMARY_AVAILABLE.
                if not dry_run:summary_pointer.unlink()
        removed = 0
        for p in obsolete + incomplete:
            size = sum(f.stat().st_size for f in self.owned_files(p)) if dry_run else self.remove(p)
            removed += size
            telemetry('cleanup_snapshot', snapshot_id=p.name, cleanup_bytes=size, dry_run=dry_run)
        daily = sorted(self.root.glob('daily-????-??-??.json'), reverse=True)
        for p in daily[30:]:
            no_links(p)
            if p.is_file() and p.stat().st_nlink == 1:
                removed += p.stat().st_size
                if not dry_run:p.unlink()
        usage = shutil.disk_usage(self.root)
        result = dict(cleanup_bytes=removed, retained_snapshot_count=len(keep),dry_run=dry_run,
                      incomplete_snapshot_count=len(incomplete), unknown_snapshot_count=unknown,
                      export_bytes=self.usage(), disk_free_bytes=usage.free, disk_used_bytes=usage.used)
        telemetry('cleanup', **result)
        return result

    def preflight(self, estimate):
        used = self.usage()
        free = shutil.disk_usage(self.root).free
        reason = ('MIN_FREE' if free < self.policy.min_free + estimate else
                  'EXPORT_BUDGET' if used + estimate > self.policy.budget else None)
        telemetry('disk_guard', disk_free_bytes=free, export_bytes=used, estimated_bytes=estimate,
                  min_free_bytes=self.policy.min_free, budget_bytes=self.policy.budget,
                  skipped_export_reason=reason)
        if reason:
            raise ExportSkipped(reason)

    def checkpoint(self, deadline):
        if time.monotonic() > deadline:
            raise ExportSkipped('TIME_LIMIT')
        if shutil.disk_usage(self.root).free < self.policy.min_free + 16 * 1024**2:
            raise ExportSkipped('MIN_FREE_DURING_EXPORT')


def serialized(method):
    def wrapped(self, *args, **kwargs):
        with self.safety.locked():
            return method(self, *args, **kwargs)
    return wrapped
