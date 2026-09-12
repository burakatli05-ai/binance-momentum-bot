"""Fail-closed persistent SQLite startup; never import bot before verification."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time
import uuid

BASELINE = json.loads(Path(__file__).with_name('db_baseline.json').read_text())


def check_db(path, manifest=BASELINE):
    if not path.is_file():
        raise RuntimeError('Database missing')
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as conn:
        for pragma in ('quick_check', 'integrity_check'):
            if conn.execute('PRAGMA ' + pragma).fetchall() != [('ok',)]:
                raise RuntimeError('Database integrity failed')
        counts = {}
        for anchor in manifest.get('anchors', []):
            row = conn.execute('SELECT id,ts,symbol,price FROM signals_v2 WHERE id=?',
                               (anchor[0],)).fetchone()
            if row is None or list(row) != anchor:
                raise RuntimeError('Database history identity mismatch')
        for table, minimum in manifest['table_counts'].items():
            quoted = '"' + table.replace('"', '""') + '"'
            count = conn.execute('SELECT count(*) FROM ' + quoted).fetchone()[0]
            if count < minimum:
                raise RuntimeError('Missing historical rows: ' + table)
            counts[table] = count
    return counts


def prepare_db(target, backup, manifest):
    try:
        return check_db(target, manifest)
    except (RuntimeError, sqlite3.Error):
        pass
    # Verify source before touching an existing database or its WAL files.
    if not backup.is_file() or backup.stat().st_size != manifest['bytes']:
        raise RuntimeError('Verified restore source unavailable; bot will not start')
    with backup.open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != manifest['sha256']:
            raise RuntimeError('Restore SHA256 mismatch')
    check_db(backup, manifest)
    staging = target.with_name(target.name + '.restore-' + uuid.uuid4().hex)
    shutil.copyfile(backup, staging)
    check_db(staging, manifest)
    with staging.open('rb+') as stream:
        os.fsync(stream.fileno())
    # Quarantine instead of deleting an unexpected DB, including WAL/SHM.
    quarantine = target.parent / ('quarantine-' + uuid.uuid4().hex)
    for suffix in ('', '-wal', '-shm'):
        old = Path(str(target) + suffix)
        if old.exists():
            quarantine.mkdir(exist_ok=True)
            os.replace(old, quarantine / old.name)
    os.replace(staging, target)
    if os.name == 'posix':
        fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return check_db(target, manifest)


def main():
    # Explicit maintenance mode lets operators attach/upload without opening DB.
    if os.getenv('DB_MAINTENANCE') == '1':
        print('DB MAINTENANCE: bot disabled; waiting for verified restore', flush=True)
        while True:
            time.sleep(30)
    mount = Path(os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '/data')).resolve()
    if not mount.is_mount():
        raise RuntimeError('Persistent volume is not mounted')
    target = Path(os.environ.get('DB_PATH', '/data/signals.db')).resolve()
    if target.parent != mount:
        raise RuntimeError('DB_PATH must be directly on the persistent volume')
    # A cutover manifest can require newer history than the original snapshot.
    manifest_path = mount / 'restore-manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else BASELINE
    if manifest.get('anchors') != BASELINE.get('anchors'):
        raise RuntimeError('Restore manifest history identity mismatch')
    for table, count in BASELINE['table_counts'].items():
        if manifest['table_counts'].get(table, -1) < count:
            raise RuntimeError('Restore manifest predates verified history')
    backup = mount / 'restore-source.db'
    counts = prepare_db(target, backup, manifest)
    os.environ['DB_PATH'] = str(target)
    os.environ['AUTO_TRADE_LIVE_ALLOWED'] = '0'
    os.environ['AUTO_TRADE_BOOT_MODE'] = 'OFF'
    os.environ['PYTHON_DOTENV_DISABLED'] = '1'
    for key in ('X_WATCHER_NOTIFY', 'RESEARCH_NOTIFY', 'LIQ_V3_NOTIFY',
                'SHADOW_EXIT_NOTIFY', 'GAINERS_NOTIFY', 'TREND_BUILDUP_NOTIFY'):
        os.environ[key] = '0'
    if not os.getenv('X_BEARER_TOKEN'):
        os.environ['X_WATCHER_ENABLED'] = '0'
    print(json.dumps({'startup': 'verified', 'db_path': str(target),
                      'legacy_tables': len(counts), 'counts': counts,
                      'live_allowed': 0, 'boot_mode': 'OFF'}), flush=True)
    os.chdir(Path(__file__).resolve().parent)
    os.execv(sys.executable, [sys.executable, '-u', 'bot.py'])


if __name__ == '__main__':
    main()
