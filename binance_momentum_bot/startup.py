"""Fail-closed persistent SQLite startup; never import bot before verification."""
from contextlib import closing
import hashlib
import faulthandler
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid

BASELINE = json.loads(Path(__file__).with_name('db_baseline.json').read_text())


def _startup_trace(stage, **fields):
    # Diagnostic metadata only: no credentials, rows or database writes.
    print(json.dumps({'startup_stage': stage, **fields}), flush=True)


def check_db(path, manifest=BASELINE):
    if not path.is_file():
        raise RuntimeError('Database missing')
    _startup_trace('db_check_begin', file=path.name, bytes=path.stat().st_size)
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as conn:
        for pragma in ('quick_check', 'integrity_check'):
            _startup_trace('pragma_begin', check=pragma)
            if conn.execute('PRAGMA ' + pragma).fetchall() != [('ok',)]:
                raise RuntimeError('Database integrity failed')
            _startup_trace('pragma_ok', check=pragma)
        counts = {}
        _startup_trace('history_anchors_begin')
        for anchor in manifest.get('anchors', []):
            row = conn.execute('SELECT id,ts,symbol,price FROM signals_v2 WHERE id=?',
                               (anchor[0],)).fetchone()
            if row is None or list(row) != anchor:
                raise RuntimeError('Database history identity mismatch')
        _startup_trace('history_anchors_ok')
        for table, minimum in manifest['table_counts'].items():
            quoted = '"' + table.replace('"', '""') + '"'
            _startup_trace('table_count_begin', table=table)
            count = conn.execute('SELECT count(*) FROM ' + quoted).fetchone()[0]
            if count < minimum:
                raise RuntimeError('Missing historical rows: ' + table)
            counts[table] = count
            _startup_trace('table_count_ok', table=table)
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
    _startup_trace('entrypoint_entered')
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
    _startup_trace('db_verification_begin')
    counts = prepare_db(target, backup, manifest)
    _startup_trace('db_verification_ok')
    os.environ['DB_PATH'] = str(target)
    live_allowed = os.getenv('AUTO_TRADE_LIVE_ALLOWED', '0').strip().lower() in ('1', 'true', 'yes', 'on')
    os.environ['AUTO_TRADE_LIVE_ALLOWED'] = '1' if live_allowed else '0'
    os.environ['AUTO_TRADE_BOOT_MODE'] = 'OFF'
    os.environ['PYTHON_DOTENV_DISABLED'] = '1'
    # Shadow Telegram delivery requires explicit opt-in, including at boot.
    runner_notify = os.getenv('RUNNER_SCORE_V1_NOTIFY', '0').strip().lower() in ('1', 'true', 'yes', 'on')
    os.environ['RUNNER_SCORE_V1_NOTIFY'] = '1' if runner_notify else '0'
    for key in ('X_WATCHER_NOTIFY', 'RESEARCH_NOTIFY', 'LIQ_V3_NOTIFY',
                'SHADOW_EXIT_NOTIFY', 'GAINERS_NOTIFY', 'TREND_BUILDUP_NOTIFY'):
        os.environ[key] = '0'
    if not os.getenv('X_BEARER_TOKEN'):
        os.environ['X_WATCHER_ENABLED'] = '0'
    print(json.dumps({'startup': 'verified', 'db_path': str(target),
                      'legacy_tables': len(counts), 'counts': counts,
                      'live_allowed': int(live_allowed), 'boot_mode': 'OFF',
                      'runner_score_v1_notify': int(runner_notify)}), flush=True)
    os.chdir(Path(__file__).resolve().parent)

    # The exporter is a separate read-only consumer of the verified production DB.
    # Previously research_export.py existed but was never launched by startup.py,
    # so hourly snapshots and ASSISTANT_BRIDGE_V1 log packages could not appear.
    export_enabled = os.getenv('RESEARCH_EXPORT_ENABLED', '1').strip().lower() in ('1', 'true', 'yes', 'on')
    if export_enabled:
        export_output = os.getenv('RESEARCH_EXPORT_DIR', '/data/research_exports')
        try:
            subprocess.Popen(
                [sys.executable, '-u', 'research_export.py', '--source', str(target),
                 '--output', export_output, '--loop'],
                env=os.environ.copy(),
            )
            print(json.dumps({'research_exporter': 'started', 'output': export_output}), flush=True)
        except Exception as exc:
            print(json.dumps({'research_exporter': 'launch_failed', 'error': type(exc).__name__,
                              'production_continues': True}), flush=True)
    else:
        print(json.dumps({'research_exporter': 'disabled'}), flush=True)

    _startup_trace('bot_exec')
    os.execv(sys.executable, [sys.executable, '-u', 'bot.py'])


if __name__ == '__main__':
    faulthandler.dump_traceback_later(60, repeat=True)
    try:
        main()
    finally:
        faulthandler.cancel_dump_traceback_later()
