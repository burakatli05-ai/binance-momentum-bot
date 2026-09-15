"""Bounded research export. Only the SQLite Backup API reads the live database.

No bot import, schema migration, scheduler mutation, or order API is needed here.
"""
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import threading
import time
import zipfile

from research_export import IST, readonly

EXPORT_VERSION = 'analysis-export-v1'
_LOCK = threading.Lock()

# Explicit allowlist: retain every column (including feature/reason JSON), never
# infer inclusion from table prefixes. Units are declared, not guessed by size.
ROOTS = {
    'signals_v2': ('ts', 1000),
    'candidate_events': ('ts', 1000),
    'radar_signals': ('ts', 1000),
    'gainers_events': ('ts', 1000),
    'research_events': ('ts', 1000),
    'shadow_exit_events': ('ts', 1000),
    'entry_stage_forward_shadow': ('created_ts_ms', 1),
    'causal_cohorts': ('decision_time_ms', 1),
    'runner_score_v1_shadow': ('event_ts_ms', 1),
    'runner_watch_v1_shadow': ('created_ts_ms', 1),
    'missed_runner_audit': ('event_ts', 1000),
}
# Child rows are selected by the retained parent, not by their update time.
# Observations must also have occurred by the effective snapshot cutoff.
CHILDREN = {
    'signal_outcomes': ('signal_id', 'signals_v2', 'id', 'ts', 1000),
    'radar_outcomes': ('radar_id', 'radar_signals', 'id', 'ts', 1000),
    'gainers_outcomes': ('event_id', 'gainers_events', 'id', 'ts', 1000),
    'research_outcomes': ('event_id', 'research_events', 'id', 'ts', 1000),
    'shadow_event_outcomes': ('shadow_event_id', 'shadow_exit_events', 'id', 'ts', 1000),
    'causal_cohort_events': ('cohort_id', 'causal_cohorts', 'id', 'observed_time_ms', 1),
    'causal_cohort_outcomes': ('cohort_id', 'causal_cohorts', 'id', 'observed_time_ms', 1),
    'non_runner_veto_v1_shadow': ('watch_id', 'runner_watch_v1_shadow', 'watch_id', 'observed_ts_ms', 1),
    'runner_watch_outcome_v1_shadow': ('watch_id', 'runner_watch_v1_shadow', 'watch_id', 'matured_ts_ms', 1),
}
SIGNAL_CHILDREN = {
    'signal_meta': (None, 1),
    'signal_paths': (None, 1),
    'premium_radar_links': ('premium_ts', 1000),
    'premium_wave_tracking': (None, 1),
    'premium_wave_events': (None, 1),
    'premium_context': ('signal_generated_ts_ms', 1),
    'premium_micro_snapshots': ('observed_ts_ms', 1),
    'premium_entry_validation': ('finalized_ts_ms', 1),
    'premium_liquidity_snapshots': ('observed_ts_ms', 1),
    'premium_progress_validation': ('finalized_ts_ms', 1),
    'premium_execution_composite': ('finalized_ts_ms', 1),
    'premium_failure_risk': ('observed_ts_ms', 1),
    'premium_conflict_events': ('observed_ts_ms', 1),
    'failure_risk_reclaims': (None, 1),
    'premium_execution_gate_shadow': ('observed_ts_ms', 1),
    'premium_execution_gate_v2_shadow': ('finalized_ts_ms', 1),
    'premium_execution_gate_v21_shadow': (None, 1),
    'premium_gate_counterfactual': ('observed_ts_ms', 1),
    'premium_exit_forward_shadow': (None, 1),
    'premium_delayed_entry_shadow': (None, 1),
    'premium_liquidity_transition_v3': ('finalized_ts_ms', 1),
    'premium_fatigue_shadow': ('decision_time_ms', 1),
}
for _table, (_clock, _unit) in SIGNAL_CHILDREN.items():
    CHILDREN[_table] = ('signal_id', 'signals_v2', 'id', _clock, _unit)
EPISODES = {
    'momentum_episodes': 'end_ts',
    'discovery_episode_audit': 'last_event_ts',
}
INCLUDED_TABLES = tuple(ROOTS) + tuple(CHILDREN) + tuple(EPISODES)


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def sha256(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _latest_valid(worker):
    """Scan manifests: a broken/missing latest pointer must not hide older VALIDs."""
    candidates = []
    for path in worker.root.glob('*/manifest.json'):
        try:
            directory = path.parent.resolve()
            if directory.parent != worker.root or path.is_symlink():
                continue
            manifest = json.loads(path.read_text(encoding='utf-8'))
            if isinstance(manifest, dict) and manifest.get('valid') is True and manifest.get('snapshot_id') == directory.name:
                candidates.append((int(manifest['created_time_ms']), directory, manifest))
        except (OSError, ValueError, KeyError, TypeError):
            continue
    for _, directory, manifest in sorted(candidates, key=lambda row: row[0], reverse=True):
        source = directory / 'signals.db'
        try:
            if source.is_symlink() or sha256(source) != manifest['sha256']:
                continue
            with closing(readonly(source)) as conn:
                if conn.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                    continue
                if conn.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
                    continue
            return source, manifest
        except (OSError, sqlite3.Error, KeyError):
            continue
    return None


def _backup(source, target):
    deadline = time.monotonic() + 120

    def progress(status, remaining, total):
        if time.monotonic() > deadline:
            raise TimeoutError('analysis snapshot time limit')

    with closing(readonly(source)) as src, closing(sqlite3.connect(target)) as dst:
        src.backup(dst, pages=512, progress=progress, sleep=.05)


def _commit():
    value = os.getenv('RAILWAY_GIT_COMMIT_SHA') or os.getenv('GITHUB_SHA')
    if value:
        return value
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).parent,
            stderr=subprocess.DEVNULL, timeout=5, text=True).strip()
    except (OSError, subprocess.SubprocessError):
        return 'UNKNOWN'


def _copy(source, target, start, end):
    # ATTACH is explicitly read-only; all CREATE/INSERT operations target main.
    with closing(sqlite3.connect(target, uri=True)) as dst:
        dst.execute('ATTACH DATABASE ? AS source', (source.resolve().as_uri() + '?mode=ro',))
        schemas = dict(dst.execute("SELECT name,sql FROM source.sqlite_master WHERE type='table'"))
        missing = sorted(set(INCLUDED_TABLES) - schemas.keys())
        if missing:
            raise ValueError('Snapshot missing required analysis tables: ' + ', '.join(missing))
        for table in INCLUDED_TABLES:
            dst.execute(schemas[table])
        for table, (field, unit) in ROOTS.items():
            dst.execute(f'INSERT INTO main.{quote(table)} SELECT * FROM source.{quote(table)} '
                        f'WHERE {quote(field)} * ? >= ? AND {quote(field)} * ? <= ?',
                        (unit, start, unit, end))
        for table, (key, parent, parent_key, field, unit) in CHILDREN.items():
            sql = (f'INSERT INTO main.{quote(table)} SELECT * FROM source.{quote(table)} '
                   f'WHERE {quote(key)} IN (SELECT {quote(parent_key)} FROM main.{quote(parent)})')
            params = ()
            if field:
                sql += f' AND ({quote(field)} IS NULL OR {quote(field)} * ? <= ?)'
                params = (unit, end)
            dst.execute(sql, params)
        for table, last in EPISODES.items():
            if end < start:
                continue
            dst.execute(f'INSERT INTO main.{quote(table)} SELECT * FROM source.{quote(table)} '
                        f'WHERE start_ts * 1000 <= ? AND ({quote(last)} IS NULL OR {quote(last)} * 1000 >= ?)',
                        (end, start))
        version = dst.execute('PRAGMA source.user_version').fetchone()[0]
        dst.execute(f'PRAGMA user_version={version}')
        dst.commit()
        quick = [r[0] for r in dst.execute('PRAGMA quick_check')]
        integrity = [r[0] for r in dst.execute('PRAGMA integrity_check')]
        if quick != ['ok'] or integrity != ['ok']:
            raise ValueError('Analysis database integrity check failed')
        tables = {}
        for table in INCLUDED_TABLES:
            columns = [r[1] for r in dst.execute(f'PRAGMA table_info({quote(table)})')]
            # Explicit timestamp names/patterns avoid mislabelling age/horizon ms as dates.
            clocks = [c for c in columns if c == 'ts' or c.endswith(('_ts', '_ts_ms', '_time_ms'))
                      or c in ('last_observed_ms', 'matured_ts_ms', 'reclaim_since_ms')]
            ranges = {field: list(dst.execute(f'SELECT MIN({quote(field)}),MAX({quote(field)}) '
                                             f'FROM {quote(table)}').fetchone()) for field in clocks}
            tables[table] = {'rows': dst.execute(f'SELECT COUNT(*) FROM {quote(table)}').fetchone()[0],
                             'columns': columns, 'timestamp_ranges': ranges}
        return {'tables': tables, 'table_count': len(tables), 'schema_version': version,
                'quick_check': quick, 'integrity_check': integrity}


README = """Runner analysis export v1 (read-only research, not a restorable production DB)
Window: seven complete Europe/Istanbul calendar days plus today through the
earlier of request time and snapshot cutoff. No outcomes are fabricated.
Root events use their event/decision time; child data follows retained IDs.
All columns of the allowlisted research tables are retained, including JSON
features, reject reasons, IDs, episode/wave links and partial/mature 60m outcomes.
Episode summaries overlapping the window retain their original start/context.
References to events before the window retain their IDs, but those events are
not imported; do not interpret a missing boundary parent as a missing signal.
Pending outcomes remain pending: inspect completion/label/observation_gap fields.
Timestamp ranges use original units: *_ts and ts are seconds, *_ts_ms and
*_time_ms are milliseconds. Age/horizon/offset fields are durations, not dates.
Raw ticks, images, X content, account/trading state and unrelated telemetry are
excluded. Only table definitions/primary keys are copied; no triggers or views.
Manifest source size/hash refer to the consistent source SNAPSHOT, not a hash
of a concurrently changing production database. Backup fallback is temporary.
Existing scheduler snapshots and latestexport pointers are never changed.
"""


@contextmanager
def package(worker, now_ms=None):
    """Yield (ZIP path, manifest); clean all temporary files after send/failure.

    The process-wide lock covers construction and upload, bounding disk usage.
    A second command fails explicitly instead of queuing unbounded work.
    """
    if not _LOCK.acquire(blocking=False):
        raise RuntimeError('Analysis export already running')
    try:
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        local = datetime.fromtimestamp(now / 1000, IST)
        start = int((local.replace(hour=0, minute=0, second=0, microsecond=0)
                     - timedelta(days=7)).timestamp() * 1000)
        with tempfile.TemporaryDirectory(prefix='analysis-', dir=worker.root) as temporary:
            directory = Path(temporary)
            latest = _latest_valid(worker)
            if latest:
                source, snapshot = latest
                source_kind = 'VALID_SNAPSHOT'
            else:
                source = directory / 'source.db'
                _backup(worker.source, source)
                with closing(readonly(source)) as conn:
                    for check in ('quick_check', 'integrity_check'):
                        if conn.execute('PRAGMA ' + check).fetchall() != [('ok',)]:
                            raise ValueError('Backup snapshot failed ' + check)
                snapshot = {'snapshot_id': directory.name, 'created_time_ms': now}
                source_kind = 'READONLY_BACKUP'
            end = min(now, int(snapshot['created_time_ms']))
            target = directory / 'analysis.db'
            stats = _copy(source, target, start, end)
            manifest = {
                'export_version': EXPORT_VERSION,
                'created_at_utc': datetime.fromtimestamp(now / 1000, timezone.utc).isoformat(),
                'created_at_europe_istanbul': local.isoformat(),
                'timezone': 'Europe/Istanbul',
                'requested_range': {'start_ms': start, 'end_ms': now},
                'effective_range': {'start_ms': start, 'end_ms': end, 'empty': end < start},
                'source_db_path': str(worker.source), 'source_snapshot_path': str(source),
                'source_snapshot_id': snapshot['snapshot_id'], 'source_kind': source_kind,
                'source_snapshot_created_time_ms': snapshot['created_time_ms'],
                'source_sha256': sha256(source), 'source_size_bytes': source.stat().st_size,
                'analysis_sha256': sha256(target), 'analysis_size_bytes': target.stat().st_size,
                'git_commit': _commit(), 'code_version': worker.version,
                'deployment_id': worker.deployment, **stats,
            }
            (directory / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
            (directory / 'README.txt').write_text(README, encoding='utf-8')
            bundle = directory / 'analysis-export.zip'
            with zipfile.ZipFile(bundle, 'w', zipfile.ZIP_DEFLATED) as archive:
                for name in ('analysis.db', 'manifest.json', 'README.txt'):
                    archive.write(directory / name, name)
            yield bundle, manifest
    finally:
        _LOCK.release()


async def handle(bot, session, chat_id, user_id):
    """Private admin command; all blocking export work runs off the event loop."""
    async def notify(message):
        try:
            await bot['telegram_send'](session, message, chat_id=chat_id)
        except Exception as exc:
            bot['log'].error('Analysis export notification failed: %s', type(exc).__name__)

    if not bot['_at_admin_allowed'](chat_id, user_id):
        return True
    # Telegram private chat IDs are positive and equal to the sender's user ID.
    if not str(chat_id).isdigit() or int(chat_id) <= 0 or str(chat_id) != str(user_id):
        await notify('❌ /analysisexport yalnız özel admin sohbetinde kullanılabilir.')
        return True
    worker = bot['export_worker']
    if worker is None:
        await notify('Export kapalı: RESEARCH_EXPORT_ENABLED=0.')
        return True
    context = package(worker)
    build = asyncio.create_task(asyncio.to_thread(context.__enter__))
    entered = False
    try:
        try:
            bundle, manifest = await asyncio.shield(build)
            entered = True
        except asyncio.CancelledError:
            # Let a running disk operation finish before releasing its files/lock.
            try:
                await build
                entered = True
            finally:
                raise
        caption = ('Runner analysis: analysis.db + manifest + README | '
                   f"{manifest['source_size_bytes']:,} → {manifest['analysis_size_bytes']:,} "
                   f'→ {bundle.stat().st_size:,} bayt (source → analysis → ZIP)')
        await bot['telegram_send_document'](session, str(bundle), caption, chat_id=chat_id)
    except Exception as exc:
        bot['log'].error('Analysis export failed: %s', type(exc).__name__)
        await notify(f'❌ Analysis export paketi hazırlanamadı/gönderilemedi: {type(exc).__name__}. '
                     'Başka bir export çalışıyor olabilir; snapshot ve disk durumunu kontrol edin.')
    finally:
        if entered:
            try:
                await asyncio.to_thread(context.__exit__, None, None, None)
            except Exception as exc:
                bot['log'].error('Analysis export cleanup failed: %s', type(exc).__name__)
                await notify('❌ Analysis export geçici dosyaları temizlenemedi; disk durumunu kontrol edin.')
    return True
