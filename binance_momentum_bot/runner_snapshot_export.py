"""Explicit snapshot-only research export. No bot import or live DB fallback."""
import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
import zipfile

from analysis_export import quote, sha256

VERSION = 'runner-snapshot-export-v1'
ROOT = Path('/data/research_exports')
SNAPSHOT_ID = '20260916T190728Z-6cf5b8ff'
# Columns are explicit too: new columns, arbitrary JSON and notification state
# must never become part of this research package through schema evolution.
FIELDS = {
    'runner_score_v1_shadow': 'id source_key symbol stage event_ts_ms episode_id signal_id price score model_version mfe_60_pct mae_60_pct last_observed_ms observation_gap outcome_label outcome_ts_ms'.split(),
    'runner_watch_v1_shadow': 'watch_id parent_watch_id score_id symbol kind anchor_ts_ms anchor_price anchor_score expires_ts_ms outcome_due_ts_ms state decision decision_ts_ms decision_price mfe_pct mae_pct last_observed_ms observation_gap classic_premium_ts_ms created_ts_ms updated_ts_ms'.split(),
    'runner_watch_outcome_v1_shadow': 'watch_id matured_ts_ms mfe_pct mae_pct label observation_gap model_version'.split(),
    'non_runner_veto_v1_shadow': 'watch_id horizon_s observed_ts_ms price return_pct mfe_pct mae_pct decision model_version'.split(),
    'signals_v2': 'id ts symbol level score price episode_id'.split(),
    'signal_meta': 'signal_id premium'.split(),
    'signal_outcomes': 'signal_id horizon_s return_pct mfe_pct mae_pct ts'.split(),
}
README = '''Snapshot-only runner research export v1
payload.json contains ONLY explicitly listed research columns and tables.
All rows of these tables in the selected snapshot are retained; no wall-clock
window is applied. Manifest creation time is not an exact observation cutoff.
Source database is opened with mode=ro&immutable=1 and query_only=ON.
No live DB fallback, bot startup, Telegram delivery or retention operation.
The source manifest is NOT copied (it contains unrelated metadata).
Raw JSON features, reason JSON and notification state are deliberately omitted.
signals_v2 + signal_meta provide Premium identity; join by signal_id/id and
episode_id. Symbol/time-only matches do not prove the same episode.
Watch and watch-outcome MFE/MAE are anchor-based, NOT post-ALLOW performance.
Decision timestamps/prices do not constitute a post-decision price path.
Post-ALLOW MFE/MAE cannot be calculated from this package alone.
Pending outcomes and observation gaps must not be counted as successes.
Timestamp units: ts=Unix seconds; *_ts_ms and last_observed_ms=Unix milliseconds.
Durations such as horizon_s are not timestamps. NULL ranges mean no observations.
Verify SHA-256 of payload.json against manifest.payload_sha256 before analysis.
'''


def checked_path(path):
    """Reject traversal, links/junctions in any component, and linked files."""
    path = Path(path)
    if '..' in path.parts:
        raise ValueError('Path traversal refused')
    path = path.absolute()
    for component in reversed((path, *path.parents)):
        info = component.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise ValueError('Symlink/reparse path refused')
    info = path.stat()
    if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
        raise ValueError('Hard-linked file refused')
    return path


def source_files(snapshot_id, root):
    if not re.fullmatch(r'\d{8}T\d{6}Z-[0-9a-f]{8}', snapshot_id):
        raise ValueError('Invalid snapshot ID (database paths are not accepted)')
    root = checked_path(root)
    directory = checked_path(root / snapshot_id)
    source = checked_path(directory / 'signals.db')
    manifest_path = checked_path(directory / 'manifest.json')
    if not source.is_file() or not manifest_path.is_file():
        raise ValueError('Snapshot requires regular files')
    for suffix in ('-wal', '-shm', '-journal'):
        if os.path.lexists(str(source) + suffix):
            raise ValueError('Snapshot sidecar refused')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    if manifest.get('valid') is not True or manifest.get('snapshot_id') != snapshot_id:
        raise ValueError('Valid matching snapshot required')
    for check in ('quick_check', 'integrity_check', 'restore_smoke_check'):
        if manifest.get(check) != ['ok']:
            raise ValueError('Snapshot check missing or failed: ' + check)
    digest = manifest.get('sha256', '')
    if not re.fullmatch('[0-9a-f]{64}', digest) or sha256(source) != digest:
        raise ValueError('Snapshot hash mismatch')
    return source, manifest


def export_snapshot(snapshot_id, output, *, root=ROOT):
    """root is injectable for isolated tests; CLI always uses the fixed root.

    Snapshot directories must be operator-controlled and immutable throughout
    execution. Checks reject pre-existing links; this is not a hostile-filesystem
    sandbox against a privileged concurrent directory-replacement attacker.
    """
    source, source_manifest = source_files(snapshot_id, root)
    output = Path(output).absolute()
    parent = checked_path(output.parent)
    if output.suffix != '.zip' or '..' in output.parts:
        raise ValueError('Output must be a new ZIP')
    if parent == checked_path(root) or checked_path(root) in parent.parents:
        raise ValueError('Output must be outside the snapshot tree')
    if os.path.lexists(output):
        raise ValueError('Output already exists')
    tables, stats = {}, {}
    with closing(sqlite3.connect(source.as_uri() + '?mode=ro&immutable=1', uri=True)) as conn:
        conn.execute('PRAGMA query_only=ON')
        conn.execute('PRAGMA trusted_schema=OFF')
        for check in ('quick_check', 'integrity_check'):
            if conn.execute('PRAGMA ' + check).fetchall() != [('ok',)]:
                raise ValueError('Source failed ' + check)
        version = conn.execute('PRAGMA user_version').fetchone()[0]
        for table, fields in FIELDS.items():
            kind = conn.execute('SELECT type,sql FROM sqlite_master WHERE name=?', (table,)).fetchone()
            if not kind or kind[0] != 'table' or not kind[1].lstrip().upper().startswith('CREATE TABLE'):
                raise ValueError('Required ordinary table missing: ' + table)
            schema = {r[1]: r[2] for r in conn.execute('PRAGMA table_info(' + quote(table) + ')')}
            if not set(fields) <= schema.keys():
                raise ValueError('Required columns missing: ' + table)
            selection = ','.join(map(quote, fields))
            rows = conn.execute('SELECT ' + selection + ' FROM ' + quote(table) + ' ORDER BY ' + selection).fetchall()
            expected = source_manifest.get('tables', {}).get(table, {}).get('rows')
            if type(expected) is not int or len(rows) != expected:
                raise ValueError('Manifest row count mismatch: ' + table)
            tables[table] = {'columns': fields, 'rows': rows}
            clocks = [f for f in fields if f == 'ts' or f.endswith('_ts_ms') or f == 'last_observed_ms']
            ranges = {}
            for field in clocks:
                values = [row[fields.index(field)] for row in rows if row[fields.index(field)] is not None]
                ranges[field] = {'min': min(values) if values else None, 'max': max(values) if values else None,
                                 'unit': 'seconds' if field == 'ts' else 'milliseconds'}
            stats[table] = {'rows': len(rows), 'columns': {f: schema[f] for f in fields}, 'timestamp_ranges': ranges}
    # Revalidate both manifest and DB before publishing anything.
    after_source, after_manifest = source_files(snapshot_id, root)
    if after_source != source or after_manifest != source_manifest:
        raise ValueError('Snapshot changed during export')
    payload = json.dumps({'export_version': VERSION, 'tables': tables}, ensure_ascii=False,
                         separators=(',', ':'), allow_nan=False).encode('utf-8')
    manifest = {'export_version': VERSION, 'schema_version': version, 'snapshot_id': snapshot_id,
                'source_sha256': source_manifest['sha256'], 'source_hash_unchanged': True,
                'source_snapshot_created_time_ms': source_manifest['created_time_ms'],
                'payload_sha256': hashlib.sha256(payload).hexdigest(), 'payload_bytes': len(payload),
                'tables': stats, 'table_count': len(stats), 'post_allow_path_available': False}
    # Build in a private temporary directory; exclusive creation never overwrites
    # an existing output or follows an output symlink.
    with tempfile.TemporaryDirectory(prefix='runner-export-', dir=parent) as temporary:
        bundle = Path(temporary) / 'export.zip'
        with zipfile.ZipFile(bundle, 'w', zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('payload.json', payload)
            archive.writestr('manifest.json', json.dumps(manifest, indent=2))
            archive.writestr('README.txt', README)
        checked_path(parent)
        with output.open('xb') as dst:
            try:
                with bundle.open('rb') as src:
                    import shutil
                    shutil.copyfileobj(src, dst)
            except BaseException:
                dst.close()
                output.unlink()
                raise
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-id', default=SNAPSHOT_ID)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    manifest = export_snapshot(args.snapshot_id, args.output)
    print(json.dumps({'output': args.output, 'snapshot_id': manifest['snapshot_id'],
                      'payload_sha256': manifest['payload_sha256'],
                      'row_counts': {k: v['rows'] for k, v in manifest['tables'].items()}}))


if __name__ == '__main__':
    main()
