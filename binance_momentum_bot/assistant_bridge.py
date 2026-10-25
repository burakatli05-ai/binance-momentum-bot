"""Compact read-only research bridge for ChatGPT/Railway log retrieval.

Snapshot callers retain their existing API. The live entry point reads one
bounded, read-only SQLite transaction, without creating a full DB backup.
Only an explicit Premium allowlist is serialized; the logical subset is not a
restorable production database.
"""
from contextlib import closing, contextmanager
import base64
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import zlib

BRIDGE_VERSION = "assistant-bridge-v1"
LOG_PREFIX = "ASSISTANT_BRIDGE_V1"
CHUNK_CHARS = 12000
PAGE_SIZE = 500
MAX_RAW_BYTES = 16 * 1024 ** 2
MAX_ROWS = 100000
MAX_SECONDS = 30

# These tables are intentionally small and sufficient for Premium/path research.
# Keep this list explicit: never export tables by wildcard/prefix.
INCLUDED_TABLES = (
    "signals_v2",
    "signal_outcomes",
    "signal_paths",
    "signal_meta",
    "premium_radar_links",
    "premium_wave_tracking",
    "premium_wave_events",
    "premium_context",
    "premium_exit_forward_shadow",
    "premium_delayed_entry_shadow",
)

KEYS = {name: ('signal_id',) for name in INCLUDED_TABLES}
KEYS.update(signals_v2=('id',), signal_outcomes=('signal_id', 'horizon_s'),
            premium_wave_events=('signal_id', 'wave_no'),
            premium_delayed_entry_shadow=('signal_id', 'strategy'))
REQUIRED_FIELDS = {
    'signals_v2': ('id', 'ts', 'symbol', 'level', 'price', 'score', 'episode_id',
                   'chg10', 'chg30', 'chg60', 'flow10', 'flow30', 'flow60',
                   'buy10', 'buy30', 'buy60'),
    'signal_meta': ('signal_id', 'premium', 'qv24', 'entry_quality', 'rise_score', 'candidate_runup'),
    'premium_context': ('signal_id', 'signal_generated_ts_ms', 'seconds_since_episode_peak'),
}


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'),
                      default=_json_default, allow_nan=False).encode('utf-8')


def _guard(checkpoint):
    deadline = time.monotonic() + MAX_SECONDS

    def check():
        checkpoint()
        if time.monotonic() > deadline:
            raise ValueError('PREMIUM_TIME_LIMIT')
    return check


@contextmanager
def _live_connection(path, check):
    """Preserve the actual guard reason when SQLite interrupts a long query."""
    interrupted = []

    def progress():
        try:
            check()
            return 0
        except Exception as exc:
            interrupted.append(exc)
            return 1

    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as conn:
        conn.execute('PRAGMA query_only=ON')
        conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_RAW_BYTES)
        conn.set_progress_handler(progress, 1000)
        try:
            yield conn
        except sqlite3.Error as exc:
            if interrupted:
                raise interrupted[0] from exc
            raise


def _page_records(columns, rows, keys):
    positions = [columns.index(key) for key in keys]
    result = []
    previous = None
    for offset in range(0, len(rows), PAGE_SIZE):
        page = rows[offset:offset + PAGE_SIZE]
        values = []
        for row in page:
            if len(row) != len(columns):
                raise ValueError('PREMIUM_ROW_WIDTH')
            key = tuple(row[i] for i in positions)
            if any(value is None for value in key) or (previous is not None and key <= previous):
                raise ValueError('PREMIUM_KEY_ORDER')
            previous = key
            values.append(key)
        result.append({'number': len(result) + 1, 'rows': len(page),
                       'first_key': list(values[0]), 'last_key': list(values[-1]),
                       'sha256': hashlib.sha256(_json_bytes(page)).hexdigest()})
    return result


def _reconcile(table, columns, rows, parent_ids):
    keys = KEYS[table]
    key_at = columns.index(keys[0])
    required = REQUIRED_FIELDS.get(table, keys)
    return {
        'row_count': len(rows), 'key_columns': list(keys),
        'pages': _page_records(columns, rows, keys),
        'null_counts': {column: sum(row[i] is None for row in rows)
                        for i, column in enumerate(columns)},
        'missing_required_columns': [field for field in required if field not in columns],
        'required_null_ids': {field: [row[key_at] for row in rows if row[columns.index(field)] is None]
                              for field in required if field in columns},
        'missing_parent_ids': sorted(parent_ids - {row[key_at] for row in rows})
                              if table != 'signals_v2' and len(keys) == 1 else [],
    }


def validate_live_payload(payload):
    """Reconcile the delivered rows, schema, fields and page seals; no DB access."""
    logical = payload['logical_export']
    tables = payload['tables']
    if (set(tables) != set(INCLUDED_TABLES) or set(logical['schema']) != set(INCLUDED_TABLES)
            or logical['logical_not_restorable'] is not True or logical['page_size'] != PAGE_SIZE
            or logical['kind'] != 'PREMIUM_READONLY_TRANSACTION'):
        raise ValueError('PREMIUM_SCOPE_MISMATCH')
    if hashlib.sha256(_json_bytes(logical['schema'])).hexdigest() != logical['schema_sha256']:
        raise ValueError('PREMIUM_SCHEMA_HASH')
    root = tables['signals_v2']
    id_at, level_at = root['columns'].index('id'), root['columns'].index('level')
    ids = {row[id_at] for row in root['rows']}
    if (len(ids) != logical['premium_count'] or max(ids, default=0) != logical['max_signal_id'] or
            any(not isinstance(value, int) or value > logical['max_signal_id'] for value in ids) or
            any(row[level_at] != 'CONFIRMED' for row in root['rows'])):
        raise ValueError('PREMIUM_ROOT_MISMATCH')
    total = 0
    for table in INCLUDED_TABLES:
        data = tables[table]
        if data['columns'] != [column['name'] for column in logical['schema'][table]['columns']]:
            raise ValueError('PREMIUM_SCHEMA_COLUMNS')
        expected = _reconcile(table, data['columns'], data['rows'], ids)
        stored = payload['reconciliation'][table]
        if any(stored.get(key) != value for key, value in expected.items()):
            raise ValueError('PREMIUM_RECONCILIATION_MISMATCH')
        if stored['source_row_count'] != len(data['rows']):
            raise ValueError('PREMIUM_COUNT_MISMATCH')
        if table != 'signals_v2' and any(row[data['columns'].index('signal_id')] not in ids for row in data['rows']):
            raise ValueError('PREMIUM_CHILD_MISMATCH')
        orphans = stored['orphan_keys']
        if (stored['orphan_count'] != len(orphans) or stored['orphan_source_count'] != len(orphans) or
                any(key[0] in ids or key[0] > logical['max_signal_id'] for key in orphans) or
                any(tuple(a) >= tuple(b) for a, b in zip(orphans, orphans[1:]))):
            raise ValueError('PREMIUM_ORPHAN_MISMATCH')
        total += len(data['rows']) + len(orphans)
    if total != logical['total_rows'] or total > MAX_ROWS:
        raise ValueError('PREMIUM_ROW_LIMIT')
    if len(_json_bytes(payload)) > MAX_RAW_BYTES:
        raise ValueError('PREMIUM_BYTE_LIMIT')
    return True


def build_live_payload(source_path, manifest, checkpoint=lambda: None):
    """Read only the existing Premium rows from one pinned SQLite transaction."""
    path = Path(source_path).resolve()
    if not path.is_file():
        raise ValueError('source missing')
    check = _guard(checkpoint)
    budget = {'rows': 0, 'bytes': 0}

    def pages(conn, table, columns, where, params):
        result, after = [], None
        keys = KEYS[table]
        order = ','.join(_quote(key) for key in keys)
        while True:
            check()
            tail, values = '', list(params)
            if after is not None:
                tail = ' AND (' + order + ') > (' + ','.join('?' for _ in keys) + ')'
                values.extend(after)
            cursor = conn.execute('SELECT ' + ','.join(_quote(c) for c in columns) +
                                  ' FROM ' + _quote(table) + ' WHERE ' + where + tail +
                                  ' ORDER BY ' + order + ' LIMIT ?', (*values, PAGE_SIZE))
            page = []
            for raw in cursor:
                check()
                row = list(raw)
                budget['rows'] += 1
                budget['bytes'] += len(_json_bytes(row)) + 1
                if budget['rows'] > MAX_ROWS:
                    raise ValueError('PREMIUM_ROW_LIMIT')
                if budget['bytes'] > MAX_RAW_BYTES:
                    raise ValueError('PREMIUM_BYTE_LIMIT')
                page.append(row)
            if not page:
                break
            result.extend(page)
            after = tuple(page[-1][columns.index(key)] for key in keys)
            if any(value is None for value in after):
                raise ValueError('PREMIUM_NULL_KEY')
            if len(page) < PAGE_SIZE:
                break
        return result

    with _live_connection(path, check) as conn:
        conn.execute('BEGIN')
        check()
        schema_rows = conn.execute('SELECT name,sql FROM sqlite_master WHERE type=\'table\' '
                                   'AND name IN (' + ','.join('?' for _ in INCLUDED_TABLES) + ')',
                                   INCLUDED_TABLES).fetchall()
        read_at = int(time.time() * 1000)
        schemas = dict(schema_rows)
        if set(schemas) != set(INCLUDED_TABLES):
            raise ValueError('PREMIUM_MISSING_TABLE: ' + ','.join(sorted(set(INCLUDED_TABLES) - set(schemas))))
        schema = {}
        for table in INCLUDED_TABLES:
            info = conn.execute('PRAGMA table_info(' + _quote(table) + ')').fetchall()
            columns = [{'name': r[1], 'type': r[2], 'notnull': r[3], 'default': r[4], 'pk': r[5]} for r in info]
            keys = tuple(r[1] for r in sorted(info, key=lambda r: r[5]) if r[5])
            if keys != KEYS[table] or (table == 'signals_v2' and 'level' not in {r[1] for r in info}):
                raise ValueError('PREMIUM_SCHEMA_KEY')
            schema[table] = {'sql': schemas[table], 'columns': columns}
        maximum, count = conn.execute("SELECT COALESCE(MAX(id),0),COUNT(*) FROM signals_v2 WHERE level='CONFIRMED'").fetchone()
        root_where = "level='CONFIRMED' AND id<=?"
        child_where = "signal_id IN (SELECT id FROM signals_v2 WHERE level='CONFIRMED' AND id<=?)"
        tables, reconciliation = {}, {}
        for table in INCLUDED_TABLES:
            columns = [c['name'] for c in schema[table]['columns']]
            if table != 'signals_v2' and conn.execute(
                    'SELECT 1 FROM ' + _quote(table) + ' WHERE signal_id IS NULL LIMIT 1').fetchone():
                raise ValueError('PREMIUM_NULL_PARENT_KEY: ' + table)
            where = root_where if table == 'signals_v2' else child_where
            source_count = conn.execute('SELECT COUNT(*) FROM ' + _quote(table) + ' WHERE ' + where, (maximum,)).fetchone()[0]
            rows = pages(conn, table, columns, where, (maximum,))
            if len(rows) != source_count:
                raise ValueError('PREMIUM_COUNT_MISMATCH')
            tables[table] = {'columns': columns, 'rows': rows}
            if table == 'signals_v2':
                ids = {row[columns.index('id')] for row in rows}
            stats = _reconcile(table, columns, rows, ids)
            stats['source_row_count'] = source_count
            orphan_where = 'signal_id<=? AND NOT EXISTS (SELECT 1 FROM signals_v2 WHERE id=' + _quote(table) + '.signal_id)'
            orphan_count = 0 if table == 'signals_v2' else conn.execute(
                'SELECT COUNT(*) FROM ' + _quote(table) + ' WHERE ' + orphan_where, (maximum,)).fetchone()[0]
            orphans = [] if table == 'signals_v2' else pages(conn, table, list(KEYS[table]), orphan_where, (maximum,))
            if len(orphans) != orphan_count:
                raise ValueError('PREMIUM_ORPHAN_COUNT_MISMATCH')
            stats.update(orphan_keys=orphans, orphan_count=len(orphans), orphan_source_count=orphan_count)
            reconciliation[table] = stats
        check()
        payload = {
            'bridge_version': BRIDGE_VERSION,
            'snapshot_id': manifest.get('snapshot_id', 'UNKNOWN'),
            'snapshot_created_time_ms': read_at,
            'deployment_id': manifest.get('deployment_id', 'UNKNOWN'),
            'source_sha256': None,
            'source_identity': {key: manifest[key] for key in ('version', 'git_commit', 'module_sha256',
                                                             'source_code_sha256', 'config_identity',
                                                             'configidentity') if key in manifest},
            'logical_export': {'kind': 'PREMIUM_READONLY_TRANSACTION', 'logical_not_restorable': True,
                               'read_at_ms': read_at, 'read_completed_at_ms': int(time.time() * 1000),
                               'max_signal_id': maximum, 'premium_count': count,
                               'total_rows': budget['rows'], 'page_size': PAGE_SIZE,
                               'schema': schema, 'schema_sha256': hashlib.sha256(_json_bytes(schema)).hexdigest(),
                               'orphan_scope': 'signal_id <= max_signal_id and no physical signals_v2 parent at this cut',
                               'data_completeness': 'UNKNOWN; package integrity is not persistence assurance'},
            'tables': tables, 'reconciliation': reconciliation,
        }
        conn.rollback()
    check()
    validate_live_payload(payload)
    check()
    return payload


def emit_live(source_path, manifest, printer=print, checkpoint=lambda: None):
    """Emit the independent logical export only after reconciliation and roundtrip."""
    check = _guard(checkpoint)
    payload = build_live_payload(source_path, manifest, checkpoint=check)
    validate_live_payload(payload)
    raw, compressed, digest, chunks = encode_payload(payload)
    if len(raw) > MAX_RAW_BYTES:
        raise ValueError('PREMIUM_BYTE_LIMIT')
    if decode_chunks(chunks, digest) != json.loads(raw.decode('utf-8')):
        raise ValueError('PREMIUM_ROUNDTRIP_MISMATCH')
    check()
    return _emit_encoded(payload, raw, compressed, digest, chunks, printer, check)


def _quote(name):
    return '"' + str(name).replace('"', '""') + '"'


def _json_default(value):
    if isinstance(value, bytes):
        return {"__bytes_b64__": base64.b64encode(value).decode("ascii")}
    raise TypeError(type(value).__name__)


def build_payload(snapshot_path, manifest):
    """Return a compact, lossless payload from a verified immutable snapshot."""
    path = Path(snapshot_path).resolve()
    if not path.is_file():
        raise ValueError("snapshot missing")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as conn:
        existing = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        missing = [name for name in INCLUDED_TABLES if name not in existing]
        if missing:
            raise ValueError("assistant bridge missing tables: " + ", ".join(missing))
        tables = {}
        for table in INCLUDED_TABLES:
            q = _quote(table)
            columns = [row[1] for row in conn.execute(f"PRAGMA table_info({q})")]
            rows = [list(row) for row in conn.execute(f"SELECT * FROM {q}")]
            tables[table] = {"columns": columns, "rows": rows}
    return {
        "bridge_version": BRIDGE_VERSION,
        "snapshot_id": manifest.get("snapshot_id", "UNKNOWN"),
        "snapshot_created_time_ms": manifest.get("created_time_ms"),
        "deployment_id": manifest.get("deployment_id", "UNKNOWN"),
        "source_sha256": manifest.get("sha256"),
        "tables": tables,
    }


def encode_payload(payload):
    raw = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), default=_json_default
    ).encode("utf-8")
    compressed = zlib.compress(raw, 9)
    encoded = base64.b64encode(compressed).decode("ascii")
    digest = hashlib.sha256(raw).hexdigest()
    chunks = [encoded[i:i + CHUNK_CHARS] for i in range(0, len(encoded), CHUNK_CHARS)] or [""]
    return raw, compressed, digest, chunks


def emit(snapshot_path, manifest, printer=print):
    """Emit a reconstructable payload to stdout in bounded log lines."""
    payload = build_payload(snapshot_path, manifest)
    raw, compressed, digest, chunks = encode_payload(payload)
    return _emit_encoded(payload, raw, compressed, digest, chunks, printer)


def _emit_encoded(payload, raw, compressed, digest, chunks, printer, checkpoint=lambda: None):
    sid = str(payload["snapshot_id"])
    meta = {
        "bridge_version": BRIDGE_VERSION,
        "snapshot_id": sid,
        "chunks": len(chunks),
        "raw_bytes": len(raw),
        "compressed_bytes": len(compressed),
        "sha256": digest,
        "tables": {name: len(data["rows"]) for name, data in payload["tables"].items()},
    }
    if 'logical_export' in payload:
        meta.update(source_kind=payload['logical_export']['kind'], logical_not_restorable=True,
                    max_signal_id=payload['logical_export']['max_signal_id'],
                    premium_count=payload['logical_export']['premium_count'])
    checkpoint()
    printer(f"{LOG_PREFIX} META " + json.dumps(meta, separators=(",", ":")), flush=True)
    for index, chunk in enumerate(chunks, 1):
        checkpoint()
        printer(f"{LOG_PREFIX} CHUNK {sid} {index}/{len(chunks)} {chunk}", flush=True)
    checkpoint()
    printer(f"{LOG_PREFIX} END {sid} {digest}", flush=True)
    return meta


def decode_chunks(chunks, expected_sha256=None):
    """Test/recovery helper used by the consumer to reconstruct a logged payload."""
    encoded = "".join(chunks)
    compressed = base64.b64decode(encoded.encode("ascii"), validate=True)
    decoder = zlib.decompressobj()
    raw = decoder.decompress(compressed) + decoder.flush()
    if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError('assistant bridge incomplete or trailing compressed data')
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha256 and digest != expected_sha256:
        raise ValueError("assistant bridge sha256 mismatch")
    return json.loads(raw.decode("utf-8"))
