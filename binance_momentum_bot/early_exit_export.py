"""Bounded read-only export of Public Early stage rows for offline exit research.

Never writes the production database and never changes scanner decisions. The payload
contains only the minimum stage-entry fields needed to join Public Early entries to
public historical market data in a separate research environment.
"""
from contextlib import closing
import base64
import hashlib
import json
import math
import os
import platform
import re
import time
import uuid
from pathlib import Path
import sqlite3
import zlib

VERSION = "early-exit-export-v1"
LOG_PREFIX = "EARLY_EXIT_EXPORT_V1"
CHUNK_CHARS = 12000
START_TS_MS = 1790629200000  # 2026-09-29 00:00 TRT
END_TS_MS = 1791234000000    # 2026-10-06 00:00 TRT
MIN_STAGE_ID = 282763
MAX_STAGE_ID = 373429
MAX_ROWS = 5000
MAX_RAW_BYTES = 2_000_000

COLUMNS = (
    "id","symbol","episode_id","stage","signal_id","decision","created_ts_ms",
    "entry_age_s","entry_price","stop_price","tp1_price","tp2_price",
    "mfe_pct","mae_pct","close60_price","completed_60m"
)


def _readonly(path):
    conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    conn.execute("PRAGMA query_only=ON")
    return conn


def build_payload(db_path, start_ts_ms=START_TS_MS, end_ts_ms=END_TS_MS):
    """Read the exact Public Early stage cohort for one closed window."""
    with closing(_readonly(db_path)) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='entry_stage_forward_shadow'"
        ).fetchone()
        if not exists:
            raise ValueError("entry_stage_forward_shadow missing")
        observed = {row[1] for row in conn.execute("PRAGMA table_info(entry_stage_forward_shadow)")}
        missing = [name for name in COLUMNS if name not in observed]
        if missing:
            raise ValueError("early export missing columns: " + ", ".join(missing))
        select = ",".join('"' + c.replace('"','""') + '"' for c in COLUMNS)
        rows = [
            list(row) for row in conn.execute(
                f"""SELECT {select}
                    FROM entry_stage_forward_shadow
                    WHERE id BETWEEN ? AND ?
                      AND stage='EARLY'
                      AND created_ts_ms>=? AND created_ts_ms<?
                    ORDER BY created_ts_ms,id""",
                (MIN_STAGE_ID, MAX_STAGE_ID, int(start_ts_ms), int(end_ts_ms)),
            )
        ]
        if len(rows) > MAX_ROWS:
            raise ValueError("early export row budget exceeded")
    return {
        "version": VERSION,
        "window_start_ts_ms": int(start_ts_ms),
        "window_end_ts_ms": int(end_ts_ms),
        "stage": "EARLY",
        "columns": list(COLUMNS),
        "rows": rows,
    }


def encode_payload(payload):
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(raw) > MAX_RAW_BYTES:
        raise ValueError("early export byte budget exceeded")
    compressed = zlib.compress(raw, 9)
    encoded = base64.b64encode(compressed).decode("ascii")
    digest = hashlib.sha256(raw).hexdigest()
    chunks = [encoded[i:i + CHUNK_CHARS] for i in range(0, len(encoded), CHUNK_CHARS)] or [""]
    return raw, compressed, digest, chunks


def emit(db_path, printer=print, start_ts_ms=START_TS_MS, end_ts_ms=END_TS_MS):
    selector = os.getenv(H1_SELECTOR, "")
    if selector:
        if selector != H1_REQUEST_ID:
            raise ValueError("H1_UNKNOWN_REQUEST")
        return emit_h1(db_path, printer)
    payload = build_payload(db_path, start_ts_ms, end_ts_ms)
    raw, compressed, digest, chunks = encode_payload(payload)
    meta = {
        "version": VERSION,
        "window_start_ts_ms": payload["window_start_ts_ms"],
        "window_end_ts_ms": payload["window_end_ts_ms"],
        "rows": len(payload["rows"]),
        "chunks": len(chunks),
        "raw_bytes": len(raw),
        "compressed_bytes": len(compressed),
        "sha256": digest,
    }
    printer(LOG_PREFIX + " META " + json.dumps(meta, separators=(",", ":")), flush=True)
    for index, chunk in enumerate(chunks, 1):
        printer(f"{LOG_PREFIX} CHUNK {index}/{len(chunks)} {chunk}", flush=True)
    printer(f"{LOG_PREFIX} END {digest}", flush=True)
    return meta


# H1-only opt-in. Bounds/schema are code-reviewed constants, never caller SQL.
H1_REQUEST_ID = "h1-checkpoint-cut-20261006T180000Z-v1"
H1_SELECTOR = "H1_BOUNDED_EXPORT_REQUEST_ID"
H1_VERSION = "h1-bounded-export-v1"
H1_LOG_PREFIX = "H1_CHECKPOINT_EXPORT_V1"
H1_DB_PATH = "/data/signals.db"
H1_TABLE = "early_checkpoint_latency_shadow_v1"
H1_START_MS = 1791147600000
H1_END_MS = 1791309600000
H1_EXPIRES_MS = 1791406800000
H1_SQL_SECONDS = 5.0
# name, declared type, PRAGMA notnull, default SQL, primary-key position.
H1_SCHEMA = (
    ("episode_id", "INTEGER", 0, None, 1),
    ("symbol", "TEXT", 1, None, 0),
    ("candidate_start_ts_ms", "INTEGER", 1, None, 0),
    ("first_pass2_ts_ms", "INTEGER", 0, None, 0),
    ("first_ready_ts_ms", "INTEGER", 0, None, 0),
    ("first_ready_price", "REAL", 0, None, 0),
    ("first_ready_confirm_passes", "INTEGER", 0, None, 0),
    ("prior_eval_ts_ms", "INTEGER", 0, None, 0),
    ("prior_failed_gates_json", "TEXT", 1, "'[]'", 0),
    ("would_create_radar", "INTEGER", 0, None, 0),
    ("actual_early_ts_ms", "INTEGER", 0, None, 0),
    ("actual_early_price", "REAL", 0, None, 0),
    ("actual_confirm_passes", "INTEGER", 0, None, 0),
    ("seconds_saved", "REAL", 0, None, 0),
    ("price_saved_pct", "REAL", 0, None, 0),
    ("end_ts_ms", "INTEGER", 0, None, 0),
    ("end_reason", "TEXT", 0, None, 0),
    ("status", "TEXT", 1, None, 0),
    ("restart_gap", "INTEGER", 1, "0", 0),
    ("version", "TEXT", 1, None, 0),
    ("updated_ts_ms", "INTEGER", 1, None, 0),
)
H1_COLUMNS = tuple(field[0] for field in H1_SCHEMA)
H1_SELECT = """SELECT
    episode_id,symbol,candidate_start_ts_ms,first_pass2_ts_ms,
    first_ready_ts_ms,first_ready_price,first_ready_confirm_passes,
    prior_eval_ts_ms,prior_failed_gates_json,would_create_radar,
    actual_early_ts_ms,actual_early_price,actual_confirm_passes,
    seconds_saved,price_saved_pct,end_ts_ms,end_reason,status,
    restart_gap,version,updated_ts_ms
    FROM early_checkpoint_latency_shadow_v1
    WHERE candidate_start_ts_ms>=? AND candidate_start_ts_ms<?
    ORDER BY candidate_start_ts_ms,episode_id LIMIT ?"""


def _h1_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _h1_time():
    stamp = time.time_ns() // 1_000_000
    if not H1_END_MS <= stamp < H1_EXPIRES_MS:
        raise ValueError("H1_OUTSIDE_ACTIVATION_WINDOW")
    return stamp


def _h1_path(db_path):
    path = Path(db_path).resolve()
    if path != Path(H1_DB_PATH) or not path.is_file():
        raise ValueError("H1_SOURCE_PATH")
    return str(path)


def _h1_row(row):
    if len(row) != len(H1_SCHEMA):
        raise ValueError("H1_ROW_WIDTH")
    for value, (name, kind, required, _, pk) in zip(row, H1_SCHEMA):
        if value is None:
            if required or pk:
                raise ValueError("H1_REQUIRED_NULL:" + name)
        elif kind == "INTEGER":
            if type(value) is not int:
                raise ValueError("H1_STORAGE_CLASS:" + name)
        elif kind == "REAL":
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError("H1_STORAGE_CLASS:" + name)
        elif type(value) is not str:
            raise ValueError("H1_STORAGE_CLASS:" + name)
    return list(row)


def build_h1_payload(db_path, *, attempt_id):
    """One consistent H1 read; no write-capable bot connector or migration."""
    path = _h1_path(db_path)
    started = _h1_time()
    if not re.fullmatch(r"[0-9a-f]{32}", attempt_id):
        raise ValueError("H1_ATTEMPT_ID")
    commit = os.getenv("RAILWAY_GIT_COMMIT_SHA", "")
    deployment = os.getenv("RAILWAY_DEPLOYMENT_ID", "")
    payload = {
        "version": H1_VERSION, "mode": "H1", "request_id": H1_REQUEST_ID,
        "attempt_id": attempt_id, "table": H1_TABLE, "source_db_path": path,
        "source_commit": commit if re.fullmatch(r"[0-9a-f]{40}", commit) else "UNKNOWN",
        "deployment_id": deployment if re.fullmatch(r"[0-9a-f-]{36}", deployment) else "UNKNOWN",
        "exporter_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python_version": platform.python_version(), "sqlite_version": sqlite3.sqlite_version,
        "window_field": "candidate_start_ts_ms",
        "window_start_ts_ms": H1_START_MS, "window_end_ts_ms": H1_END_MS,
        "selection_end_exclusive": True, "snapshot_semantics": "OBSERVED_AT_READ_NOT_HISTORICAL_ASOF",
        "old_270_cohort_reconstructed": False,
        "max_rows": MAX_ROWS, "max_raw_bytes": MAX_RAW_BYTES,
        "columns": list(H1_COLUMNS), "schema": [list(x) for x in H1_SCHEMA],
        "schema_sha256": hashlib.sha256(_h1_json(H1_SCHEMA)).hexdigest(),
        "read_start_ts_ms": started, "snapshot_acquisition_before_ns": None,
        "snapshot_acquisition_after_ns": None, "read_end_ts_ms": None,
        "timestamp_unit": "unix_ms", "timestamp_precision": "source_integer_ms",
        "timestamp_ranges": {}, "row_count": 0, "rows": [],
    }
    deadline = time.monotonic() + H1_SQL_SECONDS

    def check_deadline():
        if time.monotonic() >= deadline:
            raise TimeoutError("H1_SQL_DEADLINE")

    with closing(_readonly(path)) as conn:
        conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 10000)
        conn.execute("BEGIN")
        payload["snapshot_acquisition_before_ns"] = time.time_ns()
        kind = conn.execute(
            "SELECT type,sql FROM sqlite_master WHERE name='early_checkpoint_latency_shadow_v1'"
        ).fetchone()
        payload["snapshot_acquisition_after_ns"] = time.time_ns()
        if (kind is None or kind[0] != "table" or not kind[1]
                or not kind[1].lstrip().upper().startswith("CREATE TABLE")
                or re.search(r"\bGENERATED\b", kind[1], re.IGNORECASE)):
            raise ValueError("H1_ORDINARY_TABLE_REQUIRED")
        observed = list(conn.execute("PRAGMA table_info(early_checkpoint_latency_shadow_v1)"))
        expected = [(i, *field) for i, field in enumerate(H1_SCHEMA)]
        if observed != expected:
            raise ValueError("H1_SCHEMA_MISMATCH")
        check_deadline()
        # Conservative incremental byte count, followed by an exact final check.
        estimated_bytes = len(_h1_json(payload))
        clock_indices = [i for i, name in enumerate(H1_COLUMNS) if name.endswith("_ts_ms")]
        ranges = {H1_COLUMNS[i]: [None, None] for i in clock_indices}
        previous = None
        with closing(conn.execute(H1_SELECT, (H1_START_MS, H1_END_MS, MAX_ROWS + 1))) as cursor:
            for row in cursor:
                check_deadline()
                if len(payload["rows"]) == MAX_ROWS:
                    raise ValueError("H1_ROW_CAP_EXCEEDED")
                row = _h1_row(row)
                order = (row[2], row[0])
                if not H1_START_MS <= row[2] < H1_END_MS or (previous is not None and order <= previous):
                    raise ValueError("H1_ORDER_OR_WINDOW")
                previous = order
                estimated_bytes += len(_h1_json(row)) + 1
                if estimated_bytes > MAX_RAW_BYTES:
                    raise ValueError("H1_BYTE_CAP_EXCEEDED")
                payload["rows"].append(row)
                for i in clock_indices:
                    value = row[i]
                    if value is not None:
                        span = ranges[H1_COLUMNS[i]]
                        span[0] = value if span[0] is None else min(span[0], value)
                        span[1] = value if span[1] is None else max(span[1], value)
        check_deadline()
        payload["timestamp_ranges"] = ranges
        payload["row_count"] = len(payload["rows"])
    # The connection/read transaction is closed before encode/compress/log delivery.
    payload["read_end_ts_ms"] = _h1_time()
    if not payload["rows"]:
        raise ValueError("H1_EMPTY_WINDOW")
    if len(_h1_json(payload)) > MAX_RAW_BYTES:
        raise ValueError("H1_BYTE_CAP_EXCEEDED")
    return payload


def emit_h1(db_path, printer=print):
    """Bounded log transport. Each attempt is distinct; no persistent marker."""
    _h1_path(db_path)
    _h1_time()
    attempt = uuid.uuid4().hex
    identity = {"version": H1_VERSION, "mode": "H1",
                "request_id": H1_REQUEST_ID, "attempt_id": attempt}
    def line(kind, fields):
        printer(H1_LOG_PREFIX + " " + kind + " " +
                json.dumps(fields, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                flush=True)
    line("START", identity)
    try:
        payload = build_h1_payload(db_path, attempt_id=attempt)
        raw, compressed, digest, chunks = encode_payload(payload)
        _h1_time()
        meta = dict(identity, table=H1_TABLE, window_start_ts_ms=H1_START_MS,
                    window_end_ts_ms=H1_END_MS, schema_sha256=payload["schema_sha256"],
                    columns=list(H1_COLUMNS), rows=payload["row_count"],
                    raw_bytes=len(raw), compressed_bytes=len(compressed),
                    chunks=len(chunks), sha256=digest)
        line("META", meta)
        for index, chunk in enumerate(chunks, 1):
            _h1_time()
            printer(f"{H1_LOG_PREFIX} CHUNK {H1_REQUEST_ID} {attempt} {index}/{len(chunks)} {chunk}",
                    flush=True)
        _h1_time()
        line("END", meta)
        line("COMPLETE", meta)
        return meta
    except Exception as exc:
        # Safe type only; no row, SQL text, environment dump or automatic retry.
        line("ERROR", dict(identity, error_type=type(exc).__name__))
        raise
