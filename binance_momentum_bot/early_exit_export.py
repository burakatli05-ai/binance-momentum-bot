"""Bounded read-only export of Public Early stage rows for offline exit research.

Never writes the production database and never changes scanner decisions. The payload
contains only the minimum stage-entry fields needed to join Public Early entries to
public historical market data in a separate research environment.
"""
from contextlib import closing
import base64
import hashlib
import json
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
