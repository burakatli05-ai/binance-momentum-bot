"""Compact read-only research bridge for ChatGPT/Railway log retrieval.

The bridge never reads the live production database. Callers pass an immutable,
verified research snapshot. Only an explicit allowlist of small Premium tables is
serialized; account, order, API, X, raw-tick and unrelated research tables are
never included.
"""
from contextlib import closing
import base64
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib

BRIDGE_VERSION = "assistant-bridge-v1"
LOG_PREFIX = "ASSISTANT_BRIDGE_V1"
CHUNK_CHARS = 12000

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
    printer(f"{LOG_PREFIX} META " + json.dumps(meta, separators=(",", ":")), flush=True)
    for index, chunk in enumerate(chunks, 1):
        printer(f"{LOG_PREFIX} CHUNK {sid} {index}/{len(chunks)} {chunk}", flush=True)
    printer(f"{LOG_PREFIX} END {sid} {digest}", flush=True)
    return meta


def decode_chunks(chunks, expected_sha256=None):
    """Test/recovery helper used by the consumer to reconstruct a logged payload."""
    encoded = "".join(chunks)
    raw = zlib.decompress(base64.b64decode(encoded.encode("ascii")))
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha256 and digest != expected_sha256:
        raise ValueError("assistant bridge sha256 mismatch")
    return json.loads(raw.decode("utf-8"))
