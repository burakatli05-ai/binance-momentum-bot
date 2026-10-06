"""Test-owned SQLite only; no bot, client, credentials or production access."""
import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
import zlib

import early_exit_export as ex
import early_checkpoint_shadow as shadow

NOW = ex.H1_END_MS + 60_000
ATTEMPT = "a" * 32


def decode(lines):
    """Independent consumer: strict identity, completeness, order and byte checks."""
    prefix = "H1_CHECKPOINT_EXPORT_V1 "
    records = [line[len(prefix):] for line in lines if line.startswith(prefix)]
    if len(records) < 5 or not records[0].startswith("START ") or not records[1].startswith("META "):
        raise ValueError("missing start/meta")
    start = json.loads(records[0][6:])
    meta = json.loads(records[1][5:])
    identity = (meta["request_id"], meta["attempt_id"])
    if identity != (start["request_id"], start["attempt_id"]):
        raise ValueError("start identity")
    if identity[0] != "h1-checkpoint-cut-20261006T180000Z-v1":
        raise ValueError("request")
    if len(records) != meta["chunks"] + 4:
        raise ValueError("record count")
    pieces = []
    for index, line in enumerate(records[2:-2], 1):
        fields = line.split(" ")
        if (len(fields) != 5 or fields[0] != "CHUNK" or tuple(fields[1:3]) != identity
                or fields[3] != f"{index}/{meta['chunks']}"):
            raise ValueError("chunk identity/order")
        pieces.append(fields[4])
    end = json.loads(records[-2].removeprefix("END "))
    complete = json.loads(records[-1].removeprefix("COMPLETE "))
    if end != meta or complete != meta or not records[-2].startswith("END ") or not records[-1].startswith("COMPLETE "):
        raise ValueError("closure")
    compressed = base64.b64decode("".join(pieces), validate=True)
    inflator = zlib.decompressobj()
    raw = inflator.decompress(compressed) + inflator.flush()
    if not inflator.eof or inflator.unused_data or inflator.unconsumed_tail:
        raise ValueError("compression closure")
    if len(raw) != meta["raw_bytes"] or len(compressed) != meta["compressed_bytes"]:
        raise ValueError("sizes")
    if hashlib.sha256(raw).hexdigest() != meta["sha256"]:
        raise ValueError("hash")
    data = json.loads(raw)
    if (data["request_id"], data["attempt_id"]) != identity:
        raise ValueError("payload identity")
    if len(data["rows"]) != meta["rows"] or data["row_count"] != meta["rows"]:
        raise ValueError("rows")
    if data["columns"] != meta["columns"] or len(data["columns"]) != 21:
        raise ValueError("columns")
    if len(raw) > 2_000_000 or len(data["rows"]) > 5000:
        raise ValueError("budget")
    keys = [(r[2], r[0]) for r in data["rows"]]
    if keys != sorted(set(keys)) or len({r[0] for r in data["rows"]}) != len(keys):
        raise ValueError("order/identity")
    if any(not 1791147600000 <= r[2] < 1791309600000 for r in data["rows"]):
        raise ValueError("window")
    return data, raw, meta


class H1ExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name, "h1.db").resolve())
        c = sqlite3.connect(self.path)
        shadow.migrate(c)
        c.commit()
        c.close()
        self.addCleanup(patch.stopall)
        patch.object(ex, "H1_DB_PATH", self.path).start()
        patch.object(ex.time, "time_ns", return_value=NOW * 1_000_000).start()
        patch.dict(os.environ, {ex.H1_SELECTOR: ""}).start()
        self.lines = []

    def insert(self, episode=1, stamp=None, **values):
        data = dict(episode_id=episode, symbol="TESTUSDT",
                    candidate_start_ts_ms=ex.H1_START_MS if stamp is None else stamp,
                    status="TRACKING", version="h1-early-checkpoint-latency-shadow-v1",
                    updated_ts_ms=NOW)
        data.update(values)
        with sqlite3.connect(self.path) as c:
            c.execute("INSERT INTO early_checkpoint_latency_shadow_v1 (" +
                      ",".join(data) + ") VALUES (" + ",".join("?" for _ in data) + ")",
                      tuple(data.values()))

    def payload(self):
        return ex.build_h1_payload(self.path, attempt_id=ATTEMPT)

    def printer(self, *parts, **kwargs):
        self.lines.append(" ".join(str(x) for x in parts))

    def test_frozen_spec(self):
        self.assertEqual((ex.H1_START_MS, ex.H1_END_MS, ex.H1_EXPIRES_MS),
                         (1791147600000, 1791309600000, 1791406800000))
        self.assertEqual((ex.MAX_ROWS, ex.MAX_RAW_BYTES, ex.CHUNK_CHARS), (5000, 2_000_000, 12000))
        self.assertEqual(ex.H1_REQUEST_ID, "h1-checkpoint-cut-20261006T180000Z-v1")
        self.assertEqual(len(ex.H1_COLUMNS), 21)
        self.assertEqual(ex.H1_COLUMNS[0], "episode_id")

    def test_window_order_all_statuses_and_late_updates(self):
        self.insert(9, ex.H1_START_MS - 1)
        self.insert(8, ex.H1_END_MS)
        for i, status in [(3, "READY_NO_PUBLIC"), (1, "PUBLIC_EARLY"), (2, "NO_READY"), (4, "READY"), (5, "TRACKING")]:
            self.insert(i, status=status, end_ts_ms=NOW, end_reason="CONTINUITY_REJECT")
        p = self.payload()
        self.assertEqual([r[0] for r in p["rows"]], [1, 2, 3, 4, 5])
        self.assertEqual(p["rows"][0][-1], NOW)
        self.assertFalse(p["old_270_cohort_reconstructed"])

    def test_null_empty_zero_json_preserved(self):
        text = '[ "FLOW30",  "BUY30" ]'
        self.insert(seconds_saved=0.0, would_create_radar=0, end_reason="",
                    prior_failed_gates_json=text, first_ready_price=None)
        row = dict(zip(ex.H1_COLUMNS, self.payload()["rows"][0]))
        self.assertIsNone(row["first_ready_price"])
        self.assertEqual(row["end_reason"], "")
        self.assertEqual(row["would_create_radar"], 0)
        self.assertEqual(row["seconds_saved"], 0.0)
        self.assertEqual(row["prior_failed_gates_json"], text)

    def test_5000_allowed_5001_rejected_without_payload(self):
        with sqlite3.connect(self.path) as c:
            c.executemany("INSERT INTO early_checkpoint_latency_shadow_v1"
                          "(episode_id,symbol,candidate_start_ts_ms,status,version,updated_ts_ms)"
                          " VALUES (?,?,?,?,?,?)",
                          [(i, "X", ex.H1_START_MS, "NO_READY", "v", NOW) for i in range(1, 5001)])
        self.assertEqual(self.payload()["row_count"], 5000)
        self.insert(5001)
        with self.assertRaisesRegex(ValueError, "ROW_CAP"):
            ex.emit_h1(self.path, self.printer)
        self.assertFalse(any(" META " in x or " CHUNK " in x or " END " in x for x in self.lines))

    def test_byte_cap_rejects_without_partial_success(self):
        self.insert(prior_failed_gates_json="x" * 2_000_000)
        with self.assertRaisesRegex(ValueError, "BYTE_CAP"):
            ex.emit_h1(self.path, self.printer)
        self.assertTrue(self.lines[-1].startswith(ex.H1_LOG_PREFIX + " ERROR "))
        self.assertFalse(any(" CHUNK " in x or " END " in x for x in self.lines))

    def test_exact_encoder_byte_boundary(self):
        base = {"text": ""}
        overhead = len(json.dumps(base, separators=(",", ":")).encode())
        base["text"] = "x" * (2_000_000 - overhead)
        self.assertEqual(len(ex.encode_payload(base)[0]), 2_000_000)
        base["text"] += "x"
        with self.assertRaises(ValueError):
            ex.encode_payload(base)

    def test_missing_file_never_created(self):
        absent = str(Path(self.tmp.name, "absent.db"))
        with patch.object(ex, "H1_DB_PATH", absent), self.assertRaises(ValueError):
            ex.emit_h1(absent, self.printer)
        self.assertFalse(Path(absent).exists())
        self.assertEqual(self.lines, [])

    def test_different_path_rejected(self):
        self.insert()
        with patch.object(ex, "H1_DB_PATH", "/data/signals.db"), self.assertRaisesRegex(ValueError, "SOURCE_PATH"):
            self.payload()

    def test_missing_table_rejected(self):
        with sqlite3.connect(self.path) as c:
            c.execute("DROP TABLE early_checkpoint_latency_shadow_v1")
        with self.assertRaisesRegex(ValueError, "ORDINARY_TABLE"):
            self.payload()

    def test_view_rejected(self):
        with sqlite3.connect(self.path) as c:
            c.execute("DROP TABLE early_checkpoint_latency_shadow_v1")
            c.execute("CREATE VIEW early_checkpoint_latency_shadow_v1 AS SELECT 1")
        with self.assertRaisesRegex(ValueError, "ORDINARY_TABLE"):
            self.payload()

    def test_schema_missing_extra_or_type_changed_rejected(self):
        self.insert()
        with sqlite3.connect(self.path) as c:
            c.execute("ALTER TABLE early_checkpoint_latency_shadow_v1 ADD COLUMN unexpected TEXT")
        with self.assertRaisesRegex(ValueError, "SCHEMA_MISMATCH"):
            self.payload()
        with sqlite3.connect(self.path) as c:
            c.execute("DROP TABLE early_checkpoint_latency_shadow_v1")
            c.execute("CREATE TABLE early_checkpoint_latency_shadow_v1(episode_id TEXT PRIMARY KEY)")
        with self.assertRaisesRegex(ValueError, "SCHEMA_MISMATCH"):
            self.payload()

    def test_wrong_storage_and_nonfinite_rejected(self):
        self.insert(first_pass2_ts_ms="not-integer")
        with self.assertRaisesRegex(ValueError, "STORAGE_CLASS"):
            self.payload()
        with sqlite3.connect(self.path) as c:
            c.execute("UPDATE early_checkpoint_latency_shadow_v1 SET first_pass2_ts_ms=NULL,first_ready_price=?", (float("inf"),))
        with self.assertRaisesRegex(ValueError, "STORAGE_CLASS"):
            self.payload()

    def test_empty_window_not_success(self):
        with self.assertRaisesRegex(ValueError, "EMPTY_WINDOW"):
            ex.emit_h1(self.path, self.printer)
        self.assertFalse(any(" END " in x for x in self.lines))

    def test_expiry_and_not_before_cutoff(self):
        self.insert()
        for stamp in (ex.H1_END_MS - 1, ex.H1_EXPIRES_MS, ex.H1_EXPIRES_MS + 1):
            with self.subTest(stamp=stamp), patch.object(ex.time, "time_ns", return_value=stamp * 1_000_000):
                with self.assertRaisesRegex(ValueError, "ACTIVATION_WINDOW"):
                    ex.emit_h1(self.path, self.printer)
        self.assertEqual(self.lines, [])

    def test_deadline_rejects_without_retry_and_closes_reader(self):
        self.insert()
        real = ex._readonly
        connections = []
        def opened(p):
            conn = real(p)
            connections.append(conn)
            return conn
        with patch.object(ex, "_readonly", side_effect=opened), patch.object(ex.time, "monotonic", side_effect=[0, 6]):
            with self.assertRaisesRegex(TimeoutError, "DEADLINE"):
                self.payload()
        self.assertEqual(len(connections), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            connections[0].execute("SELECT 1")

    def test_readonly_rejects_writes_and_source_bytes_unchanged(self):
        self.insert()
        before = Path(self.path).read_bytes()
        c = ex._readonly(self.path)
        try:
            self.assertEqual(c.execute("PRAGMA query_only").fetchone(), (1,))
            with self.assertRaises(sqlite3.OperationalError):
                c.execute("DELETE FROM early_checkpoint_latency_shadow_v1")
        finally:
            c.close()
        self.payload()
        self.assertEqual(Path(self.path).read_bytes(), before)

    def test_fixed_sql_only_and_connection_closed_before_delivery(self):
        self.insert()
        trace, connections = [], []
        real = ex._readonly
        def opened(p):
            conn = real(p)
            conn.set_trace_callback(trace.append)
            connections.append(conn)
            return conn
        def printer(*a, **kw):
            if connections:
                with self.assertRaises(sqlite3.ProgrammingError):
                    connections[0].execute("SELECT 1")
            self.printer(*a, **kw)
        with patch.object(ex, "_readonly", side_effect=opened):
            ex.emit_h1(self.path, printer)
        self.assertEqual(len(connections), 1)
        self.assertEqual(trace[0], "BEGIN")
        self.assertEqual(len(trace), 4)
        self.assertIn("WHERE name='early_checkpoint_latency_shadow_v1'", trace[1])
        self.assertEqual(trace[2], "PRAGMA table_info(early_checkpoint_latency_shadow_v1)")
        self.assertIn("LIMIT 5001", trace[3])
        self.assertNotIn("entry_stage_forward_shadow", "".join(trace))

    def test_wal_snapshot_coherent_against_later_commit(self):
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
        self.insert(status="READY")
        real = ex._readonly
        changed = []
        def opened(p):
            conn = real(p)
            def trace(sql):
                if sql == "PRAGMA table_info(early_checkpoint_latency_shadow_v1)":
                    with sqlite3.connect(self.path) as writer:
                        writer.execute("UPDATE early_checkpoint_latency_shadow_v1 SET status='PUBLIC_EARLY'")
                    changed.append(True)
            conn.set_trace_callback(trace)
            return conn
        with patch.object(ex, "_readonly", side_effect=opened):
            p = self.payload()
        self.assertEqual(changed, [True])
        self.assertEqual(p["rows"][0][17], "READY")
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute("SELECT status FROM early_checkpoint_latency_shadow_v1").fetchone()[0], "PUBLIC_EARLY")

    def test_exact_selector_dispatch_only_h1(self):
        self.insert()
        with patch.dict(os.environ, {ex.H1_SELECTOR: ex.H1_REQUEST_ID}), patch.object(ex, "build_payload", side_effect=AssertionError("Early path")):
            ex.emit(self.path, self.printer)
        p, raw, meta = decode(self.lines)
        self.assertEqual(p["row_count"], 1)
        self.assertEqual(p["columns"], list(ex.H1_COLUMNS))
        self.assertEqual(meta["sha256"], hashlib.sha256(raw).hexdigest())

    def test_invalid_selector_fails_before_open(self):
        with patch.dict(os.environ, {ex.H1_SELECTOR: "not-approved"}), patch.object(ex, "_readonly") as opened:
            with self.assertRaisesRegex(ValueError, "UNKNOWN_REQUEST"):
                ex.emit(self.path, self.printer)
        opened.assert_not_called()
        self.assertEqual(self.lines, [])

    def test_unset_and_empty_preserve_original_export_call(self):
        sentinel = {"sentinel": "original"}
        for missing in (True, False):
            with patch.dict(os.environ, clear=False):
                if missing:
                    os.environ.pop(ex.H1_SELECTOR, None)
                else:
                    os.environ[ex.H1_SELECTOR] = ""
                with patch.object(ex, "build_payload", return_value=sentinel) as build, patch.object(ex, "emit_h1") as h1:
                    # The legacy path needs rows/window keys; inspect actual argument dispatch.
                    with patch.object(ex, "encode_payload", side_effect=RuntimeError("legacy reached")):
                        with self.assertRaisesRegex(RuntimeError, "legacy reached"):
                            ex.emit(self.path, self.printer, 101, 202)
                build.assert_called_once_with(self.path, 101, 202)
                h1.assert_not_called()

    def test_attempt_identity_distinct(self):
        self.insert()
        ex.emit_h1(self.path, self.printer)
        first = decode(self.lines)[0]
        self.lines.clear()
        ex.emit_h1(self.path, self.printer)
        second = decode(self.lines)[0]
        self.assertNotEqual(first["attempt_id"], second["attempt_id"])
        self.assertEqual(first["rows"], second["rows"])

    def test_transport_negatives_fail_closed(self):
        self.insert(prior_failed_gates_json="".join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(1000)))
        ex.emit_h1(self.path, self.printer)
        self.assertGreater(decode(self.lines)[2]["chunks"], 1)
        variants = {}
        variants["missing"] = self.lines[:2] + self.lines[3:]
        variants["duplicate"] = self.lines[:3] + [self.lines[2]] + self.lines[3:]
        swapped = list(self.lines); swapped[2], swapped[3] = swapped[3], swapped[2]
        variants["reordered"] = swapped
        conflict = list(self.lines); conflict[2] = conflict[2].replace(" CHUNK " + ex.H1_REQUEST_ID, " CHUNK wrong", 1)
        variants["request"] = conflict
        cross = list(self.lines); f = cross[2].split(" "); f[3] = "b" * 32; cross[2] = " ".join(f)
        variants["attempt"] = cross
        variants["no_end"] = self.lines[:-2] + self.lines[-1:]
        corrupt = list(self.lines); f = corrupt[2].split(" "); f[-1] = ("A" if f[-1][0] != "A" else "B") + f[-1][1:]; corrupt[2] = " ".join(f)
        variants["corrupt"] = corrupt
        bad_meta = list(self.lines); meta = json.loads(bad_meta[1].split(" META ", 1)[1]); meta["rows"] += 1
        bad_meta[1] = ex.H1_LOG_PREFIX + " META " + json.dumps(meta)
        variants["count"] = bad_meta
        for name, lines in variants.items():
            with self.subTest(name=name), self.assertRaises((ValueError, zlib.error)):
                decode(lines)

    def test_printer_failure_stops_without_retry(self):
        self.insert()
        calls = []
        def printer(*parts, **kw):
            calls.append(parts[0])
            if " CHUNK " in parts[0]:
                raise OSError("test sink")
        with self.assertRaises(OSError):
            ex.emit_h1(self.path, printer)
        self.assertEqual(sum(" CHUNK " in x for x in calls), 1)
        self.assertFalse(any(" END " in x or " COMPLETE " in x for x in calls))

    def test_no_unsafe_imports_or_sql_interfaces(self):
        tree = ast.parse(Path(ex.__file__).read_text())
        imports = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        imports.update(n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom))
        self.assertLessEqual(imports, {"contextlib", "pathlib", "base64", "hashlib", "json", "math", "os", "platform", "re", "time", "uuid", "sqlite3", "zlib"})
        self.assertNotIn("bot", Path(ex.__file__).read_text().split("import "))
        self.assertNotIn("JOIN", ex.H1_SELECT)
        self.assertNotIn("ATTACH", ex.H1_SELECT)
        self.assertNotIn("SELECT *", ex.H1_SELECT)


if __name__ == "__main__":
    unittest.main()
