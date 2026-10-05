import base64
import hashlib
import json
import sqlite3
import tempfile
import unittest
import zlib
from pathlib import Path

import early_exit_export as ex


SCHEMA = """
CREATE TABLE entry_stage_forward_shadow (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    episode_id INTEGER,
    stage TEXT NOT NULL,
    signal_id INTEGER,
    decision TEXT,
    created_ts_ms INTEGER NOT NULL,
    entry_age_s REAL,
    entry_price REAL NOT NULL,
    stop_price REAL,
    tp1_price REAL,
    tp2_price REAL,
    mfe_pct REAL DEFAULT 0,
    mae_pct REAL DEFAULT 0,
    close60_price REAL,
    completed_60m INTEGER DEFAULT 0
)
"""


class EarlyExitExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "signals.db")
        c = sqlite3.connect(self.path)
        c.execute(SCHEMA)
        rows = [
            (1,"AAAUSDT",101,"EARLY",None,"PUBLIC_EARLY_2OF3",ex.START_TS_MS-1,16.0,100.0,97.0,101.0,102.0,5.0,-3.2,104.0,1),
            (2,"BBBUSDT",102,"EARLY",None,"PUBLIC_EARLY_2OF3",ex.START_TS_MS,16.0,200.0,194.0,202.0,204.0,2.2,-1.0,201.0,1),
            (3,"CCCUSDT",103,"PREMIUM",3,"IMMEDIATE_PREMIUM",ex.START_TS_MS+1000,0.0,300.0,291.0,303.0,306.0,6.0,-0.5,310.0,1),
            (4,"DDDUSDT",104,"EARLY",None,"PUBLIC_EARLY_2OF3",ex.END_TS_MS-1,31.0,400.0,388.0,404.0,408.0,1.5,-2.0,399.0,1),
            (5,"EEEUSDT",105,"EARLY",None,"PUBLIC_EARLY_2OF3",ex.END_TS_MS,16.0,500.0,485.0,505.0,510.0,0.5,-0.4,501.0,1),
        ]
        c.executemany(
            "INSERT INTO entry_stage_forward_shadow VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        c.commit()
        c.close()

    def test_exact_closed_window_and_stage_filter(self):
        before = Path(self.path).stat().st_size
        payload = ex.build_payload(self.path)
        after = Path(self.path).stat().st_size
        self.assertEqual(before, after)
        self.assertEqual(["BBBUSDT","DDDUSDT"], [r[1] for r in payload["rows"]])
        self.assertTrue(all(r[3] == "EARLY" for r in payload["rows"]))
        self.assertEqual(ex.START_TS_MS, payload["rows"][0][6])
        self.assertEqual(ex.END_TS_MS-1, payload["rows"][1][6])

    def test_emit_is_reconstructable_and_hashed(self):
        lines = []
        class Printer:
            def __call__(self, *args, **kwargs):
                lines.append(" ".join(str(x) for x in args))
        meta = ex.emit(self.path, printer=Printer())
        self.assertEqual(2, meta["rows"])
        chunks = []
        for line in lines:
            if line.startswith(ex.LOG_PREFIX + " CHUNK "):
                chunks.append(line.split(" ", 3)[3])
        raw = zlib.decompress(base64.b64decode("".join(chunks)))
        self.assertEqual(meta["sha256"], hashlib.sha256(raw).hexdigest())
        payload = json.loads(raw.decode("utf-8"))
        self.assertEqual(["BBBUSDT","DDDUSDT"], [r[1] for r in payload["rows"]])

    def test_missing_table_fails_closed(self):
        other = str(Path(self.tmp.name) / "missing.db")
        sqlite3.connect(other).close()
        with self.assertRaisesRegex(ValueError, "entry_stage_forward_shadow missing"):
            ex.build_payload(other)


if __name__ == "__main__":
    unittest.main()
