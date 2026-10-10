"""Read-only downgrade gate. Use an offline DB copy and fresh exchange evidence."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import time
import premium_step_lock as sl


def check(db_path, exchange_snapshot=None, *, now_ms=None):
    stamp = int(time.time()*1000) if now_ms is None else now_ms
    uri = Path(db_path).resolve().as_uri()+"?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as db:
        db.row_factory = sqlite3.Row
        rows = [dict(r) for r in db.execute("SELECT * FROM autotrade_trades")]
        settings = dict(db.execute("SELECT key,value FROM autotrade_settings"))
    snapshot = exchange_snapshot or {}
    fresh = (snapshot.get("complete") is True and snapshot.get("scope") == "ALL_BOT_STEP_LOCK_ORDERS"
             and isinstance(snapshot.get("observed_ts_ms"), int)
             and 0 <= stamp-snapshot["observed_ts_ms"] <= 60000
             and isinstance(snapshot.get("orders"), list))
    result = sl.rollback_readiness(rows, mode=settings.get("mode"),
                                  global_profile=settings.get("exit_profile"),
                                  exchange_orders=snapshot["orders"] if fresh else None)
    result["evidence_scope"] = "caller_supplied_offline_snapshot_not_exchange_attestation"
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--db',required=True,help='Read-only offline database copy')
    p.add_argument('--exchange-snapshot',help='Complete fresh JSON evidence; omission fails closed')
    a=p.parse_args()
    snapshot=json.loads(Path(a.exchange_snapshot).read_text(encoding='utf-8')) if a.exchange_snapshot else None
    result=check(a.db,snapshot)
    print(json.dumps(result,indent=2))
    return 0 if result['rollback_safe'] else 2


if __name__=='__main__': raise SystemExit(main())
