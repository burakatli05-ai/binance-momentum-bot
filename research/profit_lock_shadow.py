"""Offline/sidecar replay of Early V1 + raw aggTrade exports, without bot imports.

All inputs are read-only. Output is a NEW directory. No production DB writes,
network, credentials, startup hook, notification, order or deployment capability.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'binance_momentum_bot'))
from position_management_v2 import Policy, ShadowPosition, VERSION, summarize


def jsonl(path):
    with Path(path).open(encoding='utf-8') as source:
        for line_no, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError('object required')
                yield value
            except (ValueError, TypeError) as exc:
                raise ValueError(f'{path}:{line_no}: {exc}') from exc


def early_snapshot(path):
    """Saved radar_signals snapshot, NOT a live DB. Every notified V1 Early.

    A local entry_price or Premium linkage is intentionally never used as fill.
    The signal-reference SL is an explicitly labelled -3% research assumption.
    """
    uri = Path(path).resolve().as_uri() + '?mode=ro'
    connection = sqlite3.connect(uri, uri=True)
    try:
        connection.execute('PRAGMA query_only=ON')
        connection.row_factory = sqlite3.Row
        columns = {r[1] for r in connection.execute('PRAGMA table_info(radar_signals)')}
        episode = 'episode_id' if 'episode_id' in columns else 'NULL AS episode_id'
        sql = ('SELECT id,symbol,price,COALESCE(notify_ts,ts) AS stamp,' + episode
               + ' FROM radar_signals WHERE notified=1 ORDER BY stamp,id')
        for row in connection.execute(sql):
            yield dict(kind='EARLY', early_id=f"radar:{row['id']}", symbol=row['symbol'],
                       episode_id=row['episode_id'], decision_ms=row['stamp']*1000,
                       signal_price=row['price'], initial_stop_price=row['price']*.97,
                       initial_stop_source='ASSUMED_MINUS_3_PERCENT_SIGNAL_PROXY',
                       fills=[], source='radar_signals.notified=1')
    finally:
        connection.close()


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda:handle.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def run(cohorts, trades, *, observation_end_ms, coverage_complete=False,
        horizons=(3600000, 43200000), policy=None, allow_signal_proxy=False,
        max_cohorts=5000):
    if not horizons or len(set(horizons)) != len(horizons):
        raise ValueError('unique horizons required')
    positions = []
    by_symbol = {}
    identities = {}
    for cohort in cohorts:
        if cohort.get('kind') != 'EARLY':
            continue
        key = cohort['early_id']
        if key in identities:
            if identities[key] != cohort:
                raise ValueError('conflicting duplicate Early identity')
            continue
        if len(identities) >= max_cohorts:
            raise ValueError('cohort limit exceeded; partition input explicitly')
        identities[key] = cohort
        for horizon in horizons:
            position = ShadowPosition(cohort, horizon, policy, allow_signal_proxy)
            positions.append(position)
            if position.entry:
                by_symbol.setdefault(cohort['symbol'], []).append(position)
    # Arrival order is preserved. Sorting trade history can conceal feed faults.
    for trade in trades:
        if 'symbol' not in trade:
            raise ValueError('trade symbol required')
        for position in by_symbol.get(trade['symbol'], ()):
            position.tick(trade)
    records = [p.finish(observation_end_ms, coverage_complete) for p in positions]
    return records, summarize(records)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--cohorts', type=Path, help='Normalized Early + fill allocation JSONL')
    source.add_argument('--early-snapshot', type=Path, help='Read-only saved radar SQLite snapshot')
    parser.add_argument('--trades', required=True, type=Path, help='Raw aggTrade normalized JSONL')
    parser.add_argument('--out', required=True, type=Path, help='New output directory; never overwrites')
    parser.add_argument('--observation-end-ms', required=True, type=int)
    parser.add_argument('--coverage-complete', action='store_true',
                        help='Exporter asserts no capture/restart gaps; checked against tick gaps too')
    parser.add_argument('--allow-signal-proxy', action='store_true')
    parser.add_argument('--first-lock-pct', type=float, default=.25)
    parser.add_argument('--horizons-ms', type=int, nargs='+', default=[3600000, 43200000])
    args = parser.parse_args(argv)
    if args.out.exists():
        parser.error('--out must be a new directory')
    policy = Policy(first_lock_pct=args.first_lock_pct)
    source_path = args.cohorts or args.early_snapshot
    hashes = {str(p.resolve()): sha256(p) for p in (source_path, args.trades)}
    cohorts = jsonl(args.cohorts) if args.cohorts else early_snapshot(args.early_snapshot)
    records, report = run(cohorts, jsonl(args.trades), observation_end_ms=args.observation_end_ms,
                          coverage_complete=args.coverage_complete, horizons=args.horizons_ms,
                          policy=policy, allow_signal_proxy=args.allow_signal_proxy)
    if hashes != {str(p.resolve()): sha256(p) for p in (source_path, args.trades)}:
        raise ValueError('input changed during replay; use immutable exports')
    manifest = dict(version=VERSION, input_sha256=hashes, policy=asdict(policy),
                    policy_hash=policy.digest, horizons_ms=args.horizons_ms,
                    observation_end_ms=args.observation_end_ms,
                    exporter_asserted_coverage=args.coverage_complete,
                    allow_signal_proxy=args.allow_signal_proxy, shadow_only=True)
    # All validation and analysis finish before any output is created.
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out/'records.jsonl').write_text(''.join(json.dumps(r, sort_keys=True, allow_nan=False)+'\n'
                                                for r in records), encoding='utf-8')
    for name, data in [('summary.json', report), ('manifest.json', manifest)]:
        (args.out/name).write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False)+'\n',
                                   encoding='utf-8')
    print(json.dumps(dict(shadow_only=True, records=len(records), output=str(args.out))))


if __name__ == '__main__':
    main()
