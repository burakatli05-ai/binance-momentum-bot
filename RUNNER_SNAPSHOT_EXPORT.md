# Snapshot-only runner export

Run once from `binance_momentum_bot/` in an operator-controlled container:

```sh
python runner_snapshot_export.py --snapshot-id 20260916T190728Z-6cf5b8ff --output /tmp/runner-20260916T190728Z-6cf5b8ff.zip
```

No deployment, startup hook, scheduler change, live DB access, Telegram send,
strategy change or retention change is part of this CLI. Existing
`/analysisexport` behavior is unchanged; **do not use that command for this task**,
because its legacy backup fallback is not snapshot-only. The new CLI reuses
`analysis_export` quoting/hash helpers but never calls `package`, `_backup`,
`Exporter`, or bot initialization. It accepts a snapshot ID, never a DB path.
The CLI source root is fixed to `/data/research_exports`.

Only a matching valid manifest with successful quick/integrity/restore checks
and a matching SHA-256 permits export. Source is opened using
`mode=ro&immutable=1`, plus `query_only`; quick and integrity checks are rerun.
Both hash and manifest are revalidated after extraction. Missing tables,
columns or mismatching row counts fail without publishing an export.
Snapshot creation time is metadata, not an exact cutoff: all snapshot rows are
retained, including observations made while the original backup was completing.

Source links, hardlinks, reparse points, traversal and SQLite sidecars are
rejected. The output must be a new `.zip` outside the snapshot tree. All parent
directories must already exist. Existing output files are never overwritten.
As with the existing snapshot worker, source directories must be immutable
and operator-controlled during execution; this CLI is not a sandbox against
a privileged attacker concurrently replacing filesystem paths.

The ZIP contains `payload.json`, `manifest.json`, `README.txt`. Tables are
represented as ordered `columns` and `rows` arrays. The manifest gives selected
column types, row counts, timestamp ranges/units, schema/export versions,
snapshot ID, original source hash, and SHA-256 of the exact payload bytes.
The broad source manifest, environment and unrelated tables are never copied.

Allowlist: four runner shadow tables, `signals_v2` identity/episode/price,
`signal_meta` Premium flag, and `signal_outcomes` horizon/return/MFE/MAE.
Raw feature/reason/metrics JSON and notification fields are intentionally
excluded. This supports watch/decision/maturity/gap counts, anchor outcomes
and episode matching. It cannot establish true post-ALLOW MFE/MAE without
a separate post-decision price path.

Validation:

```sh
python -m unittest discover -s tests -p test_runner_snapshot_export.py -v
python -m unittest discover -s tests -v
```

The symlink test requires OS symlink privileges; Linux CI executes it without
the Windows developer-mode restriction. Download the resulting ZIP using an
authenticated operator file-transfer channel, verify its payload hash, and keep
research data out of GitHub. Do not publish it in logs or PR attachments.
