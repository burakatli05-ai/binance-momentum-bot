# `/analysisexport`

Admin-only research export for Issue #15. Send the command in the configured
administrator's **private Telegram chat**. `TELEGRAM_ADMIN_CHAT_ID` must match
that private chat; when `TELEGRAM_ADMIN_USER_ID` is configured, it must also
match the sender. Group delivery is rejected. `RESEARCH_EXPORT_ENABLED` must
be enabled, as for the existing export commands.

The command selects the newest hash-verified, quick/integrity-checked VALID
snapshot under `RESEARCH_EXPORT_DIR`. An invalid/missing latest pointer does
not prevent selection of another valid snapshot. If none exists, the SQLite
Backup API opens the source with `mode=ro` and writes a temporary consistent
snapshot, including committed WAL data. Heavy SQL runs only on the snapshot.
Missing required research tables fail the export instead of silently sending
an incomplete dataset. The command never migrates the source schema.

## Window and relationships

The start is midnight Europe/Istanbul seven calendar days before today; the
end is the earlier of request time and snapshot creation time. Both bounds are
inclusive. A stale snapshot ending before the start produces an explicitly
empty window. Inspect `source_snapshot_created_time_ms` and `effective_range`;
this command does not refresh an existing valid snapshot.

Root events are selected by their event/decision timestamp. Child tables are
selected through the retained signal, radar, research, shadow, cohort or watch
IDs; dated observations cannot exceed the effective cutoff. All columns,
including feature JSON, premium rejection reasons, MFE/MAE, maturity flags,
runner labels and episode/wave IDs, are retained. Pending outcomes stay pending.
Episode summaries overlapping the window retain original start/context fields.
IDs referencing events outside the window remain intact, but the older events
are not included. Account/trading state, X content, raw tick archives, blobs,
unrelated telemetry, views and triggers are excluded.

The exact versioned table allowlist is `analysis_export.INCLUDED_TABLES`, with
time units and parent relations declared in `ROOTS`, `CHILDREN` and `EPISODES`.
Each package's manifest also lists every included table, all its columns,
row counts and timestamp ranges. Native seconds/milliseconds are preserved.

## Package and safety

ZIP members are exactly `analysis.db`, `manifest.json`, `README.txt`.
The manifest includes source snapshot identity/path/size/SHA256, analysis DB
size/SHA256, UTC/Istanbul creation timestamps, requested/effective window,
schema/export/code versions, Git commit and deployment ID (or `UNKNOWN`),
table statistics and SQLite quick/integrity checks.
Source size/hash describe the consistent snapshot; hashing a concurrently
changing production DB would not establish snapshot identity.

The Telegram caption reports source → analysis → ZIP byte sizes. The existing
`sendDocument` helper enforces the Telegram limit and reports upload failures.
One export/upload runs at a time per bot process; another request fails clearly.
Temporary files are removed after success, failure or cancellation. Existing
`latestexport`, `latestexportfull`, scheduler pointers and production settings
are unchanged. No order, startup, sizing, exit or Premium strategy changes.

## Validation

```sh
python -m compileall -q binance_momentum_bot tests
python -m unittest discover -s tests -v
```

`test_analysis_export.py` covers private admin authorization, all allowlisted
schemas and rows, seconds/milliseconds boundaries, child outcomes, stale and
invalid snapshots, fallback/WAL consistency, source bytes, sizes/hashes,
manifest ranges/counts, repeated export, concurrency, missing tables, Telegram
size and HTTP errors, and cleanup. Existing production and export baselines
are intentionally unchanged. Synthetic fixture size measurements in the PR
are reproducible examples, not measurements of `/data/signals.db`.
