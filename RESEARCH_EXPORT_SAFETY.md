# Research export disk safety

The hourly exporter defaults to the newest **3 successful snapshots**, a **20 GiB
export directory budget**, and **8 GiB free volume reserve**. Before an attempt it
cleans recognized old exports, then reserves **3 × the greater of logical SQLite
size and source file size + 256 MiB** for the snapshot, restore validation and
growth. At a 1.34 GB source this requires approximately 12 GiB free to begin.
If either budget fails, it logs `SKIPPED` and the main bot continues.

Configuration (invalid/unsafe values disable exporter initialization):

| Variable | Default |
| --- | --- |
| RESEARCH_EXPORT_RETAIN | 3 |
| RESEARCH_EXPORT_MIN_FREE_BYTES | 8589934592 |
| RESEARCH_EXPORT_BUDGET_BYTES | 21474836480 |
| RESEARCH_EXPORT_STALE_SECONDS | 21600 |
| RESEARCH_EXPORT_TIMEOUT_SECONDS | 300 |

Snapshots are built in `.partial-<snapshot_id>`. Backup, integrity and restore
checks complete before a same-filesystem rename publishes the final directory;
`latest.json` is then atomically replaced. Failed attempts remove their own partial
directory; interrupted attempts are eligible after six hours. Legacy incomplete
directories use the same age gate. Malformed manifests and unrecognized content
are retained for inspection, never guessed safe to delete. Daily reports keep 30
files. Removing an old referenced summary clears its pointer first; downloads then
explicitly report that no summary is available until a new one is generated.

Cleanup accepts only direct snapshot children of the dedicated export root and
known exporter files. Symlinks, junctions, hardlinked files, nested unexpected
content, source ancestors and root replacement are rejected. It never deletes or
opens `/data/signals.db` for writing. An OS lock serializes snapshot, download and
cleanup operations across threads/processes, releases on crash, and supplements
the original scheduler lease. Exporter initialization, worker, metadata, lease and
report failures are isolated from the bot. Trading functions/config are unchanged.

The restore copy and ZIP streams recheck free space; backup and integrity queries
are time limited. The reserve is an exporter guard, not a filesystem quota: another
process or production DB growth can still consume volume space. Unknown existing
files can keep usage above budget, in which case exports stop until investigated.
Failed ZIP writes keep only a `.partial` file, bounded by the directory budget and
removed with that snapshot's retention; no partial ZIP is returned to callers.

`RESEARCH_EXPORT` log events record policy, disk usage, estimates, skip reason,
retained count, incomplete count and cleanup bytes. Enable `RESEARCH_EXPORT_ENABLED`
only after safety tests pass. Linux test command:

```sh
python -m unittest discover -s tests -p test_export_safety.py -v
```
