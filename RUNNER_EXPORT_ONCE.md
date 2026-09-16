# One-attempt runner export operation

Default railway.toml and startup.py remain unchanged. Temporary config file:
`/binance_momentum_bot/railway.runner-export.toml`. This launches the wrapper,
which always execs the original Python startup.py with its original environment
and working directory. No live database, strategy, Telegram or retention edits.

The export is disabled unless RUNNER_EXPORT_ONCE equals exactly
20260916T190728Z-6cf5b8ff and RUNNER_EXPORT_SOURCE_SHA256 is an independently
recorded 64-character lowercase source hash. Only this fixed snapshot is used.
The supervisor writes/fsyncs an exclusive 0600 attempt marker in a 0700 directory
under /data/runner-export-once before sleeping 90 seconds. Failed attempts are
not retried automatically; never delete the marker to hide a failed attempt.

The worker uses PR #20's snapshot-only exporter, 512 MiB address-space limit,
120 CPU seconds, nice +15, 256 MiB per-file limit and a separate 600-second
watchdog. Preliminary disk budget is max(1 GiB, 3x snapshot size) in /tmp,
16 MiB in the state volume, and memory headroom of 768 MiB using host and cgroup
v2 limits. Unknown cgroup/resource state fails closed. The exporter loads data
in memory: these conservative bounds may reject a large production export.
CPU time/address-space limits do not provide a zero-impact I/O guarantee.
All child environments are fixed system-only dictionaries, without account,
Binance, Telegram, PATH/PYTHONPATH inheritance or raw download tokens.

Only after reopening the ZIP, checking the payload hash/table allowlist/counts,
source hash and ZIP hash is an exclusive success receipt written. Only status,
ID, hashes, byte count and table counts enter logs. A partial ZIP is not served.

## Temporary download channel

Generate a 256-bit random bearer token locally. Keep the raw token only locally;
set RUNNER_DOWNLOAD_SHA256 to its SHA-256 and RUNNER_DOWNLOAD_EXPIRES to an
absolute Unix timestamp within two hours. The download process receives only
the digest and expiry in argv and a clean environment. Railway's HTTPS proxy
must route a temporary domain to port 8787; never send the token over HTTP or
in a URL/query string. The single-purpose endpoint has no directory listing,
arbitrary paths, database download or request logging. It serves only:

- Authenticated /health: channel test, with no data access.
- No snapshot/manifest/database endpoint. Narrow preflight metadata is written only to authenticated Railway operation logs.
- Authenticated /runner-20260916T190728Z-6cf5b8ff.zip: fixed ZIP only with success receipt and matching
  size/hash, streamed from the same verified descriptor.

Invalid/expired credentials return 404. Responses are no-store. The process
expires after at most two hours, including across restarts (absolute deadline).
Request read/write timeouts bound slow clients. Rollback disables the process.
Use TLS certificate validation and reject redirects in the download client.

## Ordered operation and rollback

1. Review the PR and pass focused Linux safety tests. Record any baseline CI failures.
2. Merge additive code with the original config still selected (export disabled).
3. Select the temporary config and set only download digest/expiry; keep export
   disabled. Deploy and verify SUCCESS and fresh bot signals.
4. Create the temporary HTTPS domain on port 8787. Verify wrong-token 404 and
   authenticated /health 200 over TLS. Read the PREFLIGHT operation log and record the source hash and resource readiness.
5. Set the exact export ID and pinned hash, then deploy once to enable export.
6. After SUCCESS receipt, download ZIP over authenticated TLS and verify ZIP,
   payload, source hashes and table counts locally. NO further deploy before
   this download has completed. If export fails, leave marker and report failure;
   do not silently retry or deploy away the container output.
7. Set RUNNER_EXPORT_ONCE=0, clear download digest/expiry and source hash, restore
   config path /binance_momentum_bot/railway.toml and start command python startup.py.
   Deploy only after verified download, then verify SUCCESS and fresh signals.
   Remove the temporary domain where supported. Cleared digest/expiry plus the
   original start command disable access even if domain removal is unavailable.

Never put ZIP bytes in Railway logs, GitHub, or a public object store. Source
snapshot files and production DB are never changed by these helpers.

Operational update: this service rejects custom railway config paths because of Railway deprecation. Use the service startCommand override python runner_export_once.py, verified at runtime, while preserving both original config files. Rollback sets startCommand to python startup.py. After local ZIP/payload verification, authenticated POST /close immediately terminates the download listener before the rollback deploy. No manifest is available over HTTPS.
