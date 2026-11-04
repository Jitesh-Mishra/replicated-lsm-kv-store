# KVDown

**Fires when** Prometheus cannot scrape the instance for 30 s.

1. `curl -sf $HOST/healthz`. Fails: check the process (`docker ps`, `kubectl get pods`) and its last log lines.
2. Crash loop on startup: look for `CorruptTable` in the log. A table failed its checksum.
   Do not delete files by hand. Restore from the latest snapshot plus WAL (`lsmkv.engine.db.restore`), then
   compare key counts.
3. Process up but not answering: check CPU and the disk (`KVWalFsyncSlow`). Restart once; recovery replays the WAL.
4. Leader down with a follower: promote it (`POST $FOLLOWER/admin/promote`) and point clients at it.
   In async mode, writes the follower had not received are lost; record `kv_replication_lag_records` at the time.
