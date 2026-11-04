# KVCompactionStuck

**Fires when** a compaction job has been running for more than 60 s, for 30 s (`kv_compaction_job_age_seconds > 60`).
A healthy compaction at this data size finishes in well under a second.

**Impact.** New SSTables pile up behind the stuck job. Reads probe more files, disk use grows (inputs are not
deleted), and soon `KVCompactionBacklog` fires too. Writes are not affected until the disk fills.

## Diagnose
1. `curl -s $HOST/admin/status | jq .compaction`
   - `running: true` with a large `job_age_s` confirms the job is stuck, not slow.
   - `backlog_tables` says how far behind it is.
2. Check the trend: `kv_sstables` rising and `kv_compactions` flat over the same window.
3. Rule out disk: `KVWalFsyncSlow` firing, or `df` on the data volume near full, means the disk is the problem.
   Go to KVWalFsyncSlow / KVDiskUsageHigh instead.

## Mitigate
4. Restart the compaction worker: `curl -s -X POST $HOST/admin/compaction/restart`.
   It aborts the stuck job, removes its partial output (an orphan the MANIFEST never named) and starts a new worker.
   This is safe: the MANIFEST still names the input tables, so no data moves until a new compaction commits.
5. Watch `kv_compaction_backlog_tables` fall to 0 and `kv_sstables` drop. Both alerts clear on their own.

## If that does not work
6. Restart the process (`kubectl rollout restart` / `docker restart`). Recovery replays the WAL in milliseconds.
7. Still stuck: take a snapshot (`POST /admin/snapshot?dest=...`), page the storage owner, keep serving.

## After
- File a postmortem if it paged. The drill's postmortem is in `docs/incident_postmortem.md`.
