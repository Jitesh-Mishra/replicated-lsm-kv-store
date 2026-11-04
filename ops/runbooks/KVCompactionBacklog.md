# KVCompactionBacklog

**Fires when** 12 or more SSTables are waiting in an eligible size tier, for 30 s.

**Impact.** Each read can probe more tables (bloom filters skip most of them, so latency degrades slowly).
Disk use grows because overwritten and deleted data is not reclaimed.

## Diagnose
1. `curl -s $HOST/admin/status | jq .compaction`
   - `running: true` and large `job_age_s`: a stuck job. Follow KVCompactionStuck.
   - `running: false` and backlog growing: the worker is not running. Restart it (step 3).
   - `running: true`, short jobs, backlog still growing: write rate exceeds compaction throughput. Go to step 4.
2. Compare `rate(kv_flushes[5m])` with `rate(kv_compactions[5m])`.

## Mitigate
3. `curl -s -X POST $HOST/admin/compaction/restart`
4. Write rate too high: raise `MEMTABLE_BYTES` (fewer, bigger flushes), throttle the heaviest client, or scale out.

## Verify
5. Backlog falls to 0; `kv_sstables` steady.
