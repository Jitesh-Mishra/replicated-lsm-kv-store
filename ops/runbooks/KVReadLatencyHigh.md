# KVReadLatencyHigh

**Fires when** get p99 is above 50 ms for 2 minutes.

1. Is compaction behind? `kv_sstables` and `kv_compaction_backlog_tables`. If yes, follow KVCompactionBacklog.
2. Read amplification: `rate(kv_read_tables_probed[5m]) / rate(kv_requests_total{op="get"}[5m])`.
   Well above 1 means bloom filters are not helping (very small filters, or lookups for keys that exist in many tables).
3. CPU saturation on the host, or a burst of scans (`kv_requests_total{op="scan"}`). Scans hold the process longer.
4. Mitigate: compaction first, then shed scan traffic, then scale out reads to a follower.
