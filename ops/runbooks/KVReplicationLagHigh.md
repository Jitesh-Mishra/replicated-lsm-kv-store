# KVReplicationLagHigh

**Fires when** the follower is more than 1,000 records behind for a minute (async mode).

**Why it matters.** In async mode those records are exactly what a leader failure would lose.

1. `curl -s $LEADER/admin/status | jq .replication`: `queued` growing means shipping fails or is too slow.
2. Follower healthy? `curl -sf $FOLLOWER/healthz`. Network between them?
3. `kv_replication_ship_seconds` p99 rising: follower disk (its WAL fsyncs) is slow.
4. Mitigate: fix the follower; if failover risk is unacceptable meanwhile, switch to `REPL_MODE=sync`
   (writes then wait on the follower, so throughput drops; see reports/failover.json).
