# ADR 0004: Leader-follower log shipping with a sync/async switch

**Context.** One node is a single point of failure. The question an operator has to answer is how much data a
failover may lose, and what avoiding that loss costs.

**Options.**
- Sync: acknowledge after the follower fsyncs the record.
- Async: acknowledge locally, ship in batches.
- Consensus (Raft).

**Decision.** Ship WAL records leader to follower, with a per-deployment `REPL_MODE`. Measured over 5 leader kills
each: sync lost 0 acknowledged writes at 258 ops/s and 30 ms p50; async lost 257 (38 to 78 per kill) at 881 ops/s
and 8 ms p50.

**Trade-off.**
- Sync couples write availability to the follower: if it is down, writes fail (503).
- Promotion is manual and there is no fencing, so a partition could create two leaders. Raft would solve both;
  it's out of scope for this MVP.
