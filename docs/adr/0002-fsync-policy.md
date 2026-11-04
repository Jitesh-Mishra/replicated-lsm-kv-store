# ADR 0002: fsync before acknowledging, configurable per deployment

**Context.** fsync costs about 200 µs on the test VM, and in the durable tier it dominates the write path: 4,535
inserts/s with an fsync per write vs 61,696/s with batched fsync.

**Options.**
- `always`: fsync before every acknowledgement.
- `batch`: fsync at most every N ms; an acknowledged write can be lost on power loss inside that window.
- `none`: leave flushing to the OS.

**Decision.** `always` is the default (service, Docker image, crash gate). `batch` and `none` exist for workloads
that accept a bounded loss window, and the benchmark reports both tiers.

**Trade-off.** Durable throughput is one fsync per write because there is no group commit yet. Group commit (one
fsync for many concurrent writers) is the next step.
