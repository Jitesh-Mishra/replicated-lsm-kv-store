# Probe prep

Read this, then run `pytest -q` and `python -m chaos.crash_harness --rounds 50` yourself before any interview.

## How the code works (5 lines)
1. `DB.put` assigns a sequence number, appends a checksummed record to the WAL (fsync), then inserts into a skip list (`engine/db.py`, `wal.py`, `memtable.py`).
2. When the skip list passes its size limit, it is written out as a sorted SSTable with a sparse index and bloom filter (`sstable.py`). A new MANIFEST naming that table is then renamed into place, and only then is the old WAL retired.
3. `DB.get` checks the memtable, then tables newest to oldest. Each table is skipped by key range or bloom filter, else binary search plus a scan of at most 16 entries.
4. Compaction merges 4 or more neighbouring similar-size tables with a heap merge (newest wins) and commits by MANIFEST swap (`db.py: _compact_once`).
5. The service (`service/server.py`) wraps it in FastAPI with Prometheus metrics, ships WAL records to a follower (sync or async), and exposes admin endpoints that the runbooks use.

## Durability

**What does "durable" mean here?**
- 15 s: A write is acknowledged only after its WAL record is fsynced. Everything else can be rebuilt from the WAL plus the MANIFEST.
- 60 s: Each record has a CRC. Replay stops at the first torn or bad record and truncates there. Flush and compaction commit by renaming a new MANIFEST, after fsyncing the temp file, and then the directory is fsynced. Anything on disk the MANIFEST doesn't name is removed on open.
- Evidence: `lsmkv/engine/wal.py`, `docs/adr/0001-manifest-is-the-commit-point.md`.

**How do you know it works?**
- 15 s: 500 SIGKILLs at random moments and at named points inside flush and compaction, 365,005 acknowledged ops, 0 lost, 0 corrupt reads.
- 60 s: The parent only trusts operations the child reported as acknowledged. The one op in flight at the kill may land or not; 388 landed, 99 didn't, both correct. And the harness does catch bugs: moving one line (retire the WAL before the MANIFEST) lost 1,233 writes in 150 rounds in CI while all unit tests passed.
- Evidence: `reports/crash_fsync_always.json`, `chaos/crash_harness.py`.

**Doesn't SIGKILL leave data in the page cache? Then fsync isn't tested.**
- Yes, that's right. SIGKILL proves crash consistency of the on-disk protocol: ordering, checksums, orphan cleanup. `fsync=none` also loses nothing under SIGKILL, and I say so in the README.
- Power-loss safety rests on fsync before ack, plus the torn-tail tests.
- To really test it I'd use a fault-injecting filesystem (dm-flakey, or LazyFS) that drops unsynced writes.

**Recovery time?** 1.1 ms p50 and 3.6 ms p99 here, since the memtable is small (median 66 WAL records replayed). It scales with WAL size, i.e. the memtable limit.

## Engine

**Why an LSM tree rather than a B-tree?**
- Writes are sequential appends plus an in-memory insert. Measured without a per-commit fsync: 3.0x SQLite on bulk load, 3.8x on updates.
- The price is reads (several tables) and compaction work. SQLite reads 1.4x to 1.5x faster here.

**Why does SQLite win the durable tier?**
- Both wait on one fsync per commit, about 140 to 180 µs, and the rest is code speed: C vs Python.
- The fix on my side is group commit: one fsync for many concurrent writers.

**Bloom filter math?**
- m = -n ln p / (ln 2)^2 bits, k = (m/n) ln 2 hashes, with double hashing.
- A 1% target gives 9.6 bits per key and 7 hashes; measured 1.02% false positives on 200,000 absent keys (theory 1.00%).

**Why must compaction merge neighbouring tables?**
- Newest wins on equal keys. If you merge tables 1 and 3 and skip 2, the output sits at one age, and either 2's version wrongly shadows 3's newer one, or the reverse.

**When can a tombstone be dropped?**
- Only when the merge includes the oldest table. Otherwise an older table might still hold the value it hides.

**Read amplification?**
- 0.35 tables probed per get on the benchmark, with bloom filters skipping another 0.35. Most hot Zipfian keys sit in the memtable.

## Replication

**Sync vs async?**
- Over 5 leader kills each: sync lost 0 acknowledged writes at 258 ops/s (30 ms p50); async lost 257 (38 to 78 per kill) at 881 ops/s (8 ms p50).
- Async loses about one shipping interval: 881 ops/s × 50 ms is about 44.

**Failure modes you haven't solved?**
- Manual promotion and no fencing, so a partition could give two leaders. Raft or a lease would fix that.
- In sync mode, a write can be in the leader's WAL but get a 503 because the follower didn't ack, so clients must retry idempotently.

## Operations

**Walk me through an incident.**
- The drill: the compaction worker hangs. The backlog alert goes pending at 16 s and fires at 46 s.
- `/admin/status` shows a 41 s job with 37 tables waiting. Restarting the compactor cleared everything in 0.6 s, with 0 client errors.
- The postmortem's main finding: the job-age alert was too loose, and the backlog alert beat it.
- Honest caveat: the responder was a script, so no human reaction time is included.
- Evidence: `reports/incident_drill.json`, `docs/incident_postmortem.md`.

**Why did latency not move during the incident?** Bloom filters skip most extra tables. A stalled compaction shows up as table count and disk long before latency, which is why I page on backlog.

**How do backups work, and how do you know a restore is right?**
- A snapshot copies the MANIFEST and its tables; the WAL is archived after it.
- A restore replays records after the snapshot up to a timestamp.
- Checked 6 restores × 20,000 keys against an oracle: 120,000 of 120,000 matched.

**How does CI protect releases?**
- The release workflow runs 150 SIGKILL rounds and publishes only with 0 lost and 0 corrupt.
- The canary branch with the ordering bug passes unit tests and fails the gate.
- Evidence: `reports/canary_gate.md`.

## Weak spots to own
- Python, single node per role, and one write lock: no group commit.
- Tables load into memory on open: no block cache.
- No power-loss test harness.
- The drill responder was automated.
