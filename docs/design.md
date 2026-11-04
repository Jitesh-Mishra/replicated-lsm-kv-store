# Design

## Write path
1. Take the write lock, assign the next sequence number.
2. Append `length | crc32 | seq | ts | op | key | value` to the WAL. With `fsync=always`, fsync before going on.
3. Insert into the skip-list memtable (a delete inserts a tombstone).
4. If the memtable is over its size limit, flush (below).
5. Return the sequence number. Only now is the write acknowledged.

## Flush (memtable to SSTable)
1. Write `sst-<id>.sst.tmp`, fsync it, rename it to `sst-<id>.sst`, fsync the directory.
2. Write a new MANIFEST naming the table and the next WAL generation (temp file, fsync, rename, directory fsync).
   **This rename is the commit point.**
3. Open the new WAL generation, then retire the old one (delete it, or move it to `archive/` for point-in-time restore).

A crash before step 2 leaves an orphan table (removed on open) and the old WAL (replayed). A crash after step 2
leaves the old WAL as an orphan (its records are already in the table). The crash harness kills the process at
each of these points; see `lsmkv/engine/faults.py`.

## Read path
Memtable, then SSTables newest to oldest. For each table: skip it if the key is outside `[min_key, max_key]` or
the bloom filter says no; otherwise binary-search the sparse index (every 16th key) and scan at most 16
entries. The first hit wins, and a tombstone is a hit that means "deleted".

## Compaction (size-tiered)
- Tables are ordered by age. A compaction picks the first run of at least 4 **neighbouring** tables whose sizes are
  within 0.5x to 1.5x of the run's average, and merges them with a k-way heap merge where the newer table wins
  on equal keys.
- Neighbouring matters: merging two tables that have a third one between them in age would let the merged output
  shadow, or be shadowed by, the wrong version.
- Tombstones are dropped only when the run includes the oldest table, since nothing older can hold a value they
  need to hide.
- The output is committed by a MANIFEST swap, then the inputs are deleted.
- In the service it runs on a background thread. Reads take a snapshot of the table list, and tables are immutable,
  so reads never block on it.

## Replication
- The leader holds one write lock across "local WAL append + ship", so the follower sees records in sequence order.
- **sync:** the HTTP response waits for the follower to apply the record to its own WAL (fsync) and acknowledge it.
- **async:** records queue on the leader and ship every 50 ms.
- The follower applies records with the leader's sequence numbers and ignores anything it already has, so a retried
  batch is harmless.
- Promotion is manual (`POST /admin/promote`).

## Backup and point-in-time restore
- **Snapshot:** flush, then copy the MANIFEST and every table it names, recording the sequence number.
- With `WAL_ARCHIVE=1`, retired WAL segments are kept.
- **Restore to time T:** copy the snapshot, then replay every archived and live WAL record with a sequence number
  above the snapshot's and a timestamp at or before T.

## Observability
- `/metrics` exposes request latency by operation, WAL fsync latency, memtable bytes, SSTable count, compaction
  backlog, compaction job age, disk bytes, replication lag, and read amplification (tables probed vs skipped by
  bloom filters).
- Alert rules: `ops/alerts.yml`. Runbooks: `ops/runbooks/`. Dashboard: `ops/grafana/lsmkv-dashboard.json`.
