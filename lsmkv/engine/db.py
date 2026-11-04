"""The LSM-tree store: WAL + skip-list memtable + SSTables + size-tiered compaction, tied together by a MANIFEST.

Directory layout
  MANIFEST            JSON: live tables (newest first), current WAL generation, next table id, last flushed seq
  wal-<gen>.log       the live write-ahead log
  sst-<id>.sst        immutable sorted tables
  archive/            closed WAL segments, kept only when wal_archive=True (point-in-time restore)

Crash safety rests on one ordering rule: a change becomes visible only when the MANIFEST that names it is
renamed into place. Anything on disk the MANIFEST doesn't name is an orphan from a crash and is removed on open.
  flush:      write sst -> write MANIFEST (new table, next WAL generation) -> open new WAL -> retire old WAL
  compaction: write merged sst -> write MANIFEST (inputs replaced) -> delete inputs
"""
from __future__ import annotations

import heapq
import json
import os
import re
import shutil
import threading
import time

from . import faults
from .memtable import SkipList
from .sstable import SSTable, fsync_dir, write_sstable
from .wal import DELETE, PUT, WAL, Record, read_records

MIN_MERGE = 4
MAX_MERGE = 32


class CompactionAborted(Exception):
    pass


class DB:
    def __init__(self, path: str, *, memtable_bytes: int = 1 << 20, fsync: str = "always", batch_ms: float = 10.0,
                 background_compaction: bool = False, wal_archive: bool = False, bloom_fp: float = 0.01,
                 on_fsync=None):
        self.path = path
        self.memtable_limit = memtable_bytes
        self.fsync_policy, self.batch_ms, self.on_fsync = fsync, batch_ms, on_fsync
        self.wal_archive = wal_archive
        self.bloom_fp = bloom_fp
        self.lock = threading.RLock()
        self.on_commit = None                         # callback(Record) after a write is in the WAL (replication)
        self.stats = {"gets": 0, "tables_probed": 0, "bloom_negatives": 0, "flushes": 0, "compactions": 0,
                      "compaction_seconds_total": 0.0, "compaction_bytes_written": 0}
        os.makedirs(path, exist_ok=True)
        if wal_archive:
            os.makedirs(self._p("archive"), exist_ok=True)
        t0 = time.perf_counter()
        self._load_manifest()
        self._remove_orphans()
        self.tables = [SSTable(self._p(f"sst-{i}.sst"), i) for i in self.manifest["tables"]]
        self.mem = SkipList()
        self.wal = WAL(self._wal_path(), fsync, batch_ms, on_fsync)
        self.seq = self.manifest["last_seq"]
        for r in self.wal.recovered:
            self.mem.put(r.key, r.value)
            self.seq = max(self.seq, r.seq)
        self.recovered_records = len(self.wal.recovered)
        self.torn_bytes = self.wal.torn_bytes
        self.recovery_seconds = time.perf_counter() - t0
        # background compaction
        self.background = background_compaction
        self._cv = threading.Condition(self.lock)
        self._closed = False
        self._stall = threading.Event()               # fault: compaction hangs (incident drill)
        self._abort = threading.Event()
        self.compaction_state = {"running": False, "started": None, "error": None}
        self._worker = None
        if background_compaction:
            self._start_worker()

    # ------------------------------------------------------------------ files
    def _p(self, name):
        return os.path.join(self.path, name)

    def _wal_path(self, gen=None):
        return self._p(f"wal-{self.manifest['wal_gen'] if gen is None else gen}.log")

    def _load_manifest(self):
        mp = self._p("MANIFEST")
        if os.path.exists(mp):
            with open(mp) as f:
                self.manifest = json.load(f)
        else:
            self.manifest = {"tables": [], "wal_gen": 1, "next_table": 1, "last_seq": 0}
            self._write_manifest(self.manifest)

    def _write_manifest(self, m):
        tmp = self._p("MANIFEST.tmp")
        with open(tmp, "w") as f:
            json.dump(m, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._p("MANIFEST"))
        fsync_dir(self.path)
        self.manifest = m

    def _remove_orphans(self):
        live = {f"sst-{i}.sst" for i in self.manifest["tables"]}
        for name in os.listdir(self.path):
            m = re.fullmatch(r"wal-(\d+)\.log", name)
            if name.endswith(".tmp") or (name.startswith("sst-") and name not in live):
                os.remove(self._p(name))
            elif m and int(m.group(1)) < self.manifest["wal_gen"]:
                self._retire_wal(int(m.group(1)))

    def _retire_wal(self, gen):
        src = self._wal_path(gen)
        if self.wal_archive:
            os.replace(src, self._p(f"archive/wal-{gen}.log"))
            fsync_dir(self._p("archive"))
        else:
            os.remove(src)
        fsync_dir(self.path)

    # ------------------------------------------------------------------ writes
    def put(self, key: bytes, value: bytes, *, _seq=None, _ts=None) -> int:
        return self._write(PUT, key, value, _seq, _ts)

    def delete(self, key: bytes, *, _seq=None, _ts=None) -> int:
        return self._write(DELETE, key, None, _seq, _ts)

    def _write(self, op, key, value, seq, ts) -> int:
        if not isinstance(key, bytes) or not key or len(key) > 1024:
            raise ValueError("key must be 1 to 1024 bytes")
        with self.lock:
            self.seq = seq if seq is not None else self.seq + 1
            rec = Record(self.seq, ts if ts is not None else time.time(), op, key, value)
            faults.point("write.before_wal")
            self.wal.append(rec)
            faults.point("write.after_wal")
            self.mem.put(key, value)
            if self.mem.bytes >= self.memtable_limit:
                self._flush()
            cb = self.on_commit
        if cb:
            cb(rec)
        return rec.seq

    def flush(self):
        with self.lock:
            if len(self.mem):
                self._flush()

    def _flush(self):
        m = self.manifest
        tid = m["next_table"]
        write_sstable(self._p(f"sst-{tid}.sst"), self.mem.items(), len(self.mem), self.bloom_fp)
        faults.point("flush.after_sstable")
        old_gen = m["wal_gen"]
        self._write_manifest({"tables": [tid] + m["tables"], "wal_gen": old_gen + 1, "next_table": tid + 1,
                              "last_seq": self.seq})
        faults.point("flush.after_manifest")
        self.wal.close()
        self.wal = WAL(self._wal_path(), self.fsync_policy, self.batch_ms, self.on_fsync)
        self._retire_wal(old_gen)
        faults.point("flush.after_wal_retired")
        self.tables.insert(0, SSTable(self._p(f"sst-{tid}.sst"), tid))
        self.mem = SkipList()
        self.stats["flushes"] += 1
        if self.background:
            self._cv.notify_all()
        else:
            while self._compact_once():
                pass

    # ------------------------------------------------------------------ reads
    def get(self, key: bytes):
        with self.lock:
            found, v = self.mem.get(key)
            tables = list(self.tables)
        self.stats["gets"] += 1
        if found:
            return v
        for t in tables:
            found, v, probed = t.get(key)
            if probed:
                self.stats["tables_probed"] += 1
            else:
                self.stats["bloom_negatives"] += 1
            if found:
                return v
        return None

    def scan(self, start: bytes = b"", end: bytes | None = None, limit: int = 1000):
        """Live (key, value) pairs with start <= key < end, merged across memtable and tables, newest wins."""
        with self.lock:
            sources = [list(self.mem.items(start or None))] + [t for t in self.tables]
        iters = [iter(sources[0])] + [t.items(start or None) for t in sources[1:]]
        out = []
        for k, v in _merge(iters):
            if end is not None and k >= end:
                break
            if v is not None:
                out.append((k, v))
                if len(out) >= limit:
                    break
        return out

    # ------------------------------------------------------------------ compaction
    def pick_compaction(self, tables=None):
        """Size-tiered: the first run of >= MIN_MERGE contiguous (by age) tables whose sizes are within
        0.5x to 1.5x of the run's average. Contiguity keeps newest-wins correct after the merge."""
        tables = self.tables if tables is None else tables
        i = 0
        while i < len(tables):
            group = [tables[i]]
            j = i + 1
            while j < len(tables) and len(group) < MAX_MERGE:
                avg = sum(t.size for t in group) / len(group)
                if 0.5 * avg <= tables[j].size <= 1.5 * avg:
                    group.append(tables[j])
                    j += 1
                else:
                    break
            if len(group) >= MIN_MERGE:
                return group
            i += 1
        return None

    def backlog(self) -> int:
        """Tables that are waiting in an eligible size tier (0 when compaction is caught up)."""
        with self.lock:
            tables = list(self.tables)
        n, rest = 0, tables
        while True:
            g = self.pick_compaction(rest)
            if not g:
                return n
            n += len(g)
            idx = rest.index(g[-1]) + 1
            rest = rest[idx:]

    def _compact_once(self) -> bool:
        with self.lock:
            group = self.pick_compaction()
            if not group:
                return False
            tid = self.manifest["next_table"]
            self._write_manifest({**self.manifest, "next_table": tid + 1})       # reserve the id
            drop_tombstones = group[-1] is self.tables[-1]
        t0 = time.perf_counter()
        self.compaction_state.update(running=True, started=time.time(), error=None)
        try:
            n = sum(t.n for t in group)

            def merged():
                for i, (k, v) in enumerate(_merge([t.items() for t in group])):
                    if self._stall.is_set():                          # injected fault: compaction hangs
                        while self._stall.is_set() and not self._abort.is_set():
                            time.sleep(0.05)
                    if self._abort.is_set():
                        raise CompactionAborted()
                    if v is None and drop_tombstones:
                        continue
                    yield k, v
            path = self._p(f"sst-{tid}.sst")
            write_sstable(path, merged(), n, self.bloom_fp)
            faults.point("compaction.before_manifest")
            with self.lock:
                ids = [t.id for t in self.tables]
                gids = [t.id for t in group]
                pos = ids.index(gids[0])
                assert ids[pos:pos + len(gids)] == gids, "compaction inputs no longer contiguous"
                new_ids = ids[:pos] + [tid] + ids[pos + len(gids):]
                self._write_manifest({**self.manifest, "tables": new_ids})
                faults.point("compaction.after_manifest")
                new = SSTable(path, tid)
                self.tables = self.tables[:pos] + [new] + self.tables[pos + len(gids):]
            for t in group:
                os.remove(t.path)
            fsync_dir(self.path)
            self.stats["compactions"] += 1
            self.stats["compaction_bytes_written"] += new.size
            return True
        except CompactionAborted:
            self.compaction_state["error"] = "aborted"
            return False
        finally:
            self.stats["compaction_seconds_total"] += time.perf_counter() - t0
            self.compaction_state["running"] = False

    def compact_all(self):
        while self._compact_once():
            pass

    def _start_worker(self):
        self._abort.clear()

        def loop():
            while True:
                with self.lock:
                    while not self._closed and not self.pick_compaction():
                        self._cv.wait(timeout=1.0)
                    if self._closed:
                        return
                self._compact_once()
                if self._abort.is_set():
                    return
        self._worker = threading.Thread(target=loop, daemon=True, name="compactor")
        self._worker.start()

    def restart_compactor(self):
        """Runbook action: abort a stuck compaction job (its partial output is an orphan) and start a new worker."""
        self._abort.set()
        self._stall.clear()
        if self._worker:
            self._worker.join(timeout=10)
        self._remove_orphans_locked()
        self._start_worker()

    def _remove_orphans_locked(self):
        with self.lock:
            live = {f"sst-{i}.sst" for i in self.manifest["tables"]}
            for name in os.listdir(self.path):
                if name.endswith(".tmp") or (name.startswith("sst-") and name not in live):
                    os.remove(self._p(name))

    # ------------------------------------------------------------------ backup
    def snapshot(self, dest: str) -> dict:
        """Consistent snapshot: flush, then copy the MANIFEST and the tables it names. Returns its metadata."""
        with self.lock:
            if len(self.mem):
                self._flush()
            self.wal.sync()
            os.makedirs(dest, exist_ok=True)
            for i in self.manifest["tables"]:
                shutil.copy2(self._p(f"sst-{i}.sst"), os.path.join(dest, f"sst-{i}.sst"))
            meta = {"seq": self.seq, "ts": time.time(), "manifest": self.manifest}
            with open(os.path.join(dest, "MANIFEST"), "w") as f:
                json.dump({**self.manifest, "wal_gen": 1}, f)
            with open(os.path.join(dest, "SNAPSHOT.json"), "w") as f:
                json.dump(meta, f)
        return meta

    def disk_bytes(self) -> int:
        return sum(os.path.getsize(self._p(n)) for n in os.listdir(self.path) if os.path.isfile(self._p(n)))

    def close(self):
        with self.lock:
            self._closed = True
            self._abort.set()
            self._stall.clear()
            self._cv.notify_all()
        if self._worker:
            self._worker.join(timeout=10)
        self.wal.close()


def _merge(iters):
    """k-way merge of sorted iterators; iterator 0 is newest. For equal keys the newest value wins."""
    heap = []
    for age, it in enumerate(iters):
        for k, v in it:
            heap.append((k, age, v, it))
            break
    heapq.heapify(heap)
    last = None
    while heap:
        k, age, v, it = heapq.heappop(heap)
        if k != last:
            yield k, v
            last = k
        for k2, v2 in it:
            heapq.heappush(heap, (k2, age, v2, it))
            break


def restore(snapshot_dir: str, wal_dirs: list[str], dest: str, until_ts: float | None = None, **db_kw) -> dict:
    """Point-in-time restore: copy the snapshot, then replay WAL records newer than the snapshot up to until_ts."""
    if os.path.exists(dest):
        shutil.rmtree(dest)
    shutil.copytree(snapshot_dir, dest)
    with open(os.path.join(dest, "SNAPSHOT.json")) as f:
        snap_seq = json.load(f)["seq"]
    os.remove(os.path.join(dest, "SNAPSHOT.json"))
    recs = {}
    for d in wal_dirs:
        for name in sorted(os.listdir(d)):
            if re.fullmatch(r"wal-\d+\.log", name):
                for r in read_records(os.path.join(d, name))[0]:
                    recs[r.seq] = r
    db = DB(dest, **db_kw)
    applied = 0
    for s in sorted(recs):
        r = recs[s]
        if s <= snap_seq or (until_ts is not None and r.ts > until_ts):
            continue
        if r.op == PUT:
            db.put(r.key, r.value, _seq=r.seq, _ts=r.ts)
        else:
            db.delete(r.key, _seq=r.seq, _ts=r.ts)
        applied += 1
    db.close()
    return {"snapshot_seq": snap_seq, "replayed": applied}
