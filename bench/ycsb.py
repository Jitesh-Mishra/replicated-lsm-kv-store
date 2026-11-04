"""YCSB-style benchmark: lsmkv vs SQLite (WAL mode), in-process, single thread, same machine, same workloads.

Records: 100,000 keys "user0000000000" style, 100-byte values. Request keys are Zipfian (constant 0.99, YCSB's
default), scrambled so hot keys are spread over the key space. Every write is its own commit.

Durability is matched in two tiers:
  durable   lsmkv fsync=always         vs SQLite journal_mode=WAL, synchronous=FULL   (fsync per commit)
  relaxed   lsmkv fsync=batch (10 ms)  vs SQLite journal_mode=WAL, synchronous=NORMAL (no fsync per commit)

Workloads (20,000 operations each, after a load phase):
  load  100% inserts of the 100,000 records (each its own commit)
  A     50% read, 50% update
  B     95% read, 5% update
  C     100% read
  W     100% update (write-heavy)

    python -m bench.ycsb --out reports/bench_vs_sqlite.csv
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
import sqlite3
import tempfile
import time

import numpy as np

from lsmkv.engine.db import DB

N, OPS, VSIZE = 100_000, 20_000, 100
WORKLOADS = {"A": 0.5, "B": 0.95, "C": 1.0, "W": 0.0}


def zipf_keys(n_items, n, theta=0.99, seed=0):
    rng = np.random.default_rng(seed)
    w = 1.0 / np.arange(1, n_items + 1) ** theta
    cdf = np.cumsum(w / w.sum())
    ranks = np.searchsorted(cdf, rng.random(n))
    # scramble rank -> key id so the hot set is spread across the key space (YCSB "scrambled zipfian")
    return [int.from_bytes(hashlib.blake2b(int(r).to_bytes(8, "little"), digest_size=8).digest(), "little") % n_items
            for r in ranks]


def key(i):
    return f"user{i:010d}".encode()


class LSM:
    def __init__(self, path, durable):
        self.db = DB(path, fsync="always" if durable else "batch", batch_ms=10, memtable_bytes=4 << 20)

    def put(self, k, v):
        self.db.put(k, v)

    def get(self, k):
        return self.db.get(k)

    def close(self):
        self.db.close()

    def disk(self):
        return self.db.disk_bytes()


class SQLite:
    def __init__(self, path, durable):
        os.makedirs(path, exist_ok=True)
        self.path = os.path.join(path, "kv.db")
        self.c = sqlite3.connect(self.path, isolation_level=None)
        self.c.execute("PRAGMA journal_mode=WAL")
        self.c.execute(f"PRAGMA synchronous={'FULL' if durable else 'NORMAL'}")
        self.c.execute("CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID")

    def put(self, k, v):
        self.c.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (k, v))

    def get(self, k):
        r = self.c.execute("SELECT v FROM kv WHERE k = ?", (k,)).fetchone()
        return r[0] if r else None

    def close(self):
        self.c.close()

    def disk(self):
        d = os.path.dirname(self.path)
        return sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d))


def timed(fn, ops):
    lat = np.empty(len(ops))
    t0 = time.perf_counter()
    for i, op in enumerate(ops):
        t = time.perf_counter()
        fn(op)
        lat[i] = time.perf_counter() - t
    wall = time.perf_counter() - t0
    return len(ops) / wall, np.percentile(lat, 50) * 1e6, np.percentile(lat, 99) * 1e6


def run(out, n=N, ops=OPS):
    rows = []
    # a fresh value for every write: SQLite skips the page write (and the fsync) when a row is overwritten with
    # identical bytes, which would make its update numbers meaningless
    vals = [os.urandom(VSIZE) for _ in range(max(n, ops))]
    load_order = np.random.default_rng(1).permutation(n)
    for durable in (True, False):
        for name, cls in (("lsmkv", LSM), ("sqlite", SQLite)):
            work = tempfile.mkdtemp(prefix="bench-")
            s = cls(os.path.join(work, "db"), durable)
            tier = "durable" if durable else "relaxed"
            ops_s, p50, p99 = timed(lambda i: s.put(key(int(i)), vals[int(i)]), load_order)
            rows.append({"system": name, "tier": tier, "workload": "load", "ops": n, "ops_per_s": round(ops_s),
                         "p50_us": round(float(p50), 1), "p99_us": round(float(p99), 1)})
            print(rows[-1], flush=True)
            for wl, read_frac in WORKLOADS.items():
                ks = zipf_keys(n, ops, seed=hash(wl) % 1000)
                rng = np.random.default_rng(2)
                is_read = rng.random(ops) < read_frac
                seq = [(bool(r), key(k), vals[j]) for j, (r, k) in enumerate(zip(is_read, ks))]
                ops_s, p50, p99 = timed(lambda o: s.get(o[1]) if o[0] else s.put(o[1], o[2]), seq)
                rows.append({"system": name, "tier": tier, "workload": wl, "ops": ops, "ops_per_s": round(ops_s),
                             "p50_us": round(float(p50), 1), "p99_us": round(float(p99), 1)})
                print(rows[-1], flush=True)
            if name == "lsmkv":
                st = s.db.stats
                rows.append({"system": name, "tier": tier, "workload": "read_amplification",
                             "ops": st["gets"], "ops_per_s": "",
                             "p50_us": round(st["tables_probed"] / max(1, st["gets"]), 3),
                             "p99_us": f"sstables={len(s.db.tables)} bloom_skips_per_get="
                                       f"{st['bloom_negatives'] / max(1, st['gets']):.2f}"})
            rows.append({"system": name, "tier": tier, "workload": "disk_bytes", "ops": "", "ops_per_s": "",
                         "p50_us": s.disk(), "p99_us": ""})
            s.close()
            shutil.rmtree(work)
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def bloom_check(out):
    """Measured bloom false-positive rate vs theory, on a real SSTable."""
    import json

    from lsmkv.engine.sstable import SSTable, write_sstable
    work = tempfile.mkdtemp()
    res = []
    for fp in (0.01, 0.05):
        p = os.path.join(work, "sst-1.sst")
        write_sstable(p, ((key(i), b"v") for i in range(0, 2 * N, 2)), N, fp)
        t = SSTable(p, 1)
        probes = 200_000
        hits = sum(t.bloom.might_contain(key(i)) for i in range(1, 2 * probes, 2))
        res.append({"target_fp": fp, "keys": N, "bits_per_key": round(t.bloom.m / N, 2), "hashes": t.bloom.k,
                    "theoretical_fp": round(t.bloom.theoretical_fp_rate(N), 5), "measured_fp": hits / probes,
                    "absent_probes": probes})
    shutil.rmtree(work)
    with open(out, "w") as f:
        json.dump(res, f, indent=1)
    print(res)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="reports/bench_vs_sqlite.csv")
    ap.add_argument("--n", type=int, default=N)
    ap.add_argument("--ops", type=int, default=OPS)
    a = ap.parse_args()
    bloom_check("reports/bloom_fp.json")
    run(a.out, a.n, a.ops)
