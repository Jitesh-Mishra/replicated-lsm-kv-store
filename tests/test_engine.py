import os
import random

import pytest

from lsmkv.engine.bloom import BloomFilter
from lsmkv.engine.db import DB, restore
from lsmkv.engine.memtable import SkipList
from lsmkv.engine.sstable import CorruptTable, SSTable, write_sstable
from lsmkv.engine.wal import PUT, WAL, Record, read_records


def rkey(rng, n=500):
    return f"k{rng.randrange(n):05d}".encode()


def test_skiplist_sorted_and_overwrites():
    s, ref, rng = SkipList(seed=1), {}, random.Random(1)
    for _ in range(3000):
        k = rkey(rng)
        v = None if rng.random() < 0.2 else os.urandom(8)
        s.put(k, v)
        ref[k] = v
    assert [k for k, _ in s.items()] == sorted(ref)
    assert all(s.get(k) == (True, v) for k, v in ref.items())
    assert s.get(b"nope") == (False, None)


def test_bloom_no_false_negatives_and_fp_near_theory():
    b = BloomFilter.for_capacity(10_000, 0.01)
    for i in range(10_000):
        b.add(f"in{i}".encode())
    assert all(b.might_contain(f"in{i}".encode()) for i in range(10_000))
    fp = sum(b.might_contain(f"out{i}".encode()) for i in range(50_000)) / 50_000
    assert fp < 0.02


def test_sstable_roundtrip_and_corruption(tmp_path):
    items = sorted((f"k{i:05d}".encode(), None if i % 7 == 0 else f"v{i}".encode()) for i in range(1000))
    p = str(tmp_path / "sst-1.sst")
    write_sstable(p, items, len(items))
    t = SSTable(p, 1)
    for k, v in items:
        assert t.get(k)[:2] == (True, v)
    assert t.get(b"k99999")[0] is False
    assert list(t.items(b"k00500"))[0][0] == b"k00500"
    with open(p, "r+b") as f:
        f.seek(100)
        b = f.read(1)
        f.seek(100)
        f.write(bytes([b[0] ^ 0x01]))
    with pytest.raises(CorruptTable):
        SSTable(p, 1)


def test_wal_torn_tail_is_truncated(tmp_path):
    p = str(tmp_path / "wal-1.log")
    w = WAL(p)
    for i in range(10):
        w.append(Record(i + 1, 0.0, PUT, f"k{i}".encode(), b"v"))
    w.close()
    with open(p, "ab") as f:
        f.write(Record(11, 0.0, PUT, b"k10", b"v").encode()[:-3])      # half-written record
    w = WAL(p)
    assert [r.seq for r in w.recovered] == list(range(1, 11)) and w.torn_bytes > 0
    w.append(Record(11, 0.0, PUT, b"k10", b"v"))
    w.close()
    assert [r.seq for r in read_records(p)[0]] == list(range(1, 12))


@pytest.mark.parametrize("seed", range(5))
def test_random_ops_match_dict_oracle(tmp_path, seed):
    """Random puts, deletes, gets and scans with a tiny memtable so flushes and compactions happen constantly,
    and the store reopened from disk several times."""
    rng = random.Random(seed)
    ref = {}
    db = DB(str(tmp_path / "db"), memtable_bytes=2048, fsync="none")
    mismatches, compactions = 0, 0
    for step in range(6000):
        k, r = rkey(rng), rng.random()
        if r < 0.5:
            v = os.urandom(rng.randrange(1, 40))
            db.put(k, v)
            ref[k] = v
        elif r < 0.65:
            db.delete(k)
            ref.pop(k, None)
        elif r < 0.98:
            mismatches += db.get(k) != ref.get(k)
        else:
            lo = rkey(rng)
            want = sorted((kk, vv) for kk, vv in ref.items() if kk >= lo)[:50]
            mismatches += db.scan(lo, limit=50) != want
        if step % 1500 == 1499:
            compactions += db.stats["compactions"]
            db.close()
            db = DB(str(tmp_path / "db"), memtable_bytes=2048, fsync="none")
    mismatches += sum(db.get(k) != v for k, v in ref.items())
    assert compactions > 0
    db.close()
    assert mismatches == 0


def test_tombstones_dropped_only_at_bottom(tmp_path):
    db = DB(str(tmp_path / "db"), memtable_bytes=1 << 30, fsync="none")
    for i in range(100):
        db.put(f"k{i}".encode(), b"x")
    db.flush()
    for i in range(50):
        db.delete(f"k{i}".encode())
    db.flush()
    for _ in range(3):
        db.put(b"zz", b"y")
        db.flush()
    db.compact_all()
    assert all(db.get(f"k{i}".encode()) is None for i in range(50))
    assert all(db.get(f"k{i}".encode()) == b"x" for i in range(50, 100))
    db.close()


def test_point_in_time_restore(tmp_path):
    import time
    db = DB(str(tmp_path / "db"), memtable_bytes=4096, fsync="none", wal_archive=True)
    for i in range(300):
        db.put(f"a{i}".encode(), b"1")
    db.snapshot(str(tmp_path / "snap"))
    for i in range(300):
        db.put(f"b{i}".encode(), b"2")
    time.sleep(0.01)
    cut = time.time()
    time.sleep(0.01)
    for i in range(300):
        db.put(f"c{i}".encode(), b"3")
    db.close()
    restore(str(tmp_path / "snap"), [str(tmp_path / "db/archive"), str(tmp_path / "db")], str(tmp_path / "r"),
            until_ts=cut, fsync="none")
    r = DB(str(tmp_path / "r"), fsync="none")
    assert all(r.get(f"a{i}".encode()) == b"1" and r.get(f"b{i}".encode()) == b"2" for i in range(300))
    assert all(r.get(f"c{i}".encode()) is None for i in range(300))
    r.close()
