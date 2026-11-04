"""Backup and point-in-time restore, verified key by key.

1. Write 50,000 operations (puts and deletes over 20,000 keys) to a store with WAL archiving on.
2. Take a snapshot after the first 20,000 operations.
3. Pick 5 restore points at random after the snapshot, plus "latest".
4. For each: restore = copy the snapshot, replay archived and live WAL records up to that timestamp.
   Compare every one of the 20,000 keys with an oracle built from the operation log up to the same moment.

    python -m ops.backup_restore --out reports/backup_restore.json
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import tempfile
import time

from lsmkv.engine.db import DB, restore

KEYS, OPS, SNAP_AT = 20_000, 50_000, 20_000


def main(out, seed=3):
    rng = random.Random(seed)
    work = tempfile.mkdtemp(prefix="pitr-")
    db = DB(f"{work}/db", memtable_bytes=256 << 10, fsync="batch", wal_archive=True)
    log = []                                   # (ts, key, value or None)
    snap = None
    for i in range(OPS):
        if i == SNAP_AT:
            t = time.perf_counter()
            meta = db.snapshot(f"{work}/snap")
            snap = {"at_op": i, "seq": meta["seq"], "seconds": round(time.perf_counter() - t, 3),
                    "tables": len(meta["manifest"]["tables"])}
        k = f"key{rng.randrange(KEYS):06d}".encode()
        if rng.random() < 0.85:
            v = f"{i}".encode() + os.urandom(16).hex().encode()
            db.put(k, v)
        else:
            v = None
            db.delete(k)
        log.append((time.time(), k, v))
    db.close()
    points = sorted(rng.sample(range(SNAP_AT, OPS), 5))
    results = []
    for idx in points + [OPS - 1]:
        ts = log[idx][0]
        # ops at the same timestamp as idx but after it would be replayed too; use the exact boundary
        while idx + 1 < OPS and log[idx + 1][0] == ts:
            idx += 1
        oracle = {}
        for t, k, v in log[:idx + 1]:
            oracle[k] = v
        t0 = time.perf_counter()
        info = restore(f"{work}/snap", [f"{work}/db/archive", f"{work}/db"], f"{work}/restored", until_ts=ts,
                       fsync="none")
        secs = time.perf_counter() - t0
        r = DB(f"{work}/restored", fsync="none")
        ok = sum(1 for i in range(KEYS) if r.get(f"key{i:06d}".encode()) == oracle.get(f"key{i:06d}".encode()))
        r.close()
        results.append({"restore_to_op": idx + 1, "keys_checked": KEYS, "keys_matching": ok,
                        "wal_records_replayed": info["replayed"], "restore_seconds": round(secs, 2)})
        print(results[-1], flush=True)
    shutil.rmtree(work)
    res = {"keys": KEYS, "ops": OPS, "snapshot": snap, "restores": results,
           "total_keys_checked": sum(r["keys_checked"] for r in results),
           "total_keys_matching": sum(r["keys_matching"] for r in results)}
    with open(out, "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps({k: v for k, v in res.items() if k != "restores"}, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="reports/backup_restore.json")
    main(ap.parse_args().out)
