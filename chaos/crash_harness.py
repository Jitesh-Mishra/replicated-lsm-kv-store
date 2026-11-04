"""SIGKILL crash-injection harness.

A child process writes to the store as fast as it can and reports each operation twice on stdout: an intent line
before calling the store and an ACK line after the store returns. The parent kills the child, either at a random
moment (timer) or at a named point inside the engine (write, flush, compaction; see engine/faults.py), then
reopens the store itself and checks it against a model built only from acknowledged operations:

  lost acknowledged write   a key whose value is older than its last ACKed operation
  corrupt read              a value that was never written for that key, or an error while reading
  the one in-flight op      (intent seen, no ACK) may or may not have landed; both outcomes are correct

Every round continues on the same directory, so damage would accumulate. SIGKILL tests a process crash, not
power loss: data the OS has accepted survives either way. Power loss is modelled separately by the WAL's
torn-tail tests and the fsync policy.

    python -m chaos.crash_harness --rounds 500 --out reports/crash_results.json
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time

CRASH_POINTS = {"write.before_wal": 0.002, "write.after_wal": 0.002, "flush.after_sstable": 0.15,
                "flush.after_manifest": 0.15, "flush.after_wal_retired": 0.1, "sstable.before_rename": 0.03,
                "compaction.before_manifest": 0.3, "compaction.after_manifest": 0.3}
KEYS = 2000


def child(path: str, seed: int, fsync: str, memtable: int):
    from lsmkv.engine.db import DB
    rng = random.Random(seed)
    db = DB(path, memtable_bytes=memtable, fsync=fsync)
    out = sys.stdout
    out.write("READY\n")
    out.flush()
    n = 0
    while True:
        k = f"key{rng.randrange(KEYS):05d}"
        if rng.random() < 0.75:
            v = f"{seed}-{n}-" + "x" * rng.randrange(8, 64)
            out.write(f"P {k} {v}\n")
            out.flush()
            db.put(k.encode(), v.encode())
        else:
            out.write(f"D {k}\n")
            out.flush()
            db.delete(k.encode())
        out.write("A\n")
        out.flush()
        n += 1


def run(rounds: int, out: str, fsync: str = "always", memtable: int = 8192, seed: int = 7):
    from lsmkv.engine.db import DB
    rng = random.Random(seed)
    work = tempfile.mkdtemp(prefix="crash-")
    path = os.path.join(work, "db")
    model: dict[str, str | None] = {}
    written: dict[str, set] = {}
    res = {"rounds": 0, "acked_ops": 0, "lost_acked_writes": 0, "corrupt_reads": 0, "inflight_landed": 0,
           "inflight_not_landed": 0, "kill_sites": {}, "recovery_ms": [], "wal_records_replayed": [],
           "torn_tail_bytes_total": 0, "tables_after": []}
    t_start = time.time()
    for r in range(rounds):
        use_points = r % 2 == 1
        env = dict(os.environ, PYTHONPATH=os.getcwd())
        if use_points:
            env["LSMKV_CRASH_POINTS"] = ",".join(f"{k}:{v}" for k, v in CRASH_POINTS.items())
            env["LSMKV_CRASH_SEED"] = str(rng.randrange(1, 1 << 30))
        p = subprocess.Popen([sys.executable, "-m", "chaos.crash_harness", "--child", path, str(1000 + r), fsync,
                              str(memtable)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        lines = []
        reader = threading.Thread(target=lambda: lines.extend(p.stdout), daemon=True)
        reader.start()
        site = "timer"
        if use_points:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.send_signal(signal.SIGKILL)
                p.wait()
        else:
            deadline = time.time() + 0.15 + rng.random() * 0.6
            while time.time() < deadline and p.poll() is None:
                time.sleep(0.005)
            p.send_signal(signal.SIGKILL)
            p.wait()
        reader.join(timeout=5)
        err = p.stderr.read()
        for ln in err.splitlines():
            if ln.startswith("CRASH "):
                site = ln.split()[1]
        if "Traceback" in err:
            raise RuntimeError(err)
        res["kill_sites"][site] = res["kill_sites"].get(site, 0) + 1
        # apply ACKed ops to the model; remember the one in flight
        pending = None
        for ln in lines:
            ln = ln.rstrip("\n")
            if ln == "READY" or not ln:
                continue
            if ln == "A":
                op = pending
                pending = None
                if op[0] == "P":
                    model[op[1]] = op[2]
                else:
                    model[op[1]] = None
                res["acked_ops"] += 1
                continue
            parts = ln.split(" ", 2)
            pending = parts
            if parts[0] == "P":
                written.setdefault(parts[1], set()).add(parts[2])
        # reopen and verify
        t0 = time.perf_counter()
        db = DB(path, memtable_bytes=memtable, fsync=fsync)
        res["recovery_ms"].append((time.perf_counter() - t0) * 1000)
        res["wal_records_replayed"].append(db.recovered_records)
        res["torn_tail_bytes_total"] += db.torn_bytes
        if pending is not None:
            k = pending[1]
            want_new = pending[2] if pending[0] == "P" else None
            try:
                got = db.get(k.encode())
                got = got.decode() if got is not None else None
            except Exception:
                got = "<error>"
            if got == want_new and model.get(k) != want_new:
                model[k] = want_new
                res["inflight_landed"] += 1
            else:
                res["inflight_not_landed"] += 1
        for i in range(KEYS):
            k = f"key{i:05d}"
            try:
                v = db.get(k.encode())
                v = v.decode() if v is not None else None
            except Exception:
                res["corrupt_reads"] += 1
                continue
            if v != model.get(k):
                if v is not None and v not in written.get(k, ()):
                    res["corrupt_reads"] += 1
                else:
                    res["lost_acked_writes"] += 1
        scan = {k.decode(): v.decode() for k, v in db.scan(limit=10 ** 9)}
        if scan != {k: v for k, v in model.items() if v is not None}:
            res["corrupt_reads"] += 1
        res["tables_after"].append(len(db.tables))
        db.close()
        res["rounds"] += 1
        if (r + 1) % 50 == 0:
            print(f"round {r + 1}: acked={res['acked_ops']} lost={res['lost_acked_writes']} "
                  f"corrupt={res['corrupt_reads']}", flush=True)
    rec = res.pop("recovery_ms")
    res["recovery_ms_p50"] = round(statistics.median(rec), 2)
    res["recovery_ms_p99"] = round(sorted(rec)[int(0.99 * (len(rec) - 1))], 2)
    res["recovery_ms_max"] = round(max(rec), 2)
    wr = res.pop("wal_records_replayed")
    res["wal_records_replayed_median"] = statistics.median(wr)
    ta = res.pop("tables_after")
    res["sstables_after_round_max"] = max(ta)
    res["fsync"] = fsync
    res["memtable_bytes"] = memtable
    res["keys"] = KEYS
    res["seconds"] = round(time.time() - t_start, 1)
    shutil.rmtree(work)
    if out:
        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        with open(out, "w") as f:
            json.dump(res, f, indent=1)
    print(json.dumps(res, indent=1))
    return res


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        child(sys.argv[2], int(sys.argv[3]), sys.argv[4], int(sys.argv[5]))
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=500)
    ap.add_argument("--fsync", default="always")
    ap.add_argument("--memtable", type=int, default=8192)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    r = run(a.rounds, a.out, a.fsync, a.memtable, a.seed)
    sys.exit(1 if r["lost_acked_writes"] or r["corrupt_reads"] else 0)
