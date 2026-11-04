"""Leader failover: how many acknowledged writes does each replication mode lose?

Per trial: start a follower and a leader (separate processes), 8 client threads write unique keys to the leader
as fast as it acknowledges, the leader is SIGKILLed at a random moment between 3 and 5 s, the follower is
promoted, and every key the leader acknowledged is read back from the new leader.

    python -m chaos.failover --trials 5 --out reports/failover.json
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

import httpx


def start(port, env_extra, data):
    env = dict(os.environ, DATA_DIR=data, PYTHONPATH=os.getcwd(), **env_extra)
    p = subprocess.Popen([sys.executable, "-m", "uvicorn", "lsmkv.service.server:app", "--port", str(port),
                          "--log-level", "warning"], env=env)
    for _ in range(100):
        try:
            if httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=0.5).status_code == 200:
                return p
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError("server did not start")


def trial(mode: str, seed: int, threads: int = 8, batch_ms: int = 50):
    rng = random.Random(seed)
    work = tempfile.mkdtemp(prefix="failover-")
    fol = start(9102, {"ROLE": "follower"}, f"{work}/f")
    lead = start(9101, {"ROLE": "leader", "FOLLOWER_URL": "http://127.0.0.1:9102", "REPL_MODE": mode,
                        "REPL_BATCH_MS": str(batch_ms)}, f"{work}/l")
    acked, lock, stop = [], threading.Lock(), threading.Event()
    lat = []

    def client(i):
        c = httpx.Client(timeout=2.0)
        n = 0
        while not stop.is_set():
            k = f"t{i}-{n:07d}"
            t = time.perf_counter()
            try:
                r = c.put(f"http://127.0.0.1:9101/kv/{k}", content=b"v" * 32)
                if r.status_code == 200:
                    with lock:
                        acked.append(k)
                        lat.append(time.perf_counter() - t)
            except httpx.HTTPError:
                pass
            n += 1
    ts = [threading.Thread(target=client, args=(i,), daemon=True) for i in range(threads)]
    t0 = time.time()
    for t in ts:
        t.start()
    time.sleep(3 + 2 * rng.random())
    lead.send_signal(signal.SIGKILL)
    lead.wait()
    killed_after = time.time() - t0
    stop.set()
    for t in ts:
        t.join(timeout=5)
    c = httpx.Client(timeout=5.0)
    c.post("http://127.0.0.1:9102/admin/promote").raise_for_status()
    t_up = time.perf_counter()
    c.put("http://127.0.0.1:9102/kv/after-failover", content=b"ok").raise_for_status()
    first_write_ms = (time.perf_counter() - t_up) * 1000
    lost = sum(1 for k in acked if c.get(f"http://127.0.0.1:9102/kv/{k}").status_code != 200)
    fol.send_signal(signal.SIGTERM)
    fol.wait()
    shutil.rmtree(work)
    lat.sort()
    return {"mode": mode, "acked_writes": len(acked), "lost_acked_writes": lost,
            "leader_killed_after_s": round(killed_after, 2), "write_ops_per_s": round(len(acked) / killed_after, 1),
            "put_p50_ms": round(1000 * lat[len(lat) // 2], 2), "put_p99_ms": round(1000 * lat[int(0.99 * len(lat))], 2),
            "promoted_first_write_ms": round(first_write_ms, 1)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--out", default="reports/failover.json")
    a = ap.parse_args()
    rows = []
    for mode in ("sync", "async"):
        for s in range(a.trials):
            r = trial(mode, 100 + s)
            print(r, flush=True)
            rows.append(r)
    summary = {}
    for mode in ("sync", "async"):
        rs = [r for r in rows if r["mode"] == mode]
        summary[mode] = {"trials": len(rs), "acked_writes": sum(r["acked_writes"] for r in rs),
                         "lost_acked_writes": sum(r["lost_acked_writes"] for r in rs),
                         "lost_per_trial": [r["lost_acked_writes"] for r in rs],
                         "write_ops_per_s_median": statistics.median(r["write_ops_per_s"] for r in rs),
                         "put_p50_ms_median": statistics.median(r["put_p50_ms"] for r in rs),
                         "put_p99_ms_median": statistics.median(r["put_p99_ms"] for r in rs)}
    out = {"setup": "leader and follower as separate uvicorn processes on one machine, fsync=always on both, "
                    "8 client threads writing unique 32-byte values, leader SIGKILLed at a random 3-5 s, "
                    "async ships every 50 ms", "summary": summary, "trials": rows}
    os.makedirs("reports", exist_ok=True)
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1)
    print(json.dumps(summary, indent=1))
