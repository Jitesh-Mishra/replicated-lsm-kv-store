"""HTTP service around the engine: key-value API, Prometheus metrics, admin endpoints, leader-follower replication.

Environment
  DATA_DIR         store directory (default ./data)
  ROLE             leader | follower (default leader)
  FOLLOWER_URL     leader only: where to ship the log (unset = no replication)
  REPL_MODE        sync | async. sync acknowledges a write only after the follower has it in its own WAL;
                   async acknowledges after the local WAL and ships in the background every REPL_BATCH_MS
  FSYNC            always | batch | none (default always)
  MEMTABLE_BYTES   flush threshold (default 1 MiB)
  WAL_ARCHIVE      1 keeps closed WAL segments for point-in-time restore
  ENABLE_FAULTS    1 enables /admin/fault/* (incident drills only)

    uvicorn lsmkv.service.server:app --port 8080
"""
from __future__ import annotations

import base64
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from lsmkv.engine.db import DB
from lsmkv.engine.wal import DELETE, PUT, Record

LAT_BUCKETS = (0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5)
REQS = Counter("kv_requests_total", "Requests by operation and status", ["op", "status"])
LAT = Histogram("kv_request_seconds", "Request latency by operation", ["op"], buckets=LAT_BUCKETS)
FSYNC = Histogram("kv_wal_fsync_seconds", "WAL fsync duration", buckets=LAT_BUCKETS)
REPL_SHIP = Histogram("kv_replication_ship_seconds", "Time to ship a batch to the follower", buckets=LAT_BUCKETS)


class Node:
    def __init__(self):
        self.role = os.environ.get("ROLE", "leader")
        self.follower = os.environ.get("FOLLOWER_URL")
        self.mode = os.environ.get("REPL_MODE", "sync")
        self.batch_s = float(os.environ.get("REPL_BATCH_MS", "50")) / 1000
        self.db = DB(os.environ.get("DATA_DIR", "./data"),
                     memtable_bytes=int(os.environ.get("MEMTABLE_BYTES", str(1 << 20))),
                     fsync=os.environ.get("FSYNC", "always"),
                     background_compaction=True,
                     wal_archive=os.environ.get("WAL_ARCHIVE") == "1",
                     on_fsync=FSYNC.observe)
        self.write_lock = threading.Lock()
        self.queue: deque[Record] = deque()
        self.follower_seq = 0
        self.http = httpx.Client(timeout=5.0)
        if self.role == "leader" and self.follower and self.mode == "async":
            threading.Thread(target=self._shipper, daemon=True).start()

    # replication ---------------------------------------------------------
    def _ship(self, recs: list[Record]):
        t = time.perf_counter()
        body = [{"seq": r.seq, "ts": r.ts, "op": r.op, "k": base64.b64encode(r.key).decode(),
                 "v": base64.b64encode(r.value).decode() if r.value is not None else None} for r in recs]
        r = self.http.post(f"{self.follower}/replicate", json=body)
        r.raise_for_status()
        self.follower_seq = r.json()["applied_seq"]
        REPL_SHIP.observe(time.perf_counter() - t)

    def _shipper(self):
        while True:
            time.sleep(self.batch_s)
            batch = []
            while self.queue and len(batch) < 1000:
                batch.append(self.queue.popleft())
            if batch:
                try:
                    self._ship(batch)
                except Exception:
                    for r in reversed(batch):          # keep order, retry next tick
                        self.queue.appendleft(r)

    def write(self, op: int, key: bytes, value: bytes | None) -> int:
        if self.role != "leader":
            raise HTTPException(409, "not the leader")
        with self.write_lock:                          # one ordered log; sync mode ships inside the lock
            seq = self.db.put(key, value) if op == PUT else self.db.delete(key)
            if self.follower:
                rec = Record(seq, time.time(), op, key, value)
                if self.mode == "sync":
                    try:
                        self._ship([rec])
                    except Exception as e:
                        raise HTTPException(503, f"follower did not acknowledge: {e}")
                else:
                    self.queue.append(rec)
        return seq

    def apply(self, body: list[dict]) -> int:
        with self.write_lock:
            for r in body:
                if r["seq"] <= self.db.seq:
                    continue
                k = base64.b64decode(r["k"])
                if r["op"] == PUT:
                    self.db.put(k, base64.b64decode(r["v"]), _seq=r["seq"], _ts=r["ts"])
                else:
                    self.db.delete(k, _seq=r["seq"], _ts=r["ts"])
            return self.db.seq


node: Node | None = None


@asynccontextmanager
async def lifespan(_app):
    global node
    node = Node()
    yield
    node.db.close()


app = FastAPI(title="lsmkv", version="0.1", lifespan=lifespan)


class Gauges:
    def collect(self):
        if node is None:
            return
        db = node.db
        st = db.compaction_state
        vals = {
            "kv_memtable_bytes": ("Bytes in the memtable", db.mem.bytes),
            "kv_sstables": ("Live SSTables", len(db.tables)),
            "kv_compaction_backlog_tables": ("SSTables waiting in an eligible size tier", db.backlog()),
            "kv_compaction_running": ("1 while a compaction job runs", int(st["running"])),
            "kv_compaction_job_age_seconds": ("Age of the running compaction job",
                                              time.time() - st["started"] if st["running"] and st["started"] else 0),
            "kv_disk_bytes": ("Bytes in the data directory", db.disk_bytes()),
            "kv_last_seq": ("Last sequence number", db.seq),
            "kv_flushes": ("Memtable flushes since start", db.stats["flushes"]),
            "kv_compactions": ("Compactions since start", db.stats["compactions"]),
            "kv_read_tables_probed": ("SSTables read past the bloom filter", db.stats["tables_probed"]),
            "kv_read_bloom_negatives": ("SSTable lookups skipped by the bloom filter", db.stats["bloom_negatives"]),
            "kv_replication_lag_records": ("Leader seq minus follower-acknowledged seq",
                                           db.seq - node.follower_seq if node.follower else 0),
            "kv_is_leader": ("1 on the leader", int(node.role == "leader")),
        }
        for name, (doc, v) in vals.items():
            g = GaugeMetricFamily(name, doc)
            g.add_metric([], float(v))
            yield g


REGISTRY.register(Gauges())


def _timed(op):
    class T:
        def __enter__(self):
            self.t = time.perf_counter()
            self.status = "200"
            return self

        def __exit__(self, et, e, tb):
            if et is not None:
                self.status = str(getattr(e, "status_code", 500))
            LAT.labels(op).observe(time.perf_counter() - self.t)
            REQS.labels(op, self.status).inc()
    return T()


@app.put("/kv/{key}")
async def put(key: str, request: Request):
    body = await request.body()
    with _timed("put") as t:
        seq = await _run(node.write, PUT, key.encode(), body)
        return {"seq": seq}


@app.delete("/kv/{key}")
async def delete(key: str):
    with _timed("delete"):
        return {"seq": await _run(node.write, DELETE, key.encode(), None)}


@app.get("/kv/{key}")
async def get(key: str):
    with _timed("get") as t:
        v = node.db.get(key.encode())
        if v is None:
            t.status = "404"
            raise HTTPException(404, "not found")
        return Response(v, media_type="application/octet-stream")


@app.get("/scan")
def scan(start: str = "", end: str | None = None, limit: int = 100):
    with _timed("scan"):
        rows = node.db.scan(start.encode(), end.encode() if end else None, min(limit, 10_000))
        return {"items": [[k.decode(), v.decode(errors="replace")] for k, v in rows]}


@app.post("/replicate")
def replicate(body: list[dict]):
    if node.role != "follower":
        raise HTTPException(409, "not a follower")
    return {"applied_seq": node.apply(body)}


@app.post("/admin/promote")
def promote():
    node.role = "leader"
    node.follower = None
    return {"role": node.role, "seq": node.db.seq}


@app.get("/admin/status")
def status():
    db = node.db
    st = db.compaction_state
    return {"role": node.role, "seq": db.seq, "sstables": len(db.tables), "memtable_bytes": db.mem.bytes,
            "compaction": {"running": st["running"], "error": st["error"],
                           "job_age_s": round(time.time() - st["started"], 1) if st["running"] else 0,
                           "backlog_tables": db.backlog(), "stalled_fault": db._stall.is_set()},
            "replication": {"follower": node.follower, "mode": node.mode, "follower_seq": node.follower_seq,
                            "queued": len(node.queue)}}


@app.post("/admin/compaction/restart")
def compaction_restart():
    node.db.restart_compactor()
    return status()


@app.post("/admin/snapshot")
def snapshot(dest: str):
    return node.db.snapshot(dest)


@app.post("/admin/fault/{name}")
def fault(name: str):
    if os.environ.get("ENABLE_FAULTS") != "1":
        raise HTTPException(403, "faults disabled")
    if name == "stall_compaction":
        node.db._stall.set()
    elif name == "clear":
        node.db._stall.clear()
    else:
        raise HTTPException(404, name)
    return {"fault": name}


@app.get("/healthz")
def healthz():
    return {"ok": True, "role": node.role}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def _run(fn, *a):
    import anyio
    return await anyio.to_thread.run_sync(fn, *a)
