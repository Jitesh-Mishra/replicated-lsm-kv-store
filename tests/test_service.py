import os
import random
import time

from fastapi.testclient import TestClient


def test_api_roundtrip_metrics_and_compaction_restart(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "d"))
    monkeypatch.setenv("MEMTABLE_BYTES", "4096")
    monkeypatch.setenv("ENABLE_FAULTS", "1")
    from lsmkv.service import server
    with TestClient(server.app) as c:
        rng, ref = random.Random(0), {}
        for i in range(800):
            k = f"k{rng.randrange(200):04d}"
            if rng.random() < 0.8:
                v = os.urandom(20).hex()
                assert c.put(f"/kv/{k}", content=v).status_code == 200
                ref[k] = v
            else:
                c.delete(f"/kv/{k}")
                ref.pop(k, None)
        for k, v in ref.items():
            assert c.get(f"/kv/{k}").text == v
        assert c.get("/kv/missing").status_code == 404
        items = c.get("/scan", params={"start": "k0100", "limit": 5}).json()["items"]
        assert [k for k, _ in items] == sorted(k for k in ref if k >= "k0100")[:5]
        m = c.get("/metrics").text
        for name in ("kv_request_seconds_bucket", "kv_sstables", "kv_compaction_backlog_tables",
                     "kv_wal_fsync_seconds_count", "kv_disk_bytes"):
            assert name in m
        # stuck compaction, then the runbook action
        c.post("/admin/fault/stall_compaction")
        for i in range(600):
            c.put(f"/kv/s{i:04d}", content="x" * 50)
        assert c.post("/admin/compaction/restart").status_code == 200
        deadline = time.time() + 20
        while c.get("/admin/status").json()["compaction"]["backlog_tables"] and time.time() < deadline:
            time.sleep(0.2)
        assert c.get("/admin/status").json()["compaction"]["backlog_tables"] == 0
        for k, v in ref.items():
            assert c.get(f"/kv/{k}").text == v
