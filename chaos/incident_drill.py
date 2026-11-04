"""Simulated on-call incident: a stuck compaction, detected by a real Prometheus alert and fixed by the runbook.

1. Start lsmkv (64 KiB memtable, so it flushes often) and Prometheus (1 s scrape and rule evaluation) loaded
   with the production rules in ops/alerts.yml.
2. Drive steady load: 600 writes/s and 600 reads/s.
3. Inject the fault: the compaction worker hangs mid-job (POST /admin/fault/stall_compaction).
4. Wait until Prometheus reports an alert firing.
5. Follow ops/runbooks/KVCompactionStuck.md, as a script: read /admin/status, confirm the job age and backlog,
   restart the compaction worker (POST /admin/compaction/restart).
6. Wait until no alert is firing or pending, then record the timeline.

The responder is a script, so "time to mitigate" has no human reaction time in it. Detection and recovery
times come from the alert rules (their "for" windows) and the system itself.

    python -m chaos.incident_drill --prometheus /path/to/prometheus --out reports/incident_drill.json
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

import httpx

PORT, PROM_PORT = 9180, 9190


def wait_http(url, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if httpx.get(url, timeout=0.5).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"{url} did not come up")


def alerts():
    r = httpx.get(f"http://127.0.0.1:{PROM_PORT}/api/v1/alerts", timeout=2).json()
    return [(a["labels"]["alertname"], a["state"]) for a in r["data"]["alerts"]]


def query_range(q, start, end):
    r = httpx.get(f"http://127.0.0.1:{PROM_PORT}/api/v1/query_range",
                  params={"query": q, "start": start, "end": end, "step": "1"}, timeout=10).json()
    res = r["data"]["result"]
    return [(float(t) - start, float(v)) for t, v in res[0]["values"]] if res else []


def main(prom_bin, out):
    work = tempfile.mkdtemp(prefix="drill-")
    env = dict(os.environ, DATA_DIR=f"{work}/data", MEMTABLE_BYTES="65536", ENABLE_FAULTS="1",
               PYTHONPATH=os.getcwd())
    srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "lsmkv.service.server:app", "--port", str(PORT),
                            "--log-level", "warning"], env=env)
    with open(f"{work}/prometheus.yml", "w") as f:
        f.write(f"""global: {{scrape_interval: 1s, evaluation_interval: 1s}}
rule_files: ["{os.path.abspath('ops/alerts.yml')}"]
scrape_configs:
  - job_name: lsmkv
    static_configs: [{{targets: ["127.0.0.1:{PORT}"]}}]
""")
    prom = subprocess.Popen([prom_bin, f"--config.file={work}/prometheus.yml", f"--storage.tsdb.path={work}/tsdb",
                             f"--web.listen-address=127.0.0.1:{PROM_PORT}", "--log.level=warn"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_http(f"http://127.0.0.1:{PORT}/healthz")
    wait_http(f"http://127.0.0.1:{PROM_PORT}/-/ready")
    stop = threading.Event()
    errors = [0]

    def load(kind, rate, seed):
        c = httpx.Client(timeout=5)
        rng = random.Random(seed)
        nxt = time.perf_counter()
        while not stop.is_set():
            k = f"user{rng.randrange(50_000):06d}"
            try:
                if kind == "w":
                    r = c.put(f"http://127.0.0.1:{PORT}/kv/{k}", content=os.urandom(100))
                else:
                    r = c.get(f"http://127.0.0.1:{PORT}/kv/{k}")
                if r.status_code >= 500:
                    errors[0] += 1
            except httpx.HTTPError:
                errors[0] += 1
            nxt += 1 / rate
            time.sleep(max(0, nxt - time.perf_counter()))
    threads = [threading.Thread(target=load, args=(k, 300, i), daemon=True)
               for i, k in enumerate(["w", "w", "r", "r"])]
    t_start = time.time()
    for t in threads:
        t.start()
    time.sleep(60)                                     # healthy baseline
    assert not [a for a in alerts() if a[1] == "firing"], alerts()
    tl = {"load": "600 writes/s + 600 reads/s, 50,000-key space, 100-byte values"}
    t_inject = time.time()
    httpx.post(f"http://127.0.0.1:{PORT}/admin/fault/stall_compaction").raise_for_status()
    first = None
    seen = {}
    while time.time() - t_inject < 600:
        for name, state in alerts():
            seen.setdefault((name, state), round(time.time() - t_inject, 1))
            if state == "firing" and first is None:
                first = (name, time.time())
        if first:
            break
        time.sleep(0.5)
    t_detect = first[1]
    # runbook KVCompactionStuck / KVCompactionBacklog, step 1 and 2: look at status
    st = httpx.get(f"http://127.0.0.1:{PORT}/admin/status").json()
    diagnosis = st["compaction"]
    # step 3: restart the compaction worker
    t_mitigate = time.time()
    httpx.post(f"http://127.0.0.1:{PORT}/admin/compaction/restart", timeout=30).raise_for_status()
    while True:
        act = alerts()
        for name, state in act:
            seen.setdefault((name, state), round(time.time() - t_inject, 1))
        if not act:
            break
        time.sleep(0.5)
    t_resolved = time.time()
    time.sleep(20)
    stop.set()
    end = time.time()
    series = {
        "sstables": query_range("kv_sstables", t_start, end),
        "compaction_backlog_tables": query_range("kv_compaction_backlog_tables", t_start, end),
        "compaction_job_age_s": query_range("kv_compaction_job_age_seconds", t_start, end),
        "get_p99_ms": query_range('1000 * histogram_quantile(0.99, sum by (le) (rate(kv_request_seconds_bucket{op="get"}[15s])))',
                                  t_start, end),
        "put_p99_ms": query_range('1000 * histogram_quantile(0.99, sum by (le) (rate(kv_request_seconds_bucket{op="put"}[15s])))',
                                  t_start, end),
        "tables_probed_per_get": query_range("rate(kv_read_tables_probed[15s]) / clamp_min(rate(kv_requests_total{op=\"get\"}[15s]), 1)",
                                             t_start, end),
    }
    res = {**tl, "fault": "compaction worker hangs mid-job",
           "first_alert": first[0],
           "time_to_detect_s": round(t_detect - t_inject, 1),
           "time_to_mitigate_s": round(t_mitigate - t_inject, 1),
           "time_to_resolve_s": round(t_resolved - t_inject, 1),
           "alert_events_s_after_injection": {f"{n} {s}": v for (n, s), v in sorted(seen.items(), key=lambda x: x[1])},
           "diagnosis_from_status": diagnosis,
           "client_errors": errors[0],
           "inject_at_s": round(t_inject - t_start, 1), "resolved_at_s": round(t_resolved - t_start, 1),
           "series": series}
    srv.send_signal(signal.SIGTERM)
    prom.send_signal(signal.SIGTERM)
    srv.wait()
    prom.wait()
    shutil.rmtree(work)
    os.makedirs("reports", exist_ok=True)
    with open(out, "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps({k: v for k, v in res.items() if k != "series"}, indent=1))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prometheus", default=shutil.which("prometheus") or "prometheus")
    ap.add_argument("--out", default="reports/incident_drill.json")
    a = ap.parse_args()
    main(a.prometheus, a.out)
