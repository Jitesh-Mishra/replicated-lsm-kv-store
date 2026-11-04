# Canary release gate: evidence

The release workflow (`.github/workflows/release.yml`) runs the unit tests, then 150 SIGKILL rounds of
`chaos/crash_harness.py` with fsync=always. The release job runs only if the gate finds 0 lost acknowledged writes
and 0 corrupt reads.

| Branch | Commit | Change | Unit tests | Crash gate | Release job | Run |
|---|---|---|---|---|---|---|
| canary-demo/wal-retired-before-manifest | 0672d60 | retire the old WAL before the MANIFEST names the new SSTable (one line moved) | 12 passed | **failed**: 1,233 acknowledged writes lost in 150 rounds | **skipped** | [41897070804](https://github.com/Jitesh-Mishra/replicated-lsm-kv-store/actions/runs/41897070804) |
| main | 360c25c | none | 12 passed | passed: 104,103 acknowledged ops, 0 lost, 0 corrupt | ran (no tag, so nothing published) | [41897071264](https://github.com/Jitesh-Mishra/replicated-lsm-kv-store/actions/runs/41897071264) |

The broken commit passes every unit test. Only the crash gate catches it: if the process dies between the WAL
being retired and the MANIFEST being written, the flushed SSTable is an orphan and the WAL holding the same
writes is already gone.
