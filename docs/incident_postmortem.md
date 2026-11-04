# Postmortem: stuck compaction (simulated, drill of 2026-11-04)

**Status:** resolved. **Severity:** page (`KVCompactionBacklog`). **Customer impact:** none measured. 0 client
errors, and get and put p99 unchanged. **Detection:** alert, 45.8 s after the fault. **Resolution:** 46.4 s after
the fault.

This was a drill: the fault was injected on purpose (`POST /admin/fault/stall_compaction`) by
`chaos/incident_drill.py`, and the responder was a script following the runbook. Raw data:
`reports/incident_drill.json`.

## Timeline (seconds after the fault)
| t | Event |
|---:|---|
| 0.0 | Compaction worker hangs mid-merge. Load steady at 600 writes/s and 600 reads/s. |
| ~5 | Backlog starts climbing: flushes keep adding 64 KiB tables, nothing merges them. |
| 15.9 | `KVCompactionBacklog` pending (backlog at or above 12 tables). |
| 45.8 | `KVCompactionBacklog` firing (after its 30 s `for` window). Runbook opened. |
| 45.8 | `GET /admin/status`: compaction `running: true`, `job_age_s: 40.8`, `backlog_tables: 37`. That is a stuck job, not a slow one (a healthy merge here takes well under a second). |
| 45.8 | `POST /admin/compaction/restart`: aborts the stuck job, deletes its partial output, starts a new worker. |
| 46.4 | New worker merges the backlog; all alerts clear. |

## What went well
- The backlog alert fired before anything a user would notice. Reads stayed flat because bloom filters skip most of
  the extra tables.
- The runbook's diagnosis step (job age plus backlog in `/admin/status`) distinguished "stuck" from "slow" in one call.
- The restart was safe by design: the MANIFEST still named the input tables, and the half-written output was an
  orphan the MANIFEST never referenced.

## What went poorly
- `KVCompactionStuck` (job age over 60 s for 30 s) would have fired only at about 90 s. The backlog alert beat it.
  For this workload, the job-age threshold is too loose.
- Detection time is mostly the alert's own `for` window (30 s), on top of 16 s for the backlog to reach 12.
- Nothing records *why* the job hung. A real stall would need a stack dump before the restart.

## Action items
1. Lower the `KVCompactionStuck` threshold to 30 s, or make it relative to recent job durations. (Owner: me.)
2. Add a "dump compactor stack" step before restart in the runbook, and an endpoint for it.
3. Add a watchdog that restarts the compactor on its own after N minutes of no progress, and pages anyway.
4. Run the same drill against a slow disk (`KVWalFsyncSlow`) and a dead follower (`KVReplicationLagHigh`).
