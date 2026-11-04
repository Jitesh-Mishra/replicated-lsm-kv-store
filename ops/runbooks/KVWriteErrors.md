# KVWriteErrors

**Fires when** puts or deletes return 5xx for a minute.

1. Status `503 follower did not acknowledge` (sync replication): the follower is down or slow.
   Check `$FOLLOWER/healthz`. Restore it, or, as a deliberate trade-off, switch the leader to `REPL_MODE=async`
   and record that failover may lose writes until it is back.
   Note: with sync replication a 503 can follow a write that is already in the leader's WAL; clients must retry
   idempotently.
2. Disk full or read-only (OSError in the log): see KVDiskUsageHigh.
3. Anything else: capture the traceback, restart once, open a ticket.
