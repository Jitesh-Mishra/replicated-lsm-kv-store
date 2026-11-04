# KVWalFsyncSlow

**Fires when** WAL fsync p99 is above 50 ms for 2 minutes. With `FSYNC=always`, every write waits on it.

1. Disk health and saturation: `iostat -x 1`, noisy neighbours on the volume, throttled IOPS on the cloud disk.
2. Short term: move the data directory to faster storage, or (deliberately, with sign-off) switch to
   `FSYNC=batch`, which bounds loss on power failure to the batch window.
3. Record the decision and revert when the disk is fixed.
