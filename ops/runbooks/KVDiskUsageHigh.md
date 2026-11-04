# KVDiskUsageHigh

**Fires when** the data directory is above 50 GB for 5 minutes.

1. Is compaction behind (dead data not reclaimed)? Follow KVCompactionBacklog first.
2. `WAL_ARCHIVE=1` keeps every closed WAL segment for point-in-time restore. Check `archive/` size; move segments
   older than the newest snapshot to backup storage.
3. Real growth: expand the volume, then plan capacity.
