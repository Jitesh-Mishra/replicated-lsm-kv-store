# ADR 0003: Size-tiered compaction first

**Context.** Flushes create many small tables. Reads probe more tables and disk holds dead versions until they
are merged.

**Options.**
1. Size-tiered: merge runs of similar-size tables. Low write amplification, more space and read amplification.
2. Leveled: bounded read and space amplification, more write amplification.

**Decision.** Size-tiered, merging runs of at least 4 neighbouring tables. It's simpler to make crash-safe (one
output, one MANIFEST swap) and fits a write-heavy store. Bloom filters cover most of the extra read cost: 0.35
tables probed per get in the benchmark.

**Trade-off.** Space amplification can reach about 2x during a large merge, and tombstones live until a merge
reaches the oldest table. Leveled compaction is on the next list, for comparison.
