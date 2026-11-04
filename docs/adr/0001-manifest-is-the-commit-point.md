# ADR 0001: The MANIFEST rename is the commit point

**Context.** A flush touches three things: a new SSTable, the WAL, and the list of live tables. A crash can stop
between any two of those steps.

**Options.**
1. Infer state from the files present.
2. A single MANIFEST, replaced atomically with temp file, fsync, rename and directory fsync, that names exactly the
   live tables and the live WAL generation.

**Decision.** Option 2. On open, anything the MANIFEST does not name is an orphan and is removed. Old WAL
generations are retired (deleted or archived).

**Trade-off.** One extra fsync plus rename per flush and per compaction. In exchange, there is one place to reason
about recovery, and the crash harness can kill at every step around it. Getting the order wrong is exactly the bug
the canary branch demonstrates: retiring the WAL before the MANIFEST is written lost 843 acknowledged writes in
40 rounds.
