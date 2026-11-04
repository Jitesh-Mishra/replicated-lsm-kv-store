"""Fault injection points for the crash harness and the incident drill.

LSMKV_CRASH_POINTS="flush.after_sstable:0.3,compaction.before_manifest:0.5" makes the process SIGKILL itself
at that point with that probability. Unset (the normal case) costs one dict lookup per point.
"""
from __future__ import annotations

import os
import random
import signal

_points: dict[str, float] = {}
for item in filter(None, os.environ.get("LSMKV_CRASH_POINTS", "").split(",")):
    name, _, p = item.partition(":")
    _points[name.strip()] = float(p or 1.0)

_rng = random.Random(int(os.environ.get("LSMKV_CRASH_SEED", "0")) or None)


def point(name: str) -> None:
    p = _points.get(name)
    if p is not None and _rng.random() < p:
        os.write(2, f"CRASH {name}\n".encode())
        os.kill(os.getpid(), signal.SIGKILL)


def configure(points: dict[str, float]) -> None:
    """Set crash points programmatically (tests)."""
    _points.clear()
    _points.update(points)
