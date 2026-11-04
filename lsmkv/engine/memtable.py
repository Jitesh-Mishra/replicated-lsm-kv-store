"""Skip-list memtable. Expected O(log n) insert and lookup, sorted iteration for the flush.

Values are bytes, or TOMBSTONE for a delete (it must shadow older SSTables until compaction drops it).
"""
from __future__ import annotations

import random

TOMBSTONE = None          # a deleted key is stored with value None
MAX_LEVEL = 16
P = 0.25


class _Node:
    __slots__ = ("key", "value", "next")

    def __init__(self, key, value, level):
        self.key = key
        self.value = value
        self.next = [None] * level


class SkipList:
    def __init__(self, seed: int | None = None):
        self.head = _Node(b"", None, MAX_LEVEL)
        self.level = 1
        self.n = 0
        self.bytes = 0
        self._rng = random.Random(seed)

    def _random_level(self) -> int:
        lvl = 1
        while lvl < MAX_LEVEL and self._rng.random() < P:
            lvl += 1
        return lvl

    def _find(self, key: bytes, update=None):
        x = self.head
        for i in range(self.level - 1, -1, -1):
            while x.next[i] is not None and x.next[i].key < key:
                x = x.next[i]
            if update is not None:
                update[i] = x
        return x.next[0]

    def put(self, key: bytes, value: bytes | None) -> None:
        update = [self.head] * MAX_LEVEL
        x = self._find(key, update)
        vlen = len(value) if value is not None else 0
        if x is not None and x.key == key:
            self.bytes += vlen - (len(x.value) if x.value is not None else 0)
            x.value = value
            return
        lvl = self._random_level()
        if lvl > self.level:
            self.level = lvl
        node = _Node(key, value, lvl)
        for i in range(lvl):
            node.next[i] = update[i].next[i]
            update[i].next[i] = node
        self.n += 1
        self.bytes += len(key) + vlen + 16

    def get(self, key: bytes):
        """(found, value). found=True with value None means a tombstone."""
        x = self._find(key)
        if x is not None and x.key == key:
            return True, x.value
        return False, None

    def items(self, start: bytes | None = None):
        x = self._find(start) if start is not None else self.head.next[0]
        while x is not None:
            yield x.key, x.value
            x = x.next[0]

    def __len__(self):
        return self.n
