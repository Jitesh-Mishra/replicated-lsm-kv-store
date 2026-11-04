"""Immutable sorted string table.

Layout: data entries | sparse index | bloom filter | footer
  entry:  key length u16 | value length i32 (-1 = tombstone) | key | value
  index:  count u32, then (key length u16 | key | data offset u64) for every INDEX_EVERY-th entry
  footer: index offset u64 | bloom offset u64 | entries u64 | crc32 of everything before the footer u32 | magic

A lookup checks the bloom filter, binary-searches the sparse index for the block that could hold the key,
and scans at most INDEX_EVERY entries. The whole file is checksummed and verified on open.
Files are written to a temp name, fsynced, renamed and the directory fsynced, so a crash leaves either the
old state or a complete table, never a partial one under the real name.
"""
from __future__ import annotations

import bisect
import os
import struct
import zlib

from . import faults
from .bloom import BloomFilter

ENTRY = struct.Struct("<Hi")
FOOTER = struct.Struct("<QQQI8s")
MAGIC = b"LSMKVSST"
INDEX_EVERY = 16


class CorruptTable(Exception):
    pass


def fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_sstable(path: str, items, n_hint: int, fp_rate: float = 0.01) -> int:
    """Write sorted (key, value|None) pairs. Returns number of entries."""
    bloom = BloomFilter.for_capacity(n_hint, fp_rate)
    data = bytearray()
    index = []
    n = 0
    for k, v in items:
        if n % INDEX_EVERY == 0:
            index.append((k, len(data)))
        data += ENTRY.pack(len(k), -1 if v is None else len(v)) + k + (v or b"")
        bloom.add(k)
        n += 1
    idx = bytearray(struct.pack("<I", len(index)))
    for k, off in index:
        idx += struct.pack("<H", len(k)) + k + struct.pack("<Q", off)
    body = bytes(data) + bytes(idx) + bloom.to_bytes()
    footer = FOOTER.pack(len(data), len(data) + len(idx), n, zlib.crc32(body), MAGIC)
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(body + footer)
        f.flush()
        os.fsync(f.fileno())
    faults.point("sstable.before_rename")
    os.replace(tmp, path)
    fsync_dir(os.path.dirname(path) or ".")
    return n


class SSTable:
    def __init__(self, path: str, table_id: int):
        self.path = path
        self.id = table_id
        with open(path, "rb") as f:
            buf = f.read()
        if len(buf) < FOOTER.size:
            raise CorruptTable(path)
        self.index_off, self.bloom_off, self.n, crc, magic = FOOTER.unpack_from(buf, len(buf) - FOOTER.size)
        body = buf[:-FOOTER.size]
        if magic != MAGIC or zlib.crc32(body) != crc:
            raise CorruptTable(path)
        self.size = len(buf)
        self.data = memoryview(body)[:self.index_off]
        cnt = struct.unpack_from("<I", body, self.index_off)[0]
        off = self.index_off + 4
        self.index_keys, self.index_offs = [], []
        for _ in range(cnt):
            kl = struct.unpack_from("<H", body, off)[0]
            self.index_keys.append(bytes(body[off + 2:off + 2 + kl]))
            self.index_offs.append(struct.unpack_from("<Q", body, off + 2 + kl)[0])
            off += 2 + kl + 8
        self.bloom = BloomFilter.from_bytes(bytes(body[self.bloom_off:]))
        self.min_key = self.index_keys[0] if self.index_keys else b""
        self.max_key = self._last_key()

    def _entries(self, start: int, end: int):
        d, off = self.data, start
        while off < end:
            kl, vl = ENTRY.unpack_from(d, off)
            off += ENTRY.size
            k = bytes(d[off:off + kl])
            off += kl
            if vl < 0:
                v = None
            else:
                v = bytes(d[off:off + vl])
                off += vl
            yield k, v

    def _last_key(self) -> bytes:
        if not self.index_offs:
            return b""
        last = b""
        for k, _ in self._entries(self.index_offs[-1], self.index_off):
            last = k
        return last

    def get(self, key: bytes):
        """(found, value, bloom_passed). found with value None is a tombstone."""
        if not self.index_keys or key < self.min_key or key > self.max_key:
            return False, None, False
        if not self.bloom.might_contain(key):
            return False, None, False
        i = bisect.bisect_right(self.index_keys, key) - 1
        end = self.index_offs[i + 1] if i + 1 < len(self.index_offs) else self.index_off
        for k, v in self._entries(self.index_offs[i], end):
            if k == key:
                return True, v, True
            if k > key:
                break
        return False, None, True

    def items(self, start: bytes | None = None):
        if not self.index_offs:
            return
        i = 0 if start is None else max(0, bisect.bisect_right(self.index_keys, start) - 1)
        for k, v in self._entries(self.index_offs[i], self.index_off):
            if start is None or k >= start:
                yield k, v
