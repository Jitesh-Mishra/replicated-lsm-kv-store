"""Append-only write-ahead log.

Record: [payload length u32][crc32(payload) u32][payload]
Payload: seq u64 | ts f64 (unix seconds) | op u8 (1 put, 2 delete) | key length u16 | value length u32 | key | value

Replay stops at the first truncated or checksum-failing record (a torn tail from a crash) and the file is
truncated there, so later appends never sit behind garbage. Records before it are intact by checksum.

fsync policy:
  always  fsync before the write is acknowledged (durable against power loss)
  batch   fsync at most every batch_ms; an acknowledged write can be lost on power loss inside that window
  none    leave it to the OS (still survives a process crash: the data is in the page cache)
"""
from __future__ import annotations

import os
import struct
import time
import zlib
from dataclasses import dataclass

HDR = struct.Struct("<II")
BODY = struct.Struct("<QdBHI")
PUT, DELETE = 1, 2


@dataclass
class Record:
    seq: int
    ts: float
    op: int
    key: bytes
    value: bytes | None

    def encode(self) -> bytes:
        v = self.value or b""
        payload = BODY.pack(self.seq, self.ts, self.op, len(self.key), len(v)) + self.key + v
        return HDR.pack(len(payload), zlib.crc32(payload)) + payload


def decode_stream(buf: bytes):
    """Yield (record, end_offset) for each intact record; stop at the first bad one."""
    off = 0
    while off + HDR.size <= len(buf):
        n, crc = HDR.unpack_from(buf, off)
        start = off + HDR.size
        if n < BODY.size or start + n > len(buf):
            return
        payload = buf[start:start + n]
        if zlib.crc32(payload) != crc:
            return
        seq, ts, op, kl, vl = BODY.unpack_from(payload)
        if BODY.size + kl + vl != n or op not in (PUT, DELETE):
            return
        key = payload[BODY.size:BODY.size + kl]
        val = payload[BODY.size + kl:] if op == PUT else None
        off = start + n
        yield Record(seq, ts, op, key, val), off


def read_records(path: str) -> tuple[list[Record], int, int]:
    """(records, valid_bytes, file_bytes)."""
    if not os.path.exists(path):
        return [], 0, 0
    with open(path, "rb") as f:
        buf = f.read()
    recs, end = [], 0
    for r, off in decode_stream(buf):
        recs.append(r)
        end = off
    return recs, end, len(buf)


class WAL:
    def __init__(self, path: str, fsync: str = "always", batch_ms: float = 10.0, on_fsync=None):
        assert fsync in ("always", "batch", "none")
        self.path = path
        self.policy = fsync
        self.batch_s = batch_ms / 1000
        self.on_fsync = on_fsync                 # callback(seconds) for metrics
        recs, valid, size = read_records(path)
        self.recovered = recs
        self.torn_bytes = size - valid
        self.f = open(path, "ab")
        if size > valid:
            self.f.truncate(valid)
            self._fsync()
        self.bytes = valid
        self._last_sync = time.monotonic()

    def _fsync(self):
        t = time.perf_counter()
        os.fsync(self.f.fileno())
        if self.on_fsync:
            self.on_fsync(time.perf_counter() - t)
        self._last_sync = time.monotonic()

    def append(self, rec: Record) -> None:
        b = rec.encode()
        self.f.write(b)
        self.f.flush()
        self.bytes += len(b)
        if self.policy == "always" or (self.policy == "batch" and time.monotonic() - self._last_sync >= self.batch_s):
            self._fsync()

    def sync(self):
        self.f.flush()
        self._fsync()

    def close(self):
        if not self.f.closed:
            self.f.flush()
            if self.policy != "none":
                os.fsync(self.f.fileno())
            self.f.close()
