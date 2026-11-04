"""Bloom filter with double hashing (Kirsch and Mitzenmacher): h_i(x) = h1 + i * h2 mod m.

Sized for a target false-positive rate p with n keys: m = -n ln p / (ln 2)^2 bits, k = (m / n) ln 2 hashes.
"""
from __future__ import annotations

import hashlib
import math
import struct


class BloomFilter:
    def __init__(self, m_bits: int, k: int, bits: bytearray | None = None):
        self.m = max(8, m_bits)
        self.k = max(1, k)
        self.bits = bits if bits is not None else bytearray((self.m + 7) // 8)

    @classmethod
    def for_capacity(cls, n: int, fp_rate: float = 0.01) -> "BloomFilter":
        n = max(1, n)
        m = int(math.ceil(-n * math.log(fp_rate) / (math.log(2) ** 2)))
        k = max(1, round(m / n * math.log(2)))
        return cls(m, k)

    def _hashes(self, key: bytes):
        d = hashlib.blake2b(key, digest_size=16).digest()
        h1, h2 = struct.unpack("<QQ", d)
        h2 |= 1                                   # odd step so the probes cover the table
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    def add(self, key: bytes) -> None:
        for h in self._hashes(key):
            self.bits[h >> 3] |= 1 << (h & 7)

    def might_contain(self, key: bytes) -> bool:
        return all(self.bits[h >> 3] & (1 << (h & 7)) for h in self._hashes(key))

    def theoretical_fp_rate(self, n: int) -> float:
        return (1 - math.exp(-self.k * n / self.m)) ** self.k

    def to_bytes(self) -> bytes:
        return struct.pack("<QI", self.m, self.k) + bytes(self.bits)

    @classmethod
    def from_bytes(cls, b: bytes) -> "BloomFilter":
        m, k = struct.unpack_from("<QI", b)
        return cls(m, k, bytearray(b[12:]))
