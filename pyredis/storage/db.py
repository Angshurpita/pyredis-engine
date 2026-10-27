"""
Redis Database (Keyspace) implementation.
Handles:
- In-memory key-value dictionary with typed RedisObject
- TTL & Key expiration (Passive lazy check + Active randomized background cycle)
- Memory management & eviction enforcement
- Glob pattern matching for KEYS command
"""
import fnmatch
import random
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

from .eviction import EvictionPolicy, select_key_to_evict
from .types import RedisDataType, RedisObject


class OutOfMemoryError(Exception):
    """Raised when maxmemory is reached and no keys can be evicted."""
    pass


class Database:
    """
    Represents a single Redis database instance (e.g. DB 0).
    """

    def __init__(
        self,
        db_id: int = 0,
        maxmemory: int = 0,  # 0 means unlimited
        maxmemory_policy: EvictionPolicy = EvictionPolicy.NOEVICTION,
    ):
        self.db_id = db_id
        self.maxmemory = maxmemory
        self.maxmemory_policy = maxmemory_policy

        self._dict: Dict[bytes, RedisObject] = {}
        self._expires: Dict[bytes, float] = {}  # key -> unix timestamp in seconds

    # --- Key Access and Expiration ---

    def _is_expired(self, key: bytes, now: Optional[float] = None) -> bool:
        """Internal check to see if key is expired."""
        exp = self._expires.get(key)
        if exp is None:
            return False
        if now is None:
            now = time.time()
        return now >= exp

    def _lazy_expire(self, key: bytes) -> bool:
        """
        Passive eviction: Check if key has expired.
        If expired, delete it immediately.
        Returns True if deleted due to expiry, False otherwise.
        """
        if self._is_expired(key):
            self.delete(key)
            return True
        return False

    def get(self, key: bytes) -> Optional[RedisObject]:
        """Retrieve RedisObject for key, touching LRU metadata if found."""
        if self._lazy_expire(key):
            return None

        obj = self._dict.get(key)
        if obj is not None:
            obj.touch()
        return obj

    def set(
        self,
        key: bytes,
        obj: RedisObject,
        expire_at: Optional[float] = None,
    ) -> None:
        """Store key-value with optional expiration timestamp."""
        self._dict[key] = obj
        if expire_at is not None:
            self._expires[key] = expire_at
        else:
            self._expires.pop(key, None)

        # Evict keys if memory limit exceeded
        self._enforce_memory_limit()

    def delete(self, key: bytes) -> bool:
        """Delete key from keyspace and expires."""
        self._expires.pop(key, None)
        return self._dict.pop(key, None) is not None

    def exists(self, key: bytes) -> bool:
        """Check if key exists and is not expired."""
        if self._lazy_expire(key):
            return False
        return key in self._dict

    def set_expire(self, key: bytes, expire_at: float) -> bool:
        """Set absolute expiration time in seconds."""
        if not self.exists(key):
            return False
        self._expires[key] = expire_at
        return True

    def get_ttl(self, key: bytes) -> float:
        """
        Return remaining TTL in seconds.
        -2 if the key does not exist (or expired).
        -1 if the key exists but has no associated expire.
        >= 0 remaining time in seconds.
        """
        if not self.exists(key):
            return -2.0
        exp = self._expires.get(key)
        if exp is None:
            return -1.0
        remaining = exp - time.time()
        return max(0.0, remaining)

    def persist(self, key: bytes) -> bool:
        """Remove the existing timeout on key."""
        if not self.exists(key):
            return False
        if key in self._expires:
            del self._expires[key]
            return True
        return False

    # --- Active Background Expiry Cycle ---

    def active_expire_cycle(
        self, sample_size: int = 20, threshold_pct: float = 0.25, max_loops: int = 16
    ) -> int:
        """
        Redis activeExpireCycle algorithm:
        1. Sample up to `sample_size` keys from _expires.
        2. Delete expired keys.
        3. If > threshold_pct (default 25%) were expired, repeat.
        4. Bound maximum iterations to prevent blocking the event loop.
        """
        total_evicted = 0
        now = time.time()

        for _ in range(max_loops):
            if not self._expires:
                break

            keys = list(self._expires.keys())
            sample = random.sample(keys, min(sample_size, len(keys)))
            expired_in_sample = 0

            for k in sample:
                if self._is_expired(k, now):
                    self.delete(k)
                    expired_in_sample += 1
                    total_evicted += 1

            # If less than 25% expired, sample is mostly alive, stop cycling
            if expired_in_sample < (len(sample) * threshold_pct):
                break

        return total_evicted

    # --- Memory Management & Eviction ---

    def approximate_memory_bytes(self) -> int:
        """
        Estimate memory usage of keys and values in bytes.
        """
        total = 0
        for k, obj in self._dict.items():
            total += len(k) + 64  # Overhead per key
            if obj.data_type == RedisDataType.STRING:
                total += len(obj.value) if isinstance(obj.value, bytes) else 32
            elif obj.data_type in (RedisDataType.LIST, RedisDataType.SET):
                total += sum(len(x) + 32 for x in obj.value)
            elif obj.data_type == RedisDataType.HASH:
                total += sum(len(hk) + len(hv) + 64 for hk, hv in obj.value.items())
            elif obj.data_type == RedisDataType.ZSET:
                total += len(obj.value) * 128
        return total

    def _enforce_memory_limit(self) -> None:
        """Evict keys if memory limit exceeded."""
        if self.maxmemory <= 0:
            return

        while self.approximate_memory_bytes() > self.maxmemory:
            key_to_evict = select_key_to_evict(
                self.maxmemory_policy,
                self._dict,
                self._expires,
            )
            if key_to_evict is None:
                raise OutOfMemoryError(
                    "OOM command not allowed when used memory > 'maxmemory'."
                )
            self.delete(key_to_evict)

    # --- Keyspace Inspection ---

    def keys(self, pattern: bytes = b"*") -> List[bytes]:
        """Return list of keys matching glob pattern, ignoring expired keys."""
        pattern_str = pattern.decode("latin1")
        matching = []
        for k in list(self._dict.keys()):
            if not self._lazy_expire(k):
                if fnmatch.fnmatch(k.decode("latin1"), pattern_str):
                    matching.append(k)
        return matching

    def flush(self) -> int:
        """Clear database. Returns number of keys deleted."""
        count = len(self._dict)
        self._dict.clear()
        self._expires.clear()
        return count

    def dbsize(self) -> int:
        """Return active key count (cleaning up expired lazily during count)."""
        count = 0
        now = time.time()
        for k in list(self._dict.keys()):
            if self._is_expired(k, now):
                self.delete(k)
            else:
                count += 1
        return count
