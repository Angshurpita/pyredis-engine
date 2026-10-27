"""
In-memory Redis object types and wrapper data structures.
"""
from collections import deque
from dataclasses import dataclass, field
import time
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from pyredis.datastructures.skiplist import SkipList


class RedisDataType:
    STRING = "string"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    HASH = "hash"


@dataclass
class RedisObject:
    """Wrapper holding value, data type, and access metadata for LRU/LFU."""
    data_type: str
    value: Any
    created_at: float = field(default_factory=time.time)
    last_accessed_at: float = field(default_factory=time.time)
    access_count: int = 1

    def touch(self) -> None:
        self.last_accessed_at = time.time()
        self.access_count += 1


class RedisZSet:
    """
    Redis Sorted Set (ZSET).
    Maintains:
    - dict: member -> float score (O(1) lookup and update)
    - SkipList: ordered by score, tie-broken by member bytes (O(log N) rank and range scans)
    """

    def __init__(self):
        self._dict: Dict[bytes, float] = {}
        self._sl = SkipList()

    def add(self, score: float, member: bytes) -> bool:
        """
        Add or update member with score.
        Returns True if a new element was added, False if an existing element was updated.
        """
        if member in self._dict:
            old_score = self._dict[member]
            if old_score == score:
                return False  # No change
            self._sl.delete(old_score, member)
            self._dict[member] = score
            self._sl.insert(score, member)
            return False

        self._dict[member] = score
        self._sl.insert(score, member)
        return True

    def remove(self, member: bytes) -> bool:
        """Remove member from sorted set."""
        if member not in self._dict:
            return False
        score = self._dict.pop(member)
        self._sl.delete(score, member)
        return True

    def score(self, member: bytes) -> Optional[float]:
        """Return score of member or None."""
        return self._dict.get(member)

    def rank(self, member: bytes, reverse: bool = False) -> Optional[int]:
        """Return 0-based rank of member or None."""
        if member not in self._dict:
            return None
        score = self._dict[member]
        # get_rank is 1-based in SkipList
        r = self._sl.get_rank(score, member)
        if r == 0:
            return None
        if not reverse:
            return r - 1
        return len(self) - r

    def range_by_rank(
        self, start: int, stop: int, reverse: bool = False
    ) -> List[Tuple[bytes, float]]:
        """Return list of (member, score) by rank."""
        return self._sl.range_by_rank(start, stop, reverse=reverse)

    def __len__(self) -> int:
        return len(self._dict)

    def __contains__(self, member: bytes) -> bool:
        return member in self._dict
