"""
Cache eviction strategies for maxmemory limits.
"""
from enum import Enum
import random
import time
from typing import Dict, List, Optional, Set

from .types import RedisObject


class EvictionPolicy(str, Enum):
    NOEVICTION = "noeviction"
    ALLKEYS_LRU = "allkeys-lru"
    VOLATILE_LRU = "volatile-lru"
    ALLKEYS_LFU = "allkeys-lfu"
    VOLATILE_LFU = "volatile-lfu"
    ALLKEYS_RANDOM = "allkeys-random"
    VOLATILE_RANDOM = "volatile-random"
    VOLATILE_TTL = "volatile-ttl"


def select_key_to_evict(
    policy: EvictionPolicy,
    keyspace: Dict[bytes, RedisObject],
    expires: Dict[bytes, float],
    sample_size: int = 5,
) -> Optional[bytes]:
    """
    Select a key to evict based on the configured policy.
    Uses Redis's probabilistic sampling approach (sampling N keys and picking best candidate).
    This keeps O(1) memory and CPU overhead without maintaining massive global sorted indexes.
    """
    if policy == EvictionPolicy.NOEVICTION:
        return None

    is_volatile = policy.value.startswith("volatile-")
    candidate_keys = list(expires.keys()) if is_volatile else list(keyspace.keys())

    if not candidate_keys:
        return None

    # Sample candidates
    sample = random.sample(candidate_keys, min(sample_size, len(candidate_keys)))

    if policy in (EvictionPolicy.ALLKEYS_RANDOM, EvictionPolicy.VOLATILE_RANDOM):
        return sample[0]

    if policy in (EvictionPolicy.ALLKEYS_LRU, EvictionPolicy.VOLATILE_LRU):
        # Pick key with oldest last_accessed_at
        return min(sample, key=lambda k: keyspace[k].last_accessed_at if k in keyspace else float("inf"))

    if policy in (EvictionPolicy.ALLKEYS_LFU, EvictionPolicy.VOLATILE_LFU):
        # Pick key with smallest access_count
        return min(sample, key=lambda k: keyspace[k].access_count if k in keyspace else float("inf"))

    if policy == EvictionPolicy.VOLATILE_TTL:
        # Pick key with smallest expiry timestamp (expiring soonest)
        now = time.time()
        return min(sample, key=lambda k: expires.get(k, float("inf")))

    return None
