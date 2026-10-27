"""
Unit tests for storage engine, SkipList, TTL expiration, and cache eviction.
"""
import time
import unittest

from pyredis.datastructures.skiplist import SkipList
from pyredis.storage import (
    Database,
    EvictionPolicy,
    OutOfMemoryError,
    RedisDataType,
    RedisObject,
    RedisZSet,
)


class TestStorageAndDataStructures(unittest.TestCase):
    def test_skiplist_operations(self):
        sl = SkipList()
        self.assertEqual(len(sl), 0)

        # Insert elements with scores
        sl.insert(10.0, b"alice")
        sl.insert(5.0, b"bob")
        sl.insert(20.0, b"charlie")
        sl.insert(15.0, b"dave")
        self.assertEqual(len(sl), 4)

        # Check ranking (1-based in SkipList)
        # bob (5) -> alice (10) -> dave (15) -> charlie (20)
        self.assertEqual(sl.get_rank(5.0, b"bob"), 1)
        self.assertEqual(sl.get_rank(10.0, b"alice"), 2)
        self.assertEqual(sl.get_rank(15.0, b"dave"), 3)
        self.assertEqual(sl.get_rank(20.0, b"charlie"), 4)

        # Range by rank
        res = sl.range_by_rank(0, 1)
        self.assertEqual(res, [(b"bob", 5.0), (b"alice", 10.0)])

        # Reverse range
        rev_res = sl.range_by_rank(0, 1, reverse=True)
        self.assertEqual(rev_res, [(b"charlie", 20.0), (b"dave", 15.0)])

        # Delete
        self.assertTrue(sl.delete(10.0, b"alice"))
        self.assertEqual(len(sl), 3)
        self.assertEqual(sl.get_rank(10.0, b"alice"), 0)

    def test_redis_zset_wrapper(self):
        zset = RedisZSet()
        self.assertTrue(zset.add(100.0, b"player1"))
        self.assertTrue(zset.add(200.0, b"player2"))
        # Update existing
        self.assertFalse(zset.add(50.0, b"player1"))

        self.assertEqual(zset.score(b"player1"), 50.0)
        self.assertEqual(zset.rank(b"player1"), 0)
        self.assertEqual(zset.rank(b"player2"), 1)

        members = zset.range_by_rank(0, -1)
        self.assertEqual(members, [(b"player1", 50.0), (b"player2", 200.0)])

    def test_database_get_set_delete(self):
        db = Database()
        obj = RedisObject(RedisDataType.STRING, b"hello")
        db.set(b"foo", obj)

        self.assertTrue(db.exists(b"foo"))
        res = db.get(b"foo")
        self.assertIsNotNone(res)
        self.assertEqual(res.value, b"hello")

        self.assertTrue(db.delete(b"foo"))
        self.assertFalse(db.exists(b"foo"))
        self.assertIsNone(db.get(b"foo"))

    def test_lazy_expiration(self):
        db = Database()
        obj = RedisObject(RedisDataType.STRING, b"temp")
        # Expire in 0.05 seconds
        db.set(b"temp_key", obj, expire_at=time.time() + 0.05)

        self.assertTrue(db.exists(b"temp_key"))
        time.sleep(0.06)

        # Lazy check should evict and return False / None
        self.assertFalse(db.exists(b"temp_key"))
        self.assertIsNone(db.get(b"temp_key"))

    def test_active_expire_cycle(self):
        db = Database()
        now = time.time()
        for i in range(10):
            db.set(
                f"k{i}".encode("ascii"),
                RedisObject(RedisDataType.STRING, b"val"),
                expire_at=now - 1.0,  # Already expired
            )
        self.assertEqual(len(db._expires), 10)
        evicted = db.active_expire_cycle(sample_size=10)
        self.assertEqual(evicted, 10)
        self.assertEqual(len(db._expires), 0)

    def test_memory_eviction_lru(self):
        # Set maxmemory small enough to only hold ~2 keys
        db = Database(maxmemory=200, maxmemory_policy=EvictionPolicy.ALLKEYS_LRU)
        db.set(b"k1", RedisObject(RedisDataType.STRING, b"val1"))
        db.set(b"k2", RedisObject(RedisDataType.STRING, b"val2"))

        # Access k1 so k2 becomes the least recently used
        db.get(b"k1")

        # Adding k3 should trigger eviction of k2
        db.set(b"k3", RedisObject(RedisDataType.STRING, b"val3"))

        self.assertTrue(db.exists(b"k1"))
        self.assertTrue(db.exists(b"k3"))
        self.assertFalse(db.exists(b"k2"))

    def test_memory_eviction_oom(self):
        # Noeviction should raise OutOfMemoryError when full
        db = Database(maxmemory=200, maxmemory_policy=EvictionPolicy.NOEVICTION)
        db.set(b"k1", RedisObject(RedisDataType.STRING, b"a" * 100))
        with self.assertRaises(OutOfMemoryError):
            db.set(b"k2", RedisObject(RedisDataType.STRING, b"b" * 100))


if __name__ == "__main__":
    unittest.main()
