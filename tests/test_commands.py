"""
Comprehensive unit tests for all Redis data structures and commands.
"""
import time
import unittest

from pyredis.protocol import (
    Array,
    BulkString,
    ErrorResponse,
    Integer,
    SimpleString,
    OK,
    PONG,
    NULL_BULK,
)
from pyredis.server import ClientConnection, ExecutionContext, Server


class TestCommands(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        # Reader/writer mock not needed for direct command tests
        self.conn = ClientConnection(None, None)  # type: ignore

    def exec(self, *parts: bytes):
        args = [p if isinstance(p, bytes) else p.encode("utf-8") for p in parts]
        return self.server.execute_single_command(self.conn, args)

    # --- String Commands ---

    def test_strings_get_set(self):
        res = self.exec(b"SET", b"foo", b"bar")
        self.assertEqual(res, OK)

        res = self.exec(b"GET", b"foo")
        self.assertEqual(res, BulkString(b"bar"))

        res = self.exec(b"GET", b"nonexistent")
        self.assertEqual(res, NULL_BULK)

    def test_set_options_nx_xx(self):
        # NX: set only if not exists
        self.assertEqual(self.exec(b"SET", b"nx_key", b"1", b"NX"), OK)
        self.assertEqual(self.exec(b"SET", b"nx_key", b"2", b"NX"), NULL_BULK)
        self.assertEqual(self.exec(b"GET", b"nx_key"), BulkString(b"1"))

        # XX: set only if exists
        self.assertEqual(self.exec(b"SET", b"xx_key", b"val", b"XX"), NULL_BULK)
        self.exec(b"SET", b"xx_key", b"initial")
        self.assertEqual(self.exec(b"SET", b"xx_key", b"updated", b"XX"), OK)
        self.assertEqual(self.exec(b"GET", b"xx_key"), BulkString(b"updated"))

    def test_mget_mset(self):
        self.assertEqual(self.exec(b"MSET", b"k1", b"v1", b"k2", b"v2"), OK)
        res = self.exec(b"MGET", b"k1", b"k2", b"missing")
        self.assertEqual(res, [BulkString(b"v1"), BulkString(b"v2"), NULL_BULK])

    def test_incr_decr(self):
        self.assertEqual(self.exec(b"INCR", b"counter"), Integer(1))
        self.assertEqual(self.exec(b"INCR", b"counter"), Integer(2))
        self.assertEqual(self.exec(b"DECR", b"counter"), Integer(1))
        self.assertEqual(self.exec(b"INCRBY", b"counter", b"10"), Integer(11))
        self.assertEqual(self.exec(b"DECRBY", b"counter", b"5"), Integer(6))

    def test_append_strlen_getset(self):
        self.assertEqual(self.exec(b"APPEND", b"msg", b"hello"), Integer(5))
        self.assertEqual(self.exec(b"APPEND", b"msg", b" world"), Integer(11))
        self.assertEqual(self.exec(b"STRLEN", b"msg"), Integer(11))
        self.assertEqual(self.exec(b"GETSET", b"msg", b"new"), BulkString(b"hello world"))
        self.assertEqual(self.exec(b"GET", b"msg"), BulkString(b"new"))

    # --- Hash Commands ---

    def test_hash_commands(self):
        # HSET & HGET
        self.assertEqual(self.exec(b"HSET", b"user:1", b"name", b"Alice", b"age", b"30"), Integer(2))
        self.assertEqual(self.exec(b"HGET", b"user:1", b"name"), BulkString(b"Alice"))
        self.assertEqual(self.exec(b"HGET", b"user:1", b"missing"), NULL_BULK)

        # HEXISTS & HLEN
        self.assertEqual(self.exec(b"HEXISTS", b"user:1", b"name"), Integer(1))
        self.assertEqual(self.exec(b"HEXISTS", b"user:1", b"email"), Integer(0))
        self.assertEqual(self.exec(b"HLEN", b"user:1"), Integer(2))

        # HGETALL
        hgetall = self.exec(b"HGETALL", b"user:1")
        self.assertEqual(len(hgetall), 4)

        # HDEL
        self.assertEqual(self.exec(b"HDEL", b"user:1", b"age"), Integer(1))
        self.assertEqual(self.exec(b"HLEN", b"user:1"), Integer(1))

    # --- List Commands ---

    def test_list_commands(self):
        # RPUSH & LPUSH
        self.assertEqual(self.exec(b"RPUSH", b"mylist", b"b", b"c"), Integer(2))
        self.assertEqual(self.exec(b"LPUSH", b"mylist", b"a"), Integer(3))

        # LRANGE
        lrange = self.exec(b"LRANGE", b"mylist", b"0", b"-1")
        self.assertEqual(lrange, [BulkString(b"a"), BulkString(b"b"), BulkString(b"c")])

        # LPOP & RPOP
        self.assertEqual(self.exec(b"LPOP", b"mylist"), BulkString(b"a"))
        self.assertEqual(self.exec(b"RPOP", b"mylist"), BulkString(b"c"))
        self.assertEqual(self.exec(b"LLEN", b"mylist"), Integer(1))

    # --- Set Commands ---

    def test_set_commands(self):
        self.assertEqual(self.exec(b"SADD", b"myset", b"apple", b"banana", b"apple"), Integer(2))
        self.assertEqual(self.exec(b"SISMEMBER", b"myset", b"apple"), Integer(1))
        self.assertEqual(self.exec(b"SISMEMBER", b"myset", b"cherry"), Integer(0))
        self.assertEqual(self.exec(b"SCARD", b"myset"), Integer(2))

        # SUNION
        self.exec(b"SADD", b"set2", b"banana", b"cherry")
        union = self.exec(b"SUNION", b"myset", b"set2")
        union_vals = {elem.value for elem in union}
        self.assertEqual(union_vals, {b"apple", b"banana", b"cherry"})

    # --- Sorted Set (ZSet) Commands ---

    def test_zset_commands(self):
        # ZADD
        self.assertEqual(self.exec(b"ZADD", b"scores", b"100", b"alice", b"200", b"bob", b"50", b"charlie"), Integer(3))

        # ZSCORE
        self.assertEqual(self.exec(b"ZSCORE", b"scores", b"bob"), BulkString(b"200"))

        # ZRANK & ZREVRANK
        self.assertEqual(self.exec(b"ZRANK", b"scores", b"charlie"), Integer(0))
        self.assertEqual(self.exec(b"ZRANK", b"scores", b"bob"), Integer(2))
        self.assertEqual(self.exec(b"ZREVRANK", b"scores", b"bob"), Integer(0))

        # ZRANGE with WITHSCORES
        res = self.exec(b"ZRANGE", b"scores", b"0", b"1", b"WITHSCORES")
        self.assertEqual(res, [BulkString(b"charlie"), BulkString(b"50"), BulkString(b"alice"), BulkString(b"100")])

        # ZCARD & ZREM
        self.assertEqual(self.exec(b"ZCARD", b"scores"), Integer(3))
        self.assertEqual(self.exec(b"ZREM", b"scores", b"charlie"), Integer(1))
        self.assertEqual(self.exec(b"ZCARD", b"scores"), Integer(2))

    # --- Keyspace & TTL Commands ---

    def test_keyspace_and_ttl(self):
        self.exec(b"SET", b"test_key", b"test_val")
        self.assertEqual(self.exec(b"EXISTS", b"test_key"), Integer(1))
        self.assertEqual(self.exec(b"TYPE", b"test_key"), SimpleString("string"))

        # EXPIRE and TTL
        self.assertEqual(self.exec(b"EXPIRE", b"test_key", b"60"), Integer(1))
        ttl = self.exec(b"TTL", b"test_key")
        self.assertGreater(ttl.value, 0)

        # PERSIST
        self.assertEqual(self.exec(b"PERSIST", b"test_key"), Integer(1))
        self.assertEqual(self.exec(b"TTL", b"test_key"), Integer(-1))

        # DEL
        self.assertEqual(self.exec(b"DEL", b"test_key"), Integer(1))
        self.assertEqual(self.exec(b"EXISTS", b"test_key"), Integer(0))

    # --- Server Commands ---

    def test_server_metadata(self):
        self.assertEqual(self.exec(b"PING"), PONG)
        self.assertEqual(self.exec(b"PING", b"hello"), BulkString(b"hello"))
        self.assertEqual(self.exec(b"ECHO", b"hello"), BulkString(b"hello"))
        info = self.exec(b"INFO")
        self.assertIn(b"redis_version:7.0.0-pyredis", info.value)


if __name__ == "__main__":
    unittest.main()
