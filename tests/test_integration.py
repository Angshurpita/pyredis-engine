"""
End-to-end integration tests over real TCP sockets.
"""
import asyncio
import threading
import time
import unittest

from pyredis.client import Client, RedisError
from pyredis.config import ServerConfig
from pyredis.server import Server


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 16379
        cls.config = ServerConfig(port=cls.port)
        cls.server = Server(cls.config)

        # Run asyncio server in background thread
        cls.loop = asyncio.new_event_loop()
        cls.server_thread = threading.Thread(
            target=cls._run_server_loop,
            args=(cls.loop, cls.server),
            daemon=True,
        )
        cls.server_thread.start()

        # Wait for server to bind
        time.sleep(0.3)

    @classmethod
    def _run_server_loop(cls, loop, server):
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.start())
        loop.run_forever()

    @classmethod
    def tearDownClass(cls):
        future = asyncio.run_coroutine_threadsafe(cls.server.stop(), cls.loop)
        try:
            future.result(timeout=2.0)
        except Exception:
            pass
        cls.loop.call_soon_threadsafe(cls.loop.stop)
        cls.server_thread.join(timeout=2.0)

    def setUp(self):
        self.client = Client(port=self.port)

    def tearDown(self):
        self.client.close()

    def test_ping_and_strings(self):
        self.assertEqual(self.client.ping(), "PONG")
        self.assertEqual(self.client.ping("hello"), b"hello")

        self.assertTrue(self.client.set("user:100", "Alice"))
        self.assertEqual(self.client.get("user:100"), b"Alice")
        self.assertIsNone(self.client.get("nonexistent"))

        self.assertEqual(self.client.incr("counter"), 1)
        self.assertEqual(self.client.incr("counter"), 2)
        self.assertEqual(self.client.decr("counter"), 1)

    def test_hashes_and_lists(self):
        # Hashes
        self.client.hset("profile", name="Bob", role="Engineer")
        self.assertEqual(self.client.hget("profile", "name"), b"Bob")
        profile = self.client.hgetall("profile")
        self.assertEqual(profile, {b"name": b"Bob", b"role": b"Engineer"})

        # Lists
        self.client.rpush("tasks", "task1", "task2")
        self.client.lpush("tasks", "task0")
        items = self.client.lrange("tasks", 0, -1)
        self.assertEqual(items, [b"task0", b"task1", b"task2"])
        self.assertEqual(self.client.lpop("tasks"), b"task0")

    def test_sets_and_zsets(self):
        # Sets
        self.client.sadd("tags", "python", "database", "redis")
        members = set(self.client.smembers("tags"))
        self.assertEqual(members, {b"python", b"database", b"redis"})

        # Sorted Sets (ZSet)
        self.client.zadd("leaderboard", {"alice": 100.0, "bob": 250.0, "charlie": 50.0})
        self.assertEqual(self.client.zscore("leaderboard", "bob"), 250.0)
        top = self.client.zrange("leaderboard", 0, -1)
        self.assertEqual(top, [b"charlie", b"alice", b"bob"])

    def test_pipelining(self):
        pipe = self.client.pipeline()
        pipe.set("p1", "v1").set("p2", "v2").incr("p_count").get("p1")
        results = pipe.execute()

        self.assertEqual(results, ["OK", "OK", 1, b"v1"])
        self.assertEqual(self.client.get("p2"), b"v2")


if __name__ == "__main__":
    unittest.main()
