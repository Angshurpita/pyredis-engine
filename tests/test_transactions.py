"""
Unit tests for atomic transactions (MULTI, EXEC, DISCARD).
"""
import unittest

from pyredis.protocol import BulkString, Integer, OK, QUEUED
from pyredis.server import ClientConnection, Server


class TestTransactions(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.conn = ClientConnection(None, None)  # type: ignore

    def test_multi_exec_transaction(self):
        # 1. Start MULTI
        self.assertEqual(
            self.server.execute_single_command(self.conn, [b"MULTI"]),
            OK,
        )
        self.assertTrue(self.conn.in_transaction)

        # 2. Queue commands
        self.server._process_frame(self.conn, BulkString(b"SET a 10"))
        self.server._process_frame(self.conn, BulkString(b"INCR a"))
        self.server._process_frame(self.conn, BulkString(b"GET a"))

        self.assertEqual(len(self.conn.transaction_queue), 3)

        # 3. EXEC
        results = self.server.execute_single_command(self.conn, [b"EXEC"])
        self.assertFalse(self.conn.in_transaction)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], OK)
        self.assertEqual(results[1], Integer(11))
        self.assertEqual(results[2], BulkString(b"11"))

    def test_multi_discard(self):
        self.server.execute_single_command(self.conn, [b"MULTI"])
        self.server._process_frame(self.conn, BulkString(b"SET foo bar"))
        self.assertEqual(
            self.server.execute_single_command(self.conn, [b"DISCARD"]),
            OK,
        )
        self.assertFalse(self.conn.in_transaction)
        # Check that key was NOT set
        self.assertFalse(self.server.databases[0].exists(b"foo"))


if __name__ == "__main__":
    unittest.main()
