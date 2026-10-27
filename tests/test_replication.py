"""
Unit tests for Master-Replica Replication and Failover.
"""
import unittest

from pyredis.protocol import BulkString, ErrorResponse, OK, SimpleString
from pyredis.server import ClientConnection, Server


class MockReplicaConnection(ClientConnection):
    def __init__(self):
        super().__init__(None, None)  # type: ignore
        self.sent_bytes = bytearray()

    def send_raw(self, data: bytes):
        self.sent_bytes.extend(data)


class TestReplication(unittest.TestCase):
    def setUp(self):
        self.master = Server()
        self.replica = Server()
        self.client_conn = ClientConnection(None, None)  # type: ignore

    def test_replica_read_only_protection(self):
        # Configure node as replica
        self.replica.replication_manager.role = "slave"

        # Reads should succeed
        res_read = self.replica.execute_single_command(self.client_conn, [b"GET", b"key"])
        self.assertNotIsInstance(res_read, ErrorResponse)

        # Writes must be rejected with READONLY error
        res_write = self.replica.execute_single_command(self.client_conn, [b"SET", b"key", b"val"])
        self.assertIsInstance(res_write, ErrorResponse)
        self.assertIn("READONLY", res_write.message)

    def test_promotion_to_master(self):
        self.replica.replication_manager.role = "slave"
        # REPLICAOF NO ONE
        res = self.replica.execute_single_command(self.client_conn, [b"REPLICAOF", b"NO", b"ONE"])
        self.assertEqual(res, OK)
        self.assertEqual(self.replica.replication_manager.role, "master")

        # Now writes should be allowed
        res_write = self.replica.execute_single_command(self.client_conn, [b"SET", b"key", b"val"])
        self.assertEqual(res_write, OK)

    def test_master_command_propagation(self):
        rep_conn = MockReplicaConnection()
        self.master.replication_manager.register_replica_connection(rep_conn)

        # Clear initial snapshot buffer
        rep_conn.sent_bytes.clear()

        # Execute mutating command on master
        self.master.execute_single_command(self.client_conn, [b"SET", b"city", b"Tokyo"])

        # Check that replica connection received the command in RESP stream
        self.assertIn(b"SET", bytes(rep_conn.sent_bytes))
        self.assertIn(b"city", bytes(rep_conn.sent_bytes))
        self.assertIn(b"Tokyo", bytes(rep_conn.sent_bytes))


if __name__ == "__main__":
    unittest.main()
