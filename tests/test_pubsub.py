"""
Unit tests for Pub/Sub messaging across multiple client connections.
"""
import unittest

from pyredis.protocol import BulkString, Integer
from pyredis.server import ClientConnection, Server


class MockClientConnection(ClientConnection):
    def __init__(self):
        super().__init__(None, None)  # type: ignore
        self.received_frames = []

    def send_frame(self, frame):
        self.received_frames.append(frame)


class TestPubSub(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.client1 = MockClientConnection()
        self.client2 = MockClientConnection()
        self.client3 = MockClientConnection()
        self.server.clients.update([self.client1, self.client2, self.client3])

    def test_pubsub_flow(self):
        # Client 1 subscribes to "news"
        res1 = self.server.execute_single_command(self.client1, [b"SUBSCRIBE", b"news"])
        # In SUBSCRIBE, cmd_subscribe returns list of subscription frames
        self.assertEqual(len(res1), 1)
        self.assertEqual(res1[0][0], BulkString(b"subscribe"))
        self.assertEqual(res1[0][1], BulkString(b"news"))
        self.assertEqual(res1[0][2], Integer(1))

        # Client 2 subscribes to "news" and "sports"
        res2 = self.server.execute_single_command(self.client2, [b"SUBSCRIBE", b"news", b"sports"])
        self.assertEqual(len(res2), 2)

        # Client 3 publishes to "news"
        delivered = self.server.execute_single_command(self.client3, [b"PUBLISH", b"news", b"breaking"])
        self.assertEqual(delivered, Integer(2))

        # Verify client1 received message
        self.assertEqual(len(self.client1.received_frames), 1)
        self.assertEqual(
            self.client1.received_frames[0],
            [BulkString(b"message"), BulkString(b"news"), BulkString(b"breaking")],
        )

        # Verify client2 received message
        self.assertEqual(len(self.client2.received_frames), 1)

        # Client 3 publishes to "sports" (only client2 subscribed)
        delivered_sports = self.server.execute_single_command(self.client3, [b"PUBLISH", b"sports", b"goal"])
        self.assertEqual(delivered_sports, Integer(1))
        self.assertEqual(len(self.client1.received_frames), 1)  # Client 1 still only 1
        self.assertEqual(len(self.client2.received_frames), 2)  # Client 2 now has 2

        # Client 1 unsubscribes
        res_unsub = self.server.execute_single_command(self.client1, [b"UNSUBSCRIBE", b"news"])
        self.assertEqual(res_unsub[0][0], BulkString(b"unsubscribe"))
        self.assertFalse(self.client1.is_pubsub)


if __name__ == "__main__":
    unittest.main()
