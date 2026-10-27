"""
Unit tests for RESP parser and serializer.
"""
import unittest

from pyredis.protocol import (
    Array,
    BulkString,
    ErrorResponse,
    Integer,
    ProtocolSyntaxError,
    RESPParser,
    SimpleString,
    encode_resp,
    OK,
    NULL_BULK,
    NULL_ARRAY,
)


class TestRESPProtocol(unittest.TestCase):
    def setUp(self):
        self.parser = RESPParser()

    def test_parse_simple_string(self):
        self.parser.feed(b"+OK\r\n")
        res = self.parser.parse_one()
        self.assertIsInstance(res, SimpleString)
        self.assertEqual(res.value, "OK")

    def test_parse_error(self):
        self.parser.feed(b"-ERR unknown command 'foobar'\r\n")
        res = self.parser.parse_one()
        self.assertIsInstance(res, ErrorResponse)
        self.assertEqual(res.message, "ERR unknown command 'foobar'")

    def test_parse_integer(self):
        self.parser.feed(b":1024\r\n:-42\r\n:0\r\n")
        self.assertEqual(self.parser.parse_one(), Integer(1024))
        self.assertEqual(self.parser.parse_one(), Integer(-42))
        self.assertEqual(self.parser.parse_one(), Integer(0))

    def test_parse_bulk_string(self):
        self.parser.feed(b"$6\r\nfoobar\r\n")
        res = self.parser.parse_one()
        self.assertEqual(res, BulkString(b"foobar"))

    def test_parse_bulk_string_with_crlf(self):
        # Bulk string containing embedded \r\n
        content = b"hello\r\nworld"
        payload = f"${len(content)}\r\n".encode("ascii") + content + b"\r\n"
        self.parser.feed(payload)
        res = self.parser.parse_one()
        self.assertEqual(res, BulkString(content))

    def test_parse_null_bulk_string(self):
        self.parser.feed(b"$-1\r\n")
        res = self.parser.parse_one()
        self.assertEqual(res, NULL_BULK)

    def test_parse_array(self):
        # Array of 2 bulk strings: ["SET", "key"]
        self.parser.feed(b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n")
        res = self.parser.parse_one()
        self.assertIsInstance(res, Array)
        self.assertEqual(len(res), 2)
        self.assertEqual(res.elements[0], BulkString(b"SET"))
        self.assertEqual(res.elements[1], BulkString(b"key"))

    def test_parse_nested_array(self):
        self.parser.feed(b"*2\r\n*1\r\n:1\r\n*1\r\n:2\r\n")
        res = self.parser.parse_one()
        self.assertIsInstance(res, Array)
        self.assertEqual(len(res), 2)
        self.assertEqual(res.elements[0].elements[0], Integer(1))
        self.assertEqual(res.elements[1].elements[0], Integer(2))

    def test_chunked_streaming(self):
        # Feed one byte at a time to test incremental parsing and buffer retention
        full_msg = b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nAlice\r\n"
        for byte in full_msg[:-1]:
            self.parser.feed(bytes([byte]))
            self.assertIsNone(self.parser.parse_one())

        # Feed the final byte
        self.parser.feed(bytes([full_msg[-1]]))
        res = self.parser.parse_one()
        self.assertIsNotNone(res)
        self.assertEqual(len(res), 3)
        self.assertEqual(res.elements[2], BulkString(b"Alice"))

    def test_pipelining(self):
        # Multiple commands in one buffer
        buffer = (
            b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
            b"*1\r\n$4\r\nPING\r\n"
        )
        self.parser.feed(buffer)
        all_frames = self.parser.parse_all()
        self.assertEqual(len(all_frames), 2)
        self.assertEqual(all_frames[0].elements[0], BulkString(b"ECHO"))
        self.assertEqual(all_frames[1].elements[0], BulkString(b"PING"))

    def test_inline_command(self):
        self.parser.feed(b"PING\r\n")
        res = self.parser.parse_one()
        self.assertIsInstance(res, Array)
        self.assertEqual(res.elements[0], BulkString(b"PING"))

    def test_encode_resp_types(self):
        self.assertEqual(encode_resp(OK), b"+OK\r\n")
        self.assertEqual(encode_resp(ErrorResponse("ERR failure")), b"-ERR failure\r\n")
        self.assertEqual(encode_resp(100), b":100\r\n")
        self.assertEqual(encode_resp(True), b":1\r\n")
        self.assertEqual(encode_resp(False), b":0\r\n")
        self.assertEqual(encode_resp("hello"), b"$5\r\nhello\r\n")
        self.assertEqual(encode_resp(b"raw"), b"$3\r\nraw\r\n")
        self.assertEqual(encode_resp(None), b"$-1\r\n")
        self.assertEqual(encode_resp(["a", 1]), b"*2\r\n$1\r\na\r\n:1\r\n")


if __name__ == "__main__":
    unittest.main()
