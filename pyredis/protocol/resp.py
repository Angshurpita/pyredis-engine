"""
Stream-oriented RESP (REdis Serialization Protocol) Parser & Serializer.

Handles:
- Incremental parsing from TCP streams (handles fragmented buffers)
- Pipelined requests (multiple commands in a single buffer)
- Full RESP2 specification (+, -, :, $, *)
- Inline commands (fallback for plain telnet / netcat clients)
"""
from io import BytesIO
from typing import Any, List, Optional, Tuple, Union

from .types import (
    Array,
    BulkString,
    ErrorResponse,
    IncompleteBufferError,
    Integer,
    ProtocolSyntaxError,
    SimpleString,
    NULL_BULK,
    NULL_ARRAY,
)

CRLF = b"\r\n"
CRLF_LEN = 2


class RESPParser:
    """
    Incremental stream parser for RESP protocol.
    Maintains an internal bytearray buffer and extracts complete RESP frames.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> None:
        """Append incoming TCP bytes into the internal buffer."""
        self._buffer.extend(data)

    def parse_one(self) -> Optional[Any]:
        """
        Attempt to parse a single complete RESP frame from the buffer.
        Returns the parsed object, or None if the buffer is incomplete.
        Raises ProtocolSyntaxError if invalid protocol syntax is detected.
        """
        if not self._buffer:
            return None

        # Try parsing from index 0
        try:
            val, consumed = self._parse_frame(0)
            del self._buffer[:consumed]
            return val
        except IncompleteBufferError:
            return None

    def parse_all(self) -> List[Any]:
        """Parse all complete frames available in the buffer (for pipelining)."""
        frames: List[Any] = []
        while True:
            frame = self.parse_one()
            if frame is None:
                break
            frames.append(frame)
        return frames

    def _parse_frame(self, offset: int) -> Tuple[Any, int]:
        """
        Parse a frame starting at `offset`.
        Returns (parsed_value, total_bytes_consumed).
        Raises IncompleteBufferError or ProtocolSyntaxError.
        """
        if offset >= len(self._buffer):
            raise IncompleteBufferError()

        prefix = chr(self._buffer[offset])

        if prefix == "+":
            return self._parse_simple_string(offset)
        elif prefix == "-":
            return self._parse_error(offset)
        elif prefix == ":":
            return self._parse_integer(offset)
        elif prefix == "$":
            return self._parse_bulk_string(offset)
        elif prefix == "*":
            return self._parse_array(offset)
        else:
            # Inline command fallback (e.g. "PING\r\n" or "SET a b\r\n" without RESP prefix)
            return self._parse_inline(offset)

    def _read_line(self, offset: int) -> Tuple[bytes, int]:
        """Find CRLF starting at or after offset. Returns (line_bytes_without_crlf, total_consumed)."""
        idx = self._buffer.find(CRLF, offset)
        if idx == -1:
            raise IncompleteBufferError()
        line = bytes(self._buffer[offset:idx])
        return line, (idx - offset) + CRLF_LEN

    def _parse_simple_string(self, offset: int) -> Tuple[SimpleString, int]:
        line, consumed = self._read_line(offset + 1)
        try:
            text = line.decode("utf-8")
        except UnicodeDecodeError:
            text = line.decode("latin1")
        return SimpleString(text), consumed + 1

    def _parse_error(self, offset: int) -> Tuple[ErrorResponse, int]:
        line, consumed = self._read_line(offset + 1)
        try:
            msg = line.decode("utf-8")
        except UnicodeDecodeError:
            msg = line.decode("latin1")
        return ErrorResponse(msg), consumed + 1

    def _parse_integer(self, offset: int) -> Tuple[Integer, int]:
        line, consumed = self._read_line(offset + 1)
        try:
            val = int(line)
        except ValueError:
            raise ProtocolSyntaxError(f"Invalid integer: {line!r}")
        return Integer(val), consumed + 1

    def _parse_bulk_string(self, offset: int) -> Tuple[BulkString, int]:
        line, len_consumed = self._read_line(offset + 1)
        try:
            length = int(line)
        except ValueError:
            raise ProtocolSyntaxError(f"Invalid bulk string length: {line!r}")

        if length == -1:
            return NULL_BULK, 1 + len_consumed

        start = offset + 1 + len_consumed
        end = start + length

        # Check if entire payload + CRLF is in buffer
        if end + CRLF_LEN > len(self._buffer):
            raise IncompleteBufferError()

        if self._buffer[end:end + CRLF_LEN] != CRLF:
            raise ProtocolSyntaxError("Bulk string does not terminate with CRLF")

        data = bytes(self._buffer[start:end])
        total_consumed = 1 + len_consumed + length + CRLF_LEN
        return BulkString(data), total_consumed

    def _parse_array(self, offset: int) -> Tuple[Array, int]:
        line, len_consumed = self._read_line(offset + 1)
        try:
            count = int(line)
        except ValueError:
            raise ProtocolSyntaxError(f"Invalid array length: {line!r}")

        if count == -1:
            return NULL_ARRAY, 1 + len_consumed

        current_offset = offset + 1 + len_consumed
        elements: List[Any] = []
        for _ in range(count):
            item, consumed = self._parse_frame(current_offset)
            elements.append(item)
            current_offset += consumed

        return Array(elements), current_offset - offset

    def _parse_inline(self, offset: int) -> Tuple[Array, int]:
        """Support for inline commands like 'PING\r\n' or 'SET foo bar\r\n'"""
        line, consumed = self._read_line(offset)
        try:
            text = line.decode("utf-8").strip()
        except UnicodeDecodeError:
            text = line.decode("latin1").strip()

        if not text:
            return Array([]), consumed

        parts = text.split()
        elements = [BulkString(p.encode("utf-8")) for p in parts]
        return Array(elements), consumed


def encode_resp(data: Any) -> bytes:
    """
    Serialize native Python objects or RESP types into RESP byte sequence.
    """
    buf = BytesIO()
    _encode_into(buf, data)
    return buf.getvalue()


def _encode_into(buf: BytesIO, data: Any) -> None:
    if data is None:
        buf.write(b"$-1\r\n")
    elif isinstance(data, SimpleString):
        buf.write(f"+{data.value}\r\n".encode("utf-8"))
    elif isinstance(data, ErrorResponse):
        buf.write(f"-{data.message}\r\n".encode("utf-8"))
    elif isinstance(data, BulkString):
        if data.value is None:
            buf.write(b"$-1\r\n")
        else:
            buf.write(f"${len(data.value)}\r\n".encode("ascii"))
            buf.write(data.value)
            buf.write(CRLF)
    elif isinstance(data, Integer):
        buf.write(f":{data.value}\r\n".encode("ascii"))
    elif isinstance(data, Array):
        if data.elements is None:
            buf.write(b"*-1\r\n")
        else:
            buf.write(f"*{len(data.elements)}\r\n".encode("ascii"))
            for elem in data.elements:
                _encode_into(buf, elem)
    elif isinstance(data, bool):
        # Redis boolean in integer format :1 or :0 (RESP2)
        buf.write(b":1\r\n" if data else b":0\r\n")
    elif isinstance(data, int):
        buf.write(f":{data}\r\n".encode("ascii"))
    elif isinstance(data, float):
        # Floats are typically returned as bulk strings in Redis (e.g. ZSCORE)
        s = f"{data:g}".encode("ascii")
        buf.write(f"${len(s)}\r\n".encode("ascii") + s + CRLF)
    elif isinstance(data, bytes):
        buf.write(f"${len(data)}\r\n".encode("ascii"))
        buf.write(data)
        buf.write(CRLF)
    elif isinstance(data, str):
        encoded = data.encode("utf-8")
        buf.write(f"${len(encoded)}\r\n".encode("ascii"))
        buf.write(encoded)
        buf.write(CRLF)
    elif isinstance(data, (list, tuple)):
        buf.write(f"*{len(data)}\r\n".encode("ascii"))
        for item in data:
            _encode_into(buf, item)
    elif isinstance(data, set):
        buf.write(f"*{len(data)}\r\n".encode("ascii"))
        for item in sorted(data, key=lambda x: str(x)):
            _encode_into(buf, item)
    elif isinstance(data, dict):
        # Flatten dictionary to alternating key-value array in RESP2
        buf.write(f"*{len(data) * 2}\r\n".encode("ascii"))
        for k, v in data.items():
            _encode_into(buf, k)
            _encode_into(buf, v)
    elif isinstance(data, Exception):
        msg = str(data) or data.__class__.__name__
        if not msg.startswith("ERR "):
            msg = f"ERR {msg}"
        buf.write(f"-{msg}\r\n".encode("utf-8"))
    else:
        # Fallback to string bulk representation
        encoded = str(data).encode("utf-8")
        buf.write(f"${len(encoded)}\r\n".encode("ascii"))
        buf.write(encoded)
        buf.write(CRLF)
