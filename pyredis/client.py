"""
High-level Python Client for pyredis with Pipelining support.
Zero external dependencies. Works seamlessly with pyredis or official Redis servers.
"""
import socket
from typing import Any, Dict, List, Optional, Tuple, Union

from pyredis.protocol import (
    Array,
    BulkString,
    ErrorResponse,
    Integer,
    RESPParser,
    SimpleString,
    encode_resp,
)


class RedisError(Exception):
    """Raised when Redis returns an error response."""
    pass


class Pipeline:
    """Pipelines multiple commands into a single TCP write and batch reads responses."""

    def __init__(self, client: "Client"):
        self.client = client
        self.commands: List[Tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> "Pipeline":
        self.commands.append(args)
        return self

    def get(self, key: str) -> "Pipeline":
        return self.execute_command("GET", key)

    def set(self, key: str, value: Any, **kwargs) -> "Pipeline":
        cmd = ["SET", key, value]
        if kwargs.get("ex"):
            cmd.extend(["EX", kwargs["ex"]])
        if kwargs.get("px"):
            cmd.extend(["PX", kwargs["px"]])
        if kwargs.get("nx"):
            cmd.append("NX")
        if kwargs.get("xx"):
            cmd.append("XX")
        return self.execute_command(*cmd)

    def incr(self, key: str) -> "Pipeline":
        return self.execute_command("INCR", key)

    def hset(self, key: str, field: str, value: Any) -> "Pipeline":
        return self.execute_command("HSET", key, field, value)

    def lpush(self, key: str, *values: Any) -> "Pipeline":
        return self.execute_command("LPUSH", key, *values)

    def rpush(self, key: str, *values: Any) -> "Pipeline":
        return self.execute_command("RPUSH", key, *values)

    def execute(self) -> List[Any]:
        """Send all queued commands in one batch and return all results."""
        if not self.commands:
            return []

        payload = bytearray()
        for cmd in self.commands:
            # Convert args to bytes or str
            args_bytes = [
                a if isinstance(a, bytes) else str(a).encode("utf-8")
                for a in cmd
            ]
            payload.extend(encode_resp(args_bytes))

        # Send all in 1 socket write
        self.client.sock.sendall(payload)

        # Read back all responses
        results = []
        for _ in range(len(self.commands)):
            resp = self.client._read_response()
            results.append(resp)

        self.commands.clear()
        return results


class Client:
    """
    Redis client connection and command interface.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 6379, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((host, port))
        self.parser = RESPParser()

    def execute_command(self, *args: Any) -> Any:
        """Send command to server, read and parse response."""
        args_bytes = [
            a if isinstance(a, bytes) else str(a).encode("utf-8")
            for a in args
        ]
        payload = encode_resp(args_bytes)
        self.sock.sendall(payload)
        return self._read_response()

    def _read_response(self) -> Any:
        while True:
            frame = self.parser.parse_one()
            if frame is not None:
                return self._unwrap(frame)

            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("Server closed connection")
            self.parser.feed(chunk)

    def _unwrap(self, frame: Any) -> Any:
        """Convert RESP objects to standard Python primitives."""
        if isinstance(frame, SimpleString):
            return frame.value
        elif isinstance(frame, ErrorResponse):
            raise RedisError(frame.message)
        elif isinstance(frame, Integer):
            return frame.value
        elif isinstance(frame, BulkString):
            return frame.value
        elif isinstance(frame, Array):
            if frame.elements is None:
                return None
            return [self._unwrap(e) for e in frame.elements]
        return frame

    def pipeline(self) -> Pipeline:
        return Pipeline(self)

    # --- Convenience Command Methods ---

    def ping(self, message: Optional[str] = None) -> Union[str, bytes]:
        if message:
            return self.execute_command("PING", message)
        return self.execute_command("PING")

    def get(self, key: str) -> Optional[bytes]:
        return self.execute_command("GET", key)

    def set(
        self,
        key: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        cmd = ["SET", key, value]
        if ex:
            cmd.extend(["EX", ex])
        if px:
            cmd.extend(["PX", px])
        if nx:
            cmd.append("NX")
        if xx:
            cmd.append("XX")
        res = self.execute_command(*cmd)
        return res == "OK"

    def delete(self, *keys: str) -> int:
        return self.execute_command("DEL", *keys)

    def exists(self, *keys: str) -> int:
        return self.execute_command("EXISTS", *keys)

    def incr(self, key: str) -> int:
        return self.execute_command("INCR", key)

    def decr(self, key: str) -> int:
        return self.execute_command("DECR", key)

    def expire(self, key: str, seconds: int) -> int:
        return self.execute_command("EXPIRE", key, seconds)

    def ttl(self, key: str) -> int:
        return self.execute_command("TTL", key)

    def hset(self, key: str, *args: Any, **mapping: Any) -> int:
        cmd = ["HSET", key]
        cmd.extend(args)
        for f, v in mapping.items():
            cmd.extend([f, v])
        return self.execute_command(*cmd)

    def hget(self, key: str, field: str) -> Optional[bytes]:
        return self.execute_command("HGET", key, field)

    def hgetall(self, key: str) -> Dict[bytes, bytes]:
        raw = self.execute_command("HGETALL", key)
        if not raw:
            return {}
        return dict(zip(raw[::2], raw[1::2]))

    def lpush(self, key: str, *values: Any) -> int:
        return self.execute_command("LPUSH", key, *values)

    def rpush(self, key: str, *values: Any) -> int:
        return self.execute_command("RPUSH", key, *values)

    def lpop(self, key: str) -> Optional[bytes]:
        return self.execute_command("LPOP", key)

    def rpop(self, key: str) -> Optional[bytes]:
        return self.execute_command("RPOP", key)

    def lrange(self, key: str, start: int, stop: int) -> List[bytes]:
        return self.execute_command("LRANGE", key, start, stop)

    def sadd(self, key: str, *members: Any) -> int:
        return self.execute_command("SADD", key, *members)

    def smembers(self, key: str) -> List[bytes]:
        return self.execute_command("SMEMBERS", key)

    def zadd(self, key: str, mapping: Dict[Any, float]) -> int:
        cmd = ["ZADD", key]
        for member, score in mapping.items():
            cmd.extend([score, member])
        return self.execute_command(*cmd)

    def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> List[Any]:
        cmd = ["ZRANGE", key, start, stop]
        if withscores:
            cmd.append("WITHSCORES")
        return self.execute_command(*cmd)

    def zscore(self, key: str, member: Any) -> Optional[float]:
        res = self.execute_command("ZSCORE", key, member)
        return float(res) if res is not None else None

    def close(self) -> None:
        try:
            self.sock.close()
        except Exception:
            pass
