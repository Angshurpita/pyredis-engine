"""
RESP (REdis Serialization Protocol) data types and exceptions.
Supports RESP2 standard types.
"""
from dataclasses import dataclass
from typing import List, Optional, Union, Any


class RESPError(Exception):
    """Base exception for RESP protocol errors."""
    pass


class IncompleteBufferError(RESPError):
    """Raised when the buffer doesn't contain a complete RESP message yet."""
    pass


class ProtocolSyntaxError(RESPError):
    """Raised when incoming bytes violate the RESP protocol."""
    pass


@dataclass(frozen=True)
class SimpleString:
    value: str

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ErrorResponse:
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class BulkString:
    value: Optional[bytes]

    @property
    def is_null(self) -> bool:
        return self.value is None

    def as_str(self, encoding: str = "utf-8") -> Optional[str]:
        if self.value is None:
            return None
        return self.value.decode(encoding, errors="replace")


@dataclass(frozen=True)
class Integer:
    value: int

    def __int__(self) -> int:
        return self.value


@dataclass(frozen=True)
class Array:
    elements: Optional[List[Any]]

    @property
    def is_null(self) -> bool:
        return self.elements is None

    def __len__(self) -> int:
        return 0 if self.elements is None else len(self.elements)

    def __iter__(self):
        return iter(self.elements or [])


# Convenient singletons
OK = SimpleString("OK")
PONG = SimpleString("PONG")
QUEUED = SimpleString("QUEUED")
NULL_BULK = BulkString(None)
NULL_ARRAY = Array(None)
