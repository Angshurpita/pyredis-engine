"""
LRU / LFU and Access Tracking structures for Cache Eviction.
"""
import time
from typing import Dict, Generic, Iterator, Optional, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class Node(Generic[K, V]):
    __slots__ = ("key", "value", "prev", "next", "access_time", "access_count")

    def __init__(self, key: K, value: V):
        self.key: K = key
        self.value: V = value
        self.prev: Optional["Node[K, V]"] = None
        self.next: Optional["Node[K, V]"] = None
        self.access_time: float = time.monotonic()
        self.access_count: int = 1


class DoublyLinkedList(Generic[K, V]):
    """Doubly linked list with dummy head and tail for O(1) operations."""

    def __init__(self):
        self.head: Node[K, V] = Node(None, None)  # Dummy head
        self.tail: Node[K, V] = Node(None, None)  # Dummy tail
        self.head.next = self.tail
        self.tail.prev = self.head
        self._size = 0

    def add_to_head(self, node: Node[K, V]) -> None:
        """Add node right after head (most recently used)."""
        node.next = self.head.next
        node.prev = self.head
        if self.head.next:
            self.head.next.prev = node
        self.head.next = node
        self._size += 1

    def remove(self, node: Node[K, V]) -> None:
        """Unlink node from list."""
        if node.prev:
            node.prev.next = node.next
        if node.next:
            node.next.prev = node.prev
        node.prev = None
        node.next = None
        self._size -= 1

    def remove_tail(self) -> Optional[Node[K, V]]:
        """Remove and return the least recently used node (before dummy tail)."""
        if self._size == 0 or self.tail.prev == self.head:
            return None
        lru_node = self.tail.prev
        if lru_node is not None:
            self.remove(lru_node)
            return lru_node
        return None

    def move_to_head(self, node: Node[K, V]) -> None:
        """Move existing node to head."""
        self.remove(node)
        node.access_time = time.monotonic()
        node.access_count += 1
        self.add_to_head(node)

    def __len__(self) -> int:
        return self._size
