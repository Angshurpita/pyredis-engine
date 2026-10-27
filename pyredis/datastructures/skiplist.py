"""
SkipList implementation for Redis Sorted Sets (ZSet).
Provides O(log N) average-time insertion, deletion, lookup, and range queries.
Matches Redis zset design (SkipList ordered by score, tie-broken by member bytes).
"""
import random
from typing import List, Optional, Tuple


class SkipListNode:
    __slots__ = ("score", "member", "forward", "backward", "span")

    def __init__(self, level: int, score: float = 0.0, member: bytes = b""):
        self.score = score
        self.member = member
        self.forward: List[Optional["SkipListNode"]] = [None] * level
        self.span: List[int] = [0] * level
        self.backward: Optional["SkipListNode"] = None


class SkipList:
    MAX_LEVEL = 16
    P = 0.25  # Redis uses 0.25 probability for level promotion

    def __init__(self):
        self.level = 1
        self.length = 0
        self.header = SkipListNode(self.MAX_LEVEL, score=-float("inf"), member=b"")
        self.tail: Optional[SkipListNode] = None

    def _random_level(self) -> int:
        lvl = 1
        while random.random() < self.P and lvl < self.MAX_LEVEL:
            lvl += 1
        return lvl

    def insert(self, score: float, member: bytes) -> SkipListNode:
        """Insert a (score, member) pair into the skiplist."""
        update: List[Optional[SkipListNode]] = [None] * self.MAX_LEVEL
        rank: List[int] = [0] * self.MAX_LEVEL
        curr = self.header

        for i in range(self.level - 1, -1, -1):
            rank[i] = 0 if i == (self.level - 1) else rank[i + 1]
            while curr.forward[i] is not None and (
                curr.forward[i].score < score
                or (curr.forward[i].score == score and curr.forward[i].member < member)
            ):
                rank[i] += curr.span[i]
                curr = curr.forward[i]
            update[i] = curr

        level = self._random_level()
        if level > self.level:
            for i in range(self.level, level):
                rank[i] = 0
                update[i] = self.header
                update[i].span[i] = self.length
            self.level = level

        node = SkipListNode(level, score, member)
        for i in range(level):
            node.forward[i] = update[i].forward[i]
            update[i].forward[i] = node

            # Update span
            node.span[i] = update[i].span[i] - (rank[0] - rank[i])
            update[i].span[i] = (rank[0] - rank[i]) + 1

        for i in range(level, self.level):
            update[i].span[i] += 1

        node.backward = None if update[0] == self.header else update[0]
        if node.forward[0] is not None:
            node.forward[0].backward = node
        else:
            self.tail = node

        self.length += 1
        return node

    def delete(self, score: float, member: bytes) -> bool:
        """Delete a (score, member) pair from the skiplist."""
        update: List[Optional[SkipListNode]] = [None] * self.MAX_LEVEL
        curr = self.header

        for i in range(self.level - 1, -1, -1):
            while curr.forward[i] is not None and (
                curr.forward[i].score < score
                or (curr.forward[i].score == score and curr.forward[i].member < member)
            ):
                curr = curr.forward[i]
            update[i] = curr

        curr = curr.forward[0]
        if curr is not None and curr.score == score and curr.member == member:
            self._delete_node(curr, update)
            return True
        return False

    def _delete_node(self, node: SkipListNode, update: List[Optional[SkipListNode]]) -> None:
        for i in range(self.level):
            if update[i].forward[i] == node:
                update[i].span[i] += node.span[i] - 1
                update[i].forward[i] = node.forward[i]
            else:
                update[i].span[i] -= 1

        if node.forward[0] is not None:
            node.forward[0].backward = node.backward
        else:
            self.tail = node.backward

        while self.level > 1 and self.header.forward[self.level - 1] is None:
            self.level -= 1

        self.length -= 1

    def get_rank(self, score: float, member: bytes) -> int:
        """Return 1-based rank of the member, or 0 if not found."""
        rank = 0
        curr = self.header
        for i in range(self.level - 1, -1, -1):
            while curr.forward[i] is not None and (
                curr.forward[i].score < score
                or (curr.forward[i].score == score and curr.forward[i].member <= member)
            ):
                rank += curr.span[i]
                curr = curr.forward[i]

        if curr != self.header and curr.member == member:
            return rank
        return 0

    def get_node_by_rank(self, rank: int) -> Optional[SkipListNode]:
        """Return node at 1-based rank."""
        if rank < 1 or rank > self.length:
            return None
        traversed = 0
        curr = self.header
        for i in range(self.level - 1, -1, -1):
            while curr.forward[i] is not None and (traversed + curr.span[i]) <= rank:
                traversed += curr.span[i]
                curr = curr.forward[i]
            if traversed == rank:
                return curr
        return None

    def range_by_rank(
        self, start: int, stop: int, reverse: bool = False
    ) -> List[Tuple[bytes, float]]:
        """
        Return list of (member, score) between start and stop (0-indexed, negative supported).
        """
        if self.length == 0:
            return []

        # Convert negative indices to positive 0-based
        if start < 0:
            start = self.length + start
        if stop < 0:
            stop = self.length + stop

        start = max(0, start)
        stop = min(self.length - 1, stop)

        if start > stop or start >= self.length:
            return []

        results: List[Tuple[bytes, float]] = []

        if not reverse:
            # 1-based rank for get_node_by_rank
            node = self.get_node_by_rank(start + 1)
            count = stop - start + 1
            while node is not None and count > 0:
                results.append((node.member, node.score))
                node = node.forward[0]
                count -= 1
        else:
            # Reverse: rank from high to low
            # The highest rank is length - start
            node = self.get_node_by_rank(self.length - start)
            count = stop - start + 1
            while node is not None and count > 0:
                results.append((node.member, node.score))
                node = node.backward
                count -= 1

        return results

    def __len__(self) -> int:
        return self.length
