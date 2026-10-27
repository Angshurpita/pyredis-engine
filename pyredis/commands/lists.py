"""
List command handlers: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX.
"""
from collections import deque
from typing import Deque

from pyredis.protocol import NULL_BULK, BulkString, Integer
from pyredis.storage import RedisDataType, RedisObject
from .base import CommandError, WrongArgCountError, WrongTypeError, registry


@registry.register("LPUSH", is_mutating=True, min_args=2)
def cmd_lpush(ctx, key: bytes, *elements: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        dq: Deque[bytes] = deque()
        obj = RedisObject(RedisDataType.LIST, dq)
        ctx.db.set(key, obj)
    elif obj.data_type != RedisDataType.LIST:
        raise WrongTypeError()
    else:
        dq = obj.value

    # Redis LPUSH with multiple arguments pushes each from left,
    # so LPUSH mylist a b c results in [c, b, a]
    for elem in elements:
        dq.appendleft(elem)

    return Integer(len(dq))


@registry.register("RPUSH", is_mutating=True, min_args=2)
def cmd_rpush(ctx, key: bytes, *elements: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        dq: Deque[bytes] = deque()
        obj = RedisObject(RedisDataType.LIST, dq)
        ctx.db.set(key, obj)
    elif obj.data_type != RedisDataType.LIST:
        raise WrongTypeError()
    else:
        dq = obj.value

    for elem in elements:
        dq.append(elem)

    return Integer(len(dq))


@registry.register("LPOP", is_mutating=True, min_args=1, max_args=1)
def cmd_lpop(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return NULL_BULK
    if obj.data_type != RedisDataType.LIST:
        raise WrongTypeError()

    if not obj.value:
        return NULL_BULK

    val = obj.value.popleft()
    if not obj.value:
        ctx.db.delete(key)
    return BulkString(val)


@registry.register("RPOP", is_mutating=True, min_args=1, max_args=1)
def cmd_rpop(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return NULL_BULK
    if obj.data_type != RedisDataType.LIST:
        raise WrongTypeError()

    if not obj.value:
        return NULL_BULK

    val = obj.value.pop()
    if not obj.value:
        ctx.db.delete(key)
    return BulkString(val)


@registry.register("LRANGE", is_mutating=False, min_args=3, max_args=3)
def cmd_lrange(ctx, key: bytes, start_b: bytes, stop_b: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return []
    if obj.data_type != RedisDataType.LIST:
        raise WrongTypeError()

    try:
        start = int(start_b)
        stop = int(stop_b)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")

    items = list(obj.value)
    length = len(items)

    if start < 0:
        start = length + start
    if stop < 0:
        stop = length + stop

    start = max(0, start)
    stop = min(length - 1, stop)

    if start > stop or start >= length:
        return []

    return [BulkString(x) for x in items[start:stop + 1]]


@registry.register("LLEN", is_mutating=False, min_args=1, max_args=1)
def cmd_llen(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.LIST:
        raise WrongTypeError()
    return Integer(len(obj.value))


@registry.register("LINDEX", is_mutating=False, min_args=2, max_args=2)
def cmd_lindex(ctx, key: bytes, index_b: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return NULL_BULK
    if obj.data_type != RedisDataType.LIST:
        raise WrongTypeError()

    try:
        idx = int(index_b)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")

    items = list(obj.value)
    length = len(items)
    if idx < 0:
        idx = length + idx

    if 0 <= idx < length:
        return BulkString(items[idx])
    return NULL_BULK
