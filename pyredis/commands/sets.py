"""
Set command handlers: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER.
"""
from typing import Set

from pyredis.protocol import BulkString, Integer
from pyredis.storage import RedisDataType, RedisObject
from .base import CommandError, WrongArgCountError, WrongTypeError, registry


@registry.register("SADD", is_mutating=True, min_args=2)
def cmd_sadd(ctx, key: bytes, *members: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        s: Set[bytes] = set()
        obj = RedisObject(RedisDataType.SET, s)
        ctx.db.set(key, obj)
    elif obj.data_type != RedisDataType.SET:
        raise WrongTypeError()
    else:
        s = obj.value

    added = 0
    for m in members:
        if m not in s:
            s.add(m)
            added += 1

    return Integer(added)


@registry.register("SREM", is_mutating=True, min_args=2)
def cmd_srem(ctx, key: bytes, *members: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.SET:
        raise WrongTypeError()

    removed = 0
    for m in members:
        if m in obj.value:
            obj.value.remove(m)
            removed += 1

    if not obj.value:
        ctx.db.delete(key)

    return Integer(removed)


@registry.register("SMEMBERS", is_mutating=False, min_args=1, max_args=1)
def cmd_smembers(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return []
    if obj.data_type != RedisDataType.SET:
        raise WrongTypeError()
    return [BulkString(m) for m in obj.value]


@registry.register("SISMEMBER", is_mutating=False, min_args=2, max_args=2)
def cmd_sismember(ctx, key: bytes, member: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.SET:
        raise WrongTypeError()
    return Integer(1 if member in obj.value else 0)


@registry.register("SCARD", is_mutating=False, min_args=1, max_args=1)
def cmd_scard(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.SET:
        raise WrongTypeError()
    return Integer(len(obj.value))


@registry.register("SUNION", is_mutating=False, min_args=1)
def cmd_sunion(ctx, *keys: bytes):
    result: Set[bytes] = set()
    for k in keys:
        obj = ctx.db.get(k)
        if obj is not None:
            if obj.data_type != RedisDataType.SET:
                raise WrongTypeError()
            result.update(obj.value)
    return [BulkString(m) for m in result]


@registry.register("SINTER", is_mutating=False, min_args=1)
def cmd_sinter(ctx, *keys: bytes):
    first = True
    result: Set[bytes] = set()
    for k in keys:
        obj = ctx.db.get(k)
        if obj is None:
            return []
        if obj.data_type != RedisDataType.SET:
            raise WrongTypeError()
        if first:
            result = set(obj.value)
            first = False
        else:
            result.intersection_update(obj.value)
    return [BulkString(m) for m in result]
