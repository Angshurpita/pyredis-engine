"""
Hash command handlers: HSET, HGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN.
"""
from typing import Dict

from pyredis.protocol import NULL_BULK, BulkString, Integer
from pyredis.storage import RedisDataType, RedisObject
from .base import CommandError, WrongArgCountError, WrongTypeError, registry


@registry.register("HSET", is_mutating=True, min_args=3)
def cmd_hset(ctx, key: bytes, *args: bytes):
    if len(args) % 2 != 0:
        raise WrongArgCountError("hset")

    obj = ctx.db.get(key)
    if obj is None:
        hash_dict: Dict[bytes, bytes] = {}
        obj = RedisObject(RedisDataType.HASH, hash_dict)
        ctx.db.set(key, obj)
    elif obj.data_type != RedisDataType.HASH:
        raise WrongTypeError()
    else:
        hash_dict = obj.value

    new_fields = 0
    for i in range(0, len(args), 2):
        field_name = args[i]
        field_val = args[i + 1]
        if field_name not in hash_dict:
            new_fields += 1
        hash_dict[field_name] = field_val

    return Integer(new_fields)


@registry.register("HGET", is_mutating=False, min_args=2, max_args=2)
def cmd_hget(ctx, key: bytes, field_name: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return NULL_BULK
    if obj.data_type != RedisDataType.HASH:
        raise WrongTypeError()

    val = obj.value.get(field_name)
    if val is None:
        return NULL_BULK
    return BulkString(val)


@registry.register("HDEL", is_mutating=True, min_args=2)
def cmd_hdel(ctx, key: bytes, *fields: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.HASH:
        raise WrongTypeError()

    deleted = 0
    for f in fields:
        if f in obj.value:
            del obj.value[f]
            deleted += 1

    if not obj.value:
        ctx.db.delete(key)

    return Integer(deleted)


@registry.register("HEXISTS", is_mutating=False, min_args=2, max_args=2)
def cmd_hexists(ctx, key: bytes, field_name: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.HASH:
        raise WrongTypeError()
    return Integer(1 if field_name in obj.value else 0)


@registry.register("HGETALL", is_mutating=False, min_args=1, max_args=1)
def cmd_hgetall(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return []
    if obj.data_type != RedisDataType.HASH:
        raise WrongTypeError()

    items = []
    for k, v in obj.value.items():
        items.append(BulkString(k))
        items.append(BulkString(v))
    return items


@registry.register("HKEYS", is_mutating=False, min_args=1, max_args=1)
def cmd_hkeys(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return []
    if obj.data_type != RedisDataType.HASH:
        raise WrongTypeError()
    return [BulkString(k) for k in obj.value.keys()]


@registry.register("HVALS", is_mutating=False, min_args=1, max_args=1)
def cmd_hvals(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return []
    if obj.data_type != RedisDataType.HASH:
        raise WrongTypeError()
    return [BulkString(v) for v in obj.value.values()]


@registry.register("HLEN", is_mutating=False, min_args=1, max_args=1)
def cmd_hlen(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.HASH:
        raise WrongTypeError()
    return Integer(len(obj.value))
