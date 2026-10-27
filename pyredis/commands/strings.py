"""
String command handlers: GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN, GETSET.
"""
import time
from typing import List, Optional

from pyredis.protocol import OK, NULL_BULK, BulkString, Integer, SimpleString
from pyredis.storage import RedisDataType, RedisObject
from .base import CommandError, WrongArgCountError, WrongTypeError, registry


@registry.register("GET", is_mutating=False, min_args=1, max_args=1)
def cmd_get(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return NULL_BULK
    if obj.data_type != RedisDataType.STRING:
        raise WrongTypeError()
    return BulkString(obj.value)


@registry.register("SET", is_mutating=True, min_args=2)
def cmd_set(ctx, key: bytes, value: bytes, *options: bytes):
    expire_at: Optional[float] = None
    nx = False
    xx = False

    i = 0
    while i < len(options):
        opt = options[i].upper()
        if opt == b"EX":
            if i + 1 >= len(options):
                raise CommandError("ERR syntax error")
            try:
                seconds = float(options[i + 1])
                expire_at = time.time() + seconds
                i += 2
            except ValueError:
                raise CommandError("ERR value is not an integer or out of range")
        elif opt == b"PX":
            if i + 1 >= len(options):
                raise CommandError("ERR syntax error")
            try:
                ms = float(options[i + 1])
                expire_at = time.time() + (ms / 1000.0)
                i += 2
            except ValueError:
                raise CommandError("ERR value is not an integer or out of range")
        elif opt == b"NX":
            nx = True
            i += 1
        elif opt == b"XX":
            xx = True
            i += 1
        else:
            raise CommandError("ERR syntax error")

    exists = ctx.db.exists(key)
    if nx and exists:
        return NULL_BULK
    if xx and not exists:
        return NULL_BULK

    obj = RedisObject(RedisDataType.STRING, value)
    ctx.db.set(key, obj, expire_at=expire_at)
    return OK


@registry.register("MGET", is_mutating=False, min_args=1)
def cmd_mget(ctx, *keys: bytes):
    results = []
    for k in keys:
        obj = ctx.db.get(k)
        if obj is None or obj.data_type != RedisDataType.STRING:
            results.append(NULL_BULK)
        else:
            results.append(BulkString(obj.value))
    return results


@registry.register("MSET", is_mutating=True, min_args=2)
def cmd_mset(ctx, *args: bytes):
    if len(args) % 2 != 0:
        raise WrongArgCountError("mset")

    for i in range(0, len(args), 2):
        key = args[i]
        val = args[i + 1]
        obj = RedisObject(RedisDataType.STRING, val)
        ctx.db.set(key, obj)
    return OK


def _increment_by(ctx, key: bytes, delta: int) -> int:
    obj = ctx.db.get(key)
    if obj is None:
        new_val = delta
        ctx.db.set(key, RedisObject(RedisDataType.STRING, str(new_val).encode("ascii")))
        return new_val

    if obj.data_type != RedisDataType.STRING:
        raise WrongTypeError()

    try:
        current_val = int(obj.value)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")

    new_val = current_val + delta
    obj.value = str(new_val).encode("ascii")
    return new_val


@registry.register("INCR", is_mutating=True, min_args=1, max_args=1)
def cmd_incr(ctx, key: bytes):
    val = _increment_by(ctx, key, 1)
    return Integer(val)


@registry.register("DECR", is_mutating=True, min_args=1, max_args=1)
def cmd_decr(ctx, key: bytes):
    val = _increment_by(ctx, key, -1)
    return Integer(val)


@registry.register("INCRBY", is_mutating=True, min_args=2, max_args=2)
def cmd_incrby(ctx, key: bytes, increment: bytes):
    try:
        delta = int(increment)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")
    val = _increment_by(ctx, key, delta)
    return Integer(val)


@registry.register("DECRBY", is_mutating=True, min_args=2, max_args=2)
def cmd_decrby(ctx, key: bytes, decrement: bytes):
    try:
        delta = int(decrement)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")
    val = _increment_by(ctx, key, -delta)
    return Integer(val)


@registry.register("APPEND", is_mutating=True, min_args=2, max_args=2)
def cmd_append(ctx, key: bytes, value: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        ctx.db.set(key, RedisObject(RedisDataType.STRING, value))
        return Integer(len(value))

    if obj.data_type != RedisDataType.STRING:
        raise WrongTypeError()

    obj.value += value
    return Integer(len(obj.value))


@registry.register("STRLEN", is_mutating=False, min_args=1, max_args=1)
def cmd_strlen(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.STRING:
        raise WrongTypeError()
    return Integer(len(obj.value))


@registry.register("GETSET", is_mutating=True, min_args=2, max_args=2)
def cmd_getset(ctx, key: bytes, value: bytes):
    old_obj = ctx.db.get(key)
    if old_obj is not None and old_obj.data_type != RedisDataType.STRING:
        raise WrongTypeError()

    ctx.db.set(key, RedisObject(RedisDataType.STRING, value))
    if old_obj is None:
        return NULL_BULK
    return BulkString(old_obj.value)
