"""
Generic keyspace and TTL commands: DEL, EXISTS, EXPIRE, PEXPIRE, TTL, PTTL, PERSIST, TYPE, KEYS, FLUSHDB, DBSIZE, SELECT.
"""
import time

from pyredis.protocol import OK, Integer, SimpleString
from .base import CommandError, registry


@registry.register("DEL", is_mutating=True, min_args=1)
def cmd_del(ctx, *keys: bytes):
    deleted = 0
    for k in keys:
        if ctx.db.delete(k):
            deleted += 1
    return Integer(deleted)


@registry.register("EXISTS", is_mutating=False, min_args=1)
def cmd_exists(ctx, *keys: bytes):
    count = 0
    for k in keys:
        if ctx.db.exists(k):
            count += 1
    return Integer(count)


@registry.register("EXPIRE", is_mutating=True, min_args=2, max_args=2)
def cmd_expire(ctx, key: bytes, seconds_b: bytes):
    try:
        seconds = float(seconds_b)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")
    expire_at = time.time() + seconds
    success = ctx.db.set_expire(key, expire_at)
    return Integer(1 if success else 0)


@registry.register("PEXPIRE", is_mutating=True, min_args=2, max_args=2)
def cmd_pexpire(ctx, key: bytes, ms_b: bytes):
    try:
        ms = float(ms_b)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")
    expire_at = time.time() + (ms / 1000.0)
    success = ctx.db.set_expire(key, expire_at)
    return Integer(1 if success else 0)


@registry.register("TTL", is_mutating=False, min_args=1, max_args=1)
def cmd_ttl(ctx, key: bytes):
    ttl = ctx.db.get_ttl(key)
    if ttl < 0:
        return Integer(int(ttl))
    return Integer(int(ttl))


@registry.register("PTTL", is_mutating=False, min_args=1, max_args=1)
def cmd_pttl(ctx, key: bytes):
    ttl = ctx.db.get_ttl(key)
    if ttl < 0:
        return Integer(int(ttl))
    return Integer(int(ttl * 1000.0))


@registry.register("PERSIST", is_mutating=True, min_args=1, max_args=1)
def cmd_persist(ctx, key: bytes):
    return Integer(1 if ctx.db.persist(key) else 0)


@registry.register("TYPE", is_mutating=False, min_args=1, max_args=1)
def cmd_type(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return SimpleString("none")
    return SimpleString(obj.data_type)


@registry.register("KEYS", is_mutating=False, min_args=1, max_args=1)
def cmd_keys(ctx, pattern: bytes):
    matched = ctx.db.keys(pattern)
    return matched


@registry.register("FLUSHDB", is_mutating=True, min_args=0, max_args=1)
def cmd_flushdb(ctx, *args):
    ctx.db.flush()
    return OK


@registry.register("FLUSHALL", is_mutating=True, min_args=0, max_args=1)
def cmd_flushall(ctx, *args):
    for database in ctx.server.databases:
        database.flush()
    return OK


@registry.register("DBSIZE", is_mutating=False, min_args=0, max_args=0)
def cmd_dbsize(ctx):
    return Integer(ctx.db.dbsize())


@registry.register("SELECT", is_mutating=False, min_args=1, max_args=1)
def cmd_select(ctx, index_b: bytes):
    try:
        idx = int(index_b)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")

    if idx < 0 or idx >= len(ctx.server.databases):
        raise CommandError("ERR DB index is out of range")

    ctx.connection.db_index = idx
    return OK
