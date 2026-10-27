"""
Sorted Set (ZSET) command handlers: ZADD, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZCARD, ZREM.
Backed by SkipList + Hash Table.
"""
from pyredis.protocol import NULL_BULK, BulkString, Integer
from pyredis.storage import RedisDataType, RedisObject, RedisZSet
from .base import CommandError, WrongArgCountError, WrongTypeError, registry


@registry.register("ZADD", is_mutating=True, min_args=3)
def cmd_zadd(ctx, key: bytes, *args: bytes):
    if len(args) % 2 != 0:
        raise WrongArgCountError("zadd")

    obj = ctx.db.get(key)
    if obj is None:
        zset = RedisZSet()
        obj = RedisObject(RedisDataType.ZSET, zset)
        ctx.db.set(key, obj)
    elif obj.data_type != RedisDataType.ZSET:
        raise WrongTypeError()
    else:
        zset = obj.value

    added_count = 0
    for i in range(0, len(args), 2):
        score_b = args[i]
        member = args[i + 1]
        try:
            score = float(score_b)
        except ValueError:
            raise CommandError("ERR value is not a valid float")

        if zset.add(score, member):
            added_count += 1

    return Integer(added_count)


@registry.register("ZSCORE", is_mutating=False, min_args=2, max_args=2)
def cmd_zscore(ctx, key: bytes, member: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return NULL_BULK
    if obj.data_type != RedisDataType.ZSET:
        raise WrongTypeError()

    score = obj.value.score(member)
    if score is None:
        return NULL_BULK
    # Redis returns float formatted as string in bulk format
    return BulkString(f"{score:g}".encode("ascii"))


@registry.register("ZRANK", is_mutating=False, min_args=2, max_args=2)
def cmd_zrank(ctx, key: bytes, member: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return NULL_BULK
    if obj.data_type != RedisDataType.ZSET:
        raise WrongTypeError()

    rank = obj.value.rank(member, reverse=False)
    if rank is None:
        return NULL_BULK
    return Integer(rank)


@registry.register("ZREVRANK", is_mutating=False, min_args=2, max_args=2)
def cmd_zrevrank(ctx, key: bytes, member: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return NULL_BULK
    if obj.data_type != RedisDataType.ZSET:
        raise WrongTypeError()

    rank = obj.value.rank(member, reverse=True)
    if rank is None:
        return NULL_BULK
    return Integer(rank)


@registry.register("ZRANGE", is_mutating=False, min_args=3)
def cmd_zrange(ctx, key: bytes, start_b: bytes, stop_b: bytes, *options: bytes):
    with_scores = any(opt.upper() == b"WITHSCORES" for opt in options)

    obj = ctx.db.get(key)
    if obj is None:
        return []
    if obj.data_type != RedisDataType.ZSET:
        raise WrongTypeError()

    try:
        start = int(start_b)
        stop = int(stop_b)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")

    items = obj.value.range_by_rank(start, stop, reverse=False)
    res = []
    for member, score in items:
        res.append(BulkString(member))
        if with_scores:
            res.append(BulkString(f"{score:g}".encode("ascii")))
    return res


@registry.register("ZREVRANGE", is_mutating=False, min_args=3)
def cmd_zrevrange(ctx, key: bytes, start_b: bytes, stop_b: bytes, *options: bytes):
    with_scores = any(opt.upper() == b"WITHSCORES" for opt in options)

    obj = ctx.db.get(key)
    if obj is None:
        return []
    if obj.data_type != RedisDataType.ZSET:
        raise WrongTypeError()

    try:
        start = int(start_b)
        stop = int(stop_b)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range")

    items = obj.value.range_by_rank(start, stop, reverse=True)
    res = []
    for member, score in items:
        res.append(BulkString(member))
        if with_scores:
            res.append(BulkString(f"{score:g}".encode("ascii")))
    return res


@registry.register("ZCARD", is_mutating=False, min_args=1, max_args=1)
def cmd_zcard(ctx, key: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.ZSET:
        raise WrongTypeError()
    return Integer(len(obj.value))


@registry.register("ZREM", is_mutating=True, min_args=2)
def cmd_zrem(ctx, key: bytes, *members: bytes):
    obj = ctx.db.get(key)
    if obj is None:
        return Integer(0)
    if obj.data_type != RedisDataType.ZSET:
        raise WrongTypeError()

    removed = 0
    for m in members:
        if obj.value.remove(m):
            removed += 1

    if len(obj.value) == 0:
        ctx.db.delete(key)

    return Integer(removed)
