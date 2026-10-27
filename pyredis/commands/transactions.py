"""
Transaction command handlers: MULTI, EXEC, DISCARD.
"""
from pyredis.protocol import OK, NULL_ARRAY, SimpleString
from .base import CommandError, registry


@registry.register("MULTI", is_mutating=False, min_args=0, max_args=0)
def cmd_multi(ctx):
    if ctx.connection.in_transaction:
        raise CommandError("ERR MULTI calls can not be nested")
    ctx.connection.in_transaction = True
    ctx.connection.transaction_queue.clear()
    return OK


@registry.register("EXEC", is_mutating=False, min_args=0, max_args=0)
def cmd_exec(ctx):
    if not ctx.connection.in_transaction:
        raise CommandError("ERR EXEC without MULTI")

    queued = list(ctx.connection.transaction_queue)
    ctx.connection.in_transaction = False
    ctx.connection.transaction_queue.clear()

    if not queued:
        return []

    results = []
    for cmd_parts in queued:
        # Execute each queued command through server dispatcher
        res = ctx.server.execute_single_command(ctx.connection, cmd_parts, in_multi=True)
        results.append(res)

    return results


@registry.register("DISCARD", is_mutating=False, min_args=0, max_args=0)
def cmd_discard(ctx):
    if not ctx.connection.in_transaction:
        raise CommandError("ERR DISCARD without MULTI")
    ctx.connection.in_transaction = False
    ctx.connection.transaction_queue.clear()
    return OK
