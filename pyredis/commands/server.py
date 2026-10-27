"""
Server administrative and metadata commands: PING, ECHO, INFO, TIME, COMMAND, CLIENT, CONFIG, BGREWRITEAOF, BGSAVE.
"""
import time

from pyredis.protocol import OK, PONG, BulkString, Integer, SimpleString
from .base import registry


@registry.register("PING", is_mutating=False, min_args=0, max_args=1)
def cmd_ping(ctx, *args: bytes):
    if not args:
        return PONG
    return BulkString(args[0])


@registry.register("ECHO", is_mutating=False, min_args=1, max_args=1)
def cmd_echo(ctx, message: bytes):
    return BulkString(message)


@registry.register("TIME", is_mutating=False, min_args=0, max_args=0)
def cmd_time(ctx):
    now = time.time()
    seconds = int(now)
    microseconds = int((now - seconds) * 1_000_000)
    return [
        BulkString(str(seconds).encode("ascii")),
        BulkString(str(microseconds).encode("ascii")),
    ]


@registry.register("COMMAND", is_mutating=False, min_args=0)
def cmd_command(ctx, *args):
    # Returns OK or empty list to satisfy redis-cli client initialization
    return []


@registry.register("CLIENT", is_mutating=False, min_args=1)
def cmd_client(ctx, subcmd: bytes, *args: bytes):
    sub = subcmd.upper()
    if sub == b"SETNAME" and args:
        ctx.connection.name = args[0].decode("utf-8", errors="replace")
        return OK
    elif sub == b"GETNAME":
        name = getattr(ctx.connection, "name", None)
        return BulkString(name.encode("utf-8")) if name else None
    return OK


@registry.register("CONFIG", is_mutating=False, min_args=1)
def cmd_config(ctx, subcmd: bytes, *args: bytes):
    sub = subcmd.upper()
    if sub == b"GET" and args:
        param = args[0].decode("latin1").lower()
        if param in ("maxmemory", "*"):
            return [BulkString(b"maxmemory"), BulkString(str(ctx.server.config.maxmemory).encode("ascii"))]
        if param in ("port", "*"):
            return [BulkString(b"port"), BulkString(str(ctx.server.config.port).encode("ascii"))]
        return []
    return OK


@registry.register("INFO", is_mutating=False, min_args=0, max_args=1)
def cmd_info(ctx, *args: bytes):
    uptime = int(time.time() - ctx.server.start_time)
    connected_clients = len(ctx.server.clients)
    total_keys = sum(len(d._dict) for d in ctx.server.databases)
    role = ctx.server.replication_manager.role

    info_text = (
        f"# Server\r\n"
        f"redis_version:7.0.0-pyredis\r\n"
        f"os:Darwin\r\n"
        f"uptime_in_seconds:{uptime}\r\n"
        f"# Clients\r\n"
        f"connected_clients:{connected_clients}\r\n"
        f"# Memory\r\n"
        f"used_memory:{sum(d.approximate_memory_bytes() for d in ctx.server.databases)}\r\n"
        f"# Persistence\r\n"
        f"aof_enabled:{1 if ctx.server.aof.enabled else 0}\r\n"
        f"# Replication\r\n"
        f"role:{role}\r\n"
        f"connected_slaves:{len(ctx.server.replication_manager.replicas)}\r\n"
        f"# Keyspace\r\n"
        f"db0:keys={total_keys},expires={len(ctx.server.databases[0]._expires)}\r\n"
    )
    return BulkString(info_text.encode("utf-8"))


@registry.register("BGREWRITEAOF", is_mutating=False, min_args=0, max_args=0)
def cmd_bgrewriteaof(ctx):
    ctx.server.aof.rewrite(ctx.server.databases)
    return SimpleString("Background append only file rewriting started")


@registry.register("BGSAVE", is_mutating=False, min_args=0, max_args=0)
def cmd_bgsave(ctx):
    ctx.server.snapshot.save(ctx.server.databases)
    return SimpleString("Background saving started")
