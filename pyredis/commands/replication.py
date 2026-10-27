"""
Replication command handlers: REPLICAOF, SLAVEOF, PSYNC, REPLCONF.
"""
from pyredis.protocol import OK, SimpleString
from .base import CommandError, registry


@registry.register("REPLICAOF", is_mutating=False, min_args=2, max_args=2)
def cmd_replicaof(ctx, host_b: bytes, port_b: bytes):
    host = host_b.decode("utf-8")
    port_str = port_b.decode("utf-8")

    if host.upper() == "NO" and port_str.upper() == "ONE":
        # Promote to master
        ctx.server.replication_manager.make_master()
        return OK

    try:
        port = int(port_str)
    except ValueError:
        raise CommandError("ERR port is not a valid integer")

    ctx.server.replication_manager.connect_to_master(host, port)
    return OK


@registry.register("SLAVEOF", is_mutating=False, min_args=2, max_args=2)
def cmd_slaveof(ctx, host_b: bytes, port_b: bytes):
    return cmd_replicaof(ctx, host_b, port_b)


@registry.register("PSYNC", is_mutating=False, min_args=2, max_args=2)
def cmd_psync(ctx, repl_id_b: bytes, offset_b: bytes):
    master_repl_id = ctx.server.replication_manager.replication_id
    # Register this connection as a replica client
    ctx.server.replication_manager.register_replica_connection(ctx.connection)
    return SimpleString(f"FULLRESYNC {master_repl_id} 0")


@registry.register("REPLCONF", is_mutating=False, min_args=1)
def cmd_replconf(ctx, *args: bytes):
    return OK
