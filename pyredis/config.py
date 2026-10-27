"""
Server configuration dataclass with CLI argument parsing support.
"""
import argparse
from dataclasses import dataclass, field
from typing import Optional, Tuple

from pyredis.persistence.aof import AOFSyncPolicy
from pyredis.storage.eviction import EvictionPolicy


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 6379
    databases: int = 16
    max_clients: int = 10000
    maxmemory: int = 0  # 0 means unlimited
    maxmemory_policy: EvictionPolicy = EvictionPolicy.NOEVICTION

    # Persistence settings
    aof_enabled: bool = False
    aof_filepath: str = "appendonly.aof"
    aof_sync_policy: AOFSyncPolicy = AOFSyncPolicy.EVERYSEC

    rdb_enabled: bool = False
    rdb_filepath: str = "dump.rdb"

    # Replication
    replicaof: Optional[Tuple[str, int]] = None

    @classmethod
    def from_args(cls, args: Optional[argparse.Namespace] = None) -> "ServerConfig":
        if args is None:
            return cls()

        replica_tuple = None
        if getattr(args, "replicaof", None):
            parts = args.replicaof.split()
            if len(parts) == 2:
                replica_tuple = (parts[0], int(parts[1]))

        return cls(
            host=args.host,
            port=args.port,
            databases=args.databases,
            maxmemory=args.maxmemory,
            maxmemory_policy=EvictionPolicy(args.maxmemory_policy),
            aof_enabled=args.aof,
            aof_filepath=args.aof_file,
            aof_sync_policy=AOFSyncPolicy(args.aof_sync),
            rdb_enabled=args.rdb,
            rdb_filepath=args.rdb_file,
            replicaof=replica_tuple,
        )
