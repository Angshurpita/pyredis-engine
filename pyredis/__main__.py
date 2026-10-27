"""
Entry point for running pyredis server from CLI: python3 -m pyredis
"""
import argparse
import asyncio
import logging
import signal
import sys

from pyredis.config import ServerConfig
from pyredis.server import Server


def parse_arguments() -> ServerConfig:
    parser = argparse.ArgumentParser(
        description="pyredis - High-Performance In-Memory Key-Value Database"
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("-p", "--port", type=int, default=6379, help="Listen port (default: 6379)")
    parser.add_argument("--databases", type=int, default=16, help="Number of databases (default: 16)")
    parser.add_argument("--maxmemory", type=int, default=0, help="Max memory in bytes (0 = unlimited)")
    parser.add_argument(
        "--maxmemory-policy",
        default="noeviction",
        choices=[
            "noeviction",
            "allkeys-lru",
            "volatile-lru",
            "allkeys-lfu",
            "volatile-lfu",
            "allkeys-random",
            "volatile-random",
            "volatile-ttl",
        ],
        help="Eviction policy when maxmemory is reached",
    )
    parser.add_argument("--aof", action="store_true", help="Enable Append-Only File (AOF) persistence")
    parser.add_argument("--aof-file", default="appendonly.aof", help="Path to AOF file")
    parser.add_argument(
        "--aof-sync",
        default="everysec",
        choices=["always", "everysec", "no"],
        help="AOF fsync policy (default: everysec)",
    )
    parser.add_argument("--rdb", action="store_true", help="Enable RDB snapshotting")
    parser.add_argument("--rdb-file", default="dump.rdb", help="Path to RDB dump file")
    parser.add_argument(
        "--replicaof",
        default=None,
        help="Replicate from master host:port (e.g. '127.0.0.1 6379')",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING)")

    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return ServerConfig.from_args(args)


async def main():
    config = parse_arguments()
    server = Server(config)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown_signal():
        logging.info("Received shutdown signal. Stopping pyredis server...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_signal)
        except NotImplementedError:
            pass  # Windows or non-main thread

    await server.start()
    await stop_event.wait()
    await server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
