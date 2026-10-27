"""
Master-Replica Replication Engine.
Handles:
- PSYNC Handshake and initial data synchronization
- Streaming real-time command propagation from Primary to all Replicas
- Read-only protection for Replicas
- Promotion to Master ('REPLICAOF NO ONE')
"""
import asyncio
import logging
import secrets
from typing import Any, List, Optional, Set

from pyredis.protocol import RESPParser, encode_resp
from pyredis.protocol.types import Array, BulkString
from pyredis.storage.types import RedisDataType

logger = logging.getLogger(__name__)


class ReplicationManager:
    """Manages node replication role, connections, and command streaming."""

    def __init__(self, server: Any):
        self.server = server
        self.role: str = "master"
        self.replication_id: str = secrets.token_hex(20)  # 40-char hex
        self.replication_offset: int = 0
        self.master_host: Optional[str] = None
        self.master_port: Optional[int] = None

        self.replicas: Set[Any] = set()
        self._master_task: Optional[asyncio.Task] = None
        self._master_writer: Optional[asyncio.StreamWriter] = None

    def make_master(self) -> None:
        """Promote this node to Master."""
        logger.info("Promoting node to Master (REPLICAOF NO ONE)")
        self.role = "master"
        self.master_host = None
        self.master_port = None
        if self._master_task is not None:
            self._master_task.cancel()
            self._master_task = None
        if self._master_writer is not None:
            try:
                self._master_writer.close()
            except Exception:
                pass
            self._master_writer = None
        self.replication_id = secrets.token_hex(20)

    def connect_to_master(self, host: str, port: int) -> None:
        """Switch node to Replica of given host:port and initiate sync."""
        logger.info(f"Configuring as replica of {host}:{port}")
        self.role = "slave"
        self.master_host = host
        self.master_port = port

        if self._master_task is not None:
            self._master_task.cancel()

        self._master_task = asyncio.create_task(self._sync_with_master_loop(host, port))

    def register_replica_connection(self, conn: Any) -> None:
        """Master registers a replica connection and streams current snapshot."""
        conn.is_replica = True
        self.replicas.add(conn)
        logger.info(f"Registered new replica: {conn.peer_address}")

        # Send full snapshot of current keyspace to replica
        for db_idx, db in enumerate(self.server.databases):
            if len(self.server.databases) > 1:
                conn.send_raw(encode_resp([b"SELECT", str(db_idx).encode("ascii")]))

            for key, obj in list(db._dict.items()):
                if db._is_expired(key):
                    continue

                if obj.data_type == RedisDataType.STRING:
                    conn.send_raw(encode_resp([b"SET", key, obj.value]))
                elif obj.data_type == RedisDataType.LIST:
                    if obj.value:
                        conn.send_raw(encode_resp([b"RPUSH", key] + list(obj.value)))
                elif obj.data_type == RedisDataType.SET:
                    if obj.value:
                        conn.send_raw(encode_resp([b"SADD", key] + list(obj.value)))
                elif obj.data_type == RedisDataType.HASH:
                    if obj.value:
                        cmd = [b"HSET", key]
                        for f, v in obj.value.items():
                            cmd.extend([f, v])
                        conn.send_raw(encode_resp(cmd))
                elif obj.data_type == RedisDataType.ZSET:
                    zset = obj.value
                    if len(zset) > 0:
                        cmd = [b"ZADD", key]
                        for member, score in zset.range_by_rank(0, -1):
                            cmd.extend([str(score).encode("ascii"), member])
                        conn.send_raw(encode_resp(cmd))

    def propagate_command(self, command_parts: List[bytes]) -> None:
        """Stream a write command to all connected replicas."""
        if not self.replicas or self.role != "master":
            return

        payload = encode_resp(command_parts)
        self.replication_offset += len(payload)

        for rep in list(self.replicas):
            try:
                rep.send_raw(payload)
            except Exception:
                self.replicas.discard(rep)

    async def _sync_with_master_loop(self, host: str, port: int) -> None:
        """Replica handshake with master and continuous command stream consumption."""
        try:
            reader, writer = await asyncio.open_connection(host, port)
            self._master_writer = writer

            # 1. PING
            writer.write(encode_resp([b"PING"]))
            await writer.drain()
            resp = await reader.readline()  # Expect +PONG\r\n

            # 2. REPLCONF listening-port
            port_bytes = str(self.server.config.port).encode("ascii")
            writer.write(encode_resp([b"REPLCONF", b"listening-port", port_bytes]))
            await writer.drain()
            resp = await reader.readline()  # Expect +OK\r\n

            # 3. PSYNC
            writer.write(encode_resp([b"PSYNC", b"?", b"-1"]))
            await writer.drain()
            resp = await reader.readline()  # Expect +FULLRESYNC <id> <offset>\r\n

            logger.info(f"Replication handshake completed with {host}:{port}. Streaming writes...")

            # 4. Stream incoming commands from master and apply them locally
            parser = RESPParser()
            while True:
                data = await reader.read(65536)
                if not data:
                    break

                parser.feed(data)
                while True:
                    frame = parser.parse_one()
                    if frame is None:
                        break

                    if isinstance(frame, Array) and frame.elements:
                        args: List[bytes] = []
                        for elem in frame.elements:
                            if isinstance(elem, BulkString) and elem.value is not None:
                                args.append(elem.value)
                            elif isinstance(elem, (str, bytes)):
                                args.append(elem.encode("utf-8") if isinstance(elem, str) else elem)
                            else:
                                args.append(str(elem).encode("utf-8"))

                        if args:
                            # Apply replicated command locally (bypass read-only check)
                            self.server.execute_replicated_command(args)

        except asyncio.CancelledError:
            logger.info("Replication stream canceled")
        except Exception as e:
            logger.warning(f"Replication stream error: {e}")
        finally:
            if self._master_writer:
                try:
                    self._master_writer.close()
                    await self._master_writer.wait_closed()
                except Exception:
                    pass
                self._master_writer = None
