"""
High-Performance Asyncio Redis Server.

Features:
- Non-blocking I/O multiplexing event loop with asyncio
- Zero-copy incremental RESP parsing with pipelining support
- Multi-database support (default 16)
- Background active probabilistic expiration cycle (every 100ms)
- Durability via AOF (Append-Only File) and RDB-style snapshotting
- Master-Replica real-time replication streaming
- Pub/Sub channels broadcasting
- MULTI / EXEC atomic transaction blocks
- Clean graceful shutdown handling
"""
import asyncio
import logging
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from pyredis.commands import (
    CommandError,
    WrongArgCountError,
    WrongTypeError,
    registry,
)
from pyredis.config import ServerConfig
from pyredis.persistence import AOFManager, SnapshotManager
from pyredis.protocol import (
    Array,
    BulkString,
    ErrorResponse,
    IncompleteBufferError,
    ProtocolSyntaxError,
    RESPParser,
    SimpleString,
    encode_resp,
    QUEUED,
)
from pyredis.replication import ReplicationManager
from pyredis.storage import Database, OutOfMemoryError

logger = logging.getLogger("pyredis")


class ClientConnection:
    """Represents a connected client session and state."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.db_index: int = 0
        self.name: Optional[str] = None
        self.parser = RESPParser()

        # Transaction state
        self.in_transaction: bool = False
        self.transaction_queue: List[List[bytes]] = []

        # Pub/Sub state
        self.is_pubsub: bool = False
        self.subscribed_channels: Set[bytes] = set()

        # Replication state
        self.is_replica: bool = False

    @property
    def peer_address(self) -> str:
        if self.writer is None:
            return "mock:0"
        peer = self.writer.get_extra_info("peername")
        return f"{peer[0]}:{peer[1]}" if peer else "unknown"

    def send_frame(self, frame: Any) -> None:
        """Serialize RESP frame and send to client."""
        payload = encode_resp(frame)
        self.send_raw(payload)

    def send_raw(self, data: bytes) -> None:
        """Write raw bytes into connection buffer."""
        if self.writer is not None and not self.writer.is_closing():
            self.writer.write(data)

    async def flush(self) -> None:
        """Drain pending writes to socket."""
        if self.writer is not None and not self.writer.is_closing():
            await self.writer.drain()

    def close(self) -> None:
        """Close client connection."""
        if self.writer is not None and not self.writer.is_closing():
            try:
                self.writer.close()
            except Exception:
                pass


class ExecutionContext:
    """Context passed into command handler functions."""

    def __init__(self, server: "Server", connection: ClientConnection):
        self.server = server
        self.connection = connection

    @property
    def db(self) -> Database:
        return self.server.databases[self.connection.db_index]


class Server:
    """The central Redis Server instance."""

    def __init__(self, config: Optional[ServerConfig] = None):
        self.config = config or ServerConfig()
        self.start_time = time.time()

        # Initialize databases
        self.databases: List[Database] = [
            Database(
                db_id=i,
                maxmemory=self.config.maxmemory,
                maxmemory_policy=self.config.maxmemory_policy,
            )
            for i in range(self.config.databases)
        ]

        # Connected clients and pub/sub channels
        self.clients: Set[ClientConnection] = set()
        self.pubsub_channels: Dict[bytes, Set[ClientConnection]] = {}

        # Persistence managers
        self.aof = AOFManager(
            filepath=self.config.aof_filepath,
            sync_policy=self.config.aof_sync_policy,
            enabled=self.config.aof_enabled,
        )
        self.snapshot = SnapshotManager(filepath=self.config.rdb_filepath)

        # Replication manager
        self.replication_manager = ReplicationManager(self)

        self._server: Optional[asyncio.Server] = None
        self._running: bool = False
        self._expire_task: Optional[asyncio.Task] = None

    def _recover_persistence(self) -> None:
        """Restore state from disk on startup."""
        if self.config.rdb_enabled:
            loaded = self.snapshot.load(self.databases)
            if loaded:
                logger.info(f"Loaded snapshot from {self.config.rdb_filepath}")

        if self.config.aof_enabled:
            # Replay commands into DB 0
            dummy_conn = ClientConnection(None, None)  # type: ignore

            def replay_callback(cmd_parts: List[bytes]):
                self.execute_single_command(dummy_conn, cmd_parts, in_multi=True, is_replay=True)

            replayed = self.aof.replay(replay_callback)
            if replayed:
                logger.info(f"Replayed {replayed} commands from AOF {self.config.aof_filepath}")

    async def start(self) -> None:
        """Start listening for incoming TCP client connections."""
        self._running = True
        self._recover_persistence()

        # Connect to master if configured as replica
        if self.config.replicaof:
            host, port = self.config.replicaof
            self.replication_manager.connect_to_master(host, port)

        # Start periodic background active expiry cycle
        self._expire_task = asyncio.create_task(self._active_expire_loop())

        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.config.host,
            port=self.config.port,
        )

        addr = self._server.sockets[0].getsockname()
        logger.info(f"pyredis server running on {addr[0]}:{addr[1]} (PID: {sys.argv[0]})")

    async def _active_expire_loop(self) -> None:
        """Periodically trigger active probabilistic expiration across databases."""
        while self._running:
            try:
                await asyncio.sleep(0.1)  # Every 100ms
                for db in self.databases:
                    db.active_expire_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in active expire cycle: {e}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Connection loop for an individual client connection."""
        client = ClientConnection(reader, writer)
        self.clients.add(client)

        try:
            while self._running:
                data = await reader.read(65536)
                if not data:
                    break  # Client closed connection cleanly

                client.parser.feed(data)

                # Process all complete RESP frames in buffer (pipelining)
                has_pending_writes = False
                while True:
                    try:
                        frame = client.parser.parse_one()
                    except ProtocolSyntaxError as e:
                        client.send_frame(ErrorResponse(f"ERR Protocol error: {e}"))
                        has_pending_writes = True
                        break

                    if frame is None:
                        break  # Buffer waiting for more data

                    # Dispatch command
                    self._process_frame(client, frame)
                    has_pending_writes = True

                if has_pending_writes:
                    await client.flush()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Client {client.peer_address} error: {e}")
        finally:
            self._cleanup_client(client)

    def _cleanup_client(self, client: ClientConnection) -> None:
        """Remove client from subscriptions, replicas, and client pool."""
        self.clients.discard(client)
        self.replication_manager.replicas.discard(client)

        # Unsubscribe from channels
        for chan in client.subscribed_channels:
            subscribers = self.pubsub_channels.get(chan)
            if subscribers:
                subscribers.discard(client)
                if not subscribers:
                    del self.pubsub_channels[chan]

        client.close()

    def _process_frame(self, client: ClientConnection, frame: Any) -> None:
        """Extract command arguments and execute."""
        command_parts: List[bytes] = []

        if isinstance(frame, Array):
            if frame.elements:
                for elem in frame.elements:
                    if isinstance(elem, BulkString):
                        command_parts.append(elem.value or b"")
                    elif isinstance(elem, (str, bytes)):
                        command_parts.append(elem.encode("utf-8") if isinstance(elem, str) else elem)
                    else:
                        command_parts.append(str(elem).encode("utf-8"))
        elif isinstance(frame, BulkString):
            if frame.value:
                command_parts = frame.value.split()
        elif isinstance(frame, SimpleString):
            command_parts = frame.value.encode("utf-8").split()

        if not command_parts:
            return

        cmd_name = command_parts[0].upper().decode("latin1", errors="replace")

        # Check transaction queuing
        if client.in_transaction and cmd_name not in ("EXEC", "DISCARD", "MULTI", "QUIT"):
            client.transaction_queue.append(command_parts)
            client.send_frame(QUEUED)
            return

        # Execute command
        result = self.execute_single_command(client, command_parts)
        if result is not None:
            # Pub/sub subscribe commands may return multiple frames
            if cmd_name in ("SUBSCRIBE", "UNSUBSCRIBE") and isinstance(result, list):
                for sub_frame in result:
                    client.send_frame(sub_frame)
            else:
                client.send_frame(result)

    def execute_single_command(
        self,
        client: ClientConnection,
        command_parts: List[bytes],
        in_multi: bool = False,
        is_replay: bool = False,
    ) -> Any:
        """
        Validate, dispatch, persist, and replicate a single command.
        """
        cmd_name = command_parts[0].upper().decode("latin1", errors="replace")
        spec = registry.get(cmd_name)

        if spec is None:
            return ErrorResponse(f"ERR unknown command '{cmd_name}'")

        args = command_parts[1:]

        # Validate argument count
        if len(args) < spec.min_args or (spec.max_args is not None and len(args) > spec.max_args):
            return ErrorResponse(f"ERR wrong number of arguments for '{cmd_name.lower()}' command")

        # Read-only replica protection: block client writes if this node is a replica
        if spec.is_mutating and self.replication_manager.role == "slave" and not is_replay:
            return ErrorResponse("READONLY You can't write against a read only replica.")

        ctx = ExecutionContext(self, client)

        try:
            response = spec.handler(ctx, *args)

            # If write command succeeded, log to AOF and stream to Replicas
            if spec.is_mutating and not is_replay:
                self.aof.append(command_parts)
                self.replication_manager.propagate_command(command_parts)

            return response

        except WrongTypeError:
            return ErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value")
        except OutOfMemoryError as e:
            return ErrorResponse(str(e))
        except CommandError as e:
            return ErrorResponse(str(e))
        except Exception as e:
            logger.exception(f"Unexpected error executing {cmd_name}")
            return ErrorResponse(f"ERR server error: {e}")

    def execute_replicated_command(self, command_parts: List[bytes]) -> None:
        """Execute a write command streamed from the Primary node."""
        dummy_conn = ClientConnection(None, None)  # type: ignore
        self.execute_single_command(dummy_conn, command_parts, is_replay=True)

    async def stop(self) -> None:
        """Gracefully shutdown the server."""
        self._running = False

        if self._expire_task:
            self._expire_task.cancel()

        # Close all active client connections
        for client in list(self.clients):
            client.close()
        self.clients.clear()

        # Close server socket
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        # Flush persistence
        self.aof.close()
        logger.info("pyredis server stopped cleanly.")
