"""
Append-Only File (AOF) Persistence Engine.

Supports:
- Writing mutating commands in standard RESP format
- Configurable fsync policies: 'always', 'everysec', 'no'
- Replaying AOF on server startup for crash recovery
- Background / Online AOF Rewrite (Log Compaction) to minimize disk usage
"""
from enum import Enum
import os
import threading
import time
from typing import Any, Callable, List, Optional

from pyredis.protocol import RESPParser, encode_resp
from pyredis.protocol.types import Array, BulkString
from pyredis.storage.types import RedisDataType, RedisObject


class AOFSyncPolicy(str, Enum):
    ALWAYS = "always"
    EVERYSEC = "everysec"
    NO = "no"


class AOFManager:
    """Manages the Append-Only File lifecycle, syncing, and rewrites."""

    def __init__(
        self,
        filepath: str,
        sync_policy: AOFSyncPolicy = AOFSyncPolicy.EVERYSEC,
        enabled: bool = True,
    ):
        self.filepath = filepath
        self.sync_policy = sync_policy
        self.enabled = enabled

        self._file = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._sync_thread: Optional[threading.Thread] = None

        if self.enabled:
            # Ensure parent directory exists
            parent = os.path.dirname(os.path.abspath(filepath))
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._file = open(self.filepath, "a+b")

            if self.sync_policy == AOFSyncPolicy.EVERYSEC:
                self._sync_thread = threading.Thread(
                    target=self._background_fsync_worker,
                    daemon=True,
                    name="AOF-Fsync-Worker",
                )
                self._sync_thread.start()

    def _background_fsync_worker(self) -> None:
        """Background thread fsyncing the AOF every second."""
        while not self._stop_event.is_set():
            if self._stop_event.wait(1.0):
                break
            self.flush(force=True)

    def append(self, command_parts: List[bytes]) -> None:
        """Serialize command as RESP Array and append to AOF."""
        if not self.enabled or self._file is None:
            return

        payload = encode_resp(command_parts)
        with self._lock:
            self._file.write(payload)

            if self.sync_policy == AOFSyncPolicy.ALWAYS:
                self._file.flush()
                os.fsync(self._file.fileno())

    def flush(self, force: bool = False) -> None:
        """Flush buffer and optionally fsync to disk."""
        if not self.enabled or self._file is None:
            return

        with self._lock:
            try:
                self._file.flush()
                if force:
                    os.fsync(self._file.fileno())
            except (ValueError, OSError):
                pass

    def replay(self, execute_command_callback: Callable[[List[bytes]], None]) -> int:
        """
        Replay all commands in AOF from the beginning to reconstruct database state.
        Returns the number of commands successfully executed.
        """
        if not self.enabled or not os.path.exists(self.filepath):
            return 0

        parser = RESPParser()
        count = 0

        with open(self.filepath, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                parser.feed(chunk)
                while True:
                    frame = parser.parse_one()
                    if frame is None:
                        break

                    # Convert RESP frame to list of byte arguments
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
                            execute_command_callback(args)
                            count += 1

        return count

    def rewrite(self, databases: List[Any]) -> None:
        """
        AOF Rewrite (Compaction).
        Iterates over the active dataset across all databases and writes
        minimal generation commands into a temporary AOF file, then atomically
        replaces the current AOF file.
        """
        if not self.enabled:
            return

        temp_path = f"{self.filepath}.temp.{os.getpid()}"
        with open(temp_path, "wb") as temp_file:
            for db_idx, db in enumerate(databases):
                # If multiple databases, write SELECT
                if len(databases) > 1:
                    temp_file.write(encode_resp([b"SELECT", str(db_idx).encode("ascii")]))

                now = time.time()
                for key, obj in list(db._dict.items()):
                    # Skip expired keys
                    if db._is_expired(key, now):
                        continue

                    # Write commands based on data type
                    if obj.data_type == RedisDataType.STRING:
                        cmd = [b"SET", key, obj.value]
                        temp_file.write(encode_resp(cmd))

                    elif obj.data_type == RedisDataType.LIST:
                        if obj.value:
                            cmd = [b"RPUSH", key] + list(obj.value)
                            temp_file.write(encode_resp(cmd))

                    elif obj.data_type == RedisDataType.SET:
                        if obj.value:
                            cmd = [b"SADD", key] + list(obj.value)
                            temp_file.write(encode_resp(cmd))

                    elif obj.data_type == RedisDataType.HASH:
                        if obj.value:
                            cmd = [b"HSET", key]
                            for field_name, field_val in obj.value.items():
                                cmd.extend([field_name, field_val])
                            temp_file.write(encode_resp(cmd))

                    elif obj.data_type == RedisDataType.ZSET:
                        zset = obj.value
                        if len(zset) > 0:
                            cmd = [b"ZADD", key]
                            for member, score in zset.range_by_rank(0, -1):
                                cmd.extend([str(score).encode("ascii"), member])
                            temp_file.write(encode_resp(cmd))

                    # Restore TTL if key has expiry
                    exp = db._expires.get(key)
                    if exp is not None and exp > now:
                        ttl_sec = int(exp - now)
                        if ttl_sec > 0:
                            temp_file.write(encode_resp([b"EXPIRE", key, str(ttl_sec).encode("ascii")]))

            temp_file.flush()
            os.fsync(temp_file.fileno())

        # Atomically replace old AOF
        with self._lock:
            if self._file is not None:
                self._file.close()

            os.replace(temp_path, self.filepath)
            self._file = open(self.filepath, "a+b")

    def close(self) -> None:
        """Flush and cleanly close AOF."""
        self._stop_event.set()
        if self._sync_thread and self._sync_thread.is_alive():
            self._sync_thread.join(timeout=1.0)

        with self._lock:
            if self._file is not None:
                try:
                    self._file.flush()
                    os.fsync(self._file.fileno())
                    self._file.close()
                except OSError:
                    pass
                self._file = None
