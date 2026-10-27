# pyredis: Systems Architecture Deep Dive

This document details the internal design, concurrency model, data structures, and algorithmic trade-offs of `pyredis`.

---

## 1. High-Level Architectural Overview

```
                        +----------------------------+
                        |   Client Connections       |
                        | (pyredis-cli, redis-py)    |
                        +--------------+-------------+
                                       |
                                       v  [TCP Sockets]
                    +------------------------------------+
                    |        Asyncio Event Loop          |
                    | (epoll / kqueue non-blocking I/O)  |
                    +------------------+-----------------+
                                       |
                                       v
                    +------------------------------------+
                    |        Streaming RESP Parser       |
                    |   (Zero-copy bytearray buffer)     |
                    +------------------+-----------------+
                                       |
                                       v
                    +------------------------------------+
                    |     Command Dispatch & Context     |
                    |  (Read-Only Check, Auth, MULTI)    |
                    +---+--------------+--------------+--+
                        |              |              |
                        v              v              v
               +------------+   +------------+   +------------+
               |  Strings / |   |   AOF Log  |   | Master ->  |
               | Lists / ZSet|   | Persistence|   | Replicas   |
               | SkipList   |   | (Fsync/    |   | Streaming  |
               |  Storage   |   | Compaction)|   | Stream     |
               +------------+   +------------+   +------------+
                        |
                        v
               +---------------------------------+
               | TTL Engine & Eviction Manager   |
               | (Active Sampling + LRU/LFU/TTL) |
               +---------------------------------+
```

---

## 2. Concurrency & I/O Multiplexing Model

### Why Single-Threaded Event Loop over Multi-Threading?
Traditional Redis is famous for its single-threaded execution core. We deliberately mirror this design using Python's native `asyncio` (backed by `kqueue` on macOS and `epoll` on Linux) for several reasons:

1. **Lock-Free In-Memory Datastores**: Because commands execute sequentially on the event loop, complex data structure modifications (e.g., SkipList pointer updates or hash table resizes) require zero mutexes, spinlocks, or read-write locks. This eliminates deadlocks and thread contention entirely.
2. **C10k Scalability**: Thousands of concurrent socket connections are multiplexed over a single OS thread using non-blocking socket selectors. Connection state (`ClientConnection`) occupies minimal memory.
3. **Pipelining Without Blocking**: Network read buffers are decoupled from execution. Pipelined command batches arrive in a single TCP payload and are executed back-to-back with zero network roundtrips between individual operations.

---

## 3. Streaming Zero-Copy RESP Parser

The Redis Serialization Protocol (RESP2) is a binary-safe, human-readable wire protocol.

### Buffer State Machine
A common flaw in toy implementations is assuming `socket.recv()` returns an entire command. Over TCP:
- Large commands (e.g. bulk strings) may be split across multiple packets.
- Multiple pipelined commands may arrive in a single packet.

`pyredis.protocol.RESPParser` solves this using an incremental state machine:
- Incoming chunks are appended to a contiguous `bytearray` buffer.
- `parse_one()` extracts complete frames. If a CRLF or payload is incomplete, it raises `IncompleteBufferError` and preserves the buffer without state corruption.
- Once a frame is parsed, `del self._buffer[:consumed]` shifts the buffer window.
- Handles inline commands (e.g., plain `PING\r\n` from `telnet` or health probes).

---

## 4. SkipList for Sorted Sets (ZSet)

Sorted Sets require two operations with opposing performance requirements:
1. Fast member score lookup: `ZSCORE key member` -> must be $O(1)$.
2. Fast rank queries and range scans: `ZRANGE key start stop` -> must be $O(\log N + M)$.

`pyredis` implements `RedisZSet` combining:
- A Python dictionary (`member -> float score`) for $O(1)$ lookups.
- A customized **SkipList** with rank spans:
  - Each node contains a `forward` array of pointers across randomized levels (geometric distribution with promotion factor $P = 0.25$ and `MAX_LEVEL = 16`).
  - Each forward pointer maintains a `span` (number of base elements skipped), enabling $O(\log N)$ rank calculation (`ZRANK`) and index range extraction (`ZRANGE`).
  - Doubly-linked backward pointers at level 0 enable bidirectional $O(1)$ range traversal (`ZREVRANGE`).

---

## 5. Expiration & Memory Eviction Engine

Redis does not use timer threads per key because millions of keys would exhaust OS resources. `pyredis` implements Redis's two-tier expiration architecture:

### Tier 1: Passive Expiration (Lazy Deletion)
Whenever a key is accessed (`GET`, `EXISTS`, `HGET`), `_lazy_expire(key)` checks if `now >= expire_at`. If expired, the key is evicted immediately and treated as non-existent.

### Tier 2: Active Probabilistic Sampling Cycle
To reclaim memory for keys that are never queried again, a background coroutine runs every 100ms:
1. Samples 20 random keys with configured TTLs.
2. Deletes all expired keys found in the sample.
3. If more than 25% of the sampled keys were expired, it immediately repeats the cycle.
4. Total execution is capped at 16 iterations to prevent blocking the event loop.

### Eviction Policies (`maxmemory`)
When memory exceeds the configured `maxmemory` threshold:
- `allkeys-lru` / `volatile-lru`: Samples $N$ random keys and evicts the one with the oldest `last_accessed_at` timestamp.
- `allkeys-lfu` / `volatile-lfu`: Samples $N$ keys and evicts the one with the lowest `access_count`.
- `volatile-ttl`: Samples $N$ keys and evicts the key closest to expiration.
- `noeviction`: Fails writes with an `OutOfMemoryError`.

---

## 6. Durability (AOF & RDB Snapshotting)

### Append-Only File (AOF)
- Mutating commands (`SET`, `DEL`, `LPUSH`, `HSET`, etc.) are converted to standard RESP array frames and written to `appendonly.aof`.
- Supports three `fsync` policies:
  - `always`: Syncs to disk after every write.
  - `everysec`: Background thread calls `os.fsync()` once per second.
  - `no`: Delegates flush timing to the OS kernel.

### Online AOF Compaction (Rewrite)
Repeated mutations bloat the AOF log. `pyredis` implements an online rewrite:
1. Iterates over active live keys across all databases.
2. Emits minimal restoration commands into a temporary file (`appendonly.aof.temp.<pid>`).
3. Atomically replaces the active log using `os.replace()` to ensure zero crash-window vulnerability.

---

## 7. Master-Replica Replication Engine

`pyredis` provides real-time distributed replication:

```
    Replica Node                             Master Node
         |                                        |
         | -------- PING -----------------------> |
         | <------- +PONG ----------------------- |
         |                                        |
         | -------- REPLCONF listening-port ----> |
         | <------- +OK ------------------------- |
         |                                        |
         | -------- PSYNC ? -1 -----------------> |
         | <------- +FULLRESYNC <id> 0 ---------- |
         |                                        |
         | <======= Full Dataset Stream ========= | (Reconstructs state)
         |                                        |
         | <======= Real-time Write Stream ====== | (Propagated writes)
         |          (SET, DEL, LPUSH...)          |
```

1. **Read-Only Protection**: Replica nodes strictly reject mutating commands from clients with `-READONLY You can't write against a read only replica.`
2. **Streaming Command Propagation**: When the Master executes any write command, it transparently writes the encoded RESP frame to all connected replica socket descriptors.
3. **Dynamic Promotion**: Issuing `REPLICAOF NO ONE` terminates the master sync task, resets the node role to `master`, and enables write acceptance immediately.
