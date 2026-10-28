# pyredis-engine

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue.svg)](https://python.org)
[![Tests](https://img.shields.io/badge/tests-43%20passed-brightgreen.svg)]()
[![Throughput](https://img.shields.io/badge/throughput-128%2C000%2B%20req%2Fsec-success.svg)]()
[![License](https://img.shields.io/badge/license-MIT-purple.svg)](LICENSE)

A lightweight, from-scratch reimplementation of the core Redis storage engine in pure Python.

No external packages. No C bindings. Just standard Python libraries (`asyncio`, `socket`, `heapq`, `dataclasses`), implementing everything from the raw byte-level RESP protocol to SkipList sorted sets, background AOF log compaction, and live master-replica replication.

---

## Why build this?

Most tutorials on building an in-memory store stop at wrapping a Python `dict` with a simple TCP socket. Real databases have to deal with much harder problems:

- **TCP stream framing:** What happens when client commands are split across packets, or when multiple pipelined commands arrive in a single packet?
- **Sorted sets without balance-tree rebalancing:** How does Redis deliver fast range queries *and* rank queries without complex AVL or Red-Black trees? (Answer: SkipLists with span widths).
- **Managing memory without freezing:** How do you expire millions of keys without running an $O(N)$ sweep that stalls all client requests?
- **Durability without blocking writes:** How do you compact an ever-growing append-only log without stopping live traffic?

I built `pyredis-engine` to deeply understand how these systems puzzles fit together in a working server.

---

## Architecture at a Glance

The server follows Redis's single-threaded event loop design. Because requests are processed sequentially on an `asyncio` event loop, in-memory operations are inherently atomic and require zero locks or mutexes.

```
+-------------------------------------------------------------------------+
|                        Client Layer (RESP2)                             |
|        redis-py  |  pyredis-cli  |  redis-cli  |  Pipelined Batch       |
+------------------------------------+------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                    Asyncio Non-Blocking Event Loop                      |
|                  epoll / kqueue socket multiplexing                     |
+------------------------------------+------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                  Streaming Zero-Copy RESP Parser                        |
|       Handles packet fragmentation, pipelining, and inline commands     |
+------------------------------------+------------------------------------+
                                     |
        +----------------------------+----------------------------+
        |                            |                            |
        v                            v                            v
+----------------+          +----------------+          +-----------------+
|  In-Memory DB  |          | Persistence    |          | Replication     |
|  - Strings     |          | - AOF (fsync)  |          | - PSYNC Handshake|
|  - Hashes      |          | - AOF Rewrite  |          | - Stream writes |
|  - Lists       |          | - RDB Snapshot |          | - Read-only     |
|  - Sets        |          +----------------+          |   enforcement   |
|  - ZSets (SkipList)                                   +-----------------+
+-------+--------+
        |
        v
+-------------------------------------------------------------------------+
|               TTL & Cache Eviction Engine                               |
|   - Passive lazy deletion + 100ms active probabilistic sampling         |
|   - LRU / LFU / TTL eviction policies with maxmemory limits             |
+-------------------------------------------------------------------------+
```

---

## What's Under the Hood?

### 1. True RESP2 Protocol Compatibility
You don't need a special client to talk to this server. You can connect using the official `redis-cli`, `redis-py`, or even `nc` / `telnet`:
```bash
$ redis-cli -p 6379 PING
PONG
```
The streaming parser (`pyredis.protocol.RESPParser`) uses an incremental bytearray state machine so it handles partial packet reads, pipelined requests, and plain text inline commands without desynchronizing.

### 2. SkipList-Backed Sorted Sets (`ZSet`)
Rather than relying on sorted Python lists or simple heaps, sorted sets use a custom probabilistic **SkipList** with rank spans:
- **$O(1)$** score lookup via hash table (`ZSCORE`).
- **$O(\log N)$** rank calculations (`ZRANK`) and range slices (`ZRANGE`) by tracking how many elements each forward pointer skips.
- Doubly-linked base level for $O(1)$ bidirectional step-through (`ZREVRANGE`).

### 3. Two-Tier Expiration & Eviction
To avoid stalling when keys expire:
- **Lazy deletion:** Keys are checked when accessed. If expired, they are cleaned up immediately.
- **Active sampling:** Every 100ms, a background coroutine samples 20 keys with TTLs. If more than 25% are expired, it cleans them and repeats (capped at 16 loops) to keep memory bounded without freezing the event loop.
- **Configurable eviction:** When memory hits `--maxmemory`, it supports `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-lfu`, `volatile-ttl`, or `noeviction`.

### 4. Append-Only Persistence & Live Log Compaction
- Writes mutating commands (`SET`, `HSET`, `DEL`...) to an append-only file (`appendonly.aof`).
- Configurable fsync modes: `always`, `everysec` (background thread), or `no` (OS-managed).
- **AOF Rewrite (Compaction):** When logs grow large, `BGREWRITEAOF` iterates over the live dataset, dumps only minimal creation commands into a temporary file, and uses an atomic POSIX rename (`os.replace`) to swap the log with zero downtime.

### 5. Live Master-Replica Streaming
- Full 3-step replication handshake (`PING` -> `REPLCONF` -> `PSYNC`).
- Master streams commands to all replica sockets as they execute.
- Replicas automatically enforce read-only mode (`-READONLY`).
- Instant promotion with `REPLICAOF NO ONE` if the master goes down.

---

## Performance

Tested on Apple Silicon using the internal async benchmark tool (`benchmarks/benchmark.py`) with 10 concurrent connections and batch pipelining (size 16):

| Operation | Total Requests | Throughput | Avg Latency | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **GET** | 10,000 | **128,810 req/s** | **0.077 ms** | 0.075 ms | **0.119 ms** |
| **INCR** | 10,000 | **118,547 req/s** | **0.083 ms** | 0.080 ms | **0.139 ms** |
| **SET** | 10,000 | **102,710 req/s** | **0.096 ms** | 0.093 ms | **0.155 ms** |

---

## Getting Started

### 1. Run the Server

```bash
# Start on default port 6379
python3 -m pyredis

# Or customize port, enable AOF persistence and 100MB LRU limit
python3 -m pyredis -p 6380 --aof --maxmemory 104857600 --maxmemory-policy allkeys-lru
```

### 2. Try the Built-in CLI

```bash
python3 -m pyredis.cli -p 6379
```

```text
127.0.0.1:6379> SET user:101 "Alice"
"OK"
127.0.0.1:6379> GET user:101
"Alice"
127.0.0.1:6379> ZADD leaderboard 100 alice 250 bob 50 charlie
(integer) 3
127.0.0.1:6379> ZRANGE leaderboard 0 -1 WITHSCORES
1) "charlie"
2) "50"
3) "alice"
4) "100"
5) "bob"
6) "250"
127.0.0.1:6379> EXPIRE user:101 60
(integer) 1
127.0.0.1:6379> TTL user:101
(integer) 59
```

### 3. Use in Python Code

`pyredis` also includes a lightweight client library with pipelining:

```python
from pyredis import Client

client = Client(host="127.0.0.1", port=6379)

# Basic commands
client.set("product:42", "Mechanical Keyboard", ex=300)
item = client.get("product:42")  # b"Mechanical Keyboard"

# Hashes
client.hset("session:xyz", user_id="101", role="admin")
session = client.hgetall("session:xyz")

# Batched pipelining
pipe = client.pipeline()
pipe.set("k1", "v1").set("k2", "v2").incr("counter")
results = pipe.execute()
```

---

## Replication Walkthrough

Open two terminals to see live replication in action:

**Terminal 1 (Master on port 6379):**
```bash
python3 -m pyredis -p 6379
```

**Terminal 2 (Replica on port 6380):**
```bash
python3 -m pyredis -p 6380 --replicaof "127.0.0.1 6379"
```

Now write to the master and observe it on the replica:
```bash
# Write to Master
python3 -m pyredis.cli -p 6379
127.0.0.1:6379> SET message "hello from master"
"OK"

# Read from Replica
python3 -m pyredis.cli -p 6380
127.0.0.1:6380> GET message
"hello from master"

# Replica rejects writes
127.0.0.1:6380> SET message "tamper"
(error) READONLY You can't write against a read only replica.
```

---

## Supported Commands

| Data Type / Domain | Commands |
| :--- | :--- |
| **Strings** | `GET`, `SET` (with `EX`, `PX`, `NX`, `XX`), `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `GETSET` |
| **Hashes** | `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN` |
| **Lists** | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX` |
| **Sets** | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SUNION`, `SINTER` |
| **Sorted Sets (ZSet)** | `ZADD`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZCARD`, `ZREM` |
| **Keyspace / TTL** | `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `TYPE`, `KEYS`, `FLUSHDB`, `DBSIZE`, `SELECT` |
| **Transactions** | `MULTI`, `EXEC`, `DISCARD` |
| **Pub/Sub** | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` |
| **Replication** | `REPLICAOF`, `SLAVEOF`, `PSYNC`, `REPLCONF` |
| **Server** | `PING`, `ECHO`, `INFO`, `TIME`, `COMMAND`, `CLIENT`, `CONFIG`, `BGREWRITEAOF`, `BGSAVE` |

---

## Tests & Benchmarking

```bash
# Run unit and integration tests (43 passed)
make test
# or
python3 -m unittest discover tests

# Run high-concurrency benchmarks
make benchmark
```

For a deeper dive into memory layouts, algorithms, and design trade-offs, check out the [Systems Architecture Deep Dive](docs/ARCHITECTURE.md).

---

## License

MIT License. Feel free to use this for learning, experimentation, or inspiration.
