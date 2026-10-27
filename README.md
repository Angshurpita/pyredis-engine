# pyredis

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue.svg)](https://python.org)
[![Tests](https://img.shields.io/badge/tests-43%20passed-brightgreen.svg)]()
[![Throughput](https://img.shields.io/badge/throughput-128%2C000%2B%20req%2Fsec-success.svg)]()
[![License](https://img.shields.io/badge/license-MIT-purple.svg)](LICENSE)

A high-performance in-memory key-value database built from scratch in Python with zero external dependencies. Designed following modern production database architecture, complete with an **Asyncio event loop**, **SkipList-backed Sorted Sets**, **RESP2 wire protocol**, **Append-Only File (AOF) persistence with online compaction**, and **real-time Master-Replica streaming replication**.

---

## Architecture Diagram

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

## Performance Benchmarks

Measured on Apple Silicon using the built-in async benchmark harness (`benchmarks/benchmark.py`) with 10 concurrent clients and a pipeline batch size of 16:

| Command | Total Operations | Throughput | Avg Latency | p50 Latency | p90 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **GET** | 10,000 | **128,810 req/sec** | **0.077 ms** | 0.075 ms | 0.084 ms | **0.119 ms** |
| **INCR** | 10,000 | **118,547 req/sec** | **0.083 ms** | 0.080 ms | 0.093 ms | **0.139 ms** |
| **SET** | 10,000 | **102,710 req/sec** | **0.096 ms** | 0.093 ms | 0.111 ms | **0.155 ms** |

---

## Key Differentiators & Features

1. **Zero External Dependencies**: Implemented strictly using the Python standard library (`asyncio`, `socket`, `heapq`, `dataclasses`).
2. **True RESP2 Protocol Compliance**: Compatible with standard Redis clients (like `redis-py` or `redis-cli`) and telnet/netcat.
3. **SkipList for Sorted Sets (ZSet)**: Implements a custom probabilistic SkipList with pointer spans, providing $O(\log N)$ insert, rank lookup (`ZRANK`), and range queries (`ZRANGE`).
4. **Two-Tier Expiration Engine**:
   - **Passive (Lazy)**: Deletes expired keys on read.
   - **Active Probabilistic Sampling**: Runs every 100ms, sampling volatile keys and bounding memory usage without $O(N)$ keyspace scans.
5. **Configurable Memory Eviction Policies**: `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-lfu`, `volatile-ttl`, and `noeviction` with `maxmemory` threshold.
6. **Append-Only File (AOF) & Online Compaction**:
   - Configurable fsync policies: `always`, `everysec`, `no`.
   - Online rewrite compacts mutating history into minimal generation commands using atomic POSIX rename.
7. **Master-Replica Distributed Replication**:
   - Full 3-phase handshake (`PING` -> `REPLCONF` -> `PSYNC`).
   - Streams live mutating commands to all replica sockets in real-time.
   - Read-only protection for replicas and dynamic failover (`REPLICAOF NO ONE`).
8. **Transactions & Pub/Sub**:
   - Atomic transaction blocks with `MULTI`, `EXEC`, and `DISCARD`.
   - High-throughput channel broadcasting with `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`.

---

## Quickstart

### 1. Start the Server

```bash
# Default on port 6379
python3 -m pyredis

# With custom port, AOF persistence, and LRU eviction limit
python3 -m pyredis -p 6380 --aof --maxmemory 104857600 --maxmemory-policy allkeys-lru
```

### 2. Connect via the Interactive REPL (`pyredis-cli`)

```bash
python3 -m pyredis.cli -p 6379
```

```text
127.0.0.1:6379> SET user:1 "Alice"
"OK"
127.0.0.1:6379> GET user:1
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
127.0.0.1:6379> EXPIRE user:1 60
(integer) 1
127.0.0.1:6379> TTL user:1
(integer) 59
```

### 3. Use the Python Client Library

```python
from pyredis import Client

client = Client(host="127.0.0.1", port=6379)

# String operations
client.set("product:42", "Laptop", ex=300)
name = client.get("product:42")  # b"Laptop"

# Hashes
client.hset("session:xyz", user_id="100", role="admin")
session = client.hgetall("session:xyz")

# Pipelining (batch roundtrips)
pipe = client.pipeline()
pipe.set("k1", "v1").set("k2", "v2").incr("counter")
results = pipe.execute()
```

---

## Master-Replica Replication Demo

Start Master on port `6379`:
```bash
python3 -m pyredis -p 6379
```

Start Replica on port `6380` pointing to Master:
```bash
python3 -m pyredis -p 6380 --replicaof "127.0.0.1 6379"
```

Write to Master:
```bash
python3 -m pyredis.cli -p 6379
127.0.0.1:6379> SET status "operational"
"OK"
```

Read instantly from Replica:
```bash
python3 -m pyredis.cli -p 6380
127.0.0.1:6380> GET status
"operational"
127.0.0.1:6380> SET status "hacked"
(error) READONLY You can't write against a read only replica.
```

---

## Supported Commands

| Category | Commands |
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

## Running Tests & Benchmarks

```bash
# Run all unit and integration tests (43 tests)
make test

# Run high-concurrency benchmark
make benchmark
```

---

## Documentation

- [Systems Architecture Deep Dive](docs/ARCHITECTURE.md)
