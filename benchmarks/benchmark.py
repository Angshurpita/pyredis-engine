"""
High-Performance Benchmark Suite for pyredis.
Measures Throughput (requests/sec) and Latency Percentiles (p50, p95, p99, max).
Compares standard sequential vs pipelined throughput.
"""
import argparse
import asyncio
import os
import sys
import time
from typing import List, Tuple

# Ensure pyredis is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyredis.protocol import Array, BulkString, RESPParser, encode_resp


async def run_worker(
    host: str,
    port: int,
    total_ops: int,
    pipeline_size: int,
    command_builder,
) -> List[float]:
    """Execute a stream of commands and measure latency per roundtrip."""
    reader, writer = await asyncio.open_connection(host, port)
    parser = RESPParser()
    latencies: List[float] = []

    remaining = total_ops
    batch_idx = 0

    while remaining > 0:
        current_batch = min(pipeline_size, remaining)
        payload = bytearray()

        t0 = time.perf_counter()
        for i in range(current_batch):
            cmd = command_builder(batch_idx + i)
            payload.extend(encode_resp([c.encode("ascii") if isinstance(c, str) else c for c in cmd]))

        writer.write(payload)
        await writer.drain()

        # Read back responses
        received = 0
        while received < current_batch:
            data = await reader.read(65536)
            if not data:
                break
            parser.feed(data)
            while True:
                frame = parser.parse_one()
                if frame is None:
                    break
                received += 1

        elapsed = (time.perf_counter() - t0) * 1000.0  # ms
        latencies.append(elapsed / current_batch)

        remaining -= current_batch
        batch_idx += current_batch

    writer.close()
    await writer.wait_closed()
    return latencies


def print_stats(title: str, latencies: List[float], total_time: float, total_ops: int):
    rps = total_ops / total_time if total_time > 0 else 0
    latencies.sort()
    n = len(latencies)

    p50 = latencies[int(n * 0.50)] if n else 0
    p90 = latencies[int(n * 0.90)] if n else 0
    p99 = latencies[int(n * 0.99)] if n else 0
    p_max = latencies[-1] if n else 0
    avg = sum(latencies) / n if n else 0

    print(f"\n{'-' * 45}")
    print(f"==> {title}")
    print(f"Total Operations:  {total_ops:,}")
    print(f"Time Taken:        {total_time:.3f} s")
    print(f"Throughput:        {rps:,.0f} requests/sec")
    print(f"Latency Avg:       {avg:.3f} ms")
    print(f"Latency p50:       {p50:.3f} ms")
    print(f"Latency p90:       {p90:.3f} ms")
    print(f"Latency p99:       {p99:.3f} ms")
    print(f"Latency Max:       {p_max:.3f} ms")
    print(f"{'-' * 45}")


async def benchmark_suite(
    host: str,
    port: int,
    num_requests: int = 10000,
    concurrency: int = 10,
    pipeline_size: int = 1,
):
    ops_per_worker = num_requests // concurrency

    actual_total_ops = ops_per_worker * concurrency

    # 1. Benchmark SET
    t0 = time.perf_counter()
    tasks = [
        run_worker(
            host,
            port,
            ops_per_worker,
            pipeline_size,
            lambda i: [b"SET", f"key:{i}".encode("ascii"), b"benchmark_value_100_bytes"],
        )
        for _ in range(concurrency)
    ]
    results = await asyncio.gather(*tasks)
    total_time = time.perf_counter() - t0
    all_latencies = [lat for r in results for lat in r]
    print_stats(
        f"SET (Concurrency: {concurrency}, Pipeline: {pipeline_size})",
        all_latencies,
        total_time,
        actual_total_ops,
    )

    # 2. Benchmark GET
    t0 = time.perf_counter()
    tasks = [
        run_worker(
            host,
            port,
            ops_per_worker,
            pipeline_size,
            lambda i: [b"GET", f"key:{i}".encode("ascii")],
        )
        for _ in range(concurrency)
    ]
    results = await asyncio.gather(*tasks)
    total_time = time.perf_counter() - t0
    all_latencies = [lat for r in results for lat in r]
    print_stats(
        f"GET (Concurrency: {concurrency}, Pipeline: {pipeline_size})",
        all_latencies,
        total_time,
        actual_total_ops,
    )

    # 3. Benchmark INCR
    t0 = time.perf_counter()
    tasks = [
        run_worker(
            host,
            port,
            ops_per_worker,
            pipeline_size,
            lambda i: [b"INCR", b"bench_counter"],
        )
        for _ in range(concurrency)
    ]
    results = await asyncio.gather(*tasks)
    total_time = time.perf_counter() - t0
    all_latencies = [lat for r in results for lat in r]
    print_stats(
        f"INCR (Concurrency: {concurrency}, Pipeline: {pipeline_size})",
        all_latencies,
        total_time,
        actual_total_ops,
    )


def main():
    parser = argparse.ArgumentParser(description="pyredis Benchmark Suite")
    parser.add_argument("-H", "--host", default="127.0.0.1", help="Server host")
    parser.add_argument("-p", "--port", type=int, default=6379, help="Server port")
    parser.add_argument("-n", "--requests", type=int, default=10000, help="Total requests")
    parser.add_argument("-c", "--concurrency", type=int, default=10, help="Concurrent clients")
    parser.add_argument("-P", "--pipeline", type=int, default=1, help="Pipeline batch size")
    args = parser.parse_args()

    print(f"Starting pyredis benchmark on {args.host}:{args.port}")
    print(f"Requests: {args.requests}, Concurrency: {args.concurrency}, Pipeline: {args.pipeline}")
    asyncio.run(benchmark_suite(args.host, args.port, args.requests, args.concurrency, args.pipeline))


if __name__ == "__main__":
    main()
