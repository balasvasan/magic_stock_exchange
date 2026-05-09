#!/usr/bin/env python3
"""
FLOW-SIM — ARGUS Replay Simulator
=================================
PRD reference: §3 (Day 1 setup), Module 1 Lab 1.3.

Replays the synthetic JSONL files into Kafka. Two modes:

  --mode oneshot
        Bulk-loads every event from every JSONL file as fast as Kafka
        will accept. Used on Day 1 to populate Bronze immediately.
        Finishes in 5–15 minutes for 2.5M events on a healthy cluster.

  --mode continuous
        Streams events at a configurable rate (--rate events/sec) in an
        infinite loop. NiFi needs *live* Kafka traffic to demonstrate
        backpressure, DLQ routing, and Module 1 CP-03 throughput.
        Loops over the input files indefinitely; press Ctrl+C to stop.

Runs against the per-student streaming Kafka topics:
    argus.${SID}.orders.v1, argus.${SID}.trades.v1, argus.${SID}.bbo.v1

The CSV-only files (KAVACH/PRATEEK/SMRITI/SEBI/news) are loaded by the
NiFi flows directly via SFTP/REST/file-tail; FLOW-SIM doesn't touch
them.

Topic names are resolved per-student via src.common.naming, so STUDENT_ID
must be set in the environment before running. Two students running
FLOW-SIM concurrently produce to disjoint topic sets and never collide.

Usage:
    # Day 1 bulk-load
    python src/ingest/replay_simulator.py --mode oneshot \\
        --data-dir data/generated/ \\
        --bootstrap-servers ${KAFKA_BROKERS}

    # Module 1 continuous stream at 50K events/sec
    python src/ingest/replay_simulator.py --mode continuous \\
        --data-dir data/generated/ \\
        --bootstrap-servers ${KAFKA_BROKERS} \\
        --rate 50000

    # CP-03 peak-volume test — 150K events/sec for 10 minutes
    python src/ingest/replay_simulator.py --mode continuous \\
        --data-dir data/generated/ \\
        --bootstrap-servers ${KAFKA_BROKERS} \\
        --rate 150000 --duration 600
"""

from __future__ import annotations

import argparse
import gzip
import json
import signal
import sys
import time
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError as exc:  # pragma: no cover
    print("ERROR: kafka-python not installed. Run: pip install kafka-python", file=sys.stderr)
    raise SystemExit(1) from exc

from src.common.naming import topic as resolve_topic, get_student_id


def build_routes() -> list[tuple[str, str, callable]]:
    """Resolve the file → topic → key-extractor mapping at call time.

    Topic names are resolved here (not at import time) because they
    depend on STUDENT_ID, which must be in the environment.

    Each entry: (input_filename, kafka_topic, function-from-event → bytes-key)
    """
    return [
        ("orders_synthetic.jsonl.gz", resolve_topic("orders.v1"),
         lambda e: str(e.get("instrument_code", "")).encode()),
        ("trades_synthetic.jsonl.gz", resolve_topic("trades.v1"),
         lambda e: str(e.get("instrument_code", "")).encode()),
        ("bbo_synthetic.jsonl.gz",    resolve_topic("bbo.v1"),
         lambda e: str(e.get("instrument_code", "")).encode()),
    ]


def open_input(path: Path):
    """Open a .jsonl.gz file or .jsonl file transparently."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def stream_events(path: Path) -> Iterator[dict]:
    """Yield one event dict per line, swallowing malformed records."""
    with open_input(path) as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  [warn] {path.name}:{line_no} skipped — {exc}", file=sys.stderr)


def build_producer(bootstrap_servers: str, linger_ms: int) -> KafkaProducer:
    """Configure for high-throughput batched writes."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="lz4",
        linger_ms=linger_ms,
        batch_size=1 << 16,            # 64 KiB
        max_in_flight_requests_per_connection=5,
        acks="1",                      # leader ack — throughput over durability
        retries=3,
    )


def oneshot(data_dir: Path, bootstrap_servers: str) -> int:
    """Mode 1: bulk-load every event as fast as Kafka accepts."""
    producer = build_producer(bootstrap_servers, linger_ms=20)
    total = 0
    started = time.time()

    for filename, topic_name, key_fn in build_routes():
        path = data_dir / filename
        if not path.exists():
            print(f"==> [skip] {filename} not found in {data_dir}")
            continue
        print(f"==> [oneshot] {filename} → {topic_name}")
        sent = 0
        t0 = time.time()
        for event in stream_events(path):
            producer.send(topic_name, value=event, key=key_fn(event))
            sent += 1
            if sent % 100_000 == 0:
                rate = sent / max(time.time() - t0, 1e-3)
                print(f"      {sent:>10,} events  ({rate:>7,.0f} ev/s)")
        producer.flush()
        rate = sent / max(time.time() - t0, 1e-3)
        print(f"      [done] {sent:,} events at {rate:,.0f} ev/s")
        total += sent

    producer.close()
    elapsed = time.time() - started
    print(f"==> oneshot complete: {total:,} events in {elapsed:.1f}s "
          f"({total / max(elapsed, 1e-3):,.0f} ev/s)")
    return total


def continuous(data_dir: Path, bootstrap_servers: str,
               rate: int, duration: int | None) -> int:
    """Mode 2: infinite loop, rate-limited.

    Uses a token-bucket-style sleep approach. Round-robins across the routes
    so all three streaming topics receive traffic simultaneously, weighted by
    file size (which approximates real-world traffic mix: orders >> trades >> bbo).
    """
    producer = build_producer(bootstrap_servers, linger_ms=10)

    # Open all input files once; loop over each forever, interleaved.
    streams: list[tuple[str, callable, Iterator[dict], Path]] = []
    weights: list[int] = []
    for filename, topic_name, key_fn in build_routes():
        path = data_dir / filename
        if not path.exists():
            print(f"==> [skip] {filename} not found", file=sys.stderr)
            continue
        streams.append((topic_name, key_fn, stream_events(path), path))
        # Approximate weight by file size — orders is biggest by ~10x
        weights.append(max(path.stat().st_size // 1_000_000, 1))

    if not streams:
        print("ERROR: no input files found in --data-dir", file=sys.stderr)
        return 0

    # Compute per-stream target rate from weights
    total_weight = sum(weights)
    per_stream_rate = [int(rate * w / total_weight) for w in weights]
    print(f"==> [continuous] target rate = {rate:,} ev/s; per-topic split:")
    for (topic_name, _, _, _), r in zip(streams, per_stream_rate):
        print(f"      {topic_name:>32}  →  {r:>7,} ev/s")

    started = time.time()
    total = 0
    sent_this_window = [0] * len(streams)
    window_start = time.time()

    # SIGINT handler — flush gracefully on Ctrl+C
    interrupted = {"flag": False}
    def _handle_sigint(_sig, _frame):
        interrupted["flag"] = True
        print("\n==> SIGINT received — flushing producer and exiting...")
    signal.signal(signal.SIGINT, _handle_sigint)

    try:
        while not interrupted["flag"]:
            now = time.time()
            elapsed = now - window_start

            # 1-second windows: pause when we hit the per-stream target
            if elapsed >= 1.0:
                window_start = now
                sent_this_window = [0] * len(streams)
                if duration and (now - started) >= duration:
                    break

            for idx, (topic_name, key_fn, stream_iter, path) in enumerate(streams):
                if sent_this_window[idx] >= per_stream_rate[idx]:
                    continue
                try:
                    event = next(stream_iter)
                except StopIteration:
                    # rewind — replay the file
                    streams[idx] = (topic_name, key_fn, stream_events(path), path)
                    continue
                producer.send(topic_name, value=event, key=key_fn(event))
                sent_this_window[idx] += 1
                total += 1

            # Brief sleep if every stream has hit its quota for the window
            if all(sent_this_window[i] >= per_stream_rate[i] for i in range(len(streams))):
                time.sleep(max(0, 1.0 - (time.time() - window_start)))

            # Status line every 5 seconds
            if total > 0 and total % 250_000 == 0:
                actual_rate = total / max(time.time() - started, 1e-3)
                print(f"      {total:>12,} events sent  ({actual_rate:>7,.0f} ev/s avg)")

    finally:
        producer.flush()
        producer.close()

    elapsed = time.time() - started
    print(f"==> continuous run: {total:,} events in {elapsed:.1f}s "
          f"({total / max(elapsed, 1e-3):,.0f} ev/s avg)")
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="FLOW-SIM — ARGUS replay simulator")
    parser.add_argument("--mode", choices=["oneshot", "continuous"], required=True)
    parser.add_argument("--data-dir", type=Path, required=True,
                        help="Directory containing the synthetic *.jsonl.gz files")
    parser.add_argument("--bootstrap-servers", required=True,
                        help="Comma-separated Kafka broker host:port list")
    parser.add_argument("--rate", type=int, default=50_000,
                        help="Continuous mode: target events/sec (default 50000)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Continuous mode: stop after N seconds (default: infinite)")
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        parser.error(f"--data-dir does not exist or is not a directory: {args.data_dir}")

    print(f"==> FLOW-SIM mode={args.mode}")
    print(f"    student-id: {get_student_id()}")
    print(f"    data-dir:   {args.data_dir}")
    print(f"    brokers:    {args.bootstrap_servers}")
    if args.mode == "continuous":
        print(f"    rate:       {args.rate:,} ev/s")
        if args.duration:
            print(f"    duration:   {args.duration}s")

    if args.mode == "oneshot":
        oneshot(args.data_dir, args.bootstrap_servers)
    else:
        continuous(args.data_dir, args.bootstrap_servers, args.rate, args.duration)
    return 0


if __name__ == "__main__":
    sys.exit(main())
