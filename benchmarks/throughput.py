"""
StreamForge Throughput Benchmark
================================

Measures telemetry event production throughput.

This benchmark is intentionally separate from the production producer
so that performance measurements do not alter the main StreamForge
runtime.

Usage from the backend directory:

    python ..\benchmarks\throughput.py

Or from the project root:

    python benchmarks\throughput.py

The benchmark can operate in two modes:

1. Local generation benchmark
   Measures how quickly Python can generate telemetry events.

2. Kafka benchmark
   Measures actual event publishing throughput to Kafka.

Kafka mode is enabled by default when Kafka is reachable.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------
# Make the backend package importable when this file is executed
# directly from the project root or benchmarks directory.
# ---------------------------------------------------------------------

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
BACKEND_DIR = PROJECT_ROOT / "backend"

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


from streamforge.kafka import (  # noqa: E402
    check_connection,
    close_producer,
    create_producer,
    json_serializer,
    kafka_bootstrap_servers,
    telemetry_topic,
)

from streamforge.producer import (  # noqa: E402
    DeviceSimulator,
    get_device_count,
)


# =====================================================================
# Benchmark result
# =====================================================================

@dataclass
class BenchmarkResult:
    """
    Stores the result of one benchmark run.
    """

    mode: str

    duration_seconds: float

    events: int

    successful_events: int

    failed_events: int

    bytes_processed: int

    events_per_second: float

    megabytes_per_second: float

    average_event_size: float

    minimum_event_time_ms: float

    maximum_event_time_ms: float

    average_event_time_ms: float

    p95_event_time_ms: float

    kafka_bootstrap_servers: Optional[str] = None

    kafka_topic: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the result to a JSON-compatible dictionary.
        """

        return {
            "mode": self.mode,
            "duration_seconds": self.duration_seconds,
            "events": self.events,
            "successful_events": self.successful_events,
            "failed_events": self.failed_events,
            "bytes_processed": self.bytes_processed,
            "events_per_second": self.events_per_second,
            "megabytes_per_second": self.megabytes_per_second,
            "average_event_size": self.average_event_size,
            "minimum_event_time_ms": self.minimum_event_time_ms,
            "maximum_event_time_ms": self.maximum_event_time_ms,
            "average_event_time_ms": self.average_event_time_ms,
            "p95_event_time_ms": self.p95_event_time_ms,
            "kafka_bootstrap_servers": self.kafka_bootstrap_servers,
            "kafka_topic": self.kafka_topic,
        }


# =====================================================================
# Utility functions
# =====================================================================

def percentile(
    values: List[float],
    percentage: float,
) -> float:
    """
    Calculate a percentile without requiring NumPy.

    Uses linear interpolation between observations.
    """

    if not values:
        return 0.0

    ordered = sorted(values)

    if len(ordered) == 1:
        return ordered[0]

    position = (
        (len(ordered) - 1)
        * (percentage / 100.0)
    )

    lower = int(position)
    upper = min(
        lower + 1,
        len(ordered) - 1,
    )

    fraction = position - lower

    return (
        ordered[lower]
        + (
            ordered[upper]
            - ordered[lower]
        )
        * fraction
    )


def format_number(
    value: float,
) -> str:
    """
    Format large numeric values for console output.
    """

    return f"{value:,.2f}"


# =====================================================================
# Device pool
# =====================================================================

class BenchmarkEventGenerator:
    """
    Generates deterministic telemetry events for benchmarking.
    """

    def __init__(
        self,
        device_count: int,
    ) -> None:

        if device_count < 1:
            raise ValueError(
                "device_count must be at least 1."
            )

        self.device_count = device_count

        self.devices = [
            DeviceSimulator(
                device_id=(
                    f"benchmark-device-{index + 1:03d}"
                ),
                seed=5000 + index,
            )
            for index in range(device_count)
        ]

        self._index = 0

    def next_event(
        self,
    ) -> Dict[str, Any]:
        """
        Generate the next telemetry event.
        """

        device = self.devices[
            self._index
        ]

        self._index = (
            self._index + 1
        ) % self.device_count

        return device.generate_event().to_dict()


# =====================================================================
# Local generation benchmark
# =====================================================================

def run_generation_benchmark(
    events: int,
    device_count: int,
) -> BenchmarkResult:
    """
    Benchmark only event generation and JSON serialization.

    This establishes a local upper bound before Kafka/network
    overhead is included.
    """

    generator = (
        BenchmarkEventGenerator(
            device_count
        )
    )

    event_times: List[float] = []

    bytes_processed = 0

    successful = 0

    start = time.perf_counter()

    for _ in range(events):

        event_start = (
            time.perf_counter()
        )

        event = (
            generator.next_event()
        )

        encoded = (
            json_serializer(event)
        )

        event_end = (
            time.perf_counter()
        )

        event_times.append(
            (
                event_end
                - event_start
            )
            * 1000.0
        )

        bytes_processed += len(
            encoded
        )

        successful += 1

    elapsed = (
        time.perf_counter()
        - start
    )

    elapsed = max(
        elapsed,
        1e-9,
    )

    events_per_second = (
        successful
        / elapsed
    )

    megabytes_per_second = (
        bytes_processed
        / elapsed
        / 1024
        / 1024
    )

    average_event_size = (
        bytes_processed
        / successful
        if successful
        else 0.0
    )

    return BenchmarkResult(
        mode="generation",
        duration_seconds=elapsed,
        events=events,
        successful_events=successful,
        failed_events=0,
        bytes_processed=bytes_processed,
        events_per_second=events_per_second,
        megabytes_per_second=megabytes_per_second,
        average_event_size=average_event_size,
        minimum_event_time_ms=(
            min(event_times)
            if event_times
            else 0.0
        ),
        maximum_event_time_ms=(
            max(event_times)
            if event_times
            else 0.0
        ),
        average_event_time_ms=(
            statistics.mean(event_times)
            if event_times
            else 0.0
        ),
        p95_event_time_ms=(
            percentile(
                event_times,
                95.0,
            )
        ),
    )


# =====================================================================
# Kafka benchmark
# =====================================================================

def run_kafka_benchmark(
    events: int,
    device_count: int,
    client_id: str,
) -> BenchmarkResult:
    """
    Benchmark actual Kafka publishing.

    Each telemetry event is published using device_id as the
    Kafka message key so that events from the same device remain
    partition-affine.
    """

    generator = (
        BenchmarkEventGenerator(
            device_count
        )
    )

    producer = None

    event_times: List[float] = []

    bytes_processed = 0

    successful = 0

    failed = 0

    start = time.perf_counter()

    try:

        producer = create_producer(
            client_id=client_id,
        )

        topic = telemetry_topic()

        for _ in range(events):

            event = (
                generator.next_event()
            )

            event_start = (
                time.perf_counter()
            )

            try:

                future = producer.send(
                    topic,
                    key=event[
                        "device_id"
                    ],
                    value=event,
                )

                # Waiting for metadata/ack means this benchmark
                # measures confirmed Kafka publication rather
                # than merely placing records into a local buffer.

                future.get(
                    timeout=30
                )

                event_end = (
                    time.perf_counter()
                )

                event_times.append(
                    (
                        event_end
                        - event_start
                    )
                    * 1000.0
                )

                encoded = (
                    json_serializer(
                        event
                    )
                )

                bytes_processed += len(
                    encoded
                )

                successful += 1

            except Exception as exc:

                failed += 1

                print(
                    (
                        f"Kafka publish failed "
                        f"for event {_ + 1}: {exc}"
                    ),
                    file=sys.stderr,
                )

    finally:

        elapsed = (
            time.perf_counter()
            - start
        )

        if producer is not None:

            close_producer(
                producer
            )

    elapsed = max(
        elapsed,
        1e-9,
    )

    events_per_second = (
        successful
        / elapsed
    )

    megabytes_per_second = (
        bytes_processed
        / elapsed
        / 1024
        / 1024
    )

    average_event_size = (
        bytes_processed
        / successful
        if successful
        else 0.0
    )

    return BenchmarkResult(
        mode="kafka",
        duration_seconds=elapsed,
        events=events,
        successful_events=successful,
        failed_events=failed,
        bytes_processed=bytes_processed,
        events_per_second=events_per_second,
        megabytes_per_second=megabytes_per_second,
        average_event_size=average_event_size,
        minimum_event_time_ms=(
            min(event_times)
            if event_times
            else 0.0
        ),
        maximum_event_time_ms=(
            max(event_times)
            if event_times
            else 0.0
        ),
        average_event_time_ms=(
            statistics.mean(event_times)
            if event_times
            else 0.0
        ),
        p95_event_time_ms=(
            percentile(
                event_times,
                95.0,
            )
        ),
        kafka_bootstrap_servers=(
            kafka_bootstrap_servers()
        ),
        kafka_topic=topic,
    )


# =====================================================================
# Result printing
# =====================================================================

def print_result(
    result: BenchmarkResult,
) -> None:
    """
    Print a human-readable benchmark report.
    """

    print()
    print("=" * 70)
    print("StreamForge Throughput Benchmark")
    print("=" * 70)

    print(
        f"Mode:                 {result.mode}"
    )

    if result.kafka_topic:

        print(
            f"Kafka bootstrap:      "
            f"{result.kafka_bootstrap_servers}"
        )

        print(
            f"Kafka topic:          "
            f"{result.kafka_topic}"
        )

    print(
        f"Requested events:     "
        f"{result.events:,}"
    )

    print(
        f"Successful events:    "
        f"{result.successful_events:,}"
    )

    print(
        f"Failed events:        "
        f"{result.failed_events:,}"
    )

    print(
        f"Duration:             "
        f"{result.duration_seconds:.3f} s"
    )

    print(
        f"Throughput:           "
        f"{format_number(result.events_per_second)} events/s"
    )

    print(
        f"Data throughput:      "
        f"{format_number(result.megabytes_per_second)} MB/s"
    )

    print(
        f"Average event size:   "
        f"{result.average_event_size:.2f} bytes"
    )

    print()
    print("Per-event timing")
    print("-" * 70)

    print(
        f"Minimum:              "
        f"{result.minimum_event_time_ms:.3f} ms"
    )

    print(
        f"Average:              "
        f"{result.average_event_time_ms:.3f} ms"
    )

    print(
        f"P95:                  "
        f"{result.p95_event_time_ms:.3f} ms"
    )

    print(
        f"Maximum:              "
        f"{result.maximum_event_time_ms:.3f} ms"
    )

    print("=" * 70)


# =====================================================================
# JSON output
# =====================================================================

def save_result(
    result: BenchmarkResult,
    output_path: str,
) -> None:
    """
    Save benchmark results as JSON.
    """

    output = Path(
        output_path
    )

    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            result.to_dict(),
            file,
            indent=2,
        )

    print(
        f"Benchmark result saved to: {output}"
    )


# =====================================================================
# Kafka availability
# =====================================================================

def kafka_available() -> bool:
    """
    Check whether Kafka is reachable.
    """

    try:

        return bool(
            check_connection()
        )

    except Exception:

        return False


# =====================================================================
# Self-test
# =====================================================================

def run_self_test() -> None:
    """
    Validate benchmark functionality without requiring Kafka.
    """

    print(
        "StreamForge throughput benchmark self-test"
    )

    print(
        "-" * 60
    )

    # --------------------------------------------------------------
    # Generator
    # --------------------------------------------------------------

    generator = (
        BenchmarkEventGenerator(
            device_count=3
        )
    )

    event = (
        generator.next_event()
    )

    assert isinstance(
        event,
        dict,
    )

    assert (
        "event_id"
        in event
    )

    assert (
        "device_id"
        in event
    )

    assert (
        "timestamp"
        in event
    )

    assert (
        "temperature"
        in event
    )

    print(
        "Benchmark event generation: OK"
    )

    # --------------------------------------------------------------
    # Serialization
    # --------------------------------------------------------------

    encoded = (
        json_serializer(event)
    )

    assert isinstance(
        encoded,
        bytes,
    )

    assert len(encoded) > 0

    print(
        "Benchmark serialization: OK"
    )

    # --------------------------------------------------------------
    # Generation benchmark
    # --------------------------------------------------------------

    result = run_generation_benchmark(
        events=100,
        device_count=3,
    )

    assert (
        result.mode
        == "generation"
    )

    assert (
        result.events
        == 100
    )

    assert (
        result.successful_events
        == 100
    )

    assert (
        result.failed_events
        == 0
    )

    assert (
        result.events_per_second
        > 0
    )

    assert (
        result.bytes_processed
        > 0
    )

    print(
        "Generation benchmark: OK"
    )

    # --------------------------------------------------------------
    # Percentile
    # --------------------------------------------------------------

    values = [
        1.0,
        2.0,
        3.0,
        4.0,
        5.0,
    ]

    p95 = percentile(
        values,
        95.0,
    )

    assert (
        4.8
        <= p95
        <= 5.0
    )

    print(
        "Percentile calculation: OK"
    )

    # --------------------------------------------------------------
    # Result serialization
    # --------------------------------------------------------------

    result_dict = (
        result.to_dict()
    )

    assert isinstance(
        result_dict,
        dict,
    )

    encoded_result = json.dumps(
        result_dict
    )

    assert len(
        encoded_result
    ) > 0

    print(
        "Benchmark result serialization: OK"
    )

    # --------------------------------------------------------------
    # Device count configuration
    # --------------------------------------------------------------

    assert (
        get_device_count()
        > 0
    )

    print(
        "Producer configuration integration: OK"
    )

    # --------------------------------------------------------------
    # Final
    # --------------------------------------------------------------

    print(
        "-" * 60
    )

    print(
        "StreamForge throughput benchmark self-test: OK"
    )


# =====================================================================
# Command-line interface
# =====================================================================

def build_argument_parser() -> argparse.ArgumentParser:
    """
    Create command-line argument parser.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Benchmark StreamForge telemetry "
            "generation and Kafka throughput."
        )
    )

    parser.add_argument(
        "--mode",
        choices=[
            "generation",
            "kafka",
        ],
        default="generation",
        help=(
            "Benchmark mode. "
            "Default: generation"
        ),
    )

    parser.add_argument(
        "--events",
        type=int,
        default=10_000,
        help=(
            "Number of events to benchmark. "
            "Default: 10000"
        ),
    )

    parser.add_argument(
        "--devices",
        type=int,
        default=None,
        help=(
            "Number of simulated devices. "
            "Defaults to StreamForge producer configuration."
        ),
    )

    parser.add_argument(
        "--client-id",
        default=(
            "streamforge-throughput-benchmark"
        ),
        help=(
            "Kafka client ID used in Kafka mode."
        ),
    )

    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Optional JSON output path."
        ),
    )

    parser.add_argument(
        "--check-kafka",
        action="store_true",
        help=(
            "Check Kafka connectivity before running."
        ),
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Run benchmark self-test."
        ),
    )

    return parser


# =====================================================================
# Main
# =====================================================================

def main() -> None:
    """
    Command-line entry point.
    """

    parser = (
        build_argument_parser()
    )

    args = parser.parse_args()

    if args.self_test:

        run_self_test()

        return

    if args.events < 1:

        parser.error(
            "--events must be at least 1."
        )

    device_count = (
        args.devices
        if args.devices is not None
        else get_device_count()
    )

    if device_count < 1:

        parser.error(
            "--devices must be at least 1."
        )

    print(
        "Starting StreamForge throughput benchmark..."
    )

    print(
        f"Project root: {PROJECT_ROOT}"
    )

    print(
        f"Events:       {args.events:,}"
    )

    print(
        f"Devices:      {device_count}"
    )

    if args.check_kafka:

        print(
            "Checking Kafka connectivity..."
        )

        if not kafka_available():

            print(
                "Kafka is not reachable."
            )

            print(
                "Use --mode generation for a "
                "Kafka-independent benchmark."
            )

            raise SystemExit(1)

        print(
            "Kafka connectivity: OK"
        )

    if args.mode == "kafka":

        if not kafka_available():

            print()
            print(
                "Kafka is not reachable."
            )

            print(
                "Make sure the StreamForge Kafka "
                "container is running."
            )

            raise SystemExit(1)

        result = run_kafka_benchmark(
            events=args.events,
            device_count=device_count,
            client_id=args.client_id,
        )

    else:

        result = run_generation_benchmark(
            events=args.events,
            device_count=device_count,
        )

    print_result(
        result
    )

    if args.output:

        save_result(
            result,
            args.output,
        )


if __name__ == "__main__":
    main()